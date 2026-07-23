use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use sinan_execution::ServerClock;
use sinan_store::{
    ClaimDurableWork, CompleteInboundAdmission, CompleteSessionResumeAdmission, DeadletterReason,
    DurableInboundAdmissionOutcome, DurableSessionResumeAdmissionOutcome, FailInboundAdmission,
    FailSessionResumeAdmission, NewDeadletterEvent, NewInboundAdmission, NewSessionResumeAdmission,
    ReclaimDurableWork, SqliteStateStore, StoreError, StoredInboundAdmission,
    StoredSessionResumeAdmission, WriteTransaction,
};
use sinan_types::RequestId;
use thiserror::Error;

use crate::{
    AuthenticatedSessionContext, InboundAdmission, InboundAdmissionError, InboundAdmissionFuture,
    InboundMessage, InboundMessagePort, SessionResumeError, SessionResumeFuture, SessionResumePort,
    SessionResumeRequest,
};

#[derive(Clone)]
pub struct DurableInboundMessagePort {
    store: SqliteStateStore,
}

impl DurableInboundMessagePort {
    pub const fn new(store: SqliteStateStore) -> Self {
        Self { store }
    }
}

impl InboundMessagePort for DurableInboundMessagePort {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        message: InboundMessage,
    ) -> InboundAdmissionFuture<'a> {
        Box::pin(async move {
            let sequence = message.envelope.sequence.ok_or_else(|| {
                InboundAdmissionError::new("durable inbound message has no sequence")
            })?;
            let raw = std::str::from_utf8(&message.wire_bytes)
                .map_err(|_| InboundAdmissionError::new("durable inbound message is not UTF-8"))?;
            let envelope = sinan_store::CanonicalJson::parse(raw)
                .map_err(|error| inbound_error("canonicalize inbound envelope", error))?;
            let raw_payload_length = u64::try_from(message.wire_bytes.len()).map_err(|_| {
                InboundAdmissionError::new("durable inbound message length exceeds u64")
            })?;
            let outcome = self
                .store
                .admit_inbound(NewInboundAdmission {
                    message_id: message.envelope.message_id,
                    session_id: session.session_id.clone(),
                    client_id: session.client_id.clone(),
                    account_id: session.account_id.clone(),
                    terminal_id: session.terminal_id.clone(),
                    message_type: message.envelope.message_type.to_string(),
                    schema_version: message.envelope.schema_version,
                    sequence,
                    correlation_id: message.envelope.correlation_id,
                    causation_id: message.envelope.causation_id,
                    envelope,
                    raw_payload_length: Some(raw_payload_length),
                    received_at: message.received_at,
                    created_at: message.received_at,
                })
                .await
                .map_err(|error| inbound_error("persist inbound admission", error))?;
            Ok(match outcome {
                DurableInboundAdmissionOutcome::Accepted(_) => InboundAdmission::Accepted,
                DurableInboundAdmissionOutcome::Duplicate(_) => InboundAdmission::Duplicate,
                DurableInboundAdmissionOutcome::Rejected(rejection) => InboundAdmission::Rejected {
                    reason: rejection.reason,
                },
            })
        })
    }
}

#[derive(Clone)]
pub struct DurableSessionResumePort {
    store: SqliteStateStore,
}

impl DurableSessionResumePort {
    pub const fn new(store: SqliteStateStore) -> Self {
        Self { store }
    }
}

impl SessionResumePort for DurableSessionResumePort {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        request: SessionResumeRequest,
    ) -> SessionResumeFuture<'a> {
        Box::pin(async move {
            let cursor = sinan_store::CanonicalJson::from_serializable(&request.cursor)
                .map_err(|error| resume_error("canonicalize resume cursor", error))?;
            let outcome = self
                .store
                .admit_session_resume(NewSessionResumeAdmission {
                    hello_message_id: request.hello_message_id,
                    session_id: session.session_id.clone(),
                    client_id: session.client_id.clone(),
                    account_id: session.account_id.clone(),
                    terminal_id: session.terminal_id.clone(),
                    cursor,
                    received_at: request.received_at,
                    created_at: request.received_at,
                })
                .await
                .map_err(|error| resume_error("persist session resume admission", error))?;
            match outcome {
                DurableSessionResumeAdmissionOutcome::Accepted(_)
                | DurableSessionResumeAdmissionOutcome::Duplicate(_) => Ok(()),
            }
        })
    }
}

fn inbound_error(operation: &str, error: StoreError) -> InboundAdmissionError {
    InboundAdmissionError::new(format!("{operation}: {error}"))
}

fn resume_error(operation: &str, error: StoreError) -> SessionResumeError {
    SessionResumeError::new(format!("{operation}: {error}"))
}

pub type DurableInboundHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), DurableRecoveryHandlerError>> + Send + 'a>>;

pub trait DurableInboundHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        transaction: &'a mut WriteTransaction,
        admission: &'a StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SessionResumeHandlingOutcome {
    pub reconciliation_request_id: Option<RequestId>,
}

pub type DurableSessionResumeHandlerFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<SessionResumeHandlingOutcome, DurableRecoveryHandlerError>>
            + Send
            + 'a,
    >,
>;

pub trait DurableSessionResumeHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        transaction: &'a mut WriteTransaction,
        admission: &'a StoredSessionResumeAdmission,
    ) -> DurableSessionResumeHandlerFuture<'a>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableRecoveryFailureClass {
    TerminalDeadletter(DeadletterReason),
    RetryableInfrastructure,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("durable recovery handler failed: {message}")]
pub struct DurableRecoveryHandlerError {
    class: DurableRecoveryFailureClass,
    message: String,
}

impl DurableRecoveryHandlerError {
    pub fn terminal_deadletter(reason: DeadletterReason, message: impl Into<String>) -> Self {
        Self {
            class: DurableRecoveryFailureClass::TerminalDeadletter(reason),
            message: message.into(),
        }
    }

    pub fn retryable_infrastructure(message: impl Into<String>) -> Self {
        Self {
            class: DurableRecoveryFailureClass::RetryableInfrastructure,
            message: message.into(),
        }
    }

    pub const fn class(&self) -> DurableRecoveryFailureClass {
        self.class
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRecoveryConfig {
    pub worker_id: String,
    pub max_items_per_batch: usize,
    pub lease_duration: Duration,
    pub handler_timeout: Duration,
    pub finalization_budget: Duration,
}

impl DurableRecoveryConfig {
    pub fn validate(&self) -> Result<(), DurableRecoveryError> {
        if self.worker_id.trim().is_empty() {
            return Err(DurableRecoveryError::InvalidConfig("worker_id"));
        }
        if self.max_items_per_batch == 0 {
            return Err(DurableRecoveryError::InvalidConfig("max_items_per_batch"));
        }
        let lease_ms = duration_ms(self.lease_duration)?;
        if lease_ms == 0 {
            return Err(DurableRecoveryError::InvalidConfig(
                "lease_duration must be at least one millisecond",
            ));
        }
        let handler_ms = duration_ms(self.handler_timeout)?;
        if handler_ms == 0 {
            return Err(DurableRecoveryError::InvalidConfig(
                "handler_timeout must be at least one millisecond",
            ));
        }
        let finalization_ms = duration_ms(self.finalization_budget)?;
        if finalization_ms == 0 {
            return Err(DurableRecoveryError::InvalidConfig(
                "finalization_budget must be at least one millisecond",
            ));
        }
        let reserved = self
            .handler_timeout
            .checked_add(self.finalization_budget)
            .ok_or(DurableRecoveryError::TimestampOverflow)?;
        let persisted_lease = Duration::from_millis(
            u64::try_from(lease_ms).map_err(|_| DurableRecoveryError::TimestampOverflow)?,
        );
        if reserved > persisted_lease {
            return Err(DurableRecoveryError::InvalidConfig(
                "handler_timeout plus finalization_budget must not exceed lease_duration",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DurableRecoveryBatchReport {
    pub claimed: usize,
    pub reclaimed: usize,
    pub handled: usize,
    pub failed: usize,
    pub timed_out: usize,
}

#[derive(Debug, Error)]
pub enum DurableRecoveryError {
    #[error("invalid durable recovery configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("durable recovery timestamp overflow")]
    TimestampOverflow,

    #[error("durable recovery server clock returned an invalid timestamp: {0}")]
    InvalidServerTime(i64),

    #[error("durable recovery handler reported retryable infrastructure failure: {0}")]
    RetryableHandler(String),

    #[error(transparent)]
    Store(#[from] StoreError),
}

pub struct DurableRecoveryDispatcher {
    store: SqliteStateStore,
    inbound_handler: Arc<dyn DurableInboundHandler>,
    resume_handler: Arc<dyn DurableSessionResumeHandler>,
    clock: Arc<dyn ServerClock>,
    config: DurableRecoveryConfig,
}

impl DurableRecoveryDispatcher {
    pub fn new(
        store: SqliteStateStore,
        inbound_handler: Arc<dyn DurableInboundHandler>,
        resume_handler: Arc<dyn DurableSessionResumeHandler>,
        clock: Arc<dyn ServerClock>,
        config: DurableRecoveryConfig,
    ) -> Result<Self, DurableRecoveryError> {
        config.validate()?;
        Ok(Self {
            store,
            inbound_handler,
            resume_handler,
            clock,
            config,
        })
    }

    /// Claims and handles at most `max_items_per_batch` records.
    ///
    /// A process crash leaves the current row in `PROCESSING`; a later call at
    /// or after its lease deadline reclaims that row with a new revision.
    pub async fn dispatch_batch(&self) -> Result<DurableRecoveryBatchReport, DurableRecoveryError> {
        let mut report = DurableRecoveryBatchReport::default();
        for index in 0..self.config.max_items_per_batch {
            // Claim and transaction acquisition consume the handler window so
            // the configured finalization budget remains inside the lease.
            let lease_started_at = tokio::time::Instant::now();
            let handler_deadline = lease_started_at
                .checked_add(self.config.handler_timeout)
                .ok_or(DurableRecoveryError::TimestampOverflow)?;
            let claimed_at = self.server_now()?;
            let lease_expires_at = claimed_at
                .checked_add(duration_ms(self.config.lease_duration)?)
                .ok_or(DurableRecoveryError::TimestampOverflow)?;
            let prefer_inbound = index % 2 == 0;
            let item = if prefer_inbound {
                match self.claim_inbound(claimed_at, lease_expires_at).await? {
                    Some(item) => Some(RecoveryItem::Inbound(item)),
                    None => self
                        .claim_resume(claimed_at, lease_expires_at)
                        .await?
                        .map(RecoveryItem::Resume),
                }
            } else {
                match self.claim_resume(claimed_at, lease_expires_at).await? {
                    Some(item) => Some(RecoveryItem::Resume(item)),
                    None => self
                        .claim_inbound(claimed_at, lease_expires_at)
                        .await?
                        .map(RecoveryItem::Inbound),
                }
            };
            let Some(item) = item else {
                break;
            };
            report.claimed += 1;
            report.reclaimed += usize::from(item.was_reclaimed());
            self.handle_item(item, handler_deadline, &mut report)
                .await?;
        }
        Ok(report)
    }

    async fn claim_inbound(
        &self,
        now: i64,
        lease_expires_at: i64,
    ) -> Result<Option<ClaimedInbound>, StoreError> {
        if let Some(admission) = self
            .store
            .reclaim_expired_inbound(ReclaimDurableWork {
                worker_id: self.config.worker_id.clone(),
                reclaimed_at: now,
                lease_expires_at,
            })
            .await?
        {
            return Ok(Some(ClaimedInbound {
                admission,
                reclaimed: true,
            }));
        }
        Ok(self
            .store
            .claim_next_inbound(ClaimDurableWork {
                worker_id: self.config.worker_id.clone(),
                claimed_at: now,
                lease_expires_at,
            })
            .await?
            .map(|admission| ClaimedInbound {
                admission,
                reclaimed: false,
            }))
    }

    async fn claim_resume(
        &self,
        now: i64,
        lease_expires_at: i64,
    ) -> Result<Option<ClaimedResume>, StoreError> {
        if let Some(admission) = self
            .store
            .reclaim_expired_session_resume(ReclaimDurableWork {
                worker_id: self.config.worker_id.clone(),
                reclaimed_at: now,
                lease_expires_at,
            })
            .await?
        {
            return Ok(Some(ClaimedResume {
                admission,
                reclaimed: true,
            }));
        }
        Ok(self
            .store
            .claim_next_session_resume(ClaimDurableWork {
                worker_id: self.config.worker_id.clone(),
                claimed_at: now,
                lease_expires_at,
            })
            .await?
            .map(|admission| ClaimedResume {
                admission,
                reclaimed: false,
            }))
    }

    async fn handle_item(
        &self,
        item: RecoveryItem,
        handler_deadline: tokio::time::Instant,
        report: &mut DurableRecoveryBatchReport,
    ) -> Result<(), DurableRecoveryError> {
        match item {
            RecoveryItem::Inbound(item) => {
                let mut transaction = self.store.begin_write().await?;
                let outcome = tokio::time::timeout_at(
                    handler_deadline,
                    self.inbound_handler
                        .handle(&mut transaction, &item.admission),
                )
                .await;
                match outcome {
                    Ok(Ok(())) => {
                        let completed_at = match self.server_now() {
                            Ok(completed_at) => completed_at,
                            Err(error) => {
                                transaction.rollback().await?;
                                return Err(error);
                            }
                        };
                        let completion = transaction
                            .complete_inbound(CompleteInboundAdmission {
                                message_id: item.admission.message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                completed_at,
                            })
                            .await;
                        if let Err(error) = completion {
                            transaction.rollback().await?;
                            return Err(error.into());
                        }
                        transaction.commit().await?;
                        report.handled += 1;
                    }
                    Ok(Err(error)) => {
                        transaction.rollback().await?;
                        match error.class() {
                            DurableRecoveryFailureClass::TerminalDeadletter(reason) => {
                                self.fail_terminal_inbound(&item.admission, reason, error)
                                    .await?;
                                report.failed += 1;
                            }
                            DurableRecoveryFailureClass::RetryableInfrastructure => {
                                return Err(DurableRecoveryError::RetryableHandler(bounded_error(
                                    error.to_string(),
                                )));
                            }
                        }
                    }
                    Err(_) => {
                        transaction.rollback().await?;
                        let failed_at = self.server_now()?;
                        self.store
                            .fail_inbound(FailInboundAdmission {
                                message_id: item.admission.message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                failed_at,
                                error: "durable recovery handler timed out".to_owned(),
                            })
                            .await?;
                        report.failed += 1;
                        report.timed_out += 1;
                    }
                }
            }
            RecoveryItem::Resume(item) => {
                let mut transaction = self.store.begin_write().await?;
                let outcome = tokio::time::timeout_at(
                    handler_deadline,
                    self.resume_handler
                        .handle(&mut transaction, &item.admission),
                )
                .await;
                match outcome {
                    Ok(Ok(outcome)) => {
                        let completed_at = match self.server_now() {
                            Ok(completed_at) => completed_at,
                            Err(error) => {
                                transaction.rollback().await?;
                                return Err(error);
                            }
                        };
                        let completion = transaction
                            .complete_session_resume(CompleteSessionResumeAdmission {
                                hello_message_id: item.admission.hello_message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                completed_at,
                                reconciliation_request_id: outcome.reconciliation_request_id,
                            })
                            .await;
                        if let Err(error) = completion {
                            transaction.rollback().await?;
                            return Err(error.into());
                        }
                        transaction.commit().await?;
                        report.handled += 1;
                    }
                    Ok(Err(error)) => {
                        transaction.rollback().await?;
                        match error.class() {
                            DurableRecoveryFailureClass::TerminalDeadletter(_) => {
                                let failed_at = self.server_now()?;
                                self.store
                                    .fail_session_resume(FailSessionResumeAdmission {
                                        hello_message_id: item.admission.hello_message_id,
                                        expected_revision: item.admission.revision,
                                        worker_id: self.config.worker_id.clone(),
                                        failed_at,
                                        error: bounded_error(error.to_string()),
                                    })
                                    .await?;
                                report.failed += 1;
                            }
                            DurableRecoveryFailureClass::RetryableInfrastructure => {
                                return Err(DurableRecoveryError::RetryableHandler(bounded_error(
                                    error.to_string(),
                                )));
                            }
                        }
                    }
                    Err(_) => {
                        transaction.rollback().await?;
                        let failed_at = self.server_now()?;
                        self.store
                            .fail_session_resume(FailSessionResumeAdmission {
                                hello_message_id: item.admission.hello_message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                failed_at,
                                error: "durable recovery handler timed out".to_owned(),
                            })
                            .await?;
                        report.failed += 1;
                        report.timed_out += 1;
                    }
                }
            }
        }
        Ok(())
    }

    async fn fail_terminal_inbound(
        &self,
        admission: &StoredInboundAdmission,
        reason: DeadletterReason,
        handler_error: DurableRecoveryHandlerError,
    ) -> Result<(), DurableRecoveryError> {
        let failed_at = self.server_now()?;
        let error = bounded_error(handler_error.to_string());
        let mut transaction = self.store.begin_write().await?;
        let deadletter = transaction
            .append_deadletter_event(NewDeadletterEvent {
                deadletter_id: durable_inbound_deadletter_id(admission),
                message_id: Some(admission.message_id.clone()),
                message_type: Some(admission.message_type.clone()),
                schema_version: Some(admission.schema_version.clone()),
                reason,
                source: "trading-core.durable-recovery".to_owned(),
                raw_payload: None,
                raw_payload_length: admission.raw_payload_length,
                error_message: error.clone(),
                received_at: admission.received_at,
                created_at: failed_at,
            })
            .await;
        if let Err(store_error) = deadletter {
            transaction.rollback().await?;
            return Err(store_error.into());
        }
        let failure = transaction
            .fail_inbound(FailInboundAdmission {
                message_id: admission.message_id.clone(),
                expected_revision: admission.revision,
                worker_id: self.config.worker_id.clone(),
                failed_at,
                error,
            })
            .await;
        if let Err(store_error) = failure {
            transaction.rollback().await?;
            return Err(store_error.into());
        }
        transaction.commit().await?;
        Ok(())
    }

    fn server_now(&self) -> Result<i64, DurableRecoveryError> {
        let now = self.clock.now_ms();
        if now < 0 {
            Err(DurableRecoveryError::InvalidServerTime(now))
        } else {
            Ok(now)
        }
    }
}

fn durable_inbound_deadletter_id(admission: &StoredInboundAdmission) -> String {
    format!("durable-inbound:{}", admission.message_id)
}

fn bounded_error(mut error: String) -> String {
    const MAX_ERROR_BYTES: usize = 1_024;
    if error.len() <= MAX_ERROR_BYTES {
        return error;
    }
    let mut boundary = MAX_ERROR_BYTES;
    while !error.is_char_boundary(boundary) {
        boundary -= 1;
    }
    error.truncate(boundary);
    error
}

fn duration_ms(duration: Duration) -> Result<i64, DurableRecoveryError> {
    i64::try_from(duration.as_millis()).map_err(|_| DurableRecoveryError::TimestampOverflow)
}

struct ClaimedInbound {
    admission: StoredInboundAdmission,
    reclaimed: bool,
}

struct ClaimedResume {
    admission: StoredSessionResumeAdmission,
    reclaimed: bool,
}

enum RecoveryItem {
    Inbound(ClaimedInbound),
    Resume(ClaimedResume),
}

impl RecoveryItem {
    const fn was_reclaimed(&self) -> bool {
        match self {
            Self::Inbound(item) => item.reclaimed,
            Self::Resume(item) => item.reclaimed,
        }
    }
}

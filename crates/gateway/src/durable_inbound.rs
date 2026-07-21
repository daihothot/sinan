use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use sinan_store::{
    ClaimDurableWork, CompleteInboundAdmission, CompleteSessionResumeAdmission,
    DurableInboundAdmissionOutcome, DurableSessionResumeAdmissionOutcome, FailInboundAdmission,
    FailSessionResumeAdmission, NewInboundAdmission, NewSessionResumeAdmission, ReclaimDurableWork,
    SqliteStateStore, StoreError, StoredInboundAdmission, StoredSessionResumeAdmission,
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
        admission: &'a StoredSessionResumeAdmission,
    ) -> DurableSessionResumeHandlerFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("durable recovery handler failed: {message}")]
pub struct DurableRecoveryHandlerError {
    message: String,
}

impl DurableRecoveryHandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRecoveryConfig {
    pub worker_id: String,
    pub max_items_per_batch: usize,
    pub lease_duration: Duration,
    pub handler_timeout: Duration,
}

impl DurableRecoveryConfig {
    pub fn validate(&self) -> Result<(), DurableRecoveryError> {
        if self.worker_id.trim().is_empty() {
            return Err(DurableRecoveryError::InvalidConfig("worker_id"));
        }
        if self.max_items_per_batch == 0 {
            return Err(DurableRecoveryError::InvalidConfig("max_items_per_batch"));
        }
        if self.lease_duration.is_zero() {
            return Err(DurableRecoveryError::InvalidConfig("lease_duration"));
        }
        if self.handler_timeout.is_zero() || self.handler_timeout >= self.lease_duration {
            return Err(DurableRecoveryError::InvalidConfig(
                "handler_timeout must be shorter than lease_duration",
            ));
        }
        let _ = duration_ms(self.lease_duration)?;
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

    #[error(transparent)]
    Store(#[from] StoreError),
}

pub struct DurableRecoveryDispatcher {
    store: SqliteStateStore,
    inbound_handler: Arc<dyn DurableInboundHandler>,
    resume_handler: Arc<dyn DurableSessionResumeHandler>,
    config: DurableRecoveryConfig,
}

impl DurableRecoveryDispatcher {
    pub fn new(
        store: SqliteStateStore,
        inbound_handler: Arc<dyn DurableInboundHandler>,
        resume_handler: Arc<dyn DurableSessionResumeHandler>,
        config: DurableRecoveryConfig,
    ) -> Result<Self, DurableRecoveryError> {
        config.validate()?;
        Ok(Self {
            store,
            inbound_handler,
            resume_handler,
            config,
        })
    }

    /// Claims and handles at most `max_items_per_batch` records.
    ///
    /// A process crash leaves the current row in `PROCESSING`; a later call at
    /// or after its lease deadline reclaims that row with a new revision.
    pub async fn dispatch_batch(
        &self,
        server_now: i64,
    ) -> Result<DurableRecoveryBatchReport, DurableRecoveryError> {
        if server_now < 0 {
            return Err(DurableRecoveryError::InvalidConfig("server_now"));
        }
        let lease_expires_at = server_now
            .checked_add(duration_ms(self.config.lease_duration)?)
            .ok_or(DurableRecoveryError::TimestampOverflow)?;
        let mut report = DurableRecoveryBatchReport::default();
        for index in 0..self.config.max_items_per_batch {
            let prefer_inbound = index % 2 == 0;
            let item = if prefer_inbound {
                match self.claim_inbound(server_now, lease_expires_at).await? {
                    Some(item) => Some(RecoveryItem::Inbound(item)),
                    None => self
                        .claim_resume(server_now, lease_expires_at)
                        .await?
                        .map(RecoveryItem::Resume),
                }
            } else {
                match self.claim_resume(server_now, lease_expires_at).await? {
                    Some(item) => Some(RecoveryItem::Resume(item)),
                    None => self
                        .claim_inbound(server_now, lease_expires_at)
                        .await?
                        .map(RecoveryItem::Inbound),
                }
            };
            let Some(item) = item else {
                break;
            };
            report.claimed += 1;
            report.reclaimed += usize::from(item.was_reclaimed());
            self.handle_item(item, server_now, &mut report).await?;
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
        now: i64,
        report: &mut DurableRecoveryBatchReport,
    ) -> Result<(), StoreError> {
        match item {
            RecoveryItem::Inbound(item) => {
                match tokio::time::timeout(
                    self.config.handler_timeout,
                    self.inbound_handler.handle(&item.admission),
                )
                .await
                {
                    Ok(Ok(())) => {
                        self.store
                            .complete_inbound(CompleteInboundAdmission {
                                message_id: item.admission.message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                completed_at: now,
                            })
                            .await?;
                        report.handled += 1;
                    }
                    outcome => {
                        let (error, timed_out) = handler_failure(outcome);
                        self.store
                            .fail_inbound(FailInboundAdmission {
                                message_id: item.admission.message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                failed_at: now,
                                error,
                            })
                            .await?;
                        report.failed += 1;
                        report.timed_out += usize::from(timed_out);
                    }
                }
            }
            RecoveryItem::Resume(item) => {
                match tokio::time::timeout(
                    self.config.handler_timeout,
                    self.resume_handler.handle(&item.admission),
                )
                .await
                {
                    Ok(Ok(outcome)) => {
                        self.store
                            .complete_session_resume(CompleteSessionResumeAdmission {
                                hello_message_id: item.admission.hello_message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                completed_at: now,
                                reconciliation_request_id: outcome.reconciliation_request_id,
                            })
                            .await?;
                        report.handled += 1;
                    }
                    outcome => {
                        let (error, timed_out) = handler_failure(outcome);
                        self.store
                            .fail_session_resume(FailSessionResumeAdmission {
                                hello_message_id: item.admission.hello_message_id,
                                expected_revision: item.admission.revision,
                                worker_id: self.config.worker_id.clone(),
                                failed_at: now,
                                error,
                            })
                            .await?;
                        report.failed += 1;
                        report.timed_out += usize::from(timed_out);
                    }
                }
            }
        }
        Ok(())
    }
}

fn handler_failure<T>(
    outcome: Result<Result<T, DurableRecoveryHandlerError>, tokio::time::error::Elapsed>,
) -> (String, bool) {
    match outcome {
        Ok(Err(error)) => (bounded_error(error.to_string()), false),
        Err(_) => ("durable recovery handler timed out".to_owned(), true),
        Ok(Ok(_)) => unreachable!("successful handler result is handled by the caller"),
    }
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

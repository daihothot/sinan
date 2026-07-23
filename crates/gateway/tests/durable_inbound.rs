use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use serde_json::json;
use sinan_execution::ServerClock;
use sinan_gateway::{
    AuthenticatedSessionContext, ClientSecretEpoch, DurableInboundHandler,
    DurableInboundHandlerFuture, DurableInboundMessagePort, DurableRecoveryConfig,
    DurableRecoveryDispatcher, DurableRecoveryError, DurableRecoveryHandlerError,
    DurableSessionResumeHandler, DurableSessionResumeHandlerFuture, DurableSessionResumePort,
    ExecutionTransport, InboundAdmission, InboundMessage, InboundMessagePort,
    PersistedTransportEvent, ProductionTransportEventPort, SessionResumeHandlingOutcome,
    SessionResumePort, SessionResumeRequest, TransportEvent, TransportEventEvidence,
    TransportEventKind, TransportEventPort, TransportEventPublishFuture, TransportEventPublisher,
    TransportPersistenceIdGenerator,
};
use sinan_protocol::{
    ExecutionClientMessageType, ExecutionClientPlatform, ResumeCursor, WireMessage,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{
    CanonicalJson, ClaimDurableWork, DeadletterReason, DurableWorkStatus, NewEventStreamRecord,
    NewSessionRecord, SqliteStateStore, StoreOptions, WriteTransaction,
};
use sinan_types::{
    AccountId, ClientId, ClockSyncStatus, EventStreamTopic, MessageId, SessionId, SessionStatus,
};

async fn store() -> SqliteStateStore {
    let mut options = StoreOptions::new("sqlite::memory:");
    options.max_connections = 1;
    SqliteStateStore::connect(options).await.unwrap()
}

async fn seed_session(store: &SqliteStateStore) {
    store
        .replace_active_session(NewSessionRecord {
            session_id: SessionId::from("session-1"),
            client_id: ClientId::from("client-1"),
            account_id: AccountId::from("account-1"),
            terminal_id: None,
            platform: "MT5".to_owned(),
            status: SessionStatus::Active,
            capabilities: CanonicalJson::from_value(json!([])).unwrap(),
            remote_addr: Some("127.0.0.1:5000".to_owned()),
            connected_at: 10,
            last_heartbeat_at: Some(10),
            last_time_sync_at: Some(10),
            clock_sync_status: Some(ClockSyncStatus::Synced),
            disconnected_at: None,
            max_inflight_commands: 8,
            updated_at: 10,
        })
        .await
        .unwrap();
}

fn context(session_id: &str) -> AuthenticatedSessionContext {
    AuthenticatedSessionContext {
        transport: ExecutionTransport::NativeTcp,
        session_id: SessionId::from(session_id),
        client_id: ClientId::from("client-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        platform: ExecutionClientPlatform::Mt5,
        capabilities: vec!["market.tick".to_owned()],
        client_auth_secret_epoch: ClientSecretEpoch::Active,
        authenticated_at: 10,
        remote_addr: Some("127.0.0.1:5000".to_owned()),
    }
}

fn inbound_message(message_id: &str, sequence: u64, value: i64) -> InboundMessage {
    let envelope = WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::MarketTick,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from("client-1")),
        session_id: Some(SessionId::from("session-1")),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(90),
        sequence: Some(sequence),
        payload: json!({"value": value}),
    };
    InboundMessage {
        wire_bytes: serde_json::to_vec(&envelope).unwrap(),
        envelope,
        received_at: 100,
    }
}

#[tokio::test]
async fn production_ports_only_succeed_after_durable_spool_admission() {
    let store = store().await;
    seed_session(&store).await;
    let inbound = DurableInboundMessagePort::new(store.clone());
    let session = context("session-1");
    let first = inbound_message("message-1", 1, 10);
    let first_raw_payload_length = first.wire_bytes.len().try_into().unwrap();

    assert_eq!(
        inbound.admit(&session, first).await.unwrap(),
        InboundAdmission::Accepted
    );
    let mut duplicate = inbound_message("message-1", 1, 10);
    duplicate.received_at = 101;
    assert_eq!(
        inbound.admit(&session, duplicate).await.unwrap(),
        InboundAdmission::Duplicate
    );
    assert!(matches!(
        inbound
            .admit(&session, inbound_message("message-1", 1, 11))
            .await
            .unwrap(),
        InboundAdmission::Rejected { .. }
    ));
    let stored = store
        .get_inbound_admission(&MessageId::from("message-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, DurableWorkStatus::Pending);
    assert!(stored.envelope.as_str().contains("\"payload\""));
    assert_eq!(stored.raw_payload_length, Some(first_raw_payload_length),);

    let mut optional_client = inbound_message("message-2", 2, 20);
    optional_client.envelope.client_id = None;
    let mut raw = serde_json::to_value(&optional_client.envelope).unwrap();
    raw.as_object_mut()
        .unwrap()
        .insert("extension".to_owned(), json!({"preserved": true}));
    optional_client.wire_bytes = serde_json::to_vec(&raw).unwrap();
    assert_eq!(
        inbound.admit(&session, optional_client).await.unwrap(),
        InboundAdmission::Accepted
    );
    assert!(store
        .get_inbound_admission(&MessageId::from("message-2"))
        .await
        .unwrap()
        .unwrap()
        .envelope
        .as_str()
        .contains("\"extension\""));

    let missing_session = context("missing-session");
    let mut missing_message = inbound_message("message-3", 3, 30);
    missing_message.envelope.session_id = Some(SessionId::from("missing-session"));
    missing_message.wire_bytes = serde_json::to_vec(&missing_message.envelope).unwrap();
    assert!(inbound
        .admit(&missing_session, missing_message)
        .await
        .is_err());
    assert!(store
        .get_inbound_admission(&MessageId::from("message-3"))
        .await
        .unwrap()
        .is_none());

    let resume = DurableSessionResumePort::new(store.clone());
    let request = SessionResumeRequest {
        hello_message_id: MessageId::from("hello-1"),
        cursor: ResumeCursor {
            previous_session_id: Some(SessionId::from("old-session")),
            pending_command_ids: Some(vec!["command-1".into()]),
            ..ResumeCursor::default()
        },
        received_at: 100,
    };
    let new_session = context("new-session");
    resume.admit(&new_session, request.clone()).await.unwrap();
    resume.admit(&new_session, request.clone()).await.unwrap();
    let mut drift = request;
    drift.cursor.pending_command_ids = Some(vec!["command-2".into()]);
    assert!(resume.admit(&new_session, drift).await.is_err());
}

#[derive(Default)]
struct RecordingInboundHandler {
    messages: Mutex<Vec<MessageId>>,
}

impl DurableInboundHandler for RecordingInboundHandler {
    fn handle<'a>(
        &'a self,
        _transaction: &'a mut WriteTransaction,
        admission: &'a sinan_store::StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a> {
        Box::pin(async move {
            self.messages
                .lock()
                .unwrap()
                .push(admission.message_id.clone());
            Ok(())
        })
    }
}

#[derive(Default)]
struct RecordingResumeHandler {
    messages: Mutex<Vec<MessageId>>,
}

impl DurableSessionResumeHandler for RecordingResumeHandler {
    fn handle<'a>(
        &'a self,
        _transaction: &'a mut WriteTransaction,
        admission: &'a sinan_store::StoredSessionResumeAdmission,
    ) -> DurableSessionResumeHandlerFuture<'a> {
        Box::pin(async move {
            self.messages
                .lock()
                .unwrap()
                .push(admission.hello_message_id.clone());
            Ok(SessionResumeHandlingOutcome {
                reconciliation_request_id: Some("reconciliation-1".into()),
            })
        })
    }
}

struct PendingResumeHandler;

impl DurableSessionResumeHandler for PendingResumeHandler {
    fn handle<'a>(
        &'a self,
        _transaction: &'a mut WriteTransaction,
        _admission: &'a sinan_store::StoredSessionResumeAdmission,
    ) -> DurableSessionResumeHandlerFuture<'a> {
        Box::pin(std::future::pending())
    }
}

struct TestClock(AtomicU64);

impl TestClock {
    fn new(now: i64) -> Self {
        Self(AtomicU64::new(now.try_into().unwrap()))
    }

    fn set(&self, now: i64) {
        self.0.store(now.try_into().unwrap(), Ordering::Release);
    }
}

impl ServerClock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire).try_into().unwrap()
    }
}

#[tokio::test]
async fn recovery_dispatcher_reclaims_crashed_work_and_respects_batch_bound() {
    let store = store().await;
    seed_session(&store).await;
    let inbound_port = DurableInboundMessagePort::new(store.clone());
    let session = context("session-1");
    for sequence in 1..=3 {
        inbound_port
            .admit(
                &session,
                inbound_message(&format!("message-{sequence}"), sequence, sequence as i64),
            )
            .await
            .unwrap();
    }

    let crashed = store
        .claim_next_inbound(ClaimDurableWork {
            worker_id: "crashed-worker".to_owned(),
            claimed_at: 105,
            lease_expires_at: 110,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(crashed.message_id, MessageId::from("message-1"));

    let inbound_handler = Arc::new(RecordingInboundHandler::default());
    let resume_handler = Arc::new(RecordingResumeHandler::default());
    let clock = Arc::new(TestClock::new(110));
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        inbound_handler.clone(),
        resume_handler,
        clock.clone(),
        DurableRecoveryConfig {
            worker_id: "recovery-worker".to_owned(),
            max_items_per_batch: 2,
            lease_duration: Duration::from_secs(5),
            handler_timeout: Duration::from_secs(1),
            finalization_budget: Duration::from_secs(1),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(report.claimed, 2);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.handled, 2);
    assert_eq!(inbound_handler.messages.lock().unwrap().len(), 2);

    clock.set(111);
    let remaining = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(remaining.handled, 1);
    assert_eq!(inbound_handler.messages.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn recovery_dispatcher_completes_resume_with_explicit_reconciliation_evidence() {
    let store = store().await;
    let resume_port = DurableSessionResumePort::new(store.clone());
    let session = context("new-session");
    resume_port
        .admit(
            &session,
            SessionResumeRequest {
                hello_message_id: MessageId::from("hello-recovery"),
                cursor: ResumeCursor {
                    pending_command_ids: Some(vec!["command-1".into()]),
                    ..ResumeCursor::default()
                },
                received_at: 100,
            },
        )
        .await
        .unwrap();
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(RecordingInboundHandler::default()),
        Arc::new(RecordingResumeHandler::default()),
        Arc::new(TestClock::new(110)),
        DurableRecoveryConfig {
            worker_id: "resume-recovery".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(5),
            handler_timeout: Duration::from_secs(1),
            finalization_budget: Duration::from_secs(1),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(report.handled, 1);
    let stored = store
        .get_session_resume_admission(&MessageId::from("hello-recovery"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, DurableWorkStatus::Handled);
    assert_eq!(
        stored.reconciliation_request_id.as_deref(),
        Some("reconciliation-1")
    );
}

#[derive(Clone, Copy)]
enum FactHandlerOutcome {
    Success,
    Failure,
    Retryable,
    Pending,
}

struct FactWritingInboundHandler {
    event_id: &'static str,
    outcome: FactHandlerOutcome,
}

impl DurableInboundHandler for FactWritingInboundHandler {
    fn handle<'a>(
        &'a self,
        transaction: &'a mut WriteTransaction,
        admission: &'a sinan_store::StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a> {
        Box::pin(async move {
            transaction
                .append_event_stream_record(NewEventStreamRecord {
                    event_id: self.event_id.to_owned(),
                    topic: EventStreamTopic::SystemEvent,
                    account_id: None,
                    event_type: "test.durable-recovery-fact".to_owned(),
                    payload: CanonicalJson::from_value(json!({
                        "message_id": admission.message_id,
                    }))
                    .unwrap(),
                    created_at: admission.updated_at,
                })
                .await
                .map_err(|error| {
                    DurableRecoveryHandlerError::retryable_infrastructure(error.to_string())
                })?;

            match self.outcome {
                FactHandlerOutcome::Success => Ok(()),
                FactHandlerOutcome::Failure => {
                    Err(DurableRecoveryHandlerError::terminal_deadletter(
                        DeadletterReason::SchemaValidationFailed,
                        "injected handler failure",
                    ))
                }
                FactHandlerOutcome::Retryable => {
                    Err(DurableRecoveryHandlerError::retryable_infrastructure(
                        "injected infrastructure failure",
                    ))
                }
                FactHandlerOutcome::Pending => std::future::pending().await,
            }
        })
    }
}

struct SequenceClock {
    samples: Vec<i64>,
    next: AtomicUsize,
}

impl SequenceClock {
    fn new(samples: impl Into<Vec<i64>>) -> Self {
        Self {
            samples: samples.into(),
            next: AtomicUsize::new(0),
        }
    }
}

impl ServerClock for SequenceClock {
    fn now_ms(&self) -> i64 {
        let index = self.next.fetch_add(1, Ordering::AcqRel);
        self.samples[index]
    }
}

fn recovery_config(worker_id: &str) -> DurableRecoveryConfig {
    DurableRecoveryConfig {
        worker_id: worker_id.to_owned(),
        max_items_per_batch: 1,
        lease_duration: Duration::from_secs(1),
        handler_timeout: Duration::from_millis(100),
        finalization_budget: Duration::from_millis(100),
    }
}

#[test]
fn recovery_config_reserves_a_positive_finalization_budget_inside_the_lease() {
    let mut config = recovery_config("config-worker");
    config.finalization_budget = Duration::ZERO;
    assert!(matches!(
        config.validate(),
        Err(DurableRecoveryError::InvalidConfig(
            "finalization_budget must be at least one millisecond"
        ))
    ));

    let mut config = recovery_config("config-worker");
    config.handler_timeout = Duration::from_millis(901);
    config.finalization_budget = Duration::from_millis(100);
    assert!(matches!(
        config.validate(),
        Err(DurableRecoveryError::InvalidConfig(
            "handler_timeout plus finalization_budget must not exceed lease_duration"
        ))
    ));

    let mut config = recovery_config("config-worker");
    config.finalization_budget = Duration::from_nanos(1);
    assert!(matches!(
        config.validate(),
        Err(DurableRecoveryError::InvalidConfig(
            "finalization_budget must be at least one millisecond"
        ))
    ));
}

async fn admit_recovery_message(store: &SqliteStateStore, message_id: &str, sequence: u64) {
    DurableInboundMessagePort::new(store.clone())
        .admit(
            &context("session-1"),
            inbound_message(message_id, sequence, 10),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn recovery_dispatcher_commits_business_fact_with_handled_status() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-success", 1).await;
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(FactWritingInboundHandler {
            event_id: "business-success",
            outcome: FactHandlerOutcome::Success,
        }),
        Arc::new(RecordingResumeHandler::default()),
        Arc::new(TestClock::new(110)),
        recovery_config("success-worker"),
    )
    .unwrap();

    let report = dispatcher.dispatch_batch().await.unwrap();

    assert_eq!(report.handled, 1);
    assert!(store
        .get_event_stream_record("business-success")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        store
            .get_inbound_admission(&MessageId::from("message-success"))
            .await
            .unwrap()
            .unwrap()
            .status,
        DurableWorkStatus::Handled
    );
}

#[tokio::test]
async fn recovery_dispatcher_rolls_back_business_fact_before_recording_failure() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-failure", 1).await;
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(FactWritingInboundHandler {
            event_id: "business-failure",
            outcome: FactHandlerOutcome::Failure,
        }),
        Arc::new(RecordingResumeHandler::default()),
        Arc::new(TestClock::new(110)),
        recovery_config("failure-worker"),
    )
    .unwrap();

    let report = dispatcher.dispatch_batch().await.unwrap();

    assert_eq!(report.failed, 1);
    assert!(store
        .get_event_stream_record("business-failure")
        .await
        .unwrap()
        .is_none());
    let admission = store
        .get_inbound_admission(&MessageId::from("message-failure"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Failed);
    assert_eq!(
        admission.last_error.as_deref(),
        Some("durable recovery handler failed: injected handler failure")
    );
    let deadletter = store
        .get_deadletter_event("durable-inbound:message-failure")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deadletter.message_id, Some(admission.message_id));
    assert_eq!(deadletter.message_type.as_deref(), Some("market.tick"));
    assert_eq!(deadletter.schema_version.as_deref(), Some("ecp.v1.0"));
    assert_eq!(deadletter.reason, DeadletterReason::SchemaValidationFailed);
    assert_eq!(deadletter.source, "trading-core.durable-recovery");
    assert_eq!(deadletter.raw_payload, None);
    assert!(admission.raw_payload_length.is_some());
    assert_eq!(deadletter.raw_payload_length, admission.raw_payload_length);
    assert_eq!(deadletter.error_message, admission.last_error.unwrap());
}

#[tokio::test]
async fn recovery_dispatcher_leaves_infrastructure_failure_reclaimable_and_stops_batch() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-retryable-1", 1).await;
    admit_recovery_message(&store, "message-retryable-2", 2).await;
    let clock = Arc::new(TestClock::new(110));
    let mut config = recovery_config("retryable-worker");
    config.max_items_per_batch = 2;
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(FactWritingInboundHandler {
            event_id: "business-retryable",
            outcome: FactHandlerOutcome::Retryable,
        }),
        Arc::new(RecordingResumeHandler::default()),
        clock.clone(),
        config,
    )
    .unwrap();

    let error = dispatcher.dispatch_batch().await.unwrap_err();

    assert!(matches!(
        error,
        DurableRecoveryError::RetryableHandler(ref message)
            if message.contains("injected infrastructure failure")
    ));
    assert!(store
        .get_event_stream_record("business-retryable")
        .await
        .unwrap()
        .is_none());
    let first_claim = store
        .get_inbound_admission(&MessageId::from("message-retryable-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_claim.status, DurableWorkStatus::Processing);
    assert_eq!(
        store
            .get_inbound_admission(&MessageId::from("message-retryable-2"))
            .await
            .unwrap()
            .unwrap()
            .status,
        DurableWorkStatus::Pending
    );
    assert!(store
        .get_deadletter_event("durable-inbound:message-retryable-1")
        .await
        .unwrap()
        .is_none());

    clock.set(1_110);
    assert!(matches!(
        dispatcher.dispatch_batch().await,
        Err(DurableRecoveryError::RetryableHandler(_))
    ));
    let reclaimed = store
        .get_inbound_admission(&MessageId::from("message-retryable-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.status, DurableWorkStatus::Processing);
    assert!(reclaimed.revision > first_claim.revision);
    assert_eq!(reclaimed.lease_expires_at, Some(2_110));
}

#[tokio::test]
async fn recovery_dispatcher_terminally_records_handler_timeout() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-timeout", 1).await;
    let clock = Arc::new(TestClock::new(110));
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(FactWritingInboundHandler {
            event_id: "business-timeout",
            outcome: FactHandlerOutcome::Pending,
        }),
        Arc::new(RecordingResumeHandler::default()),
        clock.clone(),
        DurableRecoveryConfig {
            worker_id: "timeout-worker".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(1),
            handler_timeout: Duration::from_millis(1),
            finalization_budget: Duration::from_millis(100),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(report.timed_out, 1);
    assert!(store
        .get_event_stream_record("business-timeout")
        .await
        .unwrap()
        .is_none());
    let stored = store
        .get_inbound_admission(&MessageId::from("message-timeout"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, DurableWorkStatus::Failed);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("durable recovery handler timed out")
    );

    clock.set(2_000);
    let replay = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(replay.claimed, 0);
    assert_eq!(
        store
            .get_inbound_admission(&MessageId::from("message-timeout"))
            .await
            .unwrap()
            .unwrap()
            .status,
        DurableWorkStatus::Failed
    );
}

#[tokio::test]
async fn recovery_dispatcher_terminally_records_resume_handler_timeout() {
    let store = store().await;
    let resume_port = DurableSessionResumePort::new(store.clone());
    resume_port
        .admit(
            &context("resume-timeout-session"),
            SessionResumeRequest {
                hello_message_id: MessageId::from("hello-timeout"),
                cursor: ResumeCursor::default(),
                received_at: 100,
            },
        )
        .await
        .unwrap();
    let clock = Arc::new(TestClock::new(110));
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(RecordingInboundHandler::default()),
        Arc::new(PendingResumeHandler),
        clock.clone(),
        DurableRecoveryConfig {
            worker_id: "resume-timeout-worker".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(1),
            handler_timeout: Duration::from_millis(1),
            finalization_budget: Duration::from_millis(100),
        },
    )
    .unwrap();

    let report = dispatcher.dispatch_batch().await.unwrap();

    assert_eq!(report.failed, 1);
    assert_eq!(report.timed_out, 1);
    let stored = store
        .get_session_resume_admission(&MessageId::from("hello-timeout"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, DurableWorkStatus::Failed);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("durable recovery handler timed out")
    );

    clock.set(2_000);
    let replay = dispatcher.dispatch_batch().await.unwrap();
    assert_eq!(replay.claimed, 0);
    assert_eq!(
        store
            .get_session_resume_admission(&MessageId::from("hello-timeout"))
            .await
            .unwrap()
            .unwrap()
            .status,
        DurableWorkStatus::Failed
    );
}

#[derive(Default)]
struct ObservingInboundHandler {
    claims: Mutex<Vec<(MessageId, i64, i64)>>,
}

impl DurableInboundHandler for ObservingInboundHandler {
    fn handle<'a>(
        &'a self,
        _transaction: &'a mut WriteTransaction,
        admission: &'a sinan_store::StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a> {
        Box::pin(async move {
            self.claims.lock().unwrap().push((
                admission.message_id.clone(),
                admission.updated_at,
                admission.lease_expires_at.unwrap(),
            ));
            Ok(())
        })
    }
}

#[tokio::test]
async fn recovery_dispatcher_samples_claim_and_completion_time_for_each_item() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-clock-1", 1).await;
    admit_recovery_message(&store, "message-clock-2", 2).await;
    let handler = Arc::new(ObservingInboundHandler::default());
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        handler.clone(),
        Arc::new(RecordingResumeHandler::default()),
        Arc::new(SequenceClock::new(vec![110, 111, 120, 121])),
        DurableRecoveryConfig {
            worker_id: "clock-worker".to_owned(),
            max_items_per_batch: 2,
            lease_duration: Duration::from_millis(10),
            handler_timeout: Duration::from_millis(1),
            finalization_budget: Duration::from_millis(2),
        },
    )
    .unwrap();

    let report = dispatcher.dispatch_batch().await.unwrap();

    assert_eq!(report.handled, 2);
    assert_eq!(
        *handler.claims.lock().unwrap(),
        vec![
            (MessageId::from("message-clock-1"), 110, 120),
            (MessageId::from("message-clock-2"), 120, 130),
        ]
    );
    assert_eq!(
        store
            .get_inbound_admission(&MessageId::from("message-clock-1"))
            .await
            .unwrap()
            .unwrap()
            .finished_at,
        Some(111)
    );
    assert_eq!(
        store
            .get_inbound_admission(&MessageId::from("message-clock-2"))
            .await
            .unwrap()
            .unwrap()
            .finished_at,
        Some(121)
    );
}

#[tokio::test]
async fn recovery_dispatcher_rolls_back_fact_when_completion_lease_has_expired() {
    let store = store().await;
    seed_session(&store).await;
    admit_recovery_message(&store, "message-expired", 1).await;
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(FactWritingInboundHandler {
            event_id: "business-expired",
            outcome: FactHandlerOutcome::Success,
        }),
        Arc::new(RecordingResumeHandler::default()),
        Arc::new(SequenceClock::new(vec![110, 120])),
        DurableRecoveryConfig {
            worker_id: "expired-worker".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_millis(10),
            handler_timeout: Duration::from_millis(1),
            finalization_budget: Duration::from_millis(2),
        },
    )
    .unwrap();

    let error = dispatcher.dispatch_batch().await.unwrap_err();

    assert!(matches!(
        error,
        DurableRecoveryError::Store(sinan_store::StoreError::StaleWrite { .. })
    ));
    assert!(store
        .get_event_stream_record("business-expired")
        .await
        .unwrap()
        .is_none());
    let admission = store
        .get_inbound_admission(&MessageId::from("message-expired"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Processing);
    assert_eq!(admission.lease_expires_at, Some(120));
}

#[derive(Default)]
struct DeterministicTransportIds {
    system: AtomicU64,
    deadletter: AtomicU64,
}

impl TransportPersistenceIdGenerator for DeterministicTransportIds {
    fn next_system_event_id(&self) -> String {
        format!("system-{}", self.system.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn next_deadletter_id(&self) -> String {
        format!(
            "deadletter-{}",
            self.deadletter.fetch_add(1, Ordering::Relaxed) + 1
        )
    }
}

#[derive(Default)]
struct RecordingPublisher {
    events: Mutex<Vec<PersistedTransportEvent>>,
}

impl TransportEventPublisher for RecordingPublisher {
    fn publish<'a>(&'a self, event: PersistedTransportEvent) -> TransportEventPublishFuture<'a> {
        Box::pin(async move {
            self.events.lock().unwrap().push(event);
            Ok(())
        })
    }
}

#[tokio::test]
async fn production_transport_event_port_classifies_persists_and_publishes() {
    let store = store().await;
    let publisher = Arc::new(RecordingPublisher::default());
    let port = ProductionTransportEventPort::new(
        store.clone(),
        publisher.clone(),
        Arc::new(DeterministicTransportIds::default()),
    );

    port.record(TransportEvent {
        transport: ExecutionTransport::NativeTcp,
        kind: TransportEventKind::SchemaRejected,
        occurred_at: 100,
        remote_addr: Some("127.0.0.1:5000".to_owned()),
        session_id: Some(SessionId::from("session-1")),
        message_id: Some(MessageId::from("message-1")),
        evidence: TransportEventEvidence {
            message_type: Some(ExecutionClientMessageType::MarketTick),
            schema_version: Some(SUPPORTED_SCHEMA_VERSION),
            raw_payload_length: Some(321),
        },
        detail: "invalid market.tick schema".to_owned(),
    })
    .await
    .unwrap();
    let deadletter = store
        .get_deadletter_event("deadletter-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deadletter.reason,
        sinan_store::DeadletterReason::SchemaValidationFailed
    );
    assert_eq!(deadletter.message_type.as_deref(), Some("market.tick"));
    assert_eq!(deadletter.schema_version.as_deref(), Some("ecp.v1.0"));
    assert_eq!(deadletter.raw_payload, None);
    assert_eq!(deadletter.raw_payload_length, Some(321));

    port.record(TransportEvent {
        transport: ExecutionTransport::ExecutionWebSocket,
        kind: TransportEventKind::TimeSyncUnhealthy,
        occurred_at: 101,
        remote_addr: None,
        session_id: Some(SessionId::from("session-1")),
        message_id: Some(MessageId::from("message-2")),
        evidence: TransportEventEvidence::default(),
        detail: "clock sample expired".to_owned(),
    })
    .await
    .unwrap();
    let system = store.get_system_event("system-1").await.unwrap().unwrap();
    assert_eq!(system.event_type, "TIME_SYNC_UNHEALTHY");
    assert_eq!(publisher.events.lock().unwrap().len(), 2);
}

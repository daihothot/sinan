use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use serde_json::json;
use sinan_gateway::{
    AuthenticatedSessionContext, ClientSecretEpoch, DurableInboundHandler,
    DurableInboundHandlerFuture, DurableInboundMessagePort, DurableRecoveryConfig,
    DurableRecoveryDispatcher, DurableSessionResumeHandler, DurableSessionResumeHandlerFuture,
    DurableSessionResumePort, ExecutionTransport, InboundAdmission, InboundMessage,
    InboundMessagePort, PersistedTransportEvent, ProductionTransportEventPort,
    SessionResumeHandlingOutcome, SessionResumePort, SessionResumeRequest, TransportEvent,
    TransportEventEvidence, TransportEventKind, TransportEventPort, TransportEventPublishFuture,
    TransportEventPublisher, TransportPersistenceIdGenerator,
};
use sinan_protocol::{
    ExecutionClientMessageType, ExecutionClientPlatform, ResumeCursor, WireMessage,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{
    CanonicalJson, ClaimDurableWork, DurableWorkStatus, NewSessionRecord, SqliteStateStore,
    StoreOptions,
};
use sinan_types::{AccountId, ClientId, ClockSyncStatus, MessageId, SessionId, SessionStatus};

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

    assert_eq!(
        inbound
            .admit(&session, inbound_message("message-1", 1, 10))
            .await
            .unwrap(),
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
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        inbound_handler.clone(),
        resume_handler,
        DurableRecoveryConfig {
            worker_id: "recovery-worker".to_owned(),
            max_items_per_batch: 2,
            lease_duration: Duration::from_secs(5),
            handler_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch(110).await.unwrap();
    assert_eq!(report.claimed, 2);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.handled, 2);
    assert_eq!(inbound_handler.messages.lock().unwrap().len(), 2);

    let remaining = dispatcher.dispatch_batch(111).await.unwrap();
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
        DurableRecoveryConfig {
            worker_id: "resume-recovery".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(5),
            handler_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch(110).await.unwrap();
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

struct PendingInboundHandler;

impl DurableInboundHandler for PendingInboundHandler {
    fn handle<'a>(
        &'a self,
        _admission: &'a sinan_store::StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn recovery_dispatcher_terminally_records_handler_timeout() {
    let store = store().await;
    seed_session(&store).await;
    DurableInboundMessagePort::new(store.clone())
        .admit(
            &context("session-1"),
            inbound_message("message-timeout", 1, 10),
        )
        .await
        .unwrap();
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(PendingInboundHandler),
        Arc::new(RecordingResumeHandler::default()),
        DurableRecoveryConfig {
            worker_id: "timeout-worker".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(1),
            handler_timeout: Duration::from_millis(1),
        },
    )
    .unwrap();
    let report = dispatcher.dispatch_batch(110).await.unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(report.timed_out, 1);
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

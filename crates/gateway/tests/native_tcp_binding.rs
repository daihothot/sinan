use std::{
    collections::VecDeque,
    fs, io,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};
use sinan_execution::ServerClock;
use sinan_gateway::{
    AuthenticatedSessionContext, ClientCredential, ConfiguredClientAuthenticator,
    ExecutionTransport, ExecutionTransportConfig, GatewayConnectionService, GatewayIdGenerator,
    GatewaySessionConfig, GatewaySessionRegistry, InboundAdmission, InboundAdmissionError,
    InboundAdmissionFuture, InboundMessage, InboundMessagePort, LiveSessionRegistry,
    NativeTcpBinding, NativeTcpError, SessionResumeError, SessionResumeFuture, SessionResumePort,
    SessionResumeRequest, TransportEvent, TransportEventFuture, TransportEventKind,
    TransportEventPort,
};
use sinan_protocol::{
    decode_wire_message, ExecutionClientMessageType, ExecutionClientPlatform, HeartbeatPayload,
    HelloAcceptedPayload, HelloPayload, NativeTcpFrameEncoder, ProtocolReason, ResumeCursor,
    SessionRejected, TransportAck, TransportAckStatus, WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{SqliteStateStore, StoreOptions};
use sinan_types::{
    AccountId, ClientId, ClockSyncStatus, CommandId, ErrorCode, MessageId, SessionId,
    SessionStatus, TerminalId,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, Notify},
    task::JoinHandle,
    time,
};

const CLIENT_ID: &str = "client_1";
const ACCOUNT_ID: &str = "account_1";
const TERMINAL_ID: &str = "terminal_1";
const CLIENT_SECRET: &str = "client-auth-secret";
const MAX_FRAME_BYTES: usize = 2_048;
const IO_TIMEOUT: Duration = Duration::from_secs(1);

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn unique() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock should be after Unix epoch")
            .as_nanos();
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "sinan-native-tcp-{}-{timestamp}-{sequence}.sqlite",
            std::process::id()
        )))
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.0.display())
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

struct FixedClock(i64);

impl ServerClock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

#[derive(Default)]
struct DeterministicIds {
    next_session: AtomicU64,
    next_message: AtomicU64,
}

impl GatewayIdGenerator for DeterministicIds {
    fn next_session_id(&self) -> SessionId {
        let number = self.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        SessionId::new(format!("session_{number}"))
    }

    fn next_message_id(&self) -> MessageId {
        let number = self.next_message.fetch_add(1, Ordering::Relaxed) + 1;
        MessageId::new(format!("gateway_message_{number}"))
    }
}

#[derive(Default)]
struct RecordingInboundPort {
    admitted: Mutex<Vec<(AuthenticatedSessionContext, InboundMessage)>>,
    outcomes: Mutex<VecDeque<InboundAdmission>>,
    fail: AtomicBool,
    block_next: AtomicBool,
    blocked: Notify,
    release: Notify,
}

impl RecordingInboundPort {
    fn admitted(&self) -> Vec<(AuthenticatedSessionContext, InboundMessage)> {
        self.admitted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn queue_outcome(&self, outcome: InboundAdmission) {
        self.outcomes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push_back(outcome);
    }

    fn block_next(&self) {
        self.block_next.store(true, Ordering::Release);
    }

    async fn wait_until_blocked(&self) {
        self.blocked.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

impl InboundMessagePort for RecordingInboundPort {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        message: InboundMessage,
    ) -> InboundAdmissionFuture<'a> {
        Box::pin(async move {
            if self.fail.load(Ordering::Acquire) {
                return Err(InboundAdmissionError::new("injected admission failure"));
            }
            if self.block_next.swap(false, Ordering::AcqRel) {
                self.blocked.notify_one();
                self.release.notified().await;
            }
            self.admitted
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((session.clone(), message));
            Ok(self
                .outcomes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pop_front()
                .unwrap_or(InboundAdmission::Accepted))
        })
    }
}

#[derive(Default)]
struct RecordingResumePort {
    admitted: Mutex<Vec<(AuthenticatedSessionContext, SessionResumeRequest)>>,
    reject: AtomicBool,
    pending: AtomicBool,
}

impl RecordingResumePort {
    fn admitted(&self) -> Vec<(AuthenticatedSessionContext, SessionResumeRequest)> {
        self.admitted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

impl SessionResumePort for RecordingResumePort {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        request: SessionResumeRequest,
    ) -> SessionResumeFuture<'a> {
        Box::pin(async move {
            if self.reject.load(Ordering::Acquire) {
                return Err(SessionResumeError::new("injected resume rejection"));
            }
            if self.pending.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            self.admitted
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((session.clone(), request));
            Ok(())
        })
    }
}

#[derive(Default)]
struct RecordingEventPort {
    events: Mutex<Vec<TransportEvent>>,
}

impl RecordingEventPort {
    fn kinds(&self) -> Vec<TransportEventKind> {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|event| event.kind)
            .collect()
    }

    fn recorded(&self) -> Vec<TransportEvent> {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

impl TransportEventPort for RecordingEventPort {
    fn record<'a>(&'a self, event: TransportEvent) -> TransportEventFuture<'a> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event);
            Ok(())
        })
    }
}

struct TestServer {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    server_task: JoinHandle<Result<(), NativeTcpError>>,
    store: SqliteStateStore,
    inbound: Arc<RecordingInboundPort>,
    resumes: Arc<RecordingResumePort>,
    events: Arc<RecordingEventPort>,
    _database: TestDatabase,
}

impl TestServer {
    async fn start() -> Self {
        let database = TestDatabase::unique();
        let mut store_options = StoreOptions::new(database.url());
        store_options.max_connections = 4;
        store_options.busy_timeout = Duration::from_secs(1);
        let store = SqliteStateStore::connect(store_options)
            .await
            .expect("test state store should connect");
        let sessions = GatewaySessionRegistry::new(
            store.clone(),
            Arc::new(LiveSessionRegistry::new()),
            Arc::new(FixedClock(1_800_000_000_000)),
            GatewaySessionConfig {
                max_clock_offset_ms: 250,
                max_time_sync_age_ms: 5_000,
                max_time_sync_rtt_ms: 1_000,
            },
        )
        .expect("session registry config should be valid");
        let authenticator = ConfiguredClientAuthenticator::new([ClientCredential::new(
            CLIENT_ID,
            ACCOUNT_ID,
            CLIENT_SECRET,
            None,
        )])
        .expect("client credential should be valid");
        let inbound = Arc::new(RecordingInboundPort::default());
        let resumes = Arc::new(RecordingResumePort::default());
        let events = Arc::new(RecordingEventPort::default());
        let config = ExecutionTransportConfig {
            handshake_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(500),
            inbound_admission_timeout: Duration::from_millis(150),
            event_write_timeout: Duration::from_millis(500),
            heartbeat_interval_ms: 1_000,
            heartbeat_timeout_ms: 5_000,
            time_sync_interval_ms: 1_000,
            max_time_sync_rtt_ms: 1_000,
            max_clock_offset_ms: 250,
            max_inflight_commands: 8,
            max_frame_bytes: MAX_FRAME_BYTES,
            max_message_bytes: MAX_FRAME_BYTES,
            outbound_queue_capacity: 8,
            tcp_read_chunk_bytes: 32,
            max_connections: 8,
            max_pending_handshakes: 8,
        };
        let service = GatewayConnectionService::new(
            sessions,
            Arc::new(authenticator),
            Arc::new(DeterministicIds::default()),
            inbound.clone(),
            resumes.clone(),
            events.clone(),
            config,
        )
        .expect("connection service config should be valid");
        let server = NativeTcpBinding::new(service)
            .bind("127.0.0.1:0")
            .await
            .expect("Native TCP server should bind");
        let addr = server
            .local_addr()
            .expect("bound Native TCP server should expose its address");
        let (shutdown, receiver) = watch::channel(false);
        let server_task = tokio::spawn(server.serve(receiver));

        Self {
            addr,
            shutdown,
            server_task,
            store,
            inbound,
            resumes,
            events,
            _database: database,
        }
    }

    async fn connect(&self) -> TcpStream {
        time::timeout(IO_TIMEOUT, TcpStream::connect(self.addr))
            .await
            .expect("loopback connect should not time out")
            .expect("loopback connect should succeed")
    }

    async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        let result = time::timeout(IO_TIMEOUT, &mut self.server_task)
            .await
            .expect("Native TCP server shutdown should not time out")
            .expect("Native TCP server task should not panic");
        result.expect("Native TCP server should stop cleanly");
    }
}

fn hello(message_id: &str, token: &str) -> WireMessage<HelloPayload> {
    WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::SessionHello,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: None,
        correlation_id: None,
        causation_id: None,
        sent_at: None,
        sequence: Some(1),
        payload: HelloPayload {
            client_id: ClientId::from(CLIENT_ID),
            platform: ExecutionClientPlatform::Mt5,
            terminal_id: Some(TerminalId::from(TERMINAL_ID)),
            account_id: AccountId::from(ACCOUNT_ID),
            token: token.to_owned(),
            capabilities: vec!["orders".to_owned(), "snapshots".to_owned()],
            resume: None,
        },
    }
}

fn client_message(message_id: &str, session_id: &SessionId, sequence: u64) -> WireMessage<Value> {
    WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::MarketTick,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(1_800_000_000_000),
        sequence: Some(sequence),
        payload: json!({
            "account_id": ACCOUNT_ID,
            "symbol": "EURUSD",
            "observed_at": 1_800_000_000_000_i64
        }),
    }
}

fn heartbeat(
    message_id: &str,
    session_id: &SessionId,
    sequence: u64,
    effective_server_now: i64,
    status: ClockSyncStatus,
    sample: Option<(i64, u64)>,
) -> WireMessage<HeartbeatPayload> {
    WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::Heartbeat,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(1_800_000_000_000),
        sequence: Some(sequence),
        payload: HeartbeatPayload {
            effective_server_now,
            clock_sync_status: status,
            last_time_sync_at_server_ms: sample.map(|(at, _)| at),
            last_time_sync_rtt_ms: sample.map(|(_, rtt)| rtt),
            server_time_offset_ms: Some(0),
            send_queue_depth: None,
            command_inbox_depth: None,
        },
    }
}

fn frame<T: serde::Serialize>(message: &T) -> Vec<u8> {
    NativeTcpFrameEncoder::new(MAX_FRAME_BYTES)
        .encode_json(message)
        .expect("test WireMessage should fit the configured frame limit")
}

async fn write_fragmented(stream: &mut TcpStream, bytes: &[u8]) {
    assert!(bytes.len() > 9, "test frame should exercise every fragment");
    for fragment in [&bytes[..1], &bytes[1..3], &bytes[3..9], &bytes[9..]] {
        stream
            .write_all(fragment)
            .await
            .expect("fragmented loopback write should succeed");
        tokio::task::yield_now().await;
    }
}

async fn read_payload(stream: &mut TcpStream) -> Vec<u8> {
    time::timeout(IO_TIMEOUT, async {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "server emitted an invalid Native TCP frame length",
            ));
        }
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;
        Ok::<_, io::Error>(payload)
    })
    .await
    .expect("Native TCP response should not time out")
    .expect("Native TCP response should contain one complete frame")
}

async fn read_accepted(stream: &mut TcpStream) -> WireMessage<HelloAcceptedPayload> {
    let payload = read_payload(stream).await;
    decode_wire_message(&payload, SUPPORTED_SCHEMA_VERSION)
        .expect("response should be a valid session.accepted WireMessage")
}

async fn read_ack(stream: &mut TcpStream) -> WireMessage<TransportAck> {
    let payload = read_payload(stream).await;
    decode_wire_message(&payload, SUPPORTED_SCHEMA_VERSION)
        .expect("response should be a valid transport.ack WireMessage")
}

async fn expect_closed_without_frame(stream: &mut TcpStream) {
    let mut byte = [0_u8; 1];
    match time::timeout(IO_TIMEOUT, stream.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(read)) => panic!("connection emitted {read} unexpected response byte(s)"),
        Err(_) => panic!("connection remained open without producing EOF"),
    }
}

async fn authenticate(
    stream: &mut TcpStream,
    message_id: &str,
) -> WireMessage<HelloAcceptedPayload> {
    stream
        .write_all(&frame(&hello(message_id, CLIENT_SECRET)))
        .await
        .expect("session.hello write should succeed");
    read_accepted(stream).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_hello_and_coalesced_messages_share_the_inbound_pipeline() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;

    write_fragmented(&mut stream, &frame(&hello("hello_1", CLIENT_SECRET))).await;
    let accepted = read_accepted(&mut stream).await;
    assert_eq!(
        accepted.message_type,
        ExecutionClientMessageType::SessionAccepted
    );
    assert_eq!(accepted.sequence, Some(1));
    assert_eq!(
        accepted.session_id,
        Some(accepted.payload.session_id.clone())
    );

    let first = client_message("client_message_1", &accepted.payload.session_id, 1);
    let second = client_message("client_message_2", &accepted.payload.session_id, 2);
    let mut coalesced = frame(&first);
    coalesced.extend(frame(&second));
    stream
        .write_all(&coalesced)
        .await
        .expect("coalesced Native TCP write should succeed");

    let first_ack = read_ack(&mut stream).await;
    let second_ack = read_ack(&mut stream).await;
    assert_eq!(
        first_ack.message_type,
        ExecutionClientMessageType::TransportAck
    );
    assert_eq!(first_ack.sequence, Some(2));
    assert_eq!(first_ack.payload.acked_message_id, first.message_id);
    assert_eq!(first_ack.payload.status, TransportAckStatus::Accepted);
    assert_eq!(
        second_ack.message_type,
        ExecutionClientMessageType::TransportAck
    );
    assert_eq!(second_ack.sequence, Some(3));
    assert_eq!(second_ack.payload.acked_message_id, second.message_id);
    assert_eq!(second_ack.payload.status, TransportAckStatus::Accepted);

    let admitted = server.inbound.admitted();
    assert_eq!(admitted.len(), 2);
    assert_eq!(admitted[0].0.transport, ExecutionTransport::NativeTcp);
    assert_eq!(admitted[0].0.session_id, accepted.payload.session_id);
    assert_eq!(admitted[1].0.session_id, admitted[0].0.session_id);
    assert_eq!(
        admitted[0].1.envelope.message_id.as_str(),
        "client_message_1"
    );
    assert_eq!(
        admitted[1].1.envelope.message_id.as_str(),
        "client_message_2"
    );

    drop(stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_ack_waits_for_durable_inbound_admission() {
    let server = TestServer::start().await;
    server.inbound.block_next();
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_delayed_admission").await;
    let message = client_message("delayed_admission", &accepted.payload.session_id, 1);
    stream
        .write_all(&frame(&message))
        .await
        .expect("message awaiting durable admission should be written");
    time::timeout(IO_TIMEOUT, server.inbound.wait_until_blocked())
        .await
        .expect("inbound handler should reach the durable admission gate");

    assert!(server.inbound.admitted().is_empty());
    let mut response_byte = [0_u8; 1];
    assert!(
        time::timeout(Duration::from_millis(50), stream.read(&mut response_byte))
            .await
            .is_err(),
        "transport ACK must not be readable before durable admission completes"
    );

    server.inbound.release();
    let ack = read_ack(&mut stream).await;
    assert_eq!(ack.payload.acked_message_id, message.message_id);
    assert_eq!(ack.payload.status, TransportAckStatus::Accepted);
    assert_eq!(server.inbound.admitted().len(), 1);

    drop(stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_duplicate_and_typed_rejection_map_to_transport_ack() {
    let server = TestServer::start().await;
    server.inbound.queue_outcome(InboundAdmission::Duplicate);
    server.inbound.queue_outcome(InboundAdmission::Rejected {
        reason: ErrorCode::BadRequest,
    });
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_admission_outcomes").await;
    let duplicate = client_message("duplicate_admission", &accepted.payload.session_id, 1);
    let rejected = client_message("rejected_admission", &accepted.payload.session_id, 2);
    let mut messages = frame(&duplicate);
    messages.extend(frame(&rejected));
    stream
        .write_all(&messages)
        .await
        .expect("scripted admission messages should be written");

    let duplicate_ack = read_ack(&mut stream).await;
    assert_eq!(duplicate_ack.payload.acked_message_id, duplicate.message_id);
    assert_eq!(duplicate_ack.payload.status, TransportAckStatus::Duplicate);
    assert_eq!(duplicate_ack.payload.reason, None);

    let rejected_ack = read_ack(&mut stream).await;
    assert_eq!(rejected_ack.payload.acked_message_id, rejected.message_id);
    assert_eq!(rejected_ack.payload.status, TransportAckStatus::Rejected);
    assert_eq!(
        rejected_ack.payload.reason,
        Some(ProtocolReason::Error(ErrorCode::BadRequest))
    );
    assert_eq!(server.inbound.admitted().len(), 2);

    drop(stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_length_prefixes_close_without_waiting_for_payload_or_sending_an_ack() {
    let server = TestServer::start().await;

    let mut zero_length = server.connect().await;
    zero_length
        .write_all(&0_u32.to_be_bytes())
        .await
        .expect("zero prefix write should succeed");
    expect_closed_without_frame(&mut zero_length).await;

    let mut oversized = server.connect().await;
    oversized
        .write_all(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes())
        .await
        .expect("oversized prefix write should succeed");
    expect_closed_without_frame(&mut oversized).await;

    assert!(server.inbound.admitted().is_empty());
    let event_kinds = server.events.kinds();
    assert!(event_kinds.contains(&TransportEventKind::WireProtocolViolation));
    assert!(event_kinds.contains(&TransportEventKind::WireFrameTooLarge));
    let events = server.events.recorded();
    assert_eq!(
        events
            .iter()
            .find(|event| event.kind == TransportEventKind::WireProtocolViolation)
            .expect("zero-length frame should emit a protocol violation")
            .evidence
            .raw_payload_length,
        Some(0)
    );
    assert_eq!(
        events
            .iter()
            .find(|event| event.kind == TransportEventKind::WireFrameTooLarge)
            .expect("oversized frame should emit a size violation")
            .evidence
            .raw_payload_length,
        Some((MAX_FRAME_BYTES + 1) as u64)
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_session_hello_is_a_redacted_schema_deadletter_event() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    let malformed_hello = br#"{
        "message_id":"malformed_hello",
        "type":"session.hello",
        "schema_version":"ecp.v1.0",
        "payload":{"token":"must-not-be-retained","hmac":"must-not-be-retained"}
    }"#;
    let framed = NativeTcpFrameEncoder::new(MAX_FRAME_BYTES)
        .encode(malformed_hello)
        .expect("malformed hello should fit the configured frame limit");
    stream
        .write_all(&framed)
        .await
        .expect("malformed hello write should succeed");
    expect_closed_without_frame(&mut stream).await;

    let events = server.events.recorded();
    let event = events
        .iter()
        .find(|event| event.kind == TransportEventKind::SchemaRejected)
        .expect("malformed hello should emit a schema rejection");
    assert_eq!(
        event.evidence.message_type,
        Some(ExecutionClientMessageType::SessionHello)
    );
    assert_eq!(
        event.evidence.schema_version,
        Some(SUPPORTED_SCHEMA_VERSION)
    );
    assert_eq!(
        event.evidence.raw_payload_length,
        Some(malformed_hello.len() as u64)
    );
    assert!(!event.detail.contains("must-not-be-retained"));
    assert!(
        events
            .iter()
            .all(|event| event.kind != TransportEventKind::HandshakeRejected),
        "decode failures must not be downgraded to handshake rejection system events"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_non_hello_first_message_remains_a_handshake_rejection() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    let mut wrong_direction = hello("wrong_first_type", CLIENT_SECRET);
    wrong_direction.message_type = ExecutionClientMessageType::MarketTick;
    wrong_direction.session_id = Some(SessionId::from("unbound-session"));
    wrong_direction.sent_at = Some(1_800_000_000_000);
    stream
        .write_all(&frame(&wrong_direction))
        .await
        .expect("wrong first message write should succeed");
    expect_closed_without_frame(&mut stream).await;

    let event_kinds = server.events.kinds();
    assert!(event_kinds.contains(&TransportEventKind::HandshakeRejected));
    assert!(!event_kinds.contains(&TransportEventKind::SchemaRejected));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_rejection_records_only_typed_redacted_wire_evidence() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_schema_evidence").await;
    let mut message = client_message("bad_schema", &accepted.payload.session_id, 1);
    message.schema_version = "ecp.v2.0".to_owned();
    message.payload["credential"] = json!("must-not-be-retained");
    let wire_bytes = serde_json::to_vec(&message).expect("test message should encode");
    let framed = NativeTcpFrameEncoder::new(MAX_FRAME_BYTES)
        .encode(&wire_bytes)
        .expect("test message should fit the configured frame limit");
    stream
        .write_all(&framed)
        .await
        .expect("invalid-schema message write should succeed");
    expect_closed_without_frame(&mut stream).await;

    assert!(server.inbound.admitted().is_empty());
    let event = server
        .events
        .recorded()
        .into_iter()
        .find(|event| event.kind == TransportEventKind::SchemaRejected)
        .expect("invalid schema should emit a schema rejection");
    assert_eq!(
        event.evidence.message_type,
        Some(ExecutionClientMessageType::MarketTick)
    );
    assert_eq!(
        event
            .evidence
            .schema_version
            .map(|version| version.to_string()),
        Some("ecp.v2.0".to_owned())
    );
    assert_eq!(
        event.evidence.raw_payload_length,
        Some(wire_bytes.len() as u64)
    );
    assert!(!event.detail.contains("must-not-be-retained"));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_client_auth_receives_session_rejected_then_eof() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    stream
        .write_all(&frame(&hello("hello_bad_auth", "wrong-secret")))
        .await
        .expect("invalid session.hello write should succeed");

    let payload = read_payload(&mut stream).await;
    let rejected: WireMessage<SessionRejected> =
        decode_wire_message(&payload, SUPPORTED_SCHEMA_VERSION)
            .expect("bad auth response should be a valid session.rejected WireMessage");
    assert_eq!(
        rejected.message_type,
        ExecutionClientMessageType::SessionRejected
    );
    assert_eq!(rejected.session_id, None);
    assert_eq!(rejected.sequence, None);
    assert_eq!(rejected.payload.reason, ErrorCode::AuthenticationFailed);
    expect_closed_without_frame(&mut stream).await;

    assert!(server.inbound.admitted().is_empty());
    let event_kinds = server.events.kinds();
    assert!(event_kinds.contains(&TransportEventKind::AuthenticationFailed));
    assert!(event_kinds.contains(&TransportEventKind::HandshakeRejected));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_route_replacement_closes_the_old_socket_and_keeps_the_new_session_live() {
    let server = TestServer::start().await;
    let mut old_stream = server.connect().await;
    let old_accepted = authenticate(&mut old_stream, "hello_old").await;
    assert_eq!(old_accepted.payload.session_id.as_str(), "session_1");

    let mut new_stream = server.connect().await;
    let new_accepted = authenticate(&mut new_stream, "hello_new").await;
    assert_eq!(new_accepted.payload.session_id.as_str(), "session_2");
    assert_ne!(
        new_accepted.payload.session_id,
        old_accepted.payload.session_id
    );
    expect_closed_without_frame(&mut old_stream).await;

    let live_message = client_message("new_session_message", &new_accepted.payload.session_id, 1);
    new_stream
        .write_all(&frame(&live_message))
        .await
        .expect("replacement session write should succeed");
    let ack = read_ack(&mut new_stream).await;
    assert_eq!(ack.sequence, Some(2));
    assert_eq!(ack.payload.acked_message_id, live_message.message_id);
    assert_eq!(ack.payload.status, TransportAckStatus::Accepted);

    let old_session = server
        .store
        .get_session(&old_accepted.payload.session_id)
        .await
        .expect("old session lookup should succeed")
        .expect("old session should remain durably recorded");
    let new_session = server
        .store
        .get_session(&new_accepted.payload.session_id)
        .await
        .expect("new session lookup should succeed")
        .expect("new session should be durably recorded");
    assert_eq!(old_session.status, SessionStatus::Stale);
    assert_eq!(new_session.status, SessionStatus::Active);
    assert_eq!(server.inbound.admitted().len(), 1);

    drop(old_stream);
    drop(new_stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_server_task_disconnects_the_session_and_releases_the_socket() {
    let mut server = TestServer::start().await;
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_before_abort").await;

    server.server_task.abort();
    let join_error = time::timeout(IO_TIMEOUT, &mut server.server_task)
        .await
        .expect("aborted server task should stop within the lifecycle bound")
        .expect_err("aborted server task should report cancellation");
    assert!(join_error.is_cancelled());

    let stored = time::timeout(IO_TIMEOUT, async {
        loop {
            let stored = server
                .store
                .get_session(&accepted.payload.session_id)
                .await
                .expect("cancelled session lookup should succeed")
                .expect("cancelled session should remain durably recorded");
            if stored.status != SessionStatus::Active {
                break stored;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection cancellation should durably close the session");
    assert_eq!(stored.status, SessionStatus::Disconnected);

    expect_closed_without_frame(&mut stream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_handshake_during_durable_activation_cleans_the_committed_epoch() {
    let mut server = TestServer::start().await;
    let mut old_stream = server.connect().await;
    authenticate(&mut old_stream, "hello_before_blocked_replacement").await;

    let blocker = server
        .store
        .begin_write()
        .await
        .expect("test write transaction should block replacement commit");
    let mut replacement_stream = server.connect().await;
    replacement_stream
        .write_all(&frame(&hello("hello_blocked_replacement", CLIENT_SECRET)))
        .await
        .expect("replacement session.hello write should succeed");

    expect_closed_without_frame(&mut old_stream).await;

    server.server_task.abort();
    let join_error = time::timeout(IO_TIMEOUT, &mut server.server_task)
        .await
        .expect("aborted server task should stop within the lifecycle bound")
        .expect_err("aborted server task should report cancellation");
    assert!(join_error.is_cancelled());
    blocker
        .rollback()
        .await
        .expect("releasing the test write lock should succeed");

    let replacement_id = SessionId::from("session_2");
    let stored = time::timeout(IO_TIMEOUT, async {
        loop {
            if let Some(stored) = server
                .store
                .get_session(&replacement_id)
                .await
                .expect("replacement session lookup should succeed")
            {
                if stored.status != SessionStatus::Active {
                    break stored;
                }
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled activation should durably close a commit-uncertain session");
    assert_eq!(stored.status, SessionStatus::Disconnected);

    expect_closed_without_frame(&mut replacement_stream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_session_open_cannot_outlive_the_handshake_deadline() {
    let server = TestServer::start().await;
    let blocker = server
        .store
        .begin_write()
        .await
        .expect("test write transaction should block durable activation");
    let mut stream = server.connect().await;
    let started_at = time::Instant::now();

    stream
        .write_all(&frame(&hello("hello_blocked_open", CLIENT_SECRET)))
        .await
        .expect("blocked session.hello write should succeed");
    expect_closed_without_frame(&mut stream).await;

    assert!(
        started_at.elapsed() < IO_TIMEOUT,
        "durable session open must remain inside the transport handshake deadline"
    );
    assert!(
        server
            .events
            .kinds()
            .contains(&TransportEventKind::HandshakeRejected),
        "timing out service.open should emit a handshake rejection event"
    );

    blocker
        .rollback()
        .await
        .expect("releasing the test write lock should succeed");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_cursor_is_handed_off_without_automatic_command_replay() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    let mut hello = hello("hello_with_resume", CLIENT_SECRET);
    hello.payload.resume = Some(ResumeCursor {
        previous_session_id: Some(SessionId::from("previous_session")),
        last_gateway_message_id: Some(MessageId::from("gateway_message_41")),
        last_gateway_sequence: Some(41),
        last_client_message_id: Some(MessageId::from("client_message_19")),
        last_client_sequence: Some(19),
        pending_command_ids: Some(vec![CommandId::from("pending_command_1")]),
    });
    stream
        .write_all(&frame(&hello))
        .await
        .expect("session.hello with resume cursor should be written");
    let accepted = read_accepted(&mut stream).await;

    let admitted = server.resumes.admitted();
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].0.session_id, accepted.payload.session_id);
    assert_eq!(admitted[0].1.hello_message_id, hello.message_id);
    assert_eq!(admitted[0].1.cursor, hello.payload.resume.unwrap());
    assert!(server.inbound.admitted().is_empty());

    let no_automatic_replay =
        time::timeout(Duration::from_millis(50), read_payload(&mut stream)).await;
    assert!(
        no_automatic_replay.is_err(),
        "resume admission must not automatically replay execution.command"
    );

    drop(stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_cursor_failure_rejects_handshake_without_activating_a_session() {
    let server = TestServer::start().await;
    server.resumes.reject.store(true, Ordering::Release);
    let mut stream = server.connect().await;
    let mut hello = hello("hello_resume_rejected", CLIENT_SECRET);
    hello.payload.resume = Some(ResumeCursor {
        previous_session_id: Some(SessionId::from("previous_session")),
        ..ResumeCursor::default()
    });
    stream
        .write_all(&frame(&hello))
        .await
        .expect("session.hello with rejected resume should be written");

    let payload = read_payload(&mut stream).await;
    let rejected: WireMessage<SessionRejected> =
        decode_wire_message(&payload, SUPPORTED_SCHEMA_VERSION)
            .expect("resume failure should return session.rejected");
    assert_eq!(rejected.payload.reason, ErrorCode::ServiceUnavailable);
    expect_closed_without_frame(&mut stream).await;
    assert!(server
        .store
        .get_session(&SessionId::from("session_1"))
        .await
        .expect("session lookup should succeed")
        .is_none());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_cursor_timeout_rejects_handshake_without_activating_a_session() {
    let server = TestServer::start().await;
    server.resumes.pending.store(true, Ordering::Release);
    let mut stream = server.connect().await;
    let mut hello = hello("hello_resume_timeout", CLIENT_SECRET);
    hello.payload.resume = Some(ResumeCursor {
        previous_session_id: Some(SessionId::from("previous_session")),
        ..ResumeCursor::default()
    });
    stream
        .write_all(&frame(&hello))
        .await
        .expect("session.hello with pending resume should be written");

    let payload = read_payload(&mut stream).await;
    let rejected: WireMessage<SessionRejected> =
        decode_wire_message(&payload, SUPPORTED_SCHEMA_VERSION)
            .expect("resume timeout should return session.rejected");
    assert_eq!(rejected.payload.reason, ErrorCode::ServiceUnavailable);
    expect_closed_without_frame(&mut stream).await;
    assert!(server.resumes.admitted().is_empty());
    assert!(server
        .events
        .kinds()
        .contains(&TransportEventKind::InboundAdmissionFailed));
    assert!(server
        .store
        .get_session(&SessionId::from("session_1"))
        .await
        .expect("session lookup should succeed")
        .is_none());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_clock_anomalies_and_time_sync_recovery_emit_transport_events() {
    let server = TestServer::start().await;
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_clock_events").await;
    let session_id = &accepted.payload.session_id;

    let mut skewed_tick = client_message("skewed_tick", session_id, 1);
    skewed_tick.sent_at = Some(1_799_999_990_000);
    stream
        .write_all(&frame(&skewed_tick))
        .await
        .expect("clock-skewed market tick should be written");
    assert_eq!(
        read_ack(&mut stream).await.payload.status,
        TransportAckStatus::Accepted
    );

    let unhealthy = heartbeat(
        "heartbeat_unhealthy",
        session_id,
        2,
        1_799_999_990_000,
        ClockSyncStatus::Unsynced,
        None,
    );
    stream
        .write_all(&frame(&unhealthy))
        .await
        .expect("unhealthy heartbeat should be written");
    assert_eq!(
        read_ack(&mut stream).await.payload.status,
        TransportAckStatus::Accepted
    );

    let repeated_unhealthy = heartbeat(
        "heartbeat_still_unhealthy",
        session_id,
        3,
        1_800_000_000_000,
        ClockSyncStatus::Unsynced,
        None,
    );
    stream
        .write_all(&frame(&repeated_unhealthy))
        .await
        .expect("repeated unhealthy heartbeat should be written");
    assert_eq!(
        read_ack(&mut stream).await.payload.status,
        TransportAckStatus::Accepted
    );

    let restored = heartbeat(
        "heartbeat_restored",
        session_id,
        4,
        1_800_000_000_000,
        ClockSyncStatus::Synced,
        Some((1_800_000_000_000, 10)),
    );
    stream
        .write_all(&frame(&restored))
        .await
        .expect("restored heartbeat should be written");
    assert_eq!(
        read_ack(&mut stream).await.payload.status,
        TransportAckStatus::Accepted
    );

    let event_kinds = server.events.kinds();
    assert!(event_kinds.contains(&TransportEventKind::ClockSkewDetected));
    assert_eq!(
        event_kinds
            .iter()
            .filter(|kind| **kind == TransportEventKind::TimeSyncUnhealthy)
            .count(),
        1
    );
    assert!(event_kinds.contains(&TransportEventKind::TimeSyncRestored));
    assert_eq!(
        event_kinds
            .iter()
            .filter(|kind| **kind == TransportEventKind::TimeSyncRestored)
            .count(),
        1
    );
    let stored = server
        .store
        .get_session(session_id)
        .await
        .expect("session lookup should succeed")
        .expect("session should remain durably recorded");
    assert_eq!(stored.status, SessionStatus::Active);
    assert_eq!(stored.clock_sync_status, Some(ClockSyncStatus::Synced));

    drop(stream);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_inbound_admission_failure_emits_an_event_without_an_ack() {
    let server = TestServer::start().await;
    server.inbound.fail.store(true, Ordering::Release);
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_admission_failure").await;
    let message = client_message("failed_admission", &accepted.payload.session_id, 1);
    stream
        .write_all(&frame(&message))
        .await
        .expect("message with failing durable admission should be written");

    expect_closed_without_frame(&mut stream).await;
    assert!(server.inbound.admitted().is_empty());
    assert!(server
        .events
        .kinds()
        .contains(&TransportEventKind::InboundAdmissionFailed));
    let stored = time::timeout(IO_TIMEOUT, async {
        loop {
            let stored = server
                .store
                .get_session(&accepted.payload.session_id)
                .await
                .expect("failed-admission session lookup should succeed")
                .expect("failed-admission session should remain durably recorded");
            if stored.status != SessionStatus::Active {
                break stored;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("failed admission should durably close the session");
    assert_eq!(stored.status, SessionStatus::Disconnected);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_inbound_admission_timeout_emits_an_event_without_an_ack() {
    let server = TestServer::start().await;
    server.inbound.block_next();
    let mut stream = server.connect().await;
    let accepted = authenticate(&mut stream, "hello_admission_timeout").await;
    let message = client_message("timed_out_admission", &accepted.payload.session_id, 1);
    stream
        .write_all(&frame(&message))
        .await
        .expect("message with pending durable admission should be written");
    time::timeout(IO_TIMEOUT, server.inbound.wait_until_blocked())
        .await
        .expect("inbound handler should reach the durable admission gate");

    expect_closed_without_frame(&mut stream).await;
    assert!(server.inbound.admitted().is_empty());
    assert!(server
        .events
        .kinds()
        .contains(&TransportEventKind::InboundAdmissionFailed));
    let stored = time::timeout(IO_TIMEOUT, async {
        loop {
            let stored = server
                .store
                .get_session(&accepted.payload.session_id)
                .await
                .expect("timed-out admission session lookup should succeed")
                .expect("timed-out admission session should remain durably recorded");
            if stored.status != SessionStatus::Active {
                break stored;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed-out admission should durably close the session");
    assert_eq!(stored.status, SessionStatus::Disconnected);

    server.shutdown().await;
}

use std::{
    fs,
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use sinan_execution::ServerClock;
use sinan_gateway::{
    AuthenticatedSessionContext, ClientCredential, ConfiguredClientAuthenticator,
    ExecutionTransportConfig, ExecutionWebSocketBinding, GatewayConnectionService,
    GatewayIdGenerator, GatewaySessionConfig, GatewaySessionRegistry, InboundAdmission,
    InboundAdmissionFuture, InboundMessage, InboundMessagePort, LiveSessionRegistry,
    NoopTransportEventPort, RejectingSessionResumePort,
};
use sinan_protocol::{
    decode_wire_message, ExecutionClientMessageType, ExecutionClientPlatform, HelloAcceptedPayload,
    HelloPayload, SessionRejected, TransportAck, TransportAckStatus, WireMessage,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{SqliteStateStore, StoreOptions};
use sinan_types::{AccountId, ClientId, ErrorCode, MessageId, SessionId, TerminalId};
use tokio::{net::TcpStream, sync::watch, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WebSocketError, Message},
    MaybeTlsStream, WebSocketStream,
};

const CLIENT_ID: &str = "client_1";
const ACCOUNT_ID: &str = "account_1";
const TERMINAL_ID: &str = "terminal_1";
const CLIENT_SECRET: &str = "execution-client-secret";
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
            "sinan-execution-ws-{}-{timestamp}-{sequence}.sqlite",
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

struct FixedClock(AtomicI64);

impl FixedClock {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }
}

impl ServerClock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct DeterministicIds {
    next_session: AtomicU64,
    next_message: AtomicU64,
}

impl GatewayIdGenerator for DeterministicIds {
    fn next_session_id(&self) -> SessionId {
        SessionId::new(format!(
            "session_{}",
            self.next_session.fetch_add(1, Ordering::Relaxed) + 1
        ))
    }

    fn next_message_id(&self) -> MessageId {
        MessageId::new(format!(
            "gateway_message_{}",
            self.next_message.fetch_add(1, Ordering::Relaxed) + 1
        ))
    }
}

#[derive(Clone, Debug)]
struct RecordedInbound {
    session: AuthenticatedSessionContext,
    message: InboundMessage,
}

#[derive(Default)]
struct RecordingInboundPort {
    admitted: Mutex<Vec<RecordedInbound>>,
}

impl RecordingInboundPort {
    fn snapshot(&self) -> Vec<RecordedInbound> {
        self.admitted
            .lock()
            .expect("recording inbound mutex should not be poisoned")
            .clone()
    }
}

impl InboundMessagePort for RecordingInboundPort {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        message: InboundMessage,
    ) -> InboundAdmissionFuture<'a> {
        Box::pin(async move {
            self.admitted
                .lock()
                .expect("recording inbound mutex should not be poisoned")
                .push(RecordedInbound {
                    session: session.clone(),
                    message,
                });
            Ok(InboundAdmission::Accepted)
        })
    }
}

struct RunningServer {
    address: std::net::SocketAddr,
    inbound: Arc<RecordingInboundPort>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), sinan_gateway::ExecutionWebSocketError>>,
    _database: TestDatabase,
}

impl RunningServer {
    fn url(&self, path: &str) -> String {
        format!("ws://{}{}", self.address, path)
    }

    async fn stop(self) {
        self.shutdown
            .send(true)
            .expect("Execution WebSocket server should observe shutdown");
        deadline(self.task)
            .await
            .expect("Execution WebSocket server task should not panic")
            .expect("Execution WebSocket server should stop cleanly");
    }
}

async fn deadline<F: Future>(future: F) -> F::Output {
    timeout(TEST_TIMEOUT, future)
        .await
        .expect("network operation exceeded the test deadline")
}

async fn start_server(max_message_bytes: usize) -> RunningServer {
    let database = TestDatabase::unique();
    let mut options = StoreOptions::new(database.url());
    options.max_connections = 4;
    options.busy_timeout = TEST_TIMEOUT;
    let store = SqliteStateStore::connect(options)
        .await
        .expect("Execution WebSocket test store should connect");

    let clock = Arc::new(FixedClock::new(1_700_000_000_000));
    let sessions = GatewaySessionRegistry::new(
        store,
        Arc::new(LiveSessionRegistry::new()),
        clock,
        GatewaySessionConfig {
            max_clock_offset_ms: 250,
            max_time_sync_age_ms: 90_000,
            max_time_sync_rtt_ms: 1_000,
        },
    )
    .expect("Gateway session config should be valid");
    let authenticator = ConfiguredClientAuthenticator::new([ClientCredential::new(
        CLIENT_ID,
        ACCOUNT_ID,
        CLIENT_SECRET,
        None,
    )])
    .expect("test client credential should be valid");
    let inbound = Arc::new(RecordingInboundPort::default());
    let mut config = ExecutionTransportConfig::default();
    config.handshake_timeout = TEST_TIMEOUT;
    config.write_timeout = TEST_TIMEOUT;
    config.heartbeat_interval_ms = 30_000;
    config.heartbeat_timeout_ms = 90_000;
    config.max_message_bytes = max_message_bytes;
    config.max_frame_bytes = config.max_frame_bytes.max(max_message_bytes);

    let service = GatewayConnectionService::new(
        sessions,
        Arc::new(authenticator),
        Arc::new(DeterministicIds::default()),
        inbound.clone(),
        Arc::new(RejectingSessionResumePort),
        Arc::new(NoopTransportEventPort),
        config,
    )
    .expect("Execution WebSocket connection service should be valid");
    let server = ExecutionWebSocketBinding::new(service)
        .bind("127.0.0.1:0")
        .await
        .expect("Execution WebSocket listener should bind");
    let address = server
        .local_addr()
        .expect("Execution WebSocket listener should have an address");
    let (shutdown, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(server.serve(shutdown_rx));

    RunningServer {
        address,
        inbound,
        shutdown,
        task,
        _database: database,
    }
}

fn hello(token: &str, message_id: &str) -> WireMessage<HelloPayload> {
    WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::SessionHello,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: None,
        correlation_id: None,
        causation_id: None,
        sent_at: None,
        sequence: None,
        payload: HelloPayload {
            client_id: ClientId::from(CLIENT_ID),
            platform: ExecutionClientPlatform::Mt5,
            terminal_id: Some(TerminalId::from(TERMINAL_ID)),
            account_id: AccountId::from(ACCOUNT_ID),
            token: token.to_owned(),
            capabilities: vec!["market-data".to_owned()],
            resume: None,
        },
    }
}

async fn connect(server: &RunningServer) -> ClientSocket {
    deadline(connect_async(server.url("/execution-client")))
        .await
        .expect("Execution WebSocket endpoint should upgrade")
        .0
}

async fn send_json<T: serde::Serialize>(socket: &mut ClientSocket, value: &T) -> Vec<u8> {
    let bytes = serde_json::to_vec(value).expect("test wire message should encode");
    let text = String::from_utf8(bytes.clone()).expect("JSON should be UTF-8");
    deadline(socket.send(Message::Text(text.into())))
        .await
        .expect("WebSocket text message should send");
    bytes
}

async fn receive_text(socket: &mut ClientSocket) -> Vec<u8> {
    match deadline(socket.next())
        .await
        .expect("WebSocket should produce a message")
        .expect("WebSocket receive should succeed")
    {
        Message::Text(text) => text.as_bytes().to_vec(),
        message => panic!("expected a WebSocket Text message, got {message:?}"),
    }
}

async fn authenticate(socket: &mut ClientSocket, message_id: &str) -> HelloAcceptedPayload {
    send_json(socket, &hello(CLIENT_SECRET, message_id)).await;
    let bytes = receive_text(socket).await;
    let accepted = decode_wire_message::<HelloAcceptedPayload>(&bytes, SUPPORTED_SCHEMA_VERSION)
        .expect("server should return a valid session.accepted");
    assert_eq!(
        accepted.message_type,
        ExecutionClientMessageType::SessionAccepted
    );
    assert_eq!(accepted.sequence, Some(1));
    assert_eq!(
        accepted.session_id,
        Some(accepted.payload.session_id.clone())
    );
    accepted.payload
}

async fn assert_fail_closed(socket: &mut ClientSocket) {
    match deadline(socket.next()).await {
        None | Some(Ok(Message::Close(_))) | Some(Err(_)) => {}
        Some(Ok(message)) => panic!("expected fail-closed transport, got {message:?}"),
    }
}

#[tokio::test]
async fn only_execution_client_path_can_upgrade() {
    let server = start_server(4_096).await;

    let error = deadline(connect_async(server.url("/events")))
        .await
        .expect_err("non-Execution endpoint must reject the WebSocket upgrade");
    match error {
        WebSocketError::Http(response) => {
            assert_eq!(response.status(), 404);
        }
        error => panic!("expected HTTP rejection for the wrong path, got {error:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn text_messages_share_the_inbound_pipeline_and_receive_sequence_two_ack() {
    let server = start_server(4_096).await;
    let mut socket = connect(&server).await;
    let accepted = authenticate(&mut socket, "hello_text").await;

    let inbound = WireMessage {
        message_id: MessageId::from("market_tick_1"),
        message_type: ExecutionClientMessageType::MarketTick,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(accepted.session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(1_700_000_000_000),
        sequence: Some(1),
        payload: json!({
            "account_id": ACCOUNT_ID,
            "symbol": "EURUSD",
            "bid": 1.1,
            "ask": 1.2,
            "observed_at": 1_700_000_000_000_i64
        }),
    };
    let inbound_bytes = send_json(&mut socket, &inbound).await;

    let ack_bytes = receive_text(&mut socket).await;
    let ack = decode_wire_message::<TransportAck>(&ack_bytes, SUPPORTED_SCHEMA_VERSION)
        .expect("server should return a valid transport.ack");
    assert_eq!(ack.message_type, ExecutionClientMessageType::TransportAck);
    assert_eq!(ack.sequence, Some(2));
    assert_eq!(ack.session_id, Some(accepted.session_id.clone()));
    assert_eq!(ack.payload.acked_message_id, inbound.message_id);
    assert_eq!(
        ack.payload.acked_message_type,
        ExecutionClientMessageType::MarketTick
    );
    assert_eq!(ack.payload.status, TransportAckStatus::Accepted);

    let recorded = server.inbound.snapshot();
    assert_eq!(
        recorded.len(),
        1,
        "one Text message must admit one envelope"
    );
    assert_eq!(recorded[0].session.session_id, accepted.session_id);
    assert_eq!(recorded[0].session.client_id, ClientId::from(CLIENT_ID));
    assert_eq!(
        recorded[0].message.envelope.message_id,
        MessageId::from("market_tick_1")
    );
    assert_eq!(recorded[0].message.wire_bytes, inbound_bytes);

    deadline(socket.close(None))
        .await
        .expect("client WebSocket should close");
    server.stop().await;
}

#[tokio::test]
async fn binary_messages_fail_closed_before_and_after_authentication() {
    let server = start_server(4_096).await;

    let mut first = connect(&server).await;
    deadline(first.send(Message::Binary(vec![1, 2, 3].into())))
        .await
        .expect("binary hello should reach the server");
    assert_fail_closed(&mut first).await;
    assert!(server.inbound.snapshot().is_empty());

    let mut active = connect(&server).await;
    authenticate(&mut active, "hello_before_binary").await;
    deadline(active.send(Message::Binary(vec![4, 5, 6].into())))
        .await
        .expect("active binary message should reach the server");
    assert_fail_closed(&mut active).await;
    assert!(
        server.inbound.snapshot().is_empty(),
        "Binary messages must never reach the inbound port"
    );

    server.stop().await;
}

#[tokio::test]
async fn oversized_active_message_fails_closed_without_admission() {
    let max_message_bytes = 1_024;
    let server = start_server(max_message_bytes).await;
    let mut socket = connect(&server).await;
    authenticate(&mut socket, "hello_before_oversize").await;

    deadline(socket.send(Message::Text("x".repeat(max_message_bytes + 1).into())))
        .await
        .expect("oversized message should leave the client");
    assert_fail_closed(&mut socket).await;
    assert!(
        server.inbound.snapshot().is_empty(),
        "oversized messages must never reach the inbound port"
    );

    server.stop().await;
}

#[tokio::test]
async fn bad_client_auth_is_rejected_then_closed() {
    let server = start_server(4_096).await;
    let mut socket = connect(&server).await;

    send_json(&mut socket, &hello("wrong-secret", "hello_bad_auth")).await;
    let rejection_bytes = receive_text(&mut socket).await;
    let rejection =
        decode_wire_message::<SessionRejected>(&rejection_bytes, SUPPORTED_SCHEMA_VERSION)
            .expect("bad auth should return a valid session.rejected");
    assert_eq!(
        rejection.message_type,
        ExecutionClientMessageType::SessionRejected
    );
    assert_eq!(rejection.sequence, None);
    assert_eq!(rejection.payload.reason, ErrorCode::AuthenticationFailed);
    assert_fail_closed(&mut socket).await;
    assert!(server.inbound.snapshot().is_empty());

    server.stop().await;
}

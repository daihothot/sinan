use std::{
    fs, io,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{de::DeserializeOwned, Serialize};
use sinan_core::{
    CoreInboundProcessor, CoreSessionResumeProcessor, DurableDeliveryDisposition,
    DurableOutboundConfig, DurableOutboundProcessOutcome, DurableOutboundProcessor,
    RiskWorkflowLeg, RiskWorkflowOutcome, RiskWorkflowProcessor, TrustedExecutionResolver,
    TrustedLegExecutionParameters, TrustedRiskWorkflowContext,
};
use sinan_execution::ServerClock;
use sinan_gateway::{
    ClientCredential, ConfiguredClientAuthenticator, DurableInboundMessagePort,
    DurableRecoveryConfig, DurableRecoveryDispatcher, DurableSessionResumePort,
    ExecutionTransportConfig, GatewayConnectionService, GatewayOutboundAdapter,
    GatewayOutboundConfig, GatewaySessionConfig, GatewaySessionRegistry, LiveSessionRegistry,
    NativeTcpBinding, NativeTcpError, ProductionTransportEventPort, UuidGatewayIdGenerator,
};
use sinan_protocol::{
    decode_wire_message, CommandInboxStatus, CommandReceived, ExecutionClientMessageType,
    ExecutionClientPlatform, HeartbeatPayload, HelloAcceptedPayload, HelloPayload,
    NativeTcpFrameEncoder, ProtocolReason, ReconciliationReason, ReconciliationRequest,
    ReconciliationResult, TimeSyncRequest, TimeSyncResponse, TransportAck, TransportAckStatus,
    WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_risk::{CircuitBreakerState, RiskPolicy, StrategyRiskPolicy, POSITION_SIZING_VERSION_V1};
use sinan_store::{
    CanonicalJson, CoreEventMetadata, DurableWorkStatus, NewCircuitBreakerSnapshot,
    NewReconciliationResult, NewReconciliationRun, NewRiskCapacitySnapshot, NewTradeIntent,
    ReconciliationCompleteness, ReconciliationDisposition, ReconciliationEvaluation,
    SqliteStateStore, StoreOptions,
};
use sinan_types::{
    AccountId, AccountSnapshot, ClientId, ClockSyncStatus, CommandDeliveryAttemptStatus,
    CorrelationId, DecisionId, ExecutionCommand, ExecutionCommandStatus, ExecutionFailurePolicy,
    ExecutionLegStatus, ExecutionPlanMode, ExecutionPlanStatus, ExecutionPolicy, FillingPolicy,
    IdempotencyKey, IntentId, MarketSnapshot, MessageId, OrderType, RequestId, RiskCapacity,
    SessionId, StrategyId, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TerminalId,
    TimePolicy, TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus, WireOutboxStatus,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    task::JoinHandle,
    time,
};

const NOW: i64 = 10_000;
const OBSERVED_AT: i64 = 9_900;
const ACCOUNT_ID: &str = "account-1";
const CLIENT_ID: &str = "client-1";
const TERMINAL_ID: &str = "terminal-1";
const STRATEGY_ID: &str = "strategy-1";
const SYMBOL: &str = "SYNTH-A";
const CLIENT_SECRET: &str = "client-auth-secret";
const MAX_FRAME_BYTES: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(3);

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
            "sinan-native-execution-e2e-{}-{timestamp}-{sequence}.sqlite",
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

#[derive(Clone)]
struct ManualClock(Arc<AtomicI64>);

impl ManualClock {
    fn new(now: i64) -> Self {
        Self(Arc::new(AtomicI64::new(now)))
    }

    fn set(&self, now: i64) {
        self.0.store(now, Ordering::Release);
    }
}

impl ServerClock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

struct TestServer {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), NativeTcpError>>,
}

impl TestServer {
    async fn start(store: SqliteStateStore, clock: ManualClock) -> (Self, GatewaySessionRegistry) {
        let transport = ExecutionTransportConfig {
            handshake_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            inbound_admission_timeout: Duration::from_secs(1),
            event_write_timeout: Duration::from_secs(1),
            heartbeat_interval_ms: 1_000,
            heartbeat_timeout_ms: 5_000,
            time_sync_interval_ms: 1_000,
            max_time_sync_rtt_ms: 1_000,
            max_clock_offset_ms: 250,
            max_inflight_commands: 8,
            max_frame_bytes: MAX_FRAME_BYTES,
            max_message_bytes: MAX_FRAME_BYTES,
            outbound_queue_capacity: 8,
            tcp_read_chunk_bytes: 1_024,
            max_connections: 4,
            max_pending_handshakes: 4,
        };
        let sessions = GatewaySessionRegistry::new(
            store.clone(),
            Arc::new(LiveSessionRegistry::new()),
            Arc::new(clock),
            GatewaySessionConfig {
                max_clock_offset_ms: transport.max_clock_offset_ms,
                max_time_sync_age_ms: transport.heartbeat_timeout_ms,
                max_time_sync_rtt_ms: transport.max_time_sync_rtt_ms,
            },
        )
        .unwrap();
        let service = GatewayConnectionService::new(
            sessions.clone(),
            Arc::new(
                ConfiguredClientAuthenticator::new([ClientCredential::new(
                    CLIENT_ID,
                    ACCOUNT_ID,
                    CLIENT_SECRET,
                    None,
                )])
                .unwrap(),
            ),
            Arc::new(UuidGatewayIdGenerator),
            Arc::new(DurableInboundMessagePort::new(store.clone())),
            Arc::new(DurableSessionResumePort::new(store.clone())),
            Arc::new(ProductionTransportEventPort::without_publisher(store)),
            transport,
        )
        .unwrap();
        let server = NativeTcpBinding::new(service)
            .bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(server.serve(receiver));
        (
            Self {
                addr,
                shutdown,
                task,
            },
            sessions,
        )
    }

    async fn connect(&self) -> TcpStream {
        time::timeout(IO_TIMEOUT, TcpStream::connect(self.addr))
            .await
            .expect("Native TCP connect should not time out")
            .expect("Native TCP connect should succeed")
    }

    async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        time::timeout(IO_TIMEOUT, &mut self.task)
            .await
            .expect("Native TCP shutdown should not time out")
            .expect("Native TCP task should not panic")
            .expect("Native TCP listener should stop cleanly");
    }
}

async fn test_store() -> (TestDatabase, SqliteStateStore) {
    let database = TestDatabase::unique();
    let mut options = StoreOptions::new(database.url());
    options.max_connections = 8;
    options.busy_timeout = Duration::from_secs(3);
    let store = SqliteStateStore::connect(options).await.unwrap();
    (database, store)
}

fn frame<T: Serialize>(message: &T) -> Vec<u8> {
    NativeTcpFrameEncoder::new(MAX_FRAME_BYTES)
        .encode_json(message)
        .expect("test message should fit Native TCP frame")
}

async fn write_message<T: Serialize>(stream: &mut TcpStream, message: &T) {
    stream
        .write_all(&frame(message))
        .await
        .expect("Native TCP message write should succeed");
}

async fn read_message<T: DeserializeOwned>(stream: &mut TcpStream) -> WireMessage<T> {
    let bytes = time::timeout(IO_TIMEOUT, async {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Native TCP frame length",
            ));
        }
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;
        Ok::<_, io::Error>(payload)
    })
    .await
    .expect("Native TCP read should not time out")
    .expect("Native TCP response should contain a complete frame");
    decode_wire_message(&bytes, SUPPORTED_SCHEMA_VERSION).expect("response should be valid ECP")
}

fn hello() -> WireMessage<HelloPayload> {
    WireMessage {
        message_id: MessageId::from("hello-e2e"),
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
            token: CLIENT_SECRET.to_owned(),
            capabilities: vec!["MARKET_ORDER".to_owned(), "COMMAND_RECEIPT".to_owned()],
            resume: None,
        },
    }
}

async fn authenticate_and_sync(stream: &mut TcpStream) -> SessionId {
    write_message(stream, &hello()).await;
    let accepted: WireMessage<HelloAcceptedPayload> = read_message(stream).await;
    assert_eq!(
        accepted.message_type,
        ExecutionClientMessageType::SessionAccepted
    );
    let session_id = accepted.payload.session_id;

    let time_sync = WireMessage {
        message_id: MessageId::from("time-sync-e2e"),
        message_type: ExecutionClientMessageType::TimeSyncRequest,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: None,
        sequence: Some(1),
        payload: TimeSyncRequest {
            request_id: RequestId::from("time-sync-request-e2e"),
        },
    };
    write_message(stream, &time_sync).await;
    let synced: WireMessage<TimeSyncResponse> = read_message(stream).await;
    assert_eq!(
        synced.message_type,
        ExecutionClientMessageType::TimeSyncResponse
    );
    assert_eq!(synced.payload.request_id, time_sync.payload.request_id);

    let heartbeat = WireMessage {
        message_id: MessageId::from("heartbeat-synced-e2e"),
        message_type: ExecutionClientMessageType::Heartbeat,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(NOW),
        sequence: Some(2),
        payload: HeartbeatPayload {
            effective_server_now: synced.payload.server_time,
            clock_sync_status: ClockSyncStatus::Synced,
            last_time_sync_at_server_ms: Some(synced.payload.server_send_at),
            last_time_sync_rtt_ms: Some(1),
            server_time_offset_ms: Some(0),
            send_queue_depth: Some(0),
            command_inbox_depth: Some(0),
        },
    };
    write_message(stream, &heartbeat).await;
    let ack: WireMessage<TransportAck> = read_message(stream).await;
    assert_eq!(ack.payload.acked_message_id, heartbeat.message_id);
    assert_eq!(ack.payload.status, TransportAckStatus::Accepted);
    session_id
}

fn intent() -> TradeIntent {
    TradeIntent {
        intent_id: IntentId::from("intent-native-e2e"),
        decision_id: DecisionId::from("decision-native-e2e"),
        strategy_id: StrategyId::from(STRATEGY_ID),
        correlation_id: CorrelationId::from("correlation-native-e2e"),
        idempotency_key: IdempotencyKey::from("intent-key-native-e2e"),
        account_id: AccountId::from(ACCOUNT_ID),
        symbol: SymbolCode::from(SYMBOL),
        timeframe: TimeframeCode::from("M5"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "Native TCP execution E2E".to_owned(),
        proposed_risk_pct: 1.0,
        proposed_sl: Some(90.0),
        proposed_tp: Some(120.0),
        proposed_legs: None,
        decision_timestamp: NOW - 30,
        signal_expires_at: NOW + 5_000,
        requested_at: NOW - 20,
    }
}

fn account() -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from(ACCOUNT_ID),
        balance: 10_000.0,
        equity: 10_000.0,
        margin: 0.0,
        free_margin: 10_000.0,
        currency: "USD".to_owned(),
        observed_at: OBSERVED_AT,
    }
}

fn metadata() -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::from(ACCOUNT_ID),
        symbol: SymbolCode::from(SYMBOL),
        broker_symbol: SYMBOL.to_owned(),
        digits: 2,
        point: 0.01,
        tick_size: 1.0,
        tick_value_loss: 10.0,
        contract_size: 1.0,
        volume_min: 0.01,
        volume_max: 100.0,
        volume_step: 0.01,
        stops_level_points: 0,
        freeze_level_points: 0,
        margin_initial: Some(100.0),
        margin_maintenance: Some(100.0),
        trade_mode: SymbolTradeMode::Full,
        observed_at: OBSERVED_AT,
    }
}

fn event_metadata(event_id: &str, event_type: &str, event_at: i64) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: event_id.to_owned(),
        event_type: event_type.to_owned(),
        aggregate_type: "reconciliation".to_owned(),
        aggregate_id: "reconciliation-native-e2e".to_owned(),
        message_id: None,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at,
        received_at: event_at,
        created_at: event_at,
        source: "native-tcp-e2e".to_owned(),
    }
}

async fn seed_trusted_risk_inputs(store: &SqliteStateStore) {
    store
        .insert_trade_intent(NewTradeIntent {
            intent: intent(),
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: NOW - 10,
        })
        .await
        .unwrap();
    let request_id = RequestId::from("reconciliation-native-e2e");
    store
        .create_reconciliation_run(NewReconciliationRun {
            request: ReconciliationRequest {
                request_id: request_id.clone(),
                account_id: AccountId::from(ACCOUNT_ID),
                terminal_id: None,
                client_id: None,
                reason: ReconciliationReason::ManualRequest,
                command_ids: None,
                since_server_time: Some(OBSERVED_AT - 30),
            },
            requested_at: OBSERVED_AT - 20,
            event_metadata: event_metadata(
                "reconciliation-request-native-e2e",
                "reconciliation.request",
                OBSERVED_AT - 20,
            ),
        })
        .await
        .unwrap();
    store
        .commit_reconciliation_result(NewReconciliationResult {
            result: ReconciliationResult {
                request_id: request_id.clone(),
                account_id: AccountId::from(ACCOUNT_ID),
                terminal_id: None,
                client_id: None,
                observed_at: OBSERVED_AT,
                account: Some(account()),
                positions: Vec::new(),
                orders: Vec::new(),
                symbol_metadata: vec![metadata()],
                unresolved_command_ids: Vec::new(),
            },
            evaluation: ReconciliationEvaluation {
                request_id,
                account_id: AccountId::from(ACCOUNT_ID),
                observed_at: Some(OBSERVED_AT),
                disposition: ReconciliationDisposition::Completed,
                command_ids: Vec::new(),
                findings: Vec::new(),
            },
            completeness: ReconciliationCompleteness {
                symbol_metadata_complete: true,
                command_scope_complete: true,
            },
            event_metadata: event_metadata(
                "reconciliation-result-native-e2e",
                "reconciliation.result",
                OBSERVED_AT,
            ),
        })
        .await
        .unwrap();
    store
        .update_market_snapshot(
            &AccountId::from(ACCOUNT_ID),
            &MarketSnapshot {
                symbol: SymbolCode::from(SYMBOL),
                broker_symbol: Some(SYMBOL.to_owned()),
                bid: 99.0,
                ask: 100.0,
                spread: 1.0,
                observed_at: NOW - 50,
            },
            NOW - 40,
        )
        .await
        .unwrap();
    store
        .record_risk_capacity_snapshot(NewRiskCapacitySnapshot {
            capacity: RiskCapacity {
                account_id: AccountId::from(ACCOUNT_ID),
                strategy_id: StrategyId::from(STRATEGY_ID),
                observed_at: OBSERVED_AT,
                daily_realized_loss_pct: 0.0,
                equity_drawdown_pct: 0.0,
                remaining_account_risk_pct: 5.0,
                remaining_portfolio_risk_pct: 5.0,
                remaining_strategy_legs: 4,
            },
            recorded_at: OBSERVED_AT,
        })
        .await
        .unwrap();
    let breaker = CircuitBreakerState::new().durable_snapshot_v1();
    store
        .write_circuit_breaker_snapshot(NewCircuitBreakerSnapshot {
            expected_head_revision: None,
            schema_version: breaker.schema_version().to_owned(),
            status: "CLOSED".to_owned(),
            recovery_epoch: breaker.recovery_epoch(),
            updated_at: OBSERVED_AT,
            payload: CanonicalJson::parse(&breaker.to_json().unwrap()).unwrap(),
        })
        .await
        .unwrap();
}

fn risk_policy() -> RiskPolicy {
    RiskPolicy {
        position_sizing_version: POSITION_SIZING_VERSION_V1.to_owned(),
        max_risk_per_trade_pct: 5.0,
        max_daily_loss_pct: 4.0,
        max_drawdown_pct: 10.0,
        max_symbol_exposure_pct: 100.0,
        max_total_exposure_pct: 100.0,
        max_margin_usage_pct: 100.0,
        require_stop_loss: true,
        reject_expired_signal: true,
        max_approval_ttl_ms: 500,
        max_snapshot_age_ms: 1_000,
        max_order_snapshot_age_ms: 1_000,
        max_market_snapshot_age_ms: 1_000,
        max_symbol_metadata_age_ms: 1_000,
        max_capacity_age_ms: 1_000,
        max_concurrent_positions: 10,
        require_valid_symbol_metadata: true,
        reject_trade_mode_disabled: true,
    }
}

fn strategy_policy() -> StrategyRiskPolicy {
    StrategyRiskPolicy {
        max_risk_per_trade_pct: 5.0,
        max_concurrent_legs: 4,
        require_stop_loss: true,
        signal_expiry_bars: 3,
    }
}

fn execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        mode: ExecutionPlanMode::Sequential,
        failure_policy: ExecutionFailurePolicy::CancelAll,
        timeout_ms: 500,
        max_command_ttl_ms: 400,
        rollback_policy: None,
    }
}

struct Resolver;

impl TrustedExecutionResolver for Resolver {
    fn resolve(
        &self,
        _intent: &TradeIntent,
        _leg: &RiskWorkflowLeg,
    ) -> Result<TrustedLegExecutionParameters, String> {
        Ok(TrustedLegExecutionParameters {
            dependency: Vec::new(),
            terminal_id: Some(TerminalId::from(TERMINAL_ID)),
            client_id: Some(ClientId::from(CLIENT_ID)),
            order_type: OrderType::Market,
            price: None,
            deviation_points: Some(20),
            magic: 42,
            comment: Some("native TCP E2E".to_owned()),
            filling_policy: Some(FillingPolicy::Ioc),
            time_policy: Some(TimePolicy::Gtc),
            expiration_time: None,
            estimated_cost_per_lot: 0.0,
        })
    }
}

async fn send_transport_ack(
    stream: &mut TcpStream,
    session_id: &SessionId,
    command_message: &WireMessage<ExecutionCommand>,
    received_at: i64,
) {
    let ack = WireMessage {
        message_id: MessageId::from("client-transport-ack-e2e"),
        message_type: ExecutionClientMessageType::TransportAck,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: command_message.correlation_id.clone(),
        causation_id: Some(command_message.message_id.as_str().into()),
        sent_at: Some(received_at),
        sequence: Some(3),
        payload: TransportAck {
            acked_message_id: command_message.message_id.clone(),
            acked_message_type: ExecutionClientMessageType::ExecutionCommand,
            status: TransportAckStatus::Accepted,
            reason: Some(ProtocolReason::Ok),
            received_at,
        },
    };
    write_message(stream, &ack).await;
}

async fn send_command_received(
    stream: &mut TcpStream,
    session_id: &SessionId,
    command_message: &WireMessage<ExecutionCommand>,
    received_at: i64,
) -> MessageId {
    let message_id = MessageId::from("client-command-received-e2e");
    let received = WireMessage {
        message_id: message_id.clone(),
        message_type: ExecutionClientMessageType::CommandReceived,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(session_id.clone()),
        correlation_id: command_message.correlation_id.clone(),
        causation_id: Some(command_message.message_id.as_str().into()),
        sent_at: Some(received_at),
        sequence: Some(4),
        payload: CommandReceived {
            command_id: command_message.payload.command_id.clone(),
            idempotency_key: command_message.payload.idempotency_key.clone(),
            account_id: command_message.payload.account_id.clone(),
            terminal_id: command_message.payload.terminal_id.clone(),
            client_id: command_message.payload.client_id.clone(),
            received_at,
            inbox_status: CommandInboxStatus::Recorded,
            reason: Some(ProtocolReason::Ok),
        },
    };
    write_message(stream, &received).await;
    let server_ack: WireMessage<TransportAck> = read_message(stream).await;
    assert_eq!(server_ack.payload.acked_message_id, message_id);
    assert_eq!(server_ack.payload.status, TransportAckStatus::Accepted);
    message_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_intent_reaches_command_received_over_native_tcp() {
    let (_database, store) = test_store().await;
    let clock = ManualClock::new(NOW);
    let (server, sessions) = TestServer::start(store.clone(), clock.clone()).await;
    let mut stream = server.connect().await;
    let session_id = authenticate_and_sync(&mut stream).await;
    let session = store.get_session(&session_id).await.unwrap().unwrap();
    assert_eq!(session.clock_sync_status, Some(ClockSyncStatus::Synced));

    seed_trusted_risk_inputs(&store).await;
    let risk = risk_policy();
    let strategy = strategy_policy();
    let execution = execution_policy();
    let resolver = Resolver;
    let context = TrustedRiskWorkflowContext::new(
        &risk,
        &strategy,
        &execution,
        &resolver,
        b"native-e2e-signing-secret",
    )
    .unwrap();
    let outcome = RiskWorkflowProcessor::new(store.clone())
        .process_intent(&IntentId::from("intent-native-e2e"), NOW, &context)
        .await
        .unwrap();
    let RiskWorkflowOutcome::ExecutionReady { plan, commands, .. } = outcome else {
        panic!("trusted BUY intent should produce an execution workflow")
    };
    let command = commands.into_iter().next().unwrap();
    let created = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created.status, ExecutionCommandStatus::Created);

    let outbound = DurableOutboundProcessor::new(
        store.clone(),
        Arc::new(
            GatewayOutboundAdapter::new(
                sessions,
                GatewayOutboundConfig {
                    confirmation_timeout_ms: 5_000,
                },
            )
            .unwrap(),
        ),
        Arc::new(clock.clone()),
        DurableOutboundConfig {
            worker_id: "native-e2e-outbound".to_owned(),
            lease_duration_ms: 10_000,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 1_000,
        },
    )
    .unwrap();
    let delivery = outbound.process_next().await.unwrap();
    let command_message: WireMessage<ExecutionCommand> = read_message(&mut stream).await;
    assert_eq!(
        command_message.message_type,
        ExecutionClientMessageType::ExecutionCommand
    );
    assert_eq!(command_message.payload, command);
    assert!(matches!(
        delivery,
        DurableOutboundProcessOutcome::ExecutionCommandDelivered {
            ref command_id,
            ref message_id,
            disposition: DurableDeliveryDisposition::Sent,
        } if command_id == &command.command_id && message_id == &command_message.message_id
    ));
    let dispatched = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dispatched.status, ExecutionCommandStatus::Dispatched);

    clock.set(NOW + 10);
    send_transport_ack(&mut stream, &session_id, &command_message, NOW + 10).await;
    clock.set(NOW + 20);
    let received_message_id =
        send_command_received(&mut stream, &session_id, &command_message, NOW + 20).await;
    clock.set(NOW + 30);
    let recovery = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(CoreInboundProcessor),
        Arc::new(CoreSessionResumeProcessor),
        Arc::new(clock),
        DurableRecoveryConfig {
            worker_id: "native-e2e-inbound".to_owned(),
            max_items_per_batch: 8,
            lease_duration: Duration::from_secs(1),
            handler_timeout: Duration::from_millis(100),
            finalization_budget: Duration::from_millis(100),
        },
    )
    .unwrap();
    let report = recovery.dispatch_batch().await.unwrap();
    assert_eq!(report.handled, 2, "recovery report: {report:?}");
    assert_eq!(report.failed, 0, "recovery report: {report:?}");

    let received = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.status, ExecutionCommandStatus::CommandReceived);
    assert_eq!(received.delivery_attempts, 1);
    assert_eq!(received.command_received_at, Some(NOW + 20));
    let stored_plan = store
        .get_execution_plan(&plan.definition.plan_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_plan.plan.state.status, ExecutionPlanStatus::Pending);
    assert_eq!(
        stored_plan.plan.legs[0].state.status,
        ExecutionLegStatus::CommandReceived
    );
    let durable_delivery = store
        .get_outbound_delivery(&command_message.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable_delivery.outbox.status, WireOutboxStatus::Acked);
    assert_eq!(
        durable_delivery.attempt.status,
        CommandDeliveryAttemptStatus::Acked
    );
    assert_eq!(durable_delivery.attempt.acked_at, Some(NOW + 20));
    let receipt_admission = store
        .get_inbound_admission(&received_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt_admission.status, DurableWorkStatus::Handled);

    drop(stream);
    server.shutdown().await;
}

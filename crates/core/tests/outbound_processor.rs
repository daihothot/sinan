use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sinan_core::{
    DurableDeliveryDisposition, DurableOutboundConfig, DurableOutboundProcessOutcome,
    DurableOutboundProcessor,
};
use sinan_execution::{
    initial_command_state, transition_command, CommandEvidence, DeliveryFailure, DeliveryFuture,
    DeliveryInfrastructureError, DeliveryOutcome, DeliveryReceipt, DeliveryRejection,
    DeliveryRejectionReason, DeliveryRequest, DeliveryUncertainty, OutboundDeliveryPort,
    ServerClock,
};
use sinan_protocol::{CommandInboxStatus, CommandReceived, ProtocolReason};
use sinan_store::{
    ClaimOutboundDeliveryWork, CommandStateUpdate, CoreEventMetadata, NewExecutionCommand,
    NewReconciliationRun, NewRiskResult, NewTradeIntent, OutboundDeliveryWorkOutcome,
    OutboundDeliveryWorkStatus, SqliteStateStore, StoreOptions,
};
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLeg, AdjustedRiskLegAction, ClientId, CommandId,
    CorrelationId, DecisionId, ErrorCodeOrString, ExecutionAction, ExecutionCommand,
    ExecutionCommandStatus, IdempotencyKey, IntentId, MessageId, RequestId, RiskId, RiskResult,
    SessionId, SizingCandidateProvenance, StrategyId, SymbolCode, TerminalId, TimeframeCode,
    TradeIntent, TradeIntentAction, TradeIntentStatus,
};
use tokio::sync::Barrier;

const CREATED_AT: i64 = 1_000;
const NOW: i64 = 1_200;

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

async fn test_store() -> (TestDatabase, SqliteStateStore) {
    let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database = TestDatabase(std::env::temp_dir().join(format!(
        "sinan-core-outbound-{}-{nanos}-{sequence}.sqlite",
        std::process::id()
    )));
    let mut options = StoreOptions::new(format!("sqlite://{}", database.0.display()));
    options.max_connections = 8;
    options.busy_timeout = Duration::from_secs(5);
    let store = SqliteStateStore::connect(options).await.unwrap();
    (database, store)
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

#[derive(Clone, Debug)]
enum Script {
    Sent,
    Unconfirmed(&'static str),
    Rejected(DeliveryRejectionReason),
    DefinitelyNotWritten(&'static str),
    Infrastructure(&'static str),
}

#[derive(Clone)]
struct ScriptedPort {
    clock: ManualClock,
    command_scripts: Arc<Mutex<VecDeque<Script>>>,
    reconciliation_scripts: Arc<Mutex<VecDeque<Script>>>,
    command_message_ids: Arc<Mutex<Vec<MessageId>>>,
    reconciliation_message_ids: Arc<Mutex<Vec<MessageId>>>,
}

impl ScriptedPort {
    fn new(clock: ManualClock, command_scripts: Vec<Script>) -> Self {
        Self {
            clock,
            command_scripts: Arc::new(Mutex::new(command_scripts.into())),
            reconciliation_scripts: Arc::new(Mutex::new(VecDeque::new())),
            command_message_ids: Arc::new(Mutex::new(Vec::new())),
            reconciliation_message_ids: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn command_ids(&self) -> Vec<MessageId> {
        self.command_message_ids.lock().unwrap().clone()
    }

    fn outcome(
        &self,
        script: Script,
        message_id: MessageId,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let now = self.clock.now_ms();
        match script {
            Script::Sent => Ok(DeliveryOutcome::Sent(DeliveryReceipt {
                attempt_id: format!("attempt:{message_id}"),
                message_id,
                session_id: SessionId::from("session-1"),
                sequence: 2,
                sent_at: now,
                confirmation_deadline_at: now + 5_000,
            })),
            Script::Unconfirmed(error) => Ok(DeliveryOutcome::Unconfirmed(DeliveryUncertainty {
                attempt_id: format!("attempt:{message_id}"),
                message_id,
                session_id: Some(SessionId::from("session-1")),
                sequence: Some(2),
                write_started_at: now,
                observed_at: now,
                error: error.to_owned(),
            })),
            Script::Rejected(reason) => Ok(DeliveryOutcome::Rejected(DeliveryRejection {
                attempt_id: format!("attempt:{message_id}"),
                message_id,
                session_id: None,
                rejected_at: now,
                reason,
            })),
            Script::DefinitelyNotWritten(error) => {
                Ok(DeliveryOutcome::DefinitelyNotWritten(DeliveryFailure {
                    attempt_id: format!("attempt:{message_id}"),
                    message_id,
                    session_id: Some(SessionId::from("session-1")),
                    failed_at: now,
                    error: error.to_owned(),
                }))
            }
            Script::Infrastructure(error) => Err(DeliveryInfrastructureError::new(error)),
        }
    }
}

impl OutboundDeliveryPort for ScriptedPort {
    fn deliver_execution_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> DeliveryFuture<'_> {
        let message_id = request.message.message_id;
        self.command_message_ids
            .lock()
            .unwrap()
            .push(message_id.clone());
        let script = self
            .command_scripts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected execution delivery");
        let outcome = self.outcome(script, message_id);
        Box::pin(async move { outcome })
    }

    fn deliver_reconciliation_request(
        &self,
        request: DeliveryRequest<sinan_protocol::ReconciliationRequest>,
    ) -> DeliveryFuture<'_> {
        let message_id = request.message.message_id;
        self.reconciliation_message_ids
            .lock()
            .unwrap()
            .push(message_id.clone());
        let script = self
            .reconciliation_scripts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected reconciliation delivery");
        let outcome = self.outcome(script, message_id);
        Box::pin(async move { outcome })
    }
}

#[derive(Clone, Copy)]
enum BlockingDelivery {
    Sent,
    Unconfirmed,
}

#[derive(Clone)]
struct BlockingPort {
    clock: ManualClock,
    delivery: BlockingDelivery,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl BlockingPort {
    fn new(clock: ManualClock, delivery: BlockingDelivery) -> Self {
        Self {
            clock,
            delivery,
            entered: Arc::new(Barrier::new(2)),
            release: Arc::new(Barrier::new(2)),
        }
    }
}

impl OutboundDeliveryPort for BlockingPort {
    fn deliver_execution_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> DeliveryFuture<'_> {
        let message_id = request.message.message_id;
        let delivery = self.delivery;
        let evidence_at = self.clock.now_ms();
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            entered.wait().await;
            release.wait().await;
            Ok(match delivery {
                BlockingDelivery::Sent => DeliveryOutcome::Sent(DeliveryReceipt {
                    attempt_id: format!("attempt:{message_id}"),
                    message_id,
                    session_id: SessionId::from("session-1"),
                    sequence: 2,
                    sent_at: evidence_at,
                    confirmation_deadline_at: evidence_at + 5_000,
                }),
                BlockingDelivery::Unconfirmed => {
                    DeliveryOutcome::Unconfirmed(DeliveryUncertainty {
                        attempt_id: format!("attempt:{message_id}"),
                        message_id,
                        session_id: Some(SessionId::from("session-1")),
                        sequence: Some(2),
                        write_started_at: evidence_at,
                        observed_at: evidence_at,
                        error: "connection closed after write started".to_owned(),
                    })
                }
            })
        })
    }

    fn deliver_reconciliation_request(
        &self,
        _request: DeliveryRequest<sinan_protocol::ReconciliationRequest>,
    ) -> DeliveryFuture<'_> {
        panic!("unexpected reconciliation delivery")
    }
}

fn processor(
    store: SqliteStateStore,
    port: ScriptedPort,
    clock: ManualClock,
) -> DurableOutboundProcessor {
    processor_with_port(store, Arc::new(port), clock)
}

fn processor_with_port(
    store: SqliteStateStore,
    port: Arc<dyn OutboundDeliveryPort>,
    clock: ManualClock,
) -> DurableOutboundProcessor {
    DurableOutboundProcessor::new(
        store,
        port,
        Arc::new(clock),
        DurableOutboundConfig {
            worker_id: "outbound-worker".to_owned(),
            lease_duration_ms: 1_000,
            retry_base_delay_ms: 100,
            retry_max_delay_ms: 1_000,
        },
    )
    .unwrap()
}

async fn seed_command(
    store: &SqliteStateStore,
    command_id: &str,
    expires_at: i64,
) -> ExecutionCommand {
    let intent_id = IntentId::from(format!("intent-{command_id}"));
    let risk_id = RiskId::from(format!("risk-{command_id}"));
    let decision_id = DecisionId::from(format!("decision-{command_id}"));
    let strategy_id = StrategyId::from("strategy-1");
    store
        .insert_trade_intent(NewTradeIntent {
            intent: TradeIntent {
                intent_id: intent_id.clone(),
                decision_id: decision_id.clone(),
                strategy_id: strategy_id.clone(),
                correlation_id: CorrelationId::from(format!("correlation-{command_id}")),
                idempotency_key: IdempotencyKey::from(format!("intent-key-{command_id}")),
                account_id: AccountId::from("account-1"),
                symbol: SymbolCode::from("XAUUSD"),
                timeframe: TimeframeCode::from("H4"),
                action: TradeIntentAction::Buy,
                confidence: 0.8,
                reason: "outbound processor test".to_owned(),
                proposed_risk_pct: 1.0,
                proposed_sl: Some(2_320.5),
                proposed_tp: Some(2_365.5),
                proposed_legs: None,
                decision_timestamp: 800,
                signal_expires_at: 20_000,
                requested_at: 900,
            },
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: 901,
        })
        .await
        .unwrap();
    let leg_id = single_leg_id(&intent_id);
    store
        .insert_risk_result(NewRiskResult {
            result: RiskResult {
                risk_id: risk_id.clone(),
                request_id: RequestId::from(format!("risk-request-{command_id}")),
                intent_id: intent_id.clone(),
                account_id: AccountId::from("account-1"),
                risk_request_hash: "a".repeat(64),
                approved: true,
                reason: ErrorCodeOrString::from("OK"),
                message: None,
                sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
                risk_base_amount: Some(10_000.0),
                risk_budget_amount: Some(100.0),
                adjusted_risk_pct: Some(0.98),
                sizing_candidates: Some(vec![SizingCandidateProvenance {
                    leg_id: leg_id.clone(),
                    symbol: SymbolCode::from("XAUUSD"),
                    action: AdjustedRiskLegAction::Buy,
                    ratio: 1.0,
                    worst_entry_price: 2_350.0,
                    stop_loss_price: 2_320.5,
                    estimated_cost_per_lot: 0.0,
                }]),
                adjusted_legs: Some(vec![AdjustedRiskLeg {
                    leg_id,
                    symbol: SymbolCode::from("XAUUSD"),
                    action: AdjustedRiskLegAction::Buy,
                    lots: 0.07,
                    risk_amount: 98.0,
                    risk_pct: 0.98,
                    sizing_entry_price: 2_350.0,
                    approved_sl: 2_320.5,
                    loss_per_lot: 1_400.0,
                    reason: Some(ErrorCodeOrString::from("OK")),
                }]),
                decision_id,
                snapshot_age_ms: 125,
                market_snapshot_age_ms: 75,
                symbol_metadata_age_ms: 250,
                capacity_age_ms: 100,
                evaluated_at: 950,
                valid_until: 20_000,
            },
        })
        .await
        .unwrap();
    let command = ExecutionCommand {
        command_id: CommandId::from(command_id),
        plan_id: None,
        leg_id: None,
        strategy_id,
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: Some("XAUUSD".to_owned()),
        action: ExecutionAction::Cancel,
        order_type: None,
        lots: None,
        price: None,
        sl: None,
        tp: None,
        deviation_points: None,
        magic: 1,
        comment: None,
        position_ticket: None,
        broker_order_id: Some("broker-order-1".into()),
        filling_policy: None,
        time_policy: None,
        expiration_time: None,
        expires_at,
        idempotency_key: IdempotencyKey::from(format!("command-key-{command_id}")),
        hmac: "a".repeat(64),
    };
    store
        .insert_execution_command(NewExecutionCommand {
            command: command.clone(),
            risk_id,
            created_at: CREATED_AT,
        })
        .await
        .unwrap();
    store
        .insert_execution_command_state(initial_command_state(&command, CREATED_AT).unwrap())
        .await
        .unwrap();
    command
}

async fn advance_to_command_received(store: &SqliteStateStore, command: &ExecutionCommand) {
    let current = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    let dispatched = transition_command(command, &current, CommandEvidence::Dispatched { at: NOW })
        .unwrap()
        .into_state();
    store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: current.status,
            expected_updated_at: current.updated_at,
            state: dispatched.clone(),
        })
        .await
        .unwrap();
    let receipt = CommandReceived {
        command_id: command.command_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        received_at: NOW + 1,
        inbox_status: CommandInboxStatus::Recorded,
        reason: Some(ProtocolReason::Ok),
    };
    let received = transition_command(
        command,
        &dispatched,
        CommandEvidence::ReceivedRecorded(&receipt),
    )
    .unwrap()
    .into_state();
    store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: dispatched.status,
            expected_updated_at: dispatched.updated_at,
            state: received,
        })
        .await
        .unwrap();
}

async fn assert_inflight_receipt_supersedes(delivery: BlockingDelivery, command_id: &str) {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, command_id, 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = BlockingPort::new(clock.clone(), delivery);
    let worker = processor_with_port(store.clone(), Arc::new(port.clone()), clock.clone());
    let task = tokio::spawn(async move { worker.process_next().await });

    port.entered.wait().await;
    advance_to_command_received(&store, &command).await;
    clock.set(NOW + 2);
    port.release.wait().await;

    let outcome = task.await.unwrap().unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::DeliveryStopped {
            outcome: OutboundDeliveryWorkOutcome::Superseded,
            ..
        }
    ));
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::CommandReceived);
    assert_eq!(state.delivery_attempts, 1);
    assert_eq!(state.command_received_at, Some(NOW + 1));
    assert_eq!(state.last_delivery_error, None);
    let work = store
        .get_outbound_delivery_work(&format!("execution.command:{}", command.command_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work.status, OutboundDeliveryWorkStatus::Delivered);
    assert_eq!(
        work.last_outcome,
        Some(OutboundDeliveryWorkOutcome::Superseded)
    );
    assert!(store
        .get_reconciliation_run(&RequestId::from(format!(
            "reconciliation:delivery-unconfirmed:{}",
            command.command_id
        )))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sent_delivery_dispatches_once_and_completes_work() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-sent", 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(clock.clone(), vec![Script::Sent]);
    let worker = processor(store.clone(), port.clone(), clock);

    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::ExecutionCommandDelivered {
            disposition: DurableDeliveryDisposition::Sent,
            ..
        }
    ));
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::Dispatched);
    assert_eq!(state.delivery_attempts, 1);
    assert_eq!(port.command_ids().len(), 1);
}

#[tokio::test]
async fn sent_completion_is_superseded_when_receipt_wins_in_flight() {
    assert_inflight_receipt_supersedes(BlockingDelivery::Sent, "command-sent-race").await;
}

#[tokio::test]
async fn unconfirmed_completion_is_superseded_when_receipt_wins_in_flight() {
    assert_inflight_receipt_supersedes(BlockingDelivery::Unconfirmed, "command-unconfirmed-race")
        .await;
}

#[tokio::test]
async fn infrastructure_error_replays_same_generation_after_backoff() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-infrastructure", 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(
        clock.clone(),
        vec![Script::Infrastructure("database unavailable"), Script::Sent],
    );
    let worker = processor(store.clone(), port.clone(), clock.clone());

    let first = worker.process_next().await.unwrap();
    let retry_at = match first {
        DurableOutboundProcessOutcome::RetryScheduled {
            failed_message_id,
            next_message_id,
            retry_at,
            ..
        } => {
            assert_eq!(failed_message_id, next_message_id);
            retry_at
        }
        other => panic!("unexpected first outcome: {other:?}"),
    };
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    clock.set(retry_at);
    worker.process_next().await.unwrap();

    let ids = port.command_ids();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    assert_eq!(
        store
            .get_execution_command_state(&command.command_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionCommandStatus::Dispatched
    );
}

#[tokio::test]
async fn definitely_not_written_advances_to_a_new_deterministic_generation() {
    let (_database, store) = test_store().await;
    seed_command(&store, "command-not-written", 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(
        clock.clone(),
        vec![Script::DefinitelyNotWritten("write rejected before bytes")],
    );
    let worker = processor(store, port, clock);

    let outcome = worker.process_next().await.unwrap();
    match outcome {
        DurableOutboundProcessOutcome::RetryScheduled {
            failed_message_id,
            next_message_id,
            ..
        } => assert_ne!(failed_message_id, next_message_id),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[tokio::test]
async fn expired_rejection_terminates_work_and_expires_the_command() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-expired", 1_100).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(
        clock.clone(),
        vec![Script::Rejected(DeliveryRejectionReason::Expired)],
    );
    let worker = processor(store.clone(), port.clone(), clock);

    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::DeliveryStopped {
            outcome: OutboundDeliveryWorkOutcome::Expired,
            ..
        }
    ));
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::Expired);
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    assert_eq!(port.command_ids().len(), 1);
}

#[tokio::test]
async fn identity_mismatch_fails_closed_without_retrying_or_advancing_the_command() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-identity", 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(
        clock.clone(),
        vec![Script::Rejected(
            DeliveryRejectionReason::IdentityMismatch {
                field: "account_id",
            },
        )],
    );
    let worker = processor(store.clone(), port.clone(), clock);

    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::DeliveryStopped {
            outcome: OutboundDeliveryWorkOutcome::PermanentRejection,
            ..
        }
    ));
    assert_eq!(
        store
            .get_execution_command_state(&command.command_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionCommandStatus::Created
    );
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    assert_eq!(port.command_ids().len(), 1);
}

#[tokio::test]
async fn unconfirmed_delivery_records_lifecycle_and_requests_targeted_reconciliation() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-uncertain", 10_000).await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(
        clock.clone(),
        vec![Script::Unconfirmed("connection closed after write started")],
    );
    let worker = processor(store.clone(), port, clock);

    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::ExecutionCommandDelivered {
            disposition: DurableDeliveryDisposition::Unconfirmed,
            ..
        }
    ));
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::DeliveryUnconfirmed);
    assert_eq!(state.delivery_attempts, 1);
    assert_eq!(state.dispatched_at, Some(NOW));
    assert_eq!(
        state.last_delivery_error.as_deref(),
        Some("connection closed after write started")
    );
    let request_id = RequestId::from(format!(
        "reconciliation:delivery-unconfirmed:{}",
        command.command_id
    ));
    let run = store
        .get_reconciliation_run(&request_id)
        .await
        .unwrap()
        .expect("uncertainty must create a durable reconciliation run");
    assert_eq!(run.request.command_ids, Some(vec![command.command_id]));
    assert_eq!(
        run.request.reason,
        sinan_protocol::ReconciliationReason::DeliveryUnconfirmed
    );
}

#[tokio::test]
async fn expired_claim_is_completed_as_superseded_after_receipt_advances_lifecycle() {
    let (_database, store) = test_store().await;
    let command = seed_command(&store, "command-raced", 10_000).await;
    let claimed = store
        .claim_next_outbound_delivery(ClaimOutboundDeliveryWork {
            worker_id: "crashed-worker".to_owned(),
            claimed_at: NOW,
            lease_expires_at: NOW + 100,
        })
        .await
        .unwrap()
        .unwrap();
    let work_id = claimed.work().work_id.clone();
    let clock = ManualClock::new(NOW + 50);
    let port = ScriptedPort::new(clock.clone(), vec![]);
    let worker = processor(store.clone(), port.clone(), clock.clone());
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    assert!(port.command_ids().is_empty());

    let current = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    let dispatched = transition_command(
        &command,
        &current,
        CommandEvidence::Dispatched { at: NOW + 50 },
    )
    .unwrap()
    .into_state();
    store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: current.status,
            expected_updated_at: current.updated_at,
            state: dispatched,
        })
        .await
        .unwrap();

    clock.set(NOW + 100);
    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::DeliveryStopped {
            outcome: OutboundDeliveryWorkOutcome::Superseded,
            ..
        }
    ));
    assert!(port.command_ids().is_empty());
    let work = store
        .get_outbound_delivery_work(&work_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work.status, OutboundDeliveryWorkStatus::Delivered);
    assert_eq!(
        work.last_outcome,
        Some(OutboundDeliveryWorkOutcome::Superseded)
    );
}

#[tokio::test]
async fn requested_reconciliation_is_delivered_once() {
    let (_database, store) = test_store().await;
    seed_reconciliation(&store, "reconciliation-manual").await;
    let clock = ManualClock::new(NOW);
    let port = ScriptedPort::new(clock.clone(), vec![]);
    port.reconciliation_scripts
        .lock()
        .unwrap()
        .push_back(Script::Sent);
    let worker = processor(store, port.clone(), clock);

    let outcome = worker.process_next().await.unwrap();
    assert!(matches!(
        outcome,
        DurableOutboundProcessOutcome::ReconciliationRequestDelivered {
            disposition: DurableDeliveryDisposition::Sent,
            ..
        }
    ));
    assert_eq!(
        worker.process_next().await.unwrap(),
        DurableOutboundProcessOutcome::NoWork
    );
    assert_eq!(port.reconciliation_message_ids.lock().unwrap().len(), 1);
}

async fn seed_reconciliation(store: &SqliteStateStore, request_id: &str) {
    let request = sinan_protocol::ReconciliationRequest {
        request_id: RequestId::from(request_id),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        reason: sinan_protocol::ReconciliationReason::ManualRequest,
        command_ids: None,
        since_server_time: None,
    };
    store
        .create_reconciliation_run(NewReconciliationRun {
            request,
            requested_at: NOW,
            event_metadata: CoreEventMetadata {
                event_id: format!("event-{request_id}"),
                event_type: "reconciliation.request".to_owned(),
                aggregate_type: "reconciliation".to_owned(),
                aggregate_id: request_id.to_owned(),
                message_id: None,
                schema_version: "ecp.v1.0".to_owned(),
                correlation_id: None,
                causation_id: None,
                account_id: Some(AccountId::from("account-1")),
                client_id: Some(ClientId::from("client-1")),
                terminal_id: Some(TerminalId::from("terminal-1")),
                strategy_id: None,
                intent_id: None,
                plan_id: None,
                leg_id: None,
                command_id: None,
                idempotency_key: None,
                event_at: NOW,
                received_at: NOW,
                created_at: NOW,
                source: "outbound-test".to_owned(),
            },
        })
        .await
        .unwrap();
}

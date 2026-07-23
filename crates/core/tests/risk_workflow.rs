use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sinan_core::{
    RiskWorkflowError, RiskWorkflowLeg, RiskWorkflowOutcome, RiskWorkflowProcessor,
    TrustedExecutionResolver, TrustedLegExecutionParameters, TrustedRiskWorkflowContext,
};
use sinan_protocol::{ReconciliationReason, ReconciliationRequest, ReconciliationResult};
use sinan_risk::{
    restore_circuit_breaker_snapshot, CircuitBreakerState, RiskPolicy, StrategyRiskPolicy,
    POSITION_SIZING_VERSION_V1,
};
use sinan_store::{
    CanonicalJson, CoreEventMetadata, NewCircuitBreakerSnapshot, NewReconciliationResult,
    NewReconciliationRun, NewRiskCapacitySnapshot, NewTradeIntent, ReconciliationCompleteness,
    ReconciliationDisposition, ReconciliationEvaluation, SqliteStateStore, StoreError,
    StoreOptions,
};
use sinan_types::{
    AccountId, AccountSnapshot, ClientId, CorrelationId, DecisionId, ExecutionFailurePolicy,
    ExecutionPlanMode, ExecutionPolicy, FillingPolicy, IdempotencyKey, IntentId, MarketSnapshot,
    OrderType, PositionId, PositionSide, PositionSnapshot, RequestId, RiskCapacity, RiskId,
    StrategyId, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TerminalId, TimePolicy,
    TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus,
};

const NOW: i64 = 10_000;
const RECON_REQUESTED_AT: i64 = 9_800;
const OBSERVED_AT: i64 = 9_900;
const ACCOUNT: &str = "account-1";
const STRATEGY: &str = "strategy-1";
const SYMBOL: &str = "SYNTH-A";

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
        "sinan-core-risk-workflow-{}-{nanos}-{sequence}.sqlite",
        std::process::id()
    )));
    let mut options = StoreOptions::new(format!("sqlite://{}", database.0.display()));
    options.max_connections = 4;
    options.busy_timeout = Duration::from_secs(5);
    let store = SqliteStateStore::connect(options).await.unwrap();
    (database, store)
}

fn intent(intent_id: &str, action: TradeIntentAction) -> TradeIntent {
    let actionable = matches!(action, TradeIntentAction::Buy | TradeIntentAction::Sell);
    TradeIntent {
        intent_id: IntentId::new(intent_id),
        decision_id: DecisionId::new(format!("decision-{intent_id}")),
        strategy_id: StrategyId::new(STRATEGY),
        correlation_id: CorrelationId::new(format!("correlation-{intent_id}")),
        idempotency_key: IdempotencyKey::new(format!("intent-{intent_id}")),
        account_id: AccountId::new(ACCOUNT),
        symbol: SymbolCode::new(SYMBOL),
        timeframe: TimeframeCode::new("M5"),
        action,
        confidence: 0.8,
        reason: "workflow test".to_owned(),
        proposed_risk_pct: if actionable { 1.0 } else { 0.0 },
        proposed_sl: actionable.then_some(90.0),
        proposed_tp: actionable.then_some(120.0),
        proposed_legs: None,
        decision_timestamp: NOW - 30,
        signal_expires_at: NOW + 5_000,
        requested_at: NOW - 20,
    }
}

fn account() -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::new(ACCOUNT),
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
        account_id: AccountId::new(ACCOUNT),
        symbol: SymbolCode::new(SYMBOL),
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

fn market() -> MarketSnapshot {
    MarketSnapshot {
        symbol: SymbolCode::new(SYMBOL),
        broker_symbol: Some(SYMBOL.to_owned()),
        bid: 99.0,
        ask: 100.0,
        spread: 1.0,
        observed_at: NOW - 50,
    }
}

fn capacity(daily_loss: f64) -> RiskCapacity {
    RiskCapacity {
        account_id: AccountId::new(ACCOUNT),
        strategy_id: StrategyId::new(STRATEGY),
        observed_at: OBSERVED_AT,
        daily_realized_loss_pct: daily_loss,
        equity_drawdown_pct: 0.0,
        remaining_account_risk_pct: 5.0,
        remaining_portfolio_risk_pct: 5.0,
        remaining_strategy_legs: 4,
    }
}

fn event_metadata(event_id: &str, event_type: &str, event_at: i64) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: event_id.to_owned(),
        event_type: event_type.to_owned(),
        aggregate_type: "reconciliation".to_owned(),
        aggregate_id: "reconciliation-risk-workflow".to_owned(),
        message_id: None,
        schema_version: "ecp.v1.0".to_owned(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::new(ACCOUNT)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at,
        received_at: event_at + 1,
        created_at: event_at + 2,
        source: "risk-workflow-test".to_owned(),
    }
}

async fn seed_intent(store: &SqliteStateStore, value: TradeIntent) {
    store
        .insert_trade_intent(NewTradeIntent {
            intent: value,
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: NOW - 10,
        })
        .await
        .unwrap();
}

async fn seed_checkpoint(store: &SqliteStateStore) {
    let request_id = RequestId::new("reconciliation-risk-workflow");
    store
        .create_reconciliation_run(NewReconciliationRun {
            request: ReconciliationRequest {
                request_id: request_id.clone(),
                account_id: AccountId::new(ACCOUNT),
                terminal_id: None,
                client_id: None,
                reason: ReconciliationReason::ManualRequest,
                command_ids: None,
                since_server_time: Some(RECON_REQUESTED_AT - 10),
            },
            requested_at: RECON_REQUESTED_AT,
            event_metadata: event_metadata(
                "reconciliation-request-risk-workflow",
                "reconciliation.request",
                RECON_REQUESTED_AT,
            ),
        })
        .await
        .unwrap();
    store
        .commit_reconciliation_result(NewReconciliationResult {
            result: ReconciliationResult {
                request_id: request_id.clone(),
                account_id: AccountId::new(ACCOUNT),
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
                account_id: AccountId::new(ACCOUNT),
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
                "reconciliation-result-risk-workflow",
                "reconciliation.result",
                OBSERVED_AT,
            ),
        })
        .await
        .unwrap();
}

async fn seed_account_without_checkpoint(store: &SqliteStateStore) {
    let mut account_event = event_metadata(
        "account-without-checkpoint",
        "account.snapshot",
        OBSERVED_AT,
    );
    account_event.aggregate_type = "account.snapshot".to_owned();
    account_event.aggregate_id = ACCOUNT.to_owned();
    store
        .ingest_account_snapshot(account_event, &account())
        .await
        .unwrap();
}

async fn seed_market_capacity_breaker(
    store: &SqliteStateStore,
    include_capacity: bool,
    daily_loss: f64,
) {
    store
        .update_market_snapshot(&AccountId::new(ACCOUNT), &market(), NOW - 40)
        .await
        .unwrap();
    if include_capacity {
        store
            .record_risk_capacity_snapshot(NewRiskCapacitySnapshot {
                capacity: capacity(daily_loss),
                recorded_at: OBSERVED_AT + 3,
            })
            .await
            .unwrap();
    }
    let snapshot = CircuitBreakerState::new().durable_snapshot_v1();
    let payload = CanonicalJson::parse(&snapshot.to_json().unwrap()).unwrap();
    store
        .write_circuit_breaker_snapshot(NewCircuitBreakerSnapshot {
            expected_head_revision: None,
            schema_version: snapshot.schema_version().to_owned(),
            status: "CLOSED".to_owned(),
            recovery_epoch: snapshot.recovery_epoch(),
            updated_at: OBSERVED_AT,
            payload,
        })
        .await
        .unwrap();
}

async fn seed_ready(
    store: &SqliteStateStore,
    value: TradeIntent,
    include_capacity: bool,
    daily_loss: f64,
) {
    seed_intent(store, value).await;
    seed_checkpoint(store).await;
    seed_market_capacity_breaker(store, include_capacity, daily_loss).await;
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

struct FixedResolver {
    market_command_price: Option<f64>,
}

impl TrustedExecutionResolver for FixedResolver {
    fn resolve(
        &self,
        _intent: &TradeIntent,
        _leg: &RiskWorkflowLeg,
    ) -> Result<TrustedLegExecutionParameters, String> {
        Ok(TrustedLegExecutionParameters {
            dependency: Vec::new(),
            terminal_id: Some(TerminalId::new("terminal-1")),
            client_id: Some(ClientId::new("client-1")),
            order_type: OrderType::Market,
            price: self.market_command_price,
            deviation_points: Some(20),
            magic: 42,
            comment: Some("risk workflow test".to_owned()),
            filling_policy: Some(FillingPolicy::Ioc),
            time_policy: Some(TimePolicy::Gtc),
            expiration_time: None,
            estimated_cost_per_lot: 0.0,
        })
    }
}

fn context<'a>(
    risk: &'a RiskPolicy,
    strategy: &'a StrategyRiskPolicy,
    execution: &'a ExecutionPolicy,
    resolver: &'a FixedResolver,
) -> TrustedRiskWorkflowContext<'a> {
    TrustedRiskWorkflowContext::new(
        risk,
        strategy,
        execution,
        resolver,
        b"workflow-signing-secret",
    )
    .unwrap()
}

#[tokio::test]
async fn approval_commits_exact_lots_and_is_idempotent() {
    let (_database, store) = test_store().await;
    let intent_id = IntentId::new("approved");
    seed_ready(
        &store,
        intent(intent_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let processor = RiskWorkflowProcessor::new(store.clone());

    let outcome = processor
        .process_intent(
            &intent_id,
            NOW,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap();
    let RiskWorkflowOutcome::ExecutionReady {
        result,
        plan,
        commands,
    } = outcome
    else {
        panic!("BUY should produce an execution bundle")
    };
    let approved_lots = result.adjusted_legs.as_ref().unwrap()[0].lots;
    assert_eq!(commands[0].lots, Some(approved_lots));
    assert_eq!(plan.legs[0].definition.lots, Some(approved_lots));

    let stored = store
        .get_execution_workflow(&plan.definition.plan_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.commands[0].command.lots, Some(approved_lots));
    assert_eq!(
        stored.plan.plan.legs[0].definition.lots,
        Some(approved_lots)
    );
    assert!(matches!(
        processor
            .process_intent(
                &intent_id,
                NOW,
                &context(&risk, &strategy, &execution, &resolver),
            )
            .await
            .unwrap(),
        RiskWorkflowOutcome::AlreadyProcessed { .. }
    ));
}

#[tokio::test]
async fn rejection_and_hold_persist_only_risk_results() {
    for (intent_id, action, daily_loss, approved) in [
        ("rejected", TradeIntentAction::Buy, 4.0, false),
        ("hold", TradeIntentAction::Hold, 0.0, true),
    ] {
        let (_database, store) = test_store().await;
        seed_ready(&store, intent(intent_id, action), true, daily_loss).await;
        let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
        let resolver = FixedResolver {
            market_command_price: None,
        };
        let outcome = RiskWorkflowProcessor::new(store.clone())
            .process_next(NOW, &context(&risk, &strategy, &execution, &resolver))
            .await
            .unwrap();
        let RiskWorkflowOutcome::RiskOnly { result } = outcome else {
            panic!("rejected and HOLD intents must not build execution")
        };
        assert_eq!(result.approved, approved);
        assert!(store
            .get_execution_workflow(&sinan_types::PlanId::new(format!(
                "plan:{intent_id}:initial"
            )))
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn missing_capacity_or_checkpoint_leaves_intent_pending_without_a_result() {
    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    for missing_capacity in [true, false] {
        let (_database, store) = test_store().await;
        let intent_id = IntentId::new(if missing_capacity {
            "missing-capacity"
        } else {
            "missing-checkpoint"
        });
        seed_intent(&store, intent(intent_id.as_str(), TradeIntentAction::Buy)).await;
        if missing_capacity {
            seed_checkpoint(&store).await;
            seed_market_capacity_breaker(&store, false, 0.0).await;
        } else {
            seed_account_without_checkpoint(&store).await;
            seed_market_capacity_breaker(&store, true, 0.0).await;
        }
        let error = RiskWorkflowProcessor::new(store.clone())
            .process_intent(
                &intent_id,
                NOW,
                &context(&risk, &strategy, &execution, &resolver),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RiskWorkflowError::Store(StoreError::SnapshotUnavailable { .. })
        ));
        assert!(store
            .get_risk_result(&RiskId::new(format!("risk:{intent_id}:initial")))
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn process_next_skips_an_older_intent_without_risk_capacity() {
    let (_database, store) = test_store().await;
    let blocked_id = IntentId::new("blocked-older");
    let mut blocked = intent(blocked_id.as_str(), TradeIntentAction::Buy);
    blocked.strategy_id = StrategyId::new("strategy-without-capacity");
    blocked.decision_timestamp = NOW - 110;
    blocked.requested_at = NOW - 100;
    seed_intent(&store, blocked).await;

    let ready_id = IntentId::new("ready-newer");
    seed_ready(
        &store,
        intent(ready_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;

    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let outcome = RiskWorkflowProcessor::new(store.clone())
        .process_next(NOW, &context(&risk, &strategy, &execution, &resolver))
        .await
        .unwrap();
    let RiskWorkflowOutcome::ExecutionReady { result, .. } = outcome else {
        panic!("the newer ready intent should not be starved")
    };
    assert_eq!(result.intent_id, ready_id);
    assert!(store
        .get_risk_result(&RiskId::new(format!("risk:{blocked_id}:initial")))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn incomplete_symbol_context_stays_pending_and_does_not_starve_ready_work() {
    let (_database, store) = test_store().await;
    let blocked_id = IntentId::new("blocked-missing-symbol");
    let mut blocked = intent(blocked_id.as_str(), TradeIntentAction::Buy);
    blocked.symbol = SymbolCode::new("SYNTH-MISSING");
    blocked.decision_timestamp = NOW - 110;
    blocked.requested_at = NOW - 100;
    seed_intent(&store, blocked).await;

    let ready_id = IntentId::new("ready-complete-symbol");
    seed_ready(
        &store,
        intent(ready_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;

    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let processor = RiskWorkflowProcessor::new(store.clone());
    let error = processor
        .process_intent(
            &blocked_id,
            NOW,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RiskWorkflowError::Store(StoreError::SnapshotUnavailable { .. })
    ));
    assert!(store
        .get_risk_result(&RiskId::new(format!("risk:{blocked_id}:initial")))
        .await
        .unwrap()
        .is_none());

    let outcome = processor
        .process_next(NOW, &context(&risk, &strategy, &execution, &resolver))
        .await
        .unwrap();
    let RiskWorkflowOutcome::ExecutionReady { result, .. } = outcome else {
        panic!("the newer intent with complete symbol context should be selected")
    };
    assert_eq!(result.intent_id, ready_id);
}

#[tokio::test]
async fn post_checkpoint_partial_state_fails_closed_without_a_result() {
    let (_database, store) = test_store().await;
    let intent_id = IntentId::new("partial-state");
    seed_ready(
        &store,
        intent(intent_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let position = PositionSnapshot {
        account_id: AccountId::new(ACCOUNT),
        symbol: SymbolCode::new(SYMBOL),
        position_id: PositionId::new("position-after-checkpoint"),
        side: PositionSide::Buy,
        lots: 0.1,
        open_price: 100.0,
        sl: Some(90.0),
        tp: None,
        floating_pnl: 0.0,
        observed_at: OBSERVED_AT + 1,
    };
    let mut metadata = event_metadata(
        "position-after-checkpoint",
        "position.snapshot",
        OBSERVED_AT + 1,
    );
    metadata.aggregate_type = "position.snapshot".to_owned();
    metadata.aggregate_id = "position-after-checkpoint".to_owned();
    store
        .ingest_position_snapshot(metadata, &position)
        .await
        .unwrap();

    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let error = RiskWorkflowProcessor::new(store.clone())
        .process_intent(
            &intent_id,
            NOW,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RiskWorkflowError::Store(StoreError::SnapshotUnavailable { .. })
    ));
    assert!(store
        .get_risk_result(&RiskId::new(format!("risk:{intent_id}:initial")))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn late_builder_failure_rolls_back_all_workflow_outputs() {
    let (_database, store) = test_store().await;
    let intent_id = IntentId::new("builder-failure");
    seed_ready(
        &store,
        intent(intent_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: Some(100.0),
    };
    let error = RiskWorkflowProcessor::new(store.clone())
        .process_intent(
            &intent_id,
            NOW,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RiskWorkflowError::ExecutionBuild(_)));
    assert!(store
        .get_risk_result(&RiskId::new(format!("risk:{intent_id}:initial")))
        .await
        .unwrap()
        .is_none());
    assert!(store.get_trade_intent(&intent_id).await.unwrap().is_some());
}

#[tokio::test]
async fn concurrent_workers_commit_one_initial_workflow() {
    let (_database, store) = test_store().await;
    seed_ready(
        &store,
        intent("concurrent", TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let context = context(&risk, &strategy, &execution, &resolver);
    let processor = RiskWorkflowProcessor::new(store.clone());

    let (left, right) = tokio::join!(
        processor.process_next(NOW, &context),
        processor.process_next(NOW, &context)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RiskWorkflowOutcome::ExecutionReady { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RiskWorkflowOutcome::NoPendingIntent))
            .count(),
        1
    );
}

#[tokio::test]
async fn command_created_after_checkpoint_blocks_the_next_intent() {
    let (_database, store) = test_store().await;
    let first = IntentId::new("first-command");
    seed_ready(
        &store,
        intent(first.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let processor = RiskWorkflowProcessor::new(store.clone());
    assert!(matches!(
        processor
            .process_intent(
                &first,
                NOW,
                &context(&risk, &strategy, &execution, &resolver),
            )
            .await
            .unwrap(),
        RiskWorkflowOutcome::ExecutionReady { .. }
    ));

    let second = IntentId::new("second-command");
    seed_intent(&store, intent(second.as_str(), TradeIntentAction::Buy)).await;
    let error = processor
        .process_intent(
            &second,
            NOW + 1,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RiskWorkflowError::Store(StoreError::SnapshotUnavailable { .. })
    ));
    assert!(store
        .get_risk_result(&RiskId::new(format!("risk:{second}:initial")))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn durable_open_breaker_persists_an_audited_rejection_without_execution() {
    let (_database, store) = test_store().await;
    let intent_id = IntentId::new("breaker-open");
    seed_ready(
        &store,
        intent(intent_id.as_str(), TradeIntentAction::Buy),
        true,
        0.0,
    )
    .await;
    let head = store
        .get_latest_circuit_breaker_snapshot()
        .await
        .unwrap()
        .unwrap();
    let open = restore_circuit_breaker_snapshot(None, NOW - 1).state;
    let snapshot = open.durable_snapshot_v1();
    store
        .write_circuit_breaker_snapshot(NewCircuitBreakerSnapshot {
            expected_head_revision: Some(head.state_revision),
            schema_version: snapshot.schema_version().to_owned(),
            status: "OPEN".to_owned(),
            recovery_epoch: snapshot.recovery_epoch(),
            updated_at: NOW - 1,
            payload: CanonicalJson::parse(&snapshot.to_json().unwrap()).unwrap(),
        })
        .await
        .unwrap();

    let (risk, strategy, execution) = (risk_policy(), strategy_policy(), execution_policy());
    let resolver = FixedResolver {
        market_command_price: None,
    };
    let outcome = RiskWorkflowProcessor::new(store.clone())
        .process_intent(
            &intent_id,
            NOW,
            &context(&risk, &strategy, &execution, &resolver),
        )
        .await
        .unwrap();
    let RiskWorkflowOutcome::RiskOnly { result } = outcome else {
        panic!("OPEN breaker must not allow execution")
    };
    assert!(!result.approved);
    assert!(store
        .get_execution_workflow(&sinan_types::PlanId::new(format!(
            "plan:{intent_id}:initial"
        )))
        .await
        .unwrap()
        .is_none());
}

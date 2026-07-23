mod common;

use common::test_store;
use sinan_protocol::{ReconciliationReason, ReconciliationRequest};
use sinan_store::{
    CanonicalJson, CoreEventMetadata, DeliverySubject, ExecutionLifecycleUpdate, LegStateUpdate,
    NewDeliveryAttempt, NewExecutionCommand, NewExecutionPlan, NewExecutionWorkflow,
    NewReconciliationRun, NewRiskResult, NewSessionRecord, NewTradeIntent, PlanStateUpdate,
    StoreError, WriteOutcome,
};
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLeg, AdjustedRiskLegAction, ClientId, ClockSyncStatus,
    CommandDeliveryAttemptStatus, CommandId, CorrelationId, DecisionId, ErrorCodeOrString,
    ExecutionAction, ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus,
    ExecutionFailurePolicy, ExecutionLeg, ExecutionLegDefinition, ExecutionLegState,
    ExecutionLegStatus, ExecutionPlan, ExecutionPlanDefinition, ExecutionPlanMode,
    ExecutionPlanState, ExecutionPlanStatus, FillingPolicy, IdempotencyKey, IntentId, OrderType,
    PlanId, RequestId, RiskId, RiskResult, SessionId, SessionStatus, SizingCandidateProvenance,
    StrategyId, SymbolCode, TerminalId, TimePolicy, TimeframeCode, TradeIntent, TradeIntentAction,
    TradeIntentStatus,
};

const INTENT_RECORDED_AT: i64 = 1_010;
const RISK_EVALUATED_AT: i64 = 1_100;
const CREATED_AT: i64 = 1_200;

fn workflow() -> NewExecutionWorkflow {
    let intent_id = IntentId::from("intent_1");
    let risk_id = RiskId::from("risk_1");
    let plan_id = PlanId::from("plan_1");
    let leg_id = single_leg_id(&intent_id);
    let account_id = AccountId::from("account_1");
    let strategy_id = StrategyId::from("strategy_1");
    let symbol = SymbolCode::from("XAUUSD");
    let intent = TradeIntent {
        intent_id: intent_id.clone(),
        decision_id: DecisionId::from("decision_1"),
        strategy_id: strategy_id.clone(),
        correlation_id: CorrelationId::from("correlation_1"),
        idempotency_key: IdempotencyKey::from("intent_key_1"),
        account_id: account_id.clone(),
        symbol: symbol.clone(),
        timeframe: TimeframeCode::from("H4"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "breakout".to_owned(),
        proposed_risk_pct: 1.0,
        proposed_sl: Some(2_320.5),
        proposed_tp: Some(2_365.5),
        proposed_legs: None,
        decision_timestamp: 900,
        signal_expires_at: 5_000,
        requested_at: 1_000,
    };
    let risk_result = RiskResult {
        risk_id: risk_id.clone(),
        request_id: "risk_request_1".into(),
        intent_id: intent_id.clone(),
        account_id: account_id.clone(),
        risk_request_hash: "b".repeat(64),
        approved: true,
        reason: ErrorCodeOrString::from("OK"),
        message: None,
        sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
        risk_base_amount: Some(10_000.0),
        risk_budget_amount: Some(100.0),
        adjusted_risk_pct: Some(0.98),
        sizing_candidates: Some(vec![SizingCandidateProvenance {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 2_350.0,
            stop_loss_price: 2_320.5,
            estimated_cost_per_lot: 0.0,
        }]),
        adjusted_legs: Some(vec![AdjustedRiskLeg {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            lots: 0.07,
            risk_amount: 98.0,
            risk_pct: 0.98,
            sizing_entry_price: 2_350.0,
            approved_sl: 2_320.5,
            loss_per_lot: 1_400.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        }]),
        decision_id: DecisionId::from("decision_1"),
        snapshot_age_ms: 125,
        market_snapshot_age_ms: 75,
        symbol_metadata_age_ms: 250,
        capacity_age_ms: 100,
        evaluated_at: RISK_EVALUATED_AT,
        valid_until: 5_000,
    };
    let leg = ExecutionLeg {
        definition: ExecutionLegDefinition {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: ExecutionAction::Buy,
            lots: Some(0.07),
            sl: Some(2_320.5),
            tp: Some(2_365.5),
            ratio: 1.0,
            dependency: Vec::new(),
        },
        state: ExecutionLegState {
            status: ExecutionLegStatus::Pending,
        },
    };
    let plan = ExecutionPlan {
        definition: ExecutionPlanDefinition {
            plan_id: plan_id.clone(),
            account_id: account_id.clone(),
            strategy_id: strategy_id.clone(),
            mode: ExecutionPlanMode::Sequential,
            failure_policy: ExecutionFailurePolicy::CancelAll,
            rollback_policy: None,
        },
        legs: vec![leg],
        state: ExecutionPlanState {
            status: ExecutionPlanStatus::Pending,
            filled_legs: Vec::new(),
            failed_legs: Vec::new(),
        },
    };
    let command = ExecutionCommand {
        command_id: CommandId::from("command_1"),
        plan_id: Some(plan_id),
        leg_id: Some(leg_id),
        strategy_id,
        account_id: account_id.clone(),
        terminal_id: None,
        client_id: None,
        symbol,
        broker_symbol: Some("XAUUSD".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.07),
        price: None,
        sl: Some(2_320.5),
        tp: Some(2_365.5),
        deviation_points: Some(20),
        magic: 42,
        comment: Some("workflow test".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at: 3_000,
        idempotency_key: IdempotencyKey::from("command_key_1"),
        hmac: "a".repeat(64),
    };
    let state = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id,
        plan_id: command.plan_id.clone(),
        leg_id: command.leg_id.clone(),
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: CREATED_AT,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: CREATED_AT,
    };

    NewExecutionWorkflow {
        intent: NewTradeIntent {
            intent,
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: INTENT_RECORDED_AT,
        },
        risk_result: NewRiskResult {
            result: risk_result,
        },
        plan: NewExecutionPlan {
            plan,
            risk_id,
            intent_id,
            recorded_at: CREATED_AT,
        },
        commands: vec![NewExecutionCommand {
            command,
            risk_id: RiskId::from("risk_1"),
            created_at: CREATED_AT,
        }],
        command_states: vec![state],
    }
}

fn reconciliation_run(
    request_id: &str,
    command_ids: Option<Vec<CommandId>>,
) -> NewReconciliationRun {
    let request_id = RequestId::from(request_id);
    let account_id = AccountId::from("account_1");
    let client_id = ClientId::from("client_1");
    let terminal_id = TerminalId::from("terminal_1");
    NewReconciliationRun {
        request: ReconciliationRequest {
            request_id: request_id.clone(),
            account_id: account_id.clone(),
            terminal_id: Some(terminal_id.clone()),
            client_id: Some(client_id.clone()),
            reason: ReconciliationReason::ConnectionRestored,
            command_ids,
            since_server_time: None,
        },
        requested_at: CREATED_AT + 100,
        event_metadata: CoreEventMetadata {
            event_id: format!("event-{request_id}"),
            event_type: "reconciliation.request".to_owned(),
            aggregate_type: "reconciliation".to_owned(),
            aggregate_id: request_id.to_string(),
            message_id: None,
            schema_version: "ecp.v1.0".to_owned(),
            correlation_id: None,
            causation_id: None,
            account_id: Some(account_id),
            client_id: Some(client_id),
            terminal_id: Some(terminal_id),
            strategy_id: None,
            intent_id: None,
            plan_id: None,
            leg_id: None,
            command_id: None,
            idempotency_key: None,
            event_at: CREATED_AT + 100,
            received_at: CREATED_AT + 100,
            created_at: CREATED_AT + 100,
            source: "transaction-snapshot-test".to_owned(),
        },
    }
}

fn session(session_id: &str, client_id: &str, terminal_id: &str) -> NewSessionRecord {
    NewSessionRecord {
        session_id: SessionId::from(session_id),
        client_id: ClientId::from(client_id),
        account_id: AccountId::from("account_1"),
        terminal_id: Some(TerminalId::from(terminal_id)),
        platform: "MT5".to_owned(),
        status: SessionStatus::Active,
        capabilities: CanonicalJson::from_value(serde_json::json!([])).unwrap(),
        remote_addr: None,
        connected_at: CREATED_AT + 10,
        last_heartbeat_at: Some(CREATED_AT + 10),
        last_time_sync_at: Some(CREATED_AT + 10),
        clock_sync_status: Some(ClockSyncStatus::Synced),
        disconnected_at: None,
        max_inflight_commands: 1,
        updated_at: CREATED_AT + 10,
    }
}

fn delivery_attempt(attempt_id: &str, session_id: &str) -> NewDeliveryAttempt {
    NewDeliveryAttempt {
        attempt_id: attempt_id.to_owned(),
        subject: DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
        session_id: Some(SessionId::from(session_id)),
        message_id: None,
        request_payload: Some(
            CanonicalJson::from_value(serde_json::json!({"command_id": "command_1"})).unwrap(),
        ),
        status: CommandDeliveryAttemptStatus::Backpressure,
        attempted_at: CREATED_AT + 20,
        acked_at: None,
        error: Some("BACKPRESSURE".to_owned()),
        updated_at: CREATED_AT + 20,
    }
}

async fn table_count(pool: &sqlx::SqlitePool, table: &str) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {table}");
    sqlx::query_scalar(&query).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn workflow_commit_is_atomic_typed_and_idempotent() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let inserted = store
        .commit_execution_workflow(input.clone())
        .await
        .expect("complete graph should commit");
    assert!(matches!(inserted, WriteOutcome::Inserted(_)));
    let stored = inserted.into_record();
    assert_eq!(stored.intent.intent, input.intent.intent);
    assert_eq!(stored.risk_result.result, input.risk_result.result);
    assert_eq!(stored.plan.plan, input.plan.plan);
    assert_eq!(stored.commands[0].command, input.commands[0].command);
    assert_eq!(stored.command_states, input.command_states);
    assert_eq!(
        store
            .get_execution_workflow(&input.plan.plan.definition.plan_id)
            .await
            .unwrap(),
        Some(stored.clone())
    );

    assert!(matches!(
        store.commit_execution_workflow(input).await.unwrap(),
        WriteOutcome::Duplicate(record) if record == stored
    ));
    for table in [
        "trade_intents",
        "risk_results",
        "execution_plans",
        "execution_legs",
        "execution_commands",
        "execution_command_states",
    ] {
        assert_eq!(
            table_count(&pool, table).await,
            1,
            "unexpected {table} rows"
        );
    }
}

#[tokio::test]
async fn concurrent_exact_workflow_replay_inserts_once() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let (left, right) = tokio::join!(
        store.commit_execution_workflow(input.clone()),
        store.commit_execution_workflow(input)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.was_inserted())
            .count(),
        1
    );
    assert_eq!(table_count(&pool, "execution_commands").await, 1);
}

#[tokio::test]
async fn workflow_replay_rejects_parent_or_command_drift() {
    let (_database, store, _pool) = test_store().await;
    let input = workflow();
    store
        .commit_execution_workflow(input.clone())
        .await
        .unwrap();

    let mut timestamp_drift = input.clone();
    timestamp_drift.intent.recorded_at += 1;
    assert!(matches!(
        store.commit_execution_workflow(timestamp_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let mut risk_drift = input.clone();
    risk_drift.risk_result.result.message = Some("changed approval fact".to_owned());
    assert!(matches!(
        store.commit_execution_workflow(risk_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let mut command_drift = input;
    command_drift.commands[0].command.comment = Some("changed command".to_owned());
    assert!(matches!(
        store.commit_execution_workflow(command_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));
}

#[tokio::test]
async fn invalid_late_component_rolls_back_every_parent_insert() {
    let (_database, store, pool) = test_store().await;
    let mut input = workflow();
    input.risk_result.result.valid_until = RISK_EVALUATED_AT - 1;
    assert!(matches!(
        store.commit_execution_workflow(input).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    for table in [
        "trade_intents",
        "risk_results",
        "execution_plans",
        "execution_legs",
        "execution_commands",
        "execution_command_states",
    ] {
        assert_eq!(
            table_count(&pool, table).await,
            0,
            "{table} did not roll back"
        );
    }
}

#[tokio::test]
async fn workflow_savepoint_prevents_partial_commit_inside_an_explicit_transaction() {
    let (_database, store, pool) = test_store().await;
    let mut input = workflow();
    input.risk_result.result.valid_until = RISK_EVALUATED_AT - 1;
    let mut transaction = store.begin_write().await.unwrap();
    assert!(matches!(
        transaction.commit_execution_workflow(input).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    transaction.commit().await.unwrap();
    assert_eq!(table_count(&pool, "trade_intents").await, 0);
    assert_eq!(table_count(&pool, "risk_results").await, 0);
}

#[tokio::test]
async fn workflow_requires_exact_approval_mapping_and_pristine_command_states() {
    let (_database, store, _pool) = test_store().await;
    let mut lots_drift = workflow();
    lots_drift.commands[0].command.lots = Some(0.070_000_000_000_000_02);
    assert!(matches!(
        store.commit_execution_workflow(lots_drift).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut missing_state = workflow();
    missing_state.command_states.clear();
    assert!(matches!(
        store.commit_execution_workflow(missing_state).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut advanced_state = workflow();
    advanced_state.command_states[0].status = ExecutionCommandStatus::Dispatched;
    advanced_state.command_states[0].delivery_attempts = 1;
    advanced_state.command_states[0].dispatched_at = Some(CREATED_AT);
    assert!(matches!(
        store.commit_execution_workflow(advanced_state).await,
        Err(StoreError::InvalidRecord { .. })
    ));
}

#[tokio::test]
async fn plan_and_leg_state_updates_use_compare_and_swap() {
    let (_database, store, _pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    let leg_id = input.plan.plan.legs[0].definition.leg_id.clone();
    store.commit_execution_workflow(input).await.unwrap();

    let sent = store
        .update_execution_leg_state(LegStateUpdate {
            plan_id: plan_id.clone(),
            leg_id: leg_id.clone(),
            expected_status: ExecutionLegStatus::Pending,
            expected_updated_at: CREATED_AT,
            state: ExecutionLegState {
                status: ExecutionLegStatus::Sent,
            },
            updated_at: CREATED_AT + 100,
        })
        .await
        .unwrap();
    assert_eq!(sent.leg.state.status, ExecutionLegStatus::Sent);
    assert!(matches!(
        store
            .update_execution_leg_state(LegStateUpdate {
                plan_id: plan_id.clone(),
                leg_id,
                expected_status: ExecutionLegStatus::Pending,
                expected_updated_at: CREATED_AT,
                state: ExecutionLegState {
                    status: ExecutionLegStatus::Accepted,
                },
                updated_at: CREATED_AT + 200,
            })
            .await,
        Err(StoreError::StaleWrite { .. })
    ));

    let update = PlanStateUpdate {
        plan_id: plan_id.clone(),
        expected_status: ExecutionPlanStatus::Pending,
        expected_updated_at: CREATED_AT,
        state: ExecutionPlanState {
            status: ExecutionPlanStatus::Pending,
            filled_legs: Vec::new(),
            failed_legs: Vec::new(),
        },
        updated_at: CREATED_AT + 300,
    };
    let updated = store
        .update_execution_plan_state(update.clone())
        .await
        .unwrap();
    assert_eq!(updated.plan.state.status, ExecutionPlanStatus::Pending);
    assert_eq!(updated.updated_at, CREATED_AT + 300);
    assert_eq!(
        store.update_execution_plan_state(update).await.unwrap(),
        updated
    );
}

fn reconciling_update(input: &NewExecutionWorkflow) -> ExecutionLifecycleUpdate {
    let plan_id = input.plan.plan.definition.plan_id.clone();
    let leg_id = input.plan.plan.legs[0].definition.leg_id.clone();
    ExecutionLifecycleUpdate {
        plan: PlanStateUpdate {
            plan_id: plan_id.clone(),
            expected_status: ExecutionPlanStatus::Pending,
            expected_updated_at: CREATED_AT,
            state: ExecutionPlanState {
                status: ExecutionPlanStatus::Reconciling,
                filled_legs: Vec::new(),
                failed_legs: Vec::new(),
            },
            updated_at: CREATED_AT + 100,
        },
        legs: vec![LegStateUpdate {
            plan_id,
            leg_id,
            expected_status: ExecutionLegStatus::Pending,
            expected_updated_at: CREATED_AT,
            state: ExecutionLegState {
                status: ExecutionLegStatus::DeliveryUnconfirmed,
            },
            updated_at: CREATED_AT + 100,
        }],
    }
}

#[tokio::test]
async fn lifecycle_bundle_atomically_updates_plan_and_legs_and_supports_exact_replay() {
    let (_database, store, _pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store
        .commit_execution_workflow(input.clone())
        .await
        .unwrap();
    let update = reconciling_update(&input);

    let updated = store
        .update_execution_lifecycle(update.clone())
        .await
        .unwrap();
    assert_eq!(updated.plan.state.status, ExecutionPlanStatus::Reconciling);
    assert_eq!(
        updated.plan.legs[0].state.status,
        ExecutionLegStatus::DeliveryUnconfirmed
    );
    assert_eq!(updated.updated_at, CREATED_AT + 100);
    assert_eq!(
        store.update_execution_lifecycle(update).await.unwrap(),
        updated
    );
    assert_eq!(
        store.get_execution_plan(&plan_id).await.unwrap(),
        Some(updated)
    );
}

#[tokio::test]
async fn lifecycle_bundle_rejects_stale_duplicate_and_inconsistent_updates_without_mutation() {
    let (_database, store, _pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store
        .commit_execution_workflow(input.clone())
        .await
        .unwrap();
    let original = store.get_execution_plan(&plan_id).await.unwrap().unwrap();

    let mut duplicate = reconciling_update(&input);
    duplicate.legs.push(duplicate.legs[0].clone());
    assert!(matches!(
        store.update_execution_lifecycle(duplicate).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut inconsistent = reconciling_update(&input);
    inconsistent.plan.state.status = ExecutionPlanStatus::Pending;
    assert!(matches!(
        store.update_execution_lifecycle(inconsistent).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut stale = reconciling_update(&input);
    stale.legs[0].expected_updated_at -= 1;
    assert!(matches!(
        store.update_execution_lifecycle(stale).await,
        Err(StoreError::StaleWrite { .. })
    ));
    assert_eq!(
        store.get_execution_plan(&plan_id).await.unwrap(),
        Some(original)
    );
}

#[tokio::test]
async fn lifecycle_savepoint_rolls_back_leg_cas_when_the_plan_write_fails() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store
        .commit_execution_workflow(input.clone())
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_reject_plan_projection BEFORE UPDATE OF status ON execution_plans \
         BEGIN SELECT RAISE(ABORT, 'test plan projection failure'); END",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    assert!(matches!(
        transaction
            .update_execution_lifecycle(reconciling_update(&input))
            .await,
        Err(StoreError::Database(_))
    ));
    transaction.commit().await.unwrap();

    let stored = store.get_execution_plan(&plan_id).await.unwrap().unwrap();
    assert_eq!(stored.plan.state.status, ExecutionPlanStatus::Pending);
    assert_eq!(
        stored.plan.legs[0].state.status,
        ExecutionLegStatus::Pending
    );
    assert_eq!(stored.updated_at, CREATED_AT);
}

#[tokio::test]
async fn standalone_leg_update_rejects_a_cross_projection_transition() {
    let (_database, store, _pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    let leg_id = input.plan.plan.legs[0].definition.leg_id.clone();
    store.commit_execution_workflow(input).await.unwrap();

    assert!(matches!(
        store
            .update_execution_leg_state(LegStateUpdate {
                plan_id: plan_id.clone(),
                leg_id,
                expected_status: ExecutionLegStatus::Pending,
                expected_updated_at: CREATED_AT,
                state: ExecutionLegState {
                    status: ExecutionLegStatus::DeliveryUnconfirmed,
                },
                updated_at: CREATED_AT + 100,
            })
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    assert_eq!(
        store
            .get_execution_plan(&plan_id)
            .await
            .unwrap()
            .unwrap()
            .plan
            .legs[0]
            .state
            .status,
        ExecutionLegStatus::Pending
    );
}

#[tokio::test]
async fn typed_workflow_read_fails_closed_on_definition_or_lifecycle_tampering() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store.commit_execution_workflow(input).await.unwrap();

    sqlx::query("DROP TRIGGER trg_execution_commands_no_update")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_commands SET symbol = 'OTHER' WHERE plan_id = ?")
        .bind(plan_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.get_execution_workflow(&plan_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    sqlx::query("UPDATE execution_commands SET symbol = 'XAUUSD' WHERE plan_id = ?")
        .bind(plan_id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(
        "UPDATE execution_command_states SET created_at = created_at + 1 WHERE plan_id = ?",
    )
    .bind(plan_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        store.get_execution_workflow(&plan_id).await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn typed_plan_read_detects_journal_hash_tampering() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store.commit_execution_workflow(input).await.unwrap();

    sqlx::query("DROP TRIGGER trg_execution_plans_definition_no_update")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_plans SET payload_hash = ? WHERE plan_id = ?")
        .bind("c".repeat(64))
        .bind(plan_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.get_execution_plan(&plan_id).await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn typed_plan_read_detects_parent_identity_tampering() {
    let (_database, store, pool) = test_store().await;
    let input = workflow();
    let plan_id = input.plan.plan.definition.plan_id.clone();
    store.commit_execution_workflow(input).await.unwrap();

    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("DROP TRIGGER trg_execution_plans_definition_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_plans SET risk_id = 'other_risk' WHERE plan_id = ?")
        .bind(plan_id.as_str())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.get_execution_plan(&plan_id).await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn reconciliation_snapshot_includes_command_with_explicit_route() {
    let (_database, store, _pool) = test_store().await;
    let mut input = workflow();
    input.commands[0].command.client_id = Some(ClientId::from("client_1"));
    input.commands[0].command.terminal_id = Some(TerminalId::from("terminal_1"));
    store.commit_execution_workflow(input).await.unwrap();
    let request_id = RequestId::from("explicit-route");
    store
        .create_reconciliation_run(reconciliation_run(request_id.as_str(), None))
        .await
        .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    let snapshot = transaction
        .load_reconciliation_evaluation_snapshot(&request_id)
        .await
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();

    assert_eq!(snapshot.commands.len(), 1);
    assert_eq!(
        snapshot.commands[0].command.command.command_id,
        CommandId::from("command_1")
    );
}

#[tokio::test]
async fn reconciliation_snapshot_accepts_matching_delivery_session_as_route_proof() {
    let (_database, store, _pool) = test_store().await;
    store.commit_execution_workflow(workflow()).await.unwrap();
    store
        .replace_active_session(session("matching-session", "client_1", "terminal_1"))
        .await
        .unwrap();
    store
        .record_delivery_attempt(delivery_attempt("matching-attempt", "matching-session"))
        .await
        .unwrap();
    let request_id = RequestId::from("delivery-route");
    store
        .create_reconciliation_run(reconciliation_run(request_id.as_str(), None))
        .await
        .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    let snapshot = transaction
        .load_reconciliation_evaluation_snapshot(&request_id)
        .await
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();

    assert_eq!(snapshot.commands.len(), 1);
    assert_eq!(
        snapshot.commands[0].command.command.command_id,
        CommandId::from("command_1")
    );
}

#[tokio::test]
async fn reconciliation_snapshot_rejects_delivery_proof_from_another_route() {
    let (_database, store, _pool) = test_store().await;
    store.commit_execution_workflow(workflow()).await.unwrap();
    store
        .replace_active_session(session("other-session", "client_2", "terminal_2"))
        .await
        .unwrap();
    store
        .record_delivery_attempt(delivery_attempt("other-attempt", "other-session"))
        .await
        .unwrap();
    let request_id = RequestId::from("other-delivery-route");
    store
        .create_reconciliation_run(reconciliation_run(request_id.as_str(), None))
        .await
        .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    let snapshot = transaction
        .load_reconciliation_evaluation_snapshot(&request_id)
        .await
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();

    assert!(snapshot.commands.is_empty());
}

#[tokio::test]
async fn reconciliation_snapshot_fails_closed_for_unknown_targeted_command() {
    let (_database, store, _pool) = test_store().await;
    let request_id = RequestId::from("unknown-target");
    store
        .create_reconciliation_run(reconciliation_run(
            request_id.as_str(),
            Some(vec![CommandId::from("missing-command")]),
        ))
        .await
        .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    assert!(matches!(
        transaction
            .load_reconciliation_evaluation_snapshot(&request_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    transaction.rollback().await.unwrap();
}

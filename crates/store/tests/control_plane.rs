use serde_json::json;
use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, ControlPlaneStateLimits, CoreEventMetadata,
    NewExecutionCommand, NewExecutionEvent, NewExecutionPlan, NewExecutionWorkflow, NewRiskResult,
    NewSessionRecord, NewTradeIntent, StoreError, WriteOutcome,
};
use sinan_types::{
    single_leg_id, AccountId, AccountSnapshot, AdjustedRiskLeg, AdjustedRiskLegAction, ClientId,
    ClockSyncStatus, CommandId, CorrelationId, DecisionId, ErrorCodeOrString, ExecutionAction,
    ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus, ExecutionEvent,
    ExecutionEventStatus, ExecutionFailurePolicy, ExecutionId, ExecutionLeg,
    ExecutionLegDefinition, ExecutionLegState, ExecutionLegStatus, ExecutionPlan,
    ExecutionPlanDefinition, ExecutionPlanMode, ExecutionPlanState, ExecutionPlanStatus,
    FillingPolicy, IdempotencyKey, IntentId, OrderType, PlanId, RiskId, RiskResult, SessionId,
    SessionStatus, SizingCandidateProvenance, StrategyId, SymbolCode, TimePolicy, TimeframeCode,
    TradeIntent, TradeIntentAction, TradeIntentStatus,
};

mod common;

use common::test_store;

fn account_metadata(account_id: &str, sequence: i64) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: format!("account-event-{sequence}"),
        event_type: "account.snapshot".to_owned(),
        aggregate_type: "account".to_owned(),
        aggregate_id: account_id.to_owned(),
        message_id: None,
        schema_version: "ecp.v1.0".to_owned(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::from(account_id)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at: sequence,
        received_at: sequence,
        created_at: sequence,
        source: "control-plane-test".to_owned(),
    }
}

fn account(account_id: &str, observed_at: i64) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from(account_id),
        balance: 1_000.0,
        equity: 1_000.0,
        margin: 10.0,
        free_margin: 990.0,
        currency: "USD".to_owned(),
        observed_at,
    }
}

fn session(account_id: &str, session_id: &str, updated_at: i64) -> NewSessionRecord {
    NewSessionRecord {
        session_id: SessionId::from(session_id),
        client_id: ClientId::from(format!("client-{account_id}")),
        account_id: AccountId::from(account_id),
        terminal_id: None,
        platform: "MT5".to_owned(),
        status: SessionStatus::Active,
        capabilities: CanonicalJson::from_value(json!([])).unwrap(),
        remote_addr: None,
        connected_at: updated_at,
        last_heartbeat_at: Some(updated_at),
        last_time_sync_at: Some(updated_at),
        clock_sync_status: Some(ClockSyncStatus::Synced),
        disconnected_at: None,
        max_inflight_commands: 4,
        updated_at,
    }
}

fn intent(account_id: &str) -> TradeIntent {
    TradeIntent {
        intent_id: IntentId::from(format!("intent-{account_id}")),
        decision_id: DecisionId::from(format!("decision-{account_id}")),
        strategy_id: StrategyId::from("strategy-1"),
        correlation_id: CorrelationId::from(format!("correlation-{account_id}")),
        idempotency_key: IdempotencyKey::from(format!("idempotency-{account_id}")),
        account_id: AccountId::from(account_id),
        symbol: SymbolCode::from("EURUSD"),
        timeframe: TimeframeCode::from("M1"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "test".to_owned(),
        proposed_risk_pct: 0.01,
        proposed_sl: Some(1.09),
        proposed_tp: Some(1.12),
        proposed_legs: None,
        signal_expires_at: 2_000,
        requested_at: 1_000,
    }
}

fn workflow(
    prefix: &str,
    account_id: &str,
    evaluated_at: i64,
    created_at: i64,
) -> NewExecutionWorkflow {
    let intent_id = IntentId::from(format!("intent-{prefix}"));
    let risk_id = RiskId::from(format!("risk-{prefix}"));
    let plan_id = PlanId::from(format!("plan-{prefix}"));
    let leg_id = single_leg_id(&intent_id);
    let command_id = CommandId::from(format!("command-{prefix}"));
    let account_id = AccountId::from(account_id);
    let strategy_id = StrategyId::from("strategy-1");
    let symbol = SymbolCode::from("EURUSD");
    let decision_id = DecisionId::from(format!("decision-{prefix}"));
    let intent = TradeIntent {
        intent_id: intent_id.clone(),
        decision_id: decision_id.clone(),
        strategy_id: strategy_id.clone(),
        correlation_id: CorrelationId::from(format!("correlation-{prefix}")),
        idempotency_key: IdempotencyKey::from(format!("intent-key-{prefix}")),
        account_id: account_id.clone(),
        symbol: symbol.clone(),
        timeframe: TimeframeCode::from("M1"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "control plane ordering test".to_owned(),
        proposed_risk_pct: 1.0,
        proposed_sl: Some(1.09),
        proposed_tp: Some(1.12),
        proposed_legs: None,
        signal_expires_at: 10_000,
        requested_at: 100,
    };
    let result = RiskResult {
        risk_id: risk_id.clone(),
        request_id: format!("risk-request-{prefix}").into(),
        intent_id: intent_id.clone(),
        account_id: account_id.clone(),
        risk_request_hash: "b".repeat(64),
        approved: true,
        reason: ErrorCodeOrString::from("OK"),
        message: None,
        sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
        risk_base_amount: Some(10_000.0),
        risk_budget_amount: Some(100.0),
        adjusted_risk_pct: Some(1.0),
        sizing_candidates: Some(vec![SizingCandidateProvenance {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 1.1,
            stop_loss_price: 1.09,
            estimated_cost_per_lot: 0.0,
        }]),
        adjusted_legs: Some(vec![AdjustedRiskLeg {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            lots: 0.1,
            risk_amount: 100.0,
            risk_pct: 1.0,
            sizing_entry_price: 1.1,
            approved_sl: 1.09,
            loss_per_lot: 1_000.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        }]),
        decision_id,
        snapshot_age_ms: 10,
        market_snapshot_age_ms: 10,
        symbol_metadata_age_ms: 10,
        capacity_age_ms: 10,
        evaluated_at,
        valid_until: 9_000,
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
        legs: vec![ExecutionLeg {
            definition: ExecutionLegDefinition {
                leg_id: leg_id.clone(),
                symbol: symbol.clone(),
                action: ExecutionAction::Buy,
                lots: Some(0.1),
                sl: Some(1.09),
                tp: Some(1.12),
                ratio: 1.0,
                dependency: Vec::new(),
            },
            state: ExecutionLegState {
                status: ExecutionLegStatus::Pending,
            },
        }],
        state: ExecutionPlanState {
            status: ExecutionPlanStatus::Pending,
            filled_legs: Vec::new(),
            failed_legs: Vec::new(),
        },
    };
    let command = ExecutionCommand {
        command_id: command_id.clone(),
        plan_id: Some(plan_id.clone()),
        leg_id: Some(leg_id.clone()),
        strategy_id,
        account_id: account_id.clone(),
        terminal_id: None,
        client_id: None,
        symbol,
        broker_symbol: Some("EURUSD".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.1),
        price: None,
        sl: Some(1.09),
        tp: Some(1.12),
        deviation_points: Some(20),
        magic: 42,
        comment: Some("control plane ordering test".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at: 8_000,
        idempotency_key: IdempotencyKey::from(format!("command-key-{prefix}")),
        hmac: "a".repeat(64),
    };
    let state = ExecutionCommandState {
        command_id,
        account_id,
        plan_id: Some(plan_id),
        leg_id: Some(leg_id),
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: created_at,
    };

    NewExecutionWorkflow {
        intent: NewTradeIntent {
            intent,
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: 200,
        },
        risk_result: NewRiskResult { result },
        plan: NewExecutionPlan {
            plan,
            risk_id: risk_id.clone(),
            intent_id,
            recorded_at: created_at,
        },
        commands: vec![NewExecutionCommand {
            command,
            risk_id,
            created_at,
        }],
        command_states: vec![state],
    }
}

fn execution_event(input: &NewExecutionWorkflow, prefix: &str, event_at: i64) -> NewExecutionEvent {
    let command = &input.commands[0].command;
    NewExecutionEvent {
        event: ExecutionEvent {
            execution_id: ExecutionId::from(format!("execution-{prefix}")),
            command_id: command.command_id.clone(),
            plan_id: command.plan_id.clone(),
            leg_id: command.leg_id.clone(),
            account_id: command.account_id.clone(),
            terminal_id: None,
            client_id: None,
            symbol: command.symbol.clone(),
            broker_symbol: command.broker_symbol.clone(),
            status: ExecutionEventStatus::Accepted,
            broker_order_id: None,
            broker_deal_id: None,
            position_ticket: None,
            idempotency_key: Some(command.idempotency_key.clone()),
            requested_lots: command.lots,
            fill_price: None,
            filled_lots: None,
            remaining_lots: command.lots,
            event_at,
            filled_at: None,
            broker_filled_at: None,
            error_code: None,
            message: None,
        },
        created_at: event_at,
    }
}

#[tokio::test]
async fn state_query_uses_one_explicit_account_scope_for_every_projection() {
    let (_database, store, _) = test_store().await;
    for (sequence, account_id) in [(100, "account-a"), (200, "account-b")] {
        store
            .ingest_account_snapshot(
                account_metadata(account_id, sequence),
                &account(account_id, sequence),
            )
            .await
            .unwrap();
    }
    store
        .insert_session(session("account-a", "session-a", 100))
        .await
        .unwrap();
    store
        .insert_session(session("account-b", "session-b", 200))
        .await
        .unwrap();

    let state = store
        .load_control_plane_state(
            &AuthorizedAccountScope::new([AccountId::from("account-a")]),
            ControlPlaneStateLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(state.latest.accounts, vec![account("account-a", 100)]);
    assert_eq!(state.sessions.len(), 1);
    assert_eq!(state.sessions[0].account_id, AccountId::from("account-a"));

    let empty = store
        .load_control_plane_state(
            &AuthorizedAccountScope::empty(),
            ControlPlaneStateLimits::default(),
        )
        .await
        .unwrap();
    assert!(empty.latest.accounts.is_empty());
    assert!(empty.sessions.is_empty());
    assert!(empty.open_plans.is_empty());
    assert!(empty.pending_commands.is_empty());
    assert!(empty.recent_events.is_empty());
    assert!(empty.latest_risk_results.is_empty());
}

#[tokio::test]
async fn bounded_state_collections_select_latest_then_emit_time_and_id_ascending() {
    let (_database, store, _) = test_store().await;
    let fixtures = [
        ("z", "account-z", 1_100, 1_200, 1_300),
        ("b", "account-b", 2_100, 2_200, 2_300),
        ("a", "account-a", 2_100, 2_200, 2_300),
    ];
    for (prefix, account_id, evaluated_at, created_at, event_at) in fixtures {
        store
            .insert_session(session(
                account_id,
                &format!("session-{prefix}"),
                created_at,
            ))
            .await
            .unwrap();
        let workflow = workflow(prefix, account_id, evaluated_at, created_at);
        store
            .commit_execution_workflow(workflow.clone())
            .await
            .unwrap();
        store
            .append_execution_event(execution_event(&workflow, prefix, event_at))
            .await
            .unwrap();
    }

    let state = store
        .load_control_plane_state(
            &AuthorizedAccountScope::new([
                AccountId::from("account-a"),
                AccountId::from("account-b"),
                AccountId::from("account-z"),
            ]),
            ControlPlaneStateLimits::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        state
            .sessions
            .iter()
            .map(|value| value.session_id.as_str())
            .collect::<Vec<_>>(),
        ["session-z", "session-a", "session-b"]
    );
    assert_eq!(
        state
            .open_plans
            .iter()
            .map(|value| value.plan.definition.plan_id.as_str())
            .collect::<Vec<_>>(),
        ["plan-z", "plan-a", "plan-b"]
    );
    assert_eq!(
        state
            .pending_commands
            .iter()
            .map(|value| value.command_id.as_str())
            .collect::<Vec<_>>(),
        ["command-z", "command-a", "command-b"]
    );
    assert_eq!(
        state
            .recent_events
            .iter()
            .map(|value| value.event.execution_id.as_str())
            .collect::<Vec<_>>(),
        ["execution-z", "execution-a", "execution-b"]
    );
    assert_eq!(
        state
            .latest_risk_results
            .iter()
            .map(|value| value.result.risk_id.as_str())
            .collect::<Vec<_>>(),
        ["risk-z", "risk-a", "risk-b"]
    );

    let bounded = store
        .load_control_plane_state(
            &AuthorizedAccountScope::new([
                AccountId::from("account-a"),
                AccountId::from("account-b"),
                AccountId::from("account-z"),
            ]),
            ControlPlaneStateLimits {
                sessions: 2,
                open_plans: 2,
                pending_commands: 2,
                recent_events: 2,
                latest_risk_results: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        bounded
            .sessions
            .iter()
            .map(|value| value.session_id.as_str())
            .collect::<Vec<_>>(),
        ["session-a", "session-b"]
    );
    assert_eq!(
        bounded
            .open_plans
            .iter()
            .map(|value| value.plan.definition.plan_id.as_str())
            .collect::<Vec<_>>(),
        ["plan-a", "plan-b"]
    );
    assert_eq!(
        bounded
            .pending_commands
            .iter()
            .map(|value| value.command_id.as_str())
            .collect::<Vec<_>>(),
        ["command-a", "command-b"]
    );
    assert_eq!(
        bounded
            .recent_events
            .iter()
            .map(|value| value.event.execution_id.as_str())
            .collect::<Vec<_>>(),
        ["execution-a", "execution-b"]
    );
    assert_eq!(
        bounded
            .latest_risk_results
            .iter()
            .map(|value| value.result.risk_id.as_str())
            .collect::<Vec<_>>(),
        ["risk-a", "risk-b"]
    );
}

#[tokio::test]
async fn status_lookup_filters_scope_before_disclosing_existence() {
    let (_database, store, _) = test_store().await;
    let intent = intent("account-a");
    assert!(matches!(
        store
            .insert_trade_intent(NewTradeIntent {
                intent: intent.clone(),
                initial_status: TradeIntentStatus::Accepted,
                recorded_at: 1_000,
            })
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));

    assert!(store
        .get_trade_intent_workflow_status(
            &AuthorizedAccountScope::new([AccountId::from("account-b")]),
            &intent.intent_id,
        )
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_trade_intent_workflow_status(
            &AuthorizedAccountScope::new([AccountId::from("account-a")]),
            &intent.intent_id,
        )
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn state_query_rejects_zero_collection_limits() {
    let (_database, store, _) = test_store().await;
    let error = store
        .load_control_plane_state(
            &AuthorizedAccountScope::new([AccountId::from("account-a")]),
            ControlPlaneStateLimits {
                sessions: 0,
                ..ControlPlaneStateLimits::default()
            },
        )
        .await
        .expect_err("zero limits must not silently create an unbounded query");
    assert!(matches!(error, StoreError::InvalidRecord { .. }));
}

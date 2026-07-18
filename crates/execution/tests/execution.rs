use sinan_execution::{
    build_execution, decide_recovery, project_leg, project_plan, transition_command,
    validate_command_state, CommandEvidence, CommandTransitionError, ExecutionBuildError,
    ExecutionBuildOutcome, ExecutionBuildRequest, ProjectionError, RecoveryDecision,
    ResolvedLegExecution,
};
use sinan_protocol::{
    verify_execution_command_hmac, CommandInboxStatus, CommandReceived, CommandSigningFormat,
    ProtocolReason,
};
use sinan_risk::{
    evaluate, single_leg_id, CircuitBreakerState, PositionSizingCandidate, RiskCapacity,
    RiskMarketSnapshot, RiskPolicy, RiskRequest, RiskStateWatermarks, StrategyDecision,
    StrategyRiskPolicy, POSITION_SIZING_VERSION_V1,
};
use sinan_types::*;

const NOW: i64 = 1_700_000_000_000;
const ACCOUNT: &str = "account-1";
const SYMBOL: &str = "SYNTH-A";

fn risk_request() -> RiskRequest {
    let account_id = AccountId::new(ACCOUNT);
    let decision_id = DecisionId::new("decision-1");
    let strategy_id = StrategyId::new("strategy-1");
    let symbol = SymbolCode::new(SYMBOL);
    let timeframe = TimeframeCode::new("M5");
    let leg_id = single_leg_id(&IntentId::new("intent-1"));
    RiskRequest {
        request_id: RequestId::new("request-1"),
        risk_id: RiskId::new("risk-1"),
        evaluated_at: NOW,
        decision: StrategyDecision {
            decision_id: decision_id.clone(),
            strategy_id: strategy_id.clone(),
            symbol: symbol.clone(),
            timeframe: timeframe.clone(),
            action: TradeIntentAction::Buy,
            confidence: 0.8,
            reason: "fixture".to_owned(),
            proposed_risk_pct: 1.0,
            proposed_sl: Some(90.0),
            proposed_tp: Some(120.0),
            timestamp: NOW - 2_000,
            signal_expires_at: NOW + 60_000,
        },
        intent: TradeIntent {
            intent_id: IntentId::new("intent-1"),
            decision_id,
            strategy_id: strategy_id.clone(),
            correlation_id: CorrelationId::new("correlation-1"),
            idempotency_key: IdempotencyKey::new("intent-idem-1"),
            account_id: account_id.clone(),
            symbol: symbol.clone(),
            timeframe,
            action: TradeIntentAction::Buy,
            confidence: 0.8,
            reason: "fixture".to_owned(),
            proposed_risk_pct: 1.0,
            proposed_sl: Some(90.0),
            proposed_tp: Some(120.0),
            proposed_legs: None,
            signal_expires_at: NOW + 60_000,
            requested_at: NOW - 1_000,
        },
        agent_review: None,
        account: AccountSnapshot {
            account_id: account_id.clone(),
            balance: 10_000.0,
            equity: 10_000.0,
            margin: 0.0,
            free_margin: 10_000.0,
            currency: "USD".to_owned(),
            observed_at: NOW - 1_000,
        },
        positions: Vec::new(),
        orders: Vec::new(),
        symbol_metadata: vec![metadata()],
        pending_commands: Vec::new(),
        pending_command_states: Vec::new(),
        policy: RiskPolicy {
            position_sizing_version: POSITION_SIZING_VERSION_V1.to_owned(),
            max_risk_per_trade_pct: 5.0,
            max_daily_loss_pct: 4.0,
            max_drawdown_pct: 10.0,
            max_symbol_exposure_pct: 100.0,
            max_total_exposure_pct: 100.0,
            max_margin_usage_pct: 100.0,
            require_stop_loss: true,
            reject_expired_signal: true,
            max_approval_ttl_ms: 5_000,
            max_snapshot_age_ms: 10_000,
            max_order_snapshot_age_ms: 10_000,
            max_market_snapshot_age_ms: 10_000,
            max_symbol_metadata_age_ms: 10_000,
            max_capacity_age_ms: 10_000,
            max_concurrent_positions: 10,
            require_valid_symbol_metadata: true,
            reject_trade_mode_disabled: true,
        },
        strategy_policy: StrategyRiskPolicy {
            max_risk_per_trade_pct: 5.0,
            max_concurrent_legs: 4,
            require_stop_loss: true,
            signal_expiry_bars: 3,
        },
        markets: vec![RiskMarketSnapshot {
            account_id: account_id.clone(),
            snapshot: MarketSnapshot {
                symbol: symbol.clone(),
                broker_symbol: Some(SYMBOL.to_owned()),
                bid: 99.0,
                ask: 100.0,
                spread: 1.0,
                observed_at: NOW - 500,
            },
        }],
        sizing_candidates: vec![PositionSizingCandidate {
            leg_id,
            symbol,
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 100.0,
            stop_loss_price: 90.0,
            estimated_cost_per_lot: 0.0,
        }],
        state_watermarks: RiskStateWatermarks {
            positions_observed_at: NOW - 1_000,
            orders_observed_at: NOW - 1_000,
            pending_commands_reconciled_at: NOW - 1_000,
        },
        capacity: RiskCapacity {
            account_id,
            strategy_id,
            observed_at: NOW - 1_000,
            daily_realized_loss_pct: 0.0,
            equity_drawdown_pct: 0.0,
            remaining_account_risk_pct: 5.0,
            remaining_portfolio_risk_pct: 5.0,
            remaining_strategy_legs: 4,
        },
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
        observed_at: NOW - 1_000,
    }
}

fn policy() -> ExecutionPolicy {
    ExecutionPolicy {
        mode: ExecutionPlanMode::Sequential,
        failure_policy: ExecutionFailurePolicy::CancelAll,
        timeout_ms: 5_000,
        max_command_ttl_ms: 2_000,
        rollback_policy: Some(RollbackPolicy {
            mode: RollbackMode::CloseFilled,
            max_retry_attempts: Some(2),
        }),
    }
}

fn resolved(leg_id: LegId) -> ResolvedLegExecution {
    ResolvedLegExecution {
        leg_id,
        dependency: Vec::new(),
        command_id: CommandId::new("command-1"),
        idempotency_key: IdempotencyKey::new("command-idem-1"),
        terminal_id: Some(TerminalId::new("terminal-1")),
        client_id: Some(ClientId::new("client-1")),
        order_type: OrderType::Market,
        price: None,
        deviation_points: Some(20),
        magic: 42,
        comment: Some("execution test".to_owned()),
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
    }
}

fn bundle() -> (RiskRequest, RiskResult, sinan_execution::ExecutionBundle) {
    let request = risk_request();
    let result = evaluate(&request, &CircuitBreakerState::new()).unwrap();
    let resolved = vec![resolved(request.sizing_candidates[0].leg_id.clone())];
    let execution = policy();
    let outcome = build_execution(ExecutionBuildRequest {
        plan_id: PlanId::new("plan-1"),
        now_ms: NOW + 100,
        risk_request: &request,
        risk_result: &result,
        policy: &execution,
        resolved_legs: &resolved,
        signing_secret: b"execution-secret",
    })
    .unwrap();
    let ExecutionBuildOutcome::Built(bundle) = outcome else {
        panic!("BUY approval must build a bundle")
    };
    (request, result, bundle)
}

#[test]
fn builder_exactly_maps_approval_and_signs_command() {
    let (request, result, bundle) = bundle();
    let approved = &result.adjusted_legs.as_ref().unwrap()[0];
    let command = &bundle.commands[0];
    assert_eq!(command.lots.unwrap().to_bits(), approved.lots.to_bits());
    assert_eq!(
        command.sl.unwrap().to_bits(),
        approved.approved_sl.to_bits()
    );
    assert_eq!(command.tp, request.intent.proposed_tp);
    assert_eq!(command.broker_symbol.as_deref(), Some(SYMBOL));
    assert_eq!(command.expires_at, NOW + 2_100);
    assert_eq!(
        bundle.command_states[0].status,
        ExecutionCommandStatus::Created
    );
    assert_eq!(bundle.command_states[0].created_at, NOW + 100);
    bundle.plan.validate().unwrap();
    verify_execution_command_hmac(
        command,
        b"execution-secret",
        CommandSigningFormat::from_symbol_metadata(&request.symbol_metadata[0]).unwrap(),
    )
    .unwrap();

    let json = serde_json::to_value(&bundle.plan).unwrap();
    assert_eq!(json["plan_id"], "plan-1");
    assert_eq!(
        json["legs"][0]["leg_id"],
        single_leg_id(&request.intent.intent_id).as_str()
    );
    assert_eq!(json["status"], "PENDING");
    let round_trip: ExecutionPlan = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, bundle.plan);
}

#[test]
fn builder_rejects_request_drift_topology_errors_and_worse_price() {
    let mut request = risk_request();
    let result = evaluate(&request, &CircuitBreakerState::new()).unwrap();
    let execution = policy();
    let mut resolved = vec![resolved(request.sizing_candidates[0].leg_id.clone())];

    request.symbol_metadata[0].digits = 3;
    assert!(matches!(
        build_execution(ExecutionBuildRequest {
            plan_id: PlanId::new("plan-1"),
            now_ms: NOW + 100,
            risk_request: &request,
            risk_result: &result,
            policy: &execution,
            resolved_legs: &resolved,
            signing_secret: b"secret",
        }),
        Err(ExecutionBuildError::RequiresReRisk(_))
    ));

    request = risk_request();
    let result = evaluate(&request, &CircuitBreakerState::new()).unwrap();
    resolved[0].leg_id = request.sizing_candidates[0].leg_id.clone();
    resolved[0].dependency = vec![resolved[0].leg_id.clone()];
    assert!(matches!(
        build_execution(ExecutionBuildRequest {
            plan_id: PlanId::new("plan-1"),
            now_ms: NOW + 100,
            risk_request: &request,
            risk_result: &result,
            policy: &execution,
            resolved_legs: &resolved,
            signing_secret: b"secret",
        }),
        Err(ExecutionBuildError::InvalidInput { .. })
    ));

    resolved[0].dependency.clear();
    resolved[0].order_type = OrderType::Limit;
    resolved[0].price = Some(100.01);
    assert!(matches!(
        build_execution(ExecutionBuildRequest {
            plan_id: PlanId::new("plan-1"),
            now_ms: NOW + 100,
            risk_request: &request,
            risk_result: &result,
            policy: &execution,
            resolved_legs: &resolved,
            signing_secret: b"secret",
        }),
        Err(ExecutionBuildError::RequiresReRisk(_))
    ));
}

fn receipt(command: &ExecutionCommand, status: CommandInboxStatus, at: i64) -> CommandReceived {
    CommandReceived {
        command_id: command.command_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        received_at: at,
        inbox_status: status,
        reason: Some(ProtocolReason::Ok),
    }
}

fn event(command: &ExecutionCommand, status: ExecutionEventStatus, at: i64) -> ExecutionEvent {
    let fill = matches!(
        status,
        ExecutionEventStatus::PartiallyFilled | ExecutionEventStatus::Filled
    );
    ExecutionEvent {
        execution_id: ExecutionId::new(format!("event-{status}")),
        command_id: command.command_id.clone(),
        plan_id: command.plan_id.clone(),
        leg_id: command.leg_id.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        symbol: command.symbol.clone(),
        broker_symbol: command.broker_symbol.clone(),
        status,
        broker_order_id: None,
        broker_deal_id: None,
        position_ticket: None,
        idempotency_key: Some(command.idempotency_key.clone()),
        requested_lots: command.lots,
        fill_price: fill.then_some(100.0),
        filled_lots: fill.then_some(command.lots.unwrap()),
        remaining_lots: fill.then_some(0.0),
        event_at: at,
        filled_at: fill.then_some(at),
        broker_filled_at: None,
        error_code: None,
        message: None,
    }
}

#[test]
fn command_state_advances_only_from_business_evidence() {
    let (_, _, bundle) = bundle();
    let command = &bundle.commands[0];
    let mut state = bundle.command_states[0].clone();
    state = transition_command(
        command,
        &state,
        CommandEvidence::Dispatched { at: NOW + 200 },
    )
    .unwrap()
    .into_state();
    assert_eq!(state.delivery_attempts, 1);

    let duplicate = receipt(command, CommandInboxStatus::Duplicate, NOW + 300);
    assert!(matches!(
        transition_command(
            command,
            &state,
            CommandEvidence::ReceivedRecorded(&duplicate)
        ),
        Err(CommandTransitionError::InvalidEvidence(_))
    ));
    state = transition_command(
        command,
        &state,
        CommandEvidence::ReceivedDuplicateKnownSamePayload(&duplicate),
    )
    .unwrap()
    .into_state();
    assert_eq!(state.status, ExecutionCommandStatus::CommandReceived);
    let regressed_duplicate = receipt(command, CommandInboxStatus::Recorded, NOW + 250);
    assert!(matches!(
        transition_command(
            command,
            &state,
            CommandEvidence::ReceivedRecorded(&regressed_duplicate)
        ),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));

    for (index, status) in [
        ExecutionEventStatus::Accepted,
        ExecutionEventStatus::OrderSent,
        ExecutionEventStatus::PartiallyFilled,
        ExecutionEventStatus::Filled,
    ]
    .into_iter()
    .enumerate()
    {
        state = transition_command(
            command,
            &state,
            CommandEvidence::ExecutionEvent(&event(command, status, NOW + 400 + index as i64)),
        )
        .unwrap()
        .into_state();
    }
    assert_eq!(state.status, ExecutionCommandStatus::Filled);
    assert_eq!(state.completed_at, Some(NOW + 403));
    assert!(matches!(
        transition_command(command, &state, CommandEvidence::Cancel { at: NOW + 500 }),
        Err(CommandTransitionError::InvalidEvidence(_))
    ));
}

#[test]
fn projector_derives_leg_plan_and_recovery_without_mutating_definitions() {
    let (_, _, mut bundle) = bundle();
    let command = &bundle.commands[0];
    let mut state = bundle.command_states[0].clone();
    let mut events = Vec::new();
    state = transition_command(
        command,
        &state,
        CommandEvidence::Dispatched { at: NOW + 200 },
    )
    .unwrap()
    .into_state();
    let recorded = receipt(command, CommandInboxStatus::Recorded, NOW + 300);
    state = transition_command(
        command,
        &state,
        CommandEvidence::ReceivedRecorded(&recorded),
    )
    .unwrap()
    .into_state();
    let accepted = event(command, ExecutionEventStatus::Accepted, NOW + 400);
    state = transition_command(command, &state, CommandEvidence::ExecutionEvent(&accepted))
        .unwrap()
        .into_state();
    events.push(accepted);
    let order_sent = event(command, ExecutionEventStatus::OrderSent, NOW + 500);
    state = transition_command(
        command,
        &state,
        CommandEvidence::ExecutionEvent(&order_sent),
    )
    .unwrap()
    .into_state();
    events.push(order_sent);
    let partial = event(command, ExecutionEventStatus::PartiallyFilled, NOW + 600);
    state = transition_command(command, &state, CommandEvidence::ExecutionEvent(&partial))
        .unwrap()
        .into_state();
    events.push(partial);
    let failed = event(command, ExecutionEventStatus::Failed, NOW + 700);
    state = transition_command(command, &state, CommandEvidence::ExecutionEvent(&failed))
        .unwrap()
        .into_state();
    events.push(failed);

    let projected_leg = project_leg(
        &bundle.plan.definition.plan_id,
        &bundle.plan.definition.account_id,
        &bundle.plan.legs[0],
        &[state.clone()],
        &events,
    )
    .unwrap();
    assert_eq!(
        projected_leg.state.status,
        ExecutionLegStatus::PartiallyFilled
    );
    bundle.plan = project_plan(&bundle.plan, &[state], &events).unwrap();
    assert_eq!(bundle.plan.state.status, ExecutionPlanStatus::Partial);
    assert_eq!(bundle.plan.state.filled_legs.len(), 1);
    assert!(matches!(
        decide_recovery(&bundle.plan),
        RecoveryDecision::CloseFilled { .. }
    ));
}

#[test]
fn transitions_reject_out_of_order_and_invalid_expiry_boundaries() {
    let (_, _, bundle) = bundle();
    let command = &bundle.commands[0];
    let created = &bundle.command_states[0];
    assert!(matches!(
        transition_command(
            command,
            created,
            CommandEvidence::Expire {
                at: command.expires_at - 1
            }
        ),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));
    assert!(matches!(
        transition_command(
            command,
            created,
            CommandEvidence::Dispatched {
                at: command.expires_at
            }
        ),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));
    let dispatched = transition_command(
        command,
        created,
        CommandEvidence::Dispatched { at: NOW + 500 },
    )
    .unwrap()
    .into_state();
    assert!(matches!(
        transition_command(
            command,
            &dispatched,
            CommandEvidence::Expire {
                at: command.expires_at
            }
        ),
        Err(CommandTransitionError::InvalidEvidence(_))
    ));
    assert!(matches!(
        transition_command(
            command,
            &dispatched,
            CommandEvidence::Cancel { at: NOW + 600 }
        ),
        Err(CommandTransitionError::InvalidEvidence(_))
    ));
    let recorded = receipt(command, CommandInboxStatus::Recorded, NOW + 400);
    assert!(matches!(
        transition_command(
            command,
            &dispatched,
            CommandEvidence::ReceivedRecorded(&recorded)
        ),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));

    let mut malformed = created.clone();
    malformed.delivery_attempts = 1;
    assert!(matches!(
        validate_command_state(command, &malformed),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));
}

#[test]
fn same_millisecond_evidence_and_projection_identity_are_validated() {
    let (_, _, bundle) = bundle();
    let command = &bundle.commands[0];
    let dispatched = transition_command(
        command,
        &bundle.command_states[0],
        CommandEvidence::Dispatched { at: NOW + 200 },
    )
    .unwrap()
    .into_state();
    let recorded = receipt(command, CommandInboxStatus::Recorded, NOW + 200);
    let received = transition_command(
        command,
        &dispatched,
        CommandEvidence::ReceivedRecorded(&recorded),
    )
    .unwrap()
    .into_state();
    assert_eq!(received.command_received_at, Some(NOW + 200));

    let accepted_event = event(command, ExecutionEventStatus::Accepted, NOW + 200);
    let accepted = transition_command(
        command,
        &received,
        CommandEvidence::ExecutionEvent(&accepted_event),
    )
    .unwrap()
    .into_state();
    let order_sent_event = event(command, ExecutionEventStatus::OrderSent, NOW + 200);
    let order_sent = transition_command(
        command,
        &accepted,
        CommandEvidence::ExecutionEvent(&order_sent_event),
    )
    .unwrap()
    .into_state();
    let mut invalid_fill = event(command, ExecutionEventStatus::Filled, NOW + 300);
    invalid_fill.filled_at = Some(NOW);
    assert!(matches!(
        transition_command(
            command,
            &order_sent,
            CommandEvidence::ExecutionEvent(&invalid_fill)
        ),
        Err(CommandTransitionError::InvalidTimestamp(_))
    ));

    let mut wrong_account = bundle.command_states[0].clone();
    wrong_account.account_id = AccountId::new("other-account");
    assert!(matches!(
        project_plan(&bundle.plan, &[wrong_account], &[]),
        Err(ProjectionError::IdentityMismatch(_))
    ));

    let mut wrong_plan = bundle.command_states[0].clone();
    wrong_plan.plan_id = Some(PlanId::new("other-plan"));
    assert!(matches!(
        project_plan(&bundle.plan, &[wrong_plan], &[]),
        Err(ProjectionError::IdentityMismatch(_))
    ));
}

#[test]
fn plan_summary_validation_requires_the_exact_derived_sets() {
    let (_, _, mut bundle) = bundle();
    let leg_id = bundle.plan.legs[0].definition.leg_id.clone();
    bundle.plan.legs[0].state.status = ExecutionLegStatus::Filled;
    assert!(bundle.plan.validate().is_err());
    bundle.plan.state.filled_legs = vec![leg_id.clone(), leg_id.clone()];
    assert!(bundle.plan.validate().is_err());
    bundle.plan.state.filled_legs = vec![leg_id];
    assert!(bundle.plan.validate().is_err());
    bundle.plan.state.status = ExecutionPlanStatus::Completed;
    bundle.plan.validate().unwrap();
    bundle.plan.state.failed_legs = vec![LegId::new("unknown")];
    assert!(bundle.plan.validate().is_err());
}

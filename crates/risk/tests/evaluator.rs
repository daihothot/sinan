use sinan_risk::{
    evaluate, single_leg_id, transition, CircuitBreakerInput, CircuitBreakerPolicy,
    CircuitBreakerState, PositionSizingCandidate, RiskCapacity, RiskMarketSnapshot, RiskPolicy,
    RiskRequest, RiskStateWatermarks, StrategyDecision, StrategyRiskPolicy, TransitionRequest,
    POSITION_SIZING_VERSION_V1,
};
use sinan_types::{
    AccountId, AccountSnapshot, AdjustedRiskLeg, AdjustedRiskLegAction, BrokerOrderId, CommandId,
    CorrelationId, DecisionId, ErrorCode, ExecutionAction, ExecutionCommand, ExecutionCommandState,
    ExecutionCommandStatus, IdempotencyKey, IntentId, LegId, MarketSnapshot, OrderSnapshot,
    OrderSnapshotStatus, OrderType, PositionId, PositionSide, PositionSnapshot, RequestId, RiskId,
    StrategyId, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TimeframeCode, TradeIntent,
    TradeIntentAction, TradeIntentLeg, TradeIntentLegAction,
};

const NOW: i64 = 1_700_000_000_000;
const ACCOUNT_ID: &str = "account-1";
const PRIMARY_SYMBOL: &str = "SYNTH-A";
const SECONDARY_SYMBOL: &str = "SYNTH-B";

fn request() -> RiskRequest {
    let account_id = AccountId::new(ACCOUNT_ID);
    let decision_id = DecisionId::new("decision-1");
    let strategy_id = StrategyId::new("strategy-1");
    let symbol = SymbolCode::new(PRIMARY_SYMBOL);
    let timeframe = TimeframeCode::new("M5");

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
            reason: "single-leg fixture".to_owned(),
            proposed_risk_pct: 1.0,
            proposed_sl: Some(90.0),
            proposed_tp: Some(120.0),
            timestamp: NOW - 2_000,
            signal_expires_at: NOW + 60_000,
        },
        intent: TradeIntent {
            intent_id: IntentId::new("intent-1"),
            decision_id,
            strategy_id,
            correlation_id: CorrelationId::new("correlation-1"),
            idempotency_key: IdempotencyKey::new("intent-key-1"),
            account_id: account_id.clone(),
            symbol: symbol.clone(),
            timeframe,
            action: TradeIntentAction::Buy,
            confidence: 0.8,
            reason: "single-leg fixture".to_owned(),
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
        symbol_metadata: vec![metadata(ACCOUNT_ID, PRIMARY_SYMBOL, NOW - 1_000)],
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
        markets: vec![market(ACCOUNT_ID, PRIMARY_SYMBOL, 99.0, 100.0)],
        sizing_candidates: vec![candidate(
            single_leg_id(&IntentId::new("intent-1")),
            PRIMARY_SYMBOL,
            AdjustedRiskLegAction::Buy,
            1.0,
            100.0,
            90.0,
        )],
        state_watermarks: RiskStateWatermarks {
            positions_observed_at: NOW - 1_000,
            orders_observed_at: NOW - 1_000,
            pending_commands_reconciled_at: NOW - 1_000,
        },
        capacity: RiskCapacity {
            account_id: account_id.clone(),
            strategy_id: StrategyId::new("strategy-1"),
            observed_at: NOW - 1_000,
            daily_realized_loss_pct: 0.0,
            equity_drawdown_pct: 0.0,
            remaining_account_risk_pct: 5.0,
            remaining_portfolio_risk_pct: 5.0,
            remaining_strategy_legs: 4,
        },
    }
}

fn metadata(account_id: &str, symbol: &str, observed_at: i64) -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::new(account_id),
        symbol: SymbolCode::new(symbol),
        broker_symbol: symbol.to_owned(),
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
        observed_at,
    }
}

fn market(account_id: &str, symbol: &str, bid: f64, ask: f64) -> RiskMarketSnapshot {
    RiskMarketSnapshot {
        account_id: AccountId::new(account_id),
        snapshot: MarketSnapshot {
            symbol: SymbolCode::new(symbol),
            broker_symbol: Some(symbol.to_owned()),
            bid,
            ask,
            spread: ask - bid,
            observed_at: NOW - 500,
        },
    }
}

fn candidate(
    leg_id: impl Into<LegId>,
    symbol: &str,
    action: AdjustedRiskLegAction,
    ratio: f64,
    worst_entry_price: f64,
    stop_loss_price: f64,
) -> PositionSizingCandidate {
    PositionSizingCandidate {
        leg_id: leg_id.into(),
        symbol: SymbolCode::new(symbol),
        action,
        ratio,
        worst_entry_price,
        stop_loss_price,
        estimated_cost_per_lot: 0.0,
    }
}

fn evaluate_closed(request: &RiskRequest) -> sinan_types::RiskResult {
    evaluate(request, &CircuitBreakerState::new()).expect("fixture has a valid audit identity")
}

fn pending_command(symbol: &str) -> ExecutionCommand {
    ExecutionCommand {
        command_id: CommandId::new("pending-command-1"),
        plan_id: None,
        leg_id: Some(LegId::new("pending-leg-1")),
        strategy_id: StrategyId::new("strategy-1"),
        account_id: AccountId::new(ACCOUNT_ID),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::new(symbol),
        broker_symbol: Some(symbol.to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.1),
        price: None,
        sl: Some(90.0),
        tp: Some(120.0),
        deviation_points: Some(10),
        magic: 1,
        comment: None,
        position_ticket: None,
        broker_order_id: None,
        filling_policy: None,
        time_policy: None,
        expiration_time: None,
        expires_at: NOW + 60_000,
        idempotency_key: IdempotencyKey::new("pending-command-key-1"),
        hmac: "test-hmac".to_owned(),
    }
}

fn pending_command_state(command_id: CommandId) -> ExecutionCommandState {
    ExecutionCommandState {
        command_id,
        account_id: AccountId::new(ACCOUNT_ID),
        plan_id: None,
        leg_id: Some(LegId::new("pending-leg-1")),
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: NOW - 1_000,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: NOW - 1_000,
    }
}

fn active_order(symbol: &str) -> OrderSnapshot {
    OrderSnapshot {
        account_id: AccountId::new(ACCOUNT_ID),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::new(symbol),
        broker_symbol: Some(symbol.to_owned()),
        broker_order_id: BrokerOrderId::new(format!("broker-order-{symbol}")),
        position_ticket: None,
        command_id: None,
        plan_id: None,
        leg_id: None,
        idempotency_key: Some(IdempotencyKey::new(format!("order-key-{symbol}"))),
        side: PositionSide::Buy,
        order_type: OrderType::Limit,
        status: OrderSnapshotStatus::Placed,
        requested_lots: 1.0,
        filled_lots: 0.0,
        remaining_lots: 1.0,
        price: Some(200.0),
        sl: Some(190.0),
        tp: Some(220.0),
        created_at: Some(NOW - 2_000),
        updated_at: Some(NOW - 1_000),
        observed_at: NOW - 1_000,
    }
}

fn add_secondary_symbol_inputs(request: &mut RiskRequest) {
    request
        .symbol_metadata
        .push(metadata(ACCOUNT_ID, SECONDARY_SYMBOL, NOW - 1_000));
    request
        .markets
        .push(market(ACCOUNT_ID, SECONDARY_SYMBOL, 199.0, 200.0));
}

fn multi_leg_request() -> RiskRequest {
    let mut request = request();
    request.decision.symbol = SymbolCode::new("PAIR");
    request.intent.symbol = SymbolCode::new("PAIR");
    request.decision.proposed_risk_pct = 3.0;
    request.intent.proposed_risk_pct = 3.0;
    request.decision.proposed_sl = None;
    request.intent.proposed_sl = None;
    request.sizing_candidates[0].leg_id = LegId::new("leg-1");
    request.intent.proposed_legs = Some(vec![
        TradeIntentLeg {
            leg_id: LegId::new("leg-1"),
            symbol: SymbolCode::new(PRIMARY_SYMBOL),
            action: TradeIntentLegAction::Buy,
            ratio: 1.0,
            proposed_sl: Some(90.0),
            proposed_tp: None,
        },
        TradeIntentLeg {
            leg_id: LegId::new("leg-2"),
            symbol: SymbolCode::new(SECONDARY_SYMBOL),
            action: TradeIntentLegAction::Sell,
            ratio: 2.0,
            proposed_sl: Some(210.0),
            proposed_tp: None,
        },
    ]);
    request
        .symbol_metadata
        .push(metadata(ACCOUNT_ID, SECONDARY_SYMBOL, NOW - 1_000));
    request
        .markets
        .push(market(ACCOUNT_ID, SECONDARY_SYMBOL, 200.0, 201.0));
    request.sizing_candidates.push(candidate(
        "leg-2",
        SECONDARY_SYMBOL,
        AdjustedRiskLegAction::Sell,
        2.0,
        200.0,
        210.0,
    ));
    request
}

fn position(observed_at: i64) -> PositionSnapshot {
    PositionSnapshot {
        account_id: AccountId::new(ACCOUNT_ID),
        symbol: SymbolCode::new(PRIMARY_SYMBOL),
        position_id: PositionId::new("position-1"),
        side: PositionSide::Buy,
        lots: 0.1,
        open_price: 100.0,
        sl: Some(90.0),
        tp: Some(120.0),
        floating_pnl: 0.0,
        observed_at,
    }
}

fn hold_request() -> RiskRequest {
    let mut request = request();
    request.decision.action = TradeIntentAction::Hold;
    request.intent.action = TradeIntentAction::Hold;
    request.decision.proposed_risk_pct = 0.0;
    request.intent.proposed_risk_pct = 0.0;
    request.decision.proposed_sl = None;
    request.intent.proposed_sl = None;
    request.decision.proposed_tp = None;
    request.intent.proposed_tp = None;
    request.sizing_candidates.clear();
    request.symbol_metadata.clear();
    request.markets.clear();
    request
}

fn legs(result: &sinan_types::RiskResult) -> &[AdjustedRiskLeg] {
    result
        .adjusted_legs
        .as_deref()
        .expect("approved risk-increasing result must contain adjusted legs")
}

fn assert_rejected_without_lots(result: &sinan_types::RiskResult) {
    result
        .validate()
        .expect("rejected evaluator output must satisfy the RiskResult contract");
    assert!(!result.approved);
    assert!(result.adjusted_legs.is_none());
    assert!(result.sizing_version.is_none());
    assert!(result.risk_base_amount.is_none());
    assert!(result.risk_budget_amount.is_none());
    assert!(result.adjusted_risk_pct.is_none());
    assert!(result.sizing_candidates.is_none());
}

fn assert_reason(result: &sinan_types::RiskResult, expected: impl AsRef<str>) {
    assert_eq!(result.reason.as_str(), expected.as_ref());
}

#[test]
fn sizes_single_leg_from_percentage_point_budget() {
    let result = evaluate_closed(&request());

    assert!(
        result.approved,
        "unexpected rejection: {} {:?}",
        result.reason, result.message
    );
    assert_reason(&result, "OK");
    assert_eq!(
        result.sizing_version.as_deref(),
        Some(POSITION_SIZING_VERSION_V1)
    );
    assert_eq!(result.risk_base_amount, Some(10_000.0));
    assert_eq!(result.risk_budget_amount, Some(100.0));
    assert_eq!(result.adjusted_risk_pct, Some(1.0));
    assert_eq!(result.valid_until, NOW + 5_000);

    let legs = legs(&result);
    assert_eq!(legs.len(), 1);
    assert_eq!(legs[0].leg_id, single_leg_id(&request().intent.intent_id));
    assert_eq!(legs[0].lots, 1.0);
    assert_eq!(legs[0].loss_per_lot, 100.0);
    assert_eq!(legs[0].risk_amount, 100.0);
    assert_eq!(legs[0].risk_pct, 1.0);
}

#[test]
fn rejects_unknown_position_sizing_versions() {
    let mut request = request();
    request.policy.position_sizing_version = "fixed-risk-at-stop.v2".to_owned();

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::RiskInputInvalid.as_str());
}

#[test]
fn actionable_proposed_risk_percentage_must_be_within_percentage_point_range() {
    for (name, proposed_risk_pct) in [
        ("zero", 0.0),
        ("negative", -0.1),
        ("over one hundred", 100.1),
        ("not finite", f64::INFINITY),
    ] {
        let mut request = request();
        request.intent.proposed_risk_pct = proposed_risk_pct;
        request.decision.proposed_risk_pct = proposed_risk_pct;

        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }
}

#[test]
fn sums_absolute_multi_leg_risk_without_hedge_offset() {
    let request = multi_leg_request();

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(result.risk_budget_amount, Some(300.0));
    assert_eq!(result.adjusted_risk_pct, Some(3.0));
    let legs = legs(&result);
    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].lots, 1.0);
    assert_eq!(legs[1].lots, 2.0);
    assert_eq!(legs.iter().map(|leg| leg.risk_amount).sum::<f64>(), 300.0);
}

#[test]
fn includes_cost_buffer_before_flooring_volume_step() {
    let mut request = request();
    request.sizing_candidates[0].estimated_cost_per_lot = 20.0;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    let leg = &legs(&result)[0];
    assert_eq!(leg.loss_per_lot, 120.0);
    assert_eq!(leg.lots, 0.83);
    assert_eq!(leg.risk_amount, 99.6);
    assert_eq!(result.adjusted_risk_pct, Some(0.996));
}

#[test]
fn floors_lots_to_volume_step() {
    let mut request = request();
    request.decision.proposed_risk_pct = 1.23;
    request.intent.proposed_risk_pct = 1.23;
    request.symbol_metadata[0].volume_step = 0.1;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    let leg = &legs(&result)[0];
    assert_eq!(leg.lots, 1.2);
    assert_eq!(leg.risk_amount, 120.0);
    assert_eq!(result.adjusted_risk_pct, Some(1.2));
}

#[test]
fn rejects_missing_stop_loss_market_metadata_and_tick_value() {
    let cases: Vec<(&str, RiskRequest, ErrorCode)> = vec![
        (
            "missing stop loss",
            {
                let mut request = request();
                request.decision.proposed_sl = None;
                request.intent.proposed_sl = None;
                request
            },
            ErrorCode::RiskInputInvalid,
        ),
        (
            "missing market",
            {
                let mut request = request();
                request.markets.clear();
                request
            },
            ErrorCode::MarketSnapshotStale,
        ),
        (
            "missing metadata",
            {
                let mut request = request();
                request.symbol_metadata.clear();
                request
            },
            ErrorCode::SymbolMetadataStale,
        ),
        (
            "invalid tick value",
            {
                let mut request = request();
                request.symbol_metadata[0].tick_value_loss = 0.0;
                request
            },
            ErrorCode::RiskInputInvalid,
        ),
    ];

    for (name, request, expected_reason) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(result.reason.as_str(), expected_reason.as_str(), "{name}");
    }
}

#[test]
fn rejects_whole_intent_when_floored_lots_are_below_volume_min() {
    let mut request = request();
    request.symbol_metadata[0].volume_min = 1.01;

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::InvalidVolume.as_str());
}

#[test]
fn volume_max_reduces_global_scale() {
    let mut request = request();
    request.symbol_metadata[0].volume_max = 0.5;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(legs(&result)[0].lots, 0.5);
    assert_eq!(result.adjusted_risk_pct, Some(0.5));
}

#[test]
fn exposure_limit_reduces_global_scale() {
    let mut request = request();
    // notional_per_lot = ceil(100 / 1) * 10 = 1,000 account-currency units.
    request.policy.max_symbol_exposure_pct = 5.0;
    request.policy.max_total_exposure_pct = 5.0;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(legs(&result)[0].lots, 0.5);
    assert_eq!(result.adjusted_risk_pct, Some(0.5));
}

#[test]
fn margin_limit_reduces_global_scale() {
    let mut request = request();
    request.symbol_metadata[0].margin_initial = Some(1_000.0);
    request.policy.max_margin_usage_pct = 5.0;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(legs(&result)[0].lots, 0.5);
    assert_eq!(result.adjusted_risk_pct, Some(0.5));
}

#[test]
fn actual_risk_never_exceeds_budget() {
    let mut request = request();
    request.decision.proposed_risk_pct = 1.17;
    request.intent.proposed_risk_pct = 1.17;
    request.sizing_candidates[0].estimated_cost_per_lot = 13.0;
    request.symbol_metadata[0].volume_step = 0.07;
    request.symbol_metadata[0].volume_min = 0.07;
    request.symbol_metadata[0].volume_max = 99.96;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    let actual_risk = legs(&result)
        .iter()
        .map(|leg| leg.lots * leg.loss_per_lot)
        .sum::<f64>();
    assert!(actual_risk <= result.risk_budget_amount.unwrap());
}

#[test]
fn rejects_f64_lots_that_would_round_above_decimal_budget() {
    let mut request = request();
    request.decision.proposed_risk_pct = 0.05;
    request.intent.proposed_risk_pct = 0.05;
    request.decision.proposed_sl = Some(93.0);
    request.intent.proposed_sl = Some(93.0);
    request.sizing_candidates[0].stop_loss_price = 93.0;
    request.symbol_metadata[0].tick_value_loss = 1.0;
    request.symbol_metadata[0].volume_min = 0.000000000000000001;
    request.symbol_metadata[0].volume_step = 0.000000000000000001;

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::InvalidVolume.as_str());
}

#[test]
fn wider_stop_and_smaller_budget_never_increase_lots() {
    let baseline = evaluate_closed(&request());
    let baseline_lots = legs(&baseline)[0].lots;

    let mut wider_stop = request();
    wider_stop.decision.proposed_sl = Some(80.0);
    wider_stop.intent.proposed_sl = Some(80.0);
    wider_stop.sizing_candidates[0].stop_loss_price = 80.0;
    let wider_stop_lots = legs(&evaluate_closed(&wider_stop))[0].lots;

    let mut smaller_budget = request();
    smaller_budget.decision.proposed_risk_pct = 0.5;
    smaller_budget.intent.proposed_risk_pct = 0.5;
    let smaller_budget_lots = legs(&evaluate_closed(&smaller_budget))[0].lots;

    assert!(wider_stop_lots <= baseline_lots);
    assert!(smaller_budget_lots <= baseline_lots);
    assert_eq!(wider_stop_lots, 0.5);
    assert_eq!(smaller_budget_lots, 0.5);
}

#[test]
fn hold_is_approved_as_no_op_without_sizing_fields() {
    let result = evaluate_closed(&hold_request());

    assert!(
        result.approved,
        "unexpected HOLD rejection: {} {:?}",
        result.reason, result.message
    );
    assert_reason(&result, "OK");
    assert!(result.sizing_version.is_none());
    assert!(result.risk_base_amount.is_none());
    assert!(result.risk_budget_amount.is_none());
    assert!(result.adjusted_risk_pct.is_none());
    assert!(result.sizing_candidates.is_none());
    assert!(result.adjusted_legs.is_none());
}

#[test]
fn close_is_rejected_when_risk_reduction_cannot_be_proven() {
    let mut request = request();
    request.decision.action = TradeIntentAction::Close;
    request.intent.action = TradeIntentAction::Close;
    request.decision.proposed_risk_pct = 0.0;
    request.intent.proposed_risk_pct = 0.0;
    request.decision.proposed_sl = None;
    request.intent.proposed_sl = None;
    request.sizing_candidates.clear();

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::RiskReductionNotProvable.as_str());
}

#[test]
fn future_and_exact_max_age_account_snapshots_fail_closed() {
    let cases = [("future", NOW + 1), ("age equal to max", NOW - 10_000)];

    for (name, observed_at) in cases {
        let mut request = request();
        request.account.observed_at = observed_at;

        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::AccountSnapshotStale.as_str(),
            "{name}"
        );
    }
}

#[test]
fn cross_account_market_and_metadata_are_rejected() {
    let cases = [
        ("market", {
            let mut request = request();
            request.markets[0].account_id = AccountId::new("account-2");
            request
        }),
        ("metadata", {
            let mut request = request();
            request.symbol_metadata[0].account_id = AccountId::new("account-2");
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }
}

#[test]
fn risk_capacity_identity_must_match_the_target_account_and_strategy() {
    let cases = [
        ("account", {
            let mut request = request();
            request.capacity.account_id = AccountId::new("account-2");
            request
        }),
        ("strategy", {
            let mut request = request();
            request.capacity.strategy_id = StrategyId::new("strategy-2");
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }
}

#[test]
fn remaining_strategy_leg_capacity_is_a_hard_limit() {
    let mut request = multi_leg_request();
    request.capacity.remaining_strategy_legs = 1;

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::PositionLimitExceeded.as_str());
}

#[test]
fn reconciliation_and_capacity_watermarks_must_cover_the_evaluated_state() {
    let cases = [
        ("account snapshot", {
            let mut request = request();
            request.account.observed_at = NOW - 1_001;
            request
        }),
        ("position full-set", {
            let mut request = request();
            request.state_watermarks.positions_observed_at = NOW - 1_001;
            request
        }),
        ("order full-set", {
            let mut request = request();
            request.state_watermarks.orders_observed_at = NOW - 1_001;
            request
        }),
        ("risk capacity", {
            let mut request = request();
            request.capacity.observed_at = NOW - 1_001;
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::PendingExposureConflict.as_str(),
            "{name}"
        );
    }
}

#[test]
fn state_rows_must_belong_to_the_declared_full_set_snapshot() {
    let cases = [
        (
            "position",
            {
                let mut request = request();
                request.positions.push(position(NOW - 1_001));
                request
            },
            ErrorCode::AccountSnapshotStale,
        ),
        (
            "order",
            {
                let mut request = request();
                let mut order = active_order(PRIMARY_SYMBOL);
                order.observed_at = NOW - 1_001;
                request.orders.push(order);
                request
            },
            ErrorCode::OrderSnapshotStale,
        ),
    ];

    for (name, request, expected_reason) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(result.reason.as_str(), expected_reason.as_str(), "{name}");
    }
}

#[test]
fn candidate_leg_id_must_match_the_intent_leg() {
    let mut request = request();
    request.sizing_candidates[0].leg_id = LegId::new("wrong-leg");

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::RiskInputInvalid.as_str());
}

#[test]
fn pending_commands_require_matching_state_and_block_same_symbol() {
    let command = pending_command(PRIMARY_SYMBOL);

    let mut mismatched = request();
    mismatched.pending_commands.push(command.clone());
    mismatched
        .pending_command_states
        .push(pending_command_state(CommandId::new("another-command")));
    let mismatch_result = evaluate_closed(&mismatched);
    assert_rejected_without_lots(&mismatch_result);
    assert_reason(&mismatch_result, ErrorCode::RiskInputInvalid.as_str());

    let mut same_symbol = request();
    same_symbol
        .pending_command_states
        .push(pending_command_state(command.command_id.clone()));
    same_symbol.pending_commands.push(command);
    let conflict_result = evaluate_closed(&same_symbol);
    assert_rejected_without_lots(&conflict_result);
    assert_reason(
        &conflict_result,
        ErrorCode::PendingExposureConflict.as_str(),
    );
}

#[test]
fn pending_command_state_identity_and_lifecycle_must_be_reconciled() {
    let command = pending_command(PRIMARY_SYMBOL);
    let cases = [
        ("plan identity", {
            let mut request = request();
            let mut state = pending_command_state(command.command_id.clone());
            state.plan_id = Some("another-plan".into());
            request.pending_commands.push(command.clone());
            request.pending_command_states.push(state);
            request
        }),
        ("leg identity", {
            let mut request = request();
            let mut state = pending_command_state(command.command_id.clone());
            state.leg_id = Some(LegId::new("another-leg"));
            request.pending_commands.push(command.clone());
            request.pending_command_states.push(state);
            request
        }),
        ("lifecycle timestamp", {
            let mut request = request();
            let mut state = pending_command_state(command.command_id.clone());
            state.dispatched_at = Some(state.created_at - 1);
            request.pending_commands.push(command.clone());
            request.pending_command_states.push(state);
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }

    let mut uncovered = request();
    let mut state = pending_command_state(command.command_id.clone());
    state.updated_at = NOW - 500;
    uncovered.pending_commands.push(command);
    uncovered.pending_command_states.push(state);

    let result = evaluate_closed(&uncovered);
    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::PendingExposureConflict.as_str());
}

#[test]
fn command_state_completed_at_must_match_terminal_status() {
    let command = pending_command(SECONDARY_SYMBOL);
    let cases = [
        ("terminal without completion", {
            let mut request = request();
            let mut state = pending_command_state(command.command_id.clone());
            state.status = ExecutionCommandStatus::Filled;
            request.pending_commands.push(command.clone());
            request.pending_command_states.push(state);
            request
        }),
        ("active with completion", {
            let mut request = request();
            add_secondary_symbol_inputs(&mut request);
            let mut state = pending_command_state(command.command_id.clone());
            state.completed_at = Some(state.updated_at);
            request.pending_commands.push(command.clone());
            request.pending_command_states.push(state);
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }

    let mut terminal = request();
    let mut state = pending_command_state(command.command_id.clone());
    state.status = ExecutionCommandStatus::Filled;
    state.completed_at = Some(state.updated_at);
    terminal.pending_commands.push(command);
    terminal.pending_command_states.push(state);

    let result = evaluate_closed(&terminal);
    assert!(
        result.approved,
        "terminal command should not require unrelated market metadata: {:?}",
        result.message
    );
}

#[test]
fn active_limit_command_requires_a_positive_price() {
    let mut request = request();
    let mut command = pending_command(PRIMARY_SYMBOL);
    command.order_type = Some(OrderType::Limit);
    command.price = None;
    request
        .pending_command_states
        .push(pending_command_state(command.command_id.clone()));
    request.pending_commands.push(command);

    let result = evaluate_closed(&request);

    assert_rejected_without_lots(&result);
    assert_reason(&result, ErrorCode::RiskInputInvalid.as_str());
}

#[test]
fn active_modify_command_blocks_new_risk_without_a_reduction_proof() {
    let cases = [
        ("pending-order price", {
            let mut command = pending_command(SECONDARY_SYMBOL);
            command.action = ExecutionAction::Modify;
            command.order_type = None;
            command.lots = None;
            command.price = Some(110.0);
            command.sl = None;
            command.tp = None;
            command.broker_order_id = Some("broker-order-1".into());
            command
        }),
        ("pending-order lots", {
            let mut command = pending_command(SECONDARY_SYMBOL);
            command.action = ExecutionAction::Modify;
            command.order_type = None;
            command.lots = Some(2.0);
            command.price = Some(105.0);
            command.sl = None;
            command.tp = None;
            command.broker_order_id = Some("broker-order-1".into());
            command
        }),
        ("position stop", {
            let mut command = pending_command(SECONDARY_SYMBOL);
            command.action = ExecutionAction::Modify;
            command.order_type = None;
            command.lots = None;
            command.price = None;
            command.sl = Some(80.0);
            command.tp = None;
            command.position_ticket = Some("position-ticket-1".into());
            command
        }),
    ];

    for (name, command) in cases {
        let mut request = request();
        request
            .pending_command_states
            .push(pending_command_state(command.command_id.clone()));
        request.pending_commands.push(command);

        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::PendingExposureConflict.as_str(),
            "{name}"
        );
    }
}

#[test]
fn unknown_and_internally_inconsistent_broker_orders_fail_closed() {
    let cases = [
        (
            "unknown status",
            {
                let mut request = request();
                let mut order = active_order(PRIMARY_SYMBOL);
                order.status = OrderSnapshotStatus::Unknown;
                request.orders.push(order);
                request
            },
            ErrorCode::PendingExposureConflict,
        ),
        (
            "placed order has fills",
            {
                let mut request = request();
                let mut order = active_order(PRIMARY_SYMBOL);
                order.filled_lots = 0.25;
                order.remaining_lots = 0.75;
                request.orders.push(order);
                request
            },
            ErrorCode::RiskInputInvalid,
        ),
        (
            "partial-fill total drifts",
            {
                let mut request = request();
                let mut order = active_order(PRIMARY_SYMBOL);
                order.status = OrderSnapshotStatus::PartiallyFilled;
                order.filled_lots = 0.25;
                order.remaining_lots = 0.5;
                request.orders.push(order);
                request
            },
            ErrorCode::RiskInputInvalid,
        ),
        (
            "limit price missing",
            {
                let mut request = request();
                let mut order = active_order(PRIMARY_SYMBOL);
                order.price = None;
                request.orders.push(order);
                request
            },
            ErrorCode::RiskInputInvalid,
        ),
    ];

    for (name, request, expected_reason) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(result.reason.as_str(), expected_reason.as_str(), "{name}");
    }
}

#[test]
fn active_order_and_command_broker_symbols_must_match_metadata() {
    let cases = [
        ("order", {
            let mut request = request();
            add_secondary_symbol_inputs(&mut request);
            let mut order = active_order(SECONDARY_SYMBOL);
            order.broker_symbol = Some("WRONG-BROKER-SYMBOL".to_owned());
            request.orders.push(order);
            request
        }),
        ("command", {
            let mut request = request();
            add_secondary_symbol_inputs(&mut request);
            let mut command = pending_command(SECONDARY_SYMBOL);
            command.broker_symbol = Some("WRONG-BROKER-SYMBOL".to_owned());
            request
                .pending_command_states
                .push(pending_command_state(command.command_id.clone()));
            request.pending_commands.push(command);
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }
}

#[test]
fn position_and_order_identities_must_not_be_empty() {
    let cases = [
        ("position", {
            let mut request = request();
            request.positions.push(PositionSnapshot {
                account_id: AccountId::new(ACCOUNT_ID),
                symbol: SymbolCode::new(PRIMARY_SYMBOL),
                position_id: PositionId::new(""),
                side: PositionSide::Buy,
                lots: 0.1,
                open_price: 100.0,
                sl: Some(90.0),
                tp: None,
                floating_pnl: 0.0,
                observed_at: request.state_watermarks.positions_observed_at,
            });
            request
        }),
        ("order", {
            let mut request = request();
            let mut order = active_order(PRIMARY_SYMBOL);
            order.broker_order_id = BrokerOrderId::new("");
            request.orders.push(order);
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);

        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskInputInvalid.as_str(),
            "{name}"
        );
    }
}

#[test]
fn active_broker_order_margin_reduces_new_position_capacity() {
    let mut request = request();
    add_secondary_symbol_inputs(&mut request);
    request.orders.push(active_order(SECONDARY_SYMBOL));
    request.policy.max_margin_usage_pct = 1.5;

    let result = evaluate_closed(&request);

    assert!(
        result.approved,
        "unexpected rejection: {} {:?}",
        result.reason, result.message
    );
    assert_eq!(legs(&result)[0].lots, 0.5);
    assert_eq!(result.adjusted_risk_pct, Some(0.5));
}

#[test]
fn daily_loss_and_drawdown_limits_are_inclusive() {
    let cases = [
        ("daily loss", {
            let mut request = request();
            request.capacity.daily_realized_loss_pct = request.policy.max_daily_loss_pct;
            request
        }),
        ("drawdown", {
            let mut request = request();
            request.capacity.equity_drawdown_pct = request.policy.max_drawdown_pct;
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskLimitExceeded.as_str(),
            "{name}"
        );
    }
}

#[test]
fn zero_account_or_portfolio_capacity_is_a_risk_limit() {
    let cases = [
        ("account", {
            let mut request = request();
            request.capacity.remaining_account_risk_pct = 0.0;
            request
        }),
        ("portfolio", {
            let mut request = request();
            request.capacity.remaining_portfolio_risk_pct = 0.0;
            request
        }),
    ];

    for (name, request) in cases {
        let result = evaluate_closed(&request);
        assert_rejected_without_lots(&result);
        assert_eq!(
            result.reason.as_str(),
            ErrorCode::RiskLimitExceeded.as_str(),
            "{name}"
        );
    }
}

#[test]
fn valid_until_uses_the_earliest_freshness_boundary() {
    let mut request = request();
    request.symbol_metadata[0].observed_at = NOW - 9_000;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(result.symbol_metadata_age_ms, 9_000);
    assert_eq!(result.valid_until, NOW + 1_000);
}

#[test]
fn valid_until_includes_pending_command_reconciliation_freshness() {
    let mut request = request();
    request.state_watermarks.pending_commands_reconciled_at = NOW - 9_000;

    let result = evaluate_closed(&request);

    assert!(result.approved);
    assert_eq!(result.snapshot_age_ms, 9_000);
    assert_eq!(result.valid_until, NOW + 1_000);
}

#[test]
fn open_circuit_breaker_blocks_risk_increasing_intent() {
    let policy = CircuitBreakerPolicy {
        max_daily_realized_loss_bps: 100,
        max_equity_drawdown_bps: 200,
        max_consecutive_broker_rejections: 3,
        max_consecutive_command_failures: 3,
        max_time_sync_unhealthy_ms: 5_000,
        max_consecutive_snapshot_stale: 3,
        max_consecutive_symbol_metadata_stale: 3,
        half_open_observation_ms: 10_000,
        auto_reset: false,
    };
    let outcome = transition(
        &policy,
        &CircuitBreakerState::new(),
        &TransitionRequest::Observe(CircuitBreakerInput {
            daily_realized_loss_bps: 100,
            ..CircuitBreakerInput::default()
        }),
        NOW,
    );
    assert!(outcome.error.is_none());

    let result = evaluate(&request(), &outcome.state).unwrap();

    assert_rejected_without_lots(&result);
    assert_reason(
        &result,
        ErrorCode::RiskEngineCircuitBreakerTriggered.as_str(),
    );

    let hold_result = evaluate(&hold_request(), &outcome.state).unwrap();
    assert!(
        hold_result.approved,
        "unexpected HOLD rejection: {} {:?}",
        hold_result.reason, hold_result.message
    );
    assert_reason(&hold_result, "OK");
    assert!(hold_result.adjusted_legs.is_none());
}

#[test]
fn same_request_produces_identical_serialized_result_bytes() {
    let request = request();

    let first = evaluate_closed(&request);
    let second = evaluate_closed(&request);

    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
}

#[test]
fn malformed_non_finite_requests_have_distinct_audit_hashes() {
    let mut nan_request = request();
    nan_request.sizing_candidates[0].worst_entry_price = f64::NAN;
    let mut infinite_request = request();
    infinite_request.sizing_candidates[0].worst_entry_price = f64::INFINITY;

    let nan_result = evaluate_closed(&nan_request);
    let infinite_result = evaluate_closed(&infinite_request);

    assert_rejected_without_lots(&nan_result);
    assert_rejected_without_lots(&infinite_result);
    assert_ne!(
        nan_result.risk_request_hash,
        infinite_result.risk_request_hash
    );
}

#[test]
fn risk_result_validation_rejects_sizing_provenance_and_arithmetic_drift() {
    let valid = evaluate_closed(&request());
    valid.validate().unwrap();

    let cases = [
        (
            "entry provenance",
            {
                let mut result = valid.clone();
                result.adjusted_legs.as_mut().unwrap()[0].sizing_entry_price += 1.0;
                result
            },
            "adjusted_legs",
        ),
        (
            "stop provenance",
            {
                let mut result = valid.clone();
                result.adjusted_legs.as_mut().unwrap()[0].approved_sl -= 1.0;
                result
            },
            "adjusted_legs",
        ),
        (
            "declared risk",
            {
                let mut result = valid.clone();
                result.adjusted_legs.as_mut().unwrap()[0].risk_amount = 1.0;
                result
            },
            "adjusted_legs[].risk_amount",
        ),
        (
            "risk product overflow",
            {
                let mut result = valid.clone();
                let leg = &mut result.adjusted_legs.as_mut().unwrap()[0];
                leg.lots = f64::MAX;
                leg.loss_per_lot = f64::MAX;
                result
            },
            "adjusted_legs[].risk_amount",
        ),
    ];

    for (name, result, expected_field) in cases {
        let error = result.validate().unwrap_err();
        assert_eq!(error.field(), expected_field, "{name}");
    }
}

#[test]
fn invalid_audit_identity_returns_an_evaluation_error() {
    let mut request = request();
    request.request_id = RequestId::new("");

    let error = evaluate(&request, &CircuitBreakerState::new()).unwrap_err();

    assert_eq!(error.field(), "request_id");
}

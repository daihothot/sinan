use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use sinan_types::{
    single_leg_id, AdjustedRiskLeg, AdjustedRiskLegAction, ErrorCode, ErrorCodeOrString,
    ExecutionAction, ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus, LegId,
    OrderSnapshot, OrderSnapshotStatus, OrderType, PositionSnapshot, RiskResult,
    SizingCandidateProvenance, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode,
    TradeIntentAction, TradeIntentLegAction,
};

use crate::{
    AgentReviewRecommendation, CircuitBreakerAction, CircuitBreakerState, RiskMarketSnapshot,
    RiskRequest,
};

const HUNDRED: Decimal = Decimal::ONE_HUNDRED;

pub const POSITION_SIZING_VERSION_V1: &str = "fixed-risk-at-stop.v1";

type EvaluationResult<T> = Result<T, Rejection>;

#[derive(Clone, Debug)]
struct Rejection {
    code: ErrorCode,
    message: String,
}

impl Rejection {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskEvaluationError {
    field: &'static str,
    reason: String,
}

impl RiskEvaluationError {
    pub fn field(&self) -> &'static str {
        self.field
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    fn new(field: &'static str, reason: impl Into<String>) -> Self {
        Self {
            field,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for RiskEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.field, self.reason)
    }
}

impl Error for RiskEvaluationError {}

#[derive(Clone, Copy, Debug, Default)]
struct AuditAges {
    snapshot: i64,
    market: i64,
    metadata: i64,
    capacity: i64,
}

impl AuditAges {
    fn best_effort(request: &RiskRequest) -> Self {
        let now = request.evaluated_at;
        let mut snapshot = best_effort_age(now, request.account.observed_at)
            .max(best_effort_age(
                now,
                request.state_watermarks.positions_observed_at,
            ))
            .max(best_effort_age(
                now,
                request.state_watermarks.orders_observed_at,
            ))
            .max(best_effort_age(
                now,
                request.state_watermarks.pending_commands_reconciled_at,
            ));
        for position in &request.positions {
            snapshot = snapshot.max(best_effort_age(now, position.observed_at));
        }
        for order in &request.orders {
            snapshot = snapshot.max(best_effort_age(now, order.observed_at));
        }

        Self {
            snapshot,
            market: request
                .markets
                .iter()
                .map(|market| best_effort_age(now, market.snapshot.observed_at))
                .max()
                .unwrap_or(0),
            metadata: request
                .symbol_metadata
                .iter()
                .map(|metadata| best_effort_age(now, metadata.observed_at))
                .max()
                .unwrap_or(0),
            capacity: best_effort_age(now, request.capacity.observed_at),
        }
    }
}

enum Approval {
    NoOp { valid_until: i64 },
    Actionable(ActionableApproval),
}

struct ActionableApproval {
    sizing_version: String,
    risk_base: Decimal,
    risk_budget: Decimal,
    actual_risk_pct: Decimal,
    candidates: Vec<SizingCandidateProvenance>,
    legs: Vec<AdjustedRiskLeg>,
    ages: AuditAges,
    valid_until: i64,
}

#[derive(Clone)]
struct PreparedLeg {
    provenance: SizingCandidateProvenance,
    ratio: Decimal,
    entry: Decimal,
    stop: Decimal,
    loss_per_lot: Decimal,
    notional_per_lot: Decimal,
    margin_per_lot: Decimal,
    volume_min: Decimal,
    volume_max: Decimal,
    volume_step: Decimal,
}

struct SnapshotIndex<'a> {
    metadata: BTreeMap<SymbolCode, &'a SymbolMetadataSnapshot>,
    markets: BTreeMap<SymbolCode, &'a RiskMarketSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScaleLimiter {
    RiskBudget,
    Volume,
    Exposure,
    Margin,
}

/// Evaluates one complete immutable request without I/O or an implicit clock.
///
/// Business denials are represented as `approved=false` RiskResults. Arithmetic
/// and payload errors fail closed with `RISK_INPUT_INVALID`; an invalid result
/// identity is returned separately because no valid audit fact can contain it.
pub fn evaluate(
    request: &RiskRequest,
    breaker: &CircuitBreakerState,
) -> Result<RiskResult, RiskEvaluationError> {
    validate_result_identity(request)?;
    let request_hash = risk_request_hash(request);
    let best_effort_ages = AuditAges::best_effort(request);

    let result = match evaluate_inner(request, breaker) {
        Ok(Approval::NoOp { valid_until }) => RiskResult {
            risk_id: request.risk_id.clone(),
            request_id: request.request_id.clone(),
            intent_id: request.intent.intent_id.clone(),
            account_id: request.intent.account_id.clone(),
            risk_request_hash: request_hash.clone(),
            approved: true,
            reason: ErrorCodeOrString::from("OK"),
            message: None,
            sizing_version: None,
            risk_base_amount: None,
            risk_budget_amount: None,
            adjusted_risk_pct: None,
            sizing_candidates: None,
            adjusted_legs: None,
            decision_id: request.intent.decision_id.clone(),
            snapshot_age_ms: 0,
            market_snapshot_age_ms: 0,
            symbol_metadata_age_ms: 0,
            capacity_age_ms: 0,
            evaluated_at: request.evaluated_at,
            valid_until,
        },
        Ok(Approval::Actionable(approval)) => RiskResult {
            risk_id: request.risk_id.clone(),
            request_id: request.request_id.clone(),
            intent_id: request.intent.intent_id.clone(),
            account_id: request.intent.account_id.clone(),
            risk_request_hash: request_hash.clone(),
            approved: true,
            reason: ErrorCodeOrString::from("OK"),
            message: None,
            sizing_version: Some(approval.sizing_version),
            risk_base_amount: decimal_to_f64(approval.risk_base),
            risk_budget_amount: decimal_to_f64(approval.risk_budget),
            adjusted_risk_pct: decimal_to_f64(approval.actual_risk_pct),
            sizing_candidates: Some(approval.candidates),
            adjusted_legs: Some(approval.legs),
            decision_id: request.intent.decision_id.clone(),
            snapshot_age_ms: approval.ages.snapshot,
            market_snapshot_age_ms: approval.ages.market,
            symbol_metadata_age_ms: approval.ages.metadata,
            capacity_age_ms: approval.ages.capacity,
            evaluated_at: request.evaluated_at,
            valid_until: approval.valid_until,
        },
        Err(rejection) => {
            rejected_result(request, request_hash.clone(), best_effort_ages, rejection)
        }
    };

    match result.validate() {
        Ok(()) => Ok(result),
        Err(error) if result.approved => {
            let rejected = rejected_result(
                request,
                request_hash,
                best_effort_ages,
                Rejection::new(
                    ErrorCode::RiskInputInvalid,
                    format!(
                        "invalid evaluator output at {}: {}",
                        error.field(),
                        error.reason()
                    ),
                ),
            );
            rejected.validate().map_err(|fallback_error| {
                RiskEvaluationError::new(
                    fallback_error.field(),
                    format!(
                        "invalid fail-closed RiskResult: {}",
                        fallback_error.reason()
                    ),
                )
            })?;
            Ok(rejected)
        }
        Err(error) => Err(RiskEvaluationError::new(error.field(), error.reason())),
    }
}

fn validate_result_identity(request: &RiskRequest) -> Result<(), RiskEvaluationError> {
    for (field, value) in [
        ("risk_id", request.risk_id.as_str()),
        ("request_id", request.request_id.as_str()),
        ("intent_id", request.intent.intent_id.as_str()),
        ("account_id", request.intent.account_id.as_str()),
        ("decision_id", request.intent.decision_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(RiskEvaluationError::new(field, "must not be empty"));
        }
    }
    if request.evaluated_at < 0 {
        return Err(RiskEvaluationError::new(
            "evaluated_at",
            "must be a non-negative server Unix timestamp",
        ));
    }
    Ok(())
}

fn evaluate_inner(
    request: &RiskRequest,
    breaker: &CircuitBreakerState,
) -> EvaluationResult<Approval> {
    validate_request_identity(request)?;
    validate_action_shape(request)?;

    match request.intent.action {
        TradeIntentAction::Hold => {
            validate_control_time(request)?;
            let valid_until = hold_valid_until(request)?;
            return Ok(Approval::NoOp { valid_until });
        }
        TradeIntentAction::Close => {
            return Err(Rejection::new(
                ErrorCode::RiskReductionNotProvable,
                "CLOSE has no target position and close lots in the v1 TradeIntent contract",
            ));
        }
        TradeIntentAction::Buy | TradeIntentAction::Sell => {}
    }

    if !breaker
        .authorize(CircuitBreakerAction::RiskIncreasingTradeIntent)
        .allowed
    {
        return Err(Rejection::new(
            ErrorCode::RiskEngineCircuitBreakerTriggered,
            "the global circuit breaker blocks risk-increasing intents",
        ));
    }

    validate_control_time(request)?;
    validate_freshness_policy(request)?;
    let required_symbols = required_symbols(request);
    let index = index_snapshots(request, &required_symbols)?;
    let ages = validate_freshness(request, &required_symbols, &index)?;
    let risk_base = validate_policy_and_inputs(request)?;
    validate_symbol_inputs(&required_symbols, &index)?;
    let prepared = prepare_legs(request, &index)?;

    validate_loss_capacity(request)?;
    let active_commands = active_pending_commands(request)?;
    validate_pending_exposure(request, &index, &prepared, &active_commands)?;
    validate_concurrency(request, &prepared, &active_commands)?;

    let valid_until = approval_valid_until(request, &required_symbols, &index)?;
    size_and_cap(
        request,
        &index,
        &prepared,
        &active_commands,
        risk_base,
        ages,
        valid_until,
    )
    .map(Approval::Actionable)
}

fn validate_request_identity(request: &RiskRequest) -> EvaluationResult<()> {
    let intent = &request.intent;
    let decision = &request.decision;

    if request.evaluated_at < 0 {
        return invalid("evaluated_at must be a non-negative server Unix timestamp");
    }
    for (name, value) in [
        ("request_id", request.request_id.as_str()),
        ("risk_id", request.risk_id.as_str()),
        ("intent_id", intent.intent_id.as_str()),
        ("decision_id", intent.decision_id.as_str()),
        ("strategy_id", intent.strategy_id.as_str()),
        ("account_id", intent.account_id.as_str()),
        ("symbol", intent.symbol.as_str()),
        ("timeframe", intent.timeframe.as_str()),
        ("idempotency_key", intent.idempotency_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return invalid(format!("{name} must not be empty"));
        }
    }

    if decision.decision_id != intent.decision_id
        || decision.strategy_id != intent.strategy_id
        || decision.symbol != intent.symbol
        || decision.timeframe != intent.timeframe
        || decision.action != intent.action
        || !same_float(decision.confidence, intent.confidence)
        || !same_float(decision.proposed_risk_pct, intent.proposed_risk_pct)
        || !same_optional_float(decision.proposed_sl, intent.proposed_sl)
        || !same_optional_float(decision.proposed_tp, intent.proposed_tp)
        || decision.timestamp != intent.decision_timestamp
        || decision.signal_expires_at != intent.signal_expires_at
    {
        return invalid("StrategyDecision and TradeIntent identity or risk fields do not match");
    }
    if intent.account_id != request.account.account_id {
        return invalid("TradeIntent and AccountSnapshot account_id do not match");
    }
    if request.capacity.account_id != intent.account_id
        || request.capacity.strategy_id != intent.strategy_id
    {
        return invalid("RiskCapacity account_id or strategy_id does not match the intent");
    }

    for position in &request.positions {
        if position.account_id != intent.account_id {
            return invalid("PositionSnapshot belongs to another account");
        }
        if position.position_id.as_str().trim().is_empty()
            || position.symbol.as_str().trim().is_empty()
        {
            return invalid("PositionSnapshot identity must not be empty");
        }
    }
    for order in &request.orders {
        if order.account_id != intent.account_id {
            return invalid("OrderSnapshot belongs to another account");
        }
        if order.broker_order_id.as_str().trim().is_empty()
            || order.symbol.as_str().trim().is_empty()
        {
            return invalid("OrderSnapshot identity must not be empty");
        }
    }
    for metadata in &request.symbol_metadata {
        if metadata.account_id != intent.account_id {
            return invalid("SymbolMetadataSnapshot belongs to another account");
        }
    }
    for market in &request.markets {
        if market.account_id != intent.account_id {
            return invalid("MarketSnapshot belongs to another account");
        }
    }
    for command in &request.pending_commands {
        if command.account_id != intent.account_id {
            return invalid("ExecutionCommand belongs to another account");
        }
    }
    for state in &request.pending_command_states {
        if state.account_id != intent.account_id {
            return invalid("ExecutionCommandState belongs to another account");
        }
    }

    Ok(())
}

fn validate_action_shape(request: &RiskRequest) -> EvaluationResult<()> {
    let legs = request.intent.proposed_legs.as_deref().unwrap_or_default();
    match request.intent.action {
        TradeIntentAction::Hold => {
            if !legs.is_empty() || !request.sizing_candidates.is_empty() {
                return invalid("HOLD must not contain proposed legs or sizing candidates");
            }
            if request.intent.proposed_risk_pct != 0.0
                || request.intent.proposed_sl.is_some()
                || request.intent.proposed_tp.is_some()
            {
                return invalid("HOLD must not contain executable risk or stop/target fields");
            }
        }
        TradeIntentAction::Close => {
            if !request.sizing_candidates.is_empty() {
                return invalid("CLOSE must not contain risk-increasing sizing candidates");
            }
        }
        TradeIntentAction::Buy | TradeIntentAction::Sell => {
            if request.intent.proposed_legs.is_some() && legs.is_empty() {
                return invalid("proposed_legs must be omitted or non-empty");
            }
        }
    }
    Ok(())
}

fn validate_control_time(request: &RiskRequest) -> EvaluationResult<()> {
    let intent = &request.intent;
    let decision = &request.decision;
    if request.policy.max_approval_ttl_ms <= 0 {
        return invalid("max_approval_ttl_ms must be greater than zero");
    }
    if decision.timestamp < 0
        || intent.decision_timestamp < 0
        || intent.requested_at < 0
        || intent.decision_timestamp > intent.requested_at
        || intent.requested_at > request.evaluated_at
        || intent.signal_expires_at <= intent.requested_at
    {
        return Err(Rejection::new(
            ErrorCode::TradeIntentTimeInvalid,
            "decision, request, evaluation, and expiry timestamps are inconsistent",
        ));
    }
    if request.evaluated_at >= intent.signal_expires_at {
        return Err(Rejection::new(
            ErrorCode::TradeIntentExpired,
            "the trade intent signal has expired",
        ));
    }
    Ok(())
}

fn hold_valid_until(request: &RiskRequest) -> EvaluationResult<i64> {
    let ttl_boundary = request
        .evaluated_at
        .checked_add(request.policy.max_approval_ttl_ms)
        .ok_or_else(|| Rejection::new(ErrorCode::RiskInputInvalid, "approval TTL overflow"))?;
    let valid_until = ttl_boundary.min(request.intent.signal_expires_at);
    if valid_until <= request.evaluated_at {
        return Err(Rejection::new(
            ErrorCode::TradeIntentExpired,
            "HOLD validation has no positive validity window",
        ));
    }
    Ok(valid_until)
}

fn validate_freshness_policy(request: &RiskRequest) -> EvaluationResult<()> {
    for (name, value) in [
        ("max_snapshot_age_ms", request.policy.max_snapshot_age_ms),
        (
            "max_order_snapshot_age_ms",
            request.policy.max_order_snapshot_age_ms,
        ),
        (
            "max_market_snapshot_age_ms",
            request.policy.max_market_snapshot_age_ms,
        ),
        (
            "max_symbol_metadata_age_ms",
            request.policy.max_symbol_metadata_age_ms,
        ),
        ("max_capacity_age_ms", request.policy.max_capacity_age_ms),
    ] {
        if value <= 0 {
            return invalid(format!("{name} must be greater than zero"));
        }
    }
    Ok(())
}

fn required_symbols(request: &RiskRequest) -> BTreeSet<SymbolCode> {
    let mut symbols = BTreeSet::new();
    if let Some(legs) = &request.intent.proposed_legs {
        symbols.extend(legs.iter().map(|leg| leg.symbol.clone()));
    } else {
        symbols.insert(request.intent.symbol.clone());
    }
    symbols.extend(
        request
            .positions
            .iter()
            .map(|position| position.symbol.clone()),
    );
    symbols.extend(
        request
            .orders
            .iter()
            .filter(|order| order_is_active(order.status))
            .map(|order| order.symbol.clone()),
    );
    symbols.extend(
        request
            .pending_commands
            .iter()
            .filter(|command| {
                matches!(command.action, ExecutionAction::Buy | ExecutionAction::Sell)
                    && request
                        .pending_command_states
                        .iter()
                        .filter(|state| state.command_id == command.command_id)
                        .all(|state| !command_state_is_terminal(state.status))
            })
            .map(|command| command.symbol.clone()),
    );
    symbols
}

fn index_snapshots<'a>(
    request: &'a RiskRequest,
    required_symbols: &BTreeSet<SymbolCode>,
) -> EvaluationResult<SnapshotIndex<'a>> {
    let mut metadata = BTreeMap::new();
    for snapshot in &request.symbol_metadata {
        if metadata.insert(snapshot.symbol.clone(), snapshot).is_some() {
            return invalid(format!(
                "multiple symbol metadata snapshots exist for {}",
                snapshot.symbol
            ));
        }
    }
    let mut markets = BTreeMap::new();
    for market in &request.markets {
        if markets
            .insert(market.snapshot.symbol.clone(), market)
            .is_some()
        {
            return invalid(format!(
                "multiple market snapshots exist for {}",
                market.snapshot.symbol
            ));
        }
    }

    for symbol in required_symbols {
        if !metadata.contains_key(symbol) {
            return Err(Rejection::new(
                ErrorCode::SymbolMetadataStale,
                format!("symbol metadata is missing for {symbol}"),
            ));
        }
        if !markets.contains_key(symbol) {
            return Err(Rejection::new(
                ErrorCode::MarketSnapshotStale,
                format!("market snapshot is missing for {symbol}"),
            ));
        }
    }
    Ok(SnapshotIndex { metadata, markets })
}

fn validate_freshness(
    request: &RiskRequest,
    required_symbols: &BTreeSet<SymbolCode>,
    index: &SnapshotIndex<'_>,
) -> EvaluationResult<AuditAges> {
    let now = request.evaluated_at;
    let mut snapshot = checked_age(
        now,
        request.account.observed_at,
        request.policy.max_snapshot_age_ms,
        ErrorCode::AccountSnapshotStale,
        "account snapshot",
    )?;
    snapshot = snapshot.max(checked_age(
        now,
        request.state_watermarks.positions_observed_at,
        request.policy.max_snapshot_age_ms,
        ErrorCode::AccountSnapshotStale,
        "position full-set watermark",
    )?);
    snapshot = snapshot.max(checked_age(
        now,
        request.state_watermarks.orders_observed_at,
        request.policy.max_order_snapshot_age_ms,
        ErrorCode::OrderSnapshotStale,
        "order full-set watermark",
    )?);
    snapshot = snapshot.max(checked_age(
        now,
        request.state_watermarks.pending_commands_reconciled_at,
        request.policy.max_order_snapshot_age_ms,
        ErrorCode::PendingExposureConflict,
        "pending-command reconciliation watermark",
    )?);
    if request.account.observed_at < request.state_watermarks.pending_commands_reconciled_at
        || request.state_watermarks.positions_observed_at
            < request.state_watermarks.pending_commands_reconciled_at
        || request.state_watermarks.orders_observed_at
            < request.state_watermarks.pending_commands_reconciled_at
    {
        return Err(Rejection::new(
            ErrorCode::PendingExposureConflict,
            "account, position, and order snapshots must not predate command reconciliation",
        ));
    }
    for position in &request.positions {
        if position.observed_at != request.state_watermarks.positions_observed_at {
            return Err(Rejection::new(
                ErrorCode::AccountSnapshotStale,
                "position rows must belong to the declared full-set snapshot",
            ));
        }
        snapshot = snapshot.max(checked_age(
            now,
            position.observed_at,
            request.policy.max_snapshot_age_ms,
            ErrorCode::AccountSnapshotStale,
            "position snapshot",
        )?);
    }
    for order in &request.orders {
        if order.observed_at != request.state_watermarks.orders_observed_at {
            return Err(Rejection::new(
                ErrorCode::OrderSnapshotStale,
                "order rows must belong to the declared full-set snapshot",
            ));
        }
        snapshot = snapshot.max(checked_age(
            now,
            order.observed_at,
            request.policy.max_order_snapshot_age_ms,
            ErrorCode::OrderSnapshotStale,
            "order snapshot",
        )?);
    }

    let mut market_age = 0;
    let mut metadata_age = 0;
    for symbol in required_symbols {
        let market = index.markets.get(symbol).ok_or_else(|| {
            Rejection::new(
                ErrorCode::MarketSnapshotStale,
                format!("market snapshot is missing for {symbol}"),
            )
        })?;
        market_age = market_age.max(checked_age(
            now,
            market.snapshot.observed_at,
            request.policy.max_market_snapshot_age_ms,
            ErrorCode::MarketSnapshotStale,
            "market snapshot",
        )?);
        let metadata = index.metadata.get(symbol).ok_or_else(|| {
            Rejection::new(
                ErrorCode::SymbolMetadataStale,
                format!("symbol metadata is missing for {symbol}"),
            )
        })?;
        metadata_age = metadata_age.max(checked_age(
            now,
            metadata.observed_at,
            request.policy.max_symbol_metadata_age_ms,
            ErrorCode::SymbolMetadataStale,
            "symbol metadata",
        )?);
    }
    let capacity = checked_age(
        now,
        request.capacity.observed_at,
        request.policy.max_capacity_age_ms,
        ErrorCode::AccountSnapshotStale,
        "risk capacity",
    )?;
    let capacity_dependency_watermark = request
        .account
        .observed_at
        .max(request.state_watermarks.positions_observed_at)
        .max(request.state_watermarks.orders_observed_at)
        .max(request.state_watermarks.pending_commands_reconciled_at);
    if request.capacity.observed_at < capacity_dependency_watermark {
        return Err(Rejection::new(
            ErrorCode::PendingExposureConflict,
            "risk capacity must not predate account, position, order, or command evidence",
        ));
    }

    Ok(AuditAges {
        snapshot,
        market: market_age,
        metadata: metadata_age,
        capacity,
    })
}

fn validate_policy_and_inputs(request: &RiskRequest) -> EvaluationResult<Decimal> {
    let policy = &request.policy;
    let strategy = &request.strategy_policy;
    if policy.position_sizing_version != POSITION_SIZING_VERSION_V1 {
        return invalid(format!(
            "unsupported position_sizing_version {}; expected {POSITION_SIZING_VERSION_V1}",
            policy.position_sizing_version
        ));
    }
    if !policy.require_stop_loss
        || !policy.reject_expired_signal
        || !policy.require_valid_symbol_metadata
        || !policy.reject_trade_mode_disabled
        || !strategy.require_stop_loss
    {
        return invalid("v1 hard-risk policy switches must all remain enabled");
    }
    if policy.max_concurrent_positions == 0 || strategy.max_concurrent_legs == 0 {
        return invalid("concurrency limits must be greater than zero");
    }
    if request.capacity.remaining_strategy_legs > strategy.max_concurrent_legs {
        return invalid("remaining_strategy_legs exceeds the configured strategy limit");
    }
    if strategy.signal_expiry_bars == 0 {
        return invalid("signal_expiry_bars must be greater than zero");
    }

    let bounded_percentages = [
        ("max_risk_per_trade_pct", policy.max_risk_per_trade_pct),
        ("max_daily_loss_pct", policy.max_daily_loss_pct),
        ("max_drawdown_pct", policy.max_drawdown_pct),
        ("max_margin_usage_pct", policy.max_margin_usage_pct),
        (
            "strategy.max_risk_per_trade_pct",
            strategy.max_risk_per_trade_pct,
        ),
    ];
    for (name, value) in bounded_percentages {
        validate_percentage(name, value, true)?;
    }
    validate_non_negative_percentage(
        "remaining_account_risk_pct",
        request.capacity.remaining_account_risk_pct,
    )?;
    validate_non_negative_percentage(
        "remaining_portfolio_risk_pct",
        request.capacity.remaining_portfolio_risk_pct,
    )?;
    validate_percentage("proposed_risk_pct", request.intent.proposed_risk_pct, true)?;
    for (name, value) in [
        ("max_symbol_exposure_pct", policy.max_symbol_exposure_pct),
        ("max_total_exposure_pct", policy.max_total_exposure_pct),
    ] {
        let value = decimal(name, value)?;
        if value <= Decimal::ZERO {
            return invalid(format!("{name} must be greater than zero"));
        }
    }
    validate_non_negative_percentage(
        "daily_realized_loss_pct",
        request.capacity.daily_realized_loss_pct,
    )?;
    validate_non_negative_percentage("equity_drawdown_pct", request.capacity.equity_drawdown_pct)?;
    let confidence = decimal("confidence", request.intent.confidence)?;
    if confidence < Decimal::ZERO || confidence > Decimal::ONE {
        return invalid("confidence must be between zero and one");
    }
    if let Some(review) = &request.agent_review {
        if review.review_id.trim().is_empty()
            || !review.score_adjustment.is_finite()
            || !(-1.0..=1.0).contains(&review.score_adjustment)
            || review.recommendation != AgentReviewRecommendation::None
        {
            return invalid("agent review is incomplete or requests unsupported risk mutation");
        }
    }

    if request.account.currency.trim().is_empty() {
        return invalid("account currency must not be empty");
    }
    let balance = decimal("account.balance", request.account.balance)?;
    let equity = decimal("account.equity", request.account.equity)?;
    let margin = decimal("account.margin", request.account.margin)?;
    let free_margin = decimal("account.free_margin", request.account.free_margin)?;
    if margin < Decimal::ZERO || free_margin < Decimal::ZERO {
        return invalid("account margin and free_margin must be non-negative");
    }
    let risk_base = balance.max(Decimal::ZERO).min(equity.max(Decimal::ZERO));
    if risk_base <= Decimal::ZERO {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "risk base is zero because balance or equity is not positive",
        ));
    }
    Ok(risk_base)
}

fn validate_symbol_inputs(
    required_symbols: &BTreeSet<SymbolCode>,
    index: &SnapshotIndex<'_>,
) -> EvaluationResult<()> {
    for symbol in required_symbols {
        let metadata = index
            .metadata
            .get(symbol)
            .ok_or_else(|| Rejection::new(ErrorCode::SymbolMetadataStale, "metadata missing"))?;
        let market = index
            .markets
            .get(symbol)
            .ok_or_else(|| Rejection::new(ErrorCode::MarketSnapshotStale, "market missing"))?;

        if metadata.broker_symbol.trim().is_empty() {
            return invalid(format!("broker_symbol is empty for {symbol}"));
        }
        if market
            .snapshot
            .broker_symbol
            .as_ref()
            .is_some_and(|broker| broker != &metadata.broker_symbol)
        {
            return invalid(format!(
                "market and metadata broker_symbol differ for {symbol}"
            ));
        }
        let point = decimal("metadata.point", metadata.point)?;
        let tick_size = decimal("metadata.tick_size", metadata.tick_size)?;
        let tick_value = decimal("metadata.tick_value_loss", metadata.tick_value_loss)?;
        let contract_size = decimal("metadata.contract_size", metadata.contract_size)?;
        let volume_min = decimal("metadata.volume_min", metadata.volume_min)?;
        let volume_max = decimal("metadata.volume_max", metadata.volume_max)?;
        let volume_step = decimal("metadata.volume_step", metadata.volume_step)?;
        if [
            point,
            tick_size,
            tick_value,
            contract_size,
            volume_min,
            volume_max,
            volume_step,
        ]
        .into_iter()
        .any(|value| value <= Decimal::ZERO)
            || volume_min > volume_max
            || volume_step > volume_max
        {
            return invalid(format!("invalid broker metadata constraints for {symbol}"));
        }
        let margin_initial = metadata
            .margin_initial
            .ok_or_else(|| {
                Rejection::new(
                    ErrorCode::RiskInputInvalid,
                    format!("margin_initial is missing for {symbol}"),
                )
            })
            .and_then(|value| decimal("metadata.margin_initial", value))?;
        if margin_initial <= Decimal::ZERO {
            return invalid(format!("margin_initial must be positive for {symbol}"));
        }

        let bid = decimal("market.bid", market.snapshot.bid)?;
        let ask = decimal("market.ask", market.snapshot.ask)?;
        let spread = decimal("market.spread", market.snapshot.spread)?;
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask < bid || spread < Decimal::ZERO {
            return invalid(format!("invalid market snapshot prices for {symbol}"));
        }
    }
    Ok(())
}

fn prepare_legs(
    request: &RiskRequest,
    index: &SnapshotIndex<'_>,
) -> EvaluationResult<Vec<PreparedLeg>> {
    let mut candidate_by_id = BTreeMap::new();
    for candidate in &request.sizing_candidates {
        if candidate.leg_id.as_str().trim().is_empty()
            || candidate.symbol.as_str().trim().is_empty()
            || candidate_by_id
                .insert(candidate.leg_id.clone(), candidate)
                .is_some()
        {
            return invalid("sizing candidate leg ids and symbols must be non-empty and unique");
        }
    }

    let expected: Vec<ExpectedLeg> = if let Some(legs) = &request.intent.proposed_legs {
        let mut seen = BTreeSet::new();
        let mut expected = Vec::with_capacity(legs.len());
        for leg in legs {
            if !seen.insert(leg.leg_id.clone()) {
                return invalid(format!("duplicate proposed leg id {}", leg.leg_id));
            }
            let action = match leg.action {
                TradeIntentLegAction::Buy => AdjustedRiskLegAction::Buy,
                TradeIntentLegAction::Sell => AdjustedRiskLegAction::Sell,
                TradeIntentLegAction::Close => {
                    return Err(Rejection::new(
                        ErrorCode::RiskReductionNotProvable,
                        format!("CLOSE leg {} cannot prove its reduction amount", leg.leg_id),
                    ));
                }
            };
            expected.push(ExpectedLeg {
                leg_id: leg.leg_id.clone(),
                symbol: leg.symbol.clone(),
                action,
                ratio: leg.ratio,
                stop: leg.proposed_sl,
            });
        }
        expected
    } else {
        let action = match request.intent.action {
            TradeIntentAction::Buy => AdjustedRiskLegAction::Buy,
            TradeIntentAction::Sell => AdjustedRiskLegAction::Sell,
            _ => return invalid("only BUY/SELL can reach actionable sizing"),
        };
        vec![ExpectedLeg {
            leg_id: single_leg_id(&request.intent.intent_id),
            symbol: request.intent.symbol.clone(),
            action,
            ratio: 1.0,
            stop: request.intent.proposed_sl,
        }]
    };

    if expected.len() != candidate_by_id.len() {
        return invalid("sizing candidates must correspond one-to-one with proposed legs");
    }

    let mut expected = expected;
    expected.sort_by(|left, right| left.leg_id.cmp(&right.leg_id));
    let mut prepared = Vec::with_capacity(expected.len());
    for expected_leg in expected {
        let candidate = candidate_by_id.get(&expected_leg.leg_id).ok_or_else(|| {
            Rejection::new(
                ErrorCode::RiskInputInvalid,
                format!("sizing candidate is missing for {}", expected_leg.leg_id),
            )
        })?;
        if candidate.symbol != expected_leg.symbol || candidate.action != expected_leg.action {
            return invalid(format!(
                "candidate identity differs for {}",
                expected_leg.leg_id
            ));
        }
        let ratio = decimal("candidate.ratio", candidate.ratio)?;
        let expected_ratio = decimal("proposed leg ratio", expected_leg.ratio)?;
        if ratio <= Decimal::ZERO || ratio != expected_ratio {
            return invalid(format!(
                "candidate ratio differs for {}",
                expected_leg.leg_id
            ));
        }
        if request.intent.proposed_legs.is_none() && ratio != Decimal::ONE {
            return invalid("single-leg ratio must equal one");
        }
        let expected_stop = expected_leg
            .stop
            .ok_or_else(|| Rejection::new(ErrorCode::RiskInputInvalid, "stop loss is required"))?;
        let expected_stop = decimal("proposed stop loss", expected_stop)?;
        let entry = decimal("candidate.worst_entry_price", candidate.worst_entry_price)?;
        let stop = decimal("candidate.stop_loss_price", candidate.stop_loss_price)?;
        let cost = decimal(
            "candidate.estimated_cost_per_lot",
            candidate.estimated_cost_per_lot,
        )?
        .max(Decimal::ZERO);
        if entry <= Decimal::ZERO || stop <= Decimal::ZERO || stop != expected_stop {
            return invalid(format!(
                "invalid or mismatched stop for {}",
                expected_leg.leg_id
            ));
        }

        let metadata = index.metadata.get(&candidate.symbol).ok_or_else(|| {
            Rejection::new(ErrorCode::SymbolMetadataStale, "candidate metadata missing")
        })?;
        let market = index.markets.get(&candidate.symbol).ok_or_else(|| {
            Rejection::new(ErrorCode::MarketSnapshotStale, "candidate market missing")
        })?;
        validate_trade_mode(candidate.action, metadata.trade_mode)?;
        let bid = decimal("market.bid", market.snapshot.bid)?;
        let ask = decimal("market.ask", market.snapshot.ask)?;
        let direction_valid = match candidate.action {
            AdjustedRiskLegAction::Buy => stop < entry && entry >= ask,
            AdjustedRiskLegAction::Sell => stop > entry && entry <= bid,
        };
        if !direction_valid {
            return Err(Rejection::new(
                ErrorCode::InvalidStops,
                format!(
                    "entry/stop direction or conservative entry is invalid for {}",
                    candidate.leg_id
                ),
            ));
        }

        let point = decimal("metadata.point", metadata.point)?;
        let tick_size = decimal("metadata.tick_size", metadata.tick_size)?;
        let tick_value = decimal("metadata.tick_value_loss", metadata.tick_value_loss)?;
        let stop_distance = (entry - stop).abs();
        let minimum_stop = Decimal::from(metadata.stops_level_points)
            .checked_mul(point)
            .ok_or_else(arithmetic_overflow)?;
        if stop_distance < minimum_stop {
            return Err(Rejection::new(
                ErrorCode::InvalidStops,
                format!(
                    "stop distance violates stops_level for {}",
                    candidate.leg_id
                ),
            ));
        }
        let loss_ticks = stop_distance
            .checked_div(tick_size)
            .ok_or_else(arithmetic_overflow)?
            .ceil();
        let loss_per_lot = loss_ticks
            .checked_mul(tick_value)
            .and_then(|value| value.checked_add(cost))
            .ok_or_else(arithmetic_overflow)?;
        if loss_ticks <= Decimal::ZERO || loss_per_lot <= Decimal::ZERO {
            return invalid(format!("loss cannot be bounded for {}", candidate.leg_id));
        }
        let notional_per_lot = notional_per_lot(metadata, market, Some(entry))?;
        let margin_per_lot = decimal(
            "metadata.margin_initial",
            metadata.margin_initial.ok_or_else(|| {
                Rejection::new(ErrorCode::RiskInputInvalid, "margin_initial is missing")
            })?,
        )?;

        prepared.push(PreparedLeg {
            provenance: SizingCandidateProvenance {
                leg_id: candidate.leg_id.clone(),
                symbol: candidate.symbol.clone(),
                action: candidate.action,
                ratio: candidate.ratio,
                worst_entry_price: candidate.worst_entry_price,
                stop_loss_price: candidate.stop_loss_price,
                estimated_cost_per_lot: candidate.estimated_cost_per_lot,
            },
            ratio,
            entry,
            stop,
            loss_per_lot,
            notional_per_lot,
            margin_per_lot,
            volume_min: decimal("metadata.volume_min", metadata.volume_min)?,
            volume_max: decimal("metadata.volume_max", metadata.volume_max)?,
            volume_step: decimal("metadata.volume_step", metadata.volume_step)?,
        });
    }
    Ok(prepared)
}

struct ExpectedLeg {
    leg_id: LegId,
    symbol: SymbolCode,
    action: AdjustedRiskLegAction,
    ratio: f64,
    stop: Option<f64>,
}

fn validate_loss_capacity(request: &RiskRequest) -> EvaluationResult<()> {
    let daily = decimal(
        "daily_realized_loss_pct",
        request.capacity.daily_realized_loss_pct,
    )?;
    let drawdown = decimal("equity_drawdown_pct", request.capacity.equity_drawdown_pct)?;
    if daily >= decimal("max_daily_loss_pct", request.policy.max_daily_loss_pct)? {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "daily realized loss reached the configured limit",
        ));
    }
    if drawdown >= decimal("max_drawdown_pct", request.policy.max_drawdown_pct)? {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "equity drawdown reached the configured limit",
        ));
    }
    if request.capacity.remaining_account_risk_pct <= 0.0
        || request.capacity.remaining_portfolio_risk_pct <= 0.0
    {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "no account or portfolio risk capacity remains",
        ));
    }
    Ok(())
}

fn active_pending_commands<'a>(
    request: &'a RiskRequest,
) -> EvaluationResult<Vec<(&'a ExecutionCommand, &'a ExecutionCommandState)>> {
    let mut commands = BTreeMap::new();
    for command in &request.pending_commands {
        if commands
            .insert(command.command_id.clone(), command)
            .is_some()
        {
            return invalid(format!("duplicate command {}", command.command_id));
        }
    }
    let mut states = BTreeMap::new();
    for state in &request.pending_command_states {
        if states.insert(state.command_id.clone(), state).is_some() {
            return invalid(format!("duplicate command state {}", state.command_id));
        }
    }
    if commands.len() != states.len() {
        return invalid("pending commands and states must correspond one-to-one");
    }

    let mut active = Vec::new();
    for (command_id, command) in commands {
        let state = states.get(&command_id).ok_or_else(|| {
            Rejection::new(
                ErrorCode::RiskInputInvalid,
                format!("state is missing for pending command {command_id}"),
            )
        })?;
        if state.command_id != command.command_id
            || state.account_id != command.account_id
            || state.plan_id != command.plan_id
            || state.leg_id != command.leg_id
        {
            return invalid(format!("command state identity differs for {command_id}"));
        }
        validate_command_state_time(request, state)?;
        if command_state_is_terminal(state.status) {
            continue;
        }
        validate_active_command(command)?;
        if command.expires_at <= request.evaluated_at {
            return Err(Rejection::new(
                ErrorCode::PendingExposureConflict,
                format!("non-terminal command {command_id} is expired and unreconciled"),
            ));
        }
        active.push((command, *state));
    }
    Ok(active)
}

fn validate_pending_exposure(
    request: &RiskRequest,
    index: &SnapshotIndex<'_>,
    legs: &[PreparedLeg],
    active_commands: &[(&ExecutionCommand, &ExecutionCommandState)],
) -> EvaluationResult<()> {
    let new_symbols: BTreeSet<&SymbolCode> =
        legs.iter().map(|leg| &leg.provenance.symbol).collect();

    if let Some((command, _)) = active_commands
        .iter()
        .find(|(command, _)| command.action == ExecutionAction::Modify)
    {
        return Err(Rejection::new(
            ErrorCode::PendingExposureConflict,
            format!(
                "pending MODIFY command {} has no v1 risk-reduction proof",
                command.command_id
            ),
        ));
    }

    for order in &request.orders {
        if !order_is_active(order.status) {
            continue;
        }
        if matches!(order.status, OrderSnapshotStatus::Unknown) {
            return Err(Rejection::new(
                ErrorCode::PendingExposureConflict,
                format!(
                    "order {} has unknown broker status and exposure",
                    order.broker_order_id
                ),
            ));
        }
        validate_broker_symbol(
            "active order",
            order.broker_symbol.as_deref(),
            required_metadata(index, &order.symbol)?,
        )?;
        let remaining = validate_order(order)?;
        if remaining > Decimal::ZERO && new_symbols.contains(&order.symbol) {
            return Err(Rejection::new(
                ErrorCode::PendingExposureConflict,
                format!(
                    "open order {} already exposes {}",
                    order.broker_order_id, order.symbol
                ),
            ));
        }
    }
    for (command, _) in active_commands {
        if matches!(command.action, ExecutionAction::Buy | ExecutionAction::Sell) {
            validate_broker_symbol(
                "active command",
                command.broker_symbol.as_deref(),
                required_metadata(index, &command.symbol)?,
            )?;
            if new_symbols.contains(&command.symbol) {
                return Err(Rejection::new(
                    ErrorCode::PendingExposureConflict,
                    format!(
                        "pending command {} already exposes {}",
                        command.command_id, command.symbol
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_broker_symbol(
    label: &str,
    broker_symbol: Option<&str>,
    metadata: &SymbolMetadataSnapshot,
) -> EvaluationResult<()> {
    if broker_symbol.is_some_and(|symbol| symbol != metadata.broker_symbol) {
        return invalid(format!(
            "{label} broker_symbol does not match symbol metadata for {}",
            metadata.symbol
        ));
    }
    Ok(())
}

fn validate_concurrency(
    request: &RiskRequest,
    legs: &[PreparedLeg],
    active_commands: &[(&ExecutionCommand, &ExecutionCommandState)],
) -> EvaluationResult<()> {
    let active_orders = request
        .orders
        .iter()
        .filter(|order| order_is_active(order.status) && order.remaining_lots > 0.0)
        .count();
    let active_risk_commands = active_commands
        .iter()
        .filter(|(command, _)| {
            matches!(command.action, ExecutionAction::Buy | ExecutionAction::Sell)
        })
        .count();
    let prospective = request
        .positions
        .len()
        .checked_add(active_orders)
        .and_then(|value| value.checked_add(active_risk_commands))
        .and_then(|value| value.checked_add(legs.len()))
        .ok_or_else(arithmetic_overflow)?;
    if prospective > request.policy.max_concurrent_positions as usize {
        return Err(Rejection::new(
            ErrorCode::PositionLimitExceeded,
            "prospective account position count exceeds policy",
        ));
    }
    if legs.len() > request.capacity.remaining_strategy_legs as usize {
        return Err(Rejection::new(
            ErrorCode::PositionLimitExceeded,
            "new legs exceed the trusted assembler's remaining strategy capacity",
        ));
    }
    Ok(())
}

fn validate_command_state_time(
    request: &RiskRequest,
    state: &ExecutionCommandState,
) -> EvaluationResult<()> {
    if command_state_is_terminal(state.status) != state.completed_at.is_some() {
        return invalid(format!(
            "command state {} completed_at does not match terminal status",
            state.command_id
        ));
    }
    if state.created_at < 0
        || state.updated_at < state.created_at
        || state.updated_at > request.state_watermarks.pending_commands_reconciled_at
    {
        return Err(Rejection::new(
            ErrorCode::PendingExposureConflict,
            format!(
                "command state {} is not covered by reconciliation",
                state.command_id
            ),
        ));
    }
    for timestamp in [
        state.dispatched_at,
        state.command_received_at,
        state.reconciling_at,
        state.completed_at,
    ]
    .into_iter()
    .flatten()
    {
        if timestamp < state.created_at || timestamp > state.updated_at {
            return invalid(format!(
                "command state {} has an invalid lifecycle timestamp",
                state.command_id
            ));
        }
    }
    Ok(())
}

fn validate_active_command(command: &ExecutionCommand) -> EvaluationResult<()> {
    if command.command_id.as_str().trim().is_empty()
        || command.strategy_id.as_str().trim().is_empty()
        || command.symbol.as_str().trim().is_empty()
        || command.idempotency_key.as_str().trim().is_empty()
        || command.hmac.trim().is_empty()
    {
        return invalid("active command identity and HMAC fields must not be empty");
    }

    match command.action {
        ExecutionAction::Buy | ExecutionAction::Sell => {
            positive_optional_decimal("command.lots", command.lots)?;
            let order_type = command.order_type.ok_or_else(|| {
                Rejection::new(
                    ErrorCode::RiskInputInvalid,
                    "active BUY/SELL order_type is required",
                )
            })?;
            let price = command
                .price
                .map(|value| decimal("command.price", value))
                .transpose()?;
            if price.is_some_and(|value| value <= Decimal::ZERO)
                || (!matches!(order_type, OrderType::Market) && price.is_none())
            {
                return invalid("active pending-order price is missing or invalid");
            }
        }
        ExecutionAction::Close => {
            if command.position_ticket.is_none() {
                return invalid("active CLOSE command must identify its target position");
            }
            if let Some(lots) = command.lots {
                if decimal("command.lots", lots)? <= Decimal::ZERO {
                    return invalid("active CLOSE lots must be positive when present");
                }
            }
        }
        ExecutionAction::Modify => {
            if command.position_ticket.is_none() && command.broker_order_id.is_none() {
                return invalid("active MODIFY command must identify an order or position");
            }
            if command.sl.is_none()
                && command.tp.is_none()
                && command.price.is_none()
                && command.expiration_time.is_none()
            {
                return invalid("active MODIFY command has no modified field");
            }
        }
        ExecutionAction::Cancel => {
            if command.broker_order_id.is_none() {
                return invalid("active CANCEL command must identify a broker order");
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn size_and_cap(
    request: &RiskRequest,
    index: &SnapshotIndex<'_>,
    legs: &[PreparedLeg],
    active_commands: &[(&ExecutionCommand, &ExecutionCommandState)],
    risk_base: Decimal,
    ages: AuditAges,
    valid_until: i64,
) -> EvaluationResult<ActionableApproval> {
    let approved_pct = [
        decimal("proposed_risk_pct", request.intent.proposed_risk_pct)?,
        decimal(
            "max_risk_per_trade_pct",
            request.policy.max_risk_per_trade_pct,
        )?,
        decimal(
            "strategy.max_risk_per_trade_pct",
            request.strategy_policy.max_risk_per_trade_pct,
        )?,
        decimal(
            "remaining_account_risk_pct",
            request.capacity.remaining_account_risk_pct,
        )?,
        decimal(
            "remaining_portfolio_risk_pct",
            request.capacity.remaining_portfolio_risk_pct,
        )?,
    ]
    .into_iter()
    .min()
    .ok_or_else(|| Rejection::new(ErrorCode::RiskInputInvalid, "risk cap is missing"))?;
    if approved_pct <= Decimal::ZERO {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "approved risk percentage is not positive",
        ));
    }
    let risk_budget = risk_base
        .checked_mul(approved_pct)
        .and_then(|value| value.checked_div(HUNDRED))
        .ok_or_else(arithmetic_overflow)?;

    let mut weighted_loss = Decimal::ZERO;
    for leg in legs {
        weighted_loss = weighted_loss
            .checked_add(
                leg.ratio
                    .checked_mul(leg.loss_per_lot)
                    .ok_or_else(arithmetic_overflow)?,
            )
            .ok_or_else(arithmetic_overflow)?;
    }
    if weighted_loss <= Decimal::ZERO || risk_budget <= Decimal::ZERO {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "risk budget or bounded loss is not positive",
        ));
    }
    let mut scale = risk_budget
        .checked_div(weighted_loss)
        .ok_or_else(arithmetic_overflow)?;
    let mut limiter = ScaleLimiter::RiskBudget;

    for leg in legs {
        let maximum_scale = leg
            .volume_max
            .checked_div(leg.ratio)
            .ok_or_else(arithmetic_overflow)?;
        cap_scale(
            &mut scale,
            &mut limiter,
            maximum_scale,
            ScaleLimiter::Volume,
        );
    }

    let existing_exposure = existing_exposure(request, index, active_commands)?;
    let symbol_cap = risk_base
        .checked_mul(decimal(
            "max_symbol_exposure_pct",
            request.policy.max_symbol_exposure_pct,
        )?)
        .and_then(|value| value.checked_div(HUNDRED))
        .ok_or_else(arithmetic_overflow)?;
    let total_cap = risk_base
        .checked_mul(decimal(
            "max_total_exposure_pct",
            request.policy.max_total_exposure_pct,
        )?)
        .and_then(|value| value.checked_div(HUNDRED))
        .ok_or_else(arithmetic_overflow)?;

    let mut new_exposure_by_symbol: BTreeMap<SymbolCode, Decimal> = BTreeMap::new();
    for leg in legs {
        add_amount(
            &mut new_exposure_by_symbol,
            leg.provenance.symbol.clone(),
            leg.ratio
                .checked_mul(leg.notional_per_lot)
                .ok_or_else(arithmetic_overflow)?,
        )?;
    }
    for (symbol, coefficient) in &new_exposure_by_symbol {
        let existing = existing_exposure
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let available = symbol_cap
            .checked_sub(existing)
            .ok_or_else(arithmetic_overflow)?;
        if available <= Decimal::ZERO {
            return Err(Rejection::new(
                ErrorCode::ExposureLimitExceeded,
                format!("existing exposure consumes the symbol cap for {symbol}"),
            ));
        }
        let maximum_scale = available
            .checked_div(*coefficient)
            .ok_or_else(arithmetic_overflow)?;
        cap_scale(
            &mut scale,
            &mut limiter,
            maximum_scale,
            ScaleLimiter::Exposure,
        );
    }
    let existing_total = checked_sum(existing_exposure.values().copied())?;
    let new_total_coefficient = checked_sum(new_exposure_by_symbol.values().copied())?;
    let total_available = total_cap
        .checked_sub(existing_total)
        .ok_or_else(arithmetic_overflow)?;
    if total_available <= Decimal::ZERO {
        return Err(Rejection::new(
            ErrorCode::ExposureLimitExceeded,
            "existing exposure consumes the total exposure cap",
        ));
    }
    cap_scale(
        &mut scale,
        &mut limiter,
        total_available
            .checked_div(new_total_coefficient)
            .ok_or_else(arithmetic_overflow)?,
        ScaleLimiter::Exposure,
    );

    let pending_margin = pending_margin(request, index, active_commands)?;
    let free_margin = decimal("account.free_margin", request.account.free_margin)?;
    let account_margin = decimal("account.margin", request.account.margin)?;
    let margin_cap = risk_base
        .checked_mul(decimal(
            "max_margin_usage_pct",
            request.policy.max_margin_usage_pct,
        )?)
        .and_then(|value| value.checked_div(HUNDRED))
        .ok_or_else(arithmetic_overflow)?;
    let free_available = free_margin
        .checked_sub(pending_margin)
        .ok_or_else(arithmetic_overflow)?;
    let usage_available = margin_cap
        .checked_sub(account_margin)
        .and_then(|value| value.checked_sub(pending_margin))
        .ok_or_else(arithmetic_overflow)?;
    if free_available <= Decimal::ZERO || usage_available <= Decimal::ZERO {
        return Err(Rejection::new(
            ErrorCode::InsufficientMargin,
            "pending margin consumes available margin capacity",
        ));
    }
    let margin_coefficient = checked_sum(legs.iter().map(|leg| {
        leg.ratio
            .checked_mul(leg.margin_per_lot)
            .unwrap_or(Decimal::MAX)
    }))?;
    if margin_coefficient == Decimal::MAX || margin_coefficient <= Decimal::ZERO {
        return invalid("new margin coefficient is invalid");
    }
    let margin_scale = free_available
        .min(usage_available)
        .checked_div(margin_coefficient)
        .ok_or_else(arithmetic_overflow)?;
    cap_scale(&mut scale, &mut limiter, margin_scale, ScaleLimiter::Margin);

    if scale <= Decimal::ZERO {
        return Err(rejection_for_limiter(limiter));
    }

    let mut sized = Vec::with_capacity(legs.len());
    let mut actual_risk = Decimal::ZERO;
    let mut final_exposure = existing_exposure.clone();
    let mut new_margin = Decimal::ZERO;
    for leg in legs {
        let raw_lots = scale
            .checked_mul(leg.ratio)
            .ok_or_else(arithmetic_overflow)?;
        let lots = floor_to_step(raw_lots, leg.volume_step)?;
        let (lots, executable_lots) = normalize_executable_lots(lots, leg.volume_step)?;
        if lots < leg.volume_min {
            return Err(rejection_for_limiter(limiter));
        }
        if lots > leg.volume_max || floor_to_step(lots, leg.volume_step)? != lots {
            return Err(Rejection::new(
                ErrorCode::InvalidVolume,
                format!(
                    "final lots violate broker volume constraints for {}",
                    leg.provenance.leg_id
                ),
            ));
        }
        let risk_amount = lots
            .checked_mul(leg.loss_per_lot)
            .ok_or_else(arithmetic_overflow)?;
        actual_risk = actual_risk
            .checked_add(risk_amount)
            .ok_or_else(arithmetic_overflow)?;
        add_amount(
            &mut final_exposure,
            leg.provenance.symbol.clone(),
            lots.checked_mul(leg.notional_per_lot)
                .ok_or_else(arithmetic_overflow)?,
        )?;
        new_margin = new_margin
            .checked_add(
                lots.checked_mul(leg.margin_per_lot)
                    .ok_or_else(arithmetic_overflow)?,
            )
            .ok_or_else(arithmetic_overflow)?;
        sized.push((leg, lots, executable_lots, risk_amount));
    }
    if actual_risk > risk_budget {
        return Err(Rejection::new(
            ErrorCode::RiskLimitExceeded,
            "final step-normalized risk exceeds its budget",
        ));
    }
    if final_exposure.values().any(|value| *value > symbol_cap)
        || checked_sum(final_exposure.values().copied())? > total_cap
    {
        return Err(Rejection::new(
            ErrorCode::ExposureLimitExceeded,
            "final exposure exceeds a hard cap",
        ));
    }
    let incremental_margin = pending_margin
        .checked_add(new_margin)
        .ok_or_else(arithmetic_overflow)?;
    if incremental_margin > free_margin
        || account_margin
            .checked_add(incremental_margin)
            .ok_or_else(arithmetic_overflow)?
            > margin_cap
    {
        return Err(Rejection::new(
            ErrorCode::InsufficientMargin,
            "final lots exceed free-margin or usage capacity",
        ));
    }

    let actual_risk_pct = actual_risk
        .checked_div(risk_base)
        .and_then(|value| value.checked_mul(HUNDRED))
        .ok_or_else(arithmetic_overflow)?;
    let mut adjusted_legs = Vec::with_capacity(sized.len());
    let mut candidates = Vec::with_capacity(sized.len());
    for (leg, _lots, executable_lots, risk_amount) in sized {
        let risk_pct = risk_amount
            .checked_div(risk_base)
            .and_then(|value| value.checked_mul(HUNDRED))
            .ok_or_else(arithmetic_overflow)?;
        adjusted_legs.push(AdjustedRiskLeg {
            leg_id: leg.provenance.leg_id.clone(),
            symbol: leg.provenance.symbol.clone(),
            action: leg.provenance.action,
            lots: executable_lots,
            risk_amount: required_f64(risk_amount)?,
            risk_pct: required_f64(risk_pct)?,
            sizing_entry_price: required_f64(leg.entry)?,
            approved_sl: required_f64(leg.stop)?,
            loss_per_lot: required_f64(leg.loss_per_lot)?,
            reason: Some(ErrorCodeOrString::from("OK")),
        });
        candidates.push(leg.provenance.clone());
    }

    Ok(ActionableApproval {
        sizing_version: request.policy.position_sizing_version.clone(),
        risk_base,
        risk_budget,
        actual_risk_pct,
        candidates,
        legs: adjusted_legs,
        ages,
        valid_until,
    })
}

fn existing_exposure(
    request: &RiskRequest,
    index: &SnapshotIndex<'_>,
    active_commands: &[(&ExecutionCommand, &ExecutionCommandState)],
) -> EvaluationResult<BTreeMap<SymbolCode, Decimal>> {
    let mut exposure = BTreeMap::new();
    for position in &request.positions {
        let lots = validate_position(position)?;
        let metadata = required_metadata(index, &position.symbol)?;
        let market = required_market(index, &position.symbol)?;
        let per_lot = notional_per_lot(
            metadata,
            market,
            Some(decimal("position.open_price", position.open_price)?),
        )?;
        add_amount(
            &mut exposure,
            position.symbol.clone(),
            lots.checked_mul(per_lot).ok_or_else(arithmetic_overflow)?,
        )?;
    }
    for order in &request.orders {
        if !order_is_active(order.status) {
            continue;
        }
        let lots = validate_order(order)?;
        if lots == Decimal::ZERO {
            continue;
        }
        let metadata = required_metadata(index, &order.symbol)?;
        let market = required_market(index, &order.symbol)?;
        let price = order
            .price
            .map(|value| decimal("order.price", value))
            .transpose()?;
        let per_lot = notional_per_lot(metadata, market, price)?;
        add_amount(
            &mut exposure,
            order.symbol.clone(),
            lots.checked_mul(per_lot).ok_or_else(arithmetic_overflow)?,
        )?;
    }
    for (command, _) in active_commands {
        if !matches!(command.action, ExecutionAction::Buy | ExecutionAction::Sell) {
            continue;
        }
        let lots = positive_optional_decimal("command.lots", command.lots)?;
        let metadata = required_metadata(index, &command.symbol)?;
        let market = required_market(index, &command.symbol)?;
        let price = command
            .price
            .map(|value| decimal("command.price", value))
            .transpose()?;
        let per_lot = notional_per_lot(metadata, market, price)?;
        add_amount(
            &mut exposure,
            command.symbol.clone(),
            lots.checked_mul(per_lot).ok_or_else(arithmetic_overflow)?,
        )?;
    }
    Ok(exposure)
}

fn pending_margin(
    request: &RiskRequest,
    index: &SnapshotIndex<'_>,
    active_commands: &[(&ExecutionCommand, &ExecutionCommandState)],
) -> EvaluationResult<Decimal> {
    let mut total = Decimal::ZERO;
    for order in &request.orders {
        if !order_is_active(order.status) {
            continue;
        }
        let lots = validate_order(order)?;
        let metadata = required_metadata(index, &order.symbol)?;
        let margin = positive_optional_decimal("metadata.margin_initial", metadata.margin_initial)?;
        total = total
            .checked_add(lots.checked_mul(margin).ok_or_else(arithmetic_overflow)?)
            .ok_or_else(arithmetic_overflow)?;
    }
    for (command, _) in active_commands {
        if !matches!(command.action, ExecutionAction::Buy | ExecutionAction::Sell) {
            continue;
        }
        let lots = positive_optional_decimal("command.lots", command.lots)?;
        let metadata = required_metadata(index, &command.symbol)?;
        let margin = positive_optional_decimal("metadata.margin_initial", metadata.margin_initial)?;
        total = total
            .checked_add(lots.checked_mul(margin).ok_or_else(arithmetic_overflow)?)
            .ok_or_else(arithmetic_overflow)?;
    }
    Ok(total)
}

fn approval_valid_until(
    request: &RiskRequest,
    required_symbols: &BTreeSet<SymbolCode>,
    index: &SnapshotIndex<'_>,
) -> EvaluationResult<i64> {
    let mut valid_until = request.intent.signal_expires_at.min(time_boundary(
        request.evaluated_at,
        request.policy.max_approval_ttl_ms,
    )?);
    for (observed_at, max_age) in [
        (
            request.account.observed_at,
            request.policy.max_snapshot_age_ms,
        ),
        (
            request.state_watermarks.positions_observed_at,
            request.policy.max_snapshot_age_ms,
        ),
        (
            request.state_watermarks.orders_observed_at,
            request.policy.max_order_snapshot_age_ms,
        ),
        (
            request.state_watermarks.pending_commands_reconciled_at,
            request.policy.max_order_snapshot_age_ms,
        ),
        (
            request.capacity.observed_at,
            request.policy.max_capacity_age_ms,
        ),
    ] {
        valid_until = valid_until.min(time_boundary(observed_at, max_age)?);
    }
    for position in &request.positions {
        valid_until = valid_until.min(time_boundary(
            position.observed_at,
            request.policy.max_snapshot_age_ms,
        )?);
    }
    for order in &request.orders {
        valid_until = valid_until.min(time_boundary(
            order.observed_at,
            request.policy.max_order_snapshot_age_ms,
        )?);
    }
    for symbol in required_symbols {
        valid_until = valid_until.min(time_boundary(
            required_market(index, symbol)?.snapshot.observed_at,
            request.policy.max_market_snapshot_age_ms,
        )?);
        valid_until = valid_until.min(time_boundary(
            required_metadata(index, symbol)?.observed_at,
            request.policy.max_symbol_metadata_age_ms,
        )?);
    }
    if valid_until <= request.evaluated_at {
        return Err(Rejection::new(
            ErrorCode::RiskInputInvalid,
            "approval validity window is empty",
        ));
    }
    Ok(valid_until)
}

fn notional_per_lot(
    metadata: &SymbolMetadataSnapshot,
    market: &RiskMarketSnapshot,
    additional_price: Option<Decimal>,
) -> EvaluationResult<Decimal> {
    let mut price = decimal("market.bid", market.snapshot.bid)?
        .abs()
        .max(decimal("market.ask", market.snapshot.ask)?.abs());
    if let Some(additional) = additional_price {
        price = price.max(additional.abs());
    }
    let tick_size = decimal("metadata.tick_size", metadata.tick_size)?;
    let tick_value = decimal("metadata.tick_value_loss", metadata.tick_value_loss)?;
    price
        .checked_div(tick_size)
        .map(|ticks| ticks.ceil())
        .and_then(|ticks| ticks.checked_mul(tick_value))
        .filter(|value| *value > Decimal::ZERO)
        .ok_or_else(arithmetic_overflow)
}

fn validate_position(position: &PositionSnapshot) -> EvaluationResult<Decimal> {
    let lots = decimal("position.lots", position.lots)?;
    let open_price = decimal("position.open_price", position.open_price)?;
    decimal("position.floating_pnl", position.floating_pnl)?;
    if lots <= Decimal::ZERO || open_price <= Decimal::ZERO {
        return invalid(format!("invalid position {}", position.position_id));
    }
    if let Some(sl) = position.sl {
        decimal("position.sl", sl)?;
    }
    if let Some(tp) = position.tp {
        decimal("position.tp", tp)?;
    }
    Ok(lots)
}

fn validate_order(order: &OrderSnapshot) -> EvaluationResult<Decimal> {
    let requested = decimal("order.requested_lots", order.requested_lots)?;
    let filled = decimal("order.filled_lots", order.filled_lots)?;
    let remaining = decimal("order.remaining_lots", order.remaining_lots)?;
    let total = filled
        .checked_add(remaining)
        .ok_or_else(arithmetic_overflow)?;
    if requested <= Decimal::ZERO
        || filled < Decimal::ZERO
        || remaining <= Decimal::ZERO
        || filled > requested
        || remaining > requested
        || total != requested
    {
        return invalid(format!("invalid order lots for {}", order.broker_order_id));
    }
    match order.status {
        OrderSnapshotStatus::Placed if filled != Decimal::ZERO => {
            return invalid(format!(
                "PLACED order {} must not have filled lots",
                order.broker_order_id
            ));
        }
        OrderSnapshotStatus::PartiallyFilled if filled <= Decimal::ZERO => {
            return invalid(format!(
                "PARTIALLY_FILLED order {} must have filled lots",
                order.broker_order_id
            ));
        }
        _ => {}
    }
    let price = order
        .price
        .map(|value| decimal("order.price", value))
        .transpose()?;
    if price.is_some_and(|value| value <= Decimal::ZERO)
        || (!matches!(order.order_type, OrderType::Market) && price.is_none())
    {
        return invalid(format!(
            "order {} has an invalid or missing price",
            order.broker_order_id
        ));
    }
    Ok(remaining)
}

fn validate_trade_mode(
    action: AdjustedRiskLegAction,
    mode: SymbolTradeMode,
) -> EvaluationResult<()> {
    let allowed = match action {
        AdjustedRiskLegAction::Buy => {
            matches!(mode, SymbolTradeMode::Full | SymbolTradeMode::LongOnly)
        }
        AdjustedRiskLegAction::Sell => {
            matches!(mode, SymbolTradeMode::Full | SymbolTradeMode::ShortOnly)
        }
    };
    if allowed {
        Ok(())
    } else {
        Err(Rejection::new(
            ErrorCode::TradeModeDisabled,
            format!("trade mode {mode} does not permit {action}"),
        ))
    }
}

fn checked_age(
    now: i64,
    observed_at: i64,
    max_age: i64,
    code: ErrorCode,
    label: &str,
) -> EvaluationResult<i64> {
    let age = now
        .checked_sub(observed_at)
        .ok_or_else(|| Rejection::new(code, format!("{label} age arithmetic overflowed")))?;
    if age < 0 || age >= max_age {
        return Err(Rejection::new(
            code,
            format!("{label} is future-dated or stale"),
        ));
    }
    Ok(age)
}

fn time_boundary(observed_at: i64, max_age: i64) -> EvaluationResult<i64> {
    observed_at
        .checked_add(max_age)
        .ok_or_else(|| Rejection::new(ErrorCode::RiskInputInvalid, "freshness boundary overflow"))
}

fn decimal(name: &str, value: f64) -> EvaluationResult<Decimal> {
    if !value.is_finite() {
        return invalid(format!("{name} must be finite"));
    }
    value.to_string().parse::<Decimal>().map_err(|_| {
        Rejection::new(
            ErrorCode::RiskInputInvalid,
            format!("{name} is out of range"),
        )
    })
}

fn positive_optional_decimal(name: &str, value: Option<f64>) -> EvaluationResult<Decimal> {
    let value = value.ok_or_else(|| {
        Rejection::new(ErrorCode::RiskInputInvalid, format!("{name} is required"))
    })?;
    let value = decimal(name, value)?;
    if value <= Decimal::ZERO {
        return invalid(format!("{name} must be greater than zero"));
    }
    Ok(value)
}

fn validate_percentage(name: &str, value: f64, positive: bool) -> EvaluationResult<()> {
    let value = decimal(name, value)?;
    if (positive && value <= Decimal::ZERO)
        || (!positive && value < Decimal::ZERO)
        || value > HUNDRED
    {
        return invalid(format!(
            "{name} must be within the configured percentage-point range"
        ));
    }
    Ok(())
}

fn validate_non_negative_percentage(name: &str, value: f64) -> EvaluationResult<()> {
    validate_percentage(name, value, false)
}

fn floor_to_step(value: Decimal, step: Decimal) -> EvaluationResult<Decimal> {
    if value < Decimal::ZERO || step <= Decimal::ZERO {
        return invalid("volume and volume_step must be valid positive decimals");
    }
    value
        .checked_div(step)
        .map(|units| units.floor())
        .and_then(|units| units.checked_mul(step))
        .ok_or_else(arithmetic_overflow)
}

fn normalize_executable_lots(
    lots: Decimal,
    volume_step: Decimal,
) -> EvaluationResult<(Decimal, f64)> {
    let executable = required_f64(lots)?;
    let round_trip = decimal("executable lots", executable)?;
    if round_trip <= Decimal::ZERO
        || round_trip > lots
        || floor_to_step(round_trip, volume_step)? != round_trip
    {
        return Err(Rejection::new(
            ErrorCode::InvalidVolume,
            "f64 executable lots would round up or violate volume_step",
        ));
    }
    Ok((round_trip, executable))
}

fn cap_scale(
    scale: &mut Decimal,
    limiter: &mut ScaleLimiter,
    candidate: Decimal,
    candidate_limiter: ScaleLimiter,
) {
    if candidate < *scale {
        *scale = candidate;
        *limiter = candidate_limiter;
    }
}

fn rejection_for_limiter(limiter: ScaleLimiter) -> Rejection {
    match limiter {
        ScaleLimiter::RiskBudget | ScaleLimiter::Volume => Rejection::new(
            ErrorCode::InvalidVolume,
            "global scale floors at least one required leg below volume_min",
        ),
        ScaleLimiter::Exposure => Rejection::new(
            ErrorCode::ExposureLimitExceeded,
            "exposure cap floors at least one required leg below volume_min",
        ),
        ScaleLimiter::Margin => Rejection::new(
            ErrorCode::InsufficientMargin,
            "margin cap floors at least one required leg below volume_min",
        ),
    }
}

fn add_amount(
    amounts: &mut BTreeMap<SymbolCode, Decimal>,
    symbol: SymbolCode,
    amount: Decimal,
) -> EvaluationResult<()> {
    let current = amounts.get(&symbol).copied().unwrap_or(Decimal::ZERO);
    let total = current
        .checked_add(amount)
        .ok_or_else(arithmetic_overflow)?;
    amounts.insert(symbol, total);
    Ok(())
}

fn checked_sum(values: impl IntoIterator<Item = Decimal>) -> EvaluationResult<Decimal> {
    values.into_iter().try_fold(Decimal::ZERO, |total, value| {
        total.checked_add(value).ok_or_else(arithmetic_overflow)
    })
}

fn required_metadata<'a>(
    index: &'a SnapshotIndex<'a>,
    symbol: &SymbolCode,
) -> EvaluationResult<&'a SymbolMetadataSnapshot> {
    index.metadata.get(symbol).copied().ok_or_else(|| {
        Rejection::new(
            ErrorCode::SymbolMetadataStale,
            format!("metadata is missing for {symbol}"),
        )
    })
}

fn required_market<'a>(
    index: &'a SnapshotIndex<'a>,
    symbol: &SymbolCode,
) -> EvaluationResult<&'a RiskMarketSnapshot> {
    index.markets.get(symbol).copied().ok_or_else(|| {
        Rejection::new(
            ErrorCode::MarketSnapshotStale,
            format!("market is missing for {symbol}"),
        )
    })
}

fn order_is_active(status: OrderSnapshotStatus) -> bool {
    matches!(
        status,
        OrderSnapshotStatus::Placed
            | OrderSnapshotStatus::PartiallyFilled
            | OrderSnapshotStatus::Unknown
    )
}

fn command_state_is_terminal(status: ExecutionCommandStatus) -> bool {
    matches!(
        status,
        ExecutionCommandStatus::DeliveryFailed
            | ExecutionCommandStatus::Rejected
            | ExecutionCommandStatus::Filled
            | ExecutionCommandStatus::Failed
            | ExecutionCommandStatus::Expired
            | ExecutionCommandStatus::Cancelled
    )
}

fn same_float(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && left.to_bits() == right.to_bits()
}

fn same_optional_float(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => same_float(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn best_effort_age(now: i64, observed_at: i64) -> i64 {
    now.saturating_sub(observed_at).max(0)
}

/// Returns the deterministic digest that binds a RiskResult to its immutable request.
pub fn risk_request_hash(request: &RiskRequest) -> String {
    let bytes = serde_json::to_vec(request)
        .unwrap_or_else(|_| format!("NON_JSON_RISK_REQUEST:{request:?}").into_bytes());
    let mut hasher = Sha256::new();
    hasher.update(b"sinan-risk-request.v1\0");
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    hash_request_float_bits(&mut hasher, request);
    hex::encode(hasher.finalize())
}

fn hash_request_float_bits(hasher: &mut Sha256, request: &RiskRequest) {
    hash_f64(hasher, request.decision.confidence);
    hash_f64(hasher, request.decision.proposed_risk_pct);
    hash_optional_f64(hasher, request.decision.proposed_sl);
    hash_optional_f64(hasher, request.decision.proposed_tp);

    hash_f64(hasher, request.intent.confidence);
    hash_f64(hasher, request.intent.proposed_risk_pct);
    hash_optional_f64(hasher, request.intent.proposed_sl);
    hash_optional_f64(hasher, request.intent.proposed_tp);
    if let Some(legs) = &request.intent.proposed_legs {
        for leg in legs {
            hash_f64(hasher, leg.ratio);
            hash_optional_f64(hasher, leg.proposed_sl);
            hash_optional_f64(hasher, leg.proposed_tp);
        }
    }
    if let Some(review) = &request.agent_review {
        hash_f64(hasher, review.score_adjustment);
    }

    for value in [
        request.account.balance,
        request.account.equity,
        request.account.margin,
        request.account.free_margin,
    ] {
        hash_f64(hasher, value);
    }
    for position in &request.positions {
        hash_f64(hasher, position.lots);
        hash_f64(hasher, position.open_price);
        hash_optional_f64(hasher, position.sl);
        hash_optional_f64(hasher, position.tp);
        hash_f64(hasher, position.floating_pnl);
    }
    for order in &request.orders {
        hash_f64(hasher, order.requested_lots);
        hash_f64(hasher, order.filled_lots);
        hash_f64(hasher, order.remaining_lots);
        hash_optional_f64(hasher, order.price);
        hash_optional_f64(hasher, order.sl);
        hash_optional_f64(hasher, order.tp);
    }
    for metadata in &request.symbol_metadata {
        for value in [
            metadata.point,
            metadata.tick_size,
            metadata.tick_value_loss,
            metadata.contract_size,
            metadata.volume_min,
            metadata.volume_max,
            metadata.volume_step,
        ] {
            hash_f64(hasher, value);
        }
        hash_optional_f64(hasher, metadata.margin_initial);
        hash_optional_f64(hasher, metadata.margin_maintenance);
    }
    for command in &request.pending_commands {
        hash_optional_f64(hasher, command.lots);
        hash_optional_f64(hasher, command.price);
        hash_optional_f64(hasher, command.sl);
        hash_optional_f64(hasher, command.tp);
    }
    for value in [
        request.policy.max_risk_per_trade_pct,
        request.policy.max_daily_loss_pct,
        request.policy.max_drawdown_pct,
        request.policy.max_symbol_exposure_pct,
        request.policy.max_total_exposure_pct,
        request.policy.max_margin_usage_pct,
        request.strategy_policy.max_risk_per_trade_pct,
    ] {
        hash_f64(hasher, value);
    }
    for market in &request.markets {
        hash_f64(hasher, market.snapshot.bid);
        hash_f64(hasher, market.snapshot.ask);
        hash_f64(hasher, market.snapshot.spread);
    }
    for candidate in &request.sizing_candidates {
        hash_f64(hasher, candidate.ratio);
        hash_f64(hasher, candidate.worst_entry_price);
        hash_f64(hasher, candidate.stop_loss_price);
        hash_f64(hasher, candidate.estimated_cost_per_lot);
    }
    for value in [
        request.capacity.daily_realized_loss_pct,
        request.capacity.equity_drawdown_pct,
        request.capacity.remaining_account_risk_pct,
        request.capacity.remaining_portfolio_risk_pct,
    ] {
        hash_f64(hasher, value);
    }
}

fn hash_f64(hasher: &mut Sha256, value: f64) {
    hasher.update(value.to_bits().to_be_bytes());
}

fn hash_optional_f64(hasher: &mut Sha256, value: Option<f64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_f64(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|value| value.is_finite())
}

fn required_f64(value: Decimal) -> EvaluationResult<f64> {
    decimal_to_f64(value).ok_or_else(|| {
        Rejection::new(
            ErrorCode::RiskInputInvalid,
            "decimal result cannot be represented as a finite f64 audit value",
        )
    })
}

fn invalid<T>(message: impl Into<String>) -> EvaluationResult<T> {
    Err(Rejection::new(ErrorCode::RiskInputInvalid, message))
}

fn arithmetic_overflow() -> Rejection {
    Rejection::new(
        ErrorCode::RiskInputInvalid,
        "decimal or integer arithmetic overflowed",
    )
}

fn rejected_result(
    request: &RiskRequest,
    request_hash: String,
    ages: AuditAges,
    rejection: Rejection,
) -> RiskResult {
    let evaluated_at = request.evaluated_at.max(0);
    RiskResult {
        risk_id: request.risk_id.clone(),
        request_id: request.request_id.clone(),
        intent_id: request.intent.intent_id.clone(),
        account_id: request.intent.account_id.clone(),
        risk_request_hash: request_hash,
        approved: false,
        reason: rejection.code.into(),
        message: Some(rejection.message),
        sizing_version: None,
        risk_base_amount: None,
        risk_budget_amount: None,
        adjusted_risk_pct: None,
        sizing_candidates: None,
        adjusted_legs: None,
        decision_id: request.intent.decision_id.clone(),
        snapshot_age_ms: ages.snapshot,
        market_snapshot_age_ms: ages.market,
        symbol_metadata_age_ms: ages.metadata,
        capacity_age_ms: ages.capacity,
        evaluated_at,
        valid_until: evaluated_at,
    }
}

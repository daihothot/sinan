use crate::{
    AccountId, AdjustedRiskLegAction, BrokerDealId, BrokerOrderId, ClientId, CommandId,
    CorrelationId, DecisionId, ErrorCodeOrString, ExecutionAction, ExecutionCommandStatus,
    ExecutionEventStatus, ExecutionFailurePolicy, ExecutionId, ExecutionLegStatus,
    ExecutionPlanMode, ExecutionPlanStatus, FillingPolicy, IdempotencyKey, IntentId, LegId,
    OrderSnapshotStatus, OrderType, PlanId, PositionId, PositionSide, PositionTicket, RequestId,
    RiskId, RollbackMode, StrategyId, SymbolCode, SymbolTradeMode, TerminalId, TimePolicy,
    TimeframeCode, TradeIntentAction, TradeIntentLegAction,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketBar {
    pub symbol: SymbolCode,
    pub timeframe: TimeframeCode,
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub symbol: SymbolCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_symbol: Option<String>,
    pub bid: f64,
    pub ask: f64,
    pub spread: f64,
    pub observed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SymbolMetadataSnapshot {
    pub account_id: AccountId,
    pub symbol: SymbolCode,
    pub broker_symbol: String,
    pub digits: u32,
    pub point: f64,
    pub tick_size: f64,
    pub tick_value_loss: f64,
    pub contract_size: f64,
    pub volume_min: f64,
    pub volume_max: f64,
    pub volume_step: f64,
    pub stops_level_points: u32,
    pub freeze_level_points: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub margin_initial: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub margin_maintenance: Option<f64>,
    pub trade_mode: SymbolTradeMode,
    pub observed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub account_id: AccountId,
    pub balance: f64,
    pub equity: f64,
    pub margin: f64,
    pub free_margin: f64,
    pub currency: String,
    pub observed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub account_id: AccountId,
    pub symbol: SymbolCode,
    pub position_id: PositionId,
    pub side: PositionSide,
    pub lots: f64,
    pub open_price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp: Option<f64>,
    pub floating_pnl: f64,
    pub observed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub account_id: AccountId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,
    pub symbol: SymbolCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_symbol: Option<String>,
    pub broker_order_id: BrokerOrderId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_ticket: Option<PositionTicket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<CommandId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leg_id: Option<LegId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    pub side: PositionSide,
    pub order_type: OrderType,
    pub status: OrderSnapshotStatus,
    pub requested_lots: f64,
    pub filled_lots: f64,
    pub remaining_lots: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    pub observed_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentLeg {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: TradeIntentLegAction,
    pub ratio: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_tp: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeIntent {
    pub intent_id: IntentId,
    pub decision_id: DecisionId,
    pub strategy_id: StrategyId,
    pub correlation_id: CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub account_id: AccountId,
    pub symbol: SymbolCode,
    pub timeframe: TimeframeCode,
    pub action: TradeIntentAction,
    pub confidence: f64,
    pub reason: String,
    pub proposed_risk_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_tp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_legs: Option<Vec<TradeIntentLeg>>,
    pub signal_expires_at: i64,
    pub requested_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizingCandidateProvenance {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: AdjustedRiskLegAction,
    pub ratio: f64,
    pub worst_entry_price: f64,
    pub stop_loss_price: f64,
    pub estimated_cost_per_lot: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdjustedRiskLeg {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: AdjustedRiskLegAction,
    pub lots: f64,
    pub risk_amount: f64,
    pub risk_pct: f64,
    pub sizing_entry_price: f64,
    pub approved_sl: f64,
    pub loss_per_lot: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<ErrorCodeOrString>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskResult {
    pub risk_id: RiskId,
    pub request_id: RequestId,
    pub intent_id: IntentId,
    pub account_id: AccountId,
    pub risk_request_hash: String,
    pub approved: bool,
    pub reason: ErrorCodeOrString,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_base_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_budget_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjusted_risk_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sizing_candidates: Option<Vec<SizingCandidateProvenance>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjusted_legs: Option<Vec<AdjustedRiskLeg>>,
    pub decision_id: DecisionId,
    pub snapshot_age_ms: i64,
    pub market_snapshot_age_ms: i64,
    pub symbol_metadata_age_ms: i64,
    pub capacity_age_ms: i64,
    pub evaluated_at: i64,
    pub valid_until: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskResultValidationError {
    field: &'static str,
    reason: String,
}

impl RiskResultValidationError {
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

impl fmt::Display for RiskResultValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.field, self.reason)
    }
}

impl Error for RiskResultValidationError {}

impl RiskResult {
    pub fn validate(&self) -> Result<(), RiskResultValidationError> {
        validate_non_empty("risk_id", self.risk_id.as_str())?;
        validate_non_empty("request_id", self.request_id.as_str())?;
        validate_non_empty("intent_id", self.intent_id.as_str())?;
        validate_non_empty("account_id", self.account_id.as_str())?;
        validate_non_empty("decision_id", self.decision_id.as_str())?;
        validate_sha256("risk_request_hash", &self.risk_request_hash)?;

        if self.snapshot_age_ms < 0 {
            return Err(RiskResultValidationError::new(
                "snapshot_age_ms",
                "must be non-negative",
            ));
        }
        if self.symbol_metadata_age_ms < 0 {
            return Err(RiskResultValidationError::new(
                "symbol_metadata_age_ms",
                "must be non-negative",
            ));
        }
        if self.market_snapshot_age_ms < 0 {
            return Err(RiskResultValidationError::new(
                "market_snapshot_age_ms",
                "must be non-negative",
            ));
        }
        if self.capacity_age_ms < 0 {
            return Err(RiskResultValidationError::new(
                "capacity_age_ms",
                "must be non-negative",
            ));
        }
        if self.evaluated_at < 0 {
            return Err(RiskResultValidationError::new(
                "evaluated_at",
                "must be non-negative",
            ));
        }
        if self.valid_until < self.evaluated_at {
            return Err(RiskResultValidationError::new(
                "valid_until",
                "must not precede evaluated_at",
            ));
        }

        let sizing_fields_present = [
            self.sizing_version.is_some(),
            self.risk_base_amount.is_some(),
            self.risk_budget_amount.is_some(),
            self.adjusted_risk_pct.is_some(),
            self.sizing_candidates.is_some(),
            self.adjusted_legs.is_some(),
        ];
        let has_any_sizing = sizing_fields_present.iter().any(|present| *present);
        let has_all_sizing = sizing_fields_present.iter().all(|present| *present);

        if !self.approved {
            if self.reason.as_str() == "OK" {
                return Err(RiskResultValidationError::new(
                    "reason",
                    "must be an error code when approved is false",
                ));
            }
            if !matches!(&self.reason, ErrorCodeOrString::Known(_)) {
                return Err(RiskResultValidationError::new(
                    "reason",
                    "must be a centrally managed error code when approved is false",
                ));
            }
            if has_any_sizing {
                return Err(RiskResultValidationError::new(
                    "sizing",
                    "must be absent when approved is false",
                ));
            }
            return Ok(());
        }

        if self.reason.as_str() != "OK" {
            return Err(RiskResultValidationError::new(
                "reason",
                "must be OK when approved is true",
            ));
        }
        if self.valid_until == self.evaluated_at {
            return Err(RiskResultValidationError::new(
                "valid_until",
                "must follow evaluated_at when approved is true",
            ));
        }

        if !has_any_sizing {
            return Ok(());
        }
        if !has_all_sizing {
            return Err(RiskResultValidationError::new(
                "sizing",
                "must contain every sizing field or none of them",
            ));
        }

        let sizing_version = self.sizing_version.as_deref().expect("checked above");
        validate_non_empty("sizing_version", sizing_version)?;
        let risk_base = validate_positive_finite(
            "risk_base_amount",
            self.risk_base_amount.expect("checked above"),
        )?;
        let risk_budget = validate_positive_finite(
            "risk_budget_amount",
            self.risk_budget_amount.expect("checked above"),
        )?;
        let adjusted_risk_pct = validate_positive_finite(
            "adjusted_risk_pct",
            self.adjusted_risk_pct.expect("checked above"),
        )?;
        if risk_budget > risk_base {
            return Err(RiskResultValidationError::new(
                "risk_budget_amount",
                "must not exceed risk_base_amount",
            ));
        }
        if adjusted_risk_pct > 100.0 {
            return Err(RiskResultValidationError::new(
                "adjusted_risk_pct",
                "must not exceed 100",
            ));
        }

        let candidates = self.sizing_candidates.as_deref().expect("checked above");
        let adjusted_legs = self.adjusted_legs.as_deref().expect("checked above");
        validate_sizing_legs(
            candidates,
            adjusted_legs,
            risk_base,
            risk_budget,
            adjusted_risk_pct,
        )
    }
}

fn validate_sizing_legs(
    candidates: &[SizingCandidateProvenance],
    adjusted_legs: &[AdjustedRiskLeg],
    risk_base: f64,
    risk_budget: f64,
    adjusted_risk_pct: f64,
) -> Result<(), RiskResultValidationError> {
    if candidates.is_empty() {
        return Err(RiskResultValidationError::new(
            "sizing_candidates",
            "must not be empty for an actionable approval",
        ));
    }
    if adjusted_legs.is_empty() {
        return Err(RiskResultValidationError::new(
            "adjusted_legs",
            "must not be empty for an actionable approval",
        ));
    }
    if candidates.len() != adjusted_legs.len() {
        return Err(RiskResultValidationError::new(
            "adjusted_legs",
            "must correspond one-to-one with sizing_candidates",
        ));
    }

    let mut candidate_ids = HashSet::with_capacity(candidates.len());
    for candidate in candidates {
        validate_non_empty("sizing_candidates[].leg_id", candidate.leg_id.as_str())?;
        validate_non_empty("sizing_candidates[].symbol", candidate.symbol.as_str())?;
        if !candidate_ids.insert(candidate.leg_id.as_str()) {
            return Err(RiskResultValidationError::new(
                "sizing_candidates[].leg_id",
                format!("must be unique; duplicate {}", candidate.leg_id),
            ));
        }
        validate_positive_finite("sizing_candidates[].ratio", candidate.ratio)?;
        validate_positive_finite(
            "sizing_candidates[].worst_entry_price",
            candidate.worst_entry_price,
        )?;
        validate_positive_finite(
            "sizing_candidates[].stop_loss_price",
            candidate.stop_loss_price,
        )?;
        validate_finite(
            "sizing_candidates[].estimated_cost_per_lot",
            candidate.estimated_cost_per_lot,
        )?;
        let stop_is_valid = match candidate.action {
            AdjustedRiskLegAction::Buy => candidate.stop_loss_price < candidate.worst_entry_price,
            AdjustedRiskLegAction::Sell => candidate.stop_loss_price > candidate.worst_entry_price,
        };
        if !stop_is_valid {
            return Err(RiskResultValidationError::new(
                "sizing_candidates[].stop_loss_price",
                "must be below BUY entry or above SELL entry",
            ));
        }
    }

    let mut adjusted_ids = HashSet::with_capacity(adjusted_legs.len());
    let mut actual_risk = 0.0;
    for leg in adjusted_legs {
        validate_non_empty("adjusted_legs[].leg_id", leg.leg_id.as_str())?;
        validate_non_empty("adjusted_legs[].symbol", leg.symbol.as_str())?;
        if !adjusted_ids.insert(leg.leg_id.as_str()) {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].leg_id",
                format!("must be unique; duplicate {}", leg.leg_id),
            ));
        }
        validate_positive_finite("adjusted_legs[].lots", leg.lots)?;
        validate_positive_finite("adjusted_legs[].risk_amount", leg.risk_amount)?;
        validate_positive_finite("adjusted_legs[].risk_pct", leg.risk_pct)?;
        validate_positive_finite("adjusted_legs[].sizing_entry_price", leg.sizing_entry_price)?;
        validate_positive_finite("adjusted_legs[].approved_sl", leg.approved_sl)?;
        validate_positive_finite("adjusted_legs[].loss_per_lot", leg.loss_per_lot)?;
        if leg.risk_pct > 100.0 {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].risk_pct",
                "must not exceed 100",
            ));
        }
        if leg
            .reason
            .as_ref()
            .is_some_and(|reason| reason.as_str() != "OK")
        {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].reason",
                "must be absent or OK for an approved leg",
            ));
        }

        let candidate = candidates
            .iter()
            .find(|candidate| candidate.leg_id == leg.leg_id)
            .ok_or_else(|| {
                RiskResultValidationError::new(
                    "adjusted_legs[].leg_id",
                    format!("has no sizing candidate for {}", leg.leg_id),
                )
            })?;
        if candidate.symbol != leg.symbol || candidate.action != leg.action {
            return Err(RiskResultValidationError::new(
                "adjusted_legs",
                format!(
                    "identity for {} must match its sizing candidate",
                    leg.leg_id
                ),
            ));
        }
        if !same_f64_bits(leg.sizing_entry_price, candidate.worst_entry_price)
            || !same_f64_bits(leg.approved_sl, candidate.stop_loss_price)
        {
            return Err(RiskResultValidationError::new(
                "adjusted_legs",
                format!(
                    "entry and stop for {} must match its sizing candidate",
                    leg.leg_id
                ),
            ));
        }

        let expected_risk_amount = leg.lots * leg.loss_per_lot;
        if !approximately_equal(leg.risk_amount, expected_risk_amount) {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].risk_amount",
                "must equal lots * loss_per_lot",
            ));
        }

        let expected_leg_pct = leg.risk_amount / risk_base * 100.0;
        if !approximately_equal(leg.risk_pct, expected_leg_pct) {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].risk_pct",
                "must equal risk_amount / risk_base_amount * 100",
            ));
        }
        actual_risk += leg.risk_amount;
        if !actual_risk.is_finite() {
            return Err(RiskResultValidationError::new(
                "adjusted_legs[].risk_amount",
                "sum must remain finite",
            ));
        }
    }

    if actual_risk > risk_budget && !approximately_equal(actual_risk, risk_budget) {
        return Err(RiskResultValidationError::new(
            "adjusted_legs[].risk_amount",
            "sum must not exceed risk_budget_amount",
        ));
    }
    let expected_adjusted_pct = actual_risk / risk_base * 100.0;
    if !approximately_equal(adjusted_risk_pct, expected_adjusted_pct) {
        return Err(RiskResultValidationError::new(
            "adjusted_risk_pct",
            "must equal total adjusted leg risk / risk_base_amount * 100",
        ));
    }

    Ok(())
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), RiskResultValidationError> {
    if value.trim().is_empty() {
        Err(RiskResultValidationError::new(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), RiskResultValidationError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(RiskResultValidationError::new(
            field,
            "must be a 64-character lowercase SHA-256 hex digest",
        ))
    }
}

fn validate_finite(field: &'static str, value: f64) -> Result<f64, RiskResultValidationError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(RiskResultValidationError::new(field, "must be finite"))
    }
}

fn validate_positive_finite(
    field: &'static str,
    value: f64,
) -> Result<f64, RiskResultValidationError> {
    validate_finite(field, value)?;
    if value > 0.0 {
        Ok(value)
    } else {
        Err(RiskResultValidationError::new(
            field,
            "must be greater than zero",
        ))
    }
}

fn approximately_equal(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    if left == right {
        return true;
    }
    if left.is_sign_negative() != right.is_sign_negative() {
        return false;
    }
    left.to_bits().abs_diff(right.to_bits()) <= 128
}

fn same_f64_bits(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && left.to_bits() == right.to_bits()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionCommand {
    pub command_id: CommandId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leg_id: Option<LegId>,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,
    pub symbol: SymbolCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_symbol: Option<String>,
    pub action: ExecutionAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_type: Option<OrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lots: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deviation_points: Option<i64>,
    pub magic: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_ticket: Option<PositionTicket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_order_id: Option<BrokerOrderId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filling_policy: Option<FillingPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_policy: Option<TimePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_time: Option<i64>,
    pub expires_at: i64,
    pub idempotency_key: IdempotencyKey,
    pub hmac: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackPolicy {
    pub mode: RollbackMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retry_attempts: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub mode: ExecutionPlanMode,
    pub failure_policy: ExecutionFailurePolicy,
    pub timeout_ms: i64,
    pub max_command_ttl_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback_policy: Option<RollbackPolicy>,
}

/// Immutable fields of a plan leg. Runtime status is projected separately.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLegDefinition {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: ExecutionAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lots: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tp: Option<f64>,
    pub ratio: f64,
    pub dependency: Vec<LegId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLegState {
    pub status: ExecutionLegStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionLeg {
    #[serde(flatten)]
    pub definition: ExecutionLegDefinition,
    #[serde(flatten)]
    pub state: ExecutionLegState,
}

/// Immutable fields shared by every projection of an execution plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanDefinition {
    pub plan_id: PlanId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub mode: ExecutionPlanMode,
    pub failure_policy: ExecutionFailurePolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback_policy: Option<RollbackPolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanState {
    pub status: ExecutionPlanStatus,
    pub filled_legs: Vec<LegId>,
    pub failed_legs: Vec<LegId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    #[serde(flatten)]
    pub definition: ExecutionPlanDefinition,
    pub legs: Vec<ExecutionLeg>,
    #[serde(flatten)]
    pub state: ExecutionPlanState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPlanValidationError {
    field: &'static str,
    reason: String,
}

impl ExecutionPlanValidationError {
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

impl fmt::Display for ExecutionPlanValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.field, self.reason)
    }
}

impl Error for ExecutionPlanValidationError {}

impl ExecutionLeg {
    pub fn validate(&self) -> Result<(), ExecutionPlanValidationError> {
        validate_execution_non_empty("legs[].leg_id", self.definition.leg_id.as_str())?;
        validate_execution_non_empty("legs[].symbol", self.definition.symbol.as_str())?;
        validate_execution_positive("legs[].ratio", self.definition.ratio)?;
        for (field, value) in [
            ("legs[].lots", self.definition.lots),
            ("legs[].sl", self.definition.sl),
            ("legs[].tp", self.definition.tp),
        ] {
            if let Some(value) = value {
                validate_execution_positive(field, value)?;
            }
        }
        if matches!(
            self.definition.action,
            ExecutionAction::Buy | ExecutionAction::Sell
        ) && self.definition.lots.is_none()
        {
            return Err(ExecutionPlanValidationError::new(
                "legs[].lots",
                "is required for BUY/SELL",
            ));
        }
        Ok(())
    }
}

impl ExecutionPlan {
    pub fn validate(&self) -> Result<(), ExecutionPlanValidationError> {
        validate_execution_non_empty("plan_id", self.definition.plan_id.as_str())?;
        validate_execution_non_empty("account_id", self.definition.account_id.as_str())?;
        validate_execution_non_empty("strategy_id", self.definition.strategy_id.as_str())?;
        if self.legs.is_empty() {
            return Err(ExecutionPlanValidationError::new(
                "legs",
                "must not be empty",
            ));
        }
        let mut ids = HashSet::with_capacity(self.legs.len());
        for leg in &self.legs {
            leg.validate()?;
            if !ids.insert(leg.definition.leg_id.clone()) {
                return Err(ExecutionPlanValidationError::new(
                    "legs[].leg_id",
                    format!("must be unique; duplicate {}", leg.definition.leg_id),
                ));
            }
        }
        if self.definition.mode == ExecutionPlanMode::Simultaneous
            && self
                .legs
                .iter()
                .any(|leg| !leg.definition.dependency.is_empty())
        {
            return Err(ExecutionPlanValidationError::new(
                "legs[].dependency",
                "must be empty for simultaneous plans",
            ));
        }
        validate_execution_dependencies(&self.legs, &ids)?;
        validate_execution_summary(
            "filled_legs",
            &self.state.filled_legs,
            &self.legs,
            |status| {
                matches!(
                    status,
                    ExecutionLegStatus::PartiallyFilled | ExecutionLegStatus::Filled
                )
            },
        )?;
        validate_execution_summary(
            "failed_legs",
            &self.state.failed_legs,
            &self.legs,
            |status| {
                matches!(
                    status,
                    ExecutionLegStatus::Rejected | ExecutionLegStatus::Failed
                )
            },
        )?;
        Ok(())
    }
}

fn validate_execution_dependencies(
    legs: &[ExecutionLeg],
    ids: &HashSet<LegId>,
) -> Result<(), ExecutionPlanValidationError> {
    let mut indegree: HashMap<&LegId, usize> = ids.iter().map(|id| (id, 0)).collect();
    let mut outgoing: HashMap<&LegId, Vec<&LegId>> = HashMap::new();
    for leg in legs {
        let mut dependencies = HashSet::new();
        for dependency in &leg.definition.dependency {
            if dependency == &leg.definition.leg_id
                || !ids.contains(dependency)
                || !dependencies.insert(dependency)
            {
                return Err(ExecutionPlanValidationError::new(
                    "legs[].dependency",
                    "must be unique, known, and not self-referential",
                ));
            }
            *indegree
                .get_mut(&leg.definition.leg_id)
                .expect("leg id was indexed") += 1;
            outgoing
                .entry(dependency)
                .or_default()
                .push(&leg.definition.leg_id);
        }
    }
    let mut queue: VecDeque<&LegId> = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect();
    let mut visited = 0;
    while let Some(id) = queue.pop_front() {
        visited += 1;
        for target in outgoing.get(id).into_iter().flatten() {
            let count = indegree
                .get_mut(target)
                .expect("dependency target was indexed");
            *count -= 1;
            if *count == 0 {
                queue.push_back(target);
            }
        }
    }
    if visited != ids.len() {
        return Err(ExecutionPlanValidationError::new(
            "legs[].dependency",
            "must form an acyclic graph",
        ));
    }
    Ok(())
}

fn validate_execution_summary(
    field: &'static str,
    summary: &[LegId],
    legs: &[ExecutionLeg],
    expected: fn(ExecutionLegStatus) -> bool,
) -> Result<(), ExecutionPlanValidationError> {
    let derived: Vec<&LegId> = legs
        .iter()
        .filter(|leg| expected(leg.state.status))
        .map(|leg| &leg.definition.leg_id)
        .collect();
    if summary.len() != derived.len()
        || summary
            .iter()
            .zip(derived)
            .any(|(actual, expected)| actual != expected)
    {
        return Err(ExecutionPlanValidationError::new(
            field,
            "must exactly match projected legs in plan order",
        ));
    }
    Ok(())
}

fn validate_execution_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), ExecutionPlanValidationError> {
    if value.trim().is_empty() {
        Err(ExecutionPlanValidationError::new(
            field,
            "must not be empty",
        ))
    } else {
        Ok(())
    }
}

fn validate_execution_positive(
    field: &'static str,
    value: f64,
) -> Result<(), ExecutionPlanValidationError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ExecutionPlanValidationError::new(
            field,
            "must be positive and finite",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionCommandState {
    pub command_id: CommandId,
    pub account_id: AccountId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leg_id: Option<LegId>,
    pub status: ExecutionCommandStatus,
    pub delivery_attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delivery_error: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_received_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciling_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub execution_id: ExecutionId,
    pub command_id: CommandId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leg_id: Option<LegId>,
    pub account_id: AccountId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,
    pub symbol: SymbolCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_symbol: Option<String>,
    pub status: ExecutionEventStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_order_id: Option<BrokerOrderId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_deal_id: Option<BrokerDealId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_ticket: Option<PositionTicket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_lots: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_lots: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_lots: Option<f64>,
    pub event_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_filled_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<ErrorCodeOrString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

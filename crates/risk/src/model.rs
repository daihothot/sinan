use serde::{Deserialize, Serialize};
use sinan_types::{
    AccountId, AccountSnapshot, DecisionId, ExecutionCommand, ExecutionCommandState, LegId,
    MarketSnapshot, OrderSnapshot, PositionSnapshot, RequestId, RiskCapacity, RiskId, StrategyId,
    SymbolCode, SymbolMetadataSnapshot, TimeframeCode, TradeIntent, TradeIntentAction,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentReviewRecommendation {
    None,
    Skip,
    ReduceRisk,
    ManualReview,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentReview {
    pub review_id: String,
    pub score_adjustment: f64,
    pub recommendation: AgentReviewRecommendation,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub decision_id: DecisionId,
    pub strategy_id: StrategyId,
    pub symbol: SymbolCode,
    pub timeframe: TimeframeCode,
    pub action: TradeIntentAction,
    pub confidence: f64,
    pub reason: String,
    pub proposed_risk_pct: f64,
    pub proposed_sl: Option<f64>,
    pub proposed_tp: Option<f64>,
    pub timestamp: i64,
    pub signal_expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionSizingCandidate {
    pub leg_id: LegId,
    pub symbol: SymbolCode,
    pub action: sinan_types::AdjustedRiskLegAction,
    pub ratio: f64,
    pub worst_entry_price: f64,
    pub stop_loss_price: f64,
    pub estimated_cost_per_lot: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskPolicy {
    pub position_sizing_version: String,
    pub max_risk_per_trade_pct: f64,
    pub max_daily_loss_pct: f64,
    pub max_drawdown_pct: f64,
    pub max_symbol_exposure_pct: f64,
    pub max_total_exposure_pct: f64,
    pub max_margin_usage_pct: f64,
    pub require_stop_loss: bool,
    pub reject_expired_signal: bool,
    pub max_approval_ttl_ms: i64,
    pub max_snapshot_age_ms: i64,
    pub max_order_snapshot_age_ms: i64,
    pub max_market_snapshot_age_ms: i64,
    pub max_symbol_metadata_age_ms: i64,
    pub max_capacity_age_ms: i64,
    pub max_concurrent_positions: u32,
    pub require_valid_symbol_metadata: bool,
    pub reject_trade_mode_disabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskPolicy {
    pub max_risk_per_trade_pct: f64,
    pub max_concurrent_legs: u32,
    pub require_stop_loss: bool,
    pub signal_expiry_bars: u32,
}

/// Account-level watermarks proving that empty position/order vectors are fresh full snapshots.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskStateWatermarks {
    pub positions_observed_at: i64,
    pub orders_observed_at: i64,
    pub pending_commands_reconciled_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskMarketSnapshot {
    pub account_id: AccountId,
    pub snapshot: MarketSnapshot,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RiskRequest {
    pub request_id: RequestId,
    pub risk_id: RiskId,
    pub evaluated_at: i64,
    pub decision: StrategyDecision,
    pub intent: TradeIntent,
    pub agent_review: Option<AgentReview>,
    pub account: AccountSnapshot,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub symbol_metadata: Vec<SymbolMetadataSnapshot>,
    pub pending_commands: Vec<ExecutionCommand>,
    pub pending_command_states: Vec<ExecutionCommandState>,
    pub policy: RiskPolicy,
    pub strategy_policy: StrategyRiskPolicy,
    pub markets: Vec<RiskMarketSnapshot>,
    pub sizing_candidates: Vec<PositionSizingCandidate>,
    pub state_watermarks: RiskStateWatermarks,
    pub capacity: RiskCapacity,
}

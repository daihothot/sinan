use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sinan_types::{
    AccountId, AccountSnapshot, ClientId, ClockSyncStatus, CommandId, CorrelationId, DecisionId,
    ErrorCodeOrString, ExecutionCommand, ExecutionCommandState, ExecutionEvent, ExecutionPlan,
    IdempotencyKey, IntentId, LegId, OrderSnapshot, PlanId, PositionSnapshot, RiskId, RiskResult,
    SessionId, StrategyId, SymbolCode, SymbolMetadataSnapshot, TerminalId, TimeframeCode,
    TradeIntent, TradeIntentAction, TradeIntentLeg, TradeIntentLegAction, TradeIntentStatus,
};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SubmitTradeIntentRequest {
    pub intent: TradeIntent,
}

impl<'de> Deserialize<'de> for SubmitTradeIntentRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        StrictSubmitTradeIntentRequest::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictSubmitTradeIntentRequest {
    intent: StrictTradeIntent,
}

impl From<StrictSubmitTradeIntentRequest> for SubmitTradeIntentRequest {
    fn from(value: StrictSubmitTradeIntentRequest) -> Self {
        Self {
            intent: value.intent.into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTradeIntent {
    intent_id: IntentId,
    decision_id: DecisionId,
    strategy_id: StrategyId,
    correlation_id: CorrelationId,
    idempotency_key: IdempotencyKey,
    account_id: AccountId,
    symbol: SymbolCode,
    timeframe: TimeframeCode,
    action: TradeIntentAction,
    confidence: f64,
    reason: String,
    proposed_risk_pct: f64,
    proposed_sl: Option<f64>,
    proposed_tp: Option<f64>,
    proposed_legs: Option<Vec<StrictTradeIntentLeg>>,
    signal_expires_at: i64,
    requested_at: i64,
}

impl From<StrictTradeIntent> for TradeIntent {
    fn from(value: StrictTradeIntent) -> Self {
        Self {
            intent_id: value.intent_id,
            decision_id: value.decision_id,
            strategy_id: value.strategy_id,
            correlation_id: value.correlation_id,
            idempotency_key: value.idempotency_key,
            account_id: value.account_id,
            symbol: value.symbol,
            timeframe: value.timeframe,
            action: value.action,
            confidence: value.confidence,
            reason: value.reason,
            proposed_risk_pct: value.proposed_risk_pct,
            proposed_sl: value.proposed_sl,
            proposed_tp: value.proposed_tp,
            proposed_legs: value
                .proposed_legs
                .map(|legs| legs.into_iter().map(Into::into).collect()),
            signal_expires_at: value.signal_expires_at,
            requested_at: value.requested_at,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTradeIntentLeg {
    leg_id: LegId,
    symbol: SymbolCode,
    action: TradeIntentLegAction,
    ratio: f64,
    proposed_sl: Option<f64>,
    proposed_tp: Option<f64>,
}

impl From<StrictTradeIntentLeg> for TradeIntentLeg {
    fn from(value: StrictTradeIntentLeg) -> Self {
        Self {
            leg_id: value.leg_id,
            symbol: value.symbol,
            action: value.action,
            ratio: value.ratio,
            proposed_sl: value.proposed_sl,
            proposed_tp: value.proposed_tp,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SubmitTradeIntentStatus {
    /// The intent is durable. No risk result, plan, command, or broker fill is implied.
    Accepted,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeIntentStateRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_id: Option<RiskId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SubmitTradeIntentResponse {
    pub intent_id: IntentId,
    pub status: SubmitTradeIntentStatus,
    pub reason: ErrorCodeOrString,
    pub correlation_id: CorrelationId,
    pub accepted_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_ref: Option<TradeIntentStateRef>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ClockHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradingCoreTimeResponse {
    pub server_now_ms: i64,
    pub server_receive_at: i64,
    pub server_send_at: i64,
    pub clock_health: ClockHealth,
    pub max_internal_server_skew_ms: i64,
    pub max_decision_time_skew_ms: i64,
    pub max_decision_time_sync_age_ms: i64,
    pub max_decision_time_sync_rtt_ms: i64,
    pub control_plane_time_sync_interval_ms: i64,
    pub max_decision_intent_age_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradingCoreTimePolicy {
    pub clock_health: ClockHealth,
    pub max_internal_server_skew_ms: i64,
    pub max_decision_time_skew_ms: i64,
    pub max_decision_time_sync_age_ms: i64,
    pub max_decision_time_sync_rtt_ms: i64,
    pub control_plane_time_sync_interval_ms: i64,
    pub max_decision_intent_age_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SessionSummaryStatus {
    Active,
    Stale,
    Disconnected,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionSummary {
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,
    pub platform: String,
    pub status: SessionSummaryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock_sync_status: Option<ClockSyncStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CircuitBreakerStatus {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CircuitBreakerReason {
    Ok,
    DailyRealizedLossLimit,
    EquityDrawdownLimit,
    ConsecutiveBrokerRejections,
    ConsecutiveCommandFailures,
    ManualReconciliationRequired,
    StoreRecoveryReconciliationPending,
    TimeSyncUnhealthy,
    SnapshotStale,
    SymbolMetadataStale,
    ManualTrigger,
    HardRiskViolationDuringRecovery,
    SafetyInvariantViolation,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CircuitBreakerSummary {
    pub status: CircuitBreakerStatus,
    pub reason: CircuitBreakerReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggered_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_by: Option<String>,
    pub blocked_intent_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExecutionStateSummary {
    pub open_plans: Vec<ExecutionPlan>,
    pub pending_commands: Vec<ExecutionCommandState>,
    pub recent_events: Vec<ExecutionEvent>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RiskStateSummary {
    pub latest_results: Vec<RiskResult>,
    pub circuit_breaker_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerSummary>,
}

impl RiskStateSummary {
    pub(crate) fn normalize_circuit_breaker_active(&mut self) {
        if let Some(summary) = &self.circuit_breaker {
            self.circuit_breaker_active = summary.status != CircuitBreakerStatus::Closed;
        }
    }
}

/// Account-scoped aggregate assembled by the query port from one read snapshot.
///
/// The query port owns positive collection limits and deterministic ordering.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TradingCoreStateResponse {
    pub server_time: i64,
    pub clock_health: ClockHealth,
    pub accounts: Vec<AccountSnapshot>,
    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub symbols: Vec<SymbolMetadataSnapshot>,
    pub sessions: Vec<SessionSummary>,
    pub execution: ExecutionStateSummary,
    pub risk: RiskStateSummary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeIntentStatusResponse {
    pub intent_id: IntentId,
    pub status: TradeIntentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<ErrorCodeOrString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_id: Option<RiskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    pub command_ids: Vec<CommandId>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExecutionCommandStatusResponse {
    pub command_id: CommandId,
    pub state: ExecutionCommandState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<ExecutionCommand>,
    pub events: Vec<ExecutionEvent>,
}

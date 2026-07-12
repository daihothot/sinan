use crate::{
    AccountId, BrokerDealId, BrokerOrderId, ClientId, CommandId, CorrelationId, DecisionId,
    ErrorCodeOrString, ExecutionAction, ExecutionCommandStatus, ExecutionEventStatus, ExecutionId,
    FillingPolicy, IdempotencyKey, IntentId, LegId, OrderSnapshotStatus, OrderType, PlanId,
    PositionId, PositionSide, PositionTicket, StrategyId, SymbolCode, SymbolTradeMode, TerminalId,
    TimePolicy, TimeframeCode, TradeIntentAction, TradeIntentLegAction,
};
use serde::{Deserialize, Serialize};

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

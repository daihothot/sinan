use sinan_types::{
    AccountId, CausationId, ClientId, ClockSyncStatus, CommandId, CorrelationId, ExecutionCommand,
    ExecutionCommandState, ExecutionCommandStatus, ExecutionEvent, IdempotencyKey, IntentId, LegId,
    MessageId, PlanId, RiskId, SessionId, SessionStatus, StrategyId, TerminalId, TradeIntent,
    TradeIntentStatus, WireInboxStatus, WireOutboxStatus,
};

use crate::json::CanonicalJson;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOutcome<T> {
    Inserted(T),
    Duplicate(T),
}

impl<T> WriteOutcome<T> {
    pub fn record(&self) -> &T {
        match self {
            Self::Inserted(record) | Self::Duplicate(record) => record,
        }
    }

    pub fn into_record(self) -> T {
        match self {
            Self::Inserted(record) | Self::Duplicate(record) => record,
        }
    }

    pub const fn was_inserted(&self) -> bool {
        matches!(self, Self::Inserted(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreEventMetadata {
    pub event_id: String,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub message_id: Option<MessageId>,
    pub schema_version: String,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub account_id: Option<AccountId>,
    pub client_id: Option<ClientId>,
    pub terminal_id: Option<TerminalId>,
    pub strategy_id: Option<StrategyId>,
    pub intent_id: Option<IntentId>,
    pub plan_id: Option<PlanId>,
    pub leg_id: Option<LegId>,
    pub command_id: Option<CommandId>,
    pub idempotency_key: Option<IdempotencyKey>,
    pub event_at: i64,
    pub received_at: i64,
    pub created_at: i64,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewCoreEvent {
    pub metadata: CoreEventMetadata,
    pub payload: CanonicalJson,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCoreEvent {
    pub metadata: CoreEventMetadata,
    pub payload: CanonicalJson,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewTradeIntent {
    pub intent: TradeIntent,
    pub initial_status: TradeIntentStatus,
    pub recorded_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredTradeIntent {
    pub intent: TradeIntent,
    pub status: TradeIntentStatus,
    pub payload: CanonicalJson,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutionCommand {
    pub command: ExecutionCommand,
    pub risk_id: RiskId,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionCommand {
    pub command: ExecutionCommand,
    pub risk_id: RiskId,
    pub payload: CanonicalJson,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandStateUpdate {
    pub expected_status: ExecutionCommandStatus,
    pub expected_updated_at: i64,
    pub state: ExecutionCommandState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutionEvent {
    pub event: ExecutionEvent,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionEvent {
    pub event: ExecutionEvent,
    pub payload: CanonicalJson,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewWireInbox {
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub message_type: String,
    pub sequence: Option<u64>,
    pub received_at: i64,
    pub handled_at: Option<i64>,
    pub status: WireInboxStatus,
    /// Canonical form of the complete wire envelope. Only its digest is stored.
    pub wire_message: CanonicalJson,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWireInbox {
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub message_type: String,
    pub sequence: Option<u64>,
    pub received_at: i64,
    pub handled_at: Option<i64>,
    pub status: WireInboxStatus,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewWireOutbox {
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub message_type: String,
    pub sequence: Option<u64>,
    pub command_id: Option<CommandId>,
    pub payload: CanonicalJson,
    pub status: WireOutboxStatus,
    pub created_at: i64,
    pub sent_at: Option<i64>,
    pub acked_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWireOutbox {
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub message_type: String,
    pub sequence: Option<u64>,
    pub command_id: Option<CommandId>,
    pub payload: CanonicalJson,
    pub status: WireOutboxStatus,
    pub created_at: i64,
    pub sent_at: Option<i64>,
    pub acked_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewSessionRecord {
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub platform: String,
    pub status: SessionStatus,
    pub capabilities: CanonicalJson,
    pub remote_addr: Option<String>,
    pub connected_at: i64,
    pub last_heartbeat_at: Option<i64>,
    pub last_time_sync_at: Option<i64>,
    pub clock_sync_status: Option<ClockSyncStatus>,
    pub disconnected_at: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSessionRecord {
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub platform: String,
    pub status: SessionStatus,
    pub capabilities: CanonicalJson,
    pub remote_addr: Option<String>,
    pub connected_at: i64,
    pub last_heartbeat_at: Option<i64>,
    pub last_time_sync_at: Option<i64>,
    pub clock_sync_status: Option<ClockSyncStatus>,
    pub disconnected_at: Option<i64>,
}

impl From<NewSessionRecord> for StoredSessionRecord {
    fn from(record: NewSessionRecord) -> Self {
        Self {
            session_id: record.session_id,
            client_id: record.client_id,
            account_id: record.account_id,
            terminal_id: record.terminal_id,
            platform: record.platform,
            status: record.status,
            capabilities: record.capabilities,
            remote_addr: record.remote_addr,
            connected_at: record.connected_at,
            last_heartbeat_at: record.last_heartbeat_at,
            last_time_sync_at: record.last_time_sync_at,
            clock_sync_status: record.clock_sync_status,
            disconnected_at: record.disconnected_at,
        }
    }
}

impl From<NewCoreEvent> for StoredCoreEvent {
    fn from(event: NewCoreEvent) -> Self {
        Self {
            metadata: event.metadata,
            payload: event.payload,
        }
    }
}

impl From<NewWireOutbox> for StoredWireOutbox {
    fn from(message: NewWireOutbox) -> Self {
        Self {
            message_id: message.message_id,
            session_id: message.session_id,
            message_type: message.message_type,
            sequence: message.sequence,
            command_id: message.command_id,
            payload: message.payload,
            status: message.status,
            created_at: message.created_at,
            sent_at: message.sent_at,
            acked_at: message.acked_at,
            last_error: message.last_error,
        }
    }
}

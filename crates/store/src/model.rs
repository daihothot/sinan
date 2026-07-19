use sinan_protocol::TransportAckStatus;
use sinan_types::{
    AccountId, CausationId, ClientId, ClockSyncStatus, CommandDeliveryAttemptStatus, CommandId,
    CorrelationId, ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus, ExecutionEvent,
    ExecutionLeg, ExecutionLegState, ExecutionLegStatus, ExecutionPlan, ExecutionPlanState,
    ExecutionPlanStatus, IdempotencyKey, IntentId, LegId, MessageId, PlanId, RequestId, RiskId,
    RiskResult, SessionId, SessionStatus, StrategyId, TerminalId, TradeIntent, TradeIntentStatus,
    WireInboxStatus, WireOutboxStatus,
};

use crate::json::CanonicalJson;

pub const GLOBAL_CIRCUIT_BREAKER_SCOPE: &str = "GLOBAL";
pub const DELIVERY_ERROR_COMMAND_EXPIRED: &str = "COMMAND_EXPIRED";
pub const DELIVERY_ERROR_SESSION_UNAVAILABLE: &str = "SESSION_UNAVAILABLE";
pub const DELIVERY_ERROR_CLOCK_UNHEALTHY: &str = "CLOCK_UNHEALTHY";
pub const TRANSPORT_ACK_REJECTED_PREFIX: &str = "TRANSPORT_ACK_REJECTED:";

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
pub struct NewCircuitBreakerSnapshot {
    pub expected_head_revision: Option<u64>,
    pub schema_version: String,
    pub status: String,
    pub recovery_epoch: u64,
    pub updated_at: i64,
    pub payload: CanonicalJson,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCircuitBreakerSnapshot {
    pub scope: String,
    pub state_revision: u64,
    pub schema_version: String,
    pub status: String,
    pub recovery_epoch: u64,
    pub updated_at: i64,
    pub payload: CanonicalJson,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerHeadMetadata {
    pub state_revision: u64,
    pub recovery_epoch: u64,
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
pub struct NewRiskResult {
    pub result: RiskResult,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredRiskResult {
    pub result: RiskResult,
    pub payload: CanonicalJson,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutionPlan {
    pub plan: ExecutionPlan,
    pub risk_id: RiskId,
    pub intent_id: IntentId,
    pub recorded_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionPlan {
    pub plan: ExecutionPlan,
    pub risk_id: RiskId,
    pub intent_id: IntentId,
    pub payload: CanonicalJson,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionLeg {
    pub plan_id: PlanId,
    pub leg: ExecutionLeg,
    pub payload: CanonicalJson,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanStateUpdate {
    pub plan_id: PlanId,
    pub expected_status: ExecutionPlanStatus,
    pub expected_updated_at: i64,
    pub state: ExecutionPlanState,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegStateUpdate {
    pub plan_id: PlanId,
    pub leg_id: LegId,
    pub expected_status: ExecutionLegStatus,
    pub expected_updated_at: i64,
    pub state: ExecutionLegState,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionLifecycleUpdate {
    pub plan: PlanStateUpdate,
    pub legs: Vec<LegStateUpdate>,
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

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutionWorkflow {
    pub intent: NewTradeIntent,
    pub risk_result: NewRiskResult,
    pub plan: NewExecutionPlan,
    pub commands: Vec<NewExecutionCommand>,
    pub command_states: Vec<ExecutionCommandState>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionWorkflow {
    pub intent: StoredTradeIntent,
    pub risk_result: StoredRiskResult,
    pub plan: StoredExecutionPlan,
    pub commands: Vec<StoredExecutionCommand>,
    pub command_states: Vec<ExecutionCommandState>,
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
    pub request_id: Option<RequestId>,
    pub payload: CanonicalJson,
    pub status: WireOutboxStatus,
    pub created_at: i64,
    pub updated_at: i64,
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
    pub request_id: Option<RequestId>,
    pub payload: CanonicalJson,
    pub status: WireOutboxStatus,
    pub revision: u64,
    pub created_at: i64,
    pub updated_at: i64,
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
    pub max_inflight_commands: u64,
    pub updated_at: i64,
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
    pub revision: u64,
    pub updated_at: i64,
    pub last_outbound_sequence: u64,
    pub max_inflight_commands: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliverySubject {
    ExecutionCommand(CommandId),
    ReconciliationRequest(RequestId),
}

impl DeliverySubject {
    pub fn command_id(&self) -> Option<&CommandId> {
        match self {
            Self::ExecutionCommand(command_id) => Some(command_id),
            Self::ReconciliationRequest(_) => None,
        }
    }

    pub fn request_id(&self) -> Option<&RequestId> {
        match self {
            Self::ExecutionCommand(_) => None,
            Self::ReconciliationRequest(request_id) => Some(request_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryRejectionKind {
    NoActiveSession,
    AmbiguousRoute,
    StaleSession,
    ClockUnhealthy,
    Backpressure,
    InflightLimit,
    Expired,
    IdentityMismatch,
}

impl DeliveryRejectionKind {
    pub const fn attempt_status(self) -> CommandDeliveryAttemptStatus {
        match self {
            Self::NoActiveSession
            | Self::AmbiguousRoute
            | Self::StaleSession
            | Self::ClockUnhealthy => CommandDeliveryAttemptStatus::NoActiveSession,
            Self::Backpressure | Self::InflightLimit => CommandDeliveryAttemptStatus::Backpressure,
            Self::Expired => CommandDeliveryAttemptStatus::Cancelled,
            Self::IdentityMismatch => CommandDeliveryAttemptStatus::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewDeliveryAttempt {
    pub attempt_id: String,
    pub subject: DeliverySubject,
    pub session_id: Option<SessionId>,
    pub message_id: Option<MessageId>,
    pub request_payload: Option<CanonicalJson>,
    pub status: CommandDeliveryAttemptStatus,
    pub attempted_at: i64,
    pub acked_at: Option<i64>,
    pub error: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredDeliveryAttempt {
    pub attempt_id: String,
    pub subject: DeliverySubject,
    pub session_id: Option<SessionId>,
    pub message_id: Option<MessageId>,
    pub request_payload: Option<CanonicalJson>,
    pub status: CommandDeliveryAttemptStatus,
    pub attempted_at: i64,
    pub acked_at: Option<i64>,
    pub error: Option<String>,
    pub revision: u64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionReplacement {
    pub session: StoredSessionRecord,
    pub replaced_session: Option<StoredSessionRecord>,
    pub unconfirmed_attempts: Vec<StoredDeliveryAttempt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHeartbeatUpdate {
    pub session_id: SessionId,
    pub expected_revision: u64,
    pub heartbeat_at: i64,
    pub clock_sync_status: ClockSyncStatus,
    pub last_time_sync_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStatusUpdate {
    pub session_id: SessionId,
    pub expected_revision: u64,
    pub changed_at: i64,
    pub delivery_error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRouteQuery {
    pub account_id: AccountId,
    pub client_id: Option<ClientId>,
    pub terminal_id: Option<TerminalId>,
    pub fresh_after: i64,
    pub require_synced_clock: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionRouteResolution {
    Ready(StoredSessionRecord),
    NoActiveSession,
    Stale { candidate_count: usize },
    ClockUnhealthy { candidate_count: usize },
    Ambiguous { candidate_count: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReserveOutboundSequence {
    pub session_id: SessionId,
    pub expected_revision: u64,
    pub subject: DeliverySubject,
    pub fresh_after: i64,
    pub reserved_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundReservation {
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub session_revision: u64,
    pub sequence: u64,
    pub subject: DeliverySubject,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SequenceReservation {
    Reserved(OutboundReservation),
    SessionUnavailable,
    ClockUnhealthy,
    Expired,
    IdentityMismatch {
        field: &'static str,
    },
    InflightLimit {
        session_id: SessionId,
        inflight: u64,
        limit: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewReservedDelivery {
    pub reservation: OutboundReservation,
    pub attempt_id: String,
    pub message_id: MessageId,
    pub message_type: String,
    pub envelope: CanonicalJson,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOutboundDelivery {
    pub outbox: StoredWireOutbox,
    pub attempt: StoredDeliveryAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxClaimOutcome {
    Claimed(StoredOutboundDelivery),
    Expired(StoredOutboundDelivery),
    SessionUnavailable(StoredOutboundDelivery),
    ClockUnhealthy(StoredOutboundDelivery),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimWireOutbox {
    pub message_id: MessageId,
    pub expected_outbox_revision: u64,
    pub expected_attempt_revision: u64,
    pub fresh_after: i64,
    pub require_synced_clock: bool,
    pub claimed_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteTransportWrite {
    pub message_id: MessageId,
    pub expected_outbox_revision: u64,
    pub expected_attempt_revision: u64,
    pub completed_at: i64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportAckUpdate {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub message_type: String,
    pub status: TransportAckStatus,
    pub reason: Option<String>,
    pub acked_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandReceivedAttemptUpdate {
    pub source_message_id: MessageId,
    pub session_id: SessionId,
    pub command_id: CommandId,
    pub received_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryAttemptTimeout {
    pub attempt_id: String,
    pub expected_revision: u64,
    pub timed_out_at: i64,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDisconnectOutcome {
    pub session: StoredSessionRecord,
    pub unconfirmed_attempts: Vec<StoredDeliveryAttempt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryStartupFenceReport {
    pub sessions_staled: u64,
    pub outboxes_fenced: u64,
    pub attempts_unconfirmed: u64,
    pub attempts_cancelled: u64,
    pub attempts_rejected: u64,
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
            revision: 0,
            updated_at: record.updated_at,
            last_outbound_sequence: 1,
            max_inflight_commands: record.max_inflight_commands,
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
            request_id: message.request_id,
            payload: message.payload,
            status: message.status,
            revision: 0,
            created_at: message.created_at,
            updated_at: message.updated_at,
            sent_at: message.sent_at,
            acked_at: message.acked_at,
            last_error: message.last_error,
        }
    }
}

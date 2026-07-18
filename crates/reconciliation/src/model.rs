use serde::Serialize;
use sinan_execution::CommandTransitionOutcome;
use sinan_protocol::{ReconciliationReason, ReconciliationRequest, ReconciliationResult};
use sinan_types::{
    AccountId, BrokerOrderId, ClientId, CommandId, ExecutionCommand, ExecutionCommandState,
    ExecutionCommandStatus, ExecutionEventStatus, OrderSnapshotStatus, RequestId, TerminalId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationRequestInput {
    pub request_id: RequestId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub client_id: Option<ClientId>,
    pub reason: ReconciliationReason,
    /// `None` means every command in the account/route scope. `Some` is a
    /// targeted scope and must contain at least one unique command.
    pub command_ids: Option<Vec<CommandId>>,
    pub since_server_time: Option<i64>,
    pub requested_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationRequestContext {
    pub request: ReconciliationRequest,
    pub requested_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReconciliationCommand {
    pub command: ExecutionCommand,
    pub state: ExecutionCommandState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationCommandTransition {
    pub command_id: CommandId,
    pub expected_status: ExecutionCommandStatus,
    pub expected_updated_at: i64,
    pub outcome: CommandTransitionOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationRequestPlan {
    pub context: ReconciliationRequestContext,
    /// Only commands for which the execution state machine accepts
    /// `BeginReconciliation` appear here. Other account-wide in-flight states
    /// remain represented by the reconciliation run rather than regressing
    /// their command lifecycle.
    pub command_transitions: Vec<ReconciliationCommandTransition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationDisposition {
    Completed,
    PendingEvidence,
    ManualRequired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationFinding {
    ClientReportedUnresolved {
        command_id: CommandId,
    },
    ClientReportedUnresolvedDespiteAuthoritativeState {
        command_id: CommandId,
        status: ExecutionCommandStatus,
    },
    CommandAlreadyRequiresManualReconciliation {
        command_id: CommandId,
    },
    UnknownCommandReportedUnresolved {
        command_id: CommandId,
    },
    UnknownCommandObservedInOrderSnapshot {
        command_id: CommandId,
        broker_order_ids: Vec<BrokerOrderId>,
    },
    MissingAuthoritativeExecutionEvidence {
        command_id: CommandId,
        observed_broker_order_ids: Vec<BrokerOrderId>,
    },
    ExecutionProjectionPending {
        command_id: CommandId,
        event_status: ExecutionEventStatus,
    },
    MultipleOrderSnapshotsForCommand {
        command_id: CommandId,
        broker_order_ids: Vec<BrokerOrderId>,
    },
    OrderIdentityConflict {
        command_id: CommandId,
        broker_order_id: BrokerOrderId,
        field: &'static str,
    },
    SnapshotConflictsWithExecutionEvent {
        command_id: CommandId,
        broker_order_id: BrokerOrderId,
        event_status: ExecutionEventStatus,
        snapshot_status: OrderSnapshotStatus,
    },
    ReconciliationResultMissing {
        escalated_at: i64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReconciliationEvaluation {
    pub request_id: RequestId,
    pub account_id: AccountId,
    /// `None` is reserved for an explicit missing-result escalation.
    pub observed_at: Option<i64>,
    pub disposition: ReconciliationDisposition,
    /// Commands that prevent a `Completed` outcome. In a manual outcome this
    /// is the set requiring operator attention.
    pub command_ids: Vec<CommandId>,
    pub findings: Vec<ReconciliationFinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManualEscalationEvidence {
    pub request_id: RequestId,
    pub escalated_at: i64,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualReconciliationEscalation {
    pub evidence: ManualEscalationEvidence,
    pub evaluation: ReconciliationEvaluation,
    pub command_transitions: Vec<ReconciliationCommandTransition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvaluatedReconciliationResult {
    pub result: ReconciliationResult,
    pub evaluation: ReconciliationEvaluation,
}

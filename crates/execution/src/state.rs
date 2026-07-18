use sinan_protocol::{CommandInboxStatus, CommandReceived};
use sinan_types::{
    ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus, ExecutionEvent,
    ExecutionEventStatus,
};
use thiserror::Error;

/// Business evidence accepted by the command lifecycle.
///
/// A transport acknowledgement is deliberately not command evidence:
///
/// ```compile_fail
/// use sinan_execution::CommandEvidence;
/// use sinan_protocol::TransportAck;
/// fn accept(_: CommandEvidence<'_>) {}
/// fn transport_ack_cannot_advance(ack: TransportAck) { accept(ack); }
/// ```
#[derive(Clone, Copy, Debug)]
pub enum CommandEvidence<'a> {
    Dispatched { at: i64 },
    DeliveryUnconfirmed { at: i64, error: &'a str },
    BeginReconciliation { at: i64 },
    RequireManualReconciliation { at: i64 },
    ReceivedRecorded(&'a CommandReceived),
    ReceivedDuplicateKnownSamePayload(&'a CommandReceived),
    ExecutionEvent(&'a ExecutionEvent),
    Expire { at: i64 },
    Cancel { at: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandTransitionOutcome {
    Applied(ExecutionCommandState),
    Duplicate(ExecutionCommandState),
}

impl CommandTransitionOutcome {
    pub fn state(&self) -> &ExecutionCommandState {
        match self {
            Self::Applied(state) | Self::Duplicate(state) => state,
        }
    }

    pub fn into_state(self) -> ExecutionCommandState {
        match self {
            Self::Applied(state) | Self::Duplicate(state) => state,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum CommandTransitionError {
    #[error("invalid command lifecycle identity at {0}")]
    InvalidIdentity(&'static str),

    #[error("invalid command lifecycle timestamp: {0}")]
    InvalidTimestamp(&'static str),

    #[error("invalid command evidence: {0}")]
    InvalidEvidence(&'static str),

    #[error("command transition {from} -> {to} is not allowed")]
    InvalidTransition {
        from: ExecutionCommandStatus,
        to: ExecutionCommandStatus,
    },

    #[error("command lifecycle version overflow")]
    VersionOverflow,
}

pub fn initial_command_state(
    command: &ExecutionCommand,
    created_at: i64,
) -> Result<ExecutionCommandState, CommandTransitionError> {
    validate_command_identity(command)?;
    if created_at < 0 || command.expires_at <= created_at {
        return Err(CommandTransitionError::InvalidTimestamp(
            "created_at must precede expires_at",
        ));
    }
    let state = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id: command.account_id.clone(),
        plan_id: command.plan_id.clone(),
        leg_id: command.leg_id.clone(),
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: created_at,
    };
    validate_command_state(command, &state)?;
    Ok(state)
}

pub fn transition_command(
    command: &ExecutionCommand,
    state: &ExecutionCommandState,
    evidence: CommandEvidence<'_>,
) -> Result<CommandTransitionOutcome, CommandTransitionError> {
    validate_command_state(command, state)?;
    let (target, evidence_at) = evidence_target(command, evidence)?;
    if matches!(
        evidence,
        CommandEvidence::ExecutionEvent(event)
            if event
                .filled_at
                .is_some_and(|filled_at| filled_at < state.created_at)
    ) {
        return Err(CommandTransitionError::InvalidTimestamp(
            "filled_at predates command creation",
        ));
    }
    if evidence_at < state.created_at {
        return Err(CommandTransitionError::InvalidTimestamp(
            "evidence predates command creation",
        ));
    }
    if evidence_at < latest_lifecycle_evidence_at(state) {
        return Err(CommandTransitionError::InvalidTimestamp(
            "advancing evidence predates existing lifecycle evidence",
        ));
    }
    if target == state.status {
        return Ok(CommandTransitionOutcome::Duplicate(state.clone()));
    }
    if matches!(
        evidence,
        CommandEvidence::Expire { .. } | CommandEvidence::Cancel { .. }
    ) && state.status != ExecutionCommandStatus::Created
    {
        return Err(CommandTransitionError::InvalidEvidence(
            "local expiry/cancellation is only valid before dispatch",
        ));
    }
    if !transition_is_allowed(state.status, target) {
        return Err(CommandTransitionError::InvalidTransition {
            from: state.status,
            to: target,
        });
    }
    let mut next = state.clone();
    next.status = target;
    next.updated_at = state
        .updated_at
        .checked_add(1)
        .ok_or(CommandTransitionError::VersionOverflow)?
        .max(evidence_at);
    match evidence {
        CommandEvidence::Dispatched { at } => {
            next.delivery_attempts = next
                .delivery_attempts
                .checked_add(1)
                .ok_or(CommandTransitionError::VersionOverflow)?;
            next.dispatched_at = Some(at);
            next.last_delivery_error = None;
        }
        CommandEvidence::DeliveryUnconfirmed { error, .. } => {
            if error.trim().is_empty() {
                return Err(CommandTransitionError::InvalidEvidence(
                    "delivery error must not be empty",
                ));
            }
            next.last_delivery_error = Some(error.to_owned());
        }
        CommandEvidence::BeginReconciliation { at }
        | CommandEvidence::RequireManualReconciliation { at } => {
            next.reconciling_at.get_or_insert(at);
        }
        CommandEvidence::ReceivedRecorded(received)
        | CommandEvidence::ReceivedDuplicateKnownSamePayload(received) => {
            next.command_received_at.get_or_insert(received.received_at);
        }
        CommandEvidence::ExecutionEvent(_)
        | CommandEvidence::Expire { .. }
        | CommandEvidence::Cancel { .. } => {}
    }
    if command_status_is_terminal(target) {
        next.completed_at = Some(evidence_at);
    }
    validate_command_state(command, &next)?;
    Ok(CommandTransitionOutcome::Applied(next))
}

pub fn validate_command_state(
    command: &ExecutionCommand,
    state: &ExecutionCommandState,
) -> Result<(), CommandTransitionError> {
    validate_command_identity(command)?;
    if state.command_id != command.command_id
        || state.account_id != command.account_id
        || state.plan_id != command.plan_id
        || state.leg_id != command.leg_id
    {
        return Err(CommandTransitionError::InvalidIdentity(
            "state does not belong to command",
        ));
    }
    if state.created_at < 0 || state.updated_at < state.created_at {
        return Err(CommandTransitionError::InvalidTimestamp(
            "created_at/updated_at are inconsistent",
        ));
    }
    if command.expires_at <= state.created_at {
        return Err(CommandTransitionError::InvalidTimestamp(
            "command expiry must follow state creation",
        ));
    }
    if command_status_is_terminal(state.status) != state.completed_at.is_some() {
        return Err(CommandTransitionError::InvalidTimestamp(
            "completed_at must exactly match terminal status",
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
            return Err(CommandTransitionError::InvalidTimestamp(
                "lifecycle timestamp is outside state version bounds",
            ));
        }
    }
    if state.status == ExecutionCommandStatus::Created
        && (state.delivery_attempts != 0
            || state.dispatched_at.is_some()
            || state.command_received_at.is_some()
            || state.reconciling_at.is_some())
    {
        return Err(CommandTransitionError::InvalidTimestamp(
            "CREATED state cannot contain later lifecycle evidence",
        ));
    }
    if state.dispatched_at.is_some() != (state.delivery_attempts > 0) {
        return Err(CommandTransitionError::InvalidTimestamp(
            "delivery_attempts and dispatched_at must be present together",
        ));
    }
    let requires_dispatch = matches!(
        state.status,
        ExecutionCommandStatus::Dispatched
            | ExecutionCommandStatus::DeliveryUnconfirmed
            | ExecutionCommandStatus::Reconciling
            | ExecutionCommandStatus::ManualReconciliationRequired
            | ExecutionCommandStatus::CommandReceived
            | ExecutionCommandStatus::Accepted
            | ExecutionCommandStatus::Rejected
            | ExecutionCommandStatus::OrderSent
            | ExecutionCommandStatus::PartiallyFilled
            | ExecutionCommandStatus::Filled
            | ExecutionCommandStatus::Failed
    );
    if requires_dispatch && state.dispatched_at.is_none() {
        return Err(CommandTransitionError::InvalidTimestamp(
            "command status requires dispatch evidence",
        ));
    }
    if matches!(state.status, ExecutionCommandStatus::DeliveryUnconfirmed)
        && state
            .last_delivery_error
            .as_deref()
            .is_none_or(|error| error.trim().is_empty())
    {
        return Err(CommandTransitionError::InvalidEvidence(
            "DELIVERY_UNCONFIRMED requires a delivery error",
        ));
    }
    if matches!(
        state.status,
        ExecutionCommandStatus::Reconciling | ExecutionCommandStatus::ManualReconciliationRequired
    ) && state.reconciling_at.is_none()
    {
        return Err(CommandTransitionError::InvalidTimestamp(
            "reconciliation status requires reconciling_at",
        ));
    }
    if matches!(
        state.status,
        ExecutionCommandStatus::CommandReceived
            | ExecutionCommandStatus::Accepted
            | ExecutionCommandStatus::Rejected
    ) && state.command_received_at.is_none()
    {
        return Err(CommandTransitionError::InvalidTimestamp(
            "command status requires command_received_at",
        ));
    }
    Ok(())
}

fn latest_lifecycle_evidence_at(state: &ExecutionCommandState) -> i64 {
    [
        Some(state.created_at),
        state.dispatched_at,
        state.command_received_at,
        state.reconciling_at,
        state.completed_at,
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(state.created_at)
}

pub fn command_status_is_terminal(status: ExecutionCommandStatus) -> bool {
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

fn validate_command_identity(command: &ExecutionCommand) -> Result<(), CommandTransitionError> {
    for (field, value) in [
        ("command_id", command.command_id.as_str()),
        ("strategy_id", command.strategy_id.as_str()),
        ("account_id", command.account_id.as_str()),
        ("symbol", command.symbol.as_str()),
        ("idempotency_key", command.idempotency_key.as_str()),
        ("hmac", command.hmac.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(CommandTransitionError::InvalidIdentity(field));
        }
    }
    if command.leg_id.is_some() && command.plan_id.is_none() {
        return Err(CommandTransitionError::InvalidIdentity(
            "leg_id requires plan_id",
        ));
    }
    Ok(())
}

fn evidence_target(
    command: &ExecutionCommand,
    evidence: CommandEvidence<'_>,
) -> Result<(ExecutionCommandStatus, i64), CommandTransitionError> {
    let result = match evidence {
        CommandEvidence::Dispatched { at } => {
            validate_before_expiry(command, at, "dispatch cannot occur at or after expires_at")?;
            (ExecutionCommandStatus::Dispatched, at)
        }
        CommandEvidence::DeliveryUnconfirmed { at, .. } => {
            (ExecutionCommandStatus::DeliveryUnconfirmed, at)
        }
        CommandEvidence::BeginReconciliation { at } => (ExecutionCommandStatus::Reconciling, at),
        CommandEvidence::RequireManualReconciliation { at } => {
            (ExecutionCommandStatus::ManualReconciliationRequired, at)
        }
        CommandEvidence::ReceivedRecorded(received) => {
            validate_receipt(command, received, CommandInboxStatus::Recorded)?;
            validate_before_expiry(
                command,
                received.received_at,
                "new command receipt cannot occur at or after expires_at",
            )?;
            (
                ExecutionCommandStatus::CommandReceived,
                received.received_at,
            )
        }
        CommandEvidence::ReceivedDuplicateKnownSamePayload(received) => {
            validate_receipt(command, received, CommandInboxStatus::Duplicate)?;
            (
                ExecutionCommandStatus::CommandReceived,
                received.received_at,
            )
        }
        CommandEvidence::ExecutionEvent(event) => {
            validate_execution_event(command, event)?;
            (event_status(event.status), event.event_at)
        }
        CommandEvidence::Expire { at } => {
            if at < command.expires_at {
                return Err(CommandTransitionError::InvalidTimestamp(
                    "expiry evidence predates command.expires_at",
                ));
            }
            (ExecutionCommandStatus::Expired, at)
        }
        CommandEvidence::Cancel { at } => (ExecutionCommandStatus::Cancelled, at),
    };
    if result.1 < 0 {
        return Err(CommandTransitionError::InvalidTimestamp(
            "evidence timestamp must be non-negative",
        ));
    }
    Ok(result)
}

fn validate_before_expiry(
    command: &ExecutionCommand,
    at: i64,
    reason: &'static str,
) -> Result<(), CommandTransitionError> {
    if at >= command.expires_at {
        Err(CommandTransitionError::InvalidTimestamp(reason))
    } else {
        Ok(())
    }
}

fn validate_receipt(
    command: &ExecutionCommand,
    received: &CommandReceived,
    expected: CommandInboxStatus,
) -> Result<(), CommandTransitionError> {
    if received.inbox_status != expected {
        return Err(CommandTransitionError::InvalidEvidence(
            "command receipt has the wrong inbox status",
        ));
    }
    if received.command_id != command.command_id
        || received.idempotency_key != command.idempotency_key
        || received.account_id != command.account_id
        || received.terminal_id != command.terminal_id
        || received.client_id != command.client_id
    {
        return Err(CommandTransitionError::InvalidIdentity(
            "command receipt identity differs from command",
        ));
    }
    Ok(())
}

/// Validates an execution event against its immutable command identity and
/// protocol-level quantity semantics without applying a lifecycle transition.
pub fn validate_execution_event(
    command: &ExecutionCommand,
    event: &ExecutionEvent,
) -> Result<(), CommandTransitionError> {
    if event.execution_id.as_str().trim().is_empty()
        || event.event_at < 0
        || event.command_id != command.command_id
        || event.plan_id != command.plan_id
        || event.leg_id != command.leg_id
        || event.account_id != command.account_id
        || event.terminal_id != command.terminal_id
        || event.client_id != command.client_id
        || event.symbol != command.symbol
        || event
            .broker_symbol
            .as_ref()
            .is_some_and(|symbol| Some(symbol) != command.broker_symbol.as_ref())
        || event
            .idempotency_key
            .as_ref()
            .is_some_and(|key| key != &command.idempotency_key)
        || event.requested_lots.is_some_and(|lots| {
            command
                .lots
                .is_none_or(|command_lots| lots.to_bits() != command_lots.to_bits())
        })
    {
        return Err(CommandTransitionError::InvalidIdentity(
            "execution event identity differs from command",
        ));
    }
    let is_fill = matches!(
        event.status,
        ExecutionEventStatus::Filled | ExecutionEventStatus::PartiallyFilled
    );
    if is_fill != event.filled_at.is_some() {
        return Err(CommandTransitionError::InvalidEvidence(
            "filled_at must exactly match a fill status",
        ));
    }
    if is_fill
        && event
            .filled_lots
            .is_none_or(|filled_lots| !filled_lots.is_finite() || filled_lots <= 0.0)
    {
        return Err(CommandTransitionError::InvalidEvidence(
            "fill events require a positive filled_lots value",
        ));
    }
    if event
        .filled_at
        .is_some_and(|filled_at| filled_at < 0 || filled_at > event.event_at)
    {
        return Err(CommandTransitionError::InvalidTimestamp(
            "filled_at must not follow event_at",
        ));
    }
    for value in [
        event.requested_lots,
        event.fill_price,
        event.filled_lots,
        event.remaining_lots,
    ]
    .into_iter()
    .flatten()
    {
        if !value.is_finite() || value < 0.0 {
            return Err(CommandTransitionError::InvalidEvidence(
                "execution quantities and prices must be finite and non-negative",
            ));
        }
    }
    Ok(())
}

fn event_status(status: ExecutionEventStatus) -> ExecutionCommandStatus {
    match status {
        ExecutionEventStatus::Accepted => ExecutionCommandStatus::Accepted,
        ExecutionEventStatus::OrderSent => ExecutionCommandStatus::OrderSent,
        ExecutionEventStatus::Rejected => ExecutionCommandStatus::Rejected,
        ExecutionEventStatus::Filled => ExecutionCommandStatus::Filled,
        ExecutionEventStatus::PartiallyFilled => ExecutionCommandStatus::PartiallyFilled,
        ExecutionEventStatus::Failed => ExecutionCommandStatus::Failed,
        ExecutionEventStatus::Expired => ExecutionCommandStatus::Expired,
        ExecutionEventStatus::Cancelled => ExecutionCommandStatus::Cancelled,
    }
}

fn transition_is_allowed(from: ExecutionCommandStatus, to: ExecutionCommandStatus) -> bool {
    use ExecutionCommandStatus as S;
    match from {
        S::Created => matches!(to, S::Dispatched | S::Expired | S::Cancelled),
        S::Dispatched => matches!(
            to,
            S::CommandReceived | S::DeliveryUnconfirmed | S::Expired | S::Cancelled
        ),
        S::DeliveryUnconfirmed => matches!(
            to,
            S::Reconciling
                | S::CommandReceived
                | S::ManualReconciliationRequired
                | S::Expired
                | S::Failed
        ),
        S::Reconciling => matches!(
            to,
            S::CommandReceived
                | S::OrderSent
                | S::PartiallyFilled
                | S::Filled
                | S::ManualReconciliationRequired
                | S::Failed
                | S::Expired
        ),
        S::ManualReconciliationRequired => matches!(
            to,
            S::CommandReceived
                | S::OrderSent
                | S::PartiallyFilled
                | S::Filled
                | S::Failed
                | S::Expired
                | S::Cancelled
        ),
        S::CommandReceived => {
            matches!(to, S::Accepted | S::Rejected | S::Cancelled | S::Expired)
        }
        S::Accepted => matches!(
            to,
            S::OrderSent | S::Rejected | S::Failed | S::Cancelled | S::Expired
        ),
        S::OrderSent => matches!(
            to,
            S::PartiallyFilled | S::Filled | S::Cancelled | S::Failed
        ),
        S::PartiallyFilled => matches!(to, S::Filled | S::Failed | S::Cancelled),
        S::DeliveryFailed | S::Rejected | S::Filled | S::Failed | S::Expired | S::Cancelled => {
            false
        }
    }
}

use std::collections::HashSet;

use sinan_types::{
    ExecutionCommandState, ExecutionCommandStatus, ExecutionFailurePolicy, ExecutionLeg,
    ExecutionEvent, ExecutionEventStatus, ExecutionLegStatus, ExecutionPlan, ExecutionPlanStatus,
    LegId, RollbackMode,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ProjectionError {
    #[error("projection identity mismatch: {0}")]
    IdentityMismatch(&'static str),

    #[error("a leg must have exactly one primary command in the v1 projector")]
    UnsupportedCommandCardinality,

    #[error("DELIVERY_FAILED projection semantics are deferred")]
    DeliveryFailedDeferred,

    #[error("execution plan contains no legs")]
    EmptyPlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryDecision {
    None,
    CloseFilled { leg_ids: Vec<LegId> },
    LeavePartial,
    ManualReview,
}

/// Projects a leg from its single immutable v1 command lifecycle.
pub fn project_leg(
    leg: &ExecutionLeg,
    command_states: &[ExecutionCommandState],
    events: &[ExecutionEvent],
) -> Result<ExecutionLeg, ProjectionError> {
    if command_states.len() != 1 {
        return Err(ProjectionError::UnsupportedCommandCardinality);
    }
    let command = &command_states[0];
    if command.leg_id.as_ref() != Some(&leg.definition.leg_id) {
        return Err(ProjectionError::IdentityMismatch(
            "command state leg_id differs from leg",
        ));
    }
    let mut execution_ids = HashSet::with_capacity(events.len());
    let mut has_partial_fill = false;
    let mut has_full_fill = false;
    for event in events {
        if event.execution_id.as_str().trim().is_empty()
            || !execution_ids.insert(&event.execution_id)
            || event.command_id != command.command_id
            || event.account_id != command.account_id
            || event.plan_id != command.plan_id
            || event.leg_id != command.leg_id
            || event.symbol != leg.definition.symbol
            || event.event_at < command.created_at
        {
            return Err(ProjectionError::IdentityMismatch(
                "execution event does not belong to the projected leg command",
            ));
        }
        let is_fill = matches!(
            event.status,
            ExecutionEventStatus::PartiallyFilled | ExecutionEventStatus::Filled
        );
        if is_fill {
            if event.filled_at.is_none()
                || event
                    .filled_lots
                    .is_none_or(|lots| !lots.is_finite() || lots <= 0.0)
                || event
                    .filled_at
                    .is_some_and(|filled_at| filled_at < command.created_at || filled_at > event.event_at)
            {
                return Err(ProjectionError::IdentityMismatch(
                    "fill event lacks positive exposure evidence",
                ));
            }
            has_partial_fill |= event.status == ExecutionEventStatus::PartiallyFilled;
            has_full_fill |= event.status == ExecutionEventStatus::Filled;
        } else if event.filled_at.is_some() {
            return Err(ProjectionError::IdentityMismatch(
                "non-fill event carries filled_at",
            ));
        }
        if [
            event.requested_lots,
            event.fill_price,
            event.filled_lots,
            event.remaining_lots,
        ]
        .into_iter()
        .flatten()
        .any(|value| !value.is_finite() || value < 0.0)
        {
            return Err(ProjectionError::IdentityMismatch(
                "execution event contains an invalid numeric value",
            ));
        }
    }
    let lifecycle_status = match command.status {
        ExecutionCommandStatus::Created => ExecutionLegStatus::Pending,
        ExecutionCommandStatus::Dispatched => ExecutionLegStatus::Sent,
        ExecutionCommandStatus::DeliveryUnconfirmed => ExecutionLegStatus::DeliveryUnconfirmed,
        ExecutionCommandStatus::DeliveryFailed => {
            return Err(ProjectionError::DeliveryFailedDeferred)
        }
        ExecutionCommandStatus::Reconciling => ExecutionLegStatus::Reconciling,
        ExecutionCommandStatus::ManualReconciliationRequired => {
            ExecutionLegStatus::ManualReconciliationRequired
        }
        ExecutionCommandStatus::CommandReceived => ExecutionLegStatus::CommandReceived,
        ExecutionCommandStatus::Accepted => ExecutionLegStatus::Accepted,
        ExecutionCommandStatus::Rejected => ExecutionLegStatus::Rejected,
        ExecutionCommandStatus::OrderSent => ExecutionLegStatus::OrderSent,
        ExecutionCommandStatus::PartiallyFilled => ExecutionLegStatus::PartiallyFilled,
        ExecutionCommandStatus::Filled => ExecutionLegStatus::Filled,
        ExecutionCommandStatus::Failed => ExecutionLegStatus::Failed,
        ExecutionCommandStatus::Expired => ExecutionLegStatus::Expired,
        ExecutionCommandStatus::Cancelled => ExecutionLegStatus::Cancelled,
    };
    let status = if has_full_fill {
        ExecutionLegStatus::Filled
    } else if has_partial_fill {
        ExecutionLegStatus::PartiallyFilled
    } else {
        lifecycle_status
    };
    let mut projected = leg.clone();
    projected.state.status = status;
    Ok(projected)
}

/// Recomputes plan status and summary lists solely from leg projections.
pub fn project_plan(
    plan: &ExecutionPlan,
    command_states: &[ExecutionCommandState],
    events: &[ExecutionEvent],
) -> Result<ExecutionPlan, ProjectionError> {
    if plan.legs.is_empty() {
        return Err(ProjectionError::EmptyPlan);
    }
    if command_states.len() != plan.legs.len() {
        return Err(ProjectionError::UnsupportedCommandCardinality);
    }
    let mut projected = plan.clone();
    for leg in &mut projected.legs {
        let states: Vec<_> = command_states
            .iter()
            .filter(|state| state.leg_id.as_ref() == Some(&leg.definition.leg_id))
            .cloned()
            .collect();
        let leg_events: Vec<_> = events
            .iter()
            .filter(|event| event.leg_id.as_ref() == Some(&leg.definition.leg_id))
            .cloned()
            .collect();
        *leg = project_leg(leg, &states, &leg_events)?;
    }
    if events.iter().any(|event| {
        !projected
            .legs
            .iter()
            .any(|leg| event.leg_id.as_ref() == Some(&leg.definition.leg_id))
    }) {
        return Err(ProjectionError::IdentityMismatch(
            "execution event references a leg outside the plan",
        ));
    }
    projected.state.filled_legs = projected
        .legs
        .iter()
        .filter(|leg| {
            matches!(
                leg.state.status,
                ExecutionLegStatus::PartiallyFilled | ExecutionLegStatus::Filled
            )
        })
        .map(|leg| leg.definition.leg_id.clone())
        .collect();
    projected.state.failed_legs = projected
        .legs
        .iter()
        .filter(|leg| {
            matches!(
                leg.state.status,
                ExecutionLegStatus::Rejected | ExecutionLegStatus::Failed
            )
        })
        .map(|leg| leg.definition.leg_id.clone())
        .collect();

    let statuses: Vec<_> = projected.legs.iter().map(|leg| leg.state.status).collect();
    let all = |status| statuses.iter().all(|current| *current == status);
    let any = |predicate: fn(ExecutionLegStatus) -> bool| statuses.iter().copied().any(predicate);
    let all_terminal = statuses.iter().copied().all(leg_status_is_terminal);
    let any_filled = any(|status| status == ExecutionLegStatus::Filled);
    let any_partial = any(|status| status == ExecutionLegStatus::PartiallyFilled);

    projected.state.status = if all(ExecutionLegStatus::Filled) {
        ExecutionPlanStatus::Completed
    } else if all(ExecutionLegStatus::Cancelled) {
        ExecutionPlanStatus::Cancelled
    } else if all(ExecutionLegStatus::Expired) {
        ExecutionPlanStatus::Expired
    } else if any(|status| status == ExecutionLegStatus::ManualReconciliationRequired) {
        ExecutionPlanStatus::ManualReconciliationRequired
    } else if any_partial || (any_filled && !all(ExecutionLegStatus::Filled)) {
        ExecutionPlanStatus::Partial
    } else if all_terminal {
        ExecutionPlanStatus::Failed
    } else if any(|status| {
        matches!(
            status,
            ExecutionLegStatus::DeliveryUnconfirmed | ExecutionLegStatus::Reconciling
        )
    }) {
        ExecutionPlanStatus::Reconciling
    } else {
        ExecutionPlanStatus::Pending
    };
    projected
        .validate()
        .map_err(|_| ProjectionError::IdentityMismatch("projected plan is invalid"))?;
    Ok(projected)
}

pub fn decide_recovery(plan: &ExecutionPlan) -> RecoveryDecision {
    if plan.state.status != ExecutionPlanStatus::Partial {
        return RecoveryDecision::None;
    }
    match plan
        .definition
        .rollback_policy
        .as_ref()
        .map(|policy| policy.mode)
    {
        Some(RollbackMode::CloseFilled) if !plan.state.filled_legs.is_empty() => {
            RecoveryDecision::CloseFilled {
                leg_ids: plan.state.filled_legs.clone(),
            }
        }
        Some(RollbackMode::None) => RecoveryDecision::LeavePartial,
        None if plan.definition.failure_policy == ExecutionFailurePolicy::PartialFill => {
            RecoveryDecision::LeavePartial
        }
        _ => RecoveryDecision::ManualReview,
    }
}

pub fn leg_status_is_terminal(status: ExecutionLegStatus) -> bool {
    matches!(
        status,
        ExecutionLegStatus::Rejected
            | ExecutionLegStatus::Filled
            | ExecutionLegStatus::Failed
            | ExecutionLegStatus::Expired
            | ExecutionLegStatus::Cancelled
    )
}

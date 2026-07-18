use std::collections::{BTreeMap, BTreeSet};

use sinan_execution::{transition_command, CommandEvidence};
use sinan_protocol::ReconciliationResult;
use sinan_types::{
    CommandId, ExecutionCommandStatus, ExecutionEvent, ExecutionEventStatus, OrderSnapshot,
    OrderSnapshotStatus,
};

use crate::{
    validation::{
        order_identity_conflicts, validate_command_scope, validate_execution_events,
        validate_request_context, validate_result,
    },
    EvaluatedReconciliationResult, ManualEscalationEvidence, ManualReconciliationEscalation,
    ReconciliationCommand, ReconciliationCommandTransition, ReconciliationDisposition,
    ReconciliationError, ReconciliationEvaluation, ReconciliationFinding,
    ReconciliationRequestContext,
};

/// Evaluates a broker reconciliation observation without manufacturing any
/// execution fact or retry decision.
///
/// The caller must provide commands and events from one trusted State Store
/// read snapshot. For an account-wide request (`command_ids == None`), the
/// command slice must be the complete account/route scope; the pure domain
/// layer validates identities but cannot independently prove database
/// completeness.
pub fn evaluate_reconciliation_result(
    context: &ReconciliationRequestContext,
    mut result: ReconciliationResult,
    commands: &[ReconciliationCommand],
    execution_events: &[ExecutionEvent],
    received_at: i64,
) -> Result<EvaluatedReconciliationResult, ReconciliationError> {
    validate_result(context, &result, received_at)?;
    canonicalize_result_sets(&mut result);
    let indexed = validate_command_scope(context, commands, received_at)?;
    let events_by_command = validate_execution_events(&indexed, execution_events, received_at)?;
    let orders_by_command = index_orders_by_command(&result.orders);
    let client_unresolved: BTreeSet<_> = result.unresolved_command_ids.iter().cloned().collect();

    let mut findings = Vec::new();
    let mut pending = BTreeSet::new();

    for (command_id, item) in &indexed {
        let orders = orders_by_command
            .get(command_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let events = events_by_command
            .get(command_id)
            .map(Vec::as_slice)
            .unwrap_or_default();

        inspect_order_observations(
            command_id,
            &item.command,
            orders,
            events,
            result.observed_at,
            &mut findings,
        );

        if item.state.status == ExecutionCommandStatus::ManualReconciliationRequired {
            pending.insert(command_id.clone());
            findings.push(
                ReconciliationFinding::CommandAlreadyRequiresManualReconciliation {
                    command_id: command_id.clone(),
                },
            );
            if client_unresolved.contains(command_id) {
                findings.push(ReconciliationFinding::ClientReportedUnresolved {
                    command_id: command_id.clone(),
                });
            }
            continue;
        }

        if has_authoritative_delivery_evidence(item, events) {
            if client_unresolved.contains(command_id) {
                pending.insert(command_id.clone());
                findings.push(
                    ReconciliationFinding::ClientReportedUnresolvedDespiteAuthoritativeState {
                        command_id: command_id.clone(),
                        status: item.state.status,
                    },
                );
            }
            continue;
        }

        pending.insert(command_id.clone());
        if client_unresolved.contains(command_id) {
            findings.push(ReconciliationFinding::ClientReportedUnresolved {
                command_id: command_id.clone(),
            });
        }
        let mut observed_broker_order_ids: Vec<_> = orders
            .iter()
            .map(|order| order.broker_order_id.clone())
            .collect();
        observed_broker_order_ids.sort();
        findings.push(
            ReconciliationFinding::MissingAuthoritativeExecutionEvidence {
                command_id: command_id.clone(),
                observed_broker_order_ids,
            },
        );
        if let Some(latest) = events.last() {
            findings.push(ReconciliationFinding::ExecutionProjectionPending {
                command_id: command_id.clone(),
                event_status: latest.status,
            });
        }
    }

    if context.request.command_ids.is_none() {
        for (command_id, orders) in &orders_by_command {
            if !indexed.contains_key(command_id)
                && orders
                    .iter()
                    .any(|order| order_may_belong_to_request_route(context, order))
            {
                pending.insert(command_id.clone());
                findings.push(
                    ReconciliationFinding::UnknownCommandObservedInOrderSnapshot {
                        command_id: command_id.clone(),
                        broker_order_ids: orders
                            .iter()
                            .filter(|order| order_may_belong_to_request_route(context, order))
                            .map(|order| order.broker_order_id.clone())
                            .collect(),
                    },
                );
            }
        }
        for command_id in client_unresolved {
            if !indexed.contains_key(&command_id) {
                pending.insert(command_id.clone());
                findings
                    .push(ReconciliationFinding::UnknownCommandReportedUnresolved { command_id });
            }
        }
    }

    let (disposition, command_ids) = if !pending.is_empty() {
        (
            ReconciliationDisposition::PendingEvidence,
            pending.into_iter().collect(),
        )
    } else {
        (ReconciliationDisposition::Completed, Vec::new())
    };

    Ok(EvaluatedReconciliationResult {
        evaluation: ReconciliationEvaluation {
            request_id: result.request_id.clone(),
            account_id: result.account_id.clone(),
            observed_at: Some(result.observed_at),
            disposition,
            command_ids,
            findings,
        },
        result,
    })
}

/// Converts an existing pending/manual evaluation into an explicit manual
/// reconciliation decision and derives CAS targets through `sinan-execution`.
///
/// Result evaluation alone never calls this path. The caller must supply a
/// durable, auditable escalation reason and server-time timestamp.
pub fn escalate_manual_reconciliation(
    context: &ReconciliationRequestContext,
    mut evaluation: ReconciliationEvaluation,
    commands: &[ReconciliationCommand],
    evidence: ManualEscalationEvidence,
) -> Result<ManualReconciliationEscalation, ReconciliationError> {
    validate_request_context(context)?;
    if evidence.request_id != context.request.request_id
        || evaluation.request_id != context.request.request_id
        || evaluation.account_id != context.request.account_id
    {
        return Err(ReconciliationError::manual(
            "request_id",
            "evidence and evaluation must belong to the request context",
        ));
    }
    if evidence.reason.trim().is_empty() {
        return Err(ReconciliationError::manual("reason", "must not be empty"));
    }
    if evidence.escalated_at < context.requested_at
        || evaluation
            .observed_at
            .is_some_and(|observed_at| evidence.escalated_at < observed_at)
    {
        return Err(ReconciliationError::manual(
            "escalated_at",
            "must not predate the request or evaluated result",
        ));
    }
    if evaluation.disposition != ReconciliationDisposition::PendingEvidence {
        return Err(ReconciliationError::manual(
            "evaluation.disposition",
            "only a pending result evaluation can be escalated",
        ));
    }
    if evaluation.observed_at.is_none() {
        return Err(ReconciliationError::manual(
            "evaluation.observed_at",
            "result escalation requires an observed result; use missing-result escalation otherwise",
        ));
    }
    if evaluation.command_ids.is_empty()
        || evaluation
            .command_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(ReconciliationError::manual(
            "evaluation.command_ids",
            "must be a non-empty unique sorted attention scope",
        ));
    }

    let indexed = validate_command_scope(context, commands, evidence.escalated_at)?;
    if let Some(targeted) = &context.request.command_ids {
        let target: BTreeSet<_> = targeted.iter().collect();
        if evaluation
            .command_ids
            .iter()
            .any(|command_id| !target.contains(command_id))
        {
            return Err(ReconciliationError::manual(
                "evaluation.command_ids",
                "contains a command outside the targeted request scope",
            ));
        }
    }
    let unknown_attention: BTreeSet<_> = evaluation
        .command_ids
        .iter()
        .filter(|command_id| !indexed.contains_key(*command_id))
        .cloned()
        .collect();
    let unknown_findings: BTreeSet<_> = evaluation
        .findings
        .iter()
        .filter_map(|finding| match finding {
            ReconciliationFinding::UnknownCommandReportedUnresolved { command_id }
            | ReconciliationFinding::UnknownCommandObservedInOrderSnapshot { command_id, .. } => {
                Some(command_id.clone())
            }
            _ => None,
        })
        .collect();
    if unknown_attention != unknown_findings {
        return Err(ReconciliationError::manual(
            "evaluation.findings",
            "unknown attention commands must have matching unknown-command findings",
        ));
    }
    let mut command_transitions = Vec::new();
    for command_id in &evaluation.command_ids {
        let Some(item) = indexed.get(command_id) else {
            // Account-wide reconciliation can surface a client-journal command
            // unknown to Core. There is no local lifecycle row to mutate; the
            // run-level manual state remains the fail-closed owner.
            continue;
        };
        if !matches!(
            item.state.status,
            ExecutionCommandStatus::DeliveryUnconfirmed
                | ExecutionCommandStatus::Reconciling
                | ExecutionCommandStatus::ManualReconciliationRequired
        ) {
            // A run may require manual work even when a local command row has
            // already advanced or is terminal. Do not regress or manufacture
            // a lifecycle transition merely to mirror the run disposition.
            continue;
        }
        let expected_status = item.state.status;
        let expected_updated_at = item.state.updated_at;
        let outcome = transition_command(
            &item.command,
            &item.state,
            CommandEvidence::RequireManualReconciliation {
                at: evidence.escalated_at,
            },
        )
        .map_err(|source| ReconciliationError::CommandTransition {
            command_id: command_id.clone(),
            source,
        })?;
        command_transitions.push(ReconciliationCommandTransition {
            command_id: command_id.clone(),
            expected_status,
            expected_updated_at,
            outcome,
        });
    }

    evaluation.disposition = ReconciliationDisposition::ManualRequired;
    Ok(ManualReconciliationEscalation {
        evidence,
        evaluation,
        command_transitions,
    })
}

/// Explicitly escalates a reconciliation request for which no result arrived.
///
/// This function does not own a timer and never runs implicitly. The caller
/// must provide durable timeout/operator evidence. Commands already beyond
/// delivery uncertainty are not regressed; the returned run-level evaluation
/// still records the missing result as manual work.
pub fn escalate_missing_reconciliation_result(
    context: &ReconciliationRequestContext,
    commands: &[ReconciliationCommand],
    evidence: ManualEscalationEvidence,
) -> Result<ManualReconciliationEscalation, ReconciliationError> {
    validate_request_context(context)?;
    if evidence.request_id != context.request.request_id {
        return Err(ReconciliationError::manual(
            "request_id",
            "evidence must belong to the request context",
        ));
    }
    if evidence.reason.trim().is_empty() {
        return Err(ReconciliationError::manual("reason", "must not be empty"));
    }
    if evidence.escalated_at < context.requested_at {
        return Err(ReconciliationError::manual(
            "escalated_at",
            "must not predate the request",
        ));
    }

    let indexed = validate_command_scope(context, commands, evidence.escalated_at)?;
    let mut command_transitions = Vec::new();
    for (command_id, item) in &indexed {
        if !matches!(
            item.state.status,
            ExecutionCommandStatus::DeliveryUnconfirmed
                | ExecutionCommandStatus::Reconciling
                | ExecutionCommandStatus::ManualReconciliationRequired
        ) {
            continue;
        }
        let expected_status = item.state.status;
        let expected_updated_at = item.state.updated_at;
        let outcome = transition_command(
            &item.command,
            &item.state,
            CommandEvidence::RequireManualReconciliation {
                at: evidence.escalated_at,
            },
        )
        .map_err(|source| ReconciliationError::CommandTransition {
            command_id: command_id.clone(),
            source,
        })?;
        command_transitions.push(ReconciliationCommandTransition {
            command_id: command_id.clone(),
            expected_status,
            expected_updated_at,
            outcome,
        });
    }

    Ok(ManualReconciliationEscalation {
        evaluation: ReconciliationEvaluation {
            request_id: context.request.request_id.clone(),
            account_id: context.request.account_id.clone(),
            observed_at: None,
            disposition: ReconciliationDisposition::ManualRequired,
            command_ids: indexed.keys().cloned().collect(),
            findings: vec![ReconciliationFinding::ReconciliationResultMissing {
                escalated_at: evidence.escalated_at,
            }],
        },
        evidence,
        command_transitions,
    })
}

fn index_orders_by_command<'a>(
    orders: &'a [OrderSnapshot],
) -> BTreeMap<CommandId, Vec<&'a OrderSnapshot>> {
    let mut indexed: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for order in orders {
        if let Some(command_id) = &order.command_id {
            indexed.entry(command_id.clone()).or_default().push(order);
        }
    }
    for orders in indexed.values_mut() {
        orders.sort_by(|left, right| left.broker_order_id.cmp(&right.broker_order_id));
    }
    indexed
}

fn order_may_belong_to_request_route(
    context: &ReconciliationRequestContext,
    order: &OrderSnapshot,
) -> bool {
    route_component_may_match(
        context
            .request
            .terminal_id
            .as_ref()
            .map(|value| value.as_str()),
        order.terminal_id.as_ref().map(|value| value.as_str()),
    ) && route_component_may_match(
        context
            .request
            .client_id
            .as_ref()
            .map(|value| value.as_str()),
        order.client_id.as_ref().map(|value| value.as_str()),
    )
}

fn route_component_may_match(requested: Option<&str>, observed: Option<&str>) -> bool {
    requested.is_none() || observed.is_none() || requested == observed
}

fn canonicalize_result_sets(result: &mut ReconciliationResult) {
    result
        .positions
        .sort_by(|left, right| left.position_id.cmp(&right.position_id));
    result
        .orders
        .sort_by(|left, right| left.broker_order_id.cmp(&right.broker_order_id));
    result.symbol_metadata.sort_by(|left, right| {
        left.broker_symbol
            .cmp(&right.broker_symbol)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    result.unresolved_command_ids.sort();
}

fn inspect_order_observations(
    command_id: &CommandId,
    command: &sinan_types::ExecutionCommand,
    orders: &[&OrderSnapshot],
    events: &[&ExecutionEvent],
    observed_at: i64,
    findings: &mut Vec<ReconciliationFinding>,
) {
    if orders.len() > 1 {
        findings.push(ReconciliationFinding::MultipleOrderSnapshotsForCommand {
            command_id: command_id.clone(),
            broker_order_ids: orders
                .iter()
                .map(|order| order.broker_order_id.clone())
                .collect(),
        });
    }

    let latest_event_at_observation = events
        .iter()
        .rev()
        .find(|event| event.event_at <= observed_at)
        .copied();
    for order in orders {
        for field in order_identity_conflicts(order, command) {
            findings.push(ReconciliationFinding::OrderIdentityConflict {
                command_id: command_id.clone(),
                broker_order_id: order.broker_order_id.clone(),
                field,
            });
        }
        if let Some(event) = latest_event_at_observation {
            if snapshot_conflicts_with_event(event.status, order.status) {
                findings.push(ReconciliationFinding::SnapshotConflictsWithExecutionEvent {
                    command_id: command_id.clone(),
                    broker_order_id: order.broker_order_id.clone(),
                    event_status: event.status,
                    snapshot_status: order.status,
                });
            }
        }
    }
}

fn has_authoritative_delivery_evidence(
    item: &ReconciliationCommand,
    events: &[&ExecutionEvent],
) -> bool {
    if item.state.command_received_at.is_some() {
        return true;
    }
    match item.state.status {
        ExecutionCommandStatus::Created | ExecutionCommandStatus::DeliveryFailed => true,
        ExecutionCommandStatus::Expired | ExecutionCommandStatus::Cancelled => {
            item.state.dispatched_at.is_none()
                || events
                    .iter()
                    .any(|event| event_supports_status(event.status, item.state.status))
        }
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
        | ExecutionCommandStatus::Failed => events
            .iter()
            .any(|event| event_supports_status(event.status, item.state.status)),
    }
}

fn event_supports_status(event: ExecutionEventStatus, status: ExecutionCommandStatus) -> bool {
    matches!(
        (event, status),
        (
            ExecutionEventStatus::Accepted,
            ExecutionCommandStatus::Accepted
        ) | (
            ExecutionEventStatus::OrderSent,
            ExecutionCommandStatus::OrderSent
        ) | (
            ExecutionEventStatus::Rejected,
            ExecutionCommandStatus::Rejected
        ) | (
            ExecutionEventStatus::PartiallyFilled,
            ExecutionCommandStatus::PartiallyFilled
        ) | (ExecutionEventStatus::Filled, ExecutionCommandStatus::Filled)
            | (ExecutionEventStatus::Failed, ExecutionCommandStatus::Failed)
            | (
                ExecutionEventStatus::Expired,
                ExecutionCommandStatus::Expired
            )
            | (
                ExecutionEventStatus::Cancelled,
                ExecutionCommandStatus::Cancelled
            )
    )
}

fn snapshot_conflicts_with_event(
    event_status: ExecutionEventStatus,
    snapshot_status: OrderSnapshotStatus,
) -> bool {
    match event_status {
        ExecutionEventStatus::Filled => snapshot_status != OrderSnapshotStatus::Filled,
        ExecutionEventStatus::Rejected => snapshot_status != OrderSnapshotStatus::Rejected,
        ExecutionEventStatus::Expired => snapshot_status != OrderSnapshotStatus::Expired,
        ExecutionEventStatus::Cancelled => snapshot_status != OrderSnapshotStatus::Cancelled,
        ExecutionEventStatus::Accepted
        | ExecutionEventStatus::OrderSent
        | ExecutionEventStatus::PartiallyFilled
        | ExecutionEventStatus::Failed => false,
    }
}

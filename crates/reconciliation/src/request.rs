use std::collections::BTreeSet;

use sinan_execution::{transition_command, CommandEvidence};
use sinan_protocol::ReconciliationRequest;
use sinan_types::ExecutionCommandStatus;

use crate::{
    validation::validate_command_scope, ReconciliationCommand, ReconciliationCommandTransition,
    ReconciliationError, ReconciliationRequestContext, ReconciliationRequestInput,
    ReconciliationRequestPlan,
};

/// Builds a transport-independent reconciliation request.
///
/// A targeted command scope is sorted for deterministic persistence and HMAC/
/// wire-envelope composition by later layers. Duplicate IDs are rejected
/// rather than silently collapsed. `None` retains its account-wide meaning.
pub fn build_reconciliation_request(
    mut input: ReconciliationRequestInput,
) -> Result<ReconciliationRequestContext, ReconciliationError> {
    if let Some(command_ids) = &mut input.command_ids {
        if command_ids.is_empty() {
            return Err(ReconciliationError::request(
                "command_ids",
                "Some scope must not be empty; use None for account-wide scope",
            ));
        }
        let unique: BTreeSet<_> = command_ids.iter().cloned().collect();
        if unique.len() != command_ids.len() {
            return Err(ReconciliationError::request(
                "command_ids",
                "must not contain duplicates",
            ));
        }
        *command_ids = unique.into_iter().collect();
    }

    let context = ReconciliationRequestContext {
        request: ReconciliationRequest {
            request_id: input.request_id,
            account_id: input.account_id,
            terminal_id: input.terminal_id,
            client_id: input.client_id,
            reason: input.reason,
            command_ids: input.command_ids,
            since_server_time: input.since_server_time,
        },
        requested_at: input.requested_at,
    };
    crate::validation::validate_request_context(&context)?;
    Ok(context)
}

/// Builds a request and derives every command-state CAS target accepted by the
/// execution state machine at request time.
///
/// Account-wide reconciliation may cover commands whose lifecycle has already
/// advanced beyond delivery uncertainty. Those commands stay in their current
/// lifecycle; the reconciliation run tracks them without regressing state.
pub fn plan_reconciliation_request(
    input: ReconciliationRequestInput,
    commands: &[ReconciliationCommand],
) -> Result<ReconciliationRequestPlan, ReconciliationError> {
    let context = build_reconciliation_request(input)?;
    let indexed = validate_command_scope(&context, commands, context.requested_at)?;
    let mut command_transitions = Vec::new();
    for item in indexed.values() {
        if !matches!(
            item.state.status,
            ExecutionCommandStatus::DeliveryUnconfirmed | ExecutionCommandStatus::Reconciling
        ) {
            continue;
        }
        let expected_status = item.state.status;
        let expected_updated_at = item.state.updated_at;
        let outcome = transition_command(
            &item.command,
            &item.state,
            CommandEvidence::BeginReconciliation {
                at: context.requested_at,
            },
        )
        .map_err(|source| ReconciliationError::CommandTransition {
            command_id: item.command.command_id.clone(),
            source,
        })?;
        command_transitions.push(ReconciliationCommandTransition {
            command_id: item.command.command_id.clone(),
            expected_status,
            expected_updated_at,
            outcome,
        });
    }
    Ok(ReconciliationRequestPlan {
        context,
        command_transitions,
    })
}

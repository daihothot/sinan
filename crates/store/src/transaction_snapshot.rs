//! Typed snapshots used by owner transactions.

use sinan_types::{CommandId, ExecutionCommandState, PlanId, RequestId};
use sqlx::{Row, SqliteConnection};

use crate::{
    reconciliation::fetch_reconciliation_run_on,
    repository::{
        execution_event_from_row, fetch_execution_command_by_id,
        fetch_execution_command_state_by_id, fetch_execution_workflow_by_plan_id,
        validate_persisted_command_state,
    },
    StoreError, StoredExecutionCommand, StoredExecutionEvent, StoredExecutionWorkflow,
    StoredReconciliationRun, WriteTransaction,
};

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionProjectionSnapshot {
    pub workflow: StoredExecutionWorkflow,
    pub events: Vec<StoredExecutionEvent>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredExecutionCommandLifecycle {
    pub command: StoredExecutionCommand,
    pub state: ExecutionCommandState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReconciliationEvaluationSnapshot {
    pub run: StoredReconciliationRun,
    pub commands: Vec<StoredExecutionCommandLifecycle>,
    pub events: Vec<StoredExecutionEvent>,
}

impl WriteTransaction {
    /// Loads the immutable workflow, every command state, and every plan event
    /// from this transaction's single SQLite snapshot.
    pub async fn load_execution_projection(
        &mut self,
        command_id: &CommandId,
    ) -> Result<Option<ExecutionProjectionSnapshot>, StoreError> {
        let plan_id: Option<String> =
            sqlx::query_scalar("SELECT plan_id FROM execution_commands WHERE command_id = ?")
                .bind(command_id.as_str())
                .fetch_optional(self.connection())
                .await?
                .flatten();
        let Some(plan_id) = plan_id else {
            return Ok(None);
        };
        let plan_id = PlanId::from(plan_id);
        let workflow = fetch_execution_workflow_by_plan_id(self.connection(), &plan_id)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "execution_projection_snapshot",
                    command_id.to_string(),
                    format!("parent workflow {plan_id} is missing"),
                )
            })?;
        if !workflow
            .commands
            .iter()
            .any(|command| command.command.command_id == *command_id)
        {
            return Err(StoreError::corrupt(
                "execution_projection_snapshot",
                command_id.to_string(),
                format!("command is not part of parent workflow {plan_id}"),
            ));
        }
        let events = load_plan_events(self.connection(), &plan_id).await?;
        Ok(Some(ExecutionProjectionSnapshot { workflow, events }))
    }

    /// Loads the exact targeted command set, or the complete account/route
    /// command set, required by reconciliation evaluation.
    pub async fn load_reconciliation_evaluation_snapshot(
        &mut self,
        request_id: &RequestId,
    ) -> Result<Option<ReconciliationEvaluationSnapshot>, StoreError> {
        let Some(run) = fetch_reconciliation_run_on(self.connection(), request_id).await? else {
            return Ok(None);
        };

        let command_ids = match &run.request.command_ids {
            Some(command_ids) => command_ids.clone(),
            None => load_route_command_ids(self.connection(), &run).await?,
        };
        let mut commands = Vec::with_capacity(command_ids.len());
        let mut events = Vec::new();
        for command_id in command_ids {
            let command = fetch_execution_command_by_id(self.connection(), &command_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "reconciliation_evaluation_snapshot",
                        request_id.to_string(),
                        format!("scoped command {command_id} is missing"),
                    )
                })?;
            validate_reconciliation_command_route(self.connection(), &run, &command).await?;
            let state = fetch_execution_command_state_by_id(self.connection(), &command_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "reconciliation_evaluation_snapshot",
                        request_id.to_string(),
                        format!("scoped command {command_id} has no lifecycle state"),
                    )
                })?;
            validate_persisted_command_state(&state, &command).map_err(|reason| {
                StoreError::corrupt("execution_command_state", command_id.to_string(), reason)
            })?;
            events.extend(load_command_events(self.connection(), &command_id).await?);
            commands.push(StoredExecutionCommandLifecycle { command, state });
        }
        commands.sort_by(|left, right| {
            left.command
                .command
                .command_id
                .cmp(&right.command.command.command_id)
        });
        events.sort_by(|left, right| {
            (left.event.event_at, &left.event.execution_id)
                .cmp(&(right.event.event_at, &right.event.execution_id))
        });

        Ok(Some(ReconciliationEvaluationSnapshot {
            run,
            commands,
            events,
        }))
    }
}

async fn load_route_command_ids(
    connection: &mut SqliteConnection,
    run: &StoredReconciliationRun,
) -> Result<Vec<CommandId>, StoreError> {
    let terminal_id = run.request.terminal_id.as_ref().map(|value| value.as_str());
    let client_id = run.request.client_id.as_ref().map(|value| value.as_str());
    let rows = sqlx::query(
        "SELECT commands.command_id FROM execution_commands AS commands \
         WHERE commands.account_id = ? AND ( \
           ((? IS NULL OR commands.terminal_id = ?) \
             AND (? IS NULL OR commands.client_id = ?)) \
           OR EXISTS ( \
             SELECT 1 FROM command_delivery_attempts AS attempts \
             INNER JOIN execution_client_sessions AS sessions \
               ON sessions.session_id = attempts.session_id \
             WHERE attempts.command_id = commands.command_id \
               AND sessions.account_id = ? \
               AND (? IS NULL OR sessions.terminal_id = ?) \
               AND (? IS NULL OR sessions.client_id = ?) \
           ) \
         ) ORDER BY commands.command_id",
    )
    .bind(run.request.account_id.as_str())
    .bind(terminal_id)
    .bind(terminal_id)
    .bind(client_id)
    .bind(client_id)
    .bind(run.request.account_id.as_str())
    .bind(terminal_id)
    .bind(terminal_id)
    .bind(client_id)
    .bind(client_id)
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter()
        .map(|row| row.try_get::<String, _>("command_id").map(CommandId::from))
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::from)
}

async fn validate_reconciliation_command_route(
    connection: &mut SqliteConnection,
    run: &StoredReconciliationRun,
    command: &StoredExecutionCommand,
) -> Result<(), StoreError> {
    let value = &command.command;
    let account_matches = value.account_id == run.request.account_id;
    let payload_route_matches = run
        .request
        .terminal_id
        .as_ref()
        .is_none_or(|terminal_id| value.terminal_id.as_ref() == Some(terminal_id))
        && run
            .request
            .client_id
            .as_ref()
            .is_none_or(|client_id| value.client_id.as_ref() == Some(client_id));
    let route_matches = if account_matches && !payload_route_matches {
        has_delivery_route_proof(connection, run, &value.command_id).await?
    } else {
        account_matches
    };
    if route_matches {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "reconciliation_evaluation_snapshot",
            run.request.request_id.to_string(),
            format!(
                "scoped command {} is outside the request route",
                value.command_id
            ),
        ))
    }
}

async fn has_delivery_route_proof(
    connection: &mut SqliteConnection,
    run: &StoredReconciliationRun,
    command_id: &CommandId,
) -> Result<bool, StoreError> {
    let terminal_id = run.request.terminal_id.as_ref().map(|value| value.as_str());
    let client_id = run.request.client_id.as_ref().map(|value| value.as_str());
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS ( \
           SELECT 1 FROM command_delivery_attempts AS attempts \
           INNER JOIN execution_client_sessions AS sessions \
             ON sessions.session_id = attempts.session_id \
           WHERE attempts.command_id = ? \
             AND sessions.account_id = ? \
             AND (? IS NULL OR sessions.terminal_id = ?) \
             AND (? IS NULL OR sessions.client_id = ?) \
         )",
    )
    .bind(command_id.as_str())
    .bind(run.request.account_id.as_str())
    .bind(terminal_id)
    .bind(terminal_id)
    .bind(client_id)
    .bind(client_id)
    .fetch_one(&mut *connection)
    .await?;
    Ok(exists != 0)
}

async fn load_plan_events(
    connection: &mut SqliteConnection,
    plan_id: &PlanId,
) -> Result<Vec<StoredExecutionEvent>, StoreError> {
    load_events(
        connection,
        "SELECT execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
                position_ticket, event_at, filled_at, payload_json, payload_hash, created_at \
         FROM execution_events WHERE plan_id = ? ORDER BY event_at, execution_id",
        plan_id.as_str(),
    )
    .await
}

async fn load_command_events(
    connection: &mut SqliteConnection,
    command_id: &CommandId,
) -> Result<Vec<StoredExecutionEvent>, StoreError> {
    load_events(
        connection,
        "SELECT execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
                position_ticket, event_at, filled_at, payload_json, payload_hash, created_at \
         FROM execution_events WHERE command_id = ? ORDER BY event_at, execution_id",
        command_id.as_str(),
    )
    .await
}

async fn load_events(
    connection: &mut SqliteConnection,
    query: &str,
    identity: &str,
) -> Result<Vec<StoredExecutionEvent>, StoreError> {
    sqlx::query(query)
        .bind(identity)
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(execution_event_from_row)
        .collect()
}

//! Authorized, bounded Control Plane queries.

use sinan_types::{CommandId, ExecutionCommandState, IntentId};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};

use crate::{
    projection::{load_latest_state_on, AuthorizedAccountScope, LatestStateProjection},
    repository::{
        execution_command_from_row, execution_command_state_from_row, execution_event_from_row,
        execution_plan_from_row, fetch_latest_circuit_breaker_snapshot, risk_result_from_row,
        session_from_row, trade_intent_from_row, RISK_RESULT_COLUMNS,
    },
    SqliteStateStore, StoreError, StoredCircuitBreakerSnapshot, StoredExecutionCommand,
    StoredExecutionEvent, StoredExecutionPlan, StoredRiskResult, StoredSessionRecord,
    StoredTradeIntent,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlPlaneStateLimits {
    pub sessions: usize,
    pub open_plans: usize,
    pub pending_commands: usize,
    pub recent_events: usize,
    pub latest_risk_results: usize,
}

impl Default for ControlPlaneStateLimits {
    fn default() -> Self {
        Self {
            sessions: 128,
            open_plans: 128,
            pending_commands: 512,
            recent_events: 512,
            latest_risk_results: 256,
        }
    }
}

impl ControlPlaneStateLimits {
    fn validate(self) -> Result<Self, StoreError> {
        for (field, value) in [
            ("sessions", self.sessions),
            ("open_plans", self.open_plans),
            ("pending_commands", self.pending_commands),
            ("recent_events", self.recent_events),
            ("latest_risk_results", self.latest_risk_results),
        ] {
            if value == 0 || i64::try_from(value).is_err() {
                return Err(StoreError::InvalidRecord {
                    entity: "control_plane_state_limits",
                    key: field.to_owned(),
                    reason: "must be a positive value representable by SQLite".to_owned(),
                });
            }
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ControlPlaneStateSnapshot {
    pub latest: LatestStateProjection,
    pub sessions: Vec<StoredSessionRecord>,
    pub open_plans: Vec<StoredExecutionPlan>,
    pub pending_commands: Vec<ExecutionCommandState>,
    pub recent_events: Vec<StoredExecutionEvent>,
    pub latest_risk_results: Vec<StoredRiskResult>,
    pub circuit_breaker: Option<StoredCircuitBreakerSnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TradeIntentWorkflowStatus {
    pub intent: StoredTradeIntent,
    pub latest_risk_result: Option<StoredRiskResult>,
    pub plan: Option<StoredExecutionPlan>,
    pub command_ids: Vec<CommandId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionCommandStatusBundle {
    pub state: ExecutionCommandState,
    pub command: StoredExecutionCommand,
    pub events: Vec<StoredExecutionEvent>,
}

impl SqliteStateStore {
    /// Loads every account-bound Control Plane projection from one SQLite read snapshot.
    pub async fn load_control_plane_state(
        &self,
        scope: &AuthorizedAccountScope,
        limits: ControlPlaneStateLimits,
    ) -> Result<ControlPlaneStateSnapshot, StoreError> {
        let limits = limits.validate()?;
        let mut transaction = self.pool.begin().await?;
        let result = async {
            let latest = if scope.is_empty() {
                LatestStateProjection::default()
            } else {
                load_latest_state_on(&mut transaction, scope).await?
            };
            let sessions = load_sessions(&mut transaction, scope, limits.sessions).await?;
            let open_plans = load_open_plans(&mut transaction, scope, limits.open_plans).await?;
            let pending_commands =
                load_pending_commands(&mut transaction, scope, limits.pending_commands).await?;
            let recent_events =
                load_recent_events(&mut transaction, scope, limits.recent_events).await?;
            let latest_risk_results =
                load_latest_risk_results(&mut transaction, scope, limits.latest_risk_results)
                    .await?;
            let circuit_breaker = fetch_latest_circuit_breaker_snapshot(&mut *transaction).await?;

            Ok(ControlPlaneStateSnapshot {
                latest,
                sessions,
                open_plans,
                pending_commands,
                recent_events,
                latest_risk_results,
                circuit_breaker,
            })
        }
        .await;

        match result {
            Ok(snapshot) => {
                transaction.commit().await?;
                Ok(snapshot)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Finds an intent only inside the caller's account scope and loads its workflow summary.
    pub async fn get_trade_intent_workflow_status(
        &self,
        scope: &AuthorizedAccountScope,
        intent_id: &IntentId,
    ) -> Result<Option<TradeIntentWorkflowStatus>, StoreError> {
        if scope.is_empty() {
            return Ok(None);
        }
        let mut transaction = self.pool.begin().await?;
        let result = load_trade_intent_workflow_status(&mut transaction, scope, intent_id).await;
        match result {
            Ok(status) => {
                transaction.commit().await?;
                Ok(status)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Finds a command only inside the caller's account scope and loads a bounded event history.
    pub async fn get_execution_command_status_bundle(
        &self,
        scope: &AuthorizedAccountScope,
        command_id: &CommandId,
        event_limit: usize,
    ) -> Result<Option<ExecutionCommandStatusBundle>, StoreError> {
        let event_limit = checked_limit("command_events", event_limit)?;
        if scope.is_empty() {
            return Ok(None);
        }
        let mut transaction = self.pool.begin().await?;
        let result =
            load_execution_command_status(&mut transaction, scope, command_id, event_limit).await;
        match result {
            Ok(status) => {
                transaction.commit().await?;
                Ok(status)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

fn checked_limit(field: &'static str, value: usize) -> Result<i64, StoreError> {
    if value == 0 {
        return Err(StoreError::InvalidRecord {
            entity: "control_plane_query_limit",
            key: field.to_owned(),
            reason: "must be positive".to_owned(),
        });
    }
    i64::try_from(value).map_err(|_| StoreError::InvalidRecord {
        entity: "control_plane_query_limit",
        key: field.to_owned(),
        reason: "does not fit in a SQLite INTEGER".to_owned(),
    })
}

fn push_scope(builder: &mut QueryBuilder<'_, Sqlite>, scope: &AuthorizedAccountScope) {
    builder.push("(");
    let mut accounts = builder.separated(", ");
    for account_id in scope.iter() {
        accounts.push_bind(account_id.to_string());
    }
    accounts.push_unseparated(")");
}

async fn load_sessions(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    limit: usize,
) -> Result<Vec<StoredSessionRecord>, StoreError> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::new(
        "SELECT session_id, client_id, account_id, terminal_id, platform, status, \
         capabilities_json, remote_addr, connected_at, last_heartbeat_at, last_time_sync_at, \
         clock_sync_status, disconnected_at, revision, updated_at, last_outbound_sequence, \
         max_inflight_commands FROM execution_client_sessions WHERE status != 'REJECTED' \
         AND account_id IN ",
    );
    push_scope(&mut query, scope);
    query
        .push(" ORDER BY updated_at DESC, session_id DESC LIMIT ")
        .push_bind(checked_limit("sessions", limit)?);
    let rows = query.build().fetch_all(&mut *connection).await?;
    let mut values = rows
        .into_iter()
        .map(session_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        (left.updated_at, &left.session_id).cmp(&(right.updated_at, &right.session_id))
    });
    Ok(values)
}

async fn load_open_plans(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    limit: usize,
) -> Result<Vec<StoredExecutionPlan>, StoreError> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::new(
        "SELECT plan_id, risk_id, intent_id, account_id, strategy_id, status, mode, \
         failure_policy, payload_json, payload_hash, created_at, updated_at \
         FROM execution_plans WHERE account_id IN ",
    );
    push_scope(&mut query, scope);
    query.push(
        " AND status IN ('PENDING', 'RECONCILING', 'MANUAL_RECONCILIATION_REQUIRED', 'PARTIAL') \
         ORDER BY updated_at DESC, plan_id DESC LIMIT ",
    );
    query.push_bind(checked_limit("open_plans", limit)?);
    let rows = query.build().fetch_all(&mut *connection).await?;
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(execution_plan_from_row(connection, row).await?);
    }
    values.sort_by(|left, right| {
        (left.updated_at, &left.plan.definition.plan_id)
            .cmp(&(right.updated_at, &right.plan.definition.plan_id))
    });
    Ok(values)
}

async fn load_pending_commands(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    limit: usize,
) -> Result<Vec<ExecutionCommandState>, StoreError> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::new(
        "SELECT command_id, account_id, plan_id, leg_id, status, delivery_attempts, \
         last_delivery_error, created_at, dispatched_at, command_received_at, reconciling_at, \
         completed_at, updated_at FROM execution_command_states WHERE account_id IN ",
    );
    push_scope(&mut query, scope);
    query.push(
        " AND status NOT IN ('REJECTED', 'FILLED', 'FAILED', 'EXPIRED', 'CANCELLED') \
         ORDER BY updated_at DESC, command_id DESC LIMIT ",
    );
    query.push_bind(checked_limit("pending_commands", limit)?);
    let rows = query.build().fetch_all(&mut *connection).await?;
    let mut values = rows
        .into_iter()
        .map(execution_command_state_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        (left.updated_at, &left.command_id).cmp(&(right.updated_at, &right.command_id))
    });
    Ok(values)
}

async fn load_recent_events(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    limit: usize,
) -> Result<Vec<StoredExecutionEvent>, StoreError> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::new(
        "SELECT execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
         position_ticket, event_at, filled_at, payload_json, payload_hash, created_at \
         FROM execution_events WHERE account_id IN ",
    );
    push_scope(&mut query, scope);
    query
        .push(" ORDER BY event_at DESC, execution_id DESC LIMIT ")
        .push_bind(checked_limit("recent_events", limit)?);
    let rows = query.build().fetch_all(&mut *connection).await?;
    let mut values = rows
        .into_iter()
        .map(execution_event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        (left.event.event_at, &left.event.execution_id)
            .cmp(&(right.event.event_at, &right.event.execution_id))
    });
    Ok(values)
}

async fn load_latest_risk_results(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    limit: usize,
) -> Result<Vec<StoredRiskResult>, StoreError> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::new(
        "WITH ranked AS (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.intent_id \
         ORDER BY r.evaluated_at DESC, r.risk_id DESC) AS result_rank \
         FROM risk_results r WHERE r.account_id IN ",
    );
    push_scope(&mut query, scope);
    query.push(") SELECT ");
    query.push(RISK_RESULT_COLUMNS);
    query.push(
        " FROM ranked r LEFT JOIN trade_intents i ON i.intent_id = r.intent_id \
         WHERE r.result_rank = 1 ORDER BY r.evaluated_at DESC, r.risk_id DESC LIMIT ",
    );
    query.push_bind(checked_limit("latest_risk_results", limit)?);
    let rows = query.build().fetch_all(&mut *connection).await?;
    let mut values = rows
        .into_iter()
        .map(risk_result_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        (left.result.evaluated_at, &left.result.risk_id)
            .cmp(&(right.result.evaluated_at, &right.result.risk_id))
    });
    Ok(values)
}

async fn load_trade_intent_workflow_status(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    intent_id: &IntentId,
) -> Result<Option<TradeIntentWorkflowStatus>, StoreError> {
    let mut intent_query = QueryBuilder::new(
        "SELECT intent_id, decision_id, strategy_id, account_id, symbol, action, status, \
         requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash, \
         created_at, updated_at FROM trade_intents WHERE intent_id = ",
    );
    intent_query.push_bind(intent_id.to_string());
    intent_query.push(" AND account_id IN ");
    push_scope(&mut intent_query, scope);
    let Some(intent_row) = intent_query
        .build()
        .fetch_optional(&mut *connection)
        .await?
    else {
        return Ok(None);
    };
    let intent = trade_intent_from_row(intent_row)?;

    let risk_query = format!(
        "SELECT {RISK_RESULT_COLUMNS} FROM risk_results r \
         LEFT JOIN trade_intents i ON i.intent_id = r.intent_id \
         WHERE r.intent_id = ? ORDER BY r.evaluated_at DESC, r.risk_id DESC LIMIT 1"
    );
    let latest_risk_result = sqlx::query(&risk_query)
        .bind(intent_id.as_str())
        .fetch_optional(&mut *connection)
        .await?
        .map(risk_result_from_row)
        .transpose()?;

    let plan_row = sqlx::query(
        "SELECT plan_id, risk_id, intent_id, account_id, strategy_id, status, mode, \
         failure_policy, payload_json, payload_hash, created_at, updated_at \
         FROM execution_plans WHERE intent_id = ? \
         ORDER BY updated_at DESC, plan_id DESC LIMIT 1",
    )
    .bind(intent_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let plan = match plan_row {
        Some(row) => Some(execution_plan_from_row(connection, row).await?),
        None => None,
    };

    let command_ids = if let Some(plan) = &plan {
        sqlx::query(
            "SELECT command_id FROM execution_commands WHERE plan_id = ? ORDER BY command_id",
        )
        .bind(plan.plan.definition.plan_id.as_str())
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("command_id").map(CommandId::from))
        .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    Ok(Some(TradeIntentWorkflowStatus {
        intent,
        latest_risk_result,
        plan,
        command_ids,
    }))
}

async fn load_execution_command_status(
    connection: &mut SqliteConnection,
    scope: &AuthorizedAccountScope,
    command_id: &CommandId,
    event_limit: i64,
) -> Result<Option<ExecutionCommandStatusBundle>, StoreError> {
    let mut state_query = QueryBuilder::new(
        "SELECT command_id, account_id, plan_id, leg_id, status, delivery_attempts, \
         last_delivery_error, created_at, dispatched_at, command_received_at, reconciling_at, \
         completed_at, updated_at FROM execution_command_states WHERE command_id = ",
    );
    state_query.push_bind(command_id.to_string());
    state_query.push(" AND account_id IN ");
    push_scope(&mut state_query, scope);
    let Some(state_row) = state_query.build().fetch_optional(&mut *connection).await? else {
        return Ok(None);
    };
    let state = execution_command_state_from_row(state_row)?;

    let command_row = sqlx::query(
        "SELECT command_id, risk_id, plan_id, leg_id, account_id, client_id, terminal_id, \
         symbol, action, expires_at, idempotency_key, payload_json, payload_hash, hmac, created_at \
         FROM execution_commands WHERE command_id = ? AND account_id = ?",
    )
    .bind(command_id.as_str())
    .bind(state.account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?
    .ok_or_else(|| {
        StoreError::corrupt(
            "execution_command_state",
            command_id.as_str(),
            "parent command is missing",
        )
    })?;
    let command = execution_command_from_row(command_row)?;

    let rows = sqlx::query(
        "SELECT execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
         position_ticket, event_at, filled_at, payload_json, payload_hash, created_at \
         FROM execution_events WHERE command_id = ? AND account_id = ? \
         ORDER BY event_at DESC, execution_id DESC LIMIT ?",
    )
    .bind(command_id.as_str())
    .bind(state.account_id.as_str())
    .bind(event_limit)
    .fetch_all(&mut *connection)
    .await?;
    let mut events = rows
        .into_iter()
        .map(execution_event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    events.sort_by(|left, right| {
        (left.event.event_at, &left.event.execution_id)
            .cmp(&(right.event.event_at, &right.event.execution_id))
    });

    Ok(Some(ExecutionCommandStatusBundle {
        state,
        command,
        events,
    }))
}

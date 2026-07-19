use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
};

use serde::de::DeserializeOwned;
use serde_json::Value;
use sinan_protocol::{
    decode_wire_message, ExecutionClientMessageType, ReconciliationRequest,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLegAction, CausationId, ClientId, ClockSyncStatus,
    CommandId, CorrelationId, ExecutionAction, ExecutionCommand, ExecutionCommandState,
    ExecutionCommandStatus, ExecutionEvent, ExecutionId, ExecutionLeg, ExecutionLegDefinition,
    ExecutionLegState, ExecutionLegStatus, ExecutionPlan, ExecutionPlanDefinition,
    ExecutionPlanState, ExecutionPlanStatus, IdempotencyKey, IntentId, LegId, MessageId, PlanId,
    RequestId, RiskId, RiskResult, SessionId, StrategyId, TerminalId, TradeIntent,
    TradeIntentAction, TradeIntentLegAction, TradeIntentStatus,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    connection::{SqliteStateStore, WriteTransaction},
    error::StoreError,
    json::CanonicalJson,
    model::{
        CircuitBreakerHeadMetadata, CommandStateUpdate, CoreEventMetadata,
        ExecutionLifecycleUpdate, LegStateUpdate, NewCircuitBreakerSnapshot, NewCoreEvent,
        NewExecutionCommand, NewExecutionEvent, NewExecutionPlan, NewExecutionWorkflow,
        NewRiskResult, NewSessionRecord, NewTradeIntent, NewWireInbox, NewWireOutbox,
        PlanStateUpdate, StoredCircuitBreakerSnapshot, StoredCoreEvent, StoredExecutionCommand,
        StoredExecutionEvent, StoredExecutionLeg, StoredExecutionPlan, StoredExecutionWorkflow,
        StoredRiskResult, StoredSessionRecord, StoredTradeIntent, StoredWireInbox,
        StoredWireOutbox, WriteOutcome, GLOBAL_CIRCUIT_BREAKER_SCOPE,
    },
};

const CORE_EVENT_COLUMNS: &str = "event_id, event_type, aggregate_type, aggregate_id, \
    message_id, schema_version, correlation_id, causation_id, account_id, client_id, \
    terminal_id, strategy_id, intent_id, plan_id, leg_id, command_id, idempotency_key, \
    event_at, received_at, created_at, source, payload_json, payload_hash";

const RISK_RESULT_COLUMNS: &str = "r.risk_id, r.intent_id, r.account_id, r.approved, r.reason, \
    r.snapshot_age_ms, r.symbol_metadata_age_ms, r.evaluated_at, r.valid_until, r.payload_json, \
    r.payload_hash, i.intent_id AS parent_intent_id, i.decision_id AS intent_decision_id, \
    i.strategy_id AS intent_strategy_id, i.account_id AS intent_account_id, \
    i.symbol AS intent_symbol, i.action AS intent_action, i.status AS intent_status, \
    i.requested_at AS intent_requested_at, i.signal_expires_at AS intent_signal_expires_at, \
    i.idempotency_key AS intent_idempotency_key, i.payload_json AS intent_payload_json, \
    i.payload_hash AS intent_payload_hash, i.created_at AS intent_created_at, \
    i.updated_at AS intent_updated_at";

impl SqliteStateStore {
    pub async fn write_circuit_breaker_snapshot(
        &self,
        snapshot: NewCircuitBreakerSnapshot,
    ) -> Result<WriteOutcome<StoredCircuitBreakerSnapshot>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.write_circuit_breaker_snapshot(snapshot).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_latest_circuit_breaker_snapshot(
        &self,
    ) -> Result<Option<StoredCircuitBreakerSnapshot>, StoreError> {
        fetch_latest_circuit_breaker_snapshot(self.pool()).await
    }

    pub async fn get_circuit_breaker_head_revision(&self) -> Result<Option<u64>, StoreError> {
        fetch_circuit_breaker_head_revision(self.pool()).await
    }

    /// Reads recovery ordering metadata without parsing or hashing the snapshot payload.
    pub async fn get_circuit_breaker_head_metadata(
        &self,
    ) -> Result<Option<CircuitBreakerHeadMetadata>, StoreError> {
        fetch_circuit_breaker_head_metadata(self.pool()).await
    }

    pub async fn append_core_event(
        &self,
        event: NewCoreEvent,
    ) -> Result<WriteOutcome<StoredCoreEvent>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.append_core_event(event).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_trade_intent(
        &self,
        intent: NewTradeIntent,
    ) -> Result<WriteOutcome<StoredTradeIntent>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_trade_intent(intent).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_risk_result(
        &self,
        result: NewRiskResult,
    ) -> Result<WriteOutcome<StoredRiskResult>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_risk_result(result).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_execution_plan(
        &self,
        plan: NewExecutionPlan,
    ) -> Result<WriteOutcome<StoredExecutionPlan>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_execution_plan(plan).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn update_execution_plan_state(
        &self,
        update: PlanStateUpdate,
    ) -> Result<StoredExecutionPlan, StoreError> {
        let mut transaction = self.begin_write().await?;
        let plan = transaction.update_execution_plan_state(update).await?;
        transaction.commit().await?;
        Ok(plan)
    }

    pub async fn update_execution_leg_state(
        &self,
        update: LegStateUpdate,
    ) -> Result<StoredExecutionLeg, StoreError> {
        let mut transaction = self.begin_write().await?;
        let leg = transaction.update_execution_leg_state(update).await?;
        transaction.commit().await?;
        Ok(leg)
    }

    pub async fn update_execution_lifecycle(
        &self,
        update: ExecutionLifecycleUpdate,
    ) -> Result<StoredExecutionPlan, StoreError> {
        let mut transaction = self.begin_write().await?;
        let plan = transaction.update_execution_lifecycle(update).await?;
        transaction.commit().await?;
        Ok(plan)
    }

    pub async fn commit_execution_workflow(
        &self,
        workflow: NewExecutionWorkflow,
    ) -> Result<WriteOutcome<StoredExecutionWorkflow>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.commit_execution_workflow(workflow).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_execution_command(
        &self,
        command: NewExecutionCommand,
    ) -> Result<WriteOutcome<StoredExecutionCommand>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_execution_command(command).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn append_execution_event(
        &self,
        event: NewExecutionEvent,
    ) -> Result<WriteOutcome<StoredExecutionEvent>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.append_execution_event(event).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_execution_command_state(
        &self,
        state: ExecutionCommandState,
    ) -> Result<WriteOutcome<ExecutionCommandState>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_execution_command_state(state).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn update_execution_command_state(
        &self,
        update: CommandStateUpdate,
    ) -> Result<ExecutionCommandState, StoreError> {
        let mut transaction = self.begin_write().await?;
        let state = transaction.update_execution_command_state(update).await?;
        transaction.commit().await?;
        Ok(state)
    }

    pub async fn record_wire_inbox(
        &self,
        message: NewWireInbox,
    ) -> Result<WriteOutcome<StoredWireInbox>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.record_wire_inbox(message).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn enqueue_wire_outbox(
        &self,
        message: NewWireOutbox,
    ) -> Result<WriteOutcome<StoredWireOutbox>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.enqueue_wire_outbox(message).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn insert_session(
        &self,
        session: NewSessionRecord,
    ) -> Result<WriteOutcome<StoredSessionRecord>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.insert_session(session).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_core_event(
        &self,
        event_id: &str,
    ) -> Result<Option<StoredCoreEvent>, StoreError> {
        fetch_core_event_by_id(self.pool(), event_id).await
    }

    pub async fn get_trade_intent(
        &self,
        intent_id: &IntentId,
    ) -> Result<Option<StoredTradeIntent>, StoreError> {
        fetch_trade_intent_by_id(self.pool(), intent_id).await
    }

    pub async fn get_risk_result(
        &self,
        risk_id: &RiskId,
    ) -> Result<Option<StoredRiskResult>, StoreError> {
        fetch_risk_result_by_id(self.pool(), risk_id).await
    }

    pub async fn get_execution_plan(
        &self,
        plan_id: &PlanId,
    ) -> Result<Option<StoredExecutionPlan>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_execution_plan_by_id(&mut connection, plan_id).await
    }

    pub async fn get_execution_leg(
        &self,
        leg_id: &LegId,
    ) -> Result<Option<StoredExecutionLeg>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_execution_leg_by_id(&mut connection, leg_id).await
    }

    pub async fn get_execution_workflow(
        &self,
        plan_id: &PlanId,
    ) -> Result<Option<StoredExecutionWorkflow>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_execution_workflow_by_plan_id(&mut connection, plan_id).await
    }

    pub async fn get_execution_command(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<StoredExecutionCommand>, StoreError> {
        fetch_execution_command_by_id(self.pool(), command_id).await
    }

    pub async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Option<StoredExecutionEvent>, StoreError> {
        fetch_execution_event_by_id(self.pool(), execution_id).await
    }

    pub async fn get_execution_command_state(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<ExecutionCommandState>, StoreError> {
        fetch_execution_command_state_by_id(self.pool(), command_id).await
    }

    pub async fn get_wire_inbox(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<StoredWireInbox>, StoreError> {
        fetch_wire_inbox_by_id(self.pool(), message_id).await
    }

    pub async fn get_wire_outbox(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<StoredWireOutbox>, StoreError> {
        fetch_wire_outbox_by_id(self.pool(), message_id).await
    }

    pub async fn get_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<StoredSessionRecord>, StoreError> {
        fetch_session_by_id(self.pool(), session_id).await
    }
}

impl WriteTransaction {
    pub async fn write_circuit_breaker_snapshot(
        &mut self,
        snapshot: NewCircuitBreakerSnapshot,
    ) -> Result<WriteOutcome<StoredCircuitBreakerSnapshot>, StoreError> {
        write_circuit_breaker_snapshot_on(self.connection(), snapshot).await
    }

    pub async fn get_latest_circuit_breaker_snapshot(
        &mut self,
    ) -> Result<Option<StoredCircuitBreakerSnapshot>, StoreError> {
        fetch_latest_circuit_breaker_snapshot(self.connection()).await
    }

    pub async fn get_circuit_breaker_head_revision(&mut self) -> Result<Option<u64>, StoreError> {
        fetch_circuit_breaker_head_revision(self.connection()).await
    }

    /// Reads recovery ordering metadata without parsing or hashing the snapshot payload.
    pub async fn get_circuit_breaker_head_metadata(
        &mut self,
    ) -> Result<Option<CircuitBreakerHeadMetadata>, StoreError> {
        fetch_circuit_breaker_head_metadata(self.connection()).await
    }

    pub async fn append_core_event(
        &mut self,
        event: NewCoreEvent,
    ) -> Result<WriteOutcome<StoredCoreEvent>, StoreError> {
        append_core_event_on(self.connection(), event).await
    }

    pub async fn insert_trade_intent(
        &mut self,
        intent: NewTradeIntent,
    ) -> Result<WriteOutcome<StoredTradeIntent>, StoreError> {
        insert_trade_intent_on(self.connection(), intent).await
    }

    pub async fn insert_risk_result(
        &mut self,
        result: NewRiskResult,
    ) -> Result<WriteOutcome<StoredRiskResult>, StoreError> {
        insert_risk_result_on(self.connection(), result).await
    }

    pub async fn insert_execution_plan(
        &mut self,
        plan: NewExecutionPlan,
    ) -> Result<WriteOutcome<StoredExecutionPlan>, StoreError> {
        insert_execution_plan_on(self.connection(), plan).await
    }

    pub async fn get_execution_plan(
        &mut self,
        plan_id: &PlanId,
    ) -> Result<Option<StoredExecutionPlan>, StoreError> {
        fetch_execution_plan_by_id(self.connection(), plan_id).await
    }

    pub async fn get_execution_leg(
        &mut self,
        leg_id: &LegId,
    ) -> Result<Option<StoredExecutionLeg>, StoreError> {
        fetch_execution_leg_by_id(self.connection(), leg_id).await
    }

    pub async fn update_execution_plan_state(
        &mut self,
        update: PlanStateUpdate,
    ) -> Result<StoredExecutionPlan, StoreError> {
        update_execution_plan_state_on(self.connection(), update).await
    }

    pub async fn update_execution_leg_state(
        &mut self,
        update: LegStateUpdate,
    ) -> Result<StoredExecutionLeg, StoreError> {
        update_execution_leg_state_on(self.connection(), update).await
    }

    pub async fn update_execution_lifecycle(
        &mut self,
        update: ExecutionLifecycleUpdate,
    ) -> Result<StoredExecutionPlan, StoreError> {
        update_execution_lifecycle_on(self.connection(), update).await
    }

    pub async fn commit_execution_workflow(
        &mut self,
        workflow: NewExecutionWorkflow,
    ) -> Result<WriteOutcome<StoredExecutionWorkflow>, StoreError> {
        commit_execution_workflow_on(self.connection(), workflow).await
    }

    pub async fn get_execution_workflow(
        &mut self,
        plan_id: &PlanId,
    ) -> Result<Option<StoredExecutionWorkflow>, StoreError> {
        fetch_execution_workflow_by_plan_id(self.connection(), plan_id).await
    }

    pub async fn get_risk_result(
        &mut self,
        risk_id: &RiskId,
    ) -> Result<Option<StoredRiskResult>, StoreError> {
        fetch_risk_result_by_id(self.connection(), risk_id).await
    }

    pub async fn insert_execution_command(
        &mut self,
        command: NewExecutionCommand,
    ) -> Result<WriteOutcome<StoredExecutionCommand>, StoreError> {
        insert_execution_command_on(self.connection(), command).await
    }

    pub async fn append_execution_event(
        &mut self,
        event: NewExecutionEvent,
    ) -> Result<WriteOutcome<StoredExecutionEvent>, StoreError> {
        append_execution_event_on(self.connection(), event).await
    }

    pub async fn insert_execution_command_state(
        &mut self,
        state: ExecutionCommandState,
    ) -> Result<WriteOutcome<ExecutionCommandState>, StoreError> {
        insert_execution_command_state_on(self.connection(), state).await
    }

    pub async fn update_execution_command_state(
        &mut self,
        update: CommandStateUpdate,
    ) -> Result<ExecutionCommandState, StoreError> {
        update_execution_command_state_on(self.connection(), update).await
    }

    pub async fn record_wire_inbox(
        &mut self,
        message: NewWireInbox,
    ) -> Result<WriteOutcome<StoredWireInbox>, StoreError> {
        record_wire_inbox_on(self.connection(), message).await
    }

    pub async fn enqueue_wire_outbox(
        &mut self,
        message: NewWireOutbox,
    ) -> Result<WriteOutcome<StoredWireOutbox>, StoreError> {
        enqueue_wire_outbox_on(self.connection(), message).await
    }

    pub async fn insert_session(
        &mut self,
        session: NewSessionRecord,
    ) -> Result<WriteOutcome<StoredSessionRecord>, StoreError> {
        insert_session_on(self.connection(), session).await
    }
}

pub(crate) async fn write_circuit_breaker_snapshot_on(
    connection: &mut SqliteConnection,
    snapshot: NewCircuitBreakerSnapshot,
) -> Result<WriteOutcome<StoredCircuitBreakerSnapshot>, StoreError> {
    validate_new_circuit_breaker_snapshot(&snapshot)?;
    let target_revision = match snapshot.expected_head_revision {
        Some(expected) => expected.checked_add(1).ok_or(StoreError::InvalidInteger {
            field: "circuit_breaker_snapshot.state_revision",
            value: expected,
        })?,
        None => 1,
    };
    let target_revision_i64 =
        positive_u64_to_i64("circuit_breaker_snapshot.state_revision", target_revision)?;
    let head = fetch_circuit_breaker_head_revision(&mut *connection).await?;

    let expected_matches_head = match (snapshot.expected_head_revision, head) {
        (None, None) => true,
        (Some(expected), Some(head)) => expected == head,
        _ => false,
    };
    if !expected_matches_head {
        if head == Some(target_revision) {
            let existing =
                fetch_circuit_breaker_snapshot_by_revision(&mut *connection, target_revision)
                    .await?
                    .ok_or_else(|| {
                        StoreError::corrupt(
                            "circuit_breaker_snapshot",
                            circuit_breaker_snapshot_key(target_revision),
                            "head revision row is missing",
                        )
                    })?;
            if same_circuit_breaker_snapshot(&existing, &snapshot) {
                return Ok(WriteOutcome::Duplicate(existing));
            }
        }
        return Err(stale_circuit_breaker_snapshot(
            snapshot.expected_head_revision,
        ));
    }

    let recovery_epoch_i64 = non_negative_u64_to_i64(
        "circuit_breaker_snapshot.recovery_epoch",
        snapshot.recovery_epoch,
    )?;
    let insert = sqlx::query(
        "INSERT INTO circuit_breaker_snapshots (\
            scope, state_revision, schema_version, status, recovery_epoch, updated_at, \
            payload_json, payload_hash\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(GLOBAL_CIRCUIT_BREAKER_SCOPE)
    .bind(target_revision_i64)
    .bind(&snapshot.schema_version)
    .bind(&snapshot.status)
    .bind(recovery_epoch_i64)
    .bind(snapshot.updated_at)
    .bind(snapshot.payload.as_str())
    .bind(snapshot.payload.sha256_hex())
    .execute(&mut *connection)
    .await?;

    let stored = StoredCircuitBreakerSnapshot {
        scope: GLOBAL_CIRCUIT_BREAKER_SCOPE.to_owned(),
        state_revision: target_revision,
        schema_version: snapshot.schema_version.clone(),
        status: snapshot.status.clone(),
        recovery_epoch: snapshot.recovery_epoch,
        updated_at: snapshot.updated_at,
        payload: snapshot.payload.clone(),
    };
    if insert.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(stored));
    }

    match fetch_circuit_breaker_snapshot_by_revision(&mut *connection, target_revision).await? {
        Some(existing) if same_circuit_breaker_snapshot(&existing, &snapshot) => {
            Ok(WriteOutcome::Duplicate(existing))
        }
        _ => Err(stale_circuit_breaker_snapshot(
            snapshot.expected_head_revision,
        )),
    }
}

async fn fetch_latest_circuit_breaker_snapshot<'e, E>(
    executor: E,
) -> Result<Option<StoredCircuitBreakerSnapshot>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT scope, state_revision, schema_version, status, recovery_epoch, updated_at, \
                payload_json, payload_hash \
         FROM circuit_breaker_snapshots WHERE scope = ? \
         ORDER BY state_revision DESC LIMIT 1",
    )
    .bind(GLOBAL_CIRCUIT_BREAKER_SCOPE)
    .fetch_optional(executor)
    .await?;
    row.map(circuit_breaker_snapshot_from_row).transpose()
}

async fn fetch_circuit_breaker_snapshot_by_revision<'e, E>(
    executor: E,
    state_revision: u64,
) -> Result<Option<StoredCircuitBreakerSnapshot>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let state_revision =
        positive_u64_to_i64("circuit_breaker_snapshot.state_revision", state_revision)?;
    let row = sqlx::query(
        "SELECT scope, state_revision, schema_version, status, recovery_epoch, updated_at, \
                payload_json, payload_hash \
         FROM circuit_breaker_snapshots WHERE scope = ? AND state_revision = ?",
    )
    .bind(GLOBAL_CIRCUIT_BREAKER_SCOPE)
    .bind(state_revision)
    .fetch_optional(executor)
    .await?;
    row.map(circuit_breaker_snapshot_from_row).transpose()
}

async fn fetch_circuit_breaker_head_revision<'e, E>(executor: E) -> Result<Option<u64>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let revision: Option<i64> = sqlx::query_scalar(
        "SELECT state_revision FROM circuit_breaker_snapshots \
         WHERE scope = ? ORDER BY state_revision DESC LIMIT 1",
    )
    .bind(GLOBAL_CIRCUIT_BREAKER_SCOPE)
    .fetch_optional(executor)
    .await?;
    revision
        .map(|revision| circuit_breaker_revision_from_i64("head", revision))
        .transpose()
}

async fn fetch_circuit_breaker_head_metadata<'e, E>(
    executor: E,
) -> Result<Option<CircuitBreakerHeadMetadata>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT state_revision, recovery_epoch FROM circuit_breaker_snapshots \
         WHERE scope = ? ORDER BY state_revision DESC LIMIT 1",
    )
    .bind(GLOBAL_CIRCUIT_BREAKER_SCOPE)
    .fetch_optional(executor)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let revision_i64: i64 = row.try_get("state_revision")?;
    let state_revision = circuit_breaker_revision_from_i64("head", revision_i64)?;
    let recovery_epoch_i64: i64 = row.try_get("recovery_epoch")?;
    let recovery_epoch = u64::try_from(recovery_epoch_i64).map_err(|_| {
        StoreError::corrupt(
            "circuit_breaker_snapshot",
            circuit_breaker_snapshot_key(state_revision),
            "recovery_epoch must be non-negative",
        )
    })?;
    Ok(Some(CircuitBreakerHeadMetadata {
        state_revision,
        recovery_epoch,
    }))
}

fn circuit_breaker_snapshot_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredCircuitBreakerSnapshot, StoreError> {
    let scope: String = row.try_get("scope")?;
    let revision_i64: i64 = row.try_get("state_revision")?;
    let state_revision = circuit_breaker_revision_from_i64(&scope, revision_i64)?;
    let key = circuit_breaker_snapshot_key(state_revision);
    if scope != GLOBAL_CIRCUIT_BREAKER_SCOPE {
        return Err(StoreError::corrupt(
            "circuit_breaker_snapshot",
            &key,
            format!("unsupported scope {scope:?}"),
        ));
    }

    let schema_version: String = row.try_get("schema_version")?;
    let status: String = row.try_get("status")?;
    let recovery_epoch_i64: i64 = row.try_get("recovery_epoch")?;
    let recovery_epoch = u64::try_from(recovery_epoch_i64).map_err(|_| {
        StoreError::corrupt(
            "circuit_breaker_snapshot",
            &key,
            "recovery_epoch must be non-negative",
        )
    })?;
    let updated_at: i64 = row.try_get("updated_at")?;
    if schema_version.trim().is_empty() {
        return Err(StoreError::corrupt(
            "circuit_breaker_snapshot",
            &key,
            "schema_version must not be empty",
        ));
    }
    if !is_circuit_breaker_status(&status) {
        return Err(StoreError::corrupt(
            "circuit_breaker_snapshot",
            &key,
            format!("invalid status {status:?}"),
        ));
    }
    if updated_at < 0 {
        return Err(StoreError::corrupt(
            "circuit_breaker_snapshot",
            &key,
            "updated_at must be non-negative",
        ));
    }

    let payload = CanonicalJson::from_stored(
        "circuit_breaker_snapshot",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    validate_circuit_breaker_payload_aliases(&schema_version, &status, recovery_epoch, &payload)
        .map_err(|reason| StoreError::corrupt("circuit_breaker_snapshot", &key, reason))?;

    Ok(StoredCircuitBreakerSnapshot {
        scope,
        state_revision,
        schema_version,
        status,
        recovery_epoch,
        updated_at,
        payload,
    })
}

fn validate_new_circuit_breaker_snapshot(
    snapshot: &NewCircuitBreakerSnapshot,
) -> Result<(), StoreError> {
    let key = match snapshot.expected_head_revision {
        Some(revision) => format!("expected_head_revision={revision}"),
        None => "expected_head_revision=none".to_owned(),
    };
    if snapshot.expected_head_revision == Some(0) {
        return Err(StoreError::InvalidSequence {
            field: "circuit_breaker_snapshot.expected_head_revision",
        });
    }
    if snapshot.schema_version.trim().is_empty() {
        return Err(StoreError::InvalidRecord {
            entity: "circuit_breaker_snapshot",
            key,
            reason: "schema_version must not be empty".to_owned(),
        });
    }
    if !is_circuit_breaker_status(&snapshot.status) {
        return Err(StoreError::InvalidRecord {
            entity: "circuit_breaker_snapshot",
            key,
            reason: format!("invalid status {:?}", snapshot.status),
        });
    }
    if snapshot.updated_at < 0 {
        return Err(StoreError::InvalidRecord {
            entity: "circuit_breaker_snapshot",
            key,
            reason: "updated_at must be non-negative".to_owned(),
        });
    }
    if let Some(expected) = snapshot.expected_head_revision {
        positive_u64_to_i64("circuit_breaker_snapshot.expected_head_revision", expected)?;
    }
    non_negative_u64_to_i64(
        "circuit_breaker_snapshot.recovery_epoch",
        snapshot.recovery_epoch,
    )?;
    validate_circuit_breaker_payload_aliases(
        &snapshot.schema_version,
        &snapshot.status,
        snapshot.recovery_epoch,
        &snapshot.payload,
    )
    .map_err(|reason| StoreError::InvalidRecord {
        entity: "circuit_breaker_snapshot",
        key,
        reason,
    })
}

fn validate_circuit_breaker_payload_aliases(
    schema_version: &str,
    status: &str,
    recovery_epoch: u64,
    payload: &CanonicalJson,
) -> Result<(), String> {
    let value: serde_json::Value = serde_json::from_str(payload.as_str())
        .map_err(|error| format!("payload_json is invalid: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "payload_json must be an object".to_owned())?;
    if object
        .get("schema_version")
        .and_then(serde_json::Value::as_str)
        != Some(schema_version)
    {
        return Err("schema_version does not match payload_json".to_owned());
    }
    if object
        .get("recovery_epoch")
        .and_then(serde_json::Value::as_u64)
        != Some(recovery_epoch)
    {
        return Err("recovery_epoch does not match payload_json".to_owned());
    }
    if object.get("status").and_then(serde_json::Value::as_str) != Some(status) {
        return Err("status does not match payload_json".to_owned());
    }
    Ok(())
}

fn same_circuit_breaker_snapshot(
    existing: &StoredCircuitBreakerSnapshot,
    incoming: &NewCircuitBreakerSnapshot,
) -> bool {
    existing.scope == GLOBAL_CIRCUIT_BREAKER_SCOPE
        && existing.schema_version == incoming.schema_version
        && existing.status == incoming.status
        && existing.recovery_epoch == incoming.recovery_epoch
        && existing.updated_at == incoming.updated_at
        && existing.payload == incoming.payload
}

fn is_circuit_breaker_status(status: &str) -> bool {
    matches!(status, "CLOSED" | "OPEN" | "HALF_OPEN")
}

fn circuit_breaker_snapshot_key(state_revision: u64) -> String {
    format!("scope={GLOBAL_CIRCUIT_BREAKER_SCOPE},state_revision={state_revision}")
}

fn stale_circuit_breaker_snapshot(expected_head_revision: Option<u64>) -> StoreError {
    StoreError::StaleWrite {
        entity: "circuit_breaker_snapshot",
        key: match expected_head_revision {
            Some(revision) => {
                format!("scope={GLOBAL_CIRCUIT_BREAKER_SCOPE},expected_head_revision={revision}")
            }
            None => format!("scope={GLOBAL_CIRCUIT_BREAKER_SCOPE},expected_head_revision=none"),
        },
    }
}

fn circuit_breaker_revision_from_i64(key: &str, revision: i64) -> Result<u64, StoreError> {
    if revision <= 0 {
        return Err(StoreError::corrupt(
            "circuit_breaker_snapshot",
            key,
            "state_revision must be greater than zero",
        ));
    }
    u64::try_from(revision).map_err(|_| {
        StoreError::corrupt(
            "circuit_breaker_snapshot",
            key,
            "state_revision does not fit in u64",
        )
    })
}

fn positive_u64_to_i64(field: &'static str, value: u64) -> Result<i64, StoreError> {
    if value == 0 {
        return Err(StoreError::InvalidSequence { field });
    }
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

fn non_negative_u64_to_i64(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionPlanJournal {
    risk_id: RiskId,
    intent_id: IntentId,
    definition: ExecutionPlanDefinition,
    legs: Vec<ExecutionLegDefinition>,
}

pub(crate) async fn insert_execution_plan_on(
    connection: &mut SqliteConnection,
    new_plan: NewExecutionPlan,
) -> Result<WriteOutcome<StoredExecutionPlan>, StoreError> {
    validate_new_execution_plan(connection, &new_plan).await?;
    let journal = ExecutionPlanJournal {
        risk_id: new_plan.risk_id.clone(),
        intent_id: new_plan.intent_id.clone(),
        definition: new_plan.plan.definition.clone(),
        legs: new_plan
            .plan
            .legs
            .iter()
            .map(|leg| leg.definition.clone())
            .collect(),
    };
    let payload = CanonicalJson::from_serializable(&journal)?;

    if let Some(existing) =
        fetch_execution_plan_by_id(&mut *connection, &new_plan.plan.definition.plan_id).await?
    {
        return resolve_execution_plan_replay(existing, &new_plan, &payload);
    }

    let plan = &new_plan.plan;
    let insert = sqlx::query(
        "INSERT INTO execution_plans (\
            plan_id, risk_id, intent_id, account_id, strategy_id, status, mode, failure_policy, \
            payload_json, payload_hash, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(plan.definition.plan_id.as_str())
    .bind(new_plan.risk_id.as_str())
    .bind(new_plan.intent_id.as_str())
    .bind(plan.definition.account_id.as_str())
    .bind(plan.definition.strategy_id.as_str())
    .bind(plan.state.status.as_str())
    .bind(plan.definition.mode.as_str())
    .bind(plan.definition.failure_policy.as_str())
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(new_plan.recorded_at)
    .bind(new_plan.recorded_at)
    .execute(&mut *connection)
    .await?;

    if insert.rows_affected() == 0 {
        return match fetch_execution_plan_by_id(&mut *connection, &new_plan.plan.definition.plan_id)
            .await?
        {
            Some(existing) => resolve_execution_plan_replay(existing, &new_plan, &payload),
            None => Err(StoreError::conflict(
                "execution_plan",
                format!("plan_id={}", new_plan.plan.definition.plan_id),
            )),
        };
    }

    for leg in &plan.legs {
        let leg_payload = CanonicalJson::from_serializable(&leg.definition)?;
        let result = sqlx::query(
            "INSERT INTO execution_legs (\
                leg_id, plan_id, symbol, action, status, payload_json, payload_hash, updated_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT DO NOTHING",
        )
        .bind(leg.definition.leg_id.as_str())
        .bind(plan.definition.plan_id.as_str())
        .bind(leg.definition.symbol.as_str())
        .bind(leg.definition.action.as_str())
        .bind(leg.state.status.as_str())
        .bind(leg_payload.as_str())
        .bind(leg_payload.sha256_hex())
        .bind(new_plan.recorded_at)
        .execute(&mut *connection)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::conflict(
                "execution_leg",
                format!("leg_id={}", leg.definition.leg_id),
            ));
        }
    }

    fetch_execution_plan_by_id(&mut *connection, &plan.definition.plan_id)
        .await?
        .map(WriteOutcome::Inserted)
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_plan",
                plan.definition.plan_id.as_str(),
                "inserted plan could not be read back",
            )
        })
}

async fn validate_new_execution_plan(
    connection: &mut SqliteConnection,
    new_plan: &NewExecutionPlan,
) -> Result<(), StoreError> {
    let key = format!("plan_id={}", new_plan.plan.definition.plan_id);
    new_plan
        .plan
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "execution_plan",
            key: key.clone(),
            reason: error.to_string(),
        })?;
    if new_plan.recorded_at < 0 {
        return Err(StoreError::InvalidRecord {
            entity: "execution_plan",
            key,
            reason: "recorded_at must be non-negative".to_owned(),
        });
    }
    if new_plan.plan.state.status != ExecutionPlanStatus::Pending
        || !new_plan.plan.state.filled_legs.is_empty()
        || !new_plan.plan.state.failed_legs.is_empty()
        || new_plan
            .plan
            .legs
            .iter()
            .any(|leg| leg.state.status != ExecutionLegStatus::Pending)
    {
        return Err(StoreError::InvalidRecord {
            entity: "execution_plan",
            key,
            reason: "new plan and every leg must be in the initial PENDING state".to_owned(),
        });
    }

    let risk = fetch_risk_result_by_id(&mut *connection, &new_plan.risk_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "risk_result",
            key: format!("risk_id={}", new_plan.risk_id),
        })?;
    let intent = fetch_trade_intent_by_id(&mut *connection, &new_plan.intent_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "trade_intent",
            key: format!("intent_id={}", new_plan.intent_id),
        })?;
    validate_execution_plan_graph(
        &new_plan.plan,
        &new_plan.risk_id,
        &new_plan.intent_id,
        new_plan.recorded_at,
        &risk,
        &intent,
    )
    .map_err(|reason| StoreError::InvalidRecord {
        entity: "execution_plan",
        key: format!("plan_id={}", new_plan.plan.definition.plan_id),
        reason,
    })
}

fn resolve_execution_plan_replay(
    existing: StoredExecutionPlan,
    incoming: &NewExecutionPlan,
    payload: &CanonicalJson,
) -> Result<WriteOutcome<StoredExecutionPlan>, StoreError> {
    if existing.risk_id == incoming.risk_id
        && existing.intent_id == incoming.intent_id
        && existing.payload == *payload
        && existing.created_at == incoming.recorded_at
    {
        Ok(WriteOutcome::Duplicate(existing))
    } else {
        Err(StoreError::conflict(
            "execution_plan",
            format!("plan_id={}", incoming.plan.definition.plan_id),
        ))
    }
}

async fn fetch_execution_plan_by_id(
    connection: &mut SqliteConnection,
    plan_id: &PlanId,
) -> Result<Option<StoredExecutionPlan>, StoreError> {
    let row = sqlx::query(
        "SELECT plan_id, risk_id, intent_id, account_id, strategy_id, status, mode, \
                failure_policy, payload_json, payload_hash, created_at, updated_at \
         FROM execution_plans WHERE plan_id = ?",
    )
    .bind(plan_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    match row {
        Some(row) => execution_plan_from_row(connection, row).await.map(Some),
        None => Ok(None),
    }
}

async fn execution_plan_from_row(
    connection: &mut SqliteConnection,
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredExecutionPlan, StoreError> {
    let key: String = row.try_get("plan_id")?;
    let payload = CanonicalJson::from_stored(
        "execution_plan",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let journal: ExecutionPlanJournal = deserialize_payload("execution_plan", &key, &payload)?;
    validate_column(
        "execution_plan",
        &key,
        "risk_id",
        &row.try_get::<String, _>("risk_id")?,
        journal.risk_id.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "intent_id",
        &row.try_get::<String, _>("intent_id")?,
        journal.intent_id.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "plan_id",
        &key,
        journal.definition.plan_id.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "account_id",
        &row.try_get::<String, _>("account_id")?,
        journal.definition.account_id.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "strategy_id",
        &row.try_get::<String, _>("strategy_id")?,
        journal.definition.strategy_id.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "mode",
        &row.try_get::<String, _>("mode")?,
        journal.definition.mode.as_str(),
    )?;
    validate_column(
        "execution_plan",
        &key,
        "failure_policy",
        &row.try_get::<String, _>("failure_policy")?,
        journal.definition.failure_policy.as_str(),
    )?;

    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    if created_at < 0 || updated_at < created_at {
        return Err(StoreError::corrupt(
            "execution_plan",
            &key,
            "created_at/updated_at lifecycle is invalid",
        ));
    }
    let status: ExecutionPlanStatus =
        parse_enum_column("execution_plan", &key, "status", row.try_get("status")?)?;

    let leg_rows = sqlx::query(
        "SELECT leg_id, plan_id, symbol, action, status, payload_json, payload_hash, updated_at \
         FROM execution_legs WHERE plan_id = ?",
    )
    .bind(&key)
    .fetch_all(&mut *connection)
    .await?;
    let mut stored_legs = HashMap::with_capacity(leg_rows.len());
    for leg_row in leg_rows {
        let stored = execution_leg_from_row(leg_row)?;
        if stored.updated_at < created_at {
            return Err(StoreError::corrupt(
                "execution_leg",
                stored.leg.definition.leg_id.as_str(),
                "updated_at precedes parent plan created_at",
            ));
        }
        if stored_legs
            .insert(stored.leg.definition.leg_id.clone(), stored)
            .is_some()
        {
            return Err(StoreError::corrupt(
                "execution_plan",
                &key,
                "contains duplicate leg rows",
            ));
        }
    }
    if stored_legs.len() != journal.legs.len() {
        return Err(StoreError::corrupt(
            "execution_plan",
            &key,
            "leg rows do not match the immutable plan journal",
        ));
    }

    let mut legs = Vec::with_capacity(journal.legs.len());
    for expected in &journal.legs {
        let stored = stored_legs.remove(&expected.leg_id).ok_or_else(|| {
            StoreError::corrupt(
                "execution_plan",
                &key,
                format!("missing journal leg {}", expected.leg_id),
            )
        })?;
        let expected_payload = CanonicalJson::from_serializable(expected).map_err(|error| {
            StoreError::corrupt(
                "execution_plan",
                &key,
                format!("journal leg cannot canonicalize: {error}"),
            )
        })?;
        if stored.payload != expected_payload {
            return Err(StoreError::corrupt(
                "execution_plan",
                &key,
                format!("leg {} differs from the immutable journal", expected.leg_id),
            ));
        }
        legs.push(stored.leg);
    }

    let filled_legs = legs
        .iter()
        .filter(|leg| leg.state.status == ExecutionLegStatus::Filled)
        .map(|leg| leg.definition.leg_id.clone())
        .collect();
    let failed_legs = legs
        .iter()
        .filter(|leg| {
            matches!(
                leg.state.status,
                ExecutionLegStatus::Rejected | ExecutionLegStatus::Failed
            )
        })
        .map(|leg| leg.definition.leg_id.clone())
        .collect();
    let plan = ExecutionPlan {
        definition: journal.definition,
        legs,
        state: ExecutionPlanState {
            status,
            filled_legs,
            failed_legs,
        },
    };
    plan.validate().map_err(|error| {
        StoreError::corrupt(
            "execution_plan",
            &key,
            format!("payload failed semantic validation: {error}"),
        )
    })?;

    let risk_id = journal.risk_id;
    let intent_id = journal.intent_id;
    let risk = fetch_risk_result_by_id(&mut *connection, &risk_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_plan",
                &key,
                format!("parent risk result {risk_id} is missing"),
            )
        })?;
    let intent = fetch_trade_intent_by_id(&mut *connection, &intent_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_plan",
                &key,
                format!("parent trade intent {intent_id} is missing"),
            )
        })?;
    validate_execution_plan_graph(&plan, &risk_id, &intent_id, created_at, &risk, &intent)
        .map_err(|reason| StoreError::corrupt("execution_plan", &key, reason))?;

    Ok(StoredExecutionPlan {
        plan,
        risk_id,
        intent_id,
        payload,
        created_at,
        updated_at,
    })
}

fn execution_leg_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredExecutionLeg, StoreError> {
    let key: String = row.try_get("leg_id")?;
    let payload = CanonicalJson::from_stored(
        "execution_leg",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let definition: ExecutionLegDefinition = deserialize_payload("execution_leg", &key, &payload)?;
    validate_column(
        "execution_leg",
        &key,
        "leg_id",
        &key,
        definition.leg_id.as_str(),
    )?;
    validate_column(
        "execution_leg",
        &key,
        "symbol",
        &row.try_get::<String, _>("symbol")?,
        definition.symbol.as_str(),
    )?;
    validate_column(
        "execution_leg",
        &key,
        "action",
        &row.try_get::<String, _>("action")?,
        definition.action.as_str(),
    )?;
    let updated_at: i64 = row.try_get("updated_at")?;
    if updated_at < 0 {
        return Err(StoreError::corrupt(
            "execution_leg",
            &key,
            "updated_at must be non-negative",
        ));
    }
    let leg = ExecutionLeg {
        definition,
        state: ExecutionLegState {
            status: parse_enum_column("execution_leg", &key, "status", row.try_get("status")?)?,
        },
    };
    leg.validate().map_err(|error| {
        StoreError::corrupt(
            "execution_leg",
            &key,
            format!("payload failed semantic validation: {error}"),
        )
    })?;
    Ok(StoredExecutionLeg {
        plan_id: PlanId::from(row.try_get::<String, _>("plan_id")?),
        leg,
        payload,
        updated_at,
    })
}

async fn fetch_execution_leg_by_id(
    connection: &mut SqliteConnection,
    leg_id: &LegId,
) -> Result<Option<StoredExecutionLeg>, StoreError> {
    let row = sqlx::query(
        "SELECT leg_id, plan_id, symbol, action, status, payload_json, payload_hash, updated_at \
         FROM execution_legs WHERE leg_id = ?",
    )
    .bind(leg_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let leg = execution_leg_from_row(row)?;
    let plan = fetch_execution_plan_by_id(&mut *connection, &leg.plan_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_leg",
                leg_id.as_str(),
                "parent execution plan is missing",
            )
        })?;
    if !plan.plan.legs.iter().any(|candidate| candidate == &leg.leg) {
        return Err(StoreError::corrupt(
            "execution_leg",
            leg_id.as_str(),
            "leg is not part of its parent plan journal",
        ));
    }
    Ok(Some(leg))
}

pub(crate) async fn update_execution_plan_state_on(
    connection: &mut SqliteConnection,
    update: PlanStateUpdate,
) -> Result<StoredExecutionPlan, StoreError> {
    let current = fetch_execution_plan_by_id(&mut *connection, &update.plan_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_plan",
            key: format!("plan_id={}", update.plan_id),
        })?;
    let mut proposed = current.plan.clone();
    proposed.state = update.state.clone();
    proposed
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "execution_plan_state",
            key: format!("plan_id={}", update.plan_id),
            reason: error.to_string(),
        })?;
    if update.state.filled_legs != current.plan.state.filled_legs
        || update.state.failed_legs != current.plan.state.failed_legs
    {
        return Err(StoreError::InvalidRecord {
            entity: "execution_plan_state",
            key: format!("plan_id={}", update.plan_id),
            reason: "filled_legs/failed_legs must be derived from persisted leg states".to_owned(),
        });
    }

    let result = sqlx::query(
        "UPDATE execution_plans SET status = ?, updated_at = ? \
         WHERE plan_id = ? AND status = ? AND updated_at = ? AND updated_at < ?",
    )
    .bind(update.state.status.as_str())
    .bind(update.updated_at)
    .bind(update.plan_id.as_str())
    .bind(update.expected_status.as_str())
    .bind(update.expected_updated_at)
    .bind(update.updated_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() == 1 {
        return fetch_execution_plan_by_id(&mut *connection, &update.plan_id)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "execution_plan",
                key: format!("plan_id={}", update.plan_id),
            });
    }
    match fetch_execution_plan_by_id(&mut *connection, &update.plan_id).await? {
        Some(existing)
            if existing.plan.state == update.state && existing.updated_at == update.updated_at =>
        {
            Ok(existing)
        }
        Some(_) => Err(StoreError::StaleWrite {
            entity: "execution_plan_state",
            key: format!("plan_id={}", update.plan_id),
        }),
        None => Err(StoreError::NotFound {
            entity: "execution_plan",
            key: format!("plan_id={}", update.plan_id),
        }),
    }
}

pub(crate) async fn update_execution_leg_state_on(
    connection: &mut SqliteConnection,
    update: LegStateUpdate,
) -> Result<StoredExecutionLeg, StoreError> {
    let current = fetch_execution_leg_by_id(&mut *connection, &update.leg_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_leg",
            key: format!("leg_id={}", update.leg_id),
        })?;
    if current.plan_id != update.plan_id {
        return Err(StoreError::conflict(
            "execution_leg_state",
            format!("leg_id={}", update.leg_id),
        ));
    }
    let mut proposed = current.leg.clone();
    proposed.state = update.state.clone();
    proposed
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "execution_leg_state",
            key: format!("leg_id={}", update.leg_id),
            reason: error.to_string(),
        })?;
    let current_plan = fetch_execution_plan_by_id(&mut *connection, &update.plan_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_plan",
            key: format!("plan_id={}", update.plan_id),
        })?;
    let mut projected_plan = current_plan.plan;
    let projected_leg = projected_plan
        .legs
        .iter_mut()
        .find(|leg| leg.definition.leg_id == update.leg_id)
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_plan",
                update.plan_id.as_str(),
                format!("plan journal is missing leg {}", update.leg_id),
            )
        })?;
    projected_leg.state = update.state.clone();
    projected_plan
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "execution_leg_state",
            key: format!("leg_id={}", update.leg_id),
            reason: format!(
                "standalone leg update would make the plan projection inconsistent; use update_execution_lifecycle: {error}"
            ),
        })?;

    let result = sqlx::query(
        "UPDATE execution_legs SET status = ?, updated_at = ? \
         WHERE plan_id = ? AND leg_id = ? AND status = ? AND updated_at = ? AND updated_at < ?",
    )
    .bind(update.state.status.as_str())
    .bind(update.updated_at)
    .bind(update.plan_id.as_str())
    .bind(update.leg_id.as_str())
    .bind(update.expected_status.as_str())
    .bind(update.expected_updated_at)
    .bind(update.updated_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() == 1 {
        return fetch_execution_leg_by_id(&mut *connection, &update.leg_id)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "execution_leg",
                key: format!("leg_id={}", update.leg_id),
            });
    }
    match fetch_execution_leg_by_id(&mut *connection, &update.leg_id).await? {
        Some(existing)
            if existing.leg.state == update.state && existing.updated_at == update.updated_at =>
        {
            Ok(existing)
        }
        Some(existing) if existing.plan_id != update.plan_id => Err(StoreError::conflict(
            "execution_leg_state",
            format!("leg_id={}", update.leg_id),
        )),
        Some(_) => Err(StoreError::StaleWrite {
            entity: "execution_leg_state",
            key: format!("leg_id={}", update.leg_id),
        }),
        None => Err(StoreError::NotFound {
            entity: "execution_leg",
            key: format!("leg_id={}", update.leg_id),
        }),
    }
}

pub(crate) async fn update_execution_lifecycle_on(
    connection: &mut SqliteConnection,
    update: ExecutionLifecycleUpdate,
) -> Result<StoredExecutionPlan, StoreError> {
    validate_execution_lifecycle_update_shape(&update)?;
    sqlx::query("SAVEPOINT execution_lifecycle_bundle")
        .execute(&mut *connection)
        .await?;
    let result = update_execution_lifecycle_in_savepoint(connection, &update).await;
    match result {
        Ok(plan) => {
            sqlx::query("RELEASE SAVEPOINT execution_lifecycle_bundle")
                .execute(&mut *connection)
                .await?;
            Ok(plan)
        }
        Err(error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT execution_lifecycle_bundle")
                .execute(&mut *connection)
                .await?;
            sqlx::query("RELEASE SAVEPOINT execution_lifecycle_bundle")
                .execute(&mut *connection)
                .await?;
            Err(error)
        }
    }
}

fn validate_execution_lifecycle_update_shape(
    update: &ExecutionLifecycleUpdate,
) -> Result<(), StoreError> {
    let key = format!("plan_id={}", update.plan.plan_id);
    if update.legs.is_empty() {
        return Err(StoreError::InvalidRecord {
            entity: "execution_lifecycle",
            key,
            reason: "at least one leg update is required".to_owned(),
        });
    }
    let mut leg_ids = HashSet::with_capacity(update.legs.len());
    for leg in &update.legs {
        if leg.plan_id != update.plan.plan_id {
            return Err(StoreError::InvalidRecord {
                entity: "execution_lifecycle",
                key,
                reason: format!("leg {} belongs to another plan", leg.leg_id),
            });
        }
        if !leg_ids.insert(leg.leg_id.clone()) {
            return Err(StoreError::InvalidRecord {
                entity: "execution_lifecycle",
                key,
                reason: format!("duplicate leg update for {}", leg.leg_id),
            });
        }
    }
    Ok(())
}

async fn update_execution_lifecycle_in_savepoint(
    connection: &mut SqliteConnection,
    update: &ExecutionLifecycleUpdate,
) -> Result<StoredExecutionPlan, StoreError> {
    let plan_id = &update.plan.plan_id;
    let current = fetch_execution_plan_by_id(&mut *connection, plan_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_plan",
            key: format!("plan_id={plan_id}"),
        })?;

    let leg_rows = sqlx::query(
        "SELECT leg_id, plan_id, symbol, action, status, payload_json, payload_hash, updated_at \
         FROM execution_legs WHERE plan_id = ?",
    )
    .bind(plan_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let mut stored_legs = HashMap::with_capacity(leg_rows.len());
    for row in leg_rows {
        let leg = execution_leg_from_row(row)?;
        stored_legs.insert(leg.leg.definition.leg_id.clone(), leg);
    }

    let exact_plan_replay =
        current.plan.state == update.plan.state && current.updated_at == update.plan.updated_at;
    let exact_leg_replay = update.legs.iter().all(|leg| {
        stored_legs.get(&leg.leg_id).is_some_and(|stored| {
            stored.plan_id == leg.plan_id
                && stored.leg.state == leg.state
                && stored.updated_at == leg.updated_at
        })
    });
    if exact_plan_replay && exact_leg_replay {
        return Ok(current);
    }

    if current.plan.state.status != update.plan.expected_status
        || current.updated_at != update.plan.expected_updated_at
        || update.plan.updated_at <= update.plan.expected_updated_at
    {
        return Err(StoreError::StaleWrite {
            entity: "execution_lifecycle",
            key: format!("plan_id={plan_id}"),
        });
    }
    for leg in &update.legs {
        let Some(stored) = stored_legs.get(&leg.leg_id) else {
            return Err(StoreError::NotFound {
                entity: "execution_leg",
                key: format!("leg_id={}", leg.leg_id),
            });
        };
        if stored.plan_id != leg.plan_id {
            return Err(StoreError::conflict(
                "execution_lifecycle",
                format!("leg_id={}", leg.leg_id),
            ));
        }
        if stored.leg.state.status != leg.expected_status
            || stored.updated_at != leg.expected_updated_at
            || leg.updated_at <= leg.expected_updated_at
        {
            return Err(StoreError::StaleWrite {
                entity: "execution_lifecycle",
                key: format!("leg_id={}", leg.leg_id),
            });
        }
    }

    let mut final_plan = current.plan.clone();
    for leg_update in &update.legs {
        let leg = final_plan
            .legs
            .iter_mut()
            .find(|leg| leg.definition.leg_id == leg_update.leg_id)
            .ok_or_else(|| {
                StoreError::corrupt(
                    "execution_plan",
                    plan_id.as_str(),
                    format!("plan journal is missing leg {}", leg_update.leg_id),
                )
            })?;
        leg.state = leg_update.state.clone();
    }
    final_plan.state = update.plan.state.clone();
    final_plan
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "execution_lifecycle",
            key: format!("plan_id={plan_id}"),
            reason: error.to_string(),
        })?;
    let updated_leg_ids: HashSet<_> = update.legs.iter().map(|leg| &leg.leg_id).collect();
    let latest_leg_update = update
        .legs
        .iter()
        .map(|leg| leg.updated_at)
        .chain(
            stored_legs
                .values()
                .filter(|stored| !updated_leg_ids.contains(&stored.leg.definition.leg_id))
                .map(|stored| stored.updated_at),
        )
        .max()
        .unwrap_or(current.created_at);
    if update.plan.updated_at < latest_leg_update {
        return Err(StoreError::InvalidRecord {
            entity: "execution_lifecycle",
            key: format!("plan_id={plan_id}"),
            reason: "plan updated_at must not precede any final leg projection".to_owned(),
        });
    }

    for leg in &update.legs {
        let result = sqlx::query(
            "UPDATE execution_legs SET status = ?, updated_at = ? \
             WHERE plan_id = ? AND leg_id = ? AND status = ? AND updated_at = ? \
               AND updated_at < ?",
        )
        .bind(leg.state.status.as_str())
        .bind(leg.updated_at)
        .bind(leg.plan_id.as_str())
        .bind(leg.leg_id.as_str())
        .bind(leg.expected_status.as_str())
        .bind(leg.expected_updated_at)
        .bind(leg.updated_at)
        .execute(&mut *connection)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StoreError::StaleWrite {
                entity: "execution_lifecycle",
                key: format!("leg_id={}", leg.leg_id),
            });
        }
    }
    let result = sqlx::query(
        "UPDATE execution_plans SET status = ?, updated_at = ? \
         WHERE plan_id = ? AND status = ? AND updated_at = ? AND updated_at < ?",
    )
    .bind(update.plan.state.status.as_str())
    .bind(update.plan.updated_at)
    .bind(plan_id.as_str())
    .bind(update.plan.expected_status.as_str())
    .bind(update.plan.expected_updated_at)
    .bind(update.plan.updated_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "execution_lifecycle",
            key: format!("plan_id={plan_id}"),
        });
    }

    fetch_execution_plan_by_id(&mut *connection, plan_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_plan",
            key: format!("plan_id={plan_id}"),
        })
}

fn validate_execution_plan_graph(
    plan: &ExecutionPlan,
    risk_id: &RiskId,
    intent_id: &IntentId,
    created_at: i64,
    risk: &StoredRiskResult,
    intent: &StoredTradeIntent,
) -> Result<(), String> {
    if risk.result.risk_id != *risk_id
        || risk.result.intent_id != *intent_id
        || intent.intent.intent_id != *intent_id
        || risk.result.account_id != plan.definition.account_id
        || intent.intent.account_id != plan.definition.account_id
        || intent.intent.strategy_id != plan.definition.strategy_id
        || risk.result.decision_id != intent.intent.decision_id
    {
        return Err("plan identity differs from its intent or risk approval".to_owned());
    }
    if intent.status != TradeIntentStatus::Accepted {
        return Err(format!(
            "parent intent status {} is not executable",
            intent.status
        ));
    }
    if !risk.result.approved {
        return Err("execution plan cannot reference a rejected risk result".to_owned());
    }
    if created_at < risk.result.evaluated_at
        || created_at >= risk.result.valid_until
        || created_at >= intent.intent.signal_expires_at
    {
        return Err("plan was not created within the approved execution window".to_owned());
    }
    if !matches!(
        intent.intent.action,
        TradeIntentAction::Buy | TradeIntentAction::Sell
    ) {
        return Err("only actionable BUY/SELL intents may create a plan".to_owned());
    }

    let adjusted = risk
        .result
        .adjusted_legs
        .as_ref()
        .ok_or_else(|| "approved risk result has no adjusted legs".to_owned())?;
    let candidates = risk
        .result
        .sizing_candidates
        .as_ref()
        .ok_or_else(|| "approved risk result has no sizing candidates".to_owned())?;
    if adjusted.len() != plan.legs.len() || candidates.len() != plan.legs.len() {
        return Err("plan legs do not correspond one-to-one with risk legs".to_owned());
    }
    for leg in &plan.legs {
        let approved = adjusted
            .iter()
            .find(|approved| approved.leg_id == leg.definition.leg_id)
            .ok_or_else(|| format!("plan leg {} has no risk approval", leg.definition.leg_id))?;
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.leg_id == leg.definition.leg_id)
            .ok_or_else(|| {
                format!(
                    "plan leg {} has no sizing provenance",
                    leg.definition.leg_id
                )
            })?;
        let expected_action = match approved.action {
            AdjustedRiskLegAction::Buy => ExecutionAction::Buy,
            AdjustedRiskLegAction::Sell => ExecutionAction::Sell,
        };
        if leg.definition.symbol != approved.symbol
            || leg.definition.symbol != candidate.symbol
            || leg.definition.action != expected_action
            || leg
                .definition
                .lots
                .is_none_or(|lots| !same_f64_bits(lots, approved.lots))
            || leg
                .definition
                .sl
                .is_none_or(|sl| !same_f64_bits(sl, approved.approved_sl))
            || !same_f64_bits(leg.definition.ratio, candidate.ratio)
        {
            return Err(format!(
                "plan leg {} drifts from its exact risk approval",
                leg.definition.leg_id
            ));
        }
        let expected_tp = expected_intent_leg_tp(&intent.intent, &leg.definition.leg_id)?;
        if !same_optional_f64_bits(leg.definition.tp, expected_tp) {
            return Err(format!(
                "plan leg {} take-profit drifts from its intent",
                leg.definition.leg_id
            ));
        }
    }
    Ok(())
}

fn expected_intent_leg_tp(intent: &TradeIntent, leg_id: &LegId) -> Result<Option<f64>, String> {
    match intent.proposed_legs.as_ref() {
        Some(legs) => legs
            .iter()
            .find(|leg| leg.leg_id == *leg_id)
            .map(|leg| leg.proposed_tp)
            .ok_or_else(|| format!("intent has no proposed leg {leg_id}")),
        None if *leg_id == single_leg_id(&intent.intent_id) => Ok(intent.proposed_tp),
        None => Err(format!("single-leg intent does not own leg {leg_id}")),
    }
}

fn same_optional_f64_bits(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => same_f64_bits(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn same_f64_bits(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && left.to_bits() == right.to_bits()
}

pub(crate) async fn commit_execution_workflow_on(
    connection: &mut SqliteConnection,
    workflow: NewExecutionWorkflow,
) -> Result<WriteOutcome<StoredExecutionWorkflow>, StoreError> {
    validate_new_execution_workflow(&workflow)?;
    sqlx::query("SAVEPOINT execution_workflow_bundle")
        .execute(&mut *connection)
        .await?;
    let result = commit_execution_workflow_in_savepoint(connection, &workflow).await;
    match result {
        Ok(outcome) => {
            sqlx::query("RELEASE SAVEPOINT execution_workflow_bundle")
                .execute(&mut *connection)
                .await?;
            Ok(outcome)
        }
        Err(error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT execution_workflow_bundle")
                .execute(&mut *connection)
                .await?;
            sqlx::query("RELEASE SAVEPOINT execution_workflow_bundle")
                .execute(&mut *connection)
                .await?;
            Err(error)
        }
    }
}

async fn commit_execution_workflow_in_savepoint(
    connection: &mut SqliteConnection,
    workflow: &NewExecutionWorkflow,
) -> Result<WriteOutcome<StoredExecutionWorkflow>, StoreError> {
    let mut inserted_any = false;
    inserted_any |= insert_trade_intent_on(connection, workflow.intent.clone())
        .await?
        .was_inserted();
    inserted_any |= insert_risk_result_on(connection, workflow.risk_result.clone())
        .await?
        .was_inserted();
    inserted_any |= insert_execution_plan_on(connection, workflow.plan.clone())
        .await?
        .was_inserted();
    for command in workflow.commands.iter().cloned() {
        inserted_any |= insert_execution_command_on(connection, command)
            .await?
            .was_inserted();
    }
    for state in workflow.command_states.iter().cloned() {
        inserted_any |= insert_execution_command_state_on(connection, state)
            .await?
            .was_inserted();
    }

    let plan_id = &workflow.plan.plan.definition.plan_id;
    let stored = fetch_execution_workflow_by_plan_id(connection, plan_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_workflow",
                plan_id.as_str(),
                "committed workflow could not be read back",
            )
        })?;
    validate_execution_workflow_replay(&stored, &workflow)?;

    Ok(if inserted_any {
        WriteOutcome::Inserted(stored)
    } else {
        WriteOutcome::Duplicate(stored)
    })
}

async fn fetch_execution_workflow_by_plan_id(
    connection: &mut SqliteConnection,
    plan_id: &PlanId,
) -> Result<Option<StoredExecutionWorkflow>, StoreError> {
    let Some(plan) = fetch_execution_plan_by_id(&mut *connection, plan_id).await? else {
        return Ok(None);
    };
    let risk_result = fetch_risk_result_by_id(&mut *connection, &plan.risk_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_workflow",
                plan_id.as_str(),
                format!("parent risk result {} is missing", plan.risk_id),
            )
        })?;
    let intent = fetch_trade_intent_by_id(&mut *connection, &plan.intent_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_workflow",
                plan_id.as_str(),
                format!("parent trade intent {} is missing", plan.intent_id),
            )
        })?;

    let command_rows = sqlx::query(
        "SELECT command_id, risk_id, plan_id, leg_id, account_id, client_id, terminal_id, \
                symbol, action, expires_at, idempotency_key, payload_json, payload_hash, hmac, \
                created_at \
         FROM execution_commands WHERE plan_id = ?",
    )
    .bind(plan_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let mut commands_by_leg = HashMap::with_capacity(command_rows.len());
    for row in command_rows {
        let command = execution_command_from_row(row)?;
        validate_stored_execution_command_graph(&command, &plan, &risk_result, &intent).map_err(
            |reason| {
                StoreError::corrupt(
                    "execution_command",
                    command.command.command_id.as_str(),
                    reason,
                )
            },
        )?;
        let leg_id = command.command.leg_id.clone().ok_or_else(|| {
            StoreError::corrupt(
                "execution_workflow",
                plan_id.as_str(),
                format!("command {} has no leg_id", command.command.command_id),
            )
        })?;
        if commands_by_leg.insert(leg_id, command).is_some() {
            return Err(StoreError::corrupt(
                "execution_workflow",
                plan_id.as_str(),
                "multiple commands reference the same plan leg",
            ));
        }
    }
    if commands_by_leg.len() != plan.plan.legs.len() {
        return Err(StoreError::corrupt(
            "execution_workflow",
            plan_id.as_str(),
            "commands do not correspond one-to-one with plan legs",
        ));
    }

    let mut commands = Vec::with_capacity(plan.plan.legs.len());
    let mut command_states = Vec::with_capacity(plan.plan.legs.len());
    for leg in &plan.plan.legs {
        let command = commands_by_leg
            .remove(&leg.definition.leg_id)
            .ok_or_else(|| {
                StoreError::corrupt(
                    "execution_workflow",
                    plan_id.as_str(),
                    format!("plan leg {} has no command", leg.definition.leg_id),
                )
            })?;
        let state =
            fetch_execution_command_state_by_id(&mut *connection, &command.command.command_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "execution_workflow",
                        plan_id.as_str(),
                        format!(
                            "command {} has no lifecycle state",
                            command.command.command_id
                        ),
                    )
                })?;
        validate_persisted_command_state(&state, &command).map_err(|reason| {
            StoreError::corrupt(
                "execution_command_state",
                command.command.command_id.as_str(),
                reason,
            )
        })?;
        commands.push(command);
        command_states.push(state);
    }

    Ok(Some(StoredExecutionWorkflow {
        intent,
        risk_result,
        plan,
        commands,
        command_states,
    }))
}

fn validate_new_execution_workflow(workflow: &NewExecutionWorkflow) -> Result<(), StoreError> {
    let plan_id = &workflow.plan.plan.definition.plan_id;
    let invalid = |reason: String| StoreError::InvalidRecord {
        entity: "execution_workflow",
        key: format!("plan_id={plan_id}"),
        reason,
    };

    if workflow.intent.initial_status != TradeIntentStatus::Accepted {
        return Err(invalid(
            "workflow intent must have initial status ACCEPTED".to_owned(),
        ));
    }
    if workflow.intent.recorded_at < 0 {
        return Err(invalid(
            "intent recorded_at must be non-negative".to_owned(),
        ));
    }
    if workflow.plan.intent_id != workflow.intent.intent.intent_id
        || workflow.plan.risk_id != workflow.risk_result.result.risk_id
        || workflow.risk_result.result.intent_id != workflow.intent.intent.intent_id
    {
        return Err(invalid(
            "intent, risk result, and plan identities do not form one graph".to_owned(),
        ));
    }

    let mut commands_by_leg = HashMap::with_capacity(workflow.commands.len());
    let mut command_ids = HashSet::with_capacity(workflow.commands.len());
    let mut idempotency_keys = HashSet::with_capacity(workflow.commands.len());
    for command in &workflow.commands {
        validate_new_execution_command_graph(
            command,
            &workflow.plan,
            &workflow.risk_result,
            &workflow.intent,
        )
        .map_err(&invalid)?;
        if !command_ids.insert(command.command.command_id.clone()) {
            return Err(invalid(format!(
                "duplicate command_id {}",
                command.command.command_id
            )));
        }
        if !idempotency_keys.insert(command.command.idempotency_key.clone()) {
            return Err(invalid(format!(
                "duplicate command idempotency_key {}",
                command.command.idempotency_key
            )));
        }
        let leg_id = command
            .command
            .leg_id
            .clone()
            .expect("graph validation requires leg_id");
        if commands_by_leg.insert(leg_id.clone(), command).is_some() {
            return Err(invalid(format!(
                "multiple commands reference plan leg {leg_id}"
            )));
        }
    }
    if commands_by_leg.len() != workflow.plan.plan.legs.len()
        || workflow
            .plan
            .plan
            .legs
            .iter()
            .any(|leg| !commands_by_leg.contains_key(&leg.definition.leg_id))
    {
        return Err(invalid(
            "commands must correspond one-to-one with plan legs".to_owned(),
        ));
    }

    let mut states_by_command = HashMap::with_capacity(workflow.command_states.len());
    for state in &workflow.command_states {
        if states_by_command
            .insert(state.command_id.clone(), state)
            .is_some()
        {
            return Err(invalid(format!(
                "duplicate command state for {}",
                state.command_id
            )));
        }
    }
    if states_by_command.len() != workflow.commands.len() {
        return Err(invalid(
            "every command must have exactly one initial state".to_owned(),
        ));
    }
    for command in &workflow.commands {
        let state = states_by_command
            .get(&command.command.command_id)
            .ok_or_else(|| {
                invalid(format!(
                    "command {} has no initial state",
                    command.command.command_id
                ))
            })?;
        validate_initial_command_state(state, command).map_err(&invalid)?;
    }
    Ok(())
}

fn validate_new_execution_command_graph(
    command: &NewExecutionCommand,
    plan: &NewExecutionPlan,
    risk: &NewRiskResult,
    intent: &NewTradeIntent,
) -> Result<(), String> {
    validate_execution_command_graph(
        &command.command,
        &command.risk_id,
        command.created_at,
        &plan.plan,
        &plan.risk_id,
        plan.recorded_at,
        &risk.result,
        &intent.intent,
    )
}

fn validate_stored_execution_command_graph(
    command: &StoredExecutionCommand,
    plan: &StoredExecutionPlan,
    risk: &StoredRiskResult,
    intent: &StoredTradeIntent,
) -> Result<(), String> {
    validate_execution_command_graph(
        &command.command,
        &command.risk_id,
        command.created_at,
        &plan.plan,
        &plan.risk_id,
        plan.created_at,
        &risk.result,
        &intent.intent,
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_execution_command_graph(
    command: &ExecutionCommand,
    command_risk_id: &RiskId,
    command_created_at: i64,
    plan: &ExecutionPlan,
    plan_risk_id: &RiskId,
    plan_created_at: i64,
    risk: &RiskResult,
    intent: &TradeIntent,
) -> Result<(), String> {
    let plan_id = command
        .plan_id
        .as_ref()
        .ok_or_else(|| "workflow command has no plan_id".to_owned())?;
    let leg_id = command
        .leg_id
        .as_ref()
        .ok_or_else(|| "workflow command has no leg_id".to_owned())?;
    let leg = plan
        .legs
        .iter()
        .find(|leg| leg.definition.leg_id == *leg_id)
        .ok_or_else(|| format!("command references unknown plan leg {leg_id}"))?;

    if *plan_id != plan.definition.plan_id
        || *command_risk_id != *plan_risk_id
        || *plan_risk_id != risk.risk_id
        || command.account_id != plan.definition.account_id
        || command.account_id != risk.account_id
        || command.account_id != intent.account_id
        || command.strategy_id != plan.definition.strategy_id
        || command.strategy_id != intent.strategy_id
    {
        return Err("command identity differs from its plan or approval".to_owned());
    }
    if command.symbol != leg.definition.symbol
        || command.action != leg.definition.action
        || !same_optional_f64_bits(command.lots, leg.definition.lots)
        || !same_optional_f64_bits(command.sl, leg.definition.sl)
        || !same_optional_f64_bits(command.tp, leg.definition.tp)
    {
        return Err(format!(
            "command {} drifts from approved plan leg {leg_id}",
            command.command_id
        ));
    }
    if command_created_at < plan_created_at
        || command_created_at < risk.evaluated_at
        || command_created_at < 0
        || command.expires_at <= command_created_at
        || command.expires_at > risk.valid_until
        || command.expires_at > intent.signal_expires_at
    {
        return Err("command lifecycle is outside the approved execution window".to_owned());
    }
    if !is_lower_hex_64(&command.hmac) {
        return Err("command hmac must be lowercase 64-character hex".to_owned());
    }
    Ok(())
}

fn validate_initial_command_state(
    state: &ExecutionCommandState,
    command: &NewExecutionCommand,
) -> Result<(), String> {
    if state.command_id != command.command.command_id
        || state.account_id != command.command.account_id
        || state.plan_id != command.command.plan_id
        || state.leg_id != command.command.leg_id
        || state.created_at != command.created_at
        || state.updated_at != command.created_at
    {
        return Err(format!(
            "initial state identity differs from command {}",
            command.command.command_id
        ));
    }
    if state.status != ExecutionCommandStatus::Created
        || state.delivery_attempts != 0
        || state.last_delivery_error.is_some()
        || state.dispatched_at.is_some()
        || state.command_received_at.is_some()
        || state.reconciling_at.is_some()
        || state.completed_at.is_some()
    {
        return Err(format!(
            "command {} must start in a pristine CREATED state",
            command.command.command_id
        ));
    }
    Ok(())
}

fn validate_persisted_command_state(
    state: &ExecutionCommandState,
    command: &StoredExecutionCommand,
) -> Result<(), String> {
    if state.command_id != command.command.command_id
        || state.account_id != command.command.account_id
        || state.plan_id != command.command.plan_id
        || state.leg_id != command.command.leg_id
        || state.created_at != command.created_at
    {
        return Err("state identity differs from its command".to_owned());
    }
    if state.created_at < 0 || state.updated_at < state.created_at {
        return Err("created_at/updated_at lifecycle is invalid".to_owned());
    }
    for (field, timestamp) in [
        ("dispatched_at", state.dispatched_at),
        ("command_received_at", state.command_received_at),
        ("reconciling_at", state.reconciling_at),
        ("completed_at", state.completed_at),
    ] {
        if timestamp
            .is_some_and(|timestamp| timestamp < state.created_at || timestamp > state.updated_at)
        {
            return Err(format!("{field} is outside the persisted lifecycle window"));
        }
    }
    Ok(())
}

fn validate_execution_workflow_replay(
    stored: &StoredExecutionWorkflow,
    incoming: &NewExecutionWorkflow,
) -> Result<(), StoreError> {
    let plan_id = &incoming.plan.plan.definition.plan_id;
    let conflict = || StoreError::conflict("execution_workflow", format!("plan_id={plan_id}"));
    let intent_payload = CanonicalJson::from_serializable(&incoming.intent.intent)?;
    let risk_payload = CanonicalJson::from_serializable(&incoming.risk_result.result)?;
    let plan_payload = execution_plan_journal_payload(&incoming.plan)?;
    if stored.intent.payload != intent_payload
        || stored.intent.status != incoming.intent.initial_status
        || stored.intent.created_at != incoming.intent.recorded_at
        || stored.intent.updated_at != incoming.intent.recorded_at
        || stored.risk_result.payload != risk_payload
        || stored.plan.payload != plan_payload
        || stored.plan.risk_id != incoming.plan.risk_id
        || stored.plan.intent_id != incoming.plan.intent_id
        || stored.plan.created_at != incoming.plan.recorded_at
    {
        return Err(conflict());
    }

    let stored_commands: HashMap<_, _> = stored
        .commands
        .iter()
        .map(|command| (command.command.command_id.clone(), command))
        .collect();
    if stored_commands.len() != incoming.commands.len() {
        return Err(conflict());
    }
    for command in &incoming.commands {
        let payload = CanonicalJson::from_serializable(&command.command)?;
        let Some(existing) = stored_commands.get(&command.command.command_id) else {
            return Err(conflict());
        };
        if existing.risk_id != command.risk_id
            || existing.payload != payload
            || existing.created_at != command.created_at
        {
            return Err(conflict());
        }
    }

    let stored_states: HashMap<_, _> = stored
        .command_states
        .iter()
        .map(|state| (state.command_id.clone(), state))
        .collect();
    if stored_states.len() != incoming.command_states.len() {
        return Err(conflict());
    }
    for state in &incoming.command_states {
        let Some(existing) = stored_states.get(&state.command_id) else {
            return Err(conflict());
        };
        if !same_command_state_identity(existing, state) || existing.updated_at < state.updated_at {
            return Err(conflict());
        }
    }
    Ok(())
}

fn execution_plan_journal_payload(
    new_plan: &NewExecutionPlan,
) -> Result<CanonicalJson, StoreError> {
    CanonicalJson::from_serializable(&ExecutionPlanJournal {
        risk_id: new_plan.risk_id.clone(),
        intent_id: new_plan.intent_id.clone(),
        definition: new_plan.plan.definition.clone(),
        legs: new_plan
            .plan
            .legs
            .iter()
            .map(|leg| leg.definition.clone())
            .collect(),
    })
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) async fn append_core_event_on(
    connection: &mut SqliteConnection,
    event: NewCoreEvent,
) -> Result<WriteOutcome<StoredCoreEvent>, StoreError> {
    let result = sqlx::query(
        "INSERT INTO core_events (\
            event_id, event_type, aggregate_type, aggregate_id, message_id, schema_version, \
            correlation_id, causation_id, account_id, client_id, terminal_id, strategy_id, \
            intent_id, plan_id, leg_id, command_id, idempotency_key, event_at, received_at, \
            created_at, source, payload_json, payload_hash\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&event.metadata.event_id)
    .bind(&event.metadata.event_type)
    .bind(&event.metadata.aggregate_type)
    .bind(&event.metadata.aggregate_id)
    .bind(event.metadata.message_id.as_ref().map(MessageId::as_str))
    .bind(&event.metadata.schema_version)
    .bind(
        event
            .metadata
            .correlation_id
            .as_ref()
            .map(CorrelationId::as_str),
    )
    .bind(
        event
            .metadata
            .causation_id
            .as_ref()
            .map(CausationId::as_str),
    )
    .bind(event.metadata.account_id.as_ref().map(AccountId::as_str))
    .bind(event.metadata.client_id.as_ref().map(ClientId::as_str))
    .bind(event.metadata.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(event.metadata.strategy_id.as_ref().map(StrategyId::as_str))
    .bind(event.metadata.intent_id.as_ref().map(IntentId::as_str))
    .bind(event.metadata.plan_id.as_ref().map(PlanId::as_str))
    .bind(event.metadata.leg_id.as_ref().map(LegId::as_str))
    .bind(event.metadata.command_id.as_ref().map(CommandId::as_str))
    .bind(
        event
            .metadata
            .idempotency_key
            .as_ref()
            .map(IdempotencyKey::as_str),
    )
    .bind(event.metadata.event_at)
    .bind(event.metadata.received_at)
    .bind(event.metadata.created_at)
    .bind(&event.metadata.source)
    .bind(event.payload.as_str())
    .bind(event.payload.sha256_hex())
    .execute(&mut *connection)
    .await?;

    let inserted: StoredCoreEvent = event.clone().into();
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let existing = fetch_core_event_conflicts(connection, &event).await?;
    if existing.len() == 1 && same_core_event_fact(&existing[0], &event) {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        Err(StoreError::conflict(
            "core_event",
            core_event_conflict_key(&event),
        ))
    }
}

pub(crate) async fn fetch_core_event_by_id<'e, E>(
    executor: E,
    event_id: &str,
) -> Result<Option<StoredCoreEvent>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let query = format!("SELECT {CORE_EVENT_COLUMNS} FROM core_events WHERE event_id = ?");
    let row = sqlx::query(&query)
        .bind(event_id)
        .fetch_optional(executor)
        .await?;
    row.map(core_event_from_row).transpose()
}

async fn fetch_core_event_conflicts(
    connection: &mut SqliteConnection,
    event: &NewCoreEvent,
) -> Result<Vec<StoredCoreEvent>, StoreError> {
    let query = format!(
        "SELECT {CORE_EVENT_COLUMNS} FROM core_events \
         WHERE event_id = ? OR (? IS NOT NULL AND message_id = ?)"
    );
    let message_id = event.metadata.message_id.as_ref().map(MessageId::as_str);
    let rows = sqlx::query(&query)
        .bind(&event.metadata.event_id)
        .bind(message_id)
        .bind(message_id)
        .fetch_all(&mut *connection)
        .await?;
    rows.into_iter().map(core_event_from_row).collect()
}

fn core_event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredCoreEvent, StoreError> {
    let event_id: String = row.try_get("event_id")?;
    let payload = CanonicalJson::from_stored(
        "core_event",
        &event_id,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    Ok(StoredCoreEvent {
        metadata: CoreEventMetadata {
            event_id,
            event_type: row.try_get("event_type")?,
            aggregate_type: row.try_get("aggregate_type")?,
            aggregate_id: row.try_get("aggregate_id")?,
            message_id: optional_id(&row, "message_id")?,
            schema_version: row.try_get("schema_version")?,
            correlation_id: optional_id(&row, "correlation_id")?,
            causation_id: optional_id(&row, "causation_id")?,
            account_id: optional_id(&row, "account_id")?,
            client_id: optional_id(&row, "client_id")?,
            terminal_id: optional_id(&row, "terminal_id")?,
            strategy_id: optional_id(&row, "strategy_id")?,
            intent_id: optional_id(&row, "intent_id")?,
            plan_id: optional_id(&row, "plan_id")?,
            leg_id: optional_id(&row, "leg_id")?,
            command_id: optional_id(&row, "command_id")?,
            idempotency_key: optional_id(&row, "idempotency_key")?,
            event_at: row.try_get("event_at")?,
            received_at: row.try_get("received_at")?,
            created_at: row.try_get("created_at")?,
            source: row.try_get("source")?,
        },
        payload,
    })
}

fn same_core_event_fact(existing: &StoredCoreEvent, incoming: &NewCoreEvent) -> bool {
    let left = &existing.metadata;
    let right = &incoming.metadata;
    existing.payload == incoming.payload
        && left.event_id == right.event_id
        && left.message_id == right.message_id
        && left.event_type == right.event_type
        && left.aggregate_type == right.aggregate_type
        && left.aggregate_id == right.aggregate_id
        && left.schema_version == right.schema_version
        && left.correlation_id == right.correlation_id
        && left.causation_id == right.causation_id
        && left.account_id == right.account_id
        && left.client_id == right.client_id
        && left.terminal_id == right.terminal_id
        && left.strategy_id == right.strategy_id
        && left.intent_id == right.intent_id
        && left.plan_id == right.plan_id
        && left.leg_id == right.leg_id
        && left.command_id == right.command_id
        && left.idempotency_key == right.idempotency_key
        && left.event_at == right.event_at
        && left.source == right.source
}

fn core_event_conflict_key(event: &NewCoreEvent) -> String {
    match &event.metadata.message_id {
        Some(message_id) => format!(
            "event_id={} or message_id={message_id}",
            event.metadata.event_id
        ),
        None => format!("event_id={}", event.metadata.event_id),
    }
}

pub(crate) async fn insert_trade_intent_on(
    connection: &mut SqliteConnection,
    new_intent: NewTradeIntent,
) -> Result<WriteOutcome<StoredTradeIntent>, StoreError> {
    let payload = CanonicalJson::from_serializable(&new_intent.intent)?;
    let intent = &new_intent.intent;
    let result = sqlx::query(
        "INSERT INTO trade_intents (\
            intent_id, decision_id, strategy_id, account_id, symbol, action, status, \
            requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash, \
            created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(intent.intent_id.as_str())
    .bind(intent.decision_id.as_str())
    .bind(intent.strategy_id.as_str())
    .bind(intent.account_id.as_str())
    .bind(intent.symbol.as_str())
    .bind(intent.action.as_str())
    .bind(new_intent.initial_status.as_str())
    .bind(intent.requested_at)
    .bind(intent.signal_expires_at)
    .bind(intent.idempotency_key.as_str())
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(new_intent.recorded_at)
    .bind(new_intent.recorded_at)
    .execute(&mut *connection)
    .await?;

    let inserted = StoredTradeIntent {
        intent: new_intent.intent.clone(),
        status: new_intent.initial_status,
        payload: payload.clone(),
        created_at: new_intent.recorded_at,
        updated_at: new_intent.recorded_at,
    };
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let existing = fetch_trade_intent_conflicts(connection, intent).await?;
    if existing.len() == 1
        && existing[0].intent.intent_id == intent.intent_id
        && existing[0].intent.idempotency_key == intent.idempotency_key
        && existing[0].payload == payload
    {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        let key = if existing
            .iter()
            .any(|record| record.intent.intent_id == intent.intent_id)
        {
            format!("intent_id={}", intent.intent_id)
        } else {
            format!("idempotency_key={}", intent.idempotency_key)
        };
        Err(StoreError::conflict("trade_intent", key))
    }
}

async fn fetch_trade_intent_by_id<'e, E>(
    executor: E,
    intent_id: &IntentId,
) -> Result<Option<StoredTradeIntent>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT intent_id, decision_id, strategy_id, account_id, symbol, action, status, \
                requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash, \
                created_at, updated_at \
         FROM trade_intents WHERE intent_id = ?",
    )
    .bind(intent_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(trade_intent_from_row).transpose()
}

async fn fetch_trade_intent_conflicts(
    connection: &mut SqliteConnection,
    intent: &TradeIntent,
) -> Result<Vec<StoredTradeIntent>, StoreError> {
    let rows = sqlx::query(
        "SELECT intent_id, decision_id, strategy_id, account_id, symbol, action, status, \
                requested_at, signal_expires_at, idempotency_key, payload_json, payload_hash, \
                created_at, updated_at \
         FROM trade_intents WHERE intent_id = ? OR idempotency_key = ?",
    )
    .bind(intent.intent_id.as_str())
    .bind(intent.idempotency_key.as_str())
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(trade_intent_from_row).collect()
}

fn trade_intent_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredTradeIntent, StoreError> {
    let key: String = row.try_get("intent_id")?;
    let payload = CanonicalJson::from_stored(
        "trade_intent",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let intent: TradeIntent = deserialize_payload("trade_intent", &key, &payload)?;

    validate_column(
        "trade_intent",
        &key,
        "intent_id",
        &key,
        intent.intent_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "decision_id",
        &row.try_get::<String, _>("decision_id")?,
        intent.decision_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "strategy_id",
        &row.try_get::<String, _>("strategy_id")?,
        intent.strategy_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "account_id",
        &row.try_get::<String, _>("account_id")?,
        intent.account_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "symbol",
        &row.try_get::<String, _>("symbol")?,
        intent.symbol.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "action",
        &row.try_get::<String, _>("action")?,
        intent.action.as_str(),
    )?;
    validate_i64_column(
        "trade_intent",
        &key,
        "requested_at",
        row.try_get("requested_at")?,
        intent.requested_at,
    )?;
    validate_i64_column(
        "trade_intent",
        &key,
        "signal_expires_at",
        row.try_get("signal_expires_at")?,
        intent.signal_expires_at,
    )?;
    validate_column(
        "trade_intent",
        &key,
        "idempotency_key",
        &row.try_get::<String, _>("idempotency_key")?,
        intent.idempotency_key.as_str(),
    )?;

    Ok(StoredTradeIntent {
        intent,
        status: parse_enum_column("trade_intent", &key, "status", row.try_get("status")?)?,
        payload,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(crate) async fn insert_risk_result_on(
    connection: &mut SqliteConnection,
    new_result: NewRiskResult,
) -> Result<WriteOutcome<StoredRiskResult>, StoreError> {
    new_result
        .result
        .validate()
        .map_err(|error| StoreError::InvalidRecord {
            entity: "risk_result",
            key: format!("risk_id={}", new_result.result.risk_id),
            reason: error.to_string(),
        })?;
    let payload = CanonicalJson::from_serializable(&new_result.result)?;

    if let Some(existing) =
        fetch_risk_result_by_id(&mut *connection, &new_result.result.risk_id).await?
    {
        return resolve_risk_result_replay(existing, &new_result, &payload);
    }

    validate_risk_result_intent_identity(connection, &new_result).await?;
    let result = &new_result.result;
    let approved = if result.approved { 1_i64 } else { 0_i64 };
    let insert = sqlx::query(
        "INSERT INTO risk_results (\
            risk_id, intent_id, account_id, approved, reason, snapshot_age_ms, \
            symbol_metadata_age_ms, evaluated_at, valid_until, payload_json, payload_hash\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(result.risk_id.as_str())
    .bind(result.intent_id.as_str())
    .bind(result.account_id.as_str())
    .bind(approved)
    .bind(result.reason.as_str())
    .bind(result.snapshot_age_ms)
    .bind(result.symbol_metadata_age_ms)
    .bind(result.evaluated_at)
    .bind(result.valid_until)
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .execute(&mut *connection)
    .await?;

    let inserted = StoredRiskResult {
        result: new_result.result.clone(),
        payload: payload.clone(),
    };
    if insert.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    match fetch_risk_result_by_id(connection, &new_result.result.risk_id).await? {
        Some(existing) => resolve_risk_result_replay(existing, &new_result, &payload),
        None => Err(StoreError::conflict(
            "risk_result",
            format!("risk_id={}", new_result.result.risk_id),
        )),
    }
}

async fn validate_risk_result_intent_identity(
    connection: &mut SqliteConnection,
    result: &NewRiskResult,
) -> Result<(), StoreError> {
    let Some(parent) = fetch_trade_intent_by_id(&mut *connection, &result.result.intent_id).await?
    else {
        return Err(StoreError::NotFound {
            entity: "trade_intent",
            key: format!("intent_id={}", result.result.intent_id),
        });
    };

    if parent.intent.account_id != result.result.account_id
        || parent.intent.decision_id != result.result.decision_id
    {
        return Err(StoreError::conflict(
            "risk_result",
            format!(
                "risk_id={}, intent_id={}",
                result.result.risk_id, result.result.intent_id
            ),
        ));
    }

    validate_risk_result_intent_contract(&result.result, &parent.intent).map_err(|reason| {
        StoreError::InvalidRecord {
            entity: "risk_result",
            key: format!("risk_id={}", result.result.risk_id),
            reason,
        }
    })?;

    Ok(())
}

fn validate_risk_result_intent_contract(
    result: &RiskResult,
    intent: &TradeIntent,
) -> Result<(), String> {
    let has_any_sizing = result.sizing_version.is_some()
        || result.risk_base_amount.is_some()
        || result.risk_budget_amount.is_some()
        || result.adjusted_risk_pct.is_some()
        || result.sizing_candidates.is_some()
        || result.adjusted_legs.is_some();
    let has_actionable_sizing = result
        .sizing_candidates
        .as_ref()
        .is_some_and(|candidates| !candidates.is_empty())
        && result
            .adjusted_legs
            .as_ref()
            .is_some_and(|legs| !legs.is_empty());

    if result.approved
        && intent.proposed_legs.as_ref().is_some_and(|legs| {
            legs.iter()
                .any(|leg| leg.action == TradeIntentLegAction::Close)
        })
    {
        return Err("intent containing a CLOSE leg cannot be approved".to_owned());
    }

    match intent.action {
        TradeIntentAction::Buy | TradeIntentAction::Sell
            if result.approved && !has_actionable_sizing =>
        {
            return Err(format!(
                "approved {} intent must contain actionable sizing",
                intent.action
            ));
        }
        TradeIntentAction::Hold if has_any_sizing => {
            return Err("HOLD intent must not contain sizing".to_owned());
        }
        TradeIntentAction::Close if result.approved => {
            return Err("CLOSE intent cannot be approved in the first implementation".to_owned());
        }
        TradeIntentAction::Close if has_any_sizing => {
            return Err("CLOSE intent must not contain sizing".to_owned());
        }
        _ => {}
    }

    if result.approved
        && matches!(
            intent.action,
            TradeIntentAction::Buy | TradeIntentAction::Sell
        )
    {
        validate_approved_sizing_shape(result, intent)?;
    }

    if result.approved {
        if result.evaluated_at < intent.requested_at {
            return Err(format!(
                "approved result evaluated_at {} precedes intent requested_at {}",
                result.evaluated_at, intent.requested_at
            ));
        }
        if result.evaluated_at >= intent.signal_expires_at {
            return Err(format!(
                "approved result evaluated_at {} must precede intent signal_expires_at {}",
                result.evaluated_at, intent.signal_expires_at
            ));
        }
        if result.valid_until > intent.signal_expires_at {
            return Err(format!(
                "approved result valid_until {} exceeds intent signal_expires_at {}",
                result.valid_until, intent.signal_expires_at
            ));
        }
    }

    Ok(())
}

fn validate_approved_sizing_shape(result: &RiskResult, intent: &TradeIntent) -> Result<(), String> {
    let candidates = result
        .sizing_candidates
        .as_deref()
        .ok_or_else(|| "approved actionable intent is missing sizing candidates".to_owned())?;

    match intent.proposed_legs.as_deref() {
        None => {
            if candidates.len() != 1 {
                return Err(format!(
                    "approved single-leg intent must contain exactly one sizing candidate, found {}",
                    candidates.len()
                ));
            }
            let expected_action = match intent.action {
                TradeIntentAction::Buy => AdjustedRiskLegAction::Buy,
                TradeIntentAction::Sell => AdjustedRiskLegAction::Sell,
                TradeIntentAction::Close | TradeIntentAction::Hold => {
                    return Err(format!(
                        "{} intent cannot have actionable sizing",
                        intent.action
                    ));
                }
            };
            let expected_stop = intent.proposed_sl.ok_or_else(|| {
                "approved single-leg intent must define proposed_sl for sizing provenance"
                    .to_owned()
            })?;
            let candidate = &candidates[0];
            if candidate.leg_id != single_leg_id(&intent.intent_id)
                || candidate.symbol != intent.symbol
                || candidate.action != expected_action
                || !risk_shape_number_matches(candidate.ratio, 1.0)
                || !risk_shape_number_matches(candidate.stop_loss_price, expected_stop)
            {
                return Err(
                    "single-leg sizing candidate must match the derived leg id and intent symbol, action, ratio=1 and proposed_sl".to_owned(),
                );
            }
        }
        Some(proposed_legs) => {
            if proposed_legs
                .iter()
                .any(|leg| leg.action == TradeIntentLegAction::Close)
            {
                return Err("intent containing a CLOSE leg cannot be approved".to_owned());
            }
            if candidates.len() != proposed_legs.len() {
                return Err(format!(
                    "approved multi-leg intent requires one sizing candidate per proposed leg; expected {}, found {}",
                    proposed_legs.len(),
                    candidates.len()
                ));
            }

            let mut proposed_leg_ids = HashSet::with_capacity(proposed_legs.len());
            for proposed_leg in proposed_legs {
                if !proposed_leg_ids.insert(proposed_leg.leg_id.as_str()) {
                    return Err(format!(
                        "proposed multi-leg intent contains duplicate leg_id {}",
                        proposed_leg.leg_id
                    ));
                }
                let candidate = candidates
                    .iter()
                    .find(|candidate| candidate.leg_id == proposed_leg.leg_id)
                    .ok_or_else(|| {
                        format!(
                            "approved multi-leg sizing is missing proposed leg {}",
                            proposed_leg.leg_id
                        )
                    })?;
                let expected_action = match proposed_leg.action {
                    TradeIntentLegAction::Buy => AdjustedRiskLegAction::Buy,
                    TradeIntentLegAction::Sell => AdjustedRiskLegAction::Sell,
                    TradeIntentLegAction::Close => {
                        return Err("intent containing a CLOSE leg cannot be approved".to_owned());
                    }
                };
                let expected_stop = proposed_leg.proposed_sl.ok_or_else(|| {
                    format!(
                        "approved proposed leg {} must define proposed_sl for sizing provenance",
                        proposed_leg.leg_id
                    )
                })?;
                if candidate.symbol != proposed_leg.symbol
                    || candidate.action != expected_action
                    || !risk_shape_number_matches(candidate.ratio, proposed_leg.ratio)
                    || !risk_shape_number_matches(candidate.stop_loss_price, expected_stop)
                {
                    return Err(format!(
                        "sizing candidate {} must match its proposed leg symbol, action, ratio and proposed_sl",
                        candidate.leg_id
                    ));
                }
            }
        }
    }

    Ok(())
}

fn risk_shape_number_matches(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && left.to_bits() == right.to_bits()
}

fn resolve_risk_result_replay(
    existing: StoredRiskResult,
    incoming: &NewRiskResult,
    payload: &CanonicalJson,
) -> Result<WriteOutcome<StoredRiskResult>, StoreError> {
    if existing.payload == *payload {
        Ok(WriteOutcome::Duplicate(existing))
    } else {
        Err(StoreError::conflict(
            "risk_result",
            format!("risk_id={}", incoming.result.risk_id),
        ))
    }
}

async fn fetch_risk_result_by_id<'e, E>(
    executor: E,
    risk_id: &RiskId,
) -> Result<Option<StoredRiskResult>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let query = format!(
        "SELECT {RISK_RESULT_COLUMNS} FROM risk_results r \
         LEFT JOIN trade_intents i ON i.intent_id = r.intent_id \
         WHERE r.risk_id = ?"
    );
    let row = sqlx::query(&query)
        .bind(risk_id.as_str())
        .fetch_optional(executor)
        .await?;
    row.map(risk_result_from_row).transpose()
}

fn risk_result_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredRiskResult, StoreError> {
    let key: String = row.try_get("risk_id")?;
    let payload = CanonicalJson::from_stored(
        "risk_result",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let result: RiskResult = deserialize_payload("risk_result", &key, &payload)?;
    let intent_id = IntentId::from(row.try_get::<String, _>("intent_id")?);
    result.validate().map_err(|error| {
        StoreError::corrupt(
            "risk_result",
            &key,
            format!("payload failed semantic validation: {error}"),
        )
    })?;

    validate_column(
        "risk_result",
        &key,
        "risk_id",
        &key,
        result.risk_id.as_str(),
    )?;
    validate_column(
        "risk_result",
        &key,
        "intent_id",
        intent_id.as_str(),
        result.intent_id.as_str(),
    )?;
    validate_column(
        "risk_result",
        &key,
        "account_id",
        &row.try_get::<String, _>("account_id")?,
        result.account_id.as_str(),
    )?;
    validate_i64_column(
        "risk_result",
        &key,
        "approved",
        row.try_get("approved")?,
        if result.approved { 1 } else { 0 },
    )?;
    validate_column(
        "risk_result",
        &key,
        "reason",
        &row.try_get::<String, _>("reason")?,
        result.reason.as_str(),
    )?;
    validate_i64_column(
        "risk_result",
        &key,
        "snapshot_age_ms",
        row.try_get("snapshot_age_ms")?,
        result.snapshot_age_ms,
    )?;
    validate_i64_column(
        "risk_result",
        &key,
        "symbol_metadata_age_ms",
        row.try_get("symbol_metadata_age_ms")?,
        result.symbol_metadata_age_ms,
    )?;
    validate_i64_column(
        "risk_result",
        &key,
        "evaluated_at",
        row.try_get("evaluated_at")?,
        result.evaluated_at,
    )?;
    validate_i64_column(
        "risk_result",
        &key,
        "valid_until",
        row.try_get("valid_until")?,
        result.valid_until,
    )?;

    let parent = trade_intent_from_risk_result_row(&row, &key)?;
    validate_column(
        "risk_result",
        &key,
        "parent intent_id",
        parent.intent.intent_id.as_str(),
        result.intent_id.as_str(),
    )?;
    validate_column(
        "risk_result",
        &key,
        "parent intent account_id",
        parent.intent.account_id.as_str(),
        result.account_id.as_str(),
    )?;
    validate_column(
        "risk_result",
        &key,
        "parent intent decision_id",
        parent.intent.decision_id.as_str(),
        result.decision_id.as_str(),
    )?;
    validate_risk_result_intent_contract(&result, &parent.intent).map_err(|reason| {
        StoreError::corrupt(
            "risk_result",
            &key,
            format!("parent trade_intent contract mismatch: {reason}"),
        )
    })?;

    Ok(StoredRiskResult { result, payload })
}

fn trade_intent_from_risk_result_row(
    row: &sqlx::sqlite::SqliteRow,
    risk_key: &str,
) -> Result<StoredTradeIntent, StoreError> {
    let key = row
        .try_get::<Option<String>, _>("parent_intent_id")?
        .ok_or_else(|| {
            StoreError::corrupt("risk_result", risk_key, "parent trade_intent is missing")
        })?;
    let payload = CanonicalJson::from_stored(
        "trade_intent",
        &key,
        row.try_get("intent_payload_json")?,
        row.try_get("intent_payload_hash")?,
    )?;
    let intent: TradeIntent = deserialize_payload("trade_intent", &key, &payload)?;

    validate_column(
        "trade_intent",
        &key,
        "intent_id",
        &key,
        intent.intent_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "decision_id",
        &row.try_get::<String, _>("intent_decision_id")?,
        intent.decision_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "strategy_id",
        &row.try_get::<String, _>("intent_strategy_id")?,
        intent.strategy_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "account_id",
        &row.try_get::<String, _>("intent_account_id")?,
        intent.account_id.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "symbol",
        &row.try_get::<String, _>("intent_symbol")?,
        intent.symbol.as_str(),
    )?;
    validate_column(
        "trade_intent",
        &key,
        "action",
        &row.try_get::<String, _>("intent_action")?,
        intent.action.as_str(),
    )?;
    validate_i64_column(
        "trade_intent",
        &key,
        "requested_at",
        row.try_get("intent_requested_at")?,
        intent.requested_at,
    )?;
    validate_i64_column(
        "trade_intent",
        &key,
        "signal_expires_at",
        row.try_get("intent_signal_expires_at")?,
        intent.signal_expires_at,
    )?;
    validate_column(
        "trade_intent",
        &key,
        "idempotency_key",
        &row.try_get::<String, _>("intent_idempotency_key")?,
        intent.idempotency_key.as_str(),
    )?;

    Ok(StoredTradeIntent {
        intent,
        status: parse_enum_column::<TradeIntentStatus>(
            "trade_intent",
            &key,
            "status",
            row.try_get("intent_status")?,
        )?,
        payload,
        created_at: row.try_get("intent_created_at")?,
        updated_at: row.try_get("intent_updated_at")?,
    })
}

pub(crate) async fn insert_execution_command_on(
    connection: &mut SqliteConnection,
    new_command: NewExecutionCommand,
) -> Result<WriteOutcome<StoredExecutionCommand>, StoreError> {
    let payload = CanonicalJson::from_serializable(&new_command.command)?;
    let command = &new_command.command;
    let result = sqlx::query(
        "INSERT INTO execution_commands (\
            command_id, risk_id, plan_id, leg_id, account_id, client_id, terminal_id, symbol, \
            action, expires_at, idempotency_key, payload_json, payload_hash, hmac, created_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(command.command_id.as_str())
    .bind(new_command.risk_id.as_str())
    .bind(command.plan_id.as_ref().map(PlanId::as_str))
    .bind(command.leg_id.as_ref().map(LegId::as_str))
    .bind(command.account_id.as_str())
    .bind(command.client_id.as_ref().map(ClientId::as_str))
    .bind(command.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(command.symbol.as_str())
    .bind(command.action.as_str())
    .bind(command.expires_at)
    .bind(command.idempotency_key.as_str())
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(&command.hmac)
    .bind(new_command.created_at)
    .execute(&mut *connection)
    .await?;

    let inserted = StoredExecutionCommand {
        command: new_command.command.clone(),
        risk_id: new_command.risk_id.clone(),
        payload: payload.clone(),
        created_at: new_command.created_at,
    };
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let existing = fetch_execution_command_conflicts(connection, command).await?;
    if existing.len() == 1
        && existing[0].command.command_id == command.command_id
        && existing[0].command.idempotency_key == command.idempotency_key
        && existing[0].risk_id == new_command.risk_id
        && existing[0].payload == payload
    {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        let key = if existing
            .iter()
            .any(|record| record.command.command_id == command.command_id)
        {
            format!("command_id={}", command.command_id)
        } else {
            format!("idempotency_key={}", command.idempotency_key)
        };
        Err(StoreError::conflict("execution_command", key))
    }
}

async fn fetch_execution_command_by_id<'e, E>(
    executor: E,
    command_id: &CommandId,
) -> Result<Option<StoredExecutionCommand>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT command_id, risk_id, plan_id, leg_id, account_id, client_id, terminal_id, \
                symbol, action, expires_at, idempotency_key, payload_json, payload_hash, hmac, \
                created_at \
         FROM execution_commands WHERE command_id = ?",
    )
    .bind(command_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(execution_command_from_row).transpose()
}

async fn fetch_execution_command_conflicts(
    connection: &mut SqliteConnection,
    command: &ExecutionCommand,
) -> Result<Vec<StoredExecutionCommand>, StoreError> {
    let rows = sqlx::query(
        "SELECT command_id, risk_id, plan_id, leg_id, account_id, client_id, terminal_id, \
                symbol, action, expires_at, idempotency_key, payload_json, payload_hash, hmac, \
                created_at \
         FROM execution_commands WHERE command_id = ? OR idempotency_key = ?",
    )
    .bind(command.command_id.as_str())
    .bind(command.idempotency_key.as_str())
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(execution_command_from_row).collect()
}

fn execution_command_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredExecutionCommand, StoreError> {
    let key: String = row.try_get("command_id")?;
    let payload = CanonicalJson::from_stored(
        "execution_command",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let command: ExecutionCommand = deserialize_payload("execution_command", &key, &payload)?;

    validate_column(
        "execution_command",
        &key,
        "command_id",
        &key,
        command.command_id.as_str(),
    )?;
    validate_optional_column(
        "execution_command",
        &key,
        "plan_id",
        row.try_get("plan_id")?,
        command.plan_id.as_ref().map(PlanId::as_str),
    )?;
    validate_optional_column(
        "execution_command",
        &key,
        "leg_id",
        row.try_get("leg_id")?,
        command.leg_id.as_ref().map(LegId::as_str),
    )?;
    validate_column(
        "execution_command",
        &key,
        "account_id",
        &row.try_get::<String, _>("account_id")?,
        command.account_id.as_str(),
    )?;
    validate_optional_column(
        "execution_command",
        &key,
        "client_id",
        row.try_get("client_id")?,
        command.client_id.as_ref().map(ClientId::as_str),
    )?;
    validate_optional_column(
        "execution_command",
        &key,
        "terminal_id",
        row.try_get("terminal_id")?,
        command.terminal_id.as_ref().map(TerminalId::as_str),
    )?;
    validate_column(
        "execution_command",
        &key,
        "symbol",
        &row.try_get::<String, _>("symbol")?,
        command.symbol.as_str(),
    )?;
    validate_column(
        "execution_command",
        &key,
        "action",
        &row.try_get::<String, _>("action")?,
        command.action.as_str(),
    )?;
    validate_i64_column(
        "execution_command",
        &key,
        "expires_at",
        row.try_get("expires_at")?,
        command.expires_at,
    )?;
    validate_column(
        "execution_command",
        &key,
        "idempotency_key",
        &row.try_get::<String, _>("idempotency_key")?,
        command.idempotency_key.as_str(),
    )?;
    validate_column(
        "execution_command",
        &key,
        "hmac",
        &row.try_get::<String, _>("hmac")?,
        &command.hmac,
    )?;

    Ok(StoredExecutionCommand {
        command,
        risk_id: RiskId::from(row.try_get::<String, _>("risk_id")?),
        payload,
        created_at: row.try_get("created_at")?,
    })
}

pub(crate) async fn insert_execution_command_state_on(
    connection: &mut SqliteConnection,
    state: ExecutionCommandState,
) -> Result<WriteOutcome<ExecutionCommandState>, StoreError> {
    validate_command_state_identity(connection, &state).await?;
    let result = sqlx::query(
        "INSERT INTO execution_command_states (\
            command_id, account_id, plan_id, leg_id, status, delivery_attempts, \
            last_delivery_error, created_at, dispatched_at, command_received_at, reconciling_at, \
            completed_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(state.command_id.as_str())
    .bind(state.account_id.as_str())
    .bind(state.plan_id.as_ref().map(PlanId::as_str))
    .bind(state.leg_id.as_ref().map(LegId::as_str))
    .bind(state.status.as_str())
    .bind(i64::from(state.delivery_attempts))
    .bind(&state.last_delivery_error)
    .bind(state.created_at)
    .bind(state.dispatched_at)
    .bind(state.command_received_at)
    .bind(state.reconciling_at)
    .bind(state.completed_at)
    .bind(state.updated_at)
    .execute(&mut *connection)
    .await?;

    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(state));
    }

    let Some(existing) =
        fetch_execution_command_state_by_id(&mut *connection, &state.command_id).await?
    else {
        return Err(StoreError::conflict(
            "execution_command_state",
            format!("command_id={}", state.command_id),
        ));
    };
    if existing == state
        || (same_command_state_identity(&existing, &state)
            && existing.updated_at > state.updated_at)
    {
        Ok(WriteOutcome::Duplicate(existing))
    } else {
        Err(StoreError::conflict(
            "execution_command_state",
            format!("command_id={}", state.command_id),
        ))
    }
}

async fn validate_command_state_identity(
    connection: &mut SqliteConnection,
    state: &ExecutionCommandState,
) -> Result<(), StoreError> {
    let row = sqlx::query(
        "SELECT account_id, plan_id, leg_id, created_at \
         FROM execution_commands WHERE command_id = ?",
    )
    .bind(state.command_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Err(StoreError::NotFound {
            entity: "execution_command",
            key: format!("command_id={}", state.command_id),
        });
    };

    let account_id: String = row.try_get("account_id")?;
    let plan_id: Option<String> = row.try_get("plan_id")?;
    let leg_id: Option<String> = row.try_get("leg_id")?;
    let created_at: i64 = row.try_get("created_at")?;
    if account_id == state.account_id.as_str()
        && plan_id.as_deref() == state.plan_id.as_ref().map(PlanId::as_str)
        && leg_id.as_deref() == state.leg_id.as_ref().map(LegId::as_str)
        && created_at == state.created_at
    {
        Ok(())
    } else {
        Err(StoreError::conflict(
            "execution_command_state",
            format!("command_id={}", state.command_id),
        ))
    }
}

pub(crate) async fn update_execution_command_state_on(
    connection: &mut SqliteConnection,
    update: CommandStateUpdate,
) -> Result<ExecutionCommandState, StoreError> {
    let state = &update.state;
    let result = sqlx::query(
        "UPDATE execution_command_states SET \
            status = ?, delivery_attempts = ?, last_delivery_error = ?, dispatched_at = ?, \
            command_received_at = ?, reconciling_at = ?, completed_at = ?, updated_at = ? \
         WHERE command_id = ? AND account_id = ? AND plan_id IS ? AND leg_id IS ? \
           AND created_at = ? AND status = ? AND updated_at = ? AND updated_at < ?",
    )
    .bind(state.status.as_str())
    .bind(i64::from(state.delivery_attempts))
    .bind(&state.last_delivery_error)
    .bind(state.dispatched_at)
    .bind(state.command_received_at)
    .bind(state.reconciling_at)
    .bind(state.completed_at)
    .bind(state.updated_at)
    .bind(state.command_id.as_str())
    .bind(state.account_id.as_str())
    .bind(state.plan_id.as_ref().map(PlanId::as_str))
    .bind(state.leg_id.as_ref().map(LegId::as_str))
    .bind(state.created_at)
    .bind(update.expected_status.as_str())
    .bind(update.expected_updated_at)
    .bind(state.updated_at)
    .execute(&mut *connection)
    .await?;

    if result.rows_affected() == 1 {
        return Ok(update.state);
    }

    match fetch_execution_command_state_by_id(&mut *connection, &state.command_id).await? {
        Some(existing) if existing == update.state => Ok(existing),
        Some(existing) if !same_command_state_identity(&existing, state) => {
            Err(StoreError::conflict(
                "execution_command_state",
                format!("command_id={}", state.command_id),
            ))
        }
        Some(_) => Err(StoreError::StaleWrite {
            entity: "execution_command_state",
            key: format!("command_id={}", state.command_id),
        }),
        None => Err(StoreError::NotFound {
            entity: "execution_command_state",
            key: format!("command_id={}", state.command_id),
        }),
    }
}

fn same_command_state_identity(
    existing: &ExecutionCommandState,
    proposed: &ExecutionCommandState,
) -> bool {
    existing.command_id == proposed.command_id
        && existing.account_id == proposed.account_id
        && existing.plan_id == proposed.plan_id
        && existing.leg_id == proposed.leg_id
        && existing.created_at == proposed.created_at
}

async fn fetch_execution_command_state_by_id<'e, E>(
    executor: E,
    command_id: &CommandId,
) -> Result<Option<ExecutionCommandState>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT command_id, account_id, plan_id, leg_id, status, delivery_attempts, \
                last_delivery_error, created_at, dispatched_at, command_received_at, \
                reconciling_at, completed_at, updated_at \
         FROM execution_command_states WHERE command_id = ?",
    )
    .bind(command_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(execution_command_state_from_row).transpose()
}

fn execution_command_state_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ExecutionCommandState, StoreError> {
    let key: String = row.try_get("command_id")?;
    let delivery_attempts: i64 = row.try_get("delivery_attempts")?;
    let delivery_attempts = u32::try_from(delivery_attempts).map_err(|_| {
        StoreError::corrupt(
            "execution_command_state",
            &key,
            "delivery_attempts does not fit in u32",
        )
    })?;

    Ok(ExecutionCommandState {
        command_id: CommandId::from(key),
        account_id: AccountId::from(row.try_get::<String, _>("account_id")?),
        plan_id: optional_id(&row, "plan_id")?,
        leg_id: optional_id(&row, "leg_id")?,
        status: parse_enum_column(
            "execution_command_state",
            row.try_get::<String, _>("command_id")?.as_str(),
            "status",
            row.try_get("status")?,
        )?,
        delivery_attempts,
        last_delivery_error: row.try_get("last_delivery_error")?,
        created_at: row.try_get("created_at")?,
        dispatched_at: row.try_get("dispatched_at")?,
        command_received_at: row.try_get("command_received_at")?,
        reconciling_at: row.try_get("reconciling_at")?,
        completed_at: row.try_get("completed_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(crate) async fn append_execution_event_on(
    connection: &mut SqliteConnection,
    new_event: NewExecutionEvent,
) -> Result<WriteOutcome<StoredExecutionEvent>, StoreError> {
    let payload = CanonicalJson::from_serializable(&new_event.event)?;
    let event = &new_event.event;
    let result = sqlx::query(
        "INSERT INTO execution_events (\
            execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
            position_ticket, event_at, filled_at, payload_json, payload_hash, created_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(event.execution_id.as_str())
    .bind(event.command_id.as_str())
    .bind(event.plan_id.as_ref().map(PlanId::as_str))
    .bind(event.leg_id.as_ref().map(LegId::as_str))
    .bind(event.account_id.as_str())
    .bind(event.status.as_str())
    .bind(event.broker_order_id.as_ref().map(|id| id.as_str()))
    .bind(event.position_ticket.as_ref().map(|id| id.as_str()))
    .bind(event.event_at)
    .bind(event.filled_at)
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(new_event.created_at)
    .execute(&mut *connection)
    .await?;

    let inserted = StoredExecutionEvent {
        event: new_event.event.clone(),
        payload: payload.clone(),
        created_at: new_event.created_at,
    };
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let Some(existing) = fetch_execution_event_by_id(&mut *connection, &event.execution_id).await?
    else {
        return Err(StoreError::conflict(
            "execution_event",
            format!("execution_id={}", event.execution_id),
        ));
    };
    if existing.event.execution_id == event.execution_id && existing.payload == payload {
        Ok(WriteOutcome::Duplicate(existing))
    } else {
        Err(StoreError::conflict(
            "execution_event",
            format!("execution_id={}", event.execution_id),
        ))
    }
}

async fn fetch_execution_event_by_id<'e, E>(
    executor: E,
    execution_id: &ExecutionId,
) -> Result<Option<StoredExecutionEvent>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT execution_id, command_id, plan_id, leg_id, account_id, status, broker_order_id, \
                position_ticket, event_at, filled_at, payload_json, payload_hash, created_at \
         FROM execution_events WHERE execution_id = ?",
    )
    .bind(execution_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(execution_event_from_row).transpose()
}

fn execution_event_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredExecutionEvent, StoreError> {
    let key: String = row.try_get("execution_id")?;
    let payload = CanonicalJson::from_stored(
        "execution_event",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let event: ExecutionEvent = deserialize_payload("execution_event", &key, &payload)?;

    validate_column(
        "execution_event",
        &key,
        "execution_id",
        &key,
        event.execution_id.as_str(),
    )?;
    validate_column(
        "execution_event",
        &key,
        "command_id",
        &row.try_get::<String, _>("command_id")?,
        event.command_id.as_str(),
    )?;
    validate_optional_column(
        "execution_event",
        &key,
        "plan_id",
        row.try_get("plan_id")?,
        event.plan_id.as_ref().map(PlanId::as_str),
    )?;
    validate_optional_column(
        "execution_event",
        &key,
        "leg_id",
        row.try_get("leg_id")?,
        event.leg_id.as_ref().map(LegId::as_str),
    )?;
    validate_column(
        "execution_event",
        &key,
        "account_id",
        &row.try_get::<String, _>("account_id")?,
        event.account_id.as_str(),
    )?;
    validate_column(
        "execution_event",
        &key,
        "status",
        &row.try_get::<String, _>("status")?,
        event.status.as_str(),
    )?;
    validate_optional_column(
        "execution_event",
        &key,
        "broker_order_id",
        row.try_get("broker_order_id")?,
        event.broker_order_id.as_ref().map(|id| id.as_str()),
    )?;
    validate_optional_column(
        "execution_event",
        &key,
        "position_ticket",
        row.try_get("position_ticket")?,
        event.position_ticket.as_ref().map(|id| id.as_str()),
    )?;
    validate_i64_column(
        "execution_event",
        &key,
        "event_at",
        row.try_get("event_at")?,
        event.event_at,
    )?;
    validate_optional_i64_column(
        "execution_event",
        &key,
        "filled_at",
        row.try_get("filled_at")?,
        event.filled_at,
    )?;

    Ok(StoredExecutionEvent {
        event,
        payload,
        created_at: row.try_get("created_at")?,
    })
}

pub(crate) async fn record_wire_inbox_on(
    connection: &mut SqliteConnection,
    message: NewWireInbox,
) -> Result<WriteOutcome<StoredWireInbox>, StoreError> {
    let sequence = sequence_to_i64("wire_inbox.sequence", message.sequence)?;
    let result = sqlx::query(
        "INSERT INTO wire_inbox (\
            message_id, session_id, message_type, sequence, received_at, handled_at, status, \
            payload_hash\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(message.message_id.as_str())
    .bind(message.session_id.as_ref().map(SessionId::as_str))
    .bind(&message.message_type)
    .bind(sequence)
    .bind(message.received_at)
    .bind(message.handled_at)
    .bind(message.status.as_str())
    .bind(message.wire_message.sha256_hex())
    .execute(&mut *connection)
    .await?;

    let inserted = StoredWireInbox {
        message_id: message.message_id.clone(),
        session_id: message.session_id.clone(),
        message_type: message.message_type.clone(),
        sequence: message.sequence,
        received_at: message.received_at,
        handled_at: message.handled_at,
        status: message.status,
        payload_hash: message.wire_message.sha256_hex().to_owned(),
    };
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let existing = fetch_wire_inbox_conflicts(connection, &message).await?;
    if existing.len() == 1 && same_wire_inbox_fact(&existing[0], &message) {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        Err(StoreError::conflict(
            "wire_inbox",
            format!("message_id={}", message.message_id),
        ))
    }
}

async fn fetch_wire_inbox_by_id<'e, E>(
    executor: E,
    message_id: &MessageId,
) -> Result<Option<StoredWireInbox>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT message_id, session_id, message_type, sequence, received_at, handled_at, status, \
                payload_hash \
         FROM wire_inbox WHERE message_id = ?",
    )
    .bind(message_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(wire_inbox_from_row).transpose()
}

async fn fetch_wire_inbox_conflicts(
    connection: &mut SqliteConnection,
    message: &NewWireInbox,
) -> Result<Vec<StoredWireInbox>, StoreError> {
    let rows = if let (Some(session_id), Some(sequence)) = (&message.session_id, message.sequence) {
        let sequence = sequence_to_i64("wire_inbox.sequence", Some(sequence))?
            .expect("Some sequence remains Some");
        sqlx::query(
            "SELECT message_id, session_id, message_type, sequence, received_at, handled_at, \
                    status, payload_hash \
             FROM wire_inbox \
             WHERE message_id = ? OR (session_id = ? AND sequence = ?)",
        )
        .bind(message.message_id.as_str())
        .bind(session_id.as_str())
        .bind(sequence)
        .fetch_all(&mut *connection)
        .await?
    } else {
        sqlx::query(
            "SELECT message_id, session_id, message_type, sequence, received_at, handled_at, \
                    status, payload_hash \
             FROM wire_inbox WHERE message_id = ?",
        )
        .bind(message.message_id.as_str())
        .fetch_all(&mut *connection)
        .await?
    };
    rows.into_iter().map(wire_inbox_from_row).collect()
}

fn wire_inbox_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredWireInbox, StoreError> {
    let key: String = row.try_get("message_id")?;
    let payload_hash: String = row.try_get("payload_hash")?;
    validate_hash("wire_inbox", &key, &payload_hash)?;
    Ok(StoredWireInbox {
        message_id: MessageId::from(key.clone()),
        session_id: optional_id(&row, "session_id")?,
        message_type: row.try_get("message_type")?,
        sequence: sequence_from_row("wire_inbox", &key, row.try_get("sequence")?)?,
        received_at: row.try_get("received_at")?,
        handled_at: row.try_get("handled_at")?,
        status: parse_enum_column("wire_inbox", &key, "status", row.try_get("status")?)?,
        payload_hash,
    })
}

fn same_wire_inbox_fact(existing: &StoredWireInbox, incoming: &NewWireInbox) -> bool {
    existing.message_id == incoming.message_id
        && existing.session_id == incoming.session_id
        && existing.message_type == incoming.message_type
        && existing.sequence == incoming.sequence
        && existing.payload_hash == incoming.wire_message.sha256_hex()
}

pub(crate) async fn enqueue_wire_outbox_on(
    connection: &mut SqliteConnection,
    message: NewWireOutbox,
) -> Result<WriteOutcome<StoredWireOutbox>, StoreError> {
    let inserted: StoredWireOutbox = message.clone().into();
    validate_stored_wire_outbox(&inserted).map_err(|error| match error {
        StoreError::CorruptData { reason, .. } => StoreError::InvalidRecord {
            entity: "wire_outbox",
            key: message.message_id.to_string(),
            reason,
        },
        other => other,
    })?;
    let sequence = sequence_to_i64("wire_outbox.sequence", message.sequence)?;
    let result = sqlx::query(
        "INSERT INTO wire_outbox (\
            message_id, session_id, message_type, sequence, command_id, request_id, payload_json, \
            payload_hash, status, revision, created_at, updated_at, sent_at, acked_at, last_error\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(message.message_id.as_str())
    .bind(message.session_id.as_ref().map(SessionId::as_str))
    .bind(&message.message_type)
    .bind(sequence)
    .bind(message.command_id.as_ref().map(CommandId::as_str))
    .bind(message.request_id.as_ref().map(RequestId::as_str))
    .bind(message.payload.as_str())
    .bind(message.payload.sha256_hex())
    .bind(message.status.as_str())
    .bind(message.created_at)
    .bind(message.updated_at)
    .bind(message.sent_at)
    .bind(message.acked_at)
    .bind(&message.last_error)
    .execute(&mut *connection)
    .await?;

    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    let existing = fetch_wire_outbox_conflicts(connection, &message).await?;
    if existing.len() == 1 && same_wire_outbox_fact(&existing[0], &message) {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        Err(StoreError::conflict(
            "wire_outbox",
            format!("message_id={}", message.message_id),
        ))
    }
}

pub(crate) async fn fetch_wire_outbox_by_id<'e, E>(
    executor: E,
    message_id: &MessageId,
) -> Result<Option<StoredWireOutbox>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT message_id, session_id, message_type, sequence, command_id, request_id, \
                payload_json, payload_hash, status, revision, created_at, updated_at, sent_at, \
                acked_at, last_error \
         FROM wire_outbox WHERE message_id = ?",
    )
    .bind(message_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(wire_outbox_from_row).transpose()
}

async fn fetch_wire_outbox_conflicts(
    connection: &mut SqliteConnection,
    message: &NewWireOutbox,
) -> Result<Vec<StoredWireOutbox>, StoreError> {
    let rows = if let (Some(session_id), Some(sequence)) = (&message.session_id, message.sequence) {
        let sequence = sequence_to_i64("wire_outbox.sequence", Some(sequence))?
            .expect("Some sequence remains Some");
        sqlx::query(
            "SELECT message_id, session_id, message_type, sequence, command_id, request_id, \
                    payload_json, payload_hash, status, revision, created_at, updated_at, sent_at, \
                    acked_at, last_error \
             FROM wire_outbox \
             WHERE message_id = ? OR (session_id = ? AND sequence = ?)",
        )
        .bind(message.message_id.as_str())
        .bind(session_id.as_str())
        .bind(sequence)
        .fetch_all(&mut *connection)
        .await?
    } else {
        sqlx::query(
            "SELECT message_id, session_id, message_type, sequence, command_id, request_id, \
                    payload_json, payload_hash, status, revision, created_at, updated_at, sent_at, \
                    acked_at, last_error \
             FROM wire_outbox WHERE message_id = ?",
        )
        .bind(message.message_id.as_str())
        .fetch_all(&mut *connection)
        .await?
    };
    rows.into_iter().map(wire_outbox_from_row).collect()
}

pub(crate) fn wire_outbox_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredWireOutbox, StoreError> {
    let key: String = row.try_get("message_id")?;
    let payload = CanonicalJson::from_stored(
        "wire_outbox",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    let stored = StoredWireOutbox {
        message_id: MessageId::from(key.clone()),
        session_id: optional_id(&row, "session_id")?,
        message_type: row.try_get("message_type")?,
        sequence: sequence_from_row("wire_outbox", &key, row.try_get("sequence")?)?,
        command_id: optional_id(&row, "command_id")?,
        request_id: optional_id(&row, "request_id")?,
        payload,
        status: parse_enum_column("wire_outbox", &key, "status", row.try_get("status")?)?,
        revision: non_negative_from_row("wire_outbox", &key, "revision", row.try_get("revision")?)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        sent_at: row.try_get("sent_at")?,
        acked_at: row.try_get("acked_at")?,
        last_error: row.try_get("last_error")?,
    };
    validate_stored_wire_outbox(&stored)?;
    Ok(stored)
}

fn same_wire_outbox_fact(existing: &StoredWireOutbox, incoming: &NewWireOutbox) -> bool {
    existing.message_id == incoming.message_id
        && existing.session_id == incoming.session_id
        && existing.message_type == incoming.message_type
        && existing.sequence == incoming.sequence
        && existing.command_id == incoming.command_id
        && existing.request_id == incoming.request_id
        && existing.payload == incoming.payload
}

pub(crate) async fn insert_session_on(
    connection: &mut SqliteConnection,
    session: NewSessionRecord,
) -> Result<WriteOutcome<StoredSessionRecord>, StoreError> {
    let result = sqlx::query(
        "INSERT INTO execution_client_sessions (\
            session_id, client_id, account_id, terminal_id, platform, status, capabilities_json, \
            remote_addr, connected_at, last_heartbeat_at, last_time_sync_at, clock_sync_status, \
            disconnected_at, revision, updated_at, last_outbound_sequence, max_inflight_commands\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 1, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(session.session_id.as_str())
    .bind(session.client_id.as_str())
    .bind(session.account_id.as_str())
    .bind(session.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(&session.platform)
    .bind(session.status.as_str())
    .bind(session.capabilities.as_str())
    .bind(&session.remote_addr)
    .bind(session.connected_at)
    .bind(session.last_heartbeat_at)
    .bind(session.last_time_sync_at)
    .bind(session.clock_sync_status.map(ClockSyncStatus::as_str))
    .bind(session.disconnected_at)
    .bind(session.updated_at)
    .bind(positive_u64_to_i64(
        "execution_client_sessions.max_inflight_commands",
        session.max_inflight_commands,
    )?)
    .execute(&mut *connection)
    .await?;

    let inserted: StoredSessionRecord = session.clone().into();
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(inserted));
    }

    if let Some(existing) = fetch_session_by_id(&mut *connection, &session.session_id).await? {
        if same_session_identity(&existing, &session) {
            return Ok(WriteOutcome::Duplicate(existing));
        }
        return Err(StoreError::conflict(
            "execution_client_session",
            format!("session_id={}", session.session_id),
        ));
    }

    Err(StoreError::conflict(
        "execution_client_session",
        format!(
            "active identity client_id={},account_id={},terminal_id={}",
            session.client_id,
            session.account_id,
            session.terminal_id.as_deref().unwrap_or("")
        ),
    ))
}

pub(crate) async fn fetch_session_by_id<'e, E>(
    executor: E,
    session_id: &SessionId,
) -> Result<Option<StoredSessionRecord>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT session_id, client_id, account_id, terminal_id, platform, status, \
                capabilities_json, remote_addr, connected_at, last_heartbeat_at, \
                last_time_sync_at, clock_sync_status, disconnected_at, revision, updated_at, \
                last_outbound_sequence, max_inflight_commands \
         FROM execution_client_sessions WHERE session_id = ?",
    )
    .bind(session_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(session_from_row).transpose()
}

pub(crate) fn session_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredSessionRecord, StoreError> {
    let key: String = row.try_get("session_id")?;
    let capabilities_text: String = row.try_get("capabilities_json")?;
    let capabilities = CanonicalJson::parse(&capabilities_text).map_err(|error| {
        StoreError::corrupt(
            "execution_client_session",
            &key,
            format!("invalid capabilities_json: {error}"),
        )
    })?;
    if capabilities.as_str() != capabilities_text {
        return Err(StoreError::corrupt(
            "execution_client_session",
            &key,
            "capabilities_json is not in canonical form",
        ));
    }

    let session = StoredSessionRecord {
        session_id: SessionId::from(key.clone()),
        client_id: ClientId::from(row.try_get::<String, _>("client_id")?),
        account_id: AccountId::from(row.try_get::<String, _>("account_id")?),
        terminal_id: optional_id(&row, "terminal_id")?,
        platform: row.try_get("platform")?,
        status: parse_enum_column(
            "execution_client_session",
            &key,
            "status",
            row.try_get("status")?,
        )?,
        capabilities,
        remote_addr: row.try_get("remote_addr")?,
        connected_at: row.try_get("connected_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        last_time_sync_at: row.try_get("last_time_sync_at")?,
        clock_sync_status: parse_optional_enum_column(
            "execution_client_session",
            &key,
            "clock_sync_status",
            row.try_get("clock_sync_status")?,
        )?,
        disconnected_at: row.try_get("disconnected_at")?,
        revision: non_negative_from_row(
            "execution_client_session",
            &key,
            "revision",
            row.try_get("revision")?,
        )?,
        updated_at: row.try_get("updated_at")?,
        last_outbound_sequence: positive_from_row(
            "execution_client_session",
            &key,
            "last_outbound_sequence",
            row.try_get("last_outbound_sequence")?,
        )?,
        max_inflight_commands: positive_from_row(
            "execution_client_session",
            &key,
            "max_inflight_commands",
            row.try_get("max_inflight_commands")?,
        )?,
    };
    if session.updated_at < session.connected_at
        || session
            .last_heartbeat_at
            .is_some_and(|at| at < session.connected_at || at > session.updated_at)
        || session
            .last_time_sync_at
            .is_some_and(|at| at < session.connected_at || at > session.updated_at)
        || (session.status == sinan_types::SessionStatus::Active
            && session.disconnected_at.is_some())
        || (session.clock_sync_status == Some(ClockSyncStatus::Synced)
            && session.last_time_sync_at.is_none())
        || (session.last_time_sync_at.is_some() && session.clock_sync_status.is_none())
    {
        return Err(StoreError::corrupt(
            "execution_client_session",
            &key,
            "session timestamps, status, or clock-sync evidence are inconsistent",
        ));
    }
    Ok(session)
}

fn same_session_identity(existing: &StoredSessionRecord, incoming: &NewSessionRecord) -> bool {
    existing.session_id == incoming.session_id
        && existing.client_id == incoming.client_id
        && existing.account_id == incoming.account_id
        && existing.terminal_id == incoming.terminal_id
        && existing.platform == incoming.platform
        && existing.capabilities == incoming.capabilities
        && existing.remote_addr == incoming.remote_addr
        && existing.connected_at == incoming.connected_at
        && existing.status == incoming.status
        && existing.last_heartbeat_at == incoming.last_heartbeat_at
        && existing.last_time_sync_at == incoming.last_time_sync_at
        && existing.clock_sync_status == incoming.clock_sync_status
        && existing.disconnected_at == incoming.disconnected_at
        && existing.updated_at == incoming.updated_at
        && existing.max_inflight_commands == incoming.max_inflight_commands
}

pub(crate) fn validate_stored_wire_outbox(message: &StoredWireOutbox) -> Result<(), StoreError> {
    let key = message.message_id.as_str();
    let wire = decode_wire_message::<Value>(
        message.payload.as_str().as_bytes(),
        SUPPORTED_SCHEMA_VERSION,
    )
    .map_err(|error| StoreError::corrupt("wire_outbox", key, error.to_string()))?;

    if wire.message_id != message.message_id
        || wire.message_type.as_str() != message.message_type
        || wire.session_id != message.session_id
        || wire.sequence != message.sequence
    {
        return Err(StoreError::corrupt(
            "wire_outbox",
            key,
            "wire envelope identity does not match denormalized columns",
        ));
    }

    match wire.message_type {
        ExecutionClientMessageType::ExecutionCommand => {
            let payload: ExecutionCommand =
                serde_json::from_value(wire.payload).map_err(|error| {
                    StoreError::corrupt(
                        "wire_outbox",
                        key,
                        format!("invalid execution.command payload: {error}"),
                    )
                })?;
            if message.command_id.as_ref() != Some(&payload.command_id)
                || message.request_id.is_some()
            {
                return Err(StoreError::corrupt(
                    "wire_outbox",
                    key,
                    "execution.command subject does not match payload",
                ));
            }
        }
        ExecutionClientMessageType::ReconciliationRequest => {
            let payload: ReconciliationRequest =
                serde_json::from_value(wire.payload).map_err(|error| {
                    StoreError::corrupt(
                        "wire_outbox",
                        key,
                        format!("invalid reconciliation.request payload: {error}"),
                    )
                })?;
            if message.request_id.as_ref() != Some(&payload.request_id)
                || message.command_id.is_some()
            {
                return Err(StoreError::corrupt(
                    "wire_outbox",
                    key,
                    "reconciliation.request subject does not match payload",
                ));
            }
        }
        _ if message.command_id.is_some() || message.request_id.is_some() => {
            return Err(StoreError::corrupt(
                "wire_outbox",
                key,
                "message type must not carry a delivery subject",
            ));
        }
        _ => {}
    }

    if message.updated_at < message.created_at {
        return Err(StoreError::corrupt(
            "wire_outbox",
            key,
            "updated_at precedes created_at",
        ));
    }
    Ok(())
}

fn deserialize_payload<T: DeserializeOwned>(
    entity: &'static str,
    key: &str,
    payload: &CanonicalJson,
) -> Result<T, StoreError> {
    serde_json::from_str(payload.as_str()).map_err(|error| {
        StoreError::corrupt(entity, key, format!("payload_json DTO mismatch: {error}"))
    })
}

fn optional_id<T>(
    row: &sqlx::sqlite::SqliteRow,
    column: &'static str,
) -> Result<Option<T>, StoreError>
where
    T: From<String>,
{
    Ok(row.try_get::<Option<String>, _>(column)?.map(T::from))
}

fn parse_enum_column<T>(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: String,
) -> Result<T, StoreError>
where
    T: FromStr,
    T::Err: Display,
{
    value.parse().map_err(|error| {
        StoreError::corrupt(
            entity,
            key,
            format!("invalid {column} value {value:?}: {error}"),
        )
    })
}

fn parse_optional_enum_column<T>(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: Option<String>,
) -> Result<Option<T>, StoreError>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .map(|value| parse_enum_column(entity, key, column, value))
        .transpose()
}

fn validate_column(
    entity: &'static str,
    key: &str,
    column: &'static str,
    stored: &str,
    payload: &str,
) -> Result<(), StoreError> {
    if stored == payload {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            format!("{column} does not match payload_json"),
        ))
    }
}

fn validate_optional_column(
    entity: &'static str,
    key: &str,
    column: &'static str,
    stored: Option<String>,
    payload: Option<&str>,
) -> Result<(), StoreError> {
    if stored.as_deref() == payload {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            format!("{column} does not match payload_json"),
        ))
    }
}

fn validate_i64_column(
    entity: &'static str,
    key: &str,
    column: &'static str,
    stored: i64,
    payload: i64,
) -> Result<(), StoreError> {
    if stored == payload {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            format!("{column} does not match payload_json"),
        ))
    }
}

fn validate_optional_i64_column(
    entity: &'static str,
    key: &str,
    column: &'static str,
    stored: Option<i64>,
    payload: Option<i64>,
) -> Result<(), StoreError> {
    if stored == payload {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            format!("{column} does not match payload_json"),
        ))
    }
}

fn sequence_to_i64(field: &'static str, sequence: Option<u64>) -> Result<Option<i64>, StoreError> {
    sequence
        .map(|value| {
            if value == 0 {
                return Err(StoreError::InvalidSequence { field });
            }
            i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
        })
        .transpose()
}

fn sequence_from_row(
    entity: &'static str,
    key: &str,
    sequence: Option<i64>,
) -> Result<Option<u64>, StoreError> {
    sequence
        .map(|value| {
            if value <= 0 {
                return Err(StoreError::corrupt(
                    entity,
                    key,
                    "sequence must be greater than zero",
                ));
            }
            u64::try_from(value)
                .map_err(|_| StoreError::corrupt(entity, key, "sequence does not fit in u64"))
        })
        .transpose()
}

fn non_negative_from_row(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: i64,
) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::corrupt(entity, key, format!("{column} must be non-negative")))
}

fn positive_from_row(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: i64,
) -> Result<u64, StoreError> {
    if value <= 0 {
        return Err(StoreError::corrupt(
            entity,
            key,
            format!("{column} must be greater than zero"),
        ));
    }
    u64::try_from(value)
        .map_err(|_| StoreError::corrupt(entity, key, format!("{column} does not fit in u64")))
}

fn validate_hash(entity: &'static str, key: &str, hash: &str) -> Result<(), StoreError> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            entity,
            key,
            "payload_hash is not lowercase SHA-256 hex",
        ))
    }
}

use std::{collections::HashSet, fmt::Display, str::FromStr};

use serde::de::DeserializeOwned;
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLegAction, CausationId, ClientId, ClockSyncStatus,
    CommandId, CorrelationId, ExecutionCommand, ExecutionCommandState, ExecutionEvent, ExecutionId,
    IdempotencyKey, IntentId, LegId, MessageId, PlanId, RiskId, RiskResult, SessionId, StrategyId,
    TerminalId, TradeIntent, TradeIntentAction, TradeIntentLegAction, TradeIntentStatus,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    connection::{SqliteStateStore, WriteTransaction},
    error::StoreError,
    json::CanonicalJson,
    model::{
        CommandStateUpdate, CoreEventMetadata, NewCoreEvent, NewExecutionCommand,
        NewExecutionEvent, NewRiskResult, NewSessionRecord, NewTradeIntent, NewWireInbox,
        NewWireOutbox, StoredCoreEvent, StoredExecutionCommand, StoredExecutionEvent,
        StoredRiskResult, StoredSessionRecord, StoredTradeIntent, StoredWireInbox,
        StoredWireOutbox, WriteOutcome,
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

async fn fetch_core_event_by_id<'e, E>(
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
    let sequence = sequence_to_i64("wire_outbox.sequence", message.sequence)?;
    let result = sqlx::query(
        "INSERT INTO wire_outbox (\
            message_id, session_id, message_type, sequence, command_id, payload_json, \
            payload_hash, status, created_at, sent_at, acked_at, last_error\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(message.message_id.as_str())
    .bind(message.session_id.as_ref().map(SessionId::as_str))
    .bind(&message.message_type)
    .bind(sequence)
    .bind(message.command_id.as_ref().map(CommandId::as_str))
    .bind(message.payload.as_str())
    .bind(message.payload.sha256_hex())
    .bind(message.status.as_str())
    .bind(message.created_at)
    .bind(message.sent_at)
    .bind(message.acked_at)
    .bind(&message.last_error)
    .execute(&mut *connection)
    .await?;

    let inserted: StoredWireOutbox = message.clone().into();
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

async fn fetch_wire_outbox_by_id<'e, E>(
    executor: E,
    message_id: &MessageId,
) -> Result<Option<StoredWireOutbox>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT message_id, session_id, message_type, sequence, command_id, payload_json, \
                payload_hash, status, created_at, sent_at, acked_at, last_error \
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
            "SELECT message_id, session_id, message_type, sequence, command_id, payload_json, \
                    payload_hash, status, created_at, sent_at, acked_at, last_error \
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
            "SELECT message_id, session_id, message_type, sequence, command_id, payload_json, \
                    payload_hash, status, created_at, sent_at, acked_at, last_error \
             FROM wire_outbox WHERE message_id = ?",
        )
        .bind(message.message_id.as_str())
        .fetch_all(&mut *connection)
        .await?
    };
    rows.into_iter().map(wire_outbox_from_row).collect()
}

fn wire_outbox_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredWireOutbox, StoreError> {
    let key: String = row.try_get("message_id")?;
    let payload = CanonicalJson::from_stored(
        "wire_outbox",
        &key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    Ok(StoredWireOutbox {
        message_id: MessageId::from(key.clone()),
        session_id: optional_id(&row, "session_id")?,
        message_type: row.try_get("message_type")?,
        sequence: sequence_from_row("wire_outbox", &key, row.try_get("sequence")?)?,
        command_id: optional_id(&row, "command_id")?,
        payload,
        status: parse_enum_column("wire_outbox", &key, "status", row.try_get("status")?)?,
        created_at: row.try_get("created_at")?,
        sent_at: row.try_get("sent_at")?,
        acked_at: row.try_get("acked_at")?,
        last_error: row.try_get("last_error")?,
    })
}

fn same_wire_outbox_fact(existing: &StoredWireOutbox, incoming: &NewWireOutbox) -> bool {
    existing.message_id == incoming.message_id
        && existing.session_id == incoming.session_id
        && existing.message_type == incoming.message_type
        && existing.sequence == incoming.sequence
        && existing.command_id == incoming.command_id
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
            disconnected_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
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

async fn fetch_session_by_id<'e, E>(
    executor: E,
    session_id: &SessionId,
) -> Result<Option<StoredSessionRecord>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT session_id, client_id, account_id, terminal_id, platform, status, \
                capabilities_json, remote_addr, connected_at, last_heartbeat_at, \
                last_time_sync_at, clock_sync_status, disconnected_at \
         FROM execution_client_sessions WHERE session_id = ?",
    )
    .bind(session_id.as_str())
    .fetch_optional(executor)
    .await?;
    row.map(session_from_row).transpose()
}

fn session_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredSessionRecord, StoreError> {
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

    Ok(StoredSessionRecord {
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
    })
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

//! Durable, bounded-replay event stream storage.

use std::{collections::HashSet, str::FromStr};

use sinan_types::{AccountId, EventStreamTopic};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection};
use thiserror::Error;

use crate::{
    AuthorizedAccountScope, CanonicalJson, SqliteStateStore, StoreError, WriteOutcome,
    WriteTransaction,
};

const EVENT_STREAM_COLUMNS: &str = "stream_sequence, event_id, topic, account_id, event_type, \
    payload_json, payload_hash, created_at";

/// A summary event ready to be appended to the bounded replay log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewEventStreamRecord {
    pub event_id: String,
    pub topic: EventStreamTopic,
    /// `None` denotes a global event visible to every authorized event subscriber.
    pub account_id: Option<AccountId>,
    pub event_type: String,
    pub payload: CanonicalJson,
    pub created_at: i64,
}

/// A persisted summary event ordered by its database-assigned stream sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventStreamRecord {
    pub stream_sequence: u64,
    pub event_id: String,
    pub topic: EventStreamTopic,
    pub account_id: Option<AccountId>,
    pub event_type: String,
    pub payload: CanonicalJson,
    pub created_at: i64,
}

/// Topic and account authorization applied to replay reads.
///
/// Global system/deadletter records remain visible even when the account scope
/// is empty. Account-bound records require an explicit scope entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventStreamFilter {
    topics: HashSet<EventStreamTopic>,
    account_scope: AuthorizedAccountScope,
}

impl EventStreamFilter {
    pub fn new(
        topics: impl IntoIterator<Item = EventStreamTopic>,
        account_scope: AuthorizedAccountScope,
    ) -> Self {
        Self {
            topics: topics.into_iter().collect(),
            account_scope,
        }
    }

    pub fn all_topics(account_scope: AuthorizedAccountScope) -> Self {
        Self::new(EventStreamTopic::ALL.iter().copied(), account_scope)
    }

    pub fn topics(&self) -> impl Iterator<Item = EventStreamTopic> + '_ {
        self.topics.iter().copied()
    }

    pub fn account_scope(&self) -> &AuthorizedAccountScope {
        &self.account_scope
    }

    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }

    pub fn matches(&self, record: &EventStreamRecord) -> bool {
        self.topics.contains(&record.topic)
            && event_is_visible(
                record.topic,
                record.account_id.as_ref(),
                &self.account_scope,
            )
    }
}

/// One cursor-exclusive replay snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventReplayWindow {
    /// Global high-water sequence captured in the same read transaction as replay.
    pub high_water: Option<u64>,
    pub records: Vec<EventStreamRecord>,
    /// More matching records existed before `high_water` than the requested limit.
    pub has_more: bool,
}

/// Retention constraints are combined as deletion conditions.
///
/// A record is pruned when it falls outside the latest-count window or is older
/// than `created_at_or_after`. At least one constraint must be configured.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventRetentionPolicy {
    pub retain_latest: Option<u64>,
    pub created_at_or_after: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventRetentionOutcome {
    pub deleted: u64,
    pub oldest_remaining_event_id: Option<String>,
    pub oldest_remaining_sequence: Option<u64>,
}

#[derive(Debug, Error)]
pub enum EventReplayError {
    #[error("event stream cursor {event_id} is expired or outside the authorized account scope")]
    CursorExpired { event_id: String },

    #[error("event replay limit must be greater than zero")]
    InvalidLimit,

    #[error(transparent)]
    Store(#[from] StoreError),
}

impl SqliteStateStore {
    /// Appends one event durably before it may be offered to live subscribers.
    pub async fn append_event_stream_record(
        &self,
        event: NewEventStreamRecord,
    ) -> Result<WriteOutcome<EventStreamRecord>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = transaction.append_event_stream_record(event).await;
        match result {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn get_event_stream_record(
        &self,
        event_id: &str,
    ) -> Result<Option<EventStreamRecord>, StoreError> {
        fetch_event_stream_record(self.pool(), event_id).await
    }

    pub async fn event_stream_high_water(&self) -> Result<Option<u64>, StoreError> {
        let sequence: Option<i64> =
            sqlx::query_scalar("SELECT MAX(stream_sequence) FROM event_stream_log")
                .fetch_one(self.pool())
                .await?;
        sequence
            .map(|value| sequence_from_sql("event_stream", "high_water", value))
            .transpose()
    }

    /// Captures a high-water mark and returns authorized events strictly after
    /// `last_event_id`, all from one SQLite read snapshot.
    ///
    /// With no cursor, this establishes a live-only boundary and returns no
    /// historical records. A missing or unauthorized cursor has the same
    /// `CursorExpired` outcome so record existence is not disclosed.
    pub async fn replay_event_stream(
        &self,
        last_event_id: Option<&str>,
        filter: &EventStreamFilter,
        limit: u64,
    ) -> Result<EventReplayWindow, EventReplayError> {
        if limit == 0 {
            return Err(EventReplayError::InvalidLimit);
        }

        let mut transaction = self.pool.begin().await.map_err(StoreError::from)?;
        let result = replay_event_stream_on(&mut transaction, last_event_id, filter, limit).await;
        match result {
            Ok(window) => {
                transaction.commit().await.map_err(StoreError::from)?;
                Ok(window)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn prune_event_stream(
        &self,
        policy: EventRetentionPolicy,
    ) -> Result<EventRetentionOutcome, StoreError> {
        validate_retention_policy(policy)?;
        let mut transaction = self.begin_write().await?;
        let result = prune_event_stream_on(transaction.connection(), policy).await;
        match result {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

impl WriteTransaction {
    pub async fn append_event_stream_record(
        &mut self,
        event: NewEventStreamRecord,
    ) -> Result<WriteOutcome<EventStreamRecord>, StoreError> {
        append_event_stream_record_on(self.connection(), event).await
    }
}

async fn append_event_stream_record_on(
    connection: &mut SqliteConnection,
    event: NewEventStreamRecord,
) -> Result<WriteOutcome<EventStreamRecord>, StoreError> {
    validate_new_event(&event)?;
    let result = sqlx::query(
        "INSERT INTO event_stream_log (\
             event_id, topic, account_id, event_type, payload_json, payload_hash, created_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING",
    )
    .bind(&event.event_id)
    .bind(event.topic.as_str())
    .bind(event.account_id.as_ref().map(AccountId::as_str))
    .bind(&event.event_type)
    .bind(event.payload.as_str())
    .bind(event.payload.sha256_hex())
    .bind(event.created_at)
    .execute(&mut *connection)
    .await?;

    let stored = fetch_event_stream_record(&mut *connection, &event.event_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "event_stream",
            key: event.event_id.clone(),
        })?;

    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(stored));
    }

    if same_event_stream_record(&stored, &event) {
        Ok(WriteOutcome::Duplicate(stored))
    } else {
        Err(StoreError::conflict("event_stream", event.event_id))
    }
}

async fn replay_event_stream_on(
    connection: &mut SqliteConnection,
    last_event_id: Option<&str>,
    filter: &EventStreamFilter,
    limit: u64,
) -> Result<EventReplayWindow, EventReplayError> {
    let high_water_sql: Option<i64> =
        sqlx::query_scalar("SELECT MAX(stream_sequence) FROM event_stream_log")
            .fetch_one(&mut *connection)
            .await
            .map_err(StoreError::from)?;
    let high_water = high_water_sql
        .map(|value| sequence_from_sql("event_stream", "high_water", value))
        .transpose()?;

    let Some(last_event_id) = last_event_id else {
        return Ok(EventReplayWindow {
            high_water,
            records: Vec::new(),
            has_more: false,
        });
    };

    let cursor = fetch_event_stream_record(&mut *connection, last_event_id)
        .await?
        .filter(|record| {
            event_is_visible(
                record.topic,
                record.account_id.as_ref(),
                &filter.account_scope,
            )
        })
        .ok_or_else(|| EventReplayError::CursorExpired {
            event_id: last_event_id.to_owned(),
        })?;

    let Some(high_water) = high_water else {
        return Err(EventReplayError::CursorExpired {
            event_id: last_event_id.to_owned(),
        });
    };

    if filter.is_empty() || cursor.stream_sequence >= high_water {
        return Ok(EventReplayWindow {
            high_water: Some(high_water),
            records: Vec::new(),
            has_more: false,
        });
    }

    let fetch_limit = limit.checked_add(1).ok_or(StoreError::InvalidInteger {
        field: "event_replay_limit",
        value: limit,
    })?;
    let fetch_limit = i64::try_from(fetch_limit).map_err(|_| StoreError::InvalidInteger {
        field: "event_replay_limit",
        value: fetch_limit,
    })?;
    let high_water_sql = i64::try_from(high_water).map_err(|_| StoreError::InvalidInteger {
        field: "event_stream_high_water",
        value: high_water,
    })?;
    let cursor_sql =
        i64::try_from(cursor.stream_sequence).map_err(|_| StoreError::InvalidInteger {
            field: "event_stream_cursor",
            value: cursor.stream_sequence,
        })?;

    let mut query = QueryBuilder::<Sqlite>::new(format!(
        "SELECT {EVENT_STREAM_COLUMNS} FROM event_stream_log WHERE stream_sequence > "
    ));
    query.push_bind(cursor_sql);
    query.push(" AND stream_sequence <= ");
    query.push_bind(high_water_sql);
    query.push(" AND topic IN (");
    {
        let mut topics = query.separated(", ");
        for topic in filter.topics() {
            topics.push_bind(topic.as_str());
        }
    }
    query.push(") AND ((account_id IS NULL AND topic IN ('system.event', 'deadletter.summary'))");
    if !filter.account_scope.is_empty() {
        query.push(" OR account_id IN (");
        {
            let mut accounts = query.separated(", ");
            for account_id in filter.account_scope.iter() {
                accounts.push_bind(account_id.as_str());
            }
        }
        query.push(')');
    }
    query.push(") ORDER BY stream_sequence ASC LIMIT ");
    query.push_bind(fetch_limit);

    let rows = query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(StoreError::from)?;
    let mut records = rows
        .into_iter()
        .map(event_stream_record_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = u64::try_from(records.len()).is_ok_and(|length| length > limit);
    if has_more {
        records.pop();
    }

    Ok(EventReplayWindow {
        high_water: Some(high_water),
        records,
        has_more,
    })
}

async fn prune_event_stream_on(
    connection: &mut SqliteConnection,
    policy: EventRetentionPolicy,
) -> Result<EventRetentionOutcome, StoreError> {
    let sequence_cutoff = if let Some(retain_latest) = policy.retain_latest {
        let offset = i64::try_from(retain_latest).map_err(|_| StoreError::InvalidInteger {
            field: "event_retention_retain_latest",
            value: retain_latest,
        })?;
        sqlx::query_scalar::<_, i64>(
            "SELECT stream_sequence FROM event_stream_log \
             ORDER BY stream_sequence DESC LIMIT 1 OFFSET ?",
        )
        .bind(offset)
        .fetch_optional(&mut *connection)
        .await?
    } else {
        None
    };

    let deleted = if sequence_cutoff.is_some() || policy.created_at_or_after.is_some() {
        let mut query = QueryBuilder::<Sqlite>::new("DELETE FROM event_stream_log WHERE ");
        let mut has_condition = false;
        if let Some(sequence_cutoff) = sequence_cutoff {
            query.push("stream_sequence <= ").push_bind(sequence_cutoff);
            has_condition = true;
        }
        if let Some(created_at) = policy.created_at_or_after {
            if has_condition {
                query.push(" OR ");
            }
            query.push("created_at < ").push_bind(created_at);
        }
        query
            .build()
            .execute(&mut *connection)
            .await?
            .rows_affected()
    } else {
        0
    };

    let oldest = sqlx::query(
        "SELECT stream_sequence, event_id FROM event_stream_log \
         ORDER BY stream_sequence ASC LIMIT 1",
    )
    .fetch_optional(&mut *connection)
    .await?;
    let (oldest_remaining_sequence, oldest_remaining_event_id) = match oldest {
        Some(row) => {
            let event_id: String = row.try_get("event_id")?;
            let sequence =
                sequence_from_sql("event_stream", &event_id, row.try_get("stream_sequence")?)?;
            (Some(sequence), Some(event_id))
        }
        None => (None, None),
    };

    Ok(EventRetentionOutcome {
        deleted,
        oldest_remaining_event_id,
        oldest_remaining_sequence,
    })
}

async fn fetch_event_stream_record<'e, E>(
    executor: E,
    event_id: &str,
) -> Result<Option<EventStreamRecord>, StoreError>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let query = format!("SELECT {EVENT_STREAM_COLUMNS} FROM event_stream_log WHERE event_id = ?");
    let row = sqlx::query(&query)
        .bind(event_id)
        .fetch_optional(executor)
        .await?;
    row.map(event_stream_record_from_row).transpose()
}

fn event_stream_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<EventStreamRecord, StoreError> {
    let event_id: String = row.try_get("event_id")?;
    let sequence = sequence_from_sql("event_stream", &event_id, row.try_get("stream_sequence")?)?;
    let topic_text: String = row.try_get("topic")?;
    let topic =
        EventStreamTopic::from_str(&topic_text).map_err(|error| StoreError::CorruptData {
            entity: "event_stream",
            key: event_id.clone(),
            reason: error.to_string(),
        })?;
    let payload = CanonicalJson::from_stored(
        "event_stream",
        &event_id,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;

    Ok(EventStreamRecord {
        stream_sequence: sequence,
        event_id,
        topic,
        account_id: row
            .try_get::<Option<String>, _>("account_id")?
            .map(AccountId::from),
        event_type: row.try_get("event_type")?,
        payload,
        created_at: row.try_get("created_at")?,
    })
}

fn validate_new_event(event: &NewEventStreamRecord) -> Result<(), StoreError> {
    if event.event_id.is_empty() {
        return Err(StoreError::InvalidRecord {
            entity: "event_stream",
            key: event.event_id.clone(),
            reason: "event_id must not be empty".to_owned(),
        });
    }
    if event.event_type.is_empty() {
        return Err(StoreError::InvalidRecord {
            entity: "event_stream",
            key: event.event_id.clone(),
            reason: "event_type must not be empty".to_owned(),
        });
    }
    if event.account_id.as_ref().is_some_and(AccountId::is_empty) {
        return Err(StoreError::InvalidRecord {
            entity: "event_stream",
            key: event.event_id.clone(),
            reason: "account_id must be absent or non-empty".to_owned(),
        });
    }
    if event.account_id.is_none() && !is_global_topic(event.topic) {
        return Err(StoreError::InvalidRecord {
            entity: "event_stream",
            key: event.event_id.clone(),
            reason: format!(
                "topic {} requires a non-empty account_id",
                event.topic.as_str()
            ),
        });
    }
    Ok(())
}

fn event_is_visible(
    topic: EventStreamTopic,
    account_id: Option<&AccountId>,
    account_scope: &AuthorizedAccountScope,
) -> bool {
    match account_id {
        Some(account_id) => account_scope.contains(account_id),
        None => is_global_topic(topic),
    }
}

fn is_global_topic(topic: EventStreamTopic) -> bool {
    matches!(
        topic,
        EventStreamTopic::SystemEvent | EventStreamTopic::DeadletterSummary
    )
}

fn validate_retention_policy(policy: EventRetentionPolicy) -> Result<(), StoreError> {
    if policy.retain_latest.is_none() && policy.created_at_or_after.is_none() {
        return Err(StoreError::InvalidRecord {
            entity: "event_retention_policy",
            key: "event_stream".to_owned(),
            reason: "at least one retention bound is required".to_owned(),
        });
    }
    if policy.retain_latest == Some(0) {
        return Err(StoreError::InvalidSequence {
            field: "event_retention_retain_latest",
        });
    }
    Ok(())
}

fn same_event_stream_record(existing: &EventStreamRecord, incoming: &NewEventStreamRecord) -> bool {
    existing.event_id == incoming.event_id
        && existing.topic == incoming.topic
        && existing.account_id == incoming.account_id
        && existing.event_type == incoming.event_type
        && existing.payload == incoming.payload
        && existing.created_at == incoming.created_at
}

fn sequence_from_sql(entity: &'static str, key: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StoreError::CorruptData {
            entity,
            key: key.to_owned(),
            reason: format!("stream_sequence must be positive, found {value}"),
        })
}

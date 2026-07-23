use std::str::FromStr;

use sinan_types::MessageId;
use sqlx::Row;

use crate::{CanonicalJson, SqliteStateStore, StoreError, WriteOutcome, WriteTransaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemEventSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl SystemEventSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
            Self::Critical => "CRITICAL",
        }
    }
}

impl FromStr for SystemEventSeverity {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "INFO" => Ok(Self::Info),
            "WARNING" => Ok(Self::Warning),
            "ERROR" => Ok(Self::Error),
            "CRITICAL" => Ok(Self::Critical),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewSystemEvent {
    pub system_event_id: String,
    pub event_type: String,
    pub severity: SystemEventSeverity,
    pub component: String,
    pub message: String,
    pub metadata: Option<CanonicalJson>,
    pub timestamp: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSystemEvent {
    pub system_event_id: String,
    pub event_type: String,
    pub severity: SystemEventSeverity,
    pub component: String,
    pub message: String,
    pub metadata: Option<CanonicalJson>,
    pub timestamp: i64,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadletterReason {
    UnknownType,
    SchemaMajorMismatch,
    MissingRequiredField,
    InvalidFieldType,
    InvalidHmac,
    DecodeFailed,
    WireFrameTooLarge,
    WireProtocolViolation,
    SchemaValidationFailed,
}

impl DeadletterReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownType => "UNKNOWN_TYPE",
            Self::SchemaMajorMismatch => "SCHEMA_MAJOR_MISMATCH",
            Self::MissingRequiredField => "MISSING_REQUIRED_FIELD",
            Self::InvalidFieldType => "INVALID_FIELD_TYPE",
            Self::InvalidHmac => "INVALID_HMAC",
            Self::DecodeFailed => "DECODE_FAILED",
            Self::WireFrameTooLarge => "WIRE_FRAME_TOO_LARGE",
            Self::WireProtocolViolation => "WIRE_PROTOCOL_VIOLATION",
            Self::SchemaValidationFailed => "SCHEMA_VALIDATION_FAILED",
        }
    }
}

impl FromStr for DeadletterReason {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "UNKNOWN_TYPE" => Ok(Self::UnknownType),
            "SCHEMA_MAJOR_MISMATCH" => Ok(Self::SchemaMajorMismatch),
            "MISSING_REQUIRED_FIELD" => Ok(Self::MissingRequiredField),
            "INVALID_FIELD_TYPE" => Ok(Self::InvalidFieldType),
            "INVALID_HMAC" => Ok(Self::InvalidHmac),
            "DECODE_FAILED" => Ok(Self::DecodeFailed),
            "WIRE_FRAME_TOO_LARGE" => Ok(Self::WireFrameTooLarge),
            "WIRE_PROTOCOL_VIOLATION" => Ok(Self::WireProtocolViolation),
            "SCHEMA_VALIDATION_FAILED" => Ok(Self::SchemaValidationFailed),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewDeadletterEvent {
    pub deadletter_id: String,
    pub message_id: Option<MessageId>,
    pub message_type: Option<String>,
    pub schema_version: Option<String>,
    pub reason: DeadletterReason,
    pub source: String,
    pub raw_payload: Option<String>,
    /// Original byte length before any producer-side truncation.
    pub raw_payload_length: Option<u64>,
    pub error_message: String,
    pub received_at: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredDeadletterEvent {
    pub deadletter_id: String,
    pub message_id: Option<MessageId>,
    pub message_type: Option<String>,
    pub schema_version: Option<String>,
    pub reason: DeadletterReason,
    pub source: String,
    pub raw_payload: Option<String>,
    pub raw_payload_length: Option<u64>,
    pub error_message: String,
    pub received_at: i64,
    pub created_at: i64,
}

impl SqliteStateStore {
    pub async fn append_system_event(
        &self,
        event: NewSystemEvent,
    ) -> Result<WriteOutcome<StoredSystemEvent>, StoreError> {
        validate_system_event(&event)?;
        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "INSERT INTO system_events (\
                system_event_id, type, severity, component, message, metadata_json, timestamp, \
                created_at\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(&event.system_event_id)
        .bind(&event.event_type)
        .bind(event.severity.as_str())
        .bind(&event.component)
        .bind(&event.message)
        .bind(event.metadata.as_ref().map(CanonicalJson::as_str))
        .bind(event.timestamp)
        .bind(event.created_at)
        .execute(transaction.connection())
        .await?;
        let stored =
            required_system_event(transaction.connection(), &event.system_event_id).await?;
        if !same_system_event(&stored, &event) {
            return Err(StoreError::conflict("system_event", event.system_event_id));
        }
        transaction.commit().await?;
        Ok(if result.rows_affected() == 1 {
            WriteOutcome::Inserted(stored)
        } else {
            WriteOutcome::Duplicate(stored)
        })
    }

    pub async fn get_system_event(
        &self,
        system_event_id: &str,
    ) -> Result<Option<StoredSystemEvent>, StoreError> {
        let row = sqlx::query(
            "SELECT system_event_id, type, severity, component, message, metadata_json, \
                    timestamp, created_at \
             FROM system_events WHERE system_event_id = ?",
        )
        .bind(system_event_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(system_event_from_row).transpose()
    }

    pub async fn append_deadletter_event(
        &self,
        event: NewDeadletterEvent,
    ) -> Result<WriteOutcome<StoredDeadletterEvent>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.append_deadletter_event(event).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_deadletter_event(
        &self,
        deadletter_id: &str,
    ) -> Result<Option<StoredDeadletterEvent>, StoreError> {
        let row = sqlx::query(
            "SELECT deadletter_id, message_id, message_type, schema_version, reason, raw_payload, \
                    received_at, created_at, source, raw_payload_length, error_message \
             FROM deadletter_events WHERE deadletter_id = ?",
        )
        .bind(deadletter_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(deadletter_event_from_row).transpose()
    }
}

impl WriteTransaction {
    pub async fn append_deadletter_event(
        &mut self,
        event: NewDeadletterEvent,
    ) -> Result<WriteOutcome<StoredDeadletterEvent>, StoreError> {
        append_deadletter_event_on(self.connection(), event).await
    }
}

async fn append_deadletter_event_on(
    connection: &mut sqlx::SqliteConnection,
    event: NewDeadletterEvent,
) -> Result<WriteOutcome<StoredDeadletterEvent>, StoreError> {
    validate_deadletter_event(&event)?;
    let raw_payload_length = event
        .raw_payload_length
        .map(|value| integer("deadletter_events.raw_payload_length", value))
        .transpose()?;
    let result = sqlx::query(
        "INSERT INTO deadletter_events (\
            deadletter_id, message_id, message_type, schema_version, reason, raw_payload, \
            received_at, created_at, source, raw_payload_length, error_message\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
    )
    .bind(&event.deadletter_id)
    .bind(event.message_id.as_ref().map(MessageId::as_str))
    .bind(event.message_type.as_deref())
    .bind(event.schema_version.as_deref())
    .bind(event.reason.as_str())
    .bind(event.raw_payload.as_deref())
    .bind(event.received_at)
    .bind(event.created_at)
    .bind(&event.source)
    .bind(raw_payload_length)
    .bind(&event.error_message)
    .execute(&mut *connection)
    .await?;
    let stored = required_deadletter_event(connection, &event.deadletter_id).await?;
    if !same_deadletter_event(&stored, &event) {
        return Err(StoreError::conflict(
            "deadletter_event",
            event.deadletter_id,
        ));
    }
    Ok(if result.rows_affected() == 1 {
        WriteOutcome::Inserted(stored)
    } else {
        WriteOutcome::Duplicate(stored)
    })
}

async fn required_system_event(
    connection: &mut sqlx::SqliteConnection,
    event_id: &str,
) -> Result<StoredSystemEvent, StoreError> {
    let row = sqlx::query(
        "SELECT system_event_id, type, severity, component, message, metadata_json, \
                timestamp, created_at FROM system_events WHERE system_event_id = ?",
    )
    .bind(event_id)
    .fetch_optional(connection)
    .await?;
    row.map(system_event_from_row)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound {
            entity: "system_event",
            key: event_id.to_owned(),
        })
}

async fn required_deadletter_event(
    connection: &mut sqlx::SqliteConnection,
    event_id: &str,
) -> Result<StoredDeadletterEvent, StoreError> {
    let row = sqlx::query(
        "SELECT deadletter_id, message_id, message_type, schema_version, reason, raw_payload, \
                received_at, created_at, source, raw_payload_length, error_message \
         FROM deadletter_events WHERE deadletter_id = ?",
    )
    .bind(event_id)
    .fetch_optional(connection)
    .await?;
    row.map(deadletter_event_from_row)
        .transpose()?
        .ok_or_else(|| StoreError::NotFound {
            entity: "deadletter_event",
            key: event_id.to_owned(),
        })
}

fn system_event_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredSystemEvent, StoreError> {
    let event_id: String = row.try_get("system_event_id")?;
    let severity_raw: String = row.try_get("severity")?;
    let severity = severity_raw.parse().map_err(|_| {
        StoreError::corrupt(
            "system_event",
            &event_id,
            format!("unknown severity {severity_raw}"),
        )
    })?;
    let metadata = row
        .try_get::<Option<String>, _>("metadata_json")?
        .map(|value| CanonicalJson::parse(&value))
        .transpose()?;
    Ok(StoredSystemEvent {
        system_event_id: event_id,
        event_type: row.try_get("type")?,
        severity,
        component: row.try_get("component")?,
        message: row.try_get("message")?,
        metadata,
        timestamp: row.try_get("timestamp")?,
        created_at: row.try_get("created_at")?,
    })
}

fn deadletter_event_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredDeadletterEvent, StoreError> {
    let event_id: String = row.try_get("deadletter_id")?;
    let reason_raw: String = row.try_get("reason")?;
    let reason = reason_raw.parse().map_err(|_| {
        StoreError::corrupt(
            "deadletter_event",
            &event_id,
            format!("unknown reason {reason_raw}"),
        )
    })?;
    let raw_payload_length = row
        .try_get::<Option<i64>, _>("raw_payload_length")?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                StoreError::corrupt(
                    "deadletter_event",
                    &event_id,
                    "raw_payload_length is negative",
                )
            })
        })
        .transpose()?;
    Ok(StoredDeadletterEvent {
        deadletter_id: event_id,
        message_id: row
            .try_get::<Option<String>, _>("message_id")?
            .map(MessageId::from),
        message_type: row.try_get("message_type")?,
        schema_version: row.try_get("schema_version")?,
        reason,
        source: row.try_get("source")?,
        raw_payload: row.try_get("raw_payload")?,
        raw_payload_length,
        error_message: row.try_get("error_message")?,
        received_at: row.try_get("received_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn validate_system_event(event: &NewSystemEvent) -> Result<(), StoreError> {
    require("system_event_id", &event.system_event_id)?;
    require("system event type", &event.event_type)?;
    require("system event component", &event.component)?;
    require("system event message", &event.message)?;
    timestamp("system_event", event.timestamp)?;
    timestamp("system_event", event.created_at)
}

fn validate_deadletter_event(event: &NewDeadletterEvent) -> Result<(), StoreError> {
    require("deadletter_id", &event.deadletter_id)?;
    require("deadletter source", &event.source)?;
    require("deadletter error_message", &event.error_message)?;
    timestamp("deadletter_event", event.received_at)?;
    timestamp("deadletter_event", event.created_at)?;
    if let Some(raw_payload) = &event.raw_payload {
        let Some(original_length) = event.raw_payload_length else {
            return invalid(
                "deadletter_event",
                &event.deadletter_id,
                "raw_payload_length is required when raw_payload is present",
            );
        };
        if original_length < raw_payload.len() as u64 {
            return invalid(
                "deadletter_event",
                &event.deadletter_id,
                "raw_payload_length is shorter than the retained payload",
            );
        }
    }
    Ok(())
}

fn same_system_event(stored: &StoredSystemEvent, incoming: &NewSystemEvent) -> bool {
    stored.system_event_id == incoming.system_event_id
        && stored.event_type == incoming.event_type
        && stored.severity == incoming.severity
        && stored.component == incoming.component
        && stored.message == incoming.message
        && stored.metadata == incoming.metadata
        && stored.timestamp == incoming.timestamp
        && stored.created_at == incoming.created_at
}

fn same_deadletter_event(stored: &StoredDeadletterEvent, incoming: &NewDeadletterEvent) -> bool {
    stored.deadletter_id == incoming.deadletter_id
        && stored.message_id == incoming.message_id
        && stored.message_type == incoming.message_type
        && stored.schema_version == incoming.schema_version
        && stored.reason == incoming.reason
        && stored.source == incoming.source
        && stored.raw_payload == incoming.raw_payload
        && stored.raw_payload_length == incoming.raw_payload_length
        && stored.error_message == incoming.error_message
        && stored.received_at == incoming.received_at
        && stored.created_at == incoming.created_at
}

fn require(field: &'static str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        invalid("transport_event", field, "value must not be empty")
    } else {
        Ok(())
    }
}

fn timestamp(entity: &'static str, value: i64) -> Result<(), StoreError> {
    if value < 0 {
        invalid(entity, &value.to_string(), "timestamp must be non-negative")
    } else {
        Ok(())
    }
}

fn integer(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

fn invalid<T>(entity: &'static str, key: &str, reason: &str) -> Result<T, StoreError> {
    Err(StoreError::InvalidRecord {
        entity,
        key: key.to_owned(),
        reason: reason.to_owned(),
    })
}

use thiserror::Error;

/// Errors produced while validating or persisting state-store records.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{entity} identity conflict for {key}")]
    IdentityConflict { entity: &'static str, key: String },

    #[error("{entity} observation conflict for {key} at {observed_at}")]
    ObservationConflict {
        entity: &'static str,
        key: String,
        observed_at: i64,
    },

    #[error("{entity} not found for {key}")]
    NotFound { entity: &'static str, key: String },

    #[error("stale {entity} write for {key}")]
    StaleWrite { entity: &'static str, key: String },

    #[error("invalid {entity} record {key}: {reason}")]
    InvalidRecord {
        entity: &'static str,
        key: String,
        reason: String,
    },

    #[error("{entity} record {key} is corrupt: {reason}")]
    CorruptData {
        entity: &'static str,
        key: String,
        reason: String,
    },

    #[error("{field} value {value} does not fit in a SQLite INTEGER")]
    InvalidInteger { field: &'static str, value: u64 },

    #[error("{field} must be greater than zero")]
    InvalidSequence { field: &'static str },

    #[error("failed to initialize the state store: {0}")]
    Initialization(String),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl StoreError {
    pub(crate) fn corrupt(
        entity: &'static str,
        key: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::CorruptData {
            entity,
            key: key.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn conflict(entity: &'static str, key: impl Into<String>) -> Self {
        Self::IdentityConflict {
            entity,
            key: key.into(),
        }
    }
}

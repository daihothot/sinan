use sinan_execution::CommandTransitionError;
use sinan_types::CommandId;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ReconciliationError {
    #[error("invalid reconciliation request field {field}: {reason}")]
    InvalidRequest { field: &'static str, reason: String },

    #[error("invalid reconciliation context field {field}: {reason}")]
    InvalidContext { field: &'static str, reason: String },

    #[error("invalid reconciliation result field {field}: {reason}")]
    InvalidResult { field: &'static str, reason: String },

    #[error("invalid manual escalation field {field}: {reason}")]
    InvalidManualEscalation { field: &'static str, reason: String },

    #[error("command {command_id} cannot apply reconciliation lifecycle evidence: {source}")]
    CommandTransition {
        command_id: CommandId,
        #[source]
        source: CommandTransitionError,
    },
}

impl ReconciliationError {
    pub(crate) fn request(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            reason: reason.into(),
        }
    }

    pub(crate) fn context(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidContext {
            field,
            reason: reason.into(),
        }
    }

    pub(crate) fn result(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidResult {
            field,
            reason: reason.into(),
        }
    }

    pub(crate) fn manual(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidManualEscalation {
            field,
            reason: reason.into(),
        }
    }
}

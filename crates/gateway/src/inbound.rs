use std::{future::Future, pin::Pin};

use serde_json::Value;
use sinan_protocol::{
    ExecutionClientMessageType, ExecutionClientPlatform, ResumeCursor, SchemaVersion, WireMessage,
};
use sinan_types::{AccountId, ClientId, ErrorCode, MessageId, SessionId, TerminalId};
use thiserror::Error;

use crate::ClientSecretEpoch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionTransport {
    NativeTcp,
    ExecutionWebSocket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedSessionContext {
    pub transport: ExecutionTransport,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub platform: ExecutionClientPlatform,
    pub capabilities: Vec<String>,
    pub client_auth_secret_epoch: ClientSecretEpoch,
    pub authenticated_at: i64,
    pub remote_addr: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InboundMessage {
    pub envelope: WireMessage<Value>,
    pub wire_bytes: Vec<u8>,
    pub received_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundAdmission {
    /// The message has been durably admitted to its idempotent handler path.
    Accepted,
    /// The same message identity and payload were durably admitted before.
    Duplicate,
    /// The attributable rejection and its stable reason were durably recorded.
    Rejected { reason: ErrorCode },
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("inbound message admission failed: {message}")]
pub struct InboundAdmissionError {
    message: String,
}

impl InboundAdmissionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub type InboundAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<InboundAdmission, InboundAdmissionError>> + Send + 'a>>;

/// Application boundary used by every Execution Client transport binding.
///
/// Returning any [`InboundAdmission`] is a durability promise made before the
/// transport ACK is emitted. `Accepted` must survive a process crash,
/// `Duplicate` must match the same durable identity and payload, and `Rejected`
/// must preserve a stable, idempotent rejection reason. Queue admission without
/// durable recovery is not sufficient.
pub trait InboundMessagePort: Send + Sync {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        message: InboundMessage,
    ) -> InboundAdmissionFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionResumeRequest {
    pub hello_message_id: MessageId,
    pub cursor: ResumeCursor,
    pub received_at: i64,
}

pub type SessionResumeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), SessionResumeError>> + Send + 'a>>;

/// Durable handoff for reconnect diagnostics and pending-command reconciliation.
///
/// Returning `Ok(())` promises that the cursor, including every pending command
/// id, has reached a crash-recoverable Execution/Reconciliation handler path.
/// The transport never interprets the cursor as permission to replay a command.
pub trait SessionResumePort: Send + Sync {
    fn admit<'a>(
        &'a self,
        session: &'a AuthenticatedSessionContext,
        request: SessionResumeRequest,
    ) -> SessionResumeFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("session resume admission failed: {message}")]
pub struct SessionResumeError {
    message: String,
}

impl SessionResumeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct RejectingSessionResumePort;

impl SessionResumePort for RejectingSessionResumePort {
    fn admit<'a>(
        &'a self,
        _session: &'a AuthenticatedSessionContext,
        _request: SessionResumeRequest,
    ) -> SessionResumeFuture<'a> {
        Box::pin(async {
            Err(SessionResumeError::new(
                "no durable session resume handler is configured",
            ))
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportEventKind {
    AuthenticationFailed,
    HandshakeRejected,
    WireProtocolViolation,
    WireFrameTooLarge,
    DecodeFailed,
    SchemaRejected,
    DirectionRejected,
    SessionIdentityMismatch,
    SequenceViolation,
    InboundAdmissionFailed,
    ClockSkewDetected,
    TimeSyncUnhealthy,
    TimeSyncRestored,
    HeartbeatTimedOut,
    TransportClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportEvent {
    pub transport: ExecutionTransport,
    pub kind: TransportEventKind,
    pub occurred_at: i64,
    pub remote_addr: Option<String>,
    pub session_id: Option<SessionId>,
    pub message_id: Option<MessageId>,
    pub evidence: TransportEventEvidence,
    pub detail: String,
}

/// Redacted wire evidence that is safe to retain with a transport event.
///
/// The typed fields deliberately cannot carry arbitrary credential, HMAC, or
/// payload text. Raw payload bytes are never retained by this boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportEventEvidence {
    pub message_type: Option<ExecutionClientMessageType>,
    pub schema_version: Option<SchemaVersion>,
    pub raw_payload_length: Option<u64>,
}

impl TransportEventEvidence {
    pub fn with_raw_payload_length(raw_payload_length: usize) -> Self {
        Self {
            raw_payload_length: u64::try_from(raw_payload_length).ok(),
            ..Self::default()
        }
    }
}

pub type TransportEventFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), TransportEventError>> + Send + 'a>>;

pub trait TransportEventPort: Send + Sync {
    fn record<'a>(&'a self, event: TransportEvent) -> TransportEventFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("transport event persistence failed: {message}")]
pub struct TransportEventError {
    message: String,
}

impl TransportEventError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct NoopTransportEventPort;

impl TransportEventPort for NoopTransportEventPort {
    fn record<'a>(&'a self, _event: TransportEvent) -> TransportEventFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

pub(crate) fn client_may_send(message_type: ExecutionClientMessageType) -> bool {
    matches!(
        message_type,
        ExecutionClientMessageType::TimeSyncRequest
            | ExecutionClientMessageType::Heartbeat
            | ExecutionClientMessageType::TransportAck
            | ExecutionClientMessageType::MarketTick
            | ExecutionClientMessageType::MarketBar
            | ExecutionClientMessageType::SymbolMetadata
            | ExecutionClientMessageType::AccountSnapshot
            | ExecutionClientMessageType::PositionSnapshot
            | ExecutionClientMessageType::OrderSnapshot
            | ExecutionClientMessageType::CommandReceived
            | ExecutionClientMessageType::ExecutionEvent
            | ExecutionClientMessageType::ReconciliationResult
    )
}

pub(crate) fn validate_authenticated_identity(
    context: &AuthenticatedSessionContext,
    message: &WireMessage<Value>,
) -> Result<(), IdentityValidationError> {
    if message.session_id.as_ref() != Some(&context.session_id) {
        return Err(IdentityValidationError::Mismatch("session_id"));
    }
    if message
        .client_id
        .as_ref()
        .is_some_and(|client_id| client_id != &context.client_id)
    {
        return Err(IdentityValidationError::Mismatch("client_id"));
    }
    validate_payload_value(&message.payload, context)
}

fn validate_payload_value(
    value: &Value,
    context: &AuthenticatedSessionContext,
) -> Result<(), IdentityValidationError> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_payload_value(value, context)?;
            }
        }
        Value::Object(fields) => {
            for (field, value) in fields {
                match field.as_str() {
                    "account_id" => validate_string_identity(
                        "account_id",
                        value,
                        Some(context.account_id.as_str()),
                    )?,
                    "client_id" => validate_string_identity(
                        "client_id",
                        value,
                        Some(context.client_id.as_str()),
                    )?,
                    "terminal_id" => validate_string_identity(
                        "terminal_id",
                        value,
                        context.terminal_id.as_deref(),
                    )?,
                    _ => validate_payload_value(value, context)?,
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_string_identity(
    field: &'static str,
    value: &Value,
    expected: Option<&str>,
) -> Result<(), IdentityValidationError> {
    match value {
        Value::Null if expected.is_none() => Ok(()),
        Value::String(actual) if Some(actual.as_str()) == expected => Ok(()),
        Value::Null => Err(IdentityValidationError::Mismatch(field)),
        Value::String(_) => Err(IdentityValidationError::Mismatch(field)),
        _ => Err(IdentityValidationError::InvalidType(field)),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub(crate) enum IdentityValidationError {
    #[error("authenticated identity differs from {0}")]
    Mismatch(&'static str),
    #[error("identity field has an invalid JSON type: {0}")]
    InvalidType(&'static str),
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sinan_protocol::{ExecutionClientMessageType, WireMessage};

    use super::*;

    fn context() -> AuthenticatedSessionContext {
        AuthenticatedSessionContext {
            transport: ExecutionTransport::NativeTcp,
            session_id: SessionId::from("session_1"),
            client_id: ClientId::from("client_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: Some(TerminalId::from("terminal_1")),
            platform: ExecutionClientPlatform::Mt5,
            capabilities: vec!["MARKET_ORDER".to_owned()],
            client_auth_secret_epoch: ClientSecretEpoch::Active,
            authenticated_at: 1_000,
            remote_addr: None,
        }
    }

    fn message(payload: Value) -> WireMessage<Value> {
        WireMessage {
            message_id: MessageId::from("message_1"),
            message_type: ExecutionClientMessageType::Heartbeat,
            schema_version: "ecp.v1.0".to_owned(),
            client_id: Some(ClientId::from("client_1")),
            session_id: Some(SessionId::from("session_1")),
            correlation_id: None,
            causation_id: None,
            sent_at: Some(1_000),
            sequence: Some(1),
            payload,
        }
    }

    #[test]
    fn recursively_binds_payload_identity() {
        let valid = message(json!({
            "account_id": "account_1",
            "nested": [{"client_id": "client_1", "terminal_id": "terminal_1"}]
        }));
        assert_eq!(validate_authenticated_identity(&context(), &valid), Ok(()));

        let invalid = message(json!({"nested": {"account_id": "account_2"}}));
        assert_eq!(
            validate_authenticated_identity(&context(), &invalid),
            Err(IdentityValidationError::Mismatch("account_id"))
        );
    }

    #[test]
    fn rejects_envelope_session_or_client_drift() {
        let mut invalid = message(json!({}));
        invalid.session_id = Some(SessionId::from("session_2"));
        assert_eq!(
            validate_authenticated_identity(&context(), &invalid),
            Err(IdentityValidationError::Mismatch("session_id"))
        );

        invalid.session_id = Some(SessionId::from("session_1"));
        invalid.client_id = Some(ClientId::from("client_2"));
        assert_eq!(
            validate_authenticated_identity(&context(), &invalid),
            Err(IdentityValidationError::Mismatch("client_id"))
        );
    }

    #[test]
    fn direction_allowlist_excludes_core_only_messages() {
        assert!(client_may_send(ExecutionClientMessageType::ExecutionEvent));
        assert!(!client_may_send(
            ExecutionClientMessageType::ExecutionCommand
        ));
        assert!(!client_may_send(
            ExecutionClientMessageType::SessionAccepted
        ));
    }
}

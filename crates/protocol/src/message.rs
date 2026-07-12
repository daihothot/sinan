use std::{fmt, str::FromStr};

use serde::{de, de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use sinan_types::{CausationId, ClientId, CorrelationId, MessageId, SessionId};
use thiserror::Error;

use crate::{SchemaCompatibility, SchemaVersion, SchemaVersionError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionClientMessageType {
    SessionHello,
    SessionAccepted,
    SessionRejected,
    TimeSyncRequest,
    TimeSyncResponse,
    Heartbeat,
    TransportAck,
    MarketTick,
    MarketBar,
    SymbolMetadata,
    AccountSnapshot,
    PositionSnapshot,
    OrderSnapshot,
    ExecutionCommand,
    CommandReceived,
    ExecutionEvent,
    ReconciliationRequest,
    ReconciliationResult,
    ProtocolError,
}

impl ExecutionClientMessageType {
    pub const ALL: [Self; 19] = [
        Self::SessionHello,
        Self::SessionAccepted,
        Self::SessionRejected,
        Self::TimeSyncRequest,
        Self::TimeSyncResponse,
        Self::Heartbeat,
        Self::TransportAck,
        Self::MarketTick,
        Self::MarketBar,
        Self::SymbolMetadata,
        Self::AccountSnapshot,
        Self::PositionSnapshot,
        Self::OrderSnapshot,
        Self::ExecutionCommand,
        Self::CommandReceived,
        Self::ExecutionEvent,
        Self::ReconciliationRequest,
        Self::ReconciliationResult,
        Self::ProtocolError,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionHello => "session.hello",
            Self::SessionAccepted => "session.accepted",
            Self::SessionRejected => "session.rejected",
            Self::TimeSyncRequest => "time.sync.request",
            Self::TimeSyncResponse => "time.sync.response",
            Self::Heartbeat => "heartbeat",
            Self::TransportAck => "transport.ack",
            Self::MarketTick => "market.tick",
            Self::MarketBar => "market.bar",
            Self::SymbolMetadata => "symbol.metadata",
            Self::AccountSnapshot => "account.snapshot",
            Self::PositionSnapshot => "position.snapshot",
            Self::OrderSnapshot => "order.snapshot",
            Self::ExecutionCommand => "execution.command",
            Self::CommandReceived => "command.received",
            Self::ExecutionEvent => "execution.event",
            Self::ReconciliationRequest => "reconciliation.request",
            Self::ReconciliationResult => "reconciliation.result",
            Self::ProtocolError => "protocol.error",
        }
    }
}

impl fmt::Display for ExecutionClientMessageType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ExecutionClientMessageType {
    type Err = UnknownMessageType;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let message_type = match value {
            "session.hello" => Self::SessionHello,
            "session.accepted" => Self::SessionAccepted,
            "session.rejected" => Self::SessionRejected,
            "time.sync.request" => Self::TimeSyncRequest,
            "time.sync.response" => Self::TimeSyncResponse,
            "heartbeat" => Self::Heartbeat,
            "transport.ack" => Self::TransportAck,
            "market.tick" => Self::MarketTick,
            "market.bar" => Self::MarketBar,
            "symbol.metadata" => Self::SymbolMetadata,
            "account.snapshot" => Self::AccountSnapshot,
            "position.snapshot" => Self::PositionSnapshot,
            "order.snapshot" => Self::OrderSnapshot,
            "execution.command" => Self::ExecutionCommand,
            "command.received" => Self::CommandReceived,
            "execution.event" => Self::ExecutionEvent,
            "reconciliation.request" => Self::ReconciliationRequest,
            "reconciliation.result" => Self::ReconciliationResult,
            "protocol.error" => Self::ProtocolError,
            _ => return Err(UnknownMessageType(value.to_owned())),
        };
        Ok(message_type)
    }
}

impl Serialize for ExecutionClientMessageType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ExecutionClientMessageType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown Execution Client message type: {0}")]
pub struct UnknownMessageType(pub String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireMessage<T> {
    pub message_id: MessageId,

    #[serde(rename = "type")]
    pub message_type: ExecutionClientMessageType,

    pub schema_version: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<CausationId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,

    pub payload: T,
}

pub type ExecutionClientMessage<T> = WireMessage<T>;

impl<T> WireMessage<T> {
    pub fn schema(&self) -> Result<SchemaVersion, SchemaVersionError> {
        self.schema_version.parse()
    }

    pub fn validate(
        &self,
        supported: SchemaVersion,
    ) -> Result<SchemaCompatibility, EnvelopeValidationError> {
        if self.message_id.as_str().trim().is_empty() {
            return Err(EnvelopeValidationError::MissingRequiredField("message_id"));
        }

        validate_optional_identifier("client_id", self.client_id.as_deref())?;
        validate_optional_identifier("session_id", self.session_id.as_deref())?;
        validate_optional_identifier("correlation_id", self.correlation_id.as_deref())?;
        validate_optional_identifier("causation_id", self.causation_id.as_deref())?;

        if requires_session_and_sequence(self.message_type) {
            if self.session_id.is_none() {
                return Err(EnvelopeValidationError::MissingRequiredField("session_id"));
            }
            if self.sequence.is_none() {
                return Err(EnvelopeValidationError::MissingRequiredField("sequence"));
            }
        }

        if requires_sent_at(self.message_type) && self.sent_at.is_none() {
            return Err(EnvelopeValidationError::MissingRequiredField("sent_at"));
        }

        if matches!(self.sequence, Some(0)) {
            return Err(EnvelopeValidationError::InvalidSequence);
        }

        let received = self.schema()?;
        received
            .compatibility_with(supported)
            .map_err(EnvelopeValidationError::from)
    }
}

fn requires_session_and_sequence(message_type: ExecutionClientMessageType) -> bool {
    !matches!(
        message_type,
        ExecutionClientMessageType::SessionHello | ExecutionClientMessageType::SessionRejected
    )
}

fn requires_sent_at(message_type: ExecutionClientMessageType) -> bool {
    !matches!(
        message_type,
        ExecutionClientMessageType::SessionHello | ExecutionClientMessageType::TimeSyncRequest
    )
}

fn validate_optional_identifier(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), EnvelopeValidationError> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        return Err(EnvelopeValidationError::EmptyOptionalField(field));
    }
    Ok(())
}

pub fn decode_wire_message<T: DeserializeOwned>(
    bytes: &[u8],
    supported: SchemaVersion,
) -> Result<WireMessage<T>, WireDecodeError> {
    let probe: MessageTypeProbe = serde_json::from_slice(bytes)?;
    let _: ExecutionClientMessageType = probe.message_type.parse()?;

    let message: WireMessage<T> = serde_json::from_slice(bytes)?;
    message.validate(supported)?;
    Ok(message)
}

#[derive(Deserialize)]
struct MessageTypeProbe {
    #[serde(rename = "type")]
    message_type: String,
}

#[derive(Debug, Error)]
pub enum WireDecodeError {
    #[error("failed to decode WireMessage JSON: {0}")]
    Decode(#[from] serde_json::Error),

    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),

    #[error(transparent)]
    InvalidEnvelope(#[from] EnvelopeValidationError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeValidationError {
    #[error("missing required envelope field: {0}")]
    MissingRequiredField(&'static str),

    #[error("optional envelope field is present but empty: {0}")]
    EmptyOptionalField(&'static str),

    #[error("wire sequence must start at one")]
    InvalidSequence,

    #[error(transparent)]
    Schema(#[from] SchemaVersionError),
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn message(schema_version: &str) -> WireMessage<serde_json::Value> {
        WireMessage {
            message_id: "msg_1".into(),
            message_type: ExecutionClientMessageType::Heartbeat,
            schema_version: schema_version.to_owned(),
            client_id: None,
            session_id: Some("session_1".into()),
            correlation_id: None,
            causation_id: None,
            sent_at: Some(1_779_800_000_123),
            sequence: Some(1),
            payload: json!({"effective_server_now": 1_779_800_000_123_i64}),
        }
    }

    #[test]
    fn wire_message_round_trips_as_json() {
        let expected = message("ecp.v1.0");
        let encoded = serde_json::to_vec(&expected).unwrap();
        let decoded: WireMessage<serde_json::Value> = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn rejects_unknown_message_type() {
        let raw = r#"{"message_id":"msg_1","type":"execution.unknown","schema_version":"ecp.v1.0","payload":{}}"#;
        assert!(serde_json::from_str::<WireMessage<serde_json::Value>>(raw).is_err());
        assert!(matches!(
            decode_wire_message::<serde_json::Value>(
                raw.as_bytes(),
                SchemaVersion::new(1, 0)
            ),
            Err(WireDecodeError::UnknownMessageType(UnknownMessageType(ref value)))
                if value == "execution.unknown"
        ));
        assert!(matches!(
            decode_wire_message::<crate::HelloPayload>(
                raw.as_bytes(),
                SchemaVersion::new(1, 0)
            ),
            Err(WireDecodeError::UnknownMessageType(UnknownMessageType(ref value)))
                if value == "execution.unknown"
        ));
    }

    #[test]
    fn rejects_major_mismatch() {
        let error = message("ecp.v2.0")
            .validate(SchemaVersion::new(1, 0))
            .unwrap_err();
        assert!(matches!(
            error,
            EnvelopeValidationError::Schema(SchemaVersionError::MajorMismatch { .. })
        ));
    }

    #[test]
    fn accepts_higher_minor() {
        assert_eq!(
            message("ecp.v1.7")
                .validate(SchemaVersion::new(1, 0))
                .unwrap(),
            SchemaCompatibility::HigherMinor
        );
    }

    #[test]
    fn higher_minor_ignores_unknown_optional_fields() {
        let raw = r#"{
            "message_id":"msg_1",
            "type":"heartbeat",
            "schema_version":"ecp.v1.7",
            "session_id":"session_1",
            "sent_at":1779800000123,
            "sequence":1,
            "future_optional":{"enabled":true},
            "payload":{}
        }"#;
        let decoded =
            decode_wire_message::<serde_json::Value>(raw.as_bytes(), SchemaVersion::new(1, 0))
                .unwrap();
        assert_eq!(decoded.schema_version, "ecp.v1.7");
    }

    #[test]
    fn validates_required_and_present_optional_identifiers() {
        let mut invalid = message("ecp.v1.0");
        invalid.message_id = " ".into();
        assert_eq!(
            invalid.validate(SchemaVersion::new(1, 0)),
            Err(EnvelopeValidationError::MissingRequiredField("message_id"))
        );

        invalid.message_id = "msg_1".into();
        invalid.session_id = Some("".into());
        assert_eq!(
            invalid.validate(SchemaVersion::new(1, 0)),
            Err(EnvelopeValidationError::EmptyOptionalField("session_id"))
        );
    }

    #[test]
    fn execution_command_requires_session_sequence_and_sent_at() {
        for field in ["session_id", "sequence", "sent_at"] {
            let mut invalid = message("ecp.v1.0");
            invalid.message_type = ExecutionClientMessageType::ExecutionCommand;
            match field {
                "session_id" => invalid.session_id = None,
                "sequence" => invalid.sequence = None,
                "sent_at" => invalid.sent_at = None,
                _ => unreachable!(),
            }

            assert_eq!(
                invalid.validate(SchemaVersion::new(1, 0)),
                Err(EnvelopeValidationError::MissingRequiredField(field))
            );
        }
    }

    #[test]
    fn hello_and_time_sync_request_allow_documented_omissions() {
        let mut hello = message("ecp.v1.0");
        hello.message_type = ExecutionClientMessageType::SessionHello;
        hello.session_id = None;
        hello.sequence = None;
        hello.sent_at = None;
        assert!(hello.validate(SchemaVersion::new(1, 0)).is_ok());

        let mut time_sync = message("ecp.v1.0");
        time_sync.message_type = ExecutionClientMessageType::TimeSyncRequest;
        time_sync.sent_at = None;
        assert!(time_sync.validate(SchemaVersion::new(1, 0)).is_ok());

        let mut rejected = message("ecp.v1.0");
        rejected.message_type = ExecutionClientMessageType::SessionRejected;
        rejected.session_id = None;
        rejected.sequence = None;
        assert!(rejected.validate(SchemaVersion::new(1, 0)).is_ok());
    }

    #[test]
    fn session_accepted_requires_session_sequence_and_sent_at() {
        for field in ["session_id", "sequence", "sent_at"] {
            let mut invalid = message("ecp.v1.0");
            invalid.message_type = ExecutionClientMessageType::SessionAccepted;
            match field {
                "session_id" => invalid.session_id = None,
                "sequence" => invalid.sequence = None,
                "sent_at" => invalid.sent_at = None,
                _ => unreachable!(),
            }

            assert_eq!(
                invalid.validate(SchemaVersion::new(1, 0)),
                Err(EnvelopeValidationError::MissingRequiredField(field))
            );
        }
    }
}

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use sinan_types::{
    AccountId, AccountSnapshot, ClientId, ClockSyncStatus, CommandId, ErrorCode, IdempotencyKey,
    MessageId, OrderSnapshot, PositionSnapshot, RequestId, SessionId, SymbolCode,
    SymbolMetadataSnapshot, TerminalId,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloPayload {
    pub client_id: ClientId,
    pub platform: ExecutionClientPlatform,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,

    pub account_id: AccountId,
    pub token: String,
    pub capabilities: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionClientPlatform {
    Mt5,
    Binance,
    Okx,
    Ibkr,
    Paper,
    Backtest,
    Exchange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResumeCursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_session_id: Option<SessionId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_gateway_message_id: Option<MessageId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_gateway_sequence: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_client_message_id: Option<MessageId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_client_sequence: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_command_ids: Option<Vec<CommandId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloAcceptedPayload {
    pub session_id: SessionId,
    pub server_time: i64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub time_sync_interval_ms: u64,
    pub max_time_sync_rtt_ms: u64,
    pub max_clock_offset_ms: u64,
    pub max_inflight_commands: u64,
    pub max_frame_bytes: u64,
    pub max_message_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRejected {
    pub reason: ErrorCode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    pub server_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSyncRequest {
    pub request_id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSyncResponse {
    pub request_id: RequestId,
    pub server_receive_at: i64,
    pub server_send_at: i64,
    pub server_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub effective_server_now: i64,
    pub clock_sync_status: ClockSyncStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_time_sync_at_server_ms: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_time_sync_rtt_ms: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_time_offset_ms: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_queue_depth: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_inbox_depth: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransportAck {
    pub acked_message_id: MessageId,
    pub acked_message_type: crate::ExecutionClientMessageType,
    pub status: TransportAckStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<ProtocolReason>,

    pub received_at: i64,
}

impl<'de> Deserialize<'de> for TransportAck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = TransportAckFields::deserialize(deserializer)?;
        let ack = Self {
            acked_message_id: fields.acked_message_id,
            acked_message_type: fields.acked_message_type,
            status: fields.status,
            reason: fields.reason,
            received_at: fields.received_at,
        };
        ack.validate().map_err(de::Error::custom)?;
        Ok(ack)
    }
}

#[derive(Deserialize)]
struct TransportAckFields {
    acked_message_id: MessageId,
    acked_message_type: crate::ExecutionClientMessageType,
    status: TransportAckStatus,
    reason: Option<ProtocolReason>,
    received_at: i64,
}

impl TransportAck {
    pub fn validate(&self) -> Result<(), PayloadValidationError> {
        if self.acked_message_id.as_str().trim().is_empty() {
            return Err(PayloadValidationError::MissingRequiredField(
                "acked_message_id",
            ));
        }
        if self.status == TransportAckStatus::Rejected && self.reason.is_none() {
            return Err(PayloadValidationError::MissingRequiredField("reason"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransportAckStatus {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolError {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_message_id: Option<MessageId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_message_type: Option<crate::ExecutionClientMessageType>,

    pub reason: ErrorCode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    pub server_time: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketTick {
    pub account_id: AccountId,
    pub symbol: SymbolCode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_symbol: Option<String>,

    pub bid: f64,
    pub ask: f64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,

    pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandReceived {
    pub command_id: CommandId,
    pub idempotency_key: IdempotencyKey,
    pub account_id: AccountId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,

    pub received_at: i64,
    pub inbox_status: CommandInboxStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<ProtocolReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandInboxStatus {
    Recorded,
    Duplicate,
    Expired,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationRequest {
    pub request_id: RequestId,
    pub account_id: AccountId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,

    pub reason: ReconciliationReason,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_ids: Option<Vec<CommandId>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_server_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationReason {
    DeliveryUnconfirmed,
    ConnectionRestored,
    ManualRequest,
    StateStoreRestored,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationResult {
    pub request_id: RequestId,
    pub account_id: AccountId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<TerminalId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<ClientId>,

    pub observed_at: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<AccountSnapshot>,

    pub positions: Vec<PositionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub symbol_metadata: Vec<SymbolMetadataSnapshot>,
    pub unresolved_command_ids: Vec<CommandId>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolReason {
    Ok,
    Error(ErrorCode),
}

impl Serialize for ProtocolReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Ok => serializer.serialize_str("OK"),
            Self::Error(error) => error.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ProtocolReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        if raw == "OK" {
            return Ok(Self::Ok);
        }

        serde_json::from_value::<ErrorCode>(serde_json::Value::String(raw))
            .map(Self::Error)
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PayloadValidationError {
    #[error("missing required payload field: {0}")]
    MissingRequiredField(&'static str),
}

#[cfg(test)]
mod tests {
    use crate::{decode_wire_message, SchemaVersion, WireDecodeError};

    use super::*;

    #[test]
    fn rejected_transport_ack_requires_reason_during_decode() {
        let rejected_without_reason = br#"{
            "message_id":"msg_ack_1",
            "type":"transport.ack",
            "schema_version":"ecp.v1.0",
            "session_id":"session_1",
            "sent_at":1779800000123,
            "sequence":1,
            "payload":{
                "acked_message_id":"msg_1",
                "acked_message_type":"heartbeat",
                "status":"REJECTED",
                "received_at":1779800000122
            }
        }"#;

        assert!(matches!(
            decode_wire_message::<TransportAck>(rejected_without_reason, SchemaVersion::new(1, 0)),
            Err(WireDecodeError::Decode(_))
        ));
    }

    #[test]
    fn accepted_transport_ack_may_omit_reason() {
        let accepted_without_reason = br#"{
            "message_id":"msg_ack_1",
            "type":"transport.ack",
            "schema_version":"ecp.v1.0",
            "session_id":"session_1",
            "sent_at":1779800000123,
            "sequence":1,
            "payload":{
                "acked_message_id":"msg_1",
                "acked_message_type":"heartbeat",
                "status":"ACCEPTED",
                "received_at":1779800000122
            }
        }"#;

        let message =
            decode_wire_message::<TransportAck>(accepted_without_reason, SchemaVersion::new(1, 0))
                .unwrap();
        assert_eq!(message.payload.status, TransportAckStatus::Accepted);
        assert_eq!(message.payload.reason, None);
    }
}

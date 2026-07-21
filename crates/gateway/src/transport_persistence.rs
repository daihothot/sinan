use std::{future::Future, pin::Pin, sync::Arc};

use serde_json::json;
use sinan_store::{
    CanonicalJson, DeadletterReason, NewDeadletterEvent, NewSystemEvent, SqliteStateStore,
    StoredDeadletterEvent, StoredSystemEvent, SystemEventSeverity,
};
use uuid::Uuid;

use crate::{
    ExecutionTransport, TransportEvent, TransportEventError, TransportEventFuture,
    TransportEventKind, TransportEventPort,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedTransportEvent {
    System(StoredSystemEvent),
    Deadletter(StoredDeadletterEvent),
}

pub type TransportEventPublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), TransportEventPublishError>> + Send + 'a>>;

pub trait TransportEventPublisher: Send + Sync {
    fn publish<'a>(&'a self, event: PersistedTransportEvent) -> TransportEventPublishFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("transport event publish failed: {message}")]
pub struct TransportEventPublishError {
    message: String,
}

impl TransportEventPublishError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct NoopTransportEventPublisher;

impl TransportEventPublisher for NoopTransportEventPublisher {
    fn publish<'a>(&'a self, _event: PersistedTransportEvent) -> TransportEventPublishFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

pub trait TransportPersistenceIdGenerator: Send + Sync {
    fn next_system_event_id(&self) -> String;
    fn next_deadletter_id(&self) -> String;
}

#[derive(Default)]
pub struct UuidTransportPersistenceIdGenerator;

impl TransportPersistenceIdGenerator for UuidTransportPersistenceIdGenerator {
    fn next_system_event_id(&self) -> String {
        format!("system-event-{}", Uuid::new_v4().simple())
    }

    fn next_deadletter_id(&self) -> String {
        format!("deadletter-{}", Uuid::new_v4().simple())
    }
}

pub struct ProductionTransportEventPort {
    store: SqliteStateStore,
    publisher: Arc<dyn TransportEventPublisher>,
    ids: Arc<dyn TransportPersistenceIdGenerator>,
}

impl ProductionTransportEventPort {
    pub fn new(
        store: SqliteStateStore,
        publisher: Arc<dyn TransportEventPublisher>,
        ids: Arc<dyn TransportPersistenceIdGenerator>,
    ) -> Self {
        Self {
            store,
            publisher,
            ids,
        }
    }

    pub fn without_publisher(store: SqliteStateStore) -> Self {
        Self::new(
            store,
            Arc::new(NoopTransportEventPublisher),
            Arc::new(UuidTransportPersistenceIdGenerator),
        )
    }
}

impl TransportEventPort for ProductionTransportEventPort {
    fn record<'a>(&'a self, event: TransportEvent) -> TransportEventFuture<'a> {
        Box::pin(async move {
            let persisted = if let Some(reason) = deadletter_reason(event.kind) {
                let outcome = self
                    .store
                    .append_deadletter_event(NewDeadletterEvent {
                        deadletter_id: self.ids.next_deadletter_id(),
                        message_id: event.message_id.clone(),
                        message_type: event
                            .evidence
                            .message_type
                            .map(|message_type| message_type.to_string()),
                        schema_version: event
                            .evidence
                            .schema_version
                            .map(|schema_version| schema_version.to_string()),
                        reason,
                        source: transport_source(event.transport).to_owned(),
                        // Raw payload content may contain credentials or HMAC material.
                        raw_payload: None,
                        raw_payload_length: event.evidence.raw_payload_length,
                        error_message: event.detail.clone(),
                        received_at: event.occurred_at,
                        created_at: event.occurred_at,
                    })
                    .await
                    .map_err(|error| persistence_error("persist deadletter event", error))?;
                PersistedTransportEvent::Deadletter(outcome.into_record())
            } else {
                let metadata = CanonicalJson::from_value(json!({
                    "transport": transport_name(event.transport),
                    "remote_addr": event.remote_addr,
                    "session_id": event.session_id,
                    "message_id": event.message_id,
                    "transport_event_kind": transport_event_type(event.kind),
                }))
                .map_err(|error| persistence_error("encode system event metadata", error))?;
                let outcome = self
                    .store
                    .append_system_event(NewSystemEvent {
                        system_event_id: self.ids.next_system_event_id(),
                        event_type: transport_event_type(event.kind).to_owned(),
                        severity: system_severity(event.kind),
                        component: "trading-gateway".to_owned(),
                        message: event.detail,
                        metadata: Some(metadata),
                        timestamp: event.occurred_at,
                        created_at: event.occurred_at,
                    })
                    .await
                    .map_err(|error| persistence_error("persist system event", error))?;
                PersistedTransportEvent::System(outcome.into_record())
            };
            self.publisher
                .publish(persisted)
                .await
                .map_err(|error| TransportEventError::new(error.to_string()))
        })
    }
}

fn persistence_error(operation: &str, error: sinan_store::StoreError) -> TransportEventError {
    TransportEventError::new(format!("{operation}: {error}"))
}

fn deadletter_reason(kind: TransportEventKind) -> Option<DeadletterReason> {
    match kind {
        TransportEventKind::WireProtocolViolation => Some(DeadletterReason::WireProtocolViolation),
        TransportEventKind::WireFrameTooLarge => Some(DeadletterReason::WireFrameTooLarge),
        TransportEventKind::DecodeFailed => Some(DeadletterReason::DecodeFailed),
        TransportEventKind::SchemaRejected => Some(DeadletterReason::SchemaValidationFailed),
        _ => None,
    }
}

fn system_severity(kind: TransportEventKind) -> SystemEventSeverity {
    match kind {
        TransportEventKind::AuthenticationFailed
        | TransportEventKind::HandshakeRejected
        | TransportEventKind::DirectionRejected
        | TransportEventKind::SessionIdentityMismatch
        | TransportEventKind::SequenceViolation
        | TransportEventKind::InboundAdmissionFailed
        | TransportEventKind::HeartbeatTimedOut => SystemEventSeverity::Error,
        TransportEventKind::ClockSkewDetected | TransportEventKind::TimeSyncUnhealthy => {
            SystemEventSeverity::Warning
        }
        TransportEventKind::TimeSyncRestored | TransportEventKind::TransportClosed => {
            SystemEventSeverity::Info
        }
        TransportEventKind::WireProtocolViolation
        | TransportEventKind::WireFrameTooLarge
        | TransportEventKind::DecodeFailed
        | TransportEventKind::SchemaRejected => SystemEventSeverity::Error,
    }
}

fn transport_source(transport: ExecutionTransport) -> &'static str {
    match transport {
        ExecutionTransport::NativeTcp => "trading-gateway.native-tcp",
        ExecutionTransport::ExecutionWebSocket => "trading-gateway.execution-websocket",
    }
}

fn transport_name(transport: ExecutionTransport) -> &'static str {
    match transport {
        ExecutionTransport::NativeTcp => "NATIVE_TCP",
        ExecutionTransport::ExecutionWebSocket => "EXECUTION_WEBSOCKET",
    }
}

fn transport_event_type(kind: TransportEventKind) -> &'static str {
    match kind {
        TransportEventKind::AuthenticationFailed => "AUTHENTICATION_FAILED",
        TransportEventKind::HandshakeRejected => "HANDSHAKE_REJECTED",
        TransportEventKind::WireProtocolViolation => "WIRE_PROTOCOL_VIOLATION",
        TransportEventKind::WireFrameTooLarge => "WIRE_FRAME_TOO_LARGE",
        TransportEventKind::DecodeFailed => "DECODE_FAILED",
        TransportEventKind::SchemaRejected => "SCHEMA_REJECTED",
        TransportEventKind::DirectionRejected => "DIRECTION_REJECTED",
        TransportEventKind::SessionIdentityMismatch => "SESSION_IDENTITY_MISMATCH",
        TransportEventKind::SequenceViolation => "SEQUENCE_VIOLATION",
        TransportEventKind::InboundAdmissionFailed => "INBOUND_ADMISSION_FAILED",
        TransportEventKind::ClockSkewDetected => "CLOCK_SKEW_DETECTED",
        TransportEventKind::TimeSyncUnhealthy => "TIME_SYNC_UNHEALTHY",
        TransportEventKind::TimeSyncRestored => "TIME_SYNC_RESTORED",
        TransportEventKind::HeartbeatTimedOut => "HEARTBEAT_TIMED_OUT",
        TransportEventKind::TransportClosed => "TRANSPORT_CLOSED",
    }
}

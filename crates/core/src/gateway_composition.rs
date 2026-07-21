use std::sync::Arc;

use serde_json::json;
use sinan_events::EventStreamManager;
use sinan_gateway::{
    DurableInboundMessagePort, DurableSessionResumePort, PersistedTransportEvent,
    ProductionTransportEventPort, TransportEventPublishError, TransportEventPublishFuture,
    TransportEventPublisher, UuidTransportPersistenceIdGenerator,
};
use sinan_store::{CanonicalJson, NewEventStreamRecord, SqliteStateStore};
use sinan_types::EventStreamTopic;

/// Production Gateway ports that fulfill the ACK-before-durable-admission and
/// transport-event persistence boundaries.
pub struct ProductionGatewayPersistence {
    pub inbound: Arc<DurableInboundMessagePort>,
    pub resume: Arc<DurableSessionResumePort>,
    pub transport_events: Arc<ProductionTransportEventPort>,
}

pub fn compose_production_gateway_persistence(
    store: SqliteStateStore,
    event_stream: Arc<EventStreamManager>,
) -> ProductionGatewayPersistence {
    let publisher: Arc<dyn TransportEventPublisher> =
        Arc::new(EventStreamTransportPublisher::new(event_stream));
    ProductionGatewayPersistence {
        inbound: Arc::new(DurableInboundMessagePort::new(store.clone())),
        resume: Arc::new(DurableSessionResumePort::new(store.clone())),
        transport_events: Arc::new(ProductionTransportEventPort::new(
            store,
            publisher,
            Arc::new(UuidTransportPersistenceIdGenerator),
        )),
    }
}

#[derive(Clone)]
pub struct EventStreamTransportPublisher {
    event_stream: Arc<EventStreamManager>,
}

impl EventStreamTransportPublisher {
    pub const fn new(event_stream: Arc<EventStreamManager>) -> Self {
        Self { event_stream }
    }
}

impl TransportEventPublisher for EventStreamTransportPublisher {
    fn publish<'a>(&'a self, event: PersistedTransportEvent) -> TransportEventPublishFuture<'a> {
        Box::pin(async move {
            let event = public_transport_summary(event)?;
            self.event_stream
                .publish(event)
                .await
                .map_err(|error| TransportEventPublishError::new(error.to_string()))?;
            Ok(())
        })
    }
}

fn public_transport_summary(
    event: PersistedTransportEvent,
) -> Result<NewEventStreamRecord, TransportEventPublishError> {
    match event {
        PersistedTransportEvent::System(event) => Ok(NewEventStreamRecord {
            event_id: event.system_event_id,
            topic: EventStreamTopic::SystemEvent,
            account_id: None,
            event_type: event.event_type,
            // Global summaries are visible to every event subscriber. Detailed
            // message and metadata remain available only in the durable fact.
            payload: CanonicalJson::from_value(json!({
                "severity": event.severity.as_str(),
                "component": event.component,
                "timestamp": event.timestamp,
            }))
            .map_err(|error| publish_error("encode system event summary", error))?,
            created_at: event.created_at,
        }),
        PersistedTransportEvent::Deadletter(event) => Ok(NewEventStreamRecord {
            event_id: event.deadletter_id,
            topic: EventStreamTopic::DeadletterSummary,
            account_id: None,
            event_type: "deadletter.event".to_owned(),
            // Message identity, raw evidence, source and parser detail remain
            // restricted to the durable deadletter fact.
            payload: CanonicalJson::from_value(json!({
                "reason": event.reason.as_str(),
                "received_at": event.received_at,
            }))
            .map_err(|error| publish_error("encode deadletter summary", error))?,
            created_at: event.created_at,
        }),
    }
}

fn publish_error(operation: &str, error: impl std::fmt::Display) -> TransportEventPublishError {
    TransportEventPublishError::new(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sinan_gateway::PersistedTransportEvent;
    use sinan_store::{
        CanonicalJson, DeadletterReason, StoredDeadletterEvent, StoredSystemEvent,
        SystemEventSeverity,
    };
    use sinan_types::MessageId;

    use super::public_transport_summary;

    #[test]
    fn global_system_summary_omits_diagnostic_identity_and_detail() {
        let summary =
            public_transport_summary(PersistedTransportEvent::System(StoredSystemEvent {
                system_event_id: "system-1".to_owned(),
                event_type: "AUTHENTICATION_FAILED".to_owned(),
                severity: SystemEventSeverity::Error,
                component: "trading-gateway".to_owned(),
                message: "secret authentication detail".to_owned(),
                metadata: Some(
                    CanonicalJson::from_value(json!({
                        "remote_addr": "192.0.2.1:5000",
                        "session_id": "session-secret",
                        "message_id": "message-secret"
                    }))
                    .unwrap(),
                ),
                timestamp: 100,
                created_at: 101,
            }))
            .unwrap();

        assert_eq!(
            summary.payload.as_str(),
            r#"{"component":"trading-gateway","severity":"ERROR","timestamp":100}"#
        );
        assert!(!summary.payload.as_str().contains("secret"));
        assert!(!summary.payload.as_str().contains("remote_addr"));
    }

    #[test]
    fn global_deadletter_summary_omits_raw_evidence_and_parser_detail() {
        let summary =
            public_transport_summary(PersistedTransportEvent::Deadletter(StoredDeadletterEvent {
                deadletter_id: "deadletter-1".to_owned(),
                message_id: Some(MessageId::from("message-secret")),
                message_type: Some("execution.event".to_owned()),
                schema_version: Some("ecp.v1.0".to_owned()),
                reason: DeadletterReason::DecodeFailed,
                source: "private-source".to_owned(),
                raw_payload: Some("secret raw payload".to_owned()),
                raw_payload_length: Some(18),
                error_message: "secret parser detail".to_owned(),
                received_at: 100,
                created_at: 101,
            }))
            .unwrap();

        assert_eq!(
            summary.payload.as_str(),
            r#"{"reason":"DECODE_FAILED","received_at":100}"#
        );
        assert!(!summary.payload.as_str().contains("secret"));
        assert!(!summary.payload.as_str().contains("private-source"));
    }
}

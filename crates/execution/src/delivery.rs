use std::{future::Future, pin::Pin};

use sinan_protocol::{ExecutionClientMessage, ReconciliationRequest};
use sinan_types::{
    AccountId, ClientId, CommandId, ExecutionCommand, MessageId, SessionId, TerminalId,
};
use thiserror::Error;

/// A transport-independent request whose session fields are bound by Gateway.
///
/// `message.session_id`, `message.sequence`, and `message.sent_at` must be
/// absent when the request crosses this port. The Gateway implementation owns
/// active-session selection and fills those fields immediately before durable
/// outbox preparation.
#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryRequest<T> {
    pub account_id: AccountId,
    pub client_id: Option<ClientId>,
    pub terminal_id: Option<TerminalId>,
    pub command_id: Option<CommandId>,
    pub message: ExecutionClientMessage<T>,
    pub expires_at: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryReceipt {
    pub attempt_id: String,
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub sent_at: i64,
    pub confirmation_deadline_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryRejectionReason {
    NoActiveSession,
    AmbiguousRoute { candidate_count: usize },
    ClockUnhealthy,
    Expired,
    IdentityMismatch { field: &'static str },
    Backpressure { queue_depth: usize },
    InflightLimit { limit: u64 },
    TransportRejected { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryRejection {
    pub attempt_id: String,
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub rejected_at: i64,
    pub reason: DeliveryRejectionReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryFailure {
    pub attempt_id: String,
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub failed_at: i64,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryUncertainty {
    pub attempt_id: String,
    pub message_id: MessageId,
    pub session_id: Option<SessionId>,
    pub sequence: Option<u64>,
    pub observed_at: i64,
    pub error: String,
}

/// Delivery evidence returned to the Execution application layer.
///
/// Gateway persists transport state and returns one of these values. It must
/// never apply an [`crate::ExecutionCommandState`] transition itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    Sent(DeliveryReceipt),
    Rejected(DeliveryRejection),
    DefinitelyNotWritten(DeliveryFailure),
    Unconfirmed(DeliveryUncertainty),
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("outbound delivery infrastructure failed: {message}")]
pub struct DeliveryInfrastructureError {
    pub message: String,
}

impl DeliveryInfrastructureError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub type DeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DeliveryOutcome, DeliveryInfrastructureError>> + Send + 'a>>;

/// Execution-owned port implemented by a Gateway outbound adapter.
///
/// The boxed future keeps the trait object-safe without adding an async-trait
/// dependency. Delivery policy and command lifecycle transitions remain with
/// the calling Execution application service.
pub trait OutboundDeliveryPort: Send + Sync {
    fn deliver_execution_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> DeliveryFuture<'_>;

    fn deliver_reconciliation_request(
        &self,
        request: DeliveryRequest<ReconciliationRequest>,
    ) -> DeliveryFuture<'_>;
}

pub trait ServerClock: Send + Sync {
    fn now_ms(&self) -> i64;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ObjectSafePort;

    impl OutboundDeliveryPort for ObjectSafePort {
        fn deliver_execution_command(
            &self,
            _request: DeliveryRequest<ExecutionCommand>,
        ) -> DeliveryFuture<'_> {
            Box::pin(async { Err(DeliveryInfrastructureError::new("not configured")) })
        }

        fn deliver_reconciliation_request(
            &self,
            _request: DeliveryRequest<ReconciliationRequest>,
        ) -> DeliveryFuture<'_> {
            Box::pin(async { Err(DeliveryInfrastructureError::new("not configured")) })
        }
    }

    fn accepts_trait_object(_port: &dyn OutboundDeliveryPort) {}

    #[test]
    fn outbound_delivery_port_is_object_safe() {
        accepts_trait_object(&ObjectSafePort);
    }

    #[test]
    fn delivery_outcome_keeps_transport_uncertainty_distinct_from_failure() {
        let message_id = MessageId::from("message-1");
        let session_id = SessionId::from("session-1");
        let failure = DeliveryOutcome::DefinitelyNotWritten(DeliveryFailure {
            attempt_id: "attempt:message-1".to_owned(),
            message_id: message_id.clone(),
            session_id: Some(session_id.clone()),
            failed_at: 10,
            error: "write rejected before any bytes were accepted".to_owned(),
        });
        let uncertain = DeliveryOutcome::Unconfirmed(DeliveryUncertainty {
            attempt_id: "attempt:message-1".to_owned(),
            message_id,
            session_id: Some(session_id),
            sequence: Some(2),
            observed_at: 10,
            error: "connection closed after write started".to_owned(),
        });

        assert!(matches!(failure, DeliveryOutcome::DefinitelyNotWritten(_)));
        assert!(matches!(uncertain, DeliveryOutcome::Unconfirmed(_)));
    }
}

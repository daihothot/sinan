use std::fmt::Display;

use serde::{de::DeserializeOwned, Serialize};
use sinan_execution::{
    DeliveryFailure, DeliveryFuture, DeliveryInfrastructureError, DeliveryOutcome, DeliveryReceipt,
    DeliveryRejection, DeliveryRejectionReason, DeliveryRequest, DeliveryUncertainty,
    OutboundDeliveryPort,
};
use sinan_protocol::{ReconciliationRequest, WireMessage, SUPPORTED_SCHEMA_VERSION};
use sinan_store::{
    CanonicalJson, ClaimWireOutbox, CompleteTransportWrite, DeliverySubject, NewDeliveryAttempt,
    NewReservedDelivery, OutboxClaimOutcome, ReserveOutboundSequence, SequenceReservation,
    SessionRouteQuery, SessionRouteResolution, StoreError, StoredDeliveryAttempt,
    StoredOutboundDelivery, WriteTransaction, DELIVERY_ERROR_CLOCK_UNHEALTHY,
    DELIVERY_ERROR_COMMAND_EXPIRED, DELIVERY_ERROR_SESSION_UNAVAILABLE,
    TRANSPORT_ACK_REJECTED_PREFIX,
};
use sinan_types::{
    CommandDeliveryAttemptStatus, ExecutionCommand, MessageId, SessionId, WireOutboxStatus,
};

use crate::{
    validation::{validate_command_request, validate_reconciliation_request},
    GatewaySessionRegistry, OutboundFrame, SinkWriteOutcome,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayOutboundConfig {
    pub confirmation_timeout_ms: u64,
}

impl GatewayOutboundConfig {
    pub fn validate(self) -> Result<Self, DeliveryInfrastructureError> {
        if self.confirmation_timeout_ms == 0 {
            return Err(DeliveryInfrastructureError::new(
                "gateway timeout configuration must be greater than zero",
            ));
        }
        if self.confirmation_timeout_ms > i64::MAX as u64 {
            return Err(DeliveryInfrastructureError::new(
                "gateway timeout configuration exceeds the server-time domain",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct GatewayOutboundAdapter {
    sessions: GatewaySessionRegistry,
    config: GatewayOutboundConfig,
}

impl GatewayOutboundAdapter {
    pub fn new(
        sessions: GatewaySessionRegistry,
        config: GatewayOutboundConfig,
    ) -> Result<Self, DeliveryInfrastructureError> {
        Ok(Self {
            sessions,
            config: config.validate()?,
        })
    }

    async fn deliver_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let now = self.server_now()?;
        let subject = DeliverySubject::ExecutionCommand(request.message.payload.command_id.clone());
        let validation = validate_command_request(&request, now);
        self.prepare_and_write(request, subject, true, now, validation)
            .await
    }

    async fn deliver_reconciliation(
        &self,
        request: DeliveryRequest<ReconciliationRequest>,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let now = self.server_now()?;
        let subject =
            DeliverySubject::ReconciliationRequest(request.message.payload.request_id.clone());
        let validation = validate_reconciliation_request(&request);
        self.prepare_and_write(request, subject, false, now, validation)
            .await
    }

    async fn prepare_and_write<T>(
        &self,
        mut request: DeliveryRequest<T>,
        subject: DeliverySubject,
        require_synced_clock: bool,
        now: i64,
        validation: Result<(), DeliveryRejectionReason>,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError>
    where
        T: DeserializeOwned + PartialEq + Serialize,
    {
        let message_id = request.message.message_id.clone();
        let attempt_id = attempt_id(&message_id);
        let rejected_request_payload = rejected_request_payload(&request)?;
        let heartbeat_timeout = i64::try_from(self.sessions.config().max_time_sync_age_ms)
            .map_err(|error| infrastructure(error.to_string()))?;
        let fresh_after = now.saturating_sub(heartbeat_timeout);
        let mut transaction = self
            .sessions
            .store()
            .begin_write()
            .await
            .map_err(infrastructure)?;
        if let Some(existing) = transaction
            .get_outbound_delivery(&message_id)
            .await
            .map_err(infrastructure)?
        {
            validate_existing_delivery(&request, &subject, &existing)?;
            if matches!(
                &validation,
                Err(DeliveryRejectionReason::IdentityMismatch { .. })
            ) {
                transaction.rollback().await.map_err(infrastructure)?;
                return Err(infrastructure(
                    "replayed delivery request contains pre-bound or drifting identity",
                ));
            }
            transaction.rollback().await.map_err(infrastructure)?;
            return self.resume_existing(existing, now).await;
        }
        if let Some(existing) = transaction
            .get_delivery_attempt(&attempt_id)
            .await
            .map_err(infrastructure)?
        {
            let outcome = replay_rejection(
                existing,
                &subject,
                message_id,
                &rejected_request_payload,
                validation.as_ref(),
            )?;
            transaction.rollback().await.map_err(infrastructure)?;
            return Ok(outcome);
        }
        if let Err(reason) = validation {
            return self
                .record_rejection(
                    transaction,
                    subject,
                    message_id,
                    None,
                    rejected_request_payload,
                    now,
                    reason,
                )
                .await;
        }
        let resolution = transaction
            .resolve_session_route(SessionRouteQuery {
                account_id: request.account_id.clone(),
                client_id: request.client_id.clone(),
                terminal_id: request.terminal_id.clone(),
                fresh_after,
                require_synced_clock,
            })
            .await
            .map_err(infrastructure)?;
        let session = match resolution {
            SessionRouteResolution::Ready(session) => session,
            SessionRouteResolution::NoActiveSession => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        None,
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::NoActiveSession,
                    )
                    .await;
            }
            SessionRouteResolution::Stale { .. } => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        None,
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::NoActiveSession,
                    )
                    .await;
            }
            SessionRouteResolution::ClockUnhealthy { .. } => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        None,
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::ClockUnhealthy,
                    )
                    .await;
            }
            SessionRouteResolution::Ambiguous { candidate_count } => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        None,
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::AmbiguousRoute { candidate_count },
                    )
                    .await;
            }
        };

        let reservation = transaction
            .reserve_outbound_sequence(ReserveOutboundSequence {
                session_id: session.session_id.clone(),
                expected_revision: session.revision,
                subject: subject.clone(),
                fresh_after,
                reserved_at: now,
            })
            .await
            .map_err(infrastructure)?;
        let reservation = match reservation {
            SequenceReservation::Reserved(reservation) => reservation,
            SequenceReservation::SessionUnavailable => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        Some(session.session_id),
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::NoActiveSession,
                    )
                    .await;
            }
            SequenceReservation::ClockUnhealthy => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        Some(session.session_id),
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::ClockUnhealthy,
                    )
                    .await;
            }
            SequenceReservation::Expired => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        Some(session.session_id),
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::Expired,
                    )
                    .await;
            }
            SequenceReservation::IdentityMismatch { field } => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        Some(session.session_id),
                        rejected_request_payload.clone(),
                        now,
                        DeliveryRejectionReason::IdentityMismatch { field },
                    )
                    .await;
            }
            SequenceReservation::InflightLimit { limit, .. } => {
                return self
                    .record_rejection(
                        transaction,
                        subject,
                        message_id,
                        Some(session.session_id),
                        rejected_request_payload,
                        now,
                        DeliveryRejectionReason::InflightLimit { limit },
                    )
                    .await;
            }
        };

        bind_wire_message(&mut request.message, &reservation, now)?;
        request
            .message
            .validate(SUPPORTED_SCHEMA_VERSION)
            .map_err(|error| infrastructure(error.to_string()))?;
        let envelope = CanonicalJson::from_serializable(&request.message)
            .map_err(|error| infrastructure(error.to_string()))?;
        let prepared = transaction
            .enqueue_reserved_delivery(NewReservedDelivery {
                reservation,
                attempt_id,
                message_id: message_id.clone(),
                message_type: request.message.message_type.to_string(),
                envelope,
                created_at: now,
            })
            .await
            .map_err(infrastructure)?;
        transaction.commit().await.map_err(infrastructure)?;

        self.write_prepared(prepared).await
    }

    async fn write_prepared(
        &self,
        prepared: StoredOutboundDelivery,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let claim_at = next_projection_time(
            self.server_now()?,
            prepared.outbox.updated_at.max(prepared.attempt.updated_at),
        );
        let heartbeat_timeout = i64::try_from(self.sessions.config().max_time_sync_age_ms)
            .map_err(|error| infrastructure(error.to_string()))?;
        let claim = self
            .sessions
            .store()
            .claim_outbox(ClaimWireOutbox {
                message_id: prepared.outbox.message_id.clone(),
                expected_outbox_revision: prepared.outbox.revision,
                expected_attempt_revision: prepared.attempt.revision,
                fresh_after: claim_at.saturating_sub(heartbeat_timeout),
                require_synced_clock: matches!(
                    prepared.attempt.subject,
                    DeliverySubject::ExecutionCommand(_)
                ),
                claimed_at: claim_at,
            })
            .await;
        let claimed = match claim {
            Ok(OutboxClaimOutcome::Claimed(claimed)) => claimed,
            Ok(OutboxClaimOutcome::Expired(rejected))
            | Ok(OutboxClaimOutcome::SessionUnavailable(rejected))
            | Ok(OutboxClaimOutcome::ClockUnhealthy(rejected)) => {
                self.skip_reserved_sequence(&rejected);
                return durable_delivery_outcome(
                    rejected,
                    claim_at,
                    self.config.confirmation_timeout_ms,
                );
            }
            Err(error) if is_stale_outbound(&error) => {
                return self
                    .reload_existing(&prepared.outbox.message_id, claim_at)
                    .await;
            }
            Err(error) => return Err(infrastructure(error)),
        };
        let session_id = claimed
            .outbox
            .session_id
            .clone()
            .ok_or_else(|| infrastructure("prepared delivery has no session_id"))?;
        let sequence = claimed
            .outbox
            .sequence
            .ok_or_else(|| infrastructure("prepared delivery has no sequence"))?;
        let frame = OutboundFrame {
            session_id: session_id.clone(),
            message_id: claimed.outbox.message_id.clone(),
            sequence,
            wire_bytes: claimed.outbox.payload.as_str().as_bytes().to_vec(),
        };
        let outcome = match self.sessions.live_sessions().handle(&session_id) {
            Some(handle) => handle.write(frame).await,
            None => SinkWriteOutcome::DefinitelyNotWritten {
                error: "active session has no live transport sink".to_owned(),
            },
        };
        let completed_at = next_projection_time(
            self.server_now()?,
            claimed.outbox.updated_at.max(claimed.attempt.updated_at),
        );
        let complete = |error| CompleteTransportWrite {
            message_id: claimed.outbox.message_id.clone(),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at,
            error,
        };

        match outcome {
            SinkWriteOutcome::Written => {
                let completion = self
                    .sessions
                    .store()
                    .finish_transport_write_sent(complete(None))
                    .await;
                self.outcome_after_completion(&claimed.outbox.message_id, completion, completed_at)
                    .await
            }
            SinkWriteOutcome::Backpressure { queue_depth } => {
                let error = format!("transport write backpressure at queue depth {queue_depth}");
                let completion = self
                    .sessions
                    .store()
                    .finish_transport_write_backpressure(complete(Some(error)))
                    .await;
                self.outcome_after_completion(&claimed.outbox.message_id, completion, completed_at)
                    .await
            }
            SinkWriteOutcome::DefinitelyNotWritten { error } => {
                let completion = self
                    .sessions
                    .store()
                    .finish_transport_write_failed(complete(Some(error)))
                    .await;
                self.outcome_after_completion(&claimed.outbox.message_id, completion, completed_at)
                    .await
            }
            SinkWriteOutcome::Unconfirmed { error } => {
                let completion = self
                    .sessions
                    .store()
                    .finish_transport_write_unconfirmed(complete(Some(error)))
                    .await;
                self.outcome_after_completion(&claimed.outbox.message_id, completion, completed_at)
                    .await
            }
        }
    }

    fn skip_reserved_sequence(&self, delivery: &StoredOutboundDelivery) {
        let (Some(session_id), Some(sequence)) = (
            delivery.outbox.session_id.as_ref(),
            delivery.outbox.sequence,
        ) else {
            return;
        };
        if let Some(handle) = self.sessions.live_sessions().handle(session_id) {
            handle.skip(sequence);
        }
    }

    async fn outcome_after_completion(
        &self,
        message_id: &MessageId,
        completion: Result<StoredOutboundDelivery, StoreError>,
        observed_at: i64,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        match completion {
            Ok(completed) => durable_delivery_outcome(
                completed,
                observed_at,
                self.config.confirmation_timeout_ms,
            ),
            Err(error) if is_stale_outbound(&error) => {
                self.reload_existing(message_id, observed_at).await
            }
            Err(error) => Err(infrastructure(error)),
        }
    }

    async fn reload_existing(
        &self,
        message_id: &MessageId,
        observed_at: i64,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let existing = self
            .sessions
            .store()
            .get_outbound_delivery(message_id)
            .await
            .map_err(infrastructure)?
            .ok_or_else(|| infrastructure("raced outbound delivery disappeared"))?;
        durable_delivery_outcome(existing, observed_at, self.config.confirmation_timeout_ms)
    }

    async fn resume_existing(
        &self,
        existing: StoredOutboundDelivery,
        observed_at: i64,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        if existing.outbox.status == WireOutboxStatus::Pending
            && existing.attempt.status == CommandDeliveryAttemptStatus::Pending
        {
            return self.write_prepared(existing).await;
        }
        durable_delivery_outcome(existing, observed_at, self.config.confirmation_timeout_ms)
    }

    async fn record_rejection(
        &self,
        mut transaction: WriteTransaction,
        subject: DeliverySubject,
        message_id: MessageId,
        session_id: Option<SessionId>,
        request_payload: CanonicalJson,
        rejected_at: i64,
        reason: DeliveryRejectionReason,
    ) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
        let attempt_id = attempt_id(&message_id);
        let stored = transaction
            .record_delivery_attempt(NewDeliveryAttempt {
                attempt_id: attempt_id.clone(),
                subject,
                session_id: session_id.clone(),
                message_id: None,
                request_payload: Some(request_payload),
                status: rejection_attempt_status(&reason),
                attempted_at: rejected_at,
                acked_at: None,
                error: Some(rejection_label(&reason)),
                updated_at: rejected_at,
            })
            .await
            .map_err(infrastructure)?
            .into_record();
        transaction.commit().await.map_err(infrastructure)?;
        Ok(DeliveryOutcome::Rejected(DeliveryRejection {
            attempt_id: stored.attempt_id,
            message_id,
            session_id,
            rejected_at: stored.attempted_at,
            reason,
        }))
    }

    fn server_now(&self) -> Result<i64, DeliveryInfrastructureError> {
        self.sessions.server_now().map_err(infrastructure)
    }
}

impl OutboundDeliveryPort for GatewayOutboundAdapter {
    fn deliver_execution_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> DeliveryFuture<'_> {
        Box::pin(async move { self.deliver_command(request).await })
    }

    fn deliver_reconciliation_request(
        &self,
        request: DeliveryRequest<ReconciliationRequest>,
    ) -> DeliveryFuture<'_> {
        Box::pin(async move { self.deliver_reconciliation(request).await })
    }
}

fn bind_wire_message<T>(
    message: &mut WireMessage<T>,
    reservation: &sinan_store::OutboundReservation,
    sent_at: i64,
) -> Result<(), DeliveryInfrastructureError> {
    if reservation.account_id.as_str().trim().is_empty()
        || reservation.client_id.as_str().trim().is_empty()
        || reservation.sequence < 2
    {
        return Err(infrastructure("invalid outbound reservation identity"));
    }
    message.client_id = Some(reservation.client_id.clone());
    message.session_id = Some(reservation.session_id.clone());
    message.sequence = Some(reservation.sequence);
    message.sent_at = Some(sent_at);
    Ok(())
}

fn rejection_attempt_status(reason: &DeliveryRejectionReason) -> CommandDeliveryAttemptStatus {
    match reason {
        DeliveryRejectionReason::Backpressure { .. }
        | DeliveryRejectionReason::InflightLimit { .. } => {
            CommandDeliveryAttemptStatus::Backpressure
        }
        DeliveryRejectionReason::Expired => CommandDeliveryAttemptStatus::Cancelled,
        DeliveryRejectionReason::IdentityMismatch { .. }
        | DeliveryRejectionReason::TransportRejected { .. } => CommandDeliveryAttemptStatus::Failed,
        DeliveryRejectionReason::NoActiveSession
        | DeliveryRejectionReason::AmbiguousRoute { .. }
        | DeliveryRejectionReason::ClockUnhealthy => CommandDeliveryAttemptStatus::NoActiveSession,
    }
}

fn rejection_label(reason: &DeliveryRejectionReason) -> String {
    match reason {
        DeliveryRejectionReason::NoActiveSession => "NO_ACTIVE_SESSION".to_owned(),
        DeliveryRejectionReason::AmbiguousRoute { candidate_count } => {
            format!("AMBIGUOUS_ROUTE:{candidate_count}")
        }
        DeliveryRejectionReason::ClockUnhealthy => "TIME_SYNC_UNHEALTHY".to_owned(),
        DeliveryRejectionReason::Expired => "COMMAND_EXPIRED".to_owned(),
        DeliveryRejectionReason::IdentityMismatch { field } => {
            format!("SESSION_IDENTITY_MISMATCH:{field}")
        }
        DeliveryRejectionReason::Backpressure { queue_depth } => {
            format!("COMMAND_DISPATCH_BACKPRESSURE:{queue_depth}")
        }
        DeliveryRejectionReason::InflightLimit { limit } => {
            format!("COMMAND_INFLIGHT_LIMIT_REACHED:{limit}")
        }
        DeliveryRejectionReason::TransportRejected { reason } => {
            format!("{TRANSPORT_ACK_REJECTED_PREFIX}{reason}")
        }
    }
}

fn attempt_id(message_id: &MessageId) -> String {
    format!("attempt:{}", message_id.as_str())
}

#[derive(Serialize)]
struct RejectedRequestPayload<'a, T> {
    account_id: &'a sinan_types::AccountId,
    client_id: &'a Option<sinan_types::ClientId>,
    terminal_id: &'a Option<sinan_types::TerminalId>,
    command_id: &'a Option<sinan_types::CommandId>,
    message: &'a WireMessage<T>,
    expires_at: &'a Option<i64>,
}

fn rejected_request_payload<T: Serialize>(
    request: &DeliveryRequest<T>,
) -> Result<CanonicalJson, DeliveryInfrastructureError> {
    CanonicalJson::from_serializable(&RejectedRequestPayload {
        account_id: &request.account_id,
        client_id: &request.client_id,
        terminal_id: &request.terminal_id,
        command_id: &request.command_id,
        message: &request.message,
        expires_at: &request.expires_at,
    })
    .map_err(infrastructure)
}

fn replay_rejection(
    existing: StoredDeliveryAttempt,
    subject: &DeliverySubject,
    message_id: MessageId,
    request_payload: &CanonicalJson,
    validation: Result<&(), &DeliveryRejectionReason>,
) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
    if &existing.subject != subject
        || existing.attempt_id != attempt_id(&message_id)
        || existing.message_id.is_some()
        || existing.request_payload.as_ref() != Some(request_payload)
    {
        return Err(infrastructure(
            "message_id replay conflicts with its durable rejection subject or payload",
        ));
    }
    let reason = rejection_reason_from_attempt(&existing)?;
    if let Err(current) = validation {
        if matches!(current, DeliveryRejectionReason::IdentityMismatch { .. }) && current != &reason
        {
            return Err(infrastructure(
                "replayed rejected request contains drifting identity",
            ));
        }
    }
    Ok(DeliveryOutcome::Rejected(DeliveryRejection {
        attempt_id: existing.attempt_id,
        message_id,
        session_id: existing.session_id,
        rejected_at: existing.attempted_at,
        reason,
    }))
}

fn rejection_reason_from_attempt(
    attempt: &StoredDeliveryAttempt,
) -> Result<DeliveryRejectionReason, DeliveryInfrastructureError> {
    let label = attempt.error.as_deref().unwrap_or_default();
    match attempt.status {
        CommandDeliveryAttemptStatus::NoActiveSession => {
            if label == "NO_ACTIVE_SESSION" || label == DELIVERY_ERROR_SESSION_UNAVAILABLE {
                Ok(DeliveryRejectionReason::NoActiveSession)
            } else if label == "TIME_SYNC_UNHEALTHY" || label == DELIVERY_ERROR_CLOCK_UNHEALTHY {
                Ok(DeliveryRejectionReason::ClockUnhealthy)
            } else if let Some(candidate_count) = label
                .strip_prefix("AMBIGUOUS_ROUTE:")
                .and_then(|value| value.parse().ok())
            {
                Ok(DeliveryRejectionReason::AmbiguousRoute { candidate_count })
            } else {
                Err(infrastructure("unknown durable route rejection reason"))
            }
        }
        CommandDeliveryAttemptStatus::Cancelled if label == DELIVERY_ERROR_COMMAND_EXPIRED => {
            Ok(DeliveryRejectionReason::Expired)
        }
        CommandDeliveryAttemptStatus::Failed => {
            let field = label
                .strip_prefix("SESSION_IDENTITY_MISMATCH:")
                .and_then(identity_field)
                .ok_or_else(|| infrastructure("unknown durable identity rejection reason"))?;
            Ok(DeliveryRejectionReason::IdentityMismatch { field })
        }
        CommandDeliveryAttemptStatus::Backpressure => {
            let limit = label
                .strip_prefix("COMMAND_INFLIGHT_LIMIT_REACHED:")
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| infrastructure("unknown durable inflight rejection reason"))?;
            Ok(DeliveryRejectionReason::InflightLimit { limit })
        }
        _ => Err(infrastructure(
            "durable attempt is not an unbound delivery rejection",
        )),
    }
}

fn identity_field(field: &str) -> Option<&'static str> {
    match field {
        "message_id" => Some("message_id"),
        "message_type" => Some("message_type"),
        "session_id" => Some("session_id"),
        "sequence" => Some("sequence"),
        "sent_at" => Some("sent_at"),
        "envelope.client_id" => Some("envelope.client_id"),
        "correlation_id" => Some("correlation_id"),
        "causation_id" => Some("causation_id"),
        "schema_version" => Some("schema_version"),
        "account_id" => Some("account_id"),
        "client_id" => Some("client_id"),
        "terminal_id" => Some("terminal_id"),
        "command_id" => Some("command_id"),
        "expires_at" => Some("expires_at"),
        _ => None,
    }
}

fn durable_delivery_outcome(
    existing: StoredOutboundDelivery,
    observed_at: i64,
    confirmation_timeout_ms: u64,
) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
    match (existing.outbox.status, existing.attempt.status) {
        (
            WireOutboxStatus::WriteStarted
            | WireOutboxStatus::Sent
            | WireOutboxStatus::Acked
            | WireOutboxStatus::Failed,
            CommandDeliveryAttemptStatus::Acked,
        ) => sent_outcome(existing, confirmation_timeout_ms),
        (
            WireOutboxStatus::Failed,
            CommandDeliveryAttemptStatus::Pending
            | CommandDeliveryAttemptStatus::Sent
            | CommandDeliveryAttemptStatus::Failed
            | CommandDeliveryAttemptStatus::Unconfirmed,
        ) if transport_rejection_reason(&existing).is_some() => {
            let reason = transport_rejection_reason(&existing)
                .expect("match guard established transport rejection");
            Ok(DeliveryOutcome::Rejected(DeliveryRejection {
                attempt_id: existing.attempt.attempt_id,
                message_id: existing.outbox.message_id,
                session_id: existing.outbox.session_id,
                rejected_at: existing.outbox.updated_at.max(existing.attempt.updated_at),
                reason: DeliveryRejectionReason::TransportRejected { reason },
            }))
        }
        (WireOutboxStatus::WriteStarted, CommandDeliveryAttemptStatus::Pending)
        | (
            WireOutboxStatus::WriteStarted
            | WireOutboxStatus::Sent
            | WireOutboxStatus::Acked
            | WireOutboxStatus::Failed,
            CommandDeliveryAttemptStatus::Unconfirmed,
        ) => Ok(unconfirmed_outcome(existing, observed_at)),
        (WireOutboxStatus::Sent, CommandDeliveryAttemptStatus::Sent)
        | (
            WireOutboxStatus::Acked,
            CommandDeliveryAttemptStatus::Pending
            | CommandDeliveryAttemptStatus::Sent
            | CommandDeliveryAttemptStatus::Backpressure
            | CommandDeliveryAttemptStatus::Failed,
        ) => sent_outcome(existing, confirmation_timeout_ms),
        (WireOutboxStatus::Failed, CommandDeliveryAttemptStatus::Backpressure) => {
            let queue_depth = existing
                .attempt
                .error
                .as_deref()
                .and_then(parse_queue_depth)
                .unwrap_or(0);
            Ok(DeliveryOutcome::Rejected(DeliveryRejection {
                attempt_id: existing.attempt.attempt_id,
                message_id: existing.outbox.message_id,
                session_id: existing.outbox.session_id,
                rejected_at: existing.attempt.updated_at,
                reason: DeliveryRejectionReason::Backpressure { queue_depth },
            }))
        }
        (WireOutboxStatus::Failed, CommandDeliveryAttemptStatus::Failed) => {
            Ok(DeliveryOutcome::DefinitelyNotWritten(DeliveryFailure {
                attempt_id: existing.attempt.attempt_id,
                message_id: existing.outbox.message_id,
                session_id: existing.outbox.session_id,
                failed_at: existing.attempt.updated_at,
                error: existing
                    .attempt
                    .error
                    .or(existing.outbox.last_error)
                    .unwrap_or_else(|| "TRANSPORT_WRITE_FAILED".to_owned()),
            }))
        }
        (WireOutboxStatus::Cancelled, CommandDeliveryAttemptStatus::Cancelled) => {
            Ok(cancelled_outcome(existing))
        }
        (WireOutboxStatus::Cancelled, CommandDeliveryAttemptStatus::NoActiveSession) => {
            Ok(unavailable_outcome(existing))
        }
        _ => Err(infrastructure(format!(
            "inconsistent replay state outbox={} attempt={}",
            existing.outbox.status, existing.attempt.status
        ))),
    }
}

fn sent_outcome(
    existing: StoredOutboundDelivery,
    confirmation_timeout_ms: u64,
) -> Result<DeliveryOutcome, DeliveryInfrastructureError> {
    let session_id = existing
        .outbox
        .session_id
        .ok_or_else(|| infrastructure("sent replay has no session_id"))?;
    let sequence = existing
        .outbox
        .sequence
        .ok_or_else(|| infrastructure("sent replay has no sequence"))?;
    // A command receipt may win before the sink completion callback. The
    // WRITE_STARTED projection time is then the durable lower bound.
    let sent_at = existing
        .outbox
        .sent_at
        .unwrap_or(existing.outbox.updated_at);
    let confirmation_timeout_ms = i64::try_from(confirmation_timeout_ms)
        .map_err(|error| infrastructure(error.to_string()))?;
    let confirmation_deadline_at = sent_at
        .checked_add(confirmation_timeout_ms)
        .ok_or_else(|| infrastructure("delivery confirmation deadline overflow"))?;
    Ok(DeliveryOutcome::Sent(DeliveryReceipt {
        attempt_id: existing.attempt.attempt_id,
        message_id: existing.outbox.message_id,
        session_id,
        sequence,
        sent_at,
        confirmation_deadline_at,
    }))
}

fn unconfirmed_outcome(existing: StoredOutboundDelivery, observed_at: i64) -> DeliveryOutcome {
    DeliveryOutcome::Unconfirmed(DeliveryUncertainty {
        attempt_id: existing.attempt.attempt_id,
        message_id: existing.outbox.message_id,
        session_id: existing.outbox.session_id,
        sequence: existing.outbox.sequence,
        write_started_at: existing.outbox.created_at,
        observed_at,
        error: existing
            .attempt
            .error
            .or(existing.outbox.last_error)
            .unwrap_or_else(|| "WRITE_STARTED_RECOVERY_REQUIRED".to_owned()),
    })
}

fn cancelled_outcome(existing: StoredOutboundDelivery) -> DeliveryOutcome {
    let error = existing
        .attempt
        .error
        .clone()
        .or_else(|| existing.outbox.last_error.clone())
        .unwrap_or_else(|| "DELIVERY_CANCELLED_BEFORE_WRITE".to_owned());
    if error == DELIVERY_ERROR_COMMAND_EXPIRED {
        DeliveryOutcome::Rejected(DeliveryRejection {
            attempt_id: existing.attempt.attempt_id,
            message_id: existing.outbox.message_id,
            session_id: existing.outbox.session_id,
            rejected_at: existing.attempt.updated_at,
            reason: DeliveryRejectionReason::Expired,
        })
    } else {
        DeliveryOutcome::DefinitelyNotWritten(DeliveryFailure {
            attempt_id: existing.attempt.attempt_id,
            message_id: existing.outbox.message_id,
            session_id: existing.outbox.session_id,
            failed_at: existing.attempt.updated_at,
            error,
        })
    }
}

fn unavailable_outcome(existing: StoredOutboundDelivery) -> DeliveryOutcome {
    let reason = if existing.attempt.error.as_deref() == Some(DELIVERY_ERROR_CLOCK_UNHEALTHY)
        || existing.outbox.last_error.as_deref() == Some(DELIVERY_ERROR_CLOCK_UNHEALTHY)
    {
        DeliveryRejectionReason::ClockUnhealthy
    } else {
        DeliveryRejectionReason::NoActiveSession
    };
    DeliveryOutcome::Rejected(DeliveryRejection {
        attempt_id: existing.attempt.attempt_id,
        message_id: existing.outbox.message_id,
        session_id: existing.outbox.session_id,
        rejected_at: existing.attempt.updated_at,
        reason,
    })
}

fn transport_rejection_reason(existing: &StoredOutboundDelivery) -> Option<String> {
    existing
        .outbox
        .last_error
        .as_deref()?
        .strip_prefix(TRANSPORT_ACK_REJECTED_PREFIX)
        .filter(|reason| !reason.trim().is_empty())
        .map(str::to_owned)
}

fn is_stale_outbound(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::StaleWrite {
            entity: "outbound_delivery",
            ..
        }
    )
}

fn next_projection_time(now: i64, previous: i64) -> i64 {
    now.max(previous)
}

fn validate_existing_delivery<T>(
    request: &DeliveryRequest<T>,
    subject: &DeliverySubject,
    existing: &StoredOutboundDelivery,
) -> Result<(), DeliveryInfrastructureError>
where
    T: DeserializeOwned + PartialEq,
{
    let stored: WireMessage<T> = serde_json::from_str(existing.outbox.payload.as_str())
        .map_err(|error| infrastructure(format!("stored replay envelope is invalid: {error}")))?;
    let subject_matches = &existing.attempt.subject == subject
        && existing.outbox.command_id.as_ref() == subject.command_id()
        && existing.outbox.request_id.as_ref() == subject.request_id();
    let envelope_matches = stored.message_id == request.message.message_id
        && stored.message_type == request.message.message_type
        && stored.schema_version == request.message.schema_version
        && stored.correlation_id == request.message.correlation_id
        && stored.causation_id == request.message.causation_id
        && stored.payload == request.message.payload
        && request
            .client_id
            .as_ref()
            .is_none_or(|client_id| stored.client_id.as_ref() == Some(client_id));
    if !subject_matches
        || !envelope_matches
        || existing.attempt.attempt_id != attempt_id(&request.message.message_id)
    {
        return Err(infrastructure(
            "message_id replay conflicts with its durable subject or payload",
        ));
    }
    Ok(())
}

fn parse_queue_depth(error: &str) -> Option<usize> {
    error.rsplit_once(' ')?.1.parse().ok()
}

fn infrastructure(error: impl Display) -> DeliveryInfrastructureError {
    DeliveryInfrastructureError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use sinan_execution::ServerClock;
    use sinan_protocol::{
        ExecutionClientMessageType, ExecutionClientPlatform, ReconciliationReason,
    };
    use sinan_store::{
        CanonicalJson, CoreEventMetadata, DeliverySubject, NewExecutionCommand,
        NewReconciliationRun, NewRiskResult, NewTradeIntent, OutboundReservation, SqliteStateStore,
        StoreOptions, StoredDeliveryAttempt, StoredWireOutbox,
    };
    use sinan_types::{
        single_leg_id, AccountId, AdjustedRiskLeg, AdjustedRiskLegAction, ClientId, CommandId,
        CorrelationId, DecisionId, ErrorCodeOrString, ExecutionAction, IdempotencyKey, IntentId,
        MessageId, RequestId, RiskId, RiskResult, SessionId, SizingCandidateProvenance, StrategyId,
        SymbolCode, TerminalId, TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus,
    };

    use super::*;
    use crate::{
        GatewaySessionConfig, GatewaySessionRegistry, LiveSessionRegistry, OutboundSink,
        SessionRegistration, SinkWriteFuture,
    };

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase(PathBuf);

    impl TestDatabase {
        fn unique() -> Self {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock should be after Unix epoch")
                .as_nanos();
            let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "sinan-gateway-outbound-{}-{timestamp}-{sequence}.sqlite",
                std::process::id()
            )))
        }

        fn url(&self) -> String {
            format!("sqlite://{}", self.0.display())
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(format!("{}-wal", self.0.display()));
            let _ = fs::remove_file(format!("{}-shm", self.0.display()));
        }
    }

    struct ManualClock(AtomicI64);

    impl ManualClock {
        fn new(now: i64) -> Self {
            Self(AtomicI64::new(now))
        }

        fn set(&self, now: i64) {
            self.0.store(now, Ordering::Release);
        }
    }

    impl ServerClock for ManualClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Acquire)
        }
    }

    struct ClaimExpiryClock(AtomicU64);

    impl ClaimExpiryClock {
        fn new() -> Self {
            Self(AtomicU64::new(0))
        }
    }

    impl ServerClock for ClaimExpiryClock {
        fn now_ms(&self) -> i64 {
            if self.0.fetch_add(1, Ordering::AcqRel) == 0 {
                1_100
            } else {
                1_200
            }
        }
    }

    struct RecordingSink {
        frames: Arc<Mutex<Vec<OutboundFrame>>>,
        outcome: SinkWriteOutcome,
    }

    impl OutboundSink for RecordingSink {
        fn write<'a>(&'a self, frame: OutboundFrame) -> SinkWriteFuture<'a> {
            self.frames.lock().unwrap().push(frame);
            let outcome = self.outcome.clone();
            Box::pin(async move { outcome })
        }

        fn skip(&self, _sequence: u64) {}
    }

    struct ControlledSink {
        frames: Arc<Mutex<Vec<OutboundFrame>>>,
        started: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
        changed: Arc<tokio::sync::Notify>,
    }

    impl OutboundSink for ControlledSink {
        fn write<'a>(&'a self, frame: OutboundFrame) -> SinkWriteFuture<'a> {
            self.frames.lock().unwrap().push(frame);
            let started = Arc::clone(&self.started);
            let released = Arc::clone(&self.released);
            let changed = Arc::clone(&self.changed);
            Box::pin(async move {
                started.store(true, Ordering::Release);
                changed.notify_waiters();
                while !released.load(Ordering::Acquire) {
                    changed.notified().await;
                }
                SinkWriteOutcome::Written
            })
        }

        fn skip(&self, _sequence: u64) {}
    }

    async fn test_store() -> (TestDatabase, SqliteStateStore) {
        let database = TestDatabase::unique();
        let mut options = StoreOptions::new(database.url());
        options.max_connections = 8;
        options.busy_timeout = Duration::from_secs(5);
        let store = SqliteStateStore::connect(options)
            .await
            .expect("gateway test store should connect");
        (database, store)
    }

    fn session_registration(session_id: &str, terminal_id: Option<&str>) -> SessionRegistration {
        SessionRegistration {
            session_id: SessionId::from(session_id),
            client_id: ClientId::from("client_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: terminal_id.map(TerminalId::from),
            platform: ExecutionClientPlatform::Mt5,
            capabilities: vec!["orders".to_owned(), "snapshots".to_owned()],
            remote_addr: Some("127.0.0.1:5000".to_owned()),
            max_inflight_commands: 8,
        }
    }

    fn session_registry(
        store: SqliteStateStore,
        live: Arc<LiveSessionRegistry>,
        clock: Arc<ManualClock>,
    ) -> GatewaySessionRegistry {
        GatewaySessionRegistry::new(
            store,
            live,
            clock,
            GatewaySessionConfig {
                max_clock_offset_ms: 250,
                max_time_sync_age_ms: 500,
                max_time_sync_rtt_ms: 1_000,
            },
        )
        .unwrap()
    }

    fn outbound_adapter(
        store: SqliteStateStore,
        live: Arc<LiveSessionRegistry>,
        clock: Arc<ManualClock>,
    ) -> GatewayOutboundAdapter {
        let sessions = session_registry(store, live, clock);
        GatewayOutboundAdapter::new(
            sessions,
            GatewayOutboundConfig {
                confirmation_timeout_ms: 1_000,
            },
        )
        .unwrap()
    }

    async fn reconciliation_request(
        store: &SqliteStateStore,
        request_id: &str,
        terminal_id: Option<&str>,
    ) -> ReconciliationRequest {
        let requested_at = 900;
        let request = ReconciliationRequest {
            request_id: RequestId::from(request_id),
            account_id: AccountId::from("account_1"),
            terminal_id: terminal_id.map(TerminalId::from),
            client_id: Some(ClientId::from("client_1")),
            reason: ReconciliationReason::ManualRequest,
            command_ids: None,
            since_server_time: Some(800),
        };
        store
            .create_reconciliation_run(NewReconciliationRun {
                request: request.clone(),
                requested_at,
                event_metadata: CoreEventMetadata {
                    event_id: format!("request-event-{request_id}"),
                    event_type: "reconciliation.request".to_owned(),
                    aggregate_type: "reconciliation".to_owned(),
                    aggregate_id: request_id.to_owned(),
                    message_id: None,
                    schema_version: "ecp.v1.0".to_owned(),
                    correlation_id: None,
                    causation_id: None,
                    account_id: Some(AccountId::from("account_1")),
                    client_id: Some(ClientId::from("client_1")),
                    terminal_id: terminal_id.map(TerminalId::from),
                    strategy_id: None,
                    intent_id: None,
                    plan_id: None,
                    leg_id: None,
                    command_id: None,
                    idempotency_key: None,
                    event_at: requested_at,
                    received_at: requested_at + 1,
                    created_at: requested_at + 2,
                    source: "gateway-test".to_owned(),
                },
            })
            .await
            .unwrap();
        request
    }

    fn delivery_request(
        request: ReconciliationRequest,
        message_id: &str,
    ) -> DeliveryRequest<ReconciliationRequest> {
        DeliveryRequest {
            account_id: request.account_id.clone(),
            client_id: request.client_id.clone(),
            terminal_id: request.terminal_id.clone(),
            command_id: None,
            message: WireMessage {
                message_id: MessageId::from(message_id),
                message_type: ExecutionClientMessageType::ReconciliationRequest,
                schema_version: "ecp.v1.0".to_owned(),
                client_id: request.client_id.clone(),
                session_id: None,
                correlation_id: None,
                causation_id: None,
                sent_at: None,
                sequence: None,
                payload: request,
            },
            expires_at: None,
        }
    }

    async fn execution_command(
        store: &SqliteStateStore,
        command_id: &str,
        expires_at: i64,
    ) -> ExecutionCommand {
        let intent_id = IntentId::from(format!("intent_{command_id}"));
        let risk_id = RiskId::from(format!("risk_{command_id}"));
        let decision_id = DecisionId::from(format!("decision_{command_id}"));
        let strategy_id = StrategyId::from("strategy_1");
        store
            .insert_trade_intent(NewTradeIntent {
                intent: TradeIntent {
                    intent_id: intent_id.clone(),
                    decision_id: decision_id.clone(),
                    strategy_id: strategy_id.clone(),
                    correlation_id: CorrelationId::from(format!("correlation_{command_id}")),
                    idempotency_key: IdempotencyKey::from(format!("intent_key_{command_id}")),
                    account_id: AccountId::from("account_1"),
                    symbol: SymbolCode::from("XAUUSD"),
                    timeframe: TimeframeCode::from("H4"),
                    action: TradeIntentAction::Buy,
                    confidence: 0.8,
                    reason: "gateway command test".to_owned(),
                    proposed_risk_pct: 1.0,
                    proposed_sl: Some(2_320.5),
                    proposed_tp: Some(2_365.5),
                    proposed_legs: None,
                    decision_timestamp: 800,
                    signal_expires_at: 10_000,
                    requested_at: 900,
                },
                initial_status: TradeIntentStatus::Accepted,
                recorded_at: 901,
            })
            .await
            .unwrap();
        let leg_id = single_leg_id(&intent_id);
        store
            .insert_risk_result(NewRiskResult {
                result: RiskResult {
                    risk_id: risk_id.clone(),
                    request_id: RequestId::from(format!("risk_request_{command_id}")),
                    intent_id: intent_id.clone(),
                    account_id: AccountId::from("account_1"),
                    risk_request_hash: "a".repeat(64),
                    approved: true,
                    reason: ErrorCodeOrString::from("OK"),
                    message: None,
                    sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
                    risk_base_amount: Some(10_000.0),
                    risk_budget_amount: Some(100.0),
                    adjusted_risk_pct: Some(0.98),
                    sizing_candidates: Some(vec![SizingCandidateProvenance {
                        leg_id: leg_id.clone(),
                        symbol: SymbolCode::from("XAUUSD"),
                        action: AdjustedRiskLegAction::Buy,
                        ratio: 1.0,
                        worst_entry_price: 2_350.0,
                        stop_loss_price: 2_320.5,
                        estimated_cost_per_lot: 0.0,
                    }]),
                    adjusted_legs: Some(vec![AdjustedRiskLeg {
                        leg_id,
                        symbol: SymbolCode::from("XAUUSD"),
                        action: AdjustedRiskLegAction::Buy,
                        lots: 0.07,
                        risk_amount: 98.0,
                        risk_pct: 0.98,
                        sizing_entry_price: 2_350.0,
                        approved_sl: 2_320.5,
                        loss_per_lot: 1_400.0,
                        reason: Some(ErrorCodeOrString::from("OK")),
                    }]),
                    decision_id,
                    snapshot_age_ms: 125,
                    market_snapshot_age_ms: 75,
                    symbol_metadata_age_ms: 250,
                    capacity_age_ms: 100,
                    evaluated_at: 950,
                    valid_until: 10_000,
                },
            })
            .await
            .unwrap();
        let command = ExecutionCommand {
            command_id: CommandId::from(command_id),
            plan_id: None,
            leg_id: None,
            strategy_id,
            account_id: AccountId::from("account_1"),
            terminal_id: Some(TerminalId::from("terminal_1")),
            client_id: Some(ClientId::from("client_1")),
            symbol: SymbolCode::from("XAUUSD"),
            broker_symbol: Some("XAUUSD".to_owned()),
            action: ExecutionAction::Cancel,
            order_type: None,
            lots: None,
            price: None,
            sl: None,
            tp: None,
            deviation_points: None,
            magic: 1,
            comment: None,
            position_ticket: None,
            broker_order_id: Some("broker_order_1".into()),
            filling_policy: None,
            time_policy: None,
            expiration_time: None,
            expires_at,
            idempotency_key: IdempotencyKey::from(format!("command_key_{command_id}")),
            hmac: "a".repeat(64),
        };
        store
            .insert_execution_command(NewExecutionCommand {
                command: command.clone(),
                risk_id,
                created_at: 1_000,
            })
            .await
            .unwrap();
        command
    }

    fn command_delivery_request(
        command: ExecutionCommand,
        message_id: &str,
    ) -> DeliveryRequest<ExecutionCommand> {
        DeliveryRequest {
            account_id: command.account_id.clone(),
            client_id: command.client_id.clone(),
            terminal_id: command.terminal_id.clone(),
            command_id: Some(command.command_id.clone()),
            expires_at: Some(command.expires_at),
            message: WireMessage {
                message_id: MessageId::from(message_id),
                message_type: ExecutionClientMessageType::ExecutionCommand,
                schema_version: "ecp.v1.0".to_owned(),
                client_id: command.client_id.clone(),
                session_id: None,
                correlation_id: None,
                causation_id: None,
                sent_at: None,
                sequence: None,
                payload: command,
            },
        }
    }

    async fn synchronize_session(
        sessions: &GatewaySessionRegistry,
        clock: &ManualClock,
        session_id: &str,
    ) {
        clock.set(1_100);
        sessions
            .assess_heartbeat(
                &SessionId::from(session_id),
                &sinan_protocol::HeartbeatPayload {
                    effective_server_now: 1_100,
                    clock_sync_status: sinan_types::ClockSyncStatus::Synced,
                    last_time_sync_at_server_ms: Some(1_050),
                    last_time_sync_rtt_ms: Some(10),
                    server_time_offset_ms: Some(0),
                    send_queue_depth: None,
                    command_inbox_depth: None,
                },
            )
            .await
            .unwrap();
    }

    fn stored_delivery(
        outbox_status: WireOutboxStatus,
        attempt_status: CommandDeliveryAttemptStatus,
        outbox_error: Option<&str>,
        attempt_error: Option<&str>,
    ) -> StoredOutboundDelivery {
        StoredOutboundDelivery {
            outbox: StoredWireOutbox {
                message_id: MessageId::from("message_1"),
                session_id: Some(SessionId::from("session_1")),
                message_type: "execution.command".to_owned(),
                sequence: Some(2),
                command_id: Some(CommandId::from("command_1")),
                request_id: None,
                payload: CanonicalJson::from_value(serde_json::json!({})).unwrap(),
                status: outbox_status,
                revision: 1,
                created_at: 100,
                updated_at: 110,
                sent_at: matches!(
                    outbox_status,
                    WireOutboxStatus::Sent | WireOutboxStatus::Acked
                )
                .then_some(105),
                acked_at: (outbox_status == WireOutboxStatus::Acked).then_some(110),
                last_error: outbox_error.map(str::to_owned),
            },
            attempt: StoredDeliveryAttempt {
                attempt_id: "attempt:message_1".to_owned(),
                subject: DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
                session_id: Some(SessionId::from("session_1")),
                message_id: Some(MessageId::from("message_1")),
                request_payload: None,
                status: attempt_status,
                attempted_at: 100,
                acked_at: (attempt_status == CommandDeliveryAttemptStatus::Acked).then_some(112),
                error: attempt_error.map(str::to_owned),
                revision: 1,
                updated_at: 112,
            },
        }
    }

    fn unbound_attempt(
        status: CommandDeliveryAttemptStatus,
        session_id: Option<&str>,
        error: &str,
    ) -> StoredDeliveryAttempt {
        StoredDeliveryAttempt {
            attempt_id: "attempt:message_1".to_owned(),
            subject: DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
            session_id: session_id.map(SessionId::from),
            message_id: None,
            request_payload: Some(test_rejected_request_payload()),
            status,
            attempted_at: 100,
            acked_at: None,
            error: Some(error.to_owned()),
            revision: 0,
            updated_at: 100,
        }
    }

    fn test_rejected_request_payload() -> CanonicalJson {
        CanonicalJson::from_value(serde_json::json!({"draft": "message_1"})).unwrap()
    }

    #[test]
    fn binding_uses_gateway_owned_session_sequence_and_time() {
        let mut message = WireMessage {
            message_id: MessageId::from("message_1"),
            message_type: ExecutionClientMessageType::ExecutionCommand,
            schema_version: "ecp.v1.0".to_owned(),
            client_id: None,
            session_id: None,
            correlation_id: None,
            causation_id: None,
            sent_at: None,
            sequence: None,
            payload: serde_json::json!({}),
        };
        let reservation = OutboundReservation {
            session_id: SessionId::from("session_1"),
            client_id: ClientId::from("client_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: None,
            session_revision: 1,
            sequence: 2,
            subject: DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
        };

        bind_wire_message(&mut message, &reservation, 1_000).unwrap();

        assert_eq!(message.client_id.as_deref(), Some("client_1"));
        assert_eq!(message.session_id.as_deref(), Some("session_1"));
        assert_eq!(message.sequence, Some(2));
        assert_eq!(message.sent_at, Some(1_000));
    }

    #[test]
    fn replay_matrix_keeps_transport_and_command_acknowledgements_distinct() {
        for (outbox, attempt) in [
            (WireOutboxStatus::Sent, CommandDeliveryAttemptStatus::Sent),
            (
                WireOutboxStatus::Acked,
                CommandDeliveryAttemptStatus::Pending,
            ),
            (WireOutboxStatus::Acked, CommandDeliveryAttemptStatus::Sent),
            (
                WireOutboxStatus::Acked,
                CommandDeliveryAttemptStatus::Backpressure,
            ),
            (
                WireOutboxStatus::Acked,
                CommandDeliveryAttemptStatus::Failed,
            ),
            (WireOutboxStatus::Sent, CommandDeliveryAttemptStatus::Acked),
            (WireOutboxStatus::Acked, CommandDeliveryAttemptStatus::Acked),
            (
                WireOutboxStatus::WriteStarted,
                CommandDeliveryAttemptStatus::Acked,
            ),
        ] {
            let outcome =
                durable_delivery_outcome(stored_delivery(outbox, attempt, None, None), 120, 500)
                    .unwrap();
            assert!(matches!(outcome, DeliveryOutcome::Sent(_)));
        }

        for (outbox, attempt) in [
            (
                WireOutboxStatus::WriteStarted,
                CommandDeliveryAttemptStatus::Pending,
            ),
            (
                WireOutboxStatus::WriteStarted,
                CommandDeliveryAttemptStatus::Unconfirmed,
            ),
            (
                WireOutboxStatus::Sent,
                CommandDeliveryAttemptStatus::Unconfirmed,
            ),
            (
                WireOutboxStatus::Acked,
                CommandDeliveryAttemptStatus::Unconfirmed,
            ),
        ] {
            let outcome = durable_delivery_outcome(
                stored_delivery(outbox, attempt, None, Some("confirmation pending")),
                120,
                500,
            )
            .unwrap();
            assert!(matches!(outcome, DeliveryOutcome::Unconfirmed(_)));
        }
    }

    #[test]
    fn command_receipt_wins_but_transport_rejection_beats_uncertainty() {
        let rejected = format!("{TRANSPORT_ACK_REJECTED_PREFIX}queue rejected");
        let receipt_wins = durable_delivery_outcome(
            stored_delivery(
                WireOutboxStatus::Failed,
                CommandDeliveryAttemptStatus::Acked,
                Some(&rejected),
                None,
            ),
            120,
            500,
        )
        .unwrap();
        assert!(matches!(receipt_wins, DeliveryOutcome::Sent(_)));

        let rejection_wins = durable_delivery_outcome(
            stored_delivery(
                WireOutboxStatus::Failed,
                CommandDeliveryAttemptStatus::Unconfirmed,
                Some(&rejected),
                Some("session replaced"),
            ),
            120,
            500,
        )
        .unwrap();
        assert!(matches!(
            rejection_wins,
            DeliveryOutcome::Rejected(DeliveryRejection {
                reason: DeliveryRejectionReason::TransportRejected { ref reason },
                ..
            }) if reason == "queue rejected"
        ));
    }

    #[test]
    fn cancelled_replay_distinguishes_expiry_route_failure_and_zero_byte_failure() {
        let expired = durable_delivery_outcome(
            stored_delivery(
                WireOutboxStatus::Cancelled,
                CommandDeliveryAttemptStatus::Cancelled,
                Some(DELIVERY_ERROR_COMMAND_EXPIRED),
                Some(DELIVERY_ERROR_COMMAND_EXPIRED),
            ),
            120,
            500,
        )
        .unwrap();
        assert!(matches!(
            expired,
            DeliveryOutcome::Rejected(DeliveryRejection {
                reason: DeliveryRejectionReason::Expired,
                ..
            })
        ));

        let unavailable = durable_delivery_outcome(
            stored_delivery(
                WireOutboxStatus::Cancelled,
                CommandDeliveryAttemptStatus::NoActiveSession,
                Some(DELIVERY_ERROR_CLOCK_UNHEALTHY),
                Some(DELIVERY_ERROR_CLOCK_UNHEALTHY),
            ),
            120,
            500,
        )
        .unwrap();
        assert!(matches!(
            unavailable,
            DeliveryOutcome::Rejected(DeliveryRejection {
                reason: DeliveryRejectionReason::ClockUnhealthy,
                ..
            })
        ));

        let replaced = durable_delivery_outcome(
            stored_delivery(
                WireOutboxStatus::Cancelled,
                CommandDeliveryAttemptStatus::Cancelled,
                Some("SESSION_REPLACED"),
                Some("SESSION_REPLACED"),
            ),
            120,
            500,
        )
        .unwrap();
        assert!(matches!(replaced, DeliveryOutcome::DefinitelyNotWritten(_)));
    }

    #[test]
    fn unbound_rejection_replay_is_stable_and_preserves_selected_session() {
        let valid = ();
        let outcome = replay_rejection(
            unbound_attempt(
                CommandDeliveryAttemptStatus::Backpressure,
                Some("session_1"),
                "COMMAND_INFLIGHT_LIMIT_REACHED:8",
            ),
            &DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
            MessageId::from("message_1"),
            &test_rejected_request_payload(),
            Ok(&valid),
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DeliveryOutcome::Rejected(DeliveryRejection {
                session_id: Some(ref session_id),
                reason: DeliveryRejectionReason::InflightLimit { limit: 8 },
                rejected_at: 100,
                ..
            }) if session_id.as_str() == "session_1"
        ));
    }

    #[test]
    fn unbound_rejection_replay_rejects_subject_or_identity_drift() {
        let valid = ();
        assert!(replay_rejection(
            unbound_attempt(
                CommandDeliveryAttemptStatus::NoActiveSession,
                None,
                "NO_ACTIVE_SESSION",
            ),
            &DeliverySubject::ExecutionCommand(CommandId::from("command_2")),
            MessageId::from("message_1"),
            &test_rejected_request_payload(),
            Ok(&valid),
        )
        .is_err());

        let drifted_payload =
            CanonicalJson::from_value(serde_json::json!({"draft": "changed"})).unwrap();
        assert!(replay_rejection(
            unbound_attempt(
                CommandDeliveryAttemptStatus::NoActiveSession,
                None,
                "NO_ACTIVE_SESSION",
            ),
            &DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
            MessageId::from("message_1"),
            &drifted_payload,
            Ok(&valid),
        )
        .is_err());

        let current = DeliveryRejectionReason::IdentityMismatch {
            field: "terminal_id",
        };
        assert!(replay_rejection(
            unbound_attempt(
                CommandDeliveryAttemptStatus::Failed,
                None,
                "SESSION_IDENTITY_MISMATCH:account_id",
            ),
            &DeliverySubject::ExecutionCommand(CommandId::from("command_1")),
            MessageId::from("message_1"),
            &test_rejected_request_payload(),
            Err(&current),
        )
        .is_err());
    }

    #[tokio::test]
    async fn synced_session_delivers_and_replays_execution_command() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let sessions = session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        sessions
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        synchronize_session(&sessions, &clock, "session_1").await;
        let request = command_delivery_request(
            execution_command(&store, "command_1", 5_000).await,
            "message_1",
        );
        let adapter = outbound_adapter(store.clone(), live, clock);

        let first = adapter
            .deliver_execution_command(request.clone())
            .await
            .unwrap();
        let replay = adapter.deliver_execution_command(request).await.unwrap();

        assert!(matches!(&first, DeliveryOutcome::Sent(_)));
        assert_eq!(first, replay);
        assert_eq!(frames.lock().unwrap().len(), 1);
        let stored = store
            .get_outbound_delivery(&MessageId::from("message_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.outbox.status, WireOutboxStatus::Sent);
        assert_eq!(stored.attempt.status, CommandDeliveryAttemptStatus::Sent);
    }

    #[tokio::test]
    async fn command_expiring_between_reserve_and_claim_never_reaches_the_sink() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let session_clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let sessions =
            session_registry(store.clone(), Arc::clone(&live), Arc::clone(&session_clock));
        sessions
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        synchronize_session(&sessions, &session_clock, "session_1").await;
        let request = command_delivery_request(
            execution_command(&store, "command_1", 1_150).await,
            "message_1",
        );
        let outbound_sessions = GatewaySessionRegistry::new(
            store.clone(),
            live,
            Arc::new(ClaimExpiryClock::new()),
            GatewaySessionConfig {
                max_clock_offset_ms: 250,
                max_time_sync_age_ms: 500,
                max_time_sync_rtt_ms: 1_000,
            },
        )
        .unwrap();
        let adapter = GatewayOutboundAdapter::new(
            outbound_sessions,
            GatewayOutboundConfig {
                confirmation_timeout_ms: 1_000,
            },
        )
        .unwrap();

        let outcome = adapter.deliver_execution_command(request).await.unwrap();

        assert!(matches!(
            outcome,
            DeliveryOutcome::Rejected(DeliveryRejection {
                reason: DeliveryRejectionReason::Expired,
                ..
            })
        ));
        assert!(frames.lock().unwrap().is_empty());
        let stored = store
            .get_outbound_delivery(&MessageId::from("message_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.outbox.status, WireOutboxStatus::Cancelled);
        assert_eq!(
            stored.attempt.status,
            CommandDeliveryAttemptStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn reconciliation_delivery_is_durable_and_replay_does_not_rewrite_the_sink() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock))
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        let request = delivery_request(
            reconciliation_request(&store, "request_1", Some("terminal_1")).await,
            "message_1",
        );
        let adapter = outbound_adapter(store.clone(), live, clock);

        let first = adapter
            .deliver_reconciliation_request(request.clone())
            .await
            .unwrap();
        let replay = adapter
            .deliver_reconciliation_request(request)
            .await
            .unwrap();

        let first_receipt = match first {
            DeliveryOutcome::Sent(receipt) => receipt,
            outcome => panic!("unexpected first delivery outcome: {outcome:?}"),
        };
        let replay_receipt = match replay {
            DeliveryOutcome::Sent(receipt) => receipt,
            outcome => panic!("unexpected replay outcome: {outcome:?}"),
        };
        assert_eq!(first_receipt, replay_receipt);
        assert_eq!(first_receipt.sequence, 2);
        assert_eq!(frames.lock().unwrap().len(), 1);
        assert_eq!(
            store
                .get_session(&SessionId::from("session_1"))
                .await
                .unwrap()
                .unwrap()
                .last_outbound_sequence,
            2
        );
        let stored = store
            .get_outbound_delivery(&MessageId::from("message_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.outbox.status, WireOutboxStatus::Sent);
        assert_eq!(stored.attempt.status, CommandDeliveryAttemptStatus::Sent);
    }

    async fn run_sink_case(
        suffix: &str,
        sink_outcome: SinkWriteOutcome,
    ) -> (
        DeliveryOutcome,
        DeliveryOutcome,
        StoredOutboundDelivery,
        usize,
    ) {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock))
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: sink_outcome,
                }),
            )
            .await
            .unwrap();
        let message_id = format!("message_{suffix}");
        let request = delivery_request(
            reconciliation_request(&store, &format!("request_{suffix}"), Some("terminal_1")).await,
            &message_id,
        );
        let adapter = outbound_adapter(store.clone(), live, clock);
        let first = adapter
            .deliver_reconciliation_request(request.clone())
            .await
            .unwrap();
        let replay = adapter
            .deliver_reconciliation_request(request)
            .await
            .unwrap();
        let stored = store
            .get_outbound_delivery(&MessageId::from(message_id))
            .await
            .unwrap()
            .unwrap();
        let frame_count = frames.lock().unwrap().len();
        (first, replay, stored, frame_count)
    }

    #[tokio::test]
    async fn sink_outcomes_are_persisted_and_replayed_without_a_second_write() {
        let (first, replay, stored, writes) = run_sink_case(
            "backpressure",
            SinkWriteOutcome::Backpressure { queue_depth: 7 },
        )
        .await;
        for outcome in [first, replay] {
            assert!(matches!(
                outcome,
                DeliveryOutcome::Rejected(DeliveryRejection {
                    reason: DeliveryRejectionReason::Backpressure { queue_depth: 7 },
                    ..
                })
            ));
        }
        assert_eq!(stored.outbox.status, WireOutboxStatus::Failed);
        assert_eq!(
            stored.attempt.status,
            CommandDeliveryAttemptStatus::Backpressure
        );
        assert_eq!(writes, 1);

        let (first, replay, stored, writes) = run_sink_case(
            "failed",
            SinkWriteOutcome::DefinitelyNotWritten {
                error: "socket closed before write".to_owned(),
            },
        )
        .await;
        assert!(matches!(first, DeliveryOutcome::DefinitelyNotWritten(_)));
        assert!(matches!(replay, DeliveryOutcome::DefinitelyNotWritten(_)));
        assert_eq!(stored.outbox.status, WireOutboxStatus::Failed);
        assert_eq!(stored.attempt.status, CommandDeliveryAttemptStatus::Failed);
        assert_eq!(writes, 1);

        let (first, replay, stored, writes) = run_sink_case(
            "unconfirmed",
            SinkWriteOutcome::Unconfirmed {
                error: "connection closed after write started".to_owned(),
            },
        )
        .await;
        assert!(matches!(first, DeliveryOutcome::Unconfirmed(_)));
        assert!(matches!(replay, DeliveryOutcome::Unconfirmed(_)));
        assert_eq!(stored.outbox.status, WireOutboxStatus::WriteStarted);
        assert_eq!(
            stored.attempt.status,
            CommandDeliveryAttemptStatus::Unconfirmed
        );
        assert_eq!(writes, 1);
    }

    #[tokio::test]
    async fn no_route_rejection_replays_after_a_session_later_connects() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let request = delivery_request(
            reconciliation_request(&store, "request_1", Some("terminal_1")).await,
            "message_1",
        );
        let adapter = outbound_adapter(store.clone(), Arc::clone(&live), Arc::clone(&clock));

        let first = adapter
            .deliver_reconciliation_request(request.clone())
            .await
            .unwrap();
        let mut drifting = request.clone();
        drifting.message.correlation_id = Some("different-correlation".into());
        assert!(adapter
            .deliver_reconciliation_request(drifting)
            .await
            .is_err());
        session_registry(store, live, clock)
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        let replay = adapter
            .deliver_reconciliation_request(request)
            .await
            .unwrap();

        for outcome in [first, replay] {
            assert!(matches!(
                outcome,
                DeliveryOutcome::Rejected(DeliveryRejection {
                    reason: DeliveryRejectionReason::NoActiveSession,
                    session_id: None,
                    ..
                })
            ));
        }
        assert!(frames.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn optional_route_filters_reject_ambiguous_active_sessions() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let first_frames = Arc::new(Mutex::new(Vec::new()));
        let second_frames = Arc::new(Mutex::new(Vec::new()));
        let sessions = session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        sessions
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&first_frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        sessions
            .activate(
                session_registration("session_2", Some("terminal_2")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&second_frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        let request = delivery_request(
            reconciliation_request(&store, "request_1", None).await,
            "message_1",
        );
        let adapter = outbound_adapter(store, live, clock);

        let outcome = adapter
            .deliver_reconciliation_request(request)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            DeliveryOutcome::Rejected(DeliveryRejection {
                reason: DeliveryRejectionReason::AmbiguousRoute { candidate_count: 2 },
                session_id: None,
                ..
            })
        ));
        assert!(first_frames.lock().unwrap().is_empty());
        assert!(second_frames.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn command_inflight_limit_rejects_without_consuming_sequence_or_rewriting() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let sessions = session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        let mut registration = session_registration("session_1", Some("terminal_1"));
        registration.max_inflight_commands = 1;
        sessions
            .activate(
                registration,
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        synchronize_session(&sessions, &clock, "session_1").await;
        let first = command_delivery_request(
            execution_command(&store, "command_1", 5_000).await,
            "message_1",
        );
        let second = command_delivery_request(
            execution_command(&store, "command_2", 5_000).await,
            "message_2",
        );
        let adapter = outbound_adapter(store.clone(), live, clock);

        assert!(matches!(
            adapter.deliver_execution_command(first).await.unwrap(),
            DeliveryOutcome::Sent(_)
        ));
        let rejected = adapter
            .deliver_execution_command(second.clone())
            .await
            .unwrap();
        let replay = adapter.deliver_execution_command(second).await.unwrap();

        for outcome in [rejected, replay] {
            assert!(matches!(
                outcome,
                DeliveryOutcome::Rejected(DeliveryRejection {
                    reason: DeliveryRejectionReason::InflightLimit { limit: 1 },
                    session_id: Some(ref session_id),
                    ..
                }) if session_id.as_str() == "session_1"
            ));
        }
        assert_eq!(frames.lock().unwrap().len(), 1);
        assert_eq!(
            store
                .get_session(&SessionId::from("session_1"))
                .await
                .unwrap()
                .unwrap()
                .last_outbound_sequence,
            2
        );
    }

    #[tokio::test]
    async fn replacement_during_an_admitted_write_returns_unconfirmed() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(tokio::sync::Notify::new());
        let sessions = session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        sessions
            .activate(
                session_registration("session_old", Some("terminal_1")),
                Arc::new(ControlledSink {
                    frames: Arc::clone(&frames),
                    started: Arc::clone(&started),
                    released: Arc::clone(&released),
                    changed: Arc::clone(&changed),
                }),
            )
            .await
            .unwrap();
        let request = delivery_request(
            reconciliation_request(&store, "request_1", Some("terminal_1")).await,
            "message_1",
        );
        let adapter = outbound_adapter(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        let delivery =
            tokio::spawn(async move { adapter.deliver_reconciliation_request(request).await });
        while !started.load(Ordering::Acquire) {
            changed.notified().await;
        }

        clock.set(1_100);
        sessions
            .activate(
                session_registration("session_new", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::new(Mutex::new(Vec::new())),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        released.store(true, Ordering::Release);
        changed.notify_waiters();

        let outcome = delivery.await.unwrap().unwrap();
        assert!(matches!(outcome, DeliveryOutcome::Unconfirmed(_)));
        assert_eq!(frames.lock().unwrap().len(), 1);
        let stored = store
            .get_outbound_delivery(&MessageId::from("message_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.attempt.status,
            CommandDeliveryAttemptStatus::Unconfirmed
        );
        assert!(live.handle(&SessionId::from("session_old")).is_none());
        assert!(live.handle(&SessionId::from("session_new")).is_some());
    }

    #[tokio::test]
    async fn concurrent_deliveries_allocate_unique_sequences_from_two() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let frames = Arc::new(Mutex::new(Vec::new()));
        session_registry(store.clone(), Arc::clone(&live), Arc::clone(&clock))
            .activate(
                session_registration("session_1", Some("terminal_1")),
                Arc::new(RecordingSink {
                    frames: Arc::clone(&frames),
                    outcome: SinkWriteOutcome::Written,
                }),
            )
            .await
            .unwrap();
        let adapter = outbound_adapter(store.clone(), live, clock);
        let mut requests = Vec::new();
        for index in 0..6 {
            let request_id = format!("request_{index}");
            let message_id = format!("message_{index}");
            requests.push(delivery_request(
                reconciliation_request(&store, &request_id, Some("terminal_1")).await,
                &message_id,
            ));
        }

        let tasks: Vec<_> = requests
            .into_iter()
            .map(|request| {
                let adapter = adapter.clone();
                tokio::spawn(async move { adapter.deliver_reconciliation_request(request).await })
            })
            .collect();
        let mut sequences = Vec::new();
        for task in tasks {
            match task.await.unwrap().unwrap() {
                DeliveryOutcome::Sent(receipt) => sequences.push(receipt.sequence),
                outcome => panic!("unexpected concurrent outcome: {outcome:?}"),
            }
        }
        sequences.sort_unstable();

        assert_eq!(sequences, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(frames.lock().unwrap().len(), 6);
        assert_eq!(
            store
                .get_session(&SessionId::from("session_1"))
                .await
                .unwrap()
                .unwrap()
                .last_outbound_sequence,
            7
        );
    }
}

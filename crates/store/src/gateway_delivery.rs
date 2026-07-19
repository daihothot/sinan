use std::{fmt::Display, str::FromStr};

use serde_json::Value;
use sinan_protocol::{decode_wire_message, TransportAckStatus, SUPPORTED_SCHEMA_VERSION};
use sinan_types::{
    ClockSyncStatus, CommandDeliveryAttemptStatus, CommandId, MessageId, RequestId, SessionId,
    SessionStatus, WireOutboxStatus,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    connection::{SqliteStateStore, WriteTransaction},
    error::StoreError,
    json::CanonicalJson,
    model::{
        ClaimWireOutbox, CommandReceivedAttemptUpdate, CompleteTransportWrite,
        DeliveryAttemptTimeout, DeliveryStartupFenceReport, DeliverySubject, NewDeliveryAttempt,
        NewReservedDelivery, NewSessionRecord, NewWireOutbox, OutboundReservation,
        OutboxClaimOutcome, ReserveOutboundSequence, SequenceReservation, SessionDisconnectOutcome,
        SessionHeartbeatUpdate, SessionReplacement, SessionRouteQuery, SessionRouteResolution,
        SessionStatusUpdate, StoredDeliveryAttempt, StoredOutboundDelivery, StoredSessionRecord,
        StoredWireOutbox, TransportAckUpdate, WriteOutcome, DELIVERY_ERROR_CLOCK_UNHEALTHY,
        DELIVERY_ERROR_COMMAND_EXPIRED, DELIVERY_ERROR_SESSION_UNAVAILABLE,
        TRANSPORT_ACK_REJECTED_PREFIX,
    },
    repository::{
        enqueue_wire_outbox_on, fetch_session_by_id, fetch_wire_outbox_by_id, insert_session_on,
        session_from_row, wire_outbox_from_row,
    },
};

const SESSION_COLUMNS: &str = "session_id, client_id, account_id, terminal_id, platform, status, \
    capabilities_json, remote_addr, connected_at, last_heartbeat_at, last_time_sync_at, \
    clock_sync_status, disconnected_at, revision, updated_at, last_outbound_sequence, \
    max_inflight_commands";

const ATTEMPT_COLUMNS: &str =
    "attempt_id, command_id, request_id, session_id, message_id, status, \
    request_payload_json, request_payload_hash, attempted_at, acked_at, error, revision, updated_at";

impl SqliteStateStore {
    pub async fn replace_active_session(
        &self,
        session: NewSessionRecord,
    ) -> Result<SessionReplacement, StoreError> {
        let mut transaction = self.begin_write().await?;
        let replacement = transaction.replace_active_session(session).await?;
        transaction.commit().await?;
        Ok(replacement)
    }

    pub async fn update_session_heartbeat(
        &self,
        update: SessionHeartbeatUpdate,
    ) -> Result<StoredSessionRecord, StoreError> {
        let mut transaction = self.begin_write().await?;
        let session = transaction.update_session_heartbeat(update).await?;
        transaction.commit().await?;
        Ok(session)
    }

    pub async fn mark_session_stale(
        &self,
        update: SessionStatusUpdate,
    ) -> Result<SessionDisconnectOutcome, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.mark_session_stale(update).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn disconnect_session(
        &self,
        update: SessionStatusUpdate,
    ) -> Result<SessionDisconnectOutcome, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.disconnect_session(update).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn fence_interrupted_writes(
        &self,
        fenced_at: i64,
        error: impl Into<String>,
    ) -> Result<DeliveryStartupFenceReport, StoreError> {
        let mut transaction = self.begin_write().await?;
        let report = transaction
            .fence_interrupted_writes(fenced_at, error.into())
            .await?;
        transaction.commit().await?;
        Ok(report)
    }

    pub async fn record_delivery_attempt(
        &self,
        attempt: NewDeliveryAttempt,
    ) -> Result<WriteOutcome<StoredDeliveryAttempt>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outcome = transaction.record_delivery_attempt(attempt).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn claim_outbox(
        &self,
        claim: ClaimWireOutbox,
    ) -> Result<OutboxClaimOutcome, StoreError> {
        let mut transaction = self.begin_write().await?;
        let delivery = transaction.claim_outbox(claim).await?;
        transaction.commit().await?;
        Ok(delivery)
    }

    pub async fn finish_transport_write_sent(
        &self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        self.finish_transport_write(completion, TransportWriteResult::Sent)
            .await
    }

    pub async fn finish_transport_write_backpressure(
        &self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        self.finish_transport_write(completion, TransportWriteResult::Backpressure)
            .await
    }

    pub async fn finish_transport_write_failed(
        &self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        self.finish_transport_write(completion, TransportWriteResult::Failed)
            .await
    }

    pub async fn finish_transport_write_unconfirmed(
        &self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        self.finish_transport_write(completion, TransportWriteResult::Unconfirmed)
            .await
    }

    async fn finish_transport_write(
        &self,
        completion: CompleteTransportWrite,
        result: TransportWriteResult,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        let mut transaction = self.begin_write().await?;
        let delivery =
            finish_transport_write_on(transaction.connection(), completion, result).await?;
        transaction.commit().await?;
        Ok(delivery)
    }

    pub async fn record_transport_ack(
        &self,
        update: TransportAckUpdate,
    ) -> Result<StoredWireOutbox, StoreError> {
        let mut transaction = self.begin_write().await?;
        let outbox = transaction.record_transport_ack(update).await?;
        transaction.commit().await?;
        Ok(outbox)
    }

    pub async fn record_command_received_attempt(
        &self,
        update: CommandReceivedAttemptUpdate,
    ) -> Result<StoredDeliveryAttempt, StoreError> {
        let mut transaction = self.begin_write().await?;
        let attempt = transaction.record_command_received_attempt(update).await?;
        transaction.commit().await?;
        Ok(attempt)
    }

    pub async fn timeout_delivery_attempt(
        &self,
        timeout: DeliveryAttemptTimeout,
    ) -> Result<StoredDeliveryAttempt, StoreError> {
        let mut transaction = self.begin_write().await?;
        let attempt = transaction.timeout_delivery_attempt(timeout).await?;
        transaction.commit().await?;
        Ok(attempt)
    }

    pub async fn get_delivery_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<StoredDeliveryAttempt>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_delivery_attempt_by_id(&mut connection, attempt_id).await
    }

    pub async fn get_delivery_attempt_by_message(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<StoredDeliveryAttempt>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_delivery_attempt_by_message(&mut connection, message_id).await
    }

    pub async fn get_outbound_delivery(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<StoredOutboundDelivery>, StoreError> {
        let mut transaction = self.pool().begin().await?;
        let delivery = fetch_outbound_delivery(transaction.as_mut(), message_id).await;
        match delivery {
            Ok(delivery) => {
                transaction.commit().await?;
                Ok(delivery)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn list_session_delivery_attempts(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StoredDeliveryAttempt>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        let rows = sqlx::query(&format!(
            "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts \
             WHERE session_id = ? ORDER BY attempted_at, attempt_id"
        ))
        .bind(session_id.as_str())
        .fetch_all(&mut *connection)
        .await?;
        parse_and_validate_attempt_rows(&mut connection, rows).await
    }

    pub async fn list_pending_outbox(
        &self,
        session_id: &SessionId,
        limit: u32,
    ) -> Result<Vec<StoredWireOutbox>, StoreError> {
        if limit == 0 {
            return Err(StoreError::InvalidSequence {
                field: "list_pending_outbox.limit",
            });
        }
        let rows = sqlx::query(
            "SELECT message_id, session_id, message_type, sequence, command_id, request_id, \
                    payload_json, payload_hash, status, revision, created_at, updated_at, sent_at, \
                    acked_at, last_error \
             FROM wire_outbox WHERE session_id = ? AND status = 'PENDING' \
             ORDER BY sequence, message_id LIMIT ?",
        )
        .bind(session_id.as_str())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(wire_outbox_from_row).collect()
    }
}

impl WriteTransaction {
    pub async fn replace_active_session(
        &mut self,
        session: NewSessionRecord,
    ) -> Result<SessionReplacement, StoreError> {
        replace_active_session_on(self.connection(), session).await
    }

    pub async fn update_session_heartbeat(
        &mut self,
        update: SessionHeartbeatUpdate,
    ) -> Result<StoredSessionRecord, StoreError> {
        update_session_heartbeat_on(self.connection(), update).await
    }

    pub async fn mark_session_stale(
        &mut self,
        update: SessionStatusUpdate,
    ) -> Result<SessionDisconnectOutcome, StoreError> {
        close_session_on(self.connection(), update, SessionStatus::Stale).await
    }

    pub async fn disconnect_session(
        &mut self,
        update: SessionStatusUpdate,
    ) -> Result<SessionDisconnectOutcome, StoreError> {
        close_session_on(self.connection(), update, SessionStatus::Disconnected).await
    }

    pub async fn fence_interrupted_writes(
        &mut self,
        fenced_at: i64,
        error: String,
    ) -> Result<DeliveryStartupFenceReport, StoreError> {
        fence_interrupted_writes_on(self.connection(), fenced_at, &error).await
    }

    pub async fn resolve_session_route(
        &mut self,
        query: SessionRouteQuery,
    ) -> Result<SessionRouteResolution, StoreError> {
        resolve_session_route_on(self.connection(), query).await
    }

    pub async fn reserve_outbound_sequence(
        &mut self,
        reservation: ReserveOutboundSequence,
    ) -> Result<SequenceReservation, StoreError> {
        reserve_outbound_sequence_on(self.connection(), reservation).await
    }

    pub async fn enqueue_reserved_delivery(
        &mut self,
        delivery: NewReservedDelivery,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        enqueue_reserved_delivery_on(self.connection(), delivery).await
    }

    pub async fn record_delivery_attempt(
        &mut self,
        attempt: NewDeliveryAttempt,
    ) -> Result<WriteOutcome<StoredDeliveryAttempt>, StoreError> {
        record_delivery_attempt_on(self.connection(), attempt).await
    }

    pub async fn claim_outbox(
        &mut self,
        claim: ClaimWireOutbox,
    ) -> Result<OutboxClaimOutcome, StoreError> {
        claim_outbox_on(self.connection(), claim).await
    }

    pub async fn finish_transport_write_sent(
        &mut self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        finish_transport_write_on(self.connection(), completion, TransportWriteResult::Sent).await
    }

    pub async fn finish_transport_write_backpressure(
        &mut self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        finish_transport_write_on(
            self.connection(),
            completion,
            TransportWriteResult::Backpressure,
        )
        .await
    }

    pub async fn finish_transport_write_failed(
        &mut self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        finish_transport_write_on(self.connection(), completion, TransportWriteResult::Failed).await
    }

    pub async fn finish_transport_write_unconfirmed(
        &mut self,
        completion: CompleteTransportWrite,
    ) -> Result<StoredOutboundDelivery, StoreError> {
        finish_transport_write_on(
            self.connection(),
            completion,
            TransportWriteResult::Unconfirmed,
        )
        .await
    }

    pub async fn record_transport_ack(
        &mut self,
        update: TransportAckUpdate,
    ) -> Result<StoredWireOutbox, StoreError> {
        record_transport_ack_on(self.connection(), update).await
    }

    pub async fn record_command_received_attempt(
        &mut self,
        update: CommandReceivedAttemptUpdate,
    ) -> Result<StoredDeliveryAttempt, StoreError> {
        record_command_received_attempt_on(self.connection(), update).await
    }

    pub async fn timeout_delivery_attempt(
        &mut self,
        timeout: DeliveryAttemptTimeout,
    ) -> Result<StoredDeliveryAttempt, StoreError> {
        timeout_delivery_attempt_on(self.connection(), timeout).await
    }

    pub async fn get_outbound_delivery(
        &mut self,
        message_id: &MessageId,
    ) -> Result<Option<StoredOutboundDelivery>, StoreError> {
        fetch_outbound_delivery(self.connection(), message_id).await
    }

    pub async fn get_delivery_attempt(
        &mut self,
        attempt_id: &str,
    ) -> Result<Option<StoredDeliveryAttempt>, StoreError> {
        fetch_delivery_attempt_by_id(self.connection(), attempt_id).await
    }
}

#[derive(Clone, Copy)]
enum TransportWriteResult {
    Sent,
    Backpressure,
    Failed,
    Unconfirmed,
}

async fn replace_active_session_on(
    connection: &mut SqliteConnection,
    session: NewSessionRecord,
) -> Result<SessionReplacement, StoreError> {
    validate_new_active_session(&session)?;
    let rows = sqlx::query(&format!(
        "SELECT {SESSION_COLUMNS} FROM execution_client_sessions \
         WHERE client_id = ? AND account_id = ? AND terminal_id IS ? AND status = 'ACTIVE'"
    ))
    .bind(session.client_id.as_str())
    .bind(session.account_id.as_str())
    .bind(session.terminal_id.as_ref().map(|id| id.as_str()))
    .fetch_all(&mut *connection)
    .await?;
    let mut active: Vec<_> = rows
        .into_iter()
        .map(session_from_row)
        .collect::<Result<_, _>>()?;
    if active.len() > 1 {
        return Err(StoreError::corrupt(
            "execution_client_session",
            session.account_id.to_string(),
            "more than one active session has the same identity",
        ));
    }

    if let Some(existing) = active.pop() {
        if existing.session_id == session.session_id {
            if same_registration(&existing, &session) {
                return Ok(SessionReplacement {
                    session: existing,
                    replaced_session: None,
                    unconfirmed_attempts: Vec::new(),
                });
            }
            return Err(StoreError::conflict(
                "execution_client_session",
                format!("session_id={}", session.session_id),
            ));
        }
        if session.updated_at < existing.updated_at {
            return Err(StoreError::StaleWrite {
                entity: "execution_client_session",
                key: existing.session_id.to_string(),
            });
        }
        settle_rejected_pending_attempts_on(connection, &existing.session_id, session.updated_at)
            .await?;
        cancel_session_pending_deliveries_on(
            connection,
            &existing.session_id,
            session.updated_at,
            "SESSION_REPLACED",
        )
        .await?;
        let unconfirmed_attempts = unconfirm_session_attempts_on(
            connection,
            &existing.session_id,
            session.updated_at,
            "SESSION_REPLACED",
        )
        .await?;
        transition_session_status_on(
            connection,
            &existing,
            SessionStatus::Stale,
            session.updated_at,
        )
        .await?;
        let replaced_session = fetch_session_by_id(&mut *connection, &existing.session_id)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "execution_client_session",
                    existing.session_id,
                    "session disappeared",
                )
            })?;
        let inserted = insert_session_on(connection, session).await?.into_record();
        return Ok(SessionReplacement {
            session: inserted,
            replaced_session: Some(replaced_session),
            unconfirmed_attempts,
        });
    }

    let inserted = insert_session_on(connection, session).await?.into_record();
    Ok(SessionReplacement {
        session: inserted,
        replaced_session: None,
        unconfirmed_attempts: Vec::new(),
    })
}

async fn update_session_heartbeat_on(
    connection: &mut SqliteConnection,
    update: SessionHeartbeatUpdate,
) -> Result<StoredSessionRecord, StoreError> {
    let current = required_session(connection, &update.session_id).await?;
    require_active_session_revision(&current, update.expected_revision)?;
    if update.updated_at < current.updated_at
        || update.heartbeat_at < current.connected_at
        || current
            .last_heartbeat_at
            .is_some_and(|last| update.heartbeat_at < last)
    {
        return Err(StoreError::StaleWrite {
            entity: "execution_client_session",
            key: update.session_id.to_string(),
        });
    }
    let last_time_sync_at = match update.last_time_sync_at {
        Some(synced_at) => {
            if synced_at < current.connected_at
                || current
                    .last_time_sync_at
                    .is_some_and(|last| synced_at < last)
            {
                return Err(StoreError::StaleWrite {
                    entity: "execution_client_session",
                    key: update.session_id.to_string(),
                });
            }
            Some(synced_at)
        }
        None => current.last_time_sync_at,
    };
    if update.clock_sync_status == ClockSyncStatus::Synced && last_time_sync_at.is_none() {
        return Err(StoreError::InvalidRecord {
            entity: "execution_client_session",
            key: update.session_id.to_string(),
            reason: "SYNCED heartbeat requires time-sync evidence".to_owned(),
        });
    }
    let next_revision = checked_increment("execution_client_sessions.revision", current.revision)?;
    let result = sqlx::query(
        "UPDATE execution_client_sessions \
         SET last_heartbeat_at = ?, last_time_sync_at = ?, clock_sync_status = ?, \
             revision = ?, updated_at = ? \
         WHERE session_id = ? AND status = 'ACTIVE' AND revision = ?",
    )
    .bind(update.heartbeat_at)
    .bind(last_time_sync_at)
    .bind(update.clock_sync_status.as_str())
    .bind(u64_to_i64(
        "execution_client_sessions.revision",
        next_revision,
    )?)
    .bind(update.updated_at)
    .bind(update.session_id.as_str())
    .bind(u64_to_i64(
        "execution_client_sessions.revision",
        update.expected_revision,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_session(&update.session_id));
    }
    required_session(connection, &update.session_id).await
}

async fn close_session_on(
    connection: &mut SqliteConnection,
    update: SessionStatusUpdate,
    status: SessionStatus,
) -> Result<SessionDisconnectOutcome, StoreError> {
    require_non_empty("session status delivery_error", &update.delivery_error)?;
    let current = required_session(connection, &update.session_id).await?;
    require_active_session_revision(&current, update.expected_revision)?;
    if update.changed_at < current.updated_at {
        return Err(stale_session(&update.session_id));
    }
    settle_rejected_pending_attempts_on(connection, &update.session_id, update.changed_at).await?;
    let unconfirmed_attempts = unconfirm_session_attempts_on(
        connection,
        &update.session_id,
        update.changed_at,
        &update.delivery_error,
    )
    .await?;
    cancel_session_pending_deliveries_on(
        connection,
        &update.session_id,
        update.changed_at,
        &update.delivery_error,
    )
    .await?;
    transition_session_status_on(connection, &current, status, update.changed_at).await?;
    Ok(SessionDisconnectOutcome {
        session: required_session(connection, &update.session_id).await?,
        unconfirmed_attempts,
    })
}

async fn transition_session_status_on(
    connection: &mut SqliteConnection,
    current: &StoredSessionRecord,
    status: SessionStatus,
    changed_at: i64,
) -> Result<(), StoreError> {
    let next_revision = checked_increment("execution_client_sessions.revision", current.revision)?;
    let result = sqlx::query(
        "UPDATE execution_client_sessions \
         SET status = ?, disconnected_at = ?, revision = ?, updated_at = ? \
         WHERE session_id = ? AND status = 'ACTIVE' AND revision = ?",
    )
    .bind(status.as_str())
    .bind(changed_at)
    .bind(u64_to_i64(
        "execution_client_sessions.revision",
        next_revision,
    )?)
    .bind(changed_at)
    .bind(current.session_id.as_str())
    .bind(u64_to_i64(
        "execution_client_sessions.revision",
        current.revision,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_session(&current.session_id));
    }
    Ok(())
}

async fn resolve_session_route_on(
    connection: &mut SqliteConnection,
    query: SessionRouteQuery,
) -> Result<SessionRouteResolution, StoreError> {
    let rows = sqlx::query(&format!(
        "SELECT {SESSION_COLUMNS} FROM execution_client_sessions \
         WHERE account_id = ? AND (? IS NULL OR client_id = ?) \
           AND (? IS NULL OR terminal_id = ?) AND status = 'ACTIVE' \
         ORDER BY connected_at DESC, session_id"
    ))
    .bind(query.account_id.as_str())
    .bind(query.client_id.as_ref().map(|id| id.as_str()))
    .bind(query.client_id.as_ref().map(|id| id.as_str()))
    .bind(query.terminal_id.as_ref().map(|id| id.as_str()))
    .bind(query.terminal_id.as_ref().map(|id| id.as_str()))
    .fetch_all(&mut *connection)
    .await?;
    let candidates: Vec<_> = rows
        .into_iter()
        .map(session_from_row)
        .collect::<Result<_, _>>()?;
    if candidates.is_empty() {
        return Ok(SessionRouteResolution::NoActiveSession);
    }
    if candidates.len() > 1 {
        return Ok(SessionRouteResolution::Ambiguous {
            candidate_count: candidates.len(),
        });
    }
    let fresh: Vec<_> = candidates
        .iter()
        .filter(|session| {
            session.last_heartbeat_at.unwrap_or(session.connected_at) >= query.fresh_after
        })
        .collect();
    if fresh.is_empty() {
        return Ok(SessionRouteResolution::Stale {
            candidate_count: candidates.len(),
        });
    }
    let ready: Vec<_> = fresh
        .into_iter()
        .filter(|session| {
            !query.require_synced_clock
                || session.clock_sync_status == Some(ClockSyncStatus::Synced)
        })
        .collect();
    match ready.as_slice() {
        [session] => Ok(SessionRouteResolution::Ready((**session).clone())),
        [] => Ok(SessionRouteResolution::ClockUnhealthy { candidate_count: 1 }),
        _ => Ok(SessionRouteResolution::Ambiguous {
            candidate_count: ready.len(),
        }),
    }
}

async fn reserve_outbound_sequence_on(
    connection: &mut SqliteConnection,
    request: ReserveOutboundSequence,
) -> Result<SequenceReservation, StoreError> {
    let session = required_session(connection, &request.session_id).await?;
    if session.status != SessionStatus::Active || session.revision != request.expected_revision {
        return Ok(SequenceReservation::SessionUnavailable);
    }
    if session.last_heartbeat_at.unwrap_or(session.connected_at) < request.fresh_after {
        return Ok(SequenceReservation::SessionUnavailable);
    }
    if request.reserved_at < session.updated_at {
        return Err(stale_session(&request.session_id));
    }

    if let Some(rejection) = validate_subject_route(connection, &session, &request).await? {
        return Ok(rejection);
    }

    if matches!(request.subject, DeliverySubject::ExecutionCommand(_)) {
        let inflight: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM command_delivery_attempts a \
             LEFT JOIN wire_outbox o ON o.message_id = a.message_id \
             WHERE a.session_id = ? AND a.command_id IS NOT NULL \
               AND a.status IN ('PENDING', 'SENT', 'UNCONFIRMED') \
               AND NOT (COALESCE(o.status, '') = 'FAILED' \
                 AND COALESCE(o.last_error, '') LIKE ?)",
        )
        .bind(session.session_id.as_str())
        .bind(format!("{TRANSPORT_ACK_REJECTED_PREFIX}%"))
        .fetch_one(&mut *connection)
        .await?;
        let inflight = u64::try_from(inflight).map_err(|_| {
            StoreError::corrupt(
                "command_delivery_attempt",
                session.session_id.to_string(),
                "negative inflight count",
            )
        })?;
        if inflight >= session.max_inflight_commands {
            return Ok(SequenceReservation::InflightLimit {
                session_id: session.session_id,
                inflight,
                limit: session.max_inflight_commands,
            });
        }
    }

    let sequence = checked_increment(
        "execution_client_sessions.last_outbound_sequence",
        session.last_outbound_sequence,
    )?;
    let revision = checked_increment("execution_client_sessions.revision", session.revision)?;
    let result = sqlx::query(
        "UPDATE execution_client_sessions \
         SET last_outbound_sequence = ?, revision = ?, updated_at = ? \
         WHERE session_id = ? AND status = 'ACTIVE' AND revision = ? \
           AND last_outbound_sequence = ?",
    )
    .bind(u64_to_i64(
        "execution_client_sessions.last_outbound_sequence",
        sequence,
    )?)
    .bind(u64_to_i64("execution_client_sessions.revision", revision)?)
    .bind(request.reserved_at)
    .bind(session.session_id.as_str())
    .bind(u64_to_i64(
        "execution_client_sessions.revision",
        session.revision,
    )?)
    .bind(u64_to_i64(
        "execution_client_sessions.last_outbound_sequence",
        session.last_outbound_sequence,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_session(&request.session_id));
    }

    Ok(SequenceReservation::Reserved(OutboundReservation {
        session_id: session.session_id,
        client_id: session.client_id,
        account_id: session.account_id,
        terminal_id: session.terminal_id,
        session_revision: revision,
        sequence,
        subject: request.subject,
    }))
}

async fn validate_subject_route(
    connection: &mut SqliteConnection,
    session: &StoredSessionRecord,
    request: &ReserveOutboundSequence,
) -> Result<Option<SequenceReservation>, StoreError> {
    match &request.subject {
        DeliverySubject::ExecutionCommand(command_id) => {
            if session.clock_sync_status != Some(ClockSyncStatus::Synced) {
                return Ok(Some(SequenceReservation::ClockUnhealthy));
            }
            let row = sqlx::query(
                "SELECT account_id, client_id, terminal_id, expires_at \
                 FROM execution_commands WHERE command_id = ?",
            )
            .bind(command_id.as_str())
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "execution_command",
                key: command_id.to_string(),
            })?;
            let account_id: String = row.try_get("account_id")?;
            let client_id: Option<String> = row.try_get("client_id")?;
            let terminal_id: Option<String> = row.try_get("terminal_id")?;
            let expires_at: i64 = row.try_get("expires_at")?;
            if account_id != session.account_id.as_str() {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "account_id",
                }));
            }
            if client_id
                .as_deref()
                .is_some_and(|value| value != session.client_id.as_str())
            {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "client_id",
                }));
            }
            if terminal_id
                .as_deref()
                .is_some_and(|value| session.terminal_id.as_deref() != Some(value))
            {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "terminal_id",
                }));
            }
            if request.reserved_at >= expires_at {
                return Ok(Some(SequenceReservation::Expired));
            }
        }
        DeliverySubject::ReconciliationRequest(request_id) => {
            let row = sqlx::query(
                "SELECT account_id, client_id, terminal_id \
                 FROM reconciliation_runs WHERE request_id = ?",
            )
            .bind(request_id.as_str())
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "reconciliation_run",
                key: request_id.to_string(),
            })?;
            let account_id: String = row.try_get("account_id")?;
            let client_id: Option<String> = row.try_get("client_id")?;
            let terminal_id: Option<String> = row.try_get("terminal_id")?;
            if account_id != session.account_id.as_str() {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "account_id",
                }));
            }
            if client_id
                .as_deref()
                .is_some_and(|value| value != session.client_id.as_str())
            {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "client_id",
                }));
            }
            if terminal_id
                .as_deref()
                .is_some_and(|value| session.terminal_id.as_deref() != Some(value))
            {
                return Ok(Some(SequenceReservation::IdentityMismatch {
                    field: "terminal_id",
                }));
            }
        }
    }
    Ok(None)
}

async fn enqueue_reserved_delivery_on(
    connection: &mut SqliteConnection,
    delivery: NewReservedDelivery,
) -> Result<StoredOutboundDelivery, StoreError> {
    require_non_empty("delivery attempt_id", &delivery.attempt_id)?;
    let session = required_session(connection, &delivery.reservation.session_id).await?;
    if session.status != SessionStatus::Active
        || session.revision != delivery.reservation.session_revision
        || session.last_outbound_sequence != delivery.reservation.sequence
        || delivery.created_at < session.updated_at
    {
        return Err(StoreError::StaleWrite {
            entity: "outbound_reservation",
            key: delivery.reservation.session_id.to_string(),
        });
    }
    if session.client_id != delivery.reservation.client_id
        || session.account_id != delivery.reservation.account_id
        || session.terminal_id != delivery.reservation.terminal_id
    {
        return Err(StoreError::conflict(
            "outbound_reservation",
            delivery.reservation.session_id.to_string(),
        ));
    }
    let expected_type = match delivery.reservation.subject {
        DeliverySubject::ExecutionCommand(_) => "execution.command",
        DeliverySubject::ReconciliationRequest(_) => "reconciliation.request",
    };
    if delivery.message_type != expected_type {
        return Err(StoreError::InvalidRecord {
            entity: "outbound_delivery",
            key: delivery.message_id.to_string(),
            reason: format!(
                "message_type {:?} does not match delivery subject",
                delivery.message_type
            ),
        });
    }

    let outbox = NewWireOutbox {
        message_id: delivery.message_id.clone(),
        session_id: Some(delivery.reservation.session_id.clone()),
        message_type: delivery.message_type,
        sequence: Some(delivery.reservation.sequence),
        command_id: delivery.reservation.subject.command_id().cloned(),
        request_id: delivery.reservation.subject.request_id().cloned(),
        payload: delivery.envelope,
        status: WireOutboxStatus::Pending,
        created_at: delivery.created_at,
        updated_at: delivery.created_at,
        sent_at: None,
        acked_at: None,
        last_error: None,
    };
    let proposed_outbox: StoredWireOutbox = outbox.clone().into();
    validate_outbox_parent_payload(connection, &proposed_outbox).await?;
    let outbox_outcome = enqueue_wire_outbox_on(connection, outbox).await?;
    let attempt_outcome = record_delivery_attempt_on(
        connection,
        NewDeliveryAttempt {
            attempt_id: delivery.attempt_id,
            subject: delivery.reservation.subject,
            session_id: Some(delivery.reservation.session_id),
            message_id: Some(delivery.message_id),
            request_payload: None,
            status: CommandDeliveryAttemptStatus::Pending,
            attempted_at: delivery.created_at,
            acked_at: None,
            error: None,
            updated_at: delivery.created_at,
        },
    )
    .await?;
    if outbox_outcome.was_inserted() != attempt_outcome.was_inserted() {
        return Err(StoreError::conflict(
            "outbound_delivery",
            outbox_outcome.record().message_id.to_string(),
        ));
    }
    Ok(StoredOutboundDelivery {
        outbox: outbox_outcome.into_record(),
        attempt: attempt_outcome.into_record(),
    })
}

async fn record_delivery_attempt_on(
    connection: &mut SqliteConnection,
    attempt: NewDeliveryAttempt,
) -> Result<WriteOutcome<StoredDeliveryAttempt>, StoreError> {
    validate_new_attempt(&attempt)?;
    let result = sqlx::query(
        "INSERT INTO command_delivery_attempts (\
             attempt_id, command_id, request_id, session_id, message_id, request_payload_json, \
             request_payload_hash, status, attempted_at, acked_at, error, revision, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?) ON CONFLICT DO NOTHING",
    )
    .bind(&attempt.attempt_id)
    .bind(attempt.subject.command_id().map(CommandId::as_str))
    .bind(attempt.subject.request_id().map(RequestId::as_str))
    .bind(attempt.session_id.as_ref().map(SessionId::as_str))
    .bind(attempt.message_id.as_ref().map(MessageId::as_str))
    .bind(attempt.request_payload.as_ref().map(CanonicalJson::as_str))
    .bind(
        attempt
            .request_payload
            .as_ref()
            .map(CanonicalJson::sha256_hex),
    )
    .bind(attempt.status.as_str())
    .bind(attempt.attempted_at)
    .bind(attempt.acked_at)
    .bind(&attempt.error)
    .bind(attempt.updated_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() == 1 {
        return Ok(WriteOutcome::Inserted(
            fetch_delivery_attempt_by_id(&mut *connection, &attempt.attempt_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "command_delivery_attempt",
                        &attempt.attempt_id,
                        "attempt disappeared after insert",
                    )
                })?,
        ));
    }
    let existing = fetch_attempt_conflicts(connection, &attempt).await?;
    if existing.len() == 1 && same_attempt(&existing[0], &attempt) {
        Ok(WriteOutcome::Duplicate(
            existing.into_iter().next().expect("length checked"),
        ))
    } else {
        Err(StoreError::conflict(
            "command_delivery_attempt",
            format!("attempt_id={}", attempt.attempt_id),
        ))
    }
}

async fn claim_outbox_on(
    connection: &mut SqliteConnection,
    claim: ClaimWireOutbox,
) -> Result<OutboxClaimOutcome, StoreError> {
    let outbox = required_outbox(connection, &claim.message_id).await?;
    let attempt = required_attempt_by_message(connection, &claim.message_id).await?;
    let session_id = outbox.session_id.as_ref().ok_or_else(|| {
        StoreError::corrupt(
            "outbound_delivery",
            claim.message_id.to_string(),
            "delivery outbox has no session",
        )
    })?;
    let session = required_session(connection, session_id).await?;
    if outbox.status != WireOutboxStatus::Pending
        || attempt.status != CommandDeliveryAttemptStatus::Pending
        || outbox.revision != claim.expected_outbox_revision
        || attempt.revision != claim.expected_attempt_revision
    {
        return Err(stale_outbound(&claim.message_id));
    }
    require_monotonic_transition_time(&outbox, &attempt, claim.claimed_at)?;
    if session.status != SessionStatus::Active
        || session.last_heartbeat_at.unwrap_or(session.connected_at) < claim.fresh_after
    {
        let delivery = cancel_claimed_delivery(
            connection,
            &outbox,
            &attempt,
            claim.claimed_at,
            CommandDeliveryAttemptStatus::NoActiveSession,
            DELIVERY_ERROR_SESSION_UNAVAILABLE,
        )
        .await?;
        return Ok(OutboxClaimOutcome::SessionUnavailable(delivery));
    }
    let command_subject = matches!(attempt.subject, DeliverySubject::ExecutionCommand(_));
    if command_subject && !claim.require_synced_clock {
        return Err(invalid_outbound(
            &claim.message_id,
            "execution command claim must require a synced clock",
        ));
    }
    if claim.require_synced_clock && session.clock_sync_status != Some(ClockSyncStatus::Synced) {
        let delivery = cancel_claimed_delivery(
            connection,
            &outbox,
            &attempt,
            claim.claimed_at,
            CommandDeliveryAttemptStatus::NoActiveSession,
            DELIVERY_ERROR_CLOCK_UNHEALTHY,
        )
        .await?;
        return Ok(OutboxClaimOutcome::ClockUnhealthy(delivery));
    }
    if let DeliverySubject::ExecutionCommand(command_id) = &attempt.subject {
        let expires_at: i64 =
            sqlx::query_scalar("SELECT expires_at FROM execution_commands WHERE command_id = ?")
                .bind(command_id.as_str())
                .fetch_optional(&mut *connection)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "execution_command",
                    key: command_id.to_string(),
                })?;
        if claim.claimed_at >= expires_at {
            return Ok(OutboxClaimOutcome::Expired(
                cancel_claimed_delivery(
                    connection,
                    &outbox,
                    &attempt,
                    claim.claimed_at,
                    CommandDeliveryAttemptStatus::Cancelled,
                    DELIVERY_ERROR_COMMAND_EXPIRED,
                )
                .await?,
            ));
        }
    }
    update_outbox_state(
        connection,
        &outbox,
        WireOutboxStatus::WriteStarted,
        claim.claimed_at,
        None,
        None,
        None,
    )
    .await?;
    update_attempt_state(
        connection,
        &attempt,
        CommandDeliveryAttemptStatus::Pending,
        claim.claimed_at,
        None,
        None,
    )
    .await?;
    Ok(OutboxClaimOutcome::Claimed(
        required_outbound_delivery(connection, &claim.message_id).await?,
    ))
}

async fn cancel_claimed_delivery(
    connection: &mut SqliteConnection,
    outbox: &StoredWireOutbox,
    attempt: &StoredDeliveryAttempt,
    cancelled_at: i64,
    attempt_status: CommandDeliveryAttemptStatus,
    reason: &str,
) -> Result<StoredOutboundDelivery, StoreError> {
    update_outbox_state(
        connection,
        outbox,
        WireOutboxStatus::Cancelled,
        cancelled_at,
        None,
        None,
        Some(reason),
    )
    .await?;
    update_attempt_state(
        connection,
        attempt,
        attempt_status,
        cancelled_at,
        None,
        Some(reason),
    )
    .await?;
    required_outbound_delivery(connection, &outbox.message_id).await
}

async fn finish_transport_write_on(
    connection: &mut SqliteConnection,
    completion: CompleteTransportWrite,
    result: TransportWriteResult,
) -> Result<StoredOutboundDelivery, StoreError> {
    let outbox = required_outbox(connection, &completion.message_id).await?;
    let attempt = required_attempt_by_message(connection, &completion.message_id).await?;
    let error = completion.error.as_deref();
    match result {
        TransportWriteResult::Sent if error.is_some() => {
            return Err(invalid_outbound(
                &completion.message_id,
                "sent write must not include error",
            ));
        }
        TransportWriteResult::Sent => {}
        _ => require_non_empty("transport write error", error.unwrap_or_default())?,
    }

    let transport_rejected = outbox.status == WireOutboxStatus::Failed
        && outbox
            .last_error
            .as_deref()
            .is_some_and(|reason| reason.starts_with(TRANSPORT_ACK_REJECTED_PREFIX));
    if transport_rejected && attempt.status == CommandDeliveryAttemptStatus::Pending {
        if attempt.revision != completion.expected_attempt_revision {
            return Err(stale_outbound(&completion.message_id));
        }
        update_attempt_state(
            connection,
            &attempt,
            CommandDeliveryAttemptStatus::Sent,
            completion.completed_at.max(attempt.updated_at),
            None,
            None,
        )
        .await?;
        return required_outbound_delivery(connection, &completion.message_id).await;
    }

    if attempt.status == CommandDeliveryAttemptStatus::Unconfirmed
        || (outbox.status == WireOutboxStatus::Failed
            && attempt.status == CommandDeliveryAttemptStatus::Failed)
        || (transport_rejected
            && matches!(
                attempt.status,
                CommandDeliveryAttemptStatus::Sent | CommandDeliveryAttemptStatus::Acked
            ))
    {
        return Ok(StoredOutboundDelivery { outbox, attempt });
    }

    let transport_acked = outbox.status == WireOutboxStatus::Acked;
    let command_received = attempt.status == CommandDeliveryAttemptStatus::Acked;
    if transport_acked || command_received {
        if !matches!(
            outbox.status,
            WireOutboxStatus::WriteStarted
                | WireOutboxStatus::Sent
                | WireOutboxStatus::Acked
                | WireOutboxStatus::Failed
        ) {
            return Err(stale_outbound(&completion.message_id));
        }
        if outbox.status == WireOutboxStatus::WriteStarted && !transport_acked {
            update_outbox_state(
                connection,
                &outbox,
                WireOutboxStatus::Sent,
                completion.completed_at.max(outbox.updated_at),
                Some(completion.completed_at),
                None,
                None,
            )
            .await?;
        }
        if attempt.status == CommandDeliveryAttemptStatus::Pending
            || (transport_acked
                && matches!(
                    attempt.status,
                    CommandDeliveryAttemptStatus::Failed
                        | CommandDeliveryAttemptStatus::Backpressure
                ))
        {
            if attempt.status == CommandDeliveryAttemptStatus::Pending
                && attempt.revision != completion.expected_attempt_revision
            {
                return Err(stale_outbound(&completion.message_id));
            }
            update_attempt_state(
                connection,
                &attempt,
                CommandDeliveryAttemptStatus::Sent,
                completion.completed_at.max(attempt.updated_at),
                None,
                None,
            )
            .await?;
        }
        return required_outbound_delivery(connection, &completion.message_id).await;
    }

    if outbox.status != WireOutboxStatus::WriteStarted
        || attempt.status != CommandDeliveryAttemptStatus::Pending
        || outbox.revision != completion.expected_outbox_revision
        || attempt.revision != completion.expected_attempt_revision
    {
        return Err(stale_outbound(&completion.message_id));
    }
    require_monotonic_transition_time(&outbox, &attempt, completion.completed_at)?;
    let (outbox_status, attempt_status, sent_at) = match result {
        TransportWriteResult::Sent => (
            WireOutboxStatus::Sent,
            CommandDeliveryAttemptStatus::Sent,
            Some(completion.completed_at),
        ),
        TransportWriteResult::Backpressure => (
            WireOutboxStatus::Failed,
            CommandDeliveryAttemptStatus::Backpressure,
            None,
        ),
        TransportWriteResult::Failed => (
            WireOutboxStatus::Failed,
            CommandDeliveryAttemptStatus::Failed,
            None,
        ),
        TransportWriteResult::Unconfirmed => (
            WireOutboxStatus::WriteStarted,
            CommandDeliveryAttemptStatus::Unconfirmed,
            None,
        ),
    };
    update_outbox_state(
        connection,
        &outbox,
        outbox_status,
        completion.completed_at,
        sent_at,
        None,
        completion.error.as_deref(),
    )
    .await?;
    update_attempt_state(
        connection,
        &attempt,
        attempt_status,
        completion.completed_at,
        None,
        completion.error.as_deref(),
    )
    .await?;
    required_outbound_delivery(connection, &completion.message_id).await
}

async fn record_transport_ack_on(
    connection: &mut SqliteConnection,
    update: TransportAckUpdate,
) -> Result<StoredWireOutbox, StoreError> {
    let outbox = required_outbox(connection, &update.message_id).await?;
    if outbox.session_id.as_ref() != Some(&update.session_id)
        || outbox.message_type != update.message_type
    {
        return Err(StoreError::conflict(
            "transport_ack",
            update.message_id.to_string(),
        ));
    }
    let transition_at = update.acked_at.max(outbox.updated_at);
    match update.status {
        TransportAckStatus::Accepted | TransportAckStatus::Duplicate => {
            if outbox.status == WireOutboxStatus::Acked {
                return Ok(outbox);
            }
            if outbox
                .last_error
                .as_deref()
                .is_some_and(|reason| reason.starts_with(TRANSPORT_ACK_REJECTED_PREFIX))
            {
                return Err(StoreError::conflict(
                    "transport_ack",
                    update.message_id.to_string(),
                ));
            }
            if !matches!(
                outbox.status,
                WireOutboxStatus::WriteStarted | WireOutboxStatus::Sent | WireOutboxStatus::Failed
            ) {
                return Err(stale_outbound(&update.message_id));
            }
            let sent_at = outbox.sent_at.unwrap_or(update.acked_at);
            let acked_at = update.acked_at.max(sent_at);
            update_outbox_state(
                connection,
                &outbox,
                WireOutboxStatus::Acked,
                transition_at.max(acked_at),
                Some(sent_at),
                Some(acked_at),
                None,
            )
            .await?;
        }
        TransportAckStatus::Rejected => {
            let reason = update.reason.as_deref().ok_or_else(|| {
                invalid_outbound(&update.message_id, "rejected transport ack requires reason")
            })?;
            require_non_empty("transport ack rejection reason", reason)?;
            let durable_reason = format!("{TRANSPORT_ACK_REJECTED_PREFIX}{reason}");
            if outbox.status == WireOutboxStatus::Acked {
                return Err(StoreError::conflict(
                    "transport_ack",
                    update.message_id.to_string(),
                ));
            }
            if outbox
                .last_error
                .as_deref()
                .is_some_and(|existing| existing.starts_with(TRANSPORT_ACK_REJECTED_PREFIX))
            {
                return if outbox.last_error.as_deref() == Some(durable_reason.as_str()) {
                    Ok(outbox)
                } else {
                    Err(StoreError::conflict(
                        "transport_ack",
                        update.message_id.to_string(),
                    ))
                };
            }
            if !matches!(
                outbox.status,
                WireOutboxStatus::WriteStarted | WireOutboxStatus::Sent | WireOutboxStatus::Failed
            ) {
                return Err(stale_outbound(&update.message_id));
            }
            update_outbox_state(
                connection,
                &outbox,
                WireOutboxStatus::Failed,
                transition_at,
                Some(outbox.sent_at.unwrap_or(update.acked_at)),
                None,
                Some(&durable_reason),
            )
            .await?;
        }
    }
    required_outbox(connection, &update.message_id).await
}

async fn record_command_received_attempt_on(
    connection: &mut SqliteConnection,
    update: CommandReceivedAttemptUpdate,
) -> Result<StoredDeliveryAttempt, StoreError> {
    let attempt = required_attempt_by_message(connection, &update.source_message_id).await?;
    if attempt.session_id.as_ref() != Some(&update.session_id)
        || attempt.subject != DeliverySubject::ExecutionCommand(update.command_id)
    {
        return Err(StoreError::conflict(
            "command_received_attempt",
            update.source_message_id.to_string(),
        ));
    }
    if attempt.status == CommandDeliveryAttemptStatus::Acked {
        return Ok(attempt);
    }
    if !matches!(
        attempt.status,
        CommandDeliveryAttemptStatus::Pending
            | CommandDeliveryAttemptStatus::Sent
            | CommandDeliveryAttemptStatus::Unconfirmed
            | CommandDeliveryAttemptStatus::Failed
            | CommandDeliveryAttemptStatus::Backpressure
    ) {
        return Err(StoreError::StaleWrite {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id,
        });
    }
    if update.received_at < attempt.attempted_at {
        return Err(StoreError::InvalidRecord {
            entity: "command_received_attempt",
            key: update.source_message_id.to_string(),
            reason: "received_at precedes attempted_at".to_owned(),
        });
    }
    update_attempt_state(
        connection,
        &attempt,
        CommandDeliveryAttemptStatus::Acked,
        update.received_at.max(attempt.updated_at),
        Some(update.received_at),
        None,
    )
    .await?;
    required_attempt_by_message(connection, &update.source_message_id).await
}

async fn timeout_delivery_attempt_on(
    connection: &mut SqliteConnection,
    timeout: DeliveryAttemptTimeout,
) -> Result<StoredDeliveryAttempt, StoreError> {
    require_non_empty("delivery timeout error", &timeout.error)?;
    let attempt = fetch_delivery_attempt_by_id(&mut *connection, &timeout.attempt_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "command_delivery_attempt",
            key: timeout.attempt_id.clone(),
        })?;
    if !matches!(attempt.subject, DeliverySubject::ExecutionCommand(_)) {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: timeout.attempt_id,
            reason: "command.received timeout only applies to execution commands".to_owned(),
        });
    }
    if matches!(
        attempt.status,
        CommandDeliveryAttemptStatus::Unconfirmed | CommandDeliveryAttemptStatus::Acked
    ) {
        return Ok(attempt);
    }
    if attempt.status == CommandDeliveryAttemptStatus::Sent {
        let message_id = attempt.message_id.as_ref().ok_or_else(|| {
            StoreError::corrupt(
                "command_delivery_attempt",
                &attempt.attempt_id,
                "SENT attempt has no message_id",
            )
        })?;
        let outbox = required_outbox(connection, message_id).await?;
        if outbox.status == WireOutboxStatus::Failed
            && outbox
                .last_error
                .as_deref()
                .is_some_and(|reason| reason.starts_with(TRANSPORT_ACK_REJECTED_PREFIX))
        {
            return Ok(attempt);
        }
    }
    if attempt.status != CommandDeliveryAttemptStatus::Sent
        || attempt.revision != timeout.expected_revision
        || timeout.timed_out_at < attempt.updated_at
    {
        return Err(StoreError::StaleWrite {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id,
        });
    }
    update_attempt_state(
        connection,
        &attempt,
        CommandDeliveryAttemptStatus::Unconfirmed,
        timeout.timed_out_at,
        None,
        Some(&timeout.error),
    )
    .await?;
    fetch_delivery_attempt_by_id(&mut *connection, &timeout.attempt_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "command_delivery_attempt",
                timeout.attempt_id,
                "attempt disappeared",
            )
        })
}

async fn fence_interrupted_writes_on(
    connection: &mut SqliteConnection,
    fenced_at: i64,
    error: &str,
) -> Result<DeliveryStartupFenceReport, StoreError> {
    require_non_empty("startup fence error", error)?;
    let session_rows = sqlx::query(&format!(
        "SELECT {SESSION_COLUMNS} FROM execution_client_sessions \
         WHERE status = 'ACTIVE' ORDER BY connected_at, session_id"
    ))
    .fetch_all(&mut *connection)
    .await?;
    let active_sessions: Vec<_> = session_rows
        .into_iter()
        .map(session_from_row)
        .collect::<Result<_, _>>()?;
    let mut attempts_unconfirmed = 0_u64;
    let mut attempts_cancelled = 0_u64;
    let mut attempts_rejected = 0_u64;
    for session in &active_sessions {
        if fenced_at < session.updated_at {
            return Err(stale_session(&session.session_id));
        }
        attempts_rejected = attempts_rejected
            .checked_add(
                settle_rejected_pending_attempts_on(connection, &session.session_id, fenced_at)
                    .await?,
            )
            .ok_or(StoreError::InvalidInteger {
                field: "startup_fence.attempts_rejected",
                value: u64::MAX,
            })?;
        attempts_cancelled = attempts_cancelled
            .checked_add(
                cancel_session_pending_deliveries_on(
                    connection,
                    &session.session_id,
                    fenced_at,
                    error,
                )
                .await?,
            )
            .ok_or(StoreError::InvalidInteger {
                field: "startup_fence.attempts_cancelled",
                value: u64::MAX,
            })?;
        let newly_unconfirmed =
            unconfirm_session_attempts_on(connection, &session.session_id, fenced_at, error)
                .await?;
        attempts_unconfirmed = attempts_unconfirmed
            .checked_add(u64::try_from(newly_unconfirmed.len()).map_err(|_| {
                StoreError::InvalidInteger {
                    field: "startup_fence.attempts_unconfirmed",
                    value: u64::MAX,
                }
            })?)
            .ok_or(StoreError::InvalidInteger {
                field: "startup_fence.attempts_unconfirmed",
                value: u64::MAX,
            })?;
        transition_session_status_on(connection, session, SessionStatus::Stale, fenced_at).await?;
    }
    let message_ids: Vec<String> = sqlx::query_scalar(
        "SELECT message_id FROM wire_outbox WHERE status = 'WRITE_STARTED' \
         ORDER BY created_at, message_id",
    )
    .fetch_all(&mut *connection)
    .await?;
    for raw_message_id in &message_ids {
        let message_id = MessageId::from(raw_message_id.clone());
        let outbox = required_outbox(connection, &message_id).await?;
        if fenced_at < outbox.updated_at {
            return Err(stale_outbound(&message_id));
        }
        update_outbox_state(
            connection,
            &outbox,
            WireOutboxStatus::WriteStarted,
            fenced_at,
            outbox.sent_at,
            outbox.acked_at,
            Some(error),
        )
        .await?;
        if let Some(attempt) =
            fetch_delivery_attempt_by_message(&mut *connection, &message_id).await?
        {
            if matches!(
                attempt.status,
                CommandDeliveryAttemptStatus::Pending | CommandDeliveryAttemptStatus::Sent
            ) {
                update_attempt_state(
                    connection,
                    &attempt,
                    CommandDeliveryAttemptStatus::Unconfirmed,
                    fenced_at,
                    None,
                    Some(error),
                )
                .await?;
                attempts_unconfirmed =
                    attempts_unconfirmed
                        .checked_add(1)
                        .ok_or(StoreError::InvalidInteger {
                            field: "startup_fence.attempts_unconfirmed",
                            value: u64::MAX,
                        })?;
            }
        }
    }
    Ok(DeliveryStartupFenceReport {
        sessions_staled: u64::try_from(active_sessions.len()).map_err(|_| {
            StoreError::InvalidInteger {
                field: "startup_fence.sessions_staled",
                value: u64::MAX,
            }
        })?,
        outboxes_fenced: u64::try_from(message_ids.len()).map_err(|_| {
            StoreError::InvalidInteger {
                field: "startup_fence.outboxes_fenced",
                value: u64::MAX,
            }
        })?,
        attempts_unconfirmed,
        attempts_cancelled,
        attempts_rejected,
    })
}

async fn unconfirm_session_attempts_on(
    connection: &mut SqliteConnection,
    session_id: &SessionId,
    changed_at: i64,
    error: &str,
) -> Result<Vec<StoredDeliveryAttempt>, StoreError> {
    let rows = sqlx::query(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts a \
         WHERE a.session_id = ? AND ((a.status = 'SENT' AND EXISTS (\
             SELECT 1 FROM wire_outbox sent_outbox \
             WHERE sent_outbox.message_id = a.message_id AND sent_outbox.status <> 'FAILED'\
         )) OR (a.status = 'PENDING' AND EXISTS (\
             SELECT 1 FROM wire_outbox o \
             WHERE o.message_id = a.message_id \
               AND o.status IN ('WRITE_STARTED', 'SENT', 'ACKED')\
         ))) ORDER BY a.attempted_at, a.attempt_id"
    ))
    .bind(session_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let attempts = parse_and_validate_attempt_rows(&mut *connection, rows).await?;
    let mut updated = Vec::with_capacity(attempts.len());
    for attempt in attempts {
        if changed_at < attempt.updated_at {
            return Err(StoreError::StaleWrite {
                entity: "command_delivery_attempt",
                key: attempt.attempt_id,
            });
        }
        update_attempt_state(
            connection,
            &attempt,
            CommandDeliveryAttemptStatus::Unconfirmed,
            changed_at,
            None,
            Some(error),
        )
        .await?;
        updated.push(
            fetch_delivery_attempt_by_id(&mut *connection, &attempt.attempt_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "command_delivery_attempt",
                        &attempt.attempt_id,
                        "attempt disappeared",
                    )
                })?,
        );
    }
    Ok(updated)
}

async fn cancel_session_pending_deliveries_on(
    connection: &mut SqliteConnection,
    session_id: &SessionId,
    changed_at: i64,
    error: &str,
) -> Result<u64, StoreError> {
    let message_ids: Vec<String> = sqlx::query_scalar(
        "SELECT o.message_id FROM wire_outbox o \
         JOIN command_delivery_attempts a ON a.message_id = o.message_id \
         WHERE o.session_id = ? AND o.status = 'PENDING' AND a.status = 'PENDING' \
         ORDER BY o.sequence, o.message_id",
    )
    .bind(session_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let count = u64::try_from(message_ids.len()).map_err(|_| StoreError::InvalidInteger {
        field: "session_pending_deliveries.count",
        value: u64::MAX,
    })?;
    for raw_message_id in message_ids {
        let message_id = MessageId::from(raw_message_id);
        let outbox = required_outbox(connection, &message_id).await?;
        let attempt = required_attempt_by_message(connection, &message_id).await?;
        require_monotonic_transition_time(&outbox, &attempt, changed_at)?;
        update_outbox_state(
            connection,
            &outbox,
            WireOutboxStatus::Cancelled,
            changed_at,
            None,
            None,
            Some(error),
        )
        .await?;
        update_attempt_state(
            connection,
            &attempt,
            CommandDeliveryAttemptStatus::Cancelled,
            changed_at,
            None,
            Some(error),
        )
        .await?;
    }
    Ok(count)
}

async fn settle_rejected_pending_attempts_on(
    connection: &mut SqliteConnection,
    session_id: &SessionId,
    changed_at: i64,
) -> Result<u64, StoreError> {
    let message_ids: Vec<String> = sqlx::query_scalar(
        "SELECT o.message_id FROM wire_outbox o \
         JOIN command_delivery_attempts a ON a.message_id = o.message_id \
         WHERE o.session_id = ? AND o.status = 'FAILED' AND a.status = 'PENDING' \
           AND COALESCE(o.last_error, '') LIKE ? \
         ORDER BY o.sequence, o.message_id",
    )
    .bind(session_id.as_str())
    .bind(format!("{TRANSPORT_ACK_REJECTED_PREFIX}%"))
    .fetch_all(&mut *connection)
    .await?;
    let count = u64::try_from(message_ids.len()).map_err(|_| StoreError::InvalidInteger {
        field: "session_failed_deliveries.count",
        value: u64::MAX,
    })?;
    for raw_message_id in message_ids {
        let message_id = MessageId::from(raw_message_id);
        let outbox = required_outbox(connection, &message_id).await?;
        let attempt = required_attempt_by_message(connection, &message_id).await?;
        let reason = outbox.last_error.as_deref().ok_or_else(|| {
            StoreError::corrupt(
                "outbound_delivery",
                message_id.to_string(),
                "transport-rejected outbox has no rejection reason",
            )
        })?;
        if !reason.starts_with(TRANSPORT_ACK_REJECTED_PREFIX) {
            return Err(StoreError::corrupt(
                "outbound_delivery",
                message_id.to_string(),
                "transport-rejected outbox has an invalid rejection reason",
            ));
        }
        update_attempt_state(
            connection,
            &attempt,
            CommandDeliveryAttemptStatus::Sent,
            changed_at.max(attempt.updated_at),
            None,
            None,
        )
        .await?;
    }
    Ok(count)
}

async fn update_outbox_state(
    connection: &mut SqliteConnection,
    current: &StoredWireOutbox,
    status: WireOutboxStatus,
    updated_at: i64,
    sent_at: Option<i64>,
    acked_at: Option<i64>,
    error: Option<&str>,
) -> Result<(), StoreError> {
    let next_revision = checked_increment("wire_outbox.revision", current.revision)?;
    let result = sqlx::query(
        "UPDATE wire_outbox SET status = ?, revision = ?, updated_at = ?, sent_at = ?, \
             acked_at = ?, last_error = ? WHERE message_id = ? AND revision = ?",
    )
    .bind(status.as_str())
    .bind(u64_to_i64("wire_outbox.revision", next_revision)?)
    .bind(updated_at)
    .bind(sent_at)
    .bind(acked_at)
    .bind(error)
    .bind(current.message_id.as_str())
    .bind(u64_to_i64("wire_outbox.revision", current.revision)?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_outbound(&current.message_id));
    }
    Ok(())
}

async fn update_attempt_state(
    connection: &mut SqliteConnection,
    current: &StoredDeliveryAttempt,
    status: CommandDeliveryAttemptStatus,
    updated_at: i64,
    acked_at: Option<i64>,
    error: Option<&str>,
) -> Result<(), StoreError> {
    let next_revision = checked_increment("command_delivery_attempts.revision", current.revision)?;
    let result = sqlx::query(
        "UPDATE command_delivery_attempts \
         SET status = ?, revision = ?, updated_at = ?, acked_at = ?, error = ? \
         WHERE attempt_id = ? AND revision = ?",
    )
    .bind(status.as_str())
    .bind(u64_to_i64(
        "command_delivery_attempts.revision",
        next_revision,
    )?)
    .bind(updated_at)
    .bind(acked_at)
    .bind(error)
    .bind(&current.attempt_id)
    .bind(u64_to_i64(
        "command_delivery_attempts.revision",
        current.revision,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "command_delivery_attempt",
            key: current.attempt_id.clone(),
        });
    }
    Ok(())
}

async fn fetch_delivery_attempt_by_id(
    connection: &mut SqliteConnection,
    attempt_id: &str,
) -> Result<Option<StoredDeliveryAttempt>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts WHERE attempt_id = ?"
    ))
    .bind(attempt_id)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let attempt = delivery_attempt_from_row(row)?;
    validate_stored_attempt(connection, &attempt).await?;
    Ok(Some(attempt))
}

async fn fetch_delivery_attempt_by_message(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<Option<StoredDeliveryAttempt>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts WHERE message_id = ?"
    ))
    .bind(message_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let attempt = delivery_attempt_from_row(row)?;
    validate_stored_attempt(connection, &attempt).await?;
    Ok(Some(attempt))
}

async fn parse_and_validate_attempt_rows(
    connection: &mut SqliteConnection,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Result<Vec<StoredDeliveryAttempt>, StoreError> {
    let mut attempts = Vec::with_capacity(rows.len());
    for row in rows {
        let attempt = delivery_attempt_from_row(row)?;
        validate_stored_attempt(connection, &attempt).await?;
        attempts.push(attempt);
    }
    Ok(attempts)
}

fn delivery_attempt_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredDeliveryAttempt, StoreError> {
    let attempt_id: String = row.try_get("attempt_id")?;
    let command_id: Option<String> = row.try_get("command_id")?;
    let request_id: Option<String> = row.try_get("request_id")?;
    let subject = match (command_id, request_id) {
        (Some(command_id), None) => DeliverySubject::ExecutionCommand(CommandId::from(command_id)),
        (None, Some(request_id)) => {
            DeliverySubject::ReconciliationRequest(RequestId::from(request_id))
        }
        _ => {
            return Err(StoreError::corrupt(
                "command_delivery_attempt",
                &attempt_id,
                "exactly one delivery subject must be present",
            ));
        }
    };
    let status: CommandDeliveryAttemptStatus = parse_enum(
        "command_delivery_attempt",
        &attempt_id,
        "status",
        row.try_get("status")?,
    )?;
    let attempted_at: i64 = row.try_get("attempted_at")?;
    let acked_at: Option<i64> = row.try_get("acked_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    if updated_at < attempted_at
        || (status == CommandDeliveryAttemptStatus::Acked) != acked_at.is_some()
    {
        return Err(StoreError::corrupt(
            "command_delivery_attempt",
            &attempt_id,
            "attempt timestamps do not match status",
        ));
    }
    if status == CommandDeliveryAttemptStatus::Acked
        && !matches!(subject, DeliverySubject::ExecutionCommand(_))
    {
        return Err(StoreError::corrupt(
            "command_delivery_attempt",
            &attempt_id,
            "ACKED is only valid for execution command receipt",
        ));
    }
    let error: Option<String> = row.try_get("error")?;
    if status == CommandDeliveryAttemptStatus::Unconfirmed
        && error.as_deref().is_none_or(|value| value.trim().is_empty())
    {
        return Err(StoreError::corrupt(
            "command_delivery_attempt",
            &attempt_id,
            "UNCONFIRMED requires a non-empty error",
        ));
    }
    let request_payload_json: Option<String> = row.try_get("request_payload_json")?;
    let request_payload_hash: Option<String> = row.try_get("request_payload_hash")?;
    let request_payload = match (request_payload_json, request_payload_hash) {
        (Some(payload), Some(hash)) => Some(CanonicalJson::from_stored(
            "command_delivery_attempt",
            &attempt_id,
            payload,
            hash,
        )?),
        (None, None) => None,
        _ => {
            return Err(StoreError::corrupt(
                "command_delivery_attempt",
                &attempt_id,
                "request payload and hash must be present together",
            ));
        }
    };
    Ok(StoredDeliveryAttempt {
        attempt_id: attempt_id.clone(),
        subject,
        session_id: row
            .try_get::<Option<String>, _>("session_id")?
            .map(SessionId::from),
        message_id: row
            .try_get::<Option<String>, _>("message_id")?
            .map(MessageId::from),
        request_payload,
        status,
        attempted_at,
        acked_at,
        error,
        revision: non_negative_i64(
            "command_delivery_attempt",
            &attempt_id,
            "revision",
            row.try_get("revision")?,
        )?,
        updated_at,
    })
}

async fn validate_stored_attempt(
    connection: &mut SqliteConnection,
    attempt: &StoredDeliveryAttempt,
) -> Result<(), StoreError> {
    match (&attempt.session_id, &attempt.message_id) {
        (Some(session_id), Some(message_id)) => {
            let outbox = fetch_wire_outbox_by_id(&mut *connection, message_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt(
                        "command_delivery_attempt",
                        &attempt.attempt_id,
                        "referenced outbox is missing",
                    )
                })?;
            validate_outbox_parent_payload(connection, &outbox).await?;
            if outbox.session_id.as_ref() != Some(session_id)
                || outbox.command_id.as_ref() != attempt.subject.command_id()
                || outbox.request_id.as_ref() != attempt.subject.request_id()
            {
                return Err(StoreError::corrupt(
                    "command_delivery_attempt",
                    &attempt.attempt_id,
                    "outbox session or subject binding does not match",
                ));
            }
        }
        (None, None) | (Some(_), None) => {
            if matches!(
                attempt.status,
                CommandDeliveryAttemptStatus::Pending
                    | CommandDeliveryAttemptStatus::Sent
                    | CommandDeliveryAttemptStatus::Acked
                    | CommandDeliveryAttemptStatus::Unconfirmed
            ) {
                return Err(StoreError::corrupt(
                    "command_delivery_attempt",
                    &attempt.attempt_id,
                    "delivery status requires an outbox binding",
                ));
            }
        }
        (None, Some(_)) => {
            return Err(StoreError::corrupt(
                "command_delivery_attempt",
                &attempt.attempt_id,
                "session_id and message_id must be present together",
            ));
        }
    }
    Ok(())
}

async fn fetch_attempt_conflicts(
    connection: &mut SqliteConnection,
    attempt: &NewDeliveryAttempt,
) -> Result<Vec<StoredDeliveryAttempt>, StoreError> {
    let rows = if let Some(message_id) = &attempt.message_id {
        sqlx::query(&format!(
            "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts \
             WHERE attempt_id = ? OR message_id = ?"
        ))
        .bind(&attempt.attempt_id)
        .bind(message_id.as_str())
        .fetch_all(&mut *connection)
        .await?
    } else {
        sqlx::query(&format!(
            "SELECT {ATTEMPT_COLUMNS} FROM command_delivery_attempts WHERE attempt_id = ?"
        ))
        .bind(&attempt.attempt_id)
        .fetch_all(&mut *connection)
        .await?
    };
    parse_and_validate_attempt_rows(&mut *connection, rows).await
}

fn same_attempt(existing: &StoredDeliveryAttempt, incoming: &NewDeliveryAttempt) -> bool {
    existing.attempt_id == incoming.attempt_id
        && existing.subject == incoming.subject
        && existing.session_id == incoming.session_id
        && existing.message_id == incoming.message_id
        && existing.request_payload == incoming.request_payload
        && existing.status == incoming.status
        && existing.attempted_at == incoming.attempted_at
        && existing.acked_at == incoming.acked_at
        && existing.error == incoming.error
        && existing.updated_at == incoming.updated_at
}

fn validate_new_attempt(attempt: &NewDeliveryAttempt) -> Result<(), StoreError> {
    require_non_empty("delivery attempt_id", &attempt.attempt_id)?;
    if attempt.updated_at < attempt.attempted_at
        || (attempt.status == CommandDeliveryAttemptStatus::Acked) != attempt.acked_at.is_some()
    {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "attempt timestamps do not match status".to_owned(),
        });
    }
    if attempt.status == CommandDeliveryAttemptStatus::Acked
        && !matches!(attempt.subject, DeliverySubject::ExecutionCommand(_))
    {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "ACKED is only valid for execution command receipt".to_owned(),
        });
    }
    if attempt.status == CommandDeliveryAttemptStatus::Unconfirmed
        && attempt
            .error
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "UNCONFIRMED requires a non-empty error".to_owned(),
        });
    }
    if attempt.message_id.is_some() && attempt.session_id.is_none() {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "session_id and message_id must be present together".to_owned(),
        });
    }
    if attempt.message_id.is_some() && attempt.request_payload.is_some() {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "outbox-bound attempts must not duplicate the request payload".to_owned(),
        });
    }
    if attempt.message_id.is_none() && attempt.request_payload.is_none() {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "unbound delivery attempts require the rejected request payload".to_owned(),
        });
    }
    if attempt.message_id.is_none()
        && matches!(
            attempt.status,
            CommandDeliveryAttemptStatus::Pending
                | CommandDeliveryAttemptStatus::Sent
                | CommandDeliveryAttemptStatus::Acked
                | CommandDeliveryAttemptStatus::Unconfirmed
        )
    {
        return Err(StoreError::InvalidRecord {
            entity: "command_delivery_attempt",
            key: attempt.attempt_id.clone(),
            reason: "delivery status requires an outbox binding".to_owned(),
        });
    }
    Ok(())
}

async fn required_session(
    connection: &mut SqliteConnection,
    session_id: &SessionId,
) -> Result<StoredSessionRecord, StoreError> {
    fetch_session_by_id(&mut *connection, session_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "execution_client_session",
            key: session_id.to_string(),
        })
}

async fn required_outbox(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<StoredWireOutbox, StoreError> {
    fetch_wire_outbox_by_id(&mut *connection, message_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "wire_outbox",
            key: message_id.to_string(),
        })
}

async fn required_attempt_by_message(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<StoredDeliveryAttempt, StoreError> {
    fetch_delivery_attempt_by_message(&mut *connection, message_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "command_delivery_attempt",
            key: format!("message_id={message_id}"),
        })
}

async fn required_outbound_delivery(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<StoredOutboundDelivery, StoreError> {
    Ok(StoredOutboundDelivery {
        outbox: required_outbox(connection, message_id).await?,
        attempt: required_attempt_by_message(connection, message_id).await?,
    })
}

async fn fetch_outbound_delivery(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<Option<StoredOutboundDelivery>, StoreError> {
    let Some(outbox) = fetch_wire_outbox_by_id(&mut *connection, message_id).await? else {
        return Ok(None);
    };
    validate_outbox_parent_payload(connection, &outbox).await?;
    let attempt = fetch_delivery_attempt_by_message(&mut *connection, message_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "outbound_delivery",
                message_id.to_string(),
                "delivery outbox has no matching attempt",
            )
        })?;
    Ok(Some(StoredOutboundDelivery { outbox, attempt }))
}

async fn validate_outbox_parent_payload(
    connection: &mut SqliteConnection,
    outbox: &StoredWireOutbox,
) -> Result<(), StoreError> {
    let wire =
        decode_wire_message::<Value>(outbox.payload.as_str().as_bytes(), SUPPORTED_SCHEMA_VERSION)
            .map_err(|error| {
                StoreError::corrupt(
                    "wire_outbox",
                    outbox.message_id.to_string(),
                    error.to_string(),
                )
            })?;
    let envelope_payload = CanonicalJson::from_value(wire.payload).map_err(|error| {
        StoreError::corrupt(
            "wire_outbox",
            outbox.message_id.to_string(),
            format!("wire payload cannot be canonicalized: {error}"),
        )
    })?;
    let (entity, key, row) = match (&outbox.command_id, &outbox.request_id) {
        (Some(command_id), None) => (
            "execution_command",
            command_id.as_str(),
            sqlx::query("SELECT payload_json, payload_hash FROM execution_commands WHERE command_id = ?")
                .bind(command_id.as_str())
                .fetch_optional(&mut *connection)
                .await?,
        ),
        (None, Some(request_id)) => (
            "reconciliation_run",
            request_id.as_str(),
            sqlx::query(
                "SELECT request_payload_json AS payload_json, request_payload_hash AS payload_hash \
                 FROM reconciliation_runs WHERE request_id = ?",
            )
            .bind(request_id.as_str())
            .fetch_optional(&mut *connection)
            .await?,
        ),
        _ => return Ok(()),
    };
    let row = row.ok_or_else(|| StoreError::NotFound {
        entity,
        key: key.to_owned(),
    })?;
    let durable_payload = CanonicalJson::from_stored(
        entity,
        key,
        row.try_get("payload_json")?,
        row.try_get("payload_hash")?,
    )?;
    if durable_payload != envelope_payload {
        return Err(StoreError::conflict(
            "outbound_delivery_payload",
            outbox.message_id.to_string(),
        ));
    }
    Ok(())
}

fn validate_new_active_session(session: &NewSessionRecord) -> Result<(), StoreError> {
    if session.status != SessionStatus::Active
        || session.disconnected_at.is_some()
        || session.max_inflight_commands == 0
        || session.updated_at < session.connected_at
        || session
            .last_heartbeat_at
            .is_some_and(|at| at < session.connected_at || at > session.updated_at)
        || session
            .last_time_sync_at
            .is_some_and(|at| at < session.connected_at || at > session.updated_at)
    {
        return Err(StoreError::InvalidRecord {
            entity: "execution_client_session",
            key: session.session_id.to_string(),
            reason: "invalid active session status, limit, or timestamps".to_owned(),
        });
    }
    if (session.clock_sync_status == Some(ClockSyncStatus::Synced)
        && session.last_time_sync_at.is_none())
        || (session.last_time_sync_at.is_some() && session.clock_sync_status.is_none())
    {
        return Err(StoreError::InvalidRecord {
            entity: "execution_client_session",
            key: session.session_id.to_string(),
            reason: "time-sync evidence is inconsistent with clock_sync_status".to_owned(),
        });
    }
    Ok(())
}

fn same_registration(existing: &StoredSessionRecord, incoming: &NewSessionRecord) -> bool {
    existing.session_id == incoming.session_id
        && existing.client_id == incoming.client_id
        && existing.account_id == incoming.account_id
        && existing.terminal_id == incoming.terminal_id
        && existing.platform == incoming.platform
        && existing.capabilities == incoming.capabilities
        && existing.remote_addr == incoming.remote_addr
        && existing.connected_at == incoming.connected_at
        && existing.max_inflight_commands == incoming.max_inflight_commands
}

fn require_active_session_revision(
    session: &StoredSessionRecord,
    expected_revision: u64,
) -> Result<(), StoreError> {
    if session.status == SessionStatus::Active && session.revision == expected_revision {
        Ok(())
    } else {
        Err(stale_session(&session.session_id))
    }
}

fn require_monotonic_transition_time(
    outbox: &StoredWireOutbox,
    attempt: &StoredDeliveryAttempt,
    at: i64,
) -> Result<(), StoreError> {
    if at < outbox.updated_at || at < attempt.updated_at {
        Err(stale_outbound(&outbox.message_id))
    } else {
        Ok(())
    }
}

fn parse_enum<T>(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: String,
) -> Result<T, StoreError>
where
    T: FromStr,
    T::Err: Display,
{
    value.parse().map_err(|error| {
        StoreError::corrupt(
            entity,
            key,
            format!("invalid {column} value {value:?}: {error}"),
        )
    })
}

fn checked_increment(field: &'static str, value: u64) -> Result<u64, StoreError> {
    let next = value
        .checked_add(1)
        .ok_or(StoreError::InvalidInteger { field, value })?;
    u64_to_i64(field, next)?;
    Ok(next)
}

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

fn non_negative_i64(
    entity: &'static str,
    key: &str,
    column: &'static str,
    value: i64,
) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::corrupt(entity, key, format!("{column} must be non-negative")))
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        Err(StoreError::InvalidRecord {
            entity: "gateway_delivery",
            key: field.to_owned(),
            reason: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}

fn stale_session(session_id: &SessionId) -> StoreError {
    StoreError::StaleWrite {
        entity: "execution_client_session",
        key: session_id.to_string(),
    }
}

fn stale_outbound(message_id: &MessageId) -> StoreError {
    StoreError::StaleWrite {
        entity: "outbound_delivery",
        key: message_id.to_string(),
    }
}

fn invalid_outbound(message_id: &MessageId, reason: impl Into<String>) -> StoreError {
    StoreError::InvalidRecord {
        entity: "outbound_delivery",
        key: message_id.to_string(),
        reason: reason.into(),
    }
}

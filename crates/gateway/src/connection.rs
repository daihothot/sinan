use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;
use sinan_protocol::{
    decode_wire_message, ExecutionClientMessageType, HeartbeatPayload, HelloAcceptedPayload,
    HelloPayload, ProtocolReason, SessionRejected, TimeSyncRequest, TimeSyncResponse, TransportAck,
    TransportAckStatus, WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{ControlSequenceReservation, StoreError};
use sinan_types::{ClockSyncStatus, ErrorCode, MessageId, SessionId};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::writer::QueuedOutboundSink;
use crate::HeartbeatHealth;
use crate::{
    client_may_send, validate_authenticated_identity, AuthenticatedSessionContext,
    ClientAuthenticationError, ClientAuthenticationRequest, ClientAuthenticator,
    ExecutionTransport, ExecutionTransportConfig, GatewayIdGenerator, GatewaySessionRegistry,
    InboundAdmission, InboundAdmissionError, InboundMessage, InboundMessagePort,
    SessionRegistration, SessionRegistryError, SessionResumeError, SessionResumePort,
    SessionResumeRequest, SinkWriteOutcome, TransportConfigError, TransportEvent,
    TransportEventKind, TransportEventPort,
};

#[derive(Clone)]
pub struct GatewayConnectionService {
    inner: Arc<GatewayConnectionServiceInner>,
}

struct GatewayConnectionServiceInner {
    sessions: GatewaySessionRegistry,
    authenticator: Arc<dyn ClientAuthenticator>,
    ids: Arc<dyn GatewayIdGenerator>,
    inbound: Arc<dyn InboundMessagePort>,
    resume: Arc<dyn SessionResumePort>,
    events: Arc<dyn TransportEventPort>,
    config: ExecutionTransportConfig,
    connection_limit: Arc<Semaphore>,
    handshake_limit: Arc<Semaphore>,
}

impl GatewayConnectionService {
    pub fn new(
        sessions: GatewaySessionRegistry,
        authenticator: Arc<dyn ClientAuthenticator>,
        ids: Arc<dyn GatewayIdGenerator>,
        inbound: Arc<dyn InboundMessagePort>,
        resume: Arc<dyn SessionResumePort>,
        events: Arc<dyn TransportEventPort>,
        config: ExecutionTransportConfig,
    ) -> Result<Self, ConnectionError> {
        config.validate()?;
        let session_config = sessions.config();
        if session_config.max_clock_offset_ms != config.max_clock_offset_ms
            || session_config.max_time_sync_rtt_ms != config.max_time_sync_rtt_ms
            || session_config.max_time_sync_age_ms != config.heartbeat_timeout_ms
        {
            return Err(TransportConfigError::Invalid(
                "Gateway session policy must match the advertised transport policy",
            )
            .into());
        }
        Ok(Self {
            inner: Arc::new(GatewayConnectionServiceInner {
                sessions,
                authenticator,
                ids,
                inbound,
                resume,
                events,
                connection_limit: Arc::new(Semaphore::new(config.max_connections)),
                handshake_limit: Arc::new(Semaphore::new(config.max_pending_handshakes)),
                config,
            }),
        })
    }

    pub fn config(&self) -> &ExecutionTransportConfig {
        &self.inner.config
    }

    pub(crate) fn try_connection_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.inner.connection_limit)
            .try_acquire_owned()
            .ok()
    }

    pub(crate) fn try_handshake_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.inner.handshake_limit)
            .try_acquire_owned()
            .ok()
    }

    pub(crate) async fn record_transport_event(
        &self,
        transport: ExecutionTransport,
        kind: TransportEventKind,
        remote_addr: Option<&str>,
        session_id: Option<&SessionId>,
        detail: impl Into<String>,
    ) {
        let occurred_at = self.inner.sessions.server_now().unwrap_or_default();
        self.record_event(
            transport,
            kind,
            occurred_at,
            remote_addr,
            session_id,
            None,
            detail,
        )
        .await;
    }

    pub(crate) async fn open(
        &self,
        transport: ExecutionTransport,
        remote_addr: Option<String>,
        hello_bytes: Vec<u8>,
        sink: Arc<QueuedOutboundSink>,
    ) -> Result<ActiveConnection, ConnectionError> {
        let result = self
            .open_inner(
                transport,
                remote_addr.as_deref(),
                hello_bytes,
                Arc::clone(&sink),
            )
            .await;
        if result.is_err() {
            sink.close();
        }
        result
    }

    async fn open_inner(
        &self,
        transport: ExecutionTransport,
        remote_addr: Option<&str>,
        hello_bytes: Vec<u8>,
        sink: Arc<QueuedOutboundSink>,
    ) -> Result<ActiveConnection, ConnectionError> {
        let now = self.inner.sessions.server_now()?;
        if hello_bytes.is_empty() || hello_bytes.len() > self.inner.config.max_message_bytes {
            self.record_event(
                transport,
                TransportEventKind::WireFrameTooLarge,
                now,
                remote_addr,
                None,
                None,
                "initial Execution Client message violates the configured size bound",
            )
            .await;
            return Err(ConnectionError::InvalidHandshake("invalid hello size"));
        }
        if std::str::from_utf8(&hello_bytes).is_err() {
            self.record_event(
                transport,
                TransportEventKind::DecodeFailed,
                now,
                remote_addr,
                None,
                None,
                "session.hello is not UTF-8",
            )
            .await;
            return Err(ConnectionError::InvalidHandshake("hello is not UTF-8"));
        }
        let hello =
            match decode_wire_message::<HelloPayload>(&hello_bytes, SUPPORTED_SCHEMA_VERSION) {
                Ok(hello) if hello.message_type == ExecutionClientMessageType::SessionHello => {
                    hello
                }
                _ => {
                    self.record_event(
                        transport,
                        TransportEventKind::HandshakeRejected,
                        now,
                        remote_addr,
                        None,
                        None,
                        "first data message is not a valid session.hello envelope",
                    )
                    .await;
                    return Err(ConnectionError::InvalidHandshake("invalid session.hello"));
                }
            };
        if hello.session_id.is_some()
            || hello.sequence.is_some_and(|sequence| sequence != 1)
            || hello
                .client_id
                .as_ref()
                .is_some_and(|client_id| client_id != &hello.payload.client_id)
        {
            self.send_rejection(
                transport,
                remote_addr,
                &hello,
                now,
                ErrorCode::SessionIdentityMismatch,
                "session.hello contains pre-bound or drifting identity",
                &sink,
            )
            .await;
            return Err(ConnectionError::InvalidHandshake(
                "session.hello identity mismatch",
            ));
        }

        let authenticated =
            match self
                .inner
                .authenticator
                .authenticate(ClientAuthenticationRequest::from_hello(
                    &hello.payload,
                    remote_addr,
                )) {
                Ok(authenticated)
                    if authenticated.client_id == hello.payload.client_id
                        && authenticated.account_id == hello.payload.account_id
                        && authenticated.terminal_id == hello.payload.terminal_id
                        && authenticated.platform == hello.payload.platform
                        && authenticated.remote_addr.as_deref() == remote_addr =>
                {
                    authenticated
                }
                Ok(_) | Err(ClientAuthenticationError::Rejected) => {
                    self.record_event(
                        transport,
                        TransportEventKind::AuthenticationFailed,
                        now,
                        remote_addr,
                        None,
                        Some(&hello.message_id),
                        "Execution Client credential was rejected",
                    )
                    .await;
                    self.send_rejection(
                        transport,
                        remote_addr,
                        &hello,
                        now,
                        ErrorCode::AuthenticationFailed,
                        "client authentication failed",
                        &sink,
                    )
                    .await;
                    return Err(ConnectionError::AuthenticationFailed);
                }
            };

        let session_id = self.inner.ids.next_session_id();
        let context = AuthenticatedSessionContext {
            transport,
            session_id: session_id.clone(),
            client_id: authenticated.client_id.clone(),
            account_id: authenticated.account_id.clone(),
            terminal_id: authenticated.terminal_id.clone(),
            platform: authenticated.platform,
            capabilities: hello.payload.capabilities.clone(),
            client_auth_secret_epoch: authenticated.secret_epoch,
            authenticated_at: now,
            remote_addr: authenticated.remote_addr.clone(),
        };
        if let Some(cursor) = hello.payload.resume.clone() {
            let request = SessionResumeRequest {
                hello_message_id: hello.message_id.clone(),
                cursor,
                received_at: now,
            };
            let admission = tokio::time::timeout(
                self.inner.config.inbound_admission_timeout,
                self.inner.resume.admit(&context, request),
            )
            .await;
            if !matches!(&admission, Ok(Ok(()))) {
                self.record_event(
                    transport,
                    TransportEventKind::InboundAdmissionFailed,
                    now,
                    remote_addr,
                    Some(&session_id),
                    Some(&hello.message_id),
                    "session resume cursor was not durably admitted",
                )
                .await;
                self.send_rejection(
                    transport,
                    remote_addr,
                    &hello,
                    now,
                    ErrorCode::ServiceUnavailable,
                    "session resume admission failed",
                    &sink,
                )
                .await;
                return match admission {
                    Ok(Err(error)) => Err(ConnectionError::ResumeAdmission(error)),
                    Err(_) => Err(ConnectionError::ResumeAdmissionTimeout),
                    Ok(Ok(())) => unreachable!("successful resume admission returned early"),
                };
            }
        }
        let accepted_message_id = self.inner.ids.next_message_id();
        let accepted_client_id = authenticated.client_id.clone();
        let accepted_session_id = session_id.clone();
        let accepted_hello_message_id = hello.message_id.clone();
        let accepted_config = self.inner.config.clone();
        let accepted_sessions = self.inner.sessions.clone();
        let completion = sink.stage_bootstrap_deferred(move || {
            let sent_at = accepted_sessions
                .server_now()
                .map_err(|error| format!("failed to sample session.accepted sent_at: {error}"))?;
            let accepted = WireMessage {
                message_id: accepted_message_id,
                message_type: ExecutionClientMessageType::SessionAccepted,
                schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                client_id: Some(accepted_client_id),
                session_id: Some(accepted_session_id.clone()),
                correlation_id: Some(accepted_hello_message_id.as_str().into()),
                causation_id: Some(accepted_hello_message_id.as_str().into()),
                sent_at: Some(sent_at),
                sequence: Some(1),
                payload: HelloAcceptedPayload {
                    session_id: accepted_session_id,
                    server_time: sent_at,
                    heartbeat_interval_ms: accepted_config.heartbeat_interval_ms,
                    heartbeat_timeout_ms: accepted_config.heartbeat_timeout_ms,
                    time_sync_interval_ms: accepted_config.time_sync_interval_ms,
                    max_time_sync_rtt_ms: accepted_config.max_time_sync_rtt_ms,
                    max_clock_offset_ms: accepted_config.max_clock_offset_ms,
                    max_inflight_commands: accepted_config.max_inflight_commands,
                    max_frame_bytes: accepted_config.max_frame_bytes as u64,
                    max_message_bytes: accepted_config.max_message_bytes as u64,
                },
            };
            accepted
                .validate(SUPPORTED_SCHEMA_VERSION)
                .map_err(|_| "deferred session.accepted failed protocol validation".to_owned())?;
            serde_json::to_vec(&accepted)
                .map_err(|error| format!("failed to encode deferred session.accepted: {error}"))
        });

        let registration = SessionRegistration {
            session_id: session_id.clone(),
            client_id: authenticated.client_id.clone(),
            account_id: authenticated.account_id.clone(),
            terminal_id: authenticated.terminal_id.clone(),
            platform: authenticated.platform,
            capabilities: hello.payload.capabilities.clone(),
            remote_addr: authenticated.remote_addr.clone(),
            max_inflight_commands: self.inner.config.max_inflight_commands,
        };
        let sessions = self.inner.sessions.clone();
        let activation_sink = sink.clone();
        // Keep the durable activation future alive if the connection task is cancelled.
        // Its output owns cleanup until this task actually receives the activated epoch.
        let mut activation_guard = tokio::spawn(async move {
            let replacement = sessions.activate(registration, activation_sink).await?;
            Ok::<_, SessionRegistryError>(ActivatedSessionGuard::new(
                sessions,
                replacement.session.session_id,
            ))
        })
        .await
        .map_err(|_| ConnectionError::ActivationTaskFailed)??;
        if !sink.start_writer() {
            self.inner
                .sessions
                .disconnect(&session_id, "SESSION_ACCEPTED_WRITER_NOT_STARTED")
                .await?;
            return Err(ConnectionError::BootstrapWrite(
                "transport writer startup gate was already consumed".to_owned(),
            ));
        }
        let accepted_outcome =
            match tokio::time::timeout(self.inner.config.write_timeout, completion).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(_)) => SinkWriteOutcome::Unconfirmed {
                    error: "session.accepted writer dropped without evidence".to_owned(),
                },
                Err(_) => SinkWriteOutcome::Unconfirmed {
                    error: "session.accepted write timed out".to_owned(),
                },
            };
        if accepted_outcome != SinkWriteOutcome::Written {
            self.inner
                .sessions
                .disconnect(&session_id, "SESSION_ACCEPTED_NOT_WRITTEN")
                .await?;
            return Err(ConnectionError::BootstrapWrite(format!(
                "session.accepted write outcome: {accepted_outcome:?}"
            )));
        }
        activation_guard.disarm();

        Ok(ActiveConnection {
            service: self.clone(),
            context,
            sink,
            last_inbound_sequence: 0,
        })
    }

    async fn send_rejection(
        &self,
        transport: ExecutionTransport,
        remote_addr: Option<&str>,
        hello: &WireMessage<HelloPayload>,
        now: i64,
        reason: ErrorCode,
        message: &'static str,
        sink: &QueuedOutboundSink,
    ) {
        let rejected_message_id = self.inner.ids.next_message_id();
        let rejected_client_id = hello.payload.client_id.clone();
        let rejected_hello_message_id = hello.message_id.clone();
        let rejected_sessions = self.inner.sessions.clone();
        let completion = sink.stage_bootstrap_deferred(move || {
            let sent_at = rejected_sessions
                .server_now()
                .map_err(|error| format!("failed to sample session.rejected sent_at: {error}"))?;
            let rejected = WireMessage {
                message_id: rejected_message_id,
                message_type: ExecutionClientMessageType::SessionRejected,
                schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                client_id: Some(rejected_client_id),
                session_id: None,
                correlation_id: Some(rejected_hello_message_id.as_str().into()),
                causation_id: Some(rejected_hello_message_id.as_str().into()),
                sent_at: Some(sent_at),
                sequence: None,
                payload: SessionRejected {
                    reason,
                    message: Some(message.to_owned()),
                    server_time: sent_at,
                },
            };
            rejected
                .validate(SUPPORTED_SCHEMA_VERSION)
                .map_err(|_| "deferred session.rejected failed protocol validation".to_owned())?;
            serde_json::to_vec(&rejected)
                .map_err(|error| format!("failed to encode deferred session.rejected: {error}"))
        });
        if !sink.start_writer() {
            return;
        }
        let _ = tokio::time::timeout(self.inner.config.write_timeout, completion).await;
        self.record_event(
            transport,
            TransportEventKind::HandshakeRejected,
            now,
            remote_addr,
            None,
            Some(&hello.message_id),
            message,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_event(
        &self,
        transport: ExecutionTransport,
        kind: TransportEventKind,
        occurred_at: i64,
        remote_addr: Option<&str>,
        session_id: Option<&SessionId>,
        message_id: Option<&MessageId>,
        detail: impl Into<String>,
    ) {
        let event = TransportEvent {
            transport,
            kind,
            occurred_at,
            remote_addr: remote_addr.map(str::to_owned),
            session_id: session_id.cloned(),
            message_id: message_id.cloned(),
            detail: detail.into(),
        };
        let _ = tokio::time::timeout(
            self.inner.config.event_write_timeout,
            self.inner.events.record(event),
        )
        .await;
    }
}

pub struct ActiveConnection {
    service: GatewayConnectionService,
    context: AuthenticatedSessionContext,
    sink: Arc<QueuedOutboundSink>,
    last_inbound_sequence: u64,
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.sink.close();
        spawn_exact_disconnect(
            self.service.inner.sessions.clone(),
            self.context.session_id.clone(),
            "CONNECTION_TASK_DROPPED",
        );
    }
}

struct ActivatedSessionGuard {
    sessions: GatewaySessionRegistry,
    session_id: SessionId,
    armed: bool,
}

impl ActivatedSessionGuard {
    fn new(sessions: GatewaySessionRegistry, session_id: SessionId) -> Self {
        Self {
            sessions,
            session_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActivatedSessionGuard {
    fn drop(&mut self) {
        if self.armed {
            spawn_exact_disconnect(
                self.sessions.clone(),
                self.session_id.clone(),
                "SESSION_BOOTSTRAP_TASK_DROPPED",
            );
        }
    }
}

fn spawn_exact_disconnect(
    sessions: GatewaySessionRegistry,
    session_id: SessionId,
    reason: &'static str,
) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move {
            let _ = sessions.disconnect(&session_id, reason).await;
        });
    }
}

impl ActiveConnection {
    pub fn context(&self) -> &AuthenticatedSessionContext {
        &self.context
    }

    pub(crate) async fn closed(&self) {
        self.sink.closed().await;
    }

    pub(crate) async fn handle_message(
        &mut self,
        wire_bytes: Vec<u8>,
    ) -> Result<InboundProgress, ConnectionError> {
        let received_at = self.service.inner.sessions.server_now()?;
        if wire_bytes.is_empty() || wire_bytes.len() > self.service.inner.config.max_message_bytes {
            self.event(
                TransportEventKind::WireFrameTooLarge,
                received_at,
                None,
                "inbound message violates the configured size bound",
            )
            .await;
            return Err(ConnectionError::FatalProtocol("invalid message size"));
        }
        if std::str::from_utf8(&wire_bytes).is_err() {
            self.event(
                TransportEventKind::DecodeFailed,
                received_at,
                None,
                "inbound message is not UTF-8",
            )
            .await;
            return Err(ConnectionError::FatalProtocol("message is not UTF-8"));
        }
        let envelope = match decode_wire_message::<Value>(&wire_bytes, SUPPORTED_SCHEMA_VERSION) {
            Ok(envelope) => envelope,
            Err(_) => {
                self.event(
                    TransportEventKind::SchemaRejected,
                    received_at,
                    None,
                    "inbound message failed JSON, type, schema, or envelope validation",
                )
                .await;
                return Err(ConnectionError::FatalProtocol("invalid wire envelope"));
            }
        };
        if !client_may_send(envelope.message_type) {
            self.event(
                TransportEventKind::DirectionRejected,
                received_at,
                Some(&envelope.message_id),
                "message type is not valid in the client-to-core direction",
            )
            .await;
            return Err(ConnectionError::FatalProtocol("invalid message direction"));
        }
        if validate_authenticated_identity(&self.context, &envelope).is_err() {
            self.event(
                TransportEventKind::SessionIdentityMismatch,
                received_at,
                Some(&envelope.message_id),
                "message identity differs from the authenticated session",
            )
            .await;
            return Err(ConnectionError::FatalProtocol("session identity mismatch"));
        }
        let sent_at_clock_skew = envelope.sent_at.is_some_and(|sent_at| {
            received_at.abs_diff(sent_at) > self.service.inner.config.max_clock_offset_ms
        });
        if sent_at_clock_skew {
            self.event(
                TransportEventKind::ClockSkewDetected,
                received_at,
                Some(&envelope.message_id),
                "inbound sent_at differs from Gateway server time",
            )
            .await;
        }
        let sequence = envelope
            .sequence
            .ok_or(ConnectionError::FatalProtocol("missing inbound sequence"))?;
        if (self.last_inbound_sequence == 0 && sequence != 1)
            || (self.last_inbound_sequence != 0 && sequence <= self.last_inbound_sequence)
        {
            self.event(
                TransportEventKind::SequenceViolation,
                received_at,
                Some(&envelope.message_id),
                "inbound sequence must start at one and increase monotonically",
            )
            .await;
            return Err(ConnectionError::FatalProtocol("inbound sequence violation"));
        }
        self.last_inbound_sequence = sequence;

        match envelope.message_type {
            ExecutionClientMessageType::TimeSyncRequest => {
                let request =
                    decode_wire_message::<TimeSyncRequest>(&wire_bytes, SUPPORTED_SCHEMA_VERSION)
                        .map_err(|_| ConnectionError::FatalProtocol("invalid time.sync.request"))?;
                if request.payload.request_id.as_str().trim().is_empty() {
                    return Err(ConnectionError::FatalProtocol(
                        "time.sync.request has an empty request_id",
                    ));
                }
                self.send_control_with(
                    ExecutionClientMessageType::TimeSyncResponse,
                    Some(request.message_id),
                    move |server_send_at| TimeSyncResponse {
                        request_id: request.payload.request_id,
                        server_receive_at: received_at,
                        server_send_at,
                        server_time: server_send_at,
                    },
                )
                .await?;
                Ok(InboundProgress::Continue)
            }
            ExecutionClientMessageType::Heartbeat => {
                let heartbeat =
                    decode_wire_message::<HeartbeatPayload>(&wire_bytes, SUPPORTED_SCHEMA_VERSION)
                        .map_err(|_| ConnectionError::FatalProtocol("invalid heartbeat payload"))?;
                let assessment = match self
                    .service
                    .inner
                    .sessions
                    .assess_heartbeat(&self.context.session_id, &heartbeat.payload)
                    .await
                {
                    Ok(assessment) => Some(assessment),
                    Err(SessionRegistryError::InvalidHeartbeat(_)) => {
                        self.event(
                            TransportEventKind::TimeSyncUnhealthy,
                            received_at,
                            Some(&envelope.message_id),
                            "heartbeat contained invalid time evidence",
                        )
                        .await;
                        if !sent_at_clock_skew
                            && received_at.abs_diff(heartbeat.payload.effective_server_now)
                                > self.service.inner.config.max_clock_offset_ms
                        {
                            self.event(
                                TransportEventKind::ClockSkewDetected,
                                received_at,
                                Some(&envelope.message_id),
                                "heartbeat effective_server_now differs from Gateway server time",
                            )
                            .await;
                        }
                        None
                    }
                    Err(error) => return Err(ConnectionError::Session(error)),
                };
                let admission = if assessment.is_some() {
                    InboundAdmission::Accepted
                } else {
                    InboundAdmission::Rejected {
                        reason: ErrorCode::BadRequest,
                    }
                };
                if let Some(assessment) = assessment.as_ref() {
                    if assessment.health == HeartbeatHealth::ClockSkew && !sent_at_clock_skew {
                        self.event(
                            TransportEventKind::ClockSkewDetected,
                            received_at,
                            Some(&envelope.message_id),
                            "heartbeat effective_server_now differs from Gateway server time",
                        )
                        .await;
                    }
                    if assessment.session.clock_sync_status != Some(ClockSyncStatus::Synced)
                        && assessment.previous_clock_sync_status
                            != assessment.session.clock_sync_status
                    {
                        self.event(
                            TransportEventKind::TimeSyncUnhealthy,
                            received_at,
                            Some(&envelope.message_id),
                            "Execution Client time sync is unhealthy",
                        )
                        .await;
                    } else if assessment.session.clock_sync_status == Some(ClockSyncStatus::Synced)
                        && assessment.previous_clock_sync_status != Some(ClockSyncStatus::Synced)
                    {
                        self.event(
                            TransportEventKind::TimeSyncRestored,
                            received_at,
                            Some(&envelope.message_id),
                            "Execution Client time sync was restored",
                        )
                        .await;
                    }
                }
                self.send_transport_ack(&envelope, received_at, admission)
                    .await?;
                Ok(if assessment.is_some() {
                    InboundProgress::Heartbeat
                } else {
                    InboundProgress::Continue
                })
            }
            _ => {
                let message_type = envelope.message_type;
                let admission_result = tokio::time::timeout(
                    self.service.inner.config.inbound_admission_timeout,
                    self.service.inner.inbound.admit(
                        &self.context,
                        InboundMessage {
                            envelope: envelope.clone(),
                            wire_bytes,
                            received_at,
                        },
                    ),
                )
                .await;
                let admission = match admission_result {
                    Ok(Ok(admission)) => admission,
                    Ok(Err(error)) => {
                        self.event(
                            TransportEventKind::InboundAdmissionFailed,
                            received_at,
                            Some(&envelope.message_id),
                            "durable inbound admission failed",
                        )
                        .await;
                        return Err(ConnectionError::InboundAdmission(error));
                    }
                    Err(_) => {
                        self.event(
                            TransportEventKind::InboundAdmissionFailed,
                            received_at,
                            Some(&envelope.message_id),
                            "durable inbound admission timed out",
                        )
                        .await;
                        return Err(ConnectionError::InboundAdmissionTimeout);
                    }
                };
                if message_type != ExecutionClientMessageType::TransportAck {
                    self.send_transport_ack(&envelope, received_at, admission)
                        .await?;
                }
                Ok(InboundProgress::Continue)
            }
        }
    }

    pub(crate) async fn disconnect(&self, reason: &str) -> Result<(), ConnectionError> {
        self.sink.close();
        self.service
            .inner
            .sessions
            .disconnect(&self.context.session_id, reason)
            .await?;
        Ok(())
    }

    pub(crate) async fn mark_stale(&self, reason: &str) -> Result<(), ConnectionError> {
        self.sink.close();
        self.service
            .inner
            .sessions
            .mark_stale(&self.context.session_id, reason)
            .await?;
        Ok(())
    }

    async fn send_transport_ack(
        &self,
        envelope: &WireMessage<Value>,
        received_at: i64,
        admission: InboundAdmission,
    ) -> Result<(), ConnectionError> {
        let (status, reason) = match admission {
            InboundAdmission::Accepted => (TransportAckStatus::Accepted, None),
            InboundAdmission::Duplicate => (TransportAckStatus::Duplicate, None),
            InboundAdmission::Rejected { reason } => (
                TransportAckStatus::Rejected,
                Some(ProtocolReason::Error(reason)),
            ),
        };
        self.send_control(
            ExecutionClientMessageType::TransportAck,
            Some(envelope.message_id.clone()),
            TransportAck {
                acked_message_id: envelope.message_id.clone(),
                acked_message_type: envelope.message_type,
                status,
                reason,
                received_at,
            },
        )
        .await
    }

    async fn send_control<T: Serialize>(
        &self,
        message_type: ExecutionClientMessageType,
        causation_id: Option<MessageId>,
        payload: T,
    ) -> Result<(), ConnectionError>
    where
        T: Send + 'static,
    {
        self.send_control_with(message_type, causation_id, move |_| payload)
            .await
    }

    async fn send_control_with<T, F>(
        &self,
        message_type: ExecutionClientMessageType,
        causation_id: Option<MessageId>,
        payload: F,
    ) -> Result<(), ConnectionError>
    where
        T: Serialize,
        F: FnOnce(i64) -> T + Send + 'static,
    {
        let reserved_at = self.service.inner.sessions.server_now()?;
        let reservation = self
            .service
            .inner
            .sessions
            .reserve_control_outbound_sequence(self.context.session_id.clone(), reserved_at)
            .await?;
        let ControlSequenceReservation::Reserved(reservation) = reservation else {
            return Err(ConnectionError::SessionUnavailable);
        };
        if reservation.client_id != self.context.client_id
            || reservation.account_id != self.context.account_id
            || reservation.terminal_id != self.context.terminal_id
        {
            return Err(ConnectionError::SessionUnavailable);
        }
        let message_id = self.service.inner.ids.next_message_id();
        let sessions = self.service.inner.sessions.clone();
        let client_id = self.context.client_id.clone();
        let session_id = self.context.session_id.clone();
        let correlation_id = causation_id
            .as_ref()
            .map(|message_id| message_id.as_str().into());
        let causation_id = causation_id.map(|message_id| message_id.as_str().into());
        let sequence = reservation.sequence;
        let outcome = self
            .sink
            .enqueue_deferred(sequence, move || {
                let sent_at = sessions.server_now().map_err(|error| {
                    format!("failed to sample control message sent_at: {error}")
                })?;
                let message = WireMessage {
                    message_id,
                    message_type,
                    schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                    client_id: Some(client_id),
                    session_id: Some(session_id),
                    correlation_id,
                    causation_id,
                    sent_at: Some(sent_at),
                    sequence: Some(sequence),
                    payload: payload(sent_at),
                };
                message.validate(SUPPORTED_SCHEMA_VERSION).map_err(|_| {
                    "deferred Gateway control message failed protocol validation".to_owned()
                })?;
                serde_json::to_vec(&message).map_err(|error| {
                    format!("failed to encode deferred Gateway control message: {error}")
                })
            })
            .await;
        if outcome == SinkWriteOutcome::Written {
            Ok(())
        } else {
            Err(ConnectionError::ControlWrite(format!("{outcome:?}")))
        }
    }

    async fn event(
        &self,
        kind: TransportEventKind,
        occurred_at: i64,
        message_id: Option<&MessageId>,
        detail: &'static str,
    ) {
        self.service
            .record_event(
                self.context.transport,
                kind,
                occurred_at,
                self.context.remote_addr.as_deref(),
                Some(&self.context.session_id),
                message_id,
                detail,
            )
            .await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundProgress {
    Continue,
    Heartbeat,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error(transparent)]
    InvalidConfig(#[from] TransportConfigError),

    #[error("Execution Client handshake rejected: {0}")]
    InvalidHandshake(&'static str),

    #[error("Execution Client authentication failed")]
    AuthenticationFailed,

    #[error("failed to encode a Gateway control message")]
    EncodeControlMessage,

    #[error("session bootstrap write failed: {0}")]
    BootstrapWrite(String),

    #[error("session activation task failed before ownership transfer")]
    ActivationTaskFailed,

    #[error("fatal Execution Client protocol error: {0}")]
    FatalProtocol(&'static str),

    #[error("active session became unavailable")]
    SessionUnavailable,

    #[error("Gateway control write failed: {0}")]
    ControlWrite(String),

    #[error(transparent)]
    InboundAdmission(#[from] InboundAdmissionError),

    #[error(transparent)]
    ResumeAdmission(#[from] SessionResumeError),

    #[error("inbound message admission timed out")]
    InboundAdmissionTimeout,

    #[error("session resume admission timed out")]
    ResumeAdmissionTimeout,

    #[error(transparent)]
    Session(#[from] SessionRegistryError),

    #[error(transparent)]
    Store(#[from] StoreError),
}

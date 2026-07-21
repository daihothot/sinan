use std::{io, net::SocketAddr, sync::Arc};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{watch, OwnedSemaphorePermit},
    task::JoinSet,
    time::{self, Instant},
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        error::CapacityError,
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
        protocol::WebSocketConfig,
        Error as TungsteniteError, Message,
    },
};

use crate::{
    writer::{QueuedOutboundSink, QueuedSinkDropGuard, QueuedWriter},
    ConnectionError, ExecutionTransport, GatewayConnectionService, InboundProgress,
    SinkWriteOutcome, TransportEventEvidence, TransportEventKind,
};

pub const EXECUTION_WEBSOCKET_PATH: &str = "/execution-client";

fn websocket_error_evidence(error: &ExecutionWebSocketError) -> TransportEventEvidence {
    match error {
        ExecutionWebSocketError::Tungstenite(TungsteniteError::Capacity(
            CapacityError::MessageTooLong { size, .. },
        ))
        | ExecutionWebSocketError::BinaryMessage { length: size } => {
            TransportEventEvidence::with_raw_payload_length(*size)
        }
        _ => TransportEventEvidence::default(),
    }
}

#[derive(Clone)]
pub struct ExecutionWebSocketBinding {
    service: GatewayConnectionService,
}

impl ExecutionWebSocketBinding {
    pub fn new(service: GatewayConnectionService) -> Self {
        Self { service }
    }

    pub async fn bind(
        self,
        address: impl ToSocketAddrs,
    ) -> Result<ExecutionWebSocketServer, ExecutionWebSocketError> {
        let listener = TcpListener::bind(address).await?;
        Ok(ExecutionWebSocketServer {
            listener,
            binding: self,
        })
    }

    pub async fn serve_stream(
        &self,
        stream: TcpStream,
        remote_addr: Option<SocketAddr>,
    ) -> Result<(), ExecutionWebSocketError> {
        let connection_permit = self
            .service
            .try_connection_permit()
            .ok_or(ExecutionWebSocketError::ConnectionLimitReached)?;
        let handshake_permit = self
            .service
            .try_handshake_permit()
            .ok_or(ExecutionWebSocketError::HandshakeLimitReached)?;
        let (shutdown_guard, shutdown) = watch::channel(false);
        let result = self
            .serve_stream_until(stream, remote_addr, shutdown, handshake_permit)
            .await;
        drop(shutdown_guard);
        drop(connection_permit);
        result
    }

    async fn serve_stream_until(
        &self,
        stream: TcpStream,
        remote_addr: Option<SocketAddr>,
        mut shutdown: watch::Receiver<bool>,
        handshake_permit: OwnedSemaphorePermit,
    ) -> Result<(), ExecutionWebSocketError> {
        let remote_addr = remote_addr
            .or_else(|| stream.peer_addr().ok())
            .map(|address| address.to_string());
        let config = self.service.config().clone();
        let websocket_config = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(
                config
                    .max_message_bytes
                    .saturating_mul(2)
                    .saturating_add(1024),
            )
            .max_message_size(Some(config.max_message_bytes))
            .max_frame_size(Some(config.max_message_bytes));
        let handshake_deadline = Instant::now() + config.handshake_timeout;
        let websocket = tokio::select! {
            result = time::timeout_at(
                handshake_deadline,
                accept_hdr_async_with_config(
                    stream,
                    require_execution_client_path,
                    Some(websocket_config),
                ),
            ) => match result {
                Ok(result) => result?,
                Err(_) => return Err(ExecutionWebSocketError::HandshakeTimeout),
            },
            _ = wait_for_shutdown(&mut shutdown) => {
                return Ok(());
            }
        };
        let (write_half, mut read_half) = websocket.split();
        let (sink, writer) =
            QueuedOutboundSink::new(config.outbound_queue_capacity, config.write_timeout);
        let _sink_drop_guard = QueuedSinkDropGuard::new(Arc::clone(&sink));
        let writer_task = tokio::spawn(run_websocket_writer(
            writer,
            write_half,
            config.max_message_bytes,
            config.write_timeout,
        ));

        let hello = tokio::select! {
            result = time::timeout_at(handshake_deadline, next_text_message(&mut read_half)) => {
                match result {
                    Ok(Ok(payload)) => payload,
                    Ok(Err(error)) => {
                        let kind = match &error {
                            ExecutionWebSocketError::Tungstenite(
                                TungsteniteError::Capacity(_),
                            ) => TransportEventKind::WireFrameTooLarge,
                            _ => TransportEventKind::WireProtocolViolation,
                        };
                        self.service.record_transport_event_with_evidence(
                            ExecutionTransport::ExecutionWebSocket,
                            kind,
                            remote_addr.as_deref(),
                            None,
                            websocket_error_evidence(&error),
                            error.to_string(),
                        ).await;
                        sink.close();
                        let _ = writer_task.await;
                        return Err(error);
                    }
                    Err(_) => {
                        self.service.record_transport_event(
                            ExecutionTransport::ExecutionWebSocket,
                            TransportEventKind::HandshakeRejected,
                            remote_addr.as_deref(),
                            None,
                            "session.hello timed out",
                        ).await;
                        sink.close();
                        let _ = writer_task.await;
                        return Err(ExecutionWebSocketError::HandshakeTimeout);
                    }
                }
            }
            _ = wait_for_shutdown(&mut shutdown) => {
                sink.close();
                let _ = writer_task.await;
                return Ok(());
            }
        };
        let Some(hello) = hello else {
            sink.close();
            let _ = writer_task.await;
            return Err(ExecutionWebSocketError::ClosedDuringHandshake);
        };
        let active_result = tokio::select! {
            result = time::timeout_at(
                handshake_deadline,
                self.service.open(
                    ExecutionTransport::ExecutionWebSocket,
                    remote_addr.clone(),
                    hello,
                    Arc::clone(&sink),
                ),
            ) => match result {
                Ok(result) => result,
                Err(_) => {
                    self.service.record_transport_event(
                        ExecutionTransport::ExecutionWebSocket,
                        TransportEventKind::HandshakeRejected,
                        remote_addr.as_deref(),
                        None,
                        "session.open timed out",
                    ).await;
                    sink.close();
                    let _ = writer_task.await;
                    return Err(ExecutionWebSocketError::HandshakeTimeout);
                }
            },
            _ = wait_for_shutdown(&mut shutdown) => {
                sink.close();
                let _ = writer_task.await;
                return Ok(());
            }
        };
        drop(handshake_permit);
        let mut active = match active_result {
            Ok(active) => active,
            Err(error) => {
                sink.close();
                let _ = writer_task.await;
                return Err(ExecutionWebSocketError::Connection(error));
            }
        };

        let mut heartbeat_deadline = Instant::now() + config.heartbeat_timeout();
        let result = loop {
            tokio::select! {
                biased;
                _ = active.closed() => {
                    let close = active.disconnect("TRANSPORT_WRITER_CLOSED").await;
                    break close.map_err(ExecutionWebSocketError::Connection);
                }
                _ = wait_for_shutdown(&mut shutdown) => {
                    let _ = active.disconnect("SERVER_SHUTDOWN").await;
                    break Ok(());
                }
                _ = time::sleep_until(heartbeat_deadline) => {
                    self.service.record_transport_event(
                        ExecutionTransport::ExecutionWebSocket,
                        TransportEventKind::HeartbeatTimedOut,
                        remote_addr.as_deref(),
                        Some(&active.context().session_id),
                        "Execution Client heartbeat timed out",
                    ).await;
                    let close = active.mark_stale("HEARTBEAT_TIMEOUT").await;
                    break close.map_err(ExecutionWebSocketError::Connection);
                }
                payload = next_text_message(&mut read_half) => {
                    match payload {
                        Ok(Some(payload)) => match active.handle_message(payload).await {
                            Ok(InboundProgress::Heartbeat) => {
                                heartbeat_deadline = Instant::now() + config.heartbeat_timeout();
                            }
                            Ok(InboundProgress::Continue) => {}
                            Err(error) => {
                                let _ = active.disconnect("INBOUND_PROTOCOL_FAILURE").await;
                                break Err(ExecutionWebSocketError::Connection(error));
                            }
                        },
                        Ok(None) => {
                            let close = active.disconnect("TRANSPORT_EOF").await;
                            break close.map_err(ExecutionWebSocketError::Connection);
                        }
                        Err(error) => {
                            let kind = match &error {
                                ExecutionWebSocketError::Tungstenite(
                                    TungsteniteError::Capacity(_),
                                ) => TransportEventKind::WireFrameTooLarge,
                                _ => TransportEventKind::WireProtocolViolation,
                            };
                            self.service.record_transport_event_with_evidence(
                                ExecutionTransport::ExecutionWebSocket,
                                kind,
                                remote_addr.as_deref(),
                                Some(&active.context().session_id),
                                websocket_error_evidence(&error),
                                error.to_string(),
                            ).await;
                            let _ = active.disconnect("EXECUTION_WEBSOCKET_MESSAGE_FAILURE").await;
                            break Err(error);
                        }
                    }
                }
            }
        };
        sink.close();
        let _ = writer_task.await;
        result
    }
}

pub struct ExecutionWebSocketServer {
    listener: TcpListener,
    binding: ExecutionWebSocketBinding,
}

impl ExecutionWebSocketServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ExecutionWebSocketError> {
        let mut tasks = JoinSet::new();
        let shutdown_grace = self.binding.service.config().write_timeout;

        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => {
                    break;
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    let _ = joined;
                }
                accepted = self.listener.accept() => {
                    let (stream, remote_addr) = accepted?;
                    let Some(connection_permit) = self.binding.service.try_connection_permit() else {
                        drop(stream);
                        continue;
                    };
                    let Some(handshake_permit) = self.binding.service.try_handshake_permit() else {
                        drop(stream);
                        continue;
                    };
                    let binding = self.binding.clone();
                    let child_shutdown = shutdown.clone();
                    tasks.spawn(async move {
                        let result = binding
                            .serve_stream_until(
                                stream,
                                Some(remote_addr),
                                child_shutdown,
                                handshake_permit,
                            )
                            .await;
                        drop(connection_permit);
                        result
                    });
                }
            }
        }

        finish_connection_tasks(&mut tasks, shutdown_grace).await;
        Ok(())
    }
}

async fn finish_connection_tasks<T: 'static>(
    tasks: &mut JoinSet<T>,
    shutdown_grace: std::time::Duration,
) -> bool {
    let graceful_shutdown = async { while tasks.join_next().await.is_some() {} };
    if time::timeout(shutdown_grace, graceful_shutdown)
        .await
        .is_ok()
    {
        return true;
    }
    tasks.shutdown().await;
    false
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn require_execution_client_path(
    request: &Request,
    response: Response,
) -> Result<Response, ErrorResponse> {
    if request.uri().path() == EXECUTION_WEBSOCKET_PATH {
        return Ok(response);
    }
    Err(tokio_tungstenite::tungstenite::http::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Some("unknown WebSocket endpoint".to_owned()))
        .expect("static WebSocket rejection response is valid"))
}

async fn next_text_message<S>(stream: &mut S) -> Result<Option<Vec<u8>>, ExecutionWebSocketError>
where
    S: Stream<Item = Result<Message, TungsteniteError>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => return Ok(Some(text.as_bytes().to_vec())),
            Some(Ok(Message::Binary(payload))) => {
                return Err(ExecutionWebSocketError::BinaryMessage {
                    length: payload.len(),
                });
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | None => return Ok(None),
            Some(Ok(Message::Frame(_))) => return Err(ExecutionWebSocketError::RawFrameMessage),
            Some(Err(error)) => return Err(error.into()),
        }
    }
}

async fn run_websocket_writer<S>(
    mut writer: QueuedWriter,
    mut transport: S,
    max_message_bytes: usize,
    write_timeout: std::time::Duration,
) where
    S: Sink<Message, Error = TungsteniteError> + Unpin,
{
    while let Some(mut request) = writer.next().await {
        let wire_bytes = match request.materialize_wire_bytes() {
            Ok(wire_bytes) => wire_bytes,
            Err(error) => {
                request.complete(SinkWriteOutcome::DefinitelyNotWritten {
                    error: error.clone(),
                });
                writer.fail_closed(&error);
                break;
            }
        };
        if wire_bytes.len() > max_message_bytes {
            let error = format!("WebSocket message exceeds configured maximum {max_message_bytes}");
            request.complete(SinkWriteOutcome::DefinitelyNotWritten {
                error: error.clone(),
            });
            writer.fail_closed(&error);
            break;
        }
        let text = match std::str::from_utf8(wire_bytes) {
            Ok(text) => text.to_owned(),
            Err(error) => {
                let error = format!("outbound WebSocket JSON is not UTF-8: {error}");
                request.complete(SinkWriteOutcome::DefinitelyNotWritten {
                    error: error.clone(),
                });
                writer.fail_closed(&error);
                break;
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = writer.closed() => SinkWriteOutcome::Unconfirmed {
                error: "transport was fenced during a WebSocket write".to_owned(),
            },
            result = time::timeout(write_timeout, transport.send(Message::Text(text.into()))) => {
                match result {
                    Ok(Ok(())) => SinkWriteOutcome::Written,
                    Ok(Err(error)) => SinkWriteOutcome::Unconfirmed {
                        error: format!("Execution WebSocket write failed: {error}"),
                    },
                    Err(_) => SinkWriteOutcome::Unconfirmed {
                        error: "Execution WebSocket write timed out".to_owned(),
                    },
                }
            }
        };
        let written = outcome == SinkWriteOutcome::Written;
        request.complete(outcome);
        if !written {
            break;
        }
    }
    let _ = time::timeout(write_timeout, transport.send(Message::Close(None))).await;
    let _ = time::timeout(write_timeout, transport.close()).await;
}

#[derive(Debug, Error)]
pub enum ExecutionWebSocketError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Tungstenite(#[from] TungsteniteError),

    #[error("Execution WebSocket endpoint received a binary data message ({length} bytes)")]
    BinaryMessage { length: usize },

    #[error("Execution WebSocket surfaced an unexpected raw frame message")]
    RawFrameMessage,

    #[error("Execution WebSocket closed before session.hello")]
    ClosedDuringHandshake,

    #[error("Execution WebSocket handshake or session.hello timed out")]
    HandshakeTimeout,

    #[error("Execution WebSocket connection limit reached")]
    ConnectionLimitReached,

    #[error("Execution WebSocket pending handshake limit reached")]
    HandshakeLimitReached,

    #[error(transparent)]
    Connection(#[from] ConnectionError),
}

#[cfg(test)]
mod tests {
    use std::{future, time::Duration};

    use tokio::{task::JoinSet, time};

    use super::finish_connection_tasks;

    #[tokio::test]
    async fn shutdown_aborts_connection_tasks_after_the_grace_period() {
        let mut tasks = JoinSet::new();
        tasks.spawn(future::pending::<()>());

        assert!(!finish_connection_tasks(&mut tasks, Duration::from_millis(1)).await);
        assert!(tasks.is_empty(), "aborted connection tasks must be drained");
        assert!(
            time::timeout(Duration::from_millis(1), tasks.join_next())
                .await
                .expect("drained JoinSet should return immediately")
                .is_none(),
            "drained JoinSet must not retain a completed child"
        );
    }
}

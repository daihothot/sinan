use std::{collections::VecDeque, io, net::SocketAddr, sync::Arc};

use sinan_protocol::{FrameDecodeError, NativeTcpFrameDecoder, NativeTcpFrameEncoder};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{watch, OwnedSemaphorePermit},
    task::JoinSet,
    time::{self, Instant},
};

use crate::{
    writer::{QueuedOutboundSink, QueuedSinkDropGuard, QueuedWriter},
    ConnectionError, ExecutionTransport, GatewayConnectionService, InboundProgress,
    SinkWriteOutcome, TransportEventKind,
};

#[derive(Clone)]
pub struct NativeTcpBinding {
    service: GatewayConnectionService,
}

impl NativeTcpBinding {
    pub fn new(service: GatewayConnectionService) -> Self {
        Self { service }
    }

    pub async fn bind(
        self,
        address: impl ToSocketAddrs,
    ) -> Result<NativeTcpServer, NativeTcpError> {
        let listener = TcpListener::bind(address).await?;
        Ok(NativeTcpServer {
            listener,
            binding: self,
        })
    }

    pub async fn serve_stream(
        &self,
        stream: TcpStream,
        remote_addr: Option<SocketAddr>,
    ) -> Result<(), NativeTcpError> {
        let connection_permit = self
            .service
            .try_connection_permit()
            .ok_or(NativeTcpError::ConnectionLimitReached)?;
        let handshake_permit = self
            .service
            .try_handshake_permit()
            .ok_or(NativeTcpError::HandshakeLimitReached)?;
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
    ) -> Result<(), NativeTcpError> {
        let remote_addr = remote_addr
            .or_else(|| stream.peer_addr().ok())
            .map(|address| address.to_string());
        let config = self.service.config().clone();
        let (read_half, write_half) = stream.into_split();
        let (sink, writer) =
            QueuedOutboundSink::new(config.outbound_queue_capacity, config.write_timeout);
        let _sink_drop_guard = QueuedSinkDropGuard::new(Arc::clone(&sink));
        let writer_task = tokio::spawn(run_native_tcp_writer(
            writer,
            write_half,
            config.max_frame_bytes,
            config.max_message_bytes,
            config.write_timeout,
        ));
        let mut reader = NativeTcpPayloadReader::new(
            read_half,
            config.max_frame_bytes,
            config.max_message_bytes,
            config.tcp_read_chunk_bytes,
        );
        let handshake_deadline = Instant::now() + config.handshake_timeout;

        let hello = tokio::select! {
            result = time::timeout_at(handshake_deadline, reader.next_payload()) => {
                match result {
                    Ok(Ok(payload)) => payload,
                    Ok(Err(error)) => {
                        let kind = match &error {
                            NativeTcpError::Frame(FrameDecodeError::FrameTooLarge { .. })
                            | NativeTcpError::MessageTooLarge { .. } => {
                                TransportEventKind::WireFrameTooLarge
                            }
                            _ => TransportEventKind::WireProtocolViolation,
                        };
                        self.service.record_transport_event(
                            ExecutionTransport::NativeTcp,
                            kind,
                            remote_addr.as_deref(),
                            None,
                            error.to_string(),
                        ).await;
                        sink.close();
                        let _ = writer_task.await;
                        return Err(error);
                    }
                    Err(_) => {
                        self.service.record_transport_event(
                            ExecutionTransport::NativeTcp,
                            TransportEventKind::HandshakeRejected,
                            remote_addr.as_deref(),
                            None,
                            "session.hello timed out",
                        ).await;
                        sink.close();
                        let _ = writer_task.await;
                        return Err(NativeTcpError::HandshakeTimeout);
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
            return Err(NativeTcpError::ClosedDuringHandshake);
        };
        let active_result = tokio::select! {
            result = time::timeout_at(
                handshake_deadline,
                self.service.open(
                    ExecutionTransport::NativeTcp,
                    remote_addr.clone(),
                    hello,
                    Arc::clone(&sink),
                ),
            ) => match result {
                Ok(result) => result,
                Err(_) => {
                    self.service.record_transport_event(
                        ExecutionTransport::NativeTcp,
                        TransportEventKind::HandshakeRejected,
                        remote_addr.as_deref(),
                        None,
                        "session.open timed out",
                    ).await;
                    sink.close();
                    let _ = writer_task.await;
                    return Err(NativeTcpError::HandshakeTimeout);
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
                return Err(NativeTcpError::Connection(error));
            }
        };

        let mut heartbeat_deadline = Instant::now() + config.heartbeat_timeout();
        let result = loop {
            tokio::select! {
                biased;
                _ = active.closed() => {
                    let close = active.disconnect("TRANSPORT_WRITER_CLOSED").await;
                    break close.map_err(NativeTcpError::Connection);
                }
                _ = wait_for_shutdown(&mut shutdown) => {
                    let _ = active.disconnect("SERVER_SHUTDOWN").await;
                    break Ok(());
                }
                _ = time::sleep_until(heartbeat_deadline) => {
                    self.service.record_transport_event(
                        ExecutionTransport::NativeTcp,
                        TransportEventKind::HeartbeatTimedOut,
                        remote_addr.as_deref(),
                        Some(&active.context().session_id),
                        "Execution Client heartbeat timed out",
                    ).await;
                    let close = active.mark_stale("HEARTBEAT_TIMEOUT").await;
                    break close.map_err(NativeTcpError::Connection);
                }
                payload = reader.next_payload() => {
                    match payload {
                        Ok(Some(payload)) => match active.handle_message(payload).await {
                            Ok(InboundProgress::Heartbeat) => {
                                heartbeat_deadline = Instant::now() + config.heartbeat_timeout();
                            }
                            Ok(InboundProgress::Continue) => {}
                            Err(error) => {
                                let _ = active.disconnect("INBOUND_PROTOCOL_FAILURE").await;
                                break Err(NativeTcpError::Connection(error));
                            }
                        },
                        Ok(None) => {
                            let close = active.disconnect("TRANSPORT_EOF").await;
                            break close.map_err(NativeTcpError::Connection);
                        }
                        Err(error) => {
                            let kind = match &error {
                                NativeTcpError::Frame(FrameDecodeError::FrameTooLarge { .. }) => {
                                    TransportEventKind::WireFrameTooLarge
                                }
                                _ => TransportEventKind::WireProtocolViolation,
                            };
                            self.service.record_transport_event(
                                ExecutionTransport::NativeTcp,
                                kind,
                                remote_addr.as_deref(),
                                Some(&active.context().session_id),
                                error.to_string(),
                            ).await;
                            let _ = active.disconnect("NATIVE_TCP_FRAME_FAILURE").await;
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

pub struct NativeTcpServer {
    listener: TcpListener,
    binding: NativeTcpBinding,
}

impl NativeTcpServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), NativeTcpError> {
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

struct NativeTcpPayloadReader<R> {
    reader: R,
    decoder: NativeTcpFrameDecoder,
    pending: VecDeque<Vec<u8>>,
    read_buffer: Vec<u8>,
    max_message_bytes: usize,
}

impl<R: AsyncRead + Unpin> NativeTcpPayloadReader<R> {
    fn new(
        reader: R,
        max_frame_bytes: usize,
        max_message_bytes: usize,
        read_chunk_bytes: usize,
    ) -> Self {
        Self {
            reader,
            // A TCP frame payload is exactly one JSON message. Applying the
            // tighter limit at prefix decode prevents buffering bytes that can
            // never pass max_message_bytes.
            decoder: NativeTcpFrameDecoder::new(max_frame_bytes.min(max_message_bytes)),
            pending: VecDeque::new(),
            read_buffer: vec![0; read_chunk_bytes],
            max_message_bytes,
        }
    }

    async fn next_payload(&mut self) -> Result<Option<Vec<u8>>, NativeTcpError> {
        loop {
            if let Some(payload) = self.pending.pop_front() {
                if payload.len() > self.max_message_bytes {
                    return Err(NativeTcpError::MessageTooLarge {
                        length: payload.len(),
                        max: self.max_message_bytes,
                    });
                }
                return Ok(Some(payload));
            }
            let read = self.reader.read(&mut self.read_buffer).await?;
            if read == 0 {
                return if self.decoder.has_pending_frame() {
                    Err(NativeTcpError::UnexpectedEof)
                } else {
                    Ok(None)
                };
            }
            self.pending
                .extend(self.decoder.feed(&self.read_buffer[..read])?);
        }
    }
}

async fn run_native_tcp_writer<W: AsyncWrite + Unpin>(
    mut writer: QueuedWriter,
    mut transport: W,
    max_frame_bytes: usize,
    max_message_bytes: usize,
    write_timeout: std::time::Duration,
) {
    let encoder = NativeTcpFrameEncoder::new(max_frame_bytes);
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
            let error =
                format!("Native TCP message exceeds configured maximum {max_message_bytes}");
            request.complete(SinkWriteOutcome::DefinitelyNotWritten {
                error: error.clone(),
            });
            writer.fail_closed(&error);
            break;
        }
        let frame = match encoder.encode(wire_bytes) {
            Ok(frame) => frame,
            Err(error) => {
                let error = error.to_string();
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
                error: "transport was fenced during a Native TCP write".to_owned(),
            },
            result = time::timeout(write_timeout, transport.write_all(&frame)) => {
                match result {
                    Ok(Ok(())) => SinkWriteOutcome::Written,
                    Ok(Err(error)) => SinkWriteOutcome::Unconfirmed {
                        error: format!("Native TCP write failed: {error}"),
                    },
                    Err(_) => SinkWriteOutcome::Unconfirmed {
                        error: "Native TCP write timed out".to_owned(),
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
    let _ = time::timeout(write_timeout, transport.shutdown()).await;
}

#[derive(Debug, Error)]
pub enum NativeTcpError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Frame(#[from] FrameDecodeError),

    #[error("WIRE_FRAME_TOO_LARGE: message length {length} exceeds configured maximum {max}")]
    MessageTooLarge { length: usize, max: usize },

    #[error("Native TCP connection ended with an incomplete frame")]
    UnexpectedEof,

    #[error("Native TCP connection closed before session.hello")]
    ClosedDuringHandshake,

    #[error("Native TCP session.hello timed out")]
    HandshakeTimeout,

    #[error("Native TCP connection limit reached")]
    ConnectionLimitReached,

    #[error("Native TCP pending handshake limit reached")]
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

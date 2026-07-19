use std::time::Duration;

use sinan_types::{MessageId, SessionId};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTransportConfig {
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub inbound_admission_timeout: Duration,
    pub event_write_timeout: Duration,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub time_sync_interval_ms: u64,
    pub max_time_sync_rtt_ms: u64,
    pub max_clock_offset_ms: u64,
    pub max_inflight_commands: u64,
    pub max_frame_bytes: usize,
    pub max_message_bytes: usize,
    pub outbound_queue_capacity: usize,
    pub tcp_read_chunk_bytes: usize,
    pub max_connections: usize,
    pub max_pending_handshakes: usize,
}

impl ExecutionTransportConfig {
    pub fn validate(&self) -> Result<(), TransportConfigError> {
        for (field, value) in [
            ("heartbeat_interval_ms", self.heartbeat_interval_ms),
            ("heartbeat_timeout_ms", self.heartbeat_timeout_ms),
            ("time_sync_interval_ms", self.time_sync_interval_ms),
            ("max_time_sync_rtt_ms", self.max_time_sync_rtt_ms),
            ("max_clock_offset_ms", self.max_clock_offset_ms),
            ("max_inflight_commands", self.max_inflight_commands),
        ] {
            if value == 0 || value > i64::MAX as u64 {
                return Err(TransportConfigError::Invalid(field));
            }
        }
        if self.handshake_timeout.is_zero() {
            return Err(TransportConfigError::Invalid("handshake_timeout"));
        }
        if self.write_timeout.is_zero() {
            return Err(TransportConfigError::Invalid("write_timeout"));
        }
        if self.inbound_admission_timeout.is_zero() {
            return Err(TransportConfigError::Invalid("inbound_admission_timeout"));
        }
        if self.inbound_admission_timeout >= self.handshake_timeout {
            return Err(TransportConfigError::Invalid(
                "inbound_admission_timeout must be shorter than handshake_timeout",
            ));
        }
        if self.event_write_timeout.is_zero() {
            return Err(TransportConfigError::Invalid("event_write_timeout"));
        }
        if self.heartbeat_timeout_ms <= self.heartbeat_interval_ms {
            return Err(TransportConfigError::Invalid(
                "heartbeat_timeout_ms must exceed heartbeat_interval_ms",
            ));
        }
        if self.time_sync_interval_ms > self.heartbeat_interval_ms {
            return Err(TransportConfigError::Invalid(
                "time_sync_interval_ms must not exceed heartbeat_interval_ms",
            ));
        }
        if self.max_frame_bytes == 0 || self.max_frame_bytes > u32::MAX as usize {
            return Err(TransportConfigError::Invalid("max_frame_bytes"));
        }
        if self.max_message_bytes == 0 || self.max_message_bytes > self.max_frame_bytes {
            return Err(TransportConfigError::Invalid(
                "max_message_bytes must be in 1..=max_frame_bytes",
            ));
        }
        if self.outbound_queue_capacity < 2 {
            return Err(TransportConfigError::Invalid(
                "outbound_queue_capacity must reserve bootstrap and live capacity",
            ));
        }
        if self.tcp_read_chunk_bytes < 4
            || self.tcp_read_chunk_bytes > self.max_frame_bytes.saturating_add(4)
        {
            return Err(TransportConfigError::Invalid("tcp_read_chunk_bytes"));
        }
        if self.max_connections == 0 {
            return Err(TransportConfigError::Invalid("max_connections"));
        }
        if self.max_pending_handshakes == 0 || self.max_pending_handshakes > self.max_connections {
            return Err(TransportConfigError::Invalid(
                "max_pending_handshakes must be in 1..=max_connections",
            ));
        }
        Ok(())
    }

    pub(crate) fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_timeout_ms)
    }
}

impl Default for ExecutionTransportConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            inbound_admission_timeout: Duration::from_secs(2),
            event_write_timeout: Duration::from_secs(1),
            heartbeat_interval_ms: 5_000,
            heartbeat_timeout_ms: 15_000,
            time_sync_interval_ms: 5_000,
            max_time_sync_rtt_ms: 1_000,
            max_clock_offset_ms: 250,
            max_inflight_commands: 32,
            max_frame_bytes: 262_144,
            max_message_bytes: 262_144,
            outbound_queue_capacity: 128,
            tcp_read_chunk_bytes: 16_384,
            max_connections: 1_024,
            max_pending_handshakes: 128,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum TransportConfigError {
    #[error("invalid Execution Client transport configuration: {0}")]
    Invalid(&'static str),
}

pub trait GatewayIdGenerator: Send + Sync {
    fn next_session_id(&self) -> SessionId;
    fn next_message_id(&self) -> MessageId;
}

#[derive(Default)]
pub struct UuidGatewayIdGenerator;

impl GatewayIdGenerator for UuidGatewayIdGenerator {
    fn next_session_id(&self) -> SessionId {
        SessionId::new(format!("session_{}", Uuid::new_v4().simple()))
    }

    fn next_message_id(&self) -> MessageId {
        MessageId::new(format!("message_{}", Uuid::new_v4().simple()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_valid() {
        ExecutionTransportConfig::default().validate().unwrap();
    }

    #[test]
    fn tcp_json_limit_cannot_exceed_frame_limit() {
        let mut config = ExecutionTransportConfig::default();
        config.max_message_bytes = config.max_frame_bytes + 1;
        assert!(matches!(
            config.validate(),
            Err(TransportConfigError::Invalid(
                "max_message_bytes must be in 1..=max_frame_bytes"
            ))
        ));
    }

    #[test]
    fn time_sync_sampling_cannot_be_slower_than_heartbeat_reporting() {
        let mut config = ExecutionTransportConfig::default();
        config.time_sync_interval_ms = config.heartbeat_interval_ms + 1;
        assert!(matches!(
            config.validate(),
            Err(TransportConfigError::Invalid(
                "time_sync_interval_ms must not exceed heartbeat_interval_ms"
            ))
        ));
    }

    #[test]
    fn inbound_admission_timeout_leaves_time_for_handshake_rejection() {
        let mut config = ExecutionTransportConfig::default();
        config.inbound_admission_timeout = config.handshake_timeout;
        assert!(matches!(
            config.validate(),
            Err(TransportConfigError::Invalid(
                "inbound_admission_timeout must be shorter than handshake_timeout"
            ))
        ));
    }

    #[test]
    fn generated_ids_are_non_empty_and_unique() {
        let generator = UuidGatewayIdGenerator;
        assert_ne!(generator.next_session_id(), generator.next_session_id());
        assert_ne!(generator.next_message_id(), generator.next_message_id());
    }
}

use std::{future::Future, pin::Pin};

use sinan_types::{MessageId, SessionId};

/// A complete Execution Client Protocol message ready for a transport binding.
///
/// The bytes contain the JSON wire envelope, but no TCP length prefix or
/// WebSocket framing. Those details belong to the concrete transport binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundFrame {
    pub session_id: SessionId,
    pub message_id: MessageId,
    pub sequence: u64,
    pub wire_bytes: Vec<u8>,
}

/// What the current transport can prove about one write attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SinkWriteOutcome {
    /// The complete envelope bytes were accepted by the active transport's
    /// actual write operation, at least into its runtime or OS write buffer.
    /// Admission to a user-space queue alone is not sufficient evidence.
    Written,
    /// The bounded write queue rejected the message without accepting bytes.
    Backpressure { queue_depth: usize },
    /// The transport can prove that no message bytes were accepted.
    DefinitelyNotWritten { error: String },
    /// Some or all bytes may have been accepted, so delivery is uncertain.
    Unconfirmed { error: String },
}

pub type SinkWriteFuture<'a> = Pin<Box<dyn Future<Output = SinkWriteOutcome> + Send + 'a>>;

/// Object-safe boundary implemented later by Native TCP and Execution WS.
pub trait OutboundSink: Send + Sync {
    fn write<'a>(&'a self, frame: OutboundFrame) -> SinkWriteFuture<'a>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

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
    }

    #[tokio::test]
    async fn sink_is_object_safe_and_receives_unframed_wire_bytes() {
        let frames = Arc::new(Mutex::new(Vec::new()));
        let sink: Arc<dyn OutboundSink> = Arc::new(RecordingSink {
            frames: Arc::clone(&frames),
            outcome: SinkWriteOutcome::Written,
        });
        let frame = OutboundFrame {
            session_id: SessionId::from("session_1"),
            message_id: MessageId::from("message_1"),
            sequence: 2,
            wire_bytes: br#"{"type":"execution.command"}"#.to_vec(),
        };

        assert_eq!(sink.write(frame.clone()).await, SinkWriteOutcome::Written);
        assert_eq!(frames.lock().unwrap().as_slice(), &[frame]);
    }
}

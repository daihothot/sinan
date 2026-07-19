use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub struct NativeTcpFrameEncoder {
    max_frame_bytes: usize,
}

impl NativeTcpFrameEncoder {
    pub const fn new(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes }
    }

    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, FrameEncodeError> {
        encode_native_tcp_frame(payload, self.max_frame_bytes)
    }

    pub fn encode_json<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, FrameEncodeError> {
        let payload = serde_json::to_vec(value)?;
        self.encode(&payload)
    }
}

pub fn encode_native_tcp_frame(
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<Vec<u8>, FrameEncodeError> {
    if payload.is_empty() {
        return Err(FrameEncodeError::WireProtocolViolation);
    }
    if payload.len() > max_frame_bytes {
        return Err(FrameEncodeError::FrameTooLarge {
            length: payload.len(),
            max: max_frame_bytes,
        });
    }

    let length = u32::try_from(payload.len()).map_err(|_| FrameEncodeError::LengthOverflow)?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

#[derive(Debug)]
pub struct NativeTcpFrameDecoder {
    max_frame_bytes: usize,
    buffer: Vec<u8>,
    expected_payload_len: Option<usize>,
    poisoned: bool,
}

impl NativeTcpFrameDecoder {
    pub fn new(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes,
            buffer: Vec::new(),
            expected_payload_len: None,
            poisoned: false,
        }
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Reports an incomplete prefix or payload retained across reads.
    pub fn has_pending_frame(&self) -> bool {
        !self.buffer.is_empty() || self.expected_payload_len.is_some()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.expected_payload_len = None;
        self.poisoned = false;
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, FrameDecodeError> {
        if self.poisoned {
            return Err(FrameDecodeError::Poisoned);
        }

        let mut input = bytes;
        let mut frames = Vec::new();

        while !input.is_empty() {
            if let Some(expected_payload_len) = self.expected_payload_len {
                let needed = expected_payload_len - self.buffer.len();
                let take = needed.min(input.len());
                self.buffer.extend_from_slice(&input[..take]);
                input = &input[take..];

                if self.buffer.len() == expected_payload_len {
                    frames.push(std::mem::take(&mut self.buffer));
                    self.expected_payload_len = None;
                }
                continue;
            }

            let needed = 4 - self.buffer.len();
            let take = needed.min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];

            if self.buffer.len() < 4 {
                continue;
            }

            let length = u32::from_be_bytes(
                self.buffer[..4]
                    .try_into()
                    .expect("buffer contains a complete four-byte prefix"),
            ) as usize;
            if length == 0 {
                self.poisoned = true;
                return Err(FrameDecodeError::WireProtocolViolation);
            }
            if length > self.max_frame_bytes {
                self.poisoned = true;
                return Err(FrameDecodeError::FrameTooLarge {
                    length,
                    max: self.max_frame_bytes,
                });
            }

            self.buffer.clear();
            self.expected_payload_len = Some(length);
        }

        Ok(frames)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, FrameDecodeError> {
        self.feed(bytes)
    }

    pub fn feed_json<T: DeserializeOwned>(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<T>, FrameDecodeError> {
        let frames = self.feed(bytes)?;
        let mut decoded = Vec::with_capacity(frames.len());
        for frame in frames {
            match serde_json::from_slice(&frame) {
                Ok(value) => decoded.push(value),
                Err(error) => {
                    self.poisoned = true;
                    return Err(FrameDecodeError::Decode(error));
                }
            }
        }
        Ok(decoded)
    }
}

#[derive(Debug, Error)]
pub enum FrameEncodeError {
    #[error("WIRE_PROTOCOL_VIOLATION: a Native TCP frame cannot be empty")]
    WireProtocolViolation,

    #[error("WIRE_FRAME_TOO_LARGE: frame length {length} exceeds configured maximum {max}")]
    FrameTooLarge { length: usize, max: usize },

    #[error("WIRE_FRAME_TOO_LARGE: payload length exceeds the unsigned 32-bit frame prefix")]
    LengthOverflow,

    #[error("failed to encode WireMessage JSON: {0}")]
    Encode(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum FrameDecodeError {
    #[error("Native TCP frame decoder is poisoned after a fatal protocol error")]
    Poisoned,

    #[error("WIRE_PROTOCOL_VIOLATION: a Native TCP frame cannot be empty")]
    WireProtocolViolation,

    #[error("WIRE_FRAME_TOO_LARGE: frame length {length} exceeds configured maximum {max}")]
    FrameTooLarge { length: usize, max: usize },

    #[error("failed to decode WireMessage JSON: {0}")]
    Decode(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_fragmented_prefix_and_payload() {
        let frame = encode_native_tcp_frame(br#"{"ok":true}"#, 1024).unwrap();
        let mut decoder = NativeTcpFrameDecoder::new(1024);

        assert!(decoder.feed(&frame[..2]).unwrap().is_empty());
        assert!(decoder.feed(&frame[2..7]).unwrap().is_empty());
        assert_eq!(
            decoder.feed(&frame[7..]).unwrap(),
            vec![br#"{"ok":true}"#.to_vec()]
        );
        assert_eq!(decoder.buffered_bytes(), 0);
        assert!(!decoder.has_pending_frame());
    }

    #[test]
    fn complete_prefix_without_payload_remains_pending() {
        let mut decoder = NativeTcpFrameDecoder::new(1024);
        assert!(decoder.feed(&3_u32.to_be_bytes()).unwrap().is_empty());
        assert_eq!(decoder.buffered_bytes(), 0);
        assert!(decoder.has_pending_frame());
    }

    #[test]
    fn decodes_coalesced_frames_and_keeps_partial_tail() {
        let first = encode_native_tcp_frame(b"one", 1024).unwrap();
        let second = encode_native_tcp_frame(b"two", 1024).unwrap();
        let third = encode_native_tcp_frame(b"three", 1024).unwrap();
        let mut bytes = [first, second, third[..5].to_vec()].concat();

        let mut decoder = NativeTcpFrameDecoder::new(1024);
        assert_eq!(
            decoder.feed(&bytes).unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
        assert_eq!(decoder.buffered_bytes(), 1);

        bytes.clear();
        assert_eq!(decoder.feed(&third[5..]).unwrap(), vec![b"three".to_vec()]);
    }

    #[test]
    fn rejects_zero_length_frame() {
        let mut decoder = NativeTcpFrameDecoder::new(1024);
        assert!(matches!(
            decoder.feed(&0_u32.to_be_bytes()),
            Err(FrameDecodeError::WireProtocolViolation)
        ));
    }

    #[test]
    fn rejects_oversized_frame_from_prefix_alone() {
        let mut decoder = NativeTcpFrameDecoder::new(8);
        assert!(matches!(
            decoder.feed(&9_u32.to_be_bytes()),
            Err(FrameDecodeError::FrameTooLarge { length: 9, max: 8 })
        ));
    }

    #[test]
    fn encoder_rejects_zero_and_oversized_payloads() {
        let encoder = NativeTcpFrameEncoder::new(3);
        assert!(matches!(
            encoder.encode(b""),
            Err(FrameEncodeError::WireProtocolViolation)
        ));
        assert!(matches!(
            encoder.encode(b"four"),
            Err(FrameEncodeError::FrameTooLarge { length: 4, max: 3 })
        ));
    }

    #[test]
    fn encoder_writes_unsigned_big_endian_prefix() {
        let frame = NativeTcpFrameEncoder::new(1024).encode(b"abc").unwrap();
        assert_eq!(&frame[..4], &[0, 0, 0, 3]);
        assert_eq!(&frame[4..], b"abc");
    }

    #[test]
    fn oversized_frame_does_not_buffer_payload_and_poisons_decoder() {
        let mut input = 9_u32.to_be_bytes().to_vec();
        input.extend_from_slice(&vec![b'x'; 16 * 1024]);

        let mut decoder = NativeTcpFrameDecoder::new(8);
        assert!(matches!(
            decoder.feed(&input),
            Err(FrameDecodeError::FrameTooLarge { length: 9, max: 8 })
        ));
        assert!(decoder.is_poisoned());
        assert!(decoder.buffered_bytes() <= 4);
        assert!(matches!(
            decoder.feed(&encode_native_tcp_frame(b"ok", 8).unwrap()),
            Err(FrameDecodeError::Poisoned)
        ));

        decoder.clear();
        assert!(!decoder.is_poisoned());
        assert_eq!(
            decoder
                .feed(&encode_native_tcp_frame(b"ok", 8).unwrap())
                .unwrap(),
            vec![b"ok".to_vec()]
        );
    }

    #[test]
    fn invalid_json_poisons_decoder_until_clear() {
        let frame = encode_native_tcp_frame(b"not-json", 1024).unwrap();
        let mut decoder = NativeTcpFrameDecoder::new(1024);

        assert!(matches!(
            decoder.feed_json::<serde_json::Value>(&frame),
            Err(FrameDecodeError::Decode(_))
        ));
        assert!(decoder.is_poisoned());
        assert!(matches!(decoder.feed(b""), Err(FrameDecodeError::Poisoned)));
    }
}

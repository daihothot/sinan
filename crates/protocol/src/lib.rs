//! Execution Client Protocol primitives shared by every transport binding.

mod framing;
mod message;
mod payload;
mod signing;
mod version;

pub use framing::{
    encode_native_tcp_frame, FrameDecodeError, FrameEncodeError, NativeTcpFrameDecoder,
    NativeTcpFrameEncoder,
};
pub use message::{
    decode_wire_message, EnvelopeValidationError, ExecutionClientMessage,
    ExecutionClientMessageType, UnknownMessageType, WireDecodeError, WireMessage,
};
pub use payload::*;
pub use signing::{
    build_execution_command_signing_string, format_fixed_decimal, rfc3986_encode,
    sign_execution_command, verify_execution_command_hmac, CommandSigningFormat, SigningError,
};
pub use version::{
    SchemaCompatibility, SchemaVersion, SchemaVersionError, SUPPORTED_SCHEMA_VERSION,
};

pub use sinan_types::{
    AccountSnapshot, ExecutionCommand, ExecutionEvent, MarketBar, OrderSnapshot, PositionSnapshot,
    SymbolMetadataSnapshot,
};

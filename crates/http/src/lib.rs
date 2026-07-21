#![forbid(unsafe_code)]

//! Authenticated Control Plane REST transport.
//!
//! This crate owns HTTP schemas, authentication, and status mapping. Durable
//! intake and projection reads remain behind application/query ports so the
//! transport cannot issue SQL or manufacture a risk decision.

mod auth;
mod dto;
mod error;
mod events_ws;
mod port;
mod router;

pub use auth::*;
pub use dto::*;
pub use error::*;
pub use events_ws::*;
pub use port::*;
pub use router::*;

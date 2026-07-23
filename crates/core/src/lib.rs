#![forbid(unsafe_code)]

//! Application composition for the Trading Core correctness boundary.

mod circuit_breaker_durable;
mod control_plane;
mod gateway_composition;
mod inbound_processor;
mod outbound_processor;
mod risk_workflow;

pub use circuit_breaker_durable::*;
pub use control_plane::*;
pub use gateway_composition::*;
pub use inbound_processor::*;
pub use outbound_processor::*;
pub use risk_workflow::*;

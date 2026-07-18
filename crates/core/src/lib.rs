#![forbid(unsafe_code)]

//! Application composition for the Trading Core correctness boundary.

mod circuit_breaker_durable;

pub use circuit_breaker_durable::*;

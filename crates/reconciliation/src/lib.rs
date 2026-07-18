#![forbid(unsafe_code)]

//! Pure reconciliation request planning and result evaluation.
//!
//! Broker snapshots are observations, not execution facts. This crate never
//! manufactures an [`sinan_types::ExecutionEvent`] and never decides that an
//! uncertain command may be retried. Command lifecycle targets are produced
//! exclusively through the `sinan-execution` state machine.

mod error;
mod evaluation;
mod model;
mod request;
mod validation;

pub use error::*;
pub use evaluation::*;
pub use model::*;
pub use request::*;

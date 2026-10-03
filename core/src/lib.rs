//! The relay's pure parts. Nothing here does I/O, so the rules most likely to
//! be wrong are tested without a broker.

mod envelope;

pub use envelope::{DecodeError, Envelope};

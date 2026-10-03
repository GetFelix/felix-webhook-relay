//! The relay's pure parts. Nothing here does I/O, so the rules most likely to
//! be wrong are tested without a broker.

pub mod catalog;
mod envelope;
pub mod health;
pub mod records;
pub mod secret;
pub mod signature;

pub use envelope::{DecodeError, Envelope};

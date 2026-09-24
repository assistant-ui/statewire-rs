//! Rust client for the Statewire protocol.
//!
//! Statewire replicates one JSON document table from a host to any number of
//! clients and carries typed commands back. This crate implements the client
//! side of the wire contract: document encoding, statepatch application,
//! frame/packet envelopes, protocol negotiation, and the session state machine
//! (lanes, sequence clocks, admission recovery).
//!
//! Transports are pluggable; the crate ships the protocol layer and leaves the
//! socket to the caller or to an optional transport feature.

pub mod document;
pub mod version;

pub use version::{ProtocolOffer, ProtocolSelection, VersionRange, WIRE_VERSION};

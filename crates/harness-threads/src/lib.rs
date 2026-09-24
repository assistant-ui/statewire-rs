//! Rust types for the harness-sdk thread and runs protocols.
//!
//! harness-sdk replicates an agent thread as Statewire state: one main
//! document (`{ threads, status, runs }`), per-message documents keyed
//! `[threadId, ns, messageId]`, and a separate `harness-sdk-threads` list
//! protocol. This crate is bindings only — serde types for the wire shapes
//! and constructors for the `run/*` and `thread/*` commands. All behavior
//! (queues, steering, approvals, dispatch) lives in the host; the spec pages
//! in the harness-sdk docs are the source of truth these types transcribe.

pub mod commands;
pub mod message;
pub mod state;
pub mod threads_list;

use statewire::{ProtocolOffer, VersionRange};

/// The `harness-sdk` Statewire parent protocol.
pub const HARNESS_PROTOCOL: &str = "harness-sdk";
/// The ephemeral interest subprotocol carrying message windows.
pub const INTEREST_PROTOCOL: &str = "harness-sdk/interest";
/// The thread-list protocol, served by a list host next to per-thread hosts.
pub const THREADS_PROTOCOL: &str = "harness-sdk-threads";
/// The protocol version this crate transcribes.
pub const PROTOCOL_VERSION: &str = "2026-09-13";
/// The main thread's namespace.
pub const MAIN_NS: &str = "main";
/// The default interest window depth.
pub const DEFAULT_WINDOW: u32 = 50;

/// The offer for attaching to a per-thread host.
pub fn harness_offer() -> ProtocolOffer {
    ProtocolOffer {
        name: HARNESS_PROTOCOL.to_owned(),
        range: VersionRange::exact(PROTOCOL_VERSION),
        optional: false,
    }
}

/// The offer for attaching to a thread-list host.
pub fn threads_offer() -> ProtocolOffer {
    ProtocolOffer {
        name: THREADS_PROTOCOL.to_owned(),
        range: VersionRange::exact(PROTOCOL_VERSION),
        optional: false,
    }
}

/// The document key of a message inside its protocol:
/// `[threadId, ns, messageId]`.
pub fn message_key(thread_id: &str, ns: &str, message_id: &str) -> Vec<String> {
    vec![thread_id.to_owned(), ns.to_owned(), message_id.to_owned()]
}

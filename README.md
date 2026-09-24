# statewire-rs

Rust client for the [Statewire](https://github.com/assistant-ui/harness-sdk) protocol: one JSON document table replicated from a host to any number of clients, with typed commands back.

This crate implements the client side of the wire contract — document encoding, statepatch application, frame/packet envelopes, protocol negotiation, and the session state machine (lanes, sequence clocks, admission recovery). Transports are pluggable.

## Status

Early development. The protocol spec lives in the harness-sdk docs.

## Development

```bash
cargo test
cargo clippy --all-targets
cargo fmt --check
```

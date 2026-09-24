# statewire-rs

[![crates.io: statewire](https://img.shields.io/crates/v/statewire?label=statewire)](https://crates.io/crates/statewire) [![crates.io: harness-threads](https://img.shields.io/crates/v/harness-threads?label=harness-threads)](https://crates.io/crates/harness-threads) [![CI](https://github.com/assistant-ui/statewire-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/assistant-ui/statewire-rs/actions/workflows/ci.yaml)

Rust clients for the [harness-sdk](https://github.com/assistant-ui/harness-sdk) wire protocols.

| Crate | What it is |
| --- | --- |
| `statewire` | Client for the Statewire protocol: one JSON document table replicated from a host, typed commands back. Document encoding, statepatch application, envelopes, negotiation, the sans-IO session state machine, and `ws` / `http` transports. |
| `harness-threads` | Types for the harness-sdk thread and runs protocols on top of `statewire`: the main document, message documents, run states, and the `run/*` / `thread/*` commands. Bindings only — all behavior lives in the host. |

## Install

```sh
cargo add statewire
cargo add harness-threads
```

Alpha, tracking protocol version `2026-09-13`. The protocol specs live in the harness-sdk docs.

## Development

```bash
cargo test
cargo clippy --all-targets
cargo fmt --check
```

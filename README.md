# statewire-rs

Rust clients for the [harness-sdk](https://github.com/assistant-ui/harness-sdk) wire protocols.

| Crate | What it is |
| --- | --- |
| `statewire` | Client for the Statewire protocol: one JSON document table replicated from a host, typed commands back. Document encoding, statepatch application, envelopes, negotiation, the sans-IO session state machine, and `ws` / `http` transports. |
| `harness-threads` | Types for the harness-sdk thread and runs protocols on top of `statewire`: the main document, message documents, run states, and the `run/*` / `thread/*` commands. Bindings only — all behavior lives in the host. |

## Status

Early development. The protocol spec lives in the harness-sdk docs.

## Development

```bash
cargo test
cargo clippy --all-targets
cargo fmt --check
```

# statewire-rs

[![crates.io: statewire](https://img.shields.io/crates/v/statewire?label=statewire)](https://crates.io/crates/statewire) [![crates.io: harness-threads](https://img.shields.io/crates/v/harness-threads?label=harness-threads)](https://crates.io/crates/harness-threads) [![CI](https://github.com/assistant-ui/statewire-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/assistant-ui/statewire-rs/actions/workflows/ci.yaml)

Rust clients for the [harness-sdk](https://github.com/assistant-ui/harness-sdk) wire protocols.

| Crate | What it is |
| --- | --- |
| `statewire` | Client for the Statewire protocol: one JSON document table replicated from a host, typed commands back. Document encoding, statepatch application, envelopes, negotiation, the sans-IO session state machine, and the `ws` (WebSocket) transport. |
| `harness-threads` | Types for the harness-sdk thread and runs protocols on top of `statewire`: the main document, message documents, run states, and the `run/*` / `thread/*` commands. Bindings only — all behavior lives in the host. |

## Install

```sh
cargo add statewire
cargo add harness-threads
```

To connect over WebSocket:

```toml
[dependencies]
statewire = { version = "0.3.0", features = ["ws"] }
```

The default build contains the protocol layer without a network runtime. Enable
`ws` for the WebSocket transport (including WSS with Rustls and WebPKI roots),
which needs a Tokio runtime. `statewire::transport::ws::connect` takes the
thread URL (`https://app.example/threads/t1`; the transport dials its `/ws`)
and returns a handle and event receiver; wait for
`Event::Connected` before treating the replicated state as ready. Dropping the
handle stops the transport task.

To authenticate, pass application headers to
`statewire::transport::ws::connect_with_headers(url, config, headers)`. They
travel on the upgrade request and in the first message of every attach,
never in the URL; the host checks them in its `authorize` hook, and an
application can also check them before the upgrade. Mark secret values sensitive. A refusal
(an `unauthorized` or `forbidden` fin, or an HTTP 401/403 on the upgrade)
stops the transport; connect again with fresh credentials.

This release targets wire protocol `2026-09-13` and is tested against the
JavaScript `statewire` 0.19.4 host. Rust crate versions are independent of the
wire protocol version. The protocol specs live in the harness-sdk docs.

Breaking API changes require a new minor version until 1.0. See the [changelog](https://github.com/assistant-ui/statewire-rs/blob/main/CHANGELOG.md)
for release changes.

## Development

```bash
npm ci --prefix tests/fixtures/counter-host --no-audit --no-fund
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test -p statewire --features __http-protocol-tests --locked
cargo test -p statewire --features ws --locked
cargo test --workspace --all-features --locked
cargo package -p statewire --all-features --locked
```

The transport integration tests require Node.js and the fixture dependencies;
they fail if the host cannot start. The crate also keeps a private HTTP/SSE
transport so the protocol's HTTP binding stays tested; the internal
`__http-protocol-tests` feature compiles it for those tests and is not public
API. To test an already running counter host, set `STATEWIRE_WS_URL` (the
WebSocket endpoint) or `STATEWIRE_HTTP_URL` (the instance URL, without
`/stream`).

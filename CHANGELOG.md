# Changelog

## Unreleased

- Add `http::connect_with_headers` for application headers on streams and frames.
- Protect protocol headers and refuse redirects to avoid forwarding credentials.
- Stop HTTP transports on authorization refusals, including frame submissions.

## 0.1.1

- Preserve HTTP/SSE events when network chunks split a UTF-8 character.
- Accept LF, CRLF, and CR event boundaries, including boundaries split across
  chunks, and an initial UTF-8 byte order mark.
- Enable TLS for the WebSocket transport using Rustls and WebPKI roots.
- Include the README and MIT license in published crate archives and expose
  both optional transports on docs.rs.
- Lock the JavaScript interoperability fixture and fail transport tests when
  the fixture cannot start. Check default, HTTP, WebSocket, and combined builds.
- Update `harness-threads` to depend on `statewire` 0.1.1. Public APIs and wire
  protocol `2026-09-13` are unchanged.

## 0.1.0

- Initial Statewire protocol client with HTTP/SSE and WebSocket transports.
- Initial `harness-threads` types for thread documents, messages, and commands.

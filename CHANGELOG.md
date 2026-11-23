# Changelog

## 0.3.0

- Breaking: `statewire::transport::ws::connect` and `connect_with_headers`
  take the thread URL, as the TypeScript client does. `http(s)` (or `ws(s)`)
  becomes `ws(s)` and the socket dials `/ws` after the URL's path, keeping its
  query: `https://app.example/threads/t1` dials
  `wss://app.example/threads/t1/ws?client=…`. Migrate: drop the `/ws` suffix
  from the URL you pass.
- `statewire::transport::ws::connect_with_headers(url, config, headers)` sends
  application headers (an `http::HeaderMap`, re-exported as
  `transport::ws::HeaderMap`) on every attach and reattach, both on the
  upgrade request (for apps that authenticate at the HTTP layer) and in the
  first message beside the negotiation headers (as the TypeScript client
  does). Hosts merge the two. The headers never go in the URL, and redirects
  are not followed. `statewire-*` names and
  the WebSocket handshake's headers are refused (`WsError::ReservedHeader`),
  as are values that aren't text (`WsError::InvalidHeader`). `connect` is
  `connect_with_headers` with no headers.
- The WebSocket transport stops when the host refuses an attach: after an
  `unauthorized` fin (which it already did) and now also when the upgrade
  request gets HTTP 3xx, 400, 401, 403, 404, 406 or 410, where it used to
  retry forever. To retry with fresh credentials, connect again.
- Breaking: the HTTP/SSE transport is no longer public. The `http` feature and
  `statewire::transport::http` are gone; connect with
  `statewire::transport::ws::connect` (feature `ws`) and the host's WebSocket
  endpoint. An open SSE stream keeps a Cloudflare Durable Object from
  hibernating, so WebSocket is the one transport the crate offers. The HTTP
  transport stays inside the crate, and its live round trip against the
  JavaScript host now runs as a unit test under the internal
  `__http-protocol-tests` feature. Wire protocol `2026-09-13` is unchanged.
- `harness-threads` 0.3.0 depends on `statewire` 0.3.0; its own API is
  unchanged.

## 0.2.0

- One value rule with harness-sdk's TypeScript and Python statewire (the
  shared table, `tests/fixtures/values.json`, is a copy of harness-sdk's
  `packages/statewire/core/tests/fixtures/values.json`):
  - `Session::command` and `Client::command` return `Result<u64, ValueError>`:
    a param holding an integer past the JS safe range is refused with its
    path, as in "statewire: can't encode the number 9007199254740992 at rsvp
    params[0].n: …". serde already writes a non-finite float as `null`.
  - The client checks what it receives itself: snapshots, patches and results
    lose `__proto__` keys and `prototype` keys under `constructor`, and read
    integers past the safe range as `null`. A value nested deeper than 512 is
    a protocol error (`SessionError::TooDeep`). Packets parse with that limit
    (`values::parse`) in place of serde_json's default of 128, so documents
    128 to 512 deep that other clients read now load here too.
- Breaking: the `command` signatures above. `harness-threads` 0.2.0 depends on
  `statewire` 0.2.0. Wire protocol `2026-09-13` is unchanged.

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

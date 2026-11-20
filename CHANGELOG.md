# Changelog

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

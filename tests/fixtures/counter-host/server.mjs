// A minimal statewire host serving the testing counter element over
// WebSocket, used by the Rust integration tests. Mirrors the socket wiring
// of statewire-node's host.
//
// Usage: npm install && npm start [port]
// Prints "listening <port>" once the socket is accepting connections.

import { createServer } from "node:http";
import { Readable } from "node:stream";
import { WebSocketServer } from "ws";
import { StatewireSocketHost } from "statewire/host-internal";
import { createCounterElement } from "statewire/testing";

const WS_SUBPROTOCOL = "statewire.v1";
const CLIENT_ID = /^[A-Za-z0-9._~-]{1,256}$/;

const host = StatewireSocketHost(createCounterElement());

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  const serve = url.pathname.endsWith("/stream")
    ? host.stream.bind(host)
    : url.pathname.endsWith("/frames")
      ? host.frames.bind(host)
      : null;
  if (serve === null) {
    response.writeHead(404).end();
    return;
  }
  const body =
    request.method === "GET" || request.method === "HEAD"
      ? undefined
      : Readable.toWeb(request);
  const result = await serve(
    new Request(url, {
      method: request.method,
      headers: request.headers,
      body,
      duplex: "half",
    }),
  );
  response.writeHead(
    result.status,
    Object.fromEntries(result.headers.entries()),
  );
  if (result.body === null) {
    response.end();
    return;
  }
  for await (const chunk of result.body) response.write(chunk);
  response.end();
});

const wss = new WebSocketServer({
  noServer: true,
  handleProtocols: (protocols) =>
    protocols.has(WS_SUBPROTOCOL) ? WS_SUBPROTOCOL : false,
});

server.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url, "http://localhost");
  const clientId = url.searchParams.get("client") ?? "";
  if (!url.pathname.endsWith("/ws") || !CLIENT_ID.test(clientId)) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, (ws) => {
    const conn = host.connect(
      clientId,
      {
        send: (data) => ws.send(data),
        close: (code, reason) => ws.close(code, reason),
      },
      {
        headers: Object.fromEntries(
          Object.entries(request.headers).map(([k, v]) => [k, String(v)]),
        ),
      },
    );
    if (conn === null) return;
    ws.on("message", (data, isBinary) => {
      if (isBinary) {
        ws.close(1008, "malformed frame");
        return;
      }
      conn.message(data.toString());
    });
    ws.on("close", () => conn.close());
  });
});

const port = Number(process.argv[2] ?? 0);
server.listen(port, "127.0.0.1", () => {
  console.log(`listening ${server.address().port}`);
});

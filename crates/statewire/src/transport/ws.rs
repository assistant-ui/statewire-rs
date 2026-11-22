//! StatewireWebsocket: carries frames and packets over a WebSocket.
//!
//! [`connect`] spawns a task that owns the socket and drives a shared
//! [`Session`]: it performs the negotiation handshake, forwards packets,
//! flushes queued frames, sends heartbeats after 15 seconds of send silence,
//! reconnects after 30 seconds of receive silence, and follows each fin's
//! client action with exponential backoff and jitter.
//!
//! Application headers (credentials, account selectors) travel twice on
//! every attach, never in the URL: on the upgrade request, for applications
//! and proxies that authenticate at the HTTP layer, and in-band in the first
//! message beside the negotiation headers, as the TypeScript client sends
//! them (browsers can't set upgrade headers). Hosts merge the two before
//! authorizing, the upgrade's winning; the values are the same.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::Message;

use super::{backoff_delay, forward, Client, BACKOFF_CEILING, RECEIVE_TIMEOUT};
use crate::session::{Config, Event, FinAction, Session};
use crate::wire::Packet;

pub use super::generate_client_id;
/// The header map [`connect_with_headers`] takes (the `http` crate's, the one
/// `reqwest` also uses).
pub use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};

/// The fixed bootstrap subprotocol.
pub const WS_SUBPROTOCOL: &str = "statewire.v1";

const SEND_HEARTBEAT_AFTER: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    /// A header the transport sets itself: `statewire-*` negotiation headers
    /// and the WebSocket handshake's own headers.
    #[error("request header is managed by the transport: {0}")]
    ReservedHeader(String),
    /// A header value that is not visible ASCII text, which the first
    /// message's JSON object of strings cannot carry. Only the name is shown.
    #[error("request header value is not text: {0}")]
    InvalidHeader(String),
}

/// Connects to `url` (the host's `/ws` endpoint) and returns the client
/// handle plus the session's event stream.
///
/// The task reconnects on transport faults and `reconnect`/`error` fins,
/// resending pending durable-lane commands per the admission rules. It stops
/// on `Stop` fin actions, when the event receiver is dropped, or when the
/// handle is dropped.
pub async fn connect(
    url: &str,
    config: Config,
) -> Result<(Client, mpsc::UnboundedReceiver<Event>), WsError> {
    connect_with_headers(url, config, HeaderMap::new()).await
}

/// Connects like [`connect`], sending application `headers` (for example
/// `authorization`) on the upgrade request and in the first message of
/// every attach and reattach. Redirects are not followed, so the headers
/// only reach `url`'s origin.
///
/// The headers are fixed for this connection. An `unauthorized` or
/// `forbidden` fin stops the transport after delivering
/// [`Event::Finished`], and so does an HTTP 3xx, 400, 401, 403, 404, 406 or
/// 410 answer to the upgrade request; to retry with fresh credentials, drop
/// the client and connect again. `statewire-*` names and the WebSocket
/// handshake's headers are reserved ([`WsError::ReservedHeader`]). Repeated
/// names are joined with `, `. Mark secret values sensitive so the caller's
/// own logging of the map redacts them; errors name headers, never values.
pub async fn connect_with_headers(
    url: &str,
    config: Config,
    headers: HeaderMap,
) -> Result<(Client, mpsc::UnboundedReceiver<Event>), WsError> {
    let application = Arc::new(Application {
        first_message: application_headers(&headers)?,
        upgrade: headers,
    });
    let separator = if url.contains('?') { '&' } else { '?' };
    let url = format!("{url}{separator}client={}", config.client_id);
    url.as_str()
        .into_client_request()
        .map_err(|error| WsError::InvalidUrl(error.to_string()))?;

    let session = Arc::new(Mutex::new(Session::new(config)));
    let wake = Arc::new(Notify::new());
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = mpsc::channel(1);

    tokio::spawn(run(
        url,
        application,
        session.clone(),
        wake.clone(),
        events_tx,
        stop_rx,
    ));

    Ok((
        Client {
            session,
            wake,
            _stop: stop_tx,
        },
        events_rx,
    ))
}

/// Header names the transport owns. The host merges first-message headers
/// with the upgrade request's case-insensitively and refuses duplicates, so
/// an application header may not shadow either.
fn is_reserved(name: &str) -> bool {
    name.starts_with("statewire-")
        || name.starts_with("sec-websocket-")
        || matches!(
            name,
            "host"
                | "connection"
                | "upgrade"
                | "content-length"
                | "content-type"
                | "transfer-encoding"
                | "te"
                | "trailer"
        )
}

/// The application headers in both of their forms.
struct Application {
    /// For the upgrade request; keeps the caller's sensitive marks.
    upgrade: HeaderMap,
    /// For the first message's `headers` object.
    first_message: serde_json::Map<String, Value>,
}

fn application_headers(headers: &HeaderMap) -> Result<serde_json::Map<String, Value>, WsError> {
    let mut object = serde_json::Map::new();
    for name in headers.keys() {
        let name = name.as_str();
        if is_reserved(name) {
            return Err(WsError::ReservedHeader(name.to_owned()));
        }
        let mut joined = String::new();
        for value in headers.get_all(name) {
            let text = value
                .to_str()
                .map_err(|_| WsError::InvalidHeader(name.to_owned()))?;
            if !joined.is_empty() {
                joined.push_str(", ");
            }
            joined.push_str(text);
        }
        object.insert(name.to_owned(), Value::String(joined));
    }
    Ok(object)
}

/// Upgrade refusals that retrying with the same request cannot fix.
fn refuses_attach(status: u16) -> bool {
    matches!(status, 300..=399 | 400 | 401 | 403 | 404 | 406 | 410)
}

async fn run(
    url: String,
    application: Arc<Application>,
    session: Arc<Mutex<Session>>,
    wake: Arc<Notify>,
    events: mpsc::UnboundedSender<Event>,
    mut stop: mpsc::Receiver<()>,
) {
    let mut attempt: u32 = 0;
    let mut min_delay = Duration::ZERO;
    loop {
        if attempt > 0 || min_delay > Duration::ZERO {
            let delay = backoff_delay(attempt).max(min_delay.min(BACKOFF_CEILING));
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stop.recv() => return,
            }
        }
        min_delay = Duration::ZERO;

        match attach(&url, &application, &session, &wake, &events, &mut stop).await {
            Attach::Stopped => return,
            Attach::Fin(action) => match action {
                FinAction::Reconnect {
                    min_delay_ms,
                    is_error,
                } => {
                    attempt = if is_error { attempt + 1 } else { 1 };
                    min_delay = Duration::from_millis(min_delay_ms);
                }
                FinAction::ReattachWhenNeeded
                | FinAction::RefreshCredentials
                | FinAction::Stop { .. } => return,
            },
            Attach::TransportFault => {
                let lost = session.lock().await.on_disconnect();
                if forward(&events, lost).is_err() {
                    return;
                }
                attempt += 1;
            }
        }
    }
}

enum Attach {
    /// The event receiver or handle went away.
    Stopped,
    /// The server ended the connection; follow the fin's action.
    Fin(FinAction),
    /// The socket failed without a fin; reconnect with backoff.
    TransportFault,
}

async fn attach(
    url: &str,
    application: &Application,
    session: &Mutex<Session>,
    wake: &Notify,
    events: &mpsc::UnboundedSender<Event>,
    stop: &mut mpsc::Receiver<()>,
) -> Attach {
    let mut request = match url.into_client_request() {
        Ok(request) => request,
        Err(_) => return Attach::Stopped,
    };
    for (name, value) in &application.upgrade {
        request.headers_mut().append(name, value.clone());
    }
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        WS_SUBPROTOCOL.parse().expect("static header value"),
    );

    let (mut socket, _response) = match tokio_tungstenite::connect_async(request).await {
        Ok(connected) => connected,
        Err(TungsteniteError::Http(response)) if refuses_attach(response.status().as_u16()) => {
            return Attach::Fin(FinAction::Stop { is_error: true })
        }
        Err(_) => return Attach::TransportFault,
    };

    let mut headers = application.first_message.clone();
    for (name, value) in session.lock().await.attach_headers() {
        headers.insert(name, Value::String(value));
    }
    let initial = serde_json::json!({ "headers": headers });
    if socket
        .send(Message::text(initial.to_string()))
        .await
        .is_err()
    {
        return Attach::TransportFault;
    }

    let mut last_send = Instant::now();
    let mut last_receive = Instant::now();
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = stop.recv() => return Attach::Stopped,
            _ = wake.notified() => {
                match flush(&mut socket, session).await {
                    Ok(true) => last_send = Instant::now(),
                    Ok(false) => {}
                    Err(()) => return Attach::TransportFault,
                }
            }
            _ = timer.tick() => {
                if last_receive.elapsed() > RECEIVE_TIMEOUT {
                    return Attach::TransportFault;
                }
                if last_send.elapsed() > SEND_HEARTBEAT_AFTER {
                    if socket.send(Message::text("{}")).await.is_err() {
                        return Attach::TransportFault;
                    }
                    last_send = Instant::now();
                }
            }
            incoming = socket.next() => {
                let message = match incoming {
                    Some(Ok(message)) => message,
                    Some(Err(_)) | None => return Attach::TransportFault,
                };
                last_receive = Instant::now();
                let text = match message {
                    Message::Text(text) => text,
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => return Attach::TransportFault,
                    _ => continue,
                };
                let packet: Packet = match crate::values::parse(text.as_str()) {
                    Ok(packet) => packet,
                    Err(_) => return Attach::TransportFault,
                };
                let outcome = session.lock().await.handle_packet(packet);
                let batch = match outcome {
                    Ok(batch) => batch,
                    Err(_) => return Attach::TransportFault,
                };
                let connected = batch
                    .iter()
                    .any(|event| matches!(event, Event::Connected { .. }));
                let finished = batch.iter().find_map(|event| match event {
                    Event::Finished { action, .. } => Some(action.clone()),
                    _ => None,
                });
                if forward(events, batch).is_err() {
                    return Attach::Stopped;
                }
                if let Some(action) = finished {
                    return Attach::Fin(action);
                }
                if connected {
                    match flush(&mut socket, session).await {
                        Ok(true) => last_send = Instant::now(),
                        Ok(false) => {}
                        Err(()) => return Attach::TransportFault,
                    }
                }
            }
        }
    }
}

async fn flush(
    socket: &mut (impl SinkExt<Message> + Unpin),
    session: &Mutex<Session>,
) -> Result<bool, ()> {
    let mut sent = false;
    loop {
        let frame = session.lock().await.next_frame();
        let Some(frame) = frame else {
            return Ok(sent);
        };
        let text = serde_json::to_string(&frame).map_err(|_| ())?;
        socket.send(Message::text(text)).await.map_err(|_| ())?;
        sent = true;
    }
}

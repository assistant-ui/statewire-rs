//! StatewireWebsocket: carries frames and packets over a WebSocket.
//!
//! [`connect`] spawns a task that owns the socket and drives a shared
//! [`Session`]: it performs the negotiation handshake, forwards packets,
//! flushes queued frames, sends heartbeats after 15 seconds of send silence,
//! reconnects after 30 seconds of receive silence, and follows each fin's
//! client action with exponential backoff and jitter.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::session::{Config, Event, FinAction, Session};
use crate::wire::Packet;

/// The fixed bootstrap subprotocol.
pub const WS_SUBPROTOCOL: &str = "statewire.v1";

const SEND_HEARTBEAT_AFTER: Duration = Duration::from_secs(15);
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);
const BACKOFF_FLOOR: Duration = Duration::from_millis(250);
const BACKOFF_CEILING: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("connection stopped: {0}")]
    Stopped(String),
}

/// A handle to a running WebSocket client.
///
/// Dropping the handle stops the connection task.
pub struct WsClient {
    session: Arc<Mutex<Session>>,
    wake: Arc<Notify>,
    _stop: mpsc::Sender<()>,
}

impl WsClient {
    /// Queues a command and wakes the connection to send it.
    pub async fn command(&self, protocol: &str, method: &str, params: Vec<Value>) -> u64 {
        let seq = self.session.lock().await.command(protocol, method, params);
        self.wake.notify_one();
        seq
    }

    /// Reads from the session under its lock.
    pub async fn with_session<T>(&self, read: impl FnOnce(&Session) -> T) -> T {
        read(&*self.session.lock().await)
    }

    /// The current value of a protocol's main document.
    pub async fn main_value(&self, protocol: &str) -> Option<Value> {
        self.with_session(|session| session.documents().main(protocol).map(|d| d.value.clone()))
            .await
    }
}

/// Generates a client id: 32 lowercase hexadecimal characters.
pub fn generate_client_id() -> String {
    let mut id = String::with_capacity(32);
    for salt in 0u64..2 {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(salt);
        hasher.write_u128(std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_nanos()));
        id.push_str(&format!("{:016x}", hasher.finish()));
    }
    id
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
) -> Result<(WsClient, mpsc::UnboundedReceiver<Event>), WsError> {
    let separator = if url.contains('?') { '&' } else { '?' };
    let url = format!("{url}{separator}client={}", config.client_id);
    url.as_str()
        .into_client_request()
        .map_err(|error| WsError::InvalidUrl(error.to_string()))?;

    let session = Arc::new(Mutex::new(Session::new(config)));
    let wake = Arc::new(Notify::new());
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = mpsc::channel(1);

    tokio::spawn(run(url, session.clone(), wake.clone(), events_tx, stop_rx));

    Ok((
        WsClient {
            session,
            wake,
            _stop: stop_tx,
        },
        events_rx,
    ))
}

async fn run(
    url: String,
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

        match attach(&url, &session, &wake, &events, &mut stop).await {
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
    session: &Mutex<Session>,
    wake: &Notify,
    events: &mpsc::UnboundedSender<Event>,
    stop: &mut mpsc::Receiver<()>,
) -> Attach {
    let mut request = match url.into_client_request() {
        Ok(request) => request,
        Err(_) => return Attach::Stopped,
    };
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        WS_SUBPROTOCOL.parse().expect("static header value"),
    );

    let (mut socket, _response) = match tokio_tungstenite::connect_async(request).await {
        Ok(connected) => connected,
        Err(_) => return Attach::TransportFault,
    };

    let headers: serde_json::Map<String, Value> = session
        .lock()
        .await
        .attach_headers()
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
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
                let packet: Packet = match serde_json::from_str(text.as_str()) {
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

fn forward(events: &mpsc::UnboundedSender<Event>, batch: Vec<Event>) -> Result<(), ()> {
    for event in batch {
        events.send(event).map_err(|_| ())?;
    }
    Ok(())
}

fn backoff_delay(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let exponent = attempt.saturating_sub(1).min(7);
    let base = BACKOFF_FLOOR * 2u32.pow(exponent);
    let capped = base.min(BACKOFF_CEILING);
    // Full jitter: a uniform-ish draw in [capped/2, capped].
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u32(attempt);
    let fraction = (hasher.finish() % 1000) as f64 / 1000.0;
    capped / 2 + Duration::from_secs_f64(capped.as_secs_f64() / 2.0 * fraction)
}

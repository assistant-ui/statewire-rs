//! StatewireHttp: packets over an SSE stream, frames over `POST /frames`.
//!
//! [`connect`] spawns a task that attaches with `GET {url}/stream`, reads
//! one packet per SSE `data:` event (`: ping` comments count as liveness),
//! and submits frames through `POST {url}/frames` under the stream's
//! `Statewire-Lease`. One POST is in flight at a time, which satisfies the
//! one-POST-per-lane rule for every frame shape. Refusal statuses follow the
//! transport table: 409 reconciles watermarks, 429 waits `syn.retry`, 423
//! reattaches, 413 splits the frame, 400 stops.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{interval, Instant, MissedTickBehavior};

use super::{backoff_delay, forward, Client, BACKOFF_CEILING, RECEIVE_TIMEOUT};
use crate::session::{Config, Event, FinAction, Session};
use crate::wire::{Frame, Packet, Statement};

pub use super::generate_client_id;

/// Client id header for `GET /stream`.
pub const CLIENT_ID_HEADER: &str = "Statewire-Client-Id";
/// Lease header binding `POST /frames` to an attach.
pub const LEASE_HEADER: &str = "Statewire-Lease";

const POST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
}

/// Connects to `url` (the instance URL, without the `/stream` suffix) and
/// returns the client handle plus the session's event stream.
pub async fn connect(
    url: &str,
    config: Config,
) -> Result<(Client, mpsc::UnboundedReceiver<Event>), HttpError> {
    let url = url.trim_end_matches('/').to_owned();
    reqwest::Url::parse(&format!("{url}/stream"))
        .map_err(|error| HttpError::InvalidUrl(error.to_string()))?;

    let session = Arc::new(Mutex::new(Session::new(config)));
    let wake = Arc::new(Notify::new());
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = mpsc::channel(1);

    tokio::spawn(run(url, session.clone(), wake.clone(), events_tx, stop_rx));

    Ok((
        Client {
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
    let Ok(http) = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
    else {
        return;
    };
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

        match attach(&http, &url, &session, &wake, &events, &mut stop).await {
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
    Stopped,
    Fin(FinAction),
    TransportFault,
}

enum PostOutcome {
    Sent,
    /// The lease is stale or the frame violated the protocol per the server.
    Reattach,
    Stop,
    Fault,
}

async fn attach(
    http: &reqwest::Client,
    url: &str,
    session: &Mutex<Session>,
    wake: &Notify,
    events: &mpsc::UnboundedSender<Event>,
    stop: &mut mpsc::Receiver<()>,
) -> Attach {
    let (client_id, headers) = {
        let session = session.lock().await;
        (session.client_id().to_owned(), session.attach_headers())
    };
    let mut request = http
        .get(format!("{url}/stream"))
        .header(CLIENT_ID_HEADER, client_id);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => return Attach::TransportFault,
    };
    match response.status().as_u16() {
        200 => {}
        401 | 403 | 400 | 404 | 406 | 410 => {
            return Attach::Fin(FinAction::Stop { is_error: true })
        }
        _ => return Attach::TransportFault,
    }
    let lease = response
        .headers()
        .get(LEASE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    let mut last_receive = Instant::now();
    let mut timer = interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = stop.recv() => return Attach::Stopped,
            _ = wake.notified() => {
                match post_frames(http, url, lease.as_deref(), session, events).await {
                    PostOutcome::Sent => {}
                    PostOutcome::Reattach | PostOutcome::Fault => return Attach::TransportFault,
                    PostOutcome::Stop => return Attach::Fin(FinAction::Stop { is_error: true }),
                }
            }
            _ = timer.tick() => {
                if last_receive.elapsed() > RECEIVE_TIMEOUT {
                    return Attach::TransportFault;
                }
            }
            chunk = stream.next() => {
                let bytes = match chunk {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(_)) | None => return Attach::TransportFault,
                };
                last_receive = Instant::now();
                let Ok(text) = std::str::from_utf8(&bytes) else {
                    return Attach::TransportFault;
                };
                buffer.push_str(text);
                while let Some(boundary) = buffer.find("\n\n") {
                    let block = buffer[..boundary].to_owned();
                    buffer.drain(..boundary + 2);
                    let Some(payload) = sse_data(&block) else {
                        continue;
                    };
                    let packet: Packet = match serde_json::from_str(&payload) {
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
                        match post_frames(http, url, lease.as_deref(), session, events).await {
                            PostOutcome::Sent => {}
                            PostOutcome::Reattach | PostOutcome::Fault => {
                                return Attach::TransportFault
                            }
                            PostOutcome::Stop => {
                                return Attach::Fin(FinAction::Stop { is_error: true })
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Extracts one SSE event's data payload; comments (`: ping`) yield `None`.
fn sse_data(block: &str) -> Option<String> {
    let mut data: Option<String> = None;
    for line in block.lines() {
        let Some(rest) = line.strip_prefix("data:") else {
            continue;
        };
        let rest = rest.strip_prefix(' ').unwrap_or(rest);
        match &mut data {
            None => data = Some(rest.to_owned()),
            Some(existing) => {
                existing.push('\n');
                existing.push_str(rest);
            }
        }
    }
    data
}

/// Posts every pending frame, one POST in flight at a time.
async fn post_frames(
    http: &reqwest::Client,
    url: &str,
    lease: Option<&str>,
    session: &Mutex<Session>,
    events: &mpsc::UnboundedSender<Event>,
) -> PostOutcome {
    let Some(lease) = lease else {
        // A lease-less stream cannot write; commands stay queued.
        return PostOutcome::Sent;
    };
    let mut refusals = 0u32;
    loop {
        let frame = session.lock().await.next_frame();
        let Some(frame) = frame else {
            return PostOutcome::Sent;
        };
        match post_statements(http, url, lease, frame.cmd).await {
            Ok(None) => {}
            Ok(Some(refusal)) => {
                refusals += 1;
                if refusals > 3 {
                    return PostOutcome::Fault;
                }
                let retry = refusal
                    .syn
                    .as_ref()
                    .and_then(|syn| syn.retry)
                    .map(Duration::from_millis);
                let outcome = session.lock().await.handle_packet(refusal);
                let Ok(batch) = outcome else {
                    return PostOutcome::Fault;
                };
                if forward(events, batch).is_err() {
                    return PostOutcome::Stop;
                }
                if let Some(retry) = retry {
                    tokio::time::sleep(retry.min(BACKOFF_CEILING)).await;
                }
            }
            Err(outcome) => return outcome,
        }
    }
}

/// Posts one frame's statements, splitting on 413. Returns a refusal packet
/// for 409 and 429.
async fn post_statements(
    http: &reqwest::Client,
    url: &str,
    lease: &str,
    statements: Vec<Statement>,
) -> Result<Option<Packet>, PostOutcome> {
    let body = match serde_json::to_string(&Frame {
        cmd: statements.clone(),
    }) {
        Ok(body) => body,
        Err(_) => return Err(PostOutcome::Fault),
    };
    let response = http
        .post(format!("{url}/frames"))
        .header(LEASE_HEADER, lease)
        .header("Content-Type", "application/json")
        .timeout(POST_TIMEOUT)
        .body(body)
        .send()
        .await
        .map_err(|_| PostOutcome::Fault)?;
    match response.status().as_u16() {
        200 => Ok(None),
        409 | 429 => {
            let body = response.bytes().await.map_err(|_| PostOutcome::Fault)?;
            let refusal: Packet = serde_json::from_slice(&body).map_err(|_| PostOutcome::Fault)?;
            Ok(Some(refusal))
        }
        423 => Err(PostOutcome::Reattach),
        413 => {
            if statements.len() < 2 {
                return Err(PostOutcome::Stop);
            }
            let (first, second) = statements.split_at(statements.len() / 2);
            match Box::pin(post_statements(http, url, lease, first.to_vec())).await? {
                None => Box::pin(post_statements(http, url, lease, second.to_vec())).await,
                Some(refusal) => Ok(Some(refusal)),
            }
        }
        400 => Err(PostOutcome::Stop),
        _ => Err(PostOutcome::Fault),
    }
}

#[cfg(test)]
mod tests {
    use super::sse_data;

    #[test]
    fn parses_data_events_and_skips_comments() {
        assert_eq!(
            sse_data("data: {\"idle\":true}"),
            Some("{\"idle\":true}".into())
        );
        assert_eq!(sse_data(": ping"), None);
        assert_eq!(sse_data("data: a\ndata: b"), Some("a\nb".into()));
        assert_eq!(sse_data("data:tight"), Some("tight".into()));
    }
}

//! Application headers on the WebSocket transport, against a scripted
//! server: they ride in the first message of every attach, never in the URL
//! or on the upgrade request, and an authorization refusal stops the
//! transport.

#![cfg(feature = "ws")]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use statewire::transport::ws::{connect_with_headers, HeaderMap, HeaderValue, WsError};
use statewire::{Config, Event, FinAction, FinReason, ProtocolOffer, VersionRange, WIRE_VERSION};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const HELLO: &str = r#"{"syn":{"version":"2026-09-13","protocols":[{"name":"default","version":"2026-09-13","seq":0}]},"ops":[{"op":"mount","protocol":"default","main":true,"value":{}}]}"#;

fn config() -> Config {
    Config {
        client_id: "0123456789abcdef0123456789abcdef".into(),
        wire: VersionRange::exact(WIRE_VERSION),
        offers: vec![ProtocolOffer {
            name: "default".into(),
            range: VersionRange::exact(WIRE_VERSION),
            optional: false,
        }],
    }
}

fn credentials() -> HeaderMap {
    let mut headers = HeaderMap::new();
    let mut token = HeaderValue::from_static("Bearer fixture-token");
    token.set_sensitive(true);
    headers.insert("authorization", token);
    headers.append("x-app-account", HeaderValue::from_static("alice"));
    headers.append("x-app-account", HeaderValue::from_static("bob"));
    headers
}

/// Accepts one upgrade; returns the socket, the request URI and whether the
/// upgrade request carried any application header.
async fn accept(listener: &TcpListener) -> (WebSocketStream<TcpStream>, String, bool) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut uri = String::new();
    let mut leaked = false;
    let socket =
        tokio_tungstenite::accept_hdr_async(stream, |request: &Request, mut response: Response| {
            uri = request.uri().to_string();
            leaked = request.headers().contains_key("authorization")
                || request.headers().contains_key("x-app-account");
            response
                .headers_mut()
                .insert("sec-websocket-protocol", "statewire.v1".parse().unwrap());
            Ok(response)
        })
        .await
        .unwrap();
    (socket, uri, leaked)
}

async fn first_message(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("no first message")
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

fn assert_carries_credentials(first: &Value) {
    let headers = first["headers"].as_object().unwrap();
    assert_eq!(headers["authorization"], "Bearer fixture-token");
    assert_eq!(headers["x-app-account"], "alice, bob");
    assert!(headers["Statewire-Version"]
        .as_str()
        .unwrap()
        .contains(WIRE_VERSION));
    assert!(headers["Statewire-Protocol"]
        .as_str()
        .unwrap()
        .contains("default"));
}

async fn finished(events: &mut UnboundedReceiver<Event>) -> (FinReason, FinAction) {
    loop {
        match timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
        {
            Some(Event::Finished { fin, action }) => return (fin.reason, action),
            Some(_) => {}
            None => panic!("event stream ended without a fin"),
        }
    }
}

async fn ends(events: &mut UnboundedReceiver<Event>) {
    timeout(Duration::from_secs(3), async {
        while events.recv().await.is_some() {}
    })
    .await
    .expect("the transport must stop");
}

#[tokio::test]
async fn headers_ride_every_attach_and_a_refusal_stops_the_transport() {
    for (reason, action) in [
        ("unauthorized", FinAction::RefreshCredentials),
        ("forbidden", FinAction::Stop { is_error: true }),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/thread/ws", listener.local_addr().unwrap());
        let (_client, mut events) = connect_with_headers(&url, config(), credentials())
            .await
            .unwrap();

        // First attach: accepted, then the host asks for a reconnect.
        let (mut socket, uri, leaked) = accept(&listener).await;
        assert_eq!(uri, "/thread/ws?client=0123456789abcdef0123456789abcdef");
        assert!(!leaked, "application headers must stay off the upgrade");
        assert_carries_credentials(&first_message(&mut socket).await);
        socket.send(Message::text(HELLO)).await.unwrap();
        socket
            .send(Message::text(r#"{"fin":{"reason":"reconnect"}}"#))
            .await
            .unwrap();
        assert!(matches!(
            finished(&mut events).await,
            (FinReason::Reconnect, FinAction::Reconnect { .. })
        ));

        // Reattach: the same headers again; this time the host refuses them.
        let (mut socket, _, leaked) = accept(&listener).await;
        assert!(!leaked);
        assert_carries_credentials(&first_message(&mut socket).await);
        socket
            .send(Message::text(
                json!({"fin": {"reason": reason}}).to_string(),
            ))
            .await
            .unwrap();
        let (_, got) = finished(&mut events).await;
        assert_eq!(got, action);
        ends(&mut events).await;
        assert!(
            timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err(),
            "a refused credential must not be retried"
        );
    }
}

#[tokio::test]
async fn an_upgrade_refusal_stops_without_retrying() {
    for status in [401, 403, 404] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (_client, mut events) = connect_with_headers(&url, config(), credentials())
            .await
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 {status} Refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        ends(&mut events).await;
        assert!(timeout(Duration::from_millis(400), listener.accept())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn transport_headers_are_reserved() {
    for name in [
        "statewire-version",
        "statewire-protocol",
        "statewire-lease",
        "sec-websocket-protocol",
        "host",
        "upgrade",
        "connection",
        "content-length",
    ] {
        let mut headers = credentials();
        headers.insert(
            statewire::transport::ws::HeaderName::from_static(name),
            HeaderValue::from_static("override"),
        );
        match connect_with_headers("ws://127.0.0.1:1/ws", config(), headers).await {
            Err(WsError::ReservedHeader(reserved)) => assert_eq!(reserved, name),
            _ => panic!("{name} must be reserved"),
        }
    }
    let mut headers = HeaderMap::new();
    let mut secret = HeaderValue::from_bytes(b"secret \xff").unwrap();
    secret.set_sensitive(true);
    headers.insert("authorization", secret);
    match connect_with_headers("ws://127.0.0.1:1/ws", config(), headers).await {
        Err(error @ WsError::InvalidHeader(_)) => {
            assert!(!error.to_string().contains("secret"));
        }
        _ => panic!("a non-text value must be refused"),
    }
}

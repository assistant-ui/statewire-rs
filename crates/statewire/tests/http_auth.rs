#![cfg(feature = "http")]

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue};
use statewire::transport::http::{connect_with_headers, HttpError};
use statewire::{Config, Event, ProtocolOffer, VersionRange, WIRE_VERSION};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

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
    headers.insert("x-app-account", HeaderValue::from_static("alice"));
    headers
}

async fn read_headers(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
        assert!(bytes.len() < 8192);
    }
    String::from_utf8(bytes).unwrap().to_ascii_lowercase()
}

#[tokio::test]
async fn headers_reach_streams_and_frames_and_revocation_stops_without_retry() {
    for status in [401, 403] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/threads/main", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let stream_headers = read_headers(&mut stream).await;
            assert!(stream_headers.starts_with("get /threads/main/stream "));
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nStatewire-Lease: fixture-lease\r\nConnection: close\r\n\r\ndata: {\"syn\":{\"version\":\"2026-09-13\",\"protocols\":[{\"name\":\"default\",\"version\":\"2026-09-13\",\"seq\":0}]},\"ops\":[{\"op\":\"mount\",\"protocol\":\"default\",\"main\":true,\"value\":{}}]}\n\n").await.unwrap();
            let (mut post, _) = listener.accept().await.unwrap();
            let frame_headers = read_headers(&mut post).await;
            assert!(frame_headers.starts_with("post /threads/main/frames "));
            assert!(frame_headers.contains("statewire-lease: fixture-lease"));
            for headers in [&stream_headers, &frame_headers] {
                assert!(headers.contains("authorization: bearer fixture-token"));
                assert!(headers.contains("x-app-account: alice"));
            }
            post.write_all(
                format!(
                    "HTTP/1.1 {status} Denied\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            assert!(timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err());
        });
        let (client, mut events) = connect_with_headers(&url, config(), credentials())
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while !matches!(events.recv().await, Some(Event::Connected { .. })) {}
        })
        .await
        .unwrap();
        client.command("default", "send", vec![]).await;
        timeout(Duration::from_secs(3), async {
            while events.recv().await.is_some() {}
        })
        .await
        .expect("authorization rejection must end the transport");
        server.await.unwrap();
    }
}

#[tokio::test]
async fn refuses_redirects_instead_of_forwarding_credentials() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/thread", listener.local_addr().unwrap());
    let location = format!("http://{}/stolen", destination.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_headers(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    let (_client, mut events) = connect_with_headers(&url, config(), credentials())
        .await
        .unwrap();
    assert!(timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
        .is_none());
    assert!(timeout(Duration::from_millis(200), destination.accept())
        .await
        .is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn validates_urls_and_protects_protocol_headers() {
    for url in [
        "ftp://example.com",
        "https://user:secret@example.com/thread",
        "https://example.com/thread?token=secret",
        "https://example.com/thread#secret",
    ] {
        assert!(matches!(
            connect_with_headers(url, config(), credentials()).await,
            Err(HttpError::InvalidUrl(_))
        ));
    }
    for name in [
        "statewire-lease",
        "statewire-protocols",
        "host",
        "content-length",
        "accept",
    ] {
        let mut headers = credentials();
        headers.insert(name, HeaderValue::from_static("override"));
        assert!(matches!(
            connect_with_headers("http://localhost:1/thread", config(), headers).await,
            Err(HttpError::ReservedHeader(_))
        ));
    }
}

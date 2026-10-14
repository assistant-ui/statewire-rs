//! Live round trip against a real statewire host (the node counter fixture).
//!
//! Points at `STATEWIRE_WS_URL` when set; otherwise spawns
//! `tests/fixtures/counter-host/server.mjs` and requires Node.js and the fixture dependencies.

#![cfg(feature = "ws")]

use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use statewire::transport::ws::{connect, generate_client_id};
use statewire::{Config, Event, ProtocolOffer, Verdict, VersionRange, WIRE_VERSION};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

struct Fixture {
    url: String,
    child: Option<tokio::process::Child>,
}

async fn fixture() -> Fixture {
    if let Ok(url) = std::env::var("STATEWIRE_WS_URL") {
        return Fixture { url, child: None };
    }
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/counter-host");
    assert!(
        dir.join("node_modules/statewire").exists(),
        "fixture dependencies missing: run npm ci --prefix tests/fixtures/counter-host"
    );
    let mut child = tokio::process::Command::new("node")
        .arg("server.mjs")
        .arg("0")
        .current_dir(&dir)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to start the counter fixture; install Node.js");
    let stdout = child.stdout.take().expect("fixture stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("counter fixture did not start within 10 seconds")
        .expect("could not read counter fixture output")
        .expect("counter fixture exited before listening");
    let port = line
        .strip_prefix("listening ")
        .expect("unexpected counter fixture startup output")
        .trim()
        .to_owned();
    Fixture {
        url: format!("ws://127.0.0.1:{port}/ws"),
        child: Some(child),
    }
}

fn config() -> Config {
    Config {
        client_id: generate_client_id(),
        wire: VersionRange::exact(WIRE_VERSION),
        offers: vec![ProtocolOffer {
            name: "default".into(),
            range: VersionRange::exact(WIRE_VERSION),
            optional: false,
        }],
    }
}

async fn next_event(events: &mut UnboundedReceiver<Event>) -> Event {
    timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("timed out waiting for an event")
        .expect("event stream ended")
}

/// A ws-only build must have a TLS backend and crypto provider even when
/// reqwest (the HTTP feature) is not present to enable them transitively.
#[tokio::test]
async fn secure_websocket_starts_a_tls_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://{}/ws", listener.local_addr().unwrap());
    let (client, _events) = connect(&url, config()).await.unwrap();
    timeout(Duration::from_secs(5), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut prefix = [0; 3];
        stream.read_exact(&mut prefix).await.unwrap();
        // TLS handshake record, followed by the TLS major version.
        assert_eq!(&prefix[..2], &[0x16, 0x03]);
    })
    .await
    .expect("WSS did not send a TLS ClientHello");
    drop(client);
}

#[tokio::test]
async fn increments_the_live_counter() {
    let mut fixture = fixture().await;

    let (client, mut events) = connect(&fixture.url, config()).await.unwrap();

    let connected = next_event(&mut events).await;
    let Event::Connected { selections } = &connected else {
        panic!("expected Connected, got {connected:?}");
    };
    assert!(selections.iter().any(|s| s.name == "default"));
    assert_eq!(
        client.main_value("default").await,
        Some(json!({"count": 0}))
    );

    let seq = client.command("default", "increment", vec![json!(1)]).await;
    assert_eq!(seq, 1);

    let mut saw_state = false;
    let mut saw_result = false;
    while !(saw_state && saw_result) {
        match next_event(&mut events).await {
            Event::StateChanged => saw_state = true,
            Event::CommandUpdate { answer, terminal } if answer.seq == seq => {
                if terminal {
                    assert_eq!(answer.verdict, Some(Verdict::Result));
                    saw_result = true;
                }
            }
            Event::Idle(_) => {}
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert_eq!(
        client.main_value("default").await,
        Some(json!({"count": 1}))
    );

    let unknown = client.command("default", "no-such-command", vec![]).await;
    loop {
        if let Event::CommandUpdate {
            answer,
            terminal: true,
        } = next_event(&mut events).await
        {
            if answer.seq == unknown {
                assert_eq!(answer.verdict, Some(Verdict::Rejected));
                assert_eq!(answer.code.as_deref(), Some("unknown-command"));
                break;
            }
        }
    }

    let commands = client
        .command("default", "statewire/commands", vec![])
        .await;
    loop {
        if let Event::CommandUpdate {
            answer,
            terminal: true,
        } = next_event(&mut events).await
        {
            if answer.seq == commands {
                assert_eq!(answer.verdict, Some(Verdict::Result));
                let names: Vec<String> =
                    serde_json::from_value(answer.payload.clone().unwrap_or(Value::Null)).unwrap();
                assert!(names.contains(&"statewire/commands".to_owned()));
                break;
            }
        }
    }

    if let Some(child) = fixture.child.as_mut() {
        let _ = child.kill().await;
    }
}

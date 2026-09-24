//! Live round trip against a real statewire host (the node counter fixture).
//!
//! Points at `STATEWIRE_WS_URL` when set; otherwise spawns
//! `tests/fixtures/counter-host/server.mjs` and skips when node or the
//! fixture's dependencies are unavailable.

#![cfg(feature = "ws")]

use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use statewire::transport::ws::{connect, generate_client_id};
use statewire::{Config, Event, ProtocolOffer, Verdict, VersionRange, WIRE_VERSION};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

struct Fixture {
    url: String,
    child: Option<tokio::process::Child>,
}

async fn fixture() -> Option<Fixture> {
    if let Ok(url) = std::env::var("STATEWIRE_WS_URL") {
        return Some(Fixture { url, child: None });
    }
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/counter-host");
    if !dir.join("node_modules").exists() {
        eprintln!("skipping: fixture dependencies not installed (run npm install)");
        return None;
    }
    let mut child = tokio::process::Command::new("node")
        .arg("server.mjs")
        .arg("0")
        .current_dir(&dir)
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .ok()?
        .ok()??;
    let port = line.strip_prefix("listening ")?.trim().to_owned();
    Some(Fixture {
        url: format!("ws://127.0.0.1:{port}/ws"),
        child: Some(child),
    })
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

#[tokio::test]
async fn increments_the_live_counter() {
    let Some(mut fixture) = fixture().await else {
        return;
    };

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

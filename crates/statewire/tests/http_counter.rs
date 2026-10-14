//! Live round trip against a real statewire host over StatewireHttp.
//!
//! Points at `STATEWIRE_HTTP_URL` (the instance URL, without `/stream`) when
//! set; otherwise spawns the counter fixture and requires Node.js and the fixture dependencies.

#![cfg(feature = "http")]

use std::process::Stdio;
use std::time::Duration;

use serde_json::json;
use statewire::transport::http::{connect, generate_client_id};
use statewire::{Config, Event, ProtocolOffer, Verdict, VersionRange, WIRE_VERSION};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::timeout;

struct Fixture {
    url: String,
    child: Option<tokio::process::Child>,
}

async fn fixture() -> Fixture {
    if let Ok(url) = std::env::var("STATEWIRE_HTTP_URL") {
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
        url: format!("http://127.0.0.1:{port}/threads/t1"),
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

#[tokio::test]
async fn increments_the_live_counter_over_http() {
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

    if let Some(child) = fixture.child.as_mut() {
        let _ = child.kill().await;
    }
}

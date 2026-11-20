//! The value rules shared with harness-sdk's TypeScript and Python statewire:
//! `tests/fixtures/values.json`, a copy of
//! `packages/statewire/core/tests/fixtures/values.json` in harness-sdk. A
//! `serde_json::Value` can't hold `undefined`, holes, functions, BigInt,
//! class instances or cycles, so those cases are skipped; serde writes a
//! non-finite float as `null`.

use serde_json::{json, Value};
use statewire::document::PathSeg;
use statewire::values::{check_write, parse, receive_packet};
use statewire::{Config, Packet, ProtocolOffer, Session, VersionRange};

fn table() -> Value {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/values.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The case's input as Rust can hold it, or `None` when it can't.
fn build(node: &Value) -> Option<Value> {
    Some(match node {
        Value::Array(items) => Value::Array(items.iter().map(build).collect::<Option<_>>()?),
        Value::Object(map) => match map.get("$").and_then(Value::as_str) {
            Some("NaN") => serde_json::to_value(f64::NAN).unwrap(),
            Some("Infinity") => serde_json::to_value(f64::INFINITY).unwrap(),
            Some("-Infinity") => serde_json::to_value(f64::NEG_INFINITY).unwrap(),
            Some("unsafe") => json!(1u64 << 53),
            Some(_) => return None,
            None => Value::Object(
                map.iter()
                    .map(|(k, v)| Some((k.clone(), build(v)?)))
                    .collect::<Option<_>>()?,
            ),
        },
        other => other.clone(),
    })
}

fn path_of(value: &Value) -> Vec<PathSeg> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|seg| match seg {
            Value::String(key) => PathSeg::Key(key.clone()),
            other => PathSeg::Index(other.as_u64().unwrap() as usize),
        })
        .collect()
}

fn session() -> Session {
    Session::new(Config {
        client_id: "0123456789abcdef0123456789abcdef".into(),
        wire: VersionRange::exact("2026-09-13"),
        offers: vec![ProtocolOffer {
            name: "default".into(),
            range: VersionRange::exact("2026-09-13"),
            optional: false,
        }],
    })
}

#[test]
fn writes_follow_the_shared_table() {
    let table = table();
    let mut ran = 0;
    for case in table["cases"].as_array().unwrap() {
        let Some(input) = build(&case["input"]) else {
            continue;
        };
        ran += 1;
        let name = case["name"].as_str().unwrap();
        let sent = session().command("default", "m", vec![json!(1), input.clone()]);
        match case.get("value") {
            Some(expected) => {
                assert_eq!(&input, expected, "{name}");
                assert_eq!(check_write(&input, ""), Ok(()), "{name}");
                assert!(sent.is_ok(), "{name}");
            }
            None => {
                let mut at = vec![PathSeg::Index(1)];
                at.extend(path_of(&case["error"]));
                assert_eq!(sent.unwrap_err().path, at, "{name}");
                assert_eq!(
                    check_write(&input, "").unwrap_err().path,
                    path_of(&case["error"]),
                    "{name}"
                );
            }
        }
    }
    assert_eq!(ran, 3, "plain data, non-finite numbers, unsafe integers");
}

#[test]
fn a_write_error_names_the_method_and_path() {
    let error = session()
        .command("default", "rsvp", vec![json!({"n": 1u64 << 53})])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "statewire: can't encode the number 9007199254740992 at rsvp params[0].n: integers must be within the safe integer range, so every client reads them exactly"
    );
}

/// The ways a host hands a client a value: a snapshot, a patch, a result.
fn received(json_value: &str) -> Vec<Result<Value, String>> {
    let packets = [
        format!(
            r#"{{"ops":[{{"op":"mount","doc":0,"protocol":"default","main":true,"value":{json_value}}}]}}"#
        ),
        format!(r#"{{"ops":[{{"op":"replace","doc":0,"path":["x"],"value":{json_value}}}]}}"#),
        format!(
            r#"{{"cmd":[{{"protocol":"default","seq":1,"type":"result","payload":{json_value}}}]}}"#
        ),
    ];
    packets
        .iter()
        .map(|text| {
            let mut packet: Packet = parse(text).map_err(|e| e.to_string())?;
            receive_packet(&mut packet).map_err(|e| e.to_string())?;
            Ok(match (&packet.ops, &packet.cmd) {
                (Some(ops), _) => match &ops[0] {
                    statewire::Op::Mount { value, .. } | statewire::Op::Replace { value, .. } => {
                        value.clone()
                    }
                    _ => unreachable!(),
                },
                (_, Some(cmd)) => cmd[0].payload.clone().unwrap(),
                _ => unreachable!(),
            })
        })
        .collect()
}

#[test]
fn received_values_follow_the_shared_table() {
    let table = table();
    for case in table["received"]["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        for outcome in received(case["json"].as_str().unwrap()) {
            match case.get("value") {
                Some(expected) => assert_eq!(outcome.as_ref(), Ok(expected), "{name}"),
                None => assert!(outcome.is_err(), "{name}: {outcome:?}"),
            }
        }
    }
}

#[test]
fn a_patch_under_constructor_strips_prototype() {
    let mut packet: Packet = parse(
        r#"{"ops":[{"op":"replace","path":["a","constructor"],"value":{"prototype":1,"ok":2}}]}"#,
    )
    .unwrap();
    receive_packet(&mut packet).unwrap();
    let Some(ops) = &packet.ops else {
        unreachable!()
    };
    let statewire::Op::Replace { value, .. } = &ops[0] else {
        unreachable!()
    };
    assert_eq!(value, &json!({"ok": 2}));
}

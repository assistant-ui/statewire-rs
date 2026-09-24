//! Wire envelopes: clients send frames; servers send packets.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::document::MetaEntry;
use crate::patch::Op;
use crate::version::LaneRecord;

/// Statements per frame, across lanes.
pub const MAX_COMMANDS_PER_FRAME: usize = 256;
/// Encoded ephemeral document value limit in bytes.
pub const MAX_EPHEMERAL_DOCUMENT_BYTES: usize = 64 * 1024;
/// Encoded frame limit in bytes of UTF-8.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Admitted durable-lane commands per client that have not fully released.
pub const MAX_UNRELEASED_COMMANDS: usize = 256;

/// A client message carrying command statements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame {
    pub cmd: Vec<Statement>,
}

/// One command request: an application command with positional arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub seq: u64,
    pub protocol: String,
    pub method: String,
    pub params: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Vec<MetaEntry>>,
}

/// A server message. Clients ignore unknown keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Packet {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syn: Option<Syn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<Vec<Op>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<Answer>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fin: Option<Fin>,
}

impl Packet {
    /// A WebSocket heartbeat is an empty object.
    pub fn is_heartbeat(&self) -> bool {
        self == &Packet::default()
    }
}

/// Connection facts or admission watermarks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Syn {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocols: Option<Vec<SynProtocol>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<u64>,
}

/// One lane entry in a `syn`. The hello carries `version` and `seq`;
/// refusals and watermark refreshes carry `name` and `seq` only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SynProtocol {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<LaneRecord>,
}

/// A command answer: admission, progress, or a result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Answer {
    pub protocol: String,
    pub seq: u64,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Vec<MetaEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dur: Option<bool>,
}

/// `applied` is intermediate; every other verdict settles the command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    Applied,
    Result,
    Rejected,
    Failed,
    Crashed,
    Expired,
}

impl Verdict {
    pub fn is_terminal(self) -> bool {
        self != Verdict::Applied
    }
}

/// Connection ending.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fin {
    pub reason: FinReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Vec<MetaEntry>>,
}

/// Why a connection is ending. Unknown reasons follow the `error` path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinReason {
    #[serde(rename = "reconnect")]
    Reconnect,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "protocol-error")]
    ProtocolError,
    #[serde(rename = "unauthorized")]
    Unauthorized,
    #[serde(rename = "forbidden")]
    Forbidden,
    #[serde(rename = "gone")]
    Gone,
    #[serde(rename = "idle")]
    Idle,
    #[serde(untagged)]
    Unknown(String),
}

impl FinReason {
    /// Whether the client should reconnect with backoff.
    pub fn reconnects(&self) -> bool {
        matches!(
            self,
            FinReason::Reconnect | FinReason::Error | FinReason::Unknown(_)
        )
    }

    /// Whether this finish fails unanswered durable-lane commands.
    pub fn fails_pending(&self) -> bool {
        matches!(
            self,
            FinReason::Gone | FinReason::Forbidden | FinReason::ProtocolError
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_hello_packet() {
        let packet: Packet = serde_json::from_str(
            r#"{"syn": {"version": "2026-09-13", "protocols": [
               {"name": "harness-sdk", "version": "2026-09-13", "seq": 41},
               {"name": "harness-sdk/interest", "version": "2026-09-13", "record": "ephemeral", "seq": 0}],
               "idle": true, "id": "abc"}, "ops": [], "cmd": []}"#,
        )
        .unwrap();
        let syn = packet.syn.unwrap();
        assert_eq!(syn.version.as_deref(), Some("2026-09-13"));
        let protocols = syn.protocols.unwrap();
        assert_eq!(protocols[0].seq, Some(41));
        assert_eq!(protocols[1].record, Some(LaneRecord::Ephemeral));
        assert_eq!(packet.ops.unwrap().len(), 0);
    }

    #[test]
    fn parses_state_change_with_answer() {
        let packet: Packet = serde_json::from_str(
            r#"{"ops":[{"op":"replace","path":["count"],"value":1}],
                "cmd":[{"protocol":"default","seq":1,"type":"applied"}]}"#,
        )
        .unwrap();
        assert_eq!(packet.cmd.unwrap()[0].verdict, Some(Verdict::Applied));
    }

    #[test]
    fn statement_serializes_like_the_spec() {
        let statement = Statement {
            seq: 1,
            protocol: "default".into(),
            method: "increment".into(),
            params: vec![json!(1)],
            meta: None,
        };
        assert_eq!(
            serde_json::to_string(&Frame {
                cmd: vec![statement]
            })
            .unwrap(),
            r#"{"cmd":[{"seq":1,"protocol":"default","method":"increment","params":[1]}]}"#
        );
    }

    #[test]
    fn parses_dur_acknowledgment_without_verdict() {
        let answer: Answer =
            serde_json::from_str(r#"{"protocol": "harness-sdk", "seq": 7, "dur": true}"#).unwrap();
        assert_eq!(answer.verdict, None);
        assert_eq!(answer.dur, Some(true));
    }

    #[test]
    fn parses_refusal_and_fin() {
        let refusal: Packet = serde_json::from_str(
            r#"{"syn": {"protocols": [{"name": "harness-sdk", "seq": 41}], "retry": 1000}}"#,
        )
        .unwrap();
        assert_eq!(refusal.syn.unwrap().retry, Some(1000));

        let fin: Packet = serde_json::from_str(
            r#"{"fin": {"reason": "reconnect", "code": "overloaded", "retry": 5000}}"#,
        )
        .unwrap();
        let fin = fin.fin.unwrap();
        assert_eq!(fin.reason, FinReason::Reconnect);
        assert!(fin.reason.reconnects());
    }

    #[test]
    fn unknown_fin_reason_follows_error_path() {
        let fin: Fin = serde_json::from_str(r#"{"reason": "quantum-drift"}"#).unwrap();
        assert_eq!(fin.reason, FinReason::Unknown("quantum-drift".into()));
        assert!(fin.reason.reconnects());
        assert!(!fin.reason.fails_pending());
    }

    #[test]
    fn heartbeat_is_an_empty_object() {
        let packet: Packet = serde_json::from_str("{}").unwrap();
        assert!(packet.is_heartbeat());
    }

    #[test]
    fn unknown_packet_keys_are_ignored() {
        let packet: Packet = serde_json::from_str(r#"{"idle": true, "future": 9}"#).unwrap();
        assert_eq!(packet.idle, Some(true));
    }

    #[test]
    fn terminal_verdicts() {
        assert!(!Verdict::Applied.is_terminal());
        for verdict in [
            Verdict::Result,
            Verdict::Rejected,
            Verdict::Failed,
            Verdict::Crashed,
            Verdict::Expired,
        ] {
            assert!(verdict.is_terminal());
        }
    }
}

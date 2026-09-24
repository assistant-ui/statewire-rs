//! The `run/*` commands and their wire forms.
//!
//! A statewire host registers each command as a method under its own name
//! whose one param is the object without `type`; [`Command::statement`]
//! yields that `(method, params)` pair for `Session::command`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Serde adapter distinguishing an absent field from an explicit `null`:
/// `None` is absent (pair with `skip_serializing_if`), `Some(None)` is
/// `null`, `Some(Some(v))` is a value.
pub mod double_option {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: Serialize, S: Serializer>(
        value: &Option<Option<T>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(inner) => inner.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Option<T>>, D::Error> {
        Option::<T>::deserialize(deserializer).map(Some)
    }
}

/// A part of a user send.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SendPart {
    Text {
        text: String,
    },
    File {
        #[serde(rename = "mediaType")]
        media_type: String,
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

/// A user message as submitted and queued.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserMessage {
    /// Client-generated, globally unique, never reused.
    pub id: String,
    pub role: Role,
    pub parts: Vec<SendPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
}

impl UserMessage {
    /// A one-text-part user message.
    pub fn text(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            role: Role::User,
            parts: vec![SendPart::Text { text: text.into() }],
            metadata: None,
        }
    }
}

/// Placement inside a lane. Absent means the default; `Some(None)` names a
/// queue boundary (`null` on the wire: front for `insert_after`, end for
/// `insert_before`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuePlacement {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "double_option"
    )]
    pub insert_after: Option<Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "double_option"
    )]
    pub insert_before: Option<Option<String>>,
}

/// The reasons a `run/*` command settles `rejected`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RejectionReason {
    WrongState,
    UnknownId,
    DuplicateId,
    AlreadyDispatched,
    UnknownAnchor,
    WrongAnchor,
    NotAdjacent,
    AlreadyAnswered,
    QueueFull,
    CapabilityMissing,
    NotOwner,
    InvalidMessage,
    #[serde(untagged)]
    Unknown(String),
}

/// A rejection verdict's payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rejection {
    pub reason: RejectionReason,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// An `accepted` result: `null`, or `{ runId }` when a run-creating send
/// merged into a live run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Accepted {
    pub run_id: Option<String>,
}

/// A command with its statewire method name and params object.
pub trait Command: Serialize {
    const METHOD: &'static str;

    /// The `(method, params)` pair for `Session::command`.
    fn statement(&self) -> (&'static str, Vec<Value>) {
        let params = serde_json::to_value(self).expect("command serializes");
        (Self::METHOD, vec![params])
    }
}

/// `run/enqueue`: add a message (mint or target form) or move a queued item
/// to the regular queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEnqueue {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<UserMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Mint form only; `Some(None)` asserts an empty thread.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "double_option"
    )]
    pub run_anchor_message_id: Option<Option<String>>,
    #[serde(flatten)]
    pub placement: QueuePlacement,
}

impl Command for RunEnqueue {
    const METHOD: &'static str = "run/enqueue";
}

/// `run/steer`: like `run/enqueue`, landing in the steer lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSteer {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<UserMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "double_option"
    )]
    pub run_anchor_message_id: Option<Option<String>>,
    #[serde(flatten)]
    pub placement: QueuePlacement,
}

impl Command for RunSteer {
    const METHOD: &'static str = "run/steer";
}

/// `run/dequeue`: remove one queued item from either lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDequeue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub message_id: String,
}

impl Command for RunDequeue {
    const METHOD: &'static str = "run/dequeue";
}

/// `run/edit`: replace `source_id` on a new branch (requires `rewind`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEdit {
    /// The live run's id when redirecting it; a fresh client uuid when idle.
    pub run_id: String,
    pub message: UserMessage,
    pub source_id: String,
    /// Required; asserts `source_id`'s parent (`None` inner = root).
    pub run_anchor_message_id: Option<String>,
}

impl Command for RunEdit {
    const METHOD: &'static str = "run/edit";
}

/// `run/reload`: regenerate an assistant message (requires `rewind`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReload {
    pub run_id: String,
    pub source_id: String,
    pub run_anchor_message_id: Option<String>,
}

impl Command for RunReload {
    const METHOD: &'static str = "run/reload";
}

/// `run/stop`: stop the live run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStop {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Command for RunStop {
    const METHOD: &'static str = "run/stop";
}

/// `run/continue`: resume from error/stop, draining the steer lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunContinue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

impl Command for RunContinue {
    const METHOD: &'static str = "run/continue";
}

/// `run/input`: answer one pending input request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub request_id: String,
    pub response: Map<String, Value>,
}

impl Command for RunInput {
    const METHOD: &'static str = "run/input";
}

/// The `tool-approval` response shape for `run/input`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolApprovalResponse {
    pub decision: ApprovalDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_args: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalDecision {
    Approve,
    Reject,
    Edit,
    Respond,
}

/// The `tool-call` response shape for `run/input` (`output` required).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallResponse {
    pub output: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn enqueue_mint_form_serializes_like_the_spec() {
        let command = RunEnqueue {
            run_id: "r1".into(),
            message: Some(UserMessage::text("m1", "add a chart")),
            message_id: None,
            run_anchor_message_id: Some(None),
            placement: QueuePlacement::default(),
        };
        let (method, params) = command.statement();
        assert_eq!(method, "run/enqueue");
        assert_eq!(
            params[0],
            json!({
                "runId": "r1",
                "message": {"id": "m1", "role": "user", "parts": [{"type": "text", "text": "add a chart"}]},
                "runAnchorMessageId": null
            })
        );
    }

    #[test]
    fn placement_distinguishes_null_absent_and_value() {
        let front = QueuePlacement {
            insert_after: Some(None),
            insert_before: None,
        };
        assert_eq!(
            serde_json::to_value(&front).unwrap(),
            json!({"insertAfter": null})
        );

        let strict = QueuePlacement {
            insert_after: Some(Some("a".into())),
            insert_before: Some(Some("b".into())),
        };
        assert_eq!(
            serde_json::to_value(&strict).unwrap(),
            json!({"insertAfter": "a", "insertBefore": "b"})
        );
        let round: QueuePlacement = serde_json::from_value(json!({"insertAfter": null})).unwrap();
        assert_eq!(round, front);
    }

    #[test]
    fn move_form_omits_message() {
        let command = RunSteer {
            run_id: "r1".into(),
            message: None,
            message_id: Some("m3".into()),
            run_anchor_message_id: None,
            placement: QueuePlacement::default(),
        };
        assert_eq!(
            command.statement().1[0],
            json!({"runId": "r1", "messageId": "m3"})
        );
    }

    #[test]
    fn stop_and_input_serialize() {
        let stop = RunStop {
            run_id: "r1".into(),
            epoch: Some(3),
            reason: Some("user".into()),
        };
        assert_eq!(
            stop.statement().1[0],
            json!({"runId": "r1", "epoch": 3, "reason": "user"})
        );

        let approval = ToolApprovalResponse {
            decision: ApprovalDecision::Approve,
            edited_args: None,
            message: None,
        };
        let input = RunInput {
            run_id: None,
            request_id: "req1".into(),
            response: serde_json::from_value(serde_json::to_value(&approval).unwrap()).unwrap(),
        };
        assert_eq!(
            input.statement().1[0],
            json!({"requestId": "req1", "response": {"decision": "approve"}})
        );
    }

    #[test]
    fn rejection_reasons_parse_including_unknown() {
        let rejection: Rejection =
            serde_json::from_value(json!({"reason": "queue-full", "max": 8})).unwrap();
        assert_eq!(rejection.reason, RejectionReason::QueueFull);
        assert_eq!(rejection.extra["max"], 8);
        let future: Rejection =
            serde_json::from_value(json!({"reason": "not-yet-invented"})).unwrap();
        assert_eq!(
            future.reason,
            RejectionReason::Unknown("not-yet-invented".into())
        );
    }

    #[test]
    fn accepted_carries_the_surviving_run_id() {
        let merged: Accepted = serde_json::from_value(json!({"runId": "r9"})).unwrap();
        assert_eq!(merged.run_id.as_deref(), Some("r9"));
    }
}

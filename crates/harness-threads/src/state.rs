//! The main document and the runs-protocol state shape.
//!
//! The Python machine publishes unset optional fields as `null`; the TS
//! machine omits the key. Every such field is an `Option` with a default so
//! both forms deserialize.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::commands::UserMessage;

/// The main document: `{ threads, status, runs }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessState {
    pub threads: BTreeMap<String, ThreadState>,
    pub status: RunsStatus,
    /// At most one entry; empty while resting.
    pub runs: Vec<RunState>,
    /// The voice projection; opaque to this crate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voice: Option<Value>,
}

/// One thread's entry in the main document's `threads` map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_id: Option<String>,
    pub status: ThreadStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThreadStatus {
    Idle,
    Submitted,
    Streaming,
}

/// The top-level status: a projection of `runs[0]?.status ?? "ready"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunsStatus {
    Ready,
    Running,
    Stopping,
    Error,
    Stopped,
    InputRequired,
}

/// One run entry, per the runs-protocol state shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunState {
    /// `null` until the entry adopts the run-creating command's id (TS);
    /// set at mount on the Python machine.
    #[serde(default)]
    pub run_id: Option<String>,
    pub status: RunStatus,
    /// Counts the entry's dispatches, bumped at each dispatch.
    pub epoch: u64,
    #[serde(default)]
    pub queue: Vec<LaneItem>,
    #[serde(default)]
    pub steer_queue: Vec<LaneItem>,
    /// The current unapplied batch, or a retracted batch until its settle.
    #[serde(default)]
    pub dispatching: Option<RunDispatch>,
    #[serde(default)]
    pub stopping: Option<Stopping>,
    /// The complete dispatch that runs next.
    #[serde(default)]
    pub next_dispatch: Option<RunDispatch>,
    /// Present in `input-required` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_requests: Option<Vec<InputRequestState>>,
    /// Set only by an unhandled throw; cleared by the next user-initiated run.
    #[serde(default)]
    pub error: Option<RunError>,
    /// The reason carried by `run/stop`; cleared at the next dispatch.
    #[serde(default)]
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    Running,
    Stopping,
    Error,
    Stopped,
    InputRequired,
}

/// An open stop or rewind intent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stopping {
    pub reason: StoppingReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StoppingReason {
    Stop,
    MessageEdit,
    MessageReload,
}

/// A queued `UserMessage` carrying the host-stamped `meta` key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaneItem {
    #[serde(flatten)]
    pub message: UserMessage,
    /// Absent when the stamp carried none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

/// The run's error record: `message` plus the reject payload spread in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunError {
    pub message: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A dispatch record: the batch a run is applying or will apply next.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDispatch {
    pub trigger: DispatchTrigger,
    #[serde(default)]
    pub messages: Vec<LaneItem>,
    /// Rewind dispatches only; `null` means the root.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::commands::double_option"
    )]
    pub rollback_to: Option<Option<String>>,
    /// The run-starting op's meta.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_meta: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_outcomes: Option<Vec<InputOutcome>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DispatchTrigger {
    MessageSend,
    MessageEdit,
    MessageReload,
    InputResume,
    ErrorContinue,
    StopContinue,
    Steer,
}

/// A pending request the run parked on. Extra keys replicate verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputRequest {
    /// The answering key (`run/input`'s `requestId`), unique within the set.
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A request entry on the run; a present `response` marks it answered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputRequestState {
    #[serde(flatten)]
    pub request: InputRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

/// One answered (or unanswered, `response: null`) request riding an
/// `input-resume` dispatch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputOutcome {
    pub request: InputRequest,
    pub response: Option<Map<String, Value>>,
    pub meta: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resting_state_parses() {
        let state: HarnessState =
            serde_json::from_value(json!({"threads": {}, "status": "ready", "runs": []})).unwrap();
        assert_eq!(state.status, RunsStatus::Ready);
        assert!(state.runs.is_empty());
    }

    #[test]
    fn python_nulls_and_ts_omissions_both_parse() {
        let python: RunState = serde_json::from_value(json!({
            "runId": "r1", "status": "running", "epoch": 1,
            "queue": [], "steerQueue": [],
            "dispatching": null, "stopping": null, "nextDispatch": null,
            "error": null, "stopReason": null
        }))
        .unwrap();
        let ts: RunState = serde_json::from_value(json!({
            "runId": "r1", "status": "running", "epoch": 1,
            "queue": [], "steerQueue": []
        }))
        .unwrap();
        assert_eq!(python, ts);
        assert_eq!(python.run_id.as_deref(), Some("r1"));
    }

    #[test]
    fn input_required_entry_parses() {
        let run: RunState = serde_json::from_value(json!({
            "runId": "r2", "status": "input-required", "epoch": 3,
            "queue": [], "steerQueue": [],
            "inputRequests": [
                {"type": "tool-approval", "id": "req1", "toolCallId": "call1"},
                {"type": "tool-call", "id": "req2", "toolCallId": "call2",
                 "response": {"output": "ok"}, "meta": {"by": "phone"}}
            ]
        }))
        .unwrap();
        assert_eq!(run.status, RunStatus::InputRequired);
        let requests = run.input_requests.unwrap();
        assert_eq!(requests[0].request.kind, "tool-approval");
        assert!(requests[0].response.is_none());
        assert!(requests[1].response.is_some());
    }

    #[test]
    fn dispatch_rollback_distinguishes_null_and_absent() {
        let rewind: RunDispatch = serde_json::from_value(json!({
            "trigger": "message-edit", "messages": [], "rollbackTo": null
        }))
        .unwrap();
        assert_eq!(rewind.rollback_to, Some(None));
        let plain: RunDispatch =
            serde_json::from_value(json!({"trigger": "message-send", "messages": []})).unwrap();
        assert_eq!(plain.rollback_to, None);
    }

    #[test]
    fn error_record_keeps_reject_payload() {
        let error: RunError = serde_json::from_value(json!({
            "message": "boom", "reason": "wrong-state", "detail": 4
        }))
        .unwrap();
        assert_eq!(error.message, "boom");
        assert_eq!(error.extra["reason"], "wrong-state");
    }

    #[test]
    fn lane_item_meta_is_optional() {
        let item: LaneItem = serde_json::from_value(json!({
            "id": "m1", "role": "user",
            "parts": [{"type": "text", "text": "hello"}],
            "meta": {"src": "cli"}
        }))
        .unwrap();
        assert_eq!(item.message.id, "m1");
        assert!(item.meta.is_some());
        let round = serde_json::to_value(&item).unwrap();
        assert_eq!(round["parts"][0]["text"], "hello");
        assert_eq!(round["meta"]["src"], "cli");
    }
}

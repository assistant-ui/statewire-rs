//! Message documents: the per-message state mounted while an interest
//! window covers it, keyed `[threadId, ns, messageId]`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A message document as mounted on the wire (no `siblings`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDocument {
    pub id: String,
    pub parent_id: Option<String>,
    pub seq: u64,
    pub role: Role,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// One part of a message. User messages carry `text` and `file`; assistant
/// messages carry `reasoning`, `text`, and `tool`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Part {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<StreamState>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    Reasoning {
        text: String,
        state: StreamState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    File {
        url: String,
        #[serde(rename = "mediaType")]
        media_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    Tool {
        #[serde(rename = "toolInvocationId")]
        tool_invocation_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        input: Map<String, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<Value>,
        #[serde(rename = "isError", default, skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
        #[serde(
            rename = "elapsedSeconds",
            default,
            skip_serializing_if = "Option::is_none"
        )]
        elapsed_seconds: Option<f64>,
        state: ToolState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamState {
    Streaming,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolState {
    #[serde(rename = "streaming")]
    Streaming,
    #[serde(rename = "pendingApproval")]
    PendingApproval,
    #[serde(rename = "done")]
    Done,
}

impl MessageDocument {
    /// Concatenates the message's text parts.
    pub fn text(&self) -> String {
        self.parts
            .iter()
            .filter_map(|part| match part {
                Part::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn assistant_message_with_all_part_kinds_parses() {
        let document: MessageDocument = serde_json::from_value(json!({
            "id": "m2", "parentId": "m1", "seq": 2, "role": "assistant",
            "parts": [
                {"type": "reasoning", "text": "thinking", "state": "done"},
                {"type": "text", "text": "Chart is in.", "state": "streaming"},
                {"type": "tool", "toolInvocationId": "call1", "toolName": "bash",
                 "input": {"command": "pnpm test"}, "state": "pendingApproval"}
            ]
        }))
        .unwrap();
        assert_eq!(document.role, Role::Assistant);
        assert_eq!(document.text(), "Chart is in.");
        assert!(matches!(
            &document.parts[2],
            Part::Tool { state: ToolState::PendingApproval, tool_name, .. } if tool_name == "bash"
        ));
    }

    #[test]
    fn user_message_round_trips() {
        let value = json!({
            "id": "m1", "parentId": null, "seq": 1, "role": "user",
            "parts": [
                {"type": "text", "text": "add a chart"},
                {"type": "file", "url": "https://x/y.png", "mediaType": "image/png"}
            ]
        });
        let document: MessageDocument = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(document.parent_id, None);
        assert_eq!(serde_json::to_value(&document).unwrap(), value);
    }

    #[test]
    fn tool_output_and_error_flag_parse() {
        let part: Part = serde_json::from_value(json!({
            "type": "tool", "toolInvocationId": "c1", "toolName": "read",
            "input": {"path": "a.rs"}, "output": "contents", "isError": false,
            "elapsedSeconds": 0.4, "state": "done"
        }))
        .unwrap();
        assert!(matches!(
            part,
            Part::Tool {
                output: Some(_),
                is_error: Some(false),
                ..
            }
        ));
    }
}

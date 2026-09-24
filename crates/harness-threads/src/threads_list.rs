//! The `harness-sdk-threads` list protocol: document shape and `thread/*`
//! commands. Served by a list host next to the per-thread hosts.

use serde::{Deserialize, Serialize};

use crate::commands::Command;

/// The list host's one unkeyed main document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadsDocument {
    /// Sorted by `updated_at` descending; the host writes the whole list on
    /// every change.
    pub threads: Vec<ThreadEntry>,
}

/// The list's record of a thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadEntry {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub status: EntryStatus,
    /// ISO-8601.
    pub updated_at: String,
    /// Absent means idle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Activity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryStatus {
    Regular,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Activity {
    Running,
    Waiting,
    Error,
}

/// `thread/create`: insert a `regular` entry at the top.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadCreate {
    pub thread_id: String,
}

impl Command for ThreadCreate {
    const METHOD: &'static str = "thread/create";
}

/// `thread/rename`: set the entry's title.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRename {
    pub thread_id: String,
    pub title: String,
}

impl Command for ThreadRename {
    const METHOD: &'static str = "thread/rename";
}

/// `thread/archive`: set `status: "archived"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadArchive {
    pub thread_id: String,
}

impl Command for ThreadArchive {
    const METHOD: &'static str = "thread/archive";
}

/// `thread/unarchive`: set `status: "regular"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUnarchive {
    pub thread_id: String,
}

impl Command for ThreadUnarchive {
    const METHOD: &'static str = "thread/unarchive";
}

/// `thread/delete`: remove the entry; the per-thread host keeps its data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadDelete {
    pub thread_id: String,
}

impl Command for ThreadDelete {
    const METHOD: &'static str = "thread/delete";
}

/// The `thread/*` rejection codes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThreadRejectCode {
    InvalidParams,
    DuplicateId,
    UnknownThread,
    #[serde(untagged)]
    Unknown(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn document_parses_and_orders_by_host() {
        let document: ThreadsDocument = serde_json::from_value(json!({
            "threads": [
                {"id": "t2", "title": "usage charts", "status": "regular",
                 "updatedAt": "2026-09-24T12:00:00.000Z", "activity": "waiting"},
                {"id": "t1", "status": "archived", "updatedAt": "2026-09-23T09:00:00.000Z"}
            ]
        }))
        .unwrap();
        assert_eq!(document.threads[0].activity, Some(Activity::Waiting));
        assert_eq!(document.threads[1].status, EntryStatus::Archived);
        assert_eq!(document.threads[1].activity, None);
    }

    #[test]
    fn commands_serialize_like_the_spec() {
        assert_eq!(
            ThreadCreate {
                thread_id: "t3".into()
            }
            .statement(),
            ("thread/create", vec![json!({"threadId": "t3"})])
        );
        assert_eq!(
            ThreadRename {
                thread_id: "t3".into(),
                title: "weft".into()
            }
            .statement()
            .1,
            vec![json!({"threadId": "t3", "title": "weft"})]
        );
    }

    #[test]
    fn reject_codes_parse() {
        let code: ThreadRejectCode = serde_json::from_value(json!("unknown-thread")).unwrap();
        assert_eq!(code, ThreadRejectCode::UnknownThread);
    }
}

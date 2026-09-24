//! Statewire Document encoding: JSON values with date metadata.
//!
//! A document travels as `{ value, meta }` where `value` is plain JSON with
//! dates written as ISO 8601 UTC strings and `meta` lists which strings are
//! dates. The crate keeps values as [`serde_json::Value`]; applications decide
//! how to materialize dates.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One step of a document path: an object key or a list index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PathSeg {
    Key(String),
    Index(usize),
}

impl From<&str> for PathSeg {
    fn from(key: &str) -> Self {
        Self::Key(key.to_owned())
    }
}

impl From<usize> for PathSeg {
    fn from(index: usize) -> Self {
        Self::Index(index)
    }
}

/// A metadata annotation naming a typed value inside `value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaEntry {
    pub path: Vec<PathSeg>,
    #[serde(rename = "type")]
    pub kind: MetaKind,
}

/// Supported metadata types. Unknown server types are skipped by clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaKind {
    #[serde(rename = "date")]
    Date,
    #[serde(untagged)]
    Unknown(String),
}

/// An encoded document: `value` plus optional `meta`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EncodedDocument {
    pub value: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Vec<MetaEntry>>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DocumentError {
    #[error("meta path does not resolve to a value")]
    DanglingMetaPath,
    #[error("date meta targets a non-string value")]
    DateNotString,
    #[error("invalid ISO 8601 UTC datetime: {0:?}")]
    InvalidDate(String),
}

/// Resolves a path inside a JSON value.
pub fn resolve<'v>(value: &'v Value, path: &[PathSeg]) -> Option<&'v Value> {
    path.iter().try_fold(value, |current, seg| match seg {
        PathSeg::Key(key) => current.as_object()?.get(key),
        PathSeg::Index(index) => current.as_array()?.get(*index),
    })
}

/// Validates a document's known metadata entries.
///
/// `date` entries must point at ISO 8601 UTC datetime strings. Entries with
/// unknown types are skipped, per the client-handling rules.
pub fn validate(document: &EncodedDocument) -> Result<(), DocumentError> {
    for entry in document.meta.as_deref().unwrap_or_default() {
        if entry.kind != MetaKind::Date {
            continue;
        }
        let target =
            resolve(&document.value, &entry.path).ok_or(DocumentError::DanglingMetaPath)?;
        let text = target.as_str().ok_or(DocumentError::DateNotString)?;
        if !is_utc_datetime(text) {
            return Err(DocumentError::InvalidDate(text.to_owned()));
        }
    }
    Ok(())
}

/// Checks an ISO 8601 UTC datetime of the form
/// `YYYY-MM-DDTHH:MM:SS[.fff]Z`.
pub fn is_utc_datetime(text: &str) -> bool {
    let Some(rest) = text.strip_suffix('Z') else {
        return false;
    };
    let Some((date, time)) = rest.split_once('T') else {
        return false;
    };
    if crate::version::validate_version(date).is_err() {
        return false;
    }
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time, None),
    };
    if let Some(fraction) = fraction {
        if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
    }
    let b = hms.as_bytes();
    if b.len() != 8 || b[2] != b':' || b[5] != b':' {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| hms[range].parse::<u8>().ok();
    matches!(
        (digits(0..2), digits(3..5), digits(6..8)),
        (Some(h), Some(m), Some(s)) if h < 24 && m < 60 && s < 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_encoded_form() {
        let text = r#"{"value":{"updatedAt":"2026-09-05T12:00:00.000Z"},"meta":[{"path":["updatedAt"],"type":"date"}]}"#;
        let document: EncodedDocument = serde_json::from_str(text).unwrap();
        assert_eq!(validate(&document), Ok(()));
        assert_eq!(serde_json::to_string(&document).unwrap(), text);
    }

    #[test]
    fn meta_is_omitted_when_absent() {
        let document = EncodedDocument { value: json!({"count": 0}), meta: None };
        assert_eq!(serde_json::to_string(&document).unwrap(), r#"{"value":{"count":0}}"#);
    }

    #[test]
    fn resolves_nested_paths() {
        let value = json!({"messages": [{"text": "hi"}]});
        let path = [PathSeg::from("messages"), PathSeg::from(0), PathSeg::from("text")];
        assert_eq!(resolve(&value, &path), Some(&json!("hi")));
        assert_eq!(resolve(&value, &[PathSeg::from("missing")]), None);
    }

    #[test]
    fn rejects_bad_dates() {
        for (value, expected) in [
            (json!({"at": "2026-09-05T12:00:00Z"}), Ok(())),
            (json!({"at": "2026-09-05T12:00:00.5Z"}), Ok(())),
            (json!({"at": "2026-09-05 12:00:00Z"}), Err(DocumentError::InvalidDate("2026-09-05 12:00:00Z".into()))),
            (json!({"at": "2026-09-05T25:00:00Z"}), Err(DocumentError::InvalidDate("2026-09-05T25:00:00Z".into()))),
            (json!({"at": "2026-09-05T12:00:00"}), Err(DocumentError::InvalidDate("2026-09-05T12:00:00".into()))),
            (json!({"at": 5}), Err(DocumentError::DateNotString)),
        ] {
            let document = EncodedDocument {
                value,
                meta: Some(vec![MetaEntry { path: vec![PathSeg::from("at")], kind: MetaKind::Date }]),
            };
            assert_eq!(validate(&document), expected);
        }
    }

    #[test]
    fn unknown_meta_kinds_are_skipped() {
        let document = EncodedDocument {
            value: json!({"blob": 42}),
            meta: Some(vec![MetaEntry {
                path: vec![PathSeg::from("blob")],
                kind: MetaKind::Unknown("binary".into()),
            }]),
        };
        assert_eq!(validate(&document), Ok(()));
    }

    #[test]
    fn dangling_meta_path_is_rejected() {
        let document = EncodedDocument {
            value: json!({}),
            meta: Some(vec![MetaEntry { path: vec![PathSeg::from("at")], kind: MetaKind::Date }]),
        };
        assert_eq!(validate(&document), Err(DocumentError::DanglingMetaPath));
    }
}

//! Statepatch: ordered operations that synchronize the document table.
//!
//! Documents start with `mount`, followed by `add`, `replace`, or `remove`.
//! Operations apply in order; each sees the previous operation's changes.
//! Operations that break the path or operation rules are protocol errors,
//! while unknown operation names are skipped with a one-time warning.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::document::{MetaEntry, PathSeg};

/// One statepatch operation. `doc` defaults to `0` and is omitted by
/// producers when zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Op {
    #[serde(rename = "mount")]
    Mount {
        #[serde(default, skip_serializing_if = "is_zero")]
        doc: u64,
        protocol: String,
        #[serde(default, skip_serializing_if = "is_false")]
        main: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<Vec<String>>,
        value: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<Vec<MetaEntry>>,
    },
    #[serde(rename = "unmount")]
    Unmount {
        #[serde(default, skip_serializing_if = "is_zero")]
        doc: u64,
    },
    #[serde(rename = "add")]
    Add {
        #[serde(default, skip_serializing_if = "is_zero")]
        doc: u64,
        path: Vec<PathSeg>,
        value: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<Vec<MetaEntry>>,
    },
    #[serde(rename = "replace")]
    Replace {
        #[serde(default, skip_serializing_if = "is_zero")]
        doc: u64,
        path: Vec<PathSeg>,
        value: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<Vec<MetaEntry>>,
    },
    #[serde(rename = "remove")]
    Remove {
        #[serde(default, skip_serializing_if = "is_zero")]
        doc: u64,
        path: Vec<PathSeg>,
    },
    #[serde(untagged)]
    Unknown { op: String },
}

fn is_zero(doc: &u64) -> bool {
    *doc == 0
}

fn is_false(flag: &bool) -> bool {
    !*flag
}

/// One mounted document.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub protocol: String,
    pub main: bool,
    pub key: Option<Vec<String>>,
    pub value: Value,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PatchError {
    #[error("operation targets unmounted document {0}")]
    UnmountedDocument(u64),
    #[error("document {0} is already mounted")]
    AlreadyMounted(u64),
    #[error("a main document is already mounted for protocol {0:?}")]
    DuplicateMain(String),
    #[error("path does not resolve")]
    BadPath,
    #[error("add into a list requires an index at or below the list length")]
    BadListIndex,
    #[error("add into a string requires a string value and an in-range position")]
    BadStringInsert,
    #[error("replace targets a missing list item")]
    MissingListItem,
    #[error("remove targets a missing key or item")]
    MissingTarget,
    #[error("path continues through a scalar")]
    NotAContainer,
}

/// The client's mounted documents for one attach.
#[derive(Debug, Default)]
pub struct DocumentTable {
    documents: BTreeMap<u64, Document>,
    warned_ops: HashSet<String>,
}

impl DocumentTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, doc: u64) -> Option<&Document> {
        self.documents.get(&doc)
    }

    /// The main document mounted for `protocol`, when present.
    pub fn main(&self, protocol: &str) -> Option<&Document> {
        self.documents
            .values()
            .find(|d| d.main && d.protocol == protocol)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, &Document)> {
        self.documents.iter().map(|(id, doc)| (*id, doc))
    }

    /// Replaces the whole table, as the hello's `ops` does on reconnect.
    pub fn clear(&mut self) {
        self.documents.clear();
    }

    /// Applies a statepatch in order. Returns the names of operations that
    /// were skipped as unknown for the first time.
    pub fn apply(&mut self, ops: &[Op]) -> Result<Vec<String>, PatchError> {
        let mut new_warnings = Vec::new();
        for op in ops {
            match op {
                Op::Mount {
                    doc,
                    protocol,
                    main,
                    key,
                    value,
                    meta: _,
                } => {
                    if self.documents.contains_key(doc) {
                        return Err(PatchError::AlreadyMounted(*doc));
                    }
                    if *main && self.main(protocol).is_some() {
                        return Err(PatchError::DuplicateMain(protocol.clone()));
                    }
                    self.documents.insert(
                        *doc,
                        Document {
                            protocol: protocol.clone(),
                            main: *main,
                            key: key.clone(),
                            value: value.clone(),
                        },
                    );
                }
                Op::Unmount { doc } => {
                    self.documents
                        .remove(doc)
                        .ok_or(PatchError::UnmountedDocument(*doc))?;
                }
                Op::Add {
                    doc,
                    path,
                    value,
                    meta: _,
                } => {
                    let root = self.value_mut(*doc)?;
                    add(root, path, value)?;
                }
                Op::Replace {
                    doc,
                    path,
                    value,
                    meta: _,
                } => {
                    let root = self.value_mut(*doc)?;
                    replace(root, path, value)?;
                }
                Op::Remove { doc, path } => {
                    let root = self.value_mut(*doc)?;
                    remove(root, path)?;
                }
                Op::Unknown { op } => {
                    if self.warned_ops.insert(op.clone()) {
                        new_warnings.push(op.clone());
                    }
                }
            }
        }
        Ok(new_warnings)
    }

    fn value_mut(&mut self, doc: u64) -> Result<&mut Value, PatchError> {
        self.documents
            .get_mut(&doc)
            .map(|d| &mut d.value)
            .ok_or(PatchError::UnmountedDocument(doc))
    }
}

fn descend<'v>(root: &'v mut Value, path: &[PathSeg]) -> Result<&'v mut Value, PatchError> {
    path.iter()
        .try_fold(root, |current, seg| match (current, seg) {
            (Value::Object(map), PathSeg::Key(key)) => map.get_mut(key).ok_or(PatchError::BadPath),
            (Value::Array(items), PathSeg::Index(index)) => {
                items.get_mut(*index).ok_or(PatchError::BadPath)
            }
            (Value::Object(_) | Value::Array(_), _) => Err(PatchError::BadPath),
            _ => Err(PatchError::NotAContainer),
        })
}

fn add(root: &mut Value, path: &[PathSeg], value: &Value) -> Result<(), PatchError> {
    let (last, parents) = path.split_last().ok_or(PatchError::BadPath)?;
    let parent = descend(root, parents)?;
    let PathSeg::Index(position) = last else {
        return Err(PatchError::BadPath);
    };
    match parent {
        Value::Array(items) => {
            if *position > items.len() {
                return Err(PatchError::BadListIndex);
            }
            items.insert(*position, value.clone());
            Ok(())
        }
        Value::String(text) => {
            let Value::String(inserted) = value else {
                return Err(PatchError::BadStringInsert);
            };
            let byte_position =
                code_point_offset(text, *position).ok_or(PatchError::BadStringInsert)?;
            text.insert_str(byte_position, inserted);
            Ok(())
        }
        _ => Err(PatchError::BadPath),
    }
}

fn code_point_offset(text: &str, position: usize) -> Option<usize> {
    if position == 0 {
        return Some(0);
    }
    text.char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(text.len()))
        .nth(position)
}

fn replace(root: &mut Value, path: &[PathSeg], value: &Value) -> Result<(), PatchError> {
    let Some((last, parents)) = path.split_last() else {
        *root = value.clone();
        return Ok(());
    };
    let parent = descend(root, parents)?;
    match (parent, last) {
        (Value::Object(map), PathSeg::Key(key)) => {
            map.insert(key.clone(), value.clone());
            Ok(())
        }
        (Value::Array(items), PathSeg::Index(index)) => {
            let slot = items.get_mut(*index).ok_or(PatchError::MissingListItem)?;
            *slot = value.clone();
            Ok(())
        }
        (Value::Object(_) | Value::Array(_), _) => Err(PatchError::BadPath),
        _ => Err(PatchError::NotAContainer),
    }
}

fn remove(root: &mut Value, path: &[PathSeg]) -> Result<(), PatchError> {
    let (last, parents) = path.split_last().ok_or(PatchError::BadPath)?;
    let parent = descend(root, parents)?;
    match (parent, last) {
        (Value::Object(map), PathSeg::Key(key)) => {
            map.remove(key).map(drop).ok_or(PatchError::MissingTarget)
        }
        (Value::Array(items), PathSeg::Index(index)) => {
            if *index >= items.len() {
                return Err(PatchError::MissingTarget);
            }
            items.remove(*index);
            Ok(())
        }
        (Value::Object(_) | Value::Array(_), _) => Err(PatchError::BadPath),
        _ => Err(PatchError::NotAContainer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mounted(value: Value) -> DocumentTable {
        let mut table = DocumentTable::new();
        table
            .apply(&[Op::Mount {
                doc: 0,
                protocol: "default".into(),
                main: true,
                key: None,
                value,
                meta: None,
            }])
            .unwrap();
        table
    }

    #[test]
    fn parses_spec_examples() {
        let ops: Vec<Op> = serde_json::from_str(
            r#"[
              {"op":"mount","protocol":"harness-sdk","doc":1,"key":["t1","main","42"],"value":{"text":"Hello"}},
              {"op":"replace","path":["count"],"value":1},
              {"op":"add","path":["items",1],"value":"b"},
              {"op":"remove","path":["items",1]},
              {"op":"unmount","doc":1}
            ]"#,
        )
        .unwrap();
        assert_eq!(ops.len(), 5);
        assert!(matches!(&ops[0], Op::Mount { doc: 1, .. }));
    }

    #[test]
    fn doc_zero_is_omitted_on_serialize() {
        let op = Op::Replace {
            doc: 0,
            path: vec![PathSeg::from("count")],
            value: json!(1),
            meta: None,
        };
        assert_eq!(
            serde_json::to_string(&op).unwrap(),
            r#"{"op":"replace","path":["count"],"value":1}"#
        );
    }

    #[test]
    fn mount_then_mutate() {
        let mut table = mounted(json!({"count": 0, "items": ["a", "c"], "text": "Hello"}));
        table
            .apply(&[
                Op::Replace {
                    doc: 0,
                    path: vec![PathSeg::from("count")],
                    value: json!(1),
                    meta: None,
                },
                Op::Add {
                    doc: 0,
                    path: vec![PathSeg::from("items"), PathSeg::from(1)],
                    value: json!("b"),
                    meta: None,
                },
                Op::Add {
                    doc: 0,
                    path: vec![PathSeg::from("text"), PathSeg::from(5)],
                    value: json!(" world"),
                    meta: None,
                },
            ])
            .unwrap();
        assert_eq!(
            table.main("default").unwrap().value,
            json!({"count": 1, "items": ["a", "b", "c"], "text": "Hello world"})
        );
    }

    #[test]
    fn replace_whole_document_and_new_keys() {
        let mut table = mounted(json!({"count": 0}));
        table
            .apply(&[Op::Replace {
                doc: 0,
                path: vec![PathSeg::from("fresh")],
                value: json!(true),
                meta: None,
            }])
            .unwrap();
        table
            .apply(&[Op::Replace {
                doc: 0,
                path: vec![],
                value: json!({"reset": true}),
                meta: None,
            }])
            .unwrap();
        assert_eq!(table.get(0).unwrap().value, json!({"reset": true}));
    }

    #[test]
    fn replace_missing_list_item_is_an_error() {
        let mut table = mounted(json!({"items": []}));
        let result = table.apply(&[Op::Replace {
            doc: 0,
            path: vec![PathSeg::from("items"), PathSeg::from(0)],
            value: json!("x"),
            meta: None,
        }]);
        assert_eq!(result, Err(PatchError::MissingListItem));
    }

    #[test]
    fn remove_shifts_list_items() {
        let mut table = mounted(json!({"items": ["a", "b", "c"]}));
        table
            .apply(&[Op::Remove {
                doc: 0,
                path: vec![PathSeg::from("items"), PathSeg::from(1)],
            }])
            .unwrap();
        assert_eq!(table.get(0).unwrap().value, json!({"items": ["a", "c"]}));
    }

    #[test]
    fn string_insert_counts_code_points() {
        let mut table = mounted(json!({"text": "héllo"}));
        table
            .apply(&[Op::Add {
                doc: 0,
                path: vec![PathSeg::from("text"), PathSeg::from(2)],
                value: json!("X"),
                meta: None,
            }])
            .unwrap();
        assert_eq!(table.get(0).unwrap().value, json!({"text": "héXllo"}));
    }

    #[test]
    fn unknown_ops_warn_once() {
        let mut table = mounted(json!({}));
        let ops: Vec<Op> = serde_json::from_str(r#"[{"op":"transmute","doc":9}]"#).unwrap();
        assert_eq!(table.apply(&ops).unwrap(), vec!["transmute".to_owned()]);
        assert_eq!(table.apply(&ops).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn duplicate_main_is_rejected() {
        let mut table = mounted(json!({}));
        let result = table.apply(&[Op::Mount {
            doc: 1,
            protocol: "default".into(),
            main: true,
            key: None,
            value: json!({}),
            meta: None,
        }]);
        assert_eq!(result, Err(PatchError::DuplicateMain("default".into())));
    }

    #[test]
    fn second_attach_replaces_table() {
        let mut table = mounted(json!({"count": 3}));
        table.clear();
        assert!(table.get(0).is_none());
    }
}

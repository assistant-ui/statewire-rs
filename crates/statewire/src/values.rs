//! The value rules every statewire implementation shares (the table in
//! `tests/fixtures/values.json`, copied from harness-sdk).
//!
//! Writes follow `JSON.stringify`. A [`Value`] can't hold `undefined`,
//! functions, cycles or `NaN` (serde writes a non-finite float as `null`), so
//! the one rule left to check is integers past the JS safe range: they throw,
//! because JS clients can't read them exactly.
//!
//! Clients check what they receive themselves; hosts send values as written.
//! A received value keeps everything but `__proto__` keys, `prototype` keys
//! under a `constructor` key, and integers past the safe range (read as
//! `null`). Nesting past [`MAX_DEPTH`] is a protocol error.

use serde_json::Value;

use crate::document::PathSeg;
use crate::patch::Op;
use crate::wire::Packet;

/// Containers deeper than this are refused.
pub const MAX_DEPTH: usize = 512;

/// The largest integer every client reads exactly: `2 ** 53 - 1`.
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// A value a writer gave statewire that the wire can't carry.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "statewire: can't encode {found} at {}: {rule}",
    format_path(path, root)
)]
pub struct ValueError {
    pub path: Vec<PathSeg>,
    pub found: String,
    pub rule: &'static str,
    root: String,
}

/// A path as code reads it: `events[2].approvalRequired`, `params[2]`.
pub fn format_path(path: &[PathSeg], root: &str) -> String {
    let mut out = root.to_owned();
    for seg in path {
        match seg {
            PathSeg::Index(index) => out.push_str(&format!("[{index}]")),
            PathSeg::Key(key) if is_identifier(key) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            PathSeg::Key(key) => out.push_str(&format!(
                "[{}]",
                serde_json::to_string(key).unwrap_or_default()
            )),
        }
    }
    if out.is_empty() {
        "the root".to_owned()
    } else {
        out
    }
}

fn is_identifier(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

fn is_unsafe_integer(number: &serde_json::Number) -> bool {
    if let Some(n) = number.as_u64() {
        return n > MAX_SAFE_INTEGER;
    }
    if let Some(n) = number.as_i64() {
        return n.unsigned_abs() > MAX_SAFE_INTEGER;
    }
    number
        .as_f64()
        .is_some_and(|f| f.fract() == 0.0 && f.abs() > MAX_SAFE_INTEGER as f64)
}

/// Checks a value a writer sends (command params): an integer past the safe
/// range is an error naming its path. `root` names the value in the message.
pub fn check_write(value: &Value, root: &str) -> Result<(), ValueError> {
    let mut path = Vec::new();
    check_at(value, &mut path, root)
}

fn check_at(value: &Value, path: &mut Vec<PathSeg>, root: &str) -> Result<(), ValueError> {
    match value {
        Value::Number(number) if is_unsafe_integer(number) => Err(ValueError {
            path: path.clone(),
            found: format!("the number {number}"),
            rule:
                "integers must be within the safe integer range, so every client reads them exactly",
            root: root.to_owned(),
        }),
        Value::Array(items) => items.iter().enumerate().try_for_each(|(index, item)| {
            path.push(PathSeg::Index(index));
            let checked = check_at(item, path, root);
            path.pop();
            checked
        }),
        Value::Object(map) => map.iter().try_for_each(|(key, item)| {
            path.push(PathSeg::Key(key.clone()));
            let checked = check_at(item, path, root);
            path.pop();
            checked
        }),
        _ => Ok(()),
    }
}

/// A received value nested deeper than [`MAX_DEPTH`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("protocol error: value at {} is nested deeper than {MAX_DEPTH}", format_path(.0, ""))]
pub struct TooDeep(pub Vec<PathSeg>);

/// What a client keeps of a value the host sent, in place. `parent` is the
/// key the value sits under (a patch's last path segment), so a `prototype`
/// key under `constructor` goes too.
pub fn receive(value: &mut Value, parent: Option<&str>) -> Result<(), TooDeep> {
    let mut path = Vec::new();
    receive_at(value, parent, &mut path, 0)
}

fn receive_at(
    value: &mut Value,
    parent: Option<&str>,
    path: &mut Vec<PathSeg>,
    depth: usize,
) -> Result<(), TooDeep> {
    if matches!(value, Value::Array(_) | Value::Object(_)) && depth >= MAX_DEPTH {
        return Err(TooDeep(path.clone()));
    }
    match value {
        Value::Number(number) if is_unsafe_integer(number) => *value = Value::Null,
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                path.push(PathSeg::Index(index));
                receive_at(item, None, path, depth + 1)?;
                path.pop();
            }
        }
        Value::Object(map) => {
            map.retain(|key, _| {
                !(key == "__proto__" || key == "prototype" && parent == Some("constructor"))
            });
            for (key, item) in map.iter_mut() {
                path.push(PathSeg::Key(key.clone()));
                receive_at(item, Some(key), path, depth + 1)?;
                path.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

/// The deepest `[`/`{` nesting in JSON text, outside strings: checked before
/// parsing, so a hostile packet can't exhaust the stack.
pub fn text_depth(text: &str) -> usize {
    let (mut depth, mut deepest, mut in_string, mut escaped) = (0usize, 0usize, false, false);
    for byte in text.bytes() {
        if in_string {
            match byte {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Nesting a packet may have: a value's [`MAX_DEPTH`] plus the envelope
/// around it (as hosts bound frames).
pub const MAX_PACKET_DEPTH: usize = MAX_DEPTH + 8;

/// Why a packet's text was refused.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("protocol error: packet is nested deeper than {MAX_PACKET_DEPTH}")]
    TooDeep,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Parses a packet's JSON text with the shared depth limit in place of
/// serde_json's default of 128.
pub fn parse<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, ParseError> {
    if text_depth(text) > MAX_PACKET_DEPTH {
        return Err(ParseError::TooDeep);
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    deserializer.disable_recursion_limit();
    let value = T::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

/// Every value the host sent, as the client keeps it ([`receive`]): the
/// client checks this itself.
pub fn receive_packet(packet: &mut Packet) -> Result<(), TooDeep> {
    for op in packet.ops.iter_mut().flatten() {
        match op {
            Op::Mount { value, .. } => receive(value, None)?,
            Op::Add { path, value, .. } | Op::Replace { path, value, .. } => {
                let parent = match path.last() {
                    Some(PathSeg::Key(key)) => Some(key.clone()),
                    _ => None,
                };
                receive(value, parent.as_deref())?;
            }
            _ => {}
        }
    }
    for answer in packet.cmd.iter_mut().flatten() {
        if let Some(payload) = &mut answer.payload {
            receive(payload, None)?;
        }
    }
    if let Some(payload) = packet.fin.as_mut().and_then(|fin| fin.payload.as_mut()) {
        receive(payload, None)?;
    }
    Ok(())
}

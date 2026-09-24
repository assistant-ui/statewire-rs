//! Protocol versions, ranges, offers, and negotiation validation.
//!
//! Versions are calendar dates in fixed-width `YYYY-MM-DD` form; lexicographic
//! order of validated identifiers matches chronological order, so plain string
//! comparison is used throughout.

use serde::{Deserialize, Serialize};

/// The newest Statewire wire contract this crate implements.
pub const WIRE_VERSION: &str = "2026-09-13";

/// Header carrying the Statewire wire-contract offer.
pub const VERSION_HEADER: &str = "Statewire-Version";
/// Header carrying the application protocol offers.
pub const PROTOCOL_HEADER: &str = "Statewire-Protocol";

/// An inclusive version range: `min_version..=version`.
///
/// An omitted `min_version` equals `version`, making the range an
/// exact-version offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRange {
    pub version: String,
    pub min_version: Option<String>,
}

/// One named application protocol offer for the attach request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolOffer {
    pub name: String,
    pub range: VersionRange,
    pub optional: bool,
}

/// The server's selection from the hello packet's `syn`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolSelection {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<LaneRecord>,
}

/// Lane persistence class from `syn.protocols[].record`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaneRecord {
    #[serde(rename = "ephemeral")]
    Ephemeral,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VersionError {
    #[error("version must be a YYYY-MM-DD date: {0:?}")]
    Malformed(String),
    #[error("min-version {min:?} exceeds version {max:?}")]
    ReversedRange { min: String, max: String },
    #[error("selected version {selected:?} is outside the offered range for {name:?}")]
    OutsideOffer { name: String, selected: String },
    #[error("selection names unoffered protocol {0:?}")]
    UnofferedProtocol(String),
    #[error("selection repeats protocol {0:?}")]
    DuplicateSelection(String),
    #[error("required protocol {0:?} missing from selection")]
    MissingRequired(String),
    #[error("protocol name {0:?} is not a valid token")]
    InvalidName(String),
}

const fn is_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

/// Validates a `YYYY-MM-DD` version identifier.
pub fn validate_version(value: &str) -> Result<(), VersionError> {
    let malformed = || VersionError::Malformed(value.to_owned());
    let b = value.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(malformed());
    }
    if !(is_digit(b[0]) && is_digit(b[1]) && is_digit(b[2]) && is_digit(b[3]))
        || !(is_digit(b[5]) && is_digit(b[6]))
        || !(is_digit(b[8]) && is_digit(b[9]))
    {
        return Err(malformed());
    }
    let year: u16 = value[0..4].parse().map_err(|_| malformed())?;
    let month: u8 = value[5..7].parse().map_err(|_| malformed())?;
    let day: u8 = value[8..10].parse().map_err(|_| malformed())?;
    if year == 0 || month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return Err(malformed());
    }
    Ok(())
}

fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap {
                29
            } else {
                28
            }
        }
        _ => unreachable!(),
    }
}

/// Protocol identifiers are case-sensitive tokens.
pub fn validate_protocol_name(name: &str) -> Result<(), VersionError> {
    let mut bytes = name.bytes();
    let valid_first = matches!(bytes.next(), Some(b) if b.is_ascii_alphabetic());
    let valid_rest = name.bytes().skip(1).all(|b| {
        b.is_ascii_alphanumeric() || b"!#$%&'*+.^_`|~:/-".contains(&b)
    });
    if valid_first && valid_rest {
        Ok(())
    } else {
        Err(VersionError::InvalidName(name.to_owned()))
    }
}

impl VersionRange {
    /// An exact-version range.
    pub fn exact(version: impl Into<String>) -> Self {
        Self { version: version.into(), min_version: None }
    }

    pub fn validate(&self) -> Result<(), VersionError> {
        validate_version(&self.version)?;
        if let Some(min) = &self.min_version {
            validate_version(min)?;
            if min > &self.version {
                return Err(VersionError::ReversedRange {
                    min: min.clone(),
                    max: self.version.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn min(&self) -> &str {
        self.min_version.as_deref().unwrap_or(&self.version)
    }

    pub fn contains(&self, version: &str) -> bool {
        version >= self.min() && version <= self.version.as_str()
    }
}

/// Renders the `Statewire-Version` header value as an RFC 9651 string item.
pub fn version_header(range: &VersionRange) -> String {
    match &range.min_version {
        Some(min) if min != &range.version => {
            format!("\"{}\"; min-version=\"{min}\"", range.version)
        }
        _ => format!("\"{}\"", range.version),
    }
}

/// Renders the `Statewire-Protocol` header value for a set of named offers.
pub fn protocol_header(offers: &[ProtocolOffer]) -> String {
    offers
        .iter()
        .map(|offer| {
            let mut item = format!("{}; version=\"{}\"", offer.name, offer.range.version);
            if let Some(min) = &offer.range.min_version {
                if min != &offer.range.version {
                    item.push_str(&format!("; min-version=\"{min}\""));
                }
            }
            if offer.optional {
                item.push_str("; optional");
            }
            item
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Validates the hello's selections against the client's offers.
///
/// Accepts a subprotocol entry `<selected parent>/<child>` without an offer;
/// any other unoffered name is a protocol error. Every required offer must be
/// present, every named selection must lie within its offer's range, and
/// selections must be unique.
pub fn validate_selection(
    wire: &VersionRange,
    selected_wire: &str,
    offers: &[ProtocolOffer],
    selections: &[ProtocolSelection],
) -> Result<(), VersionError> {
    if !wire.contains(selected_wire) {
        return Err(VersionError::OutsideOffer {
            name: "statewire".to_owned(),
            selected: selected_wire.to_owned(),
        });
    }

    let mut seen: Vec<&str> = Vec::with_capacity(selections.len());
    let selected_parent = |name: &str| {
        selections.iter().any(|s| {
            !s.name.contains('/') && name.strip_prefix(s.name.as_str()).is_some_and(|rest| rest.starts_with('/'))
        })
    };
    for selection in selections {
        if seen.contains(&selection.name.as_str()) {
            return Err(VersionError::DuplicateSelection(selection.name.clone()));
        }
        seen.push(&selection.name);
        match offers.iter().find(|o| o.name == selection.name) {
            Some(offer) => {
                if !offer.range.contains(&selection.version) {
                    return Err(VersionError::OutsideOffer {
                        name: selection.name.clone(),
                        selected: selection.version.clone(),
                    });
                }
            }
            None if selection.name.contains('/') && selected_parent(&selection.name) => {}
            None => return Err(VersionError::UnofferedProtocol(selection.name.clone())),
        }
    }

    for offer in offers.iter().filter(|o| !o.optional) {
        if !selections.iter().any(|s| s.name == offer.name) {
            return Err(VersionError::MissingRequired(offer.name.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(name: &str, version: &str) -> ProtocolOffer {
        ProtocolOffer {
            name: name.to_owned(),
            range: VersionRange::exact(version),
            optional: false,
        }
    }

    fn selection(name: &str, version: &str) -> ProtocolSelection {
        ProtocolSelection { name: name.to_owned(), version: version.to_owned(), record: None }
    }

    #[test]
    fn accepts_valid_dates() {
        for v in ["2026-09-13", "2024-02-29", "1999-12-31"] {
            assert_eq!(validate_version(v), Ok(()), "{v}");
        }
    }

    #[test]
    fn rejects_invalid_dates() {
        for v in ["2026-9-13", "2026-13-01", "2025-02-29", "0000-01-01", "2026-09-13T00", "abcd-ef-gh"] {
            assert!(validate_version(v).is_err(), "{v}");
        }
    }

    #[test]
    fn range_contains_is_inclusive() {
        let range = VersionRange {
            version: "2026-09-13".into(),
            min_version: Some("2026-09-01".into()),
        };
        assert!(range.contains("2026-09-01"));
        assert!(range.contains("2026-09-13"));
        assert!(!range.contains("2026-08-31"));
        assert!(!range.contains("2026-09-14"));
    }

    #[test]
    fn reversed_range_is_rejected() {
        let range = VersionRange {
            version: "2026-09-01".into(),
            min_version: Some("2026-09-13".into()),
        };
        assert!(matches!(range.validate(), Err(VersionError::ReversedRange { .. })));
    }

    #[test]
    fn renders_headers() {
        let wire = VersionRange::exact(WIRE_VERSION);
        assert_eq!(version_header(&wire), "\"2026-09-13\"");
        let offers = [
            offer("harness-sdk", "2026-09-13"),
            ProtocolOffer {
                name: "acme-chat".into(),
                range: VersionRange {
                    version: "2026-09-05".into(),
                    min_version: Some("2026-09-01".into()),
                },
                optional: true,
            },
        ];
        assert_eq!(
            protocol_header(&offers),
            "harness-sdk; version=\"2026-09-13\", acme-chat; version=\"2026-09-05\"; min-version=\"2026-09-01\"; optional"
        );
    }

    #[test]
    fn selection_within_offers_passes() {
        let offers = [offer("harness-sdk", "2026-09-13")];
        let selections = [
            selection("harness-sdk", "2026-09-13"),
            ProtocolSelection {
                name: "harness-sdk/interest".into(),
                version: "2026-09-13".into(),
                record: Some(LaneRecord::Ephemeral),
            },
        ];
        let wire = VersionRange::exact(WIRE_VERSION);
        assert_eq!(validate_selection(&wire, WIRE_VERSION, &offers, &selections), Ok(()));
    }

    #[test]
    fn unoffered_non_child_is_rejected() {
        let offers = [offer("harness-sdk", "2026-09-13")];
        let selections = [selection("harness-sdk", "2026-09-13"), selection("acme", "2026-09-13")];
        let wire = VersionRange::exact(WIRE_VERSION);
        assert_eq!(
            validate_selection(&wire, WIRE_VERSION, &offers, &selections),
            Err(VersionError::UnofferedProtocol("acme".into()))
        );
    }

    #[test]
    fn missing_required_offer_is_rejected() {
        let offers = [offer("harness-sdk", "2026-09-13")];
        let wire = VersionRange::exact(WIRE_VERSION);
        assert_eq!(
            validate_selection(&wire, WIRE_VERSION, &offers, &[]),
            Err(VersionError::MissingRequired("harness-sdk".into()))
        );
    }

    #[test]
    fn optional_offer_may_be_omitted() {
        let mut acme = offer("acme", "2026-09-13");
        acme.optional = true;
        let offers = [offer("harness-sdk", "2026-09-13"), acme];
        let selections = [selection("harness-sdk", "2026-09-13")];
        let wire = VersionRange::exact(WIRE_VERSION);
        assert_eq!(validate_selection(&wire, WIRE_VERSION, &offers, &selections), Ok(()));
    }

    #[test]
    fn protocol_names_are_tokens() {
        assert!(validate_protocol_name("harness-sdk").is_ok());
        assert!(validate_protocol_name("harness-sdk/interest").is_ok());
        assert!(validate_protocol_name("9lives").is_err());
        assert!(validate_protocol_name("").is_err());
        assert!(validate_protocol_name("has space").is_err());
    }
}

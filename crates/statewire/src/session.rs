//! The client session state machine, independent of any transport.
//!
//! A [`Session`] tracks lanes, sequence clocks, pending commands, and the
//! document table. Transports feed it server packets and drain frames to
//! send; the session returns events for the application. This sans-IO shape
//! keeps the protocol rules testable and lets any socket implementation
//! drive them.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::patch::{DocumentTable, PatchError};
use crate::version::{
    validate_selection, LaneRecord, ProtocolOffer, ProtocolSelection, VersionError, VersionRange,
};
use crate::wire::{
    Answer, Fin, FinReason, Frame, Packet, Statement, Verdict, MAX_COMMANDS_PER_FRAME,
};

/// Session configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Client id: 32 lowercase hexadecimal characters.
    pub client_id: String,
    pub wire: VersionRange,
    pub offers: Vec<ProtocolOffer>,
}

/// What the application should do after a connection ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinAction {
    /// Reconnect with exponential backoff and jitter, waiting at least
    /// `min_delay_ms` from receipt of the fin.
    Reconnect { min_delay_ms: u64, is_error: bool },
    /// Refresh credentials and reconnect once; stop after a second
    /// consecutive denial.
    RefreshCredentials,
    /// The instance finished its work; reattach when needed.
    ReattachWhenNeeded,
    /// Stop; do not reconnect automatically.
    Stop { is_error: bool },
}

/// An observation the application consumes.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The hello was accepted; the document table holds the attach's view.
    Connected { selections: Vec<ProtocolSelection> },
    /// `ops` were applied to the document table.
    StateChanged,
    /// An answer for a tracked command. `terminal` settles the command.
    CommandUpdate { answer: Answer, terminal: bool },
    /// Commands that can no longer be answered.
    CommandsLost { protocol: String, seqs: Vec<u64> },
    /// The instance's work status changed.
    Idle(bool),
    /// Identity confirmation for a placeholder attach.
    IdentityConfirmed(String),
    /// An admission refusal asked for a delayed resend.
    RetryAfter { delay_ms: u64 },
    /// Unknown statepatch operations were skipped (first occurrence each).
    UnknownOpsSkipped(Vec<String>),
    /// The connection ended.
    Finished { fin: Fin, action: FinAction },
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SessionError {
    #[error("first packet is not a hello")]
    MissingHello,
    #[error("mid-stream change of negotiated versions")]
    MidStreamNegotiation,
    #[error("`next` is invalid beside `ops` or `fin`")]
    InvalidNext,
    #[error("packet names unknown lane {0:?}")]
    UnknownLane(String),
    #[error("not connected")]
    NotConnected,
    #[error(transparent)]
    Negotiation(#[from] VersionError),
    #[error(transparent)]
    Patch(#[from] PatchError),
}

#[derive(Debug)]
struct Pending {
    statement: Statement,
    posted: bool,
    applied_seen: bool,
}

#[derive(Debug)]
struct Lane {
    record: Option<LaneRecord>,
    /// Next sequence number to mint.
    next_seq: u64,
    /// Commands admitted (released from the resend queue) through this
    /// sequence number.
    released: u64,
    pending: Vec<Pending>,
}

impl Lane {
    fn is_ephemeral(&self) -> bool {
        self.record == Some(LaneRecord::Ephemeral)
    }

    /// Releases commands through `seq`; returns how many left the queue.
    fn release_through(&mut self, seq: u64) -> usize {
        if seq <= self.released {
            return 0;
        }
        self.released = seq;
        let before = self.pending.len();
        self.pending.retain(|p| p.statement.seq > seq);
        before - self.pending.len()
    }
}

/// The client session for one instance.
#[derive(Debug)]
pub struct Session {
    config: Config,
    lanes: BTreeMap<String, Lane>,
    table: DocumentTable,
    selections: Vec<ProtocolSelection>,
    connected: bool,
    saw_hello_this_attach: bool,
    idle: bool,
    held_answers: Vec<Answer>,
    instance_id: Option<String>,
}

impl Session {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            lanes: BTreeMap::new(),
            table: DocumentTable::new(),
            selections: Vec::new(),
            connected: false,
            saw_hello_this_attach: false,
            idle: false,
            held_answers: Vec::new(),
            instance_id: None,
        }
    }

    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    pub fn documents(&self) -> &DocumentTable {
        &self.table
    }

    pub fn is_idle(&self) -> bool {
        self.idle
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub fn selections(&self) -> &[ProtocolSelection] {
        &self.selections
    }

    /// Negotiation headers for the next attach request.
    pub fn attach_headers(&self) -> Vec<(String, String)> {
        vec![
            (
                crate::version::VERSION_HEADER.to_owned(),
                crate::version::version_header(&self.config.wire),
            ),
            (
                crate::version::PROTOCOL_HEADER.to_owned(),
                crate::version::protocol_header(&self.config.offers),
            ),
        ]
    }

    /// Queues a command on `protocol`'s lane and returns its sequence number.
    ///
    /// Commands may be queued before the first hello; they are sent once the
    /// lane is negotiated.
    pub fn command(&mut self, protocol: &str, method: &str, params: Vec<Value>) -> u64 {
        let lane = self.lanes.entry(protocol.to_owned()).or_insert(Lane {
            record: None,
            next_seq: 1,
            released: 0,
            pending: Vec::new(),
        });
        let seq = lane.next_seq;
        lane.next_seq += 1;
        lane.pending.push(Pending {
            statement: Statement {
                seq,
                protocol: protocol.to_owned(),
                method: method.to_owned(),
                params,
                meta: None,
            },
            posted: false,
            applied_seen: false,
        });
        seq
    }

    /// The next frame to send, or `None` when nothing is ready.
    ///
    /// Statements are consecutive within each lane and capped at the frame
    /// limit. Statements are marked posted; [`Session::on_disconnect`] and
    /// refusals mark them for resend.
    pub fn next_frame(&mut self) -> Option<Frame> {
        if !self.connected {
            return None;
        }
        let mut cmd = Vec::new();
        for lane in self.lanes.values_mut() {
            for pending in lane.pending.iter_mut().filter(|p| !p.posted) {
                if cmd.len() == MAX_COMMANDS_PER_FRAME {
                    break;
                }
                pending.posted = true;
                cmd.push(pending.statement.clone());
            }
        }
        if cmd.is_empty() {
            None
        } else {
            Some(Frame { cmd })
        }
    }

    /// Handles a transport close that arrived without a `fin`.
    ///
    /// Durable-lane commands stay pending for the next attach; ephemeral
    /// lanes lose their unanswered commands.
    pub fn on_disconnect(&mut self) -> Vec<Event> {
        self.connected = false;
        self.saw_hello_this_attach = false;
        self.held_answers.clear();
        self.fail_ephemeral_lanes()
    }

    fn fail_ephemeral_lanes(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        for (name, lane) in self.lanes.iter_mut() {
            if !lane.is_ephemeral() {
                for pending in lane.pending.iter_mut() {
                    pending.posted = false;
                }
                continue;
            }
            let seqs: Vec<u64> = lane.pending.iter().map(|p| p.statement.seq).collect();
            lane.pending.clear();
            lane.next_seq = 1;
            lane.released = 0;
            if !seqs.is_empty() {
                events.push(Event::CommandsLost {
                    protocol: name.clone(),
                    seqs,
                });
            }
        }
        events
    }

    /// Processes one server packet, returning events in order.
    pub fn handle_packet(&mut self, packet: Packet) -> Result<Vec<Event>, SessionError> {
        if packet.is_heartbeat() {
            return Ok(Vec::new());
        }
        if packet.next == Some(true) && (packet.ops.is_some() || packet.fin.is_some()) {
            return Err(SessionError::InvalidNext);
        }

        let mut events = Vec::new();

        if !self.saw_hello_this_attach && packet.fin.is_none() {
            return self.handle_hello(packet);
        }

        if let Some(syn) = &packet.syn {
            if syn.version.is_some()
                || syn
                    .protocols
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .any(|p| p.version.is_some() || p.record.is_some())
            {
                return Err(SessionError::MidStreamNegotiation);
            }
            self.handle_watermarks(syn.protocols.as_deref().unwrap_or_default(), &mut events)?;
            if let Some(retry) = syn.retry {
                events.push(Event::RetryAfter { delay_ms: retry });
            }
            if let Some(id) = &syn.id {
                self.instance_id = Some(id.clone());
                events.push(Event::IdentityConfirmed(id.clone()));
            }
            if let Some(idle) = syn.idle {
                self.set_idle(idle, &mut events);
            }
        }

        if let Some(ops) = &packet.ops {
            let skipped = self.table.apply(ops)?;
            if !skipped.is_empty() {
                events.push(Event::UnknownOpsSkipped(skipped));
            }
            events.push(Event::StateChanged);
            let held = std::mem::take(&mut self.held_answers);
            for answer in held {
                self.handle_answer(answer, &mut events)?;
            }
        }

        if let Some(answers) = packet.cmd {
            if packet.next == Some(true) {
                self.held_answers.extend(answers);
            } else {
                for answer in answers {
                    self.handle_answer(answer, &mut events)?;
                }
            }
        }

        if let Some(idle) = packet.idle {
            self.set_idle(idle, &mut events);
        }

        if let Some(fin) = packet.fin {
            let held = std::mem::take(&mut self.held_answers);
            for answer in held {
                self.handle_answer(answer, &mut events)?;
            }
            events.extend(self.handle_fin(fin));
        }

        Ok(events)
    }

    fn handle_hello(&mut self, packet: Packet) -> Result<Vec<Event>, SessionError> {
        let Some(syn) = &packet.syn else {
            return Err(SessionError::MissingHello);
        };
        let (Some(version), Some(protocols)) = (&syn.version, &syn.protocols) else {
            return Err(SessionError::MissingHello);
        };

        let selections: Vec<ProtocolSelection> = protocols
            .iter()
            .map(|p| {
                Ok(ProtocolSelection {
                    name: p.name.clone(),
                    version: p.version.clone().ok_or(SessionError::MissingHello)?,
                    record: p.record,
                })
            })
            .collect::<Result<_, SessionError>>()?;
        validate_selection(&self.config.wire, version, &self.config.offers, &selections)?;

        let mut events = Vec::new();
        for protocol in protocols {
            let watermark = protocol.seq.ok_or(SessionError::MissingHello)?;
            let lane = self.lanes.entry(protocol.name.clone()).or_insert(Lane {
                record: protocol.record,
                next_seq: 1,
                released: 0,
                pending: Vec::new(),
            });
            lane.record = protocol.record;
            if lane.is_ephemeral() {
                let seqs: Vec<u64> = lane.pending.iter().map(|p| p.statement.seq).collect();
                lane.pending.clear();
                lane.next_seq = 1;
                lane.released = 0;
                if !seqs.is_empty() {
                    events.push(Event::CommandsLost {
                        protocol: protocol.name.clone(),
                        seqs,
                    });
                }
            } else {
                // A durable host may report a lower watermark before loading
                // its retained admission record; pending commands keep their
                // sequence numbers and refusal watermarks resolve the rest.
                lane.next_seq = lane.next_seq.max(watermark + 1);
                for pending in lane.pending.iter_mut() {
                    pending.posted = false;
                }
            }
        }

        self.selections = selections.clone();
        self.table.clear();
        if let Some(ops) = &packet.ops {
            let skipped = self.table.apply(ops)?;
            if !skipped.is_empty() {
                events.push(Event::UnknownOpsSkipped(skipped));
            }
        }
        self.connected = true;
        self.saw_hello_this_attach = true;
        events.insert(0, Event::Connected { selections });
        self.set_idle(syn.idle.unwrap_or(false), &mut events);
        if let Some(id) = &syn.id {
            self.instance_id = Some(id.clone());
            events.push(Event::IdentityConfirmed(id.clone()));
        }
        if let Some(answers) = packet.cmd {
            for answer in answers {
                self.handle_answer(answer, &mut events)?;
            }
        }
        Ok(events)
    }

    /// Reconciles refusal or refresh watermarks.
    fn handle_watermarks(
        &mut self,
        protocols: &[crate::wire::SynProtocol],
        events: &mut Vec<Event>,
    ) -> Result<(), SessionError> {
        for entry in protocols {
            let lane = self
                .lanes
                .get_mut(&entry.name)
                .ok_or_else(|| SessionError::UnknownLane(entry.name.clone()))?;
            let watermark = entry.seq.unwrap_or(0).max(lane.released);
            lane.release_through(watermark);
            // A refusal means statements above the watermark were not
            // admitted; resend from the lane's next sequence.
            let mut lost = Vec::new();
            for pending in lane.pending.iter_mut() {
                pending.posted = false;
            }
            if let Some(first) = lane.pending.first() {
                if first.statement.seq > watermark + 1 {
                    // The prefix was discarded; held commands are lost and
                    // the clock restarts at the refusal's watermark.
                    lost = lane.pending.iter().map(|p| p.statement.seq).collect();
                    lane.pending.clear();
                    lane.next_seq = watermark + 1;
                }
            }
            if !lost.is_empty() {
                events.push(Event::CommandsLost {
                    protocol: entry.name.clone(),
                    seqs: lost,
                });
            }
        }
        Ok(())
    }

    fn handle_answer(
        &mut self,
        answer: Answer,
        events: &mut Vec<Event>,
    ) -> Result<(), SessionError> {
        let Some(lane) = self.lanes.get_mut(&answer.protocol) else {
            // Answers for lanes we never used are ignorable per the spec's
            // "results for commands they do not track" rule.
            return Ok(());
        };
        if answer.dur == Some(true) {
            lane.release_through(answer.seq);
        }
        let tracked = lane
            .pending
            .iter_mut()
            .find(|p| p.statement.seq == answer.seq);
        let Some(verdict) = answer.verdict else {
            return Ok(());
        };
        let terminal = verdict.is_terminal();
        match tracked {
            Some(pending) if verdict == Verdict::Applied => {
                pending.applied_seen = true;
            }
            Some(_) if terminal => {
                lane.pending.retain(|p| p.statement.seq != answer.seq);
            }
            _ => {}
        }
        events.push(Event::CommandUpdate { answer, terminal });
        Ok(())
    }

    fn handle_fin(&mut self, fin: Fin) -> Vec<Event> {
        self.connected = false;
        self.saw_hello_this_attach = false;
        let mut events = Vec::new();

        if fin.reason.fails_pending() {
            for (name, lane) in self.lanes.iter_mut() {
                let seqs: Vec<u64> = lane.pending.iter().map(|p| p.statement.seq).collect();
                lane.pending.clear();
                if !seqs.is_empty() {
                    events.push(Event::CommandsLost {
                        protocol: name.clone(),
                        seqs,
                    });
                }
            }
        } else {
            events.extend(self.fail_ephemeral_lanes());
        }

        let action = match &fin.reason {
            FinReason::Reconnect => FinAction::Reconnect {
                min_delay_ms: fin.retry.unwrap_or(0),
                is_error: false,
            },
            FinReason::Error | FinReason::Unknown(_) => FinAction::Reconnect {
                min_delay_ms: fin.retry.unwrap_or(0),
                is_error: true,
            },
            FinReason::Unauthorized => FinAction::RefreshCredentials,
            FinReason::Idle => FinAction::ReattachWhenNeeded,
            FinReason::ProtocolError | FinReason::Forbidden => FinAction::Stop { is_error: true },
            FinReason::Gone => FinAction::Stop { is_error: false },
        };
        events.push(Event::Finished { fin, action });
        events
    }

    fn set_idle(&mut self, idle: bool, events: &mut Vec<Event>) {
        if self.idle != idle {
            self.idle = idle;
            events.push(Event::Idle(idle));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> Config {
        Config {
            client_id: "0123456789abcdef0123456789abcdef".into(),
            wire: VersionRange::exact("2026-09-13"),
            offers: vec![ProtocolOffer {
                name: "default".into(),
                range: VersionRange::exact("2026-09-13"),
                optional: false,
            }],
        }
    }

    fn hello() -> Packet {
        serde_json::from_str(
            r#"{"syn":{"version":"2026-09-13","protocols":[{"name":"default","version":"2026-09-13","seq":0}]},
                "ops":[{"op":"mount","protocol":"default","main":true,"value":{"count":0}}]}"#,
        )
        .unwrap()
    }

    fn packet(text: &str) -> Packet {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn hello_connects_and_mounts() {
        let mut session = Session::new(config());
        let events = session.handle_packet(hello()).unwrap();
        assert!(matches!(events[0], Event::Connected { .. }));
        assert!(session.is_connected());
        assert_eq!(
            session.documents().main("default").unwrap().value,
            json!({"count": 0})
        );
    }

    #[test]
    fn first_packet_must_be_hello() {
        let mut session = Session::new(config());
        let result = session.handle_packet(packet(r#"{"idle":true}"#));
        assert_eq!(result, Err(SessionError::MissingHello));
    }

    #[test]
    fn command_lifecycle_with_dur_and_result() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();

        let seq = session.command("default", "increment", vec![json!(1)]);
        assert_eq!(seq, 1);
        let frame = session.next_frame().unwrap();
        assert_eq!(frame.cmd.len(), 1);
        assert_eq!(session.next_frame(), None);

        let events = session
            .handle_packet(packet(
                r#"{"ops":[{"op":"replace","path":["count"],"value":1}],
                    "cmd":[{"protocol":"default","seq":1,"type":"applied"},
                           {"protocol":"default","seq":1,"type":"result","payload":1,"dur":true}]}"#,
            ))
            .unwrap();
        assert!(matches!(events[0], Event::StateChanged));
        assert!(matches!(
            &events[1],
            Event::CommandUpdate { terminal: false, answer } if answer.verdict == Some(Verdict::Applied)
        ));
        assert!(matches!(
            &events[2],
            Event::CommandUpdate { terminal: true, .. }
        ));
        assert_eq!(session.next_frame(), None);
    }

    #[test]
    fn answers_deferred_by_next_wait_for_ops() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        session.command("default", "increment", vec![json!(1)]);
        session.next_frame().unwrap();

        let events = session
            .handle_packet(packet(
                r#"{"cmd":[{"protocol":"default","seq":1,"type":"applied"}],"next":true}"#,
            ))
            .unwrap();
        assert_eq!(events, vec![]);

        let events = session
            .handle_packet(packet(
                r#"{"ops":[{"op":"replace","path":["count"],"value":1}]}"#,
            ))
            .unwrap();
        assert!(matches!(events[0], Event::StateChanged));
        assert!(matches!(&events[1], Event::CommandUpdate { .. }));
    }

    #[test]
    fn next_beside_ops_is_a_protocol_error() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        let result = session.handle_packet(packet(r#"{"ops":[],"next":true}"#));
        assert_eq!(result, Err(SessionError::InvalidNext));
    }

    #[test]
    fn disconnect_resends_durable_commands() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        session.command("default", "increment", vec![json!(1)]);
        session.next_frame().unwrap();

        let events = session.on_disconnect();
        assert_eq!(events, vec![]);
        assert_eq!(session.next_frame(), None);

        session.handle_packet(hello()).unwrap();
        let frame = session.next_frame().unwrap();
        assert_eq!(frame.cmd[0].seq, 1);
    }

    #[test]
    fn refusal_with_retry_asks_for_delay() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        session.command("default", "increment", vec![json!(1)]);
        session.next_frame().unwrap();

        let events = session
            .handle_packet(packet(
                r#"{"syn":{"protocols":[{"name":"default","seq":0}],"retry":1000}}"#,
            ))
            .unwrap();
        assert!(events.contains(&Event::RetryAfter { delay_ms: 1000 }));
        let frame = session.next_frame().unwrap();
        assert_eq!(frame.cmd[0].seq, 1);
    }

    #[test]
    fn gap_refusal_releases_admitted_prefix() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        for _ in 0..4 {
            session.command("default", "increment", vec![json!(1)]);
        }
        session.next_frame().unwrap();

        let events = session
            .handle_packet(packet(
                r#"{"syn":{"protocols":[{"name":"default","seq":2}]}}"#,
            ))
            .unwrap();
        assert_eq!(events, vec![]);
        let frame = session.next_frame().unwrap();
        let seqs: Vec<u64> = frame.cmd.iter().map(|s| s.seq).collect();
        assert_eq!(seqs, vec![3, 4]);
    }

    #[test]
    fn mid_stream_version_change_is_a_protocol_error() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        let result = session.handle_packet(packet(r#"{"syn":{"version":"2026-09-14"}}"#));
        assert_eq!(result, Err(SessionError::MidStreamNegotiation));
    }

    #[test]
    fn fin_gone_fails_pending_and_stops() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        session.command("default", "increment", vec![json!(1)]);

        let events = session
            .handle_packet(packet(
                r#"{"fin":{"reason":"gone","message":"Instance deleted"}}"#,
            ))
            .unwrap();
        assert!(events.contains(&Event::CommandsLost {
            protocol: "default".into(),
            seqs: vec![1]
        }));
        assert!(matches!(
            events.last(),
            Some(Event::Finished {
                action: FinAction::Stop { is_error: false },
                ..
            })
        ));
        assert!(!session.is_connected());
    }

    #[test]
    fn fin_reconnect_keeps_durable_commands() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        session.command("default", "increment", vec![json!(1)]);

        let events = session
            .handle_packet(packet(r#"{"fin":{"reason":"reconnect","retry":5000}}"#))
            .unwrap();
        assert!(matches!(
            events.last(),
            Some(Event::Finished {
                action: FinAction::Reconnect {
                    min_delay_ms: 5000,
                    is_error: false
                },
                ..
            })
        ));
        session.handle_packet(hello()).unwrap();
        assert_eq!(session.next_frame().unwrap().cmd[0].seq, 1);
    }

    #[test]
    fn idle_transitions_emit_once() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        let events = session.handle_packet(packet(r#"{"idle":true}"#)).unwrap();
        assert_eq!(events, vec![Event::Idle(true)]);
        let events = session.handle_packet(packet(r#"{"idle":true}"#)).unwrap();
        assert_eq!(events, vec![]);
    }

    #[test]
    fn heartbeats_are_ignored() {
        let mut session = Session::new(config());
        session.handle_packet(hello()).unwrap();
        assert_eq!(session.handle_packet(packet("{}")).unwrap(), vec![]);
    }

    #[test]
    fn attach_headers_render_offers() {
        let session = Session::new(config());
        let headers = session.attach_headers();
        assert_eq!(
            headers[0],
            ("Statewire-Version".into(), "\"2026-09-13\"".into())
        );
        assert_eq!(
            headers[1],
            (
                "Statewire-Protocol".into(),
                "default; version=\"2026-09-13\"".into()
            )
        );
    }

    #[test]
    fn hello_with_unoffered_protocol_fails_validation() {
        let mut session = Session::new(config());
        let result = session.handle_packet(packet(
            r#"{"syn":{"version":"2026-09-13","protocols":[{"name":"intruder","version":"2026-09-13","seq":0}]},"ops":[]}"#,
        ));
        assert!(matches!(result, Err(SessionError::Negotiation(_))));
    }
}

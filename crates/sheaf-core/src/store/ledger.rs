//! Timeline ledger: sheaf-owned, append-only navigation and retention state,
//! stored as tagged records inside the journal stream.
//!
//! The Loro change DAG stays the content engine and keeps its ops-co-located
//! commit-message metadata; the ledger holds exactly the parts the product
//! needs to *mutate* — checkpoint labels, explicit marks, tombstones of
//! pruned captures — plus a per-capture blob registry so retention can
//! compute reachability the never-forgetting `tree_events` list cannot.
//!
//! ## Framing
//!
//! Journal payloads are self-describing: every Loro export starts with the
//! `b"loro"` magic (loro-internal `encoding.rs` decode path), so a payload
//! beginning with a ledger tag byte (0x01..=0x05) can never be confused
//! with an update. Format-1 stores contain only update frames and load
//! unchanged; format 2 additionally carries `[tag][json]` records.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SheafError};

/// First byte of every Loro-encoded export payload.
const LORO_MAGIC: &[u8; 4] = b"loro";
/// Framing tag bytes for each ledger record variant. Chosen in the range
/// `0x01..=0x05` so no ledger frame can begin with the `b"loro"` magic and be
/// mistaken for an update delta.
pub const TAG_CAPTURE: u8 = 0x01;
pub const TAG_CHECKPOINT: u8 = 0x02;
pub const TAG_MARK: u8 = 0x03;
pub const TAG_TOMBSTONE: u8 = 0x04;
pub const TAG_EPOCH: u8 = 0x05;

/// What one journal payload holds.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// A Loro update delta (format-1 and format-2 stores both carry these).
    Update(Vec<u8>),
    /// A timeline ledger record.
    Record(LedgerRecord),
}

/// Classify one raw journal payload. Unknown tags are skipped by callers
/// with a warning (forward tolerance), never fatal: a torn or future-frame
/// record must not make the store unreadable.
pub fn classify_payload(payload: &[u8]) -> Option<Frame> {
    if payload.starts_with(LORO_MAGIC) {
        return Some(Frame::Update(payload.to_vec()));
    }
    let (&tag, rest) = payload.split_first()?;
    if ![
        TAG_CAPTURE,
        TAG_CHECKPOINT,
        TAG_MARK,
        TAG_TOMBSTONE,
        TAG_EPOCH,
    ]
    .contains(&tag)
    {
        return None;
    }
    // The JSON body carries its own `k` discriminant; the tag byte is the
    // framing-level confirmation. A disagreement between the two is a
    // corrupt frame, skipped like a torn one.
    let record: LedgerRecord = serde_json::from_slice(rest).ok()?;
    let agrees = matches!(
        (&record, tag),
        (LedgerRecord::Capture { .. }, TAG_CAPTURE)
            | (LedgerRecord::Checkpoint { .. }, TAG_CHECKPOINT)
            | (LedgerRecord::Mark { .. }, TAG_MARK)
            | (LedgerRecord::Tombstone { .. }, TAG_TOMBSTONE)
            | (LedgerRecord::Epoch { .. }, TAG_EPOCH)
    );
    agrees.then_some(Frame::Record(record))
}

/// One ledger record. Serialized as `[tag][serde_json]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "snake_case")]
pub enum LedgerRecord {
    /// Navigation mirror of one capture plus the digests its batch named
    /// (the blob registry `tree_events` cannot provide under retention).
    Capture {
        id: String,
        frontier: String,
        at_ms: i64,
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        events: usize,
        #[serde(default)]
        blobs: Vec<String>,
    },
    /// A checkpoint label. Ledger-native (v2); labels found in the legacy
    /// `_sheaf.meta` map of format-1 stores are merged in at read time.
    Checkpoint {
        name: String,
        frontier: String,
        #[serde(default)]
        capture_id: Option<String>,
    },
    /// Explicit user mark: this point is collectable even though
    /// reachability would protect it — the one sanctioned way to bypass the
    /// reachability rule.
    Mark {
        capture_id: String,
        #[serde(default)]
        marked_at_ms: i64,
    },
    /// Ghost of a physically pruned capture: enough metadata for
    /// `log --pruned` to name what was lost, and for `resolve` to say
    /// who pruned it.
    Tombstone {
        capture_id: String,
        #[serde(default)]
        at_ms: i64,
        #[serde(default)]
        paths: Vec<String>,
        #[serde(default)]
        events: usize,
        /// Exact parent frontier captured before compaction removes the point.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_frontier: Option<String>,
        cause: PruneCause,
        #[serde(default)]
        pruned_at_ms: i64,
    },
    /// Bookkeeping for one retention compaction.
    Epoch {
        /// Hex frontier of the shallow boundary (empty for full snapshots).
        #[serde(default)]
        boundary: String,
        #[serde(default)]
        covered_upto: u64,
    },
}

/// Why a capture stopped being restorable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneCause {
    /// Automatic expiry (reachability-bound by construction).
    #[serde(rename = "expiry")]
    Expired,
    /// Explicit `gc mark` bypass.
    #[serde(rename = "gc mark")]
    Marked,
    /// Compaction boundary sweep (protected points survived by design).
    #[serde(rename = "compaction")]
    Epoch,
}

impl PruneCause {
    /// The stable wire/display string for this cause.
    pub fn as_str(self) -> &'static str {
        match self {
            PruneCause::Expired => "expiry",
            PruneCause::Marked => "gc mark",
            PruneCause::Epoch => "compaction",
        }
    }
}

impl LedgerRecord {
    /// One-line human summary for journal forensics.
    pub fn summary(&self) -> String {
        fn short(id: &str) -> &str {
            &id[..12.min(id.len())]
        }
        match self {
            LedgerRecord::Capture { id, blobs, .. } => {
                format!("capture {} blobs={}", short(id), blobs.len())
            }
            LedgerRecord::Checkpoint {
                name, capture_id, ..
            } => {
                format!(
                    "checkpoint '{}' at {}",
                    name,
                    capture_id.as_deref().map(short).unwrap_or("?")
                )
            }
            LedgerRecord::Mark { capture_id, .. } => format!("mark {}", short(capture_id)),
            LedgerRecord::Tombstone {
                capture_id, cause, ..
            } => {
                format!("tombstone {} cause={}", short(capture_id), cause.as_str())
            }
            LedgerRecord::Epoch {
                boundary,
                covered_upto,
            } => format!(
                "epoch boundary={} covered_upto={}",
                &boundary[..16.min(boundary.len())],
                covered_upto
            ),
        }
    }

    /// Encode to a framed journal payload: `[tag][json-with-k]`. Both the
    /// tag byte and the JSON `k` discriminant name the variant; the decode
    /// path requires them to agree.
    pub fn encode(&self) -> Vec<u8> {
        let tag = match self {
            LedgerRecord::Capture { .. } => TAG_CAPTURE,
            LedgerRecord::Checkpoint { .. } => TAG_CHECKPOINT,
            LedgerRecord::Mark { .. } => TAG_MARK,
            LedgerRecord::Tombstone { .. } => TAG_TOMBSTONE,
            LedgerRecord::Epoch { .. } => TAG_EPOCH,
        };
        let json = serde_json::to_vec(self).unwrap_or_else(|_| b"{}".to_vec());
        let mut out = Vec::with_capacity(1 + json.len());
        out.push(tag);
        out.extend_from_slice(&json);
        out
    }
}

/// Folded ledger state — everything `log`/`resolve`/`gc` need on the
/// navigation side, derivable from (manifest snapshot) + (record frames).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LedgerState {
    /// Blob-registry view of captures that have ledger records. Absence of
    /// a capture here does NOT mean it does not exist (format-1 history has
    /// no records); the DAG walk remains the enumeration authority.
    #[serde(default)]
    pub captures: BTreeMap<String, CaptureRec>,
    #[serde(default)]
    pub tombstones: BTreeMap<String, TombstoneRec>,
    #[serde(default)]
    pub marks: BTreeMap<String, i64>,
    #[serde(default)]
    pub checkpoints: BTreeMap<String, CheckpointRec>,
    #[serde(default)]
    pub epochs: Vec<EpochRec>,
}

/// Folded blob-registry view of one capture: its frontier, timestamp, touched
/// paths, event count, and the digests its batch named.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaptureRec {
    pub frontier: String,
    pub at_ms: i64,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub events: usize,
    #[serde(default)]
    pub blobs: Vec<String>,
}

/// Folded checkpoint label: the frontier it pins and the capture at that point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRec {
    pub frontier: String,
    #[serde(default)]
    pub capture_id: Option<String>,
}

/// Folded ghost of a pruned capture: enough metadata to name what was lost
/// and why, plus the parent frontier for lineage continuity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneRec {
    #[serde(default)]
    pub at_ms: i64,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub events: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_frontier: Option<String>,
    pub cause: PruneCause,
    #[serde(default)]
    pub pruned_at_ms: i64,
}

/// Folded compaction boundary: the frontier the shallow snapshot rebased onto
/// and the journal segment index it covers up to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EpochRec {
    #[serde(default)]
    pub boundary: String,
    #[serde(default)]
    pub covered_upto: u64,
}

impl LedgerState {
    /// Fold one record in (segment order).
    pub fn fold(&mut self, record: LedgerRecord) {
        match record {
            LedgerRecord::Capture {
                id,
                frontier,
                at_ms,
                paths,
                events,
                blobs,
            } => {
                self.captures.insert(
                    id,
                    CaptureRec {
                        frontier,
                        at_ms,
                        paths,
                        events,
                        blobs,
                    },
                );
            }
            LedgerRecord::Checkpoint {
                name,
                frontier,
                capture_id,
            } => {
                self.checkpoints.insert(
                    name,
                    CheckpointRec {
                        frontier,
                        capture_id,
                    },
                );
            }
            LedgerRecord::Mark {
                capture_id,
                marked_at_ms,
            } => {
                self.marks.insert(capture_id, marked_at_ms);
            }
            LedgerRecord::Tombstone {
                capture_id,
                at_ms,
                paths,
                events,
                parent_frontier,
                cause,
                pruned_at_ms,
            } => {
                self.tombstones.insert(
                    capture_id,
                    TombstoneRec {
                        at_ms,
                        paths,
                        events,
                        parent_frontier,
                        cause,
                        pruned_at_ms,
                    },
                );
            }
            LedgerRecord::Epoch {
                boundary,
                covered_upto,
            } => self.epochs.push(EpochRec {
                boundary,
                covered_upto,
            }),
        }
    }

    /// Serialize for the snapshot manifest so segment pruning never loses
    /// tombstones/marks/checkpoints.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Reconstruct folded state from a snapshot manifest's serialized form.
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| SheafError::StoreCorrupt(format!("manifest ledger state: {e}")))
    }

    /// Whether the capture has been pruned (has a tombstone).
    pub fn is_tombstoned(&self, capture_id: &str) -> bool {
        self.tombstones.contains_key(capture_id)
    }

    /// Tombstone for a capture id, when pruned.
    pub fn tombstone(&self, capture_id: &str) -> Option<&TombstoneRec> {
        self.tombstones.get(capture_id)
    }

    /// Whether the capture carries an explicit `gc mark` bypass.
    pub fn is_marked(&self, capture_id: &str) -> bool {
        self.marks.contains_key(capture_id)
    }

    /// Digests named by any non-tombstoned capture record (blob registry).
    pub fn blobs_of_survivors(&self) -> Vec<String> {
        let mut out = std::collections::BTreeSet::new();
        for (id, rec) in &self.captures {
            if !self.tombstones.contains_key(id) {
                out.extend(rec.blobs.iter().cloned());
            }
        }
        out.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip_through_payload() {
        let recs = vec![
            LedgerRecord::Capture {
                id: "abc".into(),
                frontier: "ff00".into(),
                at_ms: 42,
                paths: vec!["src/a.rs".into()],
                events: 3,
                blobs: vec!["deadbeef".into()],
            },
            LedgerRecord::Checkpoint {
                name: "before refactoring".into(),
                frontier: "ff00".into(),
                capture_id: Some("abc".into()),
            },
            LedgerRecord::Mark {
                capture_id: "abc".into(),
                marked_at_ms: 7,
            },
            LedgerRecord::Tombstone {
                capture_id: "abc".into(),
                at_ms: 42,
                paths: vec![],
                events: 3,
                parent_frontier: Some("fe00".into()),
                cause: PruneCause::Expired,
                pruned_at_ms: 99,
            },
            LedgerRecord::Epoch {
                boundary: "ff00".into(),
                covered_upto: 12,
            },
        ];
        for rec in recs {
            let payload = rec.encode();
            // The magic prefix keeps updates and records disjoint.
            assert!(!payload.starts_with(LORO_MAGIC));
            match classify_payload(&payload) {
                Some(Frame::Record(got)) => assert_eq!(got, rec),
                other => panic!("misclassified: {other:?}"),
            }
        }
    }

    #[test]
    fn loro_payload_classifies_as_update() {
        let mut payload = vec![0x6c, 0x6f, 0x72, 0x6f, 1, 2, 3];
        assert!(matches!(classify_payload(&payload), Some(Frame::Update(_))));
        payload[0] = TAG_CAPTURE;
        assert!(
            classify_payload(&payload).is_none()
                || matches!(classify_payload(&payload), Some(Frame::Record(_)))
        );
    }

    #[test]
    fn fold_and_json_roundtrip() {
        let mut state = LedgerState::default();
        state.fold(LedgerRecord::Capture {
            id: "c1".into(),
            frontier: "aa".into(),
            at_ms: 1,
            paths: vec![],
            events: 1,
            blobs: vec!["d1".into()],
        });
        state.fold(LedgerRecord::Tombstone {
            capture_id: "c0".into(),
            at_ms: 0,
            paths: vec![],
            events: 1,
            parent_frontier: None,
            cause: PruneCause::Marked,
            pruned_at_ms: 5,
        });
        state.fold(LedgerRecord::Checkpoint {
            name: "pin".into(),
            frontier: "aa".into(),
            capture_id: Some("c1".into()),
        });
        assert!(state.is_tombstoned("c0"));
        assert_eq!(state.blobs_of_survivors(), vec!["d1".to_string()]);
        let rt = LedgerState::from_json(&state.to_json()).unwrap();
        assert_eq!(rt, state);
    }

    #[test]
    fn classify_rejects_empty_unknown_and_disagreeing_frames() {
        // Empty payload: no tag at all.
        assert!(classify_payload(&[]).is_none());
        // Unknown tag bytes are skipped (forward tolerance), with or without a body.
        assert!(classify_payload(&[0x7f]).is_none());
        assert!(classify_payload(&[0x7f, b'x', b'y']).is_none());
        // A known tag whose body is not JSON is skipped, not fatal.
        assert!(classify_payload(&[TAG_CAPTURE, b'{']).is_none());

        // Tag and JSON `k` discriminant disagree: corrupt framing, skipped.
        let capture = LedgerRecord::Capture {
            id: "c".into(),
            frontier: "ff".into(),
            at_ms: 1,
            paths: vec![],
            events: 0,
            blobs: vec![],
        };
        let mut payload = vec![TAG_CHECKPOINT];
        payload.extend_from_slice(&serde_json::to_vec(&capture).unwrap());
        assert!(classify_payload(&payload).is_none());
    }

    #[test]
    fn prune_cause_strings_are_stable() {
        assert_eq!(PruneCause::Expired.as_str(), "expiry");
        assert_eq!(PruneCause::Marked.as_str(), "gc mark");
        assert_eq!(PruneCause::Epoch.as_str(), "compaction");
    }

    #[test]
    fn summaries_name_each_variant_and_truncate_ids() {
        let capture = LedgerRecord::Capture {
            id: "abcdef1234567890".into(),
            frontier: "ff".into(),
            at_ms: 1,
            paths: vec![],
            events: 2,
            blobs: vec!["x".into(), "y".into()],
        };
        assert_eq!(capture.summary(), "capture abcdef123456 blobs=2");

        let short_id = LedgerRecord::Capture {
            id: "ab".into(),
            frontier: "ff".into(),
            at_ms: 1,
            paths: vec![],
            events: 0,
            blobs: vec![],
        };
        assert_eq!(
            short_id.summary(),
            "capture ab blobs=0",
            "short ids are safe"
        );

        let checkpoint = LedgerRecord::Checkpoint {
            name: "before-refactor".into(),
            frontier: "ff".into(),
            capture_id: Some("abcdef1234567890".into()),
        };
        assert_eq!(
            checkpoint.summary(),
            "checkpoint 'before-refactor' at abcdef123456"
        );

        let unpinned = LedgerRecord::Checkpoint {
            name: "loose".into(),
            frontier: "ff".into(),
            capture_id: None,
        };
        assert_eq!(unpinned.summary(), "checkpoint 'loose' at ?");

        let mark = LedgerRecord::Mark {
            capture_id: "abcdef1234567890".into(),
            marked_at_ms: 5,
        };
        assert_eq!(mark.summary(), "mark abcdef123456");

        let tombstone = LedgerRecord::Tombstone {
            capture_id: "abcdef1234567890".into(),
            at_ms: 1,
            paths: vec![],
            events: 0,
            parent_frontier: None,
            cause: PruneCause::Marked,
            pruned_at_ms: 9,
        };
        assert_eq!(tombstone.summary(), "tombstone abcdef123456 cause=gc mark");

        let epoch = LedgerRecord::Epoch {
            boundary: "abcdef1234567890abcdef".into(),
            covered_upto: 12,
        };
        assert_eq!(
            epoch.summary(),
            "epoch boundary=abcdef1234567890 covered_upto=12"
        );
    }

    #[test]
    fn folded_state_helpers_and_tombstone_defaults() {
        let mut state = LedgerState::default();
        state.fold(LedgerRecord::Capture {
            id: "c1".into(),
            frontier: "aa".into(),
            at_ms: 1,
            paths: vec![],
            events: 1,
            blobs: vec!["b2".into(), "b1".into()],
        });
        state.fold(LedgerRecord::Capture {
            id: "c2".into(),
            frontier: "ab".into(),
            at_ms: 2,
            paths: vec![],
            events: 1,
            blobs: vec!["b3".into()],
        });
        state.fold(LedgerRecord::Tombstone {
            capture_id: "c2".into(),
            at_ms: 0,
            paths: vec![],
            events: 1,
            parent_frontier: Some("aa".into()),
            cause: PruneCause::Epoch,
            pruned_at_ms: 5,
        });
        state.fold(LedgerRecord::Mark {
            capture_id: "c2".into(),
            marked_at_ms: 6,
        });
        state.fold(LedgerRecord::Epoch {
            boundary: "aa".into(),
            covered_upto: 1,
        });
        state.fold(LedgerRecord::Epoch {
            boundary: "ab".into(),
            covered_upto: 2,
        });

        assert!(state.is_marked("c2"));
        assert!(!state.is_marked("c1"));
        assert!(!state.is_tombstoned("c1"));
        let t = state.tombstone("c2").unwrap();
        assert_eq!(t.cause, PruneCause::Epoch);
        assert_eq!(t.parent_frontier.as_deref(), Some("aa"));
        assert!(state.tombstone("c1").is_none());

        // Survivors only: c2's blobs are registry-invisible once tombstoned,
        // and the survivor list comes back sorted.
        assert_eq!(
            state.blobs_of_survivors(),
            vec!["b1".to_string(), "b2".to_string()]
        );

        // Epochs fold in segment order.
        assert_eq!(state.epochs.len(), 2);
        assert_eq!(state.epochs[0].covered_upto, 1);
        assert_eq!(state.epochs[1].covered_upto, 2);

        // Tombstone records serde-default their optional fields.
        let t: LedgerRecord =
            serde_json::from_str(r#"{"k":"tombstone","capture_id":"x","cause":"expiry"}"#).unwrap();
        assert_eq!(
            t,
            LedgerRecord::Tombstone {
                capture_id: "x".into(),
                at_ms: 0,
                paths: vec![],
                events: 0,
                parent_frontier: None,
                cause: PruneCause::Expired,
                pruned_at_ms: 0,
            }
        );

        // from_json fails closed on a manifest state of the wrong shape.
        let garbage = serde_json::json!({"captures": "not-a-map"});
        let err = LedgerState::from_json(&garbage).unwrap_err();
        assert_eq!(err.code(), "store.corrupt");
    }
}

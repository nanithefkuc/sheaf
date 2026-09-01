//! Capture-oriented timeline views and exact Loro-frontier addressing.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use loro::{CommitOptions, Frontiers, LoroDoc, ID};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{import_err, journal, log_pending, newest_manifest, store_dir, ProjectStore, META_MAP};

use crate::config;
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind};

const CAPTURE_PREFIX: &str = "sheaf:capture:v1:";
const CHECKPOINT_PREFIX: &str = "checkpoint:";
const MIN_ID_PREFIX: usize = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaptureMeta {
    at_ms: i64,
    paths: Vec<String>,
    events: usize,
    /// Prevents Loro from merging otherwise-identical commits in one second.
    #[serde(default)]
    parent: String,
    /// Present when the writer, not the watcher, authored this capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin: Option<CaptureOrigin>,
}

/// Why a capture exists, when the answer is not "someone edited a file".
///
/// A scoped restore stays on the current lineage: the state it
/// produces is built ON head, so it cannot be concurrent with head, and
/// forcing a fork would mean re-authoring every out-of-scope difference and
/// flattening those files' char-level history for files the user never asked
/// to touch. Instead the capture says plainly what it is, so the timeline
/// never presents a rollback as ordinary typing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginKind {
    /// The forward capture a path-scoped restore appended.
    Restore,
    /// The safety capture apply takes before overwriting anything.
    PreRestore,
    /// The forward capture a selection-scoped fragment restore appended;
    /// its `selections` carry the handle IDs that produced it.
    FragmentRestore,
    /// A divergent source branch squashed onto the active worktree.
    Merge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureOrigin {
    pub kind: OriginKind,
    /// Capture the restore targeted, when that point names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Root-relative scope; empty means the whole worktree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope: Vec<String>,
    /// Selection-handle IDs a fragment restore spliced in.
    /// Additive: older captures deserialize without it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selections: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Capture {
    /// Full stable SHA-256 identifier. UIs normally display `short_id()`.
    pub id: String,
    /// Canonical encoded Loro frontier (hex).
    pub frontier: String,
    /// Exact dependencies of the underlying Loro Change (hex frontier).
    pub parent_frontier: String,
    pub timestamp_ms: i64,
    pub paths: Vec<String>,
    pub events: usize,
    /// Names of checkpoint labels pinned to exactly this capture.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<String>,
    /// Restore provenance; absent for ordinary watcher captures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<CaptureOrigin>,
    /// Whether the capture sits on the worktree's current causal lineage.
    /// Trivially true for single-lineage walks; meaningful for `--all` views
    /// where abandoned futures interleave with live history.
    #[serde(default)]
    pub on_current: bool,
}

impl Capture {
    pub fn short_id(&self) -> &str {
        &self.id[..12.min(self.id.len())]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub name: String,
    pub frontier: String,
    pub capture_id: Option<String>,
    /// When the pinned capture was made, when it names one.
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    /// Whether the pinned capture sits on the worktree's current lineage.
    /// A checkpoint pinned on a branch the worktree no longer holds stays
    /// resolvable (it reads from the merged tip) — this flag is what keeps
    /// that from surprising anyone.
    #[serde(default)]
    pub on_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedPoint {
    pub frontier: String,
    pub capture_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchTip {
    pub frontier: String,
    pub capture_id: Option<String>,
}

/// A capture plus the file-level difference from its exact parent frontier.
/// This preserves the distinction between one debounced multi-file capture
/// and an arbitrary range diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureInfo {
    pub capture: Capture,
    pub diff: super::DiffOutcome,
}

/// Read-only snapshot+journal view. Opening it never creates, truncates, or appends files.
pub struct TimelineReader {
    root: PathBuf,
    doc: LoroDoc,
    ledger: super::ledger::LedgerState,
    pub(super) grep_content_cache: RefCell<super::grep::GrepContentCache>,
}

impl TimelineReader {
    /// Project root this view was assembled for.
    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// Read-only document behind this view.
    pub fn doc(&self) -> &LoroDoc {
        &self.doc
    }

    /// Folded timeline ledger (tombstones, marks, checkpoints).
    pub fn ledger(&self) -> &super::ledger::LedgerState {
        &self.ledger
    }

    pub fn open(root: &Path) -> Result<Self> {
        config::read_store_format(root)?;
        let sdir = store_dir(root);
        let doc = LoroDoc::new();
        let mut ledger = super::ledger::LedgerState::default();
        let mut covered_upto = None;
        if let Some((_manifest_path, manifest)) = newest_manifest(&sdir) {
            let snapshot = sdir.join("snapshots").join(&manifest.snapshot);
            match std::fs::read(&snapshot) {
                Ok(bytes) => {
                    let status = doc.import(&bytes).map_err(import_err)?;
                    log_pending(&status);
                    if let Some(state) = manifest.ledger.as_ref() {
                        match super::ledger::LedgerState::from_json(state) {
                            Ok(parsed) => ledger = parsed,
                            Err(error) => {
                                tracing::warn!(%error, "manifest ledger state unparseable")
                            }
                        }
                    }
                    covered_upto = Some(manifest.covered_upto);
                }
                Err(error) => tracing::warn!(
                    snapshot = %snapshot.display(),
                    %error,
                    "timeline snapshot unreadable; attempting full journal replay"
                ),
            }
        }
        let paths: Vec<_> = journal::list_segments(&sdir)
            .into_iter()
            .filter(|(idx, _)| covered_upto.is_none_or(|c| *idx > c))
            .collect();
        let mut replay_error = None;
        {
            // Batched replay: update deltas land in bulk import_batch calls
            // (12× faster than per-frame imports on real stores — see
            // ReplayBuffer), with a frame-at-a-time fallback preserving
            // exact stop-at-first-failure semantics.
            let mut buffer = super::ReplayBuffer::new(&doc, &mut ledger);
            journal::visit_records(&paths, |item| {
                let record = match item {
                    Ok(record) => record,
                    Err((seg, msg)) => {
                        replay_error =
                            Some(SheafError::StoreCorrupt(format!("segment {seg}: {msg}")));
                        return false;
                    }
                };
                let at = super::FrameAt {
                    segment: record.segment,
                    ordinal: record.ordinal,
                };
                match super::ledger::classify_payload(&record.payload) {
                    Some(super::ledger::Frame::Update(delta)) => {
                        match buffer.push_update(delta, at) {
                            Ok(()) => true,
                            Err(failure) => {
                                replay_error = Some(failure.error);
                                false
                            }
                        }
                    }
                    Some(super::ledger::Frame::Record(rec)) => {
                        buffer.push_record(rec, at);
                        true
                    }
                    None => true, // future/torn frame: skip, keep replaying
                }
            });
            if let Err(failure) = buffer.flush() {
                replay_error = replay_error.or(Some(failure.error));
            }
        }
        if let Some(error) = replay_error {
            return Err(error);
        }
        Ok(Self {
            root: root.to_path_buf(),
            doc,
            ledger,
            grep_content_cache: RefCell::new(super::grep::GrepContentCache::open(root, false)),
        })
    }

    pub fn current_frontier(&self) -> String {
        read_head_frontier(&self.root)
            .and_then(|raw| decode_frontier(&raw).ok())
            .filter(|frontier| self.doc.frontiers_to_vv(frontier).is_some())
            .map(|frontier| encode_frontier(&frontier))
            .unwrap_or_else(|| encode_frontier(&self.doc.oplog_frontiers()))
    }

    /// Detail a capture by ID prefix and compare it with its actual parent.
    /// Checkpoint metadata can sit between captures, so using `@~1` here
    /// would be incorrect; the encoded parent frontier is authoritative.
    pub fn capture_info(&self, reference: &str) -> Result<CaptureInfo> {
        capture_info_from(
            &self.root,
            &self.doc,
            &self.ledger,
            &decode_frontier(&self.current_frontier())?,
            reference,
        )
    }

    pub fn captures(
        &self,
        all_branches: bool,
        path: Option<&Path>,
        follow: bool,
        limit: usize,
    ) -> Result<Vec<Capture>> {
        let start = if all_branches {
            self.doc.oplog_frontiers()
        } else {
            decode_frontier(&self.current_frontier())?
        };
        let current = decode_frontier(&self.current_frontier())?;
        let names = path.map(|p| path_names(&self.doc, p)).filter(|_| follow);
        let mut entries = captures_from(
            &self.doc,
            &self.ledger,
            &start,
            path,
            names.as_deref(),
            limit,
        )?;
        if all_branches {
            mark_current_lineage(&self.doc, &self.ledger, &current, &mut entries);
        }
        annotate_checkpoints(&self.doc, &self.ledger, &mut entries);
        Ok(entries)
    }

    /// Tombstones of captures whose content was reclaimed (ghosts).
    pub fn pruned(&self) -> Vec<(String, super::ledger::TombstoneRec)> {
        self.ledger
            .tombstones
            .iter()
            .map(|(id, t)| (id.clone(), t.clone()))
            .collect()
    }

    pub fn checkpoints(&self) -> Vec<Checkpoint> {
        let Ok(current) = decode_frontier(&self.current_frontier()) else {
            return checkpoints_from(&self.doc, &self.ledger, None);
        };
        checkpoints_from(&self.doc, &self.ledger, Some(&current))
    }

    pub fn resolve(&self, reference: &str) -> Result<ResolvedPoint> {
        resolve_in_doc(
            &self.doc,
            &self.ledger,
            &decode_frontier(&self.current_frontier())?,
            reference,
        )
    }

    pub fn resolve_at(&self, timestamp_ms: i64) -> Result<ResolvedPoint> {
        resolve_at_in_doc(
            &self.doc,
            &self.ledger,
            &decode_frontier(&self.current_frontier())?,
            timestamp_ms,
        )
    }

    pub fn branch_tips(&self) -> Result<Vec<BranchTip>> {
        branch_tips_from(&self.doc)
    }

    /// Materialized frontier as decoded `Frontiers`, for grep's shared engine.
    pub(super) fn materialized_frontiers(&self) -> Frontiers {
        decode_frontier(&self.current_frontier()).unwrap_or_else(|_| self.doc.oplog_frontiers())
    }
}

fn capture_info_from(
    root: &Path,
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    reference: &str,
) -> Result<CaptureInfo> {
    let point = resolve_in_doc(doc, ledger, current, reference)?;
    let frontier = decode_frontier(&point.frontier)?;
    let capture = capture_at_frontier(doc, &frontier).ok_or_else(|| {
        SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
    })?;
    let parent_frontier = decode_frontier(&capture.parent_frontier)?;
    let parent = ResolvedPoint {
        frontier: capture.parent_frontier.clone(),
        capture_id: capture_id_at(doc, &parent_frontier),
    };
    let ignore = crate::ignore::IgnoreSet::for_project(root, &config::load(root)?.ignore.patterns)
        .map_err(|e| SheafError::Config(e.to_string()))?;
    let diff = super::diff::compute_diff_points(
        root,
        doc,
        current,
        parent,
        Some(ResolvedPoint {
            frontier: capture.frontier.clone(),
            capture_id: Some(capture.id.clone()),
        }),
        &[],
        &ignore,
    )?;
    Ok(CaptureInfo { capture, diff })
}

impl ProjectStore {
    pub(super) fn materialized_frontiers(&self) -> Frontiers {
        read_head_frontier(&self.root)
            .and_then(|raw| decode_frontier(&raw).ok())
            .filter(|frontier| self.doc.frontiers_to_vv(frontier).is_some())
            .unwrap_or_else(|| self.doc.state_frontiers())
    }

    pub fn current_frontier(&self) -> String {
        encode_frontier(&self.materialized_frontiers())
    }

    /// Detail a capture from the live collector without reopening the store.
    pub fn capture_info(&self, reference: &str) -> Result<CaptureInfo> {
        capture_info_from(
            &self.root,
            &self.doc,
            &self.ledger,
            &self.materialized_frontiers(),
            reference,
        )
    }

    pub fn captures(
        &self,
        all_branches: bool,
        path: Option<&Path>,
        follow: bool,
        limit: usize,
    ) -> Result<Vec<Capture>> {
        let start = if all_branches {
            self.doc.oplog_frontiers()
        } else {
            self.materialized_frontiers()
        };
        let current = self.materialized_frontiers();
        let names = path.map(|p| path_names(&self.doc, p)).filter(|_| follow);
        let mut entries = captures_from(
            &self.doc,
            &self.ledger,
            &start,
            path,
            names.as_deref(),
            limit,
        )?;
        if all_branches {
            mark_current_lineage(&self.doc, &self.ledger, &current, &mut entries);
        }
        annotate_checkpoints(&self.doc, &self.ledger, &mut entries);
        Ok(entries)
    }

    /// Tombstones of captures whose content was reclaimed (ghosts).
    pub fn pruned(&self) -> Vec<(String, super::ledger::TombstoneRec)> {
        self.ledger
            .tombstones
            .iter()
            .map(|(id, t)| (id.clone(), t.clone()))
            .collect()
    }

    pub fn resolve(&self, reference: &str) -> Result<ResolvedPoint> {
        resolve_in_doc(
            &self.doc,
            &self.ledger,
            &self.materialized_frontiers(),
            reference,
        )
    }

    pub fn resolve_at(&self, timestamp_ms: i64) -> Result<ResolvedPoint> {
        resolve_at_in_doc(
            &self.doc,
            &self.ledger,
            &self.materialized_frontiers(),
            timestamp_ms,
        )
    }

    pub fn checkpoints(&self) -> Vec<Checkpoint> {
        checkpoints_from(
            &self.doc,
            &self.ledger,
            Some(&self.materialized_frontiers()),
        )
    }

    /// Append a checkpoint label without creating a user-visible capture.
    /// Labels are ledger records (format 2): navigation state
    /// belongs in the mutable layer, and the old replicated `_sheaf.meta`
    /// read path leaned on `fork_at`, which is unimplemented on shallow
    /// (retention-trimmed) documents. Legacy meta-map labels from format-1
    /// history still list and resolve — see [`checkpoints_from`].
    pub fn create_checkpoint(&mut self, name: &str, reference: Option<&str>) -> Result<Checkpoint> {
        validate_checkpoint_name(name)?;
        // Names are unique across the whole graph, not just this lineage.
        if checkpoints_from(&self.doc, &self.ledger, None)
            .iter()
            .any(|c| c.name == name)
        {
            return Err(SheafError::CheckpointExists(name.to_owned()));
        }
        let current = self.materialized_frontiers();
        let target = match reference {
            Some(r) => resolve_in_doc(&self.doc, &self.ledger, &current, r)?,
            None => {
                let f = current;
                ResolvedPoint {
                    capture_id: capture_id_at(&self.doc, &f),
                    frontier: encode_frontier(&f),
                }
            }
        };
        let record = super::ledger::LedgerRecord::Checkpoint {
            name: name.to_owned(),
            frontier: target.frontier.clone(),
            capture_id: target.capture_id.clone(),
        };
        let payload = record.encode();
        self.journal
            .append_batch_synced(&[payload.as_slice()])
            .map_err(super::io_err)?;
        self.ledger.fold(record);
        let pinned = decode_frontier(&target.frontier)
            .ok()
            .and_then(|f| capture_at_frontier(&self.doc, &f));
        Ok(Checkpoint {
            name: name.to_owned(),
            frontier: target.frontier,
            capture_id: target.capture_id.clone(),
            timestamp_ms: pinned.as_ref().map(|c| c.timestamp_ms),
            on_current: true,
        })
    }

    /// Move only the CRDT materialized state, leaving the worktree and the
    /// head file alone. The restore engine's full mode is the
    /// worktree-safe counterpart; this stays as the bare branching primitive.
    pub fn checkout_for_branch(&mut self, frontier: &str) -> Result<()> {
        let f = decode_frontier(frontier)?;
        self.doc.checkout(&f).map_err(super::store_err)?;
        self.doc.set_detached_editing(true);
        // This bare branching primitive intentionally leaves worktree.head
        // alone, so invalidate the lineage-attributed cursor cache here rather
        // than relying on `write_head_point` as the full restore path does.
        self.grep_content_cache
            .borrow_mut()
            .invalidate_cursor_states();
        Ok(())
    }

    pub fn branch_tips(&self) -> Result<Vec<BranchTip>> {
        branch_tips_from(&self.doc)
    }
}

pub(super) fn commit_capture(
    doc: &LoroDoc,
    batch: &Batch,
    origin: Option<CaptureOrigin>,
) -> Result<Capture> {
    let meta = CaptureMeta {
        at_ms: batch.flushed_at.timestamp_millis(),
        paths: batch_paths(batch),
        events: batch.events.len(),
        parent: encode_frontier(&doc.state_frontiers()),
        origin,
    };
    let json = serde_json::to_string(&meta)
        .map_err(|e| SheafError::StoreCorrupt(format!("capture metadata: {e}")))?;
    doc.commit_with(
        CommitOptions::new()
            .origin("sheaf")
            .timestamp(batch.flushed_at.timestamp())
            .commit_msg(&format!("{CAPTURE_PREFIX}{json}")),
    );
    let frontier = doc.state_frontiers();
    capture_at_frontier(doc, &frontier).ok_or_else(|| {
        SheafError::StoreCorrupt("capture commit produced no addressable change".into())
    })
}

fn batch_paths(batch: &Batch) -> Vec<String> {
    let mut paths = BTreeSet::new();
    let rel = |p: &Path| {
        p.strip_prefix(&batch.root)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/")
    };
    for event in &batch.events {
        match &event.kind {
            EventKind::Added { path } | EventKind::Removed { path } => {
                paths.insert(rel(path));
            }
            EventKind::Touched { path } => {
                paths.insert(rel(&path.0));
            }
            EventKind::Renamed { from, to } => {
                paths.insert(rel(from));
                paths.insert(rel(to));
            }
        }
    }
    paths.into_iter().collect()
}

pub(super) fn captures_from(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    start: &Frontiers,
    path: Option<&Path>,
    follow_names: Option<&[String]>,
    limit: usize,
) -> Result<Vec<Capture>> {
    if start.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let ids: Vec<ID> = start.iter().collect();
    doc.travel_change_ancestors(&ids, &mut |change| {
        if let Some(capture) = capture_from_change(change) {
            // Tombstoned captures are not part of the navigable timeline:
            // `@~N`, time resolution, and lineage walks count
            // only what is still restorable.
            if !ledger.is_tombstoned(&capture.id) && capture_matches(&capture, path, follow_names) {
                out.push(capture);
            }
        }
        if out.len() >= limit {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .map_err(|e| SheafError::StoreCorrupt(format!("timeline traversal: {e}")))?;
    Ok(out)
}

/// Whether one frontier lies on the worktree's live causal lineage. Point
/// discovery at an abandoned-branch capture must not present its hits as
/// trunk state, so single-capture readers need the same DAG truth the
/// `--all` walk uses.
pub(super) fn frontier_on_current(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    frontier: &str,
) -> bool {
    captures_from(doc, ledger, current, None, None, usize::MAX)
        .map(|cs| cs.iter().any(|c| c.frontier == frontier))
        .unwrap_or(false)
}

/// Causal-lineage membership of every listed capture, so an `--all` view can
/// distinguish the worktree's live lineage from abandoned futures. Version
/// vectors cannot answer this after a restore (one peer, counters that skip
/// across the checkout), so the actual Change DAG is the truth.
fn mark_current_lineage(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    entries: &mut [Capture],
) {
    let lineage: BTreeSet<String> = captures_from(doc, ledger, current, None, None, usize::MAX)
        .map(|cs| cs.iter().map(|c| c.frontier.clone()).collect())
        .unwrap_or_default();
    for entry in entries.iter_mut() {
        entry.on_current = lineage.contains(&entry.frontier);
    }
}

fn capture_matches(capture: &Capture, path: Option<&Path>, names: Option<&[String]>) -> bool {
    let Some(_) = path else {
        return true;
    };
    let needles: Vec<String> = match names {
        Some(set) => set.to_vec(),
        None => vec![path
            .expect("checked")
            .to_string_lossy()
            .trim_start_matches("./")
            .replace('\\', "/")],
    };
    capture.paths.iter().any(|p| {
        needles
            .iter()
            .any(|n| p == n || p.starts_with(&format!("{n}/")))
    })
}

/// Every name a path has worn, derived from first-class rename events.
/// Prefix-aware for directory renames, transitive for chains.
pub(super) fn path_names(doc: &LoroDoc, path: &Path) -> Vec<String> {
    let needle = path
        .to_string_lossy()
        .trim_start_matches("./")
        .replace('\\', "/");
    let tip = doc.oplog_frontiers();
    let renames = if encode_frontier(&doc.state_frontiers()) == encode_frontier(&tip) {
        read_renames(doc)
    } else {
        match doc.fork_at(&tip) {
            Ok(merged) => read_renames(&merged),
            Err(error) => {
                tracing::warn!(%error, "cannot fork at tip for rename following");
                read_renames(doc)
            }
        }
    };
    let mut names = BTreeSet::from([needle]);
    loop {
        let mut grew = false;
        let snapshot: Vec<String> = names.iter().cloned().collect();
        for key in &snapshot {
            for (from, to) in &renames {
                let older = if key == to || key.starts_with(&format!("{to}/")) {
                    Some(format!("{from}{}", &key[to.len()..]))
                } else {
                    None
                };
                grew |= older.is_some_and(|o| names.insert(o));
            }
        }
        if !grew {
            break;
        }
    }
    names.into_iter().collect()
}

/// Structural rename (from, to) records, in history order.
pub(super) fn read_renames(doc: &LoroDoc) -> Vec<(String, String)> {
    let mut out = Vec::new();
    doc.get_list(super::TREE_EVENTS_LIST).for_each(|value| {
        let Ok(raw) = value.get_deep_value().into_string() else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return;
        };
        let event = &parsed["event"];
        if event["kind"] == "renamed" {
            if let (Some(from), Some(to)) = (event["from"].as_str(), event["to"].as_str()) {
                out.push((from.to_owned(), to.to_owned()));
            }
        }
    });
    out
}

fn capture_from_change(change: loro::ChangeMeta) -> Option<Capture> {
    let raw = change.message.as_deref()?.strip_prefix(CAPTURE_PREFIX)?;
    let meta: CaptureMeta = serde_json::from_str(raw).ok()?;
    if change.len == 0 {
        return None;
    }
    let end = ID::new(change.id.peer, change.id.counter + change.len as i32 - 1);
    let frontier = Frontiers::from_id(end);
    Some(Capture {
        id: frontier_id(&frontier),
        frontier: encode_frontier(&frontier),
        parent_frontier: encode_frontier(&change.deps),
        timestamp_ms: meta.at_ms,
        paths: meta.paths,
        events: meta.events,
        checkpoints: Vec::new(),
        origin: meta.origin,
        on_current: true,
    })
}

fn annotate_checkpoints(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    entries: &mut [Capture],
) {
    let mut names: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for checkpoint in checkpoints_from(doc, ledger, None) {
        if let Some(id) = checkpoint.capture_id {
            names.entry(id).or_default().push(checkpoint.name);
        }
    }
    for entry in entries {
        entry.checkpoints = names.remove(&entry.id).unwrap_or_default();
    }
}

pub(super) fn capture_at_frontier(doc: &LoroDoc, frontier: &Frontiers) -> Option<Capture> {
    let ids: Vec<_> = frontier.iter().collect();
    let mut found = None;
    let _ = doc.travel_change_ancestors(&ids, &mut |change| {
        if let Some(c) = capture_from_change(change) {
            found = Some(c);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    found
}

pub(super) fn capture_id_at(doc: &LoroDoc, frontier: &Frontiers) -> Option<String> {
    // A multi-head state is exact as a frontier but is not "the" result of
    // one capture, so do not attach an arbitrary branch's display ID.
    (frontier.len() == 1)
        .then(|| capture_at_frontier(doc, frontier))
        .flatten()
        .map(|capture| capture.id)
}

pub(super) fn resolve_in_doc(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    reference: &str,
) -> Result<ResolvedPoint> {
    if reference == "@" {
        return Ok(ResolvedPoint {
            frontier: encode_frontier(current),
            capture_id: capture_id_at(doc, current),
        });
    }
    if let Some(raw) = reference.strip_prefix("@~") {
        // Two spellings share the `@~` prefix: an integer is N captures back
        // (`@~10`), while a compact duration is wall-clock relative to now
        // (`@~2h`, `@~30m`, `@~2d`). An integer must win so `@~10` never reads
        // as a duration; only a non-integer tail is tried as a duration.
        if let Ok(n) = raw.parse::<usize>() {
            let list = captures_from(doc, ledger, current, None, None, n + 1)?;
            let capture = list.get(n).ok_or_else(|| {
                SheafError::TimelineReference(format!(
                    "reference `{reference}` is before recorded capture history"
                ))
            })?;
            return Ok(ResolvedPoint {
                frontier: capture.frontier.clone(),
                capture_id: Some(capture.id.clone()),
            });
        }
        if let Some(dur) = parse_compact_duration(raw) {
            return resolve_at_in_doc(doc, ledger, current, (Utc::now() - dur).timestamp_millis());
        }
        return Err(SheafError::TimelineReference(format!(
            "invalid relative reference `{reference}` (expected `@~N` captures or `@~<duration>` like `2h`)"
        )));
    }
    if let Some(name) = reference.strip_prefix("checkpoint:") {
        let cp = checkpoints_from(doc, ledger, None)
            .into_iter()
            .find(|c| c.name == name)
            .ok_or_else(|| SheafError::TimelineReference(format!("unknown checkpoint `{name}`")))?;
        if let Some(id) = &cp.capture_id {
            if let Some(tomb) = ledger.tombstone(id) {
                return Err(SheafError::TimelineReference(format!(
                    "checkpoint `{name}` pins capture {}, which was pruned by {} and its                      content has been reclaimed",
                    &id[..12.min(id.len())],
                    tomb.cause.as_str(),
                )));
            }
        }
        if decode_frontier(&cp.frontier)
            .ok()
            .and_then(|f| doc.frontiers_to_vv(&f))
            .is_none()
        {
            return Err(SheafError::TimelineReference(format!(
                "checkpoint `{name}` pins a point outside this store's recorded history"
            )));
        }
        return Ok(ResolvedPoint {
            frontier: cp.frontier,
            capture_id: cp.capture_id,
        });
    }
    let explicit_time = reference.strip_prefix("time:");
    if let Some(spec) = explicit_time {
        let timestamp = parse_timestamp_spec(spec)
            .ok_or_else(|| SheafError::TimelineReference(format!("invalid timestamp `{spec}`")))?;
        return resolve_at_in_doc(doc, ledger, current, timestamp);
    }
    // Bare absolute and human-relative times cannot collide with hexadecimal
    // capture IDs, so `--at "2 hours ago"` needs no namespace prefix.
    if let Some(timestamp) = parse_timestamp_spec(reference) {
        return resolve_at_in_doc(doc, ledger, current, timestamp);
    }
    // A bare checkpoint name (`restore "pre-change"`) is convenient shorthand
    // for `checkpoint:pre-change`. Tried only after time parsing so a label
    // that happens to look like a timestamp never shadows the clock, and
    // before the hex-ID path so a purely hexadecimal label is still reachable
    // by its explicit `checkpoint:` form. An unambiguous exact-name match wins.
    if let Some(cp) = checkpoints_from(doc, ledger, None)
        .into_iter()
        .find(|c| c.name == reference)
    {
        if let Some(id) = &cp.capture_id {
            if let Some(tomb) = ledger.tombstone(id) {
                return Err(SheafError::TimelineReference(format!(
                    "checkpoint `{}` pins capture {}, which was pruned by {} and its                      content has been reclaimed",
                    cp.name,
                    &id[..12.min(id.len())],
                    tomb.cause.as_str(),
                )));
            }
        }
        if decode_frontier(&cp.frontier)
            .ok()
            .and_then(|f| doc.frontiers_to_vv(&f))
            .is_none()
        {
            return Err(SheafError::TimelineReference(format!(
                "checkpoint `{}` pins a point outside this store's recorded history",
                cp.name
            )));
        }
        return Ok(ResolvedPoint {
            frontier: cp.frontier,
            capture_id: cp.capture_id,
        });
    }
    if reference.len() < MIN_ID_PREFIX || !reference.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SheafError::TimelineReference(format!(
            "unknown timeline reference `{reference}`"
        )));
    }
    // A pruned capture's ghost names who reclaimed it, so the error is an
    // explanation rather than a shrug.
    let ghosts: Vec<_> = ledger
        .tombstones
        .range(reference.to_ascii_lowercase()..)
        .take_while(|(id, _)| id.starts_with(&reference.to_ascii_lowercase()))
        .collect();
    if ghosts.len() == 1 {
        let (id, tomb) = ghosts[0];
        return Err(SheafError::TimelineReference(format!(
            "capture {} was pruned by {} at {} and its content has been reclaimed;              the earliest restorable point is at or after that time",
            &id[..12.min(id.len())],
            tomb.cause.as_str(),
            chrono::Local
                .timestamp_millis_opt(tomb.pruned_at_ms)
                .single()
                .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| tomb.pruned_at_ms.to_string()),
        )));
    }
    if ghosts.len() > 1 {
        return Err(SheafError::TimelineReference(format!(
            "ambiguous pruned capture prefix `{reference}`"
        )));
    }
    let mut matches: Vec<_> =
        captures_from(doc, ledger, &doc.oplog_frontiers(), None, None, usize::MAX)?
            .into_iter()
            .filter(|c| c.id.starts_with(&reference.to_ascii_lowercase()))
            .collect();
    matches.dedup_by(|a, b| a.id == b.id);
    match matches.len() {
        0 => Err(SheafError::TimelineReference(format!(
            "unknown capture `{reference}`"
        ))),
        1 => {
            let c = matches.remove(0);
            Ok(ResolvedPoint {
                frontier: c.frontier,
                capture_id: Some(c.id),
            })
        }
        _ => Err(SheafError::TimelineReference(format!(
            "ambiguous capture prefix `{reference}`"
        ))),
    }
}

fn resolve_at_in_doc(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    timestamp_ms: i64,
) -> Result<ResolvedPoint> {
    let mut candidate: Option<Capture> = None;
    for capture in captures_from(doc, ledger, current, None, None, usize::MAX)? {
        if capture.timestamp_ms <= timestamp_ms
            && candidate
                .as_ref()
                .is_none_or(|found| capture.timestamp_ms > found.timestamp_ms)
        {
            candidate = Some(capture);
        }
    }
    let capture = candidate.ok_or_else(|| {
        SheafError::TimelineReference("timestamp is before recorded capture history".into())
    })?;
    Ok(ResolvedPoint {
        frontier: capture.frontier,
        capture_id: Some(capture.id),
    })
}

/// A compact relative duration: `<count><unit>` with unit in
/// s/m/h/d (seconds, minutes, hours, days). Used by the `@~<duration>`
/// timeline reference. Returns None for anything else (e.g. a bare integer,
/// which the caller treats as N-captures-back instead).
fn parse_compact_duration(spec: &str) -> Option<Duration> {
    let spec = spec.trim();
    let (num, unit) = spec.split_at(spec.find(|c: char| !c.is_ascii_digit())?);
    let count: i64 = num.parse().ok()?;
    match unit {
        "s" => Some(Duration::seconds(count)),
        "m" => Some(Duration::minutes(count)),
        "h" => Some(Duration::hours(count)),
        "d" => Some(Duration::days(count)),
        _ => None,
    }
}

fn parse_timestamp_spec(spec: &str) -> Option<i64> {
    if let Some(ms) = parse_absolute_timestamp(spec) {
        return Some(ms);
    }
    let lower = spec.trim().to_ascii_lowercase();
    if let Some(clock) = lower.strip_prefix("yesterday ") {
        let time = NaiveTime::parse_from_str(clock.trim(), "%H:%M").ok()?;
        let date = Local::now()
            .date_naive()
            .checked_sub_signed(Duration::days(1))?;
        return Local
            .from_local_datetime(&date.and_time(time))
            .single()
            .map(|dt| dt.timestamp_millis());
    }
    let parts: Vec<_> = lower.split_whitespace().collect();
    if parts.len() == 3 && parts[2] == "ago" {
        let count: i64 = parts[0].parse().ok()?;
        let duration = match parts[1].trim_end_matches('s') {
            "second" => Duration::seconds(count),
            "minute" => Duration::minutes(count),
            "hour" => Duration::hours(count),
            "day" => Duration::days(count),
            _ => return None,
        };
        return Some((Utc::now() - duration).timestamp_millis());
    }
    None
}

fn parse_absolute_timestamp(spec: &str) -> Option<i64> {
    let spec = spec.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(spec) {
        return Some(dt.timestamp_millis());
    }
    // Timezone-less `T`-separated forms (`2026-08-27T10:30[:00]`), as the
    // product README writes them, interpreted in local time. Tried before the
    // space-separated forms because the `T` separator is what the doc shows.
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(spec, fmt) {
            return Local
                .from_local_datetime(&dt)
                .single()
                .map(|v| v.timestamp_millis());
        }
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(spec, "%Y-%m-%d %H:%M:%S") {
        return Local
            .from_local_datetime(&dt)
            .single()
            .map(|v| v.timestamp_millis());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(spec, "%Y-%m-%d %H:%M") {
        return Local
            .from_local_datetime(&dt)
            .single()
            .map(|v| v.timestamp_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(spec, "%Y-%m-%d") {
        return Local
            .from_local_datetime(&date.and_hms_opt(0, 0, 0)?)
            .single()
            .map(|v| v.timestamp_millis());
    }
    // Clock-only (`10:30` / `10:30:00`) means that time today, local — the
    // README's `sheaf restore '10:30'`. Placed last so a full date always
    // wins; a bare clock cannot collide with a hex capture ID (it has `:`).
    for fmt in ["%H:%M:%S", "%H:%M"] {
        if let Ok(time) = NaiveTime::parse_from_str(spec, fmt) {
            let today = Local::now().date_naive();
            return Local
                .from_local_datetime(&today.and_time(time))
                .single()
                .map(|v| v.timestamp_millis());
        }
    }
    None
}

/// Checkpoints are labels over the whole version graph, not over whichever
/// point the worktree happens to sit on. Reading them from the materialized
/// state would make every name created after a restore target vanish the
/// moment that restore repositioned the worktree, so they are read from the
/// merged oplog tip where the CRDT map holds every branch's labels.
///
/// `current` (when known) additionally pins each label's capture timestamp
/// and whether it sits on the worktree's live lineage.
pub(super) fn checkpoints_from(
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: Option<&Frontiers>,
) -> Vec<Checkpoint> {
    // Ledger-native labels first; legacy format-1 labels survive
    // in the `_sheaf.meta` map and are merged underneath, with the ledger
    // winning any name the two layers share.
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    {
        let tip = doc.oplog_frontiers();
        let legacy = if encode_frontier(&doc.state_frontiers()) == encode_frontier(&tip) {
            checkpoint_labels(doc)
        } else {
            // fork_at is unimplemented on shallow docs; the fallback reads
            // current-state labels, which for a trimmed store is every
            // label that still matters (pre-boundary targets are refused
            // at resolution time anyway).
            match doc.fork_at(&tip) {
                Ok(merged) => checkpoint_labels(&merged),
                Err(_) => checkpoint_labels(doc),
            }
        };
        for (name, frontier) in legacy {
            labels.insert(name, frontier);
        }
    }
    for (name, rec) in &ledger.checkpoints {
        labels.insert(name.clone(), rec.frontier.clone());
    }
    let lineage: Option<BTreeSet<String>> = current.map(|cur| {
        captures_from(doc, ledger, cur, None, None, usize::MAX)
            .map(|cs| cs.iter().map(|c| c.frontier.clone()).collect())
            .unwrap_or_default()
    });
    let mut out: Vec<Checkpoint> = labels
        .into_iter()
        .map(|(name, frontier)| {
            let capture = decode_frontier(&frontier)
                .ok()
                .and_then(|f| capture_at_frontier(doc, &f));
            // When the pinned capture was pruned, the ledger record still
            // knows its id — resolution needs it to explain the prune.
            let recorded_id = ledger
                .checkpoints
                .get(&name)
                .and_then(|r| r.capture_id.clone());
            Checkpoint {
                capture_id: capture.as_ref().map(|c| c.id.clone()).or(recorded_id),
                timestamp_ms: capture.as_ref().map(|c| c.timestamp_ms),
                on_current: match &lineage {
                    None => true,
                    Some(set) => set.contains(&frontier),
                },
                name,
                frontier,
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn checkpoint_labels(doc: &LoroDoc) -> Vec<(String, String)> {
    let mut out = Vec::new();
    doc.get_map(META_MAP).for_each(|key, value| {
        let Some(name) = key.strip_prefix(CHECKPOINT_PREFIX) else {
            return;
        };
        let Ok(frontier) = value.get_deep_value().into_string() else {
            return;
        };
        out.push((name.to_owned(), frontier.to_string()));
    });
    out
}

pub(super) fn branch_tips_from(doc: &LoroDoc) -> Result<Vec<BranchTip>> {
    let mut out = Vec::new();
    for id in doc.oplog_frontiers().iter() {
        let f = Frontiers::from_id(id);
        out.push(BranchTip {
            frontier: encode_frontier(&f),
            capture_id: capture_id_at(doc, &f),
        });
    }
    out.sort_by(|a, b| a.frontier.cmp(&b.frontier));
    Ok(out)
}

/// Checkpoint labels are human-facing and may contain spaces (the product
/// surface advertises names like `before refactoring`). A name is one whole
/// argument, so spaces are unambiguous; we still reject control characters
/// (including newlines/tabs) and leading/trailing whitespace so a label can
/// never corrupt a line-oriented listing or hide surrounding blanks.
fn validate_checkpoint_name(name: &str) -> Result<()> {
    let trimmed_len_matches = name == name.trim();
    let ok = !name.is_empty()
        && name.len() <= 128
        && trimmed_len_matches
        && name.chars().all(|c| !c.is_control());
    if ok {
        Ok(())
    } else {
        Err(SheafError::Config(format!(
            "invalid checkpoint name `{name}`"
        )))
    }
}

fn frontier_id(frontier: &Frontiers) -> String {
    hex::encode(Sha256::digest(frontier.encode()))
}

pub fn encode_frontier(frontier: &Frontiers) -> String {
    hex::encode(frontier.encode())
}

pub fn decode_frontier(encoded: &str) -> Result<Frontiers> {
    let bytes = hex::decode(encoded)
        .map_err(|_| SheafError::TimelineReference("invalid frontier encoding".into()))?;
    Frontiers::decode(&bytes)
        .map_err(|e| SheafError::TimelineReference(format!("invalid frontier: {e}")))
}

pub(super) fn read_head_frontier(root: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(crate::config::worktree_head_path(root)).ok()?;
    serde_json::from_str::<serde_json::Value>(&raw)
        .ok()?
        .get("frontier")?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Batch, EventKind, FsEvent, TouchedPath};
    use crate::store::StoreLimits;
    use tempfile::tempdir;

    fn limits() -> StoreLimits {
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 3,
        }
    }

    /// Open a fresh store at `root` (skeleton included) and leave it closed.
    fn opened(root: &Path) -> ProjectStore {
        crate::config::write_skeleton(root).unwrap();
        ProjectStore::open(root, limits()).unwrap()
    }

    /// Write one file and return the batch that captures the touch.
    fn touch(root: &Path, rel: &str) -> Batch {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, format!("// {rel}\n")).unwrap();
        Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Touched {
                path: TouchedPath(path),
            })],
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn compact_duration_parses_units_and_rejects_junk() {
        for (spec, expected_secs) in [
            ("90s", 90),
            ("5m", 300),
            ("2h", 7_200),
            ("3d", 259_200),
            (" 4h ", 14_400), // surrounding whitespace tolerated
        ] {
            let got = parse_compact_duration(spec).unwrap_or_else(|| panic!("{spec:?} must parse"));
            assert_eq!(got.num_seconds(), expected_secs, "{spec:?}");
        }
        for junk in [
            "",
            "   ",
            "12",
            "h",
            "5x",
            "x5",
            "-3h",
            "99999999999999999999s",
        ] {
            assert!(
                parse_compact_duration(junk).is_none(),
                "{junk:?} must not parse"
            );
        }
    }

    #[test]
    fn relative_ago_and_yesterday_specs_resolve_to_wall_clock() {
        let now = chrono::Utc::now().timestamp_millis();
        let got = parse_timestamp_spec("3 minutes ago").unwrap();
        let low = now - chrono::Duration::minutes(4).num_milliseconds();
        let high = now - chrono::Duration::minutes(2).num_milliseconds();
        assert!((low..=high).contains(&got), "{got} outside window");

        assert!(parse_timestamp_spec("1 second ago").is_some());
        assert!(parse_timestamp_spec("2 hours ago").is_some());
        assert!(parse_timestamp_spec("5 days ago").is_some());

        // yesterday 10:30 is in the past, within ~36 hours.
        let got = parse_timestamp_spec("yesterday 10:30").unwrap();
        assert!(got < now && got > now - chrono::Duration::hours(49).num_milliseconds());

        for junk in [
            "3 fortnights ago",
            "minutes ago",
            "x minutes ago",
            "yesterday 25:00",
        ] {
            assert!(
                parse_timestamp_spec(junk).is_none(),
                "{junk:?} must not parse"
            );
        }
    }

    #[test]
    fn absolute_timestamps_parse_every_documented_form_and_reject_garbage() {
        // Fixed UTC instants are exact regardless of local timezone.
        assert_eq!(
            parse_absolute_timestamp("2020-01-02T03:04:05Z"),
            Some(1_577_934_245_000)
        );
        assert_eq!(
            parse_absolute_timestamp("2020-01-02T03:04:05.500Z"),
            Some(1_577_934_245_500)
        );
        assert_eq!(
            parse_absolute_timestamp(" 2020-01-02T03:04:05 "),
            parse_absolute_timestamp("2020-01-02T03:04:05")
        );
        assert_eq!(
            parse_absolute_timestamp("2020-01-02T03:04"),
            parse_absolute_timestamp("2020-01-02T03:04:00")
        );
        assert_eq!(
            parse_absolute_timestamp("2020-01-02 03:04"),
            parse_absolute_timestamp("2020-01-02T03:04:00")
        );

        // Date-only is local midnight; clock-only is today at that local time.
        let date_only = parse_absolute_timestamp("2020-01-02").unwrap();
        let clock_only = parse_absolute_timestamp("10:30").unwrap();
        let clock_secs = parse_absolute_timestamp("10:30:00").unwrap();
        assert_eq!(clock_only, clock_secs);
        assert!(date_only < clock_only);

        for junk in [
            "",
            "   ",
            "nonsense",
            "2020-13-40",
            "99:99",
            "2020-01-02T99:00",
        ] {
            assert!(
                parse_absolute_timestamp(junk).is_none(),
                "{junk:?} must not parse"
            );
        }
        assert!(parse_timestamp_spec("2020-01-02T03:04:05Z").is_some());
    }

    #[test]
    fn checkpoint_names_accept_spaces_and_reject_control_and_padding() {
        for good in ["before-refactor", "before work", &"x".repeat(128)] {
            validate_checkpoint_name(good).unwrap_or_else(|e| panic!("{good:?}: {e}"));
        }
        for bad in [
            "",
            " padded",
            "padded ",
            &"x".repeat(129),
            "line\nbreak",
            "tab\there",
            "\u{7}bell",
        ] {
            assert!(
                matches!(
                    validate_checkpoint_name(bad),
                    Err(SheafError::Config(msg)) if msg.contains("invalid checkpoint name")
                ),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn frontiers_roundtrip_through_hex_and_reject_bad_payloads() {
        let frontier = Frontiers::from_id(ID::new(42, 7));
        let encoded = encode_frontier(&frontier);
        let decoded = decode_frontier(&encoded).unwrap();
        assert_eq!(encode_frontier(&decoded), encoded);

        assert!(matches!(
            decode_frontier("zzzz-not-hex"),
            Err(SheafError::TimelineReference(msg)) if msg.contains("encoding")
        ));
        let junk_hex = hex::encode(b"junk-bytes");
        assert!(matches!(
            decode_frontier(&junk_hex),
            Err(SheafError::TimelineReference(msg)) if msg.contains("invalid frontier")
        ));
    }

    #[test]
    fn head_frontier_reader_tolerates_missing_and_malformed_files() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        // No state file at all.
        assert_eq!(read_head_frontier(root), None);

        let state = root.join(".sheaf/state");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(state.join("worktree.head"), r#"{"frontier":"abc123"}"#).unwrap();
        assert_eq!(read_head_frontier(root).as_deref(), Some("abc123"));

        std::fs::write(state.join("worktree.head"), "{}").unwrap();
        assert_eq!(
            read_head_frontier(root),
            None,
            "head without a frontier field"
        );

        std::fs::write(state.join("worktree.head"), r#"{"frontier": 7}"#).unwrap();
        assert_eq!(read_head_frontier(root), None, "non-string frontier");

        std::fs::write(state.join("worktree.head"), "not json").unwrap();
        assert_eq!(read_head_frontier(root), None);
    }

    fn cap(paths: &[&str]) -> Capture {
        Capture {
            id: "abc123def456".into(),
            frontier: "ff00".into(),
            parent_frontier: "00ff".into(),
            timestamp_ms: 0,
            paths: paths.iter().map(|s| s.to_string()).collect(),
            events: 1,
            checkpoints: Vec::new(),
            origin: None,
            on_current: true,
        }
    }

    #[test]
    fn capture_matching_uses_exact_or_directory_prefix_or_follow_names() {
        fn path(p: &str) -> Option<&Path> {
            Some(Path::new(p))
        }
        // No path filter: everything matches.
        assert!(capture_matches(&cap(&["a"]), None, None));
        // Exact match and directory-prefix match.
        assert!(capture_matches(
            &cap(&["src/lib.rs"]),
            path("src/lib.rs"),
            None
        ));
        assert!(capture_matches(&cap(&["src/lib.rs"]), path("src"), None));
        assert!(!capture_matches(&cap(&["srcward/x.rs"]), path("src"), None));
        // "./" needles are normalized before matching.
        assert!(capture_matches(
            &cap(&["src/lib.rs"]),
            path("./src/lib.rs"),
            None
        ));
        // Follow-names set: any name counts.
        let names = vec!["old.rs".to_string(), "new.rs".to_string()];
        assert!(capture_matches(
            &cap(&["new.rs"]),
            path("current.rs"),
            Some(&names)
        ));
        assert!(!capture_matches(
            &cap(&["other.rs"]),
            path("current.rs"),
            Some(&names)
        ));
        // No overlap at all.
        assert!(!capture_matches(
            &cap(&["other.rs"]),
            path("src/lib.rs"),
            None
        ));
    }

    #[test]
    fn batch_paths_are_root_relative_deduped_and_sorted() {
        let outside = tempdir().unwrap();
        let root = tempdir().unwrap();
        let batch = Batch {
            root: root.path().to_path_buf(),
            events: vec![
                FsEvent::now(EventKind::Added {
                    path: root.path().join("b.txt"),
                }),
                FsEvent::now(EventKind::Removed {
                    path: root.path().join("b.txt"), // dedups with the Added above
                }),
                FsEvent::now(EventKind::Touched {
                    path: TouchedPath(root.path().join("a.txt")),
                }),
                FsEvent::now(EventKind::Renamed {
                    from: root.path().join("old.txt"),
                    to: root.path().join("new.txt"),
                }),
                FsEvent::now(EventKind::Added {
                    path: outside.path().join("elsewhere.txt"), // outside root: kept whole
                }),
            ],
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
        };
        let paths = batch_paths(&batch);
        let outside = outside
            .path()
            .join("elsewhere.txt")
            .to_string_lossy()
            .into_owned();
        // BTreeSet ordering: "/" sorts before alphanumerics.
        assert_eq!(
            paths,
            vec![&outside, "a.txt", "b.txt", "new.txt", "old.txt"]
        );
    }

    #[test]
    fn checkpoint_create_validates_names_and_lists_labels() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = opened(root);
        store.apply_batch(&touch(root, "src/a.rs")).unwrap();
        store.apply_batch(&touch(root, "src/b.rs")).unwrap();

        for bad in ["", " padded", "padded ", "line\nbreak"] {
            assert!(
                store.create_checkpoint(bad, None).is_err(),
                "{bad:?} must be rejected"
            );
        }
        let cp = store.create_checkpoint("before work", None).unwrap();
        assert_eq!(cp.name, "before work");
        assert!(cp.on_current);
        assert!(store.checkpoints().iter().any(|c| c.name == "before work"));
    }

    #[test]
    fn resolve_rejects_unknown_references_and_resolves_head() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = opened(root);
        store.apply_batch(&touch(root, "src/a.rs")).unwrap();
        store.apply_batch(&touch(root, "src/b.rs")).unwrap();

        let head = store.resolve("@").unwrap();
        assert!(head.capture_id.is_some(), "head resolves to a capture");

        for bad in ["not-a-real-ref", "@~9999"] {
            assert!(
                matches!(store.resolve(bad), Err(SheafError::TimelineReference(_))),
                "{bad:?} must not resolve"
            );
        }
    }

    #[test]
    fn captures_filter_by_path_and_capture_info_roundtrips() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = opened(root);
        store.apply_batch(&touch(root, "src/lib.rs")).unwrap();
        store.apply_batch(&touch(root, "docs/x.md")).unwrap();

        let all = store.captures(false, None, false, usize::MAX).unwrap();
        assert_eq!(all.len(), 2);
        let src = store
            .captures(false, Some(Path::new("src")), false, usize::MAX)
            .unwrap();
        assert_eq!(src.len(), 1);
        assert!(src[0].paths.iter().any(|p| p == "src/lib.rs"));

        let info = store.capture_info(&all[0].id).unwrap();
        assert_eq!(info.capture.id, all[0].id);
        assert!(matches!(
            store.capture_info("not-a-real-ref"),
            Err(SheafError::TimelineReference(_))
        ));
    }
    #[test]
    fn capture_walk_empty_and_limited_frontiers_are_safe() {
        let doc = LoroDoc::new();
        let ledger = super::super::ledger::LedgerState::default();
        assert!(
            captures_from(&doc, &ledger, &Frontiers::default(), None, None, usize::MAX)
                .unwrap()
                .is_empty()
        );
        assert!(
            captures_from(&doc, &ledger, &Frontiers::default(), None, None, 0)
                .unwrap()
                .is_empty()
        );
        assert!(!frontier_on_current(
            &doc,
            &ledger,
            &Frontiers::default(),
            "missing"
        ));
        assert!(capture_at_frontier(&doc, &Frontiers::default()).is_none());
        assert!(capture_id_at(&doc, &Frontiers::default()).is_none());
    }

    #[test]
    fn rename_reader_ignores_malformed_records_and_follows_directory_names() {
        let doc = LoroDoc::new();
        let list = doc.get_list(super::super::TREE_EVENTS_LIST);
        list.insert(0, "not-json").unwrap();
        list.insert(
            1,
            serde_json::json!({"event":{"kind":"renamed","from":"a","to":"b"}}).to_string(),
        )
        .unwrap();
        list.insert(
            2,
            serde_json::json!({"event":{"kind":"renamed","from":"b","to":"c"}}).to_string(),
        )
        .unwrap();
        list.insert(
            3,
            serde_json::json!({"event":{"kind":"renamed","from":"x"}}).to_string(),
        )
        .unwrap();
        assert_eq!(
            read_renames(&doc),
            vec![("a".into(), "b".into()), ("b".into(), "c".into())]
        );
        assert_eq!(
            path_names(&doc, Path::new("c/file.rs")),
            vec!["a/file.rs", "b/file.rs", "c/file.rs"]
        );
    }

    #[test]
    fn capture_id_requires_single_head_and_checkpoint_resolution_reports_unknown() {
        let tmp = tempdir().unwrap();
        let mut store = opened(tmp.path());
        store.apply_batch(&touch(tmp.path(), "a.txt")).unwrap();
        assert!(store.resolve("checkpoint:missing").is_err());
        let one = store.materialized_frontiers();
        let mut multi = one.clone();
        multi.push(ID::new(999, 1));
        assert!(capture_id_at(&store.doc, &multi).is_none());
    }
}

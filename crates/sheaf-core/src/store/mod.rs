//! ProjectStore: one logical Loro document per enrolled project, persisted
//! through its on-disk layout (journal deltas + periodic snapshots +
//! content-addressed blobs + advisory head file).
//!
//! Single-writer discipline: the daemon's collector thread owns the store
//! entirely; cross-process exclusivity rides on `.sheaf/lock` flock.
//!
//! Text mapping strategy: reconcile each touched/added path
//! against disk state at flush time via a minimal char-level splice
//! (common-prefix/suffix trim ⇒ delete-range + insert), so multibyte edits
//! become precise CRDT ops without external diff machinery.

mod blobs;
mod diff;
mod fragment;
mod frames;
mod fsutil;
mod grep;
mod grep_trigram;
mod journal;
mod ledger;
mod maintenance;
mod restore;
mod selection;
mod smart;
mod squash;
mod timeline;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use chrono::Utc;
use loro::{ExportMode, LoroDoc, LoroResult, LoroText, VersionVector};
use serde::{Deserialize, Serialize};

/// Atomic durable file write (tmp + fsync + rename + parent-dir fsync) for
/// out-of-crate writers such as the CLI's service unit installer.
pub fn atomic_write_public(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    fsutil::atomic_write(path, bytes)
}

pub use blobs::hash_of;
pub use diff::{DiffKind, DiffOutcome, FileDiff, SideContent, SideDesc};
pub use fragment::{
    FragmentAction, FragmentActionKind, FragmentCondition, FragmentConflict, FragmentFilePlan,
    FragmentMode, FragmentPlan, FragmentRange,
};
pub use frames::{
    append_frame, can_stamp_complete_frame, frames_path, newest_partial_anchor,
    partial_frame_count, read_frames, CommitFrame, FrameKind, Projection,
};
pub use grep::{
    GrepAnchor, GrepBackfillOptions, GrepBackfillReport, GrepCacheWatermark, GrepEvent, GrepHit,
    GrepMode, GrepQuery, GrepReport, GrepRequest, GrepSink, GrepStreamRecord,
};
pub use journal::{list_segments, read_records};
pub use ledger::{
    classify_payload, CaptureRec, CheckpointRec, EpochRec, Frame, LedgerRecord, LedgerState,
    PruneCause, TombstoneRec,
};
pub use maintenance::{
    doctor, doctor_fix, gc_apply, gc_plan, gc_run, gc_run_store, retention_mark, AppliedFix, Check,
    DoctorReply, GcOutcome, GcPlan, GcReport, IntegrityReport, MarkedCapture, ProtectedPoint,
    PrunableCapture, Refusal, RepairOutcome, RetentionFacts,
};
pub use restore::{
    pending_restore_at, scope_key, ActionKind, ContentKind, Obstacle, Obstruction, RestoreAction,
    RestoreIntent, RestoreMode, RestoreOutcome, RestorePlan,
};
pub use selection::{
    lifecycle_transitions, rebind_exact, rebind_symbol, BoundSelection, ByteRange,
    HistoricalPathContent, LifecycleEvent, LifecycleKind, LifecycleObservation, LifecycleState,
    ParsedSymbol, RebindOutcome, RustPrototypeParser, SearchBudget, SearchCursor, SearchStopReason,
    SearchUsage, SelectionCandidate, SelectionError, SelectionExtent, SelectionHandle,
    SemanticIdentity, SymbolParseError, SymbolParser, SELECTION_CONTEXT_BYTES,
    SELECTION_HANDLE_VERSION,
};
pub use smart::{
    draft_smart_message, draft_smart_subject, patch_digest, plan_smart, smart_attribution,
    SmartAttribution, SmartCandidate, SmartCondition, SmartConflict, SmartFilePlan, SmartKind,
    SmartPlan, SmartSelection, SmartSide,
};
pub use squash::{
    anchor_sha, collect_span, draft_message, draft_subject, frame_anchor, passthrough_has_message,
    span_stats, split_range, SpanStats,
};
pub use timeline::{
    decode_frontier, encode_frontier, BranchTip, Capture, CaptureInfo, CaptureOrigin, Checkpoint,
    OriginKind, ResolvedPoint, TimelineReader,
};

use crate::config::{self, sheaf_dir};
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind};

const FILES_MAP: &str = "files";
const BINARIES_MAP: &str = "binaries";
const MODES_MAP: &str = "modes";
/// The modes-map value marking an executable path; absence means plain.
const MODES_EXEC: &str = "exec";
const TREE_EVENTS_LIST: &str = "tree_events";
const META_MAP: &str = "_sheaf.meta";

/// Files above this size take the binary path on capture, even when their
/// bytes are valid UTF-8. Char-level splicing means materializing the file
/// twice in RAM per flush AND carrying its full bytes inside every journal
/// delta that first admits it; a content-addressed blob restores it
/// byte-exactly at flat memory cost. Real source files are KB-scale —
/// anything MiB-scale and text-shaped is logs, exports, or machine data,
/// where the fidelity ceiling is the whole file anyway, not the character.
/// An honest, bounded trade recorded here.
pub const TEXT_MAX_BYTES: u64 = 1024 * 1024;
/// Per-capture admission ceiling for NEW text containers. A single debounced
/// batch over an exploded tree (toolchains, caches, state directories) can
/// present thousands of valid-UTF-8 files at once; without a batch cap the
/// one delta exported for that capture inline-carries all of them, and one
/// such delta is enough to make every later store open replay it at a
/// ~100x RAM multiple. Files past the cap take the binary path this batch
/// and may be admitted as text by a later, smaller capture.
pub const TEXT_BATCH_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Boot/restore scans flush incrementally so path lists and genesis exports
/// never scale to the total number of files in an existing project.
pub const RECONCILE_BATCH_EVENTS: usize = 256;

#[derive(Debug, Clone, Serialize)]
pub struct StoreOutcome {
    pub seq: u64,
    pub events_applied: usize,
    pub text_ops_spliced: usize,
    pub text_created: usize,
    /// UTF-8 files routed to byte-exact blobs because the aggregate CRDT
    /// text budget was exhausted.
    pub text_budget_fallbacks: usize,
    pub binaries_stored: usize,
    pub blob_files_written: usize,
    pub tree_records: usize,
    pub update_bytes: usize,
    pub rotated: bool,
    pub snapshotted: bool,
    /// The capture this batch became; `None` only for an empty batch.
    pub capture: Option<Capture>,
}

fn store_dir(root: &Path) -> PathBuf {
    sheaf_dir(root).join("store")
}

fn state_dir(root: &Path) -> PathBuf {
    sheaf_dir(root).join("state")
}

// ------------------------------------------------------------------ limits

/// Store cadence knobs, configurable via the `[store]` section of
/// `config.toml` (`snapshot_edit_size`, `max_segment_bytes`); older files
/// keep these defaults. Values bind at store open.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreLimits {
    /// Rotate the active journal segment at this many bytes.
    #[serde(default = "default_max_segment_bytes")]
    pub max_segment_bytes: u64,
    /// Snapshot+prune whenever the TOTAL number of flushed edit batches
    /// reaches a multiple of this (`num_edits % snapshot_edit_size == 0`).
    /// Zero disables cadence snapshots entirely.
    #[serde(
        default = "default_snapshot_edit_size",
        alias = "snapshot_every_batches"
    )]
    pub snapshot_edit_size: u64,
}

fn default_max_segment_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_snapshot_edit_size() -> u64 {
    512
}

impl Default for StoreLimits {
    fn default() -> Self {
        StoreLimits {
            max_segment_bytes: default_max_segment_bytes(),
            snapshot_edit_size: default_snapshot_edit_size(),
        }
    }
}

// ------------------------------------------------------------------- store

#[derive(Serialize, Deserialize)]
struct Manifest {
    snapshot: String,
    covered_upto: u64,
    /// Total edit batches persisted as of this snapshot's coverage. The
    /// compaction cadence is a modulo of that total, so the count must
    /// survive compaction: a reopen reconstructs it as `total_edits` plus
    /// the update frames replayed beyond `covered_upto` — one delta per
    /// committed capture, so the sum is exact.
    #[serde(default)]
    total_edits: u64,
    /// Hex frontier of the shallow boundary when this snapshot trims
    /// pre-boundary history; `None` for full-history snapshots.
    #[serde(default)]
    shallow_since: Option<String>,
    /// Materialized [`LedgerState`] so pruning covered segments never
    /// loses tombstones/marks/checkpoints.
    #[serde(default)]
    ledger: Option<serde_json::Value>,
}

pub struct ProjectStore {
    root: PathBuf,
    sdir: PathBuf,
    doc: LoroDoc,
    last_vv: VersionVector,
    seq: u64,
    journal: journal::JournalWriter,
    limits: StoreLimits,
    /// Total edit batches ever persisted to this store, summed across
    /// every writer lifetime. Reconstructed at open (manifest baseline +
    /// replayed tail) and advanced once per committed capture, so the
    /// snapshot cadence `num_edits % snapshot_edit_size == 0` is a
    /// function of persisted state alone — never of how long any single
    /// daemon happened to stay alive.
    num_edits: u64,
    /// Journal payload bytes appended (or replayed at open) since the newest
    /// snapshot baseline. The edit-count cadence alone can lag arbitrarily
    /// far behind a single huge capture — one poisoned segment then replays
    /// at a large RAM multiple on every open until the next cadence multiple
    /// — so crossing `max_segment_bytes` also forces compaction. Anchored to
    /// persisted state at open exactly like `num_edits`.
    bytes_since_snapshot: u64,
    /// Folded timeline ledger: checkpoints, marks, tombstones,
    /// blob registry. Seeded from the manifest, extended by record frames.
    pub(crate) ledger: ledger::LedgerState,
    /// Digests named by the in-flight batch's binary tree events; drained
    /// into the Capture ledger record at commit.
    pending_blobs: Vec<String>,
    /// Aggregate source bytes represented as char-level Loro text. This is a
    /// hard admission bound; overflow takes the recoverable blob path.
    tracked_text_bytes: u64,
    max_tracked_bytes: u64,
    /// Non-UTF-8 paths already complained about, so a build tool that keeps
    /// such a file hot does not flood the log. Keys are lossy renderings —
    /// they are for de-duplication only, never for tracking.
    warned_keys: BTreeSet<String>,
    /// Bounded daemon-resident exact point reads shared across grep calls.
    grep_content_cache: RefCell<grep::GrepContentCache>,
}

impl ProjectStore {
    /// Open (or recover) the persistent store under `<root>/.sheaf/`.
    /// Caller is responsible for holding `.sheaf/lock` exclusively.
    pub fn open(root: &Path, limits: StoreLimits) -> Result<ProjectStore> {
        Self::open_with_text_budget(root, limits, config::DEFAULT_MAX_TRACKED_BYTES)
    }

    /// Open with an explicit aggregate char-level text budget. The daemon
    /// supplies `[watch].max_tracked_bytes`; direct callers receive the
    /// conservative default through [`ProjectStore::open`].
    pub fn open_with_text_budget(
        root: &Path,
        limits: StoreLimits,
        max_tracked_bytes: u64,
    ) -> Result<ProjectStore> {
        config::read_store_format(root)?;
        // Writer-owned capability bump: ledger frames need format
        // 2 so older builds fail closed instead of choking on record frames.
        if config::upgrade_store_format(root)? {
            tracing::info!(root = %root.display(), "store upgraded to format {}", config::STORE_FORMAT_VERSION);
        }
        let sdir = store_dir(root);
        std::fs::create_dir_all(journal::journal_dir(&sdir))?;
        std::fs::create_dir_all(sdir.join("snapshots"))?;
        std::fs::create_dir_all(blobs::blobs_dir(&sdir))?;
        let sd = state_dir(root);
        std::fs::create_dir_all(&sd)?;

        let doc = LoroDoc::new();
        // A persisted debounce batch is exactly one user-visible capture;
        // never let Loro coalesce adjacent explicit commits.
        doc.set_change_merge_interval(0);
        doc.set_detached_editing(true);

        // ---- persistent writer identity ---------------------------------
        // CRDT ops are authored per-peer; a reopened document MUST keep the
        // same peer id or replayed deltas from the old epoch resolve with
        // unresolved dependencies. The journal has exactly one writer
        // (the flock holder), so one identity file is one causal chain.
        // If identity is lost while history exists, we continue under a new
        // id — safe, just a fork point — never a corruption.
        let ident_path = sd.join("identity");
        let (peer, _fresh) = load_or_create_identity(&ident_path)?;
        doc.set_peer_id(peer).map_err(store_err)?;

        // Freshness gate: seed meta ONLY into an empty history; on any
        // existing baseline it is already present (same peer ⇒ true no-op).
        let had_history =
            !journal::list_segments(&sdir).is_empty() || newest_manifest(&sdir).is_some();

        // --- recovery baseline: newest valid manifest --------------------
        let mut covered_upto: Option<u64> = None;
        let mut manifest_total_edits = 0u64;
        let mut ledger = ledger::LedgerState::default();
        if let Some((manifest_path, manifest)) = newest_manifest(&sdir) {
            let snap_path = sdir.join("snapshots").join(&manifest.snapshot);
            match std::fs::read(&snap_path) {
                Ok(bytes) => {
                    let status = doc.import(&bytes).map_err(import_err)?;
                    log_pending(&status);
                    // Manifest carries the materialized ledger so covered
                    // segments' records survive their pruning.
                    match manifest.ledger.as_ref().map(ledger::LedgerState::from_json) {
                        Some(Ok(state)) => ledger = state,
                        Some(Err(e)) => tracing::warn!(
                            manifest = %manifest_path.display(),
                            error = %e,
                            "manifest ledger state unparseable; records since the last epoch refold, older tombstones/marks are lost"
                        ),
                        None => {}
                    }
                    tracing::info!(
                        manifest = %manifest_path.display(),
                        shallow_since = manifest.shallow_since.as_deref().unwrap_or(""),
                        "store baseline from snapshot"
                    );
                    covered_upto = Some(manifest.covered_upto);
                    manifest_total_edits = manifest.total_edits;
                }
                Err(e) => {
                    tracing::warn!(
                        snapshot = %snap_path.display(),
                        error = %e,
                        "manifest present but snapshot unreadable; full replay"
                    );
                }
            }
        }

        // --- replay uncovered segments -----------------------------------
        let replay_paths: Vec<_> = journal::list_segments(&sdir)
            .into_iter()
            .filter(|(idx, _)| covered_upto.is_none_or(|c| *idx > c))
            .collect();
        let mut replayed = 0usize;
        let mut replay_bytes = 0u64;
        let mut unknown_frames = 0usize;
        let mut replay_error: Option<SheafError> = None;
        {
            let mut buffer = ReplayBuffer::new(&doc, &mut ledger);
            journal::visit_records(&replay_paths, |item| {
                let record = match item {
                    Ok(record) => record,
                    Err((seg, msg)) => {
                        tracing::warn!(segment = seg, %msg, "segment skipped");
                        return false;
                    }
                };
                replay_bytes += record.payload.len() as u64;
                let at = FrameAt {
                    segment: record.segment,
                    ordinal: record.ordinal,
                };
                match ledger::classify_payload(&record.payload) {
                    Some(ledger::Frame::Update(delta)) => match buffer.push_update(delta, at) {
                        Ok(()) => {
                            replayed += 1;
                            true
                        }
                        Err(failure) => {
                            tracing::warn!(
                                segment = failure.at.segment,
                                ordinal = failure.at.ordinal,
                                error = %failure.error,
                                "delta import failed; stopping replay at this point"
                            );
                            replay_error = Some(failure.error);
                            false
                        }
                    },
                    Some(ledger::Frame::Record(rec)) => {
                        buffer.push_record(rec, at);
                        true
                    }
                    None => {
                        unknown_frames += 1;
                        tracing::warn!(
                            segment = record.segment,
                            ordinal = record.ordinal,
                            "unclassifiable journal frame skipped (future or torn frame)"
                        );
                        true
                    }
                }
            });
            if let Err(failure) = buffer.flush() {
                tracing::warn!(
                    segment = failure.at.segment,
                    ordinal = failure.at.ordinal,
                    error = %failure.error,
                    "delta import failed; stopping replay at this point"
                );
                replay_error = replay_error.or(Some(failure.error));
            }
        }
        if let Some(error) = replay_error {
            return Err(error);
        }
        if unknown_frames > 0 {
            tracing::warn!(
                count = unknown_frames,
                "store holds frames this build does not recognize"
            );
        }

        // Export-basis discipline (append-only): deltas must be
        // self-sufficient for a loader holding exactly `(snapshot?)+
        // segments` — nothing more.
        //  - virgin store ⇒ EMPTY basis so flush #1 spans full genesis
        //    (meta seeding included);
        //  - recovered history ⇒ post-replay frontier basis, matching what
        //    the persisted bytes deliver downstream.
        let last_vv = if had_history {
            doc.oplog_vv()
        } else {
            // Reserved document metadata; authored exactly once, and it
            // rides inside the genesis delta thanks to the empty basis.
            let meta = doc.get_map(META_MAP);
            meta.insert("format", "1".to_string()).map_err(store_err)?;
            VersionVector::default()
        };
        let journal = journal::JournalWriter::resume(&sdir, covered_upto, limits.max_segment_bytes)
            .map_err(|e| SheafError::StoreCorrupt(format!("journal resume: {e}")))?;

        // Lineage discipline. Importing every segment leaves
        // DocState at the MERGED frontier of all branches. After a restore
        // repositioned the worktree, that merge would silently resurrect the
        // abandoned future in the next capture's parent. The head file names
        // the lineage the worktree actually holds, so the writer edits there.
        if had_history {
            if let Some(head) = timeline::read_head_frontier(root)
                .and_then(|raw| timeline::decode_frontier(&raw).ok())
                .filter(|f| doc.frontiers_to_vv(f).is_some())
                .filter(|f| timeline::encode_frontier(f) != encode_frontier(&doc.state_frontiers()))
            {
                doc.checkout(&head).map_err(store_err)?;
                doc.set_detached_editing(true);
                // checkout to an old/concurrent version builds Loro's
                // history cache and diff calculator; the writer edits
                // forward from here and rebuilds them only if a restore
                // jumps again, so release both now. On a large store these
                // caches can retain tens of MiB for a jump that already
                // happened.
                doc.free_history_cache();
                doc.free_diff_calculator();
                tracing::info!(
                    root = %root.display(),
                    frontier = %timeline::encode_frontier(&head),
                    "worktree head is behind the oplog tip; editing continues on its lineage"
                );
            }
        }

        let mut tracked_text_bytes = 0u64;
        doc.get_map(FILES_MAP).for_each(|_, value| {
            if let loro::ValueOrContainer::Container(loro::Container::Text(text)) = value {
                tracked_text_bytes =
                    tracked_text_bytes.saturating_add(text.to_string().len() as u64);
            }
        });
        if tracked_text_bytes > max_tracked_bytes {
            tracing::warn!(
                root = %root.display(),
                tracked_text_bytes,
                max_tracked_bytes,
                "existing CRDT text exceeds the configured ingest budget; new text uses blobs until it falls below the bound"
            );
        }

        tracing::debug!(
            root = %root.display(),
            recovered_records = replayed,
            segments = journal.index,
            tracked_text_bytes,
            max_tracked_bytes,
            "project store open"
        );

        let mut store = ProjectStore {
            root: root.to_path_buf(),
            sdir,
            doc,
            last_vv,
            seq: 0,
            journal,
            limits,
            // The compaction cadence keys off the TOTAL edit count, not a
            // per-process accumulator: the manifest carries the total as
            // of its coverage, and the `replayed` update frames beyond it
            // are exactly the edits flushed since (one delta per capture).
            // A writer that reopens after every edit — the dev-loop
            // daemon — still crosses the next `snapshot_edit_size`
            // multiple on schedule. Manifests from before this field
            // existed default to 0 and simply re-anchor the phase;
            // snapshotting early or late by one cycle is always safe.
            num_edits: manifest_total_edits + replayed as u64,
            bytes_since_snapshot: replay_bytes,
            ledger,
            pending_blobs: Vec::new(),
            tracked_text_bytes,
            max_tracked_bytes,
            warned_keys: BTreeSet::new(),
            grep_content_cache: RefCell::new(grep::GrepContentCache::open(root, true)),
        };
        // Replay-burst repair: a capture large enough to push the tail past
        // one segment (then a crash before the edit-count cadence fired)
        // would otherwise be re-replayed at open forever. Compact once now
        // — the fresh baseline re-anchors every later open to it.
        if store.size_snapshot_due() {
            tracing::info!(
                root = %root.display(),
                bytes_since_snapshot = store.bytes_since_snapshot,
                "journal tail past the newest snapshot exceeds one segment; compacting on open"
            );
            store.compact()?;
        }
        Ok(store)
    }

    /// Current frontier-advisory sequence counter (flushes persisted).
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Total edit batches persisted across the store's whole history —
    /// the modulo base for the snapshot cadence.
    pub fn num_edits(&self) -> u64 {
        self.num_edits
    }

    /// Project root this store belongs to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Folded timeline ledger (checkpoints, marks, tombstones).
    pub fn ledger(&self) -> &ledger::LedgerState {
        &self.ledger
    }

    /// Read access to the live document (tests and diagnostics).
    pub fn doc_ref(&self) -> &LoroDoc {
        &self.doc
    }

    /// Fold one more ledger record into the in-memory state (writer paths
    /// call this after appending the frame).
    pub(crate) fn ledger_fold(&mut self, record: ledger::LedgerRecord) {
        self.ledger.fold(record);
    }

    /// Append one already-encoded ledger frame to the journal (fsync'd).
    pub(crate) fn append_ledger_frame(&mut self, payload: &[u8]) -> std::io::Result<()> {
        self.journal.append_batch_synced(&[payload])
    }

    /// Current aggregate source bytes held in char-level CRDT containers.
    pub fn tracked_text_bytes(&self) -> u64 {
        self.tracked_text_bytes
    }

    pub fn max_tracked_bytes(&self) -> u64 {
        self.max_tracked_bytes
    }

    /// Absolute paths this document currently tracks (text + binary maps).
    /// Boot-time reconciliation diffs these against disk so files created
    /// while the writer was DOWN enter history as ordinary Added batches.
    pub fn known_paths(&self) -> BTreeSet<PathBuf> {
        let mut out = BTreeSet::new();
        for mapname in [FILES_MAP, BINARIES_MAP] {
            self.doc.get_map(mapname).for_each(|k, _| {
                out.insert(self.root.join(k));
            });
        }
        out
    }

    /// True when the live disk bytes differ from what the document holds for
    /// a KNOWN path (text compared verbatim, binaries by digest — streamed,
    /// so a 4 GiB capture artifact costs pages, not gigabytes). Untracked
    /// paths report `None`.
    pub fn content_differs(&self, path: &Path) -> Option<bool> {
        let key = rel_key(&self.root, path).ok()?;
        if let Some(stored) = self.current_text(&key) {
            let bytes = std::fs::read(path).ok()?;
            let disk_text = std::str::from_utf8(&bytes).ok();
            return Some(match disk_text {
                Some(t) => stored.as_str() != t,
                None => true, // was text, now binary
            });
        }
        if let Some(meta) = self.binary_meta(&key) {
            let disk_hash = blobs::hash_file(path).ok()?;
            return Some(disk_hash != meta.hash);
        }
        None
    }

    /// Persist one debounced batch and fsync it. Content-bearing events are
    /// reconciled against the live worktree AT FLUSH TIME — the disk state is
    /// the final truth of whatever the burst meant.
    pub fn apply_batch(&mut self, batch: &Batch) -> Result<StoreOutcome> {
        self.apply_batch_tagged(batch, None)
    }

    /// Same, tagging the resulting capture with the writer-side reason it
    /// exists (restore provenance).
    pub(super) fn apply_batch_tagged(
        &mut self,
        batch: &Batch,
        origin: Option<timeline::CaptureOrigin>,
    ) -> Result<StoreOutcome> {
        if batch.events.is_empty() {
            return Ok(StoreOutcome {
                seq: self.seq,
                events_applied: 0,
                ..zero_outcome(self.seq)
            });
        }

        // ---- pass 1: classify into deterministic buckets ---------------
        // BTree* keeps processing order stable regardless of arrival order.
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();
        let mut removals: BTreeSet<PathBuf> = BTreeSet::new();
        // path -> any 'touched' seen (vs pure added)
        let mut upserts: BTreeMap<PathBuf, bool> = BTreeMap::new();

        for ev in &batch.events {
            match &ev.kind {
                EventKind::Renamed { from, to } => {
                    renames.push((from.clone(), to.clone()));
                    removals.remove(to);
                    upserts.remove(to);
                }
                EventKind::Removed { path } => {
                    removals.insert(path.clone());
                    upserts.remove(path);
                }
                EventKind::Added { path } => {
                    if !removals.contains(path) {
                        upserts.entry(path.clone()).or_insert(false);
                    }
                }
                EventKind::Touched { path } => {
                    if !removals.contains(&path.0) {
                        upserts.insert(path.0.clone(), true);
                    }
                }
            }
        }

        let mut outcome = zero_outcome(self.seq);

        // ---- pass 2: removals first -------------------------------------
        for p in &removals {
            // A path that cannot be keyed was never tracked (keys are UTF-8
            // strings), so its removal models nothing.
            if rel_key(&self.root, p).is_err() {
                self.warn_untrackable(p);
                continue;
            }
            // A removal the document never knew about is not history. This is
            // what keeps the watcher's echo of a restore pruning an emptied
            // directory from materializing as a phantom capture.
            if !self.delete_entry(p) {
                continue;
            }
            self.push_tree_event(serde_json::json!({
                "kind": "removed", "path": rel_str(&self.root, p),
            }))?;
            outcome.tree_records += 1;
            outcome.events_applied += 1;
        }

        // ---- pass 3: renames (moves) ------------------------------------
        for (from, to) in &renames {
            if from == to {
                continue; // defensive: a no-op rename models no history
            }
            upserts.remove(to); // move covers it
            let moved_content = self.move_entry(from, to)?;
            self.push_tree_event(serde_json::json!({
                "kind": "renamed",
                "from": rel_str(&self.root, from),
                "to": rel_str(&self.root, to),
            }))?;
            outcome.events_applied += 1;
            outcome.tree_records += 1;
            if moved_content {
                outcome.text_ops_spliced += 0; // moves copy wholesale
                outcome.text_created += 1;
            }
            // If source was unknown but destination exists on disk, treat as
            // unpaired add (watch started mid-move); falls through below.
            if !moved_content && to.is_file() {
                upserts.entry(to.clone()).or_insert(true);
            }
        }

        // ---- pass 4: content reconciliation against disk ----------------
        // New-text admission is bounded per capture (see TEXT_BATCH_MAX_BYTES)
        // so one delta never inline-carries a whole exploded tree.
        let mut batch_text_admitted: u64 = 0;
        for p in upserts.keys() {
            // Directories are structure, not content: the document keys files
            // and their paths carry the hierarchy. Reading one yields EISDIR,
            // which used to abort the whole batch and lose the window.
            if p.is_dir() {
                continue;
            }
            let Some(key) = self.key_for(p) else {
                continue;
            };
            // Memory bound: oversized files skip the slurp and take
            // the binary path at flat memory cost, even when their
            // bytes are valid UTF-8 (see TEXT_MAX_BYTES).
            let oversized = std::fs::symlink_metadata(p)
                .map(|m| m.len() > TEXT_MAX_BYTES)
                .unwrap_or(false);
            if oversized {
                self.upsert_binary_streaming(p, &key, &mut outcome)?;
                continue;
            }
            let bytes = match std::fs::read(p) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Appeared then vanished within this window: record truth.
                    self.push_tree_event(serde_json::json!({
                        "kind": "touched", "path": key, "gone": true,
                    }))?;
                    outcome.tree_records += 1;
                    continue;
                }
                Err(e) => return Err(SheafError::Io(e)),
            };
            outcome.events_applied += 1;
            match std::str::from_utf8(&bytes) {
                Ok(text_new) => {
                    let current = self.current_text(&key);
                    let old_len = current.as_ref().map_or(0, |text| text.len() as u64);
                    let projected = self
                        .tracked_text_bytes
                        .saturating_sub(old_len)
                        .saturating_add(bytes.len() as u64);
                    // A lowered limit does not demote unchanged historical
                    // text on a pure mtime/mode echo. Only new bytes need an
                    // admission decision.
                    let batch_room = batch_text_admitted.saturating_add(bytes.len() as u64)
                        <= TEXT_BATCH_MAX_BYTES;
                    if current.as_deref() == Some(text_new)
                        || (projected <= self.max_tracked_bytes && batch_room)
                    {
                        batch_text_admitted = batch_text_admitted
                            .saturating_add(bytes.len().saturating_sub(old_len as usize) as u64);
                        self.upsert_text(&key, text_new, &mut outcome)?;
                    } else {
                        // Exact bytes remain recoverable; only char-level ops
                        // yield when admitting this file would exceed the
                        // project's aggregate in-memory text bound or this
                        // capture's per-batch delta ceiling.
                        if self.upsert_binary(&key, &bytes, &mut outcome)? {
                            outcome.text_budget_fallbacks += 1;
                        }
                    }
                }
                Err(_) => {
                    // Non-UTF8 ⇒ binary path.
                    self.upsert_binary(&key, &bytes, &mut outcome)?;
                }
            }
        }
        if outcome.text_budget_fallbacks > 0 {
            tracing::warn!(
                root = %self.root.display(),
                files = outcome.text_budget_fallbacks,
                tracked_text_bytes = self.tracked_text_bytes,
                max_tracked_bytes = self.max_tracked_bytes,
                "text ingest budget reached; storing additional UTF-8 files as byte-exact blobs"
            );
        }

        // ---- pass 5: commit one capture, export, fsync, advance ----------
        // A batch that touched nothing the document models — a bare directory
        // create, an echo of an already-applied removal — is not a capture.
        // Committing it anyway burned a journal record and, worse, showed up
        // in the timeline as a user-visible event that never happened.
        if outcome.tree_records == 0 {
            return Ok(zero_outcome(self.seq));
        }
        let capture = timeline::commit_capture(&self.doc, batch, origin)?;
        let delta = self
            .doc
            .export(ExportMode::updates(&self.last_vv))
            .map_err(encode_err)?;
        // The capture's update delta and its ledger record land as one
        // fsync'd unit: a torn tail drops whole frames, so the
        // pair never half-materializes. The record's blobs registry lists
        // every digest this batch's binary tree events named.
        let blobs = std::mem::take(&mut self.pending_blobs);
        let record = ledger::LedgerRecord::Capture {
            id: capture.id.clone(),
            frontier: capture.frontier.clone(),
            at_ms: capture.timestamp_ms,
            paths: capture.paths.clone(),
            events: capture.events,
            blobs,
        };
        let encoded = record.encode();
        self.journal
            .append_batch_synced(&[&delta, &encoded])
            .map_err(io_err)?;
        self.ledger.fold(record);
        // Derived grep rows publish only after the authoritative update and
        // capture record are durable. Cache failure never rolls back history;
        // a later query falls back to exact point materialization and repairs
        // the missing row.
        self.grep_content_cache
            .borrow_mut()
            .index_capture(&self.doc, &capture);
        self.last_vv = self.doc.oplog_vv();

        self.seq += 1;
        outcome.seq = self.seq;
        outcome.update_bytes = delta.len();
        outcome.rotated = self.journal.written_in_segment == 0; // just rotated
        outcome.capture = Some(capture.clone());

        self.write_head(batch, &capture)?;

        // Cadence: every `snapshot_edit_size` edits of the running TOTAL,
        // not every N edits observed by this process. Compaction never
        // resets the count, so the phase is anchored to persisted history
        // and out-of-band compactions (gc trims, manual `compact`) cannot
        // desynchronize it. Zero disables cadence snapshots (a `% 0` would
        // panic; nobody who disables the cadence wants one per edit).
        self.num_edits += 1;
        self.bytes_since_snapshot += (delta.len() + encoded.len()) as u64;
        outcome.snapshotted = false;
        if (self.limits.snapshot_edit_size > 0
            && self.num_edits % self.limits.snapshot_edit_size == 0)
            || self.size_snapshot_due()
        {
            self.compact()?;
            outcome.snapshotted = true;
        }
        Ok(outcome)
    }

    /// True when journal payload accumulated since the newest snapshot has
    /// reached the segment size limit. Replay after a crash then re-reads at
    /// most about one segment's worth of deltas before a fresh baseline
    /// exists, bounding open-time RAM to the segment, not to history since
    /// the last cadence multiple.
    fn size_snapshot_due(&self) -> bool {
        self.limits.max_segment_bytes > 0
            && self.bytes_since_snapshot >= self.limits.max_segment_bytes
    }

    // ------------------------------------------------------------ internals

    /// Warn once per project run about a path that cannot be document-keyed.
    fn warn_untrackable(&mut self, p: &Path) {
        let lossy = p.to_string_lossy().into_owned();
        if self.warned_keys.insert(lossy) {
            tracing::warn!(
                root = %self.root.display(),
                path = %p.display(),
                "path is not valid UTF-8; it stays untracked (restore would otherwise \
                 materialize a lossy duplicate beside the real file)"
            );
        }
    }

    /// Key for a live path, or `None` (after warning) when unkeyable.
    fn key_for(&mut self, p: &Path) -> Option<String> {
        match rel_key(&self.root, p) {
            Ok(key) => Some(key),
            Err(_) => {
                self.warn_untrackable(p);
                None
            }
        }
    }

    #[allow(deprecated)] // TODO(sync-era): migrate to ensure_mergeable_* lazy children
    fn files_map_text(&self, key: &str) -> LoroResult<LoroText> {
        self.doc
            .get_map(FILES_MAP)
            .get_or_create_container(key, LoroText::new())
    }

    fn current_text(&self, key: &str) -> Option<String> {
        match self.doc.get_map(FILES_MAP).get(key)? {
            loro::ValueOrContainer::Container(loro::Container::Text(t)) => Some(t.to_string()),
            _ => None,
        }
    }

    fn current_text_len(&self, key: &str) -> u64 {
        self.current_text(key).map_or(0, |text| text.len() as u64)
    }

    /// Remove one text representation and keep aggregate accounting exact.
    fn delete_text_entry(&mut self, key: &str) -> u64 {
        let removed = self.current_text_len(key);
        let _ = self.doc.get_map(FILES_MAP).delete(key);
        self.tracked_text_bytes = self.tracked_text_bytes.saturating_sub(removed);
        removed
    }

    /// Recorded executable flag for a key. Absent means plain (git-style:
    /// only the exec bit is worth history; 0644-vs-0664 noise is not).
    fn tracked_exec(&self, key: &str) -> Option<bool> {
        let v = self.doc.get_map(MODES_MAP).get(key)?;
        Some(
            v.get_deep_value()
                .into_string()
                .is_ok_and(|s| s.as_str() == MODES_EXEC),
        )
    }

    /// Align the recorded exec flag with the live file. Returns whether the
    /// user-observable mode changed — a chmod IS history (file-mode
    /// modelling), but a plain file that was never recorded is not changing
    /// just because it is being captured for the first time. Only `exec`
    /// entries exist in the map; absence means plain.
    fn set_mode_key(&mut self, key: &str, exec: bool) -> Result<bool> {
        let changed = match self.tracked_exec(key) {
            Some(prev) => prev != exec,
            // Unrecorded ⇒ plain is the steady state, not a change.
            None => exec,
        };
        if !changed {
            return Ok(false);
        }
        let map = self.doc.get_map(MODES_MAP);
        if exec {
            map.insert(key, MODES_EXEC.to_string()).map_err(store_err)?;
        } else {
            let _ = map.delete(key);
        }
        Ok(true)
    }

    /// Text capture for one key. Returns `false` when the file already holds
    /// exactly this content and its recorded mode already matches — a pure
    /// mtime echo models nothing and must not become a capture.
    fn upsert_text(
        &mut self,
        key: &str,
        new_content: &str,
        outcome: &mut StoreOutcome,
    ) -> Result<bool> {
        let prev = self.current_text(key);
        let exec = file_exec(&self.root.join(key));
        // Mirror of the binary branch: one path, one representation.
        let _ = self.doc.get_map(BINARIES_MAP).delete(key);
        if prev.as_deref() == Some(new_content) {
            let mode_changed = self.set_mode_key(key, exec)?;
            if !mode_changed {
                return Ok(false);
            }
            // A chmod on unchanged bytes is a real user action.
            self.push_tree_event(serde_json::json!({
                "kind": "touched", "path": key,
                "mode": if exec { "exec" } else { "plain" },
            }))?;
            outcome.tree_records += 1;
            return Ok(true);
        }
        let tree_kind = if prev.is_some() { "touched" } else { "added" };
        let text = self.files_map_text(key).map_err(store_err)?;
        match prev.as_deref().map(|old| splice_ops(old, new_content)) {
            None => {
                // brand-new entry
                if !new_content.is_empty() {
                    text.insert(0, new_content).map_err(store_err)?;
                }
                outcome.text_created += 1;
            }
            Some(None) => {} // identical content — no ops
            Some(Some((pos, del_len, ins))) => {
                if del_len > 0 {
                    text.delete(pos, del_len).map_err(store_err)?;
                }
                if !ins.is_empty() {
                    text.insert(pos, &ins).map_err(store_err)?;
                }
                outcome.text_ops_spliced += 1;
            }
        }
        self.set_mode_key(key, exec)?;
        self.tracked_text_bytes = self
            .tracked_text_bytes
            .saturating_sub(prev.as_ref().map_or(0, |old| old.len() as u64))
            .saturating_add(new_content.len() as u64);
        self.push_tree_event(serde_json::json!({
            "kind": tree_kind, "path": key,
        }))?;
        outcome.tree_records += 1;
        Ok(true)
    }

    /// Binary capture for one key from in-memory bytes. Returns `false` when
    /// content and mode are already recorded (echo), like `upsert_text`.
    fn upsert_binary(
        &mut self,
        key: &str,
        bytes: &[u8],
        outcome: &mut StoreOutcome,
    ) -> Result<bool> {
        let digest = blobs::hash_of(bytes);
        let exec = file_exec(&self.root.join(key));
        let mode_changed = self.set_mode_key(key, exec)?;
        if let Some(meta) = self.binary_meta(key) {
            if meta.hash == digest {
                // One path, one representation: drop any stale text
                // container even on the echo path.
                self.delete_text_entry(key);
                if !mode_changed {
                    return Ok(false);
                }
                self.push_tree_event(serde_json::json!({
                    "kind": "touched", "path": key,
                    "mode": if exec { "exec" } else { "plain" },
                }))?;
                outcome.tree_records += 1;
                return Ok(true);
            }
        }
        let (stored, wrote_new) = blobs::store_blob(&self.sdir, bytes).map_err(io_err)?;
        debug_assert_eq!(stored, digest);
        outcome.binaries_stored += usize::from(wrote_new);
        outcome.blob_files_written += usize::from(wrote_new);
        let meta = serde_json::json!({"hash": digest, "size": bytes.len()});
        // A path holds exactly one representation. Leaving the stale text
        // container behind makes the two maps disagree about what the path
        // IS, and any state reader then has to guess (restore materialization
        // notably cannot).
        let known = self.knows(key);
        self.delete_text_entry(key);
        self.doc
            .get_map(BINARIES_MAP)
            .insert(key, meta.to_string())
            .map_err(store_err)?;
        self.push_tree_event(serde_json::json!({
            "kind": if known { "touched" } else { "added" },
            "path": key, "binary": digest,
        }))?;
        self.pending_blobs.push(digest);
        outcome.tree_records += 1;
        Ok(true)
    }

    /// Same as `upsert_binary` for oversized files: streams the payload into
    /// content-addressed storage without ever holding the whole file in RAM.
    fn upsert_binary_streaming(
        &mut self,
        p: &Path,
        key: &str,
        outcome: &mut StoreOutcome,
    ) -> Result<bool> {
        let exec = file_exec(p);
        let mode_changed = self.set_mode_key(key, exec)?;
        if let Some(meta) = self.binary_meta(key) {
            if let Ok(disk_hash) = blobs::hash_file(p) {
                if meta.hash == disk_hash {
                    self.delete_text_entry(key);
                    if !mode_changed {
                        return Ok(false);
                    }
                    self.push_tree_event(serde_json::json!({
                        "kind": "touched", "path": key,
                        "mode": if exec { "exec" } else { "plain" },
                    }))?;
                    outcome.tree_records += 1;
                    return Ok(true);
                }
            }
        }
        let (digest, wrote_new) = blobs::store_blob_from_path(&self.sdir, p).map_err(io_err)?;
        outcome.binaries_stored += usize::from(wrote_new);
        outcome.blob_files_written += usize::from(wrote_new);
        let size = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        let meta = serde_json::json!({"hash": digest, "size": size});
        let known = self.knows(key);
        self.delete_text_entry(key);
        self.doc
            .get_map(BINARIES_MAP)
            .insert(key, meta.to_string())
            .map_err(store_err)?;
        self.push_tree_event(serde_json::json!({
            "kind": if known { "touched" } else { "added" },
            "path": key, "binary": digest,
        }))?;
        self.pending_blobs.push(digest);
        outcome.tree_records += 1;
        Ok(true)
    }

    /// Parsed `{hash, size}` record a key currently holds in the binaries
    /// map, if any.
    fn binary_meta(&self, key: &str) -> Option<BinaryMeta> {
        let v = self.doc.get_map(BINARIES_MAP).get(key)?;
        let jsonstr = v.get_deep_value().into_string().ok()?.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&jsonstr).ok()?;
        Some(BinaryMeta {
            hash: parsed.get("hash")?.as_str()?.to_owned(),
            size: parsed.get("size").and_then(|s| s.as_u64()).unwrap_or(0),
        })
    }

    /// Move a map entry by wholesale copy; returns false when nothing lived
    /// at `from`. History stays intact either way (append-only).
    ///
    /// A directory move arrives as ONE structural event, so the subtree is
    /// relocated by key prefix. Without this, renaming a directory stranded
    /// every file inside it under its old path forever.
    fn move_entry(&mut self, from: &Path, to: &Path) -> Result<bool> {
        // A rename with a non-UTF-8 side cannot name any tracked key; it is
        // untracked truth, warned about, never modeled.
        if rel_key(&self.root, from).is_err() || rel_key(&self.root, to).is_err() {
            self.warn_untrackable(from);
            return Ok(false);
        }
        let fkey = rel_key(&self.root, from)?;
        let tkey = rel_key(&self.root, to)?;
        if self.move_one(&fkey, &tkey)? {
            return Ok(true);
        }
        let prefix = format!("{fkey}/");
        let mut moved = false;
        for key in self.keys_under(&prefix) {
            let suffix = &key[prefix.len()..];
            moved |= self.move_one(&key, &format!("{tkey}/{suffix}"))?;
        }
        Ok(moved)
    }

    /// Root-relative keys beneath a directory prefix, across both maps.
    fn keys_under(&self, prefix: &str) -> Vec<String> {
        let mut keys = BTreeSet::new();
        for mapname in [FILES_MAP, BINARIES_MAP] {
            self.doc.get_map(mapname).for_each(|key, _| {
                if key.starts_with(prefix) {
                    keys.insert(key.to_string());
                }
            });
        }
        keys.into_iter().collect()
    }

    fn move_one(&mut self, fkey: &str, tkey: &str) -> Result<bool> {
        let (fkey, tkey) = (fkey.to_owned(), tkey.to_owned());
        if let Some(content) = self.current_text(&fkey) {
            self.delete_text_entry(&tkey);
            let _ = self.doc.get_map(BINARIES_MAP).delete(&tkey);
            let t = self.files_map_text(&tkey).map_err(store_err)?;
            if !content.is_empty() {
                t.insert(0, &content).map_err(store_err)?;
            }
            self.move_mode_record(&fkey, &tkey)?;
            self.delete_text_entry(&fkey);
            self.tracked_text_bytes = self.tracked_text_bytes.saturating_add(content.len() as u64);
            return Ok(true);
        }
        if let Some(meta) = self.binary_meta(&fkey) {
            let payload_path = blobs::blob_path(&self.sdir, &meta.hash);
            if let Ok(bytes) = std::fs::read(payload_path) {
                let (digest, _wrote) = blobs::store_blob(&self.sdir, &bytes).map_err(io_err)?; // dedup hit expected
                let new_meta = serde_json::json!({"hash": digest, "size": bytes.len()}).to_string();
                self.move_mode_record(&fkey, &tkey)?;
                self.delete_text_entry(&tkey);
                let bmap = self.doc.get_map(BINARIES_MAP);
                bmap.insert(&tkey, new_meta).map_err(store_err)?;
                bmap.delete(&fkey).map_err(store_err)?;
                return Ok(true);
            }
            // Unrecoverable payload: drop the stale record rather than let a
            // phantom entry masquerade as content.
            self.doc
                .get_map(BINARIES_MAP)
                .delete(&fkey)
                .map_err(store_err)?;
        }
        Ok(false)
    }

    /// Carry the recorded exec flag from one key to another.
    fn move_mode_record(&mut self, fkey: &str, tkey: &str) -> Result<()> {
        if let Some(flag) = self.tracked_exec(fkey) {
            self.set_mode_key(tkey, flag)?;
            let _ = self.doc.get_map(MODES_MAP).delete(fkey);
        }
        Ok(())
    }

    /// Returns whether anything was actually tracked at (or beneath) `p`.
    fn delete_entry(&mut self, p: &Path) -> bool {
        let Ok(key) = rel_key(&self.root, p) else {
            return false;
        };
        // A removed directory takes its subtree with it. A file key can never
        // be a prefix ending in `/`, so this cannot over-delete.
        let mut victims = self.keys_under(&format!("{key}/"));
        if self.knows(&key) {
            victims.push(key.clone());
        }
        for victim in &victims {
            self.delete_text_entry(victim);
            let _ = self.doc.get_map(BINARIES_MAP).delete(victim);
            let _ = self.doc.get_map(MODES_MAP).delete(victim);
        }
        !victims.is_empty()
    }

    /// Is this root-relative key tracked in either content map?
    fn knows(&self, key: &str) -> bool {
        self.doc.get_map(FILES_MAP).get(key).is_some()
            || self.doc.get_map(BINARIES_MAP).get(key).is_some()
    }

    fn push_tree_event(&self, value: serde_json::Value) -> Result<()> {
        let list = self.doc.get_list(TREE_EVENTS_LIST);
        let stamped = serde_json::json!({ "ts": Utc::now().timestamp_millis(), "event": value });
        list.insert(list.len(), stamped.to_string())
            .map_err(store_err)?;
        Ok(())
    }

    /// Advisory pointer for degraded readers (the CLI's offline fallback).
    fn write_head(&self, batch: &Batch, capture: &Capture) -> Result<()> {
        self.write_head_point(Some(&capture.id), &capture.frontier, batch.events.len())
    }

    /// Same file, written for a point that no batch produced — a restore
    /// repositioning the worktree onto an earlier frontier.
    pub(super) fn write_head_point(
        &self,
        capture_id: Option<&str>,
        frontier: &str,
        events_flushed: usize,
    ) -> Result<()> {
        // Captures can insert before a cached anchor through the API, and a
        // restore reattributes captures between `current` and branch lineages.
        // Either changes the replay prefix even when the query fingerprint and
        // anchor ID are unchanged, so cursor reductions must replay. Content-
        // hash scan outcomes and the trigram index are unaffected.
        self.grep_content_cache
            .borrow_mut()
            .invalidate_cursor_states();
        let head = serde_json::json!({
            "seq": self.seq,
            "capture_id": capture_id,
            "frontier": frontier,
            "events_flushed": events_flushed,
            "journal_index": self.journal.index,
            "records_appended": self.journal.records_appended,
            "flushed_at": Utc::now().to_rfc3339(),
            "root": self.root.display().to_string(),
        });
        let dir = state_dir(&self.root);
        let tmp = dir.join(".worktree.head.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(head.to_string_pretty_or_compact().as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(tmp, dir.join("worktree.head")).map_err(io_err)?;
        // The rename is the publish point; its directory entry must be on
        // stable storage too or a power cut reverts the head while committed
        // journal frames survive (parent-dir fsync).
        fsutil::sync_parent_dir(&dir.join("worktree.head")).map_err(io_err)
    }

    /// Structural records as currently materialized (`tree_events`).
    /// Renames/moves/deletes are first-class here, not inferred.
    pub fn tree_events(&self) -> Vec<serde_json::Value> {
        let list = self.doc.get_list(TREE_EVENTS_LIST);
        let mut out = Vec::new();
        list.for_each(|value| {
            if let Ok(raw) = value.get_deep_value().into_string() {
                if let Ok(parsed) = serde_json::from_str(&raw) {
                    out.push(parsed);
                }
            }
        });
        out
    }

    /// Snapshot full history, commit manifest LAST (rename-order atomicity),
    /// then prune covered segments. Orphan snapshots without manifests are
    /// ignored by the loader, so every crash interleaving here is safe.
    pub fn compact(&mut self) -> Result<()> {
        self.compact_inner(None)
    }

    /// Retention compaction: the new baseline is a Loro shallow
    /// snapshot at the trim boundary — complete state, history only since
    /// the boundary — and the ledger grows tombstones for every pruned
    /// capture plus the epoch record. Tombstones land in the post-rotation
    /// segment BEFORE the manifest commits, so the pruned captures' ghosts
    /// survive the very segment pruning this compaction performs.
    pub fn compact_with_trim(
        &mut self,
        boundary: &loro::Frontiers,
        tombstones: Vec<ledger::LedgerRecord>,
    ) -> Result<()> {
        self.compact_inner(Some((boundary, tombstones)))?;
        // The trim removed captures the shallow boundary predates, so some
        // point rows now name frontiers that no longer exist. Rather than
        // discard the whole disposable cache, sweep exactly the collected
        // rows: enumerate every capture still reachable in the trimmed DAG
        // (current lineage plus divergent branch tips — both stay searchable),
        // drop mappings for any other frontier, mark-sweep content blobs no
        // surviving mapping references, and rebuild the trigram index over
        // what remains. A failure to enumerate falls back to the wholesale
        // wipe, which is always safe because the cache is derived.
        match self.retained_frontiers_after_trim() {
            Ok(retained) => self
                .grep_content_cache
                .borrow_mut()
                .sweep_to_retained(&retained),
            Err(error) => {
                tracing::warn!(%error, "grep cache retention sweep fell back to full wipe");
                self.grep_content_cache
                    .borrow_mut()
                    .invalidate_after_retention();
            }
        }
        Ok(())
    }

    /// Every capture frontier still reachable after a retention trim: the
    /// current lineage and every divergent branch. Both must remain
    /// searchable, so a mapping keyed by any of these frontiers is retained
    /// and everything else is a collected row the sweep removes.
    fn retained_frontiers_after_trim(&self) -> Result<std::collections::BTreeSet<String>> {
        let union = timeline::captures_from(
            &self.doc,
            &self.ledger,
            &self.doc.oplog_frontiers(),
            None,
            None,
            usize::MAX,
        )?;
        Ok(union.into_iter().map(|c| c.frontier).collect())
    }

    fn compact_inner(
        &mut self,
        trim: Option<(&loro::Frontiers, Vec<ledger::LedgerRecord>)>,
    ) -> Result<()> {
        let closed = self.journal.rotate().map_err(io_err)?;
        let trimmed = trim.is_some();
        let bytes = match &trim {
            Some((boundary, _)) => self
                .doc
                .export(ExportMode::shallow_snapshot(boundary))
                .map_err(encode_err)?,
            None => self.doc.export(ExportMode::Snapshot).map_err(encode_err)?,
        };

        let existing: Vec<(u64, PathBuf)> = std::fs::read_dir(self.sdir.join("snapshots"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter_map(|p| {
                        let name = p.file_name()?.to_string_lossy().into_owned();
                        let idx = name
                            .strip_prefix("snap-")?
                            .strip_suffix(".snapshot")?
                            .parse()
                            .ok()?;
                        Some((idx, p))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let next_idx = existing.iter().map(|(i, _)| *i + 1).max().unwrap_or(1);

        let snaps_dir = self.sdir.join("snapshots");
        let snap_name = format!("snap-{next_idx:06}.snapshot");
        {
            let tmp = snaps_dir.join(format!(".{snap_name}.tmp"));
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            std::fs::rename(&tmp, snaps_dir.join(&snap_name)).map_err(io_err)?;
            fsutil::sync_parent_dir(&snaps_dir.join(&snap_name)).map_err(io_err)?;
        }

        // Ledger records for this epoch ride the fresh segment (index >
        // covered_upto) so segment pruning keeps them; the manifest embeds
        // the folded state for fast open. Fold is idempotent, so replaying
        // the frames over the manifest state is harmless.
        //
        // Plain compaction of an already-shallow store propagates its
        // boundary marker: a full-history Snapshot of a shallow doc never
        // regrows the pruned prefix, so the marker stays true.
        let shallow_since = match trim.as_ref() {
            Some((boundary, _)) => Some(timeline::encode_frontier(boundary)),
            None => self
                .doc
                .is_shallow()
                .then(|| newest_manifest(&self.sdir).and_then(|(_, m)| m.shallow_since.clone()))
                .flatten(),
        };
        if let Some((_, mut tombstones)) = trim {
            tombstones.push(ledger::LedgerRecord::Epoch {
                boundary: shallow_since.clone().unwrap_or_default(),
                covered_upto: closed,
            });
            let frames: Vec<Vec<u8>> = tombstones.iter().map(|r| r.encode()).collect();
            let refs: Vec<&[u8]> = frames.iter().map(|f| f.as_slice()).collect();
            self.journal.append_batch_synced(&refs).map_err(io_err)?;
            for record in tombstones {
                self.ledger.fold(record);
            }
        }

        let manifest = Manifest {
            snapshot: snap_name.clone(),
            covered_upto: closed,
            // Anchor the cadence total at this compaction point: update
            // frames beyond `closed` are the edits replayed on reopen.
            total_edits: self.num_edits,
            shallow_since: shallow_since.clone(),
            ledger: Some(self.ledger.to_json()),
        };
        {
            let mname = format!("snap-{next_idx:06}.manifest.json");
            let tmp = snaps_dir.join(format!(".{mname}.tmp"));
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(
                serde_json::to_vec(&manifest)
                    .map_err(|e| SheafError::StoreCorrupt(e.to_string()))?
                    .as_slice(),
            )?;
            f.sync_all()?;
            std::fs::rename(&tmp, snaps_dir.join(mname)).map_err(io_err)?;
            fsutil::sync_parent_dir(&snaps_dir.join(&snap_name)).map_err(io_err)?;
        }

        // Retention trims swap the writer onto the shallow baseline: keeping
        // the full-history doc would make the next flush export deltas that
        // reference pre-boundary changes no future loader can anchor. Same
        // peer id, same lineage discipline as `open` — the worktree's head
        // file, not the merged tip, names where editing continues.
        if trimmed {
            let fresh = loro::LoroDoc::new();
            fresh.set_change_merge_interval(0);
            fresh.import(&bytes).map_err(import_err)?;
            fresh.set_peer_id(self.doc.peer_id()).map_err(store_err)?;
            fresh.set_detached_editing(true);
            if let Some(head) = timeline::read_head_frontier(&self.root)
                .and_then(|raw| timeline::decode_frontier(&raw).ok())
                .filter(|f| fresh.frontiers_to_vv(f).is_some())
                .filter(|f| {
                    timeline::encode_frontier(f) != encode_frontier(&fresh.state_frontiers())
                })
            {
                fresh.checkout(&head).map_err(store_err)?;
                fresh.set_detached_editing(true);
            }
            self.doc = fresh;
            // The shallow doc reports its full counter range, so the export
            // basis stays consistent; new deltas only cover new ops.
            self.last_vv = self.doc.oplog_vv();
            // Pruned captures' blob digests live in their tombstone records.
            self.pending_blobs.clear();
            self.tracked_text_bytes = 0;
            self.doc.get_map(FILES_MAP).for_each(|_, value| {
                if let loro::ValueOrContainer::Container(loro::Container::Text(text)) = value {
                    self.tracked_text_bytes = self
                        .tracked_text_bytes
                        .saturating_add(text.to_string().len() as u64);
                }
            });
        }

        // Prune: everything <= covered is inside the snapshot's history now.
        let mut pruned = false;
        for (idx, path) in journal::list_segments(&self.sdir) {
            if idx <= closed {
                let _ = std::fs::remove_file(path);
                pruned = true;
            }
        }
        if pruned {
            fsutil::sync_dir(&journal::journal_dir(&self.sdir)).map_err(io_err)?;
        }
        // The fresh baseline absorbs every frame this epoch appended or
        // replayed; the size cadence restarts from zero either way.
        self.bytes_since_snapshot = 0;
        tracing::info!(root = %self.root.display(), covered_upto = closed, snapshot_bytes = bytes.len(), "compacted");
        Ok(())
    }

    /// Set the executable bit on `dst` when the document records one for
    /// `key` (file-mode modelling). Best-effort: writers report
    /// failures through their own channels; a lost chmod is recoverable by
    /// re-restoring, and silence here never corrupts content.
    pub(super) fn apply_mode_for(&self, dst: &Path, key: &str) {
        use std::os::unix::fs::PermissionsExt;
        if self.tracked_exec(key) == Some(true) {
            let _ = std::fs::set_permissions(dst, std::fs::Permissions::from_mode(0o755));
        }
    }

    /// Debug/inspection helper: materialize current document state onto a
    /// directory. Also the future checkout primitive in embryo.
    pub fn materialize(&self, target_root: &Path) -> Result<usize> {
        let mut written = 0usize;
        let files = self.doc.get_map(FILES_MAP);
        files.for_each(|k, v| {
            if let loro::ValueOrContainer::Container(loro::Container::Text(t)) = v {
                let dst = target_root.join(k);
                if let Some(parent) = dst.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let out = t.to_string();
                if !out.is_empty() {
                    let _ = std::fs::write(&dst, out);
                    self.apply_mode_for(&dst, k);
                    written += 1;
                }
            }
        });
        let bins = self.doc.get_map(BINARIES_MAP);
        bins.for_each(|k, v| {
            let raw = v.get_deep_value();
            let jsonstr = match raw.into_string() {
                Ok(s) => s.to_string(),
                Err(_) => return,
            };
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&jsonstr) {
                if let Some(h) = parsed["hash"].as_str() {
                    let src = blobs::blob_path(&self.sdir, h);
                    let dst = target_root.join(k);
                    if let Some(parent) = dst.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    // Streamed: a large blob lands on disk at page cost.
                    if std::fs::File::open(&src)
                        .and_then(|mut src| {
                            let mut out = std::fs::File::create(&dst)?;
                            std::io::copy(&mut src, &mut out)?;
                            out.sync_all()?;
                            Ok(())
                        })
                        .is_ok()
                    {
                        self.apply_mode_for(&dst, k);
                        written += 1;
                    }
                }
            }
        });
        Ok(written)
    }
}

/// 8 random bytes from the OS as this writer's CRDT peer id.
fn load_or_create_identity(path: &Path) -> Result<(u64, bool)> {
    if let Ok(raw) = std::fs::read_to_string(path) {
        let id: u64 = raw.trim().parse().map_err(|_| {
            SheafError::StoreCorrupt(format!("bad identity file {}", path.display()))
        })?;
        return Ok((id, false));
    }
    let mut buf = [0u8; 8];
    match std::fs::File::open("/dev/urandom").and_then(|mut f| {
        use std::io::Read;
        f.read_exact(&mut buf)
    }) {
        Ok(()) => {}
        Err(_) => {
            // Fallback without /dev/urandom (never on Linux proper).
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            buf = (now ^ ((std::process::id() as u64) << 32)).to_le_bytes();
        }
    }
    let peer = u64::from_le_bytes(buf);
    fsutil::atomic_write(path, peer.to_string().as_bytes()).map_err(io_err)?;
    Ok((peer, true))
}

fn zero_outcome(seq: u64) -> StoreOutcome {
    StoreOutcome {
        seq,
        events_applied: 0,
        text_ops_spliced: 0,
        text_created: 0,
        text_budget_fallbacks: 0,
        binaries_stored: 0,
        blob_files_written: 0,
        tree_records: 0,
        update_bytes: 0,
        rotated: false,
        snapshotted: false,
        capture: None,
    }
}

fn newest_manifest(sdir: &Path) -> Option<(PathBuf, Manifest)> {
    let dir = sdir.join("snapshots");
    let mut best: Option<(u64, PathBuf, Manifest)> = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix("snap-") else {
            continue;
        };
        let Some(idx_raw) = rest.strip_suffix(".manifest.json") else {
            continue;
        };
        let Ok(idx) = idx_raw.parse::<u64>() else {
            continue;
        };
        let Ok(raw) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<Manifest>(&raw) else {
            continue;
        };
        let better = best.as_ref().is_none_or(|(bi, _, _)| idx > *bi);
        if better {
            best = Some((idx, entry.path(), m));
        }
    }
    best.map(|(_, p, m)| (p, m))
}

// ------------------------------------------------------------------ helpers

/// Root-relative POSIX-ish key for map containers.
///
/// Keys are document strings, so a path that is not valid UTF-8 has NO
/// lossless key. Keying it lossily (substituting U+FFFD for bad bytes) forked the
/// truth: restore would materialize a U+FFFD-named duplicate beside the real
/// file and never delete the original. Such paths are now refused here, and
/// every caller skips them with a warning — untracked-but-honest beats
/// tracked-but-wrong.
fn rel_key(root: &Path, p: &Path) -> Result<String> {
    let rel = p.strip_prefix(root).unwrap_or(p);
    let raw = rel.as_os_str().as_encoded_bytes();
    let s = std::str::from_utf8(raw).map_err(|_| {
        SheafError::Config(format!(
            "path {} is not valid UTF-8; it cannot be tracked losslessly and is skipped",
            p.display()
        ))
    })?;
    Ok(s.replace('\\', "/"))
}

fn rel_str(root: &Path, p: &Path) -> String {
    rel_key(root, p).unwrap_or_else(|_| p.display().to_string())
}

fn store_err(e: loro::LoroError) -> SheafError {
    SheafError::StoreCorrupt(format!("loro op failed: {e}"))
}

/// Parsed binaries-map record (only the digest is consulted at present;
/// size stays available for future capacity budgeting).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct BinaryMeta {
    pub hash: String,
    pub size: u64,
}

/// Live executable flag of a path (any execute bit). Missing files read plain.
pub(super) fn file_exec(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path)
        .map(|m| m.mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn import_err(e: loro::LoroError) -> SheafError {
    SheafError::StoreCorrupt(format!("import failed: {e}"))
}
fn encode_err(e: loro::LoroEncodeError) -> SheafError {
    SheafError::StoreCorrupt(format!("export failed: {e}"))
}
fn io_err(e: std::io::Error) -> SheafError {
    SheafError::Io(e)
}

fn log_pending(status: &loro::ImportStatus) {
    if let Some(range) = &status.pending {
        tracing::warn!(?range, "import has pending ranges");
    }
}

// ------------------------------------------------------------ replay batching

/// Replay flush threshold: pending update deltas are imported as one
/// `import_batch` call once they grow past this many bytes (bounded RAM,
/// unbounded frame count). Loro charges a large fixed cost per `import`
/// call — in a large-store benchmark, per-frame imports of a 9 MiB journal
/// measured ~3 ms/frame (5.5 s total) while the same frames in batch calls
/// measured ~0.3 ms/frame, and a fresh batched replay of the whole journal
/// ran 12× faster than the frame-at-a-time version.
const REPLAY_FLUSH_BYTES: usize = 64 * 1024 * 1024;

/// Where a replayed frame came from, for failure messages that name the
/// exact journal position — same diagnostics as the frame-at-a-time loop
/// this buffer replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FrameAt {
    pub segment: u64,
    pub ordinal: usize,
}

/// One frame whose application failed during replay, with its journal
/// position. Callers stop the walk here, exactly where the old loop did.
#[derive(Debug)]
pub(super) struct ReplayFailure {
    pub at: FrameAt,
    pub error: SheafError,
}

/// Buffered journal replay shared by the writer's recovery pass and
/// `TimelineReader::open`. Update deltas queue up and land in bulk
/// `import_batch` calls (order-independent by Loro's contract); ledger
/// records fold in walk order. On a batch failure the buffer replays the
/// chunk frame-by-frame so a corrupt delta still stops the walk at the
/// exact frame and leaves doc/ledger precisely as the sequential loop
/// would have — fast path for the healthy case, identical failure
/// semantics for the corrupt one.
pub(super) struct ReplayBuffer<'a> {
    doc: &'a LoroDoc,
    ledger: &'a mut ledger::LedgerState,
    /// Queued delta payloads.
    deltas: Vec<Vec<u8>>,
    /// Walk sequence number and journal position per queued delta,
    /// parallel to `deltas`.
    delta_meta: Vec<(u64, FrameAt)>,
    /// Queued ledger records with their walk sequence number and position.
    records: Vec<(u64, FrameAt, ledger::LedgerRecord)>,
    bytes: usize,
    seq: u64,
}

impl<'a> ReplayBuffer<'a> {
    pub(super) fn new(doc: &'a LoroDoc, ledger: &'a mut ledger::LedgerState) -> ReplayBuffer<'a> {
        ReplayBuffer {
            doc,
            ledger,
            deltas: Vec::new(),
            delta_meta: Vec::new(),
            records: Vec::new(),
            bytes: 0,
            seq: 0,
        }
    }

    /// Queue one classified update delta, flushing first when the pending
    /// bytes cross the threshold.
    pub(super) fn push_update(
        &mut self,
        delta: Vec<u8>,
        at: FrameAt,
    ) -> std::result::Result<(), ReplayFailure> {
        if self.bytes >= REPLAY_FLUSH_BYTES {
            self.flush()?;
        }
        self.bytes += delta.len();
        self.seq += 1;
        self.delta_meta.push((self.seq, at));
        self.deltas.push(delta);
        Ok(())
    }

    /// Queue one ledger record; records apply at flush in walk order.
    pub(super) fn push_record(&mut self, record: ledger::LedgerRecord, at: FrameAt) {
        self.seq += 1;
        self.records.push((self.seq, at, record));
    }

    /// Apply everything pending. The fast path is one `import_batch` plus
    /// in-order record folds; the fallback re-walks the chunk
    /// frame-by-frame to isolate the first failing delta.
    pub(super) fn flush(&mut self) -> std::result::Result<(), ReplayFailure> {
        let deltas = std::mem::take(&mut self.deltas);
        let delta_meta = std::mem::take(&mut self.delta_meta);
        let records = std::mem::take(&mut self.records);
        self.bytes = 0;
        if deltas.is_empty() {
            for (_, _, record) in records {
                self.ledger.fold(record);
            }
            return Ok(());
        }
        match self.doc.import_batch(&deltas) {
            Ok(status) => {
                log_pending(&status);
                for (_, _, record) in records {
                    self.ledger.fold(record);
                }
                Ok(())
            }
            Err(batch_error) => {
                tracing::warn!(error = %batch_error, "batch import failed; isolating the frame");
                // Sequential re-walk, merging deltas and records back into
                // walk order: records queued before a delta fold first,
                // and the first delta that fails to import stops the walk
                // at its exact journal position.
                let mut record_iter = records.into_iter();
                let mut next_record = record_iter.next();
                for ((seq, at), delta) in delta_meta.into_iter().zip(deltas) {
                    while let Some((rseq, _, record)) = next_record.as_ref() {
                        if *rseq < seq {
                            self.ledger.fold(record.clone());
                            next_record = record_iter.next();
                        } else {
                            break;
                        }
                    }
                    match self.doc.import(&delta) {
                        Ok(status) => log_pending(&status),
                        Err(e) => {
                            return Err(ReplayFailure {
                                at,
                                error: import_err(e),
                            })
                        }
                    }
                }
                for (_, _, record) in next_record.into_iter().chain(record_iter) {
                    self.ledger.fold(record);
                }
                Ok(())
            }
        }
    }
}

trait ToJsonOrCompact {
    fn to_string_pretty_or_compact(&self) -> String;
}
impl ToJsonOrCompact for serde_json::Value {
    fn to_string_pretty_or_compact(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| self.to_string())
    }
}

/// Minimal in-place edit script between two strings at CHAR granularity:
/// common prefix/suffix trim ⇒ (delete range, insert middle). Sufficiently
/// representative for editor-style bursts; a fuller diff library can slot
/// in later behind this same signature.
fn splice_ops(old: &str, new: &str) -> Option<(usize, usize, String)> {
    if old == new {
        return None;
    }
    let a: Vec<char> = old.chars().collect();
    let b: Vec<char> = new.chars().collect();
    let max_prefix = a.len().min(b.len());
    let mut prefix = 0usize;
    while prefix < max_prefix && a[prefix] == b[prefix] {
        prefix += 1;
    }
    let max_suffix = (a.len() - prefix).min(b.len() - prefix);
    let mut suffix = 0usize;
    while suffix < max_suffix && a[a.len() - 1 - suffix] == b[b.len() - 1 - suffix] {
        suffix += 1;
    }
    let del_len = a.len() - prefix - suffix;
    let ins: String = b[prefix..b.len() - suffix].iter().collect();
    if del_len == 0 && ins.is_empty() {
        return None;
    }
    let script = (prefix, del_len, ins.clone());
    debug_assert_eq!(
        {
            let mut rebuilt = String::new();
            rebuilt.extend(a[..prefix].iter());
            rebuilt.push_str(&ins);
            rebuilt.extend(a[a.len() - suffix..].iter());
            rebuilt
        },
        new,
        "splice must reconstruct target"
    );
    Some(script)
}

/// Try to take the project's exclusive cross-process writer lock without
/// blocking. `Some(file)` keeps the lock alive for as long as the holder
/// owns the descriptor; dropping releases it.
pub fn try_lock_shared(lock_path: &Path) -> std::io::Result<Option<std::fs::File>> {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::OpenOptions::new().read(true).open(lock_path)?;
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
    if rc == 0 {
        Ok(Some(f))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

pub fn try_lock_exclusive(lock_path: &Path) -> std::io::Result<Option<std::fs::File>> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(lock_path)?;
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(Some(f))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Batch, EventKind, FsEvent};

    /// Open + one flushed capture touching exactly `paths`.
    fn capture_files(
        root: &Path,
        limits: StoreLimits,
        paths: &[&str],
    ) -> Result<(ProjectStore, StoreOutcome)> {
        let mut store = ProjectStore::open(root, limits)?;
        let events: Vec<FsEvent> = paths
            .iter()
            .map(|p| {
                FsEvent::now(EventKind::Touched {
                    path: crate::events::TouchedPath(root.join(p)),
                })
            })
            .collect();
        let outcome = store.apply_batch(&Batch {
            root: root.to_path_buf(),
            events,
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
        })?;
        Ok((store, outcome))
    }

    #[test]
    fn utf8_over_one_mib_takes_the_blob_path_even_with_budget_room() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        std::fs::write(
            root.join("big.txt"),
            "a".repeat((TEXT_MAX_BYTES + 1) as usize),
        )
        .unwrap();
        let limits = StoreLimits {
            max_segment_bytes: 64 << 20,
            snapshot_edit_size: 1000,
        };
        let (store, outcome) = capture_files(root, limits, &["big.txt"]).expect("capture applies");
        assert_eq!(
            outcome.text_created, 0,
            "an oversized file must not become a CRDT text container"
        );
        assert!(
            outcome.binaries_stored >= 1,
            "the oversized file must land as a content-addressed blob"
        );
        assert_eq!(store.tracked_text_bytes, 0);
    }

    #[test]
    fn new_text_admission_is_capped_per_capture() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        // Ten 1 MiB files: individually text-sized, together past the
        // per-capture ceiling of TEXT_BATCH_MAX_BYTES (8 MiB).
        let files: Vec<String> = (0..10)
            .map(|i| {
                let name = format!("f{i:02}.txt");
                let mut body = String::with_capacity(1024 * 1024);
                let line = format!("line {i} of ten files\n");
                while body.len() + line.len() <= 1024 * 1024 {
                    body.push_str(&line);
                }
                std::fs::write(root.join(&name), &body).unwrap();
                name
            })
            .collect();
        let refs: Vec<&str> = files.iter().map(String::as_str).collect();
        let limits = StoreLimits {
            max_segment_bytes: 512 << 20,
            snapshot_edit_size: 1000,
        };
        let (store, outcome) = capture_files(root, limits, &refs).expect("capture applies");
        assert!(
            outcome.text_budget_fallbacks >= 2,
            "files past the per-capture ceiling must fall back to blobs: {outcome:?}"
        );
        assert!(
            store.tracked_text_bytes <= TEXT_BATCH_MAX_BYTES,
            "admitted text must never exceed the per-capture ceiling, got {}",
            store.tracked_text_bytes
        );
        assert!(
            outcome.update_bytes < 12 * 1024 * 1024,
            "the exported delta must stay bounded, got {} bytes",
            outcome.update_bytes
        );
    }

    #[test]
    fn journal_tail_past_one_segment_snapshots_on_flush_and_again_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        // ~100 KiB of text: one delta comfortably past a 32 KiB segment cap.
        std::fs::write(root.join("tail.txt"), "z".repeat(100 * 1024)).unwrap();
        let small = StoreLimits {
            max_segment_bytes: 32 * 1024,
            snapshot_edit_size: 1000,
        };
        let (_, outcome) =
            capture_files(root, small.clone(), &["tail.txt"]).expect("capture applies");
        assert!(
            outcome.snapshotted,
            "crossing max_segment_bytes since the baseline must compact: {outcome:?}"
        );

        // And the crash variant: captured under a LARGE segment cap (no
        // snapshot fired), then reopened with a small one — the tail past
        // the newest snapshot must force compaction at open.
        let tmp2 = tempfile::tempdir().unwrap();
        let root2 = tmp2.path();
        config::write_skeleton(root2).unwrap();
        std::fs::write(root2.join("tail.txt"), "z".repeat(100 * 1024)).unwrap();
        let large = StoreLimits {
            max_segment_bytes: 512 << 20,
            snapshot_edit_size: 1000,
        };
        let (_, o2) = capture_files(root2, large, &["tail.txt"]).expect("capture applies");
        assert!(!o2.snapshotted, "large cap must not snapshot yet");
        let snaps = root2.join(".sheaf/store/snapshots");
        let manifests_before = list_manifests(&snaps);
        assert!(manifests_before.is_empty(), "precondition: no snapshot yet");
        drop(ProjectStore::open(root2, small).expect("reopen compacts the tail"));
        let manifests_after = list_manifests(&snaps);
        assert_eq!(
            manifests_after.len(),
            1,
            "reopen must write exactly one fresh baseline manifest"
        );
    }

    fn list_manifests(snaps: &Path) -> Vec<std::ffi::OsString> {
        std::fs::read_dir(snaps)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name())
                    .filter(|n| n.to_string_lossy().ends_with(".manifest.json"))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn splice_char_math_with_multibyte() {
        // Functional contract: applying the script to `old` yields `new`,
        // char-positions stay valid across multibyte scalars.
        let cases: &[(&str, &str)] = &[
            ("héllo wörld ünïcode 🌍!", "héllo BIG wörld ünïcode 🌍!!"),
            ("a🌍b🌍c", "a🌍X Y🌍c"),
            ("same", "same"),           // no-op
            ("short", "shorter edits"), // multiple regions
            ("日本語のテキスト", ""),   // full delete incl multibyte
            ("", "brand new 🎉"),       // from empty
        ];
        for &(old, new) in cases {
            match splice_ops(old, new) {
                None => assert_eq!(old, new),
                Some((pos, del, ins)) => {
                    let ac: Vec<char> = old.chars().collect();
                    assert!(pos + del <= ac.len(), "range out of bounds");
                    let mut rebuilt = String::new();
                    rebuilt.extend(&ac[..pos]);
                    rebuilt.push_str(&ins);
                    rebuilt.extend(&ac[ac.len() - (ac.len() - pos - del)..]);
                    assert_eq!(rebuilt, new, "rebuild mismatch for {old:?} -> {new:?}");
                }
            }
        }
        // Unambiguous ASCII insertion pins deterministic positioning.
        let (pos, del, ins) = splice_ops("abc123", "abcX123").unwrap();
        assert_eq!((pos, del, ins.as_str()), (3, 0, "X"));
    }

    #[test]
    fn newest_manifest_picks_highest_valid_index_and_skips_junk() {
        let tmp = tempfile::tempdir().unwrap();
        let sdir = tmp.path().to_path_buf();
        let snaps = sdir.join("snapshots");
        std::fs::create_dir_all(&snaps).unwrap();
        let manifest = |snapshot: &str| format!(r#"{{"snapshot":"{snapshot}","covered_upto":7}}"#);
        std::fs::write(
            snaps.join("snap-000003.manifest.json"),
            manifest("snap-000003.snapshot"),
        )
        .unwrap();
        std::fs::write(snaps.join("snap-000005.manifest.json"), b"{not json").unwrap();
        std::fs::write(
            snaps.join("snap-nan.manifest.json"),
            manifest("snap-000009.snapshot"),
        )
        .unwrap(); // non-numeric index
        std::fs::write(
            snaps.join("snap-000006.nothy"),
            manifest("snap-000006.snapshot"),
        )
        .unwrap(); // wrong suffix
        std::fs::write(
            snaps.join("snap-000007.manifest.json"),
            manifest("snap-000007.snapshot"),
        )
        .unwrap();
        // A directory cannot be read to a string: skipped, not fatal.
        std::fs::create_dir_all(snaps.join("snap-000008.manifest.json")).unwrap();

        let (path, best) = newest_manifest(&sdir).unwrap();
        assert!(path.ends_with("snap-000007.manifest.json"), "{path:?}");
        assert_eq!(best.snapshot, "snap-000007.snapshot");
        assert_eq!(best.covered_upto, 7);
        assert!(best.total_edits == 0 && best.shallow_since.is_none() && best.ledger.is_none());

        // No snapshots directory at all: no manifest.
        assert!(newest_manifest(&tmp.path().join("absent-store")).is_none());
    }

    #[test]
    fn identity_file_roundtrips_and_refuses_garbage() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("identity");

        let (first, is_fresh) = load_or_create_identity(&path).unwrap();
        assert!(is_fresh, "first open creates the identity");
        let (again, is_fresh) = load_or_create_identity(&path).unwrap();
        assert_eq!(first, again, "peer id must be stable across reopens");
        assert!(!is_fresh);

        std::fs::write(&path, b"not-a-number").unwrap();
        let err = load_or_create_identity(&path).unwrap_err();
        assert!(
            matches!(&err, SheafError::StoreCorrupt(m) if m.contains("bad identity file")),
            "{err}"
        );
    }

    #[test]
    fn rel_key_rejects_non_utf8_paths_and_rel_str_falls_back_to_display() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        assert_eq!(
            rel_key(root, &root.join("src/lib.rs")).unwrap(),
            "src/lib.rs"
        );

        use std::os::unix::ffi::OsStringExt;
        let bad = root.join(std::ffi::OsString::from_vec(vec![0x68, 0xFF, 0x69]));
        assert!(matches!(
            rel_key(root, &bad),
            Err(SheafError::Config(m)) if m.contains("not valid UTF-8")
        ));
        assert_eq!(rel_str(root, &bad), bad.display().to_string());
    }

    #[test]
    fn exclusive_and_shared_locks_have_flock_semantics() {
        let tmp = tempfile::tempdir().unwrap();
        let lock = tmp.path().join("lock");
        std::fs::write(&lock, b"").unwrap();

        let g1 = try_lock_exclusive(&lock).unwrap();
        assert!(g1.is_some());
        assert!(
            try_lock_exclusive(&lock).is_err(),
            "second exclusive blocks"
        );
        assert!(
            try_lock_shared(&lock).is_err(),
            "shared blocks under exclusive"
        );
        drop(g1);

        let s1 = try_lock_shared(&lock).unwrap();
        let s2 = try_lock_shared(&lock).unwrap();
        assert!(s1.is_some() && s2.is_some(), "shared locks stack");
        drop((s1, s2));
        assert!(try_lock_exclusive(&lock).unwrap().is_some());

        // Shared open does not create the lock file; a missing path errors.
        let absent = tmp.path().join("absent.lock");
        assert!(try_lock_shared(&absent).is_err());
        assert!(!absent.exists());
    }

    #[test]
    fn atomic_write_public_replaces_content_and_creates_missing_parents() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("state.json");
        atomic_write_public(&file, b"one").unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"one");
        atomic_write_public(&file, b"two").unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"two");

        // The write is atomic-shaped: a missing parent directory is part of
        // the contract, so a fresh nested path lands whole.
        let nested = tmp.path().join("deep/dir/state.json");
        atomic_write_public(&nested, b"three").unwrap();
        assert_eq!(std::fs::read(&nested).unwrap(), b"three");
    }

    #[test]
    fn io_errors_wrap_into_sheaf_io() {
        let err = io_err(std::io::Error::other("boom scenario"));
        assert!(
            matches!(&err, SheafError::Io(inner) if inner.to_string().contains("boom scenario")),
            "{err}"
        );
    }

    #[test]
    fn zero_outcome_carries_only_the_sequence() {
        let outcome = zero_outcome(41);
        assert_eq!(outcome.seq, 41);
        assert_eq!(outcome.events_applied, 0);
        assert_eq!(outcome.text_ops_spliced, 0);
        assert_eq!(outcome.text_created, 0);
        assert_eq!(outcome.binaries_stored, 0);
        assert!(!outcome.rotated);
        assert!(!outcome.snapshotted);
        assert!(outcome.capture.is_none());
    }

    #[test]
    fn json_values_serialize_in_pretty_form() {
        let value = serde_json::json!({ "b": 1, "a": [1, 2] });
        let rendered = value.to_string_pretty_or_compact();
        assert!(
            rendered.contains('\n'),
            "expected pretty output: {rendered}"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rendered).unwrap(),
            value
        );
    }

    #[test]
    fn replay_flush_without_deltas_is_a_no_op() {
        let doc = LoroDoc::new();
        let mut ledger = LedgerState::default();
        let mut buffer = ReplayBuffer::new(&doc, &mut ledger);
        buffer.flush().unwrap();
        drop(buffer);
        assert!(ledger.tombstones.is_empty());
        assert!(ledger.checkpoints.is_empty());
    }

    #[test]
    fn open_refuses_a_manifest_whose_snapshot_is_not_loro_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        {
            // Lay down store directories, then leave.
            ProjectStore::open(
                root,
                StoreLimits {
                    max_segment_bytes: 4 << 20,
                    snapshot_edit_size: 3,
                },
            )
            .unwrap();
        }
        let snaps = root.join(".sheaf/store/snapshots");
        std::fs::create_dir_all(&snaps).unwrap();
        std::fs::write(snaps.join("snap-000001.snapshot"), b"definitely-not-loro").unwrap();
        std::fs::write(
            snaps.join("snap-000001.manifest.json"),
            r#"{"snapshot":"snap-000001.snapshot","covered_upto":0}"#,
        )
        .unwrap();

        let outcome = ProjectStore::open(
            root,
            StoreLimits {
                max_segment_bytes: 4 << 20,
                snapshot_edit_size: 3,
            },
        );
        let err = outcome
            .err()
            .expect("open must refuse a manifest whose snapshot is not loro bytes");
        assert!(
            matches!(&err, SheafError::StoreCorrupt(m) if m.contains("import failed")),
            "{err}"
        );
    }
    #[test]
    fn binary_echo_and_streaming_paths_distinguish_content_and_missing_renames() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        let mut store = ProjectStore::open(root, StoreLimits::default()).unwrap();
        let mut outcome = zero_outcome(0);
        assert!(store
            .upsert_binary("blob.bin", b"bytes", &mut outcome)
            .unwrap());
        assert!(!store
            .upsert_binary("blob.bin", b"bytes", &mut outcome)
            .unwrap());
        std::fs::write(root.join("stream.bin"), b"streamed").unwrap();
        assert!(store
            .upsert_binary_streaming(
                root.join("stream.bin").as_path(),
                "stream.bin",
                &mut outcome
            )
            .unwrap());
        assert!(!store
            .move_entry(Path::new("missing"), Path::new("other"))
            .unwrap());
    }

    #[test]
    fn materialize_skips_malformed_binary_records_and_empty_text() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        let mut store = ProjectStore::open(root, StoreLimits::default()).unwrap();
        let mut outcome = zero_outcome(0);
        store.upsert_text("empty.txt", "", &mut outcome).unwrap();
        store
            .doc
            .get_map(BINARIES_MAP)
            .insert("bad", "not-json")
            .unwrap();
        let target = root.join("out");
        assert_eq!(store.materialize(&target).unwrap(), 0);
        assert!(!target.join("empty.txt").exists());
    }
    #[test]
    fn content_differs_and_exec_flags_follow_live_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        let mut store = ProjectStore::open(root, StoreLimits::default()).unwrap();
        let mut outcome = zero_outcome(0);
        let path = root.join("a.txt");
        std::fs::write(&path, "same").unwrap();
        store.upsert_text("a.txt", "same", &mut outcome).unwrap();
        assert_eq!(store.content_differs(&path), Some(false));
        std::fs::write(&path, "changed").unwrap();
        assert_eq!(store.content_differs(&path), Some(true));
        assert_eq!(store.content_differs(&root.join("unknown")), None);
        assert!(!file_exec(&root.join("missing")));
        assert_eq!(splice_ops("abc", "abc"), None);
        assert_eq!(splice_ops("abc", "axc"), Some((1, 1, "x".into())));
    }
    #[test]
    fn empty_batch_preserves_sequence_without_capture() {
        let tmp = tempfile::tempdir().unwrap();
        config::write_skeleton(tmp.path()).unwrap();
        let mut store = ProjectStore::open(tmp.path(), StoreLimits::default()).unwrap();
        let before = store.seq;
        let outcome = store
            .apply_batch(&Batch {
                root: tmp.path().to_path_buf(),
                events: Vec::new(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
            })
            .unwrap();
        assert_eq!(outcome.seq, before);
        assert_eq!(outcome.events_applied, 0);
        assert!(outcome.capture.is_none());
    }

    #[test]
    fn snapshot_due_requires_a_nonzero_limit_and_reaches_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        config::write_skeleton(tmp.path()).unwrap();
        let mut store = ProjectStore::open(tmp.path(), StoreLimits::default()).unwrap();
        store.bytes_since_snapshot = 1;
        assert!(!store.size_snapshot_due());
        store.limits.max_segment_bytes = 1;
        assert!(store.size_snapshot_due());
        store.limits.max_segment_bytes = 0;
        assert!(!store.size_snapshot_due());
    }

    #[test]
    fn invalid_utf8_key_is_warned_once_and_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        config::write_skeleton(tmp.path()).unwrap();
        let mut store = ProjectStore::open(tmp.path(), StoreLimits::default()).unwrap();
        use std::os::unix::ffi::OsStringExt;
        let path = tmp
            .path()
            .join(std::ffi::OsString::from_vec(vec![b'x', 0xff]));
        assert!(store.key_for(&path).is_none());
        assert!(store.key_for(&path).is_none());
        assert_eq!(store.warned_keys.len(), 1);
    }
    #[test]
    fn open_replays_journal_when_newest_snapshot_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        {
            let mut store = ProjectStore::open(root, StoreLimits::default()).unwrap();
            std::fs::write(root.join("tracked.txt"), "recovered\n").unwrap();
            store
                .apply_batch(&Batch {
                    root: root.to_path_buf(),
                    events: vec![FsEvent::now(EventKind::Touched {
                        path: root.join("tracked.txt").into(),
                    })],
                    started_at: chrono::Utc::now(),
                    flushed_at: chrono::Utc::now(),
                })
                .unwrap();
        }
        let snapshots = root.join(".sheaf/store/snapshots");
        std::fs::write(
            snapshots.join("snap-999999.manifest.json"),
            r#"{"snapshot":"missing.snapshot","covered_upto":999999}"#,
        )
        .unwrap();
        let reopened = ProjectStore::open(root, StoreLimits::default()).unwrap();
        assert!(reopened.known_paths().contains(&root.join("tracked.txt")));
    }
    #[test]
    fn streaming_binary_boundary_is_exactly_size_sensitive() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        config::write_skeleton(root).unwrap();
        let limits = StoreLimits::default();
        let capture = |store: &mut ProjectStore, name: &str, size: usize| {
            std::fs::write(root.join(name), vec![b'x'; size]).unwrap();
            store
                .apply_batch(&Batch {
                    root: root.to_path_buf(),
                    events: vec![FsEvent::now(EventKind::Touched {
                        path: root.join(name).into(),
                    })],
                    started_at: chrono::Utc::now(),
                    flushed_at: chrono::Utc::now(),
                })
                .unwrap()
        };
        let mut store = ProjectStore::open(root, limits).unwrap();
        let exact = capture(&mut store, "exact", TEXT_MAX_BYTES as usize);
        assert_eq!(exact.text_created, 1);
        let over = capture(&mut store, "over", TEXT_MAX_BYTES as usize + 1);
        assert_eq!(over.text_created, 0);
        assert_eq!(over.binaries_stored, 1);
        let empty = capture(&mut store, "empty", 0);
        assert_eq!(empty.text_created, 1);
    }
}

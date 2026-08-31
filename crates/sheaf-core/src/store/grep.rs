//! Timeline grep: read-only literal search over retained text history,
//! reduced to lifecycle transitions.
//!
//! The engine is scan-first: it walks retained captures on the
//! requested lineage set, reads only touched text paths at each exact
//! frontier, matches literally, and emits how each matched unit was
//! introduced, changed, moved, removed, and reintroduced. Every restorable
//! hit carries the immutable [`SelectionHandle`] from the semantic selection
//! layer. Search may populate a disposable derived cache; it never writes the
//! authoritative timeline, worktree, or git.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use loro::{Frontiers, LoroDoc};
use serde::{Deserialize, Serialize};

use super::restore::HistoryView;
use super::selection::{
    ByteRange, HistoricalPathContent, LifecycleKind, SearchBudget, SearchCursor, SearchStopReason,
    SearchUsage, SelectionExtent, SelectionHandle,
};
use super::timeline::{captures_from, decode_frontier, Capture};
use super::{ProjectStore, TimelineReader};
use crate::error::{Result, SheafError};

/// Cursor sentinel meaning "resume before the first point": used when the
/// budget trips before any capture is emitted, so a resume re-enters at the
/// very first in-range capture without replaying anything.
const BEFORE_FIRST: &str = "@before-first";

/// The literal query. Regex is an additive follow-up; an unknown kind is
/// rejected at the wire boundary, never silently treated as literal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrepQuery {
    Literal { text: String },
}

impl GrepQuery {
    /// Construct a literal-text query.
    pub fn literal(text: impl Into<String>) -> Self {
        GrepQuery::Literal { text: text.into() }
    }

    fn needle(&self) -> &str {
        match self {
            GrepQuery::Literal { text } => text,
        }
    }

    fn tag(&self) -> String {
        match self {
            GrepQuery::Literal { text } => format!("literal:{text}"),
        }
    }
}

fn extent_tag(extent: SelectionExtent) -> &'static str {
    match extent {
        SelectionExtent::Match => "match",
        SelectionExtent::Line => "line",
        SelectionExtent::Hunk => "hunk",
        SelectionExtent::Symbol => "symbol",
    }
}

/// Point discovery is the new CLI default. History remains the serde default so
/// older IPC clients that omit this additive field preserve their behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrepMode {
    Point,
    #[default]
    History,
}

/// One occurrence anchor for history mode: selects the single episode the
/// walk reports. Anchor forms are mutually exclusive by
/// construction; every form must resolve inside `(from, to]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrepAnchor {
    /// `--history --at <point> --path <file> --line <one-based>` with an
    /// optional one-based Unicode-scalar column. Resolves to exactly one
    /// occurrence at `at`: zero is missing, more than one is ambiguous.
    Coordinate {
        path: String,
        line: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        column: Option<usize>,
    },
    /// A full selection handle; it carries its own anchor frontier, and an
    /// explicit `at` must agree with it.
    Selection { handle: Box<SelectionHandle> },
    /// A branch-qualified episode ID as emitted by a prior history walk.
    /// Rejected together with `at`: the ID already names the episode.
    Episode { episode_id: String },
}

impl GrepAnchor {
    /// Canonical identity for cursor binding: two requests anchoring the same
    /// occurrence share it, and every different anchor changes it.
    fn identity(&self) -> String {
        match self {
            GrepAnchor::Coordinate { path, line, column } => {
                format!("coordinate:{path}:{line}:{}", column.unwrap_or(0))
            }
            GrepAnchor::Selection { handle } => {
                format!("selection:{}:{}", handle.source_frontier, handle.id())
            }
            GrepAnchor::Episode { episode_id } => format!("episode:{episode_id}"),
        }
    }
}

/// One grep request. The public v1 extents are `Match` and `Line`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepRequest {
    pub query: GrepQuery,
    /// Explicit search mode. Omitted on legacy IPC requests means history.
    #[serde(default)]
    pub mode: GrepMode,
    /// Point-discovery state, or the anchor point for a coordinate/selection
    /// history anchor; `None` means `@` in point mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// History-only single-occurrence anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<GrepAnchor>,
    /// Oldest history bound (exclusive lower); `None` means retained floor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Inclusive history upper bound; `None` means `@`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub follow: bool,
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub every_capture: bool,
    #[serde(default = "default_extent")]
    pub extent: SelectionExtent,
    #[serde(default)]
    pub budget: SearchBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SearchCursor>,
}

fn default_extent() -> SelectionExtent {
    SelectionExtent::Match
}

impl GrepRequest {
    /// Fingerprint binding cursors and handles to one complete search intent:
    /// query, extent, lineage/verbosity flags, the full resolved scope, and the
    /// anchor identity. A cursor from a differently-scoped or
    /// differently-anchored query is rejected.
    pub fn fingerprint(&self) -> String {
        format!(
            "{}|mode={:?}|extent={}|all={}|every={}|at={}|from={}|to={}|path={}|follow={}|anchor={}",
            self.query.tag(),
            self.mode,
            extent_tag(self.extent),
            self.all as u8,
            self.every_capture as u8,
            self.at.as_deref().unwrap_or(""),
            self.from.as_deref().unwrap_or(""),
            self.to.as_deref().unwrap_or(""),
            self.path.as_deref().unwrap_or(""),
            self.follow as u8,
            self.anchor.as_ref().map(GrepAnchor::identity).unwrap_or_default(),
        )
    }

    /// Public so IPC handlers can reject malformed requests before any
    /// streamed bytes leave the daemon (run() validates again regardless).
    pub fn validate(&self) -> Result<()> {
        if self.query.needle().is_empty() {
            return Err(SheafError::Config("grep query must not be empty".into()));
        }
        if self.budget.max_results == 0 {
            // A zero budget cannot make progress: the first record always
            // overflows, so the returned cursor would repeat forever.
            return Err(SheafError::Config(
                "grep `max_results` must be at least 1".into(),
            ));
        }
        if self.budget.max_elapsed_ms == 0 {
            // Same livelock shape: the first check trips before any record
            // exists and the cursor repeats with nothing emitted.
            return Err(SheafError::Config(
                "grep `max_elapsed_ms` must be at least 1".into(),
            ));
        }
        if !matches!(self.extent, SelectionExtent::Match | SelectionExtent::Line) {
            return Err(SheafError::Config(
                "grep supports the `match` and `line` extents in this release".into(),
            ));
        }
        if let Some(anchor) = &self.anchor {
            if self.mode == GrepMode::Point {
                return Err(SheafError::Config(
                    "occurrence anchors select one episode and require `--history`".into(),
                ));
            }
            match anchor {
                GrepAnchor::Coordinate { .. } => {
                    if self.at.is_none() {
                        return Err(SheafError::Config(
                            "a coordinate anchor requires `--at <point>` naming the snapshot it resolves at".into(),
                        ));
                    }
                }
                GrepAnchor::Episode { .. } => {
                    if self.at.is_some() {
                        return Err(SheafError::Config(
                            "an episode anchor is already branch-qualified and does not take `--at`".into(),
                        ));
                    }
                }
                GrepAnchor::Selection { .. } => {}
            }
        } else if self.mode == GrepMode::History && self.at.is_some() {
            return Err(SheafError::Config(
                "history `--at` names an occurrence anchor: add --line (with --path), --selection, or --episode".into(),
            ));
        }
        if self.mode == GrepMode::Point
            && (self.from.is_some()
                || self.to.is_some()
                || self.all
                || self.follow
                || self.every_capture)
        {
            return Err(SheafError::Config(
                "point grep accepts `--at`; history range, lineage, rename, and every-capture options require `--history`".into(),
            ));
        }
        if self.follow && self.path.is_none() {
            return Err(SheafError::Config("`follow` requires `path`".into()));
        }
        if let Some(cursor) = &self.cursor {
            if cursor.query_fingerprint != self.fingerprint() {
                return Err(SheafError::BadCursor(
                    "cursor was issued for a differently-scoped query".into(),
                ));
            }
        }
        Ok(())
    }
}

/// One matched unit at one capture, ready to restore or squash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepHit {
    pub capture_id: String,
    pub frontier: String,
    pub timestamp_ms: i64,
    pub lineage_id: String,
    pub on_current: bool,
    pub path: String,
    pub kind: LifecycleKind,
    /// One-based source coordinates of the literal occurrence. These describe
    /// the match, even when the restorable extent expands to its whole line.
    #[serde(default)]
    pub line: usize,
    #[serde(default)]
    pub column: usize,
    /// Stable identity of this exact occurrence at this snapshot. Unlike a
    /// line-extent handle, two literals on one line remain distinct.
    #[serde(default)]
    pub occurrence_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    pub preview: String,
    pub handle: SelectionHandle,
    pub handle_id: String,
}

/// A reported retention gap, removal, or ambiguity diagnostic — no fabricated
/// handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepEvent {
    pub capture_id: String,
    pub frontier: String,
    pub timestamp_ms: i64,
    pub lineage_id: String,
    pub on_current: bool,
    pub kind: LifecycleKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// For a removal, the handle ID of the last-present unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_present_handle_id: Option<String>,
    /// The episode this event terminates (removed/ambiguous predecessor).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    /// Ordered candidate handle IDs for an `ambiguous` event: every current
    /// occurrence with exact selected bytes on a path this episode could
    /// continue on, in (path, byte range) order. Empty for removals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidates: Option<Vec<String>>,
}

/// The authoritative result of a grep walk: every hit and lifecycle event,
/// the resume cursor, budget usage, and whether the walk ran to completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepReport {
    pub query_fingerprint: String,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<SearchStopReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<SearchCursor>,
    pub hits: Vec<GrepHit>,
    pub events: Vec<GrepEvent>,
    pub skipped_binary: usize,
    pub pruned_intervals: usize,
    pub usage: SearchUsage,
    pub degraded: bool,
}

/// One incrementally emitted record from a grep walk. Delivery order is
/// walk order (chronological per lineage): a hit's transition kind is
/// final the moment it is pushed, so streaming callers can print each
/// record as it arrives — GNU-grep style liveness instead of a single
/// flush after the full scan. The final [`GrepReport`] remains the
/// authoritative summary and contains exactly the streamed records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GrepStreamRecord {
    Hit { hit: Box<GrepHit> },
    Event { event: GrepEvent },
}

/// Callback invoked once per finalized record during a streaming walk.
pub type GrepSink<'a> = &'a mut (dyn FnMut(GrepStreamRecord) + 'a);

/// One lineage's replay-reconstructable active occurrence episodes.
#[derive(Default, Clone)]
struct UnitState {
    present: Vec<PresentUnit>,
}

#[derive(Clone)]
struct PresentUnit {
    path: String,
    match_range: ByteRange,
    selected_sha256: String,
    before_sha256: String,
    after_sha256: String,
    line_sha256: String,
    handle_id: String,
    episode_id: String,
}

/// Enumerated capture plus the frontier and lineage it belongs to.
struct Point {
    capture: Capture,
    lineage_id: String,
    on_current: bool,
    pruned: bool,
    /// Immutable key the episode-ID derivation hashes instead of the
    /// display lineage: `"current"` or the lineage's branch-root capture.
    /// The display lineage names a branch tip, which advances as the branch
    /// grows; the root capture is fixed, so IDs stay stable and forks of the
    /// same point derive distinct child IDs.
    episode_lineage: String,
}

/// 32 MiB: content identities stay servable from the zstd sidecar on disk
/// (one decompression per miss), so the in-RAM LRU is a latency
/// optimization whose ceiling must justify itself against the daemon's
/// resident set.
const GREP_CONTENT_CACHE_BYTES: usize = 32 * 1024 * 1024;
const GREP_CACHE_SCHEMA: u8 = 1;
const GREP_CACHE_ZSTD_LEVEL: i32 = 3;
/// Backfill persists its watermark at least this often, so an interrupted
/// run resumes near where it stopped instead of from the beginning.
const GREP_BACKFILL_WATERMARK_EVERY: usize = 32;

type ContentCacheKey = (String, String);

/// Generation watermark for the derived cache: the newest current-lineage
/// capture whose every touched-path row is durably published, plus the
/// generation counter that distinguishes rebuilds. Advisory but honest —
/// it advances only along a contiguous covered chain, so it can lag (a
/// crash between the mappings append and the watermark write), never lie.
/// Queries never consult it for correctness; backfill uses it to skip the
/// covered prefix and report progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepCacheWatermark {
    pub v: u8,
    pub generation: u64,
    /// Current-lineage captures covered by this chain, counted at write
    /// time. Advisory progress reporting only.
    pub captures_indexed: usize,
    /// The newest covered capture.
    pub through_capture_id: String,
    pub through_frontier: String,
    pub updated_ms: i64,
}

/// Options for an explicit cache backfill/rebuild run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrepBackfillOptions {
    /// Also index captures exclusive to divergent branches (rows only; the
    /// watermark tracks the current lineage).
    #[serde(default)]
    pub all: bool,
    /// Wipe the cache first (bumping the generation) and republish every
    /// row from authoritative materialization.
    #[serde(default)]
    pub rebuild: bool,
    /// Stop after indexing this many not-yet-complete captures; already
    /// complete captures do not count against the limit. `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Soft wall-clock budget for the run, checked between captures: a
    /// trip stops the walk and reports `complete: false` so callers page
    /// on. `None` = unbounded. The capture in flight always finishes, so
    /// the reported elapsed can exceed the budget by one capture's
    /// materialization cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_elapsed_ms: Option<u64>,
}

/// Outcome of one backfill/rebuild run. Every counter is idempotent-safe:
/// a second run on a complete store reports zero rows written.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GrepBackfillReport {
    pub root: String,
    pub rebuilt: bool,
    pub all: bool,
    /// True when every walked capture is fully indexed after this run
    /// (bounded runs and materialization failures leave it false).
    pub complete: bool,
    pub generation: u64,
    pub captures_examined: usize,
    /// Captures that already had complete rows (nothing written).
    pub captures_skipped: usize,
    /// Captures whose missing rows were published this run.
    pub captures_indexed: usize,
    /// Captures whose rows could not be materialized (logged; rows stay
    /// missing and queries fall back).
    pub captures_failed: usize,
    pub rows_written: usize,
    pub content_blobs_written: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<GrepCacheWatermark>,
    /// On-disk size of the trigram pre-filter index after this run, in bytes.
    #[serde(default)]
    pub trigram_index_bytes: u64,
    pub elapsed_ms: u64,
}

/// Filesystem facts about the derived grep cache, for doctor's advisory
/// report. Nothing here affects store integrity: the cache is disposable
/// and every failure mode is a miss plus authoritative fallback.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GrepCacheFacts {
    pub present: bool,
    pub rows: usize,
    /// Unparseable lines in mappings.jsonl — the torn tail a crash can
    /// leave behind, or manual damage. Skipped on load; removed by rebuild.
    pub torn_lines: usize,
    /// Text mappings whose compressed content file is missing.
    pub missing_content: usize,
    /// Content files no mapping references (crash leftovers, or damage).
    pub orphan_content_files: usize,
    pub orphan_content_bytes: u64,
    pub content_files: usize,
    pub content_bytes: u64,
    pub watermark: Option<GrepCacheWatermark>,
    pub watermark_unparseable: bool,
    /// On-disk size of the trigram pre-filter index, 0 when absent.
    pub trigram_index_bytes: u64,
    /// The trigram index file exists but does not decode (corrupt/stale).
    /// Advisory only: queries fall back to scanning every distinct content.
    pub trigram_index_corrupt: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheHit {
    Miss,
    Memory,
    Disk,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CacheMapping {
    v: u8,
    frontier: String,
    path: String,
    #[serde(flatten)]
    value: CacheMappingValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CacheMappingValue {
    Text { hash: String, bytes: u64 },
    Binary { hash: String, bytes: u64 },
    Absent,
}

/// Bounded process cache backed by a derived content-addressed sidecar. Exact
/// point mappings are immutable because frontiers are immutable. Writers
/// publish the compressed content first and append its mapping second, so a
/// crash can only leave an unreferenced blob or a torn final JSON line; either
/// case is a cache miss and the timeline remains authoritative.
pub(super) struct GrepContentCache {
    entries: BTreeMap<ContentCacheKey, CachedEntry>,
    order: VecDeque<ContentCacheKey>,
    bytes: usize,
    mappings: BTreeMap<ContentCacheKey, CacheMappingValue>,
    index_dir: std::path::PathBuf,
    writable: bool,
    /// Durable coverage chain, loaded from `watermark.json`.
    watermark: Option<GrepCacheWatermark>,
    /// Generation to stamp on the next watermark write. Bumped by an
    /// explicit rebuild or a retention wipe so successive generations are
    /// distinguishable in reports.
    next_generation: u64,
    /// The mappings file's last line lacks its newline (a crash tore the
    /// tail). The next append separates first so the torn fragment cannot
    /// swallow a fresh record.
    torn_tail: bool,
    /// Daemon-resident warm state. One `ProjectStore` lives for the
    /// daemon's lifetime and serves every query, so caching here reuses work
    /// across queries. All of it is derived and disposable; a wipe or rebuild
    /// bumps `warm_generation` and the stale entries are ignored on the next
    /// read rather than eagerly cleared. The degraded reader is one-shot, so
    /// its warm state simply never accumulates.
    warm: WarmQueryState,
}

/// Cross-query warm caches keyed to the sidecar's derived state. The trigram
/// index is loaded from disk (decompress + parse) only once per generation;
/// scan outcomes for a `(query fingerprint, content hash)` are reused so a
/// repeated query never re-scans a distinct version it already resolved.
#[derive(Default)]
struct WarmQueryState {
    /// Bumped whenever the derived state changes (backfill, rebuild, retention
    /// wipe). Cached entries stamped with an older generation are stale.
    generation: u64,
    /// The parsed trigram index for `generation`, loaded lazily and shared
    /// across queries. The inner `Option` records that this generation has no
    /// index, so a query does not re-attempt the disk load every time.
    trigram: Option<(
        u64,
        std::sync::Arc<Option<super::grep_trigram::TrigramIndex>>,
    )>,
    /// Bounded `(fingerprint, content hash) -> scan outcome` cache. Repeated
    /// or overlapping queries reuse a distinct version's scan result without
    /// re-running `find`/enumeration. Bounded by entry count; oldest evicted.
    scans: BTreeMap<(String, String), ScanOutcome>,
    scan_order: VecDeque<(String, String)>,
    scan_bytes: usize,
    /// Bounded `(fingerprint, anchor capture) -> reduced lineage state` cache
    /// cache. A paged query's next page normally replays every capture up
    /// to its resume anchor with emission suppressed, only to rebuild this
    /// exact state. Caching it lets the daemon skip that replay; a miss —
    /// eviction, a new fingerprint, or a fresh process after restart — simply
    /// falls back to the authoritative replay, so the result is identical
    /// either way.
    cursor_states: BTreeMap<(String, String), CursorState>,
    cursor_order: VecDeque<(String, String)>,
    cursor_bytes: usize,
}

/// The replay-reconstructable reduction state at a cursor's anchor capture:
/// every lineage's live episodes plus which lineages have been seen. Cloning
/// it is cheap (a rare/absent query keeps `present` near-empty), and it is the
/// exact state the suppressed replay would rebuild.
#[derive(Clone, Default)]
struct CursorState {
    lineages: BTreeMap<String, UnitState>,
    seen_lineages: std::collections::BTreeSet<String>,
}

impl CursorState {
    fn approximate_owned_bytes(&self) -> usize {
        let lineage_bytes = self.lineages.iter().fold(0usize, |total, (name, state)| {
            let units = state.present.iter().fold(0usize, |units_total, unit| {
                units_total
                    .saturating_add(std::mem::size_of::<PresentUnit>())
                    .saturating_add(unit.path.capacity())
                    .saturating_add(unit.selected_sha256.capacity())
                    .saturating_add(unit.before_sha256.capacity())
                    .saturating_add(unit.after_sha256.capacity())
                    .saturating_add(unit.line_sha256.capacity())
                    .saturating_add(unit.handle_id.capacity())
                    .saturating_add(unit.episode_id.capacity())
            });
            total
                .saturating_add(std::mem::size_of::<(String, UnitState)>())
                .saturating_add(name.capacity())
                .saturating_add(units)
        });
        self.seen_lineages
            .iter()
            .fold(lineage_bytes, |total, name| {
                total
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(name.capacity())
            })
    }
}

fn cursor_cache_entry_bytes(key: &(String, String), state: &CursorState) -> usize {
    std::mem::size_of::<((String, String), CursorState)>()
        .saturating_add(key.0.capacity())
        .saturating_add(key.1.capacity())
        .saturating_add(state.approximate_owned_bytes())
}

/// Entry cap for the cursor-state cache. Each entry is one paged query's
/// anchor state; a handful of concurrently paged queries is the realistic
/// working set, so a small cap suffices and bounds memory.
const CURSOR_STATE_MAX_ENTRIES: usize = 256;
const CURSOR_STATE_MAX_BYTES: usize = 16 * 1024 * 1024;

/// Entry and approximate-owned-byte caps for the warm scan cache. Dense common
/// needles can retain tens of thousands of occurrence units in one outcome,
/// so an entry count alone is not a meaningful resident-memory bound. 32 MiB:
/// the sidecar serves cold identities from disk, so the warm set is pure
/// latency sugar — the daemon's resident ceiling matters more.
const WARM_SCAN_MAX_ENTRIES: usize = 1 << 16;
const WARM_SCAN_MAX_BYTES: usize = 32 * 1024 * 1024;

/// One warm in-memory row: the materialized content plus the content
/// identity the scanner needs without re-reading anything. `hash` is the
/// SHA-256 of text/binary content and `None` only for absent paths.
#[derive(Debug, Clone)]
struct CachedEntry {
    content: HistoricalPathContent,
    hash: Option<String>,
}

impl GrepContentCache {
    pub(super) fn open(root: &std::path::Path, writable: bool) -> Self {
        let index_dir = crate::config::sheaf_dir(root).join("state/cache/grep-v1");
        let mut cache = Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            mappings: BTreeMap::new(),
            index_dir,
            writable,
            watermark: None,
            next_generation: 0,
            torn_tail: false,
            warm: WarmQueryState::default(),
        };
        cache.load_mappings();
        cache.load_watermark();
        cache
    }

    fn mappings_path(&self) -> std::path::PathBuf {
        self.index_dir.join("mappings.jsonl")
    }

    fn watermark_path(&self) -> std::path::PathBuf {
        self.index_dir.join("watermark.json")
    }

    fn content_path(&self, hash: &str) -> std::path::PathBuf {
        self.index_dir.join("content").join(format!("{hash}.zst"))
    }

    /// (Re)build the trigram pre-filter index from every distinct text
    /// content blob currently in the sidecar. Derived-from-derived and
    /// disposable: callers treat a failure as an absent index so queries scan
    /// every distinct content. Returns the on-disk index size in bytes (0 when
    /// nothing was indexed).
    ///
    /// Building from the blobs rather than the walk keeps the index a pure
    /// function of the content cache: whatever text versions the mappings
    /// reference are exactly what a query can be filtered against, so the
    /// filter can never claim a content absent that the cache would serve.
    fn rebuild_trigram_index(&self) -> std::io::Result<u64> {
        if !self.writable {
            return Ok(super::grep_trigram::index_size(&self.index_dir));
        }
        // Only content hashes a live mapping still references are indexed, so
        // a blob orphaned by retention/GC never resurrects a candidate.
        let referenced: std::collections::BTreeSet<&str> = self
            .mappings
            .values()
            .filter_map(|value| match value {
                CacheMappingValue::Text { hash, .. } => Some(hash.as_str()),
                _ => None,
            })
            .collect();
        let mut builder = super::grep_trigram::TrigramBuilder::default();
        for hash in referenced {
            let Ok(compressed) = std::fs::read(self.content_path(hash)) else {
                continue;
            };
            let Ok(text) = zstd::stream::decode_all(&compressed[..]) else {
                continue;
            };
            // Verify before admitting the hash to the index's covered set.
            // A decodable blob with wrong bytes is not merely a false-positive
            // risk: postings extracted from those bytes could prove the named
            // hash absent for a trigram its authoritative content actually
            // contains, letting the filter skip the read that would detect the
            // mismatch. Unverified blobs therefore remain uncovered and are
            // always scanned by queries.
            if sha256_hex(&text) != hash {
                continue;
            }
            builder.add(hash, &text);
        }
        if builder.distinct_contents() == 0 {
            super::grep_trigram::remove_index(&self.index_dir);
            return Ok(0);
        }
        super::grep_trigram::store_index(&self.index_dir, &builder)
    }

    /// The daemon-resident parsed trigram index for the current generation,
    /// loading it from disk (decompress + parse) at most once per generation.
    /// Repeated queries share the parse instead of re-reading the file each
    /// time — the warm path's main trigram cost. Returns an `Arc` to an
    /// `Option`: the inner `None` is "this generation has no index".
    fn resident_trigram_index(
        &mut self,
    ) -> std::sync::Arc<Option<super::grep_trigram::TrigramIndex>> {
        let generation = self.warm.generation;
        if let Some((gen, index)) = &self.warm.trigram {
            if *gen == generation {
                return index.clone();
            }
        }
        let loaded = std::sync::Arc::new(super::grep_trigram::load_index(&self.index_dir));
        self.warm.trigram = Some((generation, loaded.clone()));
        loaded
    }

    /// The trigram pre-filter for `needle`: candidate hashes plus the resident
    /// index used for coverage membership. `None` means no filter applies
    /// (short needle or absent/corrupt index). Sharing the resident index avoids
    /// cloning its full covered-hash set for every query.
    fn trigram_filter(&mut self, needle: &str) -> Option<TrigramFilter> {
        let index = self.resident_trigram_index();
        let candidates = index.as_ref().as_ref()?.candidates(needle.as_bytes())?;
        Some(TrigramFilter { candidates, index })
    }

    /// A warm scan outcome for `(fingerprint, hash)`, if this query shape has
    /// already resolved these exact bytes in an earlier query. The clone is
    /// the occurrence-unit vector, avoiding a re-scan of the content.
    fn warm_scan_get(&self, fingerprint: &str, hash: &str) -> Option<ScanOutcome> {
        self.warm
            .scans
            .get(&(fingerprint.to_owned(), hash.to_owned()))
            .cloned()
    }

    /// Remember a scan outcome across queries, bounded by entry count and
    /// approximate owned bytes. Oversized single outcomes are not cached.
    fn warm_scan_put(&mut self, fingerprint: &str, hash: &str, outcome: &ScanOutcome) {
        let key = (fingerprint.to_owned(), hash.to_owned());
        if self.warm.scans.contains_key(&key) {
            return;
        }
        let entry_bytes = scan_cache_entry_bytes(&key, outcome);
        if entry_bytes > WARM_SCAN_MAX_BYTES {
            return;
        }
        while self.warm.scans.len() >= WARM_SCAN_MAX_ENTRIES
            || self.warm.scan_bytes.saturating_add(entry_bytes) > WARM_SCAN_MAX_BYTES
        {
            let Some(oldest) = self.warm.scan_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.warm.scans.remove(&oldest) {
                self.warm.scan_bytes = self
                    .warm
                    .scan_bytes
                    .saturating_sub(scan_cache_entry_bytes(&oldest, &removed));
            }
        }
        self.warm.scan_bytes = self.warm.scan_bytes.saturating_add(entry_bytes);
        self.warm.scans.insert(key.clone(), outcome.clone());
        self.warm.scan_order.push_back(key);
    }

    /// The cached reduction state at a cursor anchor for `(fingerprint,
    /// anchor)`, if the daemon still holds it. A hit lets the next page skip
    /// its suppressed replay; a miss falls back to replay.
    fn cursor_state_get(&self, fingerprint: &str, anchor: &str) -> Option<CursorState> {
        self.warm
            .cursor_states
            .get(&(fingerprint.to_owned(), anchor.to_owned()))
            .cloned()
    }

    /// Remember the reduction state at a page boundary so the next page can
    /// resume without replay. Bounded by entry count and approximate owned
    /// bytes; oldest entries are evicted and an oversized state is skipped.
    fn cursor_state_put(&mut self, fingerprint: &str, anchor: &str, state: CursorState) {
        let key = (fingerprint.to_owned(), anchor.to_owned());
        if self.warm.cursor_states.contains_key(&key) {
            // The state at an immutable anchor and fingerprint is deterministic.
            return;
        }
        let entry_bytes = cursor_cache_entry_bytes(&key, &state);
        if entry_bytes > CURSOR_STATE_MAX_BYTES {
            return;
        }
        while self.warm.cursor_states.len() >= CURSOR_STATE_MAX_ENTRIES
            || self.warm.cursor_bytes.saturating_add(entry_bytes) > CURSOR_STATE_MAX_BYTES
        {
            let Some(oldest) = self.warm.cursor_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.warm.cursor_states.remove(&oldest) {
                self.warm.cursor_bytes = self
                    .warm
                    .cursor_bytes
                    .saturating_sub(cursor_cache_entry_bytes(&oldest, &removed));
            }
        }
        self.warm.cursor_bytes = self.warm.cursor_bytes.saturating_add(entry_bytes);
        self.warm.cursor_states.insert(key.clone(), state);
        self.warm.cursor_order.push_back(key);
    }

    /// Invalidate all warm state (a generation bump). Called after any change
    /// to the derived sidecar so a query never reuses a stale scan or index.
    fn bump_warm_generation(&mut self) {
        self.warm.generation = self.warm.generation.wrapping_add(1);
        self.warm.trigram = None;
        self.warm.scans.clear();
        self.warm.scan_order.clear();
        self.warm.scan_bytes = 0;
        self.invalidate_cursor_states();
    }

    /// Invalidate only cursor reduction states after the timeline walk's shape
    /// or lineage attribution changes (capture append or head reposition).
    /// Hash-keyed scan outcomes and the resident trigram index remain valid.
    pub(super) fn invalidate_cursor_states(&mut self) {
        self.warm.cursor_states.clear();
        self.warm.cursor_order.clear();
        self.warm.cursor_bytes = 0;
    }

    fn load_mappings(&mut self) {
        let Ok(bytes) = std::fs::read(self.mappings_path()) else {
            return;
        };
        self.torn_tail = !bytes.is_empty() && bytes.last() != Some(&b'\n');
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(record) = serde_json::from_slice::<CacheMapping>(line) else {
                continue;
            };
            if record.v != GREP_CACHE_SCHEMA || record.frontier.is_empty() || record.path.is_empty()
            {
                continue;
            }
            self.mappings
                .insert((record.frontier, record.path), record.value);
        }
    }

    fn load_watermark(&mut self) {
        let Ok(raw) = std::fs::read_to_string(self.watermark_path()) else {
            return;
        };
        let Ok(watermark) = serde_json::from_str::<GrepCacheWatermark>(&raw) else {
            // A corrupt watermark is a lagging watermark: ignore it and let
            // the next backfill rewrite it from row-level completeness.
            tracing::warn!("timeline grep cache watermark unparseable; ignoring");
            return;
        };
        if watermark.v != GREP_CACHE_SCHEMA {
            return;
        }
        self.next_generation = watermark.generation;
        self.watermark = Some(watermark);
    }

    fn store_watermark(&self, watermark: &GrepCacheWatermark) -> std::io::Result<()> {
        let raw = serde_json::to_vec(watermark).map_err(std::io::Error::other)?;
        super::fsutil::atomic_write(&self.watermark_path(), &raw)
    }

    fn entry_bytes(key: &ContentCacheKey, value: &HistoricalPathContent) -> usize {
        let content = match value {
            HistoricalPathContent::Text(text) => text.len(),
            HistoricalPathContent::Binary { hash, .. } => hash.len() + 16,
            HistoricalPathContent::Absent => 1,
        };
        key.0.len() + key.1.len() + content
    }

    fn remember(
        &mut self,
        key: ContentCacheKey,
        value: HistoricalPathContent,
        hash: Option<String>,
    ) {
        if self.entries.contains_key(&key) {
            return;
        }
        let bytes = Self::entry_bytes(&key, &value);
        if bytes > GREP_CONTENT_CACHE_BYTES {
            return;
        }
        while self.bytes.saturating_add(bytes) > GREP_CONTENT_CACHE_BYTES {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes = self
                    .bytes
                    .saturating_sub(Self::entry_bytes(&oldest, &old.content));
            }
        }
        self.bytes += bytes;
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            CachedEntry {
                content: value,
                hash,
            },
        );
    }

    /// Content identity for a path visit without loading anything: the
    /// warm entry's hash, else the mapping row's. This is the
    /// content-dedup seam — when the query's scan memo already holds the
    /// outcome for this identity, the visit never decompresses or forks.
    ///
    /// Trust equals a disk hit, never more: a text row is only peeksable
    /// when its blob exists (one stat, no decompression), so a row whose
    /// content vanished falls through to the full read and its
    /// authoritative fallback instead of borrowing an outcome from a
    /// hash that merely matches elsewhere. Absent paths and unknown keys
    /// return `None`.
    fn peek_hash(&self, frontier: &str, path: &str) -> Option<String> {
        let key = &(frontier.to_owned(), path.to_owned());
        if let Some(entry) = self.entries.get(key) {
            return entry.hash.clone();
        }
        match self.mappings.get(key)? {
            CacheMappingValue::Text { hash, .. } => {
                self.content_path(hash).is_file().then(|| hash.clone())
            }
            CacheMappingValue::Binary { hash, .. } => Some(hash.clone()),
            CacheMappingValue::Absent => None,
        }
    }

    /// The mapping-recorded content hash for a text path visit, WITHOUT the
    /// blob-existence stat `peek_hash` performs. Used only for the trigram
    /// pre-filter's exclusion test, where the stat is unnecessary: excluding
    /// a content means its recorded hash is absent from the needle's
    /// candidate set, which is a property of the hash's trigrams alone and
    /// holds whether or not the blob is still on disk. Returns `None` for
    /// absent/binary/unknown paths (those never exclude). Avoiding the stat
    /// turns the per-capture skip from a syscall into a map lookup, which is
    /// what makes a rare needle's walk fast on a large store.
    fn peek_text_hash_unverified(&self, frontier: &str, path: &str) -> Option<String> {
        let key = &(frontier.to_owned(), path.to_owned());
        if let Some(entry) = self.entries.get(key) {
            return entry.hash.clone();
        }
        match self.mappings.get(key)? {
            CacheMappingValue::Text { hash, .. } => Some(hash.clone()),
            CacheMappingValue::Binary { .. } | CacheMappingValue::Absent => None,
        }
    }

    fn get(
        &mut self,
        frontier: &str,
        path: &str,
    ) -> Option<(HistoricalPathContent, CacheHit, Option<String>)> {
        let key = (frontier.to_owned(), path.to_owned());
        if let Some(entry) = self.entries.get(&key).cloned() {
            return Some((entry.content, CacheHit::Memory, entry.hash));
        }
        let mapping = self.mappings.get(&key)?.clone();
        let Some(content) = self.load_mapping(&mapping) else {
            // A stale/corrupt row becomes an ordinary miss. A writable caller
            // republishes the authoritative bytes after its fallback read.
            self.mappings.remove(&key);
            return None;
        };
        let hash = match &mapping {
            CacheMappingValue::Text { hash, .. } | CacheMappingValue::Binary { hash, .. } => {
                Some(hash.clone())
            }
            CacheMappingValue::Absent => None,
        };
        self.remember(key, content.clone(), hash.clone());
        Some((content, CacheHit::Disk, hash))
    }

    fn load_mapping(&self, mapping: &CacheMappingValue) -> Option<HistoricalPathContent> {
        match mapping {
            CacheMappingValue::Absent => Some(HistoricalPathContent::Absent),
            CacheMappingValue::Binary { hash, bytes } => Some(HistoricalPathContent::Binary {
                hash: hash.clone(),
                bytes: *bytes,
            }),
            CacheMappingValue::Text { hash, bytes } => {
                if *bytes > super::TEXT_MAX_BYTES {
                    return None;
                }
                let compressed = std::fs::read(self.content_path(hash)).ok()?;
                let decoder = zstd::stream::read::Decoder::new(&compressed[..]).ok()?;
                let mut decoded = Vec::with_capacity(*bytes as usize);
                use std::io::Read as _;
                decoder.take(*bytes + 1).read_to_end(&mut decoded).ok()?;
                if decoded.len() as u64 != *bytes || sha256_hex(&decoded) != *hash {
                    return None;
                }
                Some(HistoricalPathContent::Text(
                    String::from_utf8(decoded).ok()?,
                ))
            }
        }
    }

    fn mapping_for(value: &HistoricalPathContent) -> CacheMappingValue {
        match value {
            HistoricalPathContent::Text(text) => CacheMappingValue::Text {
                hash: sha256_hex(text.as_bytes()),
                bytes: text.len() as u64,
            },
            HistoricalPathContent::Binary { hash, bytes } => CacheMappingValue::Binary {
                hash: hash.clone(),
                bytes: *bytes,
            },
            HistoricalPathContent::Absent => CacheMappingValue::Absent,
        }
    }

    fn insert(&mut self, frontier: &str, path: &str, value: HistoricalPathContent) {
        self.publish_rows(vec![((frontier.to_owned(), path.to_owned()), value)]);
    }

    /// Publish pre-materialized rows: content first, then one mappings
    /// batch. Returns `(rows_written, content_blobs_written)`. A crash
    /// between the two leaves an unreferenced blob — a miss, repaired by
    /// the next publish or a rebuild; a torn mappings tail is skipped on
    /// load and separated from the next append.
    fn publish_rows(
        &mut self,
        rows: Vec<(ContentCacheKey, HistoricalPathContent)>,
    ) -> (usize, usize) {
        let mut pending = Vec::new();
        let mut blobs_written = 0usize;
        for (key, value) in rows {
            let mapping = Self::mapping_for(&value);
            let hash = match &mapping {
                CacheMappingValue::Text { hash, .. } | CacheMappingValue::Binary { hash, .. } => {
                    Some(hash.clone())
                }
                CacheMappingValue::Absent => None,
            };
            self.remember(key.clone(), value.clone(), hash);
            if !self.writable {
                continue;
            }
            if self.mappings.get(&key) == Some(&mapping) {
                continue;
            }
            match self.persist_content(&mapping, &value) {
                Ok(wrote) => blobs_written += wrote as usize,
                Err(error) => {
                    tracing::warn!(%error, path = key.1, "timeline grep content publish skipped");
                    continue;
                }
            }
            pending.push((key, mapping));
        }
        if pending.is_empty() {
            return (0, blobs_written);
        }
        if let Err(error) = self.append_mappings(&pending) {
            tracing::warn!(%error, "timeline grep mappings publish skipped");
            return (0, blobs_written);
        }
        let written = pending.len();
        self.mappings.extend(pending);
        (written, blobs_written)
    }

    /// Publish `content` under `mapping`, returning whether a new content
    /// blob was written (existing blobs are left untouched).
    fn persist_content(
        &self,
        mapping: &CacheMappingValue,
        value: &HistoricalPathContent,
    ) -> std::io::Result<bool> {
        if let (CacheMappingValue::Text { hash, .. }, HistoricalPathContent::Text(text)) =
            (mapping, value)
        {
            let path = self.content_path(hash);
            if !path.is_file() {
                let compressed = zstd::stream::encode_all(
                    std::io::Cursor::new(text.as_bytes()),
                    GREP_CACHE_ZSTD_LEVEL,
                )?;
                super::fsutil::atomic_write(&path, &compressed)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn append_mappings(
        &mut self,
        mappings: &[(ContentCacheKey, CacheMappingValue)],
    ) -> std::io::Result<()> {
        if mappings.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.index_dir)?;
        let mut payload = Vec::new();
        // A torn final line must not swallow the first fresh record: the
        // fragment has no newline, so without this separator the appended
        // record would parse as part of the junk and be lost on load.
        if self.torn_tail {
            payload.push(b'\n');
        }
        for (key, value) in mappings {
            let record = CacheMapping {
                v: GREP_CACHE_SCHEMA,
                frontier: key.0.clone(),
                path: key.1.clone(),
                value: value.clone(),
            };
            serde_json::to_writer(&mut payload, &record).map_err(std::io::Error::other)?;
            payload.push(b'\n');
        }
        use std::io::Write as _;
        let mappings_path = self.mappings_path();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&mappings_path)?;
        file.write_all(&payload)?;
        file.sync_data()?;
        super::fsutil::sync_parent_dir(&mappings_path)?;
        self.torn_tail = false;
        Ok(())
    }

    /// Wipe the cache and bump the generation for the next watermark. The
    /// directory is disposable performance state; removing it can never
    /// affect timeline or worktree bytes.
    fn wipe(&mut self) {
        let next = self
            .watermark
            .as_ref()
            .map_or(self.next_generation, |w| w.generation + 1)
            .max(self.next_generation);
        match std::fs::remove_dir_all(&self.index_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                %error,
                "could not remove disposable timeline grep cache"
            ),
        }
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
        self.mappings.clear();
        self.watermark = None;
        self.torn_tail = false;
        self.next_generation = next;
        self.bump_warm_generation();
    }

    pub(super) fn invalidate_after_retention(&mut self) {
        // A trim changes which captures exist at all, so the whole cache
        // generation restarts (rows for collected captures would
        // otherwise linger as unreachable junk).
        self.wipe();
    }

    /// Targeted retention sweep: keep only mappings whose
    /// frontier is still reachable after a trim, then mark-sweep content blobs
    /// no surviving mapping references and rebuild the trigram index over the
    /// retained corpus. Protected points and branch tips stay searchable
    /// because their frontiers are in `retained`; only collected captures'
    /// rows are removed. Everything here is derived state, so a partial
    /// failure at worst leaves an orphan blob (a miss, swept next time) and
    /// never affects timeline bytes.
    pub(super) fn sweep_to_retained(&mut self, retained: &std::collections::BTreeSet<String>) {
        if !self.writable {
            return;
        }
        // A trim starts a new coverage generation even when no cached mapping
        // names the collected point: the old watermark describes a different
        // timeline shape and must never be reused as the new chain's identity.
        let next_generation = self
            .watermark
            .as_ref()
            .map_or_else(
                || self.next_generation.saturating_add(1),
                |w| w.generation.saturating_add(1),
            )
            .max(self.next_generation.saturating_add(1));

        // Drop in-memory rows and warm entries for collected frontiers.
        let collected: Vec<ContentCacheKey> = self
            .mappings
            .keys()
            .filter(|(frontier, _)| !retained.contains(frontier))
            .cloned()
            .collect();
        if !collected.is_empty() {
            for key in &collected {
                self.mappings.remove(key);
                if let Some(entry) = self.entries.remove(key) {
                    self.bytes = self
                        .bytes
                        .saturating_sub(Self::entry_bytes(key, &entry.content));
                }
            }
            self.order.retain(|key| self.entries.contains_key(key));

            // Rewrite mappings.jsonl as the compacted survivor set (atomic
            // replace, so a crash leaves either the old or the new whole file).
            if let Err(error) = self.rewrite_mappings() {
                tracing::warn!(%error, "grep cache retention sweep could not rewrite mappings; full wipe");
                self.wipe();
                return;
            }
        }

        // Mark-sweep content blobs: delete any blob no surviving text mapping
        // references. Postings are rebuilt below over exactly these blobs.
        let referenced: std::collections::BTreeSet<String> = self
            .mappings
            .values()
            .filter_map(|value| match value {
                CacheMappingValue::Text { hash, .. } => Some(hash.clone()),
                _ => None,
            })
            .collect();
        match std::fs::read_dir(self.index_dir.join("content")) {
            Ok(rd) => {
                for file in rd {
                    let file = match file {
                        Ok(file) => file,
                        Err(error) => {
                            tracing::warn!(%error, "grep cache retention sweep could not enumerate content; full wipe");
                            self.wipe();
                            return;
                        }
                    };
                    let name = file.file_name().to_string_lossy().into_owned();
                    let hash = name.strip_suffix(".zst").unwrap_or(&name);
                    if !referenced.contains(hash) {
                        if let Err(error) = std::fs::remove_file(file.path()) {
                            tracing::warn!(%error, "grep cache retention sweep could not remove orphan content; full wipe");
                            self.wipe();
                            return;
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(%error, "grep cache retention sweep could not read content directory; full wipe");
                self.wipe();
                return;
            }
        }

        // The watermark may name a collected capture; a trim always restarts
        // the coverage chain at a distinguishable generation.
        self.watermark = None;
        self.next_generation = next_generation;
        match std::fs::remove_file(self.watermark_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(%error, "grep cache retention sweep could not remove watermark; full wipe");
                self.wipe();
                return;
            }
        }

        // Rebuild the trigram index over the retained corpus. A failed index
        // publication makes the targeted sweep incomplete, so fall back to a
        // full derived-cache wipe rather than retaining an old coverage set.
        if let Err(error) = self.rebuild_trigram_index() {
            tracing::warn!(%error, "grep cache retention sweep could not rebuild trigram index; full wipe");
            self.wipe();
            return;
        }
        self.bump_warm_generation();
    }

    /// Rewrite `mappings.jsonl` from the current in-memory survivor set as one
    /// atomic replacement. Used by the retention sweep to physically drop
    /// collected rows.
    fn rewrite_mappings(&mut self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.index_dir)?;
        let mut payload = Vec::new();
        for (key, value) in &self.mappings {
            let record = CacheMapping {
                v: GREP_CACHE_SCHEMA,
                frontier: key.0.clone(),
                path: key.1.clone(),
                value: value.clone(),
            };
            serde_json::to_writer(&mut payload, &record).map_err(std::io::Error::other)?;
            payload.push(b'\n');
        }
        super::fsutil::atomic_write(&self.mappings_path(), &payload)?;
        self.torn_tail = false;
        Ok(())
    }

    /// True when every path this capture touched has a durable mapping
    /// row — the same completeness capture-time indexing guarantees.
    fn capture_is_indexed(&self, capture: &Capture) -> bool {
        capture.paths.iter().all(|path| {
            self.mappings
                .contains_key(&(capture.frontier.clone(), path.clone()))
        })
    }

    /// A live-daemon capture extends the coverage chain only when the
    /// watermark already covers its parent (or it is the genesis capture
    /// — a parent naming zero changes — and nothing is covered yet).
    /// Otherwise older captures are missing and the prefix is not
    /// complete; an explicit backfill establishes the chain. A lagging
    /// watermark is repaired by the next backfill.
    fn advance_watermark_after_capture(&mut self, capture: &Capture) {
        let chain_extends = match &self.watermark {
            Some(wm) => wm.through_frontier == capture.parent_frontier,
            None => decode_frontier(&capture.parent_frontier)
                .map(|parent| parent.iter().next().is_none())
                .unwrap_or(false),
        };
        if !chain_extends {
            return;
        }
        let watermark = GrepCacheWatermark {
            v: GREP_CACHE_SCHEMA,
            generation: self.next_generation,
            captures_indexed: self
                .watermark
                .as_ref()
                .map_or(1, |w| w.captures_indexed + 1),
            through_capture_id: capture.id.clone(),
            through_frontier: capture.frontier.clone(),
            updated_ms: chrono::Utc::now().timestamp_millis(),
        };
        if let Err(error) = self.store_watermark(&watermark) {
            tracing::warn!(%error, "timeline grep watermark write failed; backfill repairs it");
            return;
        }
        self.watermark = Some(watermark);
    }

    pub(super) fn index_capture(&mut self, doc: &LoroDoc, capture: &Capture) {
        let rows = capture
            .paths
            .iter()
            .map(|path| match current_path_content(doc, path) {
                Ok(content) => Some(((capture.frontier.clone(), path.clone()), content)),
                Err(error) => {
                    tracing::warn!(%error, path, "timeline grep capture indexing skipped");
                    None
                }
            })
            .collect::<Vec<_>>();
        let rows: Vec<_> = rows.into_iter().flatten().collect();
        let (written, _) = self.publish_rows(rows);
        if written == 0 && !self.capture_is_indexed(capture) {
            return;
        }
        self.advance_watermark_after_capture(capture);
    }
}

fn current_path_content(doc: &LoroDoc, key: &str) -> Result<HistoricalPathContent> {
    if let Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) =
        doc.get_map(super::FILES_MAP).get(key)
    {
        return Ok(HistoricalPathContent::Text(text.to_string()));
    }
    if let Some(value) = doc.get_map(super::BINARIES_MAP).get(key) {
        let raw = value.get_deep_value().into_string().map_err(|_| {
            SheafError::StoreCorrupt(format!("binary metadata for `{key}` is not text"))
        })?;
        let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|error| {
            SheafError::StoreCorrupt(format!("binary metadata for `{key}`: {error}"))
        })?;
        let hash = parsed["hash"].as_str().ok_or_else(|| {
            SheafError::StoreCorrupt(format!("binary metadata for `{key}` has no hash"))
        })?;
        return Ok(HistoricalPathContent::Binary {
            hash: hash.to_owned(),
            bytes: parsed["size"].as_u64().unwrap_or(0),
        });
    }
    Ok(HistoricalPathContent::Absent)
}

/// The document/ledger pair a grep runs against. Both the writer store and the
/// degraded reader expose one, so the engine is written once.
struct GrepSource<'a> {
    doc: &'a LoroDoc,
    ledger: &'a super::ledger::LedgerState,
    current: Frontiers,
    warm_content: Option<&'a RefCell<GrepContentCache>>,
    /// The trigram pre-filter for this query's needle, computed once. `None`
    /// means no filter applies (short needle, or absent/corrupt index) and
    /// every content is a candidate.
    trigram: Option<TrigramFilter>,
}

/// The precomputed trigram decision for one query's needle: the candidate set
/// plus the set of hashes the index actually covers. A visit is excluded only
/// when its hash is covered AND is not a candidate — a hash the index never
/// saw (freshly captured content indexed after the last rebuild) is never
/// excluded, so a stale index can only over-scan, never drop a hit.
struct TrigramFilter {
    candidates: std::collections::BTreeSet<String>,
    index: std::sync::Arc<Option<super::grep_trigram::TrigramIndex>>,
}

impl GrepSource<'_> {
    /// When the trigram filter proves a text path visit cannot contain the
    /// needle, its mapping-recorded content hash — so the caller records
    /// `Absent` for that hash with no read, fork, or scan. `None` when the
    /// filter does not apply, the content is a candidate, the index did not
    /// cover this content, or the path has no unverified text hash. Uses the
    /// stat-free hash lookup because exclusion is a property of the hash's
    /// trigrams, independent of whether the blob is still on disk.
    fn trigram_excluded_hash(&self, frontier: &str, path: &str) -> Option<String> {
        let filter = self.trigram.as_ref()?;
        let cache = self.warm_content?;
        let hash = cache.borrow().peek_text_hash_unverified(frontier, path)?;
        // Only exclude content the index actually covers; never exclude a hash
        // the index has not seen (it would drop a real match in new content).
        (filter
            .index
            .as_ref()
            .as_ref()
            .is_some_and(|index| index.covers(&hash))
            && !filter.candidates.contains(&hash))
        .then_some(hash)
    }

    /// A warm cross-query scan outcome for `(fingerprint, hash)`, when present.
    fn warm_scan_get(&self, fingerprint: &str, hash: &str) -> Option<ScanOutcome> {
        self.warm_content?.borrow().warm_scan_get(fingerprint, hash)
    }

    /// Publish a scan outcome to the warm cross-query cache. A degraded reader
    /// is normally one-shot, so its state simply dies with that reader.
    fn warm_scan_put(&self, fingerprint: &str, hash: &str, outcome: &ScanOutcome) {
        if let Some(cache) = self.warm_content {
            cache.borrow_mut().warm_scan_put(fingerprint, hash, outcome);
        }
    }

    /// Cached reduction state at a cursor anchor, when the daemon holds it.
    fn cursor_state_get(&self, fingerprint: &str, anchor: &str) -> Option<CursorState> {
        self.warm_content?
            .borrow()
            .cursor_state_get(fingerprint, anchor)
    }

    /// Remember reduction state at a page boundary for the next page's resume.
    fn cursor_state_put(&self, fingerprint: &str, anchor: &str, state: CursorState) {
        if let Some(cache) = self.warm_content {
            cache
                .borrow_mut()
                .cursor_state_put(fingerprint, anchor, state);
        }
    }
}

fn hit_kind_is_present(kind: LifecycleKind) -> bool {
    matches!(
        kind,
        LifecycleKind::Present
            | LifecycleKind::Introduced
            | LifecycleKind::Changed
            | LifecycleKind::Relocated
            | LifecycleKind::Renamed
            | LifecycleKind::Moved
            | LifecycleKind::Reintroduced
            | LifecycleKind::Observed
    )
}

impl<'a> GrepSource<'a> {
    /// Chronological (oldest-first) captures on the requested lineage set,
    /// path-pruned and rename-followed exactly like `timeline.log`.
    fn points(&self, req: &GrepRequest) -> Result<Vec<Point>> {
        use super::timeline::{captures_from, path_names};
        let start = if req.all {
            self.doc.oplog_frontiers()
        } else {
            self.current.clone()
        };
        let path = req.path.as_deref().map(std::path::Path::new);
        let names = path.map(|p| path_names(self.doc, p)).filter(|_| req.follow);
        let mut captures = captures_from(
            self.doc,
            self.ledger,
            &start,
            path,
            names.as_deref(),
            usize::MAX,
        )?;
        // Current-lineage frontiers decide branch membership under `--all`.
        let current_set: std::collections::BTreeSet<String> = if req.all {
            captures_from(self.doc, self.ledger, &self.current, None, None, usize::MAX)?
                .into_iter()
                .map(|c| c.frontier)
                .collect()
        } else {
            captures.iter().map(|c| c.frontier.clone()).collect()
        };
        // For `--all`, every divergent capture is attributed to the branch tip
        // it is reachable from (smallest tip frontier on ties), so all captures
        // of one abandoned future share one lineage id and collapse correctly
        // instead of each looking like a fresh introduction.
        let branch_membership: Vec<(String, std::collections::BTreeSet<String>)> = if req.all {
            let mut sets = Vec::new();
            for tip in super::timeline::branch_tips_from(self.doc)? {
                if tip.frontier.is_empty() {
                    continue;
                }
                let Ok(frontier) = decode_frontier(&tip.frontier) else {
                    continue;
                };
                let reachable: std::collections::BTreeSet<String> =
                    captures_from(self.doc, self.ledger, &frontier, None, None, usize::MAX)?
                        .into_iter()
                        .map(|c| c.frontier)
                        .collect();
                sets.push((tip.frontier, reachable));
            }
            sets.sort_by(|a, b| a.0.cmp(&b.0));
            sets
        } else {
            Vec::new()
        };
        // The walk yields newest-first; grep reasons forward in time.
        captures.reverse();
        // Attribute lineage and episode key over the UNFILTERED capture list:
        // a branch's root capture is its earliest attributed capture whatever
        // range this particular query walks, so episode IDs do not shift when
        // the interval changes.
        let mut lineage_of: std::collections::HashMap<String, (String, String, bool)> =
            std::collections::HashMap::new();
        let mut roots: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for capture in &captures {
            let on_current = current_set.contains(&capture.frontier);
            let lineage_id = if on_current {
                "current".to_owned()
            } else {
                branch_membership
                    .iter()
                    .find(|(_, set)| set.contains(&capture.frontier))
                    .map(|(tip, _)| format!("branch:{tip}"))
                    .unwrap_or_else(|| format!("branch:{}", capture.frontier))
            };
            let episode_lineage = if on_current {
                "current".to_owned()
            } else {
                let root = roots
                    .entry(lineage_id.clone())
                    .or_insert_with(|| capture.id.clone());
                format!("root:{root}")
            };
            lineage_of.insert(
                capture.frontier.clone(),
                (lineage_id, episode_lineage, on_current),
            );
        }
        let (from_ms, to_ms) = self.range_bounds(req)?;
        let mut points = Vec::new();
        for capture in captures {
            if capture.timestamp_ms <= from_ms || capture.timestamp_ms > to_ms {
                continue;
            }
            let (lineage_id, episode_lineage, on_current) = lineage_of
                .get(&capture.frontier)
                .cloned()
                .unwrap_or_else(|| {
                    (
                        "current".to_owned(),
                        "current".to_owned(),
                        current_set.contains(&capture.frontier),
                    )
                });
            points.push(Point {
                capture,
                lineage_id,
                on_current,
                pruned: false,
                episode_lineage,
            });
        }
        let scope = req.path.as_deref().map(normalize_key);
        for (capture_id, tomb) in &self.ledger.tombstones {
            if tomb.at_ms <= from_ms || tomb.at_ms > to_ms {
                continue;
            }
            if let Some(scope) = &scope {
                if !tomb
                    .paths
                    .iter()
                    .any(|p| p == scope || p.starts_with(&format!("{scope}/")))
                {
                    continue;
                }
            }
            // Retention is prefix compaction, so a pruned capture's parent is
            // usually itself pruned (or the genesis, whose parent frontier is
            // empty). Such a gap is a mainline prefix trim and belongs on
            // `current`. Only attribute to a divergent branch when the parent
            // is a live frontier reachable from a branch tip but not from
            // current — the sole case where a gap provably sat off the trunk.
            let parent = tomb.parent_frontier.as_deref().filter(|p| !p.is_empty());
            let branch = parent.and_then(|p| {
                if current_set.contains(p) {
                    return None;
                }
                branch_membership
                    .iter()
                    .find(|(_, set)| set.contains(p))
                    .map(|(tip, _)| format!("branch:{tip}"))
            });
            let (lineage_id, on_current) = match branch {
                Some(tip) => (tip, false),
                None => ("current".to_owned(), true),
            };
            points.push(Point {
                capture: Capture {
                    id: capture_id.clone(),
                    frontier: String::new(),
                    parent_frontier: parent.unwrap_or_default().to_owned(),
                    timestamp_ms: tomb.at_ms,
                    paths: tomb.paths.clone(),
                    events: tomb.events,
                    checkpoints: Vec::new(),
                    origin: None,
                    on_current,
                },
                lineage_id: lineage_id.clone(),
                on_current,
                pruned: true,
                // A gap inherits the lineage's episode key; if attribution
                // left no rooted captures (everything already pruned), the
                // display lineage still keys the episode deterministically.
                episode_lineage: if on_current {
                    "current".to_owned()
                } else {
                    roots
                        .get(&lineage_id)
                        .map(|root| format!("root:{root}"))
                        .unwrap_or(lineage_id)
                },
            });
        }
        // Normative total record order: lineage key first, then
        // chronological position inside each lineage. Gaps sort before a
        // real capture sharing their millisecond so the gap terminates
        // continuity before the surviving capture re-introduces anything.
        points.sort_by(|a, b| {
            a.lineage_id
                .cmp(&b.lineage_id)
                .then_with(|| a.capture.timestamp_ms.cmp(&b.capture.timestamp_ms))
                .then_with(|| b.pruned.cmp(&a.pruned))
                .then(a.capture.id.cmp(&b.capture.id))
        });
        Ok(points)
    }

    fn range_bounds(&self, req: &GrepRequest) -> Result<(i64, i64)> {
        let from_ms = match &req.from {
            Some(reference) => self.resolve_ms(reference)?,
            None => i64::MIN,
        };
        let to_ms = match &req.to {
            Some(reference) => self.resolve_ms(reference)?,
            None => i64::MAX,
        };
        Ok((from_ms, to_ms))
    }

    fn resolve_ms(&self, reference: &str) -> Result<i64> {
        use super::timeline::{capture_at_frontier, resolve_in_doc};
        let point = resolve_in_doc(self.doc, self.ledger, &self.current, reference)?;
        let frontier = decode_frontier(&point.frontier)?;
        capture_at_frontier(self.doc, &frontier)
            .map(|c| c.timestamp_ms)
            .ok_or_else(|| {
                SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
            })
    }

    /// One path's content at one frontier without materializing the tree.
    /// The returned identity is the content SHA-256 when the cache row or
    /// the materialized bytes establish it — free on hits (rows are
    /// hash-verified on load), one digest on a fork materialization.
    fn path_at(
        &self,
        view: &mut HistoryView<'_>,
        frontier: &str,
        key: &str,
    ) -> Result<(HistoricalPathContent, CacheHit, Option<String>)> {
        if let Some(cache) = self.warm_content {
            if let Some(hit) = cache.borrow_mut().get(frontier, key) {
                return Ok(hit);
            }
        }
        let decoded = decode_frontier(frontier)?;
        if self.doc.frontiers_to_vv(&decoded).is_none() {
            return Err(SheafError::TimelineReference(
                "grep point is not part of this store's history".into(),
            ));
        }
        let content = view.path_at(&decoded, key)?;
        let hash = match &content {
            HistoricalPathContent::Text(text) => Some(sha256_hex(text.as_bytes())),
            HistoricalPathContent::Binary { hash, .. } => Some(hash.clone()),
            HistoricalPathContent::Absent => None,
        };
        if let Some(cache) = self.warm_content {
            cache.borrow_mut().insert(frontier, key, content.clone());
        }
        Ok((content, CacheHit::Miss, hash))
    }

    /// Content identity for a visit without loading content — the dedup
    /// probe. Never materializes: a miss here simply falls through to the
    /// full read.
    fn peek_content_hash(&self, frontier: &str, key: &str) -> Option<String> {
        self.warm_content
            .as_ref()
            .and_then(|cache| cache.borrow().peek_hash(frontier, key))
    }

    /// Every tracked text path present at a frontier, for an unscoped query.
    fn text_paths_at(&self, view: &mut HistoryView<'_>, frontier: &str) -> Result<Vec<String>> {
        let frontier = decode_frontier(frontier)?;
        if self.doc.frontiers_to_vv(&frontier).is_none() {
            return Ok(Vec::new());
        }
        view.text_keys_at(&frontier)
    }

    /// Every name a scoped path wore across recorded renames (prefix-aware,
    /// transitive), for a scoped `--follow` query.
    fn path_names(&self, path: &str) -> Vec<String> {
        super::timeline::path_names(self.doc, std::path::Path::new(path))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// The complete line containing byte offset `at`, used as the change-detection
/// unit for a `Match` extent so `Changed` reflects edits to the matched line.
fn match_line(text: &str, at: usize) -> &str {
    let start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
    let end = text[at..].find('\n').map(|n| at + n).unwrap_or(text.len());
    &text[start..end]
}

/// Expand a match to the requested restorable extent.
fn extent_range(
    text: &str,
    match_start: usize,
    needle_len: usize,
    extent: SelectionExtent,
) -> ByteRange {
    match extent {
        SelectionExtent::Match => ByteRange {
            start: match_start,
            end: match_start + needle_len,
        },
        SelectionExtent::Line => {
            let line_start = text[..match_start].rfind('\n').map(|n| n + 1).unwrap_or(0);
            let match_end = match_start + needle_len;
            let line_end = text[match_end..]
                .find('\n')
                .map(|n| match_end + n)
                .unwrap_or(text.len());
            ByteRange {
                start: line_start,
                end: line_end,
            }
        }
        // Hunk/symbol are barred by validate(); Match is a safe fallback.
        _ => ByteRange {
            start: match_start,
            end: match_start + needle_len,
        },
    }
}

fn preview_of(text: &str, range: ByteRange) -> String {
    const MAX: usize = 200;
    let slice = &text[range.start..range.end.min(text.len())];
    let trimmed = slice.trim_end_matches('\n');
    if trimmed.len() <= MAX {
        trimmed.to_owned()
    } else {
        let mut cut = MAX;
        while cut > 0 && !trimmed.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}…", &trimmed[..cut])
    }
}

/// Query-local scan memo: content identity → scan outcome. Each distinct
/// content version is searched once per query; revisits — the same bytes
/// under a different capture, frontier, or path — reuse the outcome and
/// rebuild only the capture-specific handle parts. Keyed by the SHA-256
/// the cache rows already carry, so a revisit is usually answered before
/// any decompression or fork.
///
/// Correctness rests on identity: equal hashes ⇒ equal bytes ⇒ equal
/// `find`, extent, line, and context results. The store already trusts
/// SHA-256 identity for content addressing, so the memo adds no new
/// collision surface.
#[derive(Default)]
struct ScanMemo {
    outcomes: std::collections::HashMap<String, ScanOutcome>,
}

/// Pathological walks are bounded, not evicted: beyond the cap the memo
/// stops growing and revisits pay a full read again. Each entry is a few
/// hundred bytes, so the cap bounds memory without disturbing real queries.
const SCAN_MEMO_MAX_ENTRIES: usize = 1 << 20;

#[derive(Clone)]
enum ScanOutcome {
    /// Text searched; the needle is absent.
    Absent,
    /// Binary content; counted per visit, never decompressed.
    Binary,
    /// Every non-overlapping literal occurrence, in byte order.
    Matches(Vec<ScannedMatch>),
    /// Enumeration exceeded the per-entry unit cap: known content, but too
    /// dense to retain. Visitors must re-scan from the text; the marker
    /// exists so absence is never faked for content the needle DOES match.
    TooLarge,
}

/// Occurrence units retained per distinct content version. One entry may
/// otherwise hold millions of units (a 16 MiB single-byte-match file), which
/// no entry-count cap bounds. Above the cap the walk stays correct by
/// re-scanning and pays CPU, not memory.
const SCAN_MEMO_MAX_UNITS: usize = 65_536;

/// The content-dependent half of one literal occurrence. `match_range` is the
/// occurrence coordinate; `range` is the explicit restorable extent and may
/// cover several occurrences when line extent is requested.
#[derive(Clone)]
struct ScannedMatch {
    match_range: ByteRange,
    range: ByteRange,
    line: usize,
    column: usize,
    selected_sha256: String,
    before_sha256: String,
    after_sha256: String,
    /// Matched-line identity — the change-detection unit for `Changed`.
    line_sha256: String,
    preview: String,
}

fn scan_cache_entry_bytes(key: &(String, String), outcome: &ScanOutcome) -> usize {
    let outcome_bytes = match outcome {
        ScanOutcome::Matches(matches) => {
            matches
                .iter()
                .fold(std::mem::size_of::<ScanOutcome>(), |total, item| {
                    total
                        .saturating_add(std::mem::size_of::<ScannedMatch>())
                        .saturating_add(item.selected_sha256.capacity())
                        .saturating_add(item.before_sha256.capacity())
                        .saturating_add(item.after_sha256.capacity())
                        .saturating_add(item.line_sha256.capacity())
                        .saturating_add(item.preview.capacity())
                })
        }
        _ => std::mem::size_of::<ScanOutcome>(),
    };
    std::mem::size_of::<((String, String), ScanOutcome)>()
        .saturating_add(key.0.capacity())
        .saturating_add(key.1.capacity())
        .saturating_add(outcome_bytes)
}

impl ScanMemo {
    /// Probe for a content identity; `None` means this query has not seen
    /// these bytes yet.
    fn get(&self, hash: &str) -> Option<&ScanOutcome> {
        self.outcomes.get(hash)
    }

    fn insert(&mut self, hash: String, outcome: ScanOutcome) {
        if self.outcomes.len() < SCAN_MEMO_MAX_ENTRIES {
            self.outcomes.insert(hash, outcome);
        }
    }
}

/// The [`ScannedMatch`] constructor: scanners know the unit's line and
/// column incrementally (a rolling newline count), because rescanning the
/// whole prefix per unit is quadratic on dense files.
fn scan_match_with_line(
    text: &str,
    match_range: ByteRange,
    range: ByteRange,
    line: usize,
    column: usize,
) -> ScannedMatch {
    let before = super::selection::context_before(text, range.start);
    let after = super::selection::context_after(text, range.end);
    ScannedMatch {
        match_range,
        range,
        line,
        column,
        line_sha256: sha256_hex(match_line(text, match_range.start).as_bytes()),
        selected_sha256: sha256_hex(&text.as_bytes()[range.start..range.end]),
        before_sha256: sha256_hex(before.as_bytes()),
        after_sha256: sha256_hex(after.as_bytes()),
        preview: preview_of(text, range),
    }
}

/// Incremental line/column tracker for single-pass scanners. Keeping the
/// current line start and its traversed char count makes both numbers O(1)
/// amortized per unit; recomputing either from the prefix is quadratic on
/// dense or single-line files.
#[derive(Debug, Clone)]
struct LineCursor {
    /// Number of `\n` in `text[..scanned_upto]`.
    seen_newlines: usize,
    scanned_upto: usize,
    /// Byte offset where the current line begins, `<= scanned_upto`.
    line_start: usize,
    /// Chars in `[line_start, scanned_upto)`.
    col_base: usize,
}

impl LineCursor {
    fn new() -> Self {
        LineCursor {
            seen_newlines: 0,
            scanned_upto: 0,
            line_start: 0,
            col_base: 0,
        }
    }

    /// Line/column of a byte offset at or beyond the traversed prefix.
    fn position(&self, text: &str, at: usize) -> (usize, usize) {
        let from = self.scanned_upto.min(at);
        let gap = &text[from..at];
        let newlines = self.seen_newlines + gap.bytes().filter(|b| *b == b'\n').count();
        let column = match gap.rfind('\n') {
            Some(n) => text[from + n + 1..at].chars().count() + 1,
            None => self.col_base + gap.chars().count() + 1,
        };
        (1 + newlines, column)
    }

    /// Absorb the traversed bytes so later positions stay O(segment).
    fn advance(&mut self, text: &str, upto: usize) {
        let from = self.scanned_upto;
        if let Some(n) = text[from..upto].rfind('\n') {
            let line_start = from + n + 1;
            self.col_base = text[line_start..upto].chars().count();
            self.line_start = line_start;
        } else {
            self.col_base += text[from..upto].chars().count();
        }
        self.seen_newlines += text[from..upto].bytes().filter(|b| *b == b'\n').count();
        self.scanned_upto = upto;
    }
}

/// The next collapsed occurrence unit at or after `match_start`, or `None`
/// when the text has no further match. Under line extent, every literal on
/// one line maps to the same expanded [`ByteRange`]; those matches collapse
/// into one restorable unit whose coordinates come from the leftmost match.
/// Under match extent ranges never repeat, so nothing collapses.
fn next_unit(
    text: &str,
    needle: &str,
    extent: SelectionExtent,
    mut match_start: usize,
    last_range: &mut Option<ByteRange>,
) -> Option<usize> {
    while let Some(offset) = text[match_start..].find(needle) {
        let at = match_start + offset;
        let range = extent_range(text, at, needle.len(), extent);
        if *last_range == Some(range) {
            // Same line unit as the previous occurrence; the leftmost match
            // already represents it.
            match_start = at + needle.len();
            continue;
        }
        *last_range = Some(range);
        return Some(at);
    }
    None
}

/// Scan a text once, memoizing every collapsed occurrence unit for the
/// content identity. Capture/path-specific identities remain lazy.
fn scan_once(memo: &mut ScanMemo, hash: &str, text: &str, needle: &str, extent: SelectionExtent) {
    if memo.get(hash).is_some() {
        return;
    }
    let matches: Vec<ScannedMatch> = enumerate_units(text, needle, extent);
    if matches.len() > SCAN_MEMO_MAX_UNITS {
        memo.insert(hash.to_owned(), ScanOutcome::TooLarge);
        return;
    }
    let outcome = if matches.is_empty() {
        ScanOutcome::Absent
    } else {
        ScanOutcome::Matches(matches)
    };
    memo.insert(hash.to_owned(), outcome);
}

/// Every collapsed occurrence unit in byte order (the memoized enumeration).
fn enumerate_units(text: &str, needle: &str, extent: SelectionExtent) -> Vec<ScannedMatch> {
    let mut matches = Vec::new();
    let mut cursor = 0usize;
    let mut last_range: Option<ByteRange> = None;
    let mut lines = LineCursor::new();
    while let Some(at) = next_unit(text, needle, extent, cursor, &mut last_range) {
        let match_range = ByteRange {
            start: at,
            end: at + needle.len(),
        };
        let range = extent_range(text, at, needle.len(), extent);
        let (line, column) = lines.position(text, at);
        matches.push(scan_match_with_line(text, match_range, range, line, column));
        lines.advance(text, at + needle.len());
        cursor = at + needle.len();
    }
    matches
}

/// The occurrence units with ordinals `[skip, skip + take)` in the text's
/// byte-ordered unit sequence, constructing ONLY those units: skipped units
/// advance by offset math, never by building a [`ScannedMatch`]. Returns the
/// window, how many of the requested `skip` units this text actually held
/// (so a global record cursor can carry the remainder to the next path), and
/// whether at least one further unit exists past the window — the cheap
/// exhaustion signal point-mode pagination needs without a full enumeration.
/// Result of a windowed scan: the constructed units, how many of the
/// requested skips this text consumed, whether at least one further unit
/// exists, and whether the wall-clock deadline stopped the scan early (the
/// caller reports a time limit instead of silently slowing down).
struct ScanWindow {
    window: Vec<ScannedMatch>,
    skipped: usize,
    more: bool,
    timed_out: bool,
}

fn scan_window(
    text: &str,
    needle: &str,
    extent: SelectionExtent,
    skip: usize,
    take: usize,
    deadline: Option<std::time::Instant>,
) -> ScanWindow {
    let mut window = Vec::new();
    let mut cursor = 0usize;
    let mut last_range: Option<ByteRange> = None;
    let mut skipped = 0usize;
    let mut more = false;
    // Rolling line/column state: skipped units pay the same single pass
    // over their bytes as constructed ones, so resume cost stays
    // proportional to the traversed span, not to unit count times prefix
    // length.
    let mut lines = LineCursor::new();
    let mut visited = 0usize;
    while let Some(at) = next_unit(text, needle, extent, cursor, &mut last_range) {
        visited += 1;
        if let Some(deadline) = deadline {
            // Per-unit clock reads would dominate the scan; a periodic
            // sample bounds overrun to a small constant.
            if visited % 1024 == 0 && std::time::Instant::now() >= deadline {
                return ScanWindow {
                    window,
                    skipped,
                    more: true,
                    timed_out: true,
                };
            }
        }
        let traversed = at + needle.len();
        if skipped < skip {
            skipped += 1;
            lines.advance(text, traversed);
            cursor = traversed;
            continue;
        }
        if window.len() >= take {
            more = true;
            break;
        }
        let match_range = ByteRange {
            start: at,
            end: traversed,
        };
        let range = extent_range(text, at, needle.len(), extent);
        let (line, column) = lines.position(text, at);
        window.push(scan_match_with_line(text, match_range, range, line, column));
        lines.advance(text, traversed);
        cursor = traversed;
    }
    ScanWindow {
        window,
        skipped,
        more,
        timed_out: false,
    }
}

impl ScannedMatch {
    /// The capture-specific handle for this scanned content, identical to
    /// what `SelectionHandle::from_source` produces over the same bytes.
    fn handle(
        &self,
        frontier: &str,
        capture_id: &str,
        path: &str,
        extent: SelectionExtent,
        query_fingerprint: &str,
    ) -> (SelectionHandle, String) {
        let handle = SelectionHandle::from_verified_parts(
            frontier.to_owned(),
            Some(capture_id.to_owned()),
            path.to_owned(),
            extent,
            self.range,
            self.selected_sha256.clone(),
            self.before_sha256.clone(),
            self.after_sha256.clone(),
            query_fingerprint.to_owned(),
        );
        let id = handle.id();
        (handle, id)
    }

    fn occurrence_id(
        &self,
        frontier: &str,
        capture_id: &str,
        path: &str,
        query_fingerprint: &str,
    ) -> String {
        let canonical = serde_json::json!({
            "frontier": frontier,
            "capture_id": capture_id,
            "path": path,
            "match_start": self.match_range.start,
            "match_end": self.match_range.end,
            "query_fingerprint": query_fingerprint,
        });
        let mut bytes = b"sheaf:grep-occurrence:v1\0".to_vec();
        bytes.extend(serde_json::to_vec(&canonical).expect("occurrence identity serializes"));
        sha256_hex(&bytes)
    }
}

/// Shared wall-clock/byte budget context, checked both between captures and
/// per path-read so a single expensive capture cannot overrun the budget.
struct BudgetCtx<'a> {
    started: std::time::Instant,
    budget: &'a SearchBudget,
}

#[derive(Default, Clone, Copy)]
struct ReplayCharge {
    bytes: u64,
    elapsed_ms: u64,
}

impl BudgetCtx<'_> {
    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn stop_reason(&self, usage: &SearchUsage, replay: ReplayCharge) -> Option<SearchStopReason> {
        let effective = SearchUsage {
            results: usage.results,
            materialized_bytes: usage.materialized_bytes.saturating_sub(replay.bytes),
            elapsed_ms: self.elapsed_ms().saturating_sub(replay.elapsed_ms),
            historical_forks: usage.historical_forks,
            historical_path_reads: usage.historical_path_reads,
            historical_cache_hits: usage.historical_cache_hits,
            historical_disk_cache_hits: usage.historical_disk_cache_hits,
            content_dedup_hits: usage.content_dedup_hits,
            cursor_replayed_captures: usage.cursor_replayed_captures,
            trigram_skipped: usage.trigram_skipped,
        };
        self.budget.stop_reason(&effective)
    }

    /// True once post-cursor time or materialized bytes are exhausted.
    /// Result-count limits are checked at capture boundaries, not mid-capture.
    fn tripped(&self, usage: &SearchUsage, replay: ReplayCharge) -> bool {
        self.elapsed_ms().saturating_sub(replay.elapsed_ms) >= self.budget.max_elapsed_ms
            || usage.materialized_bytes.saturating_sub(replay.bytes)
                >= self.budget.max_materialized_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PointOutcome {
    Done,
    BudgetHit,
    /// One capture's scanned units exceeded the dense-capture cap. History
    /// reconciliation over such a capture allocates proportionally to its
    /// occurrence count, so the walk stops at the capture boundary instead
    /// of attempting it; paging cannot get past it (the same capture is
    /// dense on every page), which is the honest answer for now — windowed
    /// reconciliation is future work.
    DenseCapture,
}

/// Record a budget stop at the last fully-processed capture boundary.
/// Cache the reduction state at a page's clean capture boundary, so
/// the next page resumes without replaying up to it. No-op unless the returned
/// cursor is a whole-capture-boundary resume (never a mid-capture one) with a
/// concrete anchor. The state is `lineages`/`seen_lineages` exactly as a
/// suppressed replay to `anchor` would rebuild them.
fn cache_boundary_state(
    source: &GrepSource,
    report: &GrepReport,
    fingerprint: &str,
    anchor: &Option<String>,
    lineages: &BTreeMap<String, UnitState>,
    seen_lineages: &std::collections::BTreeSet<String>,
) {
    let Some(anchor) = anchor else { return };
    if report
        .cursor
        .as_ref()
        .is_some_and(|c| c.resume_capture_id.is_none())
    {
        source.cursor_state_put(
            fingerprint,
            anchor,
            CursorState {
                lineages: lineages.clone(),
                seen_lineages: seen_lineages.clone(),
            },
        );
    }
}

fn stop_at_boundary(
    report: &mut GrepReport,
    stop: SearchStopReason,
    fingerprint: &str,
    last_emitted: &Option<String>,
) {
    report.complete = false;
    report.stop_reason = Some(stop);
    // Anchor at the last fully-processed capture. If none has been processed
    // yet, anchor before the first point so a resume re-enters at exactly the
    // capture the budget interrupted.
    report.cursor = Some(SearchCursor {
        query_fingerprint: fingerprint.to_owned(),
        after_capture_id: last_emitted
            .clone()
            .unwrap_or_else(|| BEFORE_FIRST.to_owned()),
        resume_capture_id: None,
        record_index: 0,
        path_index: 0,
        match_index: 0,
    });
}

fn run(
    source: &GrepSource,
    req: &GrepRequest,
    degraded: bool,
    sink: &mut Option<GrepSink<'_>>,
) -> Result<GrepReport> {
    req.validate()?;
    match req.mode {
        GrepMode::Point => run_point(source, req, degraded, sink),
        GrepMode::History => run_history(source, req, degraded, sink),
    }
}

fn empty_report(fingerprint: String, degraded: bool) -> GrepReport {
    GrepReport {
        query_fingerprint: fingerprint,
        complete: true,
        stop_reason: None,
        cursor: None,
        hits: Vec::new(),
        events: Vec::new(),
        skipped_binary: 0,
        pruned_intervals: 0,
        usage: SearchUsage {
            results: 0,
            materialized_bytes: 0,
            elapsed_ms: 0,
            historical_forks: 0,
            historical_path_reads: 0,
            historical_cache_hits: 0,
            historical_disk_cache_hits: 0,
            content_dedup_hits: 0,
            cursor_replayed_captures: 0,
            trigram_skipped: 0,
        },
        degraded,
    }
}

/// Discover every literal occurrence at one immutable point. Results have a
/// stable path/range order and can resume inside that one capture by record
/// index without persisting query state.
fn run_point(
    source: &GrepSource,
    req: &GrepRequest,
    degraded: bool,
    sink: &mut Option<GrepSink<'_>>,
) -> Result<GrepReport> {
    use super::timeline::{capture_at_frontier, resolve_in_doc};

    let fingerprint = req.fingerprint();
    let started = std::time::Instant::now();
    let reference = req.at.as_deref().unwrap_or("@");
    let resolved = resolve_in_doc(source.doc, source.ledger, &source.current, reference)?;
    let frontier = decode_frontier(&resolved.frontier)?;
    let capture = capture_at_frontier(source.doc, &frontier).ok_or_else(|| {
        SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
    })?;
    // Discovery at an abandoned-branch capture is honest about it: hits say
    // which lineage they came from instead of presenting branch state as
    // trunk.
    let on_current = super::timeline::frontier_on_current(
        source.doc,
        source.ledger,
        &source.current,
        &capture.frontier,
    );
    let lineage_id = if on_current {
        "current".to_owned()
    } else {
        format!("branch:{}", capture.frontier)
    };
    let mut report = empty_report(fingerprint.clone(), degraded);
    let mut history = HistoryView::open(source.doc)?;
    let mut paths = source.text_paths_at(&mut history, &capture.frontier)?;
    if let Some(scope) = req.path.as_deref().map(normalize_key) {
        paths.retain(|path| path == &scope || path.starts_with(&format!("{scope}/")));
    }
    paths.sort();

    // A point cursor names the exact discovery capture it paged. If it names a
    // different capture — e.g. the default `@` advanced between pages — the
    // page cannot be resumed without silently re-emitting or skipping, so it
    // fails closed rather than restarting from record 0.
    let mut skip = match req.cursor.as_ref() {
        None => 0,
        Some(cursor) => match cursor.resume_capture_id.as_deref() {
            Some(id) if id == capture.id => cursor.record_index,
            _ => {
                return Err(SheafError::BadCursor(
                    "point cursor does not resume the resolved discovery capture (did `@` move? pin `--at`)".into(),
                ));
            }
        },
    };

    // Windowed enumeration: paths stream in sorted order and every path is
    // scanned with only `[skip, skip + budget)` of ITS occurrence units
    // constructed — the global record order is (path, byte range), so the
    // concatenated windows are exactly the sorted occurrence list. A dense
    // snapshot therefore never materializes its full occurrence set (the
    // RSS ceiling fixture relies on this). The byte budget charges only
    // authoritative (cache-miss) materializations: cache hits are bounded by
    // the content cache itself, and charging them would re-trip every resumed
    // page on the same warm bytes and livelock.
    let mut budget_left = req.budget.max_results;
    let mut charged_bytes = 0u64;
    let mut emitted = 0usize;
    let mut incomplete: Option<SearchStopReason> = None;
    let mut paths_iter = paths.into_iter().peekable();
    while let Some(path) = paths_iter.next() {
        if budget_left == 0 {
            // Unscanned paths may still hold units; a fresh page reads them.
            incomplete = Some(SearchStopReason::ResultLimit);
            break;
        }
        // While the resume prefix is still being consumed (`skip > 0`) the
        // page has emitted nothing: stopping here would return a cursor
        // identical to the page's input and livelock the client. The prefix
        // re-traversal is bounded by the corpus, so both wall-clock and
        // byte stops wait until the page can make emission progress.
        if skip == 0 && started.elapsed().as_millis() as u64 >= req.budget.max_elapsed_ms {
            incomplete = Some(SearchStopReason::TimeLimit);
            break;
        }
        report.usage.historical_path_reads += 1;
        let (content, cache_hit, _identity) =
            source.path_at(&mut history, &capture.frontier, &path)?;
        match cache_hit {
            CacheHit::Miss => {}
            CacheHit::Memory => report.usage.historical_cache_hits += 1,
            CacheHit::Disk => report.usage.historical_disk_cache_hits += 1,
        }
        match content {
            HistoricalPathContent::Text(text) => {
                report.usage.materialized_bytes += text.len() as u64;
                if cache_hit == CacheHit::Miss {
                    charged_bytes += text.len() as u64;
                }
                let deadline =
                    started + std::time::Duration::from_millis(req.budget.max_elapsed_ms);
                let scanned = scan_window(
                    &text,
                    req.query.needle(),
                    req.extent,
                    skip,
                    budget_left,
                    Some(deadline),
                );
                if scanned.timed_out {
                    incomplete = Some(SearchStopReason::TimeLimit);
                    break;
                }
                skip -= scanned.skipped;
                budget_left -= scanned.window.len();
                for scanned in scanned.window {
                    let (handle, handle_id) = scanned.handle(
                        &capture.frontier,
                        &capture.id,
                        &path,
                        req.extent,
                        &fingerprint,
                    );
                    let hit = GrepHit {
                        capture_id: capture.id.clone(),
                        frontier: capture.frontier.clone(),
                        timestamp_ms: capture.timestamp_ms,
                        lineage_id: lineage_id.clone(),
                        on_current,
                        path: path.clone(),
                        kind: LifecycleKind::Present,
                        line: scanned.line,
                        column: scanned.column,
                        occurrence_id: scanned.occurrence_id(
                            &capture.frontier,
                            &capture.id,
                            &path,
                            &fingerprint,
                        ),
                        episode_id: None,
                        preview: scanned.preview.clone(),
                        handle,
                        handle_id,
                    };
                    emit_record(
                        &mut report,
                        sink,
                        GrepStreamRecord::Hit { hit: Box::new(hit) },
                    );
                    emitted += 1;
                }
                if budget_left == 0 && (scanned.more || paths_iter.peek().is_some()) {
                    incomplete = Some(SearchStopReason::ResultLimit);
                    break;
                }
                if skip == 0 && charged_bytes >= req.budget.max_materialized_bytes {
                    incomplete = Some(SearchStopReason::ByteLimit);
                    break;
                }
            }
            HistoricalPathContent::Binary { .. } => report.skipped_binary += 1,
            HistoricalPathContent::Absent => {}
        }
    }
    // A cursor naming more records than the snapshot holds is stale or
    // forged: the skip could not be consumed, so fail closed — unless a
    // budget stop ended the page first, which returns an honest incomplete
    // report the client may retry with a larger budget.
    if skip > 0 && incomplete.is_none() {
        return Err(SheafError::BadCursor(
            "point cursor record index is past the occurrence set".into(),
        ));
    }
    if let Some(stop) = incomplete {
        report.complete = false;
        report.stop_reason = Some(stop);
        report.cursor = Some(SearchCursor {
            query_fingerprint: fingerprint,
            after_capture_id: BEFORE_FIRST.to_owned(),
            resume_capture_id: Some(capture.id.clone()),
            record_index: req.cursor.as_ref().map_or(0, |c| c.record_index) + emitted,
            path_index: 0,
            match_index: 0,
        });
    }
    report.usage.elapsed_ms = started.elapsed().as_millis() as u64;
    report.usage.historical_forks = history.forks_created();
    Ok(report)
}

fn run_history(
    source: &GrepSource,
    req: &GrepRequest,
    degraded: bool,
    sink: &mut Option<GrepSink<'_>>,
) -> Result<GrepReport> {
    let fingerprint = req.fingerprint();
    let needle = req.query.needle().to_owned();
    // Resolve a coordinate/selection anchor to its episode ID before the
    // emitting walk: the followed episode's records can precede the anchor
    // capture inside `(from, to]`, so the walk cannot simply start there.
    // The suppressed pre-walk reads warm the content cache; its cost is
    // query setup, like `points()`, and does not consume the page budget.
    let anchored = match &req.anchor {
        None => None,
        Some(GrepAnchor::Episode { episode_id }) => Some(episode_id.clone()),
        Some(coordinate_or_selection) => Some(resolve_anchor_episode(
            source,
            req,
            &needle,
            coordinate_or_selection,
        )?),
    };
    let started = std::time::Instant::now();
    let points = source.points(req)?;
    // Historical names for a scoped+follow query, computed once.
    let scope_names: Vec<String> = match (&req.path, req.follow) {
        (Some(path), true) => source.path_names(path),
        (Some(path), false) => vec![normalize_key(path)],
        (None, _) => Vec::new(),
    };
    // The cumulative rename list is document-wide and history-length; read it
    // once here so process_point filters a slice instead of re-scanning and
    // re-parsing the whole tree-events list per capture (an O(N^2) walk).
    let all_renames = super::timeline::read_renames(source.doc);

    let mut report = GrepReport {
        query_fingerprint: fingerprint.clone(),
        complete: true,
        stop_reason: None,
        cursor: None,
        hits: Vec::new(),
        events: Vec::new(),
        skipped_binary: 0,
        pruned_intervals: 0,
        usage: SearchUsage {
            results: 0,
            materialized_bytes: 0,
            elapsed_ms: 0,
            historical_forks: 0,
            historical_path_reads: 0,
            historical_cache_hits: 0,
            historical_disk_cache_hits: 0,
            content_dedup_hits: 0,
            cursor_replayed_captures: 0,
            trigram_skipped: 0,
        },
        degraded,
    };

    // Content-version scan memo for this query: each distinct content
    // identity is searched once, revisits reuse the outcome.
    let mut memo = ScanMemo::default();
    let mut lineages: BTreeMap<String, UnitState> = BTreeMap::new();
    let mut seen_lineages: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    // One view per query lets every path read at a capture reuse the same
    // whole-document fork. Constructing a view inside `path_at` made an
    // N-path capture pay N identical `fork_at` calls.
    let mut history = HistoryView::open(source.doc)?;
    let ctx = BudgetCtx {
        started,
        budget: &req.budget,
    };

    // On resume, everything up to and including the cursor capture is replayed
    // with emission suppressed so per-lineage transition state (present unit,
    // seen-before) is rebuilt exactly. Replay work remains visible in usage,
    // but does not consume the next page's byte/time allowance; otherwise a
    // page can spend its whole budget rebuilding state and return the same
    // cursor forever.
    let resume_after = match req.cursor.as_ref().map(|c| c.after_capture_id.as_str()) {
        Some(BEFORE_FIRST) | None => None,
        Some(cursor) => Some(resolve_cursor_capture(&points, cursor)?),
    };
    let mut partial_capture = match req
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.resume_capture_id.as_deref())
    {
        Some(id) => Some(resolve_cursor_capture(&points, id)?),
        None => None,
    };
    let partial_record_index = req.cursor.as_ref().map_or(0, |cursor| cursor.record_index);

    // Cursor-state cache: a capture-boundary resume (no partial
    // capture) can restore the reduction state the suppressed replay would
    // otherwise rebuild. On a hit we seed the lineage state and fast-forward
    // past the anchor without processing it. On a miss (eviction, restart, new
    // fingerprint) `cached_resume` stays false and the authoritative replay
    // runs exactly as before — identical result, just slower. A mid-capture
    // (partial) resume is never served from the cache: its record index needs
    // the anchor capture's pre-state, which the boundary cache does not hold.
    let mut cached_resume = false;
    if partial_capture.is_none() {
        if let Some(anchor) = resume_after.as_deref() {
            if let Some(state) = source.cursor_state_get(&fingerprint, anchor) {
                lineages = state.lineages;
                seen_lineages = state.seen_lineages;
                cached_resume = true;
            }
        }
    }
    // A cursor carrying `resume_capture_id` always replays suppressed,
    // whichever token shape produced it: the three-part
    // AFTER:RESUME:INDEX form (resume after a fully processed capture) and
    // the two-part RESUME:INDEX form (resume mid-capture from the range
    // start) must both re-emit nothing before the resume point.
    // A cached resume needs no suppressed replay: the state is already seeded,
    // so points up to the anchor are fast-forwarded (skipped outright) and
    // real processing begins immediately after it. A non-cached resume keeps
    // the authoritative suppressed replay.
    let mut suppress = !cached_resume && (resume_after.is_some() || partial_capture.is_some());
    // While fast-forwarding a cached resume, skip every point up to and
    // including the anchor without touching it. Cleared the moment the anchor
    // is passed.
    let mut fast_forwarding = cached_resume;
    let mut replay_charge = ReplayCharge::default();
    // The cursor always anchors at a fully processed capture, including a
    // capture that emitted no hit. This guarantees forward progress for absent
    // queries and byte-limited pages.
    let mut last_processed: Option<String> = None;
    // Every page must fully process at least one point past its resume anchor,
    // or a point whose first path read alone exhausts the budget would be
    // admitted, abort mid-point, re-anchor at the previous capture, and repeat
    // forever. Until one point completes, mid-point budget trips are suppressed
    // so the page always advances by at least one capture.
    let mut progress_made = false;

    for point in points.iter() {
        // Cached-resume fast-forward: the reduction state at the anchor was
        // restored from the cursor-state cache, so every point up to and
        // including the anchor is skipped without any read or reduction. Once
        // the anchor is passed, normal processing resumes. This is the replay
        // the cache eliminates; a cache miss never reaches here (suppress runs
        // instead).
        if fast_forwarding {
            let is_anchor = resume_after.as_deref() == Some(point.capture.id.as_str());
            if is_anchor {
                fast_forwarding = false;
            }
            continue;
        }
        // Budget stop only applies once we are past the resume point and have
        // fully processed at least one capture this page; the returned cursor
        // always points at a fully-processed capture strictly after the resume
        // anchor, so pages cannot stall. The check runs before admitting a
        // capture (so no partial capture is emitted) and again per path-read
        // inside `process_point`, so one very expensive capture cannot overrun
        // the wall-clock budget.
        if !suppress && progress_made {
            report.usage.elapsed_ms = ctx.elapsed_ms();
            if let Some(stop) = ctx.stop_reason(&report.usage, replay_charge) {
                stop_at_boundary(&mut report, stop, &fingerprint, &last_processed);
                cache_boundary_state(
                    source,
                    &report,
                    &fingerprint,
                    &last_processed,
                    &lineages,
                    &seen_lineages,
                );
                report.usage.historical_forks = history.forks_created();
                return Ok(report);
            }
        }

        // A retention gap is not a baseline: it carries no content, so the
        // lineage's first surviving capture still needs the full-tree
        // introduction scan. Only real captures claim the baseline slot.
        // Baseline status is derived before the capture, but the lineage is
        // not marked seen until the capture finishes. A byte/time/density stop
        // re-anchors before the interrupted capture; inserting here would make
        // a warm cursor-state resume believe a branch's first capture had
        // already been replayed and diverge from the authoritative cold path.
        let is_baseline = !seen_lineages.contains(&point.lineage_id);
        let state = lineages.entry(point.lineage_id.clone()).or_default();
        if suppress {
            report.usage.cursor_replayed_captures += 1;
        }
        // On the partial-resume capture, the whole deterministic batch is
        // regenerated but the already-delivered prefix is suppressed by
        // record index. Elsewhere the skip is zero.
        let at_partial = partial_capture.as_deref() == Some(point.capture.id.as_str());
        // The two-part RESUME:INDEX shape has no `after` anchor, so its
        // suppression lifts on entering the partial capture itself — its
        // remaining records emit past `record_index`.
        if suppress && at_partial && resume_after.is_none() {
            suppress = false;
            replay_charge = ReplayCharge {
                bytes: report.usage.materialized_bytes,
                elapsed_ms: ctx.elapsed_ms(),
            };
        }
        let skip_prefix = if at_partial { partial_record_index } else { 0 };
        let mut batch = BatchEmit {
            skip: skip_prefix,
            emitted_in_capture: 0,
            budget_left: req.budget.max_results.saturating_sub(report.usage.results),
            overflow: false,
        };
        let outcome = if point.pruned {
            // A retention gap terminates every episode active on this
            // lineage. In an anchored walk the gap is part of the followed
            // episode's story exactly when that episode was among them.
            let held_anchor = anchored
                .as_deref()
                .is_some_and(|id| state.present.iter().any(|u| u.episode_id == id));
            state.present.clear();
            if !suppress && (anchored.is_none() || held_anchor) {
                let event = GrepEvent {
                    capture_id: point.capture.id.clone(),
                    frontier: String::new(),
                    timestamp_ms: point.capture.timestamp_ms,
                    lineage_id: point.lineage_id.clone(),
                    on_current: point.on_current,
                    kind: LifecycleKind::RetentionGap,
                    path: None,
                    last_present_handle_id: None,
                    episode_id: None,
                    candidates: None,
                };
                batch.push(&mut report, sink, GrepStreamRecord::Event { event });
            }
            PointOutcome::Done
        } else {
            process_point(
                source,
                &mut history,
                req,
                &needle,
                &fingerprint,
                &scope_names,
                &all_renames,
                anchored.as_deref(),
                point,
                state,
                is_baseline,
                suppress,
                progress_made,
                &ctx,
                replay_charge,
                &mut memo,
                &mut report,
                sink,
                &mut batch,
            )?
        };
        if partial_capture.as_deref() == Some(point.capture.id.as_str()) {
            // A resume index past the end of the regenerated batch is a stale
            // or hand-forged cursor: fail closed rather than silently dropping
            // this capture's records. `skip` only survives non-zero when the
            // batch produced fewer records than the requested prefix.
            if batch.skip > 0 {
                return Err(SheafError::BadCursor(
                    "cursor record index is past this capture's record batch".into(),
                ));
            }
            // Consumed; later captures resume from their own start.
            partial_capture = None;
        }
        if batch.overflow && !suppress {
            report.pruned_intervals += pruned_delta(point);
            report.complete = false;
            report.stop_reason = Some(SearchStopReason::ResultLimit);
            report.cursor = Some(SearchCursor {
                query_fingerprint: fingerprint.clone(),
                after_capture_id: last_processed
                    .clone()
                    .unwrap_or_else(|| BEFORE_FIRST.to_owned()),
                resume_capture_id: Some(point.capture.id.clone()),
                record_index: skip_prefix + batch.emitted_in_capture,
                path_index: 0,
                match_index: 0,
            });
            // A result-limit stop is mid-capture: `lineages` here already
            // reflects the partial capture's reconciliation, not the anchor's
            // pre-state, so it must NOT be cached — the resume needs the state
            // BEFORE the partial capture. Only the true capture-boundary stops
            // (byte/time limit, dense guard) cache their state.
            report.usage.elapsed_ms = ctx.elapsed_ms();
            report.usage.historical_forks = history.forks_created();
            return Ok(report);
        }
        if !suppress {
            report.pruned_intervals += pruned_delta(point);
        }
        if outcome == PointOutcome::BudgetHit && !suppress {
            report.usage.elapsed_ms = ctx.elapsed_ms();
            let stop = ctx
                .stop_reason(&report.usage, replay_charge)
                .unwrap_or(SearchStopReason::TimeLimit);
            stop_at_boundary(&mut report, stop, &fingerprint, &last_processed);
            cache_boundary_state(
                source,
                &report,
                &fingerprint,
                &last_processed,
                &lineages,
                &seen_lineages,
            );
            report.usage.historical_forks = history.forks_created();
            return Ok(report);
        }
        if outcome == PointOutcome::DenseCapture && !suppress {
            // Memory guard (review finding): reconciliation state grows with
            // the capture's occurrence count. Stopping here bounds the
            // page's footprint; the cursor resumes before this capture, so
            // a client that keeps hitting it sees a repeatable byte-limit
            // page rather than an OOM.
            report.usage.elapsed_ms = ctx.elapsed_ms();
            stop_at_boundary(
                &mut report,
                SearchStopReason::ByteLimit,
                &fingerprint,
                &last_processed,
            );
            cache_boundary_state(
                source,
                &report,
                &fingerprint,
                &last_processed,
                &lineages,
                &seen_lineages,
            );
            report.usage.historical_forks = history.forks_created();
            return Ok(report);
        }

        if !point.pruned {
            seen_lineages.insert(point.lineage_id.clone());
        }
        last_processed = Some(point.capture.id.clone());
        if !suppress {
            progress_made = true;
        }
        if suppress
            && resume_after
                .as_deref()
                .is_some_and(|id| id == point.capture.id)
        {
            // The cursor capture is now fully replayed; emit from the next.
            suppress = false;
            replay_charge = ReplayCharge {
                bytes: report.usage.materialized_bytes,
                elapsed_ms: ctx.elapsed_ms(),
            };
        }
    }

    // `resolve_cursor_capture` already proved the anchor is exactly one point
    // in this walk, so suppression always lifts while iterating. This invariant
    // makes a stale cursor impossible to reach here; it is upheld before the
    // loop, not rediscovered after it.
    debug_assert!(
        !suppress,
        "resume anchor resolved but never matched in walk"
    );

    report.usage.elapsed_ms = started.elapsed().as_millis() as u64;
    report.usage.historical_forks = history.forks_created();
    Ok(report)
}

/// Resolve a coordinate or selection anchor to the episode ID the emitting
/// walk will filter on. Runs a suppressed pre-walk over `(from, anchor]` so
/// the occurrence's episode is known before any record emits — the followed
/// episode's records may precede the anchor capture inside the interval.
/// The pre-walk's reads warm the content cache; its cost is query setup and
/// never charged against the emitting page's budget.
fn resolve_anchor_episode(
    source: &GrepSource,
    req: &GrepRequest,
    needle: &str,
    anchor: &GrepAnchor,
) -> Result<String> {
    use super::timeline::{capture_at_frontier, resolve_in_doc};

    let anchor_capture = match anchor {
        GrepAnchor::Coordinate { .. } => {
            let reference = req
                .at
                .as_deref()
                .ok_or_else(|| SheafError::Config("a coordinate anchor requires `--at`".into()))?;
            let resolved = resolve_in_doc(source.doc, source.ledger, &source.current, reference)?;
            let frontier = decode_frontier(&resolved.frontier)?;
            capture_at_frontier(source.doc, &frontier).ok_or_else(|| {
                SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
            })?
        }
        GrepAnchor::Selection { handle } => {
            let frontier = decode_frontier(&handle.source_frontier)?;
            let capture = capture_at_frontier(source.doc, &frontier).ok_or_else(|| {
                SheafError::Config(
                    "the selection anchor's frontier does not name a capture in this store".into(),
                )
            })?;
            if let Some(reference) = &req.at {
                let resolved =
                    resolve_in_doc(source.doc, source.ledger, &source.current, reference)?;
                let at_frontier = decode_frontier(&resolved.frontier)?;
                let at_capture =
                    capture_at_frontier(source.doc, &at_frontier).ok_or_else(|| {
                        SheafError::TimelineReference(format!(
                            "`{reference}` does not name a capture"
                        ))
                    })?;
                if at_capture.id != capture.id {
                    return Err(SheafError::Config(
                        "`--at` must agree with the selection anchor's source frontier".into(),
                    ));
                }
            }
            capture
        }
        GrepAnchor::Episode { .. } => {
            unreachable!("episode anchors name their episode and skip resolution")
        }
    };

    let points = source.points(req)?;
    let anchor_index = points
        .iter()
        .position(|point| !point.pruned && point.capture.id == anchor_capture.id)
        .ok_or_else(|| {
            SheafError::Config(
                "the anchor capture lies outside this query's interval or lineages \
                 (check --from, --to, and --all)"
                    .into(),
            )
        })?;

    // Suppressed replay up to and including the anchor capture. All usage
    // lands in a throwaway report; nothing emits.
    let scope_names: Vec<String> = match (&req.path, req.follow) {
        (Some(path), true) => source.path_names(path),
        (Some(path), false) => vec![normalize_key(path)],
        (None, _) => Vec::new(),
    };
    // The cumulative rename list is document-wide and history-length; read it
    // once here so process_point filters a slice instead of re-scanning and
    // re-parsing the whole tree-events list per capture (an O(N^2) walk).
    let all_renames = super::timeline::read_renames(source.doc);
    let fingerprint = req.fingerprint();
    let mut memo = ScanMemo::default();
    let mut lineages: BTreeMap<String, UnitState> = BTreeMap::new();
    let mut seen_lineages: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut history = HistoryView::open(source.doc)?;
    let mut scratch = empty_report(String::new(), false);
    let mut none: Option<GrepSink<'_>> = None;
    let mut batch = BatchEmit {
        skip: 0,
        emitted_in_capture: 0,
        budget_left: 0,
        overflow: false,
    };
    let ctx = BudgetCtx {
        started: std::time::Instant::now(),
        budget: &req.budget,
    };
    for point in &points[..=anchor_index] {
        let is_baseline = !seen_lineages.contains(&point.lineage_id);
        seen_lineages.insert(point.lineage_id.clone());
        let state = lineages.entry(point.lineage_id.clone()).or_default();
        if point.pruned {
            state.present.clear();
            continue;
        }
        process_point(
            source,
            &mut history,
            req,
            needle,
            &fingerprint,
            &scope_names,
            &all_renames,
            None,
            point,
            state,
            is_baseline,
            true,
            false,
            &ctx,
            ReplayCharge::default(),
            &mut memo,
            &mut scratch,
            &mut none,
            &mut batch,
        )?;
    }

    // Locate the anchored occurrence inside the anchor lineage's state and
    // return its episode.
    let anchor_point = &points[anchor_index];
    let state = lineages
        .get(&anchor_point.lineage_id)
        .ok_or_else(|| SheafError::StoreCorrupt("anchor lineage has no state".into()))?;
    match anchor {
        GrepAnchor::Coordinate { path, line, column } => {
            let normalized = normalize_key(path);
            let (content, _, _) =
                source.path_at(&mut history, &anchor_capture.frontier, &normalized)?;
            let text = match content {
                HistoricalPathContent::Text(text) => text,
                _ => {
                    return Err(SheafError::Config(format!(
                        "`{path}` is absent or binary at the anchor point"
                    )))
                }
            };
            let at_line: Vec<_> = enumerate_units(&text, needle, req.extent)
                .into_iter()
                .filter(|unit| {
                    unit.line == *line && column.is_none_or(|column| unit.column == column)
                })
                .collect();
            let unit = match at_line.as_slice() {
                [unit] => unit.clone(),
                [] => {
                    return Err(SheafError::Config(format!(
                        "the anchor resolves to no occurrence of the query at {path}:{line}"
                    )))
                }
                _ => {
                    return Err(SheafError::Config(format!(
                        "the anchor is ambiguous: {} occurrences at {path}:{line}; add --column",
                        at_line.len()
                    )))
                }
            };
            state
                .present
                .iter()
                .find(|unit_state| {
                    unit_state.path == normalized && unit_state.match_range == unit.match_range
                })
                .map(|unit_state| unit_state.episode_id.clone())
                .ok_or_else(|| {
                    SheafError::Config(
                        "the anchor occurrence exists at `--at` but this query's scope does not \
                         track its path"
                            .into(),
                    )
                })
        }
        GrepAnchor::Selection { handle } => {
            let normalized = normalize_key(&handle.historical_path);
            let (content, _, _) =
                source.path_at(&mut history, &handle.source_frontier, &normalized)?;
            let text = match content {
                HistoricalPathContent::Text(text) => text,
                _ => {
                    return Err(SheafError::Config(
                        "the selection anchor's path is absent or binary at its frontier".into(),
                    ))
                }
            };
            handle.verified_contexts(&text).map_err(|error| {
                SheafError::Config(format!(
                    "selection anchor does not verify at its source snapshot: {error:?}"
                ))
            })?;
            let tracked: Vec<_> = state
                .present
                .iter()
                .filter(|unit_state| {
                    unit_state.path == normalized
                        && unit_state.selected_sha256 == handle.selected_text_sha256
                        && unit_state.before_sha256 == handle.before_context_sha256
                        && unit_state.after_sha256 == handle.after_context_sha256
                })
                .collect();
            match tracked.as_slice() {
                [unit_state] => Ok(unit_state.episode_id.clone()),
                [] => Err(SheafError::Config(
                    "the selection anchor's occurrence is not tracked by this query's scope".into(),
                )),
                _ => Err(SheafError::Config(
                    "the selection anchor matches several tracked occurrences".into(),
                )),
            }
        }
        GrepAnchor::Episode { episode_id } => Ok(episode_id.clone()),
    }
}

/// Publish one finalized record to the report and, when streaming, to the
/// caller's sink. The push and the callback never diverge: every record
/// in the final report is delivered to a streaming consumer exactly once,
/// in the same order the report stores it.
fn emit_record(report: &mut GrepReport, sink: &mut Option<GrepSink<'_>>, record: GrepStreamRecord) {
    report.usage.results += 1;
    match &record {
        GrepStreamRecord::Hit { hit } => report.hits.push((**hit).clone()),
        GrepStreamRecord::Event { event } => report.events.push(event.clone()),
    }
    if let Some(callback) = sink.as_mut() {
        callback(record);
    }
}

fn pruned_delta(point: &Point) -> usize {
    usize::from(point.pruned)
}

/// Applies the per-capture record cursor to one capture's deterministic record
/// batch: it drops the already-delivered prefix on a resume capture, stops at
/// the result-limit boundary within the batch, and only then delegates to
/// `emit_record`. A capture's records are otherwise emitted incrementally, so
/// streaming liveness is preserved for every non-boundary record.
struct BatchEmit {
    /// Records to drop at the head of this capture (resume prefix).
    skip: usize,
    /// Records already emitted from this capture (post-skip).
    emitted_in_capture: usize,
    /// Remaining result-budget headroom at capture entry.
    budget_left: usize,
    /// Set when a record could not be emitted because the budget is spent.
    overflow: bool,
}

impl BatchEmit {
    fn push(
        &mut self,
        report: &mut GrepReport,
        sink: &mut Option<GrepSink<'_>>,
        record: GrepStreamRecord,
    ) {
        if self.overflow {
            return;
        }
        if self.skip > 0 {
            // Part of the already-delivered prefix on a resume capture.
            self.skip -= 1;
            return;
        }
        if self.emitted_in_capture >= self.budget_left {
            self.overflow = true;
            return;
        }
        self.emitted_in_capture += 1;
        emit_record(report, sink, record);
    }
}

#[allow(clippy::too_many_arguments)]
fn process_point(
    source: &GrepSource,
    history: &mut HistoryView<'_>,
    req: &GrepRequest,
    needle: &str,
    fingerprint: &str,
    scope_names: &[String],
    all_renames: &[(String, String)],
    anchored: Option<&str>,
    point: &Point,
    state: &mut UnitState,
    is_baseline: bool,
    suppress: bool,
    progress_made: bool,
    ctx: &BudgetCtx,
    replay_charge: ReplayCharge,
    memo: &mut ScanMemo,
    report: &mut GrepReport,
    sink: &mut Option<GrepSink<'_>>,
    batch: &mut BatchEmit,
) -> Result<PointOutcome> {
    let capture = &point.capture;
    // Which paths this capture could contain the unit in. A scoped query reads
    // its key (or every historical name under `--follow`); an unscoped query
    // reads the touched text paths plus the tracked path for continuity.
    let scoped_paths: Vec<String> = match &req.path {
        Some(path) => {
            if req.follow {
                scope_names.to_vec()
            } else {
                vec![normalize_key(path)]
            }
        }
        None => Vec::new(),
    };

    // Performance: a capture only alters a path's content if it touched that
    // path. A scoped query therefore materializes a point only when the capture
    // touched a path the unit could live in — its content is otherwise
    // identical to the last one read, so state carries forward untouched. This
    // turns an O(all captures) fork walk into O(captures touching the scope).
    // (A baseline read at the lineage's first point handled `is_baseline`.)
    let touched_paths: std::collections::BTreeSet<&str> = capture
        .paths
        .iter()
        .filter(|p| !p.ends_with('/'))
        .map(String::as_str)
        .collect();
    // Performance short-circuit — sound ONLY for a scoped query. A scoped
    // query watches a fixed path set, so a capture touching none of it cannot
    // change the answer and is skipped. An UNSCOPED query must not use the
    // tracked-occurrence paths as a watch set: a capture that touches only a
    // brand-new file introduces an occurrence there, and skipping it because
    // it does not intersect an already-tracked path silently drops that
    // occurrence (the very defect this phase fixes). An unscoped query
    // therefore scans every capture that touched any text path; untouched
    // tracked occurrences still carry forward because their content is
    // unchanged and their state is preserved below.
    if !is_baseline && !scoped_paths.is_empty() {
        let watch: Vec<&str> = scoped_paths.iter().map(String::as_str).collect();
        let intersects = watch.iter().any(|w| {
            touched_paths.iter().any(|t| {
                *t == *w || t.starts_with(&format!("{w}/")) || w.starts_with(&format!("{t}/"))
            })
        });
        if !intersects {
            return Ok(PointOutcome::Done);
        }
    }

    // An unscoped query scans only the paths this capture touched: any path it
    // did not touch has byte-identical content, so its tracked occurrences are
    // carried forward untouched below without a re-read (which would force a
    // historical fork and defeat the cache's zero-fork invariant). A scoped
    // query always reads its fixed path set.
    // Every-capture observation covers untouched paths too: an occurrence
    // the capture did not touch still earns its `observed` record at this
    // point. Those paths re-enter the candidate set, answered from the
    // content memo without a historical fork; without every-capture they
    // carry forward silently.
    let observe_untouched = req.every_capture && scoped_paths.is_empty();
    let untouched_present: Vec<PresentUnit> = if scoped_paths.is_empty() && !observe_untouched {
        state
            .present
            .iter()
            .filter(|prev| !touched_paths.contains(prev.path.as_str()))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    let mut candidate_paths: Vec<String> = if scoped_paths.is_empty() {
        if is_baseline {
            // The lineage's first walked point enumerates the WHOLE tree:
            // occurrences in files this capture did not touch predate the
            // walk window, and their episodes must still enter the state or
            // later touches would introduce them with wrong origins (or miss
            // them entirely). Later points scan only touched paths.
            source.text_paths_at(history, &capture.frontier)?
        } else if observe_untouched {
            let mut paths: Vec<String> = touched_paths.iter().map(|s| s.to_string()).collect();
            paths.extend(state.present.iter().map(|unit| unit.path.clone()));
            paths
        } else {
            touched_paths.iter().map(|s| s.to_string()).collect()
        }
    } else {
        scoped_paths.clone()
    };
    candidate_paths.sort();
    candidate_paths.dedup();

    // Collect every occurrence in every candidate path at this frontier; the
    // episode reducer below decides continuity per occurrence. Each distinct
    // content version is scanned at most once per query: the identity probe
    // answers revisits from the memo before anything is loaded, and handles
    // are built lazily only for occurrences that emit.
    let mut matches: Vec<(String, ScannedMatch)> = Vec::new();
    for path in &candidate_paths {
        // Per-item budget check: a single capture with many (or large) touched
        // paths cannot overrun the wall-clock/byte budget. On a hit, discard
        // this capture's partial work (state stays at the previous capture) so
        // the returned cursor cleanly re-processes it. Suppressed replay is not
        // interrupted — its state must be complete. A page that has not yet
        // fully processed any point past its resume anchor must finish this one
        // regardless of budget, or a point whose first read alone exhausts the
        // budget would re-anchor at the previous capture and never advance.
        if !suppress && progress_made && ctx.tripped(&report.usage, replay_charge) {
            return Ok(PointOutcome::BudgetHit);
        }
        report.usage.historical_path_reads += 1;
        // Trigram pre-filter first: the cheapest possible outcome.
        // A stat-free mapping lookup gives the content hash; if the index
        // proves that hash cannot contain the needle, the visit is a provable
        // `Absent` with no stat, no read, no fork, and no scan. This is the
        // skip that makes a rare or absent needle's walk O(candidates) rather
        // than O(distinct-versions). Memoize it so a revisit is free too.
        if let Some(hash) = source.trigram_excluded_hash(&capture.frontier, path) {
            memo.insert(hash, ScanOutcome::Absent);
            report.usage.trigram_skipped += 1;
            continue;
        }
        // Content-identity probe next: when this query has already scanned
        // these exact bytes — under another capture, frontier, or path name —
        // the visit is answered without decompressing, forking, or searching.
        if let Some(hash) = source.peek_content_hash(&capture.frontier, path) {
            // Warm cross-query scan cache: an earlier query with the
            // same fingerprint may have already scanned this exact content
            // version. Seed the per-query memo from it so the read+scan is
            // skipped entirely — the daemon-resident win for repeated queries.
            if memo.get(&hash).is_none() {
                if let Some(outcome) = source.warm_scan_get(fingerprint, &hash) {
                    memo.insert(hash.clone(), outcome);
                }
            }
            if let Some(outcome) = memo.get(&hash) {
                match outcome {
                    // Too-large content cannot be answered from the memo:
                    // fall through to the full read and enumerate
                    // transiently, keeping memory bounded at CPU's expense.
                    ScanOutcome::TooLarge => {}
                    ScanOutcome::Binary => {
                        report.usage.content_dedup_hits += 1;
                        report.skipped_binary += 1;
                        continue;
                    }
                    ScanOutcome::Absent => {
                        report.usage.content_dedup_hits += 1;
                        continue;
                    }
                    ScanOutcome::Matches(scanned) => {
                        report.usage.content_dedup_hits += 1;
                        matches.extend(scanned.iter().cloned().map(|m| (path.clone(), m)));
                        continue;
                    }
                }
            }
        }
        let (content, cache_hit, identity) = source.path_at(history, &capture.frontier, path)?;
        match cache_hit {
            CacheHit::Miss => {}
            CacheHit::Memory => report.usage.historical_cache_hits += 1,
            CacheHit::Disk => report.usage.historical_disk_cache_hits += 1,
        }
        match content {
            HistoricalPathContent::Text(text) => {
                report.usage.materialized_bytes += text.len() as u64;
                let hash = identity.unwrap_or_else(|| sha256_hex(text.as_bytes()));
                if memo.get(&hash).is_some() {
                    // No row identity was peeksable, so the bytes loaded —
                    // but the scan itself was answered by the memo.
                    report.usage.content_dedup_hits += 1;
                }
                scan_once(memo, &hash, &text, needle, req.extent);
                // Publish the freshly scanned outcome to the warm cross-query
                // cache so a later query with this fingerprint reuses it.
                if let Some(outcome) = memo.get(&hash) {
                    source.warm_scan_put(fingerprint, &hash, outcome);
                }
                match memo.get(&hash) {
                    Some(ScanOutcome::Matches(scanned)) => {
                        matches.extend(scanned.iter().cloned().map(|m| (path.clone(), m)));
                    }
                    Some(ScanOutcome::TooLarge) => {
                        // Dense content past the memo cap: enumerate
                        // transiently (the vector is dropped after this
                        // capture's reconciliation) so memory stays bounded.
                        let scanned = enumerate_units(&text, needle, req.extent);
                        matches.extend(scanned.into_iter().map(|m| (path.clone(), m)));
                    }
                    _ => {}
                }
            }
            HistoricalPathContent::Binary { .. } => {
                report.skipped_binary += 1;
                if let Some(hash) = identity {
                    if memo.get(&hash).is_some() {
                        report.usage.content_dedup_hits += 1;
                    }
                    memo.insert(hash, ScanOutcome::Binary);
                }
            }
            HistoricalPathContent::Absent => {}
        }
    }
    // Fast path for the overwhelmingly common capture on a rare/absent query:
    // this capture introduced no occurrence AND no occurrence was live coming
    // in, so there is nothing to reconcile, carry, or remove — the lineage
    // state is unchanged and no record is produced. Skipping the reconciliation
    // machinery (rename filter, two-phase correspondence, per-match vectors)
    // here is what keeps an unscoped whole-history walk near-linear once the
    // trigram filter has excluded the content: on a 10k-capture store a rare
    // needle takes this branch ~9,980 times. Every-capture observation and an
    // active occurrence anchor still need the full path below.
    if matches.is_empty() && state.present.is_empty() && !observe_untouched && anchored.is_none() {
        return Ok(PointOutcome::Done);
    }
    matches.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.match_range.start.cmp(&b.1.match_range.start))
            .then(a.1.match_range.end.cmp(&b.1.match_range.end))
    });
    if matches.len() > SCAN_MEMO_MAX_UNITS * 16 {
        // Dense-capture memory guard: reconciliation below allocates
        // claimed/next/pending vectors proportional to this count. Stop at
        // the boundary instead; see PointOutcome::DenseCapture.
        return Ok(PointOutcome::DenseCapture);
    }
    // Rename continuity requires a recorded rename edge whose two endpoints
    // were both touched by this capture. The cumulative rename list is a
    // document-wide constant, so it is read ONCE before the walk and passed
    // in as `all_renames`; reading it per capture re-scanned and re-parsed the
    // whole (history-length) tree-events list at every point — an O(captures ×
    // events) quadratic that dominated a long-history walk. Here we only
    // filter that precomputed list to this capture's touched endpoints.
    let renames: Vec<(String, String)> = all_renames
        .iter()
        .filter(|(from, to)| {
            let from = normalize_key(from);
            let to = normalize_key(to);
            touched_paths.contains(from.as_str()) && touched_paths.contains(to.as_str())
        })
        .cloned()
        .collect();
    // Snapshot the pre-capture live set. A mid-capture budget abort must
    // restore it: the cursor re-anchors BEFORE this capture, so its lineage
    // state must read exactly as it did entering the capture. `prior` itself
    // is consumed by the two-phase reconciliation below, so the restore reads
    // from this clone. Only captures that reach reconciliation (non-empty
    // matches or live priors — the fast path skipped the empty ones) pay it.
    let prior_snapshot = state.present.clone();
    let prior = std::mem::take(&mut state.present);
    let mut claimed = vec![false; matches.len()];
    let mut next: Vec<PresentUnit> = Vec::with_capacity(matches.len() + untouched_present.len());
    // One capture's records buffer, emitted as a sorted deterministic batch
    // after reconciliation so the normative total order holds regardless of
    // reconciliation sequence.
    let mut pending: Vec<PendingRecord> = Vec::new();

    // Occurrences in paths this capture did not touch are unchanged: carry
    // them forward verbatim so they neither re-emit nor read as removed. Only
    // occurrences in touched paths are reconciled against the fresh scan.
    let untouched_paths: std::collections::BTreeSet<&str> =
        untouched_present.iter().map(|u| u.path.as_str()).collect();
    next.extend(untouched_present.iter().cloned());

    // Two-phase correspondence (fail-closed uniqueness): a prior episode
    // continues ONLY across a proven-unique edge — exactly one current
    // occurrence with identical selected bytes and before/after context on
    // a continuable path, and that occurrence compatible with no other
    // prior. Phase A computes every prior's exact set WITHOUT claiming; a
    // greedy first-come claim would let path order decide identity when two
    // priors match one candidate, continuing one episode on unproven
    // evidence. Contested priors all report ambiguity instead.
    let path_matches = |prev_path: &str, candidate_path: &str| {
        candidate_path == prev_path
            || renames.iter().any(|(from, to)| {
                normalize_key(from) == prev_path && normalize_key(to) == candidate_path
            })
    };
    let touched_priors: Vec<PresentUnit> = prior
        .into_iter()
        .filter(|prev| !untouched_paths.contains(prev.path.as_str()))
        .collect();
    let mut exact_sets: Vec<Vec<usize>> = Vec::with_capacity(touched_priors.len());
    for prev in &touched_priors {
        // Reconciliation is O(prior x matches) on pathological captures; a
        // periodic budget sample keeps a dense capture from monopolizing the
        // page. Aborting here discards this capture's partial state — the
        // cursor re-anchors before it and the next page rebuilds.
        if exact_sets.len() % 1024 == 0
            && !exact_sets.is_empty()
            && !suppress
            && progress_made
            && ctx.tripped(&report.usage, replay_charge)
        {
            // Restore the pre-capture live set before aborting so the cursor
            // re-anchors with the lineage state it had ENTERING this capture,
            // not the torn empty map. Without this the cursor-state cache (and
            // the in-process resume) would carry a lineage that silently
            // dropped its live occurrences. Clone because a second budget
            // abort below may also need the snapshot.
            state.present = prior_snapshot.clone();
            return Ok(PointOutcome::BudgetHit);
        }
        let exact: Vec<usize> = matches
            .iter()
            .enumerate()
            .filter(|(_, (path, scanned))| {
                path_matches(&prev.path, path)
                    && scanned.selected_sha256 == prev.selected_sha256
                    && scanned.before_sha256 == prev.before_sha256
                    && scanned.after_sha256 == prev.after_sha256
            })
            .map(|(idx, _)| idx)
            .collect();
        exact_sets.push(exact);
    }
    let mut suitors = vec![0usize; matches.len()];
    for exact in &exact_sets {
        for idx in exact {
            suitors[*idx] += 1;
        }
    }
    for (i, prev) in touched_priors.into_iter().enumerate() {
        let exact = &exact_sets[i];
        let unique = exact.len() == 1 && suitors[exact[0]] == 1;
        if !unique {
            // Candidates for the ambiguity diagnostic: every current
            // occurrence with exact selected bytes on a path this episode
            // could continue on, in (path, byte range) order — reported,
            // never linked by that ordering.
            let mut raw_candidates: Vec<(usize, &str, ByteRange)> = matches
                .iter()
                .enumerate()
                .filter(|(_, (path, scanned))| {
                    path_matches(&prev.path, path)
                        && scanned.selected_sha256 == prev.selected_sha256
                })
                .map(|(idx, (path, scanned))| (idx, path.as_str(), scanned.match_range))
                .collect();
            raw_candidates.sort_by(|a, b| a.1.cmp(b.1).then(a.2.start.cmp(&b.2.start)));
            let ambiguous = !raw_candidates.is_empty();
            // An anchored walk reports only the followed episode's terminal
            // event; other episodes' records stay internal state.
            if !suppress && (anchored.is_none() || anchored == Some(prev.episode_id.as_str())) {
                let candidates = if ambiguous {
                    Some(
                        raw_candidates
                            .iter()
                            .map(|(idx, path, _)| {
                                matches[*idx]
                                    .1
                                    .handle(
                                        &capture.frontier,
                                        &capture.id,
                                        path,
                                        req.extent,
                                        &report.query_fingerprint,
                                    )
                                    .1
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    None
                };
                let event = GrepEvent {
                    capture_id: capture.id.clone(),
                    frontier: capture.frontier.clone(),
                    timestamp_ms: capture.timestamp_ms,
                    lineage_id: point.lineage_id.clone(),
                    on_current: point.on_current,
                    kind: if ambiguous {
                        LifecycleKind::Ambiguous
                    } else {
                        LifecycleKind::Removed
                    },
                    path: Some(prev.path.clone()),
                    last_present_handle_id: Some(prev.handle_id.clone()),
                    episode_id: Some(prev.episode_id.clone()),
                    candidates,
                };
                pending.push(PendingRecord {
                    rank: record_rank(event.kind),
                    path: prev.path.clone(),
                    start: prev.match_range.start,
                    end: prev.match_range.end,
                    stable_id: prev.handle_id.clone(),
                    record: GrepStreamRecord::Event { event },
                });
            }
            continue;
        }

        let idx = exact[0];
        claimed[idx] = true;
        let (path, scanned) = &matches[idx];
        let (handle, handle_id) = scanned.handle(
            &capture.frontier,
            &capture.id,
            path,
            req.extent,
            &report.query_fingerprint,
        );
        let renamed = path != &prev.path;
        let relocated = !renamed && scanned.match_range != prev.match_range;
        let changed = scanned.line_sha256 != prev.line_sha256;
        let kind = if renamed {
            Some(LifecycleKind::Renamed)
        } else if relocated {
            Some(LifecycleKind::Relocated)
        } else if changed {
            Some(LifecycleKind::Changed)
        } else if req.every_capture {
            Some(LifecycleKind::Observed)
        } else {
            None
        };
        next.push(PresentUnit {
            path: path.clone(),
            match_range: scanned.match_range,
            selected_sha256: scanned.selected_sha256.clone(),
            before_sha256: scanned.before_sha256.clone(),
            after_sha256: scanned.after_sha256.clone(),
            line_sha256: scanned.line_sha256.clone(),
            handle_id: handle_id.clone(),
            episode_id: prev.episode_id.clone(),
        });
        if !suppress
            && kind.is_some_and(hit_kind_is_present)
            && anchored.is_none_or(|id| id == prev.episode_id)
        {
            let occurrence_id = scanned.occurrence_id(
                &capture.frontier,
                &capture.id,
                path,
                &report.query_fingerprint,
            );
            let hit = GrepHit {
                capture_id: capture.id.clone(),
                frontier: capture.frontier.clone(),
                timestamp_ms: capture.timestamp_ms,
                lineage_id: point.lineage_id.clone(),
                on_current: point.on_current,
                path: path.clone(),
                kind: kind.expect("present kind"),
                line: scanned.line,
                column: scanned.column,
                occurrence_id: occurrence_id.clone(),
                episode_id: Some(prev.episode_id.clone()),
                preview: scanned.preview.clone(),
                handle,
                handle_id,
            };
            pending.push(PendingRecord {
                rank: record_rank(hit.kind),
                path: path.clone(),
                start: scanned.match_range.start,
                end: scanned.match_range.end,
                stable_id: occurrence_id,
                record: GrepStreamRecord::Hit { hit: Box::new(hit) },
            });
        }
    }

    for (idx, (path, scanned)) in matches.into_iter().enumerate() {
        if idx % 1024 == 0
            && idx > 0
            && !suppress
            && progress_made
            && ctx.tripped(&report.usage, replay_charge)
        {
            // Same restore as the reconciliation-phase abort: the cursor
            // re-anchors before this capture, so its lineage state must read
            // as it did entering the capture rather than the torn/partial map.
            state.present = prior_snapshot.clone();
            return Ok(PointOutcome::BudgetHit);
        }
        if claimed[idx] {
            continue;
        }
        let (handle, handle_id) = scanned.handle(
            &capture.frontier,
            &capture.id,
            &path,
            req.extent,
            &report.query_fingerprint,
        );
        let episode_id = episode_id(
            &point.episode_lineage,
            &capture.id,
            &path,
            scanned.match_range,
        );
        let occurrence_id = scanned.occurrence_id(
            &capture.frontier,
            &capture.id,
            &path,
            &report.query_fingerprint,
        );
        next.push(PresentUnit {
            path: path.clone(),
            match_range: scanned.match_range,
            selected_sha256: scanned.selected_sha256.clone(),
            before_sha256: scanned.before_sha256.clone(),
            after_sha256: scanned.after_sha256.clone(),
            line_sha256: scanned.line_sha256.clone(),
            handle_id,
            episode_id: episode_id.clone(),
        });
        if !suppress && anchored.is_none_or(|id| id == episode_id) {
            let hit = GrepHit {
                capture_id: capture.id.clone(),
                frontier: capture.frontier.clone(),
                timestamp_ms: capture.timestamp_ms,
                lineage_id: point.lineage_id.clone(),
                on_current: point.on_current,
                path,
                kind: LifecycleKind::Introduced,
                line: scanned.line,
                column: scanned.column,
                occurrence_id: occurrence_id.clone(),
                episode_id: Some(episode_id),
                preview: scanned.preview.clone(),
                handle,
                handle_id: next.last().expect("new episode").handle_id.clone(),
            };
            pending.push(PendingRecord {
                rank: record_rank(hit.kind),
                path: hit.path.clone(),
                start: scanned.match_range.start,
                end: scanned.match_range.end,
                stable_id: occurrence_id,
                record: GrepStreamRecord::Hit { hit: Box::new(hit) },
            });
        }
    }
    next.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then(a.match_range.start.cmp(&b.match_range.start))
    });
    state.present = next;
    // The capture's records emit as one deterministically sorted batch: kind
    // rank, then path, byte range, and stable ID (the normative total order). The
    // batch is a pure function of prior state plus capture content, so a
    // partial-capture cursor regenerates it exactly.
    pending.sort_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.start.cmp(&b.start))
            .then_with(|| a.end.cmp(&b.end))
            .then_with(|| a.stable_id.cmp(&b.stable_id))
    });
    for record in pending {
        batch.push(report, sink, record.record);
    }
    Ok(PointOutcome::Done)
}

/// Rank in the normative within-capture record order: what ended first
/// (removals), then what could not be uniquely continued (ambiguity
/// diagnostics), then what exists after the capture (transitions and
/// introductions).
fn record_rank(kind: LifecycleKind) -> u8 {
    match kind {
        LifecycleKind::Removed => 0,
        LifecycleKind::Ambiguous => 1,
        _ => 2,
    }
}

/// One buffered record of a capture's deterministic batch plus its sort key
/// in the normative total order.
struct PendingRecord {
    rank: u8,
    path: String,
    start: usize,
    end: usize,
    stable_id: String,
    record: GrepStreamRecord,
}

fn episode_id(lineage_key: &str, capture_id: &str, path: &str, range: ByteRange) -> String {
    let canonical = serde_json::json!({
        "lineage_key": lineage_key,
        "origin_capture_id": capture_id,
        "origin_path": path,
        "origin_start": range.start,
        "origin_end": range.end,
    });
    let mut bytes = b"sheaf:grep-episode:v1\0".to_vec();
    bytes.extend(serde_json::to_vec(&canonical).expect("episode identity serializes"));
    format!("ep1:{}", &sha256_hex(&bytes)[..16])
}

fn normalize_key(path: &str) -> String {
    path.trim_start_matches("./").replace('\\', "/")
}

/// Resolve a cursor's full ID or unique >=6-character prefix inside this exact
/// query walk. Ambiguous prefixes fail instead of silently resuming at the
/// oldest matching capture and duplicating output.
fn resolve_cursor_capture(points: &[Point], cursor_id: &str) -> Result<String> {
    let mut matches = points.iter().filter(|point| {
        point.capture.id == cursor_id
            || (cursor_id.len() >= 6 && point.capture.id.starts_with(cursor_id))
    });
    let Some(first) = matches.next() else {
        return Err(SheafError::BadCursor(
            "cursor capture does not resolve within this query's range".into(),
        ));
    };
    if matches.next().is_some() {
        return Err(SheafError::BadCursor(
            "cursor capture prefix is ambiguous within this query's range".into(),
        ));
    }
    Ok(first.capture.id.clone())
}

impl ProjectStore {
    /// Run a grep query and collect its full report (non-streaming).
    pub fn grep(&self, req: &GrepRequest) -> Result<GrepReport> {
        let mut none: Option<GrepSink<'_>> = None;
        self.grep_streaming(req, &mut none)
    }

    /// Run a query, invoking `sink` once per finalized hit/event as the
    /// walk produces it (walk order), then returning the authoritative
    /// report. The report contains exactly the streamed records.
    pub fn grep_streaming(
        &self,
        req: &GrepRequest,
        sink: &mut Option<GrepSink<'_>>,
    ) -> Result<GrepReport> {
        let trigram = (req.mode == GrepMode::History)
            .then(|| {
                self.grep_content_cache
                    .borrow_mut()
                    .trigram_filter(req.query.needle())
            })
            .flatten();
        let source = GrepSource {
            doc: &self.doc,
            ledger: &self.ledger,
            current: self.materialized_frontiers(),
            warm_content: Some(&self.grep_content_cache),
            trigram,
        };
        run(&source, req, false, sink)
    }

    /// Explicitly backfill (or, with `opts.rebuild`, rebuild from scratch)
    /// the derived grep cache. Idempotent by row-level completeness: a
    /// capture whose every touched path already has a durable row
    /// publishes nothing, so a second run on a complete store is a no-op.
    ///
    /// Rows come from authoritative historical materialization, never from
    /// the cache itself; the DAG walk (current lineage, plus divergent
    /// branches under `all`) decides which captures exist. The watermark
    /// advances only along contiguous coverage, so an interrupted run
    /// resumes from the last durably covered capture.
    pub fn grep_cache_backfill(&self, opts: GrepBackfillOptions) -> Result<GrepBackfillReport> {
        let started = std::time::Instant::now();
        let mut cache = self.grep_content_cache.borrow_mut();
        if opts.rebuild {
            cache.wipe();
        }
        let generation = cache.next_generation;
        let mut report = GrepBackfillReport {
            root: self.root.display().to_string(),
            rebuilt: opts.rebuild,
            all: opts.all,
            complete: true,
            generation,
            captures_examined: 0,
            captures_skipped: 0,
            captures_indexed: 0,
            captures_failed: 0,
            rows_written: 0,
            content_blobs_written: 0,
            watermark: cache.watermark.clone(),
            trigram_index_bytes: 0,
            elapsed_ms: 0,
        };

        // Walk 1: the current lineage, oldest → newest. Coverage is
        // positional and contiguous — exactly the watermark's invariant.
        let current = self.materialized_frontiers();
        let mut lineage = captures_from(&self.doc, &self.ledger, &current, None, None, usize::MAX)?;
        lineage.reverse();
        let lineage_frontiers: std::collections::BTreeSet<String> =
            lineage.iter().map(|c| c.frontier.clone()).collect();

        let mut history = HistoryView::open(&self.doc)?;
        // Coverage is the contiguous prefix of complete captures. A hole
        // (materialization failure) breaks it permanently for this walk —
        // a watermark must never name a prefix with a gap in it.
        let mut covered: Option<GrepCacheWatermark> = None;
        let mut chain_intact = true;
        let mut indexed_this_run: u32 = 0;
        let mut limit_hit = false;

        for (position, capture) in lineage.iter().enumerate() {
            if budget_tripped(&started, opts.max_elapsed_ms) {
                limit_hit = true;
                break;
            }
            report.captures_examined += 1;
            if cache.capture_is_indexed(capture) {
                report.captures_skipped += 1;
                if chain_intact {
                    covered = Some(chain_through(capture, position, generation));
                }
                continue;
            }
            if opts.limit.is_some_and(|limit| indexed_this_run >= limit) {
                limit_hit = true;
                break;
            }
            match self.backfill_capture(&mut cache, &mut history, capture) {
                Ok((rows, blobs)) => {
                    report.captures_indexed += 1;
                    report.rows_written += rows;
                    report.content_blobs_written += blobs;
                    indexed_this_run += 1;
                    if chain_intact {
                        let next = chain_through(capture, position, generation);
                        covered = Some(next.clone());
                        if indexed_this_run as usize % GREP_BACKFILL_WATERMARK_EVERY == 0
                            && cache
                                .watermark
                                .as_ref()
                                .is_none_or(|w| !same_chain(w, &next))
                        {
                            if let Err(error) = cache.store_watermark(&next) {
                                tracing::warn!(%error, "grep cache watermark write failed");
                            } else {
                                cache.watermark = Some(next);
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        capture = capture.id,
                        "grep cache backfill could not materialize a capture"
                    );
                    report.captures_failed += 1;
                    // Later captures can still receive valid rows, but the
                    // coverage chain is broken until a clean run rebuilds
                    // it end to end.
                    chain_intact = false;
                    covered = None;
                }
            }
        }

        // Walk 2 (only under `all`): divergent-branch captures the lineage
        // walk cannot reach. Rows only — the watermark tracks the current
        // lineage, and branch rows are keyed by immutable frontier anyway.
        if opts.all {
            let mut union = captures_from(
                &self.doc,
                &self.ledger,
                &self.doc.oplog_frontiers(),
                None,
                None,
                usize::MAX,
            )?;
            union.reverse();
            for capture in &union {
                if lineage_frontiers.contains(&capture.frontier) {
                    continue;
                }
                if budget_tripped(&started, opts.max_elapsed_ms) {
                    limit_hit = true;
                    break;
                }
                report.captures_examined += 1;
                if cache.capture_is_indexed(capture) {
                    report.captures_skipped += 1;
                    continue;
                }
                if opts.limit.is_some_and(|limit| indexed_this_run >= limit) {
                    limit_hit = true;
                    break;
                }
                match self.backfill_capture(&mut cache, &mut history, capture) {
                    Ok((rows, blobs)) => {
                        report.captures_indexed += 1;
                        report.rows_written += rows;
                        report.content_blobs_written += blobs;
                        indexed_this_run += 1;
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            capture = capture.id,
                            "grep cache backfill could not materialize a branch capture"
                        );
                        report.captures_failed += 1;
                    }
                }
            }
        }

        // Persist the final chain position if it moved (or was never
        // durable). A crash before this write only loses the marker: rows
        // are already durable and the next backfill rediscovers coverage
        // by completeness.
        if let Some(wm) = &covered {
            if cache.watermark.as_ref().is_none_or(|w| !same_chain(w, wm)) {
                if let Err(error) = cache.store_watermark(wm) {
                    tracing::warn!(%error, "grep cache watermark write failed");
                } else {
                    cache.watermark = Some(wm.clone());
                }
            }
        }
        report.watermark = cache.watermark.clone();
        // Rebuild the trigram pre-filter once on the terminal page. Rebuilding
        // the whole distinct-content corpus on every bounded daemon page makes
        // first backfill O(pages × corpus). Intermediate pages retain the old
        // index (or none); newly written hashes are uncovered and therefore
        // scanned, so delaying publication changes performance only.
        report.trigram_index_bytes = if limit_hit {
            super::grep_trigram::index_size(&cache.index_dir)
        } else {
            match cache.rebuild_trigram_index() {
                Ok(size) => size,
                Err(error) => {
                    tracing::warn!(%error, "timeline grep trigram index write skipped");
                    // A stale index remains sound only while every covered hash
                    // retains identical bytes, but removing it is simpler and
                    // preserves the stronger corruption contract: publication
                    // failure means no filter, never old coverage.
                    super::grep_trigram::remove_index(&cache.index_dir);
                    0
                }
            }
        };
        // The derived state just changed on disk; drop resident warm caches so
        // the next query reloads the fresh trigram index and re-derives scans.
        cache.bump_warm_generation();
        report.complete = !limit_hit && report.captures_failed == 0;
        report.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(report)
    }

    /// Materialize and publish the rows one capture is missing. Returns
    /// `(rows_written, content_blobs_written)`; an error means at least
    /// one path could not be materialized at all (nothing published for
    /// it; the capture stays incomplete).
    fn backfill_capture(
        &self,
        cache: &mut GrepContentCache,
        history: &mut HistoryView<'_>,
        capture: &Capture,
    ) -> Result<(usize, usize)> {
        let decoded = decode_frontier(&capture.frontier)?;
        let mut rows = Vec::new();
        for path in &capture.paths {
            let key = (capture.frontier.clone(), path.clone());
            if cache.mappings.contains_key(&key) {
                continue;
            }
            let content = history.path_at(&decoded, path)?;
            rows.push((key, content));
        }
        Ok(cache.publish_rows(rows))
    }
}

/// True once the run's soft wall-clock budget is exhausted. Checked
/// between captures only; the capture in flight always completes so a
/// page never tears its own row batch.
fn budget_tripped(started: &std::time::Instant, max_elapsed_ms: Option<u64>) -> bool {
    max_elapsed_ms.is_some_and(|budget| started.elapsed().as_millis() as u64 >= budget)
}

/// The watermark naming `capture` at 0-based walk `position` — the chain's
/// next link. Callers only invoke it while the chain is intact; a hole
/// means no watermark at all rather than one naming a gap.
fn chain_through(capture: &Capture, position: usize, generation: u64) -> GrepCacheWatermark {
    GrepCacheWatermark {
        v: GREP_CACHE_SCHEMA,
        generation,
        captures_indexed: position + 1,
        through_capture_id: capture.id.clone(),
        through_frontier: capture.frontier.clone(),
        updated_ms: chrono::Utc::now().timestamp_millis(),
    }
}

/// Same coverage position (everything but the timestamp), so an unchanged
/// chain is not rewritten on idempotent re-runs.
fn same_chain(a: &GrepCacheWatermark, b: &GrepCacheWatermark) -> bool {
    a.generation == b.generation
        && a.captures_indexed == b.captures_indexed
        && a.through_capture_id == b.through_capture_id
        && a.through_frontier == b.through_frontier
}

/// Filesystem facts about the derived grep cache for doctor's advisory
/// report. Read-only; nothing here can fail the integrity sweep because
/// the cache is disposable derived state.
pub(crate) fn grep_cache_facts(root: &std::path::Path) -> GrepCacheFacts {
    let index_dir = crate::config::sheaf_dir(root).join("state/cache/grep-v1");
    let mut facts = GrepCacheFacts {
        present: index_dir.is_dir(),
        ..Default::default()
    };
    if !facts.present {
        return facts;
    }

    let mut referenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    if let Ok(bytes) = std::fs::read(index_dir.join("mappings.jsonl")) {
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_slice::<CacheMapping>(line) {
                Ok(record)
                    if record.v == GREP_CACHE_SCHEMA
                        && !record.frontier.is_empty()
                        && !record.path.is_empty() =>
                {
                    facts.rows += 1;
                    if let CacheMappingValue::Text { hash, .. } = record.value {
                        referenced.insert(hash);
                    }
                }
                _ => facts.torn_lines += 1,
            }
        }
    }

    if let Ok(raw) = std::fs::read_to_string(index_dir.join("watermark.json")) {
        match serde_json::from_str::<GrepCacheWatermark>(&raw) {
            Ok(watermark) if watermark.v == GREP_CACHE_SCHEMA => {
                facts.watermark = Some(watermark);
            }
            _ => facts.watermark_unparseable = true,
        }
    }

    let content_dir = index_dir.join("content");
    if let Ok(rd) = std::fs::read_dir(&content_dir) {
        for file in rd.flatten() {
            let name = file.file_name().to_string_lossy().into_owned();
            let len = file.metadata().map(|m| m.len()).unwrap_or(0);
            facts.content_files += 1;
            facts.content_bytes += len;
            let hash = name.strip_suffix(".zst").unwrap_or(&name).to_owned();
            if !referenced.contains(&hash) {
                facts.orphan_content_files += 1;
                facts.orphan_content_bytes += len;
            }
        }
    }
    facts.missing_content = referenced
        .iter()
        .filter(|hash| !content_dir.join(format!("{hash}.zst")).is_file())
        .count();

    facts.trigram_index_bytes = super::grep_trigram::index_size(&index_dir);
    if facts.trigram_index_bytes > 0 && super::grep_trigram::load_index(&index_dir).is_none() {
        facts.trigram_index_corrupt = true;
    }
    facts
}

impl TimelineReader {
    /// Run a grep query in read-only degraded mode and collect its report.
    pub fn grep(&self, req: &GrepRequest) -> Result<GrepReport> {
        let mut none: Option<GrepSink<'_>> = None;
        self.grep_streaming(req, &mut none)
    }

    /// Every tracked text path at one retained point, sorted: the
    /// independent enumeration the occurrence-history reference reducer
    /// compares against (the per-path read itself is
    /// [`TimelineReader::historical_path_content`]).
    pub fn historical_text_paths(&self, reference: &str) -> Result<Vec<String>> {
        let point = self.resolve(reference)?;
        let frontier = decode_frontier(&point.frontier)?;
        if self.doc().frontiers_to_vv(&frontier).is_none() {
            return Err(SheafError::TimelineReference(
                "target point is not part of this store's history".into(),
            ));
        }
        let mut history = HistoryView::open(self.doc())?;
        let mut paths = history.text_keys_at(&frontier)?;
        paths.sort();
        Ok(paths)
    }

    /// The store's cumulative rename map: (old, new) pairs as recorded.
    pub fn recorded_renames(&self) -> Vec<(String, String)> {
        super::timeline::read_renames(self.doc())
    }

    /// Degraded twin of [`ProjectStore::grep_streaming`]: same records,
    /// same order, same report; only the degraded marker differs.
    pub fn grep_streaming(
        &self,
        req: &GrepRequest,
        sink: &mut Option<GrepSink<'_>>,
    ) -> Result<GrepReport> {
        let trigram = (req.mode == GrepMode::History)
            .then(|| {
                self.grep_content_cache
                    .borrow_mut()
                    .trigram_filter(req.query.needle())
            })
            .flatten();
        let source = GrepSource {
            doc: self.doc(),
            ledger: self.ledger(),
            current: self.materialized_frontiers(),
            warm_content: Some(&self.grep_content_cache),
            trigram,
        };
        run(&source, req, true, sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    fn point(id: &str) -> Point {
        Point {
            capture: Capture {
                id: id.to_owned(),
                frontier: String::new(),
                parent_frontier: String::new(),
                timestamp_ms: 0,
                paths: Vec::new(),
                events: 0,
                checkpoints: Vec::new(),
                origin: None,
                on_current: true,
            },
            lineage_id: "current".to_owned(),
            on_current: true,
            pruned: false,
            episode_lineage: "current".to_owned(),
        }
    }

    #[test]
    fn cursor_resolution_is_exact_unique_or_fails_closed() {
        let points = [
            point("aaaaaa1111"),
            point("aaaaaa2222"),
            point("bbbbbb3333"),
        ];
        // A full id resolves to itself.
        assert_eq!(
            resolve_cursor_capture(&points, "aaaaaa1111").unwrap(),
            "aaaaaa1111"
        );
        // A unique >=6-char prefix resolves to its one capture.
        assert_eq!(
            resolve_cursor_capture(&points, "bbbbbb").unwrap(),
            "bbbbbb3333"
        );
        // A prefix shared by two captures is ambiguous, not first-wins.
        assert_eq!(
            resolve_cursor_capture(&points, "aaaaaa")
                .unwrap_err()
                .code(),
            "state.bad_cursor"
        );
        // A prefix matching nothing fails closed.
        assert_eq!(
            resolve_cursor_capture(&points, "cccccc")
                .unwrap_err()
                .code(),
            "state.bad_cursor"
        );
    }

    #[test]
    fn request_validation_covers_modes_extents_anchors_and_cursor_binding() {
        let parse = |value| -> GrepRequest { serde_json::from_value(value).unwrap() };
        let base = || parse(serde_json::json!({"query":{"kind":"literal","text":"needle"}}));
        assert!(base().validate().is_ok());
        let mut req = base();
        req.query = GrepQuery::literal("");
        assert!(req.validate().is_err());
        let mut req = base();
        req.budget.max_results = 0;
        assert!(req.validate().is_err());
        let mut req = base();
        req.budget.max_elapsed_ms = 0;
        assert!(req.validate().is_err());
        let mut req = base();
        req.extent = SelectionExtent::Hunk;
        assert!(req.validate().is_err());
        let mut req = base();
        req.follow = true;
        assert!(req.validate().is_err());
        let mut req = base();
        req.mode = GrepMode::Point;
        req.from = Some("@".into());
        assert!(req.validate().is_err());
        let mut req = base();
        req.anchor = Some(GrepAnchor::Coordinate {
            path: "a".into(),
            line: 1,
            column: None,
        });
        assert!(req.validate().is_err());
        let mut req = base();
        req.anchor = Some(GrepAnchor::Episode {
            episode_id: "e".into(),
        });
        req.at = Some("@".into());
        assert!(req.validate().is_err());
        let mut req = base();
        req.anchor = Some(GrepAnchor::Coordinate {
            path: "a".into(),
            line: 1,
            column: None,
        });
        req.at = Some("@".into());
        assert!(req.validate().is_ok());
        let mut req = base();
        req.cursor = Some(SearchCursor {
            query_fingerprint: "wrong".into(),
            after_capture_id: "x".into(),
            resume_capture_id: None,
            record_index: 0,
            path_index: 0,
            match_index: 0,
        });
        assert!(req.validate().is_err());
        assert!(GrepAnchor::Coordinate {
            path: "a".into(),
            line: 2,
            column: Some(3)
        }
        .identity()
        .contains("coordinate"));
    }

    #[test]
    fn scan_helpers_cover_match_and_line_extents_and_budget_windows() {
        for extent in [SelectionExtent::Match, SelectionExtent::Line] {
            let units = enumerate_units("one needle\ntwo needle\n", "needle", extent);
            assert_eq!(units.len(), 2);
            assert!(units[0].range.end >= units[0].match_range.end);
        }
        let window = scan_window(
            "a needle b needle c needle",
            "needle",
            SelectionExtent::Match,
            1,
            1,
            None,
        );
        assert_eq!(window.window.len(), 1);
        assert_eq!(window.skipped, 1);
        assert!(window.more);
        let dense = "needle ".repeat(1025);
        let stopped = scan_window(
            &dense,
            "needle",
            SelectionExtent::Match,
            0,
            2000,
            Some(std::time::Instant::now() - std::time::Duration::from_millis(1)),
        );
        assert!(stopped.timed_out);
        assert_eq!(match_line("a\nb", 2), "b");
        assert_eq!(normalize_key("./a\\b"), "a/b");
    }

    #[test]
    fn cache_loads_bad_rows_and_watermarks_as_disposable_state() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(".sheaf/state/cache/grep-v1");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("mappings.jsonl"), b"bad\n{\"v\":99}\n").unwrap();
        std::fs::write(dir.join("watermark.json"), b"not-json").unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        assert!(cache.mappings.is_empty());
        assert!(cache.watermark.is_none());
        cache.bump_warm_generation();
        assert_eq!(cache.warm.generation, 1);
        assert!(cache.trigram_filter("ab").is_none());
    }

    #[test]
    fn extents_preview_and_cache_retention_are_deterministic() {
        assert_eq!(
            extent_range("aa\nneedle\nzz", 3, 6, SelectionExtent::Line),
            ByteRange { start: 3, end: 9 }
        );
        assert_eq!(
            extent_range("needle", 0, 6, SelectionExtent::Match),
            ByteRange { start: 0, end: 6 }
        );
        let long = "é".repeat(150);
        let preview = preview_of(
            &long,
            ByteRange {
                start: 0,
                end: long.len(),
            },
        );
        assert!(preview.ends_with('…'));
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        cache.remember(
            ("f1".into(), "p".into()),
            HistoricalPathContent::Text("alpha".into()),
            Some("h1".into()),
        );
        cache.remember(
            ("f2".into(), "p".into()),
            HistoricalPathContent::Absent,
            None,
        );
        assert_eq!(cache.entries.len(), 2);
        cache.sweep_to_retained(&BTreeSet::from(["f1".into()]));
        assert!(cache.mappings.is_empty() || cache.entries.len() <= 2);
    }

    #[test]
    fn query_fingerprint_changes_with_scope_and_mode() {
        let base: GrepRequest =
            serde_json::from_value(serde_json::json!({"query":{"kind":"literal","text":"x"}}))
                .unwrap();
        let mut path = base.clone();
        path.path = Some("src".into());
        let mut history = base;
        history.mode = GrepMode::History;
        assert_ne!(path.fingerprint(), history.fingerprint());
    }
    #[test]
    fn scan_window_exhaustion_and_zero_take_are_explicit() {
        let exhausted = scan_window("x needle", "needle", SelectionExtent::Match, 0, 1, None);
        assert_eq!(exhausted.window.len(), 1);
        assert!(!exhausted.more);
        let zero = scan_window(
            "needle needle",
            "needle",
            SelectionExtent::Match,
            0,
            0,
            None,
        );
        assert!(zero.window.is_empty());
        assert!(zero.more);
    }

    #[test]
    fn scan_memo_records_absent_binary_and_dense_outcomes() {
        let mut memo = ScanMemo::default();
        scan_once(
            &mut memo,
            "absent",
            "nothing",
            "needle",
            SelectionExtent::Match,
        );
        assert!(matches!(memo.get("absent"), Some(ScanOutcome::Absent)));
        memo.insert("binary".into(), ScanOutcome::Binary);
        assert!(matches!(memo.get("binary"), Some(ScanOutcome::Binary)));
        let dense = "x".repeat(SCAN_MEMO_MAX_UNITS + 1);
        scan_once(&mut memo, "dense", &dense, "x", SelectionExtent::Match);
        assert!(matches!(memo.get("dense"), Some(ScanOutcome::TooLarge)));
        scan_once(
            &mut memo,
            "absent",
            "needle",
            "needle",
            SelectionExtent::Match,
        );
        assert!(matches!(memo.get("absent"), Some(ScanOutcome::Absent)));
    }

    #[test]
    fn history_at_without_occurrence_anchor_is_rejected() {
        let mut req: GrepRequest = serde_json::from_value(
            serde_json::json!({"query":{"kind":"literal","text":"x"},"mode":"history","at":"@"}),
        )
        .unwrap();
        assert!(
            matches!(req.validate(), Err(SheafError::Config(message)) if message.contains("occurrence anchor"))
        );
        req.at = None;
        assert!(req.validate().is_ok());
    }
    #[test]
    fn cache_content_corruption_is_a_miss_and_torn_tail_gets_separator() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        cache.insert(
            "frontier",
            "file.txt",
            HistoricalPathContent::Text("needle".into()),
        );
        let hash = sha256_hex(b"needle");
        let content = tmp
            .path()
            .join(".sheaf/state/cache/grep-v1/content")
            .join(format!("{hash}.zst"));
        std::fs::write(&content, b"not zstd").unwrap();
        let mut cold = GrepContentCache::open(tmp.path(), true);
        assert!(cold.get("frontier", "file.txt").is_none());

        let mappings = tmp.path().join(".sheaf/state/cache/grep-v1/mappings.jsonl");
        std::fs::write(&mappings, b"partial-json").unwrap();
        let mut torn = GrepContentCache::open(tmp.path(), true);
        torn.insert(
            "next",
            "file.txt",
            HistoricalPathContent::Text("fresh".into()),
        );
        let raw = std::fs::read_to_string(mappings).unwrap();
        assert!(raw.contains("partial-json\n"));
        assert!(raw.contains("\"frontier\":\"next\""));
    }

    #[test]
    fn cache_rebuild_skips_missing_and_hash_mismatched_content() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        cache.insert(
            "f",
            "good",
            HistoricalPathContent::Text("needle here".into()),
        );
        cache.mappings.insert(
            ("f".into(), "bad".into()),
            CacheMappingValue::Text {
                hash: "deadbeef".into(),
                bytes: 4,
            },
        );
        let size = cache.rebuild_trigram_index().unwrap();
        assert!(size > 0);
        std::fs::remove_file(cache.content_path(&sha256_hex(b"needle here"))).unwrap();
        assert_eq!(cache.rebuild_trigram_index().unwrap(), 0);
    }
    #[test]
    fn warm_caches_bound_entries_and_skip_oversized_values() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        let state = CursorState::default();
        cache.cursor_state_put("q", "a", state.clone());
        cache.cursor_state_put("q", "a", state.clone());
        assert!(cache.cursor_state_get("q", "a").is_some());
        let huge = CursorState {
            lineages: (0..10)
                .map(|i| (format!("{i}"), UnitState::default()))
                .collect(),
            seen_lineages: (0..10).map(|i| format!("{i}")).collect(),
        };
        cache.cursor_state_put("q", "huge", huge);
        cache.warm_scan_put("q", "h", &ScanOutcome::Absent);
        assert!(matches!(
            cache.warm_scan_get("q", "h"),
            Some(ScanOutcome::Absent)
        ));
        cache.warm_scan_put("q", "h", &ScanOutcome::Binary);
        assert!(matches!(
            cache.warm_scan_get("q", "h"),
            Some(ScanOutcome::Absent)
        ));
        cache.bump_warm_generation();
        assert!(cache.warm_scan_get("q", "h").is_none());
    }

    #[test]
    fn cache_sweep_removes_collected_rows_blobs_and_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = GrepContentCache::open(tmp.path(), true);
        cache.insert(
            "keep",
            "a",
            HistoricalPathContent::Text("keep needle".into()),
        );
        cache.insert(
            "drop",
            "b",
            HistoricalPathContent::Text("drop needle".into()),
        );
        let orphan = cache.content_path("orphan");
        std::fs::write(&orphan, b"orphan").unwrap();
        cache.watermark = Some(GrepCacheWatermark {
            v: GREP_CACHE_SCHEMA,
            generation: 3,
            captures_indexed: 1,
            through_capture_id: "c".into(),
            through_frontier: "drop".into(),
            updated_ms: 0,
        });
        cache
            .store_watermark(cache.watermark.as_ref().unwrap())
            .unwrap();
        cache.sweep_to_retained(&BTreeSet::from(["keep".into()]));
        assert!(cache.mappings.keys().all(|(f, _)| f == "keep"));
        assert!(!orphan.exists());
        assert!(cache.watermark.is_none());
        assert!(cache.next_generation >= 4);
    }
}

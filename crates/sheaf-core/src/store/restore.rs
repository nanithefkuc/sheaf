//! Worktree restore engine.
//!
//! Two modes over one materialization core:
//!
//! * **Full** — no path scope. The whole worktree is repositioned to the
//!   target point and `state/worktree.head` moves to that exact frontier via
//!   a Loro checkout. No operations are authored; the abandoned future stays
//!   addressable and the next capture appends concurrently.
//! * **Scoped** — only the selected paths/subtrees are materialized, and the
//!   result is recorded as one ordinary forward capture on the current
//!   lineage, so restored text re-enters history as char-level ops.
//!
//! Nothing here ever rewrites, trims, or reorders the log. Apply reconciles
//! the live worktree into history *before* touching a byte, so the state a
//! restore overwrites is always itself a capture — returned as the undo
//! reference.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write as _};
use std::path::{Component, Path, PathBuf};

use chrono::Utc;
use loro::{Frontiers, LoroDoc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::fsutil;
use super::selection::HistoricalPathContent;
use super::timeline::{decode_frontier, ResolvedPoint, TimelineReader};
use super::{
    blobs, state_dir, store_dir, Capture, CaptureOrigin, OriginKind, ProjectStore, BINARIES_MAP,
    FILES_MAP, MODES_EXEC, MODES_MAP,
};
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind, FsEvent};
use crate::ignore::IgnoreSet;

/// Restart marker; written and fsync'd before the first worktree mutation.
const INTENT_FILE: &str = "restore.intent";
/// Wire-side ceiling on `progress_log` entries.
const PROGRESS_LOG_LIMIT: usize = 200;
/// Staging directory for atomic installs. Lives inside the always-ignored
/// store directory so the watcher never observes a half-written payload.
pub(super) const STAGE_DIR: &str = "restore-stage";
fn restore_intent_path(root: &Path) -> PathBuf {
    match crate::config::worktree_id(root) {
        Some(id) => crate::config::worktree_head_path(root)
            .parent()
            .expect("managed worktree head has parent")
            .join(format!("{id}.{INTENT_FILE}")),
        None => state_dir(root).join(INTENT_FILE),
    }
}

// --------------------------------------------------------------- state view

/// The content half of one tracked path's state at some point in history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Content {
    Text(String),
    Binary { hash: String, size: u64 },
}

/// One tracked path's content at some point in history, plus the file mode
/// history records for it: `exec` is the only bit worth history,
/// everything else is umask noise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) content: Content,
    pub(super) exec: bool,
}

impl Entry {
    pub(super) fn text(s: String, exec: bool) -> Entry {
        Entry {
            content: Content::Text(s),
            exec,
        }
    }

    pub(super) fn binary(hash: String, size: u64, exec: bool) -> Entry {
        Entry {
            content: Content::Binary { hash, size },
            exec,
        }
    }

    pub(super) fn content_key(&self) -> ContentKind {
        match self.content {
            Content::Text(_) => ContentKind::Text,
            Content::Binary { .. } => ContentKind::Binary,
        }
    }

    pub(super) fn byte_len(&self) -> u64 {
        match &self.content {
            Content::Text(s) => s.len() as u64,
            Content::Binary { size, .. } => *size,
        }
    }

    pub(super) fn hash(&self) -> Option<&str> {
        match &self.content {
            Content::Text(_) => None,
            Content::Binary { hash, .. } => Some(hash),
        }
    }

    /// Identity used to pair a delete with a create as one rename.
    pub(super) fn identity(&self) -> String {
        match &self.content {
            Content::Text(s) => format!("t:{}", blobs::hash_of(s.as_bytes())),
            Content::Binary { hash, .. } => format!("b:{hash}"),
        }
    }
}

/// Recorded (document) exec flag for a key: the modes map holds `exec` for
/// executable paths and nothing for plain ones (git-style).
fn doc_exec(doc: &LoroDoc, key: &str) -> bool {
    doc.get_map(MODES_MAP)
        .get(key)
        .and_then(|v| v.get_deep_value().into_string().ok())
        .is_some_and(|s| s.as_str() == MODES_EXEC)
}

/// Tracked content of the document's currently materialized state.
pub(super) fn entries_of_state(doc: &LoroDoc) -> BTreeMap<String, Entry> {
    entries_of_state_scoped(doc, &[])
}

/// Tracked content of the materialized state restricted to `scope`.
///
/// Filtering before `LoroText::to_string` matters: copying every tracked text
/// file made a one-path diff scale with the whole project.
pub(super) fn entries_of_state_scoped(doc: &LoroDoc, scope: &[String]) -> BTreeMap<String, Entry> {
    let mut out = BTreeMap::new();
    doc.get_map(FILES_MAP).for_each(|key, value| {
        if !in_scope(key, scope) {
            return;
        }
        if let loro::ValueOrContainer::Container(loro::Container::Text(text)) = value {
            out.insert(
                key.to_string(),
                Entry::text(text.to_string(), doc_exec(doc, key)),
            );
        }
    });
    doc.get_map(BINARIES_MAP).for_each(|key, value| {
        if !in_scope(key, scope) {
            return;
        }
        let Ok(raw) = value.get_deep_value().into_string() else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return;
        };
        let Some(hash) = parsed["hash"].as_str() else {
            return;
        };
        out.insert(
            key.to_string(),
            Entry::binary(
                hash.to_owned(),
                parsed["size"].as_u64().unwrap_or(0),
                doc_exec(doc, key),
            ),
        );
    });
    out
}

/// Tracked content exactly as it stood at `frontier`. Reads through a fork so
/// the live document's materialized state is never disturbed.
pub(super) fn entries_at(doc: &LoroDoc, frontier: &Frontiers) -> Result<BTreeMap<String, Entry>> {
    if doc.frontiers_to_vv(frontier).is_none() {
        return Err(SheafError::TimelineReference(
            "target point is not part of this store's history".into(),
        ));
    }
    let mut view = HistoryView::open(doc)?;
    view.entries_at(frontier)
}

/// An independent read view over the version graph.
///
/// Forking the same point twice (entries read + rename scan, both sides of
/// a comparison) is pure waste, so the view memoizes one fork per distinct
/// frontier. Note the fork must target the point directly: a fork taken at
/// the tip and repositioned with `checkout` silently loses content of
/// containers deleted after that point (verified against loro 1.13), and
/// per-checkout state recomputation costs more than the fork it avoids.
pub(super) struct HistoryView<'a> {
    doc: &'a LoroDoc,
    forks: Vec<(Frontiers, LoroDoc)>,
    forks_created: u64,
}

impl<'a> HistoryView<'a> {
    pub(super) fn open(doc: &'a LoroDoc) -> Result<Self> {
        Ok(HistoryView {
            doc,
            forks: Vec::new(),
            forks_created: 0,
        })
    }

    fn fork_for(&mut self, frontier: &Frontiers) -> Result<&LoroDoc> {
        if let Some(index) = self.forks.iter().rposition(|(f, _)| f == frontier) {
            let last = self.forks.len() - 1;
            self.forks.swap(index, last);
            return Ok(&self.forks[last].1);
        }
        // Retention-trimmed (shallow) documents cannot fork_at — loro marks
        // it not-implemented — so reconstruct the point's state through a
        // state-only snapshot round-trip instead (probe-verified against
        // loro 1.13.9, including containers deleted after the point).
        let forked = match self.doc.fork_at(frontier) {
            Ok(forked) => forked,
            Err(_) if self.doc.is_shallow() => {
                let bytes = self
                    .doc
                    .export(loro::ExportMode::state_only(Some(frontier)))
                    .map_err(|e| SheafError::StoreCorrupt(format!("state export at point: {e}")))?;
                let scratch = loro::LoroDoc::new();
                scratch
                    .import(&bytes)
                    .map_err(|e| SheafError::StoreCorrupt(format!("state import at point: {e}")))?;
                scratch
            }
            Err(e) => {
                return Err(SheafError::StoreCorrupt(format!("fork at point: {e}")));
            }
        };
        self.forks.push((frontier.clone(), forked));
        self.forks_created += 1;
        // Two points cover every current caller (both sides of a diff or
        // plan); anything more would hold whole tree snapshots for nothing.
        while self.forks.len() > 2 {
            self.forks.remove(0);
        }
        Ok(&self.forks[self.forks.len() - 1].1)
    }

    /// Number of whole-document historical materializations this view created.
    /// Reusing a frontier does not increment the count.
    pub(super) fn forks_created(&self) -> u64 {
        self.forks_created
    }

    /// Tracked content of the document state exactly at `frontier`.
    pub(super) fn entries_at(&mut self, frontier: &Frontiers) -> Result<BTreeMap<String, Entry>> {
        self.entries_at_scoped(frontier, &[])
    }

    /// Historical entries restricted before text containers are copied.
    pub(super) fn entries_at_scoped(
        &mut self,
        frontier: &Frontiers,
        scope: &[String],
    ) -> Result<BTreeMap<String, Entry>> {
        if self.doc.frontiers_to_vv(frontier).is_none() {
            return Err(SheafError::TimelineReference(
                "target point is not part of this store's history".into(),
            ));
        }
        Ok(entries_of_state_scoped(self.fork_for(frontier)?, scope))
    }

    /// One path exactly at `frontier`, without materializing the whole tree
    /// into a `BTreeMap`. A shallow document still pays the verified
    /// state-only snapshot round trip in `fork_for`, but query cost after that
    /// is proportional to the selected container rather than project size.
    pub(super) fn path_at(
        &mut self,
        frontier: &Frontiers,
        key: &str,
    ) -> Result<HistoricalPathContent> {
        if self.doc.frontiers_to_vv(frontier).is_none() {
            return Err(SheafError::TimelineReference(
                "target point is not part of this store's history".into(),
            ));
        }
        let doc = self.fork_for(frontier)?;
        if let Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) =
            doc.get_map(FILES_MAP).get(key)
        {
            return Ok(HistoricalPathContent::Text(text.to_string()));
        }
        if let Some(value) = doc.get_map(BINARIES_MAP).get(key) {
            let raw = value.get_deep_value().into_string().map_err(|_| {
                SheafError::StoreCorrupt(format!("binary metadata for `{key}` is not text"))
            })?;
            let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
                SheafError::StoreCorrupt(format!("binary metadata for `{key}`: {e}"))
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

    /// Root-relative keys of every tracked text container present at
    /// `frontier`, sorted. Used by unscoped timeline grep when a capture's
    /// touched-path list is unavailable.
    pub(super) fn text_keys_at(&mut self, frontier: &Frontiers) -> Result<Vec<String>> {
        if self.doc.frontiers_to_vv(frontier).is_none() {
            return Ok(Vec::new());
        }
        let doc = self.fork_for(frontier)?;
        let mut keys = Vec::new();
        doc.get_map(FILES_MAP).for_each(|key, value| {
            if let loro::ValueOrContainer::Container(loro::Container::Text(_)) = value {
                keys.push(key.to_string());
            }
        });
        keys.sort();
        Ok(keys)
    }

    /// Rename (from, to) records exactly at `frontier`, in history order.
    /// Points outside this store's history simply have no records.
    pub(super) fn renames_at(&mut self, frontier: &Frontiers) -> Result<Vec<(String, String)>> {
        if self.doc.frontiers_to_vv(frontier).is_none() {
            return Ok(Vec::new());
        }
        Ok(super::timeline::read_renames(self.fork_for(frontier)?))
    }

    /// Renames recorded between two points (in `later`, not in `earlier`).
    pub(super) fn renames_between(
        &mut self,
        earlier: &Frontiers,
        later: &Frontiers,
    ) -> Result<Vec<(String, String)>> {
        let before: BTreeSet<(String, String)> = self.renames_at(earlier)?.into_iter().collect();
        Ok(self
            .renames_at(later)?
            .into_iter()
            .filter(|pair| !before.contains(pair))
            .collect())
    }
}

// ---------------------------------------------------------------- the plan

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreMode {
    /// Whole worktree; repositions `worktree.head` onto a divergent point.
    Full,
    /// Path-scoped; appends one forward capture on the current lineage.
    Scoped,
    /// Selection-scoped fragment splice; appends one forward
    /// capture whose origin names the selection IDs.
    Fragment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    Text,
    Binary,
}

/// Why a path cannot be restored. Codes, not prose, are the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Obstacle {
    /// A directory occupies the path a file must take.
    DirectoryInTheWay,
    /// A symlink occupies the path; restore never writes through links.
    SymlinkInTheWay,
    /// The content-addressed payload for a binary entry is gone.
    MissingBlob,
    /// A stored key would escape the project root.
    EscapesRoot,
    /// The live path exists but cannot be read for comparison.
    Unreadable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Obstruction {
    pub path: String,
    pub obstacle: Obstacle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreAction {
    /// Root-relative POSIX path.
    pub path: String,
    pub kind: ActionKind,
    /// Absent for deletes.
    pub content: Option<ContentKind>,
    /// Size the path holds after the action; 0 for deletes.
    pub bytes: u64,
    /// Blob digest for binary content.
    pub hash: Option<String>,
    /// Whether the path is executable after the action (file-mode
    /// modelling). Absent in plans computed by older daemons.
    #[serde(default)]
    pub exec: bool,
    /// Live bytes differ from what the timeline recorded for the base point:
    /// this action overwrites work that the pre-restore capture preserves.
    /// Excluded from the plan token — it describes history, not the outcome.
    pub local_modified: bool,
}

/// A dry-run restore: pure computation, no worktree contact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePlan {
    /// SHA-256 over (mode, scope, target frontier, ordered actions).
    pub token: String,
    pub mode: RestoreMode,
    /// Root-relative scope keys; empty for a full restore.
    pub scope: Vec<String>,
    /// Where the worktree sits now.
    pub base: ResolvedPoint,
    /// Where the worktree will sit after apply.
    pub target: ResolvedPoint,
    /// Deletes first, then writes, both path-ordered.
    pub actions: Vec<RestoreAction>,
    pub obstructions: Vec<Obstruction>,
    /// In-scope paths already holding their target content.
    pub unchanged: usize,
    /// Actions overwriting uncaptured local edits.
    pub locally_modified: usize,
    /// Scope keys the user named that no side of the comparison has ever
    /// seen — almost always a typo, and silently "nothing to do" would be
    /// a surprise rather than an answer.
    #[serde(default)]
    pub scope_missing: Vec<String>,
    pub created_at_ms: i64,
    /// Computed without a live daemon, from a read-only store view.
    #[serde(default)]
    pub degraded: bool,
}

impl RestorePlan {
    pub fn is_noop(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn applicable(&self) -> bool {
        self.obstructions.is_empty()
    }

    pub fn writes(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| a.kind != ActionKind::Delete)
            .count()
    }

    pub fn deletes(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| a.kind == ActionKind::Delete)
            .count()
    }
}

/// What actually happened. `progress_log` is the streaming progress channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOutcome {
    pub token: String,
    pub mode: RestoreMode,
    pub target: ResolvedPoint,
    /// Point the worktree occupied immediately before the first mutation.
    /// Restoring to it undoes this restore exactly.
    pub undo: ResolvedPoint,
    /// Where the worktree sits now.
    pub result: ResolvedPoint,
    /// Capture that preserved uncaptured worktree state before restoring.
    pub pre_restore_capture: Option<String>,
    /// Capture appended by a scoped restore.
    pub restore_capture: Option<String>,
    pub files_written: usize,
    pub files_deleted: usize,
    pub unchanged: usize,
    /// Root-relative paths this restore wrote and removed. The daemon uses
    /// them in-process to recognize the watcher echo of its own writes; they
    /// are not serialized, because a large restore would push the response
    /// past the wire envelope cap.
    #[serde(skip)]
    pub written_paths: Vec<String>,
    #[serde(skip)]
    pub deleted_paths: Vec<String>,
    pub resumed: bool,
    pub progress_log: Vec<String>,
}

/// Persisted restart marker (mutations survive `kill -9`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreIntent {
    pub token: String,
    pub mode: RestoreMode,
    pub scope: Vec<String>,
    pub target: ResolvedPoint,
    pub started_ms: i64,
    /// Fragment-restore replay payload. Present only for fragment intents;
    /// the whole-tree modes keep replaying from (target, scope) alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment: Option<Box<super::fragment::FragmentPlan>>,
}

impl RestoreIntent {
    /// How long ago this restore was started.
    pub fn age_ms(&self) -> i64 {
        Utc::now().timestamp_millis() - self.started_ms
    }

    /// A stale intent is one whose auto-replay would surprise the operator:
    /// the worktree has plausibly moved on since the restore began. Stale
    /// intents are surfaced, never silently replayed; an explicit
    /// `restore.resume` is the operator saying "yes, I want it anyway".
    pub fn is_stale(&self, max_resume_age_ms: i64) -> bool {
        max_resume_age_ms >= 0 && self.age_ms() > max_resume_age_ms
    }
}

// ------------------------------------------------------------ scope helpers

/// Turn a user-supplied path into the root-relative POSIX key the document
/// uses. `cwd` resolves relative arguments; nothing is canonicalized because
/// a restore target legitimately may not exist yet.
pub fn scope_key(root: &Path, cwd: &Path, raw: &str) -> Result<String> {
    let raw_path = Path::new(raw);
    let joined = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        cwd.join(raw_path)
    };
    let normalized = lexical_normalize(&joined);
    let rel = normalized.strip_prefix(root).map_err(|_| {
        SheafError::Config(format!(
            "`{raw}` is outside the project at {}",
            root.display()
        ))
    })?;
    let key = rel.to_string_lossy().replace('\\', "/");
    let key = key.trim_matches('/').to_owned();
    if key.is_empty() {
        // The project root itself: a full restore expressed as a path.
        return Ok(String::new());
    }
    validate_key(&key)?;
    Ok(key)
}

/// Resolve `.` / `..` without touching the filesystem.
fn lexical_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub(super) fn validate_key(key: &str) -> Result<()> {
    let bad = key.is_empty()
        || key.starts_with('/')
        || key.split('/').any(|c| c == ".." || c == ".")
        || key == crate::config::SHEAF_DIR_NAME
        || key.starts_with(&format!("{}/", crate::config::SHEAF_DIR_NAME));
    if bad {
        return Err(SheafError::Config(format!("unusable restore path `{key}`")));
    }
    Ok(())
}

pub(super) fn in_scope(key: &str, scope: &[String]) -> bool {
    if scope.is_empty() {
        return true;
    }
    scope
        .iter()
        .any(|s| key == s.as_str() || key.starts_with(&format!("{s}/")))
}

/// Normalize, deduplicate, and collapse nested scope keys. An empty result
/// means "whole tree".
pub(super) fn canonical_scope(scope: &[String]) -> Result<Vec<String>> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    for raw in scope {
        let key = raw.trim().trim_matches('/').replace('\\', "/");
        if key.is_empty() || key == "." {
            // An explicit root scope degenerates to a full restore.
            return Ok(Vec::new());
        }
        validate_key(&key)?;
        keys.insert(key);
    }
    // Drop entries already covered by a shorter ancestor scope.
    let ordered: Vec<String> = keys.into_iter().collect();
    let mut out: Vec<String> = Vec::new();
    for key in ordered {
        if out
            .iter()
            .any(|kept| key.starts_with(&format!("{kept}/")) || *kept == key)
        {
            continue;
        }
        out.push(key);
    }
    Ok(out)
}

// -------------------------------------------------------- plan computation

#[allow(clippy::too_many_arguments)]
fn build_plan(
    root: &Path,
    sdir: &Path,
    base: ResolvedPoint,
    target: ResolvedPoint,
    base_entries: &BTreeMap<String, Entry>,
    target_entries: &BTreeMap<String, Entry>,
    scope: &[String],
    interval_renames: &[(String, String)],
    ignore: &IgnoreSet,
    degraded: bool,
) -> Result<RestorePlan> {
    let user_scope = canonical_scope(scope)?;
    // A path may have worn a different name at the target point. Renames are
    // first-class history, so the scope speaks both names: naming
    // the current path restores its former one (and removes the current), and
    // naming the former path removes its successor. Without this, a scoped
    // restore across a rename would merely delete the file.
    let scope = canonical_scope(&expand_scope_through_renames(
        &user_scope,
        interval_renames,
    )?)?;
    let mode = if scope.is_empty() {
        RestoreMode::Full
    } else {
        RestoreMode::Scoped
    };

    // Candidates are the union of both timeline points AND the live tree.
    // Untracked files matter: "how it was at X" means a file born after X is
    // gone afterwards, and a dry-run that omitted it would be lying. Including
    // them here also makes a plan stable across its own safety capture, which
    // is what turns them into tracked deletions.
    let live = live_files(root, ignore, &scope);
    let mut candidates: BTreeSet<&str> = BTreeSet::new();
    candidates.extend(base_entries.keys().map(String::as_str));
    candidates.extend(target_entries.keys().map(String::as_str));
    candidates.extend(live.iter().map(String::as_str));

    let mut writes: Vec<RestoreAction> = Vec::new();
    let mut deletes: Vec<RestoreAction> = Vec::new();
    let mut obstructions: Vec<Obstruction> = Vec::new();
    let mut unchanged = 0usize;

    for key in candidates.iter().copied() {
        if !in_scope(key, &scope) {
            continue;
        }
        if validate_key(key).is_err() {
            obstructions.push(Obstruction {
                path: key.to_owned(),
                obstacle: Obstacle::EscapesRoot,
            });
            continue;
        }
        let dst = root.join(key);
        let live_meta = match std::fs::symlink_metadata(&dst) {
            Ok(meta) => Some(meta),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => {
                obstructions.push(Obstruction {
                    path: key.to_owned(),
                    obstacle: Obstacle::Unreadable,
                });
                continue;
            }
        };
        if let Some(meta) = &live_meta {
            if meta.is_dir() {
                obstructions.push(Obstruction {
                    path: key.to_owned(),
                    obstacle: Obstacle::DirectoryInTheWay,
                });
                continue;
            }
            if meta.file_type().is_symlink() {
                obstructions.push(Obstruction {
                    path: key.to_owned(),
                    obstacle: Obstacle::SymlinkInTheWay,
                });
                continue;
            }
        }

        let disk = live_meta.is_some().then(|| std::fs::read(&dst)).transpose();
        let disk = match disk {
            Ok(bytes) => bytes,
            Err(_) => {
                obstructions.push(Obstruction {
                    path: key.to_owned(),
                    obstacle: Obstacle::Unreadable,
                });
                continue;
            }
        };
        let local_modified = match base_entries.get(key) {
            Some(entry) => !matches_bytes(disk.as_deref(), entry),
            None => disk.is_some(),
        };
        let live_exec = live_meta
            .as_ref()
            .map(|m| {
                use std::os::unix::fs::PermissionsExt;
                m.permissions().mode() & 0o111 != 0
            })
            .unwrap_or(false);

        match target_entries.get(key) {
            Some(entry) => {
                if let Content::Binary { hash, .. } = &entry.content {
                    if !blobs::blob_path(sdir, hash).exists() {
                        obstructions.push(Obstruction {
                            path: key.to_owned(),
                            obstacle: Obstacle::MissingBlob,
                        });
                        continue;
                    }
                }
                if matches_bytes(disk.as_deref(), entry) && live_exec == entry.exec {
                    unchanged += 1;
                    continue;
                }
                writes.push(RestoreAction {
                    path: key.to_owned(),
                    kind: if disk.is_some() {
                        ActionKind::Update
                    } else {
                        ActionKind::Create
                    },
                    content: Some(entry.content_key()),
                    bytes: entry.byte_len(),
                    hash: entry.hash().map(str::to_owned),
                    exec: entry.exec,
                    local_modified,
                });
            }
            None => {
                if disk.is_none() {
                    unchanged += 1;
                    continue;
                }
                deletes.push(RestoreAction {
                    path: key.to_owned(),
                    kind: ActionKind::Delete,
                    content: None,
                    bytes: 0,
                    hash: None,
                    exec: false,
                    local_modified,
                });
            }
        }
    }

    // Deletes lead: a path that was a file at head may be a directory at the
    // target point, and the file has to go before the directory can appear.
    let mut actions = deletes;
    actions.extend(writes);
    let locally_modified = actions.iter().filter(|a| a.local_modified).count();

    // Which of the paths the user actually named did anything at all. A key
    // that matched no candidate under ANY of its rename-connected names is
    // almost certainly a typo; "already there" would be a misleading answer.
    let scope_missing = user_scope
        .iter()
        .filter(|key| {
            let aliases =
                expand_names(&BTreeSet::from([key.as_str().to_owned()]), interval_renames);
            !candidates.iter().any(|c| {
                aliases
                    .iter()
                    .any(|n| *c == n.as_str() || c.starts_with(&format!("{n}/")))
            })
        })
        .cloned()
        .collect();

    Ok(RestorePlan {
        token: plan_token(mode, &scope, &target.frontier, &actions),
        mode,
        scope,
        base,
        target,
        actions,
        obstructions,
        unchanged,
        locally_modified,
        scope_missing,
        created_at_ms: Utc::now().timestamp_millis(),
        degraded,
    })
}

/// Grow a scope key set with the other names its paths wore across the given
/// rename events (prefix-aware for directory renames, transitive for chains).
fn expand_scope_through_renames(
    scope: &[String],
    renames: &[(String, String)],
) -> Result<Vec<String>> {
    if scope.is_empty() || renames.is_empty() {
        return Ok(scope.to_vec());
    }
    for (from, to) in renames {
        validate_key(from)?;
        validate_key(to)?;
    }
    Ok(expand_names(&scope.iter().cloned().collect(), renames)
        .into_iter()
        .collect())
}

/// One fixpoint pass over the rename graph: every name a seed set's paths
/// wore, both directions, prefix-aware, transitive.
pub(super) fn expand_names(
    seed: &BTreeSet<String>,
    renames: &[(String, String)],
) -> BTreeSet<String> {
    let mut names: BTreeSet<String> = seed.clone();
    loop {
        let mut grew = false;
        let snapshot: Vec<String> = names.iter().cloned().collect();
        for key in &snapshot {
            for (from, to) in renames {
                let alias = if key == to || key.starts_with(&format!("{to}/")) {
                    Some(format!("{from}{}", &key[to.len()..]))
                } else if key == from || key.starts_with(&format!("{from}/")) {
                    Some(format!("{to}{}", &key[from.len()..]))
                } else {
                    None
                };
                grew |= alias.is_some_and(|a| names.insert(a));
            }
        }
        if !grew {
            break;
        }
    }
    names
}

/// Non-ignored regular files currently in the worktree, as document keys,
/// restricted to `scope`. Ignored subtrees (`.git/`, `target/`, `.sheaf/`, …)
/// are pruned rather than filtered, so build output costs nothing to skip.
pub(super) fn live_files(root: &Path, ignore: &IgnoreSet, scope: &[String]) -> Vec<String> {
    let starts: Vec<PathBuf> = if scope.is_empty() {
        vec![root.to_path_buf()]
    } else {
        scope
            .iter()
            .map(|key| root.join(key))
            .filter(|path| path.exists())
            .collect()
    };
    let mut out = Vec::new();
    for start in starts {
        let walker = walkdir::WalkDir::new(start)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                entry
                    .path()
                    .strip_prefix(root)
                    .map(|rel| rel.as_os_str().is_empty() || !ignore.is_ignored_rel(rel))
                    .unwrap_or(false)
            });
        for entry in walker.filter_map(std::result::Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let Ok(rel) = entry.path().strip_prefix(root) else {
                continue;
            };
            let key = rel.to_string_lossy().replace('\\', "/");
            if validate_key(&key).is_ok() && in_scope(&key, scope) {
                out.push(key);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn matches_bytes(disk: Option<&[u8]>, entry: &Entry) -> bool {
    let Some(bytes) = disk else {
        return false;
    };
    match &entry.content {
        Content::Text(text) => bytes == text.as_bytes(),
        Content::Binary { hash, .. } => blobs::hash_of(bytes) == *hash,
    }
}

/// Content address of the *outcome* a plan describes. Deliberately excludes
/// the base point and `local_modified`: reconciling uncaptured worktree edits
/// before applying moves the base but changes neither the target state nor the
/// live bytes, so a plan stays valid across its own safety capture. Anything
/// that would change what lands on disk — content AND the exec bit — changes
/// the token.
fn plan_token(
    mode: RestoreMode,
    scope: &[String],
    target_frontier: &str,
    actions: &[RestoreAction],
) -> String {
    let canonical = serde_json::json!({
        "v": 2,
        "mode": mode,
        "scope": scope,
        "target": target_frontier,
        "actions": actions.iter().map(|a| serde_json::json!({
            "path": a.path,
            "kind": a.kind,
            "content": a.content,
            "bytes": a.bytes,
            "hash": a.hash,
            "exec": a.exec,
        })).collect::<Vec<_>>(),
    });
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

// ------------------------------------------------------------ read-only API

impl TimelineReader {
    /// Dry-run restore from a read-only store view (degraded mode).
    pub fn plan_restore(
        &self,
        reference: &str,
        scope: &[String],
        ignore: &IgnoreSet,
    ) -> Result<RestorePlan> {
        let target = self.resolve(reference)?;
        let base = self.resolve("@")?;
        let base_frontier = decode_frontier(&base.frontier)?;
        let target_frontier = decode_frontier(&target.frontier)?;
        let mut view = HistoryView::open(self.doc())?;
        let base_entries = view.entries_at(&base_frontier)?;
        let target_entries = view.entries_at(&target_frontier)?;
        let renames = view.renames_between(&target_frontier, &base_frontier)?;
        build_plan(
            self.root(),
            &store_dir(self.root()),
            base,
            target,
            &base_entries,
            &target_entries,
            scope,
            &renames,
            ignore,
            true,
        )
    }
}

// ----------------------------------------------------------- the write path

impl ProjectStore {
    /// Dry-run a restore of `scope` (empty = whole tree) to `reference`.
    pub fn plan_restore(
        &self,
        reference: &str,
        scope: &[String],
        ignore: &IgnoreSet,
    ) -> Result<RestorePlan> {
        let target = self.resolve(reference)?;
        self.plan_restore_at(&target, scope, ignore)
    }

    /// Same, against an already-resolved point. Apply revalidates through
    /// this so a relative reference cannot drift between plan and apply.
    pub fn plan_restore_at(
        &self,
        target: &ResolvedPoint,
        scope: &[String],
        ignore: &IgnoreSet,
    ) -> Result<RestorePlan> {
        let target_frontier = decode_frontier(&target.frontier)?;
        let base = self.resolve("@")?;
        let mut view = HistoryView::open(&self.doc)?;
        let target_entries = view.entries_at(&target_frontier)?;
        // The writer's own state is the base side — no checkout needed.
        let base_entries = entries_of_state(&self.doc);
        let renames = view.renames_between(&target_frontier, &decode_frontier(&base.frontier)?)?;
        build_plan(
            &self.root,
            &self.sdir,
            base,
            target.clone(),
            &base_entries,
            &target_entries,
            scope,
            &renames,
            ignore,
            false,
        )
    }

    /// Execute a previously computed plan.
    ///
    /// Ordering is the safety contract: reconcile the live worktree into
    /// history, revalidate the plan against what is actually on disk, mark the
    /// intent durably, install, then move the timeline. A `kill -9` anywhere
    /// after the intent lands replays to the same result.
    pub fn apply_restore(
        &mut self,
        plan: &RestorePlan,
        ignore: &IgnoreSet,
    ) -> Result<RestoreOutcome> {
        self.run_restore(plan, ignore, false)
    }

    /// A restore interrupted by a crash, if one is outstanding.
    pub fn pending_restore(&self) -> Option<RestoreIntent> {
        pending_restore_at(&self.root)
    }

    /// Finish an interrupted restore. Idempotent: the target state is
    /// immutable history and binary payloads are content-addressed, so
    /// replaying an intent converges on exactly the same worktree.
    ///
    /// `force` is the explicit operator path (`sheaf restore --resume`): it
    /// overrides the staleness bound that gates automatic boot replay. A
    /// stale intent left alone means the user's later work wins; forcing it
    /// means the operator asked for the rewind by name.
    pub fn resume_restore(
        &mut self,
        ignore: &IgnoreSet,
        force: bool,
        max_resume_age_ms: i64,
    ) -> Result<Option<RestoreOutcome>> {
        let Some(intent) = self.pending_restore() else {
            return Ok(None);
        };
        if intent.is_stale(max_resume_age_ms) && !force {
            tracing::warn!(
                root = %self.root.display(),
                token = %intent.token,
                age_ms = intent.age_ms(),
                max_resume_age_ms,
                "pending restore is past the staleness bound; NOT replaying \
                 automatically — `sheaf restore --resume` forces it, \
                 `sheaf restore --abandon` discards it"
            );
            return Ok(None);
        }
        tracing::warn!(
            root = %self.root.display(),
            token = %intent.token,
            forced = force,
            "resuming restore interrupted before completion"
        );
        // Fragment intents replay from their durable plan payload, not from
        // (target, scope): a crash mid-apply leaves files at a mix of pre
        // and planned hashes that only the recorded per-file plans can drive
        // to convergence.
        if intent.fragment.is_some() {
            return self.resume_fragment(&intent, ignore).map(Some);
        }
        let plan = self.plan_restore_at(&intent.target, &intent.scope, ignore)?;
        self.run_restore(&plan, ignore, true).map(Some)
    }

    /// Operator verb: discard an outstanding intent. The worktree stays
    /// exactly as it is; whatever the interrupted restore had already
    /// written becomes ordinary history through the standard two-sided
    /// reconciliation, so nothing on disk is uncaptured afterward.
    pub fn abandon_restore(&mut self, ignore: &IgnoreSet) -> Result<Option<Capture>> {
        let had = self.pending_restore();
        self.clear_intent();
        tracing::warn!(
            root = %self.root.display(),
            token = had.as_ref().map(|i| i.token.clone()).unwrap_or_default(),
            abandoned = had.is_some(),
            "restore intent abandoned by operator; reconciling worktree as-is"
        );
        self.reconcile_worktree(ignore)
    }

    fn run_restore(
        &mut self,
        plan: &RestorePlan,
        ignore: &IgnoreSet,
        resumed: bool,
    ) -> Result<RestoreOutcome> {
        if !plan.applicable() {
            return Err(SheafError::RestoreObstructed(describe(&plan.obstructions)));
        }
        let mut progress_log = Vec::new();

        // 1. Revalidate BEFORE anything is written — including before the
        //    safety capture. A rejected restore must leave the store exactly
        //    as it found it, history included.
        let checked = self.plan_restore_at(&plan.target, &plan.scope, ignore)?;
        if !checked.applicable() {
            return Err(SheafError::RestoreObstructed(describe(
                &checked.obstructions,
            )));
        }
        if !resumed && checked.token != plan.token {
            return Err(SheafError::RestorePlanStale(
                "the worktree or timeline moved since this plan was computed".into(),
            ));
        }

        // 2. Nothing a restore overwrites may be unrecoverable: the live
        //    worktree becomes history before the first byte moves.
        let pre_restore_capture = self.reconcile_tagged(
            ignore,
            Some(CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: plan.target.capture_id.clone(),
                scope: plan.scope.clone(),
                selections: Vec::new(),
            }),
        )?;
        if let Some(capture) = &pre_restore_capture {
            progress_log.push(format!(
                "captured pre-restore worktree state as {}",
                capture.short_id()
            ));
        }

        // The safety capture moved the base point but touched no file, so the
        // plan it validated still describes this worktree exactly. Recompute
        // only to pick up the new base; a token drift here would mean the
        // capture rewrote the tree, which is a bug, not a user race.
        let fresh = self.plan_restore_at(&plan.target, &plan.scope, ignore)?;
        if fresh.token != checked.token {
            return Err(SheafError::StoreCorrupt(
                "pre-restore capture changed the pending restore plan".into(),
            ));
        }

        let undo = self.resolve("@")?;
        let target_entries = entries_at(&self.doc, &decode_frontier(&fresh.target.frontier)?)?;

        // 3. Durable intent, then the worktree mutations it authorizes.
        self.write_intent(&fresh)?;

        let mut written_paths: Vec<String> = Vec::new();
        let mut deleted_paths: Vec<String> = Vec::new();
        let mut deleted_dirs: BTreeSet<PathBuf> = BTreeSet::new();
        for action in &fresh.actions {
            let dst = self.root.join(&action.path);
            // The safety capture above is one instant; installing a large tree
            // is not. Anything the user saved into THIS path since then would
            // otherwise be overwritten having never been captured, so each
            // path is re-checked immediately before it is touched. In the
            // ordinary case this finds nothing and costs one read.
            if let Some(rescued) = self.capture_drift(&action.path)? {
                progress_log.push(format!(
                    "captured a concurrent edit to {} as {}",
                    action.path,
                    rescued.short_id()
                ));
            }
            match action.kind {
                ActionKind::Delete => {
                    match std::fs::remove_file(&dst) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(SheafError::Io(e)),
                    }
                    if let Some(parent) = dst.parent() {
                        deleted_dirs.insert(parent.to_path_buf());
                    }
                    deleted_paths.push(action.path.clone());
                    progress_log.push(format!("delete {}", action.path));
                }
                ActionKind::Create | ActionKind::Update => {
                    let entry = target_entries.get(&action.path).ok_or_else(|| {
                        SheafError::StoreCorrupt(format!(
                            "planned content for `{}` vanished from history",
                            action.path
                        ))
                    })?;
                    self.install(&action.path, entry)?;
                    written_paths.push(action.path.clone());
                    progress_log.push(format!(
                        "{} {} ({} bytes)",
                        if action.kind == ActionKind::Create {
                            "create"
                        } else {
                            "update"
                        },
                        action.path,
                        action.bytes
                    ));
                }
            }
        }
        self.prune_empty_dirs(deleted_dirs);

        // 4. Move the timeline to match the worktree.
        let (result, restore_capture) = match fresh.mode {
            RestoreMode::Fragment => {
                // Fragment plans never route through the whole-tree engine;
                // an intent carrying one dispatches to `resume_fragment`
                // before reaching here.
                return Err(SheafError::StoreCorrupt(
                    "fragment plan reached the whole-tree restore engine".into(),
                ));
            }
            RestoreMode::Scoped => {
                let capture = self.record_scoped_restore(&fresh)?;
                let point = self.resolve("@")?;
                progress_log.push(match &capture {
                    Some(c) => format!("recorded restore as capture {}", c.short_id()),
                    None => "worktree already matched the target point".to_owned(),
                });
                (point, capture)
            }
            RestoreMode::Full => {
                self.reposition_head(&fresh.target)?;
                progress_log.push(format!(
                    "repositioned worktree head to {}",
                    fresh
                        .target
                        .capture_id
                        .as_deref()
                        .map(|id| &id[..12.min(id.len())])
                        .unwrap_or("(frontier)")
                ));
                (fresh.target.clone(), None)
            }
        };

        // The full detail is in the daemon log; the wire copy stays bounded so
        // a ten-thousand-file restore still fits one response envelope.
        let truncated = progress_log.len().saturating_sub(PROGRESS_LOG_LIMIT);
        if truncated > 0 {
            progress_log.truncate(PROGRESS_LOG_LIMIT);
            progress_log.push(format!(
                "… and {truncated} more actions (see the daemon log)"
            ));
        }

        self.clear_intent();
        tracing::info!(
            root = %self.root.display(),
            mode = ?fresh.mode,
            written = written_paths.len(),
            deleted = deleted_paths.len(),
            unchanged = fresh.unchanged,
            resumed,
            "restore applied"
        );

        Ok(RestoreOutcome {
            token: fresh.token,
            mode: fresh.mode,
            target: fresh.target,
            undo,
            result,
            pre_restore_capture: pre_restore_capture.map(|c| c.id),
            restore_capture: restore_capture.map(|c| c.id),
            files_written: written_paths.len(),
            files_deleted: deleted_paths.len(),
            unchanged: fresh.unchanged,
            written_paths,
            deleted_paths,
            resumed,
            progress_log,
        })
    }

    /// Materialize one entry atomically. Payloads are staged inside the
    /// always-ignored store directory, so the watcher only ever sees the
    /// finished rename — never a partial file under a project path.
    ///
    /// Binary payloads stream from the blob (flat memory), the
    /// recorded exec bit is applied before publish, and the rename's parent
    /// directory is fsync'd so a power cut cannot strand a half-landed
    /// restore behind a lost directory entry.
    pub(super) fn install(&self, key: &str, entry: &Entry) -> Result<()> {
        let dst = self.root.join(key);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let stage = self.sdir.join(STAGE_DIR);
        std::fs::create_dir_all(&stage)?;
        let tmp = stage.join(format!("{}.tmp", blobs::hash_of(key.as_bytes())));
        {
            let mut file = std::fs::File::create(&tmp)?;
            match &entry.content {
                Content::Text(text) => file.write_all(text.as_bytes())?,
                Content::Binary { hash, .. } => {
                    let src = blobs::blob_path(&self.sdir, hash);
                    let mut reader = std::fs::File::open(&src)
                        .map_err(|e| SheafError::StoreCorrupt(format!("blob {hash}: {e}")))?;
                    // Stream and verify: content-addressing is the safety
                    // net that lets replay converge on identical bytes.
                    let mut hasher = Sha256::new();
                    let mut buf = [0u8; 256 * 1024];
                    loop {
                        let n = reader.read(&mut buf)?;
                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                        file.write_all(&buf[..n])?;
                    }
                    let got = hex::encode(hasher.finalize());
                    if got != *hash {
                        return Err(SheafError::StoreCorrupt(format!(
                            "blob {hash} content mismatch (found {got}); \
                             run `sheaf doctor`"
                        )));
                    }
                }
            }
            file.sync_all()?;
        }
        if entry.exec {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755));
        }
        std::fs::rename(&tmp, &dst)?;
        fsutil::sync_parent_dir(&dst)?;
        Ok(())
    }

    /// Remove directories emptied by deletes, walking upward but never past
    /// the project root and never into the store.
    fn prune_empty_dirs(&self, dirs: BTreeSet<PathBuf>) {
        for dir in dirs.iter().rev() {
            let mut cur = dir.clone();
            while cur.starts_with(&self.root) && cur != self.root {
                if cur.starts_with(crate::config::sheaf_dir(&self.root)) {
                    break;
                }
                let empty = std::fs::read_dir(&cur)
                    .map(|mut rd| rd.next().is_none())
                    .unwrap_or(false);
                if !empty || std::fs::remove_dir(&cur).is_err() {
                    break;
                }
                match cur.parent() {
                    Some(parent) => cur = parent.to_path_buf(),
                    None => break,
                }
            }
        }
    }

    /// Scoped restores enter history as ordinary forward work: one capture
    /// whose text ops are the char-level splices back to the old content.
    /// Delete/create pairs carrying identical content are emitted as
    /// first-class renames so post-move history stays followable.
    fn record_scoped_restore(&mut self, plan: &RestorePlan) -> Result<Option<Capture>> {
        if plan.actions.is_empty() {
            return Ok(None);
        }
        let base_entries = entries_of_state(&self.doc);
        let target_entries = entries_at(&self.doc, &decode_frontier(&plan.target.frontier)?)?;

        let mut removed: Vec<&RestoreAction> = Vec::new();
        let mut created: Vec<&RestoreAction> = Vec::new();
        let mut updated: Vec<&RestoreAction> = Vec::new();
        for action in &plan.actions {
            match action.kind {
                ActionKind::Delete => removed.push(action),
                ActionKind::Create => created.push(action),
                ActionKind::Update => updated.push(action),
            }
        }

        // Pair by content identity, deterministically and one-to-one.
        let mut paired_from: BTreeSet<&str> = BTreeSet::new();
        let mut paired_to: BTreeSet<&str> = BTreeSet::new();
        let mut events: Vec<FsEvent> = Vec::new();
        for gone in &removed {
            let Some(from_entry) = base_entries.get(&gone.path) else {
                continue;
            };
            let identity = from_entry.identity();
            let landed = created.iter().find(|c| {
                !paired_to.contains(c.path.as_str())
                    && target_entries
                        .get(&c.path)
                        .is_some_and(|e| e.identity() == identity)
            });
            if let Some(landed) = landed {
                paired_from.insert(gone.path.as_str());
                paired_to.insert(landed.path.as_str());
                events.push(FsEvent::now(EventKind::Renamed {
                    from: self.root.join(&gone.path),
                    to: self.root.join(&landed.path),
                }));
            }
        }
        for action in &plan.actions {
            match action.kind {
                ActionKind::Delete if !paired_from.contains(action.path.as_str()) => {
                    events.push(FsEvent::now(EventKind::Removed {
                        path: self.root.join(&action.path),
                    }));
                }
                ActionKind::Create if !paired_to.contains(action.path.as_str()) => {
                    events.push(FsEvent::now(EventKind::Added {
                        path: self.root.join(&action.path),
                    }));
                }
                ActionKind::Update => events.push(FsEvent::now(EventKind::Touched {
                    path: self.root.join(&action.path).into(),
                })),
                _ => {}
            }
        }
        if events.is_empty() {
            return Ok(None);
        }
        let now = Utc::now();
        let outcome = self.apply_batch_tagged(
            &Batch {
                root: self.root.clone(),
                started_at: now,
                flushed_at: now,
                events,
            },
            Some(CaptureOrigin {
                kind: OriginKind::Restore,
                target: plan.target.capture_id.clone(),
                scope: plan.scope.clone(),
                selections: Vec::new(),
            }),
        )?;
        Ok(outcome.capture)
    }

    /// Capture whatever the worktree says about one path if it has drifted
    /// from the document since the pre-restore reconciliation. Returns the
    /// capture it appended, or `None` when the path is already faithful.
    pub(super) fn capture_drift(&mut self, key: &str) -> Result<Option<Capture>> {
        let path = self.root.join(key);
        let tracked = self.knows(key);
        let event = match (tracked, path.is_file()) {
            (true, true) if self.content_differs(&path) == Some(true) => {
                EventKind::Touched { path: path.into() }
            }
            (true, false) => EventKind::Removed { path },
            // Untracked bytes standing exactly where a restore is about to
            // write are still somebody's work.
            (false, true) => EventKind::Touched { path: path.into() },
            _ => return Ok(None),
        };
        let now = Utc::now();
        let outcome = self.apply_batch_tagged(
            &Batch {
                root: self.root.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(event)],
            },
            Some(CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: None,
                scope: vec![key.to_owned()],
                selections: Vec::new(),
            }),
        )?;
        Ok(outcome.capture)
    }

    /// Full restore: move the materialized state and the advisory head onto
    /// the target frontier. Authors nothing — the abandoned future stays
    /// reachable and the next capture diverges from here.
    fn reposition_head(&mut self, target: &ResolvedPoint) -> Result<()> {
        let frontier = decode_frontier(&target.frontier)?;
        self.doc.checkout(&frontier).map_err(super::store_err)?;
        self.doc.set_detached_editing(true);
        self.write_head_point(target.capture_id.as_deref(), &target.frontier, 0)
    }

    fn write_intent(&self, plan: &RestorePlan) -> Result<()> {
        let intent = RestoreIntent {
            token: plan.token.clone(),
            mode: plan.mode,
            scope: plan.scope.clone(),
            target: plan.target.clone(),
            started_ms: Utc::now().timestamp_millis(),
            fragment: None,
        };
        let path = restore_intent_path(&self.root);
        let bytes = serde_json::to_vec_pretty(&intent)
            .map_err(|e| SheafError::StoreCorrupt(e.to_string()))?;
        fsutil::atomic_write(&path, &bytes)?;

        Ok(())
    }

    pub(super) fn clear_intent(&self) {
        let path = restore_intent_path(&self.root);

        if std::fs::remove_file(&path).is_ok() {
            let _ = fsutil::sync_parent_dir(&path);
        }
        let _ = std::fs::remove_dir_all(self.sdir.join(STAGE_DIR));
    }

    /// Two-sided worktree↔document reconciliation: disk∖store becomes
    /// Touched, store∖disk becomes Removed, and known-but-changed bytes become
    /// Touched. Returns the capture appended, or `None` when already converged.
    pub fn reconcile_worktree(&mut self, ignore: &IgnoreSet) -> Result<Option<Capture>> {
        self.reconcile_tagged(ignore, None)
    }

    pub(super) fn reconcile_tagged(
        &mut self,
        ignore: &IgnoreSet,
        origin: Option<CaptureOrigin>,
    ) -> Result<Option<Capture>> {
        let known = self.known_paths();
        let root = self.root.clone();
        let mut events = Vec::with_capacity(super::RECONCILE_BATCH_EVENTS);
        let mut last_capture = None;
        for entry in walkdir::WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                e.path()
                    .strip_prefix(&root)
                    .map(|rel| rel.as_os_str().is_empty() || !ignore.is_ignored_rel(rel))
                    .unwrap_or(false)
            })
            .filter_map(std::result::Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if !known.contains(path) {
                events.push(FsEvent::now(EventKind::Touched {
                    path: path.to_path_buf().into(),
                }));
                if events.len() >= super::RECONCILE_BATCH_EVENTS {
                    last_capture = self
                        .flush_reconcile_events(&mut events, origin.clone())?
                        .or(last_capture);
                }
            }
        }
        for path in &known {
            // A tracked path that is ignored today (a newly-gitignored file,
            // a scratch dir added to the ignore set) is walk-invisible, so
            // every worktree diff would forever report it as deleted. Drop
            // it from the document instead: doc-only Removed, the disk file
            // is untouched. Re-ignoring heals the other way — the walk
            // above re-adds it on the next reconcile.
            let now_ignored = path
                .strip_prefix(&root)
                .ok()
                .map(|rel| !rel.as_os_str().is_empty() && ignore.is_ignored_rel(rel))
                .unwrap_or(false);
            if now_ignored || !path.exists() {
                events.push(FsEvent::now(EventKind::Removed { path: path.clone() }));
            } else if path.is_file() && self.content_differs(path) == Some(true) {
                events.push(FsEvent::now(EventKind::Touched {
                    path: path.clone().into(),
                }));
            }
            if events.len() >= super::RECONCILE_BATCH_EVENTS {
                last_capture = self
                    .flush_reconcile_events(&mut events, origin.clone())?
                    .or(last_capture);
            }
        }
        Ok(self
            .flush_reconcile_events(&mut events, origin)?
            .or(last_capture))
    }

    fn flush_reconcile_events(
        &mut self,
        events: &mut Vec<FsEvent>,
        origin: Option<CaptureOrigin>,
    ) -> Result<Option<Capture>> {
        if events.is_empty() {
            return Ok(None);
        }
        let now = Utc::now();
        let outcome = self.apply_batch_tagged(
            &Batch {
                root: self.root.clone(),
                started_at: now,
                flushed_at: now,
                events: std::mem::take(events),
            },
            origin,
        )?;
        Ok(outcome.capture)
    }
}

/// Read an outstanding restore intent without opening the store — the
/// degraded-mode and status paths need it too.
///
/// An intent that cannot be parsed is quarantined rather than ignored: left
/// in place it would be retried and skipped on every single start, hiding a
/// half-restored worktree behind silence.
pub fn pending_restore_at(root: &Path) -> Option<RestoreIntent> {
    let path = restore_intent_path(root);
    let raw = std::fs::read_to_string(&path).ok()?;
    match serde_json::from_str(&raw) {
        Ok(intent) => Some(intent),
        Err(error) => {
            tracing::error!(
                intent = %path.display(),
                %error,
                "restore intent is unreadable; quarantining it as .bad — \
                 the worktree may be half-restored, compare it against `sheaf log`"
            );
            let _ = std::fs::rename(&path, path.with_extension("intent.bad"));
            None
        }
    }
}

fn describe(obstructions: &[Obstruction]) -> String {
    obstructions
        .iter()
        .take(5)
        .map(|o| format!("{} ({:?})", o.path, o.obstacle))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_keys_normalize_and_refuse_escapes() {
        let root = Path::new("/proj");
        let cwd = Path::new("/proj/crates/core");
        assert_eq!(
            scope_key(root, cwd, "src/lib.rs").unwrap(),
            "crates/core/src/lib.rs"
        );
        assert_eq!(
            scope_key(root, cwd, "../../README.md").unwrap(),
            "README.md"
        );
        assert_eq!(scope_key(root, cwd, "/proj/a/b.txt").unwrap(), "a/b.txt");
        assert_eq!(scope_key(root, cwd, "/proj").unwrap(), "");
        assert!(scope_key(root, cwd, "/etc/passwd").is_err());
        assert!(scope_key(root, cwd, "../../../outside").is_err());
        assert!(scope_key(root, cwd, "/proj/.sheaf/lock").is_err());
    }

    #[test]
    fn nested_scopes_collapse_and_root_scope_means_full() {
        assert_eq!(
            canonical_scope(&["src/a".into(), "src".into(), "docs".into()]).unwrap(),
            vec!["docs".to_string(), "src".to_string()]
        );
        assert!(canonical_scope(&["src".into(), ".".into()])
            .unwrap()
            .is_empty());
        assert!(canonical_scope(&["../evil".into()]).is_err());
    }

    #[test]
    fn scope_matching_is_prefix_aware() {
        let scope = vec!["src".to_string()];
        assert!(in_scope("src/lib.rs", &scope));
        assert!(in_scope("src", &scope));
        assert!(!in_scope("srcery/x.rs", &scope));
        assert!(in_scope("anything", &[]));
    }

    #[test]
    fn token_ignores_base_and_local_modification_noise() {
        let action = |local| RestoreAction {
            path: "a.txt".into(),
            kind: ActionKind::Update,
            content: Some(ContentKind::Text),
            bytes: 3,
            hash: None,
            exec: false,
            local_modified: local,
        };
        let a = plan_token(RestoreMode::Full, &[], "ff00", &[action(false)]);
        let b = plan_token(RestoreMode::Full, &[], "ff00", &[action(true)]);
        assert_eq!(a, b, "safety capture must not invalidate its own plan");
        let c = plan_token(RestoreMode::Full, &[], "ff01", &[action(false)]);
        assert_ne!(a, c, "a different target is a different plan");
    }

    // ---- fixtures (mirror crates/sheaf-core/tests/restore.rs) ----

    use crate::config;
    use crate::events::{Batch, EventKind, FsEvent};
    use crate::store::StoreLimits;

    const V1: &str = "fn v1() {}\n";
    const V2: &str = "fn v2() {}\n";

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sheaf-restore-unit-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn skeleton(root: &Path) {
        std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
        config::write_skeleton(root).unwrap();
    }

    fn limits() -> StoreLimits {
        StoreLimits {
            max_segment_bytes: 64 << 20,
            snapshot_edit_size: 1000,
        }
    }

    fn ignores() -> IgnoreSet {
        IgnoreSet::from_patterns(&config::default_patterns()).unwrap()
    }

    fn open(root: &Path) -> ProjectStore {
        ProjectStore::open(root, limits()).unwrap()
    }

    fn write(root: &Path, rel: &str, bytes: &[u8]) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn flush(store: &mut ProjectStore, root: &Path, events: Vec<FsEvent>) {
        let batch = Batch {
            root: root.to_path_buf(),
            events,
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
        };
        store.apply_batch(&batch).unwrap();
    }

    fn added(root: &Path, rel: &str) -> FsEvent {
        FsEvent::now(EventKind::Added {
            path: root.join(rel),
        })
    }

    fn touched(root: &Path, rel: &str) -> FsEvent {
        FsEvent::now(EventKind::Touched {
            path: root.join(rel).into(),
        })
    }

    fn renamed(root: &Path, from: &str, to: &str) -> FsEvent {
        FsEvent::now(EventKind::Renamed {
            from: root.join(from),
            to: root.join(to),
        })
    }

    fn point(frontier: &str) -> ResolvedPoint {
        ResolvedPoint {
            frontier: frontier.into(),
            capture_id: None,
        }
    }

    fn scope(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    fn text_entry(s: &str) -> Entry {
        Entry::text(s.to_string(), false)
    }

    #[test]
    fn entry_helpers_expose_content_identity_and_size() {
        let text = text_entry("hello");
        assert_eq!(text.content_key(), ContentKind::Text);
        assert_eq!(text.byte_len(), 5);
        assert!(text.hash().is_none());
        assert!(text.identity().starts_with("t:"));

        let binary = Entry::binary("fefe".into(), 7, true);
        assert_eq!(binary.content_key(), ContentKind::Binary);
        assert_eq!(binary.byte_len(), 7);
        assert_eq!(binary.hash(), Some("fefe"));
        assert_eq!(binary.identity(), "b:fefe");
    }

    #[test]
    fn binary_metadata_is_parsed_strictly_from_the_document() {
        let doc = LoroDoc::new();
        let map = doc.get_map(BINARIES_MAP);
        map.insert(
            "ok.bin",
            serde_json::json!({"hash": "fefe", "size": 7}).to_string(),
        )
        .unwrap();
        // Malformed records are skipped by the state view...
        map.insert("bad-json.bin", "not json".to_string()).unwrap();
        map.insert("no-hash.bin", serde_json::json!({"size": 3}).to_string())
            .unwrap();
        map.insert("num.bin", 42i64).unwrap();
        #[allow(deprecated)] // mirror files_map_text until the sync-era migration
        let text = doc
            .get_map(FILES_MAP)
            .get_or_create_container("a.txt", loro::LoroText::new())
            .unwrap();
        text.insert(0, "hello").unwrap();
        doc.commit();

        let entries = entries_of_state(&doc);
        assert_eq!(entries.len(), 2, "{entries:#?}");
        assert!(matches!(
            entries["ok.bin"].content,
            Content::Binary { ref hash, size: 7 } if hash == "fefe"
        ));
        assert!(matches!(
            entries["a.txt"].content,
            Content::Text(ref s) if s == "hello"
        ));

        // ...and refused with typed errors by the path view.
        let frontier = doc.state_frontiers();
        let mut view = HistoryView::open(&doc).unwrap();
        assert!(matches!(
            view.path_at(&frontier, "ok.bin").unwrap(),
            HistoricalPathContent::Binary { ref hash, bytes: 7 } if hash == "fefe"
        ));
        assert!(matches!(
            view.path_at(&frontier, "missing.txt").unwrap(),
            HistoricalPathContent::Absent
        ));
        let error = view.path_at(&frontier, "bad-json.bin").unwrap_err();
        assert!(error.to_string().contains("bad-json.bin"), "{error}");
        let error = view.path_at(&frontier, "no-hash.bin").unwrap_err();
        assert!(error.to_string().contains("no hash"), "{error}");
        let error = view.path_at(&frontier, "num.bin").unwrap_err();
        assert!(error.to_string().contains("is not text"), "{error}");
    }

    #[test]
    fn history_view_memoizes_forks_and_rejects_foreign_frontiers() {
        let doc = LoroDoc::new();
        #[allow(deprecated)] // mirror files_map_text until the sync-era migration
        let text = doc
            .get_map(FILES_MAP)
            .get_or_create_container("a.txt", loro::LoroText::new())
            .unwrap();
        text.insert(0, "hello").unwrap();
        doc.commit();
        let frontier = doc.state_frontiers();

        // A frontier from a different history is not part of this store.
        let foreign = {
            let other = LoroDoc::new();
            #[allow(deprecated)] // mirror files_map_text until the sync-era migration
            let text = other
                .get_map(FILES_MAP)
                .get_or_create_container("a.txt", loro::LoroText::new())
                .unwrap();
            text.insert(0, "other").unwrap();
            other.commit();
            other.state_frontiers()
        };

        let error = entries_at(&doc, &foreign).unwrap_err();
        assert!(matches!(
            error,
            crate::error::SheafError::TimelineReference(_)
        ));
        assert!(error.to_string().contains("not part of this store"));

        let mut view = HistoryView::open(&doc).unwrap();
        assert!(matches!(
            view.entries_at(&foreign).unwrap_err(),
            crate::error::SheafError::TimelineReference(_)
        ));
        assert!(matches!(
            view.path_at(&foreign, "a.txt").unwrap_err(),
            crate::error::SheafError::TimelineReference(_)
        ));
        // Read paths that tolerate unknown points simply answer "nothing".
        assert!(view.text_keys_at(&foreign).unwrap().is_empty());
        assert!(view.renames_at(&foreign).unwrap().is_empty());

        // Forks are memoized per distinct frontier: a repeated read must not
        // materialize the point twice.
        view.entries_at(&frontier).unwrap();
        view.entries_at(&frontier).unwrap();
        assert_eq!(view.forks_created(), 1);
        assert!(view
            .text_keys_at(&frontier)
            .unwrap()
            .contains(&"a.txt".to_string()));
    }

    #[test]
    fn scope_keys_resolve_dot_components_and_refuse_escapes() {
        let root = Path::new("/proj");
        let cwd = root;
        // `..` at the root escapes the project.
        assert!(scope_key(root, cwd, "..").is_err());
        // Interior dot components normalize lexically, no filesystem needed.
        assert_eq!(scope_key(root, cwd, "./a/../b.txt").unwrap(), "b.txt");
        assert_eq!(scope_key(root, cwd, "a/./b").unwrap(), "a/b");
    }

    #[test]
    fn expand_scope_validates_rename_records_and_expands_transitively() {
        // Rename records with unusable keys are refused, never half-applied.
        assert!(expand_scope_through_renames(&["a".into()], &[("..".into(), "x".into())]).is_err());
        // Expansion is transitive across chains.
        let names = expand_names(
            &["a".to_string()].into_iter().collect(),
            &[
                ("a".to_string(), "b".to_string()),
                ("b".to_string(), "c".to_string()),
            ],
        );
        assert_eq!(
            names,
            ["a", "b", "c"]
                .map(str::to_string)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn build_plan_surfaces_every_obstruction_kind() {
        let root = tmp("obstructions");
        skeleton(&root);
        let mut target_entries = BTreeMap::new();
        target_entries.insert("../evil".to_string(), text_entry("x")); // escapes root
        target_entries.insert("d.txt".to_string(), text_entry("x")); // directory in the way
        target_entries.insert("l.txt".to_string(), text_entry("x")); // symlink in the way
        target_entries.insert("s.txt".to_string(), text_entry("x")); // unreadable
        target_entries.insert("b.bin".to_string(), Entry::binary("fefe".into(), 7, false)); // missing blob

        std::fs::create_dir_all(root.join("d.txt")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("elsewhere", root.join("l.txt")).unwrap();
        write(&root, "s.txt", b"secret");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.join("s.txt"), std::fs::Permissions::from_mode(0o000))
                .unwrap();
        }

        let plan = build_plan(
            &root,
            &store_dir(&root),
            point("ff"),
            point("ee"),
            &BTreeMap::new(),
            &target_entries,
            &[],
            &[],
            &ignores(),
            false,
        )
        .unwrap();
        assert!(!plan.applicable());
        for expected in [
            Obstacle::EscapesRoot,
            Obstacle::DirectoryInTheWay,
            Obstacle::SymlinkInTheWay,
            Obstacle::Unreadable,
            Obstacle::MissingBlob,
        ] {
            assert!(
                plan.obstructions.iter().any(|o| o.obstacle == expected),
                "missing {expected:?} in {:#?}",
                plan.obstructions
            );
        }
        // The obstruction summary names paths and codes.
        let described = describe(&plan.obstructions);
        assert!(described.contains("EscapesRoot") && described.contains("../evil"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                root.join("s.txt"),
                std::fs::Permissions::from_mode(0o644),
            );
        }
    }

    #[test]
    fn build_plan_counts_unchanged_paths_and_local_deletes() {
        let root = tmp("counts");
        skeleton(&root);
        // At the target point the file is gone; the live tree never had it
        // either: nothing to do, counted as unchanged.
        let mut base_entries = BTreeMap::new();
        base_entries.insert("vanished.txt".to_string(), text_entry("v"));
        // At the target point the file is gone; the live tree still holds a
        // locally modified copy: a delete that overwrites local work.
        base_entries.insert("kept.txt".to_string(), text_entry("v1"));
        write(&root, "kept.txt", b"v2 locally");

        let plan = build_plan(
            &root,
            &store_dir(&root),
            point("ff"),
            point("ee"),
            &base_entries,
            &BTreeMap::new(),
            &["vanished.txt".to_string(), "kept.txt".to_string()],
            &[],
            &ignores(),
            false,
        )
        .unwrap();
        assert_eq!(plan.unchanged, 1);
        assert_eq!(plan.deletes(), 1);
        assert_eq!(plan.writes(), 0);
        let delete = &plan.actions[0];
        assert_eq!(delete.kind, ActionKind::Delete);
        assert!(delete.local_modified, "uncaptured bytes are flagged");
        // A scope key nothing has ever seen is reported, not silently dropped.
        let plan = build_plan(
            &root,
            &store_dir(&root),
            point("ff"),
            point("ee"),
            &base_entries,
            &BTreeMap::new(),
            &["typo/ghost.txt".to_string()],
            &[],
            &ignores(),
            false,
        )
        .unwrap();
        assert_eq!(plan.scope_missing, vec!["typo/ghost.txt".to_string()]);
    }

    #[test]
    fn install_streams_verifies_blobs_and_preserves_the_exec_bit() {
        let root = tmp("install");
        skeleton(&root);
        let mut store = open(&root);

        let bytes: Vec<u8> = [0u8, 1, 2, 255, 128]
            .iter()
            .copied()
            .cycle()
            .take(100_000)
            .collect();
        write(&root, "bin.dat", &bytes);
        flush(&mut store, &root, vec![added(&root, "bin.dat")]);
        let hash = blobs::hash_of(&bytes);
        let blob = blobs::blob_path(&store_dir(&root), &hash);
        assert!(blob.exists(), "the capture stored a content-addressed blob");

        // A blob whose bytes no longer match its digest refuses to install.
        std::fs::write(&blob, b"corrupt").unwrap();
        let entry = Entry::binary(hash.clone(), bytes.len() as u64, false);
        let error = store.install("bin.dat", &entry).unwrap_err();
        assert!(error.to_string().contains("content mismatch"), "{error}");

        // With the payload intact the install is byte-exact.
        std::fs::write(&blob, &bytes).unwrap();
        store.install("bin.dat", &entry).unwrap();
        assert_eq!(std::fs::read(root.join("bin.dat")).unwrap(), bytes);

        // The recorded exec bit is applied before publish.
        store
            .install("run.sh", &Entry::text("#!/bin/sh\n".into(), true))
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(root.join("run.sh"))
                .unwrap()
                .permissions()
                .mode();
            assert_ne!(mode & 0o111, 0, "exec bit must survive the restore");
        }
    }

    #[test]
    fn pending_restore_parses_quarantines_and_ages() {
        let root = tmp("intent");
        skeleton(&root);
        let state = state_dir(&root);
        std::fs::create_dir_all(&state).unwrap();
        let intent = RestoreIntent {
            token: "tok-1".into(),
            mode: RestoreMode::Scoped,
            scope: vec!["a.txt".to_string()],
            target: point("ff"),
            started_ms: chrono::Utc::now().timestamp_millis(),
            fragment: None,
        };
        std::fs::write(
            state.join(INTENT_FILE),
            serde_json::to_vec_pretty(&intent).unwrap(),
        )
        .unwrap();
        let parsed = pending_restore_at(&root).expect("a well-formed intent parses");
        assert_eq!(parsed.token, "tok-1");

        // An unreadable intent is quarantined as .bad, never silently retried.
        std::fs::write(state.join(INTENT_FILE), "{not json").unwrap();
        assert!(pending_restore_at(&root).is_none());
        assert!(!state.join(INTENT_FILE).exists(), "quarantined away");
        assert!(state.join("restore.intent.bad").exists());

        // Staleness: past the bound and non-negative bound only.
        let mut aged = intent.clone();
        aged.started_ms = chrono::Utc::now().timestamp_millis() - 10_000;
        assert!(aged.is_stale(5_000));
        assert!(!aged.is_stale(20_000));
        assert!(!aged.is_stale(-1), "a negative bound disables staleness");
        aged.started_ms = chrono::Utc::now().timestamp_millis() + 60_000;
        assert!(!aged.is_stale(0), "a future start never reads as stale");
    }

    #[test]
    fn install_and_delete_sequencing_round_trips_a_renamed_tree() {
        let root = tmp("roundtrip");
        skeleton(&root);
        let mut store = open(&root);

        // P1: a.txt, gone-later extra file, and a file that will move.
        write(&root, "a.txt", V1.as_bytes());
        write(&root, "sub/move.txt", b"moved content\n");
        flush(
            &mut store,
            &root,
            vec![added(&root, "a.txt"), added(&root, "sub/move.txt")],
        );
        let p1 = store.resolve("@").unwrap();

        // P2: a.txt edited; sub/move.txt renamed into other/.
        write(&root, "a.txt", V2.as_bytes());
        write(&root, "extra.txt", b"extra\n");
        std::fs::create_dir_all(root.join("other")).unwrap();
        std::fs::rename(root.join("sub/move.txt"), root.join("other/moved.txt")).unwrap();
        flush(
            &mut store,
            &root,
            vec![
                touched(&root, "a.txt"),
                added(&root, "extra.txt"),
                renamed(&root, "sub/move.txt", "other/moved.txt"),
            ],
        );
        let p2 = store.resolve("@").unwrap();

        // Rewind to P1: update a.txt, delete extra.txt and other/moved.txt,
        // recreate sub/move.txt — the delete/create pair with identical
        // content must be recorded as ONE rename so history stays followable.
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.obstructions);
        assert_eq!(plan.mode, RestoreMode::Scoped);
        let outcome = store.apply_restore(&plan, &ignores()).unwrap();
        assert_eq!(outcome.files_written, 2, "{outcome:#?}");
        assert_eq!(outcome.files_deleted, 2);
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), V1);
        assert_eq!(
            std::fs::read_to_string(root.join("sub/move.txt")).unwrap(),
            "moved content\n"
        );
        assert!(!root.join("extra.txt").exists());
        assert!(!root.join("other").exists(), "emptied dirs are pruned");
        assert!(outcome.restore_capture.is_some());
        assert!(outcome
            .progress_log
            .iter()
            .any(|line| line.starts_with("delete other/moved.txt")));
        assert!(outcome
            .progress_log
            .iter()
            .any(|line| line.starts_with("create sub/move.txt")));

        // The forward capture recorded the move as a rename: following
        // either name reaches the restore capture.
        let moved_lineage = store
            .captures(false, Some(Path::new("other/moved.txt")), true, 50)
            .unwrap();
        assert!(
            moved_lineage.len() >= 2,
            "rename-aware history must cross the restore capture: {moved_lineage:#?}"
        );

        // A plan computed against the already-restored worktree is a no-op,
        // and applying it reports that instead of rewriting anything.
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        assert!(plan.is_noop());
        let outcome = store.apply_restore(&plan, &ignores()).unwrap();
        assert_eq!(outcome.files_written, 0);
        assert!(outcome.restore_capture.is_none());
        assert!(outcome
            .progress_log
            .iter()
            .any(|line| line.contains("already matched the target point")));

        // The restore is undoable by name: back to P2 re-lands every byte.
        let plan = store
            .plan_restore_at(
                &p2,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        store.apply_restore(&plan, &ignores()).unwrap();
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), V2);
        assert_eq!(
            std::fs::read_to_string(root.join("other/moved.txt")).unwrap(),
            "moved content\n"
        );
        assert!(!root.join("sub").exists(), "the rewind prunes emptied dirs");
    }

    #[test]
    fn apply_fails_closed_on_stale_and_obstructed_plans_and_rescues_drift() {
        let root = tmp("failclosed");
        skeleton(&root);
        let mut store = open(&root);
        write(&root, "a.txt", V1.as_bytes());
        flush(&mut store, &root, vec![added(&root, "a.txt")]);
        let p1 = store.resolve("@").unwrap();
        write(&root, "a.txt", V2.as_bytes());
        write(&root, "extra.txt", b"extra\n");
        flush(
            &mut store,
            &root,
            vec![touched(&root, "a.txt"), added(&root, "extra.txt")],
        );

        // A plan, then an IN-SCOPE tree change that changes the action set:
        // the apply must refuse BEFORE capturing or writing anything.
        // (Local content edits to action targets are deliberately NOT
        // staleness — the token ignores them and apply rescues them as
        // concurrent edits. A new in-scope path changes the action set
        // itself, which the token covers.)
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        write(&root, "sub/new.txt", b"born after the plan\n");
        let error = store.apply_restore(&plan, &ignores()).unwrap_err();
        assert!(
            matches!(error, crate::error::SheafError::RestorePlanStale(_)),
            "{error}"
        );
        assert!(store.pending_restore().is_none(), "nothing was started");
        std::fs::remove_file(root.join("sub/new.txt")).unwrap();

        // An obstruction discovered at revalidation refuses by code.
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        std::fs::remove_file(root.join("a.txt")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("elsewhere", root.join("a.txt")).unwrap();
        let error = store.apply_restore(&plan, &ignores()).unwrap_err();
        assert!(error.to_string().contains("SymlinkInTheWay"), "{error}");
        std::fs::remove_file(root.join("a.txt")).unwrap();
        write(&root, "a.txt", V2.as_bytes());

        // Concurrent edits to action targets are captured, not clobbered:
        // the local edit of a.txt becomes history (via the pre-restore
        // safety capture) before the rewind touches it, and the pending
        // delete of extra.txt still lands.
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        write(&root, "a.txt", "concurrent local edit\n".as_bytes());
        let outcome = store.apply_restore(&plan, &ignores()).unwrap();
        assert!(outcome.pre_restore_capture.is_some());
        let preserved = match store
            .historical_path_content("@~1", "a.txt")
            .expect("safety capture must resolve")
        {
            HistoricalPathContent::Text(text) => text,
            other => panic!("expected text at @~1:a.txt, got {other:?}"),
        };
        assert_eq!(preserved, "concurrent local edit\n");
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), V1);
        assert!(!root.join("extra.txt").exists());
        assert!(store.pending_restore().is_none(), "intent cleared");
    }

    #[test]
    fn resume_replays_only_unforced_stale_bounds_and_abandon_discards() {
        let root = tmp("resume");
        skeleton(&root);
        let mut store = open(&root);
        write(&root, "a.txt", V1.as_bytes());
        flush(&mut store, &root, vec![added(&root, "a.txt")]);
        let p1 = store.resolve("@").unwrap();
        write(&root, "a.txt", V2.as_bytes());
        flush(&mut store, &root, vec![touched(&root, "a.txt")]);

        // An outstanding intent from a crashed restore.
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        store.write_intent(&plan).unwrap();
        assert!(store.pending_restore().is_some());

        // Backdate it: an automatic replay must refuse — later work wins —
        // and the intent must survive untouched for the operator.
        let intent_path = state_dir(&root).join(INTENT_FILE);
        let mut raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&intent_path).unwrap()).unwrap();
        raw["started_ms"] = serde_json::json!(chrono::Utc::now().timestamp_millis() - 10_000);
        std::fs::write(&intent_path, raw.to_string()).unwrap();
        assert!(store
            .resume_restore(&ignores(), false, 5_000)
            .unwrap()
            .is_none());
        assert!(store.pending_restore().is_some(), "stale intent kept");
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), V2);

        // The explicit operator path forces the replay to convergence.
        let outcome = store
            .resume_restore(&ignores(), true, 5_000)
            .unwrap()
            .expect("forced resume replays the intent");
        assert!(outcome.resumed);
        assert_eq!(std::fs::read_to_string(root.join("a.txt")).unwrap(), V1);
        assert!(store.pending_restore().is_none(), "intent cleared");

        // Abandon discards an outstanding intent and reconciles the worktree
        // as it stands, so nothing on disk is uncaptured afterwards.
        write(&root, "a.txt", "abandonment edits\n".as_bytes());
        let plan = store
            .plan_restore_at(
                &p1,
                &scope(&["a.txt", "extra.txt", "sub", "other"]),
                &ignores(),
            )
            .unwrap();
        store.write_intent(&plan).unwrap();
        let capture = store.abandon_restore(&ignores()).unwrap();
        assert!(capture.is_some(), "the uncaptured edit became history");
        assert!(store.pending_restore().is_none());
        assert_eq!(
            std::fs::read_to_string(root.join("a.txt")).unwrap(),
            "abandonment edits\n"
        );
    }

    #[test]
    fn reconcile_batches_large_captures_and_drops_now_ignored_paths() {
        let root = tmp("reconcile");
        skeleton(&root);
        let mut store = open(&root);

        // 300 untracked files: the walk-side loop must flush mid-capture at
        // the batch boundary instead of buffering without bound.
        for i in 0..300 {
            write(&root, &format!("bulk/f{i:03}.txt"), b"v1");
        }
        let first = store
            .reconcile_worktree(&ignores())
            .unwrap()
            .expect("bulk capture");
        assert!(first.short_id().len() <= 12);

        // 300 modified known files (the known-side loop batches too) plus a
        // path the operator just ignored: doc-only removal, disk untouched.
        for i in 0..300 {
            write(&root, &format!("bulk/f{i:03}.txt"), b"v2");
        }
        let mut patterns = config::default_patterns();
        patterns.push("bulk/f000.txt".to_string());
        let narrowed = IgnoreSet::from_patterns(&patterns).unwrap();
        let _ = store.reconcile_worktree(&narrowed).unwrap();
        assert!(
            !store.knows("bulk/f000.txt"),
            "a newly ignored tracked path is dropped from the document"
        );
        assert!(root.join("bulk/f000.txt").exists(), "the disk file stays");

        // The known-side flush split the 300 touches across the boundary.
        let captures = store.captures(false, None, false, 3).unwrap();
        let batched: Vec<usize> = captures
            .iter()
            .filter(|c| c.paths.iter().any(|p| p.starts_with("bulk/")))
            .map(|c| c.events)
            .collect();
        assert!(
            batched.len() >= 2,
            "a 301-event reconcile must span multiple captures: {batched:#?}"
        );
    }

    #[test]
    fn linked_worktree_restore_intent_is_independent_of_primary() {
        let root = tmp("wt-intent");
        skeleton(&root);
        let mut store = open(&root);
        write(&root, "a.txt", V1.as_bytes());
        flush(&mut store, &root, vec![added(&root, "a.txt")]);
        let p1 = store.resolve("@").unwrap();
        write(&root, "a.txt", V2.as_bytes());
        flush(&mut store, &root, vec![touched(&root, "a.txt")]);

        // Materialize a linked worktree sharing this store.
        let linked = root.parent().unwrap().join(format!(
            "{}-linked",
            root.file_name().unwrap().to_string_lossy()
        ));
        store.add_worktree("@", &linked).unwrap();

        // A plan is enough to author an intent; the on-disk path is the contract.
        let plan = store.plan_restore_at(&p1, &[], &ignores()).unwrap();
        assert_ne!(restore_intent_path(&root), restore_intent_path(&linked));

        // The primary intent lands at the primary's own path only.
        store.write_intent(&plan).unwrap();
        assert!(pending_restore_at(&root).is_some());
        assert!(
            pending_restore_at(&linked).is_none(),
            "linked worktree has no intent of its own yet"
        );

        // The linked worktree writes its own, independent intent.
        store.activate_worktree(&linked).unwrap();
        store.write_intent(&plan).unwrap();
        assert!(pending_restore_at(&linked).is_some());
        assert!(
            pending_restore_at(&root).is_some(),
            "writing the linked intent must not disturb the primary's"
        );

        // Clearing the linked intent removes only that worktree's marker.
        store.clear_intent();
        assert!(pending_restore_at(&linked).is_none());
        assert!(pending_restore_at(&root).is_some());

        // The primary clears its own independently.
        store.activate_worktree(&root).unwrap();
        store.clear_intent();
        assert!(pending_restore_at(&root).is_none());
    }
}

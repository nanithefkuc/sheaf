//! Worktree/point differences rendered as git-shaped unified patches.
//!
//! One engine answers every CLI diff question:
//!
//! * `diff <point>` — the live worktree against an immutable point;
//! * `diff <a> <b>` — one point against another, across branches if need be,
//!   because both sides resolve through ordinary frontier addressing;
//! * an optional path scope narrows the comparison to files/subtrees.
//!
//! Renames are first-class: structural rename events recorded in the interval
//! between the two sides pair the old and new names even when content changed
//! along the way, and leftover delete/create pairs carrying identical content
//! pair up the same way the restore engine pairs them, so a move never reads
//! as delete-plus-create.
//!
//! Hunks ride the result for local rendering; they never serialize onto the
//! wire — the daemon-CLI IPC contract carries the rendered patch as body bytes
//! instead.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use loro::{Frontiers, LoroDoc};
use serde::{Deserialize, Serialize};

use super::restore::{
    canonical_scope, entries_of_state_scoped, in_scope, live_files, Content, Entry, HistoryView,
};
use super::timeline::{decode_frontier, resolve_in_doc, ResolvedPoint};
use super::{ProjectStore, TimelineReader};
use crate::error::Result;
use crate::ignore::IgnoreSet;

/// Lines of context around each change region.
const CONTEXT: usize = 3;
/// Edit-distance ceiling for the line differ. Past this — or past the cell
/// budget below — the file pair falls back to one whole-file rewrite hunk:
/// still a correct patch, bounded cost.
const MAX_EDIT_DISTANCE: usize = 1500;
/// Rough upper bound on forward-pass work (cells visited ≈ D × (N+M)) so a
/// pair of huge, nearly-unrelated files costs the same as a small one.
const MAX_CELL_BUDGET: usize = 12_000_000;

// ------------------------------------------------------------------ shapes

/// How one file changed between the two sides of a comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    Added,
    Deleted,
    Modified,
    /// Old and new path names for the same content lineage.
    Renamed,
    /// Text became binary or the reverse.
    TypeChanged,
}

/// Content summary for one side of a file pair. Never carries file bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideContent {
    Absent,
    Text { bytes: u64 },
    Binary { hash: String, bytes: u64 },
}

/// One file's change: its paths, kind, per-side content summary, line counts,
/// and locally-rendered hunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDiff {
    /// Root-relative POSIX path on the new side (old name for deletions).
    pub path: String,
    /// Original name, when the entry is a rename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_path: Option<String>,
    pub kind: DiffKind,
    pub old: SideContent,
    pub new: SideContent,
    #[serde(default)]
    pub added_lines: usize,
    #[serde(default)]
    pub removed_lines: usize,
    /// Rendered unified hunks (header + lines). Local only: never serialized,
    /// because the wire carries the whole patch as body bytes instead.
    #[serde(skip_serializing, default)]
    pub hunks: Vec<String>,
}

/// Which point a side of the comparison was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideDesc {
    /// `worktree` or `point`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontier: Option<String>,
}

/// Full result of a diff: the two sides described, every changed file, and
/// whether the comparison ran in read-only degraded mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffOutcome {
    pub from: SideDesc,
    pub to: SideDesc,
    pub entries: Vec<FileDiff>,
    #[serde(default)]
    pub degraded: bool,
}

impl DiffOutcome {
    /// True when the two sides are identical (no changed files).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Git-shaped unified patch for the whole comparison.
    pub fn render_patch(&self) -> Vec<u8> {
        let mut out = String::new();
        for entry in &self.entries {
            let old_path = entry.old_path.as_deref().unwrap_or(&entry.path);
            out.push_str(&format!("diff --sheaf a/{old_path} b/{}\n", entry.path));
            if let Some(original) = &entry.old_path {
                out.push_str(&format!("rename from {original}\n"));
                out.push_str(&format!("rename to {}\n", entry.path));
            }
            let binary_sides = matches!(entry.old, SideContent::Binary { .. })
                || matches!(entry.new, SideContent::Binary { .. });
            if binary_sides {
                if entry.old == entry.new {
                    continue; // pure binary rename: headers say it all
                }
                out.push_str(&format!(
                    "Binary files a/{old_path} and b/{} differ\n",
                    entry.path
                ));
                continue;
            }
            match entry.old {
                SideContent::Absent => out.push_str("--- /dev/null\n"),
                _ => out.push_str(&format!("--- a/{old_path}\n")),
            }
            match entry.new {
                SideContent::Absent => out.push_str("+++ /dev/null\n"),
                _ => out.push_str(&format!("+++ b/{}\n", entry.path)),
            }
            for hunk in &entry.hunks {
                out.push_str(hunk);
            }
        }
        out.into_bytes()
    }
}

// -------------------------------------------------------------- computation

/// Compare `from` (a timeline reference) against `to`, or against the live
/// worktree when `to` is absent. Pure computation: nothing is written.
#[allow(clippy::too_many_arguments)]
pub(super) fn compute_diff(
    root: &Path,
    doc: &LoroDoc,
    ledger: &super::ledger::LedgerState,
    current: &Frontiers,
    from_ref: &str,
    to_ref: Option<&str>,
    paths: &[String],
    ignore: &IgnoreSet,
) -> Result<DiffOutcome> {
    let from_point = resolve_in_doc(doc, ledger, current, from_ref)?;
    let to_point = match to_ref {
        Some(reference) => Some(resolve_in_doc(doc, ledger, current, reference)?),
        None => None,
    };
    compute_diff_points(root, doc, current, from_point, to_point, paths, ignore)
}

/// Compare two already-resolved immutable points. Timeline detail views use
/// this to compare a capture with its exact parent frontier, which need not
/// itself have a user-facing capture ID.
pub(super) fn compute_diff_points(
    root: &Path,
    doc: &LoroDoc,
    current: &Frontiers,
    from_point: ResolvedPoint,
    to_point: Option<ResolvedPoint>,
    paths: &[String],
    ignore: &IgnoreSet,
) -> Result<DiffOutcome> {
    let to_frontier = match &to_point {
        Some(point) => Some(decode_frontier(&point.frontier)?),
        None => None,
    };
    let from_frontier = decode_frontier(&from_point.frontier)?;
    let scope = canonical_scope(paths)?;

    // Materializing a historical fork is the dominant cost on a long
    // timeline. The common `diff @` path already has that exact state in the
    // live document, so read it directly. Scope filtering happens before text
    // copies and before the worktree walk, keeping a one-path diff independent
    // of unrelated source trees and large binary artifacts.
    let mut view = HistoryView::open(doc)?;
    let from_entries = if &from_frontier == current {
        entries_of_state_scoped(doc, &scope)
    } else {
        view.entries_at_scoped(&from_frontier, &scope)?
    };
    let to_entries = match &to_frontier {
        Some(frontier) if frontier == current => entries_of_state_scoped(doc, &scope),
        Some(frontier) => view.entries_at_scoped(frontier, &scope)?,
        None => worktree_entries(root, ignore, &scope)?,
    };

    // Renames recorded between the two sides pair names ahead of content.
    // For a worktree comparison the recorded interval ends at the
    // materialized head; a rename still sitting in an open debounce window
    // simply falls through to identity pairing below. Equal endpoints have no
    // interval and, importantly, need no historical forks.
    let rename_base = to_frontier.as_ref().unwrap_or(current);
    let interval = if &from_frontier == rename_base {
        Vec::new()
    } else {
        interval_renames(&mut view, &from_frontier, rename_base)?
    };
    let mut old_of_new: BTreeMap<String, String> = BTreeMap::new();
    for (from_name, to_name) in &interval {
        // Forward comparison: old name on the from side, new on the to side.
        if !old_of_new.contains_key(to_name)
            && from_entries.contains_key(from_name)
            && to_entries.contains_key(to_name)
        {
            old_of_new.insert(to_name.clone(), from_name.clone());
        }
        // A reversed comparison (older point second) meets the same event
        // from the other direction.
        if !old_of_new.contains_key(from_name)
            && from_entries.contains_key(to_name)
            && to_entries.contains_key(from_name)
        {
            old_of_new.insert(from_name.clone(), to_name.clone());
        }
    }

    let mut candidates: BTreeSet<&str> = BTreeSet::new();
    candidates.extend(from_entries.keys().map(String::as_str));
    candidates.extend(to_entries.keys().map(String::as_str));

    // Leftover delete/create pairs with identical content are moves the
    // interval does not know about (an unflushed rename, most often).
    let mut taken_old: BTreeSet<String> = old_of_new.values().cloned().collect();
    let mut deletes: Vec<(&String, &Entry)> = Vec::new();
    let mut adds: Vec<(&String, &Entry)> = Vec::new();
    let owned: Vec<String> = candidates.iter().map(|k| k.to_string()).collect();
    for key in &owned {
        if !in_scope(key, &scope) || old_of_new.contains_key(key.as_str()) {
            continue;
        }
        match (from_entries.get(key.as_str()), to_entries.get(key.as_str())) {
            (Some(old), None) if !taken_old.contains(key) => deletes.push((key, old)),
            (None, Some(new)) => adds.push((key, new)),
            _ => {}
        }
    }
    let mut taken_new: BTreeSet<&String> = BTreeSet::new();
    for (old_key, old_entry) in &deletes {
        let identity = old_entry.identity();
        if let Some((new_key, _)) = adds.iter().find(|(new_key, new_entry)| {
            !taken_new.contains(new_key) && new_entry.identity() == identity
        }) {
            taken_new.insert(new_key);
            taken_old.insert((*old_key).clone());
            old_of_new.insert((*new_key).clone(), (**old_key).clone());
        }
    }

    let mut entries = Vec::new();
    // Old names already spoken for by a rename entry must not resurface as
    // separate Deleted entries — that would read as data loss.
    let paired_old: BTreeSet<&str> = old_of_new.values().map(String::as_str).collect();
    for key in &owned {
        if !in_scope(key, &scope) {
            continue;
        }
        if let Some(old_name) = old_of_new.get(key.as_str()) {
            if key != old_name {
                let old = from_entries.get(old_name.as_str());
                let new = to_entries.get(key.as_str());
                let (hunks, added, removed) = text_hunks(old, new);
                entries.push(FileDiff {
                    path: (*key).clone(),
                    old_path: Some(old_name.clone()),
                    kind: DiffKind::Renamed,
                    old: side_desc(old),
                    new: side_desc(new),
                    added_lines: added,
                    removed_lines: removed,
                    hunks,
                });
                continue;
            }
        }
        if paired_old.contains(key.as_str()) {
            continue;
        }
        let old = from_entries.get(key.as_str());
        let new = to_entries.get(key.as_str());
        if old == new {
            continue; // unchanged, or a rename's old name already reported
        }
        match (old, new) {
            (Some(old_entry), Some(new_entry)) => {
                let (hunks, added, removed) = text_hunks(old, new);
                entries.push(FileDiff {
                    path: (*key).clone(),
                    old_path: None,
                    kind: if old_entry.content_key() == new_entry.content_key() {
                        DiffKind::Modified
                    } else {
                        DiffKind::TypeChanged
                    },
                    old: side_desc(old),
                    new: side_desc(new),
                    added_lines: added,
                    removed_lines: removed,
                    hunks,
                });
            }
            (None, Some(new_entry)) => {
                let (hunks, added, removed) = text_hunks(None, Some(new_entry));
                entries.push(FileDiff {
                    path: (*key).clone(),
                    old_path: None,
                    kind: DiffKind::Added,
                    old: SideContent::Absent,
                    new: side_desc(new),
                    added_lines: added,
                    removed_lines: removed,
                    hunks,
                });
            }
            (Some(old_entry), None) => {
                let (hunks, added, removed) = text_hunks(Some(old_entry), None);
                entries.push(FileDiff {
                    path: (*key).clone(),
                    old_path: None,
                    kind: DiffKind::Deleted,
                    old: side_desc(old),
                    new: SideContent::Absent,
                    added_lines: added,
                    removed_lines: removed,
                    hunks,
                });
            }
            (None, None) => {}
        }
    }

    Ok(DiffOutcome {
        from: SideDesc {
            kind: "point".into(),
            capture_id: from_point.capture_id,
            frontier: Some(from_point.frontier),
        },
        to: match to_point {
            Some(point) => SideDesc {
                kind: "point".into(),
                capture_id: point.capture_id,
                frontier: Some(point.frontier),
            },
            None => SideDesc {
                kind: "worktree".into(),
                capture_id: None,
                frontier: None,
            },
        },
        entries,
        degraded: false,
    })
}

impl ProjectStore {
    /// Diff a reference against another reference, or against the live
    /// worktree when `to` is `None`.
    pub fn diff(
        &self,
        from_ref: &str,
        to_ref: Option<&str>,
        paths: &[String],
        ignore: &IgnoreSet,
    ) -> Result<DiffOutcome> {
        let current = self.materialized_frontiers();
        compute_diff(
            &self.root,
            &self.doc,
            &self.ledger,
            &current,
            from_ref,
            to_ref,
            paths,
            ignore,
        )
    }
}

impl TimelineReader {
    /// Read-only diff (degraded mode); marks itself degraded.
    pub fn diff(
        &self,
        from_ref: &str,
        to_ref: Option<&str>,
        paths: &[String],
        ignore: &IgnoreSet,
    ) -> Result<DiffOutcome> {
        let current = decode_frontier(&self.current_frontier())?;
        let mut outcome = compute_diff(
            self.root(),
            self.doc(),
            self.ledger(),
            &current,
            from_ref,
            to_ref,
            paths,
            ignore,
        )?;
        outcome.degraded = true;
        Ok(outcome)
    }
}

// ------------------------------------------------------------------ helpers

/// Tracked content of the live non-ignored worktree.
fn worktree_entries(
    root: &Path,
    ignore: &IgnoreSet,
    scope: &[String],
) -> Result<BTreeMap<String, Entry>> {
    let mut out = BTreeMap::new();
    for key in live_files(root, ignore, scope) {
        let path = root.join(&key);
        // Same policy as capture: oversized files are hashed straight from
        // disk instead of being slurped into memory.
        let oversized = std::fs::metadata(&path)
            .map(|m| m.len() > super::TEXT_MAX_BYTES)
            .unwrap_or(false);
        let entry = if oversized {
            Entry::binary(
                super::blobs::hash_file(&path)?,
                std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
                super::file_exec(&path),
            )
        } else {
            let bytes = std::fs::read(&path)?;
            match std::str::from_utf8(&bytes) {
                Ok(text) => Entry::text(text.to_owned(), super::file_exec(&path)),
                Err(_) => Entry::binary(
                    super::blobs::hash_of(&bytes),
                    bytes.len() as u64,
                    super::file_exec(&path),
                ),
            }
        };
        out.insert(key, entry);
    }
    Ok(out)
}

fn side_desc(entry: Option<&Entry>) -> SideContent {
    match entry.map(|e| &e.content) {
        None => SideContent::Absent,
        Some(Content::Text(text)) => SideContent::Text {
            bytes: text.len() as u64,
        },
        Some(Content::Binary { hash, size }) => SideContent::Binary {
            hash: hash.clone(),
            bytes: *size,
        },
    }
}

/// Unified hunks when both sides are text; empty for binary pairs.
fn text_hunks(old: Option<&Entry>, new: Option<&Entry>) -> (Vec<String>, usize, usize) {
    let (
        Some(Entry {
            content: Content::Text(old_text),
            ..
        }),
        Some(Entry {
            content: Content::Text(new_text),
            ..
        }),
    ) = (old, new)
    else {
        return (Vec::new(), 0, 0);
    };
    unified_hunks(old_text, new_text)
}

/// Rename records present at `later` but not at `earlier`.
fn interval_renames(
    view: &mut HistoryView,
    earlier: &Frontiers,
    later: &Frontiers,
) -> Result<Vec<(String, String)>> {
    view.renames_between(earlier, later)
}

// ------------------------------------------------------------- line differ

fn split_lines(text: &str) -> Vec<&str> {
    text.split_inclusive('\n')
        .map(|line| line.strip_suffix('\n').unwrap_or(line))
        .collect()
}

/// One whole-file replacement hunk (fallback for pathological distances).
fn whole_file_hunk(a: &[&str], b: &[&str], no_eol_a: bool, no_eol_b: bool) -> Vec<String> {
    if a.is_empty() && b.is_empty() {
        return Vec::new();
    }
    let mut hunk = format!("@@ -{} +{} @@\n", span(0, a.len()), span(0, b.len()));
    for (index, line) in a.iter().enumerate() {
        hunk.push('-');
        hunk.push_str(line);
        hunk.push('\n');
        if index + 1 == a.len() && no_eol_a {
            hunk.push_str("\\ No newline at end of file\n");
        }
    }
    for (index, line) in b.iter().enumerate() {
        hunk.push('+');
        hunk.push_str(line);
        hunk.push('\n');
        if index + 1 == b.len() && no_eol_b {
            hunk.push_str("\\ No newline at end of file\n");
        }
    }
    vec![hunk]
}

/// Unified-format span text: `start+1,len` (or bare `start` when empty).
fn span(start: usize, len: usize) -> String {
    if len == 0 {
        start.to_string()
    } else {
        format!("{},{}", start + 1, len)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tag {
    Equal,
    Delete,
    Insert,
}

/// Myers O(ND) with a bounded budget; `None` means "too far apart — the
/// caller falls back to a whole-file hunk".
fn myers_ops(a: &[&str], b: &[&str]) -> Option<Vec<(Tag, usize, usize)>> {
    let n = a.len();
    let m = b.len();
    if n == 0 || m == 0 {
        let mut ops = Vec::new();
        ops.extend((0..n).map(|i| (Tag::Delete, i, 0)));
        ops.extend((0..m).map(|j| (Tag::Insert, 0, j)));
        return Some(ops);
    }
    // Trim common prefix/suffix first: typical editor bursts become tiny.
    let mut lo = 0usize;
    while lo < n && lo < m && a[lo] == b[lo] {
        lo += 1;
    }
    let mut hi = 0usize;
    while hi < n - lo && hi < m - lo && a[n - 1 - hi] == b[m - 1 - hi] {
        hi += 1;
    }
    let core_a = &a[lo..n - hi];
    let core_b = &b[lo..m - hi];
    let cn = core_a.len();
    let cm = core_b.len();
    if cn == 0 || cm == 0 {
        let mut ops = Vec::new();
        ops.extend((0..cn).map(|i| (Tag::Delete, lo + i, lo)));
        ops.extend((0..cm).map(|j| (Tag::Insert, lo, lo + j)));
        return Some(ops);
    }

    let max = cn + cm;
    let offset = max as isize;
    // Adaptive distance budget: full reach for small files, a proportional
    // slice of the cell budget for large ones (never below a floor that
    // keeps ordinary scattered edits hunk-accurate).
    let d_budget = MAX_EDIT_DISTANCE.min(MAX_CELL_BUDGET / max.max(1)).max(64);
    let mut v = vec![0isize; 2 * max + 1];
    // Per-step windows of the previous row (state after step d-1, valid for
    // k in -(d-1)..=(d-1)), indexed k+(d-1) inside the window.
    let mut trace: Vec<Vec<isize>> = Vec::new();
    let mut final_d = None;
    'search: for d in 0..=max.min(d_budget) {
        let half = d.saturating_sub(1);
        trace.push(
            v[(offset - half as isize) as usize..=(offset + half as isize) as usize].to_vec(),
        );
        let di = d as isize;
        let mut k = -di;
        while k <= di {
            // Down move (insertion) from k+1 vs right move (deletion) from
            // k-1: take whichever diagonal reached further (Myers' greedy
            // rule). The ±d edges force the only legal direction.
            let down = k == -di
                || (k != di && v[(k - 1 + offset) as usize] < v[(k + 1 + offset) as usize]);
            let mut x = if down {
                v[(k + 1 + offset) as usize]
            } else {
                v[(k - 1 + offset) as usize] + 1
            };
            let mut y = x - k;
            while (x as usize) < cn && (y as usize) < cm && core_a[x as usize] == core_b[y as usize]
            {
                x += 1;
                y += 1;
            }
            v[(k + offset) as usize] = x;
            if x as usize >= cn && y as usize >= cm {
                final_d = Some(d);
                break 'search;
            }
            k += 2;
        }
    }
    let d_final = final_d?;

    // Backtrack the ops (in reverse, then flip).
    let mut ops: Vec<(Tag, usize, usize)> = Vec::new();
    let mut x = cn as isize;
    let mut y = cm as isize;
    for d in (1..=d_final).rev() {
        let row = &trace[d];
        let half = (d - 1) as isize;
        let k = x - y;
        let get = |kk: isize| row[(kk + half) as usize];
        let down = k == -(d as isize) || (k != d as isize && get(k - 1) < get(k + 1));
        let prev_k = if down { k + 1 } else { k - 1 };
        let prev_x = get(prev_k);
        let prev_y = prev_x - prev_k;
        // Walk the snake back to the pre-move corner.
        while x > prev_x && y > prev_y {
            x -= 1;
            y -= 1;
        }
        if down {
            // Down move: one line of b was inserted at prev_y. The index
            // slot carries the b-index for inserts.
            y -= 1;
            ops.push((Tag::Insert, lo + y as usize, lo + x as usize));
        } else {
            // Right move: one line of a was deleted at prev_x; the index
            // slot carries the a-index for deletes.
            x -= 1;
            ops.push((Tag::Delete, lo + x as usize, lo + y as usize));
        }
    }
    ops.reverse();
    Some(ops)
}

/// Render unified hunks between two texts; returns (hunks, added, removed).
fn unified_hunks(old: &str, new: &str) -> (Vec<String>, usize, usize) {
    let a = split_lines(old);
    let b = split_lines(new);
    let no_eol_a = !old.is_empty() && !old.ends_with('\n');
    let no_eol_b = !new.is_empty() && !new.ends_with('\n');

    let Some(ops) = myers_ops(&a, &b) else {
        return (
            whole_file_hunk(&a, &b, no_eol_a, no_eol_b),
            b.len(),
            a.len(),
        );
    };
    // Expand ops into a full alignment (equals implied between them).
    let mut delta: Vec<(Tag, usize, usize)> = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    for &(tag, index, _) in &ops {
        match tag {
            Tag::Delete => {
                while i < index {
                    delta.push((Tag::Equal, i, j));
                    i += 1;
                    j += 1;
                }
                delta.push((Tag::Delete, i, j));
                i += 1;
            }
            Tag::Insert => {
                while j < index {
                    delta.push((Tag::Equal, i, j));
                    i += 1;
                    j += 1;
                }
                delta.push((Tag::Insert, i, j));
                j += 1;
            }
            Tag::Equal => {}
        }
    }
    while i < a.len() {
        delta.push((Tag::Equal, i, j));
        i += 1;
        j += 1;
    }

    let changed: Vec<usize> = delta
        .iter()
        .enumerate()
        .filter(|(_, (tag, _, _))| *tag != Tag::Equal)
        .map(|(position, _)| position)
        .collect();
    if changed.is_empty() {
        return (Vec::new(), 0, 0);
    }

    // Group change runs separated by at most 2*CONTEXT equal lines.
    let mut groups: Vec<(usize, usize)> = Vec::new();
    let mut start = changed[0];
    let mut prev = changed[0];
    for &position in &changed[1..] {
        if position - prev > 2 * CONTEXT + 1 {
            groups.push((start, prev));
            start = position;
        }
        prev = position;
    }
    groups.push((start, prev));

    let mut hunks = Vec::with_capacity(groups.len());
    let mut added = 0usize;
    let mut removed = 0usize;
    for (first, last) in groups {
        let lo = first.saturating_sub(CONTEXT);
        let hi = (last + CONTEXT + 1).min(delta.len());
        // Start coordinates come from the first aligned step in the window.
        let a_start = delta[lo].1;
        let b_start = delta[lo].2;
        let mut a_len = 0usize;
        let mut b_len = 0usize;
        let mut body = String::new();
        for &(tag, ai, bi) in &delta[lo..hi] {
            match tag {
                Tag::Equal => {
                    a_len += 1;
                    b_len += 1;
                    body.push(' ');
                    body.push_str(a[ai]);
                    body.push('\n');
                    // The context line is the last line of a side whose file
                    // lacks a final newline — say so, once, like diff(1).
                    if (ai + 1 == a.len() && no_eol_a) || (bi + 1 == b.len() && no_eol_b) {
                        body.push_str("\\ No newline at end of file\n");
                    }
                }
                Tag::Delete => {
                    a_len += 1;
                    removed += 1;
                    body.push('-');
                    body.push_str(a[ai]);
                    body.push('\n');
                    if ai + 1 == a.len() && no_eol_a {
                        body.push_str("\\ No newline at end of file\n");
                    }
                }
                Tag::Insert => {
                    b_len += 1;
                    added += 1;
                    body.push('+');
                    body.push_str(b[bi]);
                    body.push('\n');
                    if bi + 1 == b.len() && no_eol_b {
                        body.push_str("\\ No newline at end of file\n");
                    }
                }
            }
        }
        hunks.push(format!(
            "@@ -{} +{} @@\n{body}",
            span(a_start, a_len),
            span(b_start, b_len)
        ));
    }
    (hunks, added, removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn myers_finds_minimal_edits() {
        let a: Vec<&str> = vec!["one", "two", "three", "four"];
        let b: Vec<&str> = vec!["one", "TWO", "three", "four", "five"];
        let ops = myers_ops(&a, &b).unwrap();
        let dels: Vec<_> = ops
            .iter()
            .filter(|(t, _, _)| *t == Tag::Delete)
            .map(|(_, i, _)| *i)
            .collect();
        let ins: Vec<_> = ops
            .iter()
            .filter(|(t, _, _)| *t == Tag::Insert)
            .map(|(_, j, _)| *j)
            .collect();
        assert_eq!(dels, vec![1], "only 'two' is removed");
        assert_eq!(ins, vec![1, 4], "'TWO' replaces it, 'five' appends");
    }

    #[test]
    fn myers_handles_empty_sides_directly() {
        let ops = myers_ops(&[], &["a", "b"]).unwrap();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().all(|(t, _, _)| *t == Tag::Insert));
        let ops = myers_ops(&["a"], &[]).unwrap();
        assert_eq!(ops, vec![(Tag::Delete, 0, 0)]);
    }

    #[test]
    fn hunks_render_context_and_counts() {
        let old = "l1\nl2\nl3\nl4\nl5\nl6\nl7\nl8\nl9\nl10\n";
        let new = "l1\nl2\nl3\nl4\nl5\nl6\nCHANGED\nl8\nl9\nl10\n";
        let (hunks, added, removed) = unified_hunks(old, new);
        assert_eq!((added, removed), (1, 1));
        assert_eq!(hunks.len(), 1);
        assert!(hunks[0].starts_with("@@ -4,7 +4,7 @@"), "{}", hunks[0]);
        assert!(hunks[0].contains("-l7\n"));
        assert!(hunks[0].contains("+CHANGED\n"));
        assert!(hunks[0].contains(" l4\n"));
    }

    #[test]
    fn distant_files_fall_back_to_whole_file_hunk() {
        let old: String = (0..900).map(|i| format!("old{i}\n")).collect();
        let new: String = (0..900).map(|i| format!("new{i}\n")).collect();
        let (hunks, added, removed) = unified_hunks(&old, &new);
        assert_eq!(hunks.len(), 1);
        assert_eq!((added, removed), (900, 900));
        assert!(hunks[0].starts_with("@@ -1,900 +1,900 @@"));
    }

    #[test]
    fn missing_trailing_newline_is_marked() {
        // Both sides end without a newline: each changed line is marked.
        let (hunks, _, _) = unified_hunks("one\nlast", "one\nLAST");
        assert_eq!(hunks.len(), 1);
        assert!(hunks[0].contains("-last\n\\ No newline at end of file\n"));
        assert!(hunks[0].contains("+LAST\n\\ No newline at end of file\n"));
        // A pure addition at the end of a newline-less file marks the added
        // line; the shared context line carries the old side's marker.
        let (hunks, _, _) = unified_hunks("abc", "abc\ndef");
        assert!(hunks[0].contains("+def\n\\ No newline at end of file\n"));
    }

    #[test]
    fn identical_content_has_no_hunks() {
        let (hunks, added, removed) = unified_hunks("same\n", "same\n");
        assert!(hunks.is_empty());
        assert_eq!((added, removed), (0, 0));
    }

    #[test]
    fn patch_renders_git_shapes() {
        let outcome = DiffOutcome {
            from: SideDesc {
                kind: "point".into(),
                capture_id: Some("f".repeat(64)),
                frontier: Some("ff".into()),
            },
            to: SideDesc {
                kind: "worktree".into(),
                capture_id: None,
                frontier: None,
            },
            entries: vec![
                FileDiff {
                    path: "renamed.txt".into(),
                    old_path: Some("original.txt".into()),
                    kind: DiffKind::Renamed,
                    old: SideContent::Text { bytes: 3 },
                    new: SideContent::Text { bytes: 3 },
                    added_lines: 0,
                    removed_lines: 0,
                    hunks: vec![],
                },
                FileDiff {
                    path: "logo.bin".into(),
                    old_path: None,
                    kind: DiffKind::Modified,
                    old: SideContent::Binary {
                        hash: "aa".into(),
                        bytes: 10,
                    },
                    new: SideContent::Binary {
                        hash: "bb".into(),
                        bytes: 12,
                    },
                    added_lines: 0,
                    removed_lines: 0,
                    hunks: vec![],
                },
                FileDiff {
                    path: "new.txt".into(),
                    old_path: None,
                    kind: DiffKind::Added,
                    old: SideContent::Absent,
                    new: SideContent::Text { bytes: 2 },
                    added_lines: 1,
                    removed_lines: 0,
                    hunks: vec!["@@ -0,0 +1,1 @@\n+hi\n".into()],
                },
            ],
            degraded: false,
        };
        let patch = String::from_utf8(outcome.render_patch()).unwrap();
        assert!(patch.contains("rename from original.txt\nrename to renamed.txt"));
        assert!(patch.contains("Binary files a/logo.bin and b/logo.bin differ"));
        assert!(patch.contains("--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1,1 @@\n+hi\n"));
    }

    #[test]
    fn hunks_never_serialize_onto_the_wire() {
        let file = FileDiff {
            path: "a".into(),
            old_path: None,
            kind: DiffKind::Modified,
            old: SideContent::Text { bytes: 1 },
            new: SideContent::Text { bytes: 2 },
            added_lines: 1,
            removed_lines: 1,
            hunks: vec!["@@ big hunk that must not ride the envelope @@\n".into()],
        };
        let json = serde_json::to_value(&file).unwrap();
        assert!(json.get("hunks").is_none(), "hunks must stay local-only");
        let round: FileDiff = serde_json::from_value(json).unwrap();
        assert!(round.hunks.is_empty());
        assert_eq!(round.added_lines, 1);
    }
}

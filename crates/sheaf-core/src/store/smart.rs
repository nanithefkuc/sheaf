//! Smart squash: selection-scoped commit planning.
//!
//! Smart squash commits the net git change for one or more selected units
//! while leaving unrelated worktree edits untouched. Two comparisons carry
//! two truths: the timeline supplies attribution, but the commit
//! patch comes from `HEAD` versus the live worktree. This module computes
//! that patch from selection handles without touching git: the caller
//! supplies HEAD-side file content through a resolver, everything else is
//! derived from the store and the live tree.
//!
//! Each side uses the exact primitive it can prove. The **worktree** side
//! binds through the handle's verified context pair: the unit is the region
//! between the same two unique context
//! anchors, so an edited, inserted, or deleted unit all resolve — an
//! empty region is a real state, not an error. The **HEAD** side never
//! trusts contexts at all: neighbors may have been edited into the
//! handle's 64-byte windows, so the head extent is instead the image of
//! the worktree extent under a deterministic line diff of HEAD versus the
//! worktree (Myers, no fuzz). Unchanged lines pin both boundaries exactly;
//! a boundary inside an insertion hunk maps to that hunk's unique head
//! seam. Together the two rules cover replace, insert, and delete without
//! mode flags and fail closed on every real ambiguity.

use std::collections::BTreeMap;
use std::path::Path;

use loro::LoroDoc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::restore::{expand_names, HistoryView};
use super::selection::{
    overlapping_match_starts, rebind_exact, ByteRange, HistoricalPathContent, RebindOutcome,
    SelectionCandidate, SelectionExtent, SelectionHandle,
};
use super::timeline::{decode_frontier, OriginKind, ResolvedPoint};
use super::{ProjectStore, TimelineReader};
use crate::error::Result;

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

// ------------------------------------------------------------------ types

/// How a selection's extent resolved. `start == end` is a real state (the
/// anchored region is empty: an insertion site or a deletion scar), not an
/// error.
type SideExtent = ByteRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmartKind {
    /// Non-empty in both HEAD and worktree.
    Replace,
    /// Empty in HEAD, non-empty in the worktree: new content.
    Insert,
    /// Non-empty in HEAD, empty in the worktree: removal.
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmartCondition {
    /// Multiple context-anchored candidates.
    Ambiguous,
    /// The context anchors do not identify a region on the named side.
    Missing,
    /// The handle does not describe its own source.
    InvalidSource,
    /// Source extent absent, binary, or pruned at its frontier.
    UnsupportedSource,
    /// `symbol` extents need the prototype parser, which no mutating
    /// command may rely on, and `hunk` has no public surface yet.
    UnsupportedExtent,
    /// The destination file does not exist in HEAD: whole-file adds are
    /// ordinary squash's job, and committing a fragment of a new file
    /// would strand its imports (explicitly out of scope).
    NewFileSinceHead,
    /// The destination file is gone from the worktree: whole-file removals
    /// are ordinary squash's job.
    FileDeletedInWorktree,
    /// The handle anchors different paths on the two sides — the file was
    /// renamed between HEAD and the worktree; stage the whole file with
    /// ordinary squash.
    RenamedSinceHead,
    /// Two selections anchor overlapping HEAD-side regions in one file.
    Overlap,
    /// The HEAD↔worktree line alignment cannot express the selection
    /// (crossed boundaries or an exceeded line budget). Nothing is
    /// guessed.
    Unaligned,
    /// The destination file cannot be read.
    Unreadable,
    /// Every selection is already byte-identical on both sides: staging
    /// would commit nothing.
    EmptyPatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmartSide {
    Head,
    Worktree,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartCandidate {
    pub path: String,
    pub range: ByteRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartConflict {
    pub selection_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Which side failed, when the condition is side-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<SmartSide>,
    pub condition: SmartCondition,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<SmartCandidate>,
    pub detail: String,
}

/// One selection resolved on both sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartSelection {
    pub selection_id: String,
    pub handle: SelectionHandle,
    /// Destination path (rename-followed; identical on both sides by
    /// construction — differing paths are a `RenamedSinceHead` refusal).
    pub path: String,
    pub kind: SmartKind,
    /// HEAD-side extent, in HEAD-content coordinates.
    pub head: SideExtent,
    /// Worktree-side extent, in live-content coordinates.
    pub worktree: SideExtent,
    /// The exact worktree bytes read at binding time. The staged tree gets
    /// these, not a re-read of the live file, so planning has no window in
    /// which a concurrent edit can shift coordinates under it.
    pub staged_fragment: String,
    /// Bytes the staged tree gains (worktree extent length).
    pub staged_bytes: usize,
    /// Bytes the staged tree loses (HEAD extent length).
    pub retired_bytes: usize,
}

/// One file's staged content: HEAD text with every selected extent spliced
/// in. `staged_sha256` is the byte-for-byte proof that applying the file
/// plan to HEAD text reproduces `staged_text`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartFilePlan {
    pub path: String,
    pub head_sha256: String,
    pub staged_sha256: String,
    pub staged_text: String,
    pub added_bytes: usize,
    pub retired_bytes: usize,
}

/// A read-only smart-squash plan: no git contact, no worktree writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartPlan {
    pub selections: Vec<SmartSelection>,
    pub files: Vec<SmartFilePlan>,
    pub conflicts: Vec<SmartConflict>,
    /// Selections whose two sides already match byte-for-byte.
    pub unchanged: usize,
    /// Content digest over the staged tree change (path, head sha256,
    /// staged sha256) in path order. The frame's projection additionally
    /// records the git-blob digest computed at staging time.
    pub patch_sha256: String,
}

impl SmartPlan {
    pub fn applicable(&self) -> bool {
        self.conflicts.is_empty() && !self.files.is_empty()
    }
}

// --------------------------------------------------------------- binding

/// The handle's verified context pair. The clipping rule is the one
/// `SelectionHandle::verified_contexts` already hashes, so an anchored
/// region is exactly the bytes those recorded hashes speak about.
#[derive(Clone)]
struct Contexts<'a> {
    before: &'a str,
    after: &'a str,
}

impl Contexts<'_> {
    /// All context-anchored unit extents in `text`: for every occurrence of
    /// the before-context, the region up to the first following occurrence
    /// of the after-context. Empty before/after mean "file boundary here":
    /// the unit starts at 0 / ends at the text end, exactly as it did in
    /// the handle's source.
    fn anchor_extents(&self, text: &str) -> Vec<ByteRange> {
        let starts: Vec<usize> = if self.before.is_empty() {
            vec![0]
        } else {
            overlapping_match_starts(text, self.before)
                .into_iter()
                .map(|i| i + self.before.len())
                .collect()
        };
        let mut out = Vec::new();
        for start in starts {
            let end = if self.after.is_empty() {
                Some(text.len())
            } else {
                overlapping_match_starts(text, self.after)
                    .into_iter()
                    .find(|&j| j >= start)
            };
            let Some(end) = end else { continue };
            out.push(ByteRange { start, end });
        }
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// A pair of `(start, end)` byte offsets.
type Span = (usize, usize);

enum Anchored {
    One(ByteRange),
    None,
    Many(Vec<ByteRange>),
}

fn anchor_one(contexts: &Contexts, text: &str) -> Anchored {
    match contexts.anchor_extents(text) {
        mut one if one.len() == 1 => Anchored::One(one.remove(0)),
        none if none.is_empty() => Anchored::None,
        many => Anchored::Many(many),
    }
}

// -------------------------------------------------- line alignment (Myers)

/// Edit-script op over lines: `Equal` consumes one line from each side,
/// `Delete` only from the head side, `Insert` only from the worktree side.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LineOp {
    Equal(usize),
    Delete(usize),
    Insert(usize),
}

/// Split text into lines, each carrying its trailing newline (the final
/// line may have none). Byte offsets map through the returned slices.
fn line_slices(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push(&text[start..=i]);
            start = i + 1;
        }
    }
    if start < bytes.len() {
        out.push(&text[start..]);
    }
    out
}

fn line_hash(line: &str) -> u64 {
    // FNV-1a: line identity by hash; wrapping is part of the algorithm.
    line.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01b3)
    })
}

/// Myers line diff, forward path with a trace. Deterministic; the trace
/// budget keeps pathological inputs from allocating without bound.
fn myers_ops(head: &[&str], worktree: &[&str]) -> Option<Vec<LineOp>> {
    if head.is_empty() && worktree.is_empty() {
        return Some(Vec::new());
    }
    let a: Vec<u64> = head.iter().map(|l| line_hash(l)).collect();
    let b: Vec<u64> = worktree.iter().map(|l| line_hash(l)).collect();
    let n = a.len() as i32;
    let m = b.len() as i32;
    let max = (n + m) as usize;
    if max > 400_000 {
        return None;
    }
    let offset = max as i32;
    let mut v = vec![0i32; 2 * max + 1];
    let mut trace: Vec<Vec<i32>> = Vec::new();
    let mut found = false;
    'outer: for d in 0..=(max as i32) {
        trace.push(v.clone());
        let mut k = -d;
        while k <= d {
            let idx = |k: i32| (k + offset) as usize;
            let mut x = if k == -d || (k != d && v[idx(k - 1)] < v[idx(k + 1)]) {
                v[idx(k + 1)]
            } else {
                v[idx(k - 1)] + 1
            };
            let mut y = x - k;
            while x < n && y < m && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            v[idx(k)] = x;
            if x >= n && y >= m {
                found = true;
                break 'outer;
            }
            k += 2;
        }
    }
    if !found {
        return None;
    }
    // Backtrack the trace into an edit script.
    let mut ops: Vec<LineOp> = Vec::new();
    let mut x = n;
    let mut y = m;
    for d in (0..trace.len()).rev() {
        let v = &trace[d];
        let k = x - y;
        let idx = |k: i32| (k + offset) as usize;
        let prev_k = if k == -(d as i32) || (k != d as i32 && v[idx(k - 1)] < v[idx(k + 1)]) {
            k + 1
        } else {
            k - 1
        };
        let prev_x = v[idx(prev_k)];
        let prev_y = prev_x - prev_k;
        while x > prev_x && y > prev_y {
            push_op(&mut ops, LineOp::Equal(1));
            x -= 1;
            y -= 1;
        }
        if d == 0 {
            break;
        }
        if x == prev_x {
            push_op(&mut ops, LineOp::Insert(1));
            y -= 1;
        } else {
            push_op(&mut ops, LineOp::Delete(1));
            x -= 1;
        }
    }
    ops.reverse();
    Some(ops)
}

fn push_op(ops: &mut Vec<LineOp>, op: LineOp) {
    use std::mem::discriminant;
    if let Some(last) = ops.last_mut() {
        if discriminant(last) == discriminant(&op) {
            match (last, op) {
                (LineOp::Equal(c), LineOp::Equal(_)) => *c += 1,
                (LineOp::Delete(c), LineOp::Delete(_)) => *c += 1,
                (LineOp::Insert(c), LineOp::Insert(_)) => *c += 1,
                _ => unreachable!(),
            }
            return;
        }
    }
    ops.push(op);
}

/// Which end of the selection a boundary belongs to. A seam position can
/// legitimately map to several head positions when deletions sit at the
/// seam; the start maps before them and the end after them, so a deleted
/// unit's empty worktree extent grows to cover exactly the deleted head
/// lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    Start,
    End,
}

/// Where a worktree LINE index maps in HEAD under the edit script.
/// `Ok(pos)` is exact: the boundary sat on/inside a matched line, or at a
/// seam. `Err(())` means the boundary fell strictly inside one insertion
/// hunk, where no line-level fact pins the answer.
fn map_boundary(
    ops: &[LineOp],
    boundary: usize,
    which: Boundary,
) -> std::result::Result<usize, ()> {
    let mut head_pos = 0usize; // in LINES
    let mut wt_pos = 0usize; // in LINES
    for op in ops {
        match *op {
            LineOp::Equal(count) => {
                if boundary <= wt_pos {
                    return Ok(head_pos);
                }
                if boundary < wt_pos + count {
                    // Inside a matched run: line-level (and byte-level,
                    // for the line containing the boundary) identity.
                    return Ok(head_pos + (boundary - wt_pos));
                }
                head_pos += count;
                wt_pos += count;
            }
            LineOp::Delete(count) => {
                if boundary <= wt_pos && which == Boundary::Start {
                    // A start seam claims deleted lines only from the
                    // front; an end seam walks past them (below).
                    return Ok(head_pos);
                }
                head_pos += count;
            }
            LineOp::Insert(count) => {
                if boundary <= wt_pos + count {
                    // Inside an insertion the head position is the hunk's
                    // seam no matter which inserted lines sit before the
                    // boundary: insertions consume no head lines, so the
                    // splice is fully determined by the worktree extent.
                    return Ok(head_pos);
                }
                wt_pos += count;
            }
        }
    }
    Ok(head_pos)
}

/// The extents staging uses, both LINE-ALIGNED: the head-side image of
/// the selection and the selection's own line-rounded worktree extent.
/// Line granularity is the staging contract — git patches are line
/// patches, and identical line parts cancel, so rounding never changes the
/// staged effect while keeping the two sides byte-comparable.
fn map_extent(
    head_text: &str,
    worktree_text: &str,
    wt: ByteRange,
) -> std::result::Result<(Span, Span), MapError> {
    let head_lines = line_slices(head_text);
    let wt_lines = line_slices(worktree_text);
    // Byte offsets → line indices.
    let line_of = |lines: &[&str], byte: usize| -> usize {
        let mut acc = 0;
        for (i, line) in lines.iter().enumerate() {
            if byte < acc + line.len() {
                return i;
            }
            acc += line.len();
        }
        lines.len()
    };
    let wt_start_line = line_of(&wt_lines, wt.start);
    let wt_end_line = if wt.end == 0 {
        0
    } else {
        line_of(&wt_lines, wt.end - 1) + 1
    };
    let ops = myers_ops(&head_lines, &wt_lines).ok_or(MapError::Budget)?;
    let start =
        map_boundary(&ops, wt_start_line, Boundary::Start).map_err(|()| MapError::Unpinned)?;
    let end = map_boundary(&ops, wt_end_line, Boundary::End).map_err(|()| MapError::Unpinned)?;
    if start > end {
        return Err(MapError::Unpinned);
    }
    // Line positions → byte offsets.
    let byte_of =
        |lines: &[&str], line: usize| -> usize { lines.iter().take(line).map(|l| l.len()).sum() };
    let head_extent = (byte_of(&head_lines, start), byte_of(&head_lines, end));
    let wt_extent = (
        byte_of(&wt_lines, wt_start_line),
        byte_of(&wt_lines, wt_end_line),
    );
    Ok((head_extent, wt_extent))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MapError {
    /// A boundary fell strictly inside one insertion hunk.
    Unpinned,
    /// The diff exceeded its line budget.
    Budget,
}

// --------------------------------------------------------------- planning

/// One selection's staged outcome before grouping into file plans.
enum Resolved {
    Staged(Box<SmartSelection>),
    Noop,
    Conflict(SmartConflict),
}

fn conflict(
    selection_id: String,
    path: Option<String>,
    side: Option<SmartSide>,
    condition: SmartCondition,
    candidates: Vec<SmartCandidate>,
    detail: String,
) -> Resolved {
    Resolved::Conflict(SmartConflict {
        selection_id,
        path,
        side,
        condition,
        candidates,
        detail,
    })
}

/// Resolve one handle against the worktree and the HEAD-side resolver.
///
/// `head_text` answers "content of this path in git HEAD", `None` when the
/// path does not exist there. Both bindings use the same context-anchor
/// rule, so an edited unit resolves as replace, a new unit as insert, and a
/// removed unit as delete — with no mode flag and no fuzzy fallback.
fn resolve_selection(
    root: &Path,
    view: &mut HistoryView,
    head_frontier: &loro::Frontiers,
    head_text: &mut dyn FnMut(&str) -> Option<String>,
    handle: &SelectionHandle,
) -> Resolved {
    let selection_id = handle.id();

    // No mutating command may rely on the prototype parser, so
    // symbol extents have no mutating path yet; hunks have no surface.
    if !matches!(
        handle.extent,
        SelectionExtent::Match | SelectionExtent::Line
    ) {
        return conflict(
            selection_id,
            None,
            None,
            SmartCondition::UnsupportedExtent,
            Vec::new(),
            format!(
                "{:?} extents need a production adapter before they can authorize commits; \
                 select the unit text with grep `match`/`line` extents",
                handle.extent
            ),
        );
    }

    // The handle must describe its own source.
    let Ok(frontier) = decode_frontier(&handle.source_frontier) else {
        return conflict(
            selection_id,
            None,
            None,
            SmartCondition::InvalidSource,
            Vec::new(),
            "source frontier is malformed".into(),
        );
    };
    let source = match view.path_at(&frontier, &handle.historical_path) {
        Ok(HistoricalPathContent::Text(text)) => text,
        Ok(HistoricalPathContent::Absent) => {
            return conflict(
                selection_id,
                None,
                None,
                SmartCondition::UnsupportedSource,
                Vec::new(),
                format!(
                    "`{}` is absent at the selection's own frontier",
                    handle.historical_path
                ),
            )
        }
        Ok(HistoricalPathContent::Binary { .. }) => {
            return conflict(
                selection_id,
                None,
                None,
                SmartCondition::UnsupportedSource,
                Vec::new(),
                "binary selections are out of scope for smart squash".into(),
            )
        }
        Err(error) => {
            return conflict(
                selection_id,
                None,
                None,
                SmartCondition::UnsupportedSource,
                Vec::new(),
                format!("source read failed: {error}"),
            )
        }
    };
    let contexts = match handle.verified_contexts(&source) {
        Ok((before, after)) => Contexts { before, after },
        Err(error) => {
            return conflict(
                selection_id,
                None,
                None,
                SmartCondition::InvalidSource,
                Vec::new(),
                format!("handle does not describe its source: {error}"),
            )
        }
    };

    // Destination candidates follow recorded renames toward the head.
    let renames = match view.renames_between(&frontier, head_frontier) {
        Ok(renames) => renames,
        Err(error) => {
            return conflict(
                selection_id,
                None,
                None,
                SmartCondition::UnsupportedSource,
                Vec::new(),
                format!("rename history is unreadable: {error}"),
            )
        }
    };
    let names = expand_names(
        &std::collections::BTreeSet::from([handle.historical_path.clone()]),
        &renames,
    );

    // Worktree side: live files only. Whole-file removals are ordinary
    // squash's job.
    let mut worktree: Vec<(String, String)> = Vec::new();
    for name in &names {
        let path = root.join(name);
        if !path.is_file() {
            continue;
        }
        match std::fs::read(&path)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
        {
            Some(text) => worktree.push((name.clone(), text)),
            None => {
                return conflict(
                    selection_id,
                    Some(name.clone()),
                    Some(SmartSide::Worktree),
                    SmartCondition::Unreadable,
                    Vec::new(),
                    format!("destination `{name}` cannot be read as UTF-8 text"),
                )
            }
        }
    }
    if worktree.is_empty() {
        return conflict(
            selection_id,
            Some(handle.historical_path.clone()),
            Some(SmartSide::Worktree),
            SmartCondition::FileDeletedInWorktree,
            Vec::new(),
            "the destination file is gone from the worktree; squash the whole \
             file removal with ordinary `sheaf squash --`"
                .into(),
        );
    }

    // Worktree binding first: the handle identifies the unit in the live
    // tree through its verified contexts.

    // Worktree anchoring. First try the handle's selected bytes verbatim
    // (the common case: the unit is unchanged since the handle was cut, so
    // its exact length and contexts pin it precisely). Only when the unit
    // was edited or removed do we fall back to context anchors, which
    // locate the region between the surroundings.
    let candidates: Vec<SelectionCandidate> = worktree
        .iter()
        .map(|(path, text)| SelectionCandidate {
            path: path.clone(),
            text: text.clone(),
        })
        .collect();
    let exact = rebind_exact(
        handle,
        &source[handle.range.start..handle.range.end],
        &candidates,
    );
    let exact_bound = match exact {
        Ok(RebindOutcome::Bound { binding }) => Some((binding.path, binding.range)),
        Ok(RebindOutcome::Ambiguous { candidates }) => {
            return conflict(
                selection_id,
                candidates.first().map(|b| b.path.clone()),
                Some(SmartSide::Worktree),
                SmartCondition::Ambiguous,
                candidates
                    .into_iter()
                    .map(|b| SmartCandidate {
                        path: b.path,
                        range: b.range,
                    })
                    .collect(),
                "the selected bytes occur at more than one place in the worktree".into(),
            )
        }
        Ok(RebindOutcome::Missing) | Err(_) => None,
    };

    // Context anchoring (fallback): exactly one region across the live
    // candidates, or a typed refusal. Candidates ride along for the
    // preview.
    let bind = |texts: &[(String, String)],
                side: SmartSide|
     -> std::result::Result<Option<(String, ByteRange)>, Resolved> {
        let mut bound: Option<(String, ByteRange)> = None;
        let mut ambiguous: Vec<SmartCandidate> = Vec::new();
        for (path, text) in texts {
            match anchor_one(&contexts, text) {
                Anchored::One(range) => {
                    if bound.is_some() {
                        ambiguous.push(SmartCandidate {
                            path: path.clone(),
                            range,
                        });
                    } else {
                        bound = Some((path.clone(), range));
                    }
                }
                Anchored::Many(ranges) => {
                    ambiguous.extend(ranges.iter().map(|range| SmartCandidate {
                        path: path.clone(),
                        range: *range,
                    }));
                }
                Anchored::None => {}
            }
        }
        if !ambiguous.is_empty() {
            let path = bound
                .as_ref()
                .map(|(p, _)| p.clone())
                .or_else(|| texts.first().map(|(p, _)| p.clone()))
                .unwrap_or_default();
            return Err(conflict(
                selection_id.clone(),
                Some(path),
                Some(side),
                SmartCondition::Ambiguous,
                ambiguous,
                format!(
                    "the selection's surroundings identify more than one region on the {} side",
                    match side {
                        SmartSide::Head => "HEAD",
                        SmartSide::Worktree => "worktree",
                    }
                ),
            ));
        }
        Ok(bound)
    };

    let (wt_path, wt_range) = match exact_bound {
        Some(bound) => bound,
        None => match bind(&worktree, SmartSide::Worktree) {
            Ok(Some(bound)) => bound,
            Ok(None) => {
                return conflict(
                    selection_id,
                    worktree.first().map(|(p, _)| p.clone()),
                    Some(SmartSide::Worktree),
                    SmartCondition::Missing,
                    Vec::new(),
                    "the selection's surroundings no longer identify a worktree region \
                 (edited beyond recognition, or moved with its old site rewritten)"
                        .into(),
                )
            }
            Err(resolved) => return resolved,
        },
    };
    // HEAD side: contexts cannot be trusted here (a neighbor's edit sits
    // inside the handle's 64-byte windows), so the head extent is the
    // image of the worktree extent under a deterministic line diff of HEAD
    // versus the bound worktree text.
    let wt_bound = worktree
        .iter()
        .find(|(p, _)| *p == wt_path)
        .expect("bound candidate");
    let mut head_text_at: Option<(String, String)> = None;
    for name in &names {
        if let Some(text) = head_text(name) {
            head_text_at = Some((name.clone(), text));
            break;
        }
    }
    let Some((head_path, head_content)) = head_text_at else {
        return conflict(
            selection_id,
            Some(wt_path.clone()),
            Some(SmartSide::Head),
            SmartCondition::NewFileSinceHead,
            Vec::new(),
            "the file does not exist in HEAD; whole-file adds are ordinary \
             `sheaf squash --` territory, and a fragment of a new file would \
             strand its imports"
                .into(),
        );
    };
    if head_path != wt_path {
        return conflict(
            selection_id,
            Some(wt_path.clone()),
            None,
            SmartCondition::RenamedSinceHead,
            Vec::new(),
            format!(
                "the unit sits at `{wt_path}` in the worktree but `{head_path}` in \
                 HEAD; stage the rename with ordinary `sheaf squash --`"
            ),
        );
    }
    let ((head_start, head_end), (wt_start, wt_end)) =
        match map_extent(&head_content, &wt_bound.1, wt_range) {
            Ok(extents) => extents,
            Err(MapError::Unpinned) => {
                return conflict(
                    selection_id,
                    Some(wt_path.clone()),
                    Some(SmartSide::Head),
                    SmartCondition::Unaligned,
                    Vec::new(),
                    "the selection's boundaries cannot be expressed in the HEAD↔worktree \
                     line alignment; reselect the unit, or squash the whole file"
                        .into(),
                )
            }
            Err(MapError::Budget) => {
                return conflict(
                    selection_id,
                    Some(wt_path.clone()),
                    Some(SmartSide::Head),
                    SmartCondition::Unaligned,
                    Vec::new(),
                    "the HEAD↔worktree alignment exceeded its line budget; squash the \
                     whole file with ordinary `sheaf squash --`"
                        .into(),
                )
            }
        };
    let head_range = ByteRange {
        start: head_start,
        end: head_end,
    };
    let wt_range = ByteRange {
        start: wt_start,
        end: wt_end,
    };

    let staged_fragment = wt_bound.1[wt_range.start..wt_range.end].to_owned();
    let retired: &str = &head_content[head_range.start..head_range.end];
    if staged_fragment.as_bytes() == retired.as_bytes() {
        return Resolved::Noop;
    }
    let kind = match (retired.is_empty(), staged_fragment.is_empty()) {
        (false, false) => SmartKind::Replace,
        (true, false) => SmartKind::Insert,
        (false, true) => SmartKind::Delete,
        (true, true) => return Resolved::Noop,
    };
    Resolved::Staged(Box::new(SmartSelection {
        selection_id,
        handle: handle.clone(),
        path: wt_path,
        kind,
        head: head_range,
        worktree: wt_range,
        staged_bytes: staged_fragment.len(),
        retired_bytes: retired.len(),
        staged_fragment,
    }))
}

/// Plan a selection-scoped squash commit. Pure computation: reads the live
/// worktree and (through `head_text`) git HEAD content, writes nothing.
pub fn plan_smart(
    root: &Path,
    doc: &LoroDoc,
    base: ResolvedPoint,
    selections: &[SelectionHandle],
    head_text: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<SmartPlan> {
    let head_frontier = decode_frontier(&base.frontier)?;
    let mut view = HistoryView::open(doc)?;
    let mut by_path: BTreeMap<String, Vec<SmartSelection>> = BTreeMap::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0usize;

    for handle in selections {
        match resolve_selection(root, &mut view, &head_frontier, head_text, handle) {
            Resolved::Staged(selection) => by_path
                .entry(selection.path.clone())
                .or_default()
                .push(*selection),
            Resolved::Noop => unchanged += 1,
            Resolved::Conflict(c) => conflicts.push(c),
        }
    }

    // Group into file plans; HEAD-side extents must not overlap (staging
    // coordinates). Splices apply highest extent first so earlier
    // coordinates stay valid. Selections that share one head anchor —
    // equal, empty head extents at the same seam, i.e. a block of new
    // units selected individually — tie-break by descending worktree
    // start: every splice at the same coordinate lands its bytes before
    // the ones already spliced there, so applying last-in-worktree first
    // composes the block back into worktree order. A stable input order
    // (the ascending `grep | jq .hits` pipeline) must never leak into the
    // apply order — ascending application reverses the block.
    let selections = by_path.values().flatten().cloned().collect::<Vec<_>>();
    let mut files = Vec::new();
    'file: for (path, mut staged) in by_path {
        staged.sort_by(|a, b| {
            b.head
                .start
                .cmp(&a.head.start)
                .then(b.head.end.cmp(&a.head.end))
                .then(b.worktree.start.cmp(&a.worktree.start))
                .then(b.worktree.end.cmp(&a.worktree.end))
        });
        for pair in staged.windows(2) {
            let (later, earlier) = (&pair[0], &pair[1]);
            // Same-anchor inserts never trip the head-side check (both
            // extents are empty and equal); their worktree extents must
            // stay disjoint or the patch would splice the same live bytes
            // twice. Within an equal-anchor group the sort above is
            // descending worktree order, so `earlier` is the unit closer
            // to the file head.
            let head_overlap = earlier.head.end > later.head.start;
            let anchor_overlap = later.head.start == earlier.head.start
                && earlier.worktree.end > later.worktree.start;
            if head_overlap || anchor_overlap {
                conflicts.push(SmartConflict {
                    selection_id: earlier.selection_id.clone(),
                    path: Some(path.clone()),
                    side: Some(SmartSide::Head),
                    condition: SmartCondition::Overlap,
                    candidates: Vec::new(),
                    detail: format!(
                        "selections {} and {} anchor overlapping HEAD regions",
                        &later.selection_id[..12.min(later.selection_id.len())],
                        &earlier.selection_id[..12.min(earlier.selection_id.len())],
                    ),
                });
                continue 'file;
            }
        }
        // Splice into HEAD content. The resolver had it; re-ask for exactly
        // this path (idempotent), and let the commit-time staged-tree
        // verification catch a HEAD that moved underneath us.
        let Some(head_content) = head_text(&path) else {
            conflicts.push(SmartConflict {
                selection_id: staged[0].selection_id.clone(),
                path: Some(path),
                side: Some(SmartSide::Head),
                condition: SmartCondition::NewFileSinceHead,
                candidates: Vec::new(),
                detail: "HEAD content vanished between resolution and grouping".into(),
            });
            continue;
        };
        let head_sha256 = sha256(head_content.as_bytes());
        let mut text = head_content.into_bytes();
        for selection in &staged {
            text.splice(
                selection.head.start..selection.head.end,
                selection.staged_fragment.as_bytes().to_vec(),
            );
        }
        let staged_text = String::from_utf8(text).map_err(|_| {
            crate::error::SheafError::StoreCorrupt(format!(
                "staged content for `{path}` is not valid UTF-8"
            ))
        })?;
        files.push(SmartFilePlan {
            head_sha256,
            staged_sha256: sha256(staged_text.as_bytes()),
            added_bytes: staged.iter().map(|s| s.staged_bytes).sum(),
            retired_bytes: staged.iter().map(|s| s.retired_bytes).sum(),
            staged_text,
            path,
        });
    }

    if files.is_empty() && conflicts.is_empty() {
        conflicts.push(SmartConflict {
            selection_id: selections
                .first()
                .map(|s| s.selection_id.clone())
                .unwrap_or_default(),
            path: None,
            side: None,
            condition: SmartCondition::EmptyPatch,
            candidates: Vec::new(),
            detail: "every selection already matches HEAD byte-for-byte; nothing to commit".into(),
        });
    }

    let patch_sha256 = patch_digest(
        &files
            .iter()
            .map(|f| {
                (
                    f.path.clone(),
                    f.head_sha256.clone(),
                    f.staged_sha256.clone(),
                )
            })
            .collect::<Vec<_>>(),
    );
    Ok(SmartPlan {
        selections,
        files,
        conflicts,
        unchanged,
        patch_sha256,
    })
}

/// Digest over the staged tree change in path order. The frame's
/// projection records the git-blob equivalent computed at staging time;
/// this content-level digest is what the store can verify alone.
pub fn patch_digest(entries: &[(String, String, String)]) -> String {
    let mut ordered = entries.to_vec();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let canonical = serde_json::json!({
        "v": 1,
        "kind": "sheaf:smart-patch",
        "entries": ordered.iter().map(|(p, h, s)| serde_json::json!({
            "path": p, "head": h, "staged": s,
        })).collect::<Vec<_>>(),
    });
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

// ----------------------------------------------------- plan (read-only API)

impl ProjectStore {
    /// Plan a smart squash with HEAD-side content supplied up front (the
    /// daemon's two-phase IPC form: the CLI gathers `git show HEAD:<path>`
    /// for the candidate paths first, then calls this).
    pub fn plan_smart_with_heads(
        &self,
        selections: &[SelectionHandle],
        head_texts: &BTreeMap<String, String>,
    ) -> Result<SmartPlan> {
        let base = self.resolve("@")?;
        plan_smart(&self.root, &self.doc, base, selections, &mut |path| {
            head_texts.get(path).cloned()
        })
    }

    /// Union of every candidate destination path for the given selections
    /// (rename-followed toward the head; a handle whose frontier cannot be
    /// read degrades to its own historical path). Phase one of the
    /// two-phase IPC: these are the paths whose HEAD content the caller
    /// must fetch before the plan call.
    pub fn smart_destination_paths(&self, selections: &[SelectionHandle]) -> Vec<String> {
        let head = self.resolve("@").ok();
        let mut union = std::collections::BTreeSet::new();
        let mut view = HistoryView::open(&self.doc).ok();
        for handle in selections {
            let mut names = std::collections::BTreeSet::from([handle.historical_path.clone()]);
            if let (Some(view), Some(base), Ok(frontier)) = (
                view.as_mut(),
                head.as_ref(),
                decode_frontier(&handle.source_frontier),
            ) {
                if let Ok(base_frontier) = decode_frontier(&base.frontier) {
                    if let Ok(renames) = view.renames_between(&frontier, &base_frontier) {
                        names = expand_names(&names, &renames);
                    }
                }
            }
            union.extend(names);
        }
        union.into_iter().collect()
    }
}

impl TimelineReader {
    /// Degraded-mode counterpart: same plan from a read-only store view,
    /// with a lazy HEAD-text resolver (the local CLI can read git here).
    pub fn plan_smart_degraded(
        &self,
        selections: &[SelectionHandle],
        head_text: &mut dyn FnMut(&str) -> Option<String>,
    ) -> Result<SmartPlan> {
        let base = self.resolve("@")?;
        plan_smart(self.root(), self.doc(), base, selections, head_text)
    }
}

// ------------------------------------------------------------- attribution

/// Timeline-side context for the drafted message: what happened between
/// the squash anchor and the tip, restricted to the selection paths.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmartAttribution {
    pub captures: usize,
    pub restores: usize,
    pub checkpoints: Vec<String>,
}

pub fn smart_attribution(
    newest_first: &[super::Capture],
    paths: &std::collections::BTreeSet<String>,
) -> SmartAttribution {
    let mut attribution = SmartAttribution::default();
    for capture in newest_first {
        if !capture.paths.iter().any(|p| paths.contains(p)) {
            continue;
        }
        attribution.captures += 1;
        attribution.restores += capture.origin.as_ref().is_some_and(|o| {
            matches!(
                o.kind,
                OriginKind::Restore | OriginKind::PreRestore | OriginKind::FragmentRestore
            )
        }) as usize;
        attribution
            .checkpoints
            .extend(capture.checkpoints.iter().cloned());
    }
    attribution
}

// --------------------------------------------------------------- drafting

/// Draft the smart-squash commit message: staged patch first (what git
/// will actually commit), timeline attribution second.
pub fn draft_smart_message(plan: &SmartPlan, attribution: &SmartAttribution) -> String {
    let mut body = String::new();
    for file in &plan.files {
        body.push_str(&format!(
            "{} (+{}/-{} bytes)\n",
            file.path, file.added_bytes, file.retired_bytes
        ));
    }
    for selection in &plan.selections {
        body.push_str(&format!(
            "  {:?} {} [{}]\n",
            selection.kind,
            &selection.selection_id[..12.min(selection.selection_id.len())],
            selection.path
        ));
    }
    let mut out = format!(
        "{}\n\n{}\nSelected via {} sheaf selection handle(s){}.\n",
        draft_smart_subject(plan),
        body,
        plan.selections.len() + plan.unchanged,
        if plan.unchanged > 0 {
            format!(" ({} already current)", plan.unchanged)
        } else {
            String::new()
        },
    );
    if attribution.captures > 0 {
        out.push_str(&format!(
            "Timeline attribution: {} capture(s) touched the selection path(s) in the span.\n",
            attribution.captures
        ));
        if !attribution.checkpoints.is_empty() {
            out.push_str(&format!(
                "Checkpoints crossed: {}.\n",
                attribution.checkpoints.join(", ")
            ));
        }
    } else {
        out.push_str(
            "Timeline attribution: no captures touched the selection path(s) in the span.\n",
        );
    }
    out
}

pub fn draft_smart_subject(plan: &SmartPlan) -> String {
    match plan.files.as_slice() {
        [] => "smart squash: empty patch".to_owned(),
        [one] => format!(
            "{}: {} selected change(s) (+{}/-{} bytes)",
            one.path,
            plan.selections.len(),
            one.added_bytes,
            one.retired_bytes
        ),
        many => format!(
            "{} files: {} selected change(s)",
            many.len(),
            plan.selections.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_anchors_find_edited_and_vanished_units() {
        let source = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
        let contexts = Contexts {
            before: &source[..source.find("fn a()").unwrap()],
            after: &source[source.find("fn b()").unwrap()..],
        };
        // Unedited: the extent is exactly the unit ("fn a() {\n    1\n}\n"
        // is 17 bytes; `fn b()` starts at 17).
        assert_eq!(
            contexts.anchor_extents(source),
            vec![ByteRange { start: 0, end: 17 }]
        );
        // Edited unit: same anchors, different middle.
        let edited = source.replace("    1", "    99");
        assert_eq!(
            contexts.anchor_extents(&edited),
            vec![ByteRange { start: 0, end: 18 }]
        );
        // Deleted unit: anchors adjacent (the scar).
        let deleted = &source[17..];
        assert_eq!(
            contexts.anchor_extents(deleted),
            vec![ByteRange { start: 0, end: 0 }]
        );
    }

    #[test]
    fn boundary_contexts_anchor_at_boundaries_only() {
        // Unit at the very start of the file: empty before-context.
        let source = "alpha\nbeta\n";
        let contexts = Contexts {
            before: "",
            after: "beta",
        };
        assert_eq!(
            contexts.anchor_extents(source),
            vec![ByteRange { start: 0, end: 6 }]
        );
        // Unit right after `alpha` to end of file: empty after-context.
        let contexts = Contexts {
            before: "alpha",
            after: "",
        };
        assert_eq!(
            contexts.anchor_extents(source),
            vec![ByteRange { start: 5, end: 11 }]
        );
    }

    #[test]
    fn repeated_contexts_yield_multiple_candidates() {
        // before = `fn a() {` header, after = the newline plus the next
        // identical header: each before-occurrence pairs with the first
        // after-occurrence that follows it.
        let contexts = Contexts {
            before: "fn a() {\n",
            after: "\nfn a() {\n",
        };
        let text = "fn a() {\n    1\n}\nfn a() {\n    2\n}\n";
        // start 9 pairs with the second header (j=16); start 26 has no
        // following after-occurrence, so exactly one extent survives.
        let extents = contexts.anchor_extents(text);
        assert_eq!(extents, vec![ByteRange { start: 9, end: 16 }]);

        // A second after-occurrence after the second before-match turns
        // this into two candidates — ambiguity the planner must refuse.
        let repeat = "fn a() {\n    1\n}\nfn a() {\n    2\n}\nfn a() {\n    3\n}\n";
        assert_eq!(contexts.anchor_extents(repeat).len(), 2);
    }

    #[test]
    fn myers_maps_edited_unit_with_edited_neighbor() {
        // The acceptance geometry: A edited, B edited, one file. A's head
        // extent must be exactly A's old lines — never B's.
        let head = "fn a() {\n    1\n}\n\nfn b() {\n    2\n}\n";
        let worktree = "fn a() {\n    99\n}\n\nfn b() {\n    42\n}\n";
        let a_wt = ByteRange { start: 0, end: 17 };
        let (extent, _) = map_extent(head, worktree, a_wt).unwrap();
        let (hs, he) = extent;
        assert_eq!(&head[hs..he], "fn a() {\n    1\n}\n");
        let b_at = worktree.find("fn b()").unwrap();
        let b_wt = ByteRange {
            start: b_at,
            end: worktree.len(),
        };
        let (extent, _) = map_extent(head, worktree, b_wt).unwrap();
        let (hs, he) = extent;
        assert_eq!(&head[hs..he], "fn b() {\n    2\n}\n");
    }

    #[test]
    fn myers_maps_insertions_and_deletions() {
        let head = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
        // Inserted unit between the two.
        let with_new = "fn a() {\n    1\n}\nfn c() {\n    3\n}\nfn b() {\n    2\n}\n";
        let c_at = with_new.find("fn c()").unwrap();
        let c_end = with_new.find("fn b()").unwrap();
        let (extent, _) = map_extent(
            head,
            with_new,
            ByteRange {
                start: c_at,
                end: c_end,
            },
        )
        .unwrap();
        let (hs, he) = extent;
        assert_eq!(hs, he, "insertion maps to an empty head extent");
        // Deleted unit.
        let without_b = "fn a() {\n    1\n}\n";
        let (extent, _) = map_extent(head, without_b, ByteRange { start: 17, end: 17 }).unwrap();
        let (hs, he) = extent;
        assert_eq!(&head[hs..he], "fn b() {\n    2\n}\n");
    }

    #[test]
    fn boundary_inside_one_insertion_hunk_is_exact() {
        let head = "a\nb\n";
        let worktree = "a\nX\nY\nb\n";
        // Selecting only X (bytes 2..4): the boundary is inside the
        // two-line insertion, but the head seam is unique either way, so
        // the splice is fully determined.
        let (extent, _) = map_extent(head, worktree, ByteRange { start: 2, end: 4 }).unwrap();
        let (hs, he) = extent;
        assert_eq!((hs, he), (2, 2));
        // Selecting X and Y together: same seam.
        let (extent, _) = map_extent(head, worktree, ByteRange { start: 2, end: 6 }).unwrap();
        let (hs, he) = extent;
        assert_eq!((hs, he), (2, 2));
    }

    #[test]
    fn adversarial_alignments_stay_deterministic_and_ordered() {
        // Total reversal has many equally-short scripts; the exact image
        // of one line is a tie-break, not a fact. The contract is only
        // determinism plus ordered, in-bounds extents.
        let head = "one\ntwo\nthree\nfour\nfive\n";
        let worktree = "five\nfour\nthree\ntwo\none\n";
        for (start, end) in [(0, 5), (5, 10), (0, 10)] {
            let a = map_extent(head, worktree, ByteRange { start, end });
            let b = map_extent(head, worktree, ByteRange { start, end });
            assert_eq!(a, b, "mapping must be deterministic");
            match a {
                Ok((head_extent, wt_extent)) => assert!(
                    head_extent.0 <= head_extent.1
                        && head_extent.1 <= head.len()
                        && wt_extent.0 <= wt_extent.1
                        && wt_extent.1 <= worktree.len(),
                    "extent out of bounds: {head_extent:?} / {wt_extent:?}"
                ),
                Err(MapError::Unpinned) => {}
                Err(other) => panic!("unexpected map error: {other:?}"),
            }
        }
    }

    #[test]
    fn patch_digest_is_order_independent() {
        let a = patch_digest(&[
            ("b.rs".into(), "h1".into(), "s1".into()),
            ("a.rs".into(), "h0".into(), "s0".into()),
        ]);
        let b = patch_digest(&[
            ("a.rs".into(), "h0".into(), "s0".into()),
            ("b.rs".into(), "h1".into(), "s1".into()),
        ]);
        assert_eq!(a, b);
        let c = patch_digest(&[("a.rs".into(), "h0".into(), "sX".into())]);
        assert_ne!(a, c);
    }

    // ---- fixtures (mirror crates/sheaf-core/tests/smart.rs) ----

    use std::path::PathBuf;

    use crate::config;
    use crate::events::{Batch, EventKind, FsEvent};
    use crate::store::{Capture, CaptureOrigin, StoreLimits};

    const GOOD: &str = "fn alpha() {\n    1\n}\n\nfn beta() {\n    2\n}\n";

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sheaf-smart-unit-{tag}-{}-{}",
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

    fn captured(root: &Path, content: &str) -> ProjectStore {
        skeleton(root);
        let mut store = open(root);
        write(root, "src/lib.rs", content.as_bytes());
        flush(&mut store, root, vec![added(root, "src/lib.rs")]);
        store
    }

    fn handle_at(
        store: &ProjectStore,
        reference: &str,
        path: &str,
        needle: &str,
    ) -> SelectionHandle {
        let point = store.resolve(reference).unwrap();
        let text = match store.historical_path_content(reference, path).unwrap() {
            HistoricalPathContent::Text(text) => text,
            other => panic!("expected text at {reference}:{path}, got {other:?}"),
        };
        let start = text
            .find(needle)
            .unwrap_or_else(|| panic!("`{needle}` not found in {path} at {reference}"));
        SelectionHandle::from_source(
            point.frontier,
            point.capture_id,
            path,
            SelectionExtent::Match,
            ByteRange {
                start,
                end: start + needle.len(),
            },
            &text,
            format!("literal:{needle}"),
            None,
        )
        .unwrap()
    }

    fn heads(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(p, t)| (p.to_string(), t.to_string()))
            .collect()
    }

    fn single_condition(plan: &SmartPlan) -> SmartCondition {
        assert_eq!(plan.conflicts.len(), 1, "{plan:#?}");
        plan.conflicts[0].condition.clone()
    }

    #[test]
    fn anchor_one_classifies_none_one_and_many() {
        let contexts = Contexts {
            before: "fn a(",
            after: "\n",
        };
        // Zero anchored regions: nothing to bind.
        assert!(matches!(
            anchor_one(&contexts, "no anchor here\n"),
            Anchored::None
        ));
        // One region.
        assert!(matches!(
            anchor_one(&contexts, "fn a() {\n}\n"),
            Anchored::One(_)
        ));
        // Two regions: ambiguity the planner must refuse.
        let twice = "fn a() {\n}\nfn a() {\n}\n";
        assert!(matches!(
            anchor_one(&contexts, twice),
            Anchored::Many(ref ranges) if ranges.len() == 2
        ));
    }

    #[test]
    fn line_slices_keep_trailing_newlines_and_a_final_bare_line() {
        assert_eq!(line_slices("a\nb\n"), vec!["a\n", "b\n"]);
        // No trailing newline: the last line still counts.
        assert_eq!(line_slices("a\nb"), vec!["a\n", "b"]);
        assert!(line_slices("").is_empty());
        assert_eq!(line_slices("\n"), vec!["\n"]);
    }

    #[test]
    fn map_boundary_seams_around_deletes_and_inserts() {
        // Deleted lines: a start seam claims them from the front, an end
        // seam walks past them — a deleted unit's empty worktree extent
        // grows to cover exactly the deleted head lines.
        let ops = vec![LineOp::Equal(2), LineOp::Delete(3), LineOp::Equal(2)];
        assert_eq!(map_boundary(&ops, 2, Boundary::Start).unwrap(), 2);
        assert_eq!(map_boundary(&ops, 2, Boundary::End).unwrap(), 5);
        // A boundary past the deletion maps through it: head 2 + 3 deleted.
        assert_eq!(map_boundary(&ops, 4, Boundary::Start).unwrap(), 7);
        // Insertions consume no head lines: every boundary inside the hunk
        // maps to the same seam.
        let ins = vec![LineOp::Equal(1), LineOp::Insert(2), LineOp::Equal(1)];
        assert_eq!(map_boundary(&ins, 1, Boundary::End).unwrap(), 1);
        assert_eq!(map_boundary(&ins, 3, Boundary::End).unwrap(), 1);
        // Past every op: the run-out head position.
        assert_eq!(map_boundary(&ops, 99, Boundary::End).unwrap(), 7);
    }

    #[test]
    fn empty_extent_at_start_of_file_maps_to_line_zero() {
        // A deletion scar at the very start: byte 0 with end 0 must round
        // to line 0, not fall off the end of the line table.
        let text = "a\nb\n";
        let (head_extent, wt_extent) =
            map_extent(text, text, ByteRange { start: 0, end: 0 }).unwrap();
        assert_eq!(head_extent, (0, 0));
        assert_eq!(wt_extent, (0, 0));
    }

    #[test]
    fn oversized_diffs_refuse_at_the_line_budget() {
        let root = tmp("budget");
        // The trace ceiling counts lines (n + m > 400_000), not bytes, so
        // the fixture stays under TEXT_MAX_BYTES with short lines: it must
        // remain CRDT text, never fall to the blob path.
        let worktree: String = "a\n".repeat(200_100);
        let store = captured(&root, &worktree);
        let handle = handle_at(&store, "@", "src/lib.rs", "a\n");
        // Same line count, different content: n + m crosses the budget, so
        // the alignment refuses instead of allocating without bound.
        let mut head = worktree.clone();
        head.replace_range(10..11, "b");
        let plan = store
            .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", &head)]))
            .unwrap();
        assert_eq!(single_condition(&plan), SmartCondition::Unaligned);
        assert!(plan.conflicts[0].detail.contains("line budget"));
        assert!(plan.files.is_empty());
    }

    #[test]
    fn resolve_selection_fails_closed_by_condition() {
        let root = tmp("resolve");
        let store = captured(&root, GOOD);

        // A malformed source frontier cannot locate the handle's snapshot.
        let mut bogus = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        bogus.source_frontier = "zzz".into();
        let plan = store
            .plan_smart_with_heads(&[bogus], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        assert_eq!(single_condition(&plan), SmartCondition::InvalidSource);
        assert!(plan.conflicts[0].detail.contains("malformed"));

        // A handle naming a path absent at its own frontier.
        let mut ghost = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        ghost.historical_path = "src/ghost.rs".into();
        let plan = store
            .plan_smart_with_heads(&[ghost], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        assert_eq!(single_condition(&plan), SmartCondition::UnsupportedSource);
        assert!(plan.conflicts[0].detail.contains("ghost.rs"));

        // A handle over a binary-tracked path (fresh root: its own store).
        let root = tmp("resolve-bin");
        let mut bin_store = captured(&root, GOOD);
        write(&root, "blob.bin", b"\x00\x01\xff\xfe");
        flush(&mut bin_store, &root, vec![added(&root, "blob.bin")]);
        let mut binary = handle_at(&bin_store, "@", "src/lib.rs", "fn alpha");
        binary.historical_path = "blob.bin".into();
        let plan = bin_store
            .plan_smart_with_heads(&[binary], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        assert_eq!(single_condition(&plan), SmartCondition::UnsupportedSource);
        assert!(plan.conflicts[0].detail.contains("binary"));

        // A handle whose recorded contexts describe different bytes.
        let mut lying = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        lying.before_context_sha256 = sha256(b"elsewhere");
        let plan = store
            .plan_smart_with_heads(&[lying], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        assert_eq!(single_condition(&plan), SmartCondition::InvalidSource);
        assert!(plan.conflicts[0]
            .detail
            .contains("does not describe its source"));

        // A worktree destination that is no longer UTF-8 text (fresh root).
        let root = tmp("resolve-unreadable");
        let unreadable_store = captured(&root, GOOD);
        let handle = handle_at(&unreadable_store, "@", "src/lib.rs", "fn alpha");
        write(&root, "src/lib.rs", b"\xff\xfe\xfd");
        let plan = unreadable_store
            .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        let conflict = &plan.conflicts[0];
        assert_eq!(conflict.condition, SmartCondition::Unreadable);
        assert_eq!(conflict.side, Some(SmartSide::Worktree));

        // Worktree anchors that no longer identify any region: the unit was
        // edited beyond recognition AND its after-anchor rewritten away.
        let root = tmp("missing");
        let store = captured(&root, GOOD);
        let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        write(
            &root,
            "src/lib.rs",
            "fn alpha() {\n    gone\n}\n\nnothing familiar\n".as_bytes(),
        );
        let plan = store
            .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        let conflict = &plan.conflicts[0];
        assert_eq!(conflict.condition, SmartCondition::Missing);
        assert_eq!(conflict.side, Some(SmartSide::Worktree));
    }

    #[test]
    fn head_vanishing_between_resolution_and_grouping_is_a_conflict() {
        let root = tmp("vanish");
        let store = captured(&root, GOOD);
        let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        // The worktree holds an uncaptured edit, so resolution produces a
        // real staged action (context-anchored) and grouping must re-ask for
        // HEAD content. A stateful resolver answers the first call and
        // reports the file gone on the second: a typed refusal, never a
        // half-staged plan.
        write(
            &root,
            "src/lib.rs",
            GOOD.replace("    1", "    111").as_bytes(),
        );
        let mut calls = 0;
        let plan = plan_smart(
            &root,
            &store.doc,
            store.resolve("@").unwrap(),
            &[handle],
            &mut |_path| {
                calls += 1;
                if calls == 1 {
                    Some(GOOD.to_string())
                } else {
                    None
                }
            },
        )
        .unwrap();
        assert!(!plan.applicable());
        assert!(plan.files.is_empty());
        let conflict = &plan.conflicts[0];
        assert_eq!(conflict.condition, SmartCondition::NewFileSinceHead);
        assert!(conflict.detail.contains("vanished between resolution"));
        assert_eq!(calls, 2);
    }

    fn sample_plan() -> SmartPlan {
        let source = "fn a() {\n    1\n}\n";
        let handle = SelectionHandle::from_source(
            "ff",
            Some("cap".into()),
            "a.rs",
            SelectionExtent::Match,
            ByteRange { start: 0, end: 9 },
            source,
            "literal:test",
            None,
        )
        .unwrap();
        let selection = SmartSelection {
            selection_id: handle.id(),
            handle: handle.clone(),
            path: "a.rs".into(),
            kind: SmartKind::Replace,
            head: ByteRange { start: 0, end: 9 },
            worktree: ByteRange { start: 0, end: 9 },
            staged_fragment: "fn a() {\n    2\n}".into(),
            staged_bytes: 16,
            retired_bytes: 9,
        };
        let file = SmartFilePlan {
            path: "a.rs".into(),
            head_sha256: sha256(source.as_bytes()),
            staged_sha256: sha256(b"fn a() {\n    2\n}"),
            staged_text: "fn a() {\n    2\n}".into(),
            added_bytes: 16,
            retired_bytes: 9,
        };
        SmartPlan {
            selections: vec![selection],
            files: vec![file],
            conflicts: Vec::new(),
            unchanged: 2,
            patch_sha256: patch_digest(&[(
                "a.rs".into(),
                sha256(source.as_bytes()),
                sha256(b"fn a() {\n    2\n}"),
            )]),
        }
    }

    #[test]
    fn draft_subject_covers_empty_single_and_multi_file_plans() {
        assert_eq!(
            draft_smart_subject(&sample_plan()),
            "a.rs: 1 selected change(s) (+16/-9 bytes)"
        );
        let mut many = sample_plan();
        let mut second = many.files[0].clone();
        second.path = "b.rs".into();
        many.files.push(second);
        assert_eq!(draft_smart_subject(&many), "2 files: 1 selected change(s)");
    }

    #[test]
    fn draft_message_reports_patch_then_attribution() {
        let plan = sample_plan();
        let attribution = SmartAttribution {
            captures: 2,
            restores: 1,
            checkpoints: vec!["cp-a".into(), "cp-b".into()],
        };
        let message = draft_smart_message(&plan, &attribution);
        assert!(message.starts_with("a.rs: 1 selected change(s) (+16/-9 bytes)\n"));
        assert!(message.contains("a.rs (+16/-9 bytes)"));
        assert!(message.contains("(2 already current)"));
        assert!(
            message.contains("Timeline attribution: 2 capture(s) touched the selection path(s)")
        );
        assert!(message.contains("Checkpoints crossed: cp-a, cp-b."));

        let quiet = SmartAttribution::default();
        let message = draft_smart_message(&plan, &quiet);
        assert!(message.contains("Timeline attribution: no captures touched the selection path(s)"));
    }

    #[test]
    fn attribution_counts_only_selection_paths_and_restore_origins() {
        let cap = |paths: &[&str], kind: Option<OriginKind>, checkpoints: &[&str]| Capture {
            id: "capture-id".into(),
            frontier: "ff".into(),
            parent_frontier: "ee".into(),
            timestamp_ms: 0,
            paths: paths.iter().map(|p| p.to_string()).collect(),
            events: 1,
            checkpoints: checkpoints.iter().map(|c| c.to_string()).collect(),
            origin: kind.map(|kind| CaptureOrigin {
                kind,
                target: None,
                scope: vec![],
                selections: vec![],
            }),
            on_current: true,
        };
        let newest_first = vec![
            cap(&["src/lib.rs"], Some(OriginKind::Restore), &["cp-a"]),
            cap(&["src/other.rs"], Some(OriginKind::Restore), &[]),
            cap(&["src/lib.rs"], Some(OriginKind::FragmentRestore), &[]),
            cap(&["src/lib.rs"], None, &[]),
        ];
        let attribution = smart_attribution(
            &newest_first,
            &std::collections::BTreeSet::from(["src/lib.rs".to_string()]),
        );
        assert_eq!(
            attribution.captures, 3,
            "only selection-path captures count"
        );
        assert_eq!(attribution.restores, 2, "restore-family origins count");
        assert_eq!(attribution.checkpoints, vec!["cp-a".to_string()]);
    }
    #[test]
    fn myers_diff_handles_empty_equal_and_budget_limit() {
        assert_eq!(myers_ops(&[], &[]), Some(Vec::new()));
        assert_eq!(myers_ops(&["a\n"], &["a\n"]), Some(vec![LineOp::Equal(1)]));
        assert_eq!(
            myers_ops(&["a\n"], &["b\n"]),
            Some(vec![LineOp::Delete(1), LineOp::Insert(1)])
        );
        let huge = vec!["x\n"; 200_001];
        assert!(
            myers_ops(&huge, &huge).is_none(),
            "trace budget must reject pathological input"
        );
    }

    #[test]
    fn line_slices_and_boundary_mapping_cover_seams() {
        assert_eq!(line_slices(""), Vec::<&str>::new());
        assert_eq!(line_slices("a\nb"), vec!["a\n", "b"]);
        let ops = vec![
            LineOp::Equal(1),
            LineOp::Insert(2),
            LineOp::Delete(1),
            LineOp::Equal(1),
        ];
        assert_eq!(map_boundary(&ops, 0, Boundary::Start), Ok(0));
        assert_eq!(map_boundary(&ops, 1, Boundary::Start), Ok(1));
        assert_eq!(map_boundary(&ops, 2, Boundary::Start), Ok(1));
        assert_eq!(map_boundary(&ops, 3, Boundary::End), Ok(1));
        assert_eq!(map_boundary(&ops, 4, Boundary::End), Ok(3));
    }

    #[test]
    fn line_hash_is_sensitive_to_newlines() {
        assert_ne!(line_hash("line"), line_hash("line\n"));
        assert_eq!(line_hash("same"), line_hash("same"));
    }
    #[test]
    fn attribution_filters_paths_and_counts_restore_kinds() {
        let make = |id: &str, path: &str, kind: Option<OriginKind>, cps: &[&str]| Capture {
            id: id.into(),
            frontier: id.into(),
            parent_frontier: String::new(),
            timestamp_ms: 0,
            paths: vec![path.into()],
            events: 1,
            checkpoints: cps.iter().map(|s| (*s).into()).collect(),
            origin: kind.map(|k| CaptureOrigin {
                kind: k,
                target: None,
                scope: vec![],
                selections: vec![],
            }),
            on_current: true,
        };
        let result = smart_attribution(
            &[
                make("a", "src/a.rs", Some(OriginKind::Restore), &["cp"]),
                make("b", "src/a.rs", Some(OriginKind::FragmentRestore), &[]),
                make("c", "docs/readme", Some(OriginKind::PreRestore), &[]),
            ],
            &std::collections::BTreeSet::from(["src/a.rs".into()]),
        );
        assert_eq!(result.captures, 2);
        assert_eq!(result.restores, 2);
        assert_eq!(result.checkpoints, vec!["cp"]);
    }
    #[test]
    fn smart_subject_handles_empty_and_attribution_handles_pre_restore() {
        let mut empty = sample_plan();
        empty.files.clear();
        empty.selections.clear();
        empty.unchanged = 0;
        assert_eq!(draft_smart_subject(&empty), "smart squash: empty patch");

        let capture = Capture {
            id: "pre".into(),
            frontier: "f".into(),
            parent_frontier: "p".into(),
            timestamp_ms: 0,
            paths: vec!["a.rs".into()],
            events: 1,
            checkpoints: vec![],
            origin: Some(CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: None,
                scope: vec![],
                selections: vec![],
            }),
            on_current: true,
        };
        let result = smart_attribution(
            &[capture],
            &std::collections::BTreeSet::from(["a.rs".to_string()]),
        );
        assert_eq!(result.captures, 1);
        assert_eq!(result.restores, 1);
    }

    #[test]
    fn map_boundary_returns_end_position_after_all_operations() {
        let ops = vec![LineOp::Insert(2), LineOp::Delete(3)];
        assert_eq!(map_boundary(&ops, 99, Boundary::End), Ok(3));
        assert_eq!(map_boundary(&[], 0, Boundary::Start), Ok(0));
    }
    #[test]
    fn resolve_selection_exercises_exact_fallback_and_head_ordering() {
        let root = tmp("strategies");
        let store = captured(&root, GOOD);
        let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");

        let noop = store
            .plan_smart_with_heads(
                std::slice::from_ref(&handle),
                &heads(&[("src/lib.rs", GOOD)]),
            )
            .unwrap();
        assert_eq!(noop.unchanged, 1);

        write(
            &root,
            "src/lib.rs",
            GOOD.replace("    1", "    9").as_bytes(),
        );
        let edited = store
            .plan_smart_with_heads(
                std::slice::from_ref(&handle),
                &heads(&[("src/lib.rs", GOOD)]),
            )
            .unwrap();
        assert!(edited.applicable());
        assert_eq!(edited.selections[0].kind, SmartKind::Replace);

        std::fs::remove_file(root.join("src/lib.rs")).unwrap();
        let gone = store
            .plan_smart_with_heads(
                std::slice::from_ref(&handle),
                &heads(&[("src/lib.rs", GOOD)]),
            )
            .unwrap();
        assert_eq!(
            single_condition(&gone),
            SmartCondition::FileDeletedInWorktree
        );

        write(&root, "src/lib.rs", GOOD.as_bytes());
        let new_head = store
            .plan_smart_with_heads(std::slice::from_ref(&handle), &heads(&[]))
            .unwrap();
        assert_eq!(
            single_condition(&new_head),
            SmartCondition::NewFileSinceHead
        );
    }
}

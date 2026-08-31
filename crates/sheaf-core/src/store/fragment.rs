//! Snapshot-bound fragment restore.
//!
//! Where the whole-tree restore engine moves whole files or the whole tree,
//! a fragment restore moves exactly one historical extent — typically a
//! function found by `sheaf grep` — into the live worktree. The unit of
//! identity is the [`SelectionHandle`]: immutable source frontier plus
//! content-addressed extent. Planning rebinds that handle into
//! the destination through the shared selector engine and fails closed on
//! any ambiguity; a wrong-looking unique fragment is worse than an explicit
//! conflict, so similarity never authorizes a mutation.
//!
//! Apply inherits the restore engine's ordering contract verbatim: reconcile
//! the live worktree into history first (the pre-restore capture is the undo
//! reference), revalidate the token, fsync a restartable intent, install
//! whole files atomically, then append one forward capture whose origin
//! names the selection IDs that produced it.

use std::collections::BTreeMap;
use std::path::Path;

use loro::LoroDoc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::restore::{
    expand_names, Entry, HistoryView, RestoreIntent, RestoreMode, RestoreOutcome,
};
use super::selection::{
    overlapping_match_starts, rebind_exact, rebind_symbol, ByteRange, HistoricalPathContent,
    RebindOutcome, RustPrototypeParser, SelectionCandidate, SelectionError, SelectionExtent,
    SelectionHandle, SymbolParseError,
};
use super::timeline::{decode_frontier, CaptureOrigin, OriginKind, ResolvedPoint};
use super::{blobs, state_dir, Capture, ProjectStore, TimelineReader};
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind, FsEvent};
use crate::ignore::IgnoreSet;

/// Intent file is shared with the whole-tree engine; only its payload differs.
const INTENT_FILE: &str = "restore.intent";

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

// ------------------------------------------------------------------ types

/// What the user asked the fragment operation to do with the selection.
///
/// `Replace` is the default and the only mode that may be chosen implicitly.
/// Reinserting deleted content and deleting present content both rewrite
/// code the destination currently does not have (or does), so each requires
/// the operator to name the intent explicitly — it is never chosen as a
/// fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentMode {
    /// Splice the historical bytes over their uniquely rebound destination
    /// range. Requires the unit to be present and unambiguous.
    Replace,
    /// Reinsert a unit the destination no longer holds, anchored on the
    /// unique deletion scar its surroundings form. Requires the unit to be
    /// absent.
    Insert,
    /// Remove the unit the handle selects, from its uniquely rebound
    /// destination range. Requires the unit to be present.
    Delete,
}

impl FragmentMode {
    /// Parse the wire form (`replace`/`insert`/`delete`); errors on anything else.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "replace" => Ok(FragmentMode::Replace),
            "insert" => Ok(FragmentMode::Insert),
            "delete" => Ok(FragmentMode::Delete),
            other => Err(SheafError::Config(format!(
                "unknown fragment mode `{other}` (expected replace, insert, or delete)"
            ))),
        }
    }
}

/// The action a planned fragment restore resolved to, as reported to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentActionKind {
    Replace,
    Insert,
    Delete,
}

/// Why a selection cannot become an action. The snake_case serde forms are
/// the stable machine conditions: `selection.*` come from the selection
/// contract and `fragment.*` are this engine's additions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FragmentCondition {
    /// No destination candidate at all (`selection.missing`).
    Missing,
    /// Zero or multiple contextual candidates (`selection.ambiguous`); the
    /// conflicting bindings ride along as diagnostics.
    Ambiguous,
    /// The handle does not describe its own source bytes
    /// (`selection.invalid_source`).
    InvalidSource,
    /// The source extent is absent, binary, or pruned at its frontier
    /// (`selection.unsupported_source`).
    UnsupportedSource,
    /// No parser adapter for the handle's language
    /// (`selection.unsupported_language`).
    UnsupportedLanguage,
    /// The requested mode contradicts the destination state (insert into a
    /// present unit, delete an absent one).
    UnexpectedState,
    /// Two planned actions overlap in one file (`fragment.overlap`).
    Overlap,
    /// The destination file exists but cannot be read.
    Unreadable,
}

/// One selection that could not become an action, with the condition and any
/// candidate ranges that explain why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentConflict {
    pub selection_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub condition: FragmentCondition,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<FragmentRange>,
    pub detail: String,
}

/// A diagnostic position: path plus a byte range that is empty for pure
/// insertion points.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentRange {
    pub path: String,
    pub range: ByteRange,
}

/// One splice a plan will perform: the source handle, its resolved kind, the
/// destination range, and the before/after hashes that gate apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentAction {
    pub selection_id: String,
    /// The full source handle; apply re-derives the new bytes from immutable
    /// history so the intent never needs to carry payloads.
    pub handle: SelectionHandle,
    pub kind: FragmentActionKind,
    /// Destination range in the file's pre-action bytes. Empty
    /// (`start == end`) for `Insert`.
    pub range: ByteRange,
    /// SHA-256 of the destination bytes currently in `range`; empty-input
    /// hash for `Insert`.
    pub old_fragment_sha256: String,
    /// SHA-256 of the bytes that will occupy `range`; empty-input hash for
    /// `Delete`.
    pub new_fragment_sha256: String,
    pub old_bytes: usize,
    pub new_bytes: usize,
    /// The splice appends the line terminator that a whole-line deletion
    /// took with the unit. Line extents exclude their trailing newline, so
    /// a normal line deletion (newline included) leaves the scar
    /// `before + after[1..]` rather than the exact `before + after`; when
    /// that variant is what matched, the terminator rides with the
    /// reinserted bytes instead of living in the historical extent.
    #[serde(default)]
    pub line_glue: bool,
}

/// All of one destination file's actions, in apply order: highest range
/// first, so splices never invalidate the offsets of actions still pending.
/// The containing-file hash is the staleness anchor — any live edit to the
/// file between plan and apply changes it, and the token with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentFilePlan {
    pub path: String,
    pub file_sha256: String,
    pub result_sha256: String,
    pub actions: Vec<FragmentAction>,
}

/// A dry-run fragment restore: pure computation, no worktree contact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentPlan {
    /// SHA-256 over (mode, selection IDs, ordered file plans). Excludes the
    /// base point for the same reason the restore token does: the
    /// pre-restore safety capture moves history without touching bytes.
    pub token: String,
    pub mode: FragmentMode,
    pub selections: Vec<SelectionHandle>,
    pub files: Vec<FragmentFilePlan>,
    pub conflicts: Vec<FragmentConflict>,
    /// Selections whose destination already holds their target content.
    pub unchanged: usize,
    pub base: ResolvedPoint,
    pub created_at_ms: i64,
    /// Computed without a live daemon, from a read-only store view.
    #[serde(default)]
    pub degraded: bool,
}

impl FragmentPlan {
    /// True when the plan has no unresolved conflicts and may be applied.
    pub fn applicable(&self) -> bool {
        self.conflicts.is_empty()
    }

    /// True when applying the plan would change nothing.
    pub fn is_noop(&self) -> bool {
        self.files.iter().all(|f| f.actions.is_empty())
    }

    /// Root-relative paths the plan would touch.
    pub fn destination_paths(&self) -> Vec<String> {
        self.files.iter().map(|f| f.path.clone()).collect()
    }

    /// Handle IDs of the selections that produced this plan.
    pub fn selection_ids(&self) -> Vec<String> {
        self.selections.iter().map(SelectionHandle::id).collect()
    }
}

// --------------------------------------------------------------- planning

fn parser_for(language: &str) -> Option<RustPrototypeParser> {
    // The prototype seam proves the parser contract; rust is the only
    // adapter, and unsupported languages fail closed.
    (language == "rust").then_some(RustPrototypeParser)
}

/// One selection's planning outcome before grouping into file plans.
// The size gap between variants is fine: `Planned` is constructed once per
// selection and consumed immediately, so the memcpy clippy warns about never
// happens in a loop worth caring about.
#[allow(clippy::large_enum_variant)]
enum Planned {
    /// Action against this destination file.
    Action(String, FragmentAction),
    /// Destination already holds the target bytes.
    Noop,
    /// Recorded as a conflict on the plan.
    Conflict(FragmentConflict),
}

#[allow(clippy::too_many_arguments)]
fn plan_one_selection(
    root: &Path,
    view: &mut HistoryView,
    head_frontier: &loro::Frontiers,
    handle: &SelectionHandle,
    mode: FragmentMode,
) -> Planned {
    let selection_id = handle.id();

    // 1. The handle must describe its own source: extent, range, selected
    //    bytes, and context hashes are all re-verified at plan time.
    if handle.extent == SelectionExtent::Hunk {
        // Hunk extents are diff-derived and not emitted by any public
        // surface yet; refuse rather than guess their meaning here.
        return Planned::Conflict(FragmentConflict {
            selection_id,
            path: None,
            condition: FragmentCondition::UnsupportedSource,
            candidates: Vec::new(),
            detail: "hunk extents have no public fragment surface yet".into(),
        });
    }
    let Ok(frontier) = decode_frontier(&handle.source_frontier) else {
        return Planned::Conflict(FragmentConflict {
            selection_id,
            path: None,
            condition: FragmentCondition::InvalidSource,
            candidates: Vec::new(),
            detail: "source frontier is malformed".into(),
        });
    };
    let source = match view.path_at(&frontier, &handle.historical_path) {
        Ok(HistoricalPathContent::Text(text)) => text,
        Ok(HistoricalPathContent::Absent) => {
            return Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::UnsupportedSource,
                candidates: Vec::new(),
                detail: format!(
                    "`{}` is absent at the selection's own frontier",
                    handle.historical_path
                ),
            })
        }
        Ok(HistoricalPathContent::Binary { .. }) => {
            return Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::UnsupportedSource,
                candidates: Vec::new(),
                detail: "binary extents are out of scope for fragment restore".into(),
            })
        }
        Err(error) => {
            return Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::UnsupportedSource,
                candidates: Vec::new(),
                detail: format!("source read failed: {error}"),
            })
        }
    };
    let (before_ctx, after_ctx) = match handle.verified_contexts(&source) {
        Ok(pair) => pair,
        Err(error) => {
            return Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::InvalidSource,
                candidates: Vec::new(),
                detail: format!("handle does not describe its source: {error}"),
            })
        }
    };
    let selected_text = &source[handle.range.start..handle.range.end];

    // 2. Destination candidates follow the recorded rename graph from the
    //    handle's historical name toward the current head.
    let renames = match view.renames_between(&frontier, head_frontier) {
        Ok(renames) => renames,
        Err(error) => {
            return Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::UnsupportedSource,
                candidates: Vec::new(),
                detail: format!("rename history is unreadable: {error}"),
            })
        }
    };
    let names = expand_names(
        &std::collections::BTreeSet::from([handle.historical_path.clone()]),
        &renames,
    );
    let mut candidates: Vec<SelectionCandidate> = Vec::new();
    for name in &names {
        let path = root.join(name);
        if !path.is_file() {
            continue;
        }
        match std::fs::read(&path) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(text) => candidates.push(SelectionCandidate {
                    path: name.clone(),
                    text,
                }),
                Err(_) => {
                    // A destination that is no longer text cannot accept a
                    // text splice; it is simply not a candidate.
                }
            },
            Err(_) => {
                return Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: Some(name.clone()),
                    condition: FragmentCondition::Unreadable,
                    candidates: Vec::new(),
                    detail: format!("destination `{name}` cannot be read"),
                })
            }
        }
    }

    // 3. Rebind through the shared selector engine: parser-backed for
    //    semantic handles, exact text/context otherwise.
    let rebind = if handle.semantic.is_some() {
        let Some(wanted) = handle.semantic.as_ref() else {
            unreachable!("checked above")
        };
        match parser_for(&wanted.language) {
            Some(parser) => match rebind_symbol(handle, &candidates, &parser) {
                Ok(outcome) => outcome,
                Err(SymbolParseError::UnsupportedLanguage(language)) => {
                    return Planned::Conflict(FragmentConflict {
                        selection_id,
                        path: None,
                        condition: FragmentCondition::UnsupportedLanguage,
                        candidates: Vec::new(),
                        detail: format!("no symbol adapter for `{language}`"),
                    })
                }
                Err(SymbolParseError::InvalidSource(detail)) => {
                    return Planned::Conflict(FragmentConflict {
                        selection_id,
                        path: None,
                        condition: FragmentCondition::InvalidSource,
                        candidates: Vec::new(),
                        detail: format!("destination does not parse: {detail}"),
                    })
                }
            },
            None => {
                return Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: None,
                    condition: FragmentCondition::UnsupportedLanguage,
                    candidates: Vec::new(),
                    detail: format!("no symbol adapter for `{}`", wanted.language),
                })
            }
        }
    } else {
        match rebind_exact(handle, selected_text, &candidates) {
            Ok(outcome) => outcome,
            Err(
                error @ (SelectionError::SourceContentMismatch
                | SelectionError::UnsupportedVersion(_)),
            ) => {
                return Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: None,
                    condition: FragmentCondition::InvalidSource,
                    candidates: Vec::new(),
                    detail: format!("handle failed source validation: {error}"),
                })
            }
            Err(error) => {
                return Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: None,
                    condition: FragmentCondition::InvalidSource,
                    candidates: Vec::new(),
                    detail: format!("rebinding failed: {error}"),
                })
            }
        }
    };

    let ranges_of = |bindings: Vec<super::selection::BoundSelection>| -> Vec<FragmentRange> {
        bindings
            .into_iter()
            .map(|b| FragmentRange {
                path: b.path,
                range: b.range,
            })
            .collect()
    };

    match (mode, rebind) {
        (FragmentMode::Replace, RebindOutcome::Bound { binding }) => {
            let text = candidates
                .iter()
                .find(|c| c.path == binding.path)
                .expect("binding came from a candidate");
            let old = &text.text.as_bytes()[binding.range.start..binding.range.end];
            if old == selected_text.as_bytes() {
                return Planned::Noop;
            }
            Planned::Action(
                binding.path.clone(),
                FragmentAction {
                    selection_id,
                    handle: handle.clone(),
                    kind: FragmentActionKind::Replace,
                    range: binding.range,
                    old_fragment_sha256: sha256(old),
                    new_fragment_sha256: handle.selected_text_sha256.clone(),
                    old_bytes: old.len(),
                    new_bytes: selected_text.len(),
                    line_glue: false,
                },
            )
        }
        (FragmentMode::Delete, RebindOutcome::Bound { binding }) => {
            let text = candidates
                .iter()
                .find(|c| c.path == binding.path)
                .expect("binding came from a candidate");
            let old = &text.text.as_bytes()[binding.range.start..binding.range.end];
            Planned::Action(
                binding.path.clone(),
                FragmentAction {
                    selection_id,
                    handle: handle.clone(),
                    kind: FragmentActionKind::Delete,
                    range: binding.range,
                    old_fragment_sha256: sha256(old),
                    new_fragment_sha256: sha256(b""),
                    old_bytes: old.len(),
                    new_bytes: 0,
                    line_glue: false,
                },
            )
        }
        (FragmentMode::Insert, RebindOutcome::Bound { binding }) => {
            Planned::Conflict(FragmentConflict {
                selection_id,
                path: Some(binding.path.clone()),
                condition: FragmentCondition::UnexpectedState,
                candidates: vec![FragmentRange {
                    path: binding.path,
                    range: binding.range,
                }],
                detail: "the unit is already present; replace is the default mode".into(),
            })
        }
        (FragmentMode::Replace, RebindOutcome::Missing) => Planned::Conflict(FragmentConflict {
            selection_id,
            path: None,
            condition: FragmentCondition::Missing,
            candidates: Vec::new(),
            detail: "the selected unit is absent from the destination; pass --insert to \
                     reinsert it at its deletion scar"
                .into(),
        }),
        (FragmentMode::Delete, RebindOutcome::Missing) => Planned::Conflict(FragmentConflict {
            selection_id,
            path: None,
            condition: FragmentCondition::UnexpectedState,
            candidates: Vec::new(),
            detail: "the unit is already absent from the destination".into(),
        }),
        (_, RebindOutcome::Ambiguous { candidates: bound }) => {
            Planned::Conflict(FragmentConflict {
                selection_id,
                path: None,
                condition: FragmentCondition::Ambiguous,
                candidates: ranges_of(bound),
                detail: "multiple destinations match; name one explicitly or narrow the \
                     selection"
                    .into(),
            })
        }
        (FragmentMode::Insert, RebindOutcome::Missing) => {
            // Deletion-scar search: the joined before/after context bytes
            // must occur exactly once across every destination candidate.
            // A `line` extent excludes its trailing newline by
            // construction, and a normal editor line deletion takes the
            // newline with the line — so a line unit leaves two possible
            // scars: the exact join when the removal kept the newline,
            // and the whole-line join (`before + after[1..]`) when it
            // did not. Both variants are searched for line extents;
            // exactly one placement across the union authorizes the
            // splice, and the whole-line variant sets `line_glue` so the
            // terminator is reinserted with the unit. Match extents keep
            // the exact scar only: their unit is the string, not the
            // line, and the two variants can overlap textually there.
            let mut scars: Vec<(String, bool)> = vec![(format!("{before_ctx}{after_ctx}"), false)];
            if handle.extent == SelectionExtent::Line && after_ctx.starts_with('\n') {
                scars.push((format!("{before_ctx}{}", &after_ctx[1..]), true));
            }
            let mut placements: Vec<FragmentRange> = Vec::new();
            let mut glue = false;
            for (scar, scar_glue) in &scars {
                for candidate in &candidates {
                    let starts = if scar.is_empty() {
                        // Whole-file selection with no context on either side:
                        // the only unambiguous destination is an empty file.
                        if candidate.text.is_empty() {
                            vec![0]
                        } else {
                            Vec::new()
                        }
                    } else {
                        overlapping_match_starts(&candidate.text, scar)
                    };
                    for start in starts {
                        placements.push(FragmentRange {
                            path: candidate.path.clone(),
                            range: ByteRange {
                                start: start + before_ctx.len(),
                                end: start + before_ctx.len(),
                            },
                        });
                        glue = *scar_glue;
                    }
                }
            }
            match placements.len() {
                1 => {
                    let placement = placements.remove(0);
                    let new_bytes = if glue {
                        sha256(format!("{selected_text}\n").as_bytes())
                    } else {
                        handle.selected_text_sha256.clone()
                    };
                    Planned::Action(
                        placement.path.clone(),
                        FragmentAction {
                            selection_id,
                            handle: handle.clone(),
                            kind: FragmentActionKind::Insert,
                            range: placement.range,
                            old_fragment_sha256: sha256(b""),
                            new_fragment_sha256: new_bytes,
                            old_bytes: 0,
                            new_bytes: selected_text.len() + usize::from(glue),
                            line_glue: glue,
                        },
                    )
                }
                0 => Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: None,
                    condition: FragmentCondition::Missing,
                    candidates: Vec::new(),
                    detail: "no unique deletion scar: the surroundings changed after the \
                             unit was removed"
                        .into(),
                }),
                _ => Planned::Conflict(FragmentConflict {
                    selection_id,
                    path: None,
                    condition: FragmentCondition::Ambiguous,
                    candidates: placements,
                    detail: "the deletion scar occurs more than once".into(),
                }),
            }
        }
    }
}

fn build_fragment_plan(
    root: &Path,
    doc: &LoroDoc,
    base: ResolvedPoint,
    selections: &[SelectionHandle],
    mode: FragmentMode,
    degraded: bool,
) -> Result<FragmentPlan> {
    let head_frontier = decode_frontier(&base.frontier)?;
    let mut view = HistoryView::open(doc)?;
    let mut by_path: BTreeMap<String, Vec<FragmentAction>> = BTreeMap::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0usize;

    for handle in selections {
        match plan_one_selection(root, &mut view, &head_frontier, handle, mode) {
            Planned::Action(path, action) => by_path.entry(path).or_default().push(action),
            Planned::Noop => unchanged += 1,
            Planned::Conflict(conflict) => conflicts.push(conflict),
        }
    }

    // Group into file plans; within a file, apply order is highest range
    // first (ties: longer range first, so a replace at [x..y] runs before an
    // insert at [x..x] and lands after it in the output). Overlapping
    // actions in one file are a conflict: two splices whose offsets cannot
    // both be trusted.
    let mut files = Vec::new();
    'file: for (path, mut actions) in by_path {
        actions.sort_by(|a, b| {
            b.range
                .start
                .cmp(&a.range.start)
                .then(b.range.end.cmp(&a.range.end))
        });
        let text = std::fs::read_to_string(root.join(&path)).map_err(|e| {
            SheafError::RestoreObstructed(format!("destination `{path}` is unreadable: {e}"))
        })?;
        let file_sha256 = blobs::hash_of(text.as_bytes());
        let mut spliced = text.into_bytes();
        for pair in actions.windows(2) {
            let (later, earlier) = (&pair[0], &pair[1]);
            let overlap = earlier.range.end > later.range.start
                || (earlier.range.start == later.range.start
                    && earlier.range.end == later.range.start);
            if overlap {
                conflicts.push(FragmentConflict {
                    selection_id: earlier.selection_id.clone(),
                    path: Some(path.clone()),
                    condition: FragmentCondition::Overlap,
                    candidates: Vec::new(),
                    detail: format!(
                        "selections {} and {} overlap in `{path}`",
                        &later.selection_id[..12.min(later.selection_id.len())],
                        &earlier.selection_id[..12.min(earlier.selection_id.len())],
                    ),
                });
                continue 'file;
            }
        }
        for action in &actions {
            let new_bytes = action_new_bytes(&mut view, action)?;
            let range = action.range.start..action.range.end;
            if range.end > spliced.len()
                || sha256(&spliced[range.clone()]) != action.old_fragment_sha256
            {
                return Err(SheafError::StoreCorrupt(format!(
                    "planned fragment for `{path}` does not match its own file hash"
                )));
            }
            spliced.splice(range, new_bytes);
        }
        let result_sha256 = blobs::hash_of(&spliced);
        files.push(FragmentFilePlan {
            path,
            file_sha256,
            result_sha256,
            actions,
        });
    }

    let token = fragment_token(mode, selections, &files);
    Ok(FragmentPlan {
        token,
        mode,
        selections: selections.to_vec(),
        files,
        conflicts,
        unchanged,
        base,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
        degraded,
    })
}

/// The bytes an action writes: the handle's historical extent for replace
/// and insert (plus the glued terminator for whole-line insertions),
/// nothing for delete.
fn action_new_bytes(view: &mut HistoryView, action: &FragmentAction) -> Result<Vec<u8>> {
    match action.kind {
        FragmentActionKind::Delete => Ok(Vec::new()),
        _ => {
            let mut bytes = fragment_source_bytes(view, &action.handle)?;
            if action.line_glue {
                bytes.push(b'\n');
            }
            Ok(bytes)
        }
    }
}

/// Re-derive a handle's selected bytes from immutable history. The store
/// never carries fragment payloads in intents; replay reads them here so a
/// restart converges on identical bytes by construction.
fn fragment_source_bytes(view: &mut HistoryView, handle: &SelectionHandle) -> Result<Vec<u8>> {
    let frontier = decode_frontier(&handle.source_frontier)?;
    match view.path_at(&frontier, &handle.historical_path)? {
        HistoricalPathContent::Text(text) => {
            if handle.range.end > text.len() {
                return Err(SheafError::StoreCorrupt(format!(
                    "selection range {}..{} exceeds `{}` at its frontier",
                    handle.range.start, handle.range.end, handle.historical_path
                )));
            }
            let bytes = &text.as_bytes()[handle.range.start..handle.range.end];
            if sha256(bytes) != handle.selected_text_sha256 {
                return Err(SheafError::StoreCorrupt(format!(
                    "history bytes for `{}` no longer match the selection handle",
                    handle.historical_path
                )));
            }
            Ok(bytes.to_vec())
        }
        _ => Err(SheafError::StoreCorrupt(format!(
            "selection source `{}` is absent or binary at its frontier",
            handle.historical_path
        ))),
    }
}

/// Content address of the outcome a fragment plan describes. Selection IDs
/// are themselves content-addressed over frontier, extent, bytes, and
/// context, so the token pins both the source identity and the destination
/// splices without duplicating the handles.
fn fragment_token(
    mode: FragmentMode,
    selections: &[SelectionHandle],
    files: &[FragmentFilePlan],
) -> String {
    let canonical = serde_json::json!({
        "v": 1,
        "kind": "sheaf:fragment-plan",
        "mode": mode,
        "selections": selections.iter().map(SelectionHandle::id).collect::<Vec<_>>(),
        "files": files.iter().map(|f| serde_json::json!({
            "path": f.path,
            "file": f.file_sha256,
            "result": f.result_sha256,
            "actions": f.actions.iter().map(|a| serde_json::json!({
                "selection": a.selection_id,
                "kind": a.kind,
                "range": [a.range.start, a.range.end],
                "old": a.old_fragment_sha256,
                "new": a.new_fragment_sha256,
                "glue": a.line_glue,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    });
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

// ----------------------------------------------------- plan (read-only API)

impl ProjectStore {
    /// Dry-run a fragment restore of the given selections into the live
    /// worktree (usually via the daemon; pure computation).
    pub fn plan_fragment_restore(
        &self,
        selections: &[SelectionHandle],
        mode: FragmentMode,
    ) -> Result<FragmentPlan> {
        let base = self.resolve("@")?;
        build_fragment_plan(&self.root, &self.doc, base, selections, mode, false)
    }
}

impl TimelineReader {
    /// Degraded-mode counterpart: same plan from a read-only store view.
    pub fn plan_fragment_restore(
        &self,
        selections: &[SelectionHandle],
        mode: FragmentMode,
    ) -> Result<FragmentPlan> {
        let base = self.resolve("@")?;
        build_fragment_plan(self.root(), self.doc(), base, selections, mode, true)
    }
}

// ---------------------------------------------------------- the write path

impl ProjectStore {
    /// Execute a previously computed fragment plan. Mirrors `apply_restore`'s
    /// ordering: revalidate before anything is written, capture the live
    /// worktree as the undo reference, fsync the intent, install whole files
    /// atomically, then append one forward capture naming the selections.
    pub fn apply_fragment_restore(
        &mut self,
        plan: &FragmentPlan,
        ignore: &IgnoreSet,
    ) -> Result<RestoreOutcome> {
        self.run_fragment(plan, ignore, false)
    }

    fn run_fragment(
        &mut self,
        plan: &FragmentPlan,
        ignore: &IgnoreSet,
        resumed: bool,
    ) -> Result<RestoreOutcome> {
        if !plan.applicable() {
            return Err(SheafError::RestoreObstructed(describe_conflicts(
                &plan.conflicts,
            )));
        }
        let mut progress_log = Vec::new();

        // 1. Revalidate BEFORE anything is written — including before the
        //    safety capture, so a rejected fragment restore leaves the store
        //    exactly as it found it.
        let checked = self.plan_fragment_restore(&plan.selections, plan.mode)?;
        if !checked.applicable() {
            return Err(SheafError::RestoreObstructed(describe_conflicts(
                &checked.conflicts,
            )));
        }
        if !resumed && checked.token != plan.token {
            return Err(SheafError::RestorePlanStale(
                "the destination file(s) changed since this fragment plan was computed".into(),
            ));
        }

        // 2. Nothing a fragment overwrites may be unrecoverable: the live
        //    worktree becomes history before the first byte moves.
        let pre_restore_capture = self.reconcile_tagged(
            ignore,
            Some(CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: None,
                scope: checked.destination_paths(),
                selections: Vec::new(),
            }),
        )?;
        if let Some(capture) = &pre_restore_capture {
            progress_log.push(format!(
                "captured pre-restore worktree state as {}",
                capture.short_id()
            ));
        }

        // The safety capture moved the base point but touched no file, so
        // the validated plan still describes this worktree exactly.
        let fresh = self.plan_fragment_restore(&plan.selections, plan.mode)?;
        if fresh.token != checked.token {
            return Err(SheafError::StoreCorrupt(
                "pre-restore capture changed the pending fragment plan".into(),
            ));
        }

        let undo = self.resolve("@")?;
        let target = fresh
            .selections
            .first()
            .map(|handle| ResolvedPoint {
                frontier: handle.source_frontier.clone(),
                capture_id: handle.source_capture_id.clone(),
            })
            .unwrap_or_else(|| undo.clone());

        // 3. Durable intent, then the worktree mutations it authorizes.
        self.write_fragment_intent(&fresh)?;

        let mut written_paths: Vec<String> = Vec::new();
        for file in &fresh.files {
            if file.actions.is_empty() {
                continue;
            }
            // Same concurrent-edit guard as the whole-file engine: anything
            // the user saved into this path since the safety capture is
            // captured before it is overwritten.
            if let Some(rescued) = self.capture_drift(&file.path)? {
                progress_log.push(format!(
                    "captured a concurrent edit to {} as {}",
                    file.path,
                    rescued.short_id()
                ));
                // The drift capture changed the file's history position; the
                // bytes on disk are what the splices verify against, so the
                // plan is still exact. If the edit touched bytes, the old
                // fragment hashes below reject the apply.
            }
            let Some(bytes) = self.fragment_file_bytes(file)? else {
                // Already at its planned result (e.g. replayed watcher echo);
                // nothing to install for this file.
                continue;
            };
            let entry = Entry::text(
                String::from_utf8(bytes).map_err(|_| {
                    SheafError::StoreCorrupt(format!(
                        "fragment splice produced invalid UTF-8 for `{}`",
                        file.path
                    ))
                })?,
                self.live_exec(&file.path),
            );
            self.install(&file.path, &entry)?;
            written_paths.push(file.path.clone());
            progress_log.push(format!("spliced {} ({})", file.path, fresh.mode_as_str()));
        }

        // 4. Forward capture with selection provenance.
        let restore_capture = self.record_fragment_capture(&fresh, &written_paths)?;
        match &restore_capture {
            Some(capture) => progress_log.push(format!(
                "recorded fragment restore as capture {}",
                capture.short_id()
            )),
            None => progress_log.push("destination already held the selected bytes".into()),
        }
        let result = self.resolve("@")?;

        self.clear_intent();
        tracing::info!(
            root = %self.root.display(),
            mode = ?fresh.mode,
            files = written_paths.len(),
            unchanged = fresh.unchanged,
            resumed,
            "fragment restore applied"
        );

        Ok(RestoreOutcome {
            token: fresh.token,
            mode: RestoreMode::Fragment,
            target,
            undo,
            result,
            pre_restore_capture: pre_restore_capture.map(|c| c.id),
            restore_capture: restore_capture.map(|c| c.id),
            files_written: written_paths.len(),
            files_deleted: 0,
            unchanged: fresh.unchanged,
            written_paths,
            deleted_paths: Vec::new(),
            resumed,
            progress_log,
        })
    }

    /// Rebuild one file's post-plan bytes, or `None` when the file already
    /// holds them (idempotent replay). Errors on any file that matches
    /// neither its pre-plan hash (normal apply) nor its result hash
    /// (already applied) — those bytes were written by someone else.
    fn fragment_file_bytes(&self, file: &FragmentFilePlan) -> Result<Option<Vec<u8>>> {
        let path = self.root.join(&file.path);
        let live = std::fs::read(&path).map_err(|e| {
            SheafError::RestoreObstructed(format!("destination `{}` is unreadable: {e}", file.path))
        })?;
        let live_hash = blobs::hash_of(&live);
        if live_hash == file.result_sha256 {
            return Ok(None);
        }
        if live_hash != file.file_sha256 {
            return Err(SheafError::RestorePlanStale(format!(
                "`{}` changed since the fragment plan was computed",
                file.path
            )));
        }
        let mut view = HistoryView::open(&self.doc)?;
        let mut out = live;
        for action in &file.actions {
            let new_bytes = action_new_bytes(&mut view, action)?;
            let range = action.range.start..action.range.end;
            if range.end > out.len() || sha256(&out[range.clone()]) != action.old_fragment_sha256 {
                return Err(SheafError::RestorePlanStale(format!(
                    "the bytes at {}..{} in `{}` are no longer what the plan splices",
                    action.range.start, action.range.end, file.path
                )));
            }
            out.splice(range, new_bytes);
        }
        if blobs::hash_of(&out) != file.result_sha256 {
            return Err(SheafError::StoreCorrupt(format!(
                "fragment splice of `{}` did not reproduce the planned result hash",
                file.path
            )));
        }
        Ok(Some(out))
    }

    /// The exec bit of a destination file, preserved across a fragment
    /// splice: a content restore never silently changes file modes.
    fn live_exec(&self, key: &str) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::symlink_metadata(self.root.join(key))
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }

    fn write_fragment_intent(&self, plan: &FragmentPlan) -> Result<()> {
        let intent = RestoreIntent {
            token: plan.token.clone(),
            mode: RestoreMode::Fragment,
            scope: plan.destination_paths(),
            target: plan.base.clone(),
            started_ms: chrono::Utc::now().timestamp_millis(),
            fragment: Some(Box::new(plan.clone())),
        };
        let dir = state_dir(&self.root);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(INTENT_FILE);
        let tmp = dir.join(".restore.intent.tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(
                serde_json::to_vec_pretty(&intent)
                    .map_err(|e| SheafError::StoreCorrupt(e.to_string()))?
                    .as_slice(),
            )?;
            file.sync_all()?;
        }
        std::fs::rename(tmp, &path)?;
        super::fsutil::sync_parent_dir(&path)?;
        Ok(())
    }

    /// One forward capture whose origin names every selection ID that
    /// produced it, so the splice's provenance is recorded on the timeline.
    fn record_fragment_capture(
        &mut self,
        plan: &FragmentPlan,
        written: &[String],
    ) -> Result<Option<Capture>> {
        if written.is_empty() {
            return Ok(None);
        }
        let events = written
            .iter()
            .map(|path| {
                FsEvent::now(EventKind::Touched {
                    path: self.root.join(path).into(),
                })
            })
            .collect();
        let now = chrono::Utc::now();
        let outcome = self.apply_batch_tagged(
            &Batch {
                root: self.root.clone(),
                started_at: now,
                flushed_at: now,
                events,
            },
            Some(CaptureOrigin {
                kind: OriginKind::FragmentRestore,
                target: None,
                scope: written.to_vec(),
                selections: plan.selection_ids(),
            }),
        )?;
        Ok(outcome.capture)
    }

    /// Resume an interrupted fragment restore from its durable intent.
    /// Replay never re-plans: the intent's recorded file plans and hashes
    /// drive each file to its planned result, so a `kill -9` between file
    /// installs converges on the complete fragment restore, and a crash
    /// before the first install leaves the untouched pre-intent state
    /// (the intent is simply cleared after reconciliation).
    pub(super) fn resume_fragment(
        &mut self,
        intent: &RestoreIntent,
        ignore: &IgnoreSet,
    ) -> Result<RestoreOutcome> {
        let Some(plan) = intent.fragment.as_deref() else {
            return Err(SheafError::StoreCorrupt(
                "fragment intent carries no plan payload".into(),
            ));
        };
        if plan.token != intent.token {
            return Err(SheafError::StoreCorrupt(
                "fragment intent token does not match its payload".into(),
            ));
        }
        let mut progress_log = Vec::new();

        // Whatever the crash left behind becomes history before replay
        // continues; the per-file hashes then decide skip-or-splice.
        let pre_restore_capture = self.reconcile_tagged(
            ignore,
            Some(CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: None,
                scope: plan.destination_paths(),
                selections: Vec::new(),
            }),
        )?;
        if let Some(capture) = &pre_restore_capture {
            progress_log.push(format!(
                "captured pre-resume worktree state as {}",
                capture.short_id()
            ));
        }
        let undo = self.resolve("@")?;

        let mut written_paths: Vec<String> = Vec::new();
        for file in &plan.files {
            if file.actions.is_empty() {
                continue;
            }
            if let Some(bytes) = self.fragment_file_bytes(file)? {
                let entry = Entry::text(
                    String::from_utf8(bytes).map_err(|_| {
                        SheafError::StoreCorrupt(format!(
                            "fragment replay produced invalid UTF-8 for `{}`",
                            file.path
                        ))
                    })?,
                    self.live_exec(&file.path),
                );
                self.install(&file.path, &entry)?;
                written_paths.push(file.path.clone());
                progress_log.push(format!("replayed splice of {}", file.path));
            }
        }

        let restore_capture = self.record_fragment_capture(plan, &written_paths)?;
        if restore_capture.is_some() {
            progress_log.push("recorded replayed fragment restore".into());
        }
        let result = self.resolve("@")?;

        self.clear_intent();
        let target = plan
            .selections
            .first()
            .map(|handle| ResolvedPoint {
                frontier: handle.source_frontier.clone(),
                capture_id: handle.source_capture_id.clone(),
            })
            .unwrap_or_else(|| undo.clone());

        Ok(RestoreOutcome {
            token: plan.token.clone(),
            mode: RestoreMode::Fragment,
            target,
            undo,
            result,
            pre_restore_capture: pre_restore_capture.map(|c| c.id),
            restore_capture: restore_capture.map(|c| c.id),
            files_written: written_paths.len(),
            files_deleted: 0,
            unchanged: plan.unchanged,
            written_paths,
            deleted_paths: Vec::new(),
            resumed: true,
            progress_log,
        })
    }
}

impl FragmentPlan {
    fn mode_as_str(&self) -> &'static str {
        match self.mode {
            FragmentMode::Replace => "replace",
            FragmentMode::Insert => "insert",
            FragmentMode::Delete => "delete",
        }
    }
}

fn describe_conflicts(conflicts: &[FragmentConflict]) -> String {
    conflicts
        .iter()
        .take(5)
        .map(|c| {
            format!(
                "selection {} ({:?}) {}",
                &c.selection_id[..12.min(c.selection_id.len())],
                c.condition,
                c.detail
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(text: &str, needle: &str) -> SelectionHandle {
        let start = text.find(needle).unwrap();
        SelectionHandle::from_source(
            "ff",
            Some("cap".into()),
            "a.rs",
            SelectionExtent::Match,
            ByteRange {
                start,
                end: start + needle.len(),
            },
            text,
            "literal:test",
            None,
        )
        .unwrap()
    }

    #[test]
    fn token_binds_outcome_not_noise() {
        let text = "fn old() {}\nfn kept() {}\n";
        let h = handle(text, "fn old() {}");
        let files = vec![FragmentFilePlan {
            path: "a.rs".into(),
            file_sha256: "f".into(),
            result_sha256: "r".into(),
            actions: vec![FragmentAction {
                selection_id: h.id(),
                handle: h.clone(),
                kind: FragmentActionKind::Replace,
                range: ByteRange { start: 0, end: 10 },
                old_fragment_sha256: "old".into(),
                new_fragment_sha256: "new".into(),
                old_bytes: 10,
                new_bytes: 10,
                line_glue: false,
            }],
        }];
        let a = fragment_token(FragmentMode::Replace, std::slice::from_ref(&h), &files);
        let b = fragment_token(FragmentMode::Replace, std::slice::from_ref(&h), &files);
        assert_eq!(a, b);
        let mut other = files.clone();
        other[0].result_sha256 = "r2".into();
        assert_ne!(
            a,
            fragment_token(FragmentMode::Replace, std::slice::from_ref(&h), &other)
        );
        assert_ne!(
            a,
            fragment_token(FragmentMode::Delete, std::slice::from_ref(&h), &files)
        );
        // The glued terminator changes the spliced bytes, so it must
        // change the token: the new-fragment hash covers it.
        let mut glued = files.clone();
        glued[0].actions[0].line_glue = true;
        glued[0].actions[0].new_bytes += 1;
        assert_ne!(
            a,
            fragment_token(FragmentMode::Replace, std::slice::from_ref(&h), &glued)
        );
    }

    #[test]
    fn mode_parsing_is_total() {
        assert_eq!(FragmentMode::parse("insert").unwrap(), FragmentMode::Insert);
        assert!(FragmentMode::parse("fuzzy").is_err());
    }

    // ---- store-backed fixtures (mirror crates/sheaf-core/tests/fragment.rs) ----

    use std::path::PathBuf;

    use crate::config;
    use crate::store::{SemanticIdentity, StoreLimits};

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sheaf-fragment-unit-{tag}-{}-{}",
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

    fn line_handle_at(
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
        let at = text
            .find(needle)
            .unwrap_or_else(|| panic!("`{needle}` not found in {path} at {reference}"));
        let start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
        let end = text[at..].find('\n').map(|n| at + n).unwrap_or(text.len());
        SelectionHandle::from_source(
            point.frontier,
            point.capture_id,
            path,
            SelectionExtent::Line,
            ByteRange { start, end },
            &text,
            format!("literal:{needle}"),
            None,
        )
        .unwrap()
    }

    const TEXT: &str = "fn alpha() {\n    1\n}\n\nfn beta() {\n    2\n}\n";

    fn captured(root: &Path, content: &str) -> ProjectStore {
        skeleton(root);
        let mut store = open(root);
        write(root, "src/lib.rs", content.as_bytes());
        flush(&mut store, root, vec![added(root, "src/lib.rs")]);
        store
    }

    fn condition_of(plan: &FragmentPlan) -> FragmentCondition {
        assert_eq!(plan.conflicts.len(), 1, "{plan:#?}");
        plan.conflicts[0].condition.clone()
    }

    #[test]
    fn mode_and_condition_wire_forms_are_stable() {
        // The wire contract: parse accepts exactly the three snake_case
        // forms, and conditions serialize as their documented stable names.
        assert_eq!(
            FragmentMode::parse("replace").unwrap(),
            FragmentMode::Replace
        );
        assert_eq!(FragmentMode::parse("delete").unwrap(), FragmentMode::Delete);
        assert!(matches!(
            FragmentMode::parse("fuzzy"),
            Err(crate::error::SheafError::Config(_))
        ));
        assert_eq!(
            serde_json::to_value(FragmentMode::Insert).unwrap(),
            serde_json::json!("insert")
        );
        assert_eq!(
            serde_json::to_value(FragmentCondition::UnexpectedState).unwrap(),
            serde_json::json!("unexpected_state")
        );
        for (form, condition) in [
            ("missing", FragmentCondition::Missing),
            ("ambiguous", FragmentCondition::Ambiguous),
            ("invalid_source", FragmentCondition::InvalidSource),
            ("unsupported_source", FragmentCondition::UnsupportedSource),
            (
                "unsupported_language",
                FragmentCondition::UnsupportedLanguage,
            ),
            ("unexpected_state", FragmentCondition::UnexpectedState),
            ("overlap", FragmentCondition::Overlap),
            ("unreadable", FragmentCondition::Unreadable),
        ] {
            let round: FragmentCondition = serde_json::from_value(serde_json::json!(form)).unwrap();
            assert_eq!(round, condition);
        }
    }

    #[test]
    fn mode_state_contradictions_are_typed_conflicts() {
        let root = tmp("state");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");

        // Insert over a present unit: replace is the default, insert must be
        // named explicitly, and the present binding rides along.
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Insert)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::UnexpectedState);
        assert_eq!(plan.conflicts[0].candidates.len(), 1);
        assert!(!plan.applicable());

        // Replace of an unchanged unit is a recorded no-op, not a splice.
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Replace)
            .unwrap();
        assert!(plan.applicable());
        assert!(plan.is_noop());
        assert_eq!(plan.unchanged, 1);
        assert!(plan.destination_paths().is_empty());
        assert_eq!(plan.selection_ids(), vec![alpha.id()]);

        // Replace over an absent unit: typed missing, with the --insert hint.
        let without = TEXT.replace("fn alpha() {\n    1\n}\n\n", "");
        write(&root, "src/lib.rs", without.as_bytes());
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::Missing);
        assert!(plan.conflicts[0].detail.contains("--insert"));

        // Delete over an absent unit: the state, not the selection, is wrong.
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Delete)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::UnexpectedState);
        assert!(plan.conflicts[0].detail.contains("already absent"));
    }

    #[test]
    fn insert_rebinds_a_deleted_unit_at_its_unique_scar() {
        let root = tmp("insert");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        // Remove exactly the unit's bytes; its context join survives intact.
        let without = TEXT.replace("fn alpha() {\n    1\n}", "");
        write(&root, "src/lib.rs", without.as_bytes());
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Insert)
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        assert_eq!(plan.files.len(), 1);
        let action = &plan.files[0].actions[0];
        assert_eq!(action.kind, FragmentActionKind::Insert);
        assert_eq!(
            action.range.start, action.range.end,
            "scar is an empty range"
        );
        assert_eq!(action.old_bytes, 0);
        assert!(!action.line_glue, "match extents keep the exact scar");
    }

    #[test]
    fn insert_line_extent_glues_the_terminator_back() {
        let root = tmp("glue");
        let mut store = captured(&root, TEXT);
        let alpha_line = line_handle_at(&store, "@", "src/lib.rs", "    1");
        // A normal editor deletion takes the line AND its newline, so the
        // exact join (`before + after`) does not occur — the whole-line join
        // does, and the terminator must ride with the reinserted bytes.
        let without = TEXT.replace("    1\n", "");
        assert_ne!(without, TEXT);
        write(&root, "src/lib.rs", without.as_bytes());
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha_line), FragmentMode::Insert)
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        let action = &plan.files[0].actions[0];
        assert!(action.line_glue, "whole-line scar glues the newline");
        assert_eq!(action.new_bytes, 5 + 1);

        let outcome = store.apply_fragment_restore(&plan, &ignores()).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("src/lib.rs")).unwrap(),
            TEXT,
            "the splice must reproduce the original file byte-for-byte"
        );
        assert_eq!(outcome.files_written, 1);
        assert!(outcome
            .progress_log
            .iter()
            .any(|line| line.contains("spliced src/lib.rs (insert)")));
        assert_eq!(outcome.mode, RestoreMode::Fragment);
    }

    #[test]
    fn insert_refuses_ambiguous_and_moved_scars() {
        let root = tmp("scars");
        // Three identical regions separated by identical (>64-byte) padding:
        // deleting every unit leaves the same context join at all three
        // internal pad boundaries — nothing may be guessed.
        let region = "fn u() {\n    1\n}\n";
        // 81 non-periodic bytes, wider than a context window.
        let pad: String = (0..9).map(|i| format!("pad line {i}\n")).collect();
        let repeated = format!("{pad}{region}{pad}{region}{pad}{region}{pad}");
        let store = captured(&root, &repeated);
        let unit = handle_at(&store, "@", "src/lib.rs", region);
        let all_gone = repeated.replace(region, "");
        assert_eq!(all_gone, pad.repeat(4));
        write(&root, "src/lib.rs", all_gone.as_bytes());
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&unit), FragmentMode::Insert)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::Ambiguous);
        assert_eq!(plan.conflicts[0].candidates.len(), 3);

        // A scar that no longer exists at all is a missing scar, not a guess.
        let root = tmp("scar-gone");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        let rewrote = TEXT
            .replace("fn alpha() {\n    1\n}\n\n", "fn alpha() {\n    9\n}\n\n")
            .replace("\n\nfn beta", "\nfn beta");
        write(&root, "src/lib.rs", rewrote.as_bytes());
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Insert)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::Missing);
        assert!(plan.conflicts[0].detail.contains("no unique deletion scar"));
    }

    #[test]
    fn source_validation_fails_closed_by_condition() {
        let root = tmp("source");
        let store = captured(&root, TEXT);

        // Hunk extents have no fragment surface.
        let mut hunk = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        hunk.extent = SelectionExtent::Hunk;
        let plan = store
            .plan_fragment_restore(&[hunk], FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::UnsupportedSource);
        assert!(plan.conflicts[0].detail.contains("hunk extents"));

        // A handle whose frontier is not decodable cannot even find its own
        // source snapshot.
        let mut bogus = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        bogus.source_frontier = "zzz".into();
        let plan = store
            .plan_fragment_restore(&[bogus], FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::InvalidSource);
        assert!(plan.conflicts[0].detail.contains("malformed"));

        // A handle naming a path absent at its own frontier.
        let mut ghost = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        ghost.historical_path = "src/ghost.rs".into();
        let plan = store
            .plan_fragment_restore(&[ghost], FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::UnsupportedSource);
        assert!(plan.conflicts[0].detail.contains("ghost.rs"));

        // A handle that does not describe its source bytes/contexts.
        let mut lying = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        lying.before_context_sha256 = sha256(b"elsewhere");
        let plan = store
            .plan_fragment_restore(&[lying], FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::InvalidSource);
        assert!(plan.conflicts[0]
            .detail
            .contains("does not describe its source"));

        // Semantic handles fail closed when no adapter exists for the language.
        let mut python = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        python.semantic = Some(SemanticIdentity {
            language: "python".into(),
            kind: "function".into(),
            qualified_name: "alpha".into(),
            structural_fingerprint: "f".into(),
        });
        let plan = store
            .plan_fragment_restore(&[python], FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::UnsupportedLanguage);
        assert!(plan.conflicts[0].detail.contains("`python`"));
    }

    #[test]
    fn destination_candidate_gates() {
        let root = tmp("dest");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");

        // A destination that is no longer text is simply not a candidate:
        // the unit reads as missing rather than splicing into binary.
        write(&root, "src/lib.rs", b"\xff\xfe\xfd");
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Replace)
            .unwrap();
        assert_eq!(condition_of(&plan), FragmentCondition::Missing);

        // A destination that cannot be read at all is a distinct condition.
        write(&root, "src/lib.rs", TEXT.as_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                root.join("src/lib.rs"),
                std::fs::Permissions::from_mode(0o000),
            )
            .unwrap();
            let plan = store
                .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Replace)
                .unwrap();
            assert_eq!(condition_of(&plan), FragmentCondition::Unreadable);
            std::fs::set_permissions(
                root.join("src/lib.rs"),
                std::fs::Permissions::from_mode(0o644),
            )
            .unwrap();
        }
    }

    #[test]
    fn apply_splices_delete_with_full_provenance() {
        let root = tmp("apply");
        skeleton(&root);
        let mut store = captured(&root, TEXT);
        // A second tracked file, edited WITHOUT capturing: apply must first
        // reconcile that drift into history (the undo reference) before the
        // splice touches anything.
        write(&root, "other.txt", b"other\n");
        flush(&mut store, &root, vec![added(&root, "other.txt")]);
        write(&root, "other.txt", b"other edited\n");

        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Delete)
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        let outcome = store.apply_fragment_restore(&plan, &ignores()).unwrap();
        assert_eq!(outcome.files_written, 1);
        assert_eq!(outcome.written_paths, vec!["src/lib.rs".to_string()]);
        assert!(outcome.pre_restore_capture.is_some(), "{outcome:#?}");
        assert!(outcome.restore_capture.is_some());
        assert!(outcome
            .progress_log
            .iter()
            .any(|line| line.starts_with("captured pre-restore worktree state as ")));
        assert_eq!(
            std::fs::read_to_string(root.join("src/lib.rs")).unwrap(),
            TEXT.replace("fn alpha() {\n    1\n}", ""),
            "the historical extent is spliced out of the live file"
        );

        // The forward capture names every selection that produced it.
        let captures = store.captures(false, None, false, usize::MAX).unwrap();
        let origin = captures[0].origin.as_ref().expect("fragment origin");
        assert!(matches!(origin.kind, OriginKind::FragmentRestore));
        assert_eq!(origin.selections, vec![alpha.id()]);
    }

    #[test]
    fn apply_refuses_overlapping_and_stale_plans() {
        // The tail sits far below the selections so edits there leave the
        // handles' context windows untouched: the binding survives while the
        // plan's file hash moves — exactly what a stale token means.
        let long_text = format!(
            "{TEXT}{}",
            (0..8)
                .map(|i| format!("// comment line {i}\n"))
                .collect::<String>()
        );
        let root = tmp("refuse");
        let mut store = captured(&root, &long_text);
        let line = line_handle_at(&store, "@", "src/lib.rs", "    1");
        let inside = handle_at(&store, "@", "src/lib.rs", "1");

        // Two deletes whose offsets cannot both be trusted in one file.
        let plan = store
            .plan_fragment_restore(&[line.clone(), inside.clone()], FragmentMode::Delete)
            .unwrap();
        assert!(!plan.applicable());
        assert_eq!(plan.conflicts[0].condition, FragmentCondition::Overlap);
        let error = store.apply_fragment_restore(&plan, &ignores()).unwrap_err();
        assert!(error.to_string().contains("overlap"), "{error}");
        assert_eq!(
            std::fs::read_to_string(root.join("src/lib.rs")).unwrap(),
            long_text,
            "a refused plan writes nothing"
        );

        // A live edit between plan and apply stales the token.
        let beta = handle_at(&store, "@", "src/lib.rs", "fn beta() {\n    2\n}");
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&beta), FragmentMode::Delete)
            .unwrap();
        write(
            &root,
            "src/lib.rs",
            long_text
                .replace("comment line 6", "comment SIX")
                .as_bytes(),
        );
        let error = store.apply_fragment_restore(&plan, &ignores()).unwrap_err();
        assert!(
            matches!(error, crate::error::SheafError::RestorePlanStale(_)),
            "{error}"
        );
        assert!(store.pending_restore().is_none(), "nothing was started");
    }

    #[test]
    fn resume_replays_the_intent_and_fails_closed_on_drift() {
        let root = tmp("resume");
        let mut store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn beta() {\n    2\n}");
        // Remove exactly the unit's bytes; its context join survives intact.
        let without = TEXT.replace("fn beta() {\n    2\n}", "");
        write(&root, "src/lib.rs", without.as_bytes());

        // Crash before the first install: resume drives the recorded plan to
        // completion (never re-planning), clears the intent, reports resumed.
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Insert)
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        store.write_fragment_intent(&plan).unwrap();
        assert!(store.pending_restore().is_some());
        let outcome = store
            .resume_restore(&ignores(), false, i64::MAX)
            .unwrap()
            .expect("an outstanding fragment intent is replayed");
        assert!(outcome.resumed);
        assert_eq!(outcome.files_written, 1);
        assert_eq!(
            std::fs::read_to_string(root.join("src/lib.rs")).unwrap(),
            TEXT
        );
        assert!(store.pending_restore().is_none(), "intent is cleared");

        // A replayed splice that already landed is skipped, not rewritten.
        store.write_fragment_intent(&plan).unwrap();
        let outcome = store
            .resume_restore(&ignores(), false, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(outcome.files_written, 0, "already at its planned result");

        // An intent whose token no longer matches its payload is corruption,
        // not a plan to replay.
        store.write_fragment_intent(&plan).unwrap();
        let intent_path = state_dir(&root).join(INTENT_FILE);
        let raw = std::fs::read_to_string(&intent_path).unwrap();
        let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        value["token"] = serde_json::json!("tampered");
        std::fs::write(&intent_path, value.to_string()).unwrap();
        let error = store
            .resume_restore(&ignores(), true, i64::MAX)
            .unwrap_err();
        assert!(
            error.to_string().contains("token does not match"),
            "{error}"
        );

        // A fragment intent with no plan payload cannot drive anything.
        let bare = RestoreIntent {
            token: "t".into(),
            mode: RestoreMode::Fragment,
            scope: vec![],
            target: plan.base.clone(),
            started_ms: chrono::Utc::now().timestamp_millis(),
            fragment: None,
        };
        let error = store.resume_fragment(&bare, &ignores()).unwrap_err();
        assert!(error.to_string().contains("no plan payload"), "{error}");
        store.clear_intent();
    }

    #[test]
    fn fragment_file_bytes_fails_closed_on_any_drift() {
        let root = tmp("bytes");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        // A delete plan carries a real action against the unchanged file.
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&alpha), FragmentMode::Delete)
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        assert_eq!(plan.files.len(), 1);
        let file = &plan.files[0];

        // The planned splice reproduces the planned result hash.
        let bytes = store.fragment_file_bytes(file).unwrap().expect("a splice");
        assert_eq!(
            bytes,
            TEXT.replace("fn alpha() {\n    1\n}", "").as_bytes(),
            "the splice removes the unit"
        );
        assert_eq!(
            blobs::hash_of(&bytes),
            file.result_sha256,
            "planned bytes match the result hash"
        );

        // Live bytes changed since the plan: neither pre-plan nor result.
        let mut drifted = file.clone();
        drifted.file_sha256 = sha256(b"other");
        let error = store.fragment_file_bytes(&drifted).unwrap_err();
        assert!(matches!(
            error,
            crate::error::SheafError::RestorePlanStale(_)
        ));

        // The bytes under an action no longer hash to what the plan splices.
        let mut lying = file.clone();
        lying.actions[0].old_fragment_sha256 = sha256(b"other");
        let error = store.fragment_file_bytes(&lying).unwrap_err();
        assert!(error
            .to_string()
            .contains("no longer what the plan splices"));

        // A splice that would not reproduce the planned result is corruption.
        let mut wrong = file.clone();
        wrong.result_sha256 = sha256(b"other");
        let error = store.fragment_file_bytes(&wrong).unwrap_err();
        assert!(error.to_string().contains("did not reproduce"));

        // Source re-derivation fails closed on every broken dimension.
        let mut view = HistoryView::open(&store.doc).unwrap();
        let mut over = alpha.clone();
        over.range = ByteRange {
            start: 0,
            end: usize::MAX / 2,
        };
        let error = fragment_source_bytes(&mut view, &over).unwrap_err();
        assert!(error.to_string().contains("exceeds"));

        let mut lying_handle = alpha.clone();
        lying_handle.selected_text_sha256 = sha256(b"other");
        let error = fragment_source_bytes(&mut view, &lying_handle).unwrap_err();
        assert!(error.to_string().contains("no longer match"));

        let mut ghost = alpha.clone();
        ghost.historical_path = "src/ghost.rs".into();
        let error = fragment_source_bytes(&mut view, &ghost).unwrap_err();
        assert!(error.to_string().contains("absent or binary"));

        // A delete action's new bytes are empty by construction.
        let delete = FragmentAction {
            selection_id: alpha.id(),
            handle: alpha.clone(),
            kind: FragmentActionKind::Delete,
            range: ByteRange { start: 0, end: 1 },
            old_fragment_sha256: sha256(b"x"),
            new_fragment_sha256: sha256(b""),
            old_bytes: 1,
            new_bytes: 0,
            line_glue: false,
        };
        assert!(action_new_bytes(&mut view, &delete).unwrap().is_empty());
    }
    #[test]
    fn plan_one_rejects_malformed_frontiers_and_unparseable_destinations() {
        let root = tmp("plan-one-errors");
        let store = captured(&root, TEXT);
        let mut view = HistoryView::open(&store.doc).unwrap();
        let head = decode_frontier(&store.resolve("@").unwrap().frontier).unwrap();
        let mut malformed = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        malformed.source_frontier = "not-a-frontier".into();
        assert!(matches!(
            plan_one_selection(&root, &mut view, &head, &malformed, FragmentMode::Replace),
            Planned::Conflict(FragmentConflict {
                condition: FragmentCondition::InvalidSource,
                ..
            })
        ));

        let mut semantic = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        semantic.semantic = Some(SemanticIdentity {
            language: "rust".into(),
            kind: "function".into(),
            qualified_name: "alpha".into(),
            structural_fingerprint: "wrong".into(),
        });
        write(&root, "src/lib.rs", b"fn alpha( {");
        let result = plan_one_selection(&root, &mut view, &head, &semantic, FragmentMode::Replace);
        assert!(matches!(
            result,
            Planned::Conflict(FragmentConflict {
                condition: FragmentCondition::InvalidSource,
                ..
            })
        ));
    }

    #[test]
    fn conflict_description_is_bounded_and_mode_names_are_stable() {
        let root = tmp("description");
        let store = captured(&root, TEXT);
        let plan = store
            .plan_fragment_restore(&[], FragmentMode::Delete)
            .unwrap();
        assert_eq!(plan.mode_as_str(), "delete");
        let conflicts = (0..7)
            .map(|i| FragmentConflict {
                selection_id: format!("selection-{i}"),
                path: None,
                condition: FragmentCondition::Missing,
                candidates: vec![],
                detail: "detail".into(),
            })
            .collect::<Vec<_>>();
        let description = describe_conflicts(&conflicts);
        assert_eq!(description.matches("selection ").count(), 5);
        assert!(!description.contains("selection-5"));
    }
    #[test]
    fn parser_and_mode_helpers_cover_supported_and_unsupported_inputs() {
        assert!(parser_for("rust").is_some());
        assert!(parser_for("python").is_none());
        assert_eq!(
            FragmentMode::parse("replace").unwrap(),
            FragmentMode::Replace
        );
        assert_eq!(FragmentMode::parse("insert").unwrap(), FragmentMode::Insert);
        assert_eq!(FragmentMode::parse("delete").unwrap(), FragmentMode::Delete);
        assert!(FragmentMode::parse("unknown").is_err());
    }
    #[test]
    fn action_new_bytes_reads_source_and_glues_line_insertions() {
        let root = tmp("action-bytes");
        let store = captured(&root, TEXT);
        let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        let mut view = HistoryView::open(&store.doc).unwrap();
        let mut action = FragmentAction {
            selection_id: alpha.id(),
            handle: alpha,
            kind: FragmentActionKind::Insert,
            range: ByteRange { start: 0, end: 0 },
            old_fragment_sha256: sha256(b""),
            new_fragment_sha256: String::new(),
            old_bytes: 0,
            new_bytes: 0,
            line_glue: false,
        };
        let plain = action_new_bytes(&mut view, &action).unwrap();
        assert_eq!(plain, b"fn alpha() {\n    1\n}");
        action.line_glue = true;
        let glued = action_new_bytes(&mut view, &action).unwrap();
        assert_eq!(glued, b"fn alpha() {\n    1\n}\n");
    }

    #[test]
    fn empty_fragment_plan_is_applicable_and_has_no_destinations() {
        let root = tmp("empty-plan");
        let store = captured(&root, TEXT);
        let plan = store
            .plan_fragment_restore(&[], FragmentMode::Replace)
            .unwrap();
        assert!(plan.applicable());
        assert!(plan.files.is_empty());
        assert!(plan.destination_paths().is_empty());
    }
    #[test]
    fn apply_fragment_noop_revalidates_without_writing() {
        let root = tmp("noop-apply");
        let mut store = captured(&root, TEXT);
        let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() {\n    1\n}");
        let plan = store
            .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
            .unwrap();
        assert!(plan.applicable());
        assert!(plan.is_noop());
        let before = std::fs::read(root.join("src/lib.rs")).unwrap();
        let outcome = store.apply_fragment_restore(&plan, &ignores()).unwrap();
        assert_eq!(outcome.files_written, 0);
        assert_eq!(std::fs::read(root.join("src/lib.rs")).unwrap(), before);
        assert!(outcome.restore_capture.is_none());
    }
}

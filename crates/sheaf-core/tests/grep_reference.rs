//! The slow, authoritative occurrence-history reference reducer.
//!
//! This file deliberately shares no code with the production engine: it
//! re-derives every lifecycle decision from raw historical reads
//! (`historical_text_paths` + `historical_path_content` at every capture, no
//! memo, no scope short-circuit, no untouched-path skip) and compares its
//! normalized records against the engine's stream for the acceptance
//! fixtures. If the two disagree, the engine is wrong — the reference is the
//! definition.
//!
//! Scope: the fixtures here cover one current lineage, linear DAG order
//! (fixture ages ascend with commit order, as real wall-clock writes do),
//! and full-retention stores. Retention gaps and branch-qualified identity
//! are exercised by their own fixtures in `grep_anchor.rs`; this reference
//! does not independently re-derive those semantics.

use std::collections::BTreeSet;

use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    Capture, GrepBackfillOptions, GrepMode, GrepQuery, GrepReport, GrepRequest, GrepStreamRecord,
    HistoricalPathContent, LifecycleKind, ProjectStore, SearchBudget, SelectionExtent, StoreLimits,
    TimelineReader,
};

fn skeleton(root: &std::path::Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &std::path::Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 1_000,
        },
    )
    .unwrap()
}

fn write_capture(
    store: &mut ProjectStore,
    root: &std::path::Path,
    events: Vec<EventKind>,
    age: Duration,
) {
    let at = Utc::now() - age;
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: events.into_iter().map(FsEvent::now).collect(),
        })
        .unwrap();
}

fn touch(store: &mut ProjectStore, root: &std::path::Path, rel: &str, text: &str, age: Duration) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    write_capture(
        store,
        root,
        vec![EventKind::Touched { path: path.into() }],
        age,
    );
}

fn rename(store: &mut ProjectStore, root: &std::path::Path, from: &str, to: &str, age: Duration) {
    std::fs::rename(root.join(from), root.join(to)).unwrap();
    write_capture(
        store,
        root,
        vec![EventKind::Renamed {
            from: root.join(from),
            to: root.join(to),
        }],
        age,
    );
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex::encode(digest.finalize())
}

const CONTEXT_BYTES: usize = 64;
const PAD: &str =
    "ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp\n";

// ---------------------------------------------------------------------------
// Reference reducer (independent of the engine)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormRecord {
    capture_id: String,
    kind: LifecycleKind,
    path: String,
    line: usize,
    column: usize,
    start: usize,
    end: usize,
    episode_id: String,
}

#[derive(Debug, Clone)]
struct RefUnit {
    path: String,
    start: usize,
    end: usize,
    ext_start: usize,
    ext_end: usize,
    selected: String,
    before: String,
    after: String,
    line_sha: String,
    episode_id: String,
}

fn context_before(text: &str, at: usize) -> &str {
    let mut start = at.saturating_sub(CONTEXT_BYTES);
    while start < at && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..at]
}

fn context_after(text: &str, at: usize) -> &str {
    let mut end = (at + CONTEXT_BYTES).min(text.len());
    while end > at && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[at..end]
}

/// Line-extent expansion mirroring the documented selection contract: the
/// whole line containing the match.
fn extent_range(text: &str, at: usize, len: usize, extent: SelectionExtent) -> (usize, usize) {
    match extent {
        SelectionExtent::Match => (at, at + len),
        SelectionExtent::Line => {
            let line_start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
            let match_end = at + len;
            let line_end = text[match_end..]
                .find('\n')
                .map(|n| match_end + n)
                .unwrap_or(text.len());
            (line_start, line_end)
        }
        // The reference reducer only covers the two accepted grep extents.
        _ => panic!("reference reducer covers Match and Line extents only"),
    }
}

fn enumerate_units(text: &str, needle: &str, extent: SelectionExtent) -> Vec<(usize, usize)> {
    // (match_start, match_end) for every collapsed occurrence unit.
    let mut out = Vec::new();
    let mut last: Option<(usize, usize)> = None;
    let mut cursor = 0usize;
    while let Some(offset) = text[cursor..].find(needle) {
        let at = cursor + offset;
        let range = extent_range(text, at, needle.len(), extent);
        if last != Some(range) {
            last = Some(range);
            out.push((at, at + needle.len()));
        }
        cursor = at + needle.len();
    }
    out
}

fn line_column(text: &str, at: usize) -> (usize, usize) {
    let line_start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
    let line = text[..line_start].bytes().filter(|b| *b == b'\n').count() + 1;
    let column = text[line_start..at].chars().count() + 1;
    (line, column)
}

fn match_line(text: &str, at: usize) -> &str {
    let line_start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
    let line_end = text[at..].find('\n').map(|n| at + n).unwrap_or(text.len());
    &text[line_start..line_end]
}

fn episode_id(capture_id: &str, path: &str, start: usize, end: usize) -> String {
    let canonical = serde_json::json!({
        "lineage_key": "current",
        "origin_capture_id": capture_id,
        "origin_path": path,
        "origin_start": start,
        "origin_end": end,
    });
    let mut bytes = b"sheaf:grep-episode:v1\0".to_vec();
    bytes.extend(serde_json::to_vec(&canonical).unwrap());
    format!("ep1:{}", &sha256_hex(&bytes)[..16])
}

/// The slow authoritative reduction over the current lineage: every capture,
/// every tracked text path, read straight from history each time.
fn reference_history(
    reader: &TimelineReader,
    needle: &str,
    extent: SelectionExtent,
    every_capture: bool,
) -> Vec<NormRecord> {
    let captures: Vec<Capture> = reader
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .rev()
        .collect();
    let renames = reader.recorded_renames();
    let mut state: Vec<RefUnit> = Vec::new();
    let mut records: Vec<NormRecord> = Vec::new();

    for capture in &captures {
        let touched: BTreeSet<String> = capture
            .paths
            .iter()
            .filter(|p| !p.ends_with('/'))
            .map(|p| p.replace('\\', "/"))
            .collect();
        // Enumerate every unit at this point (the slow part).
        let mut current: Vec<RefUnit> = Vec::new();
        for path in reader.historical_text_paths(&capture.id).unwrap() {
            if let HistoricalPathContent::Text(text) =
                reader.historical_path_content(&capture.id, &path).unwrap()
            {
                for (at, match_end) in enumerate_units(&text, needle, extent) {
                    let (ext_start, ext_end) = extent_range(&text, at, needle.len(), extent);
                    current.push(RefUnit {
                        path: path.clone(),
                        start: at,
                        end: match_end,
                        ext_start,
                        ext_end,
                        selected: sha256_hex(&text.as_bytes()[ext_start..ext_end]),
                        before: sha256_hex(context_before(&text, ext_start).as_bytes()),
                        after: sha256_hex(context_after(&text, ext_end).as_bytes()),
                        line_sha: sha256_hex(match_line(&text, at).as_bytes()),
                        episode_id: episode_id(&capture.id, &path, at, match_end),
                    });
                }
            }
        }
        current.sort_by(|a, b| a.path.cmp(&b.path).then(a.start.cmp(&b.start)));

        let is_baseline = state.is_empty();
        // Every-capture observation includes untouched paths; otherwise they
        // carry forward silently.
        let observe_untouched = every_capture;
        // Partition the prior state: untouched paths carry forward verbatim.
        let mut untouched: Vec<RefUnit> = Vec::new();
        let mut reconcile: Vec<RefUnit> = Vec::new();
        for unit in state {
            if !is_baseline && !observe_untouched && !touched.contains(&unit.path) {
                untouched.push(unit);
            } else {
                reconcile.push(unit);
            }
        }

        let mut claimed = vec![false; current.len()];
        let mut batch: Vec<(u8, NormRecord)> = Vec::new();
        // Correspondence is proven, not raced: phase A computes every
        // prior's compatible-candidate set with no claiming; phase B
        // continues a prior only across a UNIQUE edge (exactly one candidate
        // and that candidate compatible with no other prior). A greedy
        // first-come claim would let iteration order decide identity when
        // two priors match one candidate.
        let compatible = |prev: &RefUnit, candidate: &RefUnit| {
            let path_ok = candidate.path == prev.path
                || renames.iter().any(|(from, to)| {
                    from.replace('\\', "/") == prev.path && to.replace('\\', "/") == candidate.path
                });
            path_ok
                && candidate.selected == prev.selected
                && candidate.before == prev.before
                && candidate.after == prev.after
        };
        let exact_sets: Vec<Vec<usize>> = reconcile
            .iter()
            .map(|prev| {
                current
                    .iter()
                    .enumerate()
                    .filter(|(_, unit)| compatible(prev, unit))
                    .map(|(idx, _)| idx)
                    .collect()
            })
            .collect();
        let mut suitors = vec![0usize; current.len()];
        for exact in &exact_sets {
            for idx in exact {
                suitors[*idx] += 1;
            }
        }
        for (i, prev) in reconcile.iter().enumerate() {
            let path_compatible = |candidate: &RefUnit| {
                candidate.path == prev.path
                    || renames.iter().any(|(from, to)| {
                        from.replace('\\', "/") == prev.path
                            && to.replace('\\', "/") == candidate.path
                    })
            };
            let exact = &exact_sets[i];
            let unique = exact.len() == 1 && suitors[exact[0]] == 1;
            let raw: Vec<usize> = current
                .iter()
                .enumerate()
                .filter(|(_, unit)| path_compatible(unit) && unit.selected == prev.selected)
                .map(|(idx, _)| idx)
                .collect();
            if !unique {
                let kind = if raw.is_empty() {
                    LifecycleKind::Removed
                } else {
                    LifecycleKind::Ambiguous
                };
                let rank = match kind {
                    LifecycleKind::Removed => 0,
                    LifecycleKind::Ambiguous => 1,
                    _ => 2,
                };
                batch.push((
                    rank,
                    NormRecord {
                        capture_id: capture.id.clone(),
                        kind,
                        path: prev.path.clone(),
                        line: 0,
                        column: 0,
                        start: 0,
                        end: 0,
                        episode_id: prev.episode_id.clone(),
                    },
                ));
                continue;
            }
            let idx = exact[0];
            claimed[idx] = true;
            let unit = &current[idx];
            let kind = if unit.path != prev.path {
                LifecycleKind::Renamed
            } else if unit.start != prev.start || unit.end != prev.end {
                LifecycleKind::Relocated
            } else if unit.line_sha != prev.line_sha {
                LifecycleKind::Changed
            } else if every_capture {
                LifecycleKind::Observed
            } else {
                LifecycleKind::Present // sentinel: no record (see below)
            };
            if kind != LifecycleKind::Present {
                // The continued unit keeps the predecessor's episode.
                let episode = prev.episode_id.clone();
                batch.push((
                    2,
                    NormRecord {
                        capture_id: capture.id.clone(),
                        kind,
                        path: unit.path.clone(),
                        line: 0,
                        column: 0,
                        start: unit.ext_start,
                        end: unit.ext_end,
                        episode_id: episode,
                    },
                ));
            }
        }

        // Unclaimed current units on touched paths are introductions; units
        // on untouched paths carry forward silently by construction.
        for (idx, unit) in current.iter().enumerate() {
            if claimed[idx] || (!is_baseline && !touched.contains(&unit.path)) {
                continue;
            }
            batch.push((
                2,
                NormRecord {
                    capture_id: capture.id.clone(),
                    kind: LifecycleKind::Introduced,
                    path: unit.path.clone(),
                    line: 0,
                    column: 0,
                    start: unit.ext_start,
                    end: unit.ext_end,
                    episode_id: unit.episode_id.clone(),
                },
            ));
        }

        // Rebuild state from the SAME proven edges as the records above:
        // only uniquely-matched pairs transfer episode identity, so a
        // contested candidate stays unowned and re-enters state as a fresh
        // introduction (mirroring the engine's ambiguous terminal event).
        let mut owned = vec![None; current.len()];
        for (i, prev) in reconcile.iter().enumerate() {
            let exact = &exact_sets[i];
            if exact.len() == 1 && suitors[exact[0]] == 1 {
                owned[exact[0]] = Some(prev.episode_id.clone());
            }
        }
        let mut next: Vec<RefUnit> = Vec::new();
        for (idx, unit) in current.iter().enumerate() {
            if !is_baseline && !observe_untouched && !touched.contains(&unit.path) {
                continue; // carried below, verbatim
            }
            next.push(RefUnit {
                episode_id: owned[idx]
                    .clone()
                    .unwrap_or_else(|| unit.episode_id.clone()),
                ..unit.clone()
            });
        }
        next.extend(untouched);
        state = next;

        // Fill line/column from the stored content now that records exist.
        for (_, record) in &mut batch {
            if record.kind == LifecycleKind::Removed || record.kind == LifecycleKind::Ambiguous {
                continue;
            }
            if let HistoricalPathContent::Text(text) = reader
                .historical_path_content(&capture.id, &record.path)
                .unwrap()
            {
                for (at, match_end) in enumerate_units(&text, needle, extent) {
                    let (ext_start, ext_end) = extent_range(&text, at, needle.len(), extent);
                    if ext_start == record.start && ext_end == record.end {
                        let (line, column) = line_column(&text, at);
                        record.line = line;
                        record.column = column;
                        let _ = match_end;
                        break;
                    }
                }
            }
        }
        batch.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.path.cmp(&b.1.path))
                .then_with(|| a.1.start.cmp(&b.1.start))
                .then_with(|| a.1.end.cmp(&b.1.end))
        });
        records.extend(batch.into_iter().map(|(_, record)| record));
    }
    records
}

fn request(needle: &str, extent: SelectionExtent, every_capture: bool) -> GrepRequest {
    GrepRequest {
        query: GrepQuery::literal(needle),
        mode: GrepMode::History,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture,
        extent,
        budget: SearchBudget::default(),
        cursor: None,
    }
}

fn production_stream(
    store: &ProjectStore,
    needle: &str,
    extent: SelectionExtent,
    every_capture: bool,
) -> Vec<NormRecord> {
    let mut streamed: Vec<GrepStreamRecord> = Vec::new();
    let mut sink = |record: GrepStreamRecord| streamed.push(record);
    store
        .grep_streaming(
            &request(needle, extent, every_capture),
            &mut Some(&mut sink),
        )
        .unwrap();
    streamed
        .into_iter()
        .map(|record| match record {
            GrepStreamRecord::Hit { hit } => NormRecord {
                capture_id: hit.capture_id,
                kind: hit.kind,
                path: hit.path,
                line: hit.line,
                column: hit.column,
                start: hit.handle.range.start,
                end: hit.handle.range.end,
                episode_id: hit.episode_id.unwrap_or_default(),
            },
            GrepStreamRecord::Event { event } => NormRecord {
                capture_id: event.capture_id,
                kind: event.kind,
                path: event.path.unwrap_or_default(),
                line: 0,
                column: 0,
                start: 0,
                end: 0,
                episode_id: event.episode_id.unwrap_or_default(),
            },
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn lifecycle_store(root: &std::path::Path) -> ProjectStore {
    // The full acceptance vocabulary in one store: multi-occurrence
    // enumeration, context-preserving relocation, rename, context-destroying
    // ambiguity, independent delete/add, and a second path's parallel
    // episodes.
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}fn header() {{}}\nTODO one\nTODO two\n{PAD}"),
        Duration::hours(9),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "TODO independent\n",
        Duration::hours(8),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}{PAD}fn header() {{}}\nTODO one\nTODO two\n{PAD}"),
        Duration::hours(7),
    );
    rename(&mut store, root, "b.rs", "c.rs", Duration::hours(6));
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}{PAD}fn header() {{}}\nTODO one!!\nTODO two\n{PAD}"),
        Duration::hours(5),
    );
    std::fs::write(root.join("a.rs"), "nothing left\n").unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Touched {
            path: root.join("a.rs").into(),
        }],
        Duration::hours(4),
    );
    touch(&mut store, root, "d.rs", "TODO fresh\n", Duration::hours(3));
    store
}

fn assert_matches_reference(
    store: &ProjectStore,
    needle: &str,
    extent: SelectionExtent,
    every: bool,
) {
    let production = production_stream(store, needle, extent, every);
    let reader = TimelineReader::open(store.root()).unwrap();
    let reference = reference_history(&reader, needle, extent, every);
    assert_eq!(
        production, reference,
        "production diverged from the reference reducer for needle={needle:?} extent={extent:?}"
    );
}

#[test]
fn reference_reducer_agrees_on_the_full_lifecycle_fixture() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = lifecycle_store(root);
    assert_matches_reference(&store, "TODO", SelectionExtent::Match, false);
    assert_matches_reference(&store, "TODO", SelectionExtent::Line, false);
    assert_matches_reference(&store, "TODO", SelectionExtent::Match, true);
    assert_matches_reference(&store, "TODO one", SelectionExtent::Match, false);
}

/// The trigram pre-filter may skip a content version only when it
/// provably cannot contain the needle, so an indexed query must produce the
/// exact same records as the unindexed authoritative reference. This builds
/// the trigram index, then re-runs every full-lifecycle assertion with the
/// filter engaged — a hit dropped by an over-eager filter would surface here.
#[test]
fn trigram_indexed_query_equals_the_authoritative_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = lifecycle_store(root);

    // Build the trigram index over the distinct-content corpus.
    let report = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(report.complete);
    assert!(
        report.trigram_index_bytes > 0,
        "the lifecycle fixture must yield a trigram index"
    );

    // Needles of three or more bytes engage the filter; equality must hold.
    assert_matches_reference(&store, "TODO", SelectionExtent::Match, false);
    assert_matches_reference(&store, "TODO", SelectionExtent::Line, false);
    assert_matches_reference(&store, "TODO", SelectionExtent::Match, true);
    assert_matches_reference(&store, "TODO one", SelectionExtent::Match, false);
    // A rare needle present in only one version — the case the filter helps
    // most — must still enumerate exactly.
    assert_matches_reference(&store, "header", SelectionExtent::Match, false);
    // An absent needle whose trigrams are all indexed: the filter proves a
    // no-match; the reference agrees (no records). Keep a small non-ignored
    // acceleration-shape assertion alongside the heavyweight 10k benchmark.
    assert_matches_reference(&store, "quokka", SelectionExtent::Match, false);
    let absent = store
        .grep(&request("quokka", SelectionExtent::Match, false))
        .unwrap();
    assert!(absent.hits.is_empty());
    assert!(
        absent.usage.trigram_skipped > 0,
        "an indexed absent needle must skip at least one content version"
    );
}

/// A content version captured AFTER the trigram index was last built is not
/// covered by that index. The filter must treat such content as a candidate
/// (scan it), never exclude it — otherwise a rare needle introduced in fresh
/// captures would be silently dropped until the next rebuild. This is the
/// stale-index safety that the "covered" set guarantees.
#[test]
fn content_captured_after_index_build_is_never_wrongly_excluded() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    // History with a common token; no rare needle yet.
    touch(
        &mut store,
        root,
        "a.rs",
        "fn common() {}\n",
        Duration::hours(5),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn common() { work() }\n",
        Duration::hours(4),
    );

    // Build the trigram index over exactly this history.
    let report = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(report.trigram_index_bytes > 0);

    // Now capture NEW content containing a rare needle the index has never
    // seen. Capture-time indexing writes its content row, but the trigram
    // index is not rebuilt, so this content is uncovered.
    touch(
        &mut store,
        root,
        "a.rs",
        "fn common() {}\nfn rare_late_marker() {}\n",
        Duration::hours(3),
    );

    // The rare needle must still be found: an uncovered hash is a candidate,
    // scanned exactly, equal to the authoritative reference.
    assert_matches_reference(&store, "rare_late_marker", SelectionExtent::Match, false);
    assert_matches_reference(&store, "common", SelectionExtent::Match, false);
}

/// A multibyte literal must survive the byte-trigram pre-filter: the accented
/// scalar decomposes into byte trigrams like any needle, and equality with
/// the reference holds across edits and enumeration.
#[test]
fn trigram_filter_preserves_multibyte_literals() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "u.rs",
        &format!("{PAD}fn función() {{}} // café\n{PAD}"),
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "u.rs",
        &format!("{PAD}fn función() {{}} // café renamed\ncafé again\n{PAD}"),
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "v.rs",
        "plain ascii only\n",
        Duration::hours(1),
    );

    let report = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(report.trigram_index_bytes > 0);

    assert_matches_reference(&store, "café", SelectionExtent::Match, false);
    assert_matches_reference(&store, "función", SelectionExtent::Match, false);
    assert_matches_reference(&store, "café", SelectionExtent::Line, false);
}

#[test]
fn reference_reducer_agrees_on_paged_and_dense_pagination() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "dense.rs",
        &format!("{PAD}a a a a a a a a a a\nb a a a a a\n{PAD}"),
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "dense.rs",
        &format!("{PAD}a a a a a a a a a a\nb a a a a a\n{PAD}extra\n"),
        Duration::hours(2),
    );

    assert_matches_reference(&store, "a", SelectionExtent::Match, false);

    // Paged concatenation must equal the unbounded stream exactly.
    let oldest_first: Vec<String> = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .rev()
        .collect();
    let index_of: std::collections::HashMap<String, usize> = oldest_first
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();
    let capture_order = |id: &str| index_of.get(id).copied().unwrap_or(usize::MAX);
    let mut paged: Vec<NormRecord> = Vec::new();
    let mut cursor = None;
    loop {
        let mut req = request("a", SelectionExtent::Match, false);
        req.budget.max_results = 3;
        req.cursor = cursor;
        let page = store.grep(&req).unwrap();
        paged.extend(production_page(&page, &capture_order));
        match page.cursor.clone() {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let direct = production_stream(&store, "a", SelectionExtent::Match, false);
    assert_eq!(paged, direct);
}

/// Cursor pagination returns identical results whether the
/// daemon's cursor-state cache is warm across pages or lost between them. A
/// warm run reuses cached anchor state; a cold run reopens the store before
/// each page — no warm state survives, so every page falls back to the
/// authoritative suppressed replay. Both must equal the unbounded stream.
#[test]
fn paged_results_survive_cursor_cache_eviction_and_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    {
        let mut store = open(root);
        // A history where a needle appears, changes, disappears, and returns
        // across several captures, so paging crosses real lifecycle
        // transitions whose state the cursor must carry.
        touch(&mut store, root, "a.rs", "needle one\n", Duration::hours(8));
        touch(
            &mut store,
            root,
            "a.rs",
            "needle one\nneedle two\n",
            Duration::hours(7),
        );
        touch(
            &mut store,
            root,
            "b.rs",
            "needle three\n",
            Duration::hours(6),
        );
        touch(&mut store, root, "a.rs", "gone\n", Duration::hours(5));
        touch(
            &mut store,
            root,
            "a.rs",
            "needle back\n",
            Duration::hours(4),
        );
        touch(
            &mut store,
            root,
            "b.rs",
            "needle three still\n",
            Duration::hours(3),
        );
        // Build the trigram index too, so the indexed path is exercised.
        store
            .grep_cache_backfill(GrepBackfillOptions::default())
            .unwrap();
    }

    let order_store = open(root);
    let oldest_first: Vec<String> = order_store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .rev()
        .collect();
    let index_of: std::collections::HashMap<String, usize> = oldest_first
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();
    let capture_order = |id: &str| index_of.get(id).copied().unwrap_or(usize::MAX);

    let direct = production_stream(&order_store, "needle", SelectionExtent::Match, false);

    // Warm run: one resident store serves every page, reusing cursor state.
    let mut warm_paged: Vec<NormRecord> = Vec::new();
    let mut cursor = None;
    let mut warm_pages = 0u32;
    let mut warm_replayed_after_first = 0u64;
    loop {
        let mut req = request("needle", SelectionExtent::Match, false);
        req.budget.max_results = 1000;
        req.budget.max_materialized_bytes = 1;
        req.cursor = cursor;
        let page = order_store.grep(&req).unwrap();
        if warm_pages > 0 {
            warm_replayed_after_first += page.usage.cursor_replayed_captures;
        }
        warm_pages += 1;
        warm_paged.extend(production_page(&page, &capture_order));
        match page.cursor.clone() {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        warm_paged, direct,
        "warm paged run must equal the direct stream"
    );
    // The whole point of the cursor-state cache: warm resumes replay nothing.
    assert_eq!(
        warm_replayed_after_first, 0,
        "warm resumes must reuse cached cursor state, not replay ({warm_replayed_after_first} replayed)"
    );

    // Cold run: reopen the store before every page, so no warm cursor state
    // survives and each resume falls back to authoritative replay.
    let mut cold_paged: Vec<NormRecord> = Vec::new();
    let mut cursor = None;
    let mut cold_pages = 0u32;
    let mut cold_replayed_after_first = 0u64;
    loop {
        let fresh = open(root);
        let mut req = request("needle", SelectionExtent::Match, false);
        req.budget.max_results = 1000;
        req.budget.max_materialized_bytes = 1;
        req.cursor = cursor;
        let page = fresh.grep(&req).unwrap();
        if cold_pages > 0 {
            cold_replayed_after_first += page.usage.cursor_replayed_captures;
        }
        cold_pages += 1;
        cold_paged.extend(production_page(&page, &capture_order));
        match page.cursor.clone() {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        cold_paged, direct,
        "cold paged run (cache evicted/restarted each page) must equal the direct stream"
    );
    // The cold run has no warm state, so it must fall back to replay — proving
    // the replay path still works and is what the warm run avoided.
    assert!(
        cold_replayed_after_first > 0,
        "cold resumes must fall back to authoritative replay"
    );
}

/// Regression for a review finding: a page that aborts its budget INSIDE a
/// capture (after `state.present` was taken but before it was restored) must
/// not persist a torn lineage state in the cursor-state cache. The abort
/// samplers fire on 1024-unit strides, so a capture holding well over 1024
/// live/scanned occurrences of a common needle plus a byte budget just above
/// the previous capture's materialization forces a mid-capture abort. Warm
/// paging must equal the cold replay and the direct unbounded stream — a torn
/// cached state would drop or mis-episode the carried occurrences.
#[test]
fn mid_capture_budget_abort_does_not_cache_torn_state() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    // First capture: >1024 occurrences of the needle on distinct lines so
    // they are distinct occurrences the reducer must carry forward.
    let mut v1 = String::new();
    for i in 0..1500 {
        v1.push_str(&format!("needle line {i}\n"));
    }
    touch(&mut store, root, "big.rs", &v1, Duration::hours(3));
    // Second capture: keep them all present (append a line so the file
    // changes) so the reducer carries >1024 live priors into a capture whose
    // reconciliation the budget can abort mid-way.
    let mut v2 = v1.clone();
    v2.push_str("tail change\n");
    touch(&mut store, root, "big.rs", &v2, Duration::hours(2));

    store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();

    let order_store = open(root);
    let oldest_first: Vec<String> = order_store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .rev()
        .collect();
    let index_of: std::collections::HashMap<String, usize> = oldest_first
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();
    let capture_order = |id: &str| index_of.get(id).copied().unwrap_or(usize::MAX);
    // Just above capture 1's materialization: the loop-top check admits capture
    // 2, whose read pushes usage over budget, and the 1024-unit sampler must
    // abort after `state.present` was taken. Unlimited results ensure the byte
    // budget — not a mid-capture result cursor — is what stops the page.
    let byte_budget = v1.len() as u64 + 1;

    let page_run = |store: &ProjectStore| -> (Vec<NormRecord>, bool) {
        let mut out: Vec<NormRecord> = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        let mut saw_mid_capture_boundary = false;
        loop {
            let mut req = request("needle", SelectionExtent::Match, false);
            req.budget.max_materialized_bytes = byte_budget;
            req.budget.max_results = usize::MAX;
            // Coverage instrumentation makes this intentionally dense fixture
            // much slower; keep the test focused on the byte-boundary abort.
            req.budget.max_elapsed_ms = 60_000;
            req.cursor = cursor;
            let page = store.grep(&req).unwrap();
            if page
                .cursor
                .as_ref()
                .is_some_and(|next| next.resume_capture_id.is_none())
                && page.usage.materialized_bytes > byte_budget
            {
                saw_mid_capture_boundary = true;
            }
            out.extend(production_page(&page, &capture_order));
            pages += 1;
            assert!(pages < 100_000, "pagination must terminate");
            match page.cursor.clone() {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        (out, saw_mid_capture_boundary)
    };

    // Warm run: one resident store, cursor-state cache active across the abort.
    let (mut warm, warm_saw_mid_capture_boundary) = page_run(&order_store);
    assert!(
        warm_saw_mid_capture_boundary,
        "fixture must exercise a capture-boundary abort after materializing capture 2"
    );
    // Cold run: a fresh store per page, so every resume falls back to the
    // authoritative suppressed replay (no cursor cache). If the F3 restore is
    // wrong, the warm run caches a torn lineage and diverges from this replay.
    let mut cold = {
        let mut out: Vec<NormRecord> = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let fresh = open(root);
            let mut req = request("needle", SelectionExtent::Match, false);
            req.budget.max_materialized_bytes = byte_budget;
            req.budget.max_results = usize::MAX;
            // Coverage instrumentation makes this intentionally dense fixture
            // much slower; keep the test focused on the byte-boundary abort.
            req.budget.max_elapsed_ms = 60_000;
            req.cursor = cursor;
            let page = fresh.grep(&req).unwrap();
            out.extend(production_page(&page, &capture_order));
            pages += 1;
            assert!(pages < 100_000, "pagination must terminate");
            match page.cursor.clone() {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        out
    };
    // Cache hits change byte charging and therefore page boundaries. Rebuild
    // the global normative order before comparison so the assertion tests the
    // record stream rather than incidental page partitioning.
    let rank = |record: &NormRecord| match record.kind {
        LifecycleKind::Removed => 0u8,
        LifecycleKind::Ambiguous => 1,
        _ => 2,
    };
    let globally_sort = |records: &mut Vec<NormRecord>| {
        records.sort_by(|a, b| {
            capture_order(&a.capture_id)
                .cmp(&capture_order(&b.capture_id))
                .then(rank(a).cmp(&rank(b)))
                .then(a.path.cmp(&b.path))
                .then(a.start.cmp(&b.start))
                .then(a.end.cmp(&b.end))
        });
    };
    globally_sort(&mut warm);
    globally_sort(&mut cold);
    // The direct unbounded stream is the normative reference both paths must
    // reproduce; comparing only warm against cold would pass on a shared defect.
    let mut direct = {
        let mut req = request("needle", SelectionExtent::Match, false);
        req.budget.max_results = usize::MAX;
        req.budget.max_elapsed_ms = 60_000;
        let page = order_store.grep(&req).unwrap();
        production_page(&page, &capture_order)
    };
    globally_sort(&mut direct);
    assert_eq!(warm, direct, "warm paging must equal the direct stream");
    assert_eq!(
        cold, direct,
        "cold replay paging must equal the direct stream"
    );
    let mismatch = warm.iter().zip(&cold).position(|(a, b)| a != b);
    assert!(
        mismatch.is_none(),
        "warm cursor-cache resume diverged from replay at index {:?}: warm={:?} cold={:?}",
        mismatch,
        mismatch.map(|i| &warm[i]),
        mismatch.map(|i| &cold[i]),
    );
}

/// A branch lineage must not enter `seen_lineages` until its first capture is
/// fully processed. Otherwise a byte-budget abort inside that first capture
/// caches a baseline that never existed at the cursor anchor and warm resume
/// diverges from the authoritative replay.
#[test]
fn branch_first_capture_abort_does_not_cache_premature_seen_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    // Dense shared base: its first visit cannot stop (no page progress yet),
    // but its first visit under the second lineage can stop after 1024 units.
    let mut base_text = String::new();
    for i in 0..1500 {
        base_text.push_str(&format!("needle branch base {i}\n"));
    }
    touch(&mut store, root, "dense.rs", &base_text, Duration::hours(4));
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    touch(
        &mut store,
        root,
        "branch.txt",
        "abandoned\n",
        Duration::hours(3),
    );
    store.checkout_for_branch(&base.frontier).unwrap();
    touch(
        &mut store,
        root,
        "branch.txt",
        "current\n",
        Duration::hours(2),
    );
    store
        .grep_cache_backfill(GrepBackfillOptions {
            all: true,
            ..Default::default()
        })
        .unwrap();

    let warm_store = open(root);
    let oldest_first: Vec<String> = warm_store
        .captures(true, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .rev()
        .collect();
    let index_of: std::collections::HashMap<String, usize> = oldest_first
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();
    let capture_order = |id: &str| index_of.get(id).copied().unwrap_or(usize::MAX);

    let run_warm = |store: &ProjectStore| -> Vec<NormRecord> {
        let mut out = Vec::new();
        let mut cursor = None;
        for _ in 0..100_000 {
            let mut req = request("needle", SelectionExtent::Match, true);
            req.budget.max_materialized_bytes = 1;
            req.cursor = cursor;
            let page = store.grep(&req).unwrap();
            out.extend(production_page(&page, &capture_order));
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => return out,
            }
        }
        panic!("warm pagination must terminate")
    };
    let mut warm = run_warm(&warm_store);

    let mut cold = Vec::new();
    let mut cursor = None;
    for _ in 0..100_000 {
        let fresh = open(root);
        let mut req = request("needle", SelectionExtent::Match, true);
        req.budget.max_materialized_bytes = 1;
        req.cursor = cursor;
        let page = fresh.grep(&req).unwrap();
        cold.extend(production_page(&page, &capture_order));
        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let rank = |record: &NormRecord| match record.kind {
        LifecycleKind::Removed => 0u8,
        LifecycleKind::Ambiguous => 1,
        _ => 2,
    };
    let globally_sort = |records: &mut Vec<NormRecord>| {
        records.sort_by(|a, b| {
            capture_order(&a.capture_id)
                .cmp(&capture_order(&b.capture_id))
                .then(a.episode_id.cmp(&b.episode_id))
                .then(rank(a).cmp(&rank(b)))
                .then(a.path.cmp(&b.path))
                .then(a.start.cmp(&b.start))
                .then(a.end.cmp(&b.end))
        });
    };
    globally_sort(&mut warm);
    globally_sort(&mut cold);
    assert_eq!(
        warm, cold,
        "warm branch-baseline resume must equal replay after a mid-capture abort"
    );
}

/// A head reposition changes `current` versus branch lineage attribution while
/// leaving capture IDs intact. The old cursor anchor may still resolve, so the
/// resident reduction cache must be explicitly invalidated and the next page
/// must fall back to authoritative replay.
#[test]
fn head_reposition_invalidates_cursor_state_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "needle base\n",
        Duration::hours(4),
    );
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    touch(
        &mut store,
        root,
        "f.rs",
        "needle abandoned\n",
        Duration::hours(3),
    );
    let abandoned = store.captures(false, None, false, 1).unwrap()[0].clone();
    store.checkout_for_branch(&base.frontier).unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "needle current\n",
        Duration::hours(2),
    );
    store
        .grep_cache_backfill(GrepBackfillOptions {
            all: true,
            ..Default::default()
        })
        .unwrap();

    let mut first_req = request("needle", SelectionExtent::Match, true);
    first_req.budget.max_materialized_bytes = 1;
    first_req.budget.max_results = usize::MAX;
    let first = store.grep(&first_req).unwrap();
    let cursor = first
        .cursor
        .expect("byte-limited page has a boundary cursor");
    assert!(cursor.resume_capture_id.is_none());

    // Before the head moves, the same next page hits the resident state.
    let mut next_req = request("needle", SelectionExtent::Match, true);
    next_req.budget.max_materialized_bytes = 1;
    next_req.budget.max_results = usize::MAX;
    next_req.cursor = Some(cursor.clone());
    let warm = store.grep(&next_req).unwrap();
    assert_eq!(warm.usage.cursor_replayed_captures, 0);

    // Reattribute the old branch as current. The anchor still exists in the
    // all-lineage walk, but its cached state names the old lineage layout.
    store.checkout_for_branch(&abandoned.frontier).unwrap();
    let replayed = store.grep(&next_req).unwrap();
    assert!(
        replayed.usage.cursor_replayed_captures > 0,
        "head reposition must force authoritative replay instead of reusing lineage-attributed state"
    );
}

/// Normalize an already-buffered page by re-deriving the total order from
/// the report arrays (records arrive rank-sorted per capture already).
fn production_page(report: &GrepReport, capture_order: &dyn Fn(&str) -> usize) -> Vec<NormRecord> {
    let mut hits: Vec<NormRecord> = report
        .hits
        .iter()
        .map(|hit| NormRecord {
            capture_id: hit.capture_id.clone(),
            kind: hit.kind,
            path: hit.path.clone(),
            line: hit.line,
            column: hit.column,
            start: hit.handle.range.start,
            end: hit.handle.range.end,
            episode_id: hit.episode_id.clone().unwrap_or_default(),
        })
        .collect();
    let events: Vec<NormRecord> = report
        .events
        .iter()
        .map(|event| NormRecord {
            capture_id: event.capture_id.clone(),
            kind: event.kind,
            path: event.path.clone().unwrap_or_default(),
            line: 0,
            column: 0,
            start: 0,
            end: 0,
            episode_id: event.episode_id.clone().unwrap_or_default(),
        })
        .collect();
    hits.extend(events);
    // The buffered wire shape carries hits and events in separate vectors;
    // reassemble the normative total order — (capture walk position, kind
    // rank, path, byte range) — so paged comparison cannot pass while
    // pagination loses capture chronology or rank interleaving. Walk
    // position comes from the capture list, not the opaque ids.
    let rank = |record: &NormRecord| match record.kind {
        LifecycleKind::Removed => 0u8,
        LifecycleKind::Ambiguous => 1,
        _ => 2,
    };
    let mut merged = hits;
    merged.sort_by(|a, b| {
        capture_order(&a.capture_id)
            .cmp(&capture_order(&b.capture_id))
            .then(rank(a).cmp(&rank(b)))
            .then(a.path.cmp(&b.path))
            .then(a.start.cmp(&b.start))
            .then(a.end.cmp(&b.end))
    });
    merged
}

#[test]
fn reference_reducer_agrees_on_utf8_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "u.txt",
        &format!("{PAD}αα TODO ββ TODO γγ\nδ TODO ε\n{PAD}"),
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "u.txt",
        &format!("{PAD}{PAD}αα TODO ββ TODO γγ\nδ TODO ε\n{PAD}"),
        Duration::hours(2),
    );
    assert_matches_reference(&store, "TODO", SelectionExtent::Match, false);
}

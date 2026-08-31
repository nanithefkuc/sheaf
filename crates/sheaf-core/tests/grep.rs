//! Timeline grep fixtures: lifecycle transitions, branches, retention,
//! cursors, and daemon/degraded parity.

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    doctor, gc_run_store, retention_mark, GrepBackfillOptions, GrepReport, GrepRequest,
    LifecycleKind, ProjectStore, SearchBudget, SearchCursor, SearchStopReason, SelectionExtent,
    StoreLimits, TimelineReader,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 1_000,
        },
    )
    .unwrap()
}

fn write_capture(store: &mut ProjectStore, root: &Path, events: Vec<EventKind>, age: Duration) {
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

fn touch(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, age: Duration) {
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

fn literal(text: &str) -> GrepRequest {
    GrepRequest {
        query: sheaf_core::store::GrepQuery::literal(text),
        mode: sheaf_core::store::GrepMode::History,
        at: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: SelectionExtent::Match,
        budget: SearchBudget::default(),
        cursor: None,
        anchor: None,
    }
}

fn kinds(report: &GrepReport) -> Vec<LifecycleKind> {
    report.hits.iter().map(|h| h.kind).collect()
}

#[test]
fn one_capture_with_many_paths_creates_one_historical_fork() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    std::fs::write(root.join("a.rs"), "fn target() {}\n").unwrap();
    std::fs::write(root.join("b.rs"), "fn other() {}\n").unwrap();
    write_capture(
        &mut store,
        root,
        vec![
            EventKind::Touched {
                path: root.join("a.rs").into(),
            },
            EventKind::Touched {
                path: root.join("b.rs").into(),
            },
        ],
        Duration::hours(1),
    );

    drop(store);
    std::fs::remove_dir_all(root.join(".sheaf/state/cache/grep-v1")).unwrap();
    let reader = TimelineReader::open(root).unwrap();
    let report = reader.grep(&literal("fn target")).unwrap();
    assert_eq!(report.usage.historical_path_reads, 2);
    assert_eq!(report.usage.historical_forks, 1);
    assert_eq!(report.usage.historical_cache_hits, 0);

    let warm = reader.grep(&literal("fn target")).unwrap();
    assert_eq!(warm.hits, report.hits);
    assert_eq!(warm.events, report.events);
    assert_eq!(warm.usage.historical_path_reads, 2);
    // The warm cross-query scan cache now answers both visits from
    // the prior query's scan outcome, so the second query neither reloads
    // content (no memory/disk content-cache hits) nor re-scans it — both
    // visits are content-dedup answers instead. Strictly less work than the
    // earlier "content cache covers the reads" path it supersedes.
    assert_eq!(warm.usage.historical_cache_hits, 0);
    assert_eq!(warm.usage.historical_disk_cache_hits, 0);
    assert_eq!(warm.usage.content_dedup_hits, 2);
    // One fork: the baseline capture's full-tree listing. Content
    // reads are answered from the warm scan cache before any fork per point.
    assert_eq!(warm.usage.historical_forks, 1);
}

#[test]
fn lifecycle_transitions_collapse_and_report_disappearance() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    // Introduce, change, unrelated capture (no change), rename, remove,
    // reintroduce.
    touch(
        &mut store,
        root,
        "a.rs",
        "fn target() { 1 }\n",
        Duration::hours(9),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn target() { 2 }\n",
        Duration::hours(8),
    );
    touch(
        &mut store,
        root,
        "other.rs",
        "unrelated\n",
        Duration::hours(7),
    );
    // Rename a.rs -> b.rs carrying identical content.
    std::fs::rename(root.join("a.rs"), root.join("b.rs")).unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Renamed {
            from: root.join("a.rs"),
            to: root.join("b.rs"),
        }],
        Duration::hours(6),
    );
    // Remove the function entirely.
    touch(
        &mut store,
        root,
        "b.rs",
        "fn other() {}\n",
        Duration::hours(5),
    );
    // Reintroduce it in a fresh file.
    touch(
        &mut store,
        root,
        "c.rs",
        "fn target() { 3 }\n",
        Duration::hours(4),
    );

    let report = store.grep(&literal("fn target")).unwrap();
    assert!(report.complete);
    assert_eq!(
        kinds(&report),
        [
            LifecycleKind::Introduced,
            LifecycleKind::Introduced,
            LifecycleKind::Renamed,
            LifecycleKind::Introduced,
        ]
    );
    // Editing inside the exact 64-byte context ends continuity, so the old
    // episode becomes ambiguous and the rewritten literal starts fresh.
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        [LifecycleKind::Ambiguous, LifecycleKind::Removed]
    );
    assert!(report.events[1].last_present_handle_id.is_some());
    // Handles round-trip and identify their capture.
    for hit in &report.hits {
        assert_eq!(hit.handle.id(), hit.handle_id);
        assert!(hit.preview.contains("fn target"));
    }
}

#[test]
fn path_scope_and_rename_follow() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "old.rs",
        "fn keep() {}\n",
        Duration::hours(3),
    );
    std::fs::rename(root.join("old.rs"), root.join("new.rs")).unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Renamed {
            from: root.join("old.rs"),
            to: root.join("new.rs"),
        }],
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "new.rs",
        "fn keep() { changed() }\n",
        Duration::hours(1),
    );

    // Without follow, scoping to new.rs sees only post-rename history.
    let mut req = literal("fn keep");
    req.path = Some("new.rs".into());
    let no_follow = store.grep(&req).unwrap();
    assert!(no_follow.hits.iter().all(|h| h.path == "new.rs"));

    // With follow, the historical name is searched too.
    req.follow = true;
    let follow = store.grep(&req).unwrap();
    assert!(follow.hits.iter().any(|h| h.path == "new.rs"));
    assert!(follow.hits.len() >= no_follow.hits.len());
    assert!(follow
        .hits
        .iter()
        .any(|h| matches!(h.kind, LifecycleKind::Introduced)));
}

#[test]
fn all_branches_distinguishes_lineage_membership() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { base }\n",
        Duration::hours(4),
    );
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { abandoned }\n",
        Duration::hours(3),
    );
    store.checkout_for_branch(&base.frontier).unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { current }\n",
        Duration::hours(2),
    );

    let default = store.grep(&literal("fn probe")).unwrap();
    assert!(default.hits.iter().all(|h| h.on_current));

    let mut req = literal("fn probe");
    req.all = true;
    let all = store.grep(&req).unwrap();
    assert!(all.hits.iter().any(|h| !h.on_current));
    assert!(all.hits.iter().any(|h| h.on_current));
    // The two divergent futures carry distinct lineage ids.
    let branch_lineages: std::collections::BTreeSet<_> = all
        .hits
        .iter()
        .filter(|h| !h.on_current)
        .map(|h| h.lineage_id.clone())
        .collect();
    assert!(!branch_lineages.is_empty());
}

#[test]
fn budget_exhaustion_returns_resumable_cursor_with_full_coverage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    for i in 0..6 {
        touch(
            &mut store,
            root,
            "f.rs",
            &format!("fn probe() {{ {i} }}\n"),
            Duration::hours(10 - i as i64),
        );
    }

    let unbounded = store.grep(&literal("fn probe")).unwrap();
    assert!(unbounded.complete);
    let total = unbounded.hits.len();
    assert!(total >= 6);

    let mut req = literal("fn probe");
    req.budget = SearchBudget {
        max_results: 2,
        max_materialized_bytes: u64::MAX,
        max_elapsed_ms: u64::MAX,
    };
    let mut collected: Vec<(String, LifecycleKind)> = Vec::new();
    let mut cursor = None;
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 20, "cursor pagination did not terminate");
        req.cursor = cursor.clone();
        let page = store.grep(&req).unwrap();
        collected.extend(page.hits.iter().map(|h| (h.capture_id.clone(), h.kind)));
        if page.complete {
            break;
        }
        assert_eq!(page.stop_reason, Some(SearchStopReason::ResultLimit));
        cursor = page.cursor.clone();
        assert!(cursor.is_some());
    }
    // Paged output must equal the unbounded run in BOTH capture order and
    // lifecycle kind: a resumed page must not re-emit Introduced for a Changed.
    let full: Vec<(String, LifecycleKind)> = unbounded
        .hits
        .iter()
        .map(|h| (h.capture_id.clone(), h.kind))
        .collect();
    assert_eq!(collected, full);
}

#[test]
fn byte_limited_absent_query_advances_every_cursor_page() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    for i in 0..6 {
        touch(
            &mut store,
            root,
            "f.rs",
            &format!("fn unrelated_{i}() {{}}\n"),
            Duration::hours(10 - i as i64),
        );
    }

    let mut req = literal("never-present");
    req.path = Some("f.rs".into());
    req.budget = SearchBudget {
        max_results: usize::MAX,
        max_materialized_bytes: 1,
        max_elapsed_ms: u64::MAX,
    };
    let mut seen_cursors = std::collections::BTreeSet::new();
    for _ in 0..10 {
        let page = store.grep(&req).unwrap();
        assert!(page.hits.is_empty());
        if page.complete {
            assert!(!seen_cursors.is_empty());
            return;
        }
        assert_eq!(page.stop_reason, Some(SearchStopReason::ByteLimit));
        let cursor = page.cursor.expect("truncated page cursor");
        assert!(
            seen_cursors.insert(cursor.after_capture_id.clone()),
            "cursor did not advance: {}",
            cursor.after_capture_id
        );
        req.cursor = Some(cursor);
    }
    panic!("byte-limited pagination did not terminate");
}

#[test]
fn byte_limited_multi_candidate_points_still_advance() {
    // An unscoped query reads every touched path at each capture, so a capture
    // that touches several files is a multi-candidate point. With a byte budget
    // smaller than one file, the first read of a fresh point exhausts it; the
    // page must still fully process that point and advance, never re-anchor at
    // the previous capture and loop.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    for i in 0..5 {
        std::fs::write(root.join("a.rs"), format!("fn a_{i}() {{}}\n")).unwrap();
        std::fs::write(root.join("b.rs"), format!("fn b_{i}() {{}}\n")).unwrap();
        std::fs::write(root.join("c.rs"), format!("fn c_{i}() {{}}\n")).unwrap();
        let at = Utc::now() - Duration::hours(10 - i as i64);
        store
            .apply_batch(&Batch {
                root: root.to_path_buf(),
                started_at: at,
                flushed_at: at,
                events: ["a.rs", "b.rs", "c.rs"]
                    .into_iter()
                    .map(|rel| {
                        FsEvent::now(EventKind::Touched {
                            path: root.join(rel).into(),
                        })
                    })
                    .collect(),
            })
            .unwrap();
    }

    let mut req = literal("never-present");
    req.budget = SearchBudget {
        max_results: usize::MAX,
        max_materialized_bytes: 1,
        max_elapsed_ms: u64::MAX,
    };
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..30 {
        let page = store.grep(&req).unwrap();
        assert!(page.hits.is_empty());
        if page.complete {
            assert!(!seen.is_empty(), "a truncated page must precede completion");
            return;
        }
        let cursor = page.cursor.expect("truncated page cursor");
        assert!(
            seen.insert(cursor.after_capture_id.clone()),
            "cursor did not advance on a multi-candidate point: {}",
            cursor.after_capture_id
        );
        req.cursor = Some(cursor);
    }
    panic!("multi-candidate byte-limited pagination did not terminate");
}

#[test]
fn cursor_prefix_resolution_fails_closed_when_it_matches_no_capture() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    for i in 0..3 {
        touch(
            &mut store,
            root,
            "f.rs",
            &format!("fn probe() {{ {i} }}\n"),
            Duration::hours(3 - i as i64),
        );
    }
    let fingerprint = store.grep(&literal("fn probe")).unwrap().query_fingerprint;

    // A full-length cursor id that names no capture in this walk fails closed
    // instead of silently restarting from the oldest capture.
    let mut absent = literal("fn probe");
    absent.cursor = Some(SearchCursor {
        query_fingerprint: fingerprint.clone(),
        after_capture_id: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into(),
        resume_capture_id: None,
        record_index: 0,
        path_index: 0,
        match_index: 0,
    });
    assert_eq!(
        store.grep(&absent).unwrap_err().code(),
        "state.bad_cursor",
        "a cursor capture outside the walk must be rejected"
    );

    // A short non-matching prefix (< the 6-char prefix floor, matched only by
    // exact equality) also fails closed rather than resuming from the start.
    let mut short = literal("fn probe");
    short.cursor = Some(SearchCursor {
        query_fingerprint: fingerprint,
        after_capture_id: "zzz".into(),
        resume_capture_id: None,
        record_index: 0,
        path_index: 0,
        match_index: 0,
    });
    assert_eq!(
        store.grep(&short).unwrap_err().code(),
        "state.bad_cursor",
        "a non-matching cursor prefix must be rejected"
    );
}

#[test]
fn cursor_from_a_differently_scoped_query_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() {}\n",
        Duration::hours(1),
    );
    let capture = store.captures(false, None, false, 1).unwrap()[0].clone();

    let mut req = literal("fn probe");
    req.cursor = Some(SearchCursor {
        query_fingerprint: "literal:other|extent=match|all=0|every=0|from=|to=|path=|follow=0"
            .into(),
        after_capture_id: capture.id.clone(),
        resume_capture_id: None,
        record_index: 0,
        path_index: 0,
        match_index: 0,
    });
    let err = store.grep(&req).unwrap_err();
    assert_eq!(err.code(), "state.bad_cursor");
}

#[test]
fn edit_inside_context_starts_a_fresh_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() {}\nfn other() { 1 }\n",
        Duration::hours(2),
    );
    // The edit is outside the matched line but inside its exact 64-byte
    // after-context, so continuity must fail closed rather than guess.
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() {}\nfn other() { 999 }\n",
        Duration::hours(1),
    );
    let report = store.grep(&literal("fn probe")).unwrap();
    assert_eq!(
        report.hits.iter().map(|h| h.kind).collect::<Vec<_>>(),
        [LifecycleKind::Introduced, LifecycleKind::Introduced]
    );
    assert_eq!(report.events.len(), 1);
    assert_eq!(report.events[0].kind, LifecycleKind::Ambiguous);
}

#[test]
fn pruned_intervals_are_reported_without_fabricated_content() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 3,
        },
    )
    .unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { a }\n",
        Duration::hours(5),
    );
    let victim = store.captures(false, None, false, 1).unwrap()[0].id.clone();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { b }\n",
        Duration::hours(4),
    );
    store.create_checkpoint("keep", None).unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { c }\n",
        Duration::hours(1),
    );

    // Explicitly mark and collect the oldest capture, creating a tombstone.
    retention_mark(&mut store, &victim[..12]).unwrap();
    gc_run_store(&mut store, true).unwrap();
    // A retention trim sweeps the derived grep cache to exactly the retained
    // captures rather than wiping it wholesale: whatever cache
    // state remains must not name the collected capture's frontier, and a
    // query still runs correctly over the trimmed timeline. The collected
    // capture no longer resolves, so its frontier is not among the survivors.
    let cache_dir = root.join(".sheaf/state/cache/grep-v1");
    let surviving_frontiers: std::collections::BTreeSet<String> = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|c| c.frontier)
        .collect();
    if let Ok(mappings) = std::fs::read_to_string(cache_dir.join("mappings.jsonl")) {
        for line in mappings.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(frontier) = v.get("frontier").and_then(|f| f.as_str()) {
                    assert!(
                        surviving_frontiers.contains(frontier),
                        "swept cache holds only retained-capture frontiers"
                    );
                }
            }
        }
    }

    let report = store.grep(&literal("fn probe")).unwrap();
    assert!(report.pruned_intervals >= 1);
    let gap = report
        .events
        .iter()
        .find(|e| e.kind == LifecycleKind::RetentionGap)
        .expect("a retention gap event");
    // A gap fabricates no handle and no preview content.
    assert!(gap.last_present_handle_id.is_none());
    assert!(gap.path.is_none());
}

#[test]
fn degraded_reader_matches_the_live_store() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    let live = store.grep(&literal("fn probe")).unwrap();
    assert_eq!(live.usage.historical_forks, 1);
    assert!(live.usage.historical_cache_hits >= 2);
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let mut degraded = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(degraded.usage.historical_forks, 1);
    assert!(degraded.usage.historical_disk_cache_hits >= 2);
    // Only the degraded marker and timing differ.
    assert!(degraded.degraded);
    assert!(!live.degraded);
    degraded.degraded = false;
    let mut live_norm = live.clone();
    live_norm.usage = degraded.usage.clone();
    degraded.usage = degraded.usage.clone();
    let mut degraded_norm = degraded.clone();
    live_norm.usage.elapsed_ms = 0;
    degraded_norm.usage.elapsed_ms = 0;
    assert_eq!(live_norm.hits, degraded_norm.hits);
    assert_eq!(live_norm.events, degraded_norm.events);
}

#[test]
fn binary_rows_survive_capture_time_index_and_cold_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let binary = root.join("blob.bin");
    std::fs::write(&binary, [0u8, 159, 146, 150]).unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Added {
            path: binary.clone(),
        }],
        Duration::hours(1),
    );

    let mut req = literal("probe");
    req.path = Some("blob.bin".into());
    let live = store.grep(&req).unwrap();
    assert_eq!(live.skipped_binary, 1);
    assert_eq!(live.usage.historical_forks, 0);
    assert_eq!(live.usage.historical_cache_hits, 1);
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let cold = reader.grep(&req).unwrap();
    assert_eq!(cold.skipped_binary, 1);
    assert_eq!(cold.usage.historical_forks, 0);
    assert_eq!(cold.usage.historical_disk_cache_hits, 1);
}

#[test]
fn corrupt_persistent_content_falls_back_to_authoritative_history() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() {}\n",
        Duration::hours(1),
    );
    let expected = store.grep(&literal("fn probe")).unwrap();
    drop(store);

    let content_dir = root.join(".sheaf/state/cache/grep-v1/content");
    let content = std::fs::read_dir(&content_dir)
        .unwrap()
        .next()
        .expect("one cached content blob")
        .unwrap()
        .path();
    std::fs::write(content, b"not-zstd").unwrap();

    let reader = TimelineReader::open(root).unwrap();
    let actual = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(actual.hits, expected.hits);
    assert_eq!(actual.events, expected.events);
    assert!(actual.usage.historical_forks >= 1);
    assert_eq!(actual.usage.historical_disk_cache_hits, 0);
}

#[test]
fn trigram_rebuild_never_covers_a_hash_from_unverified_blob_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn unique_probe_marker() {}\n",
        Duration::hours(1),
    );
    let expected = store.grep(&literal("unique_probe_marker")).unwrap();
    assert!(!expected.hits.is_empty());
    drop(store);

    // Replace the content-addressed blob with valid zstd holding bytes that do
    // not match its filename hash. A rebuild must leave that hash uncovered;
    // otherwise postings extracted from these wrong bytes could prove the
    // authoritative content absent and short-circuit the hash-verifying read.
    let content_dir = root.join(".sheaf/state/cache/grep-v1/content");
    let content = std::fs::read_dir(&content_dir)
        .unwrap()
        .next()
        .expect("one cached content blob")
        .unwrap()
        .path();
    let wrong = zstd::stream::encode_all(std::io::Cursor::new(b"decodable but unrelated bytes"), 3)
        .unwrap();
    std::fs::write(content, wrong).unwrap();

    let reopened = open(root);
    let backfill = reopened
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert_eq!(
        backfill.trigram_index_bytes, 0,
        "the only blob failed identity verification, so no hash is covered"
    );
    let actual = reopened.grep(&literal("unique_probe_marker")).unwrap();
    assert_eq!(actual.hits, expected.hits);
    assert_eq!(actual.events, expected.events);
    assert_eq!(actual.usage.trigram_skipped, 0);
    assert!(actual.usage.historical_forks >= 1);
}

#[test]
fn retention_survivors_unchanged_and_binaries_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 3,
        },
    )
    .unwrap();
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { old }\n",
        Duration::hours(5),
    );
    store.create_checkpoint("keep", None).unwrap();
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { mid }\n",
        Duration::hours(4),
    );
    // A binary file that must be skipped, not decoded.
    let bin = root.join("blob.bin");
    std::fs::write(&bin, [0u8, 159, 146, 150, b'p', b'r', b'o', b'b', b'e']).unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Added { path: bin.clone() }],
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { new }\n",
        Duration::hours(1),
    );

    let before = store.grep(&literal("fn probe")).unwrap();
    assert!(before.skipped_binary >= 1 || before.hits.iter().all(|h| h.path == "a.rs"));

    config::set_retention_expiry(root, "2h").unwrap();
    gc_run_store(&mut store, true).unwrap();
    let after = store.grep(&literal("fn probe")).unwrap();
    // The surviving hits keep byte-identical previews.
    let surviving: Vec<_> = before
        .hits
        .iter()
        .filter(|h| after.hits.iter().any(|a| a.capture_id == h.capture_id))
        .collect();
    assert!(!surviving.is_empty());
    for hit in surviving {
        let matched = after
            .hits
            .iter()
            .find(|a| a.capture_id == hit.capture_id)
            .unwrap();
        assert_eq!(matched.preview, hit.preview);
        assert_eq!(matched.handle_id, hit.handle_id);
    }
}

// ------------------------------------------------------------- cache backfill

fn cache_dir(root: &Path) -> std::path::PathBuf {
    root.join(".sheaf/state/cache/grep-v1")
}

fn mappings_file(root: &Path) -> std::path::PathBuf {
    cache_dir(root).join("mappings.jsonl")
}

fn watermark_file(root: &Path) -> std::path::PathBuf {
    cache_dir(root).join("watermark.json")
}

fn mapping_lines(root: &Path) -> Vec<String> {
    let raw = std::fs::read_to_string(mappings_file(root)).unwrap();
    raw.split('\n')
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

fn drop_last_mapping_line(root: &Path) -> String {
    // Crash window: content published, its mapping line never durable.
    let mut lines = mapping_lines(root);
    let removed = lines.pop().expect("at least one mapping line");
    let mut raw = lines.join("\n");
    if !raw.is_empty() {
        raw.push('\n');
    }
    std::fs::write(mappings_file(root), raw).unwrap();
    removed
}

fn tear_mappings_tail(root: &Path) {
    // Crash window: the append tore mid-line, no trailing newline.
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(mappings_file(root))
        .unwrap();
    file.write_all(br#"{"v":1,"frontier":"deadbeef"#).unwrap();
}

/// A store whose captures predate the cache: backfill indexes everything
/// once, then never writes a row again.
#[test]
fn backfill_is_idempotent_and_covers_the_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "fn probe() { 2 }\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 3 }\n",
        Duration::hours(1),
    );
    let total = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .len();
    drop(store);

    // Simulate a store that predates the cache: no directory at all.
    std::fs::remove_dir_all(cache_dir(root)).unwrap();
    let store = open(root);
    let first = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(first.complete);
    assert_eq!(first.captures_indexed, total);
    assert!(first.rows_written >= total);
    let through = first.watermark.as_ref().expect("watermark").clone();
    assert_eq!(through.captures_indexed, total);
    assert!(watermark_file(root).is_file());

    // Second run: pure no-op, same chain, no new rows.
    let second = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(second.complete);
    assert_eq!(second.captures_indexed, 0);
    assert_eq!(second.rows_written, 0);
    assert_eq!(second.captures_skipped, total);
    let again = second.watermark.as_ref().expect("watermark");
    assert_eq!(again.through_capture_id, through.through_capture_id);
    assert_eq!(again.generation, through.generation);

    // A cold degraded query over the backfilled cache forks nothing. The
    // unscoped engine enumerates every occurrence: a.rs is introduced, then
    // b.rs is introduced in a fresh capture that does not touch a.rs (the
    // occurrence a path-intersection skip would have silently dropped), then
    // a.rs changes — three hits, each read served from disk.
    drop(store);
    let reader = TimelineReader::open(root).unwrap();
    let report = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(report.usage.historical_forks, 1);
    assert!(
        report.usage.historical_disk_cache_hits >= 2,
        "expected disk-cache hits, got {}",
        report.usage.historical_disk_cache_hits
    );
    // Three occurrences are enumerated across the two files (the b.rs one is
    // the occurrence a path-intersection skip used to drop). The a.rs value
    // edit sits inside the match's exact context window, so it fails closed to
    // a fresh episode rather than a Changed continuation — conservative by
    // design; the point here is completeness and zero forks.
    assert_eq!(report.hits.len(), 3);
    let paths: std::collections::BTreeSet<_> =
        report.hits.iter().map(|h| h.path.as_str()).collect();
    assert!(paths.contains("a.rs") && paths.contains("b.rs"));
}

/// Rebuild wipes damage (torn lines, orphaned and missing content) and
/// bumps the generation; queries equal the authoritative control before
/// and after.
#[test]
fn rebuild_repairs_damage_and_bumps_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    drop(store);

    // Authoritative control: no cache at all, pure point reads.
    std::fs::remove_dir_all(cache_dir(root)).unwrap();
    let control = TimelineReader::open(root)
        .unwrap()
        .grep(&literal("fn probe"))
        .unwrap();
    assert!(control.complete);

    let store = open(root);
    store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    let generation = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap()
        .watermark
        .unwrap()
        .generation;

    // Damage: a torn tail, an orphan content blob, a missing content blob.
    drop(store);
    tear_mappings_tail(root);
    let content_dir = cache_dir(root).join("content");
    let victim = std::fs::read_dir(&content_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::copy(&victim, content_dir.join("0000orphan.zst")).unwrap();
    std::fs::remove_file(&victim).unwrap();

    // Damage never fails the integrity sweep; it is advisory only.
    let report = doctor(root).unwrap();
    assert!(report.ok, "cache damage must not fail store integrity");
    let cache_check = report
        .checks
        .iter()
        .find(|c| c.name == "grep_cache")
        .expect("doctor reports the derived cache");
    assert!(cache_check.ok);
    assert!(
        cache_check.detail.contains("torn"),
        "{}",
        cache_check.detail
    );
    assert!(
        cache_check.detail.contains("missing content"),
        "{}",
        cache_check.detail
    );
    assert!(
        cache_check.detail.contains("orphan"),
        "{}",
        cache_check.detail
    );

    // Damaged queries stay exact (fallback), then rebuild restores a
    // clean cache with a bumped generation.
    let damaged = TimelineReader::open(root)
        .unwrap()
        .grep(&literal("fn probe"))
        .unwrap();
    assert!(damaged.complete);
    assert_eq!(damaged.hits, control.hits);
    assert_eq!(damaged.events, control.events);

    let store = open(root);
    let rebuilt = store
        .grep_cache_backfill(GrepBackfillOptions {
            rebuild: true,
            ..Default::default()
        })
        .unwrap();
    assert!(rebuilt.complete);
    assert!(rebuilt.rebuilt);
    assert_eq!(rebuilt.generation, generation + 1);
    drop(store);

    let report = doctor(root).unwrap();
    assert!(report.ok);
    let cache_check = report
        .checks
        .iter()
        .find(|c| c.name == "grep_cache")
        .unwrap();
    assert!(
        !cache_check.detail.contains("torn"),
        "{}",
        cache_check.detail
    );
    assert!(
        !cache_check.detail.contains("missing content"),
        "{}",
        cache_check.detail
    );
    assert!(
        !cache_check.detail.contains("orphan"),
        "{}",
        cache_check.detail
    );

    let reader = TimelineReader::open(root).unwrap();
    let after = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(after.hits, control.hits);
    assert_eq!(after.events, control.events);
    assert_eq!(after.usage.historical_forks, 1);
}

/// A torn mappings tail must not swallow the first record appended after
/// it: the append separates with a newline, and a fresh load finds the
/// republished row.
#[test]
fn torn_mappings_tail_separates_fresh_records() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    drop(store);

    std::fs::remove_dir_all(cache_dir(root)).unwrap();
    let store = open(root);
    store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    drop(store);

    // Torn tail, then lose the newest mapping line: backfill republishes
    // it after the torn fragment. (Lose the line first — dropping "the
    // last line" after tearing would only remove the fragment itself.)
    drop_last_mapping_line(root);
    tear_mappings_tail(root);

    let store = open(root);
    let repaired = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(repaired.complete);
    assert_eq!(repaired.captures_indexed, 1);
    drop(store);

    // The torn fragment is inert junk (skipped on load) and the fresh
    // record resolves: a cold reader finds the row on disk.
    let reader = TimelineReader::open(root).unwrap();
    let report = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(report.usage.historical_forks, 1);
    assert!(report.usage.historical_disk_cache_hits >= 2);
    let raw = std::fs::read_to_string(mappings_file(root)).unwrap();
    assert!(
        raw.contains(r#"{"v":1,"frontier":"deadbeef"#),
        "torn fragment preserved verbatim"
    );
    // The record after the fragment parses (its line is intact).
    let lines: Vec<&str> = raw.trim_end_matches('\n').split('\n').collect();
    let last = lines.last().unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(last).is_ok());
}

/// The three crash windows — content without its mapping, mappings
/// without the watermark, and a torn tail — can never turn a query
/// incomplete or wrong.
#[test]
fn crash_windows_keep_queries_exact_and_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    drop(store);

    std::fs::remove_dir_all(cache_dir(root)).unwrap();
    let control = TimelineReader::open(root)
        .unwrap()
        .grep(&literal("fn probe"))
        .unwrap();
    assert!(control.complete);
    assert_eq!(control.hits.len(), 2);

    for damage in ["missing-mapping", "missing-watermark", "torn-tail"] {
        // Tolerant wipe: the degraded reader never recreates the cache, so
        // the directory may already be gone after the control query.
        let _ = std::fs::remove_dir_all(cache_dir(root));
        let store = open(root);
        store
            .grep_cache_backfill(GrepBackfillOptions::default())
            .unwrap();
        drop(store);
        match damage {
            "missing-mapping" => {
                drop_last_mapping_line(root);
            }
            "missing-watermark" => {
                std::fs::remove_file(watermark_file(root)).unwrap();
            }
            "torn-tail" => tear_mappings_tail(root),
            _ => unreachable!(),
        }
        let reader = TimelineReader::open(root).unwrap();
        let report = reader.grep(&literal("fn probe")).unwrap();
        assert!(report.complete, "{damage}: no false incompleteness");
        assert_eq!(report.hits, control.hits, "{damage}: hits stay exact");
        assert_eq!(report.events, control.events, "{damage}: events stay exact");
        assert!(doctor(root).unwrap().ok, "{damage}: integrity stays ok");
    }
}

/// The coverage watermark cannot start mid-history: a store that lost its
/// watermark and gains a capture stays unwatermarked until a backfill
/// re-verifies the whole chain — while a fresh store's genesis capture
/// starts the chain naturally.
#[test]
fn watermark_chain_cannot_start_mid_history() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);

    // Fresh store: capture-time indexing chains from genesis.
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(2),
    );
    let wm = std::fs::read_to_string(watermark_file(root)).expect("genesis advances watermark");
    assert!(wm.contains("\"captures_indexed\":1"), "{wm}");
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    let wm = std::fs::read_to_string(watermark_file(root)).unwrap();
    assert!(wm.contains("\"captures_indexed\":2"), "{wm}");

    // Lose the watermark entirely (crash before its write). A kill -9
    // also loses the in-memory chain, so the store must be reopened
    // before the new capture — otherwise the live chain would extend
    // from memory even though the durable marker is gone.
    std::fs::remove_file(watermark_file(root)).unwrap();
    drop(store);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "b.rs",
        "fn probe() { 3 }\n",
        Duration::minutes(30),
    );
    assert!(
        !watermark_file(root).exists(),
        "mid-history capture must not restart the chain"
    );

    // Backfill re-verifies rows and rebuilds the chain end to end.
    let report = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(report.complete);
    let wm = report.watermark.expect("backfill restores the chain");
    assert_eq!(wm.captures_indexed, 3);
    drop(store);
    assert!(watermark_file(root).is_file());
}

/// Garbage cache files degrade to authoritative reads and an advisory
/// doctor note — never a failed integrity sweep, never wrong results.
#[test]
fn garbage_cache_never_breaks_the_store() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(1),
    );
    drop(store);

    std::fs::remove_dir_all(cache_dir(root)).unwrap();
    let control = TimelineReader::open(root)
        .unwrap()
        .grep(&literal("fn probe"))
        .unwrap();

    std::fs::create_dir_all(cache_dir(root)).unwrap();
    std::fs::write(mappings_file(root), "not json at all\n\x00garbage\n").unwrap();
    std::fs::write(watermark_file(root), "{invalid").unwrap();

    let reader = TimelineReader::open(root).unwrap();
    let report = reader.grep(&literal("fn probe")).unwrap();
    assert!(report.complete);
    assert_eq!(report.hits, control.hits);
    assert_eq!(report.events, control.events);

    let integrity = doctor(root).unwrap();
    assert!(integrity.ok, "cache garbage is advisory only");
    let cache_check = integrity
        .checks
        .iter()
        .find(|c| c.name == "grep_cache")
        .unwrap();
    assert!(
        cache_check.detail.contains("torn"),
        "{}",
        cache_check.detail
    );

    // And rebuild from the garbage state restores a working cache.
    let store = open(root);
    let rebuilt = store
        .grep_cache_backfill(GrepBackfillOptions {
            rebuild: true,
            ..Default::default()
        })
        .unwrap();
    assert!(rebuilt.complete);
    drop(store);
    let after = TimelineReader::open(root)
        .unwrap()
        .grep(&literal("fn probe"))
        .unwrap();
    assert_eq!(after.hits, control.hits);
    assert_eq!(after.usage.historical_forks, 1);
}

/// Streaming delivery (proto 1.5 core seam): the sink sees every record
/// exactly once, in the same order the buffered report stores them, and
/// the streaming report equals the buffered one.
#[test]
fn streaming_records_match_the_buffered_report() {
    use sheaf_core::store::GrepStreamRecord;

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 1 }\n",
        Duration::hours(4),
    );
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 2 }\n",
        Duration::hours(3),
    );
    touch(&mut store, root, "a.rs", "other\n", Duration::hours(2));
    touch(
        &mut store,
        root,
        "a.rs",
        "fn probe() { 3 }\n",
        Duration::hours(1),
    );

    let buffered = store.grep(&literal("fn probe")).unwrap();
    let mut streamed: Vec<GrepStreamRecord> = Vec::new();
    let mut sink = |record: GrepStreamRecord| streamed.push(record);
    let report = store
        .grep_streaming(&literal("fn probe"), &mut Some(&mut sink))
        .unwrap();
    assert_eq!(report.hits, buffered.hits);
    assert_eq!(report.events, buffered.events);
    assert_eq!(report.complete, buffered.complete);

    // Multiset and order: hits arrive in report order, events in theirs.
    let hits: Vec<_> = streamed
        .iter()
        .filter_map(|r| match r {
            GrepStreamRecord::Hit { hit } => Some((**hit).clone()),
            _ => None,
        })
        .collect();
    let events: Vec<_> = streamed
        .iter()
        .filter_map(|r| match r {
            GrepStreamRecord::Event { event } => Some(event.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(hits, report.hits);
    assert_eq!(events, report.events);
    assert_eq!(streamed.len(), report.hits.len() + report.events.len());

    // Degraded twin: same records, degraded marker set.
    drop(store);
    let reader = TimelineReader::open(root).unwrap();
    let mut degraded_streamed = Vec::new();
    let mut sink2 = |record: GrepStreamRecord| degraded_streamed.push(record);
    let degraded = reader
        .grep_streaming(&literal("fn probe"), &mut Some(&mut sink2))
        .unwrap();
    assert!(degraded.degraded);
    assert_eq!(degraded.hits, report.hits);
    assert_eq!(degraded_streamed.len(), streamed.len());
}

/// Distinct-content scan: a rename carrying identical content
/// revisits already-scanned bytes under a new path and frontier — the
/// visit is answered by the content memo (no decompression, no fork,
/// no re-search), while the transition still reads as `Moved` exactly
/// like the per-visit engine.
#[test]
fn distinct_content_versions_are_scanned_once() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let v1 = "fn probe() { 1 }\n";
    touch(&mut store, root, "a.rs", v1, Duration::hours(4));
    // Rename carrying identical content: b.rs at the new frontier is the
    // same content version a.rs already scanned.
    std::fs::rename(root.join("a.rs"), root.join("b.rs")).unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Renamed {
            from: root.join("a.rs"),
            to: root.join("b.rs"),
        }],
        Duration::hours(3),
    );
    // Remove the needle entirely.
    touch(&mut store, root, "b.rs", "other\n", Duration::hours(2));

    let report = store.grep(&literal("fn probe")).unwrap();
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Renamed]
    );
    assert_eq!(report.events.len(), 1);
    assert_eq!(report.events[0].kind, LifecycleKind::Removed);
    // The rename capture's read of b.rs was a memo answer: same content
    // identity the introduction already scanned.
    assert_eq!(
        report.usage.content_dedup_hits, 1,
        "expected the rename revisit to be a memo answer (reads={})",
        report.usage.historical_path_reads
    );
    // Every emitted hit/event record counts against the result budget.
    assert_eq!(report.usage.results, 3);
    // The renamed hit kept the exact occurrence episode on the new path.
    assert_eq!(report.hits[1].path, "b.rs");
}

/// Handles rebuilt from memoized scans are exactly the handles
/// `from_source` would build: same range, same context hashes, and the
/// removed event references the present hit's capture-bound handle id.
#[test]
fn handles_rebuilt_from_dedup_match_direct_construction() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let v1 = "fn probe() { 1 }\n";
    touch(&mut store, root, "a.rs", v1, Duration::hours(4));
    touch(&mut store, root, "a.rs", "other\n", Duration::hours(3));

    let report = store.grep(&literal("fn probe")).unwrap();
    assert_eq!(kinds(&report), [LifecycleKind::Introduced]);
    let removed = &report.events[0];
    assert_eq!(removed.kind, LifecycleKind::Removed);
    // The last-present handle id is the introduced hit's id — the state
    // carried the capture-bound handle built from the memoized scan.
    assert_eq!(
        removed.last_present_handle_id.as_deref(),
        Some(report.hits[0].handle_id.as_str())
    );

    // Byte-for-byte handle equality against direct construction over the
    // same text, including context hashes the memo must have reproduced.
    let direct = sheaf_core::store::SelectionHandle::from_source(
        report.hits[0].handle.source_frontier.clone(),
        Some(report.hits[0].capture_id.clone()),
        report.hits[0].path.clone(),
        sheaf_core::store::SelectionExtent::Match,
        report.hits[0].handle.range,
        v1,
        report.query_fingerprint.clone(),
        None,
    )
    .unwrap();
    assert_eq!(report.hits[0].handle, direct);
    // And the selected text still validates against the handle.
    report.hits[0]
        .handle
        .validate_selected_text("fn probe")
        .unwrap();
}

#[test]
fn point_discovery_returns_every_occurrence_with_coordinates() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "hé needle needle\nneedle\n",
        Duration::hours(1),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "before needle after\n",
        Duration::minutes(30),
    );

    let mut req = literal("needle");
    req.mode = sheaf_core::store::GrepMode::Point;
    let report = store.grep(&req).unwrap();
    assert!(report.complete);
    assert_eq!(report.hits.len(), 4);
    assert!(report.events.is_empty());
    assert!(report
        .hits
        .iter()
        .all(|hit| hit.kind == LifecycleKind::Present));
    let coords: Vec<_> = report
        .hits
        .iter()
        .map(|hit| (hit.path.as_str(), hit.line, hit.column))
        .collect();
    assert_eq!(
        coords,
        [
            ("a.rs", 1, 4),
            ("a.rs", 1, 11),
            ("a.rs", 2, 1),
            ("b.rs", 1, 8),
        ]
    );
    let occurrence_ids: std::collections::BTreeSet<_> =
        report.hits.iter().map(|hit| &hit.occurrence_id).collect();
    assert_eq!(occurrence_ids.len(), 4);
    for hit in &report.hits {
        assert_eq!(hit.handle.id(), hit.handle_id);
        assert!(hit.episode_id.is_none());
    }
}

#[test]
fn history_tracks_each_occurrence_as_its_own_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "needle one\nneedle two\n",
        Duration::hours(2),
    );
    touch(&mut store, root, "a.rs", "gone\n", Duration::hours(1));

    let report = store.grep(&literal("needle")).unwrap();
    assert_eq!(report.hits.len(), 2);
    assert!(report
        .hits
        .iter()
        .all(|hit| hit.kind == LifecycleKind::Introduced));
    let episodes: std::collections::BTreeSet<_> = report
        .hits
        .iter()
        .map(|hit| hit.episode_id.as_deref().expect("history episode"))
        .collect();
    assert_eq!(episodes.len(), 2);
    assert_eq!(report.events.len(), 2);
    assert!(report
        .events
        .iter()
        .all(|event| event.kind == LifecycleKind::Removed));
    assert_eq!(report.usage.results, 4);
}

#[test]
fn unscoped_history_enumerates_a_new_file_while_another_is_tracked() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    // Track an occurrence in a.rs, then introduce one in a brand-new file
    // WITHOUT touching a.rs. The new occurrence must still be enumerated.
    touch(
        &mut store,
        root,
        "a.rs",
        "needle here\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "needle there\n",
        Duration::hours(1),
    );

    let report = store.grep(&literal("needle")).unwrap();
    let paths: std::collections::BTreeSet<_> =
        report.hits.iter().map(|hit| hit.path.as_str()).collect();
    assert!(
        paths.contains("a.rs") && paths.contains("b.rs"),
        "both occurrences must appear; got {paths:?}"
    );
    assert!(report
        .hits
        .iter()
        .all(|hit| hit.kind == LifecycleKind::Introduced));
}

#[test]
fn point_cursor_rejects_a_moved_discovery_point() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "needle needle needle\n",
        Duration::hours(2),
    );

    let mut req = literal("needle");
    req.mode = sheaf_core::store::GrepMode::Point;
    req.budget.max_results = 1;
    let first = store.grep(&req).unwrap();
    let cursor = first.cursor.clone().expect("partial point cursor");

    // `@` advances (a new capture lands). Resuming the default-@ cursor must
    // fail closed rather than silently restart against the new capture.
    touch(&mut store, root, "c.rs", "unrelated\n", Duration::hours(1));
    req.cursor = Some(cursor);
    assert_eq!(
        store.grep(&req).unwrap_err().code(),
        "state.bad_cursor",
        "a point cursor whose discovery capture moved must be rejected"
    );
}

#[test]
fn zero_max_results_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(&mut store, root, "a.rs", "needle\n", Duration::hours(1));
    let mut req = literal("needle");
    req.budget.max_results = 0;
    assert_eq!(store.grep(&req).unwrap_err().code(), "bad.params");
}

#[test]
fn history_retention_gap_sits_chronologically_and_ends_the_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 3,
        },
    )
    .unwrap();
    touch(&mut store, root, "f.rs", "needle a\n", Duration::hours(5));
    let victim = store.captures(false, None, false, 1).unwrap()[0].id.clone();
    touch(&mut store, root, "f.rs", "needle b\n", Duration::hours(4));
    store.create_checkpoint("keep", None).unwrap();
    touch(&mut store, root, "f.rs", "needle c\n", Duration::hours(1));

    retention_mark(&mut store, &victim[..12]).unwrap();
    gc_run_store(&mut store, true).unwrap();

    let report = store.grep(&literal("needle")).unwrap();
    // A retention gap appears at its chronological position: before the
    // surviving reintroduction, whose occurrence begins a fresh episode.
    let gap_pos = report
        .events
        .iter()
        .position(|event| event.kind == LifecycleKind::RetentionGap)
        .expect("a retention gap event");
    assert!(report.events[gap_pos].path.is_none());
    assert!(report.events[gap_pos].last_present_handle_id.is_none());
    // A mainline prefix trim is attributed to the current lineage, not a
    // phantom pruned branch.
    assert_eq!(report.events[gap_pos].lineage_id, "current");
    assert!(report.events[gap_pos].on_current);
    // The gap terminates continuity: the post-gap match is an Introduced
    // episode, never a Changed continuation across the missing history.
    assert!(report
        .hits
        .iter()
        .any(|hit| hit.kind == LifecycleKind::Introduced
            && hit.timestamp_ms > report.events[gap_pos].timestamp_ms));
    assert!(report.pruned_intervals >= 1);
}

#[test]
fn point_discovery_pages_inside_one_capture_by_record_index() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "needle needle needle needle needle\n",
        Duration::hours(1),
    );

    let mut req = literal("needle");
    req.mode = sheaf_core::store::GrepMode::Point;
    req.budget.max_results = 2;
    let first = store.grep(&req).unwrap();
    assert!(!first.complete);
    assert_eq!(first.hits.len(), 2);
    let cursor = first.cursor.clone().expect("partial point cursor");
    assert_eq!(cursor.record_index, 2);
    assert!(cursor.resume_capture_id.is_some());

    req.cursor = Some(cursor);
    let second = store.grep(&req).unwrap();
    assert!(!second.complete);
    assert_eq!(second.hits.len(), 2);
    req.cursor = second.cursor.clone();
    let third = store.grep(&req).unwrap();
    assert!(third.complete);
    assert_eq!(third.hits.len(), 1);

    let ids: std::collections::BTreeSet<_> = first
        .hits
        .iter()
        .chain(&second.hits)
        .chain(&third.hits)
        .map(|hit| hit.occurrence_id.clone())
        .collect();
    assert_eq!(ids.len(), 5);
}

#[test]
fn history_resume_token_from_a_midas_capture_page_does_not_duplicate() {
    // A first page that truncates MID-capture returns the two-part
    // RESUME:INDEX cursor shape (after=`@before-first`, resume=<capture>).
    // Resuming from it must suppress everything before the resume point —
    // the records of that same capture's earlier batch included — so paged
    // concatenation equals the unbounded stream exactly.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "a.rs",
        "needle needle needle needle needle\n",
        Duration::hours(2),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "needle needle needle\n",
        Duration::hours(1),
    );

    let full = store.grep(&literal("needle")).unwrap();
    assert!(full.complete);
    assert_eq!(full.hits.len() + full.events.len(), 8);

    let mut req = literal("needle");
    req.budget.max_results = 3;
    let mut collected = Vec::new();
    let mut cursor = None;
    loop {
        let is_first_page = cursor.is_none();
        req.cursor = cursor;
        let page = store.grep(&req).unwrap();
        assert!(page.hits.len() + page.events.len() <= 3);
        collected.extend(page.hits.iter().map(|h| h.occurrence_id.clone()));
        collected.extend(
            page.events
                .iter()
                .filter_map(|e| e.last_present_handle_id.clone()),
        );
        match page.cursor.clone() {
            Some(next) => {
                if is_first_page {
                    // The first page truncated inside its first capture:
                    // the two-part shape, no `after` anchor.
                    assert_eq!(next.after_capture_id, "@before-first");
                    assert!(next.resume_capture_id.is_some());
                }
                cursor = Some(next);
            }
            None => {
                assert!(page.complete);
                break;
            }
        }
    }
    assert_eq!(
        collected.len(),
        8,
        "no record may repeat or vanish across pages"
    );
    let unique: std::collections::BTreeSet<_> = collected.iter().collect();
    assert_eq!(unique.len(), 8, "every record identity is distinct");
}

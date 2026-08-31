//! Occurrence-anchor fixtures: single-occurrence history grammar,
//! episode identity purity, lifecycle vocabulary, record order, and the
//! line-extent collapse contract.

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    GrepAnchor, GrepHit, GrepMode, GrepQuery, GrepReport, GrepRequest, LifecycleKind, ProjectStore,
    SearchBudget, SelectionExtent, StoreLimits, TimelineReader,
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

fn rename(store: &mut ProjectStore, root: &Path, from: &str, to: &str, age: Duration) {
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

fn history(text: &str) -> GrepRequest {
    GrepRequest {
        query: GrepQuery::literal(text),
        mode: GrepMode::History,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: SelectionExtent::Match,
        budget: SearchBudget::default(),
        cursor: None,
    }
}

fn point(text: &str) -> GrepRequest {
    GrepRequest {
        mode: GrepMode::Point,
        ..history(text)
    }
}

fn hits(report: &GrepReport) -> Vec<(String, LifecycleKind, usize, usize, String)> {
    report
        .hits
        .iter()
        .map(|hit| {
            (
                hit.path.clone(),
                hit.kind,
                hit.line,
                hit.column,
                hit.episode_id.clone().unwrap_or_default(),
            )
        })
        .collect()
}

fn kinds(report: &GrepReport) -> Vec<LifecycleKind> {
    report.hits.iter().map(|h| h.kind).collect()
}

/// The three-act anchor fixture: an occurrence is introduced, relocated by a
/// context-preserving insert far above it, then becomes ambiguous when its
/// immediate context is rewritten. A second path carries an independent
/// occurrence so a correct anchor filters it out entirely.
fn anchored_store(root: &Path) -> ProjectStore {
    skeleton(root);
    let mut store = open(root);
    // Line 3 holds the anchored occurrence, with >64-byte padding on both
    // sides so a later insert above shifts its range without touching its
    // context bytes.
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}fn header() {{}}\nTODO anchor me\n{PAD}"),
        Duration::hours(9),
    );
    touch(
        &mut store,
        root,
        "b.rs",
        "TODO independent\n",
        Duration::hours(8),
    );
    // Insert one >64-byte line above: same bytes, same window context, new
    // byte range and line -> relocated.
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}{PAD}fn header() {{}}\nTODO anchor me\n{PAD}"),
        Duration::hours(7),
    );
    // Rewrite the surrounding context within the 64-byte window: exact
    // rebinding is impossible while the literal still exists -> ambiguous.
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{PAD}{PAD}fn header() {{}}\nTODO anchor me!!\n{PAD}"),
        Duration::hours(6),
    );
    store
}

/// Capture ids oldest-first (the store's `captures` walks newest-first).
fn capture_ids(store: &ProjectStore) -> Vec<String> {
    store
        .captures(false, None, false, 100)
        .unwrap()
        .into_iter()
        .map(|capture| capture.id)
        .rev()
        .collect()
}

/// A line of padding longer than the 64-byte context window, so inserting one
/// of these above an occurrence leaves its before/after context bytes
/// identical and proves a relocation.
const PAD: &str =
    "ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp\n";

#[test]
fn coordinate_anchor_reports_only_the_selected_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);
    // ids: [a.rs introduce, b.rs introduce, insert-above, context-rewrite]

    let mut req = history("TODO anchor me");
    req.at = Some(ids[2].clone());
    req.anchor = Some(GrepAnchor::Coordinate {
        path: "a.rs".into(),
        line: 4,
        column: None,
    });
    let report = store.grep(&req).unwrap();
    // The full episode story inside the interval: introduced before the
    // anchor, the relocation AT the anchor, and the ambiguous end after it.
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Relocated]
    );
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        [LifecycleKind::Ambiguous]
    );
    // The independent b.rs episode never appears.
    assert!(report.hits.iter().all(|hit| hit.path == "a.rs"));
    assert!(report
        .events
        .iter()
        .all(|event| event.path.as_deref() == Some("a.rs")));
    // Every record carries the SAME episode id — one followed episode.
    let mut episodes: Vec<String> = report
        .hits
        .iter()
        .map(|hit| hit.episode_id.clone().unwrap_or_default())
        .collect();
    episodes.extend(
        report
            .events
            .iter()
            .map(|event| event.episode_id.clone().unwrap_or_default()),
    );
    let episodes: std::collections::BTreeSet<String> = episodes.into_iter().collect();
    assert_eq!(
        episodes.len(),
        1,
        "anchored records must share one episode id, got {episodes:?}"
    );
    // The relocation moved the line from 3 to 4.
    assert_eq!(report.hits[0].line, 3);
    assert_eq!(report.hits[1].line, 4);
}

#[test]
fn coordinate_anchor_zero_and_multiple_occurrences_fail_explicitly() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);

    // Zero occurrences on that line.
    let mut missing = history("TODO anchor me");
    missing.at = Some(ids[2].clone());
    missing.anchor = Some(GrepAnchor::Coordinate {
        path: "a.rs".into(),
        line: 1,
        column: None,
    });
    let error = store.grep(&missing).unwrap_err();
    assert!(error.to_string().contains("no occurrence"), "{error}");

    // Several occurrences on one line without a column.
    let tmp2 = tempfile::tempdir().unwrap();
    let root2 = tmp2.path();
    skeleton(root2);
    let mut store2 = open(root2);
    touch(
        &mut store2,
        root2,
        "dup.rs",
        "left TODO right TODO end\n",
        Duration::hours(2),
    );
    let ids2 = capture_ids(&store2);
    let mut ambiguous = history("TODO");
    ambiguous.at = Some(ids2[0].clone());
    ambiguous.anchor = Some(GrepAnchor::Coordinate {
        path: "dup.rs".into(),
        line: 1,
        column: None,
    });
    let error = store2.grep(&ambiguous).unwrap_err();
    assert!(error.to_string().contains("ambiguous"), "{error}");

    // The column disambiguates to the leftmost occurrence.
    let mut resolved = ambiguous.clone();
    resolved.anchor = Some(GrepAnchor::Coordinate {
        path: "dup.rs".into(),
        line: 1,
        column: Some(6),
    });
    let report = store2.grep(&resolved).unwrap();
    assert_eq!(report.hits.len(), 1);
    assert_eq!(report.hits[0].line, 1);
    assert_eq!(report.hits[0].column, 6);
}

#[test]
fn anchor_must_lie_inside_the_history_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);

    let mut inside = history("TODO anchor me");
    inside.to = Some(ids[2].clone());
    inside.at = Some(ids[1].clone());
    inside.anchor = Some(GrepAnchor::Coordinate {
        path: "a.rs".into(),
        line: 3,
        column: None,
    });
    assert!(store.grep(&inside).is_ok(), "anchor at `to` is inclusive");

    // Anchor past `to` is out of range.
    let mut past = inside.clone();
    past.to = Some(ids[0].clone());
    let error = store.grep(&past).unwrap_err();
    assert!(error.to_string().contains("outside"), "{error}");

    // Anchor at or before `from` (exclusive lower bound) is out of range.
    let mut before = inside.clone();
    before.from = Some(ids[1].clone());
    let error = store.grep(&before).unwrap_err();
    assert!(error.to_string().contains("outside"), "{error}");
}

#[test]
fn point_mode_rejects_every_anchor_form() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);

    let mut req = point("TODO anchor me");
    req.at = Some(ids[0].clone());
    req.anchor = Some(GrepAnchor::Coordinate {
        path: "a.rs".into(),
        line: 2,
        column: None,
    });
    let error = store.grep(&req).unwrap_err();
    assert!(error.to_string().contains("--history"), "{error}");

    let mut episode = point("TODO anchor me");
    episode.anchor = Some(GrepAnchor::Episode {
        episode_id: "ep1:abc".into(),
    });
    assert!(store.grep(&episode).is_err());
}

#[test]
fn episode_anchor_rejects_at_and_follows_one_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);

    let unanchored = store.grep(&history("TODO anchor me")).unwrap();
    let episode = unanchored.hits[0].episode_id.clone().expect("episode id");

    let mut req = history("TODO anchor me");
    req.at = Some(ids[0].clone());
    req.anchor = Some(GrepAnchor::Episode {
        episode_id: episode.clone(),
    });
    let error = store.grep(&req).unwrap_err();
    assert!(
        error.to_string().contains("does not take `--at`"),
        "{error}"
    );

    req.at = None;
    let report = store.grep(&req).unwrap();
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Relocated]
    );
    assert_eq!(report.events.len(), 1);
    assert_eq!(
        report.events[0].episode_id.as_deref(),
        Some(episode.as_str())
    );
    // Terminal events name the predecessor episode they end.
    assert_eq!(report.events[0].kind, LifecycleKind::Ambiguous);
}

#[test]
fn selection_anchor_supplies_its_own_frontier() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);

    // Discover at the second capture, then anchor on that hit's handle.
    let mut discover = point("TODO anchor me");
    discover.at = Some(ids[1].clone());
    let discovered = store.grep(&discover).unwrap();
    let anchor_hit = discovered
        .hits
        .iter()
        .find(|hit| hit.path == "a.rs")
        .expect("a.rs occurrence at the discovery point");
    let handle = anchor_hit.handle.clone();

    let mut req = history("TODO anchor me");
    req.anchor = Some(GrepAnchor::Selection {
        handle: Box::new(handle),
    });
    let report = store.grep(&req).unwrap();
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Relocated]
    );

    // An explicit --at must agree with the handle's frontier.
    let mut disagree = req.clone();
    disagree.at = Some(ids[3].clone());
    let error = store.grep(&disagree).unwrap_err();
    assert!(error.to_string().contains("agree"), "{error}");
}

#[test]
fn episode_ids_survive_forks_and_paging_without_collision() {
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

    // Current-only run.
    let current = store.grep(&history("fn probe")).unwrap();
    assert!(current
        .hits
        .iter()
        .all(|hit| hit.episode_id.as_deref().is_some()));

    // All-branches run: the same current-lineage episode ids must appear
    // byte-for-byte, and the divergent branch's episodes must be distinct.
    let mut all_req = history("fn probe");
    all_req.all = true;
    let all = store.grep(&all_req).unwrap();
    let current_ids: std::collections::BTreeSet<String> = current
        .hits
        .iter()
        .filter(|hit| hit.on_current)
        .map(|hit| hit.episode_id.clone().unwrap())
        .collect();
    let all_current_ids: std::collections::BTreeSet<String> = all
        .hits
        .iter()
        .filter(|hit| hit.on_current)
        .map(|hit| hit.episode_id.clone().unwrap())
        .collect();
    assert_eq!(
        current_ids, all_current_ids,
        "current-lineage episode ids must agree between current-only and --all runs"
    );
    let branch_ids: std::collections::BTreeSet<String> = all
        .hits
        .iter()
        .filter(|hit| !hit.on_current)
        .map(|hit| hit.episode_id.clone().unwrap())
        .collect();
    for branch_id in &branch_ids {
        assert!(
            !all_current_ids.contains(branch_id),
            "a forked episode must never collide with the current lineage's"
        );
    }
    assert!(!branch_ids.is_empty());

    // Suppressed-replay purity: paging must reproduce episode ids byte-for-byte.
    let mut paged = history("fn probe");
    paged.all = true;
    paged.budget.max_results = 1;
    let mut seen: Vec<GrepHit> = Vec::new();
    let mut cursor = None;
    loop {
        paged.cursor = cursor;
        let page = store.grep(&paged).unwrap();
        seen.extend(page.hits.clone());
        match page.cursor.clone() {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        seen.len(),
        all.hits.len(),
        "paging must deliver every record exactly once"
    );
    let paged_ids: Vec<String> = seen
        .iter()
        .map(|hit| hit.episode_id.clone().unwrap())
        .collect();
    let direct_ids: Vec<String> = all
        .hits
        .iter()
        .map(|hit| hit.episode_id.clone().unwrap())
        .collect();
    assert_eq!(paged_ids, direct_ids, "episode ids must be replay-pure");
}

#[test]
fn independent_delete_and_add_across_paths_never_moves() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(&mut store, root, "one.rs", "TODO one\n", Duration::hours(4));
    touch(&mut store, root, "two.rs", "TODO two\n", Duration::hours(3));
    // Remove one.rs's occurrence entirely; add an unrelated one in three.rs
    // in the same capture.
    std::fs::write(root.join("one.rs"), "nothing here\n").unwrap();
    std::fs::write(root.join("three.rs"), "TODO three\n").unwrap();
    write_capture(
        &mut store,
        root,
        vec![
            EventKind::Touched {
                path: root.join("one.rs").into(),
            },
            EventKind::Touched {
                path: root.join("three.rs").into(),
            },
        ],
        Duration::hours(2),
    );

    let report = store.grep(&history("TODO")).unwrap();
    assert_eq!(
        kinds(&report),
        [
            LifecycleKind::Introduced,
            LifecycleKind::Introduced,
            LifecycleKind::Introduced
        ]
    );
    // one.rs was removed (not moved, not renamed).
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| (event.kind, event.path.clone()))
            .collect::<Vec<_>>(),
        [(LifecycleKind::Removed, Some("one.rs".to_owned()))]
    );
    assert!(report.hits.iter().all(|hit| !matches!(
        hit.kind,
        LifecycleKind::Moved | LifecycleKind::Renamed | LifecycleKind::Relocated
    )));
}

#[test]
fn duplicate_candidates_emit_ordered_ambiguity_without_linking() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "dup.rs",
        "xx TODO yy\n",
        Duration::hours(3),
    );
    // Duplicate the line: the prior occurrence's 64-byte context cannot match
    // either copy exactly (each copy sees the other in its window), but both
    // carry the exact selected bytes -> two candidates, no unique rebind.
    touch(
        &mut store,
        root,
        "dup.rs",
        "xx TODO yy\nxx TODO yy\n",
        Duration::hours(2),
    );

    let report = store.grep(&history("TODO")).unwrap();
    // The prior episode ends ambiguous; both duplicate copies start fresh.
    assert_eq!(
        kinds(&report),
        [
            LifecycleKind::Introduced,
            LifecycleKind::Introduced,
            LifecycleKind::Introduced
        ]
    );
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        [LifecycleKind::Ambiguous]
    );
    let event = &report.events[0];
    let candidates = event.candidates.as_ref().expect("ordered candidates");
    assert_eq!(candidates.len(), 2, "both duplicate lines are candidates");
    // Ordered by byte range: shorter handle prefix corresponds to the earlier
    // range in a deterministic batch; identities are distinct.
    assert_ne!(candidates[0], candidates[1]);
    assert!(candidates.iter().all(|id| id.len() >= 32));
    // Candidate ORDER is structural — (path, byte range), the same
    // normative order as records — while the handle IDs themselves are
    // opaque content hashes, so their lexicographic order proves nothing.
    // The two duplicates live on one path, so the normative order here
    // reduces to byte range; the engine test suite pins the rendering.
    // The diagnostic names the episode it terminates.
    assert!(event.episode_id.is_some());
}

#[test]
fn same_file_insert_above_is_relocation_not_a_new_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        &format!("{PAD}TODO stable\n"),
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        "f.rs",
        &format!("{PAD}{PAD}TODO stable\n"),
        Duration::hours(2),
    );
    let report = store.grep(&history("TODO stable")).unwrap();
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Relocated]
    );
    assert_eq!(
        report.hits[0].episode_id, report.hits[1].episode_id,
        "a relocation continues the same episode"
    );
    assert_eq!(report.hits[0].line, 2);
    assert_eq!(report.hits[1].line, 3);
}

#[test]
fn recorded_rename_continues_the_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "old.rs",
        "TODO carried\n",
        Duration::hours(3),
    );
    rename(&mut store, root, "old.rs", "new.rs", Duration::hours(2));
    let report = store.grep(&history("TODO carried")).unwrap();
    assert_eq!(
        kinds(&report),
        [LifecycleKind::Introduced, LifecycleKind::Renamed]
    );
    assert_eq!(report.hits[0].episode_id, report.hits[1].episode_id);
    assert_eq!(report.hits[1].path, "new.rs");
}

#[test]
fn line_extent_collapses_matches_on_one_line() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        &format!("{PAD}TODO twice TODO once\nTODO\n"),
        Duration::hours(3),
    );

    // Match extent: three occurrences, each its own coordinates.
    let mut match_req = point("TODO");
    match_req.extent = SelectionExtent::Match;
    let matches = store.grep(&match_req).unwrap();
    assert_eq!(
        hits(&matches),
        vec![
            ("f.rs".into(), LifecycleKind::Present, 2, 1, String::new()),
            ("f.rs".into(), LifecycleKind::Present, 2, 12, String::new()),
            ("f.rs".into(), LifecycleKind::Present, 3, 1, String::new()),
        ]
    );

    // Line extent: the two matches on line 2 are ONE restorable unit whose
    // coordinates come from the leftmost match; line 3 is its own unit.
    let mut line_req = point("TODO");
    line_req.extent = SelectionExtent::Line;
    let lines = store.grep(&line_req).unwrap();
    assert_eq!(lines.hits.len(), 2);
    assert_eq!((lines.hits[0].line, lines.hits[0].column), (2, 1));
    assert_eq!((lines.hits[1].line, lines.hits[1].column), (3, 1));
    // One unit per line means one handle per line: distinct line extents
    // never collide on handle identity.
    assert_ne!(lines.hits[0].handle_id, lines.hits[1].handle_id);

    // History under line extent tracks one episode per line unit across a
    // context-preserving insert above (relocation of the collapsed unit).
    touch(
        &mut store,
        root,
        "f.rs",
        &format!("{PAD}{PAD}TODO twice TODO once\nTODO\n"),
        Duration::hours(2),
    );
    let mut line_history = history("TODO");
    line_history.extent = SelectionExtent::Line;
    let report = store.grep(&line_history).unwrap();
    assert_eq!(
        kinds(&report),
        [
            LifecycleKind::Introduced,
            LifecycleKind::Introduced,
            LifecycleKind::Relocated,
            LifecycleKind::Relocated
        ]
    );
}

#[test]
fn records_order_by_lineage_then_kind_rank() {
    use sheaf_core::store::GrepStreamRecord;

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { base }\n",
        Duration::hours(5),
    );
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    // One branch capture that introduces a fresh occurrence.
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { abandoned }\nfn extra() { probe }\n",
        Duration::hours(4),
    );
    store.checkout_for_branch(&base.frontier).unwrap();
    // A current-lineage capture removing the old occurrence while
    // introducing a new one in the same file.
    std::fs::write(root.join("f.rs"), "fn fresh() { probe }\n").unwrap();
    write_capture(
        &mut store,
        root,
        vec![EventKind::Touched {
            path: root.join("f.rs").into(),
        }],
        Duration::hours(3),
    );

    let mut req = history("probe");
    req.all = true;
    let mut streamed: Vec<GrepStreamRecord> = Vec::new();
    let mut sink = |record: GrepStreamRecord| streamed.push(record);
    store.grep_streaming(&req, &mut Some(&mut sink)).unwrap();

    let on_current = |record: &GrepStreamRecord| match record {
        GrepStreamRecord::Hit { hit } => hit.on_current,
        GrepStreamRecord::Event { event } => event.on_current,
    };
    // Lineage-major: every branch record precedes every current record
    // ("branch:..." sorts before "current" bytewise), and the current
    // lineage's records form one contiguous suffix — a branch record after
    // the first current record would mean the lineages interleave.
    if let Some(index) = streamed.iter().position(on_current) {
        assert!(
            index > 0,
            "fixture must emit at least one branch record before current's"
        );
        assert!(
            streamed[index..].iter().all(on_current),
            "current records must be a contiguous suffix (no branch record after current)"
        );
    } else {
        panic!("fixture must emit current-lineage records");
    }
    // Within one capture, the removal event ranks before the introduction
    // hit of the same capture (kind rank: ended things first).
    let removal = streamed
        .iter()
        .position(|record| matches!(record, GrepStreamRecord::Event { event } if event.on_current))
        .expect("a current-lineage removal event");
    let removal_capture = match &streamed[removal] {
        GrepStreamRecord::Event { event } => event.capture_id.clone(),
        _ => unreachable!(),
    };
    let introduction_index = streamed
        .iter()
        .position(|record| {
            matches!(record, GrepStreamRecord::Hit { hit }
                if hit.on_current && hit.capture_id == removal_capture)
        })
        .expect("an introduction in the removal's capture");
    assert!(
        removal < introduction_index,
        "removals rank before introductions inside one capture's batch"
    );
}

#[test]
fn anchored_history_crosses_no_lineage_without_branch_qualified_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { base }\n",
        Duration::hours(5),
    );
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { abandoned }\n",
        Duration::hours(4),
    );
    store.checkout_for_branch(&base.frontier).unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "fn probe() { current }\n",
        Duration::hours(3),
    );

    // Anchor on the CURRENT lineage's occurrence with an --all query: only
    // the current lineage's episode records appear, never the branch's.
    let unanchored_current = store.grep(&history("fn probe")).unwrap();
    let current_episode = unanchored_current
        .hits
        .iter()
        .find(|hit| hit.kind == LifecycleKind::Introduced && hit.on_current)
        .or_else(|| unanchored_current.hits.first())
        .unwrap()
        .episode_id
        .clone()
        .unwrap();

    let mut req = history("fn probe");
    req.all = true;
    req.anchor = Some(GrepAnchor::Episode {
        episode_id: current_episode,
    });
    let report = store.grep(&req).unwrap();
    assert!(
        report.hits.iter().all(|hit| hit.on_current),
        "an anchored all-branch query never crosses lineage"
    );
    assert!(!report.hits.is_empty());
}

#[test]
fn retention_gap_swallows_an_anchored_episode_without_fabricating() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    // The victim prefix carries the followed occurrence; a later checkpoint
    // protects the surviving suffix (retention cuts are prefix-shaped).
    touch(
        &mut store,
        root,
        "f.rs",
        "TODO doomed\n",
        Duration::hours(6),
    );
    touch(
        &mut store,
        root,
        "f.rs",
        "TODO doomed\nfn tail() {}\n",
        Duration::hours(5),
    );
    let victim = capture_ids(&store)[0].clone();
    // A surviving capture that supersedes the victim's content, pinned by a
    // checkpoint so the cut boundary sits above the victim.
    touch(
        &mut store,
        root,
        "f.rs",
        "TODO doomed\nfn tail() { 2 }\n",
        Duration::hours(4),
    );
    store.create_checkpoint("keep", None).unwrap();
    touch(
        &mut store,
        root,
        "f.rs",
        "TODO doomed\nfn tail() { 3 }\n",
        Duration::hours(3),
    );

    // The pre-trim episode of the victim's occurrence.
    let episode = store.grep(&history("TODO doomed")).unwrap().hits[0]
        .episode_id
        .clone()
        .unwrap();

    sheaf_core::store::retention_mark(&mut store, &victim[..12]).unwrap();
    sheaf_core::store::gc_run_store(&mut store, true).unwrap();

    // Unanchored: the gap participates at its chronological position and the
    // post-gap occurrences are fresh episodes (continuity terminated).
    let unanchored = store.grep(&history("TODO doomed")).unwrap();
    assert!(unanchored
        .events
        .iter()
        .any(|event| event.kind == LifecycleKind::RetentionGap && event.on_current));
    assert!(unanchored.pruned_intervals >= 1);
    let post_gap_episode = unanchored
        .hits
        .iter()
        .find(|hit| {
            hit.timestamp_ms
                > unanchored
                    .events
                    .iter()
                    .find(|event| event.kind == LifecycleKind::RetentionGap)
                    .unwrap()
                    .timestamp_ms
        })
        .map(|hit| hit.episode_id.clone().unwrap())
        .expect("a post-gap episode");
    assert_ne!(
        post_gap_episode, episode,
        "a retention gap must never fabricate continuity into the old episode"
    );

    // Anchored on the swallowed episode: no records at all — nothing of it is
    // observable, and nothing is invented to bridge the gap.
    let mut req = history("TODO doomed");
    req.anchor = Some(GrepAnchor::Episode {
        episode_id: episode,
    });
    let anchored = store.grep(&req).unwrap();
    assert!(anchored.hits.is_empty());
    assert!(anchored.events.is_empty());

    // Anchoring at a capture inside the gap fails explicitly.
    let mut gone = history("TODO doomed");
    gone.at = Some(victim.clone());
    gone.anchor = Some(GrepAnchor::Coordinate {
        path: "f.rs".into(),
        line: 1,
        column: None,
    });
    let error = store.grep(&gone).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("does not name a capture")
            || message.contains("outside")
            || message.contains("pruned"),
        "unexpected error: {message}"
    );
}

#[test]
fn point_query_byte_budget_pages_and_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    for i in 0..4 {
        touch(
            &mut store,
            root,
            &format!("f{i}.rs"),
            &format!("line\nTODO {i}\nline\n"),
            Duration::hours(9 - i as i64),
        );
    }

    let direct = store.grep(&point("TODO")).unwrap();
    assert!(direct.complete);
    let total = direct.hits.len();
    assert_eq!(total, 4);

    // A byte budget smaller than one file read forces resumable pages; warm
    // cache reads no longer charge, so pages must advance and complete.
    let mut paged = point("TODO");
    paged.budget = SearchBudget {
        max_results: 100,
        max_materialized_bytes: 8,
        max_elapsed_ms: u64::MAX,
    };
    let mut collected: Vec<(String, usize, usize)> = Vec::new();
    let mut cursor = None;
    loop {
        paged.cursor = cursor;
        let page = store.grep(&paged).unwrap();
        collected.extend(
            page.hits
                .iter()
                .map(|hit| (hit.path.clone(), hit.line, hit.column)),
        );
        match page.cursor.clone() {
            Some(next) => {
                assert!(!page.hits.is_empty() || page.usage.historical_cache_hits > 0);
                cursor = Some(next);
            }
            None => {
                assert!(page.complete);
                break;
            }
        }
    }
    let direct_set: Vec<(String, usize, usize)> = direct
        .hits
        .iter()
        .map(|hit| (hit.path.clone(), hit.line, hit.column))
        .collect();
    assert_eq!(
        collected, direct_set,
        "paged results equal the unbounded query"
    );
    assert_eq!(collected.len(), total);
}

#[test]
fn degraded_reader_matches_the_live_store_for_anchored_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = anchored_store(root);
    let ids = capture_ids(&store);
    let mut req = history("TODO anchor me");
    req.at = Some(ids[1].clone());
    req.anchor = Some(GrepAnchor::Coordinate {
        path: "a.rs".into(),
        line: 3,
        column: None,
    });
    let live = store.grep(&req).unwrap();
    drop(store);
    std::fs::remove_dir_all(root.join(".sheaf/state/cache/grep-v1")).ok();
    let reader = TimelineReader::open(root).unwrap();
    let degraded = reader.grep(&req).unwrap();
    assert_eq!(degraded.hits, live.hits);
    assert_eq!(degraded.events, live.events);
    assert!(degraded.degraded);
}

#[test]
fn late_range_baseline_enumerates_untouched_paths() {
    // The walk enters at `from`, not at genesis: the first in-range capture
    // becomes the baseline and must enumerate the WHOLE tree, or occurrences
    // in files that capture did not touch are silently absent from history
    // (`--from @~3` would report nothing for a needle that point discovery
    // still finds at `@`).
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    // Genesis: both files carry the needle. Ages ascend with commit order so
    // the timeline order and the DAG order agree (real stores write with
    // wall-clock timestamps that already do).
    touch(
        &mut store,
        root,
        "watched.rs",
        "fn probe() {}\n",
        Duration::hours(3),
    );
    touch(
        &mut store,
        root,
        root_rel_quiet(),
        "fn probe() {}\n",
        Duration::hours(2),
    );
    // Second capture touches only watched.rs.
    touch(
        &mut store,
        root,
        "watched.rs",
        "fn probe() { 2 }\n",
        Duration::hours(1),
    );
    let ids = capture_ids(&store);
    assert_eq!(ids.len(), 3);

    let mut req = history("probe");
    req.from = Some(ids[1].clone()); // walk (c2, head]
    let report = store.grep(&req).unwrap();
    assert!(report.complete);
    // untouched.rs occurrences at the baseline still enter the state.
    let paths: Vec<&str> = report.hits.iter().map(|h| h.path.as_str()).collect();
    assert!(
        paths.contains(&"untouched.rs"),
        "baseline must enumerate untouched paths, got {paths:?}"
    );
    // And a later removal of the untouched file ends the episode.
    touch(
        &mut store,
        root,
        "untouched.rs",
        "// gone\n",
        Duration::hours(0),
    );
    let mut req = history("probe");
    req.from = Some(ids[1].clone());
    let report = store.grep(&req).unwrap();
    let removed_untouched = report
        .events
        .iter()
        .any(|e| e.path.as_deref() == Some("untouched.rs") && e.kind == LifecycleKind::Removed);
    assert!(removed_untouched, "removal after the range must be visible");
}

#[test]
fn every_capture_observes_untouched_paths() {
    // `--every-capture` promises an observation record at every capture an
    // occurrence survives, including captures that never touched its file.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    touch(
        &mut store,
        root,
        "stable.rs",
        "fn probe() {}\n",
        Duration::hours(1),
    );
    touch(
        &mut store,
        root,
        "other.rs",
        "fn unrelated() {}\n",
        Duration::hours(0),
    );

    let mut req = history("probe");
    req.every_capture = true;
    let report = store.grep(&req).unwrap();
    let observed_stable: Vec<usize> = report
        .hits
        .iter()
        .filter(|h| h.path == "stable.rs" && h.kind == LifecycleKind::Observed)
        .map(|h| h.line)
        .collect();
    assert_eq!(
        observed_stable.len(),
        1,
        "the untouched capture must observe the carried occurrence"
    );
    // Without the flag the carry stays silent.
    let req = history("probe");
    let report = store.grep(&req).unwrap();
    assert!(report
        .hits
        .iter()
        .all(|h| h.kind != LifecycleKind::Observed));
}

fn root_rel_quiet() -> &'static str {
    "untouched.rs"
}

#[test]
fn contested_candidate_ambiguates_both_priors_without_continuing() {
    // Two episodes whose occurrences are context-identical, then a rename
    // landing one of them on the other's path: the surviving occurrence is
    // compatible with BOTH priors, so neither edge is proven and BOTH
    // episodes must end ambiguous — path order must not decide which one
    // continues (the fail-closed correspondence contract).
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let body = format!("{PAD}needle\n{PAD}");
    touch(&mut store, root, "a.rs", &body, Duration::hours(2));
    touch(&mut store, root, "b.rs", &body, Duration::hours(1));
    // Rename a.rs onto b.rs: both endpoints touched, identical content.
    std::fs::remove_file(root.join("b.rs")).unwrap();
    rename(&mut store, root, "a.rs", "b.rs", Duration::hours(0));

    let report = store.grep(&history("needle")).unwrap();
    let ambiguous: Vec<_> = report
        .events
        .iter()
        .filter(|e| e.kind == LifecycleKind::Ambiguous)
        .collect();
    assert_eq!(
        ambiguous.len(),
        2,
        "both contested priors end ambiguous, got {:?}",
        report.events
    );
    let paths: Vec<&str> = ambiguous.iter().filter_map(|e| e.path.as_deref()).collect();
    assert!(paths.contains(&"a.rs") && paths.contains(&"b.rs"));
    // Both ambiguous events name the same surviving candidate: no episode
    // claimed it (a relocation or renamed hit would prove a claim).
    assert!(report
        .hits
        .iter()
        .all(|h| h.kind == LifecycleKind::Introduced));
    let candidates: Vec<&Vec<String>> = ambiguous
        .iter()
        .filter_map(|e| e.candidates.as_ref())
        .collect();
    assert_eq!(candidates.len(), 2);
    assert!(!candidates[0].is_empty());
    // The candidate lists are identical: the one surviving occurrence.
    assert_eq!(candidates[0], candidates[1]);
}

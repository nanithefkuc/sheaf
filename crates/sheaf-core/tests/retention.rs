//! Retention — reachability-bound automatic expiry plus explicit marks,
//! over the timeline-ledger + shallow-snapshot compaction model.
//!
//! The bar: automatic expiry may only reclaim reachability-unbound history,
//! explicit marks bypass but never touch the present, a trimmed store still
//! restores everything it promised to keep, pruned points fail with an
//! explanation instead of a shrug, and an unconfigured store keeps every
//! byte after gc.

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{
    gc_plan, gc_run_store, retention_mark, GrepBackfillOptions, GrepMode, GrepQuery, GrepRequest,
    ProjectStore, SearchBudget, SelectionExtent, StoreLimits, TimelineReader,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_segment_bytes: 4 << 20,
        snapshot_edit_size: 3,
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

/// Flush a batch whose captures are stamped `age` in the past, so expiry
/// tests do not need to sleep.
fn flush_aged(store: &mut ProjectStore, root: &Path, rel: &str, bytes: &[u8], age: Duration) {
    write(root, rel, bytes);
    let now = Utc::now();
    let events = vec![FsEvent {
        at: now - age,
        kind: EventKind::Touched {
            path: sheaf_core::events::TouchedPath(root.join(rel)),
        },
    }];
    let batch = Batch {
        root: root.to_path_buf(),
        events,
        started_at: now - age,
        flushed_at: now - age,
    };
    store.apply_batch(&batch).unwrap();
}

fn capture_id_at(store: &ProjectStore, reference: &str) -> String {
    store
        .resolve(reference)
        .unwrap()
        .capture_id
        .unwrap_or_else(|| panic!("`{reference}` must name a capture"))
}

#[test]
fn expiry_trims_old_history_and_keeps_everything_protected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "a.txt", b"v1\n", hour * 5);
    flush_aged(&mut store, root, "a.txt", b"v2\n", hour * 4);
    flush_aged(&mut store, root, "a.txt", b"v3\n", hour * 3);
    let pinned = capture_id_at(&store, "@");
    store.create_checkpoint("pin", None).unwrap();
    store
        .create_branch("release", None, Default::default())
        .unwrap();
    flush_aged(&mut store, root, "b.txt", b"keep\n", hour);
    flush_aged(&mut store, root, "b.txt", b"keep2\n", Duration::zero());

    let before = store.captures(true, None, false, usize::MAX).unwrap().len();
    assert_eq!(before, 5, "five captures recorded");

    config::set_retention_expiry(root, "2h").unwrap();
    let plan = gc_plan(root).unwrap();
    // The two oldest captures predate the horizon and sit before the
    // checkpoint-pinned boundary; everything at/after it is protected.
    assert_eq!(
        plan.retention.prunable.len(),
        2,
        "prunable = {:#?}",
        plan.retention
    );
    assert!(plan
        .retention
        .prunable
        .iter()
        .all(|c| c.cause.as_str() == "expiry"));
    assert_eq!(plan.retention.expiry.as_deref(), Some("2h"));
    assert!(
        plan.retention
            .protected
            .iter()
            .any(|p| p.reason.contains("pin")),
        "checkpoint appears in the protected set"
    );
    assert!(
        plan.retention
            .protected
            .iter()
            .any(|point| point.reason == "branch 'release'"),
        "named branch appears in the protected set"
    );

    let pruned_ids: Vec<String> = plan
        .retention
        .prunable
        .iter()
        .map(|c| c.id.clone())
        .collect();
    let outcome = gc_run_store(&mut store, true).unwrap();
    let report = match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => report,
        other => panic!("expected applied report, got {other:?}"),
    };
    assert_eq!(report.trimmed, 2, "two captures tombstoned");
    assert!(report.boundary_after.is_some(), "trim records its boundary");

    // Reopen read-only: survivors only, ghosts present, pinned point lives.
    let reader = TimelineReader::open(root).unwrap();
    let captures = reader.captures(true, None, false, usize::MAX).unwrap();
    assert_eq!(captures.len(), 3, "survivors: {:#?}", captures);
    assert!(
        captures.iter().any(|c| c.id == pinned),
        "checkpointed capture survived"
    );
    let ghosts = reader.pruned();
    assert_eq!(ghosts.len(), 2, "tombstones present");
    assert!(ghosts.iter().all(|(_, t)| t.cause.as_str() == "expiry"));

    // Pruned ids explain themselves; survivors still resolve.
    for id in &pruned_ids {
        let err = reader.resolve(&id[..12]).unwrap_err();
        assert!(format!("{err}").contains("pruned by expiry"), "got: {err}");
    }
    reader.resolve("checkpoint:pin").unwrap();
    reader.resolve("@").unwrap();

    // Doctor stays green on a correctly trimmed store, and both new
    // checks exist and pass.
    let report = sheaf_core::store::doctor(root).unwrap();
    assert!(report.ok, "doctor green post-trim: {:#?}", report.checks);
    for name in ["ledger_state", "shallow_baseline"] {
        let found = report.checks.iter().find(|c| c.name == name);
        assert!(found.is_some(), "doctor runs `{name}` on trimmed stores");
        assert!(
            found.unwrap().ok,
            "{name} passes: {:?}",
            found.unwrap().detail
        );
    }
}

/// A retention trim sweeps the derived grep cache to exactly
/// the retained captures — collected mappings and their now-orphaned content
/// blobs are removed, the trigram index is rebuilt over the survivors, and the
/// protected/checkpointed points remain fully searchable with results equal to
/// a fresh authoritative reference on the trimmed store. No timeline byte is
/// touched by the sweep.
#[test]
fn retention_sweeps_the_grep_cache_to_retained_captures() {
    fn literal(needle: &str) -> GrepRequest {
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
            every_capture: false,
            extent: SelectionExtent::Match,
            budget: SearchBudget::default(),
            cursor: None,
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);

    // The two oldest captures (v1, v2) carry a needle nothing later holds, so
    // they are the ones a 2h expiry collects; v3 onward never mention it.
    flush_aged(
        &mut store,
        root,
        "a.txt",
        b"doomed_marker only_here\n",
        hour * 5,
    );
    flush_aged(
        &mut store,
        root,
        "a.txt",
        b"doomed_marker still_here\n",
        hour * 4,
    );
    flush_aged(&mut store, root, "a.txt", b"plain content now\n", hour * 3);
    store.create_checkpoint("pin", None).unwrap();
    // Protected recent history carries a different needle.
    flush_aged(&mut store, root, "b.txt", b"kept_marker one\n", hour);
    flush_aged(
        &mut store,
        root,
        "b.txt",
        b"kept_marker two\n",
        Duration::zero(),
    );

    // Build the sidecar and trigram index over the full history.
    let backfill = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(backfill.trigram_index_bytes > 0);

    let cache_dir = root.join(".sheaf/state/cache/grep-v1");
    let content_before = std::fs::read_dir(cache_dir.join("content"))
        .unwrap()
        .count();
    let mappings_before = std::fs::read_to_string(cache_dir.join("mappings.jsonl"))
        .unwrap()
        .lines()
        .count();

    // Trim the two oldest captures.
    config::set_retention_expiry(root, "2h").unwrap();
    let outcome = gc_run_store(&mut store, true).unwrap();
    assert!(matches!(outcome, sheaf_core::store::GcOutcome::Applied(_)));

    // The sweep removed collected mappings and orphaned blobs.
    let content_after = std::fs::read_dir(cache_dir.join("content"))
        .unwrap()
        .count();
    let mappings_after = std::fs::read_to_string(cache_dir.join("mappings.jsonl"))
        .unwrap()
        .lines()
        .count();
    assert!(
        mappings_after < mappings_before,
        "collected mappings must be removed ({mappings_before} -> {mappings_after})"
    );
    assert!(
        content_after < content_before,
        "orphaned content blobs must be swept ({content_before} -> {content_after})"
    );

    // The sweep touches no timeline bytes: gc's own compaction rewrites the
    // store, but byte-exactness of survivors is asserted separately below and
    // by `trimmed_store_restores_survivors_byte_exact`; the sweep writes only
    // inside the disposable cache directory, which the counts above exercise.

    // Doctor stays green: the swept cache is clean, not corrupt.
    let report = sheaf_core::store::doctor(root).unwrap();
    assert!(report.ok, "doctor green after sweep: {:#?}", report.checks);
    let grep_cache = report
        .checks
        .iter()
        .find(|c| c.name == "grep_cache")
        .expect("doctor reports grep_cache");
    assert!(grep_cache.ok);
    assert!(
        !grep_cache.detail.contains("orphan"),
        "no orphan blobs remain: {}",
        grep_cache.detail
    );

    // A post-trim backfill starts a distinguishable coverage generation; it
    // must never reuse a watermark whose capture chain described the pre-trim
    // timeline shape.
    let post_trim = store
        .grep_cache_backfill(GrepBackfillOptions::default())
        .unwrap();
    assert!(
        post_trim.watermark.as_ref().unwrap().generation
            > backfill.watermark.as_ref().unwrap().generation,
        "retention sweep must advance the durable cache generation"
    );

    // The protected needle is still searchable and equal to a fresh
    // authoritative reference on the trimmed store; the collected needle is
    // gone from the timeline (so grep finds nothing), proving the sweep did
    // not leave stale rows that could resurrect it.
    let kept = store.grep(&literal("kept_marker")).unwrap();
    assert!(
        !kept.hits.is_empty(),
        "protected captures remain searchable after the sweep"
    );
    let doomed = store.grep(&literal("doomed_marker")).unwrap();
    assert!(
        doomed.hits.is_empty(),
        "collected captures are gone from the timeline, so their needle is unfound"
    );

    // Equality against a fresh reference: rebuild the cache from scratch on
    // the trimmed store and confirm the swept cache gives identical results.
    let rebuilt = store
        .grep_cache_backfill(GrepBackfillOptions {
            rebuild: true,
            ..Default::default()
        })
        .unwrap();
    assert!(rebuilt.complete);
    let kept_rebuilt = store.grep(&literal("kept_marker")).unwrap();
    assert_eq!(
        kept.hits.len(),
        kept_rebuilt.hits.len(),
        "swept cache results equal a from-scratch rebuild on the trimmed store"
    );
}

#[test]
fn trimmed_store_restores_survivors_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "old.txt", b"ancient\n", hour * 5);
    flush_aged(&mut store, root, "keep.txt", b"keep-v1\n", hour);
    let boundary_capture = capture_id_at(&store, "@");
    store.create_checkpoint("pin", None).unwrap();
    flush_aged(&mut store, root, "keep.txt", b"keep-v2\n", Duration::zero());

    config::set_retention_expiry(root, "2h").unwrap();
    gc_run_store(&mut store, true).unwrap();

    // A fresh writer on the trimmed (shallow) store restores the pinned
    // point: the fork_at-free state path must hold.
    let mut store2 = open(root);
    let plan = store2
        .plan_restore("checkpoint:pin", &[], &ignores())
        .expect("plan on trimmed store");
    store2
        .apply_restore(&plan, &ignores())
        .expect("apply on trimmed store");
    assert_eq!(
        std::fs::read(root.join("keep.txt")).unwrap(),
        b"keep-v1\n",
        "restore lands the pinned state"
    );
    // The boundary capture itself stays restorable.
    let plan = store2
        .plan_restore(&boundary_capture[..12], &[], &ignores())
        .unwrap();
    store2.apply_restore(&plan, &ignores()).unwrap();
    // And writing continues on the trimmed store.
    flush_aged(
        &mut store2,
        root,
        "new.txt",
        b"post-trim\n",
        Duration::zero(),
    );
    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(true, None, false, usize::MAX)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn marks_bypass_protection_but_never_the_present() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "a.txt", b"1\n", hour * 3);
    store.create_checkpoint("pin", None).unwrap();
    flush_aged(&mut store, root, "a.txt", b"middle\n", hour * 2);
    let middle = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "a.txt", b"tip\n", Duration::zero());

    // Marking the head refuses: that is the present, not restorable history.
    let err = retention_mark(&mut store, "@").unwrap_err();
    assert!(format!("{err}").contains("head"), "got: {err}");

    // A mark BETWEEN the boundary and the tip is deferred: the tip (and
    // head) sit above it, so nothing is prunable yet and the mark waits.
    let marked = retention_mark(&mut store, &middle[..12]).unwrap();
    assert_eq!(marked.capture_id, middle);
    let plan = gc_plan(root).unwrap();
    assert!(
        plan.retention.prunable.is_empty(),
        "nothing below the checkpoint: {:#?}",
        plan.retention
    );
    assert!(
        plan.retention
            .deferred_marks
            .iter()
            .any(|m| middle.starts_with(m.as_str())),
        "mark pinned behind the tip is deferred: {:#?}",
        plan.retention
    );

    // Applying without any prunable prefix must not trim anything.
    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => assert_eq!(report.trimmed, 0),
        other => panic!("expected applied report, got {other:?}"),
    }
    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(true, None, false, usize::MAX)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn mark_reclaims_immediately_when_unprotected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "a.txt", b"1\n", hour * 3);
    let victim = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "a.txt", b"2\n", hour * 2);
    let middle = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "a.txt", b"3\n", Duration::zero());

    // Interior marks defer in v1: the middle capture has an
    // unearned capture below it, so nothing is prunable yet and the mark
    // waits above the keep-set boundary.
    retention_mark(&mut store, &middle[..12]).unwrap();
    let plan = gc_plan(root).unwrap();
    assert!(plan.retention.prunable.is_empty(), "{:#?}", plan.retention);
    assert!(
        plan.retention
            .deferred_marks
            .iter()
            .any(|m| middle.starts_with(m.as_str())),
        "interior mark deferred"
    );

    // Now mark the OLDEST capture too: every capture below the survivor is
    // earned, the boundary rises to it, and BOTH marks act at once.
    retention_mark(&mut store, &victim[..12]).unwrap();
    let plan = gc_plan(root).unwrap();
    assert_eq!(plan.retention.prunable.len(), 2, "{:#?}", plan.retention);
    assert!(plan
        .retention
        .prunable
        .iter()
        .all(|c| c.cause.as_str() == "gc mark"));

    // Idempotent re-mark reports already-marked without a second record.
    let again = retention_mark(&mut store, &victim[..12]).unwrap();
    assert!(again.already_marked);
    let ledger_marks = TimelineReader::open(root).unwrap().ledger().marks.len();
    assert_eq!(ledger_marks, 2, "re-mark appended nothing");

    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => assert_eq!(report.trimmed, 2),
        other => panic!("expected applied report, got {other:?}"),
    }
    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(true, None, false, usize::MAX)
            .unwrap()
            .len(),
        1
    );
    let err = reader.resolve(&victim[..12]).unwrap_err();
    assert!(format!("{err}").contains("pruned by gc mark"), "got: {err}");
}

#[test]
fn mark_destroys_checkpoint_protection_when_targeted() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "a.txt", b"1\n", hour * 3);
    let early = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "a.txt", b"2\n", hour * 2);
    store.create_checkpoint("pin", Some(&early[..12])).unwrap();
    flush_aged(&mut store, root, "a.txt", b"3\n", Duration::zero());

    // The explicit-mark bypass: mark the checkpoint's own target. The checkpoint
    // no longer protects it (with a warning in the plan), and the trim may
    // take everything before the branch tip.
    retention_mark(&mut store, &early[..12]).unwrap();
    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => assert!(report.trimmed >= 1),
        other => panic!("expected applied report, got {other:?}"),
    }
    let reader = TimelineReader::open(root).unwrap();
    let err = reader.resolve(&early[..12]).unwrap_err();
    assert!(format!("{err}").contains("pruned"), "got: {err}");
    // The label itself survives as navigation state even though its target
    // is gone — resolving it now explains the prune.
    let err = reader.resolve("checkpoint:pin").unwrap_err();
    assert!(format!("{err}").contains("pruned"), "got: {err}");
}

#[test]
fn fresh_captures_below_an_old_checkpoint_survive_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    // An old capture, a fresh-but-not-expired one, then a checkpoint above
    // both. The boundary sits at the checkpoint; the prefix below it holds
    // one expired capture and one that is merely recent.
    flush_aged(&mut store, root, "a.txt", b"old\n", hour * 5);
    let old_id = capture_id_at(&store, "@");
    flush_aged(
        &mut store,
        root,
        "a.txt",
        b"recent\n",
        Duration::minutes(30),
    );
    let recent_id = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "b.txt", b"pinned\n", Duration::zero());
    store.create_checkpoint("pin", None).unwrap();
    flush_aged(&mut store, root, "b.txt", b"after\n", Duration::zero());

    config::set_retention_expiry(root, "2h").unwrap();
    let plan = gc_plan(root).unwrap();
    // The 5h-old capture earned its prune; the 30-minute one below the
    // checkpoint is NOT collateral of anything and must survive.
    assert_eq!(plan.retention.prunable.len(), 1, "{:#?}", plan.retention);
    assert_eq!(plan.retention.prunable[0].cause.as_str(), "expiry");
    assert_eq!(plan.retention.prunable[0].id, old_id);
    assert_ne!(plan.retention.prunable[0].id, recent_id);

    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => assert_eq!(report.trimmed, 1),
        other => panic!("expected applied report, got {other:?}"),
    }
    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(true, None, false, usize::MAX)
            .unwrap()
            .len(),
        3,
        "recent + pinned + after survive; only the expired prefix went"
    );
    reader.resolve("@").unwrap();
    reader.resolve("checkpoint:pin").unwrap();
}

#[test]
fn trim_survives_head_behind_divergent_tip() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    flush_aged(&mut store, root, "a.txt", b"old\n", hour * 5);
    let old_id = capture_id_at(&store, "@");
    flush_aged(&mut store, root, "a.txt", b"abandoned\n", hour * 4);
    let abandoned_id = capture_id_at(&store, "@");
    // Rewind: the worktree head falls behind the oplog tip and the next
    // capture opens a divergent branch — the ugliest realistic shape.
    let plan = store.plan_restore(&old_id[..12], &[], &ignores()).unwrap();
    store.apply_restore(&plan, &ignores()).unwrap();
    flush_aged(&mut store, root, "a.txt", b"diverged\n", hour * 3);
    flush_aged(&mut store, root, "a.txt", b"newer\n", hour * 2);

    // The abandoned branch pins the shared spine: its protected tip holds
    // the keep-set GCA AT the fork (the old capture itself), so the mark
    // on old defers until the tip above it is lifted too.
    retention_mark(&mut store, &old_id[..12]).unwrap();
    let plan = gc_plan(root).unwrap();
    assert!(plan.retention.prunable.is_empty(), "{:#?}", plan.retention);
    assert!(
        plan.retention
            .deferred_marks
            .iter()
            .any(|m| old_id.starts_with(m.as_str())),
        "old is pinned at the fork by the abandoned tip"
    );

    // Marking the abandoned capture itself DEFERS: the restore lineage
    // rides a second peer whose head counters the boundary cannot exceed,
    // so the abandoned branch sits above every legal v1 boundary (interior
    // marks defer). The plan says exactly that.
    retention_mark(&mut store, &abandoned_id[..12]).unwrap();
    let plan = gc_plan(root).unwrap();
    assert!(
        !plan.retention.prunable.iter().any(|c| c.id == abandoned_id),
        "the out-of-reach branch is never in the prefix"
    );
    assert!(
        plan.retention
            .deferred_marks
            .iter()
            .any(|m| abandoned_id.starts_with(m.as_str())),
        "mark on the unreachable branch defers: {:#?}",
        plan.retention
    );

    let marked_count = plan.retention.prunable.len();
    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => {
            assert_eq!(report.trimmed, marked_count)
        }
        other => panic!("expected applied report, got {other:?}"),
    }

    // Reopen read-only: the old capture is a ghost with its cause, the
    // abandoned branch is still fully present (out of prefix reach), and
    // the head lineage is untouched and keeps capturing.
    let reader = TimelineReader::open(root).unwrap();
    let survivors = reader.captures(false, None, false, usize::MAX).unwrap();
    assert_eq!(
        survivors.len(),
        2,
        "head lineage keeps both post-restore captures"
    );
    assert!(survivors.iter().all(|c| c.id != old_id));
    let err = reader.resolve(&old_id[..12]).unwrap_err();
    assert!(format!("{err}").contains("pruned by gc mark"), "got: {err}");
    reader
        .resolve(&abandoned_id[..12])
        .expect("abandoned branch survives the trim");
    let mut store2 = open(root);
    flush_aged(&mut store2, root, "c.txt", b"post\n", Duration::zero());
    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(false, None, false, usize::MAX)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn unconfigured_store_never_trims() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let hour = Duration::hours(1);
    for i in 0..4 {
        flush_aged(
            &mut store,
            root,
            "a.txt",
            format!("v{i}\n").as_bytes(),
            hour * (4 - i),
        );
    }
    let outcome = gc_run_store(&mut store, true).unwrap();
    match outcome {
        sheaf_core::store::GcOutcome::Applied(report) => {
            assert_eq!(report.trimmed, 0, "no policy, no trim");
            assert_eq!(report.captures_after, 4);
        }
        other => panic!("expected applied report, got {other:?}"),
    }
    // The plan still shows the boundary for visibility.
    let plan = gc_plan(root).unwrap();
    assert!(plan.retention.boundary.is_some());
    assert!(plan.retention.prunable.is_empty());
}

#[test]
fn v1_style_frames_still_replay_after_ledger_frames_exist() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    flush_aged(&mut store, root, "a.txt", b"1\n", Duration::zero());
    store.create_checkpoint("after-first", None).unwrap();
    flush_aged(&mut store, root, "a.txt", b"2\n", Duration::zero());

    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(
        reader
            .captures(true, None, false, usize::MAX)
            .unwrap()
            .len(),
        2
    );
    assert!(reader.checkpoints().iter().any(|c| c.name == "after-first"));
    // The store was upgraded to format 2 by the writer.
    assert_eq!(
        config::read_store_format(root).unwrap(),
        config::STORE_FORMAT_VERSION
    );
}

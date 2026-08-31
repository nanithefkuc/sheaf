use chrono::{Local, TimeZone, Utc};
use loro::{Frontiers, ID};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{ProjectStore, StoreLimits, TimelineReader};
use std::path::Path;

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn capture(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, ms: i64) {
    std::fs::write(root.join(rel), text).unwrap();
    let at = Utc.timestamp_millis_opt(ms).single().unwrap();
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: vec![FsEvent::now(EventKind::Touched {
                path: root.join(rel).into(),
            })],
        })
        .unwrap();
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_segment_bytes: 64 << 20,
        snapshot_edit_size: 1000,
    }
}

#[test]
fn capture_info_compares_a_capture_to_its_exact_parent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    capture(&mut store, root, "a.txt", "one", 1_700_000_000_100);
    capture(&mut store, root, "b.txt", "two", 1_700_000_001_100);
    let tip = store.captures(false, None, false, 1).unwrap().remove(0);

    let info = store.capture_info(tip.short_id()).unwrap();
    assert_eq!(info.capture.id, tip.id);
    assert_eq!(info.diff.entries.len(), 1);
    assert_eq!(info.diff.entries[0].path, "b.txt");
    assert_eq!(
        info.diff.entries[0].kind,
        sheaf_core::store::DiffKind::Added
    );
}

#[test]
fn captures_are_stable_and_resolve_by_relative_time_and_path() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    capture(&mut store, root, "a.txt", "one", 1_700_000_000_100);
    capture(&mut store, root, "b.txt", "two", 1_700_000_001_100);
    capture(&mut store, root, "a.txt", "three", 1_700_000_002_100);

    let all = store.captures(false, None, false, 10).unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].paths, vec!["a.txt"]);
    assert_eq!(
        store.resolve("@~1").unwrap().capture_id.as_deref(),
        Some(all[1].id.as_str())
    );
    assert_eq!(
        store
            .resolve_at(1_700_000_001_500)
            .unwrap()
            .capture_id
            .as_deref(),
        Some(all[1].id.as_str())
    );
    let at = Utc
        .timestamp_millis_opt(1_700_000_001_500)
        .single()
        .unwrap()
        .to_rfc3339();
    assert_eq!(
        store.resolve(&at).unwrap().capture_id.as_deref(),
        Some(all[1].id.as_str())
    );
    let only_b = store
        .captures(false, Some(Path::new("b.txt")), false, 10)
        .unwrap();
    assert_eq!(only_b.len(), 1);
    assert_eq!(only_b[0].id, all[1].id);
    let first_ids: Vec<_> = all.iter().map(|c| c.id.clone()).collect();
    drop(store);

    // A stale manifest whose snapshot vanished must not prevent degraded
    // readers from replaying the still-present journal.
    let snapshots = root.join(".sheaf/store/snapshots");
    std::fs::create_dir_all(&snapshots).unwrap();
    std::fs::write(
        snapshots.join("snap-999999.manifest.json"),
        r#"{"snapshot":"missing.snapshot","covered_upto":999999}"#,
    )
    .unwrap();

    let reader = TimelineReader::open(root).unwrap();
    let reopened = reader.captures(false, None, false, 10).unwrap();
    assert_eq!(
        reopened.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
        first_ids
    );
    assert_eq!(
        reader.resolve(reopened[0].short_id()).unwrap().capture_id,
        Some(reopened[0].id.clone())
    );
    assert!(reader.resolve("deadbe").is_err());

    // A head pointer can outlive a torn, un-fsync'd journal tail. Read-only
    // history must fall back to the loaded oplog rather than become unusable.
    let unknown = sheaf_core::store::encode_frontier(&Frontiers::from_id(ID::new(999, 0)));
    std::fs::write(
        root.join(".sheaf/state/worktree.head"),
        serde_json::json!({"frontier": unknown}).to_string(),
    )
    .unwrap();
    let recovered = TimelineReader::open(root).unwrap();
    assert_eq!(recovered.captures(false, None, false, 10).unwrap().len(), 3);
}

#[test]
fn identical_same_second_batches_remain_distinct_captures() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let at = 1_700_000_000_100;
    capture(&mut store, root, "f.txt", "one", at);
    capture(&mut store, root, "f.txt", "two", at);
    capture(&mut store, root, "f.txt", "three", at);
    let captures = store.captures(false, None, false, 10).unwrap();
    assert_eq!(captures.len(), 3);
    assert_ne!(captures[0].id, captures[1].id);
    assert_ne!(captures[1].id, captures[2].id);
}

#[test]
fn checkpoints_bind_exact_frontiers_and_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    capture(&mut store, root, "f.txt", "one", 1_700_000_000_100);
    let first = store.captures(false, None, false, 10).unwrap()[0].clone();
    capture(&mut store, root, "f.txt", "two", 1_700_000_001_100);
    let cp = store
        .create_checkpoint("before-refactor", Some(first.short_id()))
        .unwrap();
    assert_eq!(cp.frontier, first.frontier);
    let at = Utc
        .timestamp_millis_opt(1_700_000_000_500)
        .single()
        .unwrap()
        .to_rfc3339();
    let by_time = store.create_checkpoint("by-time", Some(&at)).unwrap();
    assert_eq!(by_time.frontier, first.frontier);
    let duplicate = store
        .create_checkpoint("before-refactor", None)
        .unwrap_err();
    assert_eq!(duplicate.code(), "exists");
    store.compact().unwrap();
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let listed = reader.checkpoints();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].name, "before-refactor");
    assert_eq!(listed[1].name, "by-time");
    assert_eq!(
        reader
            .resolve("checkpoint:before-refactor")
            .unwrap()
            .frontier,
        first.frontier
    );
}

#[test]
fn checkout_then_edit_keeps_abandoned_future_reachable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    capture(&mut store, root, "f.txt", "one", 1_700_000_000_100);
    let first = store.captures(false, None, false, 10).unwrap()[0].clone();
    capture(&mut store, root, "f.txt", "old-two", 1_700_000_001_100);
    let abandoned = store.captures(false, None, false, 10).unwrap()[0].clone();

    store.checkout_for_branch(&first.frontier).unwrap();
    capture(&mut store, root, "f.txt", "new-two", 1_700_000_002_100);
    let divergent = store.captures(false, None, false, 10).unwrap()[0].clone();
    assert_ne!(divergent.id, abandoned.id);
    assert_eq!(store.branch_tips().unwrap().len(), 2);
    assert_eq!(
        store.resolve(abandoned.short_id()).unwrap().frontier,
        abandoned.frontier
    );
    // Timestamp search stays on the selected lineage and therefore does not
    // pick the abandoned capture even though its wall time is eligible.
    assert_eq!(
        store.resolve_at(1_700_000_001_500).unwrap().frontier,
        first.frontier
    );
    drop(store);

    let reopened_store = ProjectStore::open(root, limits()).unwrap();
    let lineage = reopened_store.captures(false, None, false, 10).unwrap();
    assert!(lineage.iter().any(|c| c.id == divergent.id));
    assert!(!lineage.iter().any(|c| c.id == abandoned.id));
    drop(reopened_store);

    let reader = TimelineReader::open(root).unwrap();
    assert_eq!(reader.branch_tips().unwrap().len(), 2);
    let all = reader.captures(true, None, false, 10).unwrap();
    assert!(all.iter().any(|c| c.id == abandoned.id));
    assert!(all.iter().any(|c| c.id == divergent.id));
}

#[test]
fn follow_sees_a_paths_former_names() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();

    let at = |ms| chrono::Utc.timestamp_millis_opt(ms).single().unwrap();
    let rename = |store: &mut ProjectStore, root: &Path, from: &str, to: &str| {
        store
            .apply_batch(&Batch {
                root: root.to_path_buf(),
                started_at: at(1_700_000_000_000),
                flushed_at: at(1_700_000_000_000),
                events: vec![FsEvent::now(EventKind::Renamed {
                    from: root.join(from),
                    to: root.join(to),
                })],
            })
            .unwrap();
    };

    std::fs::create_dir_all(root.join("src")).unwrap();
    capture(&mut store, root, "src/old.rs", "one\n", 1_700_000_000_100);
    rename(&mut store, root, "src/old.rs", "src/new.rs");
    capture(&mut store, root, "src/new.rs", "two\n", 1_700_000_002_100);

    // Without follow, the pre-rename capture under the old name is invisible.
    let plain = store
        .captures(false, Some(Path::new("src/new.rs")), false, 10)
        .unwrap();
    assert_eq!(plain.len(), 2, "rename capture + later edit");

    // With follow, the whole lineage of the path's names appears.
    let followed = store
        .captures(false, Some(Path::new("src/new.rs")), true, 10)
        .unwrap();
    assert_eq!(followed.len(), 3, "old-name captures included");
    assert!(followed.iter().all(|c| c.on_current));

    // Degraded readers follow too.
    drop(store);
    let reader = TimelineReader::open(root).unwrap();
    let followed = reader
        .captures(false, Some(Path::new("src/new.rs")), true, 10)
        .unwrap();
    assert_eq!(followed.len(), 3);
}

#[test]
fn all_branch_view_marks_the_current_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore =
        sheaf_core::ignore::IgnoreSet::from_patterns(&sheaf_core::config::default_patterns())
            .unwrap();
    let mut store = ProjectStore::open(root, limits()).unwrap();

    capture(&mut store, root, "a.txt", "base\n", 1_700_000_000_100);
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    capture(
        &mut store,
        root,
        "a.txt",
        "abandoned future\n",
        1_700_000_001_100,
    );
    let abandoned = store.captures(false, None, false, 1).unwrap()[0].clone();

    // Roll back and diverge: `abandoned` is now off the live lineage.
    let plan = store.plan_restore(&base.id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    std::fs::write(root.join("a.txt"), "new future\n").unwrap();
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    let divergent = store.captures(false, None, false, 1).unwrap()[0].clone();

    let all = store.captures(true, None, false, 10).unwrap();
    assert_eq!(all.len(), 3, "base + abandoned future + divergent future");
    let by_id = |id: &str| all.iter().find(|c| c.id == id).unwrap();
    assert!(by_id(&base.id).on_current, "shared ancestors stay current");
    assert!(by_id(&divergent.id).on_current);
    assert!(
        !by_id(&abandoned.id).on_current,
        "the abandoned future must be marked divergent"
    );
}

#[test]
fn checkpoints_carry_timestamps_and_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore =
        sheaf_core::ignore::IgnoreSet::from_patterns(&sheaf_core::config::default_patterns())
            .unwrap();
    let mut store = ProjectStore::open(root, limits()).unwrap();

    capture(&mut store, root, "a.txt", "base\n", 1_700_000_000_100);
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    store
        .create_checkpoint("before-wreck", Some(base.short_id()))
        .unwrap();
    capture(&mut store, root, "a.txt", "wrecked\n", 1_700_000_001_100);
    store.create_checkpoint("after-wreck", None).unwrap();

    // Roll back past after-wreck: its capture leaves the live lineage but
    // the checkpoint stays resolvable and says where it sits.
    let plan = store.plan_restore(&base.id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();

    let cps = store.checkpoints();
    let before = cps.iter().find(|c| c.name == "before-wreck").unwrap();
    assert_eq!(before.timestamp_ms, Some(1_700_000_000_100));
    assert!(before.on_current);
    let after = cps.iter().find(|c| c.name == "after-wreck").unwrap();
    assert_eq!(after.timestamp_ms, Some(1_700_000_001_100));
    assert!(!after.on_current, "pinned into the abandoned future");

    // Log entries carry labels at the exact capture, including a label on an
    // abandoned branch when the caller asks for the complete graph.
    let logged = store.captures(true, None, false, 10).unwrap();
    assert!(logged
        .iter()
        .any(|entry| entry.id == base.id && entry.checkpoints == ["before-wreck"]));
    assert!(logged
        .iter()
        .any(|entry| entry.id == after.capture_id.clone().unwrap()
            && entry.checkpoints == ["after-wreck"]));

    // The off-lineage checkpoint still resolves and restores fine.
    let plan = store
        .plan_restore("checkpoint:after-wreck", &[], &ignore)
        .unwrap();
    assert_eq!(plan.target.frontier, after.frontier);
}

#[test]
fn checkpoint_names_allow_spaces_and_resolve_by_bare_name() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    capture(&mut store, root, "f.txt", "one", 1_700_000_000_100);
    let first = store.captures(false, None, false, 10).unwrap()[0].clone();
    capture(&mut store, root, "f.txt", "two", 1_700_000_001_100);

    // B2: a human-readable label with spaces is accepted.
    let cp = store
        .create_checkpoint("before refactoring", Some(first.short_id()))
        .unwrap();
    assert_eq!(cp.frontier, first.frontier);

    // B3: a bare checkpoint name resolves like `checkpoint:<name>`.
    assert_eq!(
        store.resolve("before refactoring").unwrap().frontier,
        first.frontier
    );
    assert_eq!(
        store
            .resolve("checkpoint:before refactoring")
            .unwrap()
            .frontier,
        first.frontier
    );

    // Control characters (newline/tab) and surrounding whitespace stay rejected
    // so a label can never corrupt a line-oriented listing.
    assert!(store.create_checkpoint("bad\nname", None).is_err());
    assert!(store.create_checkpoint(" leading", None).is_err());
    assert!(store.create_checkpoint("trailing ", None).is_err());
}

#[test]
fn relative_duration_and_clock_time_references_resolve() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();

    // Anchor captures at known offsets from "now" so the relative and
    // today-clock forms have a deterministic answer.
    let now = Utc::now();
    let three_h_ago = (now - chrono::Duration::hours(3)).timestamp_millis();
    let one_h_ago = (now - chrono::Duration::hours(1)).timestamp_millis();
    capture(&mut store, root, "old.txt", "old", three_h_ago);
    let old = store.captures(false, None, false, 10).unwrap()[0].clone();
    capture(&mut store, root, "new.txt", "new", one_h_ago);
    let newer = store.captures(false, None, false, 10).unwrap()[0].clone();

    // B4: `@~2h` picks the latest capture at-or-before two hours ago (the
    // three-hours-ago one), while `@~30m` picks the one-hour-ago capture.
    assert_eq!(
        store.resolve("@~2h").unwrap().capture_id.as_deref(),
        Some(old.id.as_str())
    );
    assert_eq!(
        store.resolve("@~30m").unwrap().capture_id.as_deref(),
        Some(newer.id.as_str())
    );
    // An integer tail is still N-captures-back, not a duration.
    assert_eq!(
        store.resolve("@~1").unwrap().capture_id.as_deref(),
        Some(old.id.as_str())
    );

    // B5: a bare clock is that time *today*, local. A capture made a moment
    // ago resolves via a clock time a minute in the past.
    let clock = Local::now()
        .checked_sub_signed(chrono::Duration::minutes(1))
        .unwrap()
        .format("%H:%M")
        .to_string();
    // Only assert when "a minute ago" is still today (skip within the first
    // minute after local midnight, where the clock would roll to yesterday).
    if Local::now().format("%H:%M").to_string() >= clock {
        assert_eq!(
            store.resolve(&clock).unwrap().capture_id.as_deref(),
            Some(newer.id.as_str())
        );
    }
}

#[test]
fn timezoneless_t_datetime_reference_resolves() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();

    // A capture at a known LOCAL wall-clock instant, addressed by the
    // README's timezone-less `T` form (B6), interpreted in local time.
    let local_dt = Local
        .with_ymd_and_hms(2026, 8, 27, 10, 30, 0)
        .single()
        .unwrap();
    capture(
        &mut store,
        root,
        "f.txt",
        "one",
        local_dt.timestamp_millis(),
    );
    let cap = store.captures(false, None, false, 10).unwrap()[0].clone();

    assert_eq!(
        store
            .resolve("2026-08-27T10:30")
            .unwrap()
            .capture_id
            .as_deref(),
        Some(cap.id.as_str())
    );
    assert_eq!(
        store
            .resolve("2026-08-27T10:30:00")
            .unwrap()
            .capture_id
            .as_deref(),
        Some(cap.id.as_str())
    );
    // The space-separated local form keeps working too.
    assert_eq!(
        store
            .resolve("2026-08-27 10:30")
            .unwrap()
            .capture_id
            .as_deref(),
        Some(cap.id.as_str())
    );
}

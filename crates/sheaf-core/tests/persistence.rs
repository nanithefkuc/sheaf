//! Persistence acceptance:
//! 1. A churned worktree round-trips BYTE-EXACT through CRDT ops alone
//!    (char-splices for text, content-addressed chunks for binaries).
//! 2. Kill -9 mid-write loses at most the open window, never the store —
//!    modeled faithfully by truncating journal tails.
//! 3. Rotation keeps ordering; snapshot+prune stays recoverable.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sheaf_core::config::{self, default_patterns};
use sheaf_core::events::{Batch, EventKind};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{ProjectStore, StoreLimits};

fn ev(kind: EventKind) -> sheaf_core::events::FsEvent {
    sheaf_core::events::FsEvent::now(kind)
}

fn added(root: &Path, rel: &str) -> EventKind {
    EventKind::Added {
        path: root.join(rel),
    }
}
fn touched(root: &Path, rel: &str) -> EventKind {
    EventKind::Touched {
        path: root.join(rel).into(),
    }
}
fn renamed(root: &Path, from: &str, to: &str) -> EventKind {
    EventKind::Renamed {
        from: root.join(from),
        to: root.join(to),
    }
}
fn removed(root: &Path, rel: &str) -> EventKind {
    EventKind::Removed {
        path: root.join(rel),
    }
}

/// Recursive rel-path ⇒ bytes map, skipping the store itself.
fn tree_map(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    for e in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let p = e.path();
        let rel = p.strip_prefix(root).unwrap().to_path_buf();
        if rel.starts_with(".sheaf") {
            continue;
        }
        out.insert(rel, std::fs::read(p).unwrap());
    }
    out
}

fn limits(max_segment_bytes: u64, every: u64) -> StoreLimits {
    StoreLimits {
        max_segment_bytes,
        snapshot_edit_size: every,
    }
}

#[test]
fn byte_exact_roundtrip_through_ops_alone() {
    let base = tempfile::tempdir().unwrap();
    let src = base.path().join("proj");
    let rec = base.path().join("reconstructed");
    std::fs::create_dir_all(&src).unwrap();
    // Fresh store skeleton so open() passes format checks.
    write_store_skeleton(&src);

    let ig = IgnoreSet::from_patterns(&default_patterns()).unwrap();
    let _ = ig; // watcher-level concern; store trusts batches

    {
        let mut store = ProjectStore::open(&src, limits(64 << 20, 1000)).unwrap();

        // ---- burst 1: files appear ------------------------------------
        std::fs::write(src.join("readme.md"), "# Hello\n\nworld\n").unwrap();
        let unicode = "héllo ünïcode 🌍 — em-dash — 日本語テキスト!";
        std::fs::write(src.join("notes.txt"), unicode).unwrap();
        let blob_a = vec![0xFFu8, 0x9F, 146, 150, 255, 0, 7]; // invalid UTF-8 by construction
        std::fs::write(src.join("img.bin"), &blob_a).unwrap();
        std::fs::write(src.join("dup.bin"), &blob_a).unwrap(); // same content ⇒ one physical blob
        store
            .apply_batch(&Batch {
                root: src.clone(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
                events: vec![
                    ev(added(&src, "readme.md")),
                    ev(touched(&src, "notes.txt")),
                    ev(touched(&src, "img.bin")),
                    ev(touched(&src, "dup.bin")),
                ],
            })
            .unwrap();
        drop(store);

        // ---- burst 2: editor-style edits incl. multibyte middle changes -
        let mut store = ProjectStore::open(&src, limits(64 << 20, 1000)).unwrap();
        let unicode2 = "héllo ünïcode 🌊🌊 — em-dash — 日本語 replaced段落!";
        std::fs::write(src.join("notes.txt"), unicode2).unwrap();
        let big = "x".repeat(4096) + "\n🌍 tail";
        std::fs::write(src.join("big.log"), &big).unwrap();
        store
            .apply_batch(&Batch {
                root: src.clone(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
                events: vec![ev(touched(&src, "notes.txt")), ev(added(&src, "big.log"))],
            })
            .unwrap();
        drop(store);

        // ---- burst 3: renames across dirs + binary move -----------------
        let mut store = ProjectStore::open(&src, limits(64 << 20, 1000)).unwrap();
        std::fs::create_dir_all(src.join("docs/deep")).unwrap();
        std::fs::rename(
            src.join("notes.txt"),
            src.join("docs/deep/notes-renamed.md"),
        )
        .unwrap();
        std::fs::rename(src.join("img.bin"), src.join("docs/moved-img.bin")).unwrap();
        store
            .apply_batch(&Batch {
                root: src.clone(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
                events: vec![
                    ev(renamed(&src, "notes.txt", "docs/deep/notes-renamed.md")),
                    ev(renamed(&src, "img.bin", "docs/moved-img.bin")),
                ],
            })
            .unwrap();
        drop(store);

        // ---- burst 4: deletions mixed types + vanished-mid-window -------
        let mut store = ProjectStore::open(&src, limits(64 << 20, 1000)).unwrap();
        std::fs::remove_file(src.join("docs/moved-img.bin")).unwrap();
        std::fs::remove_file(src.join("big.log")).unwrap();
        std::fs::write(src.join("ephemeral.txt"), b"gone soon").unwrap();
        std::fs::remove_file(src.join("ephemeral.txt")).unwrap(); // died before flush
        store
            .apply_batch(&Batch {
                root: src.clone(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
                events: vec![
                    ev(touched(&src, "ephemeral.txt")), // upsert sees gone=true path
                    ev(removed(&src, "docs/moved-img.bin")),
                    ev(removed(&src, "big.log")),
                ],
            })
            .unwrap();
    } // store dropped; nothing else touches disk after

    // ---- recovery from journal alone, then byte-exact comparison -------
    let store_final = ProjectStore::open(&src, limits(64 << 20, 1000)).unwrap();
    std::fs::create_dir_all(&rec).unwrap();
    let n = store_final.materialize(&rec).unwrap();
    assert_eq!(
        n,
        tree_map(&src).len(),
        "materialized count must match live tree"
    );

    let expect = tree_map(&src);
    let got = tree_map(&rec);
    let expected_keys: Vec<_> = expect.keys().cloned().collect();
    let got_keys: Vec<_> = got.keys().cloned().collect();
    assert_eq!(expected_keys, got_keys, "file SETS must match byte-exactly");

    for (k, want) in &expect {
        let have = &got[k];
        assert_eq!(want, have, "content mismatch for {}", k.display());
    }
}

/// Optimization-free skeleton helper mirroring what `sheaf init` writes.
fn write_store_skeleton(root: &Path) {
    let d = root.join(".sheaf");
    std::fs::create_dir_all(d.join("store")).unwrap();
    std::fs::write(d.join("config.toml"), config::render_default()).unwrap();
}

#[test]
fn torn_tail_loses_only_the_open_window() {
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);

    let mk_batch = |text: &str| -> Batch {
        std::fs::write(proj.join("f.txt"), text).unwrap();
        Batch {
            root: proj.clone(),
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
            events: vec![ev(touched(&proj, "f.txt"))],
        }
    };

    {
        let mut s = ProjectStore::open(&proj, limits(64 << 20, 1000)).unwrap();
        s.apply_batch(&mk_batch("version-one-content")).unwrap();
        s.apply_batch(&mk_batch("version-two-longer-content!!"))
            .unwrap();
        s.apply_batch(&mk_batch("version-threeFINAL")).unwrap(); // this frame will be mangled
    }

    // Simulate kill -9 mid-fsync: chop bytes off the sole segment's end.
    let seg = walkdir::WalkDir::new(proj.join(".sheaf/store/journal"))
        .into_iter()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with(".op"))
        .expect("one segment")
        .path()
        .to_path_buf();
    let len = std::fs::metadata(&seg).unwrap().len();
    {
        let f = std::fs::File::options().write(true).open(&seg).unwrap();
        f.set_len(len - 9).unwrap();
    }

    // Recovery: loader drops the torn tail; committed history stands...
    let mut recovered = ProjectStore::open(&proj, limits(64 << 20, 1000)).unwrap();

    // ...and further writes append cleanly onto the recovered journal.
    recovered
        .apply_batch(&mk_batch("post-crash-write"))
        .unwrap();

    // Reopen once more: post-crash delta replayed; doc state coherent.
    let check = ProjectStore::open(&proj, limits(64 << 20, 1000)).unwrap();
    let verify_tmp = tempfile::tempdir().unwrap();
    check.materialize(verify_tmp.path()).unwrap();
    let rebuilt = std::fs::read_to_string(verify_tmp.path().join("f.txt")).unwrap();
    assert_eq!(rebuilt, "post-crash-write");
}

#[test]
fn rotation_orders_segments_and_snapshot_prune_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);

    // Tiny thresholds force several rotations AND a couple compactions.
    let mut s = ProjectStore::open(&proj, limits(400, 4)).unwrap();
    for i in 0..10u32 {
        let body = format!("payload-{i}-{}", "z".repeat(120));
        std::fs::write(proj.join(format!("gen{i}.txt")), &body).unwrap();
        s.apply_batch(&Batch {
            root: proj.clone(),
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
            events: vec![ev(added(&proj, &format!("gen{i}.txt")))],
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    drop(s);

    // Segments survived in bounded numbers thanks to pruning.
    let segs: usize = walkdir::WalkDir::new(proj.join(".sheaf/store/journal"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && e.file_name().to_string_lossy().ends_with(".op"))
        .count();
    assert!(
        (1..10).contains(&segs),
        "rotation created many, prune trimmed some ({segs})"
    );
    assert!(
        walkdir::WalkDir::new(proj.join(".sheaf/store/snapshots"))
            .into_iter()
            .any(|e| e
                .ok()
                .is_some_and(|e| { e.file_name().to_string_lossy().ends_with(".manifest.json") })),
        "compaction manifests exist"
    );

    // Fresh open reconstructs purely from snapshot(s)+tail — final state intact.
    let fresh = ProjectStore::open(&proj, limits(400, 4)).unwrap();
    let target = tempfile::tempdir().unwrap();
    fresh.materialize(target.path()).unwrap();
    for i in 0..10u32 {
        let p = target.path().join(format!("gen{i}.txt"));
        let body = std::fs::read_to_string(&p)
            .unwrap_or_else(|e| panic!("{p:?} missing after recovery: {e}"));
        assert_eq!(body, format!("payload-{i}-{}", "z".repeat(120)));
    }
}

#[test]
fn duplicate_binaries_share_one_blob_file() {
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);
    let mut payload = [0x41u8; 512];
    payload[0] = 0xFF;
    payload[1] = 0xFE; // force binary classification

    let mut s = ProjectStore::open(&proj, limits(64 << 20, 1000)).unwrap();
    for name in ["a.bin", "b.bin"] {
        std::fs::write(proj.join(name), payload).unwrap();
    }
    s.apply_batch(&Batch {
        root: proj.clone(),
        started_at: chrono::Utc::now(),
        flushed_at: chrono::Utc::now(),
        events: vec![ev(touched(&proj, "a.bin")), ev(touched(&proj, "b.bin"))],
    })
    .unwrap();
    let blobs = walkdir::WalkDir::new(proj.join(".sheaf/store/blobs"))
        .into_iter()
        .filter(|e| e.as_ref().ok().is_some_and(|e| e.file_type().is_file()))
        .count();
    assert_eq!(blobs, 1, "identical payloads must share storage");
}

#[test]
fn snapshot_cadence_follows_total_edits_across_reopens() {
    // The cadence is `num_edits % snapshot_edit_size == 0` over the TOTAL
    // edits persisted to the store — never a per-process counter. A writer
    // that reopens between every edit (a daemon restarted on each dev-loop
    // reinstall) must still compact exactly on multiples of the interval.
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);

    let edit = |i: u32| -> Batch {
        std::fs::write(proj.join("f.txt"), format!("body-{i}")).unwrap();
        Batch {
            root: proj.clone(),
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
            events: vec![ev(touched(&proj, "f.txt"))],
        }
    };

    // Fresh open per edit: 9 edits at interval 4 ⇒ compactions at totals
    // 4 and 8, and nowhere else.
    let mut compacted_at = Vec::new();
    for i in 0..9u32 {
        let mut s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
        let outcome = s.apply_batch(&edit(i)).unwrap();
        if outcome.snapshotted {
            compacted_at.push(i + 1);
        }
    }
    assert_eq!(compacted_at, vec![4, 8], "cadence must key off total edits");

    // The persisted total survives out-of-band compaction unchanged: a
    // manual compact between multiples leaves the next cadence fire at
    // the next multiple (12), never early.
    {
        let mut s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
        s.compact().unwrap(); // num_edits is 9; this is not a cadence fire
    }
    for i in 9..12u32 {
        let mut s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
        let outcome = s.apply_batch(&edit(i)).unwrap();
        if outcome.snapshotted {
            compacted_at.push(i + 1);
        }
    }
    assert_eq!(compacted_at, vec![4, 8, 12]);

    // Reopen reconstructs the exact total: manifest baseline + replayed
    // tail (edit 12's compaction wrote total_edits=12, tail is empty).
    let s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
    assert_eq!(s.num_edits(), 12);

    // ...and with a non-empty tail: one more edit replays back as 13.
    drop(s);
    let mut s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
    s.apply_batch(&edit(12)).unwrap();
    drop(s);
    let s = ProjectStore::open(&proj, limits(64 << 20, 4)).unwrap();
    assert_eq!(s.num_edits(), 13);
}

#[test]
fn config_specified_snapshot_edit_size_drives_cadence() {
    // The `[store]` section of config.toml is the user-facing knob: a
    // project that sets snapshot_edit_size there gets that cadence through
    // the daemon's open path (config::load → StoreLimits → open), not just
    // through test-constructed limits.
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);
    std::fs::write(
        proj.join(".sheaf/config.toml"),
        "format_version = 2\n\n[store]\nsnapshot_edit_size = 3\n[ignore]\npatterns = []\n",
    )
    .unwrap();

    let limits = config::load(&proj).unwrap().store;
    assert_eq!(limits.snapshot_edit_size, 3);

    let edit = |i: u32| -> Batch {
        std::fs::write(proj.join("f.txt"), format!("v{i}")).unwrap();
        Batch {
            root: proj.clone(),
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
            events: vec![ev(touched(&proj, "f.txt"))],
        }
    };
    let mut compacted = 0;
    for i in 0..3u32 {
        let mut s = ProjectStore::open(&proj, limits.clone()).unwrap();
        if s.apply_batch(&edit(i)).unwrap().snapshotted {
            compacted += 1;
        }
    }
    assert_eq!(
        compacted, 1,
        "config's snapshot_edit_size=3 must fire at edit 3"
    );
}

#[test]
fn zero_snapshot_edit_size_disables_cadence_without_panicking() {
    let tmp = tempfile::tempdir().unwrap();
    let proj = tmp.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write_store_skeleton(&proj);

    let mut s = ProjectStore::open(&proj, limits(64 << 20, 0)).unwrap();
    for i in 0..5u32 {
        std::fs::write(proj.join("f.txt"), format!("body-{i}")).unwrap();
        let outcome = s
            .apply_batch(&Batch {
                root: proj.clone(),
                started_at: chrono::Utc::now(),
                flushed_at: chrono::Utc::now(),
                events: vec![ev(touched(&proj, "f.txt"))],
            })
            .unwrap();
        assert!(!outcome.snapshotted, "disabled cadence must never fire");
    }
}

//! Pinpoint repro: mkdir + immediate mv-into-it within one poll window.

use std::sync::mpsc::channel;
use std::time::Duration;

use sheaf_core::classify::Classifier;
use sheaf_core::config::default_patterns;
use sheaf_core::watcher::{default_backend, new_stop_flag, shared_classifier};

#[test]
fn same_poll_window_mkdir_then_move_in() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();

    std::thread::sleep(Duration::from_millis(50)); // watcher will start below

    let classifier = Classifier::from_volatile_patterns(&default_patterns()).unwrap();
    let backend = default_backend(root.clone(), shared_classifier(classifier)).unwrap();
    let (tx, rx) = channel::<sheaf_core::events::FsEvent>();
    let stop = new_stop_flag();
    let stop2 = stop.clone();
    let h = std::thread::spawn(move || backend.run(tx, stop2));

    // Baseline settle
    std::thread::sleep(Duration::from_millis(120));

    // Write+create first file (sanity: proves pipeline alive)
    std::fs::write(root.join("alpha.txt"), b"x").unwrap();

    // THE SCENARIO: mkdir then IMMEDIATELY populate via move (µs apart).
    std::fs::create_dir(root.join("sub")).unwrap();
    std::fs::rename(root.join("alpha.txt"), root.join("sub/beta.txt")).unwrap();

    // Collect for a healthy stretch.
    let deadline = std::time::Instant::now() + Duration::from_millis(1000);
    let mut seen = Vec::new();
    while std::time::Instant::now() < deadline {
        while let Ok(ev) = rx.try_recv() {
            seen.push(format!("{:?}", ev.kind));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = h.join();

    eprintln!(
        "=== EVENTS SEEN ===\n{}\n===================",
        seen.join("\n")
    );
    // Structural completeness under the registration-gap race:
    // - same-name cross-dir moves within one poll window pair as Renamed;
    // - differing names may legitimately decompose into Removed+Added —
    //   never missing, never duplicated. Both are accepted here.
    let has_renamed = seen.iter().any(|s| s.contains("Renamed"));
    let src_gone = seen
        .iter()
        .any(|s| s.contains("Removed") && s.contains("alpha.txt"));
    let dst_seen = seen
        .iter()
        .any(|s| s.contains("beta.txt") && !s.contains("Removed"));
    assert!(
        has_renamed || (src_gone && dst_seen),
        "move must be structurally represented; saw:\n{seen:?}"
    );
}

//! Control experiment for the P3 retention redesign: can a shallow snapshot
//! serve as the physical trim primitive for sheaf's capture timeline?
//!
//! Simulates the store's exact writer discipline (persistent peer, merge
//! interval 0, detached editing, one commit_with per capture, per-flush
//! `export(updates(&last_vv))` journal frames), then probes:
//!
//!  A. prefix trim    — shallow export at a mid-history protected frontier:
//!     which captures survive, is the boundary addressable, are pre-boundary
//!     points really gone, can editing continue and later deltas still be
//!     exchanged with a peer holding the same shallow snapshot?
//!  B. stale deltas   — importing a PRE-boundary journal frame into a shallow
//!     doc (must fail or pend: old segments have to die at compaction).
//!  C. fork_at        — the checkpoints_from/path_names pattern on a shallow
//!     doc (fork at tip, read a meta-map label).
//!  D. multi-branch   — protecting two divergent tips with ONE shallow export
//!     (Frontiers::push of both heads): where does the boundary land (the
//!     meet?), and does everything before it disappear?

// The store itself still uses get_or_create_container (TODO(sync-era));
// the probe mirrors the store's calls exactly.
#![allow(deprecated)]

use std::ops::ControlFlow;

use loro::{CommitOptions, ExportMode, Frontiers, LoroDoc, LoroText, VersionVector};

const CAP: &str = "sheaf:capture:v1:";

fn commit_capture(doc: &LoroDoc, label: &str, payload: &str) -> Frontiers {
    let text = doc
        .get_map("files")
        .get_or_create_container("a.txt", LoroText::new())
        .unwrap();
    let cur = text.to_string();
    if cur.is_empty() {
        text.insert(0, payload).unwrap();
    } else {
        text.insert(cur.len(), payload).unwrap();
    }
    let list = doc.get_list("tree_events");
    list.insert(list.len(), r#"{"kind":"touched","path":"a.txt"}"#)
        .unwrap();
    doc.commit_with(
        CommitOptions::new()
            .origin("sheaf")
            .timestamp(1_700_000_000)
            .commit_msg(&format!(
                "{CAP}{{\"at_ms\":0,\"paths\":[\"a.txt\"],\"label\":\"{label}\"}}"
            )),
    );
    doc.state_frontiers()
}

fn capture_messages(doc: &LoroDoc, from: &Frontiers) -> Vec<String> {
    let mut msgs = Vec::new();
    let ids: Vec<_> = from.iter().collect();
    let _ = doc.travel_change_ancestors(&ids, &mut |change| {
        if let Some(m) = change.message.as_deref().and_then(|m| m.strip_prefix(CAP)) {
            {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                    msgs.push(v["label"].as_str().unwrap_or("?").to_string());
                }
            }
        }
        ControlFlow::Continue(())
    });
    msgs
}

fn vr_summary(v: &loro::VersionRange) -> String {
    let mut parts: Vec<String> = v.iter().map(|(p, (s, e))| format!("{p}:{s}-{e}")).collect();
    parts.sort();
    if parts.is_empty() {
        "(empty)".into()
    } else {
        parts.join(",")
    }
}

fn main() {
    // ---- writer: 5 captures, journal frames captured per flush -----------
    let doc = LoroDoc::new();
    doc.set_peer_id(111).unwrap();
    doc.set_change_merge_interval(0);
    doc.set_detached_editing(true);
    doc.get_map("_sheaf.meta").insert("format", "1").unwrap();

    let mut last_vv = VersionVector::default();
    let mut frames: Vec<(String, Vec<u8>)> = Vec::new();
    let mut caps: Vec<(String, Frontiers)> = Vec::new();
    for (i, label) in ["c1", "c2", "c3", "c4", "c5"].iter().enumerate() {
        let f = commit_capture(&doc, label, &format!("line-{i}\n"));
        let delta = doc.export(ExportMode::updates(&last_vv)).unwrap();
        frames.push((label.to_string(), delta));
        last_vv = doc.oplog_vv();
        caps.push((label.to_string(), f));
    }
    let [c1, c2, c3, c4, c5] = [&caps[0].1, &caps[1].1, &caps[2].1, &caps[3].1, &caps[4].1];
    let full_text = doc
        .get_map("files")
        .get_or_create_container("a.txt", LoroText::new())
        .unwrap()
        .to_string();

    // Baseline: a fresh doc replaying the frames sees all five captures.
    let replay = LoroDoc::new();
    replay.set_peer_id(999).unwrap();
    for (_, bytes) in &frames {
        let st = replay.import(bytes).unwrap();
        assert!(st.pending.is_none(), "replay frame must import cleanly");
    }
    println!(
        "baseline: captures seen from tip = {:?}",
        capture_messages(&replay, &replay.oplog_frontiers())
    );

    // ---- A. prefix trim at c2 (protect c2..c5, reclaim c1) ---------------
    println!("\n== A. shallow export at c2 ==");
    let shallow = doc.export(ExportMode::shallow_snapshot(c2)).unwrap();
    let d2 = LoroDoc::new();
    d2.set_peer_id(222).unwrap();
    d2.set_change_merge_interval(0);
    d2.set_detached_editing(true);
    let st = d2.import(&shallow).unwrap();
    println!(
        "import: success={} pending={}",
        vr_summary(&st.success),
        st.pending
            .map(|r| vr_summary(&r))
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "is_shallow={} since={:?}",
        d2.is_shallow(),
        d2.shallow_since_frontiers()
    );
    println!(
        "captures from tip = {:?}",
        capture_messages(&d2, &d2.oplog_frontiers())
    );
    println!(
        "state text intact = {}",
        d2.get_map("files")
            .get_or_create_container("a.txt", LoroText::new())
            .unwrap()
            .to_string()
            == full_text
    );
    println!(
        "addressable: c1={} c2={} c5={}",
        d2.frontiers_to_vv(c1).is_some(),
        d2.frontiers_to_vv(c2).is_some(),
        d2.frontiers_to_vv(c5).is_some(),
    );

    // Continue the flush loop on the shallow doc exactly as the daemon does.
    let vv0 = d2.oplog_vv();
    println!("vv at shallow open (peer -> counter): {vv0:?}");
    d2.checkout(c2).unwrap();
    d2.set_detached_editing(true);
    let c6 = commit_capture(&d2, "c6", "post-trim\n");
    let delta6 = d2.export(ExportMode::updates(&vv0)).unwrap();

    let d3 = LoroDoc::new();
    d3.set_peer_id(333).unwrap();
    d3.import(&shallow).unwrap();
    let st3 = d3.import(&delta6).unwrap();
    println!(
        "post-trim delta into shallow peer: success={} pending={}",
        vr_summary(&st3.success),
        st3.pending
            .map(|r| vr_summary(&r))
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "captures from tip = {:?}",
        capture_messages(&d3, &d3.oplog_frontiers())
    );
    println!(
        "c6 addressable on d3 = {}",
        d3.frontiers_to_vv(&c6).is_some()
    );

    // ---- B. stale pre-boundary frame into a shallow doc -------------------
    println!("\n== B. pre-boundary journal frame into shallow doc ==");
    let (label, bytes) = &frames[0];
    match d2.import(bytes) {
        Ok(st) => println!(
            "frame {label}: success={} pending={}",
            vr_summary(&st.success),
            st.pending
                .map(|r| vr_summary(&r))
                .unwrap_or_else(|| "none".into())
        ),
        Err(e) => println!("frame {label}: ERROR {e}"),
    }

    // ---- C. fork_at at the shallow tip (checkpoints_from pattern) --------
    println!("\n== C. fork_at on shallow doc ==");
    match d2.fork_at(&d2.oplog_frontiers()) {
        Ok(f) => println!("fork ok; is_shallow={}", f.is_shallow()),
        Err(e) => println!("fork FAILED: {e}"),
    }

    // ---- D. multi-branch protection with one shallow export --------------
    println!("\n== D. divergent tips, single shallow export ==");
    // Branch from c3 on the ORIGINAL full doc: checkout + edit + commit.
    doc.checkout(c3).unwrap();
    doc.set_detached_editing(true);
    let c4b = commit_capture(&doc, "c4b", "branched\n");
    let mut protected = c4.clone();
    protected.push(c4b.iter().next().unwrap());
    println!(
        "protected set: len={} (c5 head + c4b head)",
        protected.len()
    );
    let multi = doc
        .export(ExportMode::shallow_snapshot(&protected))
        .unwrap();
    let d4 = LoroDoc::new();
    d4.set_peer_id(444).unwrap();
    let st = d4.import(&multi).unwrap();
    println!(
        "import: success={} pending={}",
        vr_summary(&st.success),
        st.pending
            .map(|r| vr_summary(&r))
            .unwrap_or_else(|| "none".into())
    );
    println!(
        "is_shallow={} since-frontiers={:?}",
        d4.is_shallow(),
        d4.shallow_since_frontiers()
    );
    println!(
        "captures from tip = {:?}",
        capture_messages(&d4, &d4.oplog_frontiers())
    );
    println!(
        "addressable: c1={} c2={} c3={} c4={} c5={} c4b={}",
        d4.frontiers_to_vv(c1).is_some(),
        d4.frontiers_to_vv(c2).is_some(),
        d4.frontiers_to_vv(c3).is_some(),
        d4.frontiers_to_vv(c4).is_some(),
        d4.frontiers_to_vv(c5).is_some(),
        d4.frontiers_to_vv(&c4b).is_some(),
    );
    let _ = c1;
}

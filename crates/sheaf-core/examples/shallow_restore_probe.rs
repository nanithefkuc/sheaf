//! Follow-up probe for P3: the restore engine reads entries at arbitrary
//! frontiers through `fork_at`, which is NOT IMPLEMENTED on shallow docs
//! (retention_probe finding C). This probe tests the candidate replacement —
//! `ExportMode::state_only(&frontier)` round-tripped into a scratch doc —
//! including the deleted-container case that disqualifies plain
//! checkout-on-a-fork (restore.rs HistoryView note, verified loro 1.13):
//!
//!  E. state_only at a mid-history frontier of a SHALLOW doc reconstructs the
//!     exact entry set, including a container deleted later in history.
//!  F. plain `fork()` on a shallow doc (fork_at is NYI; is fork() usable?).
//!  G. `cmp_frontiers` ordering semantics (chain + divergent) for the trim
//!     planner's "strictly before boundary" test.

// The store itself still uses get_or_create_container (TODO(sync-era));
// the probe mirrors the store's calls exactly.
#![allow(deprecated)]

use loro::{CommitOptions, ExportMode, Frontiers, LoroDoc, LoroText, VersionVector};

fn commit(doc: &LoroDoc, msg: &str) -> Frontiers {
    doc.commit_with(CommitOptions::new().origin("sheaf").commit_msg(msg));
    doc.state_frontiers()
}

fn files(doc: &LoroDoc) -> Vec<(String, String)> {
    let mut out = Vec::new();
    doc.get_map("files").for_each(|k, v| {
        if let loro::ValueOrContainer::Container(loro::Container::Text(t)) = v {
            out.push((k.to_string(), t.to_string()));
        }
    });
    out.sort();
    out
}

fn state_at_via(doc: &LoroDoc, f: &Frontiers, label: &str) -> Vec<(String, String)> {
    let bytes = doc.export(ExportMode::state_only(Some(f))).unwrap();
    let scratch = LoroDoc::new();
    scratch.set_peer_id(777).unwrap();
    let st = scratch.import(&bytes).unwrap();
    assert!(
        st.pending.is_none(),
        "{label}: state_only must import without pending"
    );
    files(&scratch)
}

fn main() {
    // Full doc: 5 captures; c4 deletes the b.txt container created at c1.
    let doc = LoroDoc::new();
    doc.set_peer_id(111).unwrap();
    doc.set_change_merge_interval(0);
    doc.set_detached_editing(true);
    let mut last_vv = VersionVector::default();
    let mut caps: Vec<Frontiers> = Vec::new();
    for i in 0..3 {
        doc.get_map("files")
            .get_or_create_container("a.txt", LoroText::new())
            .unwrap()
            .insert(0, &format!("line-{i}\n"))
            .unwrap();
        caps.push(commit(&doc, &format!("c{}", i + 1)));
        last_vv = doc.oplog_vv();
    }
    let _ = last_vv;
    doc.get_map("files")
        .get_or_create_container("b.txt", LoroText::new())
        .unwrap()
        .insert(0, "ephemeral\n")
        .unwrap();
    caps.push(commit(&doc, "c4-create-b"));
    let at_b_alive = files(&doc);
    doc.get_map("files").delete("b.txt").unwrap();
    caps.push(commit(&doc, "c5-delete-b"));
    let [c1, c2, c3, _c4, c5] = [&caps[0], &caps[1], &caps[2], &caps[3], &caps[4]];

    // Ground truth from the FULL doc via fork_at (works on full docs).
    let full_at_c3 = {
        let f = doc.fork_at(c3).unwrap();
        files(&f)
    };
    println!("full doc @c3 (fork_at): {full_at_c3:?}");
    println!(
        "full doc @c4 has b.txt: {}",
        at_b_alive.iter().any(|(k, _)| k == "b.txt")
    );

    // ---- E. shallow doc + state_only -------------------------------------
    // NOTE: Frontiers::push DEDUPS same-peer ids to the max counter, so a
    // two-point protected set on one lineage collapses to its tip (observed:
    // protecting {c3, c5} yielded since=c5 and c3 unaddressable). The trim
    // planner must compute the boundary itself: elementwise-min vv over the
    // protected points, then vv_to_frontiers.
    let min_vv = {
        let mut acc = doc.frontiers_to_vv(c3).expect("c3 resolves in full doc");
        for (peer, ctr) in doc
            .frontiers_to_vv(c5)
            .expect("c5 resolves in full doc")
            .iter()
        {
            if let Some(slot) = acc.get_mut(peer) {
                *slot = (*slot).min(*ctr);
            }
        }
        acc.retain(|_, ctr| *ctr > 0);
        acc
    };
    let boundary = doc.vv_to_frontiers(&min_vv);
    println!("computed boundary: {:?} (c3={:?})", boundary, c3);
    let shallow_bytes = doc.export(ExportMode::shallow_snapshot(&boundary)).unwrap();
    let d2 = LoroDoc::new();
    d2.set_peer_id(222).unwrap();
    d2.set_detached_editing(true);
    d2.import(&shallow_bytes).unwrap();
    println!("\n== E. shallow doc ==");
    println!("is_shallow={}", d2.is_shallow());
    println!(
        "shallow_since={:?} (c3={:?})",
        d2.shallow_since_frontiers(),
        c3
    );
    println!(
        "resolves: c1={} c2={} c3={} c4={} c5={}",
        d2.frontiers_to_vv(c1).is_some(),
        d2.frontiers_to_vv(c2).is_some(),
        d2.frontiers_to_vv(c3).is_some(),
        d2.frontiers_to_vv(&caps[3]).is_some(),
        d2.frontiers_to_vv(c5).is_some()
    );

    // E1: state_only at the boundary-protected c3 must equal the fork_at truth.
    let via_so = state_at_via(&d2, c3, "E1");
    println!("shallow @c3 (state_only): {via_so:?}");
    println!("E1 matches fork_at truth: {}", via_so == full_at_c3);

    // E2: the deleted-container case at c4 (b.txt alive then, deleted at c5).
    let via_so4 = state_at_via(&d2, &caps[3], "E2");
    println!(
        "E2 @c4 shows b.txt: {}",
        via_so4
            .iter()
            .any(|(k, v)| k == "b.txt" && v == "ephemeral\n")
    );

    // E3: same read via checkout-on-plain-fork for contrast (expected loss).
    let scratch = d2.fork();
    scratch.checkout(&caps[3]).unwrap();
    let via_co = files(&scratch);
    println!(
        "E3 checkout-on-fork @c4 shows b.txt: {}",
        via_co
            .iter()
            .any(|(k, v)| k == "b.txt" && v == "ephemeral\n")
    );

    // ---- F. fork() on shallow doc ----------------------------------------
    println!("\n== F. fork() on shallow doc ==");
    let f2 = d2.fork();
    println!(
        "fork() ok; is_shallow={}; files={:?}",
        f2.is_shallow(),
        files(&f2).len()
    );

    // ---- G. cmp_frontiers semantics --------------------------------------
    println!("\n== G. cmp_frontiers ==");
    // divergent branch off c3 for an incomparable pair
    doc.checkout(c1).unwrap();
    doc.set_detached_editing(true);
    doc.get_map("files")
        .get_or_create_container("a.txt", LoroText::new())
        .unwrap()
        .insert(0, "divergent\n")
        .unwrap();
    let divergent = commit(&doc, "c6-branch");
    let order = |a: &Frontiers, b: &Frontiers| format!("{:?}", doc.cmp_frontiers(a, b));
    println!("c1 vs c2  (ancestor): {}", order(c1, c2));
    println!("c5 vs c1  (descendant): {}", order(c5, c1));
    println!("c1 vs c1  (equal): {}", order(c1, c1));
    println!("c5 vs divergent (incomparable): {}", order(c5, &divergent));
    let _ = c2;
}

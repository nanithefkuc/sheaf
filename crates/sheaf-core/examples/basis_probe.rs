//! Control experiment: does export(updates(&EMPTY_VV)) yield a causally
//! complete frame (counter-0 included)? Three seed/orderings probed.

use loro::{ExportMode, LoroDoc, VersionVector};

fn main() {
    probe("seed-then-empty-basis", true);
    probe("empty-basis-then-seed", false);
}

#[allow(deprecated)]
fn probe(label: &str, seed_meta_first: bool) {
    let writer_peer: u64 = 111;
    let doc = LoroDoc::new();
    doc.set_peer_id(writer_peer).unwrap();

    // order under test
    let basis = if seed_meta_first {
        doc.get_map("_sheaf.meta").insert("format", "1").unwrap();
        VersionVector::default()
    } else {
        let b = VersionVector::default();
        doc.get_map("_sheaf.meta").insert("format", "1").unwrap();
        b
    };

    doc.get_map("files")
        .get_or_create_container("a.txt", loro::LoroText::new())
        .unwrap()
        .insert(0, "héllo 🌍")
        .unwrap();
    let list = doc.get_list("tree_events");
    list.insert(list.len(), r#"{"kind":"added","path":"a.txt"}"#)
        .unwrap();

    let bytes = doc.export(ExportMode::updates(&basis)).unwrap();
    println!(
        "{label}: exported {} bytes; oplog_frontier={:?}",
        bytes.len(),
        doc.oplog_frontiers()
    );

    let reader = LoroDoc::new();
    reader.set_peer_id(999).unwrap();
    let st = reader.import(&bytes).unwrap();
    println!("  -> success={:?} pending={:?}", st.success, st.pending);
    let mut keys = vec![];
    reader
        .get_map("files")
        .for_each(|k, _| keys.push(k.to_string()));
    println!("  -> files={keys:?}");
}

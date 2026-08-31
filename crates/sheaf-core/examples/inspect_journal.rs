//! Decode every journal frame and report per-record import outcomes,
//! frontiers and resulting state size — a persistence debugging scalpel.
//!
//! Usage: cargo run -p sheaf-core --example inspect_journal -- <project-root>

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("project root"));
    let sdir = root.join(".sheaf/store");
    let segments = sheaf_core::store::list_segments(&sdir);
    println!("segments: {segments:?}");

    let doc = loro::LoroDoc::new();
    for (idx, path) in &segments {
        let recs = sheaf_core::store::read_records(&[(*idx, path.clone())]);
        for r in recs {
            match r {
                Ok(rec) => {
                    // Journals hold two frame kinds now: loro
                    // updates and tagged ledger records. Classify before
                    // importing; records fold into a printed summary.
                    match sheaf_core::store::classify_payload(&rec.payload) {
                        Some(sheaf_core::store::Frame::Update(delta)) => {
                            let st = doc.import(&delta)?;
                            println!(
                                "seg{idx}[{}] bytes={} success={:?} pending={:?}",
                                rec.ordinal,
                                rec.payload.len(),
                                st.success,
                                st.pending
                            );
                        }
                        Some(sheaf_core::store::Frame::Record(record)) => {
                            println!("seg{idx}[{}] LEDGER {}", rec.ordinal, record.summary());
                        }
                        None => println!(
                            "seg{idx}[{}] bytes={} UNCLASSIFIABLE",
                            rec.ordinal,
                            rec.payload.len()
                        ),
                    }
                }
                Err((seg, msg)) => println!("seg{seg}: ERROR {msg}"),
            }
        }
    }
    let files = doc.get_map("files");
    let mut keys = Vec::new();
    files.for_each(|k, _| keys.push(k.to_string()));
    let bins = doc.get_map("binaries");
    let mut bkeys = Vec::new();
    bins.for_each(|k, _| bkeys.push(k.to_string()));
    println!(
        "frontier={:?} files={keys:?} binaries={bkeys:?}",
        doc.oplog_frontiers()
    );
    Ok(())
}

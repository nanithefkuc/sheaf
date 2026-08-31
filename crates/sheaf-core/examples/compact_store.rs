//! Force a compaction (snapshot) on a project store copy — measures what a
//! snapshot baseline does to reader-open cost.
//!
//! Usage: cargo run -p sheaf-core --release --example compact_store -- <project-root>

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("project root"));
    let mut store = sheaf_core::store::ProjectStore::open(&root, Default::default())?;
    let t = std::time::Instant::now();
    store.compact()?;
    println!("compact: {:?}", t.elapsed());
    Ok(())
}

//! Dump a project store's reconstructed state (files map) to stdout, or
//! materialize it into an optional target directory.
//!
//! Usage: cargo run -p sheaf-core --example dump_store -- <project-root> [target-dir]

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().expect("project root required"));
    let target = args.next().map(PathBuf::from);

    let store = sheaf_core::store::ProjectStore::open(&root, Default::default())?;
    match target {
        Some(dir) => {
            std::fs::create_dir_all(&dir)?;
            let n = store.materialize(&dir)?;
            println!("materialized {n} files into {}", dir.display());
        }
        None => {
            // Cheap listing through a throwaway directory lets us reuse the
            // same walker until a proper reader lands in the timeline phase.
            let tmp = tempfile::tempdir()?;
            let n = store.materialize(tmp.path())?;
            println!(
                "store holds {n} materializable files; current seq={}",
                store.seq()
            );
        }
    }
    Ok(())
}

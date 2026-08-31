//! Reproducible scan-first selection-transition probe: synthesizes a dense
//! present/absent snapshot stream, collapses it into lifecycle transitions,
//! and prints throughput and event counts as JSON. Exercises the CPU-only
//! transition path (no store I/O) for tuning the selection reducer.
//!
//! Usage: cargo run --release -p sheaf-core --example selection_probe

use std::time::Instant;

use serde_json::json;
use sheaf_core::store::{
    lifecycle_transitions, LifecycleObservation, LifecycleState, SearchBudget,
};

fn main() {
    const SNAPSHOTS: usize = 10_000;
    const BYTES_PER_SNAPSHOT: usize = 4 * 1024;
    const NEEDLE: &str = "fn selected_probe";

    let filler = "x".repeat(BYTES_PER_SNAPSHOT - NEEDLE.len() - 32);
    let start = Instant::now();
    let mut observations = Vec::with_capacity(SNAPSHOTS);
    let mut matches = 0usize;
    let mut materialized = 0u64;
    for i in 0..SNAPSHOTS {
        // Ten present snapshots followed by ten absent snapshots; content
        // changes once per present episode so transition collapsing has work.
        let present = (i / 10) % 2 == 0;
        let source = if present {
            format!("{filler}\n{NEEDLE}_{:04}() {{}}\n", i / 20)
        } else {
            format!("{filler}\nfn other_{i}() {{}}\n")
        };
        materialized += source.len() as u64;
        let found = source.match_indices(NEEDLE).next();
        let state = match found {
            Some(_) => {
                matches += 1;
                LifecycleState::Present {
                    path: "src/lib.rs".into(),
                    selection_id: format!("selection-{}", i / 20),
                    content_hash: format!("content-{}", i / 20),
                }
            }
            None => LifecycleState::Absent,
        };
        observations.push(LifecycleObservation {
            point_id: format!("capture-{i:05}"),
            lineage_id: "main".into(),
            on_current: true,
            state,
        });
    }
    let events = lifecycle_transitions(&observations, false, false);
    let elapsed = start.elapsed();
    let mib = materialized as f64 / (1024.0 * 1024.0);
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "snapshots": SNAPSHOTS,
            "materialized_bytes": materialized,
            "matches": matches,
            "transition_events": events.len(),
            "elapsed_ms": elapsed.as_millis(),
            "throughput_mib_s": mib / seconds,
            "defaults": SearchBudget::default(),
        }))
        .unwrap()
    );
}

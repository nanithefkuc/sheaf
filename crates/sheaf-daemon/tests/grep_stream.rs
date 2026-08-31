//! E2E for proto-1.5 streamed grep: a spawned sheafd serves
//! `timeline.grep` with a streamed body — one flushed frame per finalized
//! record, summary last, empty terminator — and the streamed result
//! matches the authoritative degraded reader exactly. The daemon is
//! isolated behind its own socket and an empty data home so it never
//! touches the developer's real enrollment registry.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ipc::Client;
use sheaf_core::store::{
    GrepHit, GrepReport, GrepRequest, ProjectStore, StoreLimits, TimelineReader,
};

struct Daemon {
    child: Child,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn write_capture(store: &mut ProjectStore, root: &Path, events: Vec<EventKind>, age_h: i64) {
    let at = chrono::Utc::now() - chrono::Duration::hours(age_h);
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: events.into_iter().map(FsEvent::now).collect(),
        })
        .unwrap();
}

fn touch(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, age_h: i64) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    write_capture(
        store,
        root,
        vec![EventKind::Touched { path: path.into() }],
        age_h,
    );
}

fn spawn_daemon(socket: &Path, data_home: &Path) -> Daemon {
    let child = Command::new(env!("CARGO_BIN_EXE_sheafd"))
        .arg("run")
        .arg("--socket")
        .arg(socket)
        .env("SHEAF_SOCKET", socket)
        .env("XDG_DATA_HOME", data_home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sheafd");
    Daemon { child }
}

fn connect_with_retry(socket: &Path) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect(socket, Duration::from_secs(1)) {
            return client;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("daemon never accepted connections on {}", socket.display());
}

fn literal(text: &str) -> GrepRequest {
    GrepRequest {
        query: sheaf_core::store::GrepQuery::literal(text),
        mode: sheaf_core::store::GrepMode::History,
        at: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: sheaf_core::store::SelectionExtent::Match,
        budget: sheaf_core::store::SearchBudget::default(),
        cursor: None,
        anchor: None,
    }
}

#[test]
fn timeline_grep_streams_records_and_matches_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let socket: PathBuf = tmp.path().join("sock/control.sock");
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    let data_home = tmp.path().join("data");

    // Offline history: introduce, change, remove, reintroduce.
    skeleton(&root);
    {
        let mut store = ProjectStore::open(
            &root,
            StoreLimits {
                max_segment_bytes: 4 << 20,
                snapshot_edit_size: 1_000,
            },
        )
        .unwrap();
        touch(&mut store, &root, "a.rs", "fn probe() { 1 }\n", 4);
        touch(&mut store, &root, "a.rs", "fn probe() { 2 }\n", 3);
        touch(&mut store, &root, "a.rs", "unrelated\n", 2);
        touch(&mut store, &root, "a.rs", "fn probe() { 3 }\n", 1);
    }

    let _daemon = spawn_daemon(&socket, &data_home);
    let mut client = connect_with_retry(&socket);
    let reply = client
        .call("enroll.notify", Some(&root), serde_json::json!({}), None)
        .unwrap();
    assert!(
        reply.response.ok,
        "enroll failed: {:?}",
        reply.response.error
    );

    // The watch takes a moment to open the store; retry until served.
    let params = serde_json::json!({
        "query": {"kind": "literal", "text": "fn probe"},
        "budget": {
            "max_results": 1000,
            "max_materialized_bytes": 67108864u64,
            "max_elapsed_ms": 10000u64,
        },
    });
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let streamed = 'done: {
        for _ in 0..100 {
            chunks.clear();
            let result =
                client.call_streaming("timeline.grep", Some(&root), params.clone(), &mut |chunk| {
                    chunks.push(chunk.to_vec())
                });
            if let Ok(reply) = result {
                if reply.response.ok {
                    break 'done reply;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("daemon never served timeline.grep");
    };

    // Envelope only acknowledges the stream; the body carries everything.
    assert_eq!(
        streamed
            .response
            .result
            .as_ref()
            .and_then(|v| v.get("streamed"))
            .and_then(|v| v.as_bool()),
        Some(true)
    );
    // One NDJSON record per frame, summary last (terminator excluded).
    let mut hits: Vec<GrepHit> = Vec::new();
    let mut events: Vec<sheaf_core::store::GrepEvent> = Vec::new();
    let mut summary: Option<serde_json::Value> = None;
    for (i, chunk) in chunks.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_slice(chunk).expect("chunk is one record");
        let is_last = i + 1 == chunks.len();
        assert!(
            chunk.ends_with(b"\n") && chunk.iter().filter(|b| **b == b'\n').count() == 1,
            "frame {i} is exactly one NDJSON line"
        );
        match value.get("type").and_then(|t| t.as_str()) {
            Some("hit") if !is_last => {
                hits.push(serde_json::from_value(value["hit"].clone()).unwrap());
            }
            Some("event") if !is_last => {
                events.push(serde_json::from_value(value["event"].clone()).unwrap());
            }
            Some("summary") if is_last => summary = Some(value["report"].clone()),
            other => panic!("unexpected frame at {i}: {other:?}"),
        }
    }
    let summary: GrepReport =
        serde_json::from_value(summary.expect("summary is the final frame")).unwrap();
    assert!(summary.complete);
    assert!(!summary.degraded);

    // Parity: the streamed report equals the authoritative degraded read.
    let reader = TimelineReader::open(&root).unwrap();
    let authoritative = reader.grep(&literal("fn probe")).unwrap();
    assert_eq!(summary.hits, authoritative.hits);
    assert_eq!(summary.events, authoritative.events);
    assert_eq!(summary.complete, authoritative.complete);
    assert_eq!(hits, authoritative.hits);
    assert_eq!(events, authoritative.events);
    assert!(!hits.is_empty());

    // Buffered compatibility: a plain `call` reassembles the same stream.
    let mut client2 = connect_with_retry(&socket);
    let buffered = client2
        .call("timeline.grep", Some(&root), params, None)
        .unwrap();
    assert!(buffered.response.ok);
    let buffered_hits: Vec<GrepHit> = buffered
        .body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice::<serde_json::Value>(l).unwrap())
        .filter(|v| v.get("type").and_then(|t| t.as_str()) == Some("hit"))
        .map(|v| serde_json::from_value(v["hit"].clone()).unwrap())
        .collect();
    assert_eq!(buffered_hits, authoritative.hits);
    let _ = std::io::stdout().flush();
}

#[test]
fn anchored_history_streams_and_matches_the_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let socket: PathBuf = tmp.path().join("sock/control.sock");
    std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
    let data_home = tmp.path().join("data");

    // The anchor fixture: introduce, relocate (far insert), turn ambiguous.
    skeleton(&root);
    let pad = format!("{}\n", "p".repeat(80));
    {
        let mut store = ProjectStore::open(
            &root,
            StoreLimits {
                max_segment_bytes: 4 << 20,
                snapshot_edit_size: 1_000,
            },
        )
        .unwrap();
        touch(
            &mut store,
            &root,
            "a.rs",
            &format!("{pad}fn header() {{}}\nTODO anchor me\n{pad}"),
            9,
        );
        touch(&mut store, &root, "b.rs", "TODO independent\n", 8);
        touch(
            &mut store,
            &root,
            "a.rs",
            &format!("{pad}{pad}fn header() {{}}\nTODO anchor me\n{pad}"),
            7,
        );
        touch(
            &mut store,
            &root,
            "a.rs",
            &format!("{pad}{pad}fn header() {{}}\nTODO anchor me!!\n{pad}"),
            6,
        );
    }

    // The followed episode id comes from the authoritative reader.
    let reader = TimelineReader::open(&root).unwrap();
    let episode = reader.grep(&literal("TODO anchor me")).unwrap().hits[0]
        .episode_id
        .clone()
        .expect("history hits carry episode ids");

    let _daemon = spawn_daemon(&socket, &data_home);
    let mut client = connect_with_retry(&socket);
    let reply = client
        .call("enroll.notify", Some(&root), serde_json::json!({}), None)
        .unwrap();
    assert!(
        reply.response.ok,
        "enroll failed: {:?}",
        reply.response.error
    );

    let params = serde_json::json!({
        "query": {"kind": "literal", "text": "TODO anchor me"},
        "anchor": {"kind": "episode", "episode_id": episode},
        "budget": {
            "max_results": 1000,
            "max_materialized_bytes": 67108864u64,
            "max_elapsed_ms": 10000u64,
        },
    });
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let streamed = 'done: {
        for _ in 0..100 {
            chunks.clear();
            let result =
                client.call_streaming("timeline.grep", Some(&root), params.clone(), &mut |chunk| {
                    chunks.push(chunk.to_vec())
                });
            if let Ok(reply) = result {
                if reply.response.ok {
                    break 'done reply;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("daemon never served the anchored timeline.grep");
    };
    assert!(
        streamed
            .response
            .result
            .as_ref()
            .and_then(|v| v.get("streamed"))
            .and_then(|v| v.as_bool())
            == Some(true)
    );

    let mut hits: Vec<GrepHit> = Vec::new();
    let mut events: Vec<sheaf_core::store::GrepEvent> = Vec::new();
    let mut summary: Option<serde_json::Value> = None;
    for (i, chunk) in chunks.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_slice(chunk).expect("chunk is one record");
        let is_last = i + 1 == chunks.len();
        match value.get("type").and_then(|t| t.as_str()) {
            Some("hit") if !is_last => {
                hits.push(serde_json::from_value(value["hit"].clone()).unwrap())
            }
            Some("event") if !is_last => {
                events.push(serde_json::from_value(value["event"].clone()).unwrap())
            }
            Some("summary") if is_last => summary = Some(value["report"].clone()),
            other => panic!("unexpected frame at {i}: {other:?}"),
        }
    }
    let summary: GrepReport =
        serde_json::from_value(summary.expect("summary is the final frame")).unwrap();

    // Parity: daemon, NDJSON frames, and the degraded reader agree exactly
    // on the followed episode's records — one episode, no b.rs, ending in
    // the reported ambiguity.
    let mut anchored = literal("TODO anchor me");
    anchored.anchor = Some(sheaf_core::store::GrepAnchor::Episode {
        episode_id: episode.clone(),
    });
    let authoritative = reader.grep(&anchored).unwrap();
    assert_eq!(summary.hits, authoritative.hits);
    assert_eq!(summary.events, authoritative.events);
    assert_eq!(hits, authoritative.hits);
    assert_eq!(events, authoritative.events);
    assert!(!hits.is_empty());
    assert!(
        hits.iter().all(|hit| hit.path == "a.rs"),
        "the anchor filters the independent path out"
    );
    let episodes: std::collections::BTreeSet<&str> = hits
        .iter()
        .filter_map(|hit| hit.episode_id.as_deref())
        .chain(
            events
                .iter()
                .filter_map(|event| event.episode_id.as_deref()),
        )
        .collect();
    assert_eq!(episodes, std::iter::once(episode.as_str()).collect());
    assert!(
        events
            .iter()
            .any(|event| event.kind == sheaf_core::store::LifecycleKind::Ambiguous),
        "the followed episode ends in a reported ambiguity"
    );
}

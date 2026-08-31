//! CLI-level fixtures: the anchored-history grammar end to end
//! through the real binary, degraded (no daemon), plus the NDJSON contract
//! parity against the authoritative reader.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    GrepEvent, GrepHit, GrepMode, GrepQuery, GrepRequest, LifecycleKind, ProjectStore,
    SearchBudget, SelectionExtent, StoreLimits, TimelineReader,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
    // The daemon owns the lock file; degraded reads only flock what is
    // already there. Leave an uncontended one behind, as a stopped daemon
    // would.
    std::fs::write(root.join(".sheaf/lock"), b"").unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 1_000,
        },
    )
    .unwrap()
}

fn touch(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, age_h: i64) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    let at = Utc::now() - Duration::hours(age_h);
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: vec![FsEvent::now(EventKind::Touched { path: path.into() })],
        })
        .unwrap();
}

/// The anchor fixture: one occurrence introduced, relocated by a far-away
/// insert, then made ambiguous by a context rewrite; an independent path
/// runs in parallel.
fn anchored_store(root: &Path) -> ProjectStore {
    skeleton(root);
    let mut store = open(root);
    // >64-byte padding on both sides keeps the context windows stable
    // across the relocation insert above.
    let pad = "p".repeat(80) + "\n";
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{pad}fn header() {{}}\nTODO anchor me\n{pad}"),
        9,
    );
    touch(&mut store, root, "b.rs", "TODO independent\n", 8);
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{pad}{pad}fn header() {{}}\nTODO anchor me\n{pad}"),
        7,
    );
    touch(
        &mut store,
        root,
        "a.rs",
        &format!("{pad}{pad}fn header() {{}}\nTODO anchor me!!\n{pad}"),
        6,
    );
    store
}

fn capture_ids_oldest_first(reader: &TimelineReader) -> Vec<String> {
    reader
        .captures(false, None, false, 100)
        .unwrap()
        .into_iter()
        .map(|capture| capture.id)
        .rev()
        .collect()
}

struct Gui {
    socket: PathBuf,
    #[allow(dead_code)]
    tmp: tempfile::TempDir,
}

impl Gui {
    /// A socket path nobody listens on: the CLI must fall back to the
    /// degraded read-only reader.
    fn isolated() -> Gui {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock/none.sock");
        Gui { socket, tmp }
    }

    fn grep(&self, root: &Path, extra: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_sheaf"))
            .arg("grep")
            .args(extra)
            .current_dir(root)
            .env("SHEAF_SOCKET", &self.socket)
            .output()
            .expect("sheaf grep runs")
    }
}

fn stdout_lines(output: &Output) -> Vec<String> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect()
}

fn history_request(needle: &str) -> GrepRequest {
    GrepRequest {
        query: GrepQuery::literal(needle),
        mode: GrepMode::History,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: SelectionExtent::Match,
        budget: SearchBudget::default(),
        cursor: None,
    }
}

#[test]
fn human_history_output_names_episodes_and_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let gui = Gui::isolated();

    let output = gui.grep(&root, &["--history", "TODO"]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    // Coordinates use source lines (3 -> 4), lifecycle is named, and every
    // history record carries its episode for a follow-up --episode anchor.
    assert!(text.contains("introduced"), "missing introduced: {text}");
    assert!(text.contains("relocated"), "missing relocated: {text}");
    assert!(text.contains("episode ep1:"), "missing episode id: {text}");
    assert!(
        text.contains("ambiguous"),
        "the context rewrite must surface as an ambiguity event: {text}"
    );
    // The independent path participates in unanchored history.
    assert!(
        text.contains("b.rs"),
        "unanchored history covers all paths: {text}"
    );
}

#[test]
fn ndjson_history_matches_the_authoritative_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let reader = TimelineReader::open(&root).unwrap();
    let authoritative = reader.grep(&history_request("TODO anchor me")).unwrap();

    let gui = Gui::isolated();
    let output = gui.grep(&root, &["--json", "--history", "TODO anchor me"]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut hits: Vec<GrepHit> = Vec::new();
    let mut events: Vec<GrepEvent> = Vec::new();
    let mut summary: Option<serde_json::Value> = None;
    for (i, line) in stdout_lines(&output).into_iter().enumerate() {
        let value: serde_json::Value =
            serde_json::from_str(&line).expect("one NDJSON record per line");
        match value.get("type").and_then(|t| t.as_str()) {
            Some("hit") => hits.push(serde_json::from_value(value["hit"].clone()).unwrap()),
            Some("event") => events.push(serde_json::from_value(value["event"].clone()).unwrap()),
            Some("summary") if i + 1 == stdout_line_count(&output) => {
                summary = Some(value["report"].clone())
            }
            other => panic!("unexpected NDJSON record {other:?} at {i}"),
        }
    }
    let summary = serde_json::from_value::<sheaf_core::store::GrepReport>(
        summary.expect("summary terminates the NDJSON stream"),
    )
    .unwrap();
    assert_eq!(hits, authoritative.hits);
    assert_eq!(events, authoritative.events);
    assert!(summary.degraded, "no daemon: the summary must say degraded");
}

fn stdout_line_count(output: &Output) -> usize {
    String::from_utf8_lossy(&output.stdout).lines().count()
}

#[test]
fn coordinate_anchor_cli_follows_exactly_one_episode() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let reader = TimelineReader::open(&root).unwrap();
    let ids = capture_ids_oldest_first(&reader);
    let gui = Gui::isolated();

    let output = gui.grep(
        &root,
        &[
            "--history",
            "--at",
            &ids[2],
            "--path",
            "a.rs",
            "--line",
            "4",
            "TODO anchor me",
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("introduced"));
    assert!(text.contains("relocated"));
    assert!(
        text.contains("ambiguous"),
        "the followed episode ends in a reported ambiguity: {text}"
    );
    assert!(
        !text.contains("b.rs"),
        "the anchor must filter the independent path out: {text}"
    );
    // Exactly one episode id appears across all records.
    let episodes: std::collections::BTreeSet<String> = text
        .lines()
        .filter_map(|l| l.split("episode ").nth(1))
        .map(|rest| rest.split_whitespace().next().unwrap_or("").to_owned())
        .collect();
    assert_eq!(episodes.len(), 1, "one followed episode, got {episodes:?}");
}

#[test]
fn episode_anchor_cli_round_trips_through_the_printed_id() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let gui = Gui::isolated();

    // Discover the episode id from the human output, then follow it.
    let discovery = gui.grep(&root, &["--history", "TODO anchor me"]);
    assert!(discovery.status.success());
    let text = String::from_utf8_lossy(&discovery.stdout);
    let episode = text
        .lines()
        .find_map(|l| l.split("episode ").nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .expect("a printed episode id")
        .to_owned();

    let output = gui.grep(
        &root,
        &["--history", "--episode", &episode, "TODO anchor me"],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let followed = String::from_utf8_lossy(&output.stdout);
    assert!(followed.contains(&episode));
    assert!(!followed.contains("b.rs"));
}

#[test]
fn point_mode_rejects_anchor_flags_at_the_cli_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let gui = Gui::isolated();

    // --line requires --history + --at + --path.
    let output = gui.grep(&root, &["--path", "a.rs", "--line", "4", "TODO anchor me"]);
    assert!(!output.status.success(), "clap must reject the flag mix");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--history") || stderr.contains("required"),
        "expected a usage error, got: {stderr}"
    );

    // Point mode with an episode anchor is a usage error too.
    let output = gui.grep(&root, &["--episode", "ep1:0123456789abcdef", "TODO"]);
    assert!(!output.status.success());
}

#[test]
fn selection_anchor_cli_reads_a_hit_handle_from_a_file() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let reader = TimelineReader::open(&root).unwrap();
    let ids = capture_ids_oldest_first(&reader);
    let gui = Gui::isolated();

    // Point discovery at the second capture, take the printed handle JSON.
    let output = gui.grep(
        &root,
        &[
            "--json",
            "--at",
            &ids[1],
            "--path",
            "a.rs",
            "TODO anchor me",
        ],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let hit_line = stdout_lines(&output)
        .into_iter()
        .find(|l| l.contains("\"type\":\"hit\"") || l.contains("\"type\": \"hit\""))
        .expect("a hit record in point mode");
    let handle_path = tmp.path().join("hit.json");
    std::fs::write(&handle_path, hit_line).unwrap();

    let anchored = gui.grep(
        &root,
        &[
            "--history",
            "--selection",
            handle_path.to_str().unwrap(),
            "TODO anchor me",
        ],
    );
    assert!(
        anchored.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&anchored.stderr)
    );
    let text = String::from_utf8_lossy(&anchored.stdout);
    assert!(text.contains("relocated"));
    assert!(!text.contains("b.rs"));
}

/// Point discovery is the default mode, so its human rendering is the
/// surface a developer hits with no flags at all. Several occurrences on one
/// line, another later in the same file, and one in a second file must all
/// appear with source coordinates.
fn dense_store(root: &Path) -> ProjectStore {
    skeleton(root);
    let mut store = open(root);
    // Two occurrences on line 1; line 2 puts a two-byte scalar before the
    // match so a byte column would read 7 where the scalar column is 6.
    touch(
        &mut store,
        root,
        "a.rs",
        "let x = TODO; let y = TODO;\n// \u{e9} TODO\n",
        2,
    );
    touch(&mut store, root, "b.rs", "TODO in another file\n", 1);
    store
}

#[test]
fn default_point_discovery_prints_every_occurrence_with_coordinates() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = dense_store(&root);
    drop(store);
    let gui = Gui::isolated();

    // No mode flag: discovery at @ is the default.
    let output = gui.grep(&root, &["--color", "never", "TODO"]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);

    // Every occurrence, path-then-line-then-column ordered, one-based, with
    // Unicode-scalar columns.
    for coord in ["a.rs:1:9", "a.rs:1:23", "a.rs:2:6", "b.rs:1:1"] {
        assert!(text.contains(coord), "missing {coord} in:\n{text}");
    }
    let order: Vec<usize> = ["a.rs:1:9", "a.rs:1:23", "a.rs:2:6", "b.rs:1:1"]
        .iter()
        .map(|c| text.find(c).expect("coordinate present"))
        .collect();
    assert!(
        order.windows(2).all(|w| w[0] < w[1]),
        "coordinates must print in path/line/column order:\n{text}"
    );

    // Point mode reports presence, never a lifecycle transition, and never
    // an episode ID — episodes exist only in history mode.
    assert!(text.contains("present"), "missing present kind:\n{text}");
    assert!(
        !text.contains("episode"),
        "point discovery must not print episode IDs:\n{text}"
    );

    // Each occurrence is independently restorable: distinct occurrence IDs,
    // each paired with a selection handle.
    let ids: std::collections::BTreeSet<&str> = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("occurrence "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect();
    assert_eq!(ids.len(), 4, "expected four distinct occurrences:\n{text}");
    assert_eq!(
        text.matches("selection ").count(),
        4,
        "every occurrence carries a selection handle:\n{text}"
    );
}

/// The human renderer and the NDJSON stream are two presentations of one
/// report. Nothing but a fixture keeps them from drifting apart, so parse the
/// rendered rows back and require exact agreement on coordinates, lifecycle
/// vocabulary, episode IDs, and handle prefixes.
fn human_tag(kind: LifecycleKind) -> &'static str {
    match kind {
        LifecycleKind::Present => "present",
        LifecycleKind::Introduced => "introduced",
        LifecycleKind::Reintroduced => "reintroduced",
        LifecycleKind::Changed => "changed",
        LifecycleKind::Relocated => "relocated",
        LifecycleKind::Renamed => "renamed",
        LifecycleKind::Moved => "moved",
        LifecycleKind::Observed => "observed",
        LifecycleKind::Removed => "removed",
        LifecycleKind::Ambiguous => "ambiguous",
        LifecycleKind::RetentionGap => "retention gap",
    }
}

/// One rendered record, reduced to the fields the NDJSON also carries.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    capture: String,
    tag: String,
    marker: String,
    path: String,
    /// `Some` for hits (which print coordinates), `None` for events.
    coords: Option<(usize, usize)>,
    preview: Option<String>,
    occurrence: Option<String>,
    selection: Option<String>,
    episode: Option<String>,
    candidates: Option<usize>,
}

fn parse_human_rows(text: &str) -> Vec<Row> {
    let lines: Vec<&str> = text.lines().collect();
    let mut rows = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("    ") || line.trim().is_empty() {
            i += 1;
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let capture = tokens[0].to_owned();
        // `retention gap` is the one two-word tag in the vocabulary.
        let (tag, mut next) = if tokens.get(1) == Some(&"retention") {
            ("retention gap".to_owned(), 3)
        } else {
            (tokens[1].to_owned(), 2)
        };
        let marker = tokens[next].to_owned();
        next += 1;

        // A hit is followed by its indented preview and identity lines; an
        // event stands alone.
        let is_hit = lines
            .get(i + 1)
            .is_some_and(|next| next.starts_with("    "));
        if is_hit {
            let located = tokens[next];
            let mut parts = located.rsplitn(3, ':');
            let column: usize = parts.next().unwrap().parse().expect("column");
            let line_no: usize = parts.next().unwrap().parse().expect("line");
            let path = parts.next().unwrap().to_owned();
            let preview = lines[i + 1].strip_prefix("    ").unwrap().to_owned();
            let identity: Vec<&str> = lines[i + 2].split_whitespace().collect();
            assert_eq!(
                identity[0],
                "occurrence",
                "identity line: {:?}",
                lines[i + 2]
            );
            assert_eq!(
                identity[2],
                "selection",
                "identity line: {:?}",
                lines[i + 2]
            );
            let episode = match identity.get(4) {
                Some(&"episode") => Some(identity[5].to_owned()),
                None => None,
                other => panic!("unexpected identity tail {other:?}"),
            };
            rows.push(Row {
                capture,
                tag,
                marker,
                path,
                coords: Some((line_no, column)),
                preview: Some(preview),
                occurrence: Some(identity[1].to_owned()),
                selection: Some(identity[3].to_owned()),
                episode,
                candidates: None,
            });
            i += 3;
        } else {
            let path = tokens.get(next).copied().unwrap_or_default().to_owned();
            let rest = &tokens[(next + 1).min(tokens.len())..];
            let episode = rest
                .iter()
                .position(|t| *t == "episode")
                .map(|at| rest[at + 1].to_owned());
            let candidates = rest
                .iter()
                .position(|t| *t == "candidates")
                .map(|at| rest[at + 1].parse().expect("candidate count"));
            rows.push(Row {
                capture,
                tag,
                marker,
                path,
                coords: None,
                preview: None,
                occurrence: None,
                selection: None,
                episode,
                candidates,
            });
            i += 1;
        }
    }
    rows
}

fn expected_rows(hits: &[GrepHit], events: &[GrepEvent]) -> Vec<Row> {
    let short = |s: &str, n: usize| s[..n.min(s.len())].to_owned();
    let marker = |on_current: bool| if on_current { "·" } else { "branch" }.to_owned();
    let mut rows: Vec<Row> = hits
        .iter()
        .map(|hit| Row {
            capture: short(&hit.capture_id, 12),
            tag: human_tag(hit.kind).to_owned(),
            marker: marker(hit.on_current),
            path: hit.path.clone(),
            coords: Some((hit.line, hit.column)),
            preview: Some(hit.preview.clone()),
            occurrence: Some(short(&hit.occurrence_id, 16)),
            selection: Some(short(&hit.handle_id, 16)),
            episode: hit.episode_id.clone(),
            candidates: None,
        })
        .collect();
    rows.extend(events.iter().map(|event| {
        Row {
            capture: short(&event.capture_id, 12),
            tag: human_tag(event.kind).to_owned(),
            marker: marker(event.on_current),
            path: event.path.clone().unwrap_or_default(),
            coords: None,
            preview: None,
            occurrence: None,
            selection: None,
            episode: event.episode_id.clone(),
            candidates: event
                .candidates
                .as_ref()
                .filter(|c| !c.is_empty())
                .map(|c| c.len()),
        }
    }));
    rows
}

#[test]
fn human_rendering_agrees_with_the_ndjson_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let store = anchored_store(&root);
    drop(store);
    let reader = TimelineReader::open(&root).unwrap();
    let authoritative = reader.grep(&history_request("TODO anchor me")).unwrap();
    // The fixture must exercise both presentations: hits and a real event.
    assert!(!authoritative.hits.is_empty(), "fixture needs hits");
    assert!(!authoritative.events.is_empty(), "fixture needs events");

    let gui = Gui::isolated();
    let output = gui.grep(&root, &["--color", "never", "--history", "TODO anchor me"]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);

    let mut rendered = parse_human_rows(&text);
    let mut expected = expected_rows(&authoritative.hits, &authoritative.events);
    // Human output interleaves hits and events in stream order; compare as
    // sets of records so ordering stays the streaming test's concern.
    let key = |row: &Row| {
        (
            row.capture.clone(),
            row.tag.clone(),
            row.path.clone(),
            row.coords,
        )
    };
    rendered.sort_by_key(key);
    expected.sort_by_key(key);
    assert_eq!(
        rendered, expected,
        "human rows must carry exactly the NDJSON fields:\n{text}"
    );

    // Episode IDs must be copy-pasteable in full, never truncated for display.
    for hit in &authoritative.hits {
        if let Some(episode) = &hit.episode_id {
            assert!(
                text.contains(episode),
                "episode {episode} must print in full:\n{text}"
            );
        }
    }
}

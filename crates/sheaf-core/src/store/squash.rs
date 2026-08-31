//! `sheaf squash` internals: anchor resolution, span
//! statistics, and commit-message drafting. The CLI is the front door; this
//! module is everything that does not itself talk to the daemon or git.

use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Serialize};

use super::{Capture, Checkpoint, DiffOutcome, OriginKind};

/// Split a squash range into its point forms: `A..B` yields both, anything
/// else is a single anchor reference. The CLI resolves each side the same
/// way every other timeline command does.
pub fn split_range(range: &str) -> Result<(&str, Option<&str>), String> {
    if let Some((a, b)) = range.split_once("..") {
        let (a, b) = (a.trim(), b.trim());
        if a.is_empty() || b.is_empty() {
            return Err(format!("`{range}` needs a point on both sides of `..`"));
        }
        // Refuse `A...B`-style triples early: the middle collapses to an
        // empty point and confuses resolution downstream.
        if b.starts_with('.') {
            return Err(format!(
                "`{range}`: three-dot ranges are not a squash range"
            ));
        }
        Ok((a, Some(b)))
    } else {
        Ok((range, None))
    }
}

/// The git sha carried by a `git-<short-sha>` checkpoint name, if the name
/// is a well-formed frame stamp.
pub fn anchor_sha(name: &str) -> Option<&str> {
    let sha = name.strip_prefix("git-")?;
    (sha.len() >= 7 && sha.bytes().all(|b| b.is_ascii_hexdigit())).then_some(sha)
}

/// Default squash anchor for preview mode, which runs no git queries:
/// the most recent `git-<sha>` checkpoint pinned to a capture on the
/// worktree's current lineage. Off-lineage stamps belong to abandoned or
/// switched-away futures and must not anchor a collapse.
pub fn frame_anchor(checkpoints: &[Checkpoint]) -> Option<Checkpoint> {
    checkpoints
        .iter()
        .filter(|cp| cp.on_current && anchor_sha(&cp.name).is_some())
        .max_by_key(|cp| cp.timestamp_ms.unwrap_or(i64::MIN))
        .cloned()
}

/// Statistics over the captures a squash span collapsed. Captures arrive
/// newest-first from the lineage walk; ordering is normalized here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStats {
    pub count: usize,
    /// Oldest capture in the span, ms since the epoch.
    pub first_ms: Option<i64>,
    /// Newest capture in the span, ms since the epoch.
    pub last_ms: Option<i64>,
    /// Checkpoint names pinned to captures inside the span, oldest first.
    pub checkpoints: Vec<String>,
    /// Captures whose provenance is a restore crossing the span.
    pub restores: usize,
    /// The walk hit its page budget or lineage end without finding the
    /// anchor capture: counts are partial, not exact.
    pub partial: bool,
}

pub fn span_stats(newest_first: &[Capture], partial: bool) -> SpanStats {
    let mut stats = SpanStats {
        count: newest_first.len(),
        partial,
        ..SpanStats::default()
    };
    let mut oldest: Option<&Capture> = None;
    for capture in newest_first {
        if oldest.is_none_or(|o| capture.timestamp_ms < o.timestamp_ms) {
            oldest = Some(capture);
        }
        if stats.last_ms.is_none_or(|last| capture.timestamp_ms > last) {
            stats.last_ms = Some(capture.timestamp_ms);
        }
        stats.restores += capture
            .origin
            .as_ref()
            .is_some_and(|o| matches!(o.kind, OriginKind::Restore | OriginKind::PreRestore))
            as usize;
        stats
            .checkpoints
            .extend(capture.checkpoints.iter().cloned());
    }
    stats.first_ms = oldest.map(|c| c.timestamp_ms);
    stats
}

/// Walk the current lineage newest-first until the anchor capture, fetching
/// pages through `fetch(cursor)` (each page: captures strictly older than
/// the cursor, newest first). `until_inclusive` bounds the span from above
/// — the `B` of an `A..B` range — so captures newer than B are skipped and
/// B itself is included. Returns the span's captures (newest-first) and
/// whether the walk was exact: an anchor or bound pinned off-lineage (or an
/// inverted `A..B`) ends early and marks the stats partial rather than
/// guessing. A `None` anchor (point naming no capture) collects the whole
/// walk down to the lineage end.
pub fn collect_span<E>(
    anchor_capture_id: Option<&str>,
    until_inclusive: Option<&str>,
    mut fetch: impl FnMut(Option<&str>) -> Result<Vec<Capture>, E>,
) -> Result<(Vec<Capture>, bool), E> {
    let mut span: Vec<Capture> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut entered_span = until_inclusive.is_none();
    // Page budget: each IPC page walks server-side, so a runaway loop here
    // is real cost. 64 pages × 1000 captures bounds a span at 64k captures.
    for _ in 0..64 {
        let page = fetch(cursor.as_deref())?;
        if page.is_empty() {
            return Ok((span, anchor_capture_id.is_none() && entered_span));
        }
        let mut skip = 0usize;
        if let Some(until) = until_inclusive {
            if !entered_span {
                match page.iter().position(|c| c.id == until) {
                    Some(at) => {
                        // The anchor sitting above the bound means an
                        // inverted `A..B`: nothing lies in that span.
                        if let Some(anchor) = anchor_capture_id {
                            if page[..at].iter().any(|c| c.id == anchor) {
                                return Ok((Vec::new(), false));
                            }
                        }
                        skip = at;
                        entered_span = true;
                    }
                    None => {
                        // Whole page is newer than B; the anchor here is
                        // the cross-page half of the inverted-range guard.
                        if let Some(anchor) = anchor_capture_id {
                            if page.iter().any(|c| c.id == anchor) {
                                return Ok((Vec::new(), false));
                            }
                        }
                        skip = page.len();
                    }
                }
            }
        }
        let visible = &page[skip..];
        if let Some(anchor) = anchor_capture_id {
            if let Some(idx) = visible.iter().position(|c| c.id == anchor) {
                span.extend(visible[..idx].iter().cloned());
                return Ok((span, true));
            }
        }
        let exhausted = page.len() < 1000;
        cursor = Some(page.last().expect("checked non-empty").id.clone());
        span.extend(visible.iter().cloned());
        if exhausted {
            return Ok((span, anchor_capture_id.is_none() && entered_span));
        }
    }
    Ok((span, false))
}

/// Does a `git commit` passthrough already carry a message (or template)?
/// When it does not, squash seeds git's editor with the draft via `-t`.
pub fn passthrough_has_message(args: &[String]) -> bool {
    const LONG: [&str; 7] = [
        "--message",
        "--file",
        "--template",
        "--reuse-message",
        "--reedit-message",
        "--fixup",
        "--squash",
    ];
    const SHORT: [char; 5] = ['m', 'F', 't', 'C', 'c'];
    args.iter().any(|arg| {
        LONG.iter()
            .any(|flag| arg == *flag || arg.starts_with(&format!("{flag}=")))
            || (arg.starts_with('-')
                && !arg.starts_with("--")
                && arg.len() > 1
                && arg[1..].chars().next().is_some_and(|c| SHORT.contains(&c)))
    })
}

/// Longest shared directory (component-wise) of the changed paths.
fn common_scope(entries: &DiffOutcome) -> Option<String> {
    let paths: Vec<&str> = entries.entries.iter().map(|e| e.path.as_str()).collect();
    let first = paths.first()?.split('/').collect::<Vec<_>>();
    let mut shared = first.len() - 1; // directory components only
    for path in &paths[1..] {
        let comps = path.split('/').collect::<Vec<_>>();
        let mut keep = 0;
        while keep < shared && keep + 1 < comps.len() && comps[keep] == first[keep] {
            keep += 1;
        }
        shared = keep;
    }
    (shared > 0).then(|| first[..shared].join("/"))
}

fn human_duration(ms: i64) -> String {
    let secs = ms.max(0) / 1000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (hours, rem) = (rem / 3_600, rem % 3_600);
    let minutes = rem / 60;
    match (days, hours, minutes) {
        (0, 0, 0) => format!("{secs}s"),
        (0, 0, m) => format!("{m}m"),
        (0, h, _) if h >= 10 => format!("{h}h"),
        (0, h, m) => format!("{h}h {m}m"),
        (d, _, _) if d >= 10 => format!("{d}d"),
        (d, h, _) => format!("{d}d {h}h"),
    }
}

fn local_time(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|utc| {
            let local: DateTime<Local> = utc.into();
            local.format("%Y-%m-%d %H:%M").to_string()
        })
        .unwrap_or_else(|| "?".into())
}

/// Subject line for the drafted commit message.
pub fn draft_subject(diff: &DiffOutcome) -> String {
    let added: usize = diff.entries.iter().map(|e| e.added_lines).sum();
    let removed: usize = diff.entries.iter().map(|e| e.removed_lines).sum();
    let churn = if added == 0 && removed == 0 {
        String::new()
    } else {
        format!(" (+{added}/-{removed})")
    };
    match diff.entries.len() {
        0 => "empty squash frame".to_owned(),
        1 => {
            let entry = &diff.entries[0];
            match &entry.old_path {
                Some(old) => format!("{old} => {}{}", entry.path, churn),
                None => format!("{}{}", entry.path, churn),
            }
        }
        n => match common_scope(diff) {
            Some(scope) => format!("{scope}/: {n} files{churn}"),
            None => format!("{n} files{churn}"),
        },
    }
}

/// Full drafted commit message: a metadata body under the subject, for the
/// human or agent to edit.
pub fn draft_message(stats: &SpanStats, diff: &DiffOutcome) -> String {
    let subject = draft_subject(diff);
    let mut body = String::new();
    match (stats.first_ms, stats.last_ms) {
        (Some(first), Some(last)) => body.push_str(&format!(
            "Squashed from {} sheaf {} over {} ({} → {}).",
            stats.count,
            if stats.count == 1 {
                "capture"
            } else {
                "captures"
            },
            human_duration(last - first),
            local_time(first),
            local_time(last),
        )),
        _ if stats.count > 0 => body.push_str(&format!(
            "Squashed from {} sheaf {}.",
            stats.count,
            if stats.count == 1 {
                "capture"
            } else {
                "captures"
            },
        )),
        _ => body.push_str("No sheaf captures in this span (empty frame)."),
    }
    if stats.partial {
        body.push_str("\nCapture walk ended before the anchor; counts are partial.");
    }
    if !stats.checkpoints.is_empty() {
        body.push_str(&format!(
            "\nCheckpoints crossed: {}.",
            stats.checkpoints.join(", ")
        ));
    }
    if stats.restores > 0 {
        body.push_str(&format!("\nRestores crossed the span: {}.", stats.restores));
    }
    // Top churn table: the files that dominate the change, capped so the
    // draft stays a draft and not a dump. Zero-churn entries (pure
    // renames, binary swaps) show in the stat, not here.
    let mut ranked: Vec<_> = diff
        .entries
        .iter()
        .map(|e| (e.path.as_str(), e.added_lines, e.removed_lines))
        .filter(|(_, added, removed)| *added + *removed > 0)
        .collect();
    ranked.sort_by(|a, b| (b.1 + b.2).cmp(&(a.1 + a.2)).then_with(|| a.0.cmp(b.0)));
    if ranked.len() > 1 {
        body.push_str("\n\nTop changes by churn:");
        for (path, added, removed) in ranked.iter().take(8) {
            body.push_str(&format!("\n  {path}  +{added}/-{removed}"));
        }
    }
    format!("{subject}\n\n{body}\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{CaptureOrigin, DiffKind, FileDiff, SideContent, SideDesc};

    fn capture(id: &str, at_ms: i64, paths: &[&str]) -> Capture {
        Capture {
            id: id.to_owned(),
            frontier: format!("{id:0>64}"),
            parent_frontier: String::new(),
            timestamp_ms: at_ms,
            paths: paths.iter().map(|s| s.to_string()).collect(),
            events: paths.len(),
            checkpoints: Vec::new(),
            origin: None,
            on_current: true,
        }
    }

    fn cp(name: &str, at_ms: i64, on_current: bool) -> Checkpoint {
        Checkpoint {
            name: name.to_owned(),
            frontier: "f".repeat(32),
            capture_id: Some("c".repeat(64)),
            timestamp_ms: Some(at_ms),
            on_current,
        }
    }

    fn diff_of(entries: &[(&str, usize, usize)]) -> DiffOutcome {
        DiffOutcome {
            from: SideDesc {
                kind: "point".into(),
                capture_id: None,
                frontier: None,
            },
            to: SideDesc {
                kind: "worktree".into(),
                capture_id: None,
                frontier: None,
            },
            entries: entries
                .iter()
                .map(|(path, a, r)| FileDiff {
                    path: path.to_string(),
                    old_path: None,
                    kind: DiffKind::Modified,
                    old: SideContent::Absent,
                    new: SideContent::Text { bytes: 10 },
                    added_lines: *a,
                    removed_lines: *r,
                    hunks: Vec::new(),
                })
                .collect(),
            degraded: false,
        }
    }

    #[test]
    fn range_splitting() {
        assert_eq!(split_range("@~3"), Ok(("@~3", None)));
        assert_eq!(split_range("a1b2c3..@"), Ok(("a1b2c3", Some("@"))));
        assert_eq!(
            split_range("checkpoint:x..@~2"),
            Ok(("checkpoint:x", Some("@~2")))
        );
        assert!(split_range("..@").is_err());
        assert!(split_range("@..").is_err());
        assert!(split_range("a...b").is_err());
    }

    #[test]
    fn anchor_names() {
        assert_eq!(anchor_sha("git-a1b2c3d"), Some("a1b2c3d"));
        assert_eq!(anchor_sha("git-a1b2c"), None); // too short
        assert_eq!(anchor_sha("git-xyz1234"), None); // not hex
        assert_eq!(anchor_sha("before-work"), None);
        assert_eq!(anchor_sha("git-"), None);
    }

    #[test]
    fn frame_anchor_picks_latest_on_current() {
        let cps = vec![
            cp("git-aaaaaaa", 100, true),
            cp("git-bbbbbbb", 300, true),
            cp("git-ccccccc", 900, false), // abandoned future
            cp("plain", 1000, true),       // not a frame stamp
        ];
        let anchor = frame_anchor(&cps).expect("an anchor");
        assert_eq!(anchor.name, "git-bbbbbbb");
        assert!(frame_anchor(&[]).is_none());
    }

    #[test]
    fn span_stats_aggregate() {
        let mut with_restore = capture("d", 4_000, &["src/a"]);
        with_restore.origin = Some(CaptureOrigin {
            kind: OriginKind::Restore,
            target: None,
            scope: vec![],
            selections: Vec::new(),
        });
        let mut pinned = capture("c", 3_000, &["src/b"]);
        pinned.checkpoints = vec!["before-rework".into()];
        let newest_first = vec![
            capture("e", 5_000, &["src/a"]),
            with_restore,
            pinned,
            capture("b", 2_000, &["src/c"]),
            capture("a", 1_000, &["src/d"]),
        ];
        let stats = span_stats(&newest_first, false);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.first_ms, Some(1_000));
        assert_eq!(stats.last_ms, Some(5_000));
        assert_eq!(stats.restores, 1);
        assert_eq!(stats.checkpoints, vec!["before-rework"]);
        assert!(!stats.partial);
    }

    #[test]
    fn collect_span_stops_at_anchor() {
        let all = vec![
            capture("c3", 30, &["p"]),
            capture("c2", 20, &["p"]),
            capture("c1", 10, &["p"]),
            capture("c0", 0, &["p"]),
        ];
        let (span, reached) =
            collect_span(Some("c1"), None, |_: Option<&str>| Ok::<_, ()>(all.clone())).unwrap();
        assert!(reached);
        assert_eq!(
            span.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
            ["c3", "c2"]
        );
    }

    #[test]
    fn collect_span_bounds_include_b() {
        // `A..B` = (A, B]: B itself is in the span; captures newer than B
        // ("c4") are not.
        let all = vec![
            capture("c4", 40, &["p"]),
            capture("c3", 30, &["p"]), // B
            capture("c2", 20, &["p"]),
            capture("c1", 10, &["p"]), // A
            capture("c0", 0, &["p"]),
        ];
        let (span, reached) = collect_span(Some("c1"), Some("c3"), |_: Option<&str>| {
            Ok::<_, ()>(all.clone())
        })
        .unwrap();
        assert!(reached);
        assert_eq!(
            span.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
            ["c3", "c2"]
        );
    }

    #[test]
    fn collect_span_inverted_range_is_empty_partial() {
        let all = vec![
            capture("c3", 30, &["p"]),
            capture("c2", 20, &["p"]),
            capture("c1", 10, &["p"]),
        ];
        // A ("c3") is newer than B ("c2") — nothing lies in that span.
        let (span, reached) = collect_span(Some("c3"), Some("c2"), |_: Option<&str>| {
            Ok::<_, ()>(all.clone())
        })
        .unwrap();
        assert!(span.is_empty());
        assert!(!reached);
    }

    #[test]
    fn collect_span_bound_off_lineage_is_partial() {
        let all = vec![capture("c2", 20, &["p"]), capture("c1", 10, &["p"])];
        // B ("zz") never appears: everything is skipped, walk exhausts.
        let (span, reached) = collect_span(Some("c1"), Some("zz"), |_: Option<&str>| {
            Ok::<_, ()>(all.clone())
        })
        .unwrap();
        assert!(span.is_empty());
        assert!(!reached);
    }

    #[test]
    fn collect_span_anchor_not_found_is_partial() {
        let page = vec![capture("c1", 10, &["p"])];
        let (span, reached) = collect_span(Some("zz"), None, |_: Option<&str>| {
            Ok::<_, ()>(page.clone())
        })
        .unwrap();
        assert!(!reached);
        assert_eq!(span.len(), 1);
    }

    #[test]
    fn collect_span_none_anchor_collects_all() {
        let page = vec![capture("c1", 10, &["p"]), capture("c0", 0, &["p"])];
        let (span, reached) =
            collect_span(None, None, |_: Option<&str>| Ok::<_, ()>(page.clone())).unwrap();
        assert!(reached);
        assert_eq!(span.len(), 2);
    }

    #[test]
    fn message_detection() {
        let v = |args: &[&str]| -> Vec<String> { args.iter().map(|s| s.to_string()).collect() };
        assert!(passthrough_has_message(&v(&["-m", "hi"])));
        assert!(passthrough_has_message(&v(&["-mhi"])));
        assert!(passthrough_has_message(&v(&["--message=hi"])));
        assert!(passthrough_has_message(&v(&["--file", "m.txt"])));
        assert!(passthrough_has_message(&v(&["--fixup=HEAD"])));
        assert!(passthrough_has_message(&v(&["-t", "tpl"])));
        assert!(!passthrough_has_message(&v(&["--allow-empty"])));
        assert!(!passthrough_has_message(&v(&["-a"])));
        assert!(!passthrough_has_message(&v(&["-Skeyid"])));
        // "-S" starts with S, not a message flag; "-C" IS reuse-message.
        assert!(passthrough_has_message(&v(&["-CHEAD"])));
    }

    #[test]
    fn subject_shapes() {
        let one = diff_of(&[("src/main.rs", 12, 3)]);
        assert_eq!(draft_subject(&one), "src/main.rs (+12/-3)");
        let scoped = diff_of(&[("src/a.rs", 1, 0), ("src/b.rs", 2, 0)]);
        assert_eq!(draft_subject(&scoped), "src/: 2 files (+3/-0)");
        let mixed = diff_of(&[("src/a.rs", 1, 0), ("docs/b.md", 2, 0)]);
        assert_eq!(draft_subject(&mixed), "2 files (+3/-0)");
        let empty = diff_of(&[]);
        assert_eq!(draft_subject(&empty), "empty squash frame");
    }

    #[test]
    fn message_body_mentions_restores_and_checkpoints() {
        let stats = SpanStats {
            count: 3,
            first_ms: Some(0),
            last_ms: Some(61_000),
            checkpoints: vec!["mid-work".into()],
            restores: 2,
            partial: false,
        };
        let msg = draft_message(&stats, &diff_of(&[("src/a.rs", 5, 2)]));
        assert!(msg.starts_with("src/a.rs (+5/-2)\n\n"));
        assert!(msg.contains("Squashed from 3 sheaf captures over 1m"));
        assert!(msg.contains("Checkpoints crossed: mid-work."));
        assert!(msg.contains("Restores crossed the span: 2."));
    }

    #[test]
    fn empty_span_message() {
        let msg = draft_message(&SpanStats::default(), &diff_of(&[]));
        assert!(msg.contains("No sheaf captures in this span (empty frame)."));
    }

    #[test]
    fn collect_span_handles_cross_page_bound_and_fetch_errors() {
        let mut first = vec![capture("new", 3, &["p"])];
        first.extend((0..999).map(|i| capture(&format!("f{i}"), 2, &["p"])));
        let pages = [
            (None, first),
            (
                Some("f998"),
                vec![capture("bound", 2, &["p"]), capture("old", 1, &["p"])],
            ),
        ];
        let mut calls = 0;
        let (span, reached) = collect_span(Some("old"), Some("bound"), |cursor| {
            let page = pages[calls].1.clone();
            assert_eq!(cursor, pages[calls].0);
            calls += 1;
            Ok::<_, ()>(page)
        })
        .unwrap();
        assert!(reached);
        assert_eq!(span.len(), 1);
        assert_eq!(span[0].id, "bound");

        let err = collect_span(Some("x"), None, |_cursor| Err::<Vec<Capture>, _>("failed"));
        assert_eq!(err, Err("failed"));
    }

    #[test]
    fn subject_and_message_cover_rename_duration_and_churn_table() {
        let mut diff = diff_of(&[("src/a.rs", 2, 1), ("src/b.rs", 5, 0)]);
        diff.entries[0].old_path = Some("src/old.rs".into());
        assert_eq!(
            draft_subject(&DiffOutcome {
                entries: vec![diff.entries[0].clone()],
                ..diff.clone()
            }),
            "src/old.rs => src/a.rs (+2/-1)"
        );
        let stats = SpanStats {
            count: 1,
            first_ms: Some(0),
            last_ms: Some(86_400_000 + 3_600_000),
            checkpoints: vec![],
            restores: 0,
            partial: true,
        };
        let msg = draft_message(&stats, &diff);
        assert!(msg.contains("1 sheaf capture over 1d 1h"));
        assert!(msg.contains("Capture walk ended before the anchor"));
        assert!(msg.contains("Top changes by churn"));
    }
}

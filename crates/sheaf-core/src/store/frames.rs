//! Commit frames: the append-only record pairing each `git commit` with the
//! span of sheaf captures it collapsed.
//!
//! A frame is written once, by the CLI's `squash -- <options>` path, right
//! after a successful commit: the commit sha, the anchor capture the span
//! started from, and the tip capture the `git-<short-sha>` checkpoint pinned.
//! Frames live in `.sheaf/frames.jsonl` — sheaf's own store, never git notes
//! — and are advisory metadata for anchoring and drift detection; the
//! timeline itself never reads them.

use std::fs::OpenOptions;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::sheaf_dir;
use crate::error::{Result, SheafError};

/// One recorded git-commit ↔ timeline-span pairing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitFrame {
    /// Frame record format version.
    pub v: u32,
    /// Full commit sha.
    pub sha: String,
    /// Short sha as stamped into the checkpoint name (`git-<short_sha>`).
    pub short_sha: String,
    /// Capture the span started from (the previous frame's tip); absent for
    /// spans whose anchor named no capture (e.g. an empty-store HEAD time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_capture_id: Option<String>,
    /// How the anchor was chosen: the user's explicit reference, or omitted
    /// when the frame anchor / HEAD-time fallback resolved it implicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_ref: Option<String>,
    /// Capture the `git-<short_sha>` checkpoint pinned (the span's tip).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_capture_id: Option<String>,
    /// Git committer timestamp of the commit, ms since the epoch.
    pub committed_at_ms: i64,
    /// Wall clock when the frame was stamped, ms since the epoch.
    pub stamped_at_ms: i64,
    /// Captures collapsed by the commit (span size).
    pub captures: usize,
    /// Files changed, insertions, deletions — from the collapse candidate.
    pub files: usize,
    pub added: usize,
    pub removed: usize,
    /// Captures with restore provenance inside the span, so a collapse that
    /// crossed a rewind can be flagged for review.
    #[serde(default)]
    pub restores_crossed: usize,
    /// Subject line of the drafted (or committed) message, for quick audit.
    #[serde(default)]
    pub subject: String,
    /// Equality claim. Missing on every v1 line and therefore defaults to the
    /// original complete-frame meaning.
    #[serde(default)]
    pub kind: FrameKind,
    /// Present iff this commit projected selected content out of a timeline
    /// state rather than equalling a whole-worktree frontier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<Projection>,
}

/// Whether a frame equalled a whole-worktree frontier (`Complete`) or
/// projected selected content out of a timeline state (`Partial`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FrameKind {
    #[default]
    Complete,
    Partial,
}

/// Audit context for a partial frame: the git trees before and after, the
/// selections that formed the projected patch, and its hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Projection {
    pub parent_sha: String,
    pub git_tree_before: String,
    pub git_tree_after: String,
    pub selection_ids: Vec<String>,
    pub patch_sha256: String,
    /// Captured tip inspected at commit time. This is audit context, not the
    /// top-level `tip_capture_id` of a stampable complete frame.
    pub tip_capture_id: String,
}

impl CommitFrame {
    /// The `git-<short_sha>` checkpoint name this commit's frame pins.
    pub fn checkpoint_name(&self) -> String {
        format!("git-{}", self.short_sha)
    }

    /// Only complete frames whose checkpoint pinned a capture may anchor.
    pub fn is_anchor_eligible(&self) -> bool {
        self.kind == FrameKind::Complete && self.tip_capture_id.is_some()
    }

    /// The checkpoint name if this frame may anchor a future span, else `None`.
    pub fn anchor_eligible_name(&self) -> Option<String> {
        self.is_anchor_eligible().then(|| self.checkpoint_name())
    }

    /// Convert a fully populated frame record into a projected record while
    /// clearing the top-level checkpoint claim by construction.
    pub fn into_partial(mut self, projection: Projection) -> Self {
        self.kind = FrameKind::Partial;
        self.tip_capture_id = None;
        self.projection = Some(projection);
        self
    }

    /// Parse-time semantic validation used by doctor/future smart-squash
    /// readers; serde shape compatibility alone cannot enforce pairing.
    pub fn validate_projection(&self) -> std::result::Result<(), String> {
        match (
            self.kind,
            self.projection.is_some(),
            self.tip_capture_id.is_some(),
        ) {
            // A checkpoint-write failure has always produced an unanchored
            // complete recovery record with no tip; keep that v1 state valid.
            (FrameKind::Complete, false, _) => Ok(()),
            (FrameKind::Partial, true, false) => Ok(()),
            (FrameKind::Complete, true, _) => {
                Err("complete frame must not carry a projection".into())
            }
            (FrameKind::Partial, _, _) => {
                Err("partial frame needs projection and no top-level tip_capture_id".into())
            }
        }
    }
}

/// A new ordinary `git-<sha>` stamp is truthful only at a real three-way
/// equality point: commit tree, live worktree tree, and captured tip tree.
pub fn can_stamp_complete_frame(
    commit_tree: &str,
    worktree_tree: &str,
    captured_tree: &str,
) -> bool {
    !commit_tree.is_empty() && commit_tree == worktree_tree && worktree_tree == captured_tree
}

/// Pass-through anchor for the fallback when no complete frame exists: a
/// partial (projected) commit never pins the anchor itself, so we borrow its
/// recorded span anchor instead. Invalid partial records are ignored rather
/// than allowed to influence anchoring.
pub fn newest_partial_anchor(frames: &[CommitFrame]) -> Option<&str> {
    frames.iter().rev().find_map(|frame| {
        (frame.kind == FrameKind::Partial && frame.validate_projection().is_ok())
            .then_some(frame.anchor_capture_id.as_deref())
            .flatten()
    })
}

/// Count of valid partial (projected) frames in the ledger.
pub fn partial_frame_count(frames: &[CommitFrame]) -> usize {
    frames
        .iter()
        .filter(|frame| frame.kind == FrameKind::Partial && frame.validate_projection().is_ok())
        .count()
}

/// Path of the frame ledger inside a project's store.
pub fn frames_path(root: &Path) -> std::path::PathBuf {
    sheaf_dir(root).join("frames.jsonl")
}

fn append_json_line(root: &Path, value: &impl Serialize) -> Result<()> {
    let mut line = serde_json::to_string(value)
        .map_err(|e| SheafError::StoreCorrupt(format!("serialize frame: {e}")))?;
    line.push('\n');
    let path = frames_path(root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            SheafError::Io(std::io::Error::other(format!(
                "create {}: {e}",
                parent.display()
            )))
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| {
            SheafError::Io(std::io::Error::other(format!(
                "open {}: {e}",
                path.display()
            )))
        })?;
    file.write_all(line.as_bytes())
        .and_then(|_| file.flush())
        .and_then(|_| file.sync_all())
        .map_err(|e| {
            SheafError::Io(std::io::Error::other(format!(
                "append {}: {e}",
                path.display()
            )))
        })?;
    Ok(())
}

/// Append one complete or partial frame durably: write line, flush, fsync.
pub fn append_frame(root: &Path, frame: &CommitFrame) -> Result<()> {
    append_json_line(root, frame)
}

/// Read every complete or partial frame, oldest first. A torn trailing line
/// is dropped and reported; an invalid complete line fails closed.
pub fn read_frames(root: &Path) -> Result<(Vec<CommitFrame>, usize)> {
    let path = frames_path(root);
    let Ok(file) = std::fs::File::open(&path) else {
        return Ok((Vec::new(), 0));
    };
    let mut reader = BufReader::new(file);
    let mut frames = Vec::new();
    let mut torn = 0usize;
    let mut buf: Vec<u8> = Vec::new();
    loop {
        buf.clear();
        let read = reader.read_until(b'\n', &mut buf).map_err(|e| {
            SheafError::Io(std::io::Error::other(format!(
                "read {}: {e}",
                path.display()
            )))
        })?;
        if read == 0 {
            break;
        }
        // The trailing newline separates an interrupted append from durable
        // mid-file corruption.
        let complete = buf.last() == Some(&b'\n');
        let trimmed = String::from_utf8_lossy(&buf).trim().to_owned();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<CommitFrame>(&trimmed) {
            Ok(frame) => frames.push(frame),
            Err(_) if !complete => torn += 1,
            Err(_) => {
                return Err(SheafError::StoreCorrupt(format!(
                    "{}: unparseable frame record `{}`",
                    path.display(),
                    &trimmed[..trimmed.len().min(120)]
                )))
            }
        }
    }
    Ok((frames, torn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Deserialize)]
    struct LegacyV1Frame {
        v: u32,
        sha: String,
        short_sha: String,
        committed_at_ms: i64,
        stamped_at_ms: i64,
        captures: usize,
        files: usize,
        added: usize,
        removed: usize,
    }

    fn frame(sha: &str, captures: usize) -> CommitFrame {
        CommitFrame {
            v: 1,
            sha: sha.to_owned(),
            short_sha: sha[..8].to_owned(),
            anchor_capture_id: Some("a".repeat(64)),
            anchor_ref: None,
            tip_capture_id: Some("b".repeat(64)),
            committed_at_ms: 1_000,
            stamped_at_ms: 1_001,
            captures,
            files: 3,
            added: 10,
            removed: 4,
            restores_crossed: 0,
            subject: "src: 3 files, +10/-4".to_owned(),
            kind: FrameKind::Complete,
            projection: None,
        }
    }

    fn projected(sha: &str) -> CommitFrame {
        CommitFrame {
            v: 1,
            sha: sha.to_owned(),
            short_sha: sha[..8.min(sha.len())].to_owned(),
            anchor_capture_id: Some("a".repeat(64)),
            anchor_ref: None,
            tip_capture_id: None,
            committed_at_ms: 2_000,
            stamped_at_ms: 2_001,
            captures: 4,
            files: 1,
            added: 3,
            removed: 1,
            restores_crossed: 0,
            subject: "selected function".into(),
            kind: FrameKind::Partial,
            projection: Some(Projection {
                parent_sha: "parent".into(),
                git_tree_before: "tree-before".into(),
                git_tree_after: "tree-selected".into(),
                selection_ids: vec!["selection-a".into()],
                patch_sha256: "patch".into(),
                tip_capture_id: "b".repeat(64),
            }),
        }
    }

    #[test]
    fn append_then_read_roundtrip() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        append_frame(&dir, &frame("aa111111bb222222cc", 1)).unwrap();
        append_frame(&dir, &frame("dd333333bb222222cc", 2)).unwrap();
        let (frames, torn) = read_frames(&dir).unwrap();
        assert_eq!(torn, 0);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].captures, 1);
        assert_eq!(frames[1].checkpoint_name(), "git-dd333333");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn torn_tail_dropped_with_count() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        append_frame(&dir, &frame("aa111111bb222222cc", 1)).unwrap();
        // Simulate a crash mid-append: partial JSON, no newline.
        let path = frames_path(&dir);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"{\"v\":1,\"sha\":\"trunc").unwrap();
        let (frames, torn) = read_frames(&dir).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(torn, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mid_file_corruption_fails_closed() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        append_frame(&dir, &frame("aa111111bb222222cc", 1)).unwrap();
        let path = frames_path(&dir);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"garbage line\n").unwrap();
        append_frame(&dir, &frame("dd333333bb222222cc", 2)).unwrap();
        assert!(read_frames(&dir).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn complete_and_partial_records_share_one_backward_compatible_ledger() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-mixed-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        append_frame(&dir, &frame("aa111111bb222222cc", 2)).unwrap();
        append_frame(&dir, &projected("partial-sha")).unwrap();

        let (frames, torn) = read_frames(&dir).unwrap();
        assert_eq!(torn, 0);
        assert_eq!(frames.len(), 2);
        assert!(frames[0].is_anchor_eligible());
        assert_eq!(
            frames[0].anchor_eligible_name().as_deref(),
            Some("git-aa111111")
        );
        assert_eq!(frames[0].validate_projection(), Ok(()));
        assert!(!frames[1].is_anchor_eligible());
        assert_eq!(frames[1].anchor_eligible_name(), None);
        assert_eq!(frames[1].validate_projection(), Ok(()));
        assert_eq!(partial_frame_count(&frames), 1);
        assert_eq!(
            newest_partial_anchor(&frames),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

        // Old readers ignore unknown additive fields and still see every v1
        // required key on the partial line.
        let raw = std::fs::read_to_string(frames_path(&dir)).unwrap();
        let partial_line = raw.lines().nth(1).unwrap();
        let legacy: LegacyV1Frame = serde_json::from_str(partial_line).unwrap();
        assert_eq!(legacy.sha, "partial-sha");
        let partial: serde_json::Value = serde_json::from_str(partial_line).unwrap();
        for key in [
            "v",
            "sha",
            "short_sha",
            "committed_at_ms",
            "stamped_at_ms",
            "captures",
            "files",
            "added",
            "removed",
        ] {
            assert!(
                partial.get(key).is_some(),
                "partial record keeps v1 key `{key}`"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn partial_commit_requires_later_three_way_equality_before_stamp() {
        assert!(!can_stamp_complete_frame(
            "git-tree-a",
            "worktree-a-plus-b",
            "captured-a-plus-b"
        ));
        assert!(can_stamp_complete_frame(
            "git-tree-a-plus-b",
            "git-tree-a-plus-b",
            "git-tree-a-plus-b"
        ));
    }

    #[test]
    fn malformed_kind_projection_pairs_fail_validation() {
        let mut unstamped_complete = frame("complete-sha", 1);
        unstamped_complete.tip_capture_id = None;
        assert_eq!(unstamped_complete.validate_projection(), Ok(()));
        assert!(!unstamped_complete.is_anchor_eligible());

        let mut malformed = projected("partial-sha");
        malformed.tip_capture_id = Some("wrongly-stamped".into());
        assert!(malformed.validate_projection().is_err());
        malformed.kind = FrameKind::Complete;
        assert!(malformed.validate_projection().is_err());
    }

    #[test]
    fn missing_file_reads_empty() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (frames, torn) = read_frames(&dir).unwrap();
        assert!(frames.is_empty());
        assert_eq!(torn, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn blank_lines_between_records_are_skipped() {
        let dir = std::env::temp_dir().join(format!("sheaf-frames-blank-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        append_frame(&dir, &frame("aa111111bb222222cc", 1)).unwrap();
        let mut f = OpenOptions::new()
            .append(true)
            .open(frames_path(&dir))
            .unwrap();
        f.write_all(b"\n\n").unwrap();
        append_frame(&dir, &frame("dd333333bb222222cc", 2)).unwrap();
        let (frames, torn) = read_frames(&dir).unwrap();
        assert_eq!(frames.len(), 2, "blank separator lines are not records");
        assert_eq!(torn, 0, "a complete blank line is not a torn tail either");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn store_dir_as_file_fails_closed_on_append() {
        // `.sheaf` exists as a regular file: the frame ledger's directory
        // cannot be created, and the failure is the mapped Io variant.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".sheaf"), "not a directory").unwrap();
        let err = append_frame(tmp.path(), &frame("aa111111bb222222cc", 1)).unwrap_err();
        assert!(matches!(err, SheafError::Io(_)), "{err:?}");
    }

    #[test]
    fn ledger_path_as_directory_fails_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(frames_path(tmp.path())).unwrap();
        let err = append_frame(tmp.path(), &frame("aa111111bb222222cc", 1)).unwrap_err();
        assert!(matches!(err, SheafError::Io(_)), "{err:?}");
    }

    #[test]
    fn into_partial_produces_a_valid_projected_record() {
        let complete = frame("ee555555bb222222cc", 3);
        assert!(complete.is_anchor_eligible());

        let partial = complete.into_partial(Projection {
            parent_sha: "parent".into(),
            git_tree_before: "before".into(),
            git_tree_after: "after".into(),
            selection_ids: vec!["sel-1".into()],
            patch_sha256: "digest".into(),
            tip_capture_id: "b".repeat(64),
        });
        assert_eq!(partial.kind, FrameKind::Partial);
        assert!(partial.tip_capture_id.is_none(), "top-level tip is cleared");
        assert!(partial.projection.is_some());
        assert_eq!(partial.validate_projection(), Ok(()));
        assert!(
            !partial.is_anchor_eligible(),
            "projected commits never anchor"
        );
        assert_eq!(partial.anchor_eligible_name(), None);
    }

    #[test]
    fn invalid_partial_frames_do_not_influence_anchoring_or_counts() {
        let mut bad = projected("bad-partial");
        bad.tip_capture_id = Some("wrongly-stamped".into()); // fails validation
        assert_eq!(partial_frame_count(std::slice::from_ref(&bad)), 0);

        let mut valid = projected("good-partial");
        valid.anchor_capture_id = Some("c".repeat(64));

        let frames = vec![frame("aa111111bb222222cc", 1), bad, valid];
        // The newest VALID partial supplies the borrowed anchor; the invalid
        // one is skipped rather than allowed to influence anchoring.
        assert_eq!(
            newest_partial_anchor(&frames),
            Some("c".repeat(64).as_str())
        );
        assert_eq!(
            partial_frame_count(&frames),
            1,
            "invalid partials are not counted"
        );
    }
}

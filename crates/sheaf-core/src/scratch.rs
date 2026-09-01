//! Scratch ring: a bounded, best-effort recovery net for volatile paths.
//!
//! The timeline is append-only history for durable work; volatile paths
//! (editor swap files, atomic-save temps, machine-local litter) are kept OUT
//! of it by classification. But "not work" is not "worthless" — an editor
//! crash leaves the only copy of unsaved bytes in a swap file. This module
//! is the second, deliberately dumber flight recorder for those paths:
//!
//! - JSONL segment files under `.sheaf/scratch/`, one record per line:
//!   `{ts, root, path, size, mtime, trunc, b64}` snapshots (capped at
//!   `max_file_bytes` per file) and `{ts, root, path, gone}` markers.
//! - Bounded: segments rotate at [`SEGMENT_MAX_BYTES`]; once the ring
//!   exceeds `max_bytes`, the OLDEST segments are deleted. The ring is
//!   self-limiting and invisible to `gc`, the journal, and `doctor`'s
//!   integrity checks — a corrupted ring line is skipped, never fatal.
//! - Best-effort by construction: the writer never propagates IO errors
//!   (a failing disk takes the ring, not the daemon), and the reader
//!   tolerates torn tails from crashes mid-append.
//!
//! Recovery is `sheaf recover`: list snapshots for a path, or bring the
//! latest (or a chosen) one back. It is deliberately NOT `sheaf restore`:
//! there are no timeline points, no branches, no CRDT semantics here —
//! just "the bytes it held, last time we looked".

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Default total ring bound: 256 MiB.
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Default per-file content cap: 1 MiB.
pub const DEFAULT_FILE_BYTES: u64 = 1024 * 1024;
/// Default longest gap between flushes when only volatile paths change.
pub const DEFAULT_FLUSH_MS: u64 = 30_000;
/// Segments rotate at this size so pruning can drop coarse chunks.
const SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// One ring record: a capped snapshot of a volatile path, or its end.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScratchRecord {
    pub v: u32,
    /// Wall clock when the record was appended (ms since epoch).
    pub ts_ms: i64,
    /// Absolute worktree root that observed the path (managed worktrees
    /// share one ring; this field keeps their records apart).
    pub root: PathBuf,
    /// Root-relative path.
    pub path: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gone: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ms: Option<i64>,
    /// Content was cut at the per-file cap.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trunc: bool,
    /// Head of the file's bytes, hex-encoded (2× size, zero dependencies,
    /// and the ring's bounds do the real limiting). Absent for `gone`
    /// markers and unreadable files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hex: Option<String>,
}

impl ScratchRecord {
    fn now(root: &Path, path: &str) -> Self {
        ScratchRecord {
            v: 1,
            ts_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            root: root.to_path_buf(),
            path: path.to_string(),
            gone: false,
            size: None,
            mtime_ms: None,
            trunc: false,
            content_hex: None,
        }
    }

    /// Decoded snapshot bytes, when the record carries any.
    pub fn content(&self) -> Option<Vec<u8>> {
        self.content_hex.as_ref().and_then(|h| hex::decode(h).ok())
    }
}

fn segment_name(seq: u64) -> String {
    format!("seg-{seq:08}.jsonl")
}

fn segment_seq(name: &str) -> Option<u64> {
    let stem = name.strip_prefix("seg-")?.strip_suffix(".jsonl")?;
    u64::from_str_radix(stem, 10).ok()
}

/// Existing segment files under `dir`, parseable names only, oldest first.
fn list_segments(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut out: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    let seq = segment_seq(&name)?;
                    Some((seq, e.path()))
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_unstable();
    out
}

/// The ring's writer side. One instance per store; records from every
/// watched worktree interleave, tagged by `root`.
pub struct ScratchWriter {
    dir: PathBuf,
    max_bytes: u64,
    max_file_bytes: u64,
    seg: Option<BufWriter<File>>,
    seg_path: PathBuf,
    seg_bytes: u64,
    next_seq: u64,
    /// (root, rel) -> (mtime, size) of the last appended snapshot; a file
    /// whose identity is unchanged since then is not re-read or re-appended.
    last: HashMap<(PathBuf, String), (SystemTime, u64)>,
}

impl ScratchWriter {
    /// Open (or attach to) the ring under `dir`. Existing segments are
    /// appended to when the newest is under the rotation size, so a daemon
    /// restart continues the same ring instead of resetting it.
    pub fn open(dir: &Path, max_bytes: u64, max_file_bytes: u64) -> ScratchWriter {
        let _ = std::fs::create_dir_all(dir);
        let mut next_seq = 1u64;
        let mut seg = None;
        let mut seg_path = dir.join(segment_name(1));
        let mut seg_bytes = 0u64;
        if let Some((seq, existing)) = list_segments(dir).pop() {
            next_seq = seq + 1;
            seg_bytes = std::fs::metadata(&existing).map(|m| m.len()).unwrap_or(0);
            if seg_bytes < SEGMENT_MAX_BYTES.min(max_bytes.max(1024)) {
                if let Ok(file) = OpenOptions::new().append(true).open(&existing) {
                    seg = Some(BufWriter::new(file));
                    seg_path = existing;
                }
            }
        }
        if seg.is_none() {
            // Fresh ring, or the newest segment was full: rotate to a new one.
            seg_path = dir.join(segment_name(next_seq));
            seg_bytes = 0;
            if let Ok(file) = File::create(&seg_path) {
                seg = Some(BufWriter::new(file));
            }
        }
        ScratchWriter {
            dir: dir.to_path_buf(),
            max_bytes,
            max_file_bytes,
            seg,
            seg_path,
            seg_bytes,
            next_seq,
            last: HashMap::new(),
        }
    }

    /// A ring that drops every write (`[scratch] enabled = false`).
    pub fn disabled() -> ScratchWriter {
        // A path that can never exist keeps every IO op a silent no-op.
        let impossible = PathBuf::from("/dev/null/.sheaf-scratch-disabled");
        ScratchWriter::open(&impossible, 0, 0)
    }

    pub fn is_enabled(&self) -> bool {
        self.max_bytes > 0 && self.seg.is_some()
    }

    /// Segments rotate at the segment size OR the ring bound, whichever is
    /// smaller: a small ring must still rotate so pruning has whole
    /// segments to drop.
    fn rotation_limit(&self) -> u64 {
        SEGMENT_MAX_BYTES.min(self.max_bytes.max(1024))
    }

    /// Append a capped snapshot of `abs` (root-relative `rel`) if its
    /// (mtime, size) identity changed since the last appended snapshot.
    /// Directories and vanished files append nothing; disappearance is the
    /// caller's `gone` marker to write.
    pub fn snapshot(&mut self, root: &Path, abs: &Path, rel: &str) {
        if !self.is_enabled() {
            return;
        }
        let Ok(meta) = std::fs::symlink_metadata(abs) else {
            return;
        };
        if !meta.is_file() {
            return;
        }
        let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
        let size = meta.len();
        if self
            .last
            .get(&(root.to_path_buf(), rel.to_string()))
            .is_some_and(|(m, s)| *m == mtime && *s == size)
        {
            return; // unchanged since the last snapshot
        }
        let mut rec = ScratchRecord::now(root, rel);
        rec.size = Some(size);
        rec.mtime_ms = mtime
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis() as i64);
        let mut head = Vec::with_capacity(self.max_file_bytes.min(1 << 20) as usize);
        if File::open(abs)
            .and_then(|mut f| {
                Read::by_ref(&mut f)
                    .take(self.max_file_bytes)
                    .read_to_end(&mut head)
            })
            .is_ok()
        {
            rec.trunc = (head.len() as u64) < size;
            rec.content_hex = Some(hex::encode(&head));
        }
        self.append(&rec);
        self.last
            .insert((root.to_path_buf(), rel.to_string()), (mtime, size));
    }

    /// Record that a volatile path disappeared. Metadata only — its last
    /// snapshot, if any, is what recovery would bring back.
    pub fn gone(&mut self, root: &Path, rel: &str) {
        if !self.is_enabled() {
            return;
        }
        let mut rec = ScratchRecord::now(root, rel);
        rec.gone = true;
        self.append(&rec);
    }

    /// Append one record, rotating and pruning around it. Best-effort: an
    /// IO error logs and drops the record; the ring never fails the daemon.
    fn append(&mut self, rec: &ScratchRecord) {
        let Some(seg) = self.seg.as_mut() else {
            return;
        };
        let line = match serde_json::to_string(rec) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "scratch record encode failed");
                return;
            }
        };
        let mut line = line.into_bytes();
        line.push(b'\n');
        if let Err(e) = seg.write_all(&line) {
            tracing::warn!(error = %e, "scratch append failed");
            return;
        }
        self.seg_bytes += line.len() as u64;
        if self.seg_bytes >= self.rotation_limit() {
            self.rotate();
        }
    }

    /// Flush the active segment to disk, then prune oldest segments while
    /// the ring exceeds its bound. The active segment is never pruned.
    pub fn flush(&mut self) {
        if let Some(seg) = self.seg.as_mut() {
            let _ = seg.flush();
        }
        self.prune();
    }

    fn rotate(&mut self) {
        if let Some(mut seg) = self.seg.take() {
            let _ = seg.flush();
            let _ = seg.into_inner().map(|f| f.sync_all());
        }
        self.seg_path = self.dir.join(segment_name(self.next_seq));
        self.seg_bytes = 0;
        self.next_seq += 1;
        if let Ok(file) = File::create(&self.seg_path) {
            self.seg = Some(BufWriter::new(file));
        }
        self.prune();
    }

    /// Drop oldest segments while the ring exceeds its bound. The newest
    /// CONTENT-BEARING segment is never dropped: right after a rotation the
    /// just-closed segment holds the newest records, and deleting it would
    /// discard exactly what recovery is most likely to ask for. The bound
    /// is therefore enforced modulo one segment.
    fn prune(&mut self) {
        let segments = list_segments(&self.dir);
        if segments.len() <= 1 {
            return;
        }
        let sizes: Vec<u64> = segments
            .iter()
            .map(|(_, p)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .collect();
        let mut total: u64 = sizes.iter().sum();
        let last_content = sizes
            .iter()
            .rposition(|&s| s > 0)
            .unwrap_or(segments.len() - 1);
        for i in 0..last_content {
            if total <= self.max_bytes {
                break;
            }
            match std::fs::remove_file(&segments[i].1) {
                Ok(()) => {
                    total = total.saturating_sub(sizes[i]);
                    tracing::debug!(segment = %segments[i].1.display(), "scratch ring pruned oldest segment");
                }
                Err(_) => break, // unreadable/undeletable: stop rather than loop
            }
        }
    }
}

/// Read every parseable record in the ring, oldest segment first, line
/// order within a segment. Unparseable lines (a torn crash tail, foreign
/// bytes) are skipped — the ring is advisory data, never a load gate.
pub fn read_records(dir: &Path) -> Vec<ScratchRecord> {
    let mut out = Vec::new();
    for (_, path) in list_segments(dir) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str(line) {
                Ok(rec) => out.push(rec),
                Err(_) => continue,
            }
        }
    }
    out
}

/// Every record for one path in one worktree, oldest first.
pub fn history(dir: &Path, root: &Path, rel: &str) -> Vec<ScratchRecord> {
    read_records(dir)
        .into_iter()
        .filter(|r| r.root == root && r.path == rel)
        .collect()
}

/// The newest snapshot carrying content for one path, if the ring holds one.
pub fn latest_snapshot(dir: &Path, root: &Path, rel: &str) -> Option<ScratchRecord> {
    history(dir, root, rel)
        .into_iter()
        .rev()
        .find(|r| r.content_hex.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer(dir: &Path, max_bytes: u64, file_cap: u64) -> ScratchWriter {
        ScratchWriter::open(dir, max_bytes, file_cap)
    }

    #[test]
    fn snapshot_round_trips_content_and_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join(".sheaf/scratch");
        std::fs::write(root.join("notes.md.swp"), b"unsaved buffer\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(root, &root.join("notes.md.swp"), "notes.md.swp");
        w.flush();
        let latest = latest_snapshot(&dir, root, "notes.md.swp").expect("snapshot recorded");
        assert!(!latest.gone);
        assert!(!latest.trunc);
        assert_eq!(latest.size, Some(15));
        assert_eq!(latest.content().as_deref(), Some(&b"unsaved buffer\n"[..]));
    }

    #[test]
    fn unchanged_identity_is_not_re_recorded_but_changes_are() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        let file = root.join("a.tmp");
        std::fs::write(&file, b"one\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(root, &file, "a.tmp");
        w.snapshot(root, &file, "a.tmp"); // identical (mtime, size): skipped
        w.flush();
        assert_eq!(history(&dir, root, "a.tmp").len(), 1);
        // A different size is a different identity on every filesystem.
        std::fs::write(&file, b"two and more\n").unwrap();
        w.snapshot(root, &file, "a.tmp");
        w.flush();
        let records = history(&dir, root, "a.tmp");
        assert_eq!(records.len(), 2, "{records:?}");
        assert_eq!(
            records[1].content().as_deref(),
            Some(&b"two and more\n"[..])
        );
    }

    #[test]
    fn oversize_file_is_truncated_to_the_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        std::fs::write(root.join("heap.profraw"), vec![7u8; 4096]).unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, 1024);
        w.snapshot(root, &root.join("heap.profraw"), "heap.profraw");
        w.flush();
        let rec = latest_snapshot(&dir, root, "heap.profraw").unwrap();
        assert!(rec.trunc);
        assert_eq!(rec.size, Some(4096));
        assert_eq!(rec.content().map(|c| c.len()), Some(1024));
    }

    #[test]
    fn gone_marker_carries_no_content_and_orders_last() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        std::fs::write(root.join("swap"), b"final bytes\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(root, &root.join("swap"), "swap");
        w.gone(root, "swap");
        w.flush();
        let h = history(&dir, root, "swap");
        assert_eq!(h.len(), 2);
        assert!(h[1].gone && h[1].content_hex.is_none());
        // Recovery prefers the last content-bearing record.
        assert_eq!(
            latest_snapshot(&dir, root, "swap").and_then(|r| r.content()),
            Some(b"final bytes\n".to_vec())
        );
    }

    #[test]
    fn ring_is_bounded_by_deleting_oldest_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        // 1 KiB per record, tiny rotation and total bounds.
        let mut w = writer(&dir, 3 * 1024, DEFAULT_FILE_BYTES);
        for i in 0..40 {
            let name = format!("f{i:02}.tmp");
            let file = root.join(&name);
            // Distinct path per record: the identity map keys differ, so
            // no mtime forcing is needed.
            std::fs::write(&file, vec![b'x'; 900]).unwrap();
            w.snapshot(root, &file, &name);
            w.flush();
        }
        let total: u64 = list_segments(&dir)
            .iter()
            .filter_map(|(_, p)| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        assert!(
            total <= 3 * 1024 + SEGMENT_MAX_BYTES as u64 + 2048,
            "ring stayed near its bound, got {total}"
        );
        // The newest files are still recoverable; the oldest are gone.
        assert!(latest_snapshot(&dir, root, "f39.tmp").is_some());
        assert!(latest_snapshot(&dir, root, "f00.tmp").is_none());
    }

    #[test]
    fn torn_tail_lines_are_skipped_by_the_reader() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        std::fs::write(root.join("ok.tmp"), b"fine\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(root, &root.join("ok.tmp"), "ok.tmp");
        w.flush();
        // Simulate a crash mid-append on the active segment.
        let (_, active) = list_segments(&dir).pop().unwrap();
        let mut bytes = std::fs::read(&active).unwrap();
        bytes.extend_from_slice(b"{\"v\":1,\"ts\":12345,\"path\":\"torn"); // no newline
        std::fs::write(&active, bytes).unwrap();
        let recs = read_records(&dir);
        assert!(recs.iter().any(|r| r.path == "ok.tmp"));
        assert!(recs.iter().all(|r| r.path != "torn"));
    }

    #[test]
    fn records_of_other_worktrees_do_not_leak() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("wt-a");
        let b = tmp.path().join("wt-b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let dir = tmp.path().join("scratch");
        std::fs::write(a.join("shared.name"), b"from a\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(&a, &a.join("shared.name"), "shared.name");
        w.flush();
        assert!(history(&dir, &b, "shared.name").is_empty());
        assert_eq!(history(&dir, &a, "shared.name").len(), 1);
    }

    #[test]
    fn restart_attaches_to_the_existing_ring() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("scratch");
        std::fs::write(root.join("one.tmp"), b"1\n").unwrap();
        let mut w = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w.snapshot(root, &root.join("one.tmp"), "one.tmp");
        w.flush();
        drop(w);
        std::fs::write(root.join("two.tmp"), b"2\n").unwrap();
        let mut w2 = writer(&dir, DEFAULT_MAX_BYTES, DEFAULT_FILE_BYTES);
        w2.snapshot(root, &root.join("two.tmp"), "two.tmp");
        w2.flush();
        assert!(
            latest_snapshot(&dir, root, "one.tmp").is_some(),
            "earlier records survive"
        );
        assert!(latest_snapshot(&dir, root, "two.tmp").is_some());
    }

    #[test]
    fn disabled_writer_records_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut w = ScratchWriter::disabled();
        assert!(!w.is_enabled());
        std::fs::write(root.join("x.tmp"), b"x\n").unwrap();
        w.snapshot(root, &root.join("x.tmp"), "x.tmp");
        w.gone(root, "x.tmp");
        w.flush();
        assert!(read_records(&root.join(".sheaf/scratch")).is_empty());
    }
}

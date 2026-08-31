//! Framed append-only journal: each record is
//! `[u32 le length][u32 le crc32c][payload]`, O_APPEND-written then fsync'd.
//! A torn tail (crash mid-frame) is dropped on load; earlier frames survive.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// `<root>/.sheaf/store/journal`
pub fn journal_dir(store_dir: &Path) -> PathBuf {
    store_dir.join("journal")
}

fn seg_name(idx: u64) -> String {
    format!("seg-{idx:06}.op")
}

fn parse_seg_idx(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let raw = name.strip_prefix("seg-")?.strip_suffix(".op")?;
    raw.parse().ok()
}

/// All journal segments on disk, sorted by ascending segment index.
pub fn list_segments(store_dir: &Path) -> Vec<(u64, PathBuf)> {
    let dir = journal_dir(store_dir);
    match std::fs::read_dir(&dir) {
        Ok(rd) => {
            let mut out: Vec<(u64, PathBuf)> = rd
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .map(|e| e.path())
                .filter_map(|p| parse_seg_idx(&p).map(|i| (i, p)))
                .collect();
            out.sort_by_key(|(i, _)| *i);
            out
        }
        Err(_) => Vec::new(),
    }
}

#[derive(Debug)]
enum FrameErr {
    Torn(io::Error),
    BadCrc,
    BadLength,
}

fn read_one_frame(f: &mut File) -> Result<Vec<u8>, FrameErr> {
    let mut hdr = [0u8; 8];
    match f.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) => return Err(FrameErr::Torn(e)),
    }
    let len = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(hdr[4..].try_into().unwrap());
    if len > 512 * 1024 * 1024 {
        // Insane length = corrupted header, not a legitimate record.
        let _ = len;
        return Err(FrameErr::BadLength);
    }
    let mut payload = vec![0u8; len];
    if let Err(e) = f.read_exact(&mut payload) {
        return Err(FrameErr::Torn(e));
    }
    if crc32c::crc32c(&payload) != crc_stored {
        return Err(FrameErr::BadCrc);
    }
    Ok(payload)
}

// ------------------------------------------------------------------ writer

/// The single-writer append handle: tracks the open segment, its size, and a
/// monotonic record counter, rotating to a new segment past a size threshold.
pub struct JournalWriter {
    store_dir: PathBuf,
    max_segment_bytes: u64,
    /// Index of the currently-open segment.
    pub index: u64,
    handle: Option<File>,
    /// Bytes written into the open segment (0 right after a rotation).
    pub written_in_segment: u64,
    /// Number of records appended since open (monotonic counter).
    pub records_appended: u64,
}

impl JournalWriter {
    /// Continue the highest uncovered segment, or start a fresh one at
    /// `start_index` when everything on disk is already snapshot-covered.
    pub fn resume(
        store_dir: &Path,
        covered_upto: Option<u64>,
        max_segment_bytes: u64,
    ) -> io::Result<JournalWriter> {
        std::fs::create_dir_all(journal_dir(store_dir))?;
        let segments = list_segments(store_dir);
        let target: Option<u64> = segments
            .iter()
            .filter(|(idx, _)| covered_upto.is_none_or(|c| *idx > c))
            .map(|(idx, _)| Some(*idx))
            .next_back()
            .flatten();
        let index = match target {
            Some(i) => i,
            None => match covered_upto {
                Some(c) => c + 1,
                None => segments.last().map(|(i, _)| *i + 1).unwrap_or(1),
            },
        };
        let mut w = JournalWriter {
            store_dir: store_dir.to_path_buf(),
            max_segment_bytes,
            index,
            handle: None,
            written_in_segment: 0,
            records_appended: 0,
        };
        w.open_segment()?;
        Ok(w)
    }

    fn segment_path(&self) -> PathBuf {
        journal_dir(&self.store_dir).join(seg_name(self.index))
    }

    fn open_segment(&mut self) -> io::Result<()> {
        let path = self.segment_path();
        // Repair-before-append: a crash can leave a half-written final frame.
        // Anything after the last intact frame is physically removed so all
        // future appends land on a frame boundary readers can trust. Frames
        // BEFORE it stand (they were fsync'd individually).
        if path.exists() {
            if let Some(valid_len) = scan_intact_prefix(&path) {
                let cur = std::fs::metadata(&path)?.len();
                if valid_len < cur {
                    tracing::warn!(
                        segment = %path.display(),
                        dropped_bytes = cur - valid_len,
                        "torn tail truncated before resume"
                    );
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(valid_len)?;
                    f.sync_all()?;
                }
            }
        }
        // O_APPEND: kernel guarantees atomic appends for our single-writer case;
        // also protects against accidental in-place writes after any crash.
        let fresh = !path.exists();
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .custom_flags(0)
            .mode(0o600)
            .open(path)?;
        // A newly created segment's directory entry joins the durability
        // story: fsync the directory so the empty-but-present segment cannot
        // vanish in a power cut while its index is already implied.
        if fresh {
            super::fsutil::sync_dir(&journal_dir(&self.store_dir))?;
        }
        self.written_in_segment = f.metadata()?.len();
        self.handle = Some(f);
        Ok(())
    }

    /// Append several frames with ONE fsync so they land or tear as a unit:
    /// a capture's update delta and its ledger record are a pair, so a torn
    /// tail must drop whole frames, never half of that pair's durability
    /// story.
    pub fn append_batch_synced(&mut self, payloads: &[&[u8]]) -> io::Result<()> {
        if self.handle.is_none() {
            self.open_segment()?;
        }
        let f = self.handle.as_mut().expect("segment opened");
        let mut written = 0u64;
        for payload in payloads {
            let frame_len = 8 + payload.len();
            let mut header = [0u8; 8];
            header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            header[4..].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
            // Keep each exported payload the only payload-sized allocation;
            // a crash between writes is an ordinary torn tail and recovery
            // already truncates at the preceding intact frame.
            f.write_all(&header)?;
            f.write_all(payload)?;
            written += frame_len as u64;
            self.records_appended += 1;
        }
        f.sync_all()?; // durability boundary: frames are durable only after this
        self.written_in_segment += written;

        if self.written_in_segment >= self.max_segment_bytes {
            self.rotate()?;
        }
        Ok(())
    }

    /// Close current segment and EAGERLY create its successor so any later
    /// `resume()` (even one landing in a fresh process) picks the new index
    /// rather than re-appending behind a cleanly closed frame set.
    pub fn rotate(&mut self) -> io::Result<u64> {
        if let Some(f) = self.handle.take() {
            f.sync_all()?;
        }
        let closed = self.index;
        self.index += 1;
        self.written_in_segment = 0;
        self.open_segment()?;
        Ok(closed)
    }
}

// ------------------------------------------------------------------- loader

/// Byte offset just past the last INTACT frame, walking the framing chain.
/// "Intact" = sane length + full payload present + payload CRC matches —
/// so garbage-after-a-tear never masquerades as a resync point.
pub fn scan_intact_prefix(path: &Path) -> Option<u64> {
    let mut f = File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    let mut off: u64 = 0;
    loop {
        let mut hdr = [0u8; 8];
        match read_exact_upto(&mut f, &mut hdr) {
            Ok(true) => {}
            Ok(false) | Err(_) => return Some(off), // clean end or short header
        }
        let plen = u32::from_le_bytes(hdr[..4].try_into().ok()?) as u64;
        let want_crc = u32::from_le_bytes(hdr[4..].try_into().ok()?);
        if plen > 512 * 1024 * 1024 || off + 8 + plen > len {
            return Some(off);
        }
        let mut payload = vec![0u8; plen as usize];
        match read_exact_upto(&mut f, &mut payload) {
            Ok(true) => {}
            Ok(false) | Err(_) => return Some(off),
        }
        if crc32c::crc32c(&payload) != want_crc {
            return Some(off);
        }
        off += 8 + plen;
    }
}

/// Read exactly buf.len(); true=full, false=eof-hit.
fn read_exact_upto(f: &mut File, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match f.read(&mut buf[filled..]) {
            Ok(0) => return Ok(false),
            Ok(n) => filled += n,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// One intact framed record with its segment index and in-segment ordinal.
pub struct SegmentRecord {
    pub segment: u64,
    pub ordinal: usize,
    pub payload: Vec<u8>,
}

/// Visit every intact framed record while holding only one payload in memory.
/// Returning `false` stops the replay immediately. A torn tail terminates its
/// own segment (earlier frames stand); later segments remain independently
/// readable because ordering across segments is causal by construction.
pub fn visit_records(
    paths: &[(u64, PathBuf)],
    mut visit: impl FnMut(Result<SegmentRecord, (u64, String)>) -> bool,
) {
    'segments: for &(seg, ref path) in paths {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                if !visit(Err((seg, format!("open failed: {e}")))) {
                    break;
                }
                continue;
            }
        };
        for ordinal in 0usize.. {
            match read_one_frame(&mut f) {
                Ok(payload) => {
                    if !visit(Ok(SegmentRecord {
                        segment: seg,
                        ordinal,
                        payload,
                    })) {
                        break 'segments;
                    }
                }
                Err(FrameErr::Torn(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break; // clean end or torn tail — both stop this segment
                }
                Err(e @ (FrameErr::BadCrc | FrameErr::BadLength)) => {
                    tracing::warn!(segment = seg, ordinal, error = ?e, "torn tail dropped");
                    break;
                }
                Err(FrameErr::Torn(e)) => {
                    if !visit(Err((seg, format!("read failed: {e}")))) {
                        break 'segments;
                    }
                    break;
                }
            }
        }
    }
}

/// Collecting compatibility helper for maintenance/tests. Writer recovery and
/// timeline loading use [`visit_records`] so journal size cannot become RAM.
pub fn read_records(paths: &[(u64, PathBuf)]) -> Vec<Result<SegmentRecord, (u64, String)>> {
    let mut out = Vec::new();
    visit_records(paths, |record| {
        out.push(record);
        true
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let paths = {
            let mut w = JournalWriter::resume(dir, None, 1024 * 1024).unwrap();
            for rec in [b"alpha".as_slice(), b"beta", b"gamma"] {
                w.append_batch_synced(&[rec]).unwrap();
            }
            w.rotate().unwrap(); // force seg-2
            let mut w2 = JournalWriter::resume(dir, None, 1024 * 1024).unwrap();
            assert_eq!(w2.index, 2, "continues from rotation");
            w2.append_batch_synced(&[b"delta"]).unwrap();
            drop(w2);
            list_segments(dir)
        };
        assert_eq!(paths.len(), 2);
        let got = read_records(&paths);
        let payloads: Vec<&[u8]> = got
            .iter()
            .map(|r| r.as_ref().unwrap().payload.as_slice())
            .collect();
        assert_eq!(payloads, [b"alpha".as_slice(), b"beta", b"gamma", b"delta"]);

        // Simulate kill -9 mid-frame: chop the last byte of the final record.
        let last = &paths[1].1;
        let sz = std::fs::metadata(last).unwrap().len();
        std::fs::set_permissions(last, std::os::unix::fs::PermissionsExt::from_mode(0o644))
            .unwrap();
        let f = std::fs::File::options().write(true).open(last).unwrap();
        f.set_len(sz - 3).unwrap();
        drop(f);
        let got = read_records(&list_segments(dir));
        let ok: Vec<_> = got.into_iter().filter_map(|r| r.ok()).collect();
        assert_eq!(ok.len(), 3, "torn frame dropped, earlier frames kept");
    }

    #[test]
    fn rotation_threshold_creates_successor_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut w = JournalWriter::resume(dir, None, 200).unwrap();
        for i in 0..40u32 {
            w.append_batch_synced(&[format!("record-{i}-000000000000000000").as_bytes()])
                .unwrap();
        }
        assert!(w.index > 1, "rotated past threshold");
        drop(w);
        assert!(list_segments(dir).len() > 1);
    }

    #[test]
    fn segments_listing_filters_and_sorts() {
        let tmp = tempfile::tempdir().unwrap();
        // No journal dir yet: empty listing, not an error.
        assert!(list_segments(tmp.path()).is_empty());

        let dir = journal_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("seg-000002.op"), b"two").unwrap();
        std::fs::write(dir.join("seg-000001.op"), b"one").unwrap();
        // Foreign names, unparseable indices, and directories are not segments.
        std::fs::write(dir.join("notes.txt"), b"keep out").unwrap();
        std::fs::write(dir.join("seg-notanumber.op"), b"junk").unwrap();
        std::fs::create_dir_all(dir.join("seg-000009.op")).unwrap();

        let idxs: Vec<u64> = list_segments(tmp.path())
            .into_iter()
            .map(|(i, _)| i)
            .collect();
        assert_eq!(idxs, vec![1, 2], "only parseable regular files, ascending");
    }

    #[test]
    fn resume_on_a_fresh_store_starts_after_the_covered_index() {
        let tmp = tempfile::tempdir().unwrap();
        let w = JournalWriter::resume(tmp.path(), Some(5), 1024 * 1024).unwrap();
        assert_eq!(w.index, 6, "covered-through-5 means segment 6 is next");
        assert_eq!(w.written_in_segment, 0);
        assert_eq!(w.records_appended, 0);
        drop(w);
        // The fresh segment exists on disk, ready for appends.
        assert!(journal_dir(tmp.path()).join("seg-000006.op").is_file());
    }

    #[test]
    fn resume_truncates_a_torn_tail_before_appending() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let seg = journal_dir(dir).join("seg-000001.op");
        let intact_len = {
            let mut w = JournalWriter::resume(dir, None, 1024 * 1024).unwrap();
            w.append_batch_synced(&[b"keep-me"]).unwrap();
            std::fs::metadata(&seg).unwrap().len()
        };
        // Crash mid-header: two stray bytes after the last intact frame.
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
        use std::io::Write as _;
        f.write_all(b"\x99\x99").unwrap();
        drop(f);
        assert!(std::fs::metadata(&seg).unwrap().len() > intact_len);

        // Resume repairs before appending: the stray bytes are gone and the
        // writer picks up exactly at the frame boundary.
        let mut w2 = JournalWriter::resume(dir, None, 1024 * 1024).unwrap();
        assert_eq!(w2.written_in_segment, intact_len, "torn tail truncated");
        w2.append_batch_synced(&[b"after-repair"]).unwrap();
        drop(w2);

        let payloads: Vec<Vec<u8>> = read_records(&list_segments(dir))
            .into_iter()
            .filter_map(|r| r.ok())
            .map(|r| r.payload)
            .collect();
        assert_eq!(
            payloads,
            vec![b"keep-me".to_vec(), b"after-repair".to_vec()],
            "frames before the tear stand; new appends land on the boundary"
        );
    }

    #[test]
    fn forged_insane_length_is_rejected_without_a_huge_allocation() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = journal_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        let mut bytes = Vec::new();
        let payload = b"real";
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        bytes.extend_from_slice(payload);
        // A corrupted header claiming a 4 GiB payload.
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        std::fs::write(dir.join("seg-000001.op"), &bytes).unwrap();

        let payloads: Vec<Vec<u8>> = read_records(&list_segments(tmp.path()))
            .into_iter()
            .filter_map(|r| r.ok())
            .map(|r| r.payload)
            .collect();
        assert_eq!(
            payloads,
            vec![b"real".to_vec()],
            "the insane frame ends the segment at the last sane boundary"
        );
    }

    #[test]
    fn scan_intact_prefix_brackets_frames_and_refuses_garbage() {
        let tmp = tempfile::tempdir().unwrap();
        let seg = tmp.path().join("seg-x.op");

        std::fs::write(&seg, b"").unwrap();
        assert_eq!(scan_intact_prefix(&seg), Some(0), "empty file");

        std::fs::write(&seg, b"\x01\x02").unwrap();
        assert_eq!(
            scan_intact_prefix(&seg),
            Some(0),
            "short header ends the scan"
        );

        let mut bytes = Vec::new();
        let payload = b"abcd";
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        bytes.extend_from_slice(payload);
        std::fs::write(&seg, &bytes).unwrap();
        assert_eq!(
            scan_intact_prefix(&seg),
            Some(8 + payload.len() as u64),
            "one intact frame consumes exactly its bytes"
        );

        // A frame whose payload was cut short is not intact.
        std::fs::write(&seg, &bytes[..bytes.len() - 2]).unwrap();
        assert_eq!(scan_intact_prefix(&seg), Some(0));

        // A CRC mismatch is not a resync point either.
        let mut bad = bytes.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xFF;
        std::fs::write(&seg, &bad).unwrap();
        assert_eq!(scan_intact_prefix(&seg), Some(0));
    }

    #[test]
    fn visit_reports_open_failures_and_continues_later_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = journal_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        let write_seg = |name: &str, payloads: &[&[u8]]| {
            let mut bytes = Vec::new();
            for p in payloads {
                bytes.extend_from_slice(&(p.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&crc32c::crc32c(p).to_le_bytes());
                bytes.extend_from_slice(p);
            }
            std::fs::write(dir.join(name), &bytes).unwrap();
        };
        write_seg("seg-000001.op", &[b"a"]);
        write_seg("seg-000003.op", &[b"c"]);
        let paths = vec![
            (1u64, dir.join("seg-000001.op")),
            (2u64, dir.join("seg-000002.op")), // does not exist
            (3u64, dir.join("seg-000003.op")),
        ];

        let mut events = Vec::new();
        visit_records(&paths, |r| {
            events.push(r);
            true
        });
        assert_eq!(events.len(), 3);
        assert!(matches!(
            &events[0],
            Ok(rec) if rec.segment == 1 && rec.ordinal == 0 && rec.payload == b"a"
        ));
        match &events[1] {
            Err((2, msg)) => assert!(msg.contains("open failed"), "{msg}"),
            other => panic!(
                "expected an open failure for segment 2, got {:?}",
                other.as_ref().map(|r| (r.segment, r.payload.clone()))
            ),
        }
        assert!(matches!(
            &events[2],
            Ok(rec) if rec.segment == 3 && rec.payload == b"c"
        ));
    }

    #[test]
    fn visitor_can_stop_the_walk_early() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = journal_dir(tmp.path());
        std::fs::create_dir_all(&dir).unwrap();
        let write_seg = |name: &str, payloads: &[&[u8]]| {
            let mut bytes = Vec::new();
            for p in payloads {
                bytes.extend_from_slice(&(p.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&crc32c::crc32c(p).to_le_bytes());
                bytes.extend_from_slice(p);
            }
            std::fs::write(dir.join(name), &bytes).unwrap();
        };
        write_seg("seg-000001.op", &[b"a", b"b"]);
        write_seg("seg-000002.op", &[b"c"]);
        let paths = list_segments(tmp.path());

        let mut seen = Vec::new();
        visit_records(&paths, |r| {
            let stop = matches!(&r, Ok(rec) if rec.payload == b"a");
            if let Ok(rec) = r {
                seen.push(rec.payload);
            }
            !stop
        });
        assert_eq!(
            seen,
            vec![b"a".to_vec()],
            "later records and segments unread"
        );
    }

    #[test]
    fn a_directory_segment_surfaces_a_read_error() {
        // A directory standing in for a segment: open succeeds on Linux but
        // the first read fails with EISDIR — surfaced as Err, never a panic
        // and never mistaken for a clean end of stream.
        let tmp = tempfile::tempdir().unwrap();
        let dir = journal_dir(tmp.path());
        std::fs::create_dir_all(dir.join("seg-000007.op")).unwrap();
        let paths = vec![(7u64, dir.join("seg-000007.op"))];

        let got = read_records(&paths);
        assert_eq!(got.len(), 1);
        match &got[0] {
            Err((7, msg)) => assert!(msg.contains("read failed"), "{msg}"),
            other => panic!(
                "expected a read failure for the directory segment, got {:?}",
                other.as_ref().err()
            ),
        }
    }
}

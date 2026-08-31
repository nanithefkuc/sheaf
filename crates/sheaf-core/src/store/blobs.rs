//! Blob sink: binary payloads under content-addressed storage.
//! Dedup by hash is the first mitigation for growth; a conservative garbage
//! report sits on top (`super::maintenance`), pruning a payload only when
//! branch reachability proves nothing can ever reference it again.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::fsutil;

/// `<root>/.sheaf/store/blobs`
pub fn blobs_dir(store_dir: &Path) -> PathBuf {
    store_dir.join("blobs")
}

/// SHA-256 of payload, lowercase hex.
pub fn hash_of(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    hex::encode(digest)
}

/// SHA-256 of a file read in bounded chunks — the caller never holds the
/// whole payload in memory (bounded-memory capture requirement).
pub fn hash_file(path: &Path) -> io::Result<String> {
    let mut f = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Storage path for a hex digest (two-char fanout directory).
///
/// A digest too short to have a fanout prefix can only come from a corrupt
/// or hand-edited metadata record. It yields a deliberately unreachable path
/// rather than panicking: callers then report a missing blob and fail closed,
/// instead of taking down the single writer thread.
pub fn blob_path(store_dir: &Path, hex_digest: &str) -> PathBuf {
    let short = hex_digest.get(..2).unwrap_or("__");
    blobs_dir(store_dir).join(short).join(hex_digest)
}

/// Persist payload iff absent; returns `(hex, wrote_new)`.
pub fn store_blob(store_dir: &Path, payload: &[u8]) -> std::io::Result<(String, bool)> {
    let digest = hash_of(payload);
    if publish_if_absent(
        store_dir,
        &digest,
        &mut std::io::Cursor::new(payload),
        Some(payload.len()),
    )? {
        Ok((digest, true))
    } else {
        Ok((digest, false))
    }
}

/// Persist a file's bytes iff absent, streaming: memory stays flat no matter
/// how large the source is. Returns `(hex, wrote_new)`.
pub fn store_blob_from_path(store_dir: &Path, src: &Path) -> std::io::Result<(String, bool)> {
    let digest = hash_file(src)?;
    let mut f = File::open(src)?;
    if publish_if_absent(store_dir, &digest, &mut f, None)? {
        Ok((digest, true))
    } else {
        Ok((digest, false))
    }
}

/// Write `reader` into the blob named `digest` unless it already exists.
/// `true` means a new blob file was published (tmp+fsync+rename+dirsync).
fn publish_if_absent(
    store_dir: &Path,
    digest: &str,
    reader: &mut dyn io::Read,
    known_len: Option<usize>,
) -> std::io::Result<bool> {
    std::fs::create_dir_all(blobs_dir(store_dir))?;
    let dst = blob_path(store_dir, digest);
    if dst.exists() {
        return Ok(false);
    }
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Atomic-by-rename so a concurrent reader never sees partial content;
    // the fanout directory entry is fsync'd too, so a power cut cannot leave
    // a committed journal record pointing at a blob that no longer exists.
    let tmp = dst.with_extension(format!("tmp{}", std::process::id()));
    {
        let mut f = File::create(&tmp)?;
        match known_len {
            Some(n) if n <= 4 * 1024 * 1024 => {
                let mut buf = Vec::with_capacity(n);
                reader.read_to_end(&mut buf)?;
                f.write_all(&buf)?;
            }
            _ => {
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    f.write_all(&buf[..n])?;
                }
            }
        }
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dst)?;
    fsutil::sync_parent_dir(&dst)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_by_content() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let (h1, w1) = store_blob(dir, b"same bytes").unwrap();
        let (h2, w2) = store_blob(dir, b"same bytes").unwrap();
        assert_eq!(h1, h2);
        assert!(w1 && !w2, "second identical write dedupes");
        assert_eq!(store_blob(dir, b"other").unwrap().0.len(), 64);
        let p = blob_path(dir, &h1);
        assert_eq!(p.file_name().unwrap(), h1.as_str());
        // two-char fanout directory derived from the digest itself
        assert_eq!(
            p.parent().unwrap().file_name().unwrap(),
            h1.get(..2).expect("hex prefix")
        );
    }

    #[test]
    fn degenerate_digest_gets_an_unreachable_fanout() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        assert_eq!(
            blob_path(dir, "abcd"),
            dir.join("blobs").join("ab").join("abcd")
        );
        // A 1-char digest cannot yield a fanout prefix; the "__" placeholder
        // keeps the path addressable so callers report a missing blob and
        // fail closed instead of panicking.
        let p = blob_path(dir, "z");
        assert_eq!(p.parent().unwrap().file_name().unwrap(), "__");
        assert!(!p.exists());
    }

    #[test]
    fn hash_file_matches_hash_of_bytes_and_reports_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("payload.bin");
        std::fs::write(&f, b"streamed bytes").unwrap();
        assert_eq!(hash_file(&f).unwrap(), hash_of(b"streamed bytes"));

        // Empty input is the well-known SHA-256 of nothing.
        let e = tmp.path().join("empty.bin");
        std::fs::write(&e, b"").unwrap();
        assert_eq!(
            hash_file(&e).unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        assert!(hash_file(&tmp.path().join("missing.bin")).is_err());
    }

    #[test]
    fn store_blob_from_path_streams_and_dedupes_across_apis() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        let src = tmp.path().join("video.bin");
        std::fs::write(&src, vec![9u8; 300 * 1024]).unwrap();

        let (h1, w1) = store_blob_from_path(&store, &src).unwrap();
        assert!(w1, "first publish writes a new blob");
        assert_eq!(h1, hash_file(&src).unwrap());
        assert!(blob_path(&store, &h1).is_file());

        let (h2, w2) = store_blob_from_path(&store, &src).unwrap();
        assert_eq!(h1, h2);
        assert!(!w2, "second publish dedupes by digest");

        // The in-memory API lands on the identical blob (and stays deduped).
        let (h3, w3) = store_blob(&store, &std::fs::read(&src).unwrap()).unwrap();
        assert_eq!(h3, h1);
        assert!(!w3);
    }
}

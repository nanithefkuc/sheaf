//! Filesystem durability primitives.
//!
//! Surviving a `kill -9` only needs per-file fsync; surviving a *power cut*
//! additionally requires that the directory entry renaming a file into place
//! is itself on stable storage. Every place this crate publishes a file by
//! tmp+rename goes through [`sync_parent_dir`] so the rename cannot evaporate
//! while the payload survives.

use std::io;
use std::path::Path;

/// fsync the directory that contains `path`, so its entry for `path` is
/// durable. Best-effort on filesystems that refuse directory fsync (EINVAL /
/// EACCES / EBADF): the payload file's own fsync still bounds the loss to the
/// most recent unsynced write.
pub fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };
    sync_dir(parent)
}

/// fsync one directory. See [`sync_parent_dir`] for the tolerance policy.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(dir)?;
    let rc = unsafe { libc::fsync(f.as_raw_fd()) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // Filesystems without directory-sync semantics: pretend success —
        // the caller's per-file fsync is the durability boundary there.
        Some(libc::EINVAL) | Some(libc::EACCES) | Some(libc::EBADF) | Some(libc::EROFS) => Ok(()),
        _ => Err(err),
    }
}

/// Write `bytes` to `final_path` atomically: tmp file in the same directory,
/// file fsync, rename, parent-dir fsync. The classic
/// crash-during-write leaves the previous file standing.
pub fn atomic_write(final_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = final_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp{}",
        final_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into()),
        std::process::id(),
    ));
    {
        use std::io::Write as _;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, final_path)?;
    sync_parent_dir(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_and_is_durable_shaped() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("state").join("x.json");
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        atomic_write(&dst, b"one").unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"one");
        atomic_write(&dst, b"two").unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"two");
        // No tmp litter left behind.
        let litter: Vec<_> = std::fs::read_dir(dst.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert!(litter.is_empty(), "temporary files must not accumulate");
    }

    #[test]
    fn sync_parent_tolerates_missing_and_root_paths() {
        // A path whose parent does not exist: open fails, and that is fine to
        // surface — but a bare filename (parent "") must be a clean no-op.
        assert!(sync_parent_dir(Path::new("bare-name")).is_ok());
    }

    #[test]
    fn sync_dir_surfaces_missing_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        let err = sync_dir(&missing).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn sync_dir_tolerates_filesystems_without_dir_sync() {
        // procfs directory fds refuse fsync with EINVAL — exactly the class
        // of failure the tolerance policy exists for: report success rather
        // than failing a durability step that has no meaning there.
        // (Linux-specific; the crate is unix-only.)
        if !Path::new("/proc/self").is_dir() {
            return; // environment without procfs: nothing to prove here
        }
        assert!(sync_dir(Path::new("/proc/self")).is_ok());
    }

    #[test]
    fn atomic_write_creates_missing_parents() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("a/b/c/state.bin");
        atomic_write(&dst, b"deep").unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"deep");
    }

    #[test]
    fn atomic_write_refuses_parentless_paths() {
        // A bare filename has no directory to stage the tmp file in.
        let err = atomic_write(Path::new("bare-name"), b"x").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}

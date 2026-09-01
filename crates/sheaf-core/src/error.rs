//! The crate-wide error type and `Result` alias. Each variant carries enough
//! context to render a human message and maps to a stable machine-readable
//! code for the IPC error table.

use std::path::PathBuf;

/// Every fallible operation in sheaf-core surfaces one of these variants.
#[derive(Debug, thiserror::Error)]
pub enum SheafError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("not a sheaf project (no .sheaf/FORMAT_VERSION found above {0})")]
    NoProjectRoot(PathBuf),

    #[error("store format mismatch at {root}: found {found}, supported {supported}")]
    StoreVersion {
        root: PathBuf,
        found: u32,
        supported: u32,
    },

    #[error("store corrupt or inconsistent: {0}")]
    StoreCorrupt(String),

    #[error("store busy ({0} held elsewhere)")]
    LockBusy(PathBuf),

    #[error("registry error: {0}")]
    Registry(String),

    #[error("ipc error: {0}")]
    Ipc(String),

    #[error("watcher setup failed for {root}: {message}")]
    WatchInit { root: PathBuf, message: String },

    #[error("checkpoint `{0}` already exists")]
    CheckpointExists(String),

    #[error("invalid timeline reference: {0}")]
    TimelineReference(String),

    #[error("grep cursor is not valid for this query: {0}")]
    BadCursor(String),

    #[error("restore plan is no longer valid: {0}")]
    RestorePlanStale(String),

    #[error("restore blocked by the live worktree: {0}")]
    RestoreObstructed(String),

    #[error("{0}")]
    Other(String),
}

/// Crate-wide result alias defaulting the error to [`SheafError`].
pub type Result<T> = std::result::Result<T, SheafError>;

impl SheafError {
    /// Stable machine-readable code for this error, as carried in IPC
    /// responses so clients can branch on the failure without parsing text.
    pub fn code(&self) -> &'static str {
        match self {
            SheafError::Io(_) => "internal",
            SheafError::Config(_) => "bad.params",
            SheafError::NoProjectRoot(_) => "project.no_root",
            SheafError::StoreVersion { .. } => "store.version_mismatch",
            SheafError::StoreCorrupt(_) => "store.corrupt",
            SheafError::LockBusy(_) => "conflict.store_busy",
            SheafError::Registry(_) => "internal",
            SheafError::Ipc(_) => "internal",
            SheafError::WatchInit { .. } => "unsupported",
            SheafError::CheckpointExists(_) => "exists",
            SheafError::TimelineReference(_) => "state.bad_reference",
            SheafError::BadCursor(_) => "state.bad_cursor",
            SheafError::RestorePlanStale(_) => "restore.plan_stale",
            SheafError::RestoreObstructed(_) => "restore.obstructed",

            SheafError::Other(_) => "internal",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_codes_are_stable_across_every_variant() {
        // These codes cross the IPC boundary and clients branch on them;
        // pinning all fifteen arms is the contract test.
        let cases: Vec<(SheafError, &str)> = vec![
            (SheafError::Io(std::io::Error::other("boom")), "internal"),
            (SheafError::Config("bad".into()), "bad.params"),
            (
                SheafError::NoProjectRoot(PathBuf::from("/x")),
                "project.no_root",
            ),
            (
                SheafError::StoreVersion {
                    root: PathBuf::from("/x"),
                    found: 9,
                    supported: 2,
                },
                "store.version_mismatch",
            ),
            (SheafError::StoreCorrupt("bad".into()), "store.corrupt"),
            (
                SheafError::LockBusy(PathBuf::from("/x/lock")),
                "conflict.store_busy",
            ),
            (SheafError::Registry("bad".into()), "internal"),
            (SheafError::Ipc("bad".into()), "internal"),
            (
                SheafError::WatchInit {
                    root: PathBuf::from("/x"),
                    message: "no".into(),
                },
                "unsupported",
            ),
            (SheafError::CheckpointExists("name".into()), "exists"),
            (
                SheafError::TimelineReference("nope".into()),
                "state.bad_reference",
            ),
            (SheafError::BadCursor("nope".into()), "state.bad_cursor"),
            (
                SheafError::RestorePlanStale("old".into()),
                "restore.plan_stale",
            ),
            (
                SheafError::RestoreObstructed("busy".into()),
                "restore.obstructed",
            ),
            (SheafError::Other("misc".into()), "internal"),
        ];
        for (err, code) in cases {
            assert_eq!(err.code(), code, "code for {err:?}");
        }
    }

    #[test]
    fn io_errors_convert_via_from() {
        let e: SheafError = std::io::Error::other("disk gone").into();
        assert!(matches!(e, SheafError::Io(_)));
        assert_eq!(e.code(), "internal");
        assert_eq!(e.to_string(), "io error: disk gone");
    }

    #[test]
    fn display_messages_carry_context() {
        let e = SheafError::StoreVersion {
            root: PathBuf::from("/repo"),
            found: 7,
            supported: 2,
        };
        assert_eq!(
            e.to_string(),
            "store format mismatch at /repo: found 7, supported 2"
        );
        let e = SheafError::NoProjectRoot(PathBuf::from("/repo"));
        assert_eq!(
            e.to_string(),
            "not a sheaf project (no .sheaf/FORMAT_VERSION found above /repo)"
        );
        assert_eq!(
            SheafError::LockBusy(PathBuf::from("/r/lock")).to_string(),
            "store busy (/r/lock held elsewhere)"
        );
    }
}

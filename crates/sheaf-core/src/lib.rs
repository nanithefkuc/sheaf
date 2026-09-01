//! sheaf-core: shared logic for the sheaf daemon and CLI.
//!
//! Deliberately sync-only (std threads): every API here is callable from
//! plain threads; async wiring lives in the binary crates.
pub mod classify;
pub mod config;
pub mod debounce;
pub mod error;
pub mod events;
pub mod ignore;
pub mod init;
pub mod ipc;
pub mod paths;
pub mod registry;
pub mod scratch;
pub mod store;
pub mod watcher;

pub use config::{ProjectConfig, STORE_FORMAT_VERSION};
pub use error::SheafError;
pub use events::{Batch, EventKind, FsEvent};
pub use registry::{Enrollment, Registry};

/// Human-facing daemon/CLI name used in IPC handshakes.
pub const PRODUCT: &str = "sheaf";

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Serialize tests that mutate process-global environment variables.
    /// Lib tests run on parallel threads, and `std::env` mutation is racy
    /// process-wide; every env-touching test must hold this guard.
    pub(crate) fn env_guard() -> MutexGuard<'static, ()> {
        env_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

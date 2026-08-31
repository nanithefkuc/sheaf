//! XDG-style location helpers. Every function takes explicit fallbacks and
//! returns deterministic paths; binaries own any env-override plumbing.

use std::path::PathBuf;

use crate::error::{Result, SheafError};

/// Runtime control socket: `$XDG_RUNTIME_DIR/sheaf/control.sock`,
/// falling back to `/tmp/sheaf-<uid>/control.sock` when XDG_RUNTIME_DIR is
/// unset (e.g. non-systemd environments).
///
/// `SHEAF_SOCKET` overrides the location outright — the client-side twin of
/// `sheafd run --socket`, so an end-to-end run can address its own daemon
/// instead of whatever the developer already has running.
pub fn control_socket_path() -> PathBuf {
    if let Some(explicit) = std::env::var_os("SHEAF_SOCKET") {
        if !explicit.is_empty() {
            return PathBuf::from(explicit);
        }
    }
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(fallback_runtime_base);
    socket_under(base)
}

/// Pure computation over an explicit runtime base — testable without
/// touching process-global env state.
pub fn socket_under(runtime_base: PathBuf) -> PathBuf {
    runtime_base.join("sheaf").join("control.sock")
}

/// Directory containing `control_socket_path()`'s parent (`.../sheaf`).
pub fn runtime_sheaf_dir() -> PathBuf {
    let mut p = control_socket_path();
    p.pop(); // drop control.sock
    p
}

/// Data home per XDG spec, else `$HOME/.local/share`. Used for the
/// enrollment registry: `<data_home>/sheaf/enrollments.jsonl`.
pub fn data_sheaf_dir() -> Result<PathBuf> {
    if let Some(x) = std::env::var_os("XDG_DATA_HOME") {
        if !x.is_empty() {
            return Ok(PathBuf::from(x).join("sheaf"));
        }
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| SheafError::Other("HOME not set and XDG_DATA_HOME unset".into()))?;
    Ok(home.join(".local/share").join("sheaf"))
}

fn fallback_runtime_base() -> PathBuf {
    PathBuf::from(format!("/tmp/sheaf-{}", unsafe_getuid()))
}

fn unsafe_getuid() -> u32 {
    // Tinylibc-free UID probe via /proc; falls back to 1000 if unreadable.
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("Uid:").map(|rest| rest.trim().to_string()))
        })
        .and_then(|uids| uids.split_whitespace().next().map(str::to_owned))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_shape() {
        // Explicit-base variant: no global env mutation (parallel-test safe).
        assert_eq!(
            socket_under(PathBuf::from("/tmp/rt-test")),
            PathBuf::from("/tmp/rt-test/sheaf/control.sock")
        );
        // Fallback branch lands under /tmp/sheaf-<uid>/.
        let fb = fallback_runtime_base();
        // NB: Path::starts_with is component-wise; we want a text prefix.
        assert!(
            fb.to_string_lossy().starts_with("/tmp/sheaf-"),
            "{}",
            fb.display()
        );
        assert!(socket_under(fb).ends_with("control.sock"));
    }

    #[test]
    fn control_socket_env_precedence() {
        let _g = crate::test_util::env_guard();
        let sock = std::env::var_os("SHEAF_SOCKET").map(PathBuf::from);
        let xdg_rt = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from);

        // A non-empty SHEAF_SOCKET overrides everything outright.
        std::env::set_var("SHEAF_SOCKET", "/custom/agent.sock");
        std::env::set_var("XDG_RUNTIME_DIR", "/run/user/4242");
        assert_eq!(control_socket_path(), PathBuf::from("/custom/agent.sock"));
        assert_eq!(
            runtime_sheaf_dir(),
            PathBuf::from("/custom"),
            "runtime dir is the socket's parent"
        );

        // An EMPTY override is ignored; XDG_RUNTIME_DIR then decides.
        std::env::set_var("SHEAF_SOCKET", "");
        assert_eq!(
            control_socket_path(),
            PathBuf::from("/run/user/4242/sheaf/control.sock")
        );
        assert_eq!(runtime_sheaf_dir(), PathBuf::from("/run/user/4242/sheaf"));

        // Both unset: the /tmp/sheaf-<uid> fallback applies.
        std::env::remove_var("SHEAF_SOCKET");
        std::env::remove_var("XDG_RUNTIME_DIR");
        let p = control_socket_path();
        assert!(
            p.to_string_lossy().starts_with("/tmp/sheaf-"),
            "{}",
            p.display()
        );

        // Restore whatever the ambient environment had.
        match sock {
            Some(v) => std::env::set_var("SHEAF_SOCKET", v),
            None => std::env::remove_var("SHEAF_SOCKET"),
        }
        match xdg_rt {
            Some(v) => std::env::set_var("XDG_RUNTIME_DIR", v),
            None => std::env::remove_var("XDG_RUNTIME_DIR"),
        }
    }

    #[test]
    fn data_dir_prefers_xdg_then_home_then_errors() {
        let _g = crate::test_util::env_guard();
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let xdg_data = std::env::var_os("XDG_DATA_HOME").map(PathBuf::from);

        std::env::set_var("XDG_DATA_HOME", "/xdg-data");
        assert_eq!(data_sheaf_dir().unwrap(), PathBuf::from("/xdg-data/sheaf"));

        // Empty XDG_DATA_HOME counts as unset; HOME fills in.
        std::env::set_var("XDG_DATA_HOME", "");
        std::env::set_var("HOME", "/home/tester");
        assert_eq!(
            data_sheaf_dir().unwrap(),
            PathBuf::from("/home/tester/.local/share/sheaf")
        );

        // Neither available: a clean error, not a panic.
        std::env::remove_var("XDG_DATA_HOME");
        std::env::remove_var("HOME");
        let err = data_sheaf_dir().unwrap_err();
        assert_eq!(err.code(), "internal");
        assert!(err.to_string().contains("HOME not set"));

        match home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match xdg_data {
            Some(v) => std::env::set_var("XDG_DATA_HOME", v),
            None => std::env::remove_var("XDG_DATA_HOME"),
        }
    }
}

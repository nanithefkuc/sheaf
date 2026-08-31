//! `sheaf init` implementation. Deliberately independent of the daemon:
//! everything here works by touching files, so first contact never requires
//! a running daemon. Only the final notify step is a best-effort IPC call.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::config;
use crate::error::{Result, SheafError};
use crate::registry::Registry;

const GIT_EXCLUDE_LINE: &str = ".sheaf/";

/// Outcome of an `init_project` run: which root was used and which of the
/// enrollment side effects (store creation, git-exclude, registry, daemon
/// notify) actually happened, plus human-readable notes.
#[derive(Debug, Clone, Serialize)]
pub struct InitReport {
    pub root: PathBuf,
    /// True when `.sheaf/` was created now; false when found pre-existing.
    pub store_created: bool,
    /// An ancestor already had the store; we adopted its root instead.
    pub reused_ancestor: bool,
    /// `.git/info/exclude` gained our entry (only when a git repo is present).
    pub git_exclude_updated: bool,
    /// Registry newly lists this root.
    pub newly_enrolled: bool,
    /// Live daemon acknowledged enroll.notify.
    pub daemon_notified: bool,
    pub notes: Vec<String>,
}

/// Walk upward from `start` seeking the store marker, `.sheaf/config.toml`
/// (which also carries `format_version`).
pub fn resolve_project_root(start: &Path) -> Option<PathBuf> {
    let mut cur = Some(start);
    while let Some(dir) = cur {
        if config::config_file_path(dir).is_file() {
            return Some(dir.to_path_buf());
        }
        cur = dir.parent();
    }
    None
}

/// Resolve/override helpers kept injectable for tests and for wrappers.
#[derive(Default)]
pub struct InitOptions<'a> {
    pub registry_override: Option<&'a Registry>,
    pub socket_override: Option<PathBuf>,
}

/// Enroll `target` as a sheaf project: lay down (or adopt an ancestor's)
/// store, exclude `.sheaf/` from git, add it to the registry, and best-effort
/// notify a live daemon. Idempotent; reports exactly what changed.
pub fn init_project(target: &Path, opts: InitOptions) -> Result<InitReport> {
    if !target.is_dir() {
        return Err(SheafError::Other(format!(
            "{} is not a directory",
            target.display()
        )));
    }
    let target = target.canonicalize()?;

    let mut notes = Vec::new();

    let (root, store_created, reused_ancestor) =
        if let Some(existing) = resolve_project_root(&target) {
            let is_ancestor = existing != target;
            config::read_store_format(&existing)?;
            notes.push(format!("adopting existing store at {}", existing.display()));
            (existing, false, is_ancestor)
        } else {
            config::write_skeleton(&target)?;
            (target.clone(), true, false)
        };

    // Stores laid down by older builds carry the flat FORMAT_VERSION marker;
    // config.toml is the marker now. Retire the file whenever we adopt.
    if config::migrate_legacy_format_file(&root) {
        notes.push("retired legacy FORMAT_VERSION marker".into());
    }

    let git_exclude_updated = update_git_exclude(&root)?;
    if git_exclude_updated {
        // Editing .git/info/exclude rather than tracked .gitignore keeps the
        // worktree pristine: git's tracked files stay untouched, so enrolling
        // never shows up as a pending change to commit.
        notes.push("ignored via .git/info/exclude".into());
    }

    let registry = match opts.registry_override {
        Some(r) => r.clone_shim(),
        None => Registry::global()?,
    };
    let newly_enrolled = registry.upsert(&root)?;

    let socket = opts
        .socket_override
        .unwrap_or_else(crate::paths::control_socket_path);
    let daemon_notified = match notify_daemon(&socket, &root) {
        Ok(true) => true,
        Ok(false) | Err(_) => {
            notes.push(format!(
                "daemon not reachable at {}; start it with `sheafd run`",
                socket.display()
            ));
            false
        }
    };

    Ok(InitReport {
        root,
        store_created,
        reused_ancestor,
        git_exclude_updated,
        newly_enrolled,
        daemon_notified,
        notes,
    })
}

fn update_git_exclude(root: &Path) -> Result<bool> {
    let git_dir = root.join(".git");
    if !git_dir.exists() {
        return Ok(false);
    }
    let exclude_path = git_dir.join("info/exclude");
    let content = std::fs::read_to_string(&exclude_path).unwrap_or_default();
    if content.lines().any(|l| l.trim() == GIT_EXCLUDE_LINE) {
        return Ok(false);
    }
    if let Some(parent) = exclude_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut updated = content;
    if !updated.is_empty() && !updated.ends_with('\n') {
        updated.push('\n');
    }
    updated.push_str(GIT_EXCLUDE_LINE);
    updated.push('\n');
    std::fs::write(&exclude_path, updated)?;
    Ok(true)
}

/// Best-effort enrollment ping to a live daemon. Ok(true)=acknowledged.
fn notify_daemon(socket: &Path, root: &Path) -> Result<bool> {
    let timeout = std::time::Duration::from_millis(800);
    let mut client = crate::ipc::Client::connect(socket, timeout)?;
    let reply = client.call("enroll.notify", Some(root), serde_json::json!({}), None)?;
    Ok(reply.response.ok)
}

// Small shim so override registries can be passed without lifetimes leaking
// into InitReport paths; Registry is cheap to reconstruct by path.
impl Registry {
    fn clone_shim(&self) -> Registry {
        Registry::at(self.file_path().to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Overrides that keep init hermetic: a scratch registry (caller-owned
    /// so the borrow outlives the options) and a socket that cannot exist,
    /// so the daemon-notify path is exercised as its unreachable branch
    /// without touching process env.
    fn opts<'a>(tmp: &Path, reg: &'a Registry) -> InitOptions<'a> {
        InitOptions {
            registry_override: Some(reg),
            socket_override: Some(tmp.join("no-daemon-here.sock")),
        }
    }

    #[test]
    fn resolve_root_walks_upward_to_the_store_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("proj");
        let deep = root.join("a/b/c");
        std::fs::create_dir_all(&deep).unwrap();
        assert_eq!(resolve_project_root(&deep), None, "no store anywhere above");
        config::write_skeleton(&root).unwrap();
        assert_eq!(
            resolve_project_root(&deep).as_deref(),
            Some(root.as_path()),
            "nested dirs adopt the ancestor's root"
        );
    }

    #[test]
    fn non_directory_target_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let file = tmp.path().join("plain.txt");
        std::fs::write(&file, "x").unwrap();
        let err = init_project(&file, opts(tmp.path(), &reg)).unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
    }

    #[test]
    fn fresh_init_is_idempotent_and_reports_every_side_effect() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let proj = tmp.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();

        let first = init_project(&proj, opts(tmp.path(), &reg)).unwrap();
        assert!(first.store_created);
        assert!(!first.reused_ancestor);
        assert!(
            !first.git_exclude_updated,
            "no git repo in a bare directory"
        );
        assert!(first.newly_enrolled);
        assert!(!first.daemon_notified, "the override socket does not exist");
        assert!(
            first
                .notes
                .iter()
                .any(|n| n.contains("daemon not reachable")),
            "unreachable daemon is a note, not an error: {:?}",
            first.notes
        );
        assert!(config::config_file_path(&first.root).is_file());

        // Second run: store found (not created), root already enrolled.
        let second = init_project(&proj, opts(tmp.path(), &reg)).unwrap();
        assert!(!second.store_created);
        assert!(!second.newly_enrolled, "upsert dedupes");
    }

    #[test]
    fn init_adopts_an_ancestor_store() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let root = tmp.path().join("proj");
        let nested = root.join("deep/nested");
        std::fs::create_dir_all(&nested).unwrap();
        init_project(&root, opts(tmp.path(), &reg)).unwrap();

        let report = init_project(&nested, opts(tmp.path(), &reg)).unwrap();
        assert!(report.reused_ancestor);
        assert!(!report.store_created);
        assert_eq!(report.root, root.canonicalize().unwrap());
    }

    #[test]
    fn adopted_legacy_marker_is_retired_with_a_note() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let root = tmp.path().join("proj");
        std::fs::create_dir_all(&root).unwrap();
        config::write_skeleton(&root).unwrap();
        // A store laid down by an older build.
        std::fs::write(config::format_file_path(&root), "1\n").unwrap();

        let report = init_project(&root, opts(tmp.path(), &reg)).unwrap();
        assert!(!report.store_created);
        assert!(
            report
                .notes
                .iter()
                .any(|n| n.contains("legacy FORMAT_VERSION")),
            "{:?}",
            report.notes
        );
        assert!(!config::format_file_path(&root).exists());
    }

    #[test]
    fn git_exclude_is_appended_once_with_newline_fixup() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let root = tmp.path().join("proj");
        std::fs::create_dir_all(root.join(".git/info")).unwrap();
        // Existing content without a trailing newline must not glue together.
        std::fs::write(root.join(".git/info/exclude"), "secrets.env").unwrap();

        let first = init_project(&root, opts(tmp.path(), &reg)).unwrap();
        assert!(first.git_exclude_updated);
        assert_eq!(
            std::fs::read_to_string(root.join(".git/info/exclude")).unwrap(),
            "secrets.env\n.sheaf/\n"
        );

        // A second init must not duplicate the entry.
        let second = init_project(&root, opts(tmp.path(), &reg)).unwrap();
        assert!(!second.git_exclude_updated);
        assert_eq!(
            std::fs::read_to_string(root.join(".git/info/exclude")).unwrap(),
            "secrets.env\n.sheaf/\n"
        );
    }

    #[test]
    fn git_exclude_dir_is_created_when_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::at(tmp.path().join("enrollments.jsonl"));
        let root = tmp.path().join("proj");
        std::fs::create_dir_all(root.join(".git")).unwrap(); // bare repo dir, no info/

        let report = init_project(&root, opts(tmp.path(), &reg)).unwrap();
        assert!(report.git_exclude_updated);
        assert!(root.join(".git/info/exclude").is_file());
        assert!(std::fs::read_to_string(root.join(".git/info/exclude"))
            .unwrap()
            .contains(".sheaf/"));
    }
}

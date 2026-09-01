//! `config.toml` — the per-project settings file created by `sheaf init`,
//! and the on-disk store's root marker. Defaults here are the contract;
//! users may edit.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SheafError};

/// Bumped only through the store's migration process. Format 2 adds journal
/// ledger records (tagged frames) and the retention surface
/// (expiry + marks). Readers of this build accept everything from
/// [`MIN_STORE_FORMAT`] up; older builds refuse format 2 outright
/// (fail-closed), which is the point of the bump: a v1 binary replaying a
/// v2 journal would choke on the first ledger frame.
pub const STORE_FORMAT_VERSION: u32 = 2;
/// Oldest store format this build reads. Format 1 stores carry only
/// untagged Loro-update frames; frame classification is per-payload
/// (loro magic bytes), so they load unchanged.
pub const MIN_STORE_FORMAT: u32 = 1;

/// Legacy flat marker file. The root marker is `config.toml` itself (it
/// carries `format_version`); this file is retired by
/// [`migrate_legacy_format_file`] and no longer written.
pub const FORMAT_FILE: &str = "FORMAT_VERSION";
/// Settings file and store root marker, living directly under `.sheaf/`.
pub const CONFIG_FILE: &str = "config.toml";
/// Name of the per-project store directory at the project root.
pub const SHEAF_DIR_NAME: &str = ".sheaf";
/// Format version of the lightweight `.sheaf` link file in managed worktrees.
pub const WORKTREE_LINK_VERSION: u32 = 1;


/// Debounce and buffering knobs that govern how filesystem activity is
/// coalesced into captured batches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchConfig {
    /// Quiescence window (ms): a batch flushes after this much silence.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u32,
    /// Hard cap on how long a continuously-active burst may be held (ms)
    /// before a partial flush happens anyway.
    #[serde(default = "default_max_hold_ms")]
    pub max_hold_ms: u32,
    /// Hard cap on buffered events per project before a partial flush.
    #[serde(default = "default_max_events")]
    pub max_events: usize,
    /// Aggregate UTF-8 payload allowed in the in-memory CRDT document.
    /// Further text files use byte-exact content-addressed blobs instead.
    #[serde(default = "default_max_tracked_bytes")]
    pub max_tracked_bytes: u64,
}

fn default_debounce_ms() -> u32 {
    300
}
fn default_max_hold_ms() -> u32 {
    2000
}
fn default_max_events() -> usize {
    4096
}

/// Conservative default because Loro's char-level operation graph costs a
/// multiple of the source bytes. Exact recovery continues above this bound;
/// only character-level history yields to whole-file blob history.
pub const DEFAULT_MAX_TRACKED_BYTES: u64 = 32 * 1024 * 1024;

fn default_max_tracked_bytes() -> u64 {
    DEFAULT_MAX_TRACKED_BYTES
}

impl Default for WatchConfig {
    fn default() -> Self {
        WatchConfig {
            debounce_ms: default_debounce_ms(),
            max_hold_ms: default_max_hold_ms(),
            max_events: default_max_events(),
            max_tracked_bytes: default_max_tracked_bytes(),
        }
    }
}

/// The project's configured ignore patterns, unioned at runtime with the
/// project's own `.gitignore` files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgnoreConfig {
    /// gitignore-subset patterns:
    /// trailing `/` → directory name at any depth; wildcard patterns are
    /// globbed against root-relative paths; bare names match any component.
    #[serde(default = "default_patterns")]
    pub patterns: Vec<String>,
}

/// The built-in ignore patterns every new project starts with: the store
/// and VCS dirs, the common heavy build trees, and editor swap/backup/temp
/// litter (suffix globs matched against every path segment).
pub fn default_patterns() -> Vec<String> {
    vec![
        ".sheaf/".into(),
        ".sheaf".into(), // managed worktrees carry a link file, not a store directory

        ".git/".into(),
        "node_modules/".into(),
        "target/".into(),
        // Editor swap/backup/atomic-save litter. Editors that save by writing a
        // temp file and renaming it over the target (Neovim, Vim with
        // writebackup, and others) otherwise flood the timeline with transient
        // captures of files that never outlive one save. These globs match at
        // any depth (a bare `*.ext` glob is tested against every path segment).
        "*.swp".into(), // Vim/Neovim primary swap file
        "*.swo".into(), // secondary swap files (…swo, swn, swm, …)
        "*.swn".into(),
        "*~".into(),    // Vim/Emacs/editor backup copies
        "*.bak".into(), // generic editor backups
        "*.tmp".into(), // generic atomic-write temp files (incl. `.NAME.md.tmp`)
        "*.orig".into(), // merge-tool leftovers
                        // Only suffix-globs are used here because a bare `*` crosses `/` in this
                        // grammar (so `*.swp` ignores the litter at any depth), whereas a
                        // prefix-anchored glob like `.#*` or `#*#` would only match at the repo
                        // root and cannot round-trip anyway (a leading `#` is the comment
                        // marker). Emacs users who want `#name#` / `.#name` ignored can add a
                        // scoped `.gitignore` line; the common cross-editor litter is covered
                        // by the suffix patterns above.
    ]
}

impl Default for IgnoreConfig {
    fn default() -> Self {
        IgnoreConfig {
            patterns: default_patterns(),
        }
    }
}

/// Restore-intent lifecycle knobs. An intent older than the
/// staleness bound is surfaced loudly instead of auto-replayed: a tree the
/// user has been working in for a week must never silently rewind after a
/// reboot, even though everything on it is captured.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreConfig {
    /// Auto-resume a pending interrupted restore only when its intent is
    /// younger than this (ms). Older intents wait for an explicit
    /// `sheaf restore --resume` (or `--abandon`). Seven days by default.
    #[serde(default = "default_max_resume_age_ms")]
    pub max_resume_age_ms: i64,
}

fn default_max_resume_age_ms() -> i64 {
    7 * 24 * 60 * 60 * 1000
}

impl Default for RestoreConfig {
    fn default() -> Self {
        RestoreConfig {
            max_resume_age_ms: default_max_resume_age_ms(),
        }
    }
}

/// Retention policy. Automatic expiry is
/// reachability-bound by the planner, never by this knob alone: the value
/// here only decides which captures are OLD ENOUGH to be considered.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Compact duration ("30d", "72h", "45m", "90s"). Absent = infinite
    /// retention, the flight-recorder default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
}

impl RetentionConfig {
    /// Expiry horizon in milliseconds, or `None` when retention is infinite.
    pub fn expiry_ms(&self) -> Option<i64> {
        self.expiry.as_deref().and_then(parse_duration_ms)
    }
}

/// `<count><unit>` with unit in s/m/h/d, case-sensitive. Returns None for
/// anything else (including a bare integer, which is ambiguous).
pub fn parse_duration_ms(spec: &str) -> Option<i64> {
    let spec = spec.trim();
    let split = spec.find(|c: char| !c.is_ascii_digit())?;
    let (num, unit) = spec.split_at(split);
    let count: i64 = num.parse().ok()?;
    let ms = match unit {
        "s" => count.saturating_mul(1_000),
        "m" => count.saturating_mul(60_000),
        "h" => count.saturating_mul(3_600_000),
        "d" => count.saturating_mul(86_400_000),
        _ => return None,
    };
    (ms > 0).then_some(ms)
}

/// Persist a new retention expiry into `config.toml`, preserving every
/// other setting. Writer-owned path (daemon/CLI under flock semantics).
pub fn set_retention_expiry(root: &Path, spec: &str) -> Result<()> {
    if parse_duration_ms(spec).is_none() {
        return Err(SheafError::Config(format!(
            "invalid expiry `{spec}` (expected e.g. 30d, 72h, 45m, 90s)"
        )));
    }
    let mut cfg = load(root)?;
    cfg.retention.expiry = Some(spec.to_string());
    let rendered = toml::to_string_pretty(&cfg)
        .map_err(|e| SheafError::Config(format!("config re-render: {e}")))?;
    std::fs::write(config_file_path(root), rendered)?;
    Ok(())
}

/// The full parsed contents of a project's `config.toml`: format version
/// plus every settings section, each defaulting when absent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Absent (hand-trimmed) configs are treated as the current format:
    /// the fail-closed refusal is for unknown-NEWER stores, not sparse ones.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
    #[serde(default)]
    pub watch: WatchConfig,
    #[serde(default)]
    pub ignore: IgnoreConfig,
    #[serde(default)]
    pub restore: RestoreConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Store maintenance cadence (segment rotation, snapshot cadence).
    /// Absent section or keys keep the struct's defaults; the values bind
    /// when a writer OPENS the store, so a running daemon applies edits
    /// here on its next restart (or a lazy store's next cold open).
    #[serde(default)]
    pub store: crate::store::StoreLimits,
}

fn default_format_version() -> u32 {
    STORE_FORMAT_VERSION
}
/// A managed worktree's `.sheaf` file points at the primary worktree whose
/// store and daemon writer it shares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeLink {
    pub version: u32,
    pub store_root: PathBuf,
    pub id: String,
}

/// Parse a managed-worktree link. A directory-valued `.sheaf` is the primary
/// store and therefore not a link.
pub fn worktree_link(root: &Path) -> Option<WorktreeLink> {
    let marker = root.join(SHEAF_DIR_NAME);
    if !marker.is_file() {
        return None;
    }
    let link: WorktreeLink = serde_json::from_slice(&std::fs::read(marker).ok()?).ok()?;
    (link.version == WORKTREE_LINK_VERSION
        && link.store_root.is_absolute()
        && !link.id.is_empty()
        && !link.id.chars().any(char::is_whitespace))
    .then_some(link)
}

/// Primary project root which owns the shared store.
pub fn store_root(root: &Path) -> PathBuf {
    worktree_link(root)
        .map(|link| link.store_root)
        .unwrap_or_else(|| root.to_path_buf())
}

/// Stable worktree identity. The primary worktree has no explicit ID.
pub fn worktree_id(root: &Path) -> Option<String> {
    worktree_link(root).map(|link| link.id)
}

/// Advisory head file belonging to this physical worktree.
pub fn worktree_head_path(root: &Path) -> PathBuf {
    let state = sheaf_dir(root).join("state");
    match worktree_id(root) {
        Some(id) => state.join("worktrees").join(format!("{id}.head")),
        None => state.join("worktree.head"),
    }
}

/// Write the managed-worktree marker after its directory has been
/// materialized. The caller owns path collision and atomic-directory rules.
pub fn write_worktree_link(root: &Path, link: &WorktreeLink) -> Result<()> {
    if link.version != WORKTREE_LINK_VERSION
        || !link.store_root.is_absolute()
        || link.id.is_empty()
        || link.id.chars().any(char::is_whitespace)
    {
        return Err(SheafError::Config("invalid worktree link".into()));
    }
    let bytes = serde_json::to_vec_pretty(link)
        .map_err(|e| SheafError::Config(format!("worktree link serialize: {e}")))?;
    std::fs::write(root.join(SHEAF_DIR_NAME), bytes)?;
    Ok(())
}


/// `<root>/.sheaf` for a primary worktree, or the shared store directory
/// named by a managed worktree's `.sheaf` link file.
pub fn sheaf_dir(root: &Path) -> PathBuf {
    store_root(root).join(SHEAF_DIR_NAME)
}


/// Path to the retired legacy `FORMAT_VERSION` marker, `<root>/.sheaf/FORMAT_VERSION`.
pub fn format_file_path(root: &Path) -> PathBuf {
    sheaf_dir(root).join(FORMAT_FILE)
}

/// Path to the settings file and store root marker, `<root>/.sheaf/config.toml`.
pub fn config_file_path(root: &Path) -> PathBuf {
    sheaf_dir(root).join(CONFIG_FILE)
}

/// Read the store format from `config.toml` — which is also the project-root
/// marker — refusing unknown-newer stores (fail-closed).
pub fn read_store_format(root: &Path) -> Result<u32> {
    let path = config_file_path(root);
    let raw = std::fs::read_to_string(&path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => SheafError::NoProjectRoot(root.to_owned()),
        _ => SheafError::Io(e),
    })?;
    let value: toml::Value = toml::from_str(&raw)
        .map_err(|e| SheafError::Config(format!("{} does not parse: {e}", path.display())))?;
    let v = value
        .get("format_version")
        .and_then(toml::Value::as_integer)
        .unwrap_or(STORE_FORMAT_VERSION as i64) as u32;
    if (MIN_STORE_FORMAT..=STORE_FORMAT_VERSION).contains(&v) {
        Ok(v)
    } else {
        Err(SheafError::StoreVersion {
            root: root.to_owned(),
            found: v,
            supported: STORE_FORMAT_VERSION,
        })
    }
}

/// Parse a project's `config.toml`, refusing stores outside the supported
/// format range (fail-closed).
pub fn load(root: &Path) -> Result<ProjectConfig> {
    let cfg: ProjectConfig = toml::from_str(
        &std::fs::read_to_string(config_file_path(root)).map_err(|e| {
            SheafError::Config(format!(
                "{} unreadable: {e}",
                config_file_path(root).display()
            ))
        })?,
    )
    .map_err(|e| SheafError::Config(format!("toml: {e}")))?;
    if !(MIN_STORE_FORMAT..=STORE_FORMAT_VERSION).contains(&cfg.format_version) {
        return Err(SheafError::StoreVersion {
            root: root.to_owned(),
            found: cfg.format_version,
            supported: STORE_FORMAT_VERSION,
        });
    }
    Ok(cfg)
}

/// Render the default `config.toml` contents for a new project, with every
/// settings section present so its knobs are discoverable.
pub fn render_default() -> String {
    let cfg = ProjectConfig {
        format_version: STORE_FORMAT_VERSION,
        ..Default::default()
    };
    toml::to_string_pretty(&cfg).expect("default config serializes")
}

/// Create `<root>/.sheaf/` and write the default `config.toml`, which alone
/// identifies the directory as a sheaf project.
pub fn write_skeleton(root: &Path) -> Result<()> {
    let dir = sheaf_dir(root);
    std::fs::create_dir_all(&dir)?;
    // `config.toml` alone is the store: it is the root marker (its presence
    // identifies a project) and it carries `format_version`.
    std::fs::write(config_file_path(root), render_default())?;
    Ok(())
}

/// Retire the legacy flat `FORMAT_VERSION` marker from a store laid down by
/// an older build. Best-effort by design: a read-only or busy store just
/// keeps the file until the next writer passes through. Returns true when
/// the file was removed now.
pub fn migrate_legacy_format_file(root: &Path) -> bool {
    let legacy = format_file_path(root);
    if !legacy.is_file() || read_store_format(root).is_err() {
        return false;
    }
    std::fs::remove_file(&legacy).is_ok()
}

/// Upgrade a readable older store to the current format (format 2
/// introduces ledger frames). Writer-owned: called from `sheaf init` and
/// daemon watch start, never from readers. Rewrites `config.toml`
/// atomically, preserving every other field. Old frames stay valid — frame
/// classification is per-payload — so this is a pure capability bump.
pub fn upgrade_store_format(root: &Path) -> Result<bool> {
    let current = read_store_format(root)?;
    if current >= STORE_FORMAT_VERSION {
        return Ok(false);
    }
    let mut cfg = load(root)?;
    cfg.format_version = STORE_FORMAT_VERSION;
    let rendered = toml::to_string_pretty(&cfg)
        .map_err(|e| SheafError::Config(format!("config re-render: {e}")))?;
    crate::store::atomic_write_public(&config_file_path(root), rendered.as_bytes())?;
    Ok(true)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skeleton_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        write_skeleton(tmp.path()).unwrap();
        assert_eq!(read_store_format(tmp.path()).unwrap(), STORE_FORMAT_VERSION);
        // config.toml IS the marker now: no legacy flat file is laid down.
        assert!(!format_file_path(tmp.path()).exists());
        let cfg = load(tmp.path()).unwrap();
        assert_eq!(cfg.format_version, STORE_FORMAT_VERSION);
        assert_eq!(cfg.watch.debounce_ms, 300);
        assert_eq!(cfg.watch.max_tracked_bytes, DEFAULT_MAX_TRACKED_BYTES);
        assert!(cfg.ignore.patterns.contains(&".git/".to_string()));
        assert!(cfg.retention.expiry.is_none());
        // The rendered skeleton exposes the store cadence section so the
        // knobs are discoverable in every new project's config.
        let rendered = render_default();
        assert!(rendered.contains("[store]"));
        assert!(rendered.contains("snapshot_edit_size = 512"));
    }

    #[test]
    fn managed_worktree_resolves_shared_store_and_own_head() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&linked).unwrap();
        write_skeleton(&primary).unwrap();
        let link = WorktreeLink {
            version: WORKTREE_LINK_VERSION,
            store_root: primary.clone(),
            id: "branch-a".into(),
        };
        write_worktree_link(&linked, &link).unwrap();

        assert_eq!(worktree_link(&linked), Some(link));
        assert_eq!(store_root(&linked), primary);
        assert_eq!(config_file_path(&linked), config_file_path(&primary));
        assert_eq!(read_store_format(&linked).unwrap(), STORE_FORMAT_VERSION);
        assert_eq!(
            worktree_head_path(&linked),
            sheaf_dir(&primary)
                .join("state/worktrees")
                .join("branch-a.head")
        );
        assert_eq!(
            worktree_head_path(&primary),
            sheaf_dir(&primary).join("state/worktree.head")
        );
    }

    #[test]
    fn worktree_id_distinguishes_primary_from_linked() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&linked).unwrap();
        write_skeleton(&primary).unwrap();
        // The primary worktree has a directory-valued `.sheaf` marker: no link,
        // no id, and it resolves to itself.
        assert_eq!(worktree_id(&primary), None);
        assert_eq!(worktree_link(&primary), None);
        assert_eq!(store_root(&primary), primary);
        assert_eq!(
            worktree_head_path(&primary),
            sheaf_dir(&primary).join("state/worktree.head")
        );

        write_worktree_link(
            &linked,
            &WorktreeLink {
                version: WORKTREE_LINK_VERSION,
                store_root: primary.clone(),
                id: "branch-z".into(),
            },
        )
        .unwrap();
        assert_eq!(worktree_id(&linked).as_deref(), Some("branch-z"));
        assert_eq!(store_root(&linked), primary);
        assert_eq!(
            worktree_head_path(&linked),
            sheaf_dir(&primary).join("state/worktrees/branch-z.head")
        );
        assert_ne!(worktree_head_path(&linked), worktree_head_path(&primary));
    }

    #[test]
    fn write_worktree_link_rejects_malformed_links() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let good = || WorktreeLink {
            version: WORKTREE_LINK_VERSION,
            store_root: root.to_path_buf(),
            id: "abc123".into(),
        };
        // Wrong version.
        let mut bad = good();
        bad.version = WORKTREE_LINK_VERSION + 1;
        assert!(write_worktree_link(root, &bad).is_err());
        // Relative store root.
        let mut bad = good();
        bad.store_root = PathBuf::from("relative/path");
        assert!(write_worktree_link(root, &bad).is_err());
        // Empty id.
        let mut bad = good();
        bad.id = String::new();
        assert!(write_worktree_link(root, &bad).is_err());
        // Whitespace in id.
        let mut bad = good();
        bad.id = "bad id".into();
        assert!(write_worktree_link(root, &bad).is_err());
        // A valid link writes and round-trips.
        write_worktree_link(root, &good()).unwrap();
        assert_eq!(worktree_link(root), Some(good()));
    }

    #[test]
    fn worktree_link_rejects_corrupt_marker_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let marker = root.join(SHEAF_DIR_NAME);
        // Not JSON at all.
        std::fs::write(&marker, b"not json").unwrap();
        assert_eq!(worktree_link(root), None);
        // Well-formed JSON but a stale link version is rejected.
        std::fs::write(
            &marker,
            serde_json::json!({
                "version": WORKTREE_LINK_VERSION + 1,
                "store_root": "/abs/primary",
                "id": "abc",
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(worktree_link(root), None);
        // A relative store_root is refused.
        std::fs::write(
            &marker,
            serde_json::json!({
                "version": WORKTREE_LINK_VERSION,
                "store_root": "relative",
                "id": "abc",
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(worktree_link(root), None);
        // An unlinked root (no marker at all) is simply primary.
        std::fs::remove_file(&marker).unwrap();
        assert_eq!(worktree_link(root), None);
        assert_eq!(store_root(root), root);
    }

    #[test]
    fn store_section_parses_and_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        // Absent section: struct defaults (matching every pre-[store] file).
        std::fs::write(config_file_path(tmp.path()), "format_version = 2\n").unwrap();
        let cfg = load(tmp.path()).unwrap();
        assert_eq!(cfg.store.snapshot_edit_size, 512);
        assert_eq!(cfg.store.max_segment_bytes, 64 * 1024 * 1024);
        // Partial section: per-key defaults fill the gaps.
        std::fs::write(
            config_file_path(tmp.path()),
            "format_version = 2\n\n[store]\nsnapshot_edit_size = 128\n",
        )
        .unwrap();
        let cfg = load(tmp.path()).unwrap();
        assert_eq!(cfg.store.snapshot_edit_size, 128);
        assert_eq!(cfg.store.max_segment_bytes, 64 * 1024 * 1024);
        // The pre-rename key still deserializes (serde alias), and zero is
        // a valid explicit "disable the cadence" value.
        std::fs::write(
            config_file_path(tmp.path()),
            "format_version = 2\n\n[store]\nsnapshot_every_batches = 64\n",
        )
        .unwrap();
        assert_eq!(load(tmp.path()).unwrap().store.snapshot_edit_size, 64);
        std::fs::write(
            config_file_path(tmp.path()),
            "format_version = 2\n\n[store]\nsnapshot_edit_size = 0\n",
        )
        .unwrap();
        assert_eq!(load(tmp.path()).unwrap().store.snapshot_edit_size, 0);
    }

    #[test]
    fn older_format_is_readable_and_upgradable() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        std::fs::write(
            config_file_path(tmp.path()),
            "format_version = 1\n[watch]\ndebounce_ms = 250\n",
        )
        .unwrap();
        // A v1 store loads fine under this (v2) build...
        assert_eq!(read_store_format(tmp.path()).unwrap(), 1);
        assert_eq!(load(tmp.path()).unwrap().watch.debounce_ms, 250);
        // ...and upgrades in place, preserving unrelated fields.
        assert!(upgrade_store_format(tmp.path()).unwrap());
        assert_eq!(read_store_format(tmp.path()).unwrap(), STORE_FORMAT_VERSION);
        assert_eq!(load(tmp.path()).unwrap().watch.debounce_ms, 250);
        // Idempotent.
        assert!(!upgrade_store_format(tmp.path()).unwrap());
    }

    #[test]
    fn newer_format_refused() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        std::fs::write(config_file_path(tmp.path()), "format_version = 999\n").unwrap();
        let err = read_store_format(tmp.path()).unwrap_err();
        assert_eq!(err.code(), "store.version_mismatch");
    }

    #[test]
    fn missing_format_version_key_is_current_format() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        std::fs::write(config_file_path(tmp.path()), "[watch]\ndebounce_ms = 250\n").unwrap();
        assert_eq!(read_store_format(tmp.path()).unwrap(), STORE_FORMAT_VERSION);
        assert_eq!(load(tmp.path()).unwrap().watch.debounce_ms, 250);
    }

    #[test]
    fn expiry_roundtrip_and_parsing() {
        let tmp = tempfile::tempdir().unwrap();
        write_skeleton(tmp.path()).unwrap();
        set_retention_expiry(tmp.path(), "30d").unwrap();
        assert_eq!(
            load(tmp.path()).unwrap().retention.expiry_ms(),
            Some(30 * 86_400_000)
        );
        // Invalid specs refuse instead of writing garbage.
        set_retention_expiry(tmp.path(), "forever").unwrap_err();
        set_retention_expiry(tmp.path(), "30").unwrap_err();
        assert_eq!(
            load(tmp.path()).unwrap().retention.expiry.as_deref(),
            Some("30d")
        );
        assert_eq!(parse_duration_ms("72h"), Some(72 * 3_600_000));
        assert_eq!(parse_duration_ms("45m"), Some(45 * 60_000));
        assert_eq!(parse_duration_ms("90s"), Some(90_000));
        assert_eq!(parse_duration_ms(""), None);
    }

    #[test]
    fn legacy_format_file_is_retired() {
        let tmp = tempfile::tempdir().unwrap();
        write_skeleton(tmp.path()).unwrap();
        // Simulate a store laid down by an older build.
        std::fs::write(format_file_path(tmp.path()), "1\n").unwrap();
        assert!(migrate_legacy_format_file(tmp.path()));
        assert!(!format_file_path(tmp.path()).exists());
        // Idempotent: nothing left to retire.
        assert!(!migrate_legacy_format_file(tmp.path()));
    }

    #[test]
    fn migration_never_fires_on_broken_store() {
        let tmp = tempfile::tempdir().unwrap();
        write_skeleton(tmp.path()).unwrap();
        std::fs::write(config_file_path(tmp.path()), "format_version = 999\n").unwrap();
        std::fs::write(format_file_path(tmp.path()), "1\n").unwrap();
        // A store we cannot validate must not be "migrated" by deleting
        // files next to it.
        assert!(!migrate_legacy_format_file(tmp.path()));
        assert!(format_file_path(tmp.path()).exists());
    }

    #[test]
    fn duration_parser_edges() {
        assert_eq!(parse_duration_ms("10x"), None, "unknown unit");
        assert_eq!(parse_duration_ms("0s"), None, "zero is not a horizon");
        assert_eq!(parse_duration_ms("12"), None, "bare integer is ambiguous");
        assert_eq!(
            parse_duration_ms("  45m  "),
            Some(45 * 60_000),
            "surrounding whitespace is trimmed"
        );
        assert_eq!(
            parse_duration_ms("9223372036854775807s"),
            Some(i64::MAX),
            "overflow saturates instead of wrapping into a negative"
        );
        assert_eq!(
            parse_duration_ms("99999999999999999999999d"),
            None,
            "a count that cannot parse as i64 is refused outright"
        );
    }

    #[test]
    fn load_refuses_out_of_range_formats() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        // Below MIN_STORE_FORMAT: fail closed, same as above the current.
        std::fs::write(config_file_path(tmp.path()), "format_version = 0\n").unwrap();
        let err = load(tmp.path()).unwrap_err();
        assert_eq!(err.code(), "store.version_mismatch");
        assert_eq!(
            read_store_format(tmp.path()).unwrap_err().code(),
            "store.version_mismatch"
        );
    }

    #[test]
    fn load_reports_unparseable_and_unreadable_configs() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(sheaf_dir(tmp.path())).unwrap();
        // Malformed TOML.
        std::fs::write(config_file_path(tmp.path()), "format_version = ][\n").unwrap();
        let err = load(tmp.path()).unwrap_err();
        assert_eq!(err.code(), "bad.params");
        assert!(err.to_string().contains("toml:"));

        // config.toml present but not a regular file (a directory here):
        // reading it is an io error, distinctly not "no project root".
        std::fs::remove_file(config_file_path(tmp.path())).unwrap();
        std::fs::create_dir_all(config_file_path(tmp.path())).unwrap();
        let err = read_store_format(tmp.path()).unwrap_err();
        assert_eq!(
            err.code(),
            "internal",
            "EISDIR maps to Io, not NoProjectRoot"
        );
        let err = load(tmp.path()).unwrap_err();
        assert_eq!(err.code(), "bad.params");
        assert!(err.to_string().contains("unreadable"));

        // A missing marker means there is no project root here at all.
        std::fs::remove_dir_all(config_file_path(tmp.path())).unwrap();
        let err = read_store_format(tmp.path()).unwrap_err();
        assert_eq!(err.code(), "project.no_root");
    }
}

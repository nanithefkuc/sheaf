//! Path classification: the three-tier answer to "is this change work?"
//!
//! The binary ignore of earlier versions could only answer "drop it" — which
//! made every new source of temporary files a new pattern to add, and made
//! every ignored path unrecoverable. Classification splits the question:
//!
//! - [`PathClass::Never`] — the store itself and VCS internals. Never
//!   observed, never recorded, not configurable. The watcher must not see
//!   its own output.
//! - [`PathClass::Volatile`] — not work, but not worthless either: editor
//!   swap/atomic-save temps, build trees, machine-local litter. Observed
//!   (when their events reach a watched directory) but kept OUT of the
//!   append-only timeline; the daemon mirrors their latest state into the
//!   bounded scratch ring so `sheaf recover` can bring them back.
//! - [`PathClass::Durable`] — ordinary work, captured as history.
//!
//! Where the volatile tier's patterns come from, by default: the project's
//! own ignore rule sources (`.gitignore` files, `.git/info/exclude`, git's
//! global ignore), because git's answer to "what is not source" is the one
//! the ecosystem already maintains — plus a small config-side list for the
//! litter git does not know about. A `durable` override can rescue a path
//! a volatile pattern matched; nothing can rescue a `Never` path.

use std::path::{Path, PathBuf};

use crate::config::ProjectConfig;
use crate::ignore::{append_rule_file, gitignore_patterns, ExcludesRel, IgnoreSet};

/// What the watcher may do with a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathClass {
    /// Never observed: the store's own directory and VCS internals.
    Never,
    /// Observed but excluded from the timeline; ring-buffered for recovery.
    Volatile,
    /// Ordinary work: captured into the append-only timeline.
    Durable,
}

/// A compiled three-tier classifier for one project root.
#[derive(Debug, Clone)]
pub struct Classifier {
    /// Hard core: `.sheaf` (store dir and worktree link file) and `.git`.
    never: IgnoreSet,
    /// Gitignore-derived + config volatile patterns.
    volatile: IgnoreSet,
    /// Config `durable` overrides; matched before `volatile`.
    durable: IgnoreSet,
}

impl Classifier {
    /// Patterns no configuration can override: the store must never observe
    /// its own output, and `.git` internals are the VCS's own journal.
    fn never_patterns() -> Vec<String> {
        vec![
            ".sheaf".into(),
            ".sheaf/".into(),
            ".git".into(),
            ".git/".into(),
        ]
    }

    /// Build the effective classifier for a project from its config plus
    /// the repository's own ignore rule sources.
    ///
    /// The volatile tier is the union of: `[classify] volatile` (or its
    /// defaults), the legacy `[ignore] patterns` section (read as volatile —
    /// an explicit migration, not a second meaning), every `.gitignore`
    /// under `root` plus `.git/info/exclude` when `[classify] gitignore` is
    /// on (default), and each file in `extra_rule_files` treated as a
    /// `.gitignore` at the root. Machine-global rule files stay a
    /// daemon-level input exactly as before: their content varies per
    /// machine, and library callers must stay deterministic.
    pub fn for_project_with(
        root: &Path,
        cfg: &ProjectConfig,
        extra_rule_files: &[PathBuf],
    ) -> Result<Self, String> {
        let never = IgnoreSet::from_patterns(&Self::never_patterns())?;
        let durable = IgnoreSet::from_patterns(&cfg.classify.durable)?;

        let mut volatile: Vec<String> = cfg.classify.volatile.clone();
        // Legacy `[ignore] patterns` keep their meaning minus the hard core
        // (which `never` already owns and wins over anyway).
        volatile.extend(
            cfg.ignore
                .patterns
                .iter()
                .filter(|p| {
                    let trimmed = p.trim();
                    trimmed != ".sheaf"
                        && trimmed != ".sheaf/"
                        && trimmed != ".git"
                        && trimmed != ".git/"
                })
                .cloned(),
        );
        if cfg.classify.gitignore {
            volatile.extend(gitignore_patterns(root, &volatile));
        }
        for file in extra_rule_files {
            append_rule_file(file, "", &mut volatile);
        }
        let volatile = IgnoreSet::from_patterns(&volatile)?;
        Ok(Classifier {
            never,
            volatile,
            durable,
        })
    }

    /// [`Classifier::for_project_with`] with no machine-global rule files.
    pub fn for_project(root: &Path, cfg: &ProjectConfig) -> Result<Self, String> {
        Self::for_project_with(root, cfg, &[])
    }

    /// Classify one root-relative path.
    ///
    /// Order is the contract: `Never` first (no override rescues the store
    /// or `.git`), then `durable` overrides (an explicit promotion must beat
    /// a volatile pattern), then the volatile tier, and everything else is
    /// durable work.
    pub fn classify_rel(&self, rel: &Path) -> PathClass {
        if self.never.is_ignored_rel(rel) {
            return PathClass::Never;
        }
        if self.durable.is_ignored_rel(rel) {
            return PathClass::Durable;
        }
        if self.volatile.is_ignored_rel(rel) {
            return PathClass::Volatile;
        }
        PathClass::Durable
    }

    /// Test/degraded constructor: `excluded` patterns become the volatile
    /// tier on top of the hard `Never` core; everything else is durable.
    /// This is the drop-in replacement for the old
    /// `IgnoreSet::from_patterns` call shape.
    pub fn from_volatile_patterns(excluded: &[String]) -> Result<Self, String> {
        Ok(Classifier {
            never: IgnoreSet::from_patterns(&Self::never_patterns())?,
            volatile: IgnoreSet::from_patterns(excluded)?,
            durable: IgnoreSet::empty(),
        })
    }

    /// Classify by the event's probe path (rename destinations, not sources).
    pub fn classify_event_path(&self, root: &Path, path: &Path) -> PathClass {
        match path.strip_prefix(root) {
            Ok(rel) => self.classify_rel(rel),
            Err(_) => PathClass::Never, // outside the project: not ours to touch
        }
    }

    /// A classifier that classifies everything `Durable` (safe fallback for
    /// unreadable configs: watch everything rather than silently drop work).
    pub fn all_durable() -> Self {
        Classifier {
            never: IgnoreSet::from_patterns(&Self::never_patterns())
                .expect("never patterns compile"),
            volatile: IgnoreSet::empty(),
            durable: IgnoreSet::empty(),
        }
    }
}

impl ExcludesRel for Classifier {
    /// Everything the timeline does not model: `Never ∪ Volatile`. This is
    /// the predicate restore baselines and worktree reconciliation walk
    /// with — a path only `durable` overrides rescue stays modelable here.
    fn excludes_rel(&self, rel: &Path) -> bool {
        self.classify_rel(rel) != PathClass::Durable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{default_volatile_patterns, ProjectConfig};
    use std::path::Path;

    fn classifier(root: &Path, cfg: &ProjectConfig) -> Classifier {
        Classifier::for_project(root, cfg).expect("patterns compile")
    }

    fn rel(p: &str) -> &Path {
        Path::new(p)
    }

    #[test]
    fn hard_core_is_never_regardless_of_config() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ProjectConfig::default();
        // Even explicit durable overrides cannot rescue the hard core.
        cfg.classify.durable = vec![".sheaf/**".into(), "/.git".into()];
        let c = classifier(tmp.path(), &cfg);
        assert_eq!(
            c.classify_rel(rel(".sheaf/store/journal/seg-1.op")),
            PathClass::Never
        );
        assert_eq!(c.classify_rel(rel(".git/index")), PathClass::Never);
        assert_eq!(c.classify_rel(rel("sub/.git/config")), PathClass::Never);
        // The managed-worktree link file is a FILE named `.sheaf`.
        assert_eq!(c.classify_rel(rel(".sheaf")), PathClass::Never);
    }

    #[test]
    fn default_volatile_tier_covers_editor_litter_and_heavy_trees() {
        let tmp = tempfile::tempdir().unwrap();
        let c = classifier(tmp.path(), &ProjectConfig::default());
        for p in [
            "notes.md.swp",
            "src/deep/module.rs.swo",
            "draft.txt~",
            "app.log.bak",
            "build.sh.tmp",
            "conflict.rs.orig",
        ] {
            assert_eq!(c.classify_rel(rel(p)), PathClass::Volatile, "{p}");
        }
        for p in ["node_modules/dep/index.js", "target/debug/deps/x.rlib"] {
            assert_eq!(c.classify_rel(rel(p)), PathClass::Volatile, "{p}");
        }
        for p in ["src/main.rs", "README.md", "deep/nested/notes.md"] {
            assert_eq!(c.classify_rel(rel(p)), PathClass::Durable, "{p}");
        }
    }

    #[test]
    fn gitignore_rules_become_volatile_and_can_be_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "*.profraw\n/dist/\n").unwrap();
        let c = classifier(tmp.path(), &ProjectConfig::default());
        assert_eq!(
            c.classify_rel(rel("bench/default_123.profraw")),
            PathClass::Volatile
        );
        assert_eq!(c.classify_rel(rel("dist/bundle.js")), PathClass::Volatile);

        let mut off = ProjectConfig::default();
        off.classify.gitignore = false;
        let c = classifier(tmp.path(), &off);
        assert_eq!(
            c.classify_rel(rel("bench/default_123.profraw")),
            PathClass::Durable
        );
        assert_eq!(c.classify_rel(rel("dist/bundle.js")), PathClass::Durable);
    }

    #[test]
    fn legacy_ignore_section_reads_as_volatile() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ProjectConfig::default();
        cfg.ignore.patterns = vec![".omp-home/".into(), ".*.tmpdir/".into()];
        let c = classifier(tmp.path(), &cfg);
        assert_eq!(
            c.classify_rel(rel(".omp-home/state/db")),
            PathClass::Volatile
        );
        assert_eq!(
            c.classify_rel(rel(".12345.tmpdir/inner")),
            PathClass::Volatile
        );
        // Defaults still apply alongside the legacy list.
        assert_eq!(c.classify_rel(rel("a.swp")), PathClass::Volatile);
    }

    #[test]
    fn durable_override_rescues_a_volatile_path_but_not_never() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(".gitignore"), "*.log\n").unwrap();
        let mut cfg = ProjectConfig::default();
        cfg.classify.durable = vec!["important.log".into()];
        let c = classifier(tmp.path(), &cfg);
        assert_eq!(c.classify_rel(rel("important.log")), PathClass::Durable);
        assert_eq!(c.classify_rel(rel("other.log")), PathClass::Volatile);
    }

    #[test]
    fn excludes_rel_is_everything_not_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let c = classifier(tmp.path(), &ProjectConfig::default());
        let e = &c as &dyn ExcludesRel;
        assert!(e.excludes_rel(rel(".git/HEAD")));
        assert!(e.excludes_rel(rel("a.swp")));
        assert!(!e.excludes_rel(rel("src/lib.rs")));
    }

    #[test]
    fn explicit_volatile_list_replaces_defaults_like_ignore_patterns_do() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ProjectConfig::default();
        cfg.classify.volatile = vec!["junk/".into()];
        let c = classifier(tmp.path(), &cfg);
        assert_eq!(c.classify_rel(rel("junk/x")), PathClass::Volatile);
        // Defaults are gone: swap litter is durable unless gitignored.
        assert_eq!(c.classify_rel(rel("a.swp")), PathClass::Durable);
        assert!(!default_volatile_patterns().is_empty());
    }

    #[test]
    fn all_durable_fallback_still_blinds_the_store() {
        let c = Classifier::all_durable();
        assert_eq!(c.classify_rel(rel(".sheaf/anything")), PathClass::Never);
        assert_eq!(c.classify_rel(rel("src/lib.rs")), PathClass::Durable);
    }

    #[test]
    fn uncompilable_patterns_fail_with_the_offending_pattern() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = ProjectConfig::default();
        cfg.classify.volatile = vec!["[unclosed".into()];
        let err = Classifier::for_project(tmp.path(), &cfg).unwrap_err();
        assert!(err.contains("unclosed"), "{err}");
    }
}

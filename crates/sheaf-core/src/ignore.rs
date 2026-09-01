//! Ignore-rule evaluation for the watcher.
//!
//! Pattern grammar (documented subset):
//! - `.git/`            → directory *name* matched at any depth
//! - `node_modules`     → bare name matches any path component (file or dir)
//! - `*.log`            → wildcard glob against the root-relative path
//! - `assets/generated` → anchored relative-path glob (no wildcards = exact)
//!
//! The store itself (`.sheaf`) is ALWAYS ignored, regardless of config:
//! the watcher must never observe its own output.

use std::path::{Path, PathBuf};

use globset::{Glob, GlobSet, GlobSetBuilder};

/// A compiled ignore matcher: a set of bare component names plus a glob set,
/// tested against root-relative paths and each of their ancestor prefixes.
#[derive(Debug, Clone)]
pub struct IgnoreSet {
    /// Bare component names (from plain tokens and trailing-`/` patterns).
    names: Vec<String>,
    globs: GlobSet,
}

impl IgnoreSet {
    /// An ignore set that matches nothing (used as a safe fallback).
    pub fn empty() -> Self {
        IgnoreSet {
            names: Vec::new(),
            globs: GlobSet::empty(),
        }
    }

    /// Build the effective ignore set for a project: the configured patterns
    /// unioned with the project's own ignore rule sources — every
    /// `.gitignore` under `root` plus the repository's
    /// `.git/info/exclude` (including the `gitdir:` indirection used by
    /// worktrees and submodules). The README promises "anything git ignores
    /// is automatically ignored", and for a repository that promise includes
    /// the exclude file. Nested `.gitignore` files are honored, each
    /// anchored to its own directory.
    ///
    /// Union semantics: a gitignore entry is one more reason to
    /// ignore a path; it can never *un-ignore* one. Negation lines (`!pattern`)
    /// are therefore skipped rather than partially honored — turning an ignore
    /// off is order-dependent, and quietly recording a file the user told git
    /// to skip is the worse failure. `.sheaf/` stays always-ignored regardless.
    ///
    /// Machine-global rule files (git's `core.excludesFile` default
    /// locations) are deliberately NOT read here — their content varies by
    /// machine, which would make matching nondeterministic for tests and
    /// CLI degraded mode. The daemon unions them via
    /// [`IgnoreSet::for_project_with`].
    pub fn for_project(root: &Path, config_patterns: &[String]) -> Result<Self, String> {
        Self::for_project_with(root, config_patterns, &[])
    }

    /// [`IgnoreSet::for_project`] plus extra rule files (e.g. git's global
    /// ignore), each treated as if it were a `.gitignore` at the project
    /// root: bare names match at any depth, anchored lines at the root.
    pub fn for_project_with(
        root: &Path,
        config_patterns: &[String],
        extra_rule_files: &[PathBuf],
    ) -> Result<Self, String> {
        let mut patterns: Vec<String> = config_patterns.to_vec();
        patterns.extend(gitignore_patterns(root, config_patterns));
        for file in extra_rule_files {
            append_rule_file(file, "", &mut patterns);
        }
        Self::from_patterns(&patterns)
    }

    /// Compile an ignore set directly from a list of patterns in this
    /// module's grammar, without consulting any `.gitignore` files.
    pub fn from_patterns(patterns: &[String]) -> Result<Self, String> {
        let mut names = Vec::new();
        let mut builder = GlobSetBuilder::new();
        for pat in patterns {
            let pat = pat.trim();
            if pat.is_empty() || pat.starts_with('#') {
                continue;
            }
            // A leading `/` anchors the pattern to the project root (gitignore
            // convention): it matches only at the top level, never at depth.
            // Strip the marker and always route it through the glob path so a
            // slash-less anchored name (`/secret.env`) does not fall into the
            // any-depth `names` bucket.
            if let Some(rooted) = pat.strip_prefix('/') {
                let rooted = rooted.trim_end_matches('/');
                if rooted.is_empty() {
                    continue;
                }
                builder.add(Glob::new(rooted).map_err(|e| format!("{rooted}: {e}"))?);
                // Anchored directory match also covers everything beneath it.
                if pat.ends_with('/') || !rooted.contains('/') {
                    builder.add(Glob::new(&format!("{rooted}/**")).map_err(|e| e.to_string())?);
                }
                continue;
            }
            if let Some(name) = pat.strip_suffix('/') {
                if name.contains('*') || name.contains('?') || name.contains('[') {
                    builder.add(Glob::new(&format!("**/{name}/**")).map_err(|e| e.to_string())?);
                    builder.add(Glob::new(&format!("**/{name}")).map_err(|e| e.to_string())?);
                } else {
                    names.push(name.to_string());
                }
                continue;
            }
            if !pat.contains('*') && !pat.contains('?') && !pat.contains('[') && !pat.contains('/')
            {
                names.push(pat.to_string());
                continue;
            }
            builder.add(Glob::new(pat).map_err(|e| format!("{pat}: {e}"))?);
        }
        let globs = builder.build().map_err(|e| e.to_string())?;
        Ok(IgnoreSet { names, globs })
    }

    /// Always-on ignore names plus configured ones.
    fn hard_names() -> [String; 1] {
        [crate::config::SHEAF_DIR_NAME.to_string()]
    }

    /// Does this relative path (root-relative, components given) lie in an
    /// ignored subtree? Checks every ancestor prefix as well as the full path.
    pub fn is_ignored_rel(&self, rel: &Path) -> bool {
        let mut cur = PathBufBounds::new();
        for comp in rel.components() {
            use std::path::Component;
            if let Component::Normal(c) = comp {
                cur.push_comp(c);
                let as_str = c.to_string_lossy();
                let hits_name = self.names.iter().any(|n| n.as_str() == as_str.as_ref())
                    || IgnoreSet::hard_names()
                        .iter()
                        .any(|n| n.as_str() == as_str.as_ref());
                if hits_name {
                    return true;
                }
                if self.globs.is_match(cur.as_path()) {
                    return true;
                }
            }
        }
        self.globs.is_match(rel)
    }
}

/// Predicate: does this root-relative path lie outside what the timeline
/// models? Implemented by [`IgnoreSet`] (pattern-union semantics — tests,
/// degraded callers) and by [`crate::classify::Classifier`] (three-tier
/// semantics — the daemon's live path, where durable overrides can rescue
/// a path a volatile pattern matched). Store surfaces that walk the
/// worktree — restore baselines, reconciliation, merge scoping — take this
/// instead of a concrete set so both semantics slot in.
pub trait ExcludesRel {
    fn excludes_rel(&self, rel: &Path) -> bool;
}

impl ExcludesRel for IgnoreSet {
    fn excludes_rel(&self, rel: &Path) -> bool {
        self.is_ignored_rel(rel)
    }
}

/// Collect ignore patterns from every `.gitignore` under `root` and from
/// the repository's `.git/info/exclude`, translated into this module's
/// pattern grammar and anchored to each file's directory.
///
/// Directories excluded by the always-ignored names (`.sheaf`, `.git`) and
/// the conventional heavy dirs — **and by the configured patterns** — are
/// not descended. The config layer matters as much as the hard-coded list:
/// a vendored dependency tree (`.cargo-home/`, `vendor/`, …) is
/// config-ignored precisely because its contents are not project sources,
/// and the ~100 `.gitignore` files a vendored cargo registry carries would
/// otherwise each contribute patterns like `Cargo.lock` or `.*` to the
/// project-wide set. This is a best-effort read: an unreadable
/// `.gitignore` is skipped, never fatal.
pub fn gitignore_patterns(root: &Path, config_patterns: &[String]) -> Vec<String> {
    // Names we never descend into while hunting for .gitignore files. These are
    // the always-ignored store/vcs dirs plus the common regenerable trees; a
    // .gitignore inside them is irrelevant to what sheaf should track.
    const SKIP_DIRS: &[&str] = &[
        crate::config::SHEAF_DIR_NAME,
        ".git",
        "node_modules",
        "target",
    ];
    let config_set =
        IgnoreSet::from_patterns(config_patterns).unwrap_or_else(|_| IgnoreSet::empty());
    let mut out = Vec::new();
    let walker = walkdir::WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            let name_skipped = e
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| SKIP_DIRS.contains(&n))
                .unwrap_or(false);
            if name_skipped {
                return false;
            }
            // Config-ignored trees are equally irrelevant as .gitignore
            // sources: the user already told sheaf to disregard them.
            match e.path().strip_prefix(root) {
                Ok(rel) if rel.as_os_str().is_empty() => true,
                Ok(rel) => !config_set.is_ignored_rel(rel),
                Err(_) => false,
            }
        });
    for entry in walker.filter_map(std::result::Result::ok) {
        if entry.file_name() != ".gitignore" {
            continue;
        }
        let Some(dir) = entry.path().parent() else {
            continue;
        };
        // Directory of this .gitignore, relative to root, as a forward-slash
        // prefix ("" for the root file, "sub/dir" for a nested one).
        let prefix = dir
            .strip_prefix(root)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        append_rule_file(entry.path(), &prefix, &mut out);
    }
    // The repository's own exclude file participates too: git treats it as
    // a root-level gitignore, and "sheaf ignores whatever git ignores"
    // must include the place `git config`-free workflows put their local
    // rules. Worktrees and submodules keep `.git` as a `gitdir:` pointer
    // file, so resolve that indirection too.
    if let Some(exclude) = git_exclude_file(root) {
        append_rule_file(&exclude, "", &mut out);
    }
    out
}

/// Read one rule file (`.gitignore` grammar) and append its translated
/// patterns to `out`. Missing or unreadable files contribute nothing.
pub fn append_rule_file(file: &Path, prefix: &str, out: &mut Vec<String>) {
    let Ok(text) = std::fs::read_to_string(file) else {
        return;
    };
    for line in text.lines() {
        if let Some(pat) = translate_gitignore_line(line, prefix) {
            out.push(pat);
        }
    }
}

/// Locate the repository's `info/exclude`, following the `gitdir:` pointer
/// when `.git` is a file (worktrees, submodules, old-style links).
fn git_exclude_file(root: &Path) -> Option<PathBuf> {
    let dot = root.join(".git");
    let meta = std::fs::symlink_metadata(&dot).ok()?;
    let git_dir = if meta.is_dir() {
        dot
    } else if meta.is_file() {
        let text = std::fs::read_to_string(&dot).ok()?;
        let pointed = text
            .lines()
            .find_map(|l| l.trim().strip_prefix("gitdir:"))?
            .trim();
        let p = PathBuf::from(pointed);
        if p.is_absolute() {
            p
        } else {
            root.join(p)
        }
    } else {
        return None;
    };
    Some(git_dir.join("info").join("exclude"))
}

/// Translate one `.gitignore` line (from a file in directory `prefix`
/// relative to root) into this module's pattern grammar, or None to skip it
/// (blank, comment, or negation). Union-only: negations are dropped.
///
/// Anchoring follows gitignore: a leading `/` or any interior `/` pins the
/// pattern to the .gitignore's own directory; a bare name matches at any depth
/// beneath it. Anchored patterns are emitted with a leading `/` (our
/// root-anchored marker) after being joined to `prefix`, so a nested file's
/// `gen/x` becomes `/sub/gen/x` and a root `/secret.env` becomes
/// `/secret.env` — matched only at that exact location, never at other depths.
fn translate_gitignore_line(line: &str, prefix: &str) -> Option<String> {
    let raw = line.trim();
    if raw.is_empty() || raw.starts_with('#') || raw.starts_with('!') {
        return None;
    }
    let dir_only = raw.ends_with('/');
    let core = raw.trim_end_matches('/');
    let anchored = core.starts_with('/') || core.trim_start_matches('/').contains('/');
    let name = core.trim_start_matches('/');
    if name.is_empty() {
        return None;
    }
    let joined = if anchored {
        // Anchor to root: prepend the nested directory prefix (if any) and the
        // root-anchor `/` marker.
        if prefix.is_empty() {
            format!("/{name}")
        } else {
            format!("/{prefix}/{name}")
        }
    } else if prefix.is_empty() {
        // A bare name at the root matches by component at any depth.
        name.to_string()
    } else {
        // A bare name in a nested .gitignore matches that name at any depth
        // *beneath that file's directory*, per gitignore scoping. Emitting it
        // unprefixed (any depth project-wide) once looked like the "safe"
        // direction for a union that only adds ignores — until a vendored
        // dependency's `Cargo.lock` or `.*` line started ignoring the root
        // lockfile and every dotfile of the host project. Depth scoping is
        // the correct and still-conservative reading: it can only ignore
        // things git itself would ignore under that directory.
        format!("/{prefix}/**/{name}")
    };
    Some(if dir_only {
        format!("{joined}/")
    } else {
        joined
    })
}

// Small helper avoiding a dependency on PathBuilder semantics across editions.
struct PathBufBounds(std::path::PathBuf);

impl PathBufBounds {
    fn new() -> Self {
        PathBufBounds(std::path::PathBuf::new())
    }
    fn push_comp(&mut self, c: &std::ffi::OsStr) {
        self.0.push(c);
    }
    fn as_path(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(pats: &[&str]) -> IgnoreSet {
        let owned: Vec<String> = pats.iter().map(|s| s.to_string()).collect();
        IgnoreSet::from_patterns(&owned).unwrap()
    }

    #[test]
    fn dir_names_match_any_depth() {
        let ig = set(&["target/", ".git/"]);
        for rel in ["target", "a/target/b/c.txt", ".git/config", "x/.git/HEAD"] {
            assert!(ig.is_ignored_rel(Path::new(rel)), "{rel}");
        }
        for rel in ["targeteer", "src/main.rs"] {
            assert!(!ig.is_ignored_rel(Path::new(rel)), "{rel}");
        }
    }

    #[test]
    fn wildcards_and_anchored_paths() {
        let ig = set(&["*.log", "assets/generated"]);
        assert!(ig.is_ignored_rel(Path::new("debug.log")));
        assert!(ig.is_ignored_rel(Path::new("sub/debug.log")));
        assert!(ig.is_ignored_rel(Path::new("assets/generated/x.bin")));
        assert!(!ig.is_ignored_rel(Path::new("assets/generated2"))); // anchored
    }

    #[test]
    fn store_dir_always_ignored() {
        let ig = set(&[]);
        assert!(ig.is_ignored_rel(Path::new(".sheaf/store/journal/seg.op")));
    }

    #[test]
    fn project_gitignore_is_unioned_with_config() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(
            root.join(".gitignore"),
            "# comment\n\n*.log\nbuild/\n/secret.env\n",
        )
        .unwrap();
        let ig = IgnoreSet::for_project(root, &["node_modules/".to_string()]).unwrap();

        // From .gitignore.
        assert!(ig.is_ignored_rel(Path::new("debug.log")));
        assert!(ig.is_ignored_rel(Path::new("src/deep/trace.log")));
        assert!(ig.is_ignored_rel(Path::new("build/out.o")));
        assert!(ig.is_ignored_rel(Path::new("build"))); // the dir itself
                                                        // Anchored: /secret.env matches only at the root.
        assert!(ig.is_ignored_rel(Path::new("secret.env")));
        assert!(!ig.is_ignored_rel(Path::new("conf/secret.env")));
        // From config, still honored.
        assert!(ig.is_ignored_rel(Path::new("node_modules/pkg/index.js")));
        // Unrelated files stay tracked.
        assert!(!ig.is_ignored_rel(Path::new("src/main.rs")));
        // The store is always ignored regardless of gitignore contents.
        assert!(ig.is_ignored_rel(Path::new(".sheaf/config.toml")));
    }

    #[test]
    fn nested_gitignore_anchors_to_its_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        // An anchored entry (interior slash) only applies within `sub/`.
        std::fs::write(root.join("sub/.gitignore"), "gen/artifact.bin\n*.tmp\n").unwrap();
        let ig = IgnoreSet::for_project(root, &[]).unwrap();

        // Anchored to sub/.
        assert!(ig.is_ignored_rel(Path::new("sub/gen/artifact.bin")));
        assert!(!ig.is_ignored_rel(Path::new("gen/artifact.bin")));
        // A bare-name pattern from a nested file matches by name at depth
        // beneath that file's directory — and nowhere else.
        assert!(ig.is_ignored_rel(Path::new("sub/scratch.tmp")));
        assert!(ig.is_ignored_rel(Path::new("sub/deep/scratch.tmp")));
        assert!(!ig.is_ignored_rel(Path::new("scratch.tmp")));
    }

    /// Regression: a vendored cargo registry under a config-ignored
    /// `.cargo-home/` carries many crate `.gitignore`s whose bare
    /// `Cargo.lock` / `.*` lines must not leak into the project-wide set and
    /// mark the root lockfile or dot-directories as deleted and un-capturable.
    #[test]
    fn vendored_gitignores_do_not_leak() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let vendor = root.join(".cargo-home/registry/src/some-crate-1.0");
        std::fs::create_dir_all(&vendor).unwrap();
        // Classic vendored-crate .gitignore contents.
        std::fs::write(vendor.join(".gitignore"), "Cargo.lock\n.*\n/target\n").unwrap();
        // A nested project directory of our own, with its own .gitignore.
        std::fs::create_dir_all(root.join("crates/sub")).unwrap();
        std::fs::write(root.join("crates/sub/.gitignore"), "fixtures/\n").unwrap();
        let config = [".cargo-home/".to_string()];
        let ig = IgnoreSet::for_project(root, &config).unwrap();

        // Root project files stay visible despite the vendored lines.
        assert!(!ig.is_ignored_rel(Path::new("Cargo.lock")));
        assert!(!ig.is_ignored_rel(Path::new(".config/project.toml")));
        assert!(!ig.is_ignored_rel(Path::new(".dsh/config.yml")));
        // The vendor tree itself is still ignored (config pattern).
        assert!(ig.is_ignored_rel(Path::new(".cargo-home/registry/src/some-crate-1.0/lib.rs")));
        // Our own nested .gitignore keeps working, scoped to its directory.
        assert!(ig.is_ignored_rel(Path::new("crates/sub/fixtures/data.bin")));
        assert!(!ig.is_ignored_rel(Path::new("fixtures/data.bin")));
    }

    #[test]
    fn gitignore_negations_are_skipped_not_partially_honored() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Union semantics: the negation is dropped, so *.log stays ignored and
        // keep.log is NOT un-ignored. Skipping is the safe default.
        std::fs::write(root.join(".gitignore"), "*.log\n!keep.log\n").unwrap();
        let ig = IgnoreSet::for_project(root, &[]).unwrap();
        assert!(ig.is_ignored_rel(Path::new("debug.log")));
        assert!(ig.is_ignored_rel(Path::new("keep.log")));
    }

    #[test]
    fn missing_gitignore_is_fine() {
        let tmp = tempfile::tempdir().unwrap();
        let ig = IgnoreSet::for_project(tmp.path(), &["target/".to_string()]).unwrap();
        assert!(ig.is_ignored_rel(Path::new("target/x")));
        assert!(!ig.is_ignored_rel(Path::new("src/main.rs")));
    }

    /// `.git/info/exclude` is part of "what git ignores" and must be part
    /// of what sheaf ignores: local-only rules (no `.gitignore` entry at
    /// all) live there, and skipping the file silently tracks paths the
    /// user believes are private.
    #[test]
    fn git_info_exclude_is_honored() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join(".git/info")).unwrap();
        std::fs::write(root.join(".git/info/exclude"), ".lucid/\nsecrets.local\n").unwrap();
        let ig = IgnoreSet::for_project(root, &[]).unwrap();
        assert!(ig.is_ignored_rel(Path::new(".lucid/JOURNAL.md")));
        assert!(ig.is_ignored_rel(Path::new("notes/secrets.local")));
        assert!(!ig.is_ignored_rel(Path::new("src/main.rs")));
    }

    /// Worktrees and submodules keep `.git` as a `gitdir:` pointer file;
    /// the exclude file lives beside the real git dir, not at the pointer.
    #[test]
    fn worktree_gitdir_pointer_resolves_exclude() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let git_dir = tmp.path().join("real-git-dir");
        std::fs::create_dir_all(git_dir.join("info")).unwrap();
        std::fs::write(git_dir.join("info/exclude"), "private/\n").unwrap();
        // A relative gitdir pointer resolves against the worktree root.
        std::fs::write(root.join(".git"), "gitdir: real-git-dir\n").unwrap();
        let ig = IgnoreSet::for_project(root, &[]).unwrap();
        assert!(ig.is_ignored_rel(Path::new("private/notes.txt")));
        assert!(!ig.is_ignored_rel(Path::new("public/notes.txt")));
    }

    /// Extra rule files (the daemon passes git's global ignore) union in
    /// with everything else, with root-level gitignore anchoring.
    #[test]
    fn extra_rule_files_are_unioned() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join(".gitignore"), "local-only.log\n").unwrap();
        let global = tmp.path().join("global-ignore");
        std::fs::write(&global, "*.globallog\n/global-exact.txt\n").unwrap();
        let ig = IgnoreSet::for_project_with(root, &[], std::slice::from_ref(&global)).unwrap();
        // From the repo .gitignore.
        assert!(ig.is_ignored_rel(Path::new("local-only.log")));
        // From the extra file: bare name at any depth...
        assert!(ig.is_ignored_rel(Path::new("a/b/x.globallog")));
        // ...and anchored lines at the root only.
        assert!(ig.is_ignored_rel(Path::new("global-exact.txt")));
        assert!(!ig.is_ignored_rel(Path::new("sub/global-exact.txt")));
        assert!(!ig.is_ignored_rel(Path::new("src/main.rs")));
    }

    /// Editor swap/backup/atomic-save litter must be ignored at any depth so a
    /// Neovim-style write-temp-then-rename save does not flood the timeline.
    /// The litter includes leading-dot temp names like `.<name>.md.tmp` and
    /// `.<name>.md.swp`, which must match at the root and at depth. Real source
    /// files with similar-looking names stay tracked.
    #[test]
    fn editor_temp_and_swap_files_are_ignored_by_default() {
        let ig = IgnoreSet::from_patterns(&crate::config::default_patterns()).unwrap();
        // Ignored: the litter, at root and at depth.
        for rel in [
            "notes.md.swp",
            "crates/sheaf-cli/src/main.rs.swp",
            ".notes.md.swp",
            "deep/dir/x.swo",
            "y.swn",
            "main.rs~",
            "crates/deep/main.rs~",
            "config.toml.bak",
            ".scratch.md.tmp",
            "sub/.notes.md.tmp",
            "merge.rs.orig",
        ] {
            assert!(ig.is_ignored_rel(Path::new(rel)), "should ignore {rel}");
        }
        // Tracked: real files that merely resemble the patterns.
        for rel in [
            "src/main.rs",
            "README.md",
            "swap.rs",          // not a *.sw? file
            "backup_policy.rs", // not *.bak
            "notes.md",
            "crates/sheaf-core/src/config.rs",
        ] {
            assert!(!ig.is_ignored_rel(Path::new(rel)), "should track {rel}");
        }
    }

    #[test]
    fn empty_set_ignores_nothing_but_the_store() {
        let ig = IgnoreSet::empty();
        assert!(!ig.is_ignored_rel(Path::new("anything/at/all.txt")));
        // The store stays always-ignored even with zero configured rules.
        assert!(ig.is_ignored_rel(Path::new("x/.sheaf/store.op")));
    }

    #[test]
    fn pattern_grammar_edges() {
        // Empty lines, whitespace, comments, and a bare `/` are skipped
        // rather than compiled into match-nothing globs.
        let ig = set(&["", "   ", "# a comment", "/"]);
        assert!(!ig.is_ignored_rel(Path::new("comment")));
        assert!(!ig.is_ignored_rel(Path::new("a/comment/b")));

        // A bare name (no slash, no wildcard) matches any component.
        let ig = set(&["node_modules"]);
        assert!(ig.is_ignored_rel(Path::new("node_modules")));
        assert!(ig.is_ignored_rel(Path::new("a/node_modules/b.js")));
        assert!(!ig.is_ignored_rel(Path::new("a/node_modules_b.js")));

        // A wildcard DIRECTORY pattern compiles to any-depth dir globs.
        let ig = set(&["*.tmp.d/"]);
        assert!(ig.is_ignored_rel(Path::new("some.tmp.d")));
        assert!(ig.is_ignored_rel(Path::new("x/y/scratch.tmp.d/z")));

        // A rooted wildcard anchors to the project root.
        let ig = set(&["/secrets/*.env"]);
        assert!(ig.is_ignored_rel(Path::new("secrets/prod.env")));
        assert!(!ig.is_ignored_rel(Path::new("conf/secrets/prod.env")));

        // `?` and `[...]` wildcards route through the glob path too.
        let ig = set(&["data-?.bin"]);
        assert!(ig.is_ignored_rel(Path::new("data-1.bin")));
        assert!(!ig.is_ignored_rel(Path::new("data-12.bin")));
        let ig = set(&["va[t]"]);
        assert!(ig.is_ignored_rel(Path::new("vat")));

        // An invalid glob is a hard error, never a silent match-nothing set.
        assert!(IgnoreSet::from_patterns(&["[invalid".to_string()]).is_err());
    }

    #[test]
    fn skipped_directories_are_not_hunted_for_gitignores() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // A hard-skipped dir (target/) and a config-ignored dir (vendor/)
        // both carry .gitignore files whose patterns must not leak.
        std::fs::create_dir_all(root.join("target/debug")).unwrap();
        std::fs::write(root.join("target/.gitignore"), "*.gen.rs\n").unwrap();
        std::fs::create_dir_all(root.join("vendor/crate-1.0")).unwrap();
        std::fs::write(root.join("vendor/.gitignore"), "Cargo.lock\n").unwrap();
        let ig = IgnoreSet::for_project(root, &["vendor/".to_string()]).unwrap();
        assert!(!ig.is_ignored_rel(Path::new("app.gen.rs")));
        assert!(!ig.is_ignored_rel(Path::new("Cargo.lock")));
    }

    #[test]
    fn unreadable_gitignore_is_skipped_never_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let gi = root.join(".gitignore");
        std::fs::write(&gi, "secret.txt\n").unwrap();
        std::fs::set_permissions(&gi, std::os::unix::fs::PermissionsExt::from_mode(0o000)).unwrap();
        let ig = IgnoreSet::for_project(root, &[]);
        // Restore before the tempdir cleanup so removal succeeds.
        std::fs::set_permissions(&gi, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
        let ig = ig.unwrap();
        assert!(!ig.is_ignored_rel(Path::new("secret.txt")));
    }

    #[test]
    fn degenerate_gitignore_lines_are_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        // `/` collapses to an empty name (dropped); negations and blanks too.
        std::fs::write(tmp.path().join(".gitignore"), "/\n!negated.txt\n   \n# c\n").unwrap();
        let ig = IgnoreSet::for_project(tmp.path(), &[]).unwrap();
        assert!(!ig.is_ignored_rel(Path::new("negated.txt")));
        assert!(!ig.is_ignored_rel(Path::new("anything/else.txt")));
    }
}

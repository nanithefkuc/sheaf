//! Enrollment registry: the daemon's durable list of watched projects.
//! One JSONL file, one JSON object per line, tombstoned by
//! rewrite on removal. Explicit override paths keep it testable.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Result, SheafError};

/// One registry record: a watched project root and when it was enrolled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Enrollment {
    /// Canonical absolute project root.
    pub root: PathBuf,
    pub added_at: DateTime<Utc>,
}

/// Handle to the enrollment file, identified by its path so tests and the
/// daemon can point at different locations.
#[derive(Debug, Clone)]
pub struct Registry {
    file: PathBuf,
}

const FILE_NAME: &str = "enrollments.jsonl";

impl Registry {
    /// Production registry under `$XDG_DATA_HOME/sheaf/`.
    pub fn global() -> Result<Self> {
        Ok(Registry {
            file: crate::paths::data_sheaf_dir()?.join(FILE_NAME),
        })
    }

    /// Test/explicit-location constructor.
    pub fn at(file: PathBuf) -> Self {
        Registry { file }
    }

    /// Path of the backing JSONL file.
    pub fn file_path(&self) -> &Path {
        &self.file
    }

    fn ensure_parent(&self) -> std::io::Result<()> {
        if let Some(parent) = self.file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// All enrollments; malformed lines are skipped with a warning.
    pub fn list(&self) -> Result<Vec<Enrollment>> {
        if !self.file.exists() {
            return Ok(Vec::new());
        }
        let f = std::fs::File::open(&self.file)?;
        let mut out = Vec::new();
        for line in std::io::BufReader::new(f).lines() {
            let line = line.map_err(|e| SheafError::Registry(format!("read: {e}")))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<Enrollment>(line) {
                Ok(e) => out.push(e),
                Err(e) => tracing::warn!(%line, error = %e, "skipping malformed enrollment line"),
            }
        }
        Ok(out)
    }

    /// Add root if absent (canonicalized best-effort for comparison).
    /// Returns true when newly added.
    pub fn upsert(&self, root: &Path) -> Result<bool> {
        self.ensure_parent()?;
        let canon = normalize_existing(root);
        let existing = self.list()?;
        if existing.iter().any(|e| same_root(&e.root, &canon)) {
            return Ok(false);
        }
        let entry = Enrollment {
            root: canon.clone(),
            added_at: Utc::now(),
        };
        let line = serde_json::to_string(&entry)
            .map_err(|e| SheafError::Registry(format!("serialize: {e}")))?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)
            .map_err(|e| SheafError::Registry(format!("{}: {e}", self.file.display())))?;
        writeln!(f, "{line}")
            .map_err(|e| SheafError::Registry(format!("{}: {e}", self.file.display())))?;
        f.flush()
            .map_err(|e| SheafError::Registry(format!("{}: {e}", self.file.display())))?;
        Ok(true)
    }

    /// Remove root by rewriting the file without it. Returns true if removed.
    pub fn forget(&self, root: &Path) -> Result<bool> {
        let canon = normalize_existing(root);
        let entries = self.list()?;
        let kept: Vec<&Enrollment> = entries
            .iter()
            .filter(|e| !same_root(&e.root, &canon))
            .collect();
        if kept.len() == entries.len() {
            return Ok(false);
        }
        self.ensure_parent()?;
        let tmp = self.file.with_extension("jsonl.tmp");
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| SheafError::Registry(format!("{}: {e}", tmp.display())))?;
            for e in kept {
                writeln!(
                    f,
                    "{}",
                    serde_json::to_string(e).map_err(|er| SheafError::Registry(er.to_string()))?
                )?;
            }
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.file)?;
        Ok(true)
    }
}

/// Best-effort canonicalization; falls back to cleaned input.
pub fn normalize_existing(root: &Path) -> PathBuf {
    root.canonicalize().unwrap_or_else(|_| root.to_path_buf())
}

fn same_root(a: &Path, b: &Path) -> bool {
    normalize_existing(a) == normalize_existing(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(dir: &Path) -> Registry {
        Registry::at(dir.join("enrollments.jsonl"))
    }

    #[test]
    fn upsert_list_forget_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let proj_a = tempfile::tempdir_in(tmp.path()).unwrap();
        let proj_b = tempfile::tempdir_in(tmp.path()).unwrap();
        let r = reg(tmp.path());

        assert!(r.upsert(proj_a.path()).unwrap());
        assert!(!r.upsert(proj_a.path()).unwrap()); // dedupe
        r.upsert(proj_b.path()).unwrap();

        let list = r.list().unwrap();
        assert_eq!(list.len(), 2);
        assert!(list.iter().any(|e| same_root(&e.root, proj_b.path())));

        assert!(r.forget(proj_a.path()).unwrap());
        assert!(!r.forget(proj_a.path()).unwrap()); // already gone
        let list = r.list().unwrap();
        assert_eq!(list.len(), 1);
        assert!(same_root(&list[0].root, proj_b.path()));
    }

    #[test]
    fn malformed_lines_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("enrollments.jsonl");
        std::fs::write(&f, "{not json\n{\"garbage\": 1}\n").unwrap();
        let r = Registry::at(f);
        assert_eq!(r.list().unwrap().len(), 0);
    }

    #[test]
    fn missing_file_lists_empty_and_forget_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let r = reg(tmp.path());
        assert!(
            r.list().unwrap().is_empty(),
            "absent registry reads as empty"
        );
        assert!(
            !r.forget(&tmp.path().join("never-enrolled")).unwrap(),
            "forgetting an unknown root reports nothing removed"
        );
    }

    #[test]
    fn blank_lines_are_skipped_and_valid_rows_survive_among_bad_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("enrollments.jsonl");
        let entry = Enrollment {
            root: PathBuf::from("/some/proj"),
            added_at: Utc::now(),
        };
        let good = serde_json::to_string(&entry).unwrap();
        std::fs::write(&f, format!("\n{good}\n\n{{not json\n")).unwrap();
        let r = Registry::at(f);
        let list = r.list().unwrap();
        assert_eq!(
            list.len(),
            1,
            "blank lines skipped, malformed skipped, good kept"
        );
        assert_eq!(list[0].root, PathBuf::from("/some/proj"));
    }

    #[test]
    fn global_registry_resolves_under_xdg_data_home() {
        let _g = crate::test_util::env_guard();
        std::env::set_var("XDG_DATA_HOME", "/reg-test-data");
        let r = Registry::global().unwrap();
        assert_eq!(
            r.file_path(),
            PathBuf::from("/reg-test-data/sheaf/enrollments.jsonl")
        );
        std::env::remove_var("XDG_DATA_HOME");
    }
}

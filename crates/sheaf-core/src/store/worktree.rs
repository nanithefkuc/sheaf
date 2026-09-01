//! Live physical worktrees backed by one append-only Sheaf timeline.
//!
//! A managed worktree contains a lightweight `.sheaf` link file. Its head is
//! stored separately under the primary store, while every capture still lands
//! in the same journal. The daemon remains the sole writer and checks out the
//! addressed head before reconciling that worktree.

use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::restore::{entries_at, validate_key, Content, Entry};
use super::timeline::{decode_frontier, read_head_frontier};
use super::{blobs, fsutil, ProjectStore};
use crate::config::{self, WorktreeLink, WORKTREE_LINK_VERSION};
use crate::error::{Result, SheafError};

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE: &str = "worktrees.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorktreeInfo {
    /// `None` identifies the primary worktree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub path: PathBuf,
    pub frontier: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    pub primary: bool,
    pub present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorktreeRegistry {
    version: u32,
    worktrees: Vec<WorktreeInfo>,
}

impl Default for WorktreeRegistry {
    fn default() -> Self {
        Self {
            version: REGISTRY_VERSION,
            worktrees: Vec::new(),
        }
    }
}

fn registry_path(root: &Path) -> PathBuf {
    config::sheaf_dir(root).join("state").join(REGISTRY_FILE)
}

fn load_registry(root: &Path) -> Result<WorktreeRegistry> {
    let path = registry_path(root);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WorktreeRegistry::default())
        }
        Err(error) => return Err(error.into()),
    };
    let registry: WorktreeRegistry = serde_json::from_slice(&bytes)
        .map_err(|error| SheafError::StoreCorrupt(format!("{}: {error}", path.display())))?;
    if registry.version != REGISTRY_VERSION {
        return Err(SheafError::StoreCorrupt(format!(
            "unsupported worktree registry version {}",
            registry.version
        )));
    }
    Ok(registry)
}

fn save_registry(root: &Path, registry: &WorktreeRegistry) -> Result<()> {
    let path = registry_path(root);
    let bytes = serde_json::to_vec_pretty(registry)
        .map_err(|error| SheafError::Other(format!("serialize worktrees: {error}")))?;
    fsutil::atomic_write(&path, &bytes)?;
    Ok(())
}

/// Registered linked worktrees only. The primary root is implicit.
pub fn linked_worktrees(root: &Path) -> Result<Vec<WorktreeInfo>> {
    let mut worktrees = load_registry(root)?.worktrees;
    for worktree in &mut worktrees {
        worktree.present = worktree.path.is_dir();
    }
    worktrees.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(worktrees)
}

fn canonical_new_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Err(SheafError::Config(format!(
            "worktree destination {} already exists",
            path.display()
        )));
    }
    let name = path.file_name().ok_or_else(|| {
        SheafError::Config(format!(
            "worktree destination {} has no final name",
            path.display()
        ))
    })?;
    let parent = path.parent().ok_or_else(|| {
        SheafError::Config(format!(
            "worktree destination {} has no parent",
            path.display()
        ))
    })?;
    let parent = parent.canonicalize().map_err(|error| {
        SheafError::Config(format!(
            "worktree destination parent {}: {error}",
            parent.display()
        ))
    })?;
    Ok(parent.join(name))
}

fn ensure_disjoint(candidate: &Path, roots: impl Iterator<Item = PathBuf>) -> Result<()> {
    for root in roots {
        if candidate.starts_with(&root) || root.starts_with(candidate) {
            return Err(SheafError::Config(format!(
                "worktree {} overlaps existing worktree {}",
                candidate.display(),
                root.display()
            )));
        }
    }
    Ok(())
}

fn worktree_id(frontier: &str, path: &Path) -> String {
    let mut hash = Sha256::new();
    hash.update(b"sheaf-worktree-v1\0");
    hash.update(frontier.as_bytes());
    hash.update(b"\0");
    hash.update(path.as_os_str().as_encoded_bytes());
    hash.update(b"\0");
    hash.update(
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_le_bytes(),
    );
    hex::encode(hash.finalize())[..16].to_owned()
}

fn install_entry(root: &Path, sdir: &Path, key: &str, entry: &Entry) -> Result<()> {
    validate_key(key)?;
    let dst = root.join(key);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(&dst)?;
    match &entry.content {
        Content::Text(text) => file.write_all(text.as_bytes())?,
        Content::Binary { hash, .. } => {
            let blob_path = blobs::blob_path(sdir, hash);
            let mut blob = std::fs::File::open(&blob_path).map_err(|error| {
                SheafError::StoreCorrupt(format!("missing blob {hash}: {error}"))
            })?;
            let mut hasher = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let read = blob.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                file.write_all(&buffer[..read])?;
            }
            let actual = hex::encode(hasher.finalize());
            if actual != *hash {
                return Err(SheafError::StoreCorrupt(format!(
                    "blob {hash} content mismatch (found {actual})"
                )));
            }
        }
    }
    file.sync_all()?;
    if entry.exec {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

impl ProjectStore {
    /// Primary plus every registered physical worktree.
    pub fn worktrees(&self) -> Result<Vec<WorktreeInfo>> {
        let primary_root = self.store_root().to_path_buf();
        let primary_frontier =
            read_head_frontier(&primary_root).unwrap_or_else(|| self.current_frontier());
        let primary_capture = decode_frontier(&primary_frontier)
            .ok()
            .and_then(|frontier| super::timeline::capture_id_at(self.doc_ref(), &frontier));
        let mut out = vec![WorktreeInfo {
            id: None,
            path: primary_root.clone(),
            frontier: primary_frontier,
            capture_id: primary_capture,
            primary: true,
            present: primary_root.is_dir(),
        }];
        let mut linked = linked_worktrees(&primary_root)?;
        for worktree in &mut linked {
            if let Some(frontier) = read_head_frontier(&worktree.path) {
                worktree.capture_id = decode_frontier(&frontier)
                    .ok()
                    .and_then(|point| super::timeline::capture_id_at(self.doc_ref(), &point));
                worktree.frontier = frontier;
            }
        }
        out.extend(linked);
        out.sort_by(|a, b| (!a.primary, &a.path).cmp(&(!b.primary, &b.path)));
        Ok(out)
    }

    /// Materialize one immutable timeline point as a live linked worktree.
    /// The destination publish is an atomic directory rename; registry and
    /// per-worktree head are written only after every payload is durable.
    pub fn add_worktree(&mut self, reference: &str, destination: &Path) -> Result<WorktreeInfo> {
        let point = self.resolve(reference)?;
        let destination = canonical_new_path(destination)?;
        let existing = self.worktrees()?;
        ensure_disjoint(&destination, existing.iter().map(|item| item.path.clone()))?;

        let id = worktree_id(&point.frontier, &destination);
        let parent = destination
            .parent()
            .expect("canonical destination has parent");
        let staging = parent.join(format!(".sheaf-worktree-{id}.tmp"));
        if staging.exists() {
            return Err(SheafError::Config(format!(
                "stale worktree staging directory {} exists",
                staging.display()
            )));
        }
        std::fs::create_dir(&staging)?;

        let result = (|| -> Result<()> {
            let frontier = decode_frontier(&point.frontier)?;
            let entries = entries_at(self.doc_ref(), &frontier)?;
            for (key, entry) in &entries {
                install_entry(&staging, &self.sdir, key, entry)?;
            }
            config::write_worktree_link(
                &staging,
                &WorktreeLink {
                    version: WORKTREE_LINK_VERSION,
                    store_root: self.store_root().to_path_buf(),
                    id: id.clone(),
                },
            )?;
            fsutil::sync_parent_dir(&staging.join(config::SHEAF_DIR_NAME))?;
            std::fs::rename(&staging, &destination)?;
            fsutil::sync_parent_dir(&destination)?;

            let head_path = config::worktree_head_path(&destination);
            let head = serde_json::json!({
                "seq": self.seq(),
                "capture_id": point.capture_id,
                "frontier": point.frontier,
                "events_flushed": 0,
                "flushed_at": Utc::now().to_rfc3339(),
                "root": destination,
            });
            fsutil::atomic_write(&head_path, head.to_string().as_bytes())?;

            let mut registry = load_registry(self.store_root())?;
            if registry
                .worktrees
                .iter()
                .any(|item| item.id.as_deref() == Some(&id))
            {
                return Err(SheafError::StoreCorrupt(format!(
                    "duplicate managed worktree id {id}"
                )));
            }
            registry.worktrees.push(WorktreeInfo {
                id: Some(id.clone()),
                path: destination.clone(),
                frontier: point.frontier.clone(),
                capture_id: point.capture_id.clone(),
                primary: false,
                present: true,
            });
            save_registry(self.store_root(), &registry)
        })();

        if let Err(error) = result {
            let _ = std::fs::remove_dir_all(&staging);
            if destination.is_dir() && config::worktree_id(&destination).as_deref() == Some(&id) {
                let _ = std::fs::remove_dir_all(&destination);
            }
            let _ = std::fs::remove_file(
                config::sheaf_dir(self.store_root())
                    .join("state/worktrees")
                    .join(format!("{id}.head")),
            );
            return Err(error);
        }

        Ok(WorktreeInfo {
            id: Some(id),
            path: destination,
            frontier: point.frontier,
            capture_id: point.capture_id,
            primary: false,
            present: true,
        })
    }

    /// Refuse event attribution to an unregistered directory even if it has a
    /// forged link file pointing at this store.
    pub fn is_registered_worktree(&self, root: &Path) -> Result<bool> {
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        if root == self.store_root() {
            return Ok(true);
        }
        let linked = linked_worktrees(self.store_root())?;
        Ok(linked
            .into_iter()
            .any(|item| item.path == root && item.present))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::events::{Batch, EventKind, FsEvent};
    use crate::store::StoreLimits;

    #[test]
    fn add_worktree_materializes_point_and_registers_link() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        std::fs::write(primary.join("a.txt"), "one\n").unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: primary.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Added {
                    path: primary.join("a.txt"),
                })],
            })
            .unwrap();

        let created = store.add_worktree("@", &linked).unwrap();
        assert_eq!(
            std::fs::read_to_string(linked.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_eq!(config::store_root(&linked), primary);
        assert_eq!(config::worktree_id(&linked), created.id);
        assert_eq!(store.worktrees().unwrap().len(), 2);
        assert!(store.is_registered_worktree(&linked).unwrap());
    }

    #[test]
    fn worktree_destinations_must_be_new_and_disjoint() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        assert!(store.add_worktree("@", &primary.join("nested")).is_err());
        let existing = tmp.path().join("existing");
        std::fs::create_dir(&existing).unwrap();
        assert!(store.add_worktree("@", &existing).is_err());
    }

    fn open_primary_with_capture() -> (tempfile::TempDir, ProjectStore, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        std::fs::write(primary.join("a.txt"), "one\n").unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: primary.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Added {
                    path: primary.join("a.txt"),
                })],
            })
            .unwrap();
        (tmp, store, primary)
    }

    #[test]
    fn ensure_disjoint_rejects_overlap_in_both_directions() {
        let existing = PathBuf::from("/a/b");
        // Candidate nested under an existing worktree.
        assert!(ensure_disjoint(Path::new("/a/b/c"), std::iter::once(existing.clone())).is_err());
        // Existing worktree nested under the candidate.
        assert!(ensure_disjoint(Path::new("/a"), std::iter::once(existing.clone())).is_err());
        // Disjoint sibling is fine.
        assert!(ensure_disjoint(Path::new("/a/x"), std::iter::once(existing)).is_ok());
    }

    #[test]
    fn worktrees_report_each_linked_head_and_missing_dirs() {
        let (tmp, mut store, primary) = open_primary_with_capture();
        let linked = tmp.path().join("linked");
        store.create_checkpoint("base", None).unwrap();
        let created = store.add_worktree("base", &linked).unwrap();

        // The linked worktree advances its own head independently.
        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("b.txt"), "two\n").unwrap();
        let now = Utc::now();
        let linked_capture = store
            .apply_batch(&Batch {
                root: linked.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Added {
                    path: linked.join("b.txt"),
                })],
            })
            .unwrap()
            .capture
            .unwrap()
            .id;
        store.activate_worktree(&primary).unwrap();

        let worktrees = store.worktrees().unwrap();
        assert_eq!(worktrees.len(), 2);
        let primary_info = worktrees.iter().find(|w| w.primary).unwrap();
        let linked_info = worktrees.iter().find(|w| !w.primary).unwrap();
        assert_eq!(linked_info.id, created.id);
        assert!(primary_info.present && linked_info.present);
        // Each worktree resolves to its own capture, not a shared one.
        assert_eq!(
            linked_info.capture_id.as_deref(),
            Some(linked_capture.as_str())
        );
        assert_ne!(primary_info.capture_id, linked_info.capture_id);

        // A vanished directory is reported not-present, keeping its registry row.
        std::fs::remove_dir_all(&linked).unwrap();
        let linked_rows = linked_worktrees(&primary).unwrap();
        assert_eq!(linked_rows.len(), 1);
        assert!(!linked_rows[0].present);
        // worktrees() falls back to the registry head when the dir is gone.
        let after = store.worktrees().unwrap();
        let gone = after.iter().find(|w| !w.primary).unwrap();
        assert!(!gone.present);
        assert_eq!(gone.capture_id, created.capture_id);
    }

    #[test]
    fn is_registered_worktree_rejects_unknown_and_forged_links() {
        let (tmp, store, primary) = open_primary_with_capture();
        // An unrelated directory is not a worktree of this store.
        let stranger = tmp.path().join("stranger");
        std::fs::create_dir(&stranger).unwrap();
        assert!(!store.is_registered_worktree(&stranger).unwrap());

        // A directory carrying a forged link file, but never registered, is
        // still refused: registration, not the link, is the authority.
        let forged = tmp.path().join("forged");
        std::fs::create_dir(&forged).unwrap();
        config::write_worktree_link(
            &forged,
            &WorktreeLink {
                version: WORKTREE_LINK_VERSION,
                store_root: primary.clone(),
                id: "deadbeefdeadbeef".into(),
            },
        )
        .unwrap();
        assert!(!store.is_registered_worktree(&forged).unwrap());
        // The primary root itself is always registered.
        assert!(store.is_registered_worktree(&primary).unwrap());
    }

    #[test]
    fn add_worktree_rejects_a_duplicate_or_overlapping_destination() {
        let (tmp, mut store, _primary) = open_primary_with_capture();
        let linked = tmp.path().join("linked");
        store.add_worktree("@", &linked).unwrap();
        // Re-adding the same, now-existing destination is refused.
        assert!(store.add_worktree("@", &linked).is_err());
        // A destination nested inside a live worktree overlaps it.
        assert!(store.add_worktree("@", &linked.join("inner")).is_err());
    }

    #[test]
    fn add_worktree_materializes_binary_and_exec_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        let bytes = vec![0u8, 1, 2, 250, 251, 255, 0, 9];
        std::fs::write(primary.join("blob.bin"), &bytes).unwrap();
        let script = primary.join("run.sh");
        std::fs::write(&script, "#!/bin/sh\necho hi\n").unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: primary.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![
                    FsEvent::now(EventKind::Added {
                        path: primary.join("blob.bin"),
                    }),
                    FsEvent::now(EventKind::Added {
                        path: script.clone(),
                    }),
                ],
            })
            .unwrap();

        store.add_worktree("@", &linked).unwrap();
        assert_eq!(std::fs::read(linked.join("blob.bin")).unwrap(), bytes);
        let mode = std::fs::metadata(linked.join("run.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 != 0, "exec bit is materialized");
    }

    #[test]
    fn add_worktree_cleans_up_when_a_payload_cannot_be_installed() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        std::fs::write(primary.join("blob.bin"), vec![0u8, 1, 2, 255, 254, 7]).unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: primary.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Added {
                    path: primary.join("blob.bin"),
                })],
            })
            .unwrap();
        // Sabotage the store: drop the blob the linked worktree must install.
        std::fs::remove_dir_all(blobs::blobs_dir(&store.sdir)).unwrap();

        assert!(store.add_worktree("@", &linked).is_err());
        // The destination is never published and no staging litter survives.
        assert!(!linked.exists());
        let leftover: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .contains("sheaf-worktree")
            })
            .collect();
        assert!(leftover.is_empty(), "staging directory cleaned up");
        // The registry stays empty; a failed add registers nothing.
        assert!(linked_worktrees(&primary).unwrap().is_empty());
    }

    #[test]
    fn linked_worktrees_rejects_a_corrupt_registry() {
        let (_tmp, _store, primary) = open_primary_with_capture();
        let reg = registry_path(&primary);
        std::fs::create_dir_all(reg.parent().unwrap()).unwrap();
        // An unsupported registry version is refused, not silently trusted.
        std::fs::write(
            &reg,
            serde_json::json!({ "version": REGISTRY_VERSION + 1, "worktrees": [] }).to_string(),
        )
        .unwrap();
        assert!(linked_worktrees(&primary).is_err());
        // An unparseable registry is a corruption, not an empty set.
        std::fs::write(&reg, b"{ not json").unwrap();
        assert!(linked_worktrees(&primary).is_err());
    }

    #[test]
    fn add_worktree_rejects_a_destination_whose_parent_is_missing() {
        let (tmp, mut store, _primary) = open_primary_with_capture();
        let dest = tmp.path().join("no-such-parent").join("linked");
        assert!(store.add_worktree("@", &dest).is_err());
    }

    #[test]
    fn add_worktree_rejects_a_destination_with_no_final_name() {
        let (tmp, mut store, _primary) = open_primary_with_capture();
        // A path ending in `..` has no final component to name a worktree.
        let dest = tmp.path().join("gone").join("..");
        assert!(store.add_worktree("@", &dest).is_err());
    }

    #[test]
    fn add_worktree_detects_a_corrupt_blob() {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        std::fs::write(primary.join("blob.bin"), vec![0u8, 1, 2, 255, 254, 7, 8]).unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: primary.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Added {
                    path: primary.join("blob.bin"),
                })],
            })
            .unwrap();
        // Tamper with the stored blob so its bytes no longer hash to its key.
        let shards = blobs::blobs_dir(&store.sdir);
        for shard in std::fs::read_dir(&shards).unwrap() {
            let shard = shard.unwrap().path();
            if shard.is_dir() {
                for blob in std::fs::read_dir(&shard).unwrap() {
                    std::fs::write(blob.unwrap().path(), b"tampered").unwrap();
                }
            }
        }
        // Materializing the worktree must catch the content mismatch, not
        // silently install corrupt bytes.
        assert!(store.add_worktree("@", &linked).is_err());
        assert!(!linked.exists());
    }
}

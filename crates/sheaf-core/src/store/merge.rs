//! Explicit squash merges between divergent timeline branches.
//!
//! The active worktree is always the target. Source-only changes since the
//! causal meet are applied as one forward capture; paths changed differently
//! on both sides are reported and nothing is written.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::restore::{entries_at, Content, Entry};
use super::timeline::{capture_id_at, decode_frontier, encode_frontier, CaptureOrigin, OriginKind};
use super::{blobs, fsutil, ActionKind, ContentKind, ProjectStore, ResolvedPoint};
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind, FsEvent};
use crate::ignore::IgnoreSet;

const INTENT_FILE: &str = "merge.intent";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeAction {
    pub path: String,
    pub kind: ActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<ContentKind>,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub exec: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeConflict {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergePlan {
    pub token: String,
    pub base: ResolvedPoint,
    pub source: ResolvedPoint,
    pub target: ResolvedPoint,
    pub actions: Vec<MergeAction>,
    pub conflicts: Vec<MergeConflict>,
    pub unchanged: usize,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeOutcome {
    pub token: String,
    pub source: ResolvedPoint,
    pub previous_target: ResolvedPoint,
    pub result: ResolvedPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    pub files_written: usize,
    pub files_deleted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeIntent {
    pub plan: MergePlan,
    pub worktree: PathBuf,
    pub started_ms: i64,
}

fn intent_path(root: &Path) -> PathBuf {
    let head = crate::config::worktree_head_path(root);
    match crate::config::worktree_id(root) {
        Some(id) => head
            .parent()
            .expect("managed head has parent")
            .join(format!("{id}.{INTENT_FILE}")),
        None => crate::config::sheaf_dir(root)
            .join("state")
            .join(INTENT_FILE),
    }
}

pub fn pending_merge_at(root: &Path) -> Option<MergeIntent> {
    serde_json::from_slice(&std::fs::read(intent_path(root)).ok()?).ok()
}

fn point(doc: &loro::LoroDoc, frontier: &loro::Frontiers) -> ResolvedPoint {
    ResolvedPoint {
        frontier: encode_frontier(frontier),
        capture_id: capture_id_at(doc, frontier),
    }
}

fn plan_token(
    base: &ResolvedPoint,
    source: &ResolvedPoint,
    target: &ResolvedPoint,
    actions: &[MergeAction],
    conflicts: &[MergeConflict],
) -> String {
    let value = serde_json::json!({
        "v": 1,
        "base": base.frontier,
        "source": source.frontier,
        "target": target.frontier,
        "actions": actions,
        "conflicts": conflicts,
    });
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}

fn merge_plan_between(
    doc: &loro::LoroDoc,
    source: ResolvedPoint,
    target: ResolvedPoint,
) -> Result<MergePlan> {
    let source_frontier = decode_frontier(&source.frontier)?;
    let target_frontier = decode_frontier(&target.frontier)?;
    let source_vv = doc.frontiers_to_vv(&source_frontier).ok_or_else(|| {
        SheafError::TimelineReference("merge source is outside recorded history".into())
    })?;
    let target_vv = doc.frontiers_to_vv(&target_frontier).ok_or_else(|| {
        SheafError::TimelineReference("merge target is outside recorded history".into())
    })?;
    let base_frontier = doc.vv_to_frontiers(&source_vv.intersection(&target_vv));
    let base = point(doc, &base_frontier);

    let base_entries = entries_at(doc, &base_frontier)?;
    let source_entries = entries_at(doc, &source_frontier)?;
    let target_entries = entries_at(doc, &target_frontier)?;
    let mut paths = BTreeSet::new();
    paths.extend(base_entries.keys().cloned());
    paths.extend(source_entries.keys().cloned());
    paths.extend(target_entries.keys().cloned());

    let mut deletes = Vec::new();
    let mut writes = Vec::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0usize;
    for path in paths {
        let base_entry = base_entries.get(&path);
        let source_entry = source_entries.get(&path);
        let target_entry = target_entries.get(&path);
        if source_entry == base_entry || source_entry == target_entry {
            unchanged += 1;
            continue;
        }
        if target_entry != base_entry {
            conflicts.push(MergeConflict {
                path,
                reason: "both branches changed this path differently".into(),
            });
            continue;
        }
        match source_entry {
            None => deletes.push(MergeAction {
                path,
                kind: ActionKind::Delete,
                content: None,
                bytes: 0,
                hash: None,
                exec: false,
            }),
            Some(entry) => writes.push(MergeAction {
                path,
                kind: if target_entry.is_some() {
                    ActionKind::Update
                } else {
                    ActionKind::Create
                },
                content: Some(entry.content_key()),
                bytes: entry.byte_len(),
                hash: entry.hash().map(str::to_owned),
                exec: entry.exec,
            }),
        }
    }
    let mut actions = deletes;
    actions.extend(writes);
    Ok(MergePlan {
        token: plan_token(&base, &source, &target, &actions, &conflicts),
        base,
        source,
        target,
        actions,
        conflicts,
        unchanged,
        created_at_ms: Utc::now().timestamp_millis(),
    })
}

fn disk_matches(root: &Path, sdir: &Path, path: &str, entry: Option<&Entry>) -> Result<bool> {
    let dst = root.join(path);
    let metadata = match std::fs::symlink_metadata(&dst) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entry.is_none()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let Some(entry) = entry else {
        return Ok(false);
    };
    let exec = {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o111 != 0
    };
    if exec != entry.exec {
        return Ok(false);
    }
    match &entry.content {
        Content::Text(text) => Ok(std::fs::read(&dst)? == text.as_bytes()),
        Content::Binary { hash, .. } => {
            let _ = sdir;
            Ok(blobs::hash_file(&dst)? == *hash)
        }
    }
}

fn write_entry_atomic(
    root: &Path,
    sdir: &Path,
    token: &str,
    index: usize,
    path: &str,
    entry: &Entry,
) -> Result<()> {
    super::restore::validate_key(path)?;
    let dst = root.join(path);
    let parent = dst.parent().expect("validated path has parent");
    std::fs::create_dir_all(parent)?;
    if std::fs::symlink_metadata(&dst)
        .is_ok_and(|meta| !meta.file_type().is_file() || meta.file_type().is_symlink())
    {
        return Err(SheafError::RestoreObstructed(format!(
            "{} is not a regular file",
            dst.display()
        )));
    }
    let tmp = parent.join(format!(".sheaf-merge-{}-{index}.tmp", &token[..12]));
    let mut output = std::fs::File::create(&tmp)?;
    match &entry.content {
        Content::Text(text) => output.write_all(text.as_bytes())?,
        Content::Binary { hash, .. } => {
            let mut input = std::fs::File::open(blobs::blob_path(sdir, hash)).map_err(|error| {
                SheafError::StoreCorrupt(format!("missing blob {hash}: {error}"))
            })?;
            let mut hasher = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                output.write_all(&buffer[..read])?;
            }
            let actual = hex::encode(hasher.finalize());
            if actual != *hash {
                let _ = std::fs::remove_file(&tmp);
                return Err(SheafError::StoreCorrupt(format!(
                    "blob {hash} content mismatch (found {actual})"
                )));
            }
        }
    }
    output.sync_all()?;
    if entry.exec {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }
    std::fs::rename(&tmp, &dst)?;
    fsutil::sync_parent_dir(&dst)?;
    Ok(())
}

impl ProjectStore {
    pub fn plan_merge(&self, source_reference: &str) -> Result<MergePlan> {
        let source = self.resolve(source_reference)?;
        let target_frontier = self.materialized_frontiers();
        merge_plan_between(
            self.doc_ref(),
            source,
            point(self.doc_ref(), &target_frontier),
        )
    }

    fn write_merge_intent(&self, plan: &MergePlan) -> Result<()> {
        let intent = MergeIntent {
            plan: plan.clone(),
            worktree: self.root().to_path_buf(),
            started_ms: Utc::now().timestamp_millis(),
        };
        let bytes = serde_json::to_vec_pretty(&intent)
            .map_err(|error| SheafError::Other(format!("serialize merge intent: {error}")))?;
        fsutil::atomic_write(&intent_path(self.root()), &bytes)?;
        Ok(())
    }

    fn apply_merge_files(
        &self,
        plan: &MergePlan,
    ) -> Result<(BTreeMap<String, Entry>, BTreeMap<String, Entry>)> {
        let source_frontier = decode_frontier(&plan.source.frontier)?;
        let target_frontier = decode_frontier(&plan.target.frontier)?;
        let source_entries = entries_at(self.doc_ref(), &source_frontier)?;
        let target_entries = entries_at(self.doc_ref(), &target_frontier)?;
        for action in &plan.actions {
            let disk_is_target = disk_matches(
                self.root(),
                &self.sdir,
                &action.path,
                target_entries.get(&action.path),
            )?;
            let disk_is_result = disk_matches(
                self.root(),
                &self.sdir,
                &action.path,
                source_entries.get(&action.path),
            )?;
            if !disk_is_target && !disk_is_result {
                return Err(SheafError::TimelineMergeConflict(format!(
                    "{} changed after merge planning",
                    action.path
                )));
            }
        }
        for action in plan
            .actions
            .iter()
            .filter(|action| action.kind == ActionKind::Delete)
        {
            let path = self.root().join(&action.path);
            match std::fs::remove_file(&path) {
                Ok(()) => fsutil::sync_parent_dir(&path)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        for (index, action) in plan
            .actions
            .iter()
            .filter(|action| action.kind != ActionKind::Delete)
            .enumerate()
        {
            let entry = source_entries.get(&action.path).ok_or_else(|| {
                SheafError::StoreCorrupt(format!("merge source lost {}", action.path))
            })?;
            if !disk_matches(self.root(), &self.sdir, &action.path, Some(entry))? {
                write_entry_atomic(
                    self.root(),
                    &self.sdir,
                    &plan.token,
                    index,
                    &action.path,
                    entry,
                )?;
            }
        }
        Ok((target_entries, source_entries))
    }

    fn record_merge(
        &mut self,
        plan: &MergePlan,
        target_entries: &BTreeMap<String, Entry>,
        source_entries: &BTreeMap<String, Entry>,
    ) -> Result<Option<super::Capture>> {
        let mut removed = Vec::new();
        let mut created = Vec::new();
        for action in &plan.actions {
            match action.kind {
                ActionKind::Delete => removed.push(action),
                ActionKind::Create => created.push(action),
                ActionKind::Update => {}
            }
        }
        let mut paired_from = BTreeSet::new();
        let mut paired_to = BTreeSet::new();
        let mut events = Vec::new();
        for gone in removed {
            let Some(old) = target_entries.get(&gone.path) else {
                continue;
            };
            if let Some(landed) = created.iter().find(|landed| {
                !paired_to.contains(landed.path.as_str())
                    && source_entries
                        .get(&landed.path)
                        .is_some_and(|entry| entry.identity() == old.identity())
            }) {
                paired_from.insert(gone.path.as_str());
                paired_to.insert(landed.path.as_str());
                events.push(FsEvent::now(EventKind::Renamed {
                    from: self.root().join(&gone.path),
                    to: self.root().join(&landed.path),
                }));
            }
        }
        for action in &plan.actions {
            match action.kind {
                ActionKind::Delete if !paired_from.contains(action.path.as_str()) => {
                    events.push(FsEvent::now(EventKind::Removed {
                        path: self.root().join(&action.path),
                    }));
                }
                ActionKind::Create if !paired_to.contains(action.path.as_str()) => {
                    events.push(FsEvent::now(EventKind::Added {
                        path: self.root().join(&action.path),
                    }));
                }
                ActionKind::Update => events.push(FsEvent::now(EventKind::Touched {
                    path: self.root().join(&action.path).into(),
                })),
                _ => {}
            }
        }
        if events.is_empty() {
            return Ok(None);
        }
        let now = Utc::now();
        let outcome = self.apply_batch_tagged(
            &Batch {
                root: self.root().to_path_buf(),
                started_at: now,
                flushed_at: now,
                events,
            },
            Some(CaptureOrigin {
                kind: OriginKind::Merge,
                target: plan.source.capture_id.clone(),
                scope: plan
                    .actions
                    .iter()
                    .map(|action| action.path.clone())
                    .collect(),
                selections: Vec::new(),
            }),
        )?;
        Ok(outcome.capture)
    }

    pub fn apply_merge(&mut self, plan: &MergePlan, ignore: &IgnoreSet) -> Result<MergeOutcome> {
        if !plan.conflicts.is_empty() {
            return Err(SheafError::TimelineMergeConflict(format!(
                "{} unresolved path conflict(s)",
                plan.conflicts.len()
            )));
        }
        self.reconcile_worktree(ignore)?;
        if self.current_frontier() != plan.target.frontier {
            return Err(SheafError::MergePlanStale(
                "target worktree advanced; plan the merge again".into(),
            ));
        }
        let fresh = merge_plan_between(self.doc_ref(), plan.source.clone(), plan.target.clone())?;
        if fresh.token != plan.token {
            return Err(SheafError::MergePlanStale(
                "source or target history changed; plan the merge again".into(),
            ));
        }
        self.write_merge_intent(&fresh)?;
        self.finish_merge_intent(&fresh)
    }

    fn finish_merge_intent(&mut self, plan: &MergePlan) -> Result<MergeOutcome> {
        let (target_entries, source_entries) = self.apply_merge_files(plan)?;
        let capture = self.record_merge(plan, &target_entries, &source_entries)?;
        let result = self.resolve("@")?;
        std::fs::remove_file(intent_path(self.root()))?;
        fsutil::sync_parent_dir(&intent_path(self.root()))?;
        Ok(MergeOutcome {
            token: plan.token.clone(),
            source: plan.source.clone(),
            previous_target: plan.target.clone(),
            result,
            capture_id: capture.as_ref().map(|capture| capture.id.clone()),
            files_written: plan
                .actions
                .iter()
                .filter(|action| action.kind != ActionKind::Delete)
                .count(),
            files_deleted: plan
                .actions
                .iter()
                .filter(|action| action.kind == ActionKind::Delete)
                .count(),
        })
    }

    /// Resume an interrupted merge only while every affected path is still
    /// either the old target or the planned result. User edits fail closed.
    pub fn resume_merge(&mut self) -> Result<Option<MergeOutcome>> {
        let Some(intent) = pending_merge_at(self.root()) else {
            return Ok(None);
        };
        if intent.worktree != self.root() {
            return Err(SheafError::StoreCorrupt(
                "merge intent belongs to another worktree".into(),
            ));
        }
        if self.current_frontier() != intent.plan.target.frontier {
            return Err(SheafError::MergePlanStale(
                "merge target advanced while an intent was pending".into(),
            ));
        }
        self.finish_merge_intent(&intent.plan).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::events::{Batch, EventKind, FsEvent};
    use crate::store::StoreLimits;

    fn capture_file(store: &mut ProjectStore, root: &Path, path: &str, added: bool) -> String {
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: root.to_path_buf(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(if added {
                    EventKind::Added {
                        path: root.join(path),
                    }
                } else {
                    EventKind::Touched {
                        path: root.join(path).into(),
                    }
                })],
            })
            .unwrap()
            .capture
            .unwrap()
            .id
    }

    fn branched_store() -> (tempfile::TempDir, ProjectStore, PathBuf, PathBuf, String) {
        let tmp = tempfile::tempdir().unwrap();
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");
        std::fs::create_dir(&primary).unwrap();
        config::write_skeleton(&primary).unwrap();
        std::fs::write(primary.join("base.txt"), "base\n").unwrap();
        let mut store = ProjectStore::open(&primary, StoreLimits::default()).unwrap();
        capture_file(&mut store, &primary, "base.txt", true);
        store.create_checkpoint("base", None).unwrap();
        store.add_worktree("base", &linked).unwrap();

        std::fs::write(primary.join("target.txt"), "target\n").unwrap();
        capture_file(&mut store, &primary, "target.txt", true);
        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("source.txt"), "source\n").unwrap();
        let source = capture_file(&mut store, &linked, "source.txt", true);
        store.activate_worktree(&primary).unwrap();
        (tmp, store, primary, linked, source)
    }

    #[test]
    fn squash_merge_applies_source_only_changes_as_one_capture() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        assert!(plan.conflicts.is_empty());
        assert_eq!(
            plan.actions
                .iter()
                .map(|action| action.path.as_str())
                .collect::<Vec<_>>(),
            vec!["source.txt"]
        );
        let outcome = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "source\n"
        );
        assert_eq!(
            std::fs::read_to_string(primary.join("target.txt")).unwrap(),
            "target\n"
        );
        assert!(outcome.capture_id.is_some());
        let capture = store
            .capture_info(outcome.capture_id.as_deref().unwrap())
            .unwrap();
        assert_eq!(
            capture.capture.origin.as_ref().map(|origin| origin.kind),
            Some(OriginKind::Merge)
        );
        assert!(pending_merge_at(&primary).is_none());
    }

    #[test]
    fn divergent_edits_to_one_path_are_reported_without_writes() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        std::fs::write(primary.join("base.txt"), "target edit\n").unwrap();
        capture_file(&mut store, &primary, "base.txt", false);
        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("base.txt"), "source edit\n").unwrap();
        let source = capture_file(&mut store, &linked, "base.txt", false);
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.conflicts[0].path, "base.txt");
        assert!(store.apply_merge(&plan, &IgnoreSet::empty()).is_err());
        assert_eq!(
            std::fs::read_to_string(primary.join("base.txt")).unwrap(),
            "target edit\n"
        );
        assert!(pending_merge_at(&primary).is_none());
    }

    fn capture_removed(store: &mut ProjectStore, root: &Path, path: &str) -> String {
        let now = Utc::now();
        store
            .apply_batch(&Batch {
                root: root.to_path_buf(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Removed {
                    path: root.join(path),
                })],
            })
            .unwrap()
            .capture
            .unwrap()
            .id
    }

    #[test]
    fn plan_merge_resolves_base_source_and_target() {
        let (_tmp, store, _primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        // The causal meet is the shared checkpoint, before either branch's
        // divergent capture; source and target are the two branch heads.
        assert_ne!(plan.base.frontier, plan.source.frontier);
        assert_ne!(plan.base.frontier, plan.target.frontier);
        assert_ne!(plan.source.frontier, plan.target.frontier);
        assert_eq!(
            plan.source.frontier,
            store.resolve(&source).unwrap().frontier
        );
        assert_eq!(plan.target.frontier, store.resolve("@").unwrap().frontier);
        // base.txt and target.txt are both source-side unchanged.
        assert_eq!(plan.unchanged, 2);
    }

    #[test]
    fn source_deletion_is_applied_and_recorded() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        std::fs::remove_file(linked.join("base.txt")).unwrap();
        let source = capture_removed(&mut store, &linked, "base.txt");
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        assert!(plan
            .actions
            .iter()
            .any(|action| action.path == "base.txt" && action.kind == ActionKind::Delete));
        let outcome = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        assert!(!primary.join("base.txt").exists());
        assert_eq!(outcome.files_deleted, 1);
        let capture = store
            .capture_info(outcome.capture_id.as_deref().unwrap())
            .unwrap();
        assert_eq!(
            capture.capture.origin.as_ref().map(|origin| origin.kind),
            Some(OriginKind::Merge)
        );
    }

    #[test]
    fn source_rename_lands_content_at_the_new_path() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        std::fs::rename(linked.join("base.txt"), linked.join("renamed.txt")).unwrap();
        let now = Utc::now();
        let source = store
            .apply_batch(&Batch {
                root: linked.clone(),
                started_at: now,
                flushed_at: now,
                events: vec![FsEvent::now(EventKind::Renamed {
                    from: linked.join("base.txt"),
                    to: linked.join("renamed.txt"),
                })],
            })
            .unwrap()
            .capture
            .unwrap()
            .id;
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        let outcome = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        assert!(!primary.join("base.txt").exists());
        assert_eq!(
            std::fs::read_to_string(primary.join("renamed.txt")).unwrap(),
            "base\n"
        );
        assert!(outcome.capture_id.is_some());
    }

    #[test]
    fn binary_source_file_is_merged() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        let bytes = vec![0u8, 159, 146, 150, 255, 254, 0, 1, 7];
        std::fs::write(linked.join("blob.bin"), &bytes).unwrap();
        let source = capture_file(&mut store, &linked, "blob.bin", true);
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        let outcome = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        assert_eq!(std::fs::read(primary.join("blob.bin")).unwrap(), bytes);
        assert!(outcome.capture_id.is_some());
    }

    #[test]
    fn apply_merge_refuses_a_stale_target() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        std::fs::write(primary.join("more.txt"), "more\n").unwrap();
        capture_file(&mut store, &primary, "more.txt", true);
        let err = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap_err();
        assert!(matches!(err, SheafError::MergePlanStale(_)));
    }

    #[test]
    fn resume_merge_without_intent_is_a_noop() {
        let (_tmp, mut store, _primary, _linked, _source) = branched_store();
        assert!(store.resume_merge().unwrap().is_none());
    }

    #[test]
    fn resume_merge_finishes_a_written_intent() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        store.write_merge_intent(&plan).unwrap();
        assert!(pending_merge_at(&primary).is_some());
        let outcome = store.resume_merge().unwrap().expect("intent finished");
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "source\n"
        );
        assert!(outcome.capture_id.is_some());
        assert!(pending_merge_at(&primary).is_none());
    }

    #[test]
    fn resume_merge_refuses_when_target_advanced() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        store.write_merge_intent(&plan).unwrap();
        std::fs::write(primary.join("more.txt"), "more\n").unwrap();
        capture_file(&mut store, &primary, "more.txt", true);
        let err = store.resume_merge().unwrap_err();
        assert!(matches!(err, SheafError::MergePlanStale(_)));
    }

    #[test]
    fn resume_merge_rejects_a_foreign_worktree_intent() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        let intent = MergeIntent {
            plan,
            worktree: primary.join("elsewhere"),
            started_ms: Utc::now().timestamp_millis(),
        };
        std::fs::write(intent_path(&primary), serde_json::to_vec(&intent).unwrap()).unwrap();
        let err = store.resume_merge().unwrap_err();
        assert!(matches!(err, SheafError::StoreCorrupt(_)));
    }

    #[test]
    fn resume_merge_fails_closed_on_an_uncaptured_user_edit() {
        let (_tmp, mut store, primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        store.write_merge_intent(&plan).unwrap();
        // A user writes the merged path to a third value before resume runs.
        std::fs::write(primary.join("source.txt"), "user edit\n").unwrap();
        assert!(store.resume_merge().is_err());
        // Fail-closed: the user's bytes are left untouched.
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "user edit\n"
        );
    }

    #[test]
    fn source_only_edit_updates_a_shared_path() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("base.txt"), "base edited\n").unwrap();
        let source = capture_file(&mut store, &linked, "base.txt", false);
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        assert!(plan
            .actions
            .iter()
            .any(|action| action.path == "base.txt" && action.kind == ActionKind::Update));
        let outcome = store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        assert_eq!(
            std::fs::read_to_string(primary.join("base.txt")).unwrap(),
            "base edited\n"
        );
        // Both the shared-path update and the earlier source-only create land.
        assert_eq!(outcome.files_written, 2);
    }

    #[test]
    fn executable_source_file_preserves_its_mode() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        let script = linked.join("run.sh");
        std::fs::write(&script, "#!/bin/sh\necho hi\n").unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let source = capture_file(&mut store, &linked, "run.sh", true);
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        assert!(plan
            .actions
            .iter()
            .any(|action| action.path == "run.sh" && action.exec));
        store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        let mode = std::fs::metadata(primary.join("run.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 != 0, "exec bit is carried through the merge");
    }

    #[test]
    fn re_merging_an_applied_source_records_no_capture() {
        let (_tmp, mut store, _primary, _linked, source) = branched_store();
        let plan = store.plan_merge(&source).unwrap();
        store.apply_merge(&plan, &IgnoreSet::empty()).unwrap();
        // The source no longer diverges: a second plan is empty and applying
        // it authors nothing.
        let plan2 = store.plan_merge(&source).unwrap();
        assert!(plan2.actions.is_empty());
        let outcome = store.apply_merge(&plan2, &IgnoreSet::empty()).unwrap();
        assert!(outcome.capture_id.is_none());
    }

    #[test]
    fn merge_intent_is_per_worktree_for_a_linked_target() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        // Merge the primary branch's head (which added target.txt) INTO the
        // linked worktree, making the linked tree the merge target.
        let src_ref = store.resolve("@").unwrap().capture_id.unwrap();
        store.activate_worktree(&linked).unwrap();
        let plan = store.plan_merge(&src_ref).unwrap();
        store.write_merge_intent(&plan).unwrap();
        // The intent belongs to the linked worktree's own path, not the primary.
        assert!(pending_merge_at(&linked).is_some());
        assert!(pending_merge_at(&primary).is_none());
        store.resume_merge().unwrap().expect("linked merge resumes");
        assert_eq!(
            std::fs::read_to_string(linked.join("target.txt")).unwrap(),
            "target\n"
        );
        assert!(pending_merge_at(&linked).is_none());
    }

    #[test]
    fn resume_merge_tolerates_a_delete_whose_target_is_already_gone() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        std::fs::remove_file(linked.join("base.txt")).unwrap();
        let source = capture_removed(&mut store, &linked, "base.txt");
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        store.write_merge_intent(&plan).unwrap();
        // The file the merge intends to delete is already gone from disk.
        std::fs::remove_file(primary.join("base.txt")).unwrap();
        store.resume_merge().unwrap().expect("resume finishes");
        assert!(!primary.join("base.txt").exists());
    }

    #[test]
    fn resume_merge_skips_a_binary_already_on_disk() {
        let (_tmp, mut store, primary, linked, _source) = branched_store();
        store.activate_worktree(&linked).unwrap();
        let bytes = vec![0u8, 1, 2, 255, 254, 7, 9];
        std::fs::write(linked.join("blob.bin"), &bytes).unwrap();
        let source = capture_file(&mut store, &linked, "blob.bin", true);
        store.activate_worktree(&primary).unwrap();

        let plan = store.plan_merge(&source).unwrap();
        store.write_merge_intent(&plan).unwrap();
        // The binary already sits on disk with the exact merged content, so the
        // install is recognized as a no-op instead of rewriting it.
        std::fs::write(primary.join("blob.bin"), &bytes).unwrap();
        store.resume_merge().unwrap().expect("resume finishes");
        assert_eq!(std::fs::read(primary.join("blob.bin")).unwrap(), bytes);
    }

    #[test]
    fn merge_plan_between_rejects_points_outside_history() {
        let (_tmp, store, _primary, _linked, _source) = branched_store();
        // A frontier authored by an unrelated store's document decodes fine but
        // names ops this timeline never saw.
        let other = tempfile::tempdir().unwrap();
        let op = other.path().join("p");
        std::fs::create_dir(&op).unwrap();
        config::write_skeleton(&op).unwrap();
        std::fs::write(op.join("x.txt"), "x\n").unwrap();
        let mut ostore = ProjectStore::open(&op, StoreLimits::default()).unwrap();
        capture_file(&mut ostore, &op, "x.txt", true);
        let foreign = ostore.resolve("@").unwrap();
        let real = store.resolve("@").unwrap();
        assert!(merge_plan_between(store.doc_ref(), foreign.clone(), real.clone()).is_err());
        assert!(merge_plan_between(store.doc_ref(), real, foreign).is_err());
    }
}

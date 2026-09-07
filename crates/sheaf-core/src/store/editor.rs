use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use super::timeline::{capture_at_frontier, decode_frontier, frontier_on_current};
use super::{
    hash_of, rel_key, zero_outcome, Capture, CaptureOrigin, HistoricalPathContent, OriginKind,
    ProjectStore, StoreOutcome, TEXT_MAX_BYTES,
};
use crate::classify::{Classifier, PathClass};
use crate::config;
use crate::error::{Result, SheafError};
use crate::events::{Batch, EventKind, FsEvent, TouchedPath};

const SAVED_DIGEST_TTL: Duration = Duration::from_secs(5);

pub(super) struct SavedBufferDigest {
    digest: String,
    expires_at: Instant,
}

impl ProjectStore {
    /// Register the most recently saved editor bytes so delayed watcher echoes
    /// cannot overwrite a newer unsaved snapshot.
    pub fn register_saved_editor_digest(&mut self, path: &Path, digest: &str) -> Result<()> {
        let (canonical, _) = self.editor_path(path)?;
        self.saved_editor_digests.insert(
            canonical,
            SavedBufferDigest {
                digest: digest.to_owned(),
                expires_at: Instant::now() + SAVED_DIGEST_TTL,
            },
        );
        Ok(())
    }

    /// Persist one supplied UTF-8 buffer without reading or writing its file.
    pub fn apply_editor_snapshot(
        &mut self,
        path: &Path,
        text: &str,
        at: DateTime<Utc>,
        origin: CaptureOrigin,
    ) -> Result<StoreOutcome> {
        let (canonical, key) = self.editor_path(path)?;
        if text.len() as u64 > TEXT_MAX_BYTES {
            return Err(editor_unsupported(format!(
                "{} is {} bytes, over the {TEXT_MAX_BYTES}-byte editor limit",
                canonical.display(),
                text.len()
            )));
        }

        let current = self.current_text(&key);
        if current.as_deref() == Some(text) {
            return Ok(zero_outcome(self.seq));
        }
        let old_len = current.as_ref().map_or(0, |value| value.len() as u64);
        let projected = self
            .tracked_text_bytes
            .saturating_sub(old_len)
            .saturating_add(text.len() as u64);
        if projected > self.max_tracked_bytes {
            return Err(editor_unsupported(format!(
                "capturing {} would use {projected} tracked text bytes, over the {}-byte budget",
                canonical.display(),
                self.max_tracked_bytes
            )));
        }

        let (branch_parent, prior_branch_tips) = self.capture_context()?;
        let mut outcome = zero_outcome(self.seq);
        self.upsert_text(&key, text, &mut outcome)?;
        let event = FsEvent {
            kind: EventKind::Touched {
                path: TouchedPath(canonical),
            },
            at,
        };
        let batch = Batch {
            root: self.root.clone(),
            events: vec![event],
            started_at: at,
            flushed_at: at,
        };
        self.finish_capture(
            &batch,
            Some(origin),
            outcome,
            branch_parent,
            prior_branch_tips,
        )
    }

    /// Logical capture representing the path's current text state.
    pub fn editor_cursor(&self, path: &Path) -> Result<Option<String>> {
        let (_, key) = self.editor_path(path)?;
        let Some(capture) = self
            .captures(false, Some(Path::new(&key)), false, 1)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        Ok(Some(self.logical_capture(capture)?.id))
    }

    /// Current stored UTF-8 text for an eligible editor path.
    pub fn editor_text(&self, path: &Path) -> Result<String> {
        let (_, key) = self.editor_path(path)?;
        self.current_text(&key)
            .ok_or_else(|| editor_unsupported(format!("{} is not tracked as text", path.display())))
    }

    /// Resolve a cursor and prove it belongs to the active worktree lineage.
    pub fn validate_editor_cursor(&self, reference: &str) -> Result<String> {
        let point = self.resolve(reference)?;
        let id = point.capture_id.ok_or_else(|| {
            SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
        })?;
        if !frontier_on_current(
            &self.doc,
            &self.ledger,
            &self.materialized_frontiers(),
            &point.frontier,
        ) {
            return Err(SheafError::TimelineReference(format!(
                "`{reference}` is not on the current lineage"
            )));
        }
        Ok(id)
    }

    /// Previous logical text state, skipping metadata-only and repeated-text captures.
    pub fn previous_editor_cursor(&self, cursor: &str, path: &Path) -> Result<Option<String>> {
        let (_, key) = self.editor_path(path)?;
        let mut current = self.logical_capture(self.capture_for(cursor)?)?;
        let current_text = match self.historical_path_content(&current.id, &key)? {
            HistoricalPathContent::Text(text) => text,
            HistoricalPathContent::Absent | HistoricalPathContent::Binary { .. } => {
                return Ok(None)
            }
        };
        let current_digest = hash_of(current_text.as_bytes());

        loop {
            let Some(candidate) = self.logical_predecessor(&current)? else {
                return Ok(None);
            };
            match self.historical_path_content(&candidate.id, &key)? {
                HistoricalPathContent::Text(text) => {
                    if hash_of(text.as_bytes()) != current_digest {
                        return Ok(Some(candidate.id));
                    }
                    current = candidate;
                }
                HistoricalPathContent::Absent | HistoricalPathContent::Binary { .. } => {
                    return Ok(None)
                }
            }
        }
    }

    pub(super) fn is_saved_editor_echo(&mut self, path: &Path, bytes: &[u8]) -> bool {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let Some(marker) = self.saved_editor_digests.get(&path) else {
            return false;
        };
        if Instant::now() >= marker.expires_at {
            self.saved_editor_digests.remove(&path);
            return false;
        }
        if marker.digest == hash_of(bytes) {
            return true;
        }
        self.saved_editor_digests.remove(&path);
        false
    }

    fn editor_path(&self, path: &Path) -> Result<(PathBuf, String)> {
        if !path.is_absolute() {
            return Err(editor_unsupported("editor path must be absolute"));
        }
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            editor_unsupported(format!("cannot inspect {}: {error}", path.display()))
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(editor_unsupported(format!(
                "{} is not a regular non-symlink file",
                path.display()
            )));
        }
        let canonical = path.canonicalize().map_err(|error| {
            editor_unsupported(format!("cannot resolve {}: {error}", path.display()))
        })?;
        if canonical != path {
            return Err(editor_unsupported(format!(
                "{} is not a canonical path",
                path.display()
            )));
        }
        let key = rel_key(&self.root, &canonical)
            .map_err(|error| editor_unsupported(error.to_string()))?;
        let cfg = config::load(&self.root)?;
        let classifier = Classifier::for_project(&self.root, &cfg)
            .map_err(|error| SheafError::Config(error.to_string()))?;
        if classifier.classify_rel(Path::new(&key)) != PathClass::Durable {
            return Err(editor_unsupported(format!(
                "{} is not classified as durable",
                canonical.display()
            )));
        }
        Ok((canonical, key))
    }

    fn capture_for(&self, reference: &str) -> Result<Capture> {
        let point = self.resolve(reference)?;
        let frontier = decode_frontier(&point.frontier)?;
        capture_at_frontier(&self.doc, &frontier).ok_or_else(|| {
            SheafError::TimelineReference(format!("`{reference}` does not name a capture"))
        })
    }

    fn logical_capture(&self, capture: Capture) -> Result<Capture> {
        match capture.origin.as_ref().map(|origin| origin.kind) {
            Some(OriginKind::EditorUndo | OriginKind::EditorRedo) => {
                let target = capture
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.target.as_deref())
                    .ok_or_else(|| {
                        SheafError::StoreCorrupt(
                            "editor navigation capture is missing its target".into(),
                        )
                    })?;
                self.capture_for(target)
            }
            _ => Ok(capture),
        }
    }

    fn logical_predecessor(&self, capture: &Capture) -> Result<Option<Capture>> {
        let reference = match capture.origin.as_ref() {
            Some(origin) if origin.kind == OriginKind::Editor => origin.source.clone(),
            Some(origin)
                if matches!(origin.kind, OriginKind::EditorUndo | OriginKind::EditorRedo) =>
            {
                origin.target.clone()
            }
            _ => {
                let frontier = decode_frontier(&capture.parent_frontier)?;
                capture_at_frontier(&self.doc, &frontier).map(|parent| parent.id)
            }
        };
        reference
            .map(|reference| self.capture_for(&reference))
            .transpose()?
            .map(|candidate| self.logical_capture(candidate))
            .transpose()
    }
}

fn editor_unsupported(message: impl Into<String>) -> SheafError {
    SheafError::EditorUnsupported(message.into())
}

//! Filesystem event model — the vocabulary that flows from watcher to
//! debouncer to sink. Structural events (adds, removals, renames) are
//! first-class here, not inferred after the fact.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What happened to a path: the four kinds of filesystem change the watcher
/// distinguishes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    /// New path appeared (file or directory).
    Added { path: PathBuf },
    /// Path disappeared.
    Removed { path: PathBuf },
    /// Move/rename observed as a paired operation.
    Renamed { from: PathBuf, to: PathBuf },
    /// Content of an existing file changed. Carries no content itself;
    /// reading the changed bytes is the sink's concern, not the watcher's.
    Touched { path: TouchedPath },
}

/// Newtype so a "touched" path stays obvious at the type level without
/// growing another enum arm at every use site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchedPath(pub PathBuf);

impl From<PathBuf> for TouchedPath {
    fn from(p: PathBuf) -> Self {
        TouchedPath(p)
    }
}

/// One observed filesystem change, stamped with when it was seen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsEvent {
    pub kind: EventKind,
    pub at: DateTime<Utc>,
}

impl FsEvent {
    /// Build an event of `kind` stamped with the current wall-clock time.
    pub fn now(kind: EventKind) -> Self {
        FsEvent {
            kind,
            at: Utc::now(),
        }
    }

    /// Primary path this event concerns (destination for renames).
    pub fn path(&self) -> &Path {
        match &self.kind {
            EventKind::Added { path } => path,
            EventKind::Removed { path } => path,
            EventKind::Renamed { to, .. } => to,
            EventKind::Touched { path } => &path.0,
        }
    }
}

/// A coalesced write burst: the debouncer's output and the unit the store
/// persists as one capture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub root: PathBuf,
    pub events: Vec<FsEvent>,
    /// When the earliest event in the batch arrived.
    pub started_at: DateTime<Utc>,
    /// When the debouncer released the batch.
    pub flushed_at: DateTime<Utc>,
}

impl Batch {
    /// Number of events in the batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the batch carries no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

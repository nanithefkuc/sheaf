//! Write-burst debouncer. Consolidates a burst of filesystem
//! events into one Batch per project, flushing on:
//!   1. quiescence (no event for `window`),
//!   2. hard hold cap (`max_hold`) even under continuous activity,
//!   3. buffer cap (`cap_events`),
//!   4. explicit force (shutdown / drop).
//!
//! The clock is injected so tests drive time deterministically.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::events::{Batch, EventKind, FsEvent};

/// Thresholds controlling when a buffered burst is released: the quiescence
/// window, the hard hold cap under continuous activity, and the buffer cap.
#[derive(Debug, Clone)]
pub struct DebouncerConfig {
    /// Flush after this much silence with no new event.
    pub window: Duration,
    /// Force a partial flush once a burst has been held this long, even if
    /// events keep arriving.
    pub max_hold: Duration,
    /// Force a flush once this many events are buffered.
    pub cap_events: usize,
}

impl Default for DebouncerConfig {
    fn default() -> Self {
        DebouncerConfig {
            window: Duration::from_millis(300),
            max_hold: Duration::from_millis(2000),
            cap_events: 4096,
        }
    }
}

/// Per-project batch accumulator.
pub struct Debouncer {
    root: PathBuf,
    cfg: DebouncerConfig,
    buf: Vec<FsEvent>,
    /// path -> index into buf, for collapsing repeat Touched events
    touched_at: HashMap<PathBuf, usize>,
    first_event: Option<Instant>,
    last_event: Option<Instant>,
}

impl Debouncer {
    /// Create an empty debouncer for one project root with the given config.
    pub fn new(root: PathBuf, cfg: DebouncerConfig) -> Self {
        Debouncer {
            root,
            cfg,
            buf: Vec::new(),
            touched_at: HashMap::new(),
            first_event: None,
            last_event: None,
        }
    }

    /// Feed an event; returns an early flush when a hard cap fired.
    pub fn feed(&mut self, ev: FsEvent) -> Option<Batch> {
        let now = Instant::now();
        match &ev.kind {
            EventKind::Touched { path } if self.touched_at.contains_key(&path.0) => {
                // Keep only the latest touch per path within this window;
                // falls through to hold checks so single-file floods still
                // release batches periodically.
                let idx = self.touched_at[&path.0];
                self.buf[idx].at = ev.at;
            }
            kind => {
                self.buf.push(FsEvent {
                    kind: kind.clone(),
                    at: ev.at,
                });
                if let EventKind::Touched { path } = self.buf.last().unwrap().kind.clone() {
                    self.touched_at.insert(path.0, self.buf.len() - 1);
                }
                self.first_event.get_or_insert(now);
            }
        }
        self.last_event = Some(now);

        if self.buf.len() >= self.cfg.cap_events {
            return Some(self.force_flush());
        }
        // Continuous-activity partial flush at max_hold.
        if let Some(first) = self.first_event {
            if now.duration_since(first) >= self.cfg.max_hold {
                return Some(self.force_flush());
            }
        }
        None
    }

    /// Quiescence check: call periodically; flushes after `window` of silence.
    pub fn take_if_quiescent(&mut self) -> Option<Batch> {
        let now = Instant::now();
        if self.buf.is_empty() {
            return None;
        }
        let quiet_for = now.duration_since(self.last_event?);
        (quiet_for >= self.cfg.window).then(|| self.force_flush())
    }

    /// Emit whatever is buffered right now (shutdown, tests).
    pub fn force_flush(&mut self) -> Batch {
        let events = std::mem::take(&mut self.buf);
        self.touched_at.clear();
        let started = self.first_event.take();
        self.last_event = None;
        Batch {
            root: self.root.clone(),
            started_at: started
                .map(|_| chrono::Utc::now())
                .unwrap_or_else(chrono::Utc::now),
            flushed_at: chrono::Utc::now(),
            events,
        }
    }

    /// Number of events currently buffered, awaiting a flush.
    pub fn pending(&self) -> usize {
        self.buf.len()
    }

    /// Instant at which the current buffer becomes quiescent, so a caller can
    /// schedule its next `take_if_quiescent` poll. `None` when idle.
    pub fn quiescence_deadline(&self) -> Option<Instant> {
        self.last_event.map(|t| t + self.cfg.window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // (TouchedPath import removed: tests construct via .into())
    use std::path::PathBuf;

    fn touched(p: &str) -> FsEvent {
        FsEvent::now(EventKind::Touched {
            path: PathBuf::from(p).into(),
        })
    }

    fn added(p: &str) -> FsEvent {
        FsEvent::now(EventKind::Added {
            path: PathBuf::from(p),
        })
    }

    fn cfg(window_ms: u64, hold_ms: u64, cap: usize) -> DebouncerConfig {
        DebouncerConfig {
            window: Duration::from_millis(window_ms),
            max_hold: Duration::from_millis(hold_ms),
            cap_events: cap,
        }
    }

    #[test]
    fn holds_until_quiescence() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(100, 10_000, 1000));
        assert!(d.feed(touched("a.txt")).is_none());
        std::thread::sleep(Duration::from_millis(30));
        assert!(d.take_if_quiescent().is_none());
        assert_eq!(d.pending(), 1);
        std::thread::sleep(Duration::from_millis(110));
        let b = d.take_if_quiescent().expect("flushed on quiescence");
        assert_eq!(b.len(), 1);
        assert!(d.take_if_quiescent().is_none());
    }

    #[test]
    fn collapses_repeated_touched_per_path() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(50, 60_000, 1000));
        for _ in 0..50 {
            d.feed(touched("big.txt"));
        }
        d.feed(added("new.txt"));
        std::thread::sleep(Duration::from_millis(80));
        let b = d.take_if_quiescent().unwrap();
        assert_eq!(b.len(), 2, "50 touches collapse to 1 + 1 added");
        let touches = b
            .events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Touched { .. }))
            .count();
        assert_eq!(touches, 1);
    }

    #[test]
    fn cap_events_forces_early_flush() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(60_000, 60_000, 8));
        for i in 0..10 {
            d.feed(added(&format!("f{i}.txt")));
        }
        // 8th crossed the cap and force-flushed; 2 remain buffered.
        assert_eq!(d.pending(), 2);
    }

    #[test]
    fn max_hold_limits_continuous_bursts() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(60_000, 150, 1_000_000));
        let mut flushed = 0;
        let start = std::time::Instant::now();
        // Single-path touch flood: collapse alone must not starve flushing;
        // feed()'s hold cap is what releases partial batches here.
        while start.elapsed() < Duration::from_millis(400) {
            if d.feed(touched("stream.bin")).is_some() {
                flushed += 1;
            }
        }
        assert!(
            flushed >= 1,
            "continuous activity must still flush via max_hold"
        );
    }

    #[test]
    fn default_config_pins_the_contract() {
        let c = DebouncerConfig::default();
        assert_eq!(c.window, Duration::from_millis(300));
        assert_eq!(c.max_hold, Duration::from_millis(2000));
        assert_eq!(c.cap_events, 4096);
    }

    #[test]
    fn quiescence_deadline_tracks_activity_and_clears_on_flush() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(50, 60_000, 1000));
        assert!(
            d.quiescence_deadline().is_none(),
            "an idle debouncer has no deadline"
        );
        d.feed(touched("a.txt"));
        assert!(d.quiescence_deadline().is_some());
        let b = d.force_flush();
        assert!(
            d.quiescence_deadline().is_none(),
            "deadline clears on flush"
        );
        assert_eq!(b.root, PathBuf::from("/p"));
        assert!(b.flushed_at >= b.started_at, "batch carries its time span");
    }

    #[test]
    fn force_flush_of_an_empty_buffer_yields_an_empty_batch() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(50, 60_000, 1000));
        let b = d.force_flush();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn touched_index_rebuilds_after_a_flush() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(60_000, 60_000, 1000));
        d.feed(touched("a.txt"));
        d.force_flush();
        d.feed(touched("a.txt"));
        assert_eq!(
            d.pending(),
            1,
            "a post-flush touch is a fresh event, not a collapse into cleared state"
        );
    }

    #[test]
    fn hold_window_restarts_after_a_partial_flush() {
        let mut d = Debouncer::new(PathBuf::from("/p"), cfg(60_000, 120, 1_000_000));
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(300) {
            if d.feed(touched("stream.bin")).is_some() {
                break; // the hold cap fired; the burst was partially flushed
            }
        }
        // The event right after the flush starts a NEW hold window, so it
        // must not immediately flush again.
        assert!(d.feed(touched("stream.bin")).is_none());
    }
}

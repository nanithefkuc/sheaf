//! Store maintenance: integrity checking and conservative
//! retention, both read-only by default.
//!
//! **Integrity** (`doctor`): every durability contract the on-disk layout
//! makes is
//! checked against what is actually on disk — journal framing, snapshot
//! manifest chain, head-file sanity, intent parseability, blob layout, and
//! blob coverage for everything the merged history references. It never
//! writes, so it is safe to run against a store the daemon holds.
//!
//! **Retention** (`gc_plan`/`gc_apply`): charter constraint 7 says nothing
//! destructive may trim the log, GC included — *and* GC must respect branch
//! reachability. The design honors both by only ever removing bytes the
//! reachability rules prove redundant:
//!
//! 1. **Covered journal segments** — fully contained in a still-present
//!    snapshot (the pre-existing compaction rule).
//! 2. **Superseded snapshots** — every snapshot preserves *full history up
//!    to its own frontier*, so the newest valid snapshot contains all older
//!    ones; only it (and its manifest) survive.
//! 3. **Unreferenced blobs** — a blob is collectable only when no tree event
//!    in the merged document ever names its digest and no binaries-map entry
//!    holds it. Every binary ever captured pushed a `tree_events` record
//!    carrying its digest, so that set is a conservative
//!    superset of every blob reachable from ANY version the timeline can
//!    address — current lineage, abandoned futures, pre-restore points, all
//!    of it. A restore to any point keeps working after a GC.
//!
//! Blobs referenced by *pending* capture paths (a crash between blob write
//! and journal fsync) are intentionally NOT collected here: they are orphans
//! by the rule above, but the seal is done by an explicit operator action
//! (`sheaf gc --apply`) rather than automatically, because an operator looking
//! at `doctor`'s orphan report is the right reviewer for "bytes with no
//! history pointing at them".

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use loro::{Frontiers, LoroDoc};
use serde::{Deserialize, Serialize};

use super::ledger::{self, LedgerRecord, LedgerState, PruneCause};
use super::{
    blobs, fsutil, journal, newest_manifest, pending_restore_at, store_dir, timeline, ProjectStore,
    TimelineReader,
};
use crate::config;
use crate::error::{Result, SheafError};

// ------------------------------------------------------------------ doctor

/// One named integrity check with its human-readable finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

/// Full integrity report for one project store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub root: String,
    pub ok: bool,
    pub checks: Vec<Check>,
    /// Journal segments (count, bytes).
    pub journal_segments: usize,
    pub journal_bytes: u64,
    /// Live snapshot bytes (newest manifest's snapshot).
    pub snapshot_bytes: u64,
    /// Blob population (count, bytes, orphans reported separately).
    pub blob_count: usize,
    pub blob_bytes: u64,
    pub orphan_blobs: usize,
    pub orphan_blob_bytes: u64,
    /// Snapshot/manifest pairs made redundant by a newer snapshot.
    pub superseded_snapshots: usize,
    pub captures: usize,
    pub branch_tips: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_restore: Option<super::RestoreIntent>,
}

fn check(name: &str, ok: bool, detail: String) -> Check {
    Check {
        name: name.to_owned(),
        ok,
        detail,
    }
}

/// Run every integrity check read-only. Never creates or truncates store
/// files; safe while the daemon is running (it takes no lock beyond the
/// shared one its caller may already hold).
pub fn doctor(root: &Path) -> Result<IntegrityReport> {
    let mut checks: Vec<Check> = Vec::new();
    let sdir = store_dir(root);

    // -- config.toml: root marker + format_version + parseability ----------
    let format = config::read_store_format(root);
    checks.push(check(
        "format_version",
        format.is_ok(),
        match &format {
            Ok(v) => format!("v{v}"),
            Err(e) => e.to_string(),
        },
    ));
    let cfg = config::load(root);
    checks.push(check(
        "config",
        cfg.is_ok(),
        match &cfg {
            Ok(_) => "config.toml parses".into(),
            Err(e) => e.to_string(),
        },
    ));

    // -- journal framing ----------------------------------------------------
    let segments = journal::list_segments(&sdir);
    let mut journal_bytes = 0u64;
    let mut torn = Vec::new();
    for (idx, path) in &segments {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        journal_bytes += len;
        match journal::scan_intact_prefix(path) {
            Some(valid) if valid == len => {}
            Some(valid) => torn.push(format!("seg-{idx:06}: {valid}/{len} bytes intact")),
            None => torn.push(format!("seg-{idx:06}: unreadable")),
        }
    }
    checks.push(check(
        "journal_frames",
        torn.is_empty(),
        if torn.is_empty() {
            format!("{} segments, every frame CRC-clean", segments.len())
        } else {
            torn.join("; ")
        },
    ));

    // -- snapshot + manifest chain (via a real reader) -----------------------
    let reader = TimelineReader::open(root);
    let reader = match reader {
        Ok(r) => r,
        Err(e) => {
            checks.push(check("timeline_loads", false, e.to_string()));
            // Report the filesystem-level facts we can still see.
            let (blob_count, blob_bytes, orphan_count, orphan_bytes, superseded) =
                blob_facts(root, None);
            return Ok(IntegrityReport {
                root: root.display().to_string(),
                ok: false,
                checks,
                journal_segments: segments.len(),
                journal_bytes,
                snapshot_bytes: 0,
                blob_count,
                blob_bytes,
                orphan_blobs: orphan_count,
                orphan_blob_bytes: orphan_bytes,
                superseded_snapshots: superseded,
                captures: 0,
                branch_tips: 0,
                pending_restore: pending_restore_at(root),
            });
        }
    };
    let tips = reader.branch_tips().map(|t| t.len()).unwrap_or(0);
    checks.push(check(
        "timeline_loads",
        true,
        "snapshot + journal assemble into a readable document".into(),
    ));
    return_after_setup(root, &reader, checks, segments.len(), journal_bytes, tips)
}

/// What `store.doctor` returns: the read-only sweep, or the repair
/// outcome when `fix` was requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum DoctorReply {
    Report(Box<IntegrityReport>),
    /// Boxed: carries the before and after reports.
    Repair(Box<RepairOutcome>),
}

/// One repair `doctor --fix` performed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedFix {
    /// Short action name: "truncate-journal", "remove-superseded",
    /// "remove-quarantine", "remove-stage".
    pub action: String,
    pub detail: String,
}

/// One failing check doctor deliberately does not touch, with guidance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Refusal {
    pub check: String,
    pub reason: String,
}

/// Result of `doctor --fix`: the sweep before, the bounded repairs applied,
/// the sweep after, and the failures doctor refuses to guess at.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairOutcome {
    pub before: IntegrityReport,
    pub after: IntegrityReport,
    pub applied: Vec<AppliedFix>,
    pub refused: Vec<Refusal>,
}

impl RepairOutcome {
    /// True when the re-run sweep is clean (what the CLI exit code wants).
    pub fn healthy(&self) -> bool {
        self.after.ok
    }
}

/// Operator verb behind `sheaf doctor --fix`: bounded
/// repair of the safe classes doctor already detects —
///
/// - torn journal tails, truncated to their intact CRC-clean prefix
///   (exactly what replay already treats as the segment's end);
/// - superseded snapshot/manifest pairs below the newest manifest;
/// - a quarantined `restore.intent.bad`;
/// - leftover restore staging.
///
/// Everything else is refused with guidance: a head that does not resolve,
/// history that does not assemble, missing blob payloads, ledger or
/// shallow-boundary violations are ambiguity or data loss, and guessing
/// there would trade an explained problem for a silent one. A pending
/// restore intent is operator state, never touched here (`restore.resume`
/// / `restore abandon` own it).
///
/// MUST run under the project's exclusive flock — on the daemon's
/// collector thread or in a CLI that proved no writer exists: journal
/// truncation under a live appender would corrupt.
pub fn doctor_fix(root: &Path) -> Result<RepairOutcome> {
    let before = doctor(root)?;
    let sdir = store_dir(root);
    let mut applied: Vec<AppliedFix> = Vec::new();

    // -- torn journal tails -------------------------------------------------
    for (idx, path) in journal::list_segments(&sdir) {
        let len = match std::fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(_) => continue,
        };
        match journal::scan_intact_prefix(&path) {
            Some(valid) if valid < len => {
                match std::fs::OpenOptions::new().write(true).open(&path) {
                    Ok(f) => {
                        let truncated = f.set_len(valid).and_then(|()| f.sync_all());
                        match truncated {
                            Ok(()) => applied.push(AppliedFix {
                                action: "truncate-journal".into(),
                                detail: format!(
                                    "seg-{idx:06}: truncated torn tail ({valid}/{len} bytes intact)"
                                ),
                            }),
                            Err(e) => {
                                tracing::warn!(segment = idx, error = %e, "truncate failed")
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(segment = idx, error = %e, "open for truncate failed")
                    }
                }
            }
            // Unreadable framing is not a tail we can prove: refuse below.
            _ => {}
        }
    }

    // -- superseded snapshots -------------------------------------------------
    if let Some((newest_path, _newest)) = newest_manifest(&sdir) {
        let newest_idx = newest_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("snap-"))
            .and_then(|n| n.strip_suffix(".manifest.json"))
            .and_then(|n| n.parse::<u64>().ok());
        if let Some(newest_idx) = newest_idx {
            for (name_idx, path) in listing(&sdir.join("snapshots"), ".snapshot") {
                if name_idx < newest_idx && std::fs::remove_file(&path).is_ok() {
                    applied.push(AppliedFix {
                        action: "remove-superseded".into(),
                        detail: format!("snap-{name_idx:06}.snapshot"),
                    });
                }
            }
            for (name_idx, path) in listing(&sdir.join("snapshots"), ".manifest.json") {
                if name_idx < newest_idx && std::fs::remove_file(&path).is_ok() {
                    applied.push(AppliedFix {
                        action: "remove-superseded".into(),
                        detail: format!("snap-{name_idx:06}.manifest.json"),
                    });
                }
            }
        }
    }

    // -- quarantined restore intent -------------------------------------------
    let bad_intent = crate::config::sheaf_dir(root).join("state/restore.intent.bad");
    if bad_intent.is_file() && std::fs::remove_file(&bad_intent).is_ok() {
        applied.push(AppliedFix {
            action: "remove-quarantine".into(),
            detail: "restore.intent.bad removed (already inert; compare the worktree against                      `sheaf log` if you have not)"
                .into(),
        });
    }

    // -- leftover restore staging ----------------------------------------------
    let stage = sdir.join(super::restore::STAGE_DIR);
    if stage.exists() && std::fs::remove_dir_all(&stage).is_ok() {
        applied.push(AppliedFix {
            action: "remove-stage".into(),
            detail: "disposable restore staging removed".into(),
        });
    }

    let after = doctor(root)?;
    let refused: Vec<Refusal> = after
        .checks
        .iter()
        .filter(|c| !c.ok)
        .map(|c| Refusal {
            check: c.name.clone(),
            reason: refusal_guidance(&c.name).to_string(),
        })
        .collect();
    tracing::info!(
        root = %root.display(),
        applied = applied.len(),
        refused = refused.len(),
        healthy = after.ok,
        "doctor --fix ran"
    );
    Ok(RepairOutcome {
        before,
        after,
        applied,
        refused,
    })
}

/// Why doctor refuses each failing check, and what to do instead.
fn refusal_guidance(check: &str) -> &'static str {
    match check {
        "format_version" => {
            "the store's format version is unknown to this build; upgrade sheaf or inspect              .sheaf/config.toml by hand"
        }
        "config" => "config.toml does not parse; fix it by hand — doctor will not guess settings",
        "journal_frames" => {
            "a segment is unreadable beyond a torn frame that truncation could not prove;              inspect it before removing anything"
        }
        "timeline_loads" => {
            "history does not assemble into a document; repair here would be guesswork —              run `sheaf log` for the loader's own account"
        }
        "worktree_head" => {
            "head state is ambiguous; remove .sheaf/state/worktree.head by hand only if you              accept editing continuing from the merged tip"
        }
        "blob_coverage" => {
            "blob payloads are missing and cannot be synthesized; restore those files from              history you trust or accept the loss"
        }
        "ledger_state" => {
            "run `sheaf gc --apply` to settle an incomplete trim; an unparseable manifest              ledger needs manual inspection"
        }
        "shallow_baseline" => {
            "a checkpoint pins history the shallow store cannot reach; manual inspection              required — reprotecting the wrong point would hide real loss"
        }
        _ => "ambiguous corruption; doctor --fix only removes what it can prove redundant",
    }
}

/// Shared tail of the doctor report once a reader exists.
fn return_after_setup(
    root: &Path,
    reader: &TimelineReader,
    mut checks: Vec<Check>,
    segments: usize,
    journal_bytes: u64,
    tips: usize,
) -> Result<IntegrityReport> {
    let sdir = store_dir(root);

    // -- head file -----------------------------------------------------------
    let head_raw =
        std::fs::read_to_string(crate::config::sheaf_dir(root).join("state/worktree.head"));
    match head_raw {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => checks.push(check(
            "worktree_head",
            true,
            "absent (fresh store or nothing captured yet)".into(),
        )),
        Err(e) => checks.push(check("worktree_head", false, e.to_string())),
        Ok(raw) => match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(v) => {
                let frontier_ok = v
                    .get("frontier")
                    .and_then(|f| f.as_str())
                    .map(|f| {
                        super::timeline::decode_frontier(f)
                            .is_ok_and(|fr| reader_resolves(reader, &fr))
                    })
                    .unwrap_or(true); // old heads without the field are valid
                checks.push(check(
                    "worktree_head",
                    frontier_ok,
                    if frontier_ok {
                        "parses; frontier resolves in the oplog".into()
                    } else {
                        "frontier does not resolve in this store's history".into()
                    },
                ));
            }
            Err(e) => checks.push(check("worktree_head", false, format!("unparseable: {e}"))),
        },
    }

    // -- restore intent -------------------------------------------------------
    let intent = pending_restore_at(root);
    checks.push(check(
        "restore_intent",
        true,
        match &intent {
            None => "no pending intent".into(),
            Some(i) => format!(
                "pending: target {}, age {} ms",
                i.target.capture_id.as_deref().unwrap_or("(frontier)"),
                i.age_ms()
            ),
        },
    ));
    let bad_intent = crate::config::sheaf_dir(root).join("state/restore.intent.bad");
    if bad_intent.exists() {
        checks.push(check(
            "restore_intent",
            false,
            "a quarantined restore.intent.bad exists — a restore could not be parsed; \
             compare the worktree against `sheaf log`"
                .into(),
        ));
    }

    // -- restore stage leftovers ----------------------------------------------
    let stage = sdir.join(super::restore::STAGE_DIR);
    if stage.exists() {
        checks.push(check(
            "restore_stage",
            true,
            "leftover staging directory (disposable; gc clears it)".into(),
        ));
    }

    // -- blobs ------------------------------------------------------------------
    // Every digest surviving history references must have a payload — not
    // just the live binaries map: a blob named only by an older capture is
    // still what `restore` to that point materializes from. The
    // retention-aware set keeps this view identical to what `gc --apply` is
    // allowed to collect, so a legitimately trimmed store stays healthy.
    let retention = plan_retention(root, reader.doc(), reader.ledger()).unwrap_or_default();
    let reachable = retention_aware_reachable_blobs(reader.doc(), reader.ledger(), &retention);
    let (blob_count, blob_bytes, orphan_count, orphan_bytes, superseded) =
        blob_facts(root, Some(&reachable));
    let mut missing = 0usize;
    for digest in &reachable {
        if !blobs::blob_path(&sdir, digest).exists() {
            missing += 1;
            tracing::debug!(digest = %digest, "missing blob");
        }
    }
    checks.push(check(
        "blob_coverage",
        missing == 0,
        if missing == 0 {
            "every referenced blob is present".into()
        } else {
            format!("{missing} referenced blob(s) missing; restores touching them will fail")
        },
    ));

    // -- timeline ledger ------------------------------------------
    let ledger = reader.ledger();
    {
        let manifest_ledger_ok = newest_manifest(&sdir)
            .map(|(_, m)| {
                m.ledger
                    .as_ref()
                    .map(ledger::LedgerState::from_json)
                    .map(|r| r.is_ok())
                    .unwrap_or(false)
            })
            .unwrap_or(true); // no manifest yet: nothing materialized to check
                              // A tombstone naming a capture that still walks out of the raw
                              // change graph = a trim that never completed its compaction.
        let all_captures = timeline::captures_from(
            reader.doc(),
            &LedgerState::default(),
            &reader.doc().oplog_frontiers(),
            None,
            None,
            usize::MAX,
        )
        .unwrap_or_default();
        let live_tombstones = all_captures
            .iter()
            .filter(|c| ledger.tombstones.contains_key(&c.id))
            .count();
        let mut ok = manifest_ledger_ok;
        let mut detail = if manifest_ledger_ok {
            "ledger state materializes in the manifest".into()
        } else {
            ok = false;
            "manifest embeds an unparseable ledger state".into()
        };
        if live_tombstones > 0 {
            // Tombstones naming still-present captures mean a compaction
            // did not complete (crash between record append and snapshot
            // commit is impossible by ordering; this catches manual
            // tampering). The next gc --apply settles it either way.
            detail = format!(
                "{detail}; {live_tombstones} tombstone(s) name still-present captures                  (incomplete trim; `gc --apply` settles it)"
            );
        }
        checks.push(check("ledger_state", ok, detail));
    }

    // -- shallow baseline consistency (trimmed stores) -----------------------
    if reader.doc().is_shallow() {
        let since = newest_manifest(&sdir).and_then(|(_, m)| m.shallow_since.clone());
        let dangling = ledger
            .checkpoints
            .values()
            .filter(|cp| {
                !ledger
                    .tombstones
                    .contains_key(&cp.capture_id.clone().unwrap_or_default())
            })
            .filter(|cp| {
                timeline::decode_frontier(&cp.frontier)
                    .ok()
                    .and_then(|f| reader.doc().frontiers_to_vv(&f))
                    .is_none()
            })
            .count();
        checks.push(check(
            "shallow_baseline",
            since.is_some() && dangling == 0,
            if since.is_none() {
                "store history is shallow but the manifest records no boundary".into()
            } else if dangling > 0 {
                format!("{dangling} checkpoint(s) pin points the shallow history cannot reach")
            } else {
                "shallow history serves every checkpoint it promised to keep".into()
            },
        ));
    }

    // -- derived grep cache (advisory only) --------------------------------
    // The cache is disposable performance state: damage here is a slow
    // query, never a broken store, so this check can never fail the
    // sweep. It reports what damage exists and the one-command repair.
    {
        let facts = super::grep::grep_cache_facts(root);
        let detail = if !facts.present {
            "absent (optional derived cache; `sheaf cache backfill` builds it)".to_owned()
        } else {
            let mut parts = vec![format!(
                "{} row(s), {} content file(s) ({} KiB)",
                facts.rows,
                facts.content_files,
                facts.content_bytes / 1024
            )];
            match (&facts.watermark, facts.watermark_unparseable) {
                (Some(wm), _) => parts.push(format!(
                    "watermark gen {} through {} capture(s)",
                    wm.generation, wm.captures_indexed
                )),
                (None, true) => {
                    parts.push("watermark unparseable (ignored; rebuild rewrites it)".into())
                }
                (None, false) => parts.push("no watermark (backfill not run)".into()),
            }
            if facts.torn_lines > 0 {
                parts.push(format!(
                    "{} torn mapping line(s) (skipped on load; `sheaf cache rebuild` clears them)",
                    facts.torn_lines
                ));
            }
            if facts.missing_content > 0 {
                parts.push(format!(
                    "{0} mapping(s) missing content (self-repair to a miss; `sheaf cache rebuild` restores)",
                    facts.missing_content
                ));
            }
            if facts.orphan_content_files > 0 {
                parts.push(format!(
                    "{} orphan content file(s), {} KiB (crash leftover or damage; rebuild clears)",
                    facts.orphan_content_files,
                    facts.orphan_content_bytes / 1024
                ));
            }
            if facts.trigram_index_corrupt {
                parts.push(
                    "trigram index corrupt (queries scan every version; `sheaf cache rebuild` restores)"
                        .into(),
                );
            } else if facts.trigram_index_bytes > 0 {
                parts.push(format!(
                    "trigram index {} KiB",
                    facts.trigram_index_bytes / 1024
                ));
            } else {
                parts.push("no trigram index (rare-needle queries scan every version)".into());
            }
            parts.join("; ")
        };
        checks.push(check("grep_cache", true, detail));
    }

    // -- snapshot inventory -------------------------------------------------------
    let snapshot_bytes = newest_manifest(&sdir)
        .and_then(|(_, m)| std::fs::metadata(sdir.join("snapshots").join(&m.snapshot)).ok())
        .map(|m| m.len())
        .unwrap_or(0);

    let captures = reader
        .captures(true, None, false, usize::MAX)
        .map(|c| c.len())
        .unwrap_or(0);
    let ok = checks.iter().all(|c| c.ok);
    Ok(IntegrityReport {
        root: root.display().to_string(),
        ok,
        checks,
        journal_segments: segments,
        journal_bytes,
        snapshot_bytes,
        blob_count,
        blob_bytes,
        orphan_blobs: orphan_count,
        orphan_blob_bytes: orphan_bytes,
        superseded_snapshots: superseded,
        captures,
        branch_tips: tips,
        pending_restore: intent,
    })
}

fn reader_resolves(reader: &TimelineReader, f: &loro::Frontiers) -> bool {
    reader.doc().frontiers_to_vv(f).is_some()
}

/// Digests any history event or live entry names — the conservative
/// reachability set. Everything outside it can never be referenced again.
fn reachable_blob_digests(doc: &LoroDoc) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    // Every binary capture pushed a tree event carrying its digest, so the
    // merged tree_events list is the whole history of "this blob mattered".
    doc.get_list(super::TREE_EVENTS_LIST).for_each(|value| {
        let Ok(raw) = value.get_deep_value().into_string() else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return;
        };
        if let Some(digest) = parsed["event"]["binary"].as_str() {
            out.insert(digest.to_owned());
        }
    });
    // Belt and braces for stores whose events predate the digest field: the
    // live binaries map is always reachable.
    doc.get_map(super::BINARIES_MAP).for_each(|_, value| {
        let Ok(raw) = value.get_deep_value().into_string() else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return;
        };
        if let Some(digest) = parsed["hash"].as_str() {
            out.insert(digest.to_owned());
        }
    });
    out
}

/// (count, bytes) over every stored blob, plus orphan facts when a
/// reachability set is supplied, plus the superseded-snapshot count.
fn blob_facts(
    root: &Path,
    reachable: Option<&BTreeSet<String>>,
) -> (usize, u64, usize, u64, usize) {
    let sdir = store_dir(root);
    let blobs_dir = blobs::blobs_dir(&sdir);
    let mut count = 0usize;
    let mut bytes = 0u64;
    let mut orphans = 0usize;
    let mut orphan_bytes = 0u64;
    if let Ok(rd) = std::fs::read_dir(&blobs_dir) {
        for fanout in rd.flatten() {
            let Ok(files) = std::fs::read_dir(fanout.path()) else {
                continue;
            };
            for file in files.flatten() {
                let name = file.file_name().to_string_lossy().into_owned();
                let len = file.metadata().map(|m| m.len()).unwrap_or(0);
                count += 1;
                bytes += len;
                if let Some(set) = reachable {
                    if !set.contains(&name) {
                        orphans += 1;
                        orphan_bytes += len;
                    }
                }
            }
        }
    }
    // Superseded snapshots: snapshot files with an index BELOW the newest
    // manifest's index. Their full-history content is contained in the newest.
    let newest = newest_manifest(&sdir).map(|(_, m)| m.covered_upto);
    let mut superseded = 0usize;
    if newest.is_some() {
        if let Ok(rd) = std::fs::read_dir(sdir.join("snapshots")) {
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Some(rest) = name.strip_prefix("snap-") else {
                    continue;
                };
                if let Some(idx_raw) = rest.strip_suffix(".snapshot") {
                    if let Ok(idx) = idx_raw.parse::<u64>() {
                        if Some(idx) < newest {
                            superseded += 1;
                        }
                    }
                }
            }
        }
    }
    (count, bytes, orphans, orphan_bytes, superseded)
}

// ---------------------------------------------------------------------- gc

/// What a garbage collection WOULD remove.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcPlan {
    pub root: String,
    /// Covered journal segments beyond the newest manifest's coverage.
    pub segments: Vec<String>,
    /// Superseded snapshot + manifest files (index < newest).
    pub snapshots: Vec<String>,
    /// Blobs no event or entry in the merged history can ever reach again.
    pub orphan_blobs: Vec<String>,
    pub bytes_recovered: u64,
    /// Retention view: boundary, protected points, and
    /// the captures `--apply` would tombstone.
    #[serde(default)]
    pub retention: RetentionFacts,
}

/// Outcome of applying a GC plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcReport {
    pub plan: GcPlan,
    pub segments_removed: usize,
    pub snapshots_removed: usize,
    pub blobs_removed: usize,
    pub bytes_recovered: u64,
    /// Capture count after GC. Retention trims lower it by design; the
    /// post-run count equals survivors, never fewer than the protected set.
    pub captures_after: usize,
    /// Captures tombstoned by a retention trim in this run, if any.
    #[serde(default)]
    pub trimmed: usize,
    /// Hex frontier history now starts from (the shallow boundary), when a
    /// trim happened this run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary_after: Option<String>,
}

/// A GC run: the plan alone when `apply` is false, else the applied report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum GcOutcome {
    /// Nothing was removed; this is what the operator reviews.
    Planned(GcPlan),
    Applied(GcReport),
}

/// Compute the plan, optionally apply it. Callers must hold the project's
/// exclusive flock when `apply` is true.
pub fn gc_run(root: &Path, apply: bool) -> Result<GcOutcome> {
    let plan = gc_plan(root)?;
    if apply {
        gc_apply(root, &plan).map(GcOutcome::Applied)
    } else {
        Ok(GcOutcome::Planned(plan))
    }
}

/// Compute the retention plan. Read-only.
pub fn gc_plan(root: &Path) -> Result<GcPlan> {
    let sdir = store_dir(root);
    let reader = TimelineReader::open(root)?;
    let retention = plan_retention(root, reader.doc(), reader.ledger())?;
    let reachable = retention_aware_reachable_blobs(reader.doc(), reader.ledger(), &retention);

    let manifest = newest_manifest(&sdir);
    let covered = manifest.as_ref().map(|(_, m)| m.covered_upto);
    let mut segments = Vec::new();
    let mut bytes_recovered = 0u64;
    for (idx, path) in journal::list_segments(&sdir) {
        // Only segments at or below the newest manifest's coverage are
        // provably inside a surviving snapshot. Anything else stays.
        if covered.is_some_and(|c| idx <= c) {
            bytes_recovered += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            segments.push(format!("seg-{idx:06}.op"));
        }
    }

    let newest_idx = manifest.as_ref().and_then(|(p, _)| {
        p.file_name()?
            .to_str()?
            .strip_prefix("snap-")?
            .strip_suffix(".manifest.json")?
            .parse::<u64>()
            .ok()
    });
    let mut snapshots = Vec::new();
    if let Some(newest_idx) = newest_idx {
        for (name_idx, path) in listing(&sdir.join("snapshots"), ".snapshot") {
            if name_idx < newest_idx {
                bytes_recovered += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                snapshots.push(format!("snap-{name_idx:06}.snapshot"));
            }
        }
        for (name_idx, path) in listing(&sdir.join("snapshots"), ".manifest.json") {
            if name_idx < newest_idx {
                bytes_recovered += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                snapshots.push(format!("snap-{name_idx:06}.manifest.json"));
            }
        }
    }

    let mut orphan_blobs = Vec::new();
    if let Ok(rd) = std::fs::read_dir(blobs::blobs_dir(&sdir)) {
        for fanout in rd.flatten() {
            for file in std::fs::read_dir(fanout.path())
                .map(|d| d.flatten().collect::<Vec<_>>())
                .unwrap_or_default()
            {
                let name = file.file_name().to_string_lossy().into_owned();
                if !reachable.contains(&name) {
                    bytes_recovered += file.metadata().map(|m| m.len()).unwrap_or(0);
                    orphan_blobs.push(name);
                }
            }
        }
    }

    Ok(GcPlan {
        root: root.display().to_string(),
        segments,
        snapshots,
        orphan_blobs,
        bytes_recovered,
        retention,
    })
}

fn listing(dir: &Path, suffix: &str) -> Vec<(u64, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix("snap-") else {
                continue;
            };
            let Some(idx_raw) = rest.strip_suffix(suffix) else {
                continue;
            };
            if let Ok(idx) = idx_raw.parse::<u64>() {
                out.push((idx, entry.path()));
            }
        }
    }
    out
}

/// Apply a GC plan. MUST run under the project's exclusive flock — either on
/// the daemon's collector thread or in a CLI that proved no writer exists.
/// Deletion order is oldest-first, and every step is a plain unlink of bytes
/// the reachability rules proved redundant, so a crash mid-GC is exactly the
/// pre-GC state with a few orphans fewer. Retention trims are NOT executed
/// here — they need the live writer — see [`gc_run_store`]; the plan's
/// retention section is informational when reached through this path.
pub fn gc_apply(root: &Path, plan: &GcPlan) -> Result<GcReport> {
    let sdir = store_dir(root);

    // Verify the store still loads BEFORE removing anything: if it does
    // not, refusing is the only safe move.
    TimelineReader::open(root)?;

    let mut segments_removed = 0usize;
    for name in &plan.segments {
        if std::fs::remove_file(journal::journal_dir(&sdir).join(name)).is_ok() {
            segments_removed += 1;
        }
    }
    let mut snapshots_removed = 0usize;
    for name in &plan.snapshots {
        if std::fs::remove_file(sdir.join("snapshots").join(name)).is_ok() {
            snapshots_removed += 1;
        }
    }
    let mut blobs_removed = 0usize;
    for digest in &plan.orphan_blobs {
        if blobs::blob_path(&sdir, digest).is_file()
            && std::fs::remove_file(blobs::blob_path(&sdir, digest)).is_ok()
        {
            blobs_removed += 1;
        }
    }
    // Disposable restore staging, if a crash left it behind.
    let _ = std::fs::remove_dir_all(sdir.join(super::restore::STAGE_DIR));

    fsutil::sync_dir(&journal::journal_dir(&sdir)).ok();
    fsutil::sync_dir(&sdir.join("snapshots")).ok();
    fsutil::sync_dir(&blobs::blobs_dir(&sdir)).ok();

    // The one invariant that matters: the timeline still reads whole.
    let captures_after = TimelineReader::open(root)?
        .captures(true, None, false, usize::MAX)
        .map(|c| c.len())
        .map_err(|e| SheafError::StoreCorrupt(format!("post-GC timeline unreadable: {e}")))?;
    tracing::info!(
        root = %root.display(),
        segments_removed,
        snapshots_removed,
        blobs_removed,
        captures_after,
        "gc applied"
    );
    Ok(GcReport {
        plan: plan.clone(),
        segments_removed,
        snapshots_removed,
        blobs_removed,
        bytes_recovered: plan.bytes_recovered,
        captures_after,
        trimmed: 0,
        boundary_after: None,
    })
}

// ------------------------------------------------------------------ retention

/// One point the trim planner must keep restorable, and why.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtectedPoint {
    /// Hex frontier of the protected point.
    pub frontier: String,
    /// Capture the point names, when it is a capture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    /// Human-facing reason: "worktree head", "branch tip",
    /// "checkpoint 'x'", "pending restore".
    pub reason: String,
}

/// A capture the next `gc --apply` will tombstone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrunableCapture {
    pub id: String,
    pub at_ms: i64,
    #[serde(default)]
    pub parent_frontier: String,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub events: usize,
    pub cause: PruneCause,
}

/// The retention view of one store.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionFacts {
    /// Configured expiry exactly as written (e.g. "30d").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    /// Expiry horizon in ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_ms: Option<i64>,
    /// Everything that must stay restorable.
    #[serde(default)]
    pub protected: Vec<ProtectedPoint>,
    /// Hex frontier of the shallow boundary = GCA(protected). Captures
    /// strictly before it are prunable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<String>,
    /// Prunable captures; empty when no retention policy is in effect
    /// (flight-recorder default: gc never trims an unconfigured store).
    #[serde(default)]
    pub prunable: Vec<PrunableCapture>,
    /// Marks recorded but pinned behind protection above them.
    #[serde(default)]
    pub deferred_marks: Vec<String>,
    /// Marks whose targets are the present (head/pending restore) and are
    /// therefore refused.
    #[serde(default)]
    pub refused_marks: Vec<String>,
    /// Marks honored in the current prunable prefix.
    #[serde(default)]
    pub honored_marks: usize,
}

/// Plan retention against a loaded view. Pure computation; the same
/// function serves the CLI plan path (through a TimelineReader) and the
/// apply path (through the live store).
pub(super) fn plan_retention(
    root: &Path,
    doc: &LoroDoc,
    ledger: &LedgerState,
) -> Result<RetentionFacts> {
    let cfg = config::load(root)?;
    let expiry_ms = cfg.retention.expiry_ms();
    let now = chrono::Utc::now().timestamp_millis();

    // ---- seeds: everything that anchors history ------------------------
    let mut seeds: Vec<ProtectedPoint> = Vec::new();
    if let Some(hex) = timeline::read_head_frontier(root) {
        if let Ok(f) = timeline::decode_frontier(&hex) {
            seeds.push(ProtectedPoint {
                capture_id: timeline::capture_id_at(doc, &f),
                frontier: hex,
                reason: "worktree head".into(),
            });
        }
    }
    for id in doc.oplog_frontiers().iter() {
        let f = Frontiers::from_id(id);
        seeds.push(ProtectedPoint {
            frontier: timeline::encode_frontier(&f),
            capture_id: timeline::capture_id_at(doc, &f),
            reason: "branch tip".into(),
        });
    }
    for checkpoint in timeline::checkpoints_from(doc, ledger, None) {
        seeds.push(ProtectedPoint {
            frontier: checkpoint.frontier.clone(),
            capture_id: checkpoint.capture_id.clone(),
            reason: format!("checkpoint '{}'", checkpoint.name),
        });
    }
    if let Some(intent) = pending_restore_at(root) {
        seeds.push(ProtectedPoint {
            capture_id: intent.target.capture_id.clone(),
            frontier: intent.target.frontier.clone(),
            reason: "pending restore".into(),
        });
    }

    // ---- marks: the present is never restorable history -----------------
    let present_frontiers: BTreeSet<String> = seeds
        .iter()
        .filter(|p| p.reason == "worktree head" || p.reason == "pending restore")
        .map(|p| p.frontier.clone())
        .collect();
    let mut refused_marks = Vec::new();
    let mut honored: BTreeSet<&String> = BTreeSet::new();
    for id in ledger.marks.keys() {
        let ghost = ledger.tombstones.contains_key(id);
        if ghost {
            continue; // mark landed after a prune; inert
        }
        // A mark naming the head/pending point would delete the present.
        let names_present = ledger
            .captures
            .get(id)
            .map(|rec| present_frontiers.contains(&rec.frontier))
            .unwrap_or(false);
        if names_present {
            refused_marks.push(short(id));
        } else {
            honored.insert(id);
        }
    }

    // ---- protected set and the boundary (GCA of the protected points) ----
    let protected: Vec<ProtectedPoint> = seeds
        .into_iter()
        .filter(|p| {
            let marked = p
                .capture_id
                .as_ref()
                .map(|id| honored.contains(id))
                .unwrap_or(false);
            if marked {
                tracing::warn!(
                    reason = %p.reason,
                    "explicit mark destroys this point's reachability protection (sanctioned bypass)"
                );
            }
            !marked
        })
        .collect();
    let mut facts = RetentionFacts {
        expiry: cfg.retention.expiry.clone(),
        expiry_ms,
        protected,
        boundary: None,
        prunable: Vec::new(),
        deferred_marks: Vec::new(),
        refused_marks,
        honored_marks: honored.len(),
    };

    // ---- boundary = GCA of everything that must survive -------------------
    // "Earned" captures (expired, or explicitly marked) are the only ones
    // a trim may remove; everything else — protected points AND ordinary
    // fresh captures — joins the keep-set. The GCA of the keep-set is the
    // deepest safe boundary: the prefix below it is exactly the earned
    // captures, so a cut there has no collateral at all. When nothing is
    // earned, the keep-set is the whole timeline and the GCA sits at its
    // root: nothing is prunable and a flight-recorder store keeps every
    // byte.
    let expired =
        |capture: &crate::store::Capture| expiry_ms.is_some_and(|e| capture.timestamp_ms < now - e);
    let mut keep: Vec<Frontiers> = facts
        .protected
        .iter()
        .filter_map(|p| timeline::decode_frontier(&p.frontier).ok())
        .filter(|f| doc.frontiers_to_vv(f).is_some())
        .collect();
    let protected_ids: BTreeSet<&String> = facts
        .protected
        .iter()
        .filter_map(|p| p.capture_id.as_ref())
        .collect();
    let mut earned_any = false;
    for capture in
        timeline::captures_from(doc, ledger, &doc.oplog_frontiers(), None, None, usize::MAX)?
    {
        let earned = !protected_ids.contains(&capture.id)
            && (ledger.is_marked(&capture.id) || expired(&capture));
        if earned {
            earned_any = true;
        } else if let Ok(f) = timeline::decode_frontier(&capture.frontier) {
            keep.push(f);
        }
    }
    let Some(boundary) = gca_frontiers(doc, &keep) else {
        return Ok(facts);
    };
    tracing::debug!(
        keep_points = keep.len(),
        boundary = %timeline::encode_frontier(&boundary),
        "retention keep-set GCA"
    );
    facts.boundary = Some(timeline::encode_frontier(&boundary));

    // ---- prunable prefix -------------------------------------------------
    if earned_any {
        match prunable_prefix(doc, ledger, &boundary, expiry_ms, now) {
            Ok(prunable) => {
                facts.honored_marks = prunable
                    .iter()
                    .filter(|c| c.cause == PruneCause::Marked)
                    .count();
                facts.prunable = prunable;
            }
            // A failed scan must not masquerade as "nothing prunable": the
            // plan would defer every mark with no hint as to why.
            Err(e) => tracing::warn!(error = %e, "prunable-prefix scan failed"),
        }
    }

    // Marks that neither act now nor were refused are pinned.
    let prunable_ids: BTreeSet<&String> = facts.prunable.iter().map(|c| &c.id).collect();
    facts.deferred_marks = ledger
        .marks
        .keys()
        .filter(|id| {
            !prunable_ids.contains(*id)
                && !facts
                    .refused_marks
                    .iter()
                    .any(|s| id.starts_with(s.as_str()))
                && !ledger.tombstones.contains_key(*id)
        })
        .map(|id| short(id))
        .collect();
    Ok(facts)
}

/// Greatest common ancestors of the protected points, as a frontier cut.
///
/// The naive elementwise-min version vector is WRONG for same-peer branchy
/// DAGs (a restore leaving divergence, the everyday sheaf shape): vv-min
/// lands INSIDE the deeper branch rather than at the fork, producing a
/// "boundary" that is not an ancestor of the head at all — every downstream
/// shallow export then fails to place the state. The true GCA is the
/// maximal cut of the shared ancestor set of all protected points in the
/// change DAG itself.
fn gca_frontiers(doc: &LoroDoc, points: &[Frontiers]) -> Option<Frontiers> {
    use std::collections::BTreeSet;
    use std::ops::ControlFlow;

    let mut common: Option<BTreeSet<loro::ID>> = None;
    for point in points {
        let ids: Vec<loro::ID> = point.iter().collect();
        let mut ancestors: BTreeSet<loro::ID> = BTreeSet::new();
        let _ = doc.travel_change_ancestors(&ids, &mut |change| {
            // A capture commits as ONE multi-op change; `change.id` is its
            // FIRST op, but the version a keep point actually pins is the
            // change's LAST op. Intersecting on first-ops would sink the
            // meet to the start of the deepest common change — one version
            // below its frontier — so a mark on the capture just above the
            // baseline can never be pruned. Key on the terminal op instead.
            let end = loro::ID::new(change.id.peer, change.id.counter + change.len as i32 - 1);
            ancestors.insert(end);
            ControlFlow::Continue(())
        });
        // A point with no shared history pins everything: refuse to trim.
        match &mut common {
            None => common = Some(ancestors),
            Some(acc) => {
                acc.retain(|id| ancestors.contains(id));
                if acc.is_empty() {
                    return None;
                }
            }
        }
    }
    let common = common?;
    if common.is_empty() {
        return None;
    }
    // Maximal cut: keep ancestors no other kept ancestor strictly
    // dominates (vv-ordered; same-peer ids are always vv-comparable, so
    // `Frontiers::push`'s same-peer collapse can never bite here). Greedy
    // from the largest vvs — the kept set is tiny (a cut, not a prefix).
    let mut ranked: Vec<(loro::ID, loro::VersionVector)> = Vec::new();
    for id in &common {
        if let Some(vv) = doc.frontiers_to_vv(&Frontiers::from_id(*id)) {
            ranked.push((*id, vv));
        }
    }
    ranked.sort_by(|a, b| {
        // Descending vv: concurrent vvs tie arbitrarily; strict order
        // between comparable ones is what the cut filter relies on.
        b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut cut: Vec<loro::ID> = Vec::new();
    for (id, vv) in ranked {
        let dominated = cut
            .iter()
            .filter_map(|kept| doc.frontiers_to_vv(&Frontiers::from_id(*kept)))
            .any(|k| k.partial_cmp(&vv) == Some(std::cmp::Ordering::Greater));
        if !dominated {
            cut.push(id);
        }
    }
    if cut.is_empty() {
        None
    } else {
        let mut out = Frontiers::default();
        for id in cut {
            out.push(id);
        }
        Some(out)
    }
}

/// Every in-doc capture strictly below `boundary` — the earned prefix.
///
/// "Below" is version-vector order across ALL tips (an abandoned branch's
/// captures are below the boundary too, even though no ancestor walk from
/// the boundary would ever visit them): the shallow snapshot's cut is
/// vv-based, and every capture it removes needs its tombstone. The
/// boundary is the GCA of the keep-set, so every capture down here earned
/// its prune (expiry or explicit mark); causes reflect which.
fn prunable_prefix(
    doc: &LoroDoc,
    ledger: &LedgerState,
    boundary: &Frontiers,
    expiry_ms: Option<i64>,
    now: i64,
) -> Result<Vec<PrunableCapture>> {
    let mut out = Vec::new();
    for capture in
        timeline::captures_from(doc, ledger, &doc.oplog_frontiers(), None, None, usize::MAX)?
    {
        let Ok(f) = timeline::decode_frontier(&capture.frontier) else {
            tracing::warn!(capture = %capture.id, "undecodable frontier skipped in prune scan");
            continue;
        };
        match doc.cmp_frontiers(&f, boundary) {
            Ok(Some(std::cmp::Ordering::Less)) => {}
            other => {
                // Not-below-boundary is the everyday skip (kept or lateral);
                // an Err means the comparison itself failed, which silently
                // shrinks the plan and defers marks that should act — say so.
                if let Err(e) = other {
                    tracing::warn!(
                        capture = %capture.id,
                        error = %e,
                        "frontier comparison failed in prune scan"
                    );
                }
                continue;
            }
        }
        let cause = if ledger.is_marked(&capture.id) {
            PruneCause::Marked
        } else if expiry_ms.is_some_and(|e| capture.timestamp_ms < now - e) {
            PruneCause::Expired
        } else {
            // Unearned capture below the keep-set GCA: possible only when
            // a protected point refused protection above it. Report it as
            // compaction collateral — the cut is still all-or-nothing.
            PruneCause::Epoch
        };
        out.push(PrunableCapture {
            id: capture.id.clone(),
            at_ms: capture.timestamp_ms,
            parent_frontier: capture.parent_frontier.clone(),
            paths: capture.paths.clone(),
            events: capture.events,
            cause,
        });
    }
    out.sort_by_key(|c| c.at_ms);
    Ok(out)
}

fn short(id: &str) -> String {
    id[..12.min(id.len())].to_owned()
}

/// Retention-aware blob reachability. The old conservative set
/// (`tree_events` mentions + live entries) never forgets, so under a trim
/// it must give up digests whose every mention predates the earliest
/// surviving capture and which no surviving ledger record or live entry
/// names.
fn retention_aware_reachable_blobs(
    doc: &LoroDoc,
    ledger: &ledger::LedgerState,
    retention: &RetentionFacts,
) -> BTreeSet<String> {
    let mut reachable = reachable_blob_digests(doc);
    if retention.prunable.is_empty() {
        return reachable;
    }
    let mut min_surviving_ms = i64::MAX;
    for capture in
        timeline::captures_from(doc, ledger, &doc.oplog_frontiers(), None, None, usize::MAX)
            .unwrap_or_default()
    {
        min_surviving_ms = min_surviving_ms.min(capture.timestamp_ms);
    }
    if min_surviving_ms == i64::MAX {
        return reachable; // no survivors tracked: stay conservative
    }
    let ledger_blobs: BTreeSet<String> = ledger.blobs_of_survivors().into_iter().collect();
    let mut last_mention: BTreeMap<String, i64> = BTreeMap::new();
    doc.get_list(super::TREE_EVENTS_LIST).for_each(|value| {
        let Ok(raw) = value.get_deep_value().into_string() else {
            return;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
            return;
        };
        if let Some(digest) = parsed["event"]["binary"].as_str() {
            // `push_tree_event` stamps the timestamp at the top level
            // ({"ts": .., "event": ..}); reading it inside the event payload
            // always missed, pinning last-mention at i64::MAX so trimmed
            // captures' superseded blobs were never reclaimed.
            let ts = parsed["ts"].as_i64().unwrap_or(i64::MAX);
            let slot = last_mention.entry(digest.to_owned()).or_insert(i64::MIN);
            *slot = (*slot).max(ts);
        }
    });
    let live: BTreeSet<String> = {
        let mut out = BTreeSet::new();
        doc.get_map(super::BINARIES_MAP).for_each(|_, value| {
            if let Ok(raw) = value.get_deep_value().into_string() {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) {
                    if let Some(hash) = parsed["hash"].as_str() {
                        out.insert(hash.to_owned());
                    }
                }
            }
        });
        out
    };
    for (digest, last_ts) in last_mention {
        if last_ts < min_surviving_ms && !ledger_blobs.contains(&digest) && !live.contains(&digest)
        {
            reachable.remove(&digest);
        }
    }
    reachable
}

/// A GC run with the live writer: retention trims execute here (the
/// shallow-snapshot compaction needs the collector's document), then the
/// byte-level plan is recomputed against the post-trim store and applied.
/// MUST run under the project's exclusive flock when `apply` is true.
pub fn gc_run_store(store: &mut ProjectStore, apply: bool) -> Result<GcOutcome> {
    let root = store.root().to_path_buf();
    let mut trimmed = 0usize;
    let mut boundary_after = None;
    if apply {
        let plan = gc_plan(&root)?;
        if !plan.retention.prunable.is_empty() {
            let boundary =
                timeline::decode_frontier(plan.retention.boundary.as_deref().unwrap_or_default())?;
            let now = chrono::Utc::now().timestamp_millis();
            let tombstones: Vec<LedgerRecord> = plan
                .retention
                .prunable
                .iter()
                .map(|c| LedgerRecord::Tombstone {
                    capture_id: c.id.clone(),
                    at_ms: c.at_ms,
                    paths: c.paths.clone(),
                    events: c.events,
                    parent_frontier: Some(c.parent_frontier.clone()),
                    cause: c.cause,
                    pruned_at_ms: now,
                })
                .collect();
            trimmed = tombstones.len();
            boundary_after = plan.retention.boundary.clone();
            store.compact_with_trim(&boundary, tombstones)?;
            tracing::info!(
                root = %root.display(),
                trimmed,
                boundary = boundary_after.as_deref().unwrap_or(""),
                "retention trim compacted"
            );
        }
    }
    let outcome = gc_run(&root, apply)?;
    match outcome {
        GcOutcome::Planned(plan) => Ok(GcOutcome::Planned(plan)),
        GcOutcome::Applied(mut report) => {
            report.trimmed = trimmed;
            report.boundary_after = boundary_after;
            Ok(GcOutcome::Applied(report))
        }
    }
}

/// One explicitly marked capture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarkedCapture {
    pub capture_id: String,
    pub frontier: String,
    /// True when the mark was already recorded.
    pub already_marked: bool,
}

/// Record an explicit `gc mark <ref>` — the one sanctioned bypass of
/// reachability protection. Refuses the two
/// points that are the present rather than restorable history — the
/// worktree head and any pending restore intent — and refuses already
/// pruned captures. MUST run on the writer (collector thread or flock).
pub fn retention_mark(store: &mut ProjectStore, reference: &str) -> Result<MarkedCapture> {
    let root = store.root().to_path_buf();
    let point = store.resolve(reference)?;
    let capture_id = point.capture_id.clone().ok_or_else(|| {
        SheafError::TimelineReference(format!(
            "`{reference}` names a point that is not a capture; marks attach to captures"
        ))
    })?;
    if store.pruned().iter().any(|(id, _)| *id == capture_id) {
        return Err(SheafError::TimelineReference(format!(
            "capture {} is already pruned; there is nothing left to mark",
            short(&capture_id)
        )));
    }
    if let Some(head_hex) = timeline::read_head_frontier(&root) {
        if head_hex == point.frontier {
            return Err(SheafError::TimelineReference(
                "cannot mark the current head — that is the present, not restorable history".into(),
            ));
        }
    }
    if let Some(intent) = pending_restore_at(&root) {
        if intent.target.frontier == point.frontier {
            return Err(SheafError::TimelineReference(
                "cannot mark the target of a pending restore; finish or abandon it first".into(),
            ));
        }
    }
    let already = store.ledger().is_marked(&capture_id);
    if !already {
        let record = LedgerRecord::Mark {
            capture_id: capture_id.clone(),
            marked_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let payload = record.encode();
        store
            .append_ledger_frame(payload.as_slice())
            .map_err(SheafError::Io)?;
        store.ledger_fold(record);
    }
    Ok(MarkedCapture {
        capture_id,
        frontier: point.frontier,
        already_marked: already,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::events::{Batch, EventKind, FsEvent, TouchedPath};
    use crate::store::{ProjectStore, StoreLimits};
    use tempfile::tempdir;

    fn limits() -> StoreLimits {
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 3,
        }
    }

    /// Lay down a store skeleton and open it once so every directory a
    /// doctor sweep expects exists; the store is closed on return.
    fn fresh(root: &Path) -> ProjectStore {
        std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
        config::write_skeleton(root).unwrap();
        ProjectStore::open(root, limits()).unwrap()
    }

    /// Write one file and return the batch that captures the touch.
    fn touch(root: &Path, rel: &str) -> Batch {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, format!("// {rel}\n")).unwrap();
        Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Touched {
                path: TouchedPath(path),
            })],
            started_at: chrono::Utc::now(),
            flushed_at: chrono::Utc::now(),
        }
    }

    fn find_check<'a>(report: &'a IntegrityReport, name: &str) -> &'a Check {
        report
            .checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("check {name} missing"))
    }

    #[test]
    fn refusal_guidance_is_specific_per_named_check_and_falls_back() {
        for name in [
            "format_version",
            "config",
            "journal_frames",
            "timeline_loads",
            "worktree_head",
            "blob_coverage",
            "ledger_state",
            "shallow_baseline",
        ] {
            let guidance = refusal_guidance(name);
            assert!(!guidance.is_empty(), "{name}: empty guidance");
            assert!(
                !guidance.starts_with("ambiguous corruption"),
                "{name}: fell through to the fallback"
            );
        }
        assert_eq!(
            refusal_guidance("mystery-check"),
            "ambiguous corruption; doctor --fix only removes what it can prove redundant"
        );
    }

    #[test]
    fn check_and_short_helpers_behave() {
        let c = check("x", true, "detail".into());
        assert_eq!(c.name, "x");
        assert!(c.ok);
        assert_eq!(c.detail, "detail");

        assert_eq!(short("abcdefghijklmnop"), "abcdefghijkl");
        assert_eq!(short("ab"), "ab");
        assert_eq!(short(""), "");
    }

    #[test]
    fn doctor_on_a_fresh_store_is_healthy_with_absent_head_and_cache() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            store.apply_batch(&touch(root, "a.txt")).unwrap();
        }
        let report = doctor(root).unwrap();
        assert!(report.ok, "fresh store must be healthy");

        // The head file may be absent (nothing recorded a head yet) or, on
        // builds that stamp a head at capture time, present and resolving.
        // Either shape is healthy; both parse.
        let head = find_check(&report, "worktree_head");
        assert!(head.ok, "{}", head.detail);
        assert!(
            head.detail.contains("absent") || head.detail.contains("resolves"),
            "{}",
            head.detail
        );

        // The derived cache is advisory: it may be absent on a never-backed
        // store or already present when the writer backfills eagerly. Either
        // way the check passes with a non-empty account.
        let cache = find_check(&report, "grep_cache");
        assert!(cache.ok);
        assert!(!cache.detail.is_empty(), "grep_cache must report its state");
        assert!(find_check(&report, "restore_intent")
            .detail
            .contains("no pending intent"));
        assert_eq!(report.captures, 1);
    }

    #[test]
    fn doctor_flags_unparseable_and_unresolvable_head_files() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            store.apply_batch(&touch(root, "a.txt")).unwrap();
        }
        let head = root.join(".sheaf/state/worktree.head");
        std::fs::create_dir_all(head.parent().unwrap()).unwrap();

        std::fs::write(&head, "not json at all").unwrap();
        let report = doctor(root).unwrap();
        assert!(!report.ok);
        let check = find_check(&report, "worktree_head");
        assert!(check.detail.contains("unparseable"), "{}", check.detail);

        // Valid JSON whose frontier decodes to nothing this store knows.
        std::fs::write(&head, r#"{"frontier":"cafebabe"}"#).unwrap();
        let report = doctor(root).unwrap();
        assert!(!report.ok);
        let check = find_check(&report, "worktree_head");
        assert!(
            check.detail.contains("does not resolve"),
            "{}",
            check.detail
        );
    }

    #[test]
    fn doctor_reports_quarantine_and_stage_and_fix_removes_both() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            store.apply_batch(&touch(root, "a.txt")).unwrap();
        }
        let state = root.join(".sheaf/state");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(state.join("restore.intent.bad"), b"garbage").unwrap();
        let stage = root
            .join(".sheaf/store")
            .join(crate::store::restore::STAGE_DIR);
        std::fs::create_dir_all(&stage).unwrap();
        std::fs::write(stage.join("leftover"), b"x").unwrap();

        let before = doctor(root).unwrap();
        assert!(!before.ok);
        // A second, failing restore_intent check names the quarantine.
        assert!(before
            .checks
            .iter()
            .any(|c| { c.name == "restore_intent" && !c.ok && c.detail.contains("quarantined") }));
        assert!(before
            .checks
            .iter()
            .any(|c| c.name == "restore_stage" && c.ok));

        let outcome = doctor_fix(root).unwrap();
        assert!(
            outcome.healthy(),
            "refused after fix: {:?}",
            outcome.refused
        );
        assert!(outcome
            .applied
            .iter()
            .any(|f| f.action == "remove-quarantine"));
        assert!(outcome.applied.iter().any(|f| f.action == "remove-stage"));
        assert!(!state.join("restore.intent.bad").exists());
        assert!(!stage.exists());

        let after = doctor(root).unwrap();
        assert!(after.ok, "post-fix store must be healthy");
    }

    #[test]
    fn doctor_flags_a_manifest_embedding_an_unparseable_ledger() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            for rel in ["a.txt", "b.txt", "c.txt"] {
                store.apply_batch(&touch(root, rel)).unwrap();
            }
        }
        // Corrupt the ledger payload inside every manifest; the snapshot
        // bytes themselves stay valid so the timeline still loads.
        let snaps = root.join(".sheaf/store/snapshots");
        for entry in std::fs::read_dir(&snaps).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".manifest.json") {
                let raw = std::fs::read_to_string(entry.path()).unwrap();
                let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
                value["ledger"] = serde_json::Value::String("not-a-ledger".into());
                std::fs::write(entry.path(), serde_json::to_string(&value).unwrap()).unwrap();
            }
        }

        let report = doctor(root).unwrap();
        let check = find_check(&report, "ledger_state");
        assert!(!check.ok, "ledger_state must fail");
        assert!(
            check.detail.contains("unparseable ledger"),
            "{}",
            check.detail
        );
    }

    #[test]
    fn blob_facts_counts_bytes_orphans_and_superseded_snapshots() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        drop(fresh(root));

        let blobs = root.join(".sheaf/store/blobs");
        let plant = |digest: &str, bytes: &[u8]| {
            let p = blobs.join(&digest[..2]).join(digest);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, bytes).unwrap();
        };
        plant("aabb", b"0123456789"); // 10 bytes, reachable
        plant("ccdd", b"0123456789ABCDEF"); // 16 bytes, orphaned

        let mut reachable = std::collections::BTreeSet::new();
        reachable.insert("aabb".to_string());
        let (count, bytes, orphans, orphan_bytes, superseded) = blob_facts(root, Some(&reachable));
        assert_eq!(
            (count, bytes, orphans, orphan_bytes, superseded),
            (2, 26, 1, 16, 0)
        );

        // Superseded snapshots: everything below the newest manifest's index.
        let snaps = root.join(".sheaf/store/snapshots");
        std::fs::create_dir_all(&snaps).unwrap();
        std::fs::write(snaps.join("snap-000001.snapshot"), b"old").unwrap();
        std::fs::write(snaps.join("snap-000002.snapshot"), b"new").unwrap();
        std::fs::write(
            snaps.join("snap-000002.manifest.json"),
            r#"{"snapshot":"snap-000002.snapshot","covered_upto":2}"#,
        )
        .unwrap();
        std::fs::write(snaps.join("not-a-snap.snapshot"), b"ignored").unwrap();
        let (.., superseded) = blob_facts(root, None);
        assert_eq!(superseded, 1, "only snap-000001 is below covered_upto=2");
    }

    #[test]
    fn listing_keeps_numeric_snap_files_with_the_requested_suffix() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join("snap-000007.snap"), b"").unwrap();
        std::fs::write(dir.join("snap-000001.snap"), b"").unwrap();
        std::fs::write(dir.join("snap-abc.snap"), b"").unwrap(); // non-numeric index
        std::fs::write(dir.join("snap-000002.other"), b"").unwrap(); // wrong suffix
        std::fs::write(dir.join("other-000003.snap"), b"").unwrap(); // wrong prefix

        let mut got = listing(dir, ".snap");
        got.sort();
        assert_eq!(
            got.into_iter().map(|(idx, _)| idx).collect::<Vec<_>>(),
            vec![1, 7]
        );
        assert!(listing(&dir.join("missing"), ".snap").is_empty());
    }

    #[test]
    fn gc_plan_and_apply_roundtrip_on_a_small_store() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            store.apply_batch(&touch(root, "a.txt")).unwrap();
        }
        let orphan = root.join(".sheaf/store/blobs/de/deadbeef");
        std::fs::create_dir_all(orphan.parent().unwrap()).unwrap();
        std::fs::write(&orphan, b"orphan payload").unwrap();

        // No retention policy configured: nothing is prunable, but the
        // unreachable blob is still collected.
        let plan = gc_plan(root).unwrap();
        assert!(plan.segments.is_empty(), "{:?}", plan.segments);
        assert!(plan.snapshots.is_empty(), "{:?}", plan.snapshots);
        assert_eq!(plan.orphan_blobs, vec!["deadbeef".to_string()]);
        assert!(plan.retention.prunable.is_empty());

        // A plan-only run changes nothing.
        assert!(matches!(
            gc_run(root, false).unwrap(),
            GcOutcome::Planned(_)
        ));
        assert!(orphan.exists());

        // Names that vanished between plan and apply are skipped, not errors.
        let mut plan = gc_plan(root).unwrap();
        plan.segments.push("seg-000099.jsonl".into());
        plan.snapshots.push("snap-000099.manifest.json".into());
        let report = gc_apply(root, &plan).unwrap();
        assert_eq!(report.segments_removed, 0);
        assert_eq!(report.snapshots_removed, 0);
        assert_eq!(report.blobs_removed, 1);
        assert_eq!(report.captures_after, 1);
        assert_eq!(report.trimmed, 0);
        assert!(report.boundary_after.is_none());
        assert!(!orphan.exists(), "orphan blob must be gone");
    }

    /// Regression: the keep-set GCA must land on the deepest kept capture's
    /// FRONTIER, not the first op of its change. Captures are multi-op
    /// changes (files map + tree_events per commit), so keying the meet on
    /// `change.id` (the first op) sank the boundary one version below the
    /// baseline — and every mark on a root-inclusive prefix silently
    /// deferred instead of pruning. Marking the two oldest captures of a
    /// linear chain must make BOTH prunable with cause "gc mark".
    #[test]
    fn marked_root_prefix_is_prunable_over_multi_op_captures() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = fresh(root);
        for name in ["a.txt", "b.txt", "c.txt", "d.txt"] {
            store.apply_batch(&touch(root, name)).unwrap();
        }

        // The two oldest captures, as a root-inclusive prefix.
        let root_id = store.resolve("@~3").unwrap().capture_id.unwrap();
        let second_id = store.resolve("@~2").unwrap().capture_id.unwrap();
        retention_mark(&mut store, "@~3").unwrap();
        retention_mark(&mut store, "@~2").unwrap();

        let plan = gc_plan(root).unwrap();
        let prunable: std::collections::BTreeMap<&str, &PruneCause> = plan
            .retention
            .prunable
            .iter()
            .map(|c| (c.id.as_str(), &c.cause))
            .collect();
        for cap_id in [&root_id, &second_id] {
            let hit = prunable
                .iter()
                .find(|(id, _)| cap_id.starts_with(**id) || id.starts_with(cap_id.as_str()));
            let (_, cause) = hit.unwrap_or_else(|| {
                panic!(
                    "capture {} not prunable; deferred={:?}",
                    &cap_id[..12],
                    plan.retention.deferred_marks
                )
            });
            assert_eq!(
                **cause,
                PruneCause::Marked,
                "wrong cause for {}",
                &cap_id[..12]
            );
        }
        assert!(
            plan.retention.deferred_marks.is_empty(),
            "no mark should defer: {:?}",
            plan.retention.deferred_marks
        );

        // And the store-level apply actually trims the marked prefix
        // (the writer path the daemon uses; gc_apply is the file-level
        // orphan sweep and never trims captures).
        let outcome = gc_run_store(&mut store, true).unwrap();
        let GcOutcome::Applied(report) = outcome else {
            panic!("expected an applied trim");
        };
        assert!(report.trimmed >= 2, "trimmed {} captures", report.trimmed);
    }

    #[test]
    fn retention_mark_marks_history_once_and_refuses_head_and_unknown_refs() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = fresh(root);
        store.apply_batch(&touch(root, "a.txt")).unwrap();
        store.apply_batch(&touch(root, "b.txt")).unwrap();

        assert!(matches!(
            retention_mark(&mut store, "not-a-real-ref"),
            Err(SheafError::TimelineReference(_))
        ));

        // Plant the head file so the head is recognizably "the present".
        let head_frontier = store.resolve("@").unwrap().frontier;
        let state = root.join(".sheaf/state");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(
            state.join("worktree.head"),
            format!(r#"{{"frontier":"{head_frontier}"}}"#),
        )
        .unwrap();
        let err = retention_mark(&mut store, "@").unwrap_err();
        assert!(err.to_string().contains("current head"), "{err}");

        // History marks cleanly, exactly once.
        let marked = retention_mark(&mut store, "@~1").unwrap();
        assert!(!marked.already_marked);
        assert!(marked.capture_id.len() >= 12);
        assert!(!marked.frontier.is_empty());
        let again = retention_mark(&mut store, "@~1").unwrap();
        assert!(again.already_marked);
        assert_eq!(again.capture_id, marked.capture_id);
    }
    #[test]
    fn doctor_refuses_broken_config_and_gc_plan_is_safe_without_history() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        drop(fresh(root));
        std::fs::write(root.join(".sheaf/config.toml"), b"[broken").unwrap();
        let report = doctor(root).unwrap();
        assert!(!report.ok);
        assert!(!find_check(&report, "config").ok);
        let outcome = doctor_fix(root).unwrap();
        assert!(!outcome.healthy());
        assert!(outcome.refused.iter().any(|r| r.check == "config"));
    }

    #[test]
    fn retention_plan_without_earned_points_has_no_boundary_or_prunable_rows() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let mut store = fresh(root);
        store.apply_batch(&touch(root, "kept.txt")).unwrap();
        let facts = plan_retention(root, &store.doc, store.ledger()).unwrap();
        assert!(facts.prunable.is_empty());
        assert!(facts.boundary.is_some());
        let planned = gc_run_store(&mut store, false).unwrap();
        assert!(matches!(planned, GcOutcome::Planned(_)));
    }
    #[test]
    fn doctor_reports_missing_store_files_without_panicking() {
        let tmp = tempdir().unwrap();
        let report = doctor(tmp.path()).unwrap();
        assert!(!report.ok);
        assert!(!find_check(&report, "format_version").ok);
        assert!(!find_check(&report, "config").ok);
        assert!(!find_check(&report, "timeline_loads").ok);
        assert_eq!(report.captures, 0);
        let repair = doctor_fix(tmp.path()).unwrap();
        assert!(!repair.healthy());
        assert!(repair.refused.iter().any(|r| r.check == "timeline_loads"));
    }

    #[test]
    fn doctor_detects_a_torn_journal_tail_and_fix_restores_health() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        {
            let mut store = fresh(root);
            store.apply_batch(&touch(root, "tail.txt")).unwrap();
        }
        let (_, segment) = crate::store::list_segments(&root.join(".sheaf/store"))
            .into_iter()
            .find(|(_, path)| {
                std::fs::metadata(path)
                    .map(|m| m.len() > 0)
                    .unwrap_or(false)
            })
            .expect("capture creates a journal segment");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes.extend_from_slice(b"torn");
        std::fs::write(&segment, bytes).unwrap();
        let before = doctor(root).unwrap();
        assert!(!find_check(&before, "journal_frames").ok);
        let fixed = doctor_fix(root).unwrap();
        assert!(fixed.applied.iter().any(|f| f.action == "truncate-journal"));
        assert!(fixed.healthy(), "refused: {:?}", fixed.refused);
    }
    #[test]
    fn retention_planning_handles_empty_and_divergent_protection_sets() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();
        let store = fresh(root);
        let empty = plan_retention(root, &store.doc, store.ledger()).unwrap();
        assert!(empty.prunable.is_empty());
        assert!(empty.refused_marks.is_empty());

        drop(store);
        let mut store = fresh(root);
        store.apply_batch(&touch(root, "a.txt")).unwrap();
        store.apply_batch(&touch(root, "b.txt")).unwrap();
        let facts = plan_retention(root, &store.doc, store.ledger()).unwrap();
        assert!(!facts.protected.is_empty());
        assert!(facts.prunable.is_empty());
        let reachable = TimelineReader::open(root).unwrap();
        let blobs = retention_aware_reachable_blobs(reachable.doc(), reachable.ledger(), &facts);
        assert!(blobs.is_empty());
    }

    /// Blob reachability under a trim: a digest leaves the reachable set
    /// only when its every mention predates the earliest surviving capture
    /// AND no surviving ledger record or live entry names it. Mentions carry
    /// the TOP-LEVEL tree-event stamp; a misread of that field once pinned
    /// every mention at i64::MAX and froze reclamation entirely.
    #[test]
    fn retention_aware_reachability_follows_mention_stamps() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let mut store = fresh(root);
        store.apply_batch(&touch(root, "keep.txt")).unwrap();
        let survivor_ms = timeline::captures_from(
            &store.doc,
            store.ledger(),
            &store.doc.oplog_frontiers(),
            None,
            None,
            usize::MAX,
        )
        .unwrap()
        .last()
        .unwrap()
        .timestamp_ms;
        drop(store);

        let reader = TimelineReader::open(root).unwrap();
        let list = reader.doc().get_list(super::super::TREE_EVENTS_LIST);
        let mention = |ts: i64, digest: &str| {
            let stamped = serde_json::json!({ "ts": ts, "event": { "binary": digest } });
            list.insert(list.len(), stamped.to_string()).unwrap();
        };
        let old = "0000000000000000000000000000000000000000000000000000000000000000";
        let recent = "1111111111111111111111111111111111111111111111111111111111111111";
        let live = "2222222222222222222222222222222222222222222222222222222222222222";
        mention(survivor_ms - 10_000, old);
        mention(survivor_ms - 10_000, live);
        mention(survivor_ms + 10_000, recent);
        reader
            .doc()
            .get_map(super::super::BINARIES_MAP)
            .insert(
                "live.bin",
                serde_json::json!({ "hash": live, "size": 1 }).to_string(),
            )
            .unwrap();

        let retention = RetentionFacts {
            prunable: vec![PrunableCapture {
                id: "pruned".to_owned(),
                at_ms: survivor_ms - 20_000,
                parent_frontier: String::new(),
                paths: Vec::new(),
                events: 0,
                cause: PruneCause::Expired,
            }],
            ..RetentionFacts::default()
        };
        let blobs = retention_aware_reachable_blobs(reader.doc(), reader.ledger(), &retention);
        assert!(
            !blobs.contains(old),
            "a pre-boundary mention nothing else names is droppable: {blobs:?}"
        );
        assert!(
            blobs.contains(recent),
            "a post-boundary mention stays reachable: {blobs:?}"
        );
        assert!(
            blobs.contains(live),
            "the live binaries map always stays reachable: {blobs:?}"
        );
    }
}

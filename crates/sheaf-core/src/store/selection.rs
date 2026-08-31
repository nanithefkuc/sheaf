//! Snapshot-bound content selections.
//!
//! This module freezes the data and pure algorithms shared by timeline grep,
//! fragment restore, and smart squash. It deliberately does not expose a CLI
//! or author store operations: selection identity, rebinding, lifecycle,
//! budget, parser, and historical-read semantics are proven here in pure form
//! before the read/write surfaces built on them ship.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::restore::{canonical_scope, HistoryView};
use super::timeline::decode_frontier;
use super::{ProjectStore, TimelineReader};
use crate::error::Result;

pub const SELECTION_HANDLE_VERSION: u32 = 1;
pub const SELECTION_CONTEXT_BYTES: usize = 64;

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Half-open UTF-8 byte range at one immutable source frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: usize,
    pub end: usize,
}

impl ByteRange {
    pub fn new(start: usize, end: usize) -> std::result::Result<Self, SelectionError> {
        if start > end {
            return Err(SelectionError::InvalidRange { start, end });
        }
        Ok(Self { start, end })
    }

    pub fn len(self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(self) -> bool {
        self.start == self.end
    }

    fn validate_in(self, text: &str) -> std::result::Result<(), SelectionError> {
        if self.end > text.len()
            || !text.is_char_boundary(self.start)
            || !text.is_char_boundary(self.end)
        {
            return Err(SelectionError::RangeOutsideText {
                start: self.start,
                end: self.end,
                bytes: text.len(),
            });
        }
        if self.is_empty() {
            return Err(SelectionError::EmptySelection);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionExtent {
    Match,
    Line,
    Hunk,
    Symbol,
}

/// Optional language-aware identity. The structural fingerprint is a
/// disambiguator only; it never authorizes a fuzzy mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticIdentity {
    pub language: String,
    pub kind: String,
    pub qualified_name: String,
    pub structural_fingerprint: String,
}

/// Immutable address of one selected extent in one historical snapshot.
///
/// Field order is intentionally stable: serde's struct serialization plus a
/// domain separator is the v1 canonical byte representation used by [`id`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionHandle {
    pub version: u32,
    pub source_frontier: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_capture_id: Option<String>,
    pub historical_path: String,
    pub extent: SelectionExtent,
    pub range: ByteRange,
    pub selected_text_sha256: String,
    pub before_context_sha256: String,
    pub after_context_sha256: String,
    pub query_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic: Option<SemanticIdentity>,
}

impl SelectionHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn from_source(
        source_frontier: impl Into<String>,
        source_capture_id: Option<String>,
        historical_path: impl Into<String>,
        extent: SelectionExtent,
        range: ByteRange,
        source_text: &str,
        query_fingerprint: impl Into<String>,
        semantic: Option<SemanticIdentity>,
    ) -> std::result::Result<Self, SelectionError> {
        range.validate_in(source_text)?;
        let before = context_before(source_text, range.start);
        let after = context_after(source_text, range.end);
        Ok(Self {
            version: SELECTION_HANDLE_VERSION,
            source_frontier: source_frontier.into(),
            source_capture_id,
            historical_path: historical_path.into(),
            extent,
            range,
            selected_text_sha256: sha256(&source_text.as_bytes()[range.start..range.end]),
            before_context_sha256: sha256(before.as_bytes()),
            after_context_sha256: sha256(after.as_bytes()),
            query_fingerprint: query_fingerprint.into(),
            semantic,
        })
    }

    /// Build a handle from scan-time verified parts instead of the source
    /// text: the range plus the three context hashes `from_source` would
    /// compute. The content-dedup scan memo uses this to rebuild a
    /// capture-specific handle for a content version it scanned earlier —
    /// same fields, byte-for-byte, without retaining the text. Callers
    /// must have derived the hashes from the exact bytes the range
    /// addressed; nothing here can re-validate that.
    #[allow(clippy::too_many_arguments)]
    pub fn from_verified_parts(
        source_frontier: impl Into<String>,
        source_capture_id: Option<String>,
        historical_path: impl Into<String>,
        extent: SelectionExtent,
        range: ByteRange,
        selected_text_sha256: impl Into<String>,
        before_context_sha256: impl Into<String>,
        after_context_sha256: impl Into<String>,
        query_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            version: SELECTION_HANDLE_VERSION,
            source_frontier: source_frontier.into(),
            source_capture_id,
            historical_path: historical_path.into(),
            extent,
            range,
            selected_text_sha256: selected_text_sha256.into(),
            before_context_sha256: before_context_sha256.into(),
            after_context_sha256: after_context_sha256.into(),
            query_fingerprint: query_fingerprint.into(),
            semantic: None,
        }
    }

    /// Stable content-addressed handle ID. Line/column display metadata never
    /// participates because the canonical range is already byte-addressed.
    pub fn id(&self) -> String {
        let canonical = serde_json::to_vec(self).expect("SelectionHandle is serializable");
        let mut digest = Sha256::new();
        digest.update(b"sheaf:selection-handle:v1\0");
        digest.update(canonical);
        hex::encode(digest.finalize())
    }

    pub fn validate_selected_text(
        &self,
        selected_text: &str,
    ) -> std::result::Result<(), SelectionError> {
        if self.version != SELECTION_HANDLE_VERSION {
            return Err(SelectionError::UnsupportedVersion(self.version));
        }
        if selected_text.is_empty() {
            return Err(SelectionError::EmptySelection);
        }
        if sha256(selected_text.as_bytes()) != self.selected_text_sha256 {
            return Err(SelectionError::SourceContentMismatch);
        }
        Ok(())
    }

    /// Recompute the v1 context bytes for this handle from the full source
    /// text and verify them against the recorded hashes. Fragment planning
    /// uses the returned bytes as the deletion-scar anchor; a
    /// mismatch means the handle does not describe this text.
    pub fn verified_contexts<'a>(
        &self,
        source_text: &'a str,
    ) -> std::result::Result<(&'a str, &'a str), SelectionError> {
        self.range.validate_in(source_text)?;
        self.validate_selected_text(&source_text[self.range.start..self.range.end])?;
        let before = context_before(source_text, self.range.start);
        let after = context_after(source_text, self.range.end);
        if sha256(before.as_bytes()) != self.before_context_sha256
            || sha256(after.as_bytes()) != self.after_context_sha256
        {
            return Err(SelectionError::SourceContentMismatch);
        }
        Ok((before, after))
    }
}

pub(super) fn context_before(text: &str, at: usize) -> &str {
    let mut start = at.saturating_sub(SELECTION_CONTEXT_BYTES);
    while start < at && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..at]
}

pub(super) fn context_after(text: &str, at: usize) -> &str {
    let mut end = (at + SELECTION_CONTEXT_BYTES).min(text.len());
    while end > at && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[at..end]
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SelectionError {
    #[error("invalid selection range {start}..{end}")]
    InvalidRange { start: usize, end: usize },
    #[error("selection range {start}..{end} is outside {bytes} UTF-8 bytes")]
    RangeOutsideText {
        start: usize,
        end: usize,
        bytes: usize,
    },
    #[error("selection cannot be empty")]
    EmptySelection,
    #[error("selection handle version {0} is unsupported")]
    UnsupportedVersion(u32),
    #[error("selected source bytes do not match the handle")]
    SourceContentMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionCandidate {
    pub path: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundSelection {
    pub path: String,
    pub range: ByteRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RebindOutcome {
    Bound { binding: BoundSelection },
    Missing,
    Ambiguous { candidates: Vec<BoundSelection> },
}

pub(super) fn overlapping_match_starts(haystack: &str, needle: &str) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut offset = 0usize;
    while offset <= haystack.len() {
        let Some(relative) = haystack[offset..].find(needle) else {
            break;
        };
        let start = offset + relative;
        starts.push(start);
        let advance = haystack[start..]
            .chars()
            .next()
            .map(char::len_utf8)
            .unwrap_or(1);
        offset = start + advance;
    }
    starts
}

/// Exact text/context rebinding. Raw exact matches whose context no longer
/// agrees are returned as ambiguity diagnostics, never chosen by similarity.
pub fn rebind_exact(
    handle: &SelectionHandle,
    selected_text: &str,
    destinations: &[SelectionCandidate],
) -> std::result::Result<RebindOutcome, SelectionError> {
    handle.validate_selected_text(selected_text)?;
    let mut raw = Vec::new();
    let mut contextual = Vec::new();
    for destination in destinations {
        for start in overlapping_match_starts(&destination.text, selected_text) {
            let range = ByteRange {
                start,
                end: start + selected_text.len(),
            };
            let binding = BoundSelection {
                path: destination.path.clone(),
                range,
            };
            let before = sha256(context_before(&destination.text, range.start).as_bytes());
            let after = sha256(context_after(&destination.text, range.end).as_bytes());
            if before == handle.before_context_sha256 && after == handle.after_context_sha256 {
                contextual.push(binding.clone());
            }
            raw.push(binding);
        }
    }
    match contextual.len() {
        1 => Ok(RebindOutcome::Bound {
            binding: contextual.remove(0),
        }),
        n if n > 1 => Ok(RebindOutcome::Ambiguous {
            candidates: contextual,
        }),
        _ if raw.is_empty() => Ok(RebindOutcome::Missing),
        _ => Ok(RebindOutcome::Ambiguous { candidates: raw }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedSymbol {
    pub identity: SemanticIdentity,
    pub range: ByteRange,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SymbolParseError {
    #[error("no symbol adapter for `{0}`")]
    UnsupportedLanguage(String),
    #[error("cannot parse symbols: {0}")]
    InvalidSource(String),
}

/// Parser seam. Production-quality language adapters can replace the
/// prototype without changing selection handles or rebinding rules.
pub trait SymbolParser {
    fn language(&self) -> &'static str;
    fn parse_symbols(
        &self,
        path: &Path,
        source: &str,
    ) -> std::result::Result<Vec<ParsedSymbol>, SymbolParseError>;
}

/// Minimal dependency-free Rust adapter used to prove the parser seam. It
/// recognizes ordinary `fn name(...) { ... }` items/methods while skipping
/// comments and quoted literals. Qualified names are function names only, so
/// duplicate methods intentionally surface as ambiguity.
#[derive(Debug, Default, Clone, Copy)]
pub struct RustPrototypeParser;

impl SymbolParser for RustPrototypeParser {
    fn language(&self) -> &'static str {
        "rust"
    }

    fn parse_symbols(
        &self,
        path: &Path,
        source: &str,
    ) -> std::result::Result<Vec<ParsedSymbol>, SymbolParseError> {
        if path.extension().and_then(|s| s.to_str()) != Some("rs") {
            return Err(SymbolParseError::UnsupportedLanguage(
                path.extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_owned(),
            ));
        }
        parse_rust_functions(source)
    }
}

pub fn rebind_symbol(
    handle: &SelectionHandle,
    destinations: &[SelectionCandidate],
    parser: &dyn SymbolParser,
) -> std::result::Result<RebindOutcome, SymbolParseError> {
    let Some(wanted) = handle.semantic.as_ref() else {
        return Ok(RebindOutcome::Missing);
    };
    if wanted.language != parser.language() {
        return Err(SymbolParseError::UnsupportedLanguage(
            wanted.language.clone(),
        ));
    }
    let mut identity_matches = Vec::new();
    for destination in destinations {
        for symbol in parser.parse_symbols(Path::new(&destination.path), &destination.text)? {
            if symbol.identity.language == wanted.language
                && symbol.identity.kind == wanted.kind
                && symbol.identity.qualified_name == wanted.qualified_name
            {
                identity_matches.push((
                    BoundSelection {
                        path: destination.path.clone(),
                        range: symbol.range,
                    },
                    symbol.identity.structural_fingerprint == wanted.structural_fingerprint,
                ));
            }
        }
    }
    match identity_matches.len() {
        0 => Ok(RebindOutcome::Missing),
        1 => Ok(RebindOutcome::Bound {
            binding: identity_matches.remove(0).0,
        }),
        _ => {
            let exact: Vec<_> = identity_matches
                .iter()
                .filter(|(_, fingerprint)| *fingerprint)
                .map(|(binding, _)| binding.clone())
                .collect();
            if exact.len() == 1 {
                Ok(RebindOutcome::Bound {
                    binding: exact[0].clone(),
                })
            } else {
                Ok(RebindOutcome::Ambiguous {
                    candidates: identity_matches
                        .into_iter()
                        .map(|(binding, _)| binding)
                        .collect(),
                })
            }
        }
    }
}

fn parse_rust_functions(source: &str) -> std::result::Result<Vec<ParsedSymbol>, SymbolParseError> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        i = skip_rust_trivia(source, i)?;
        if i >= bytes.len() {
            break;
        }
        if let Some(end) = skip_rust_literal(source, i)? {
            i = end;
            continue;
        }
        if word_at(bytes, i, b"fn") {
            let start = i;
            i += 2;
            i = skip_rust_trivia(source, i)?;
            let name_start = i;
            while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                i += 1;
            }
            if name_start == i
                || !(bytes[name_start] == b'_' || bytes[name_start].is_ascii_alphabetic())
            {
                return Err(SymbolParseError::InvalidSource(format!(
                    "expected Rust function name at byte {name_start}"
                )));
            }
            let name = &source[name_start..i];
            let mut scan = i;
            let mut parens = 0i32;
            let mut brackets = 0i32;
            let end = loop {
                scan = skip_rust_trivia(source, scan)?;
                if scan >= bytes.len() {
                    return Err(SymbolParseError::InvalidSource(format!(
                        "function `{name}` has no body or semicolon"
                    )));
                }
                match bytes[scan] {
                    b'(' => parens += 1,
                    b')' => parens -= 1,
                    b'[' => brackets += 1,
                    b']' => brackets -= 1,
                    b';' if parens == 0 && brackets == 0 => break scan + 1,
                    b'{' if parens == 0 && brackets == 0 => {
                        break matching_rust_brace(source, scan)?
                    }
                    _ => {}
                }
                scan += 1;
            };
            let selected = &source[start..end];
            out.push(ParsedSymbol {
                identity: SemanticIdentity {
                    language: "rust".into(),
                    kind: "function".into(),
                    qualified_name: name.to_owned(),
                    structural_fingerprint: sha256(normalize_rust_symbol(selected).as_bytes()),
                },
                range: ByteRange { start, end },
            });
            i = end;
        } else {
            i += source[i..].chars().next().map(char::len_utf8).unwrap_or(1);
        }
    }
    Ok(out)
}

fn word_at(bytes: &[u8], at: usize, word: &[u8]) -> bool {
    bytes.get(at..at + word.len()) == Some(word)
        && (at == 0 || !(bytes[at - 1] == b'_' || bytes[at - 1].is_ascii_alphanumeric()))
        && (at + word.len() == bytes.len()
            || !(bytes[at + word.len()] == b'_' || bytes[at + word.len()].is_ascii_alphanumeric()))
}

fn skip_rust_trivia(source: &str, mut at: usize) -> std::result::Result<usize, SymbolParseError> {
    let bytes = source.as_bytes();
    loop {
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        if bytes.get(at..at + 2) == Some(b"//") {
            at += 2;
            while at < bytes.len() && bytes[at] != b'\n' {
                at += 1;
            }
            continue;
        }
        if bytes.get(at..at + 2) == Some(b"/*") {
            let mut depth = 1usize;
            at += 2;
            while at < bytes.len() && depth > 0 {
                if bytes.get(at..at + 2) == Some(b"/*") {
                    depth += 1;
                    at += 2;
                } else if bytes.get(at..at + 2) == Some(b"*/") {
                    depth -= 1;
                    at += 2;
                } else {
                    at += 1;
                }
            }
            if depth != 0 {
                return Err(SymbolParseError::InvalidSource(
                    "unterminated block comment".into(),
                ));
            }
            continue;
        }
        return Ok(at);
    }
}

fn skip_rust_literal(
    source: &str,
    at: usize,
) -> std::result::Result<Option<usize>, SymbolParseError> {
    let bytes = source.as_bytes();
    if bytes.get(at) == Some(&b'"') {
        let mut i = at + 1;
        while i < bytes.len() {
            if bytes[i] == b'\\' {
                i = (i + 2).min(bytes.len());
            } else if bytes[i] == b'"' {
                return Ok(Some(i + 1));
            } else {
                i += 1;
            }
        }
        return Err(SymbolParseError::InvalidSource(
            "unterminated string literal".into(),
        ));
    }
    if bytes.get(at) == Some(&b'\'') {
        // A Rust lifetime (`'a`) is not a literal. Recognize only compact
        // character forms with an actual closing quote.
        let end = if bytes.get(at + 1) == Some(&b'\\') {
            at + 4
        } else {
            at + 3
        };
        if end <= bytes.len() && bytes.get(end - 1) == Some(&b'\'') {
            return Ok(Some(end));
        }
        return Ok(None);
    }
    // Rust raw strings: r"...", r#"..."#, ...
    if bytes.get(at) == Some(&b'r') {
        let mut i = at + 1;
        while bytes.get(i) == Some(&b'#') {
            i += 1;
        }
        if bytes.get(i) == Some(&b'"') {
            let hashes = i - (at + 1);
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"'
                    && bytes.get(i + 1..i + 1 + hashes) == Some(&vec![b'#'; hashes][..])
                {
                    return Ok(Some(i + 1 + hashes));
                }
                i += 1;
            }
            return Err(SymbolParseError::InvalidSource(
                "unterminated raw string".into(),
            ));
        }
    }
    Ok(None)
}

fn matching_rust_brace(source: &str, open: usize) -> std::result::Result<usize, SymbolParseError> {
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = skip_rust_literal(source, i)? {
            i = end;
            continue;
        }
        if bytes.get(i..i + 2) == Some(b"//") || bytes.get(i..i + 2) == Some(b"/*") {
            i = skip_rust_trivia(source, i)?;
            continue;
        }
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(SymbolParseError::InvalidSource(
        "unterminated Rust function body".into(),
    ))
}

fn normalize_rust_symbol(source: &str) -> String {
    source.chars().filter(|c| !c.is_whitespace()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LifecycleState {
    Present {
        path: String,
        selection_id: String,
        content_hash: String,
    },
    Absent,
    Pruned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleObservation {
    pub point_id: String,
    pub lineage_id: String,
    pub on_current: bool,
    pub state: LifecycleState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    /// A point-discovery occurrence, not a history transition.
    Present,
    Introduced,
    Changed,
    /// Same occurrence, uniquely rebound at a different range in one path.
    Relocated,
    /// Same occurrence carried by a recorded path rename.
    Renamed,
    /// Legacy compatibility variant; occurrence history does not emit it.
    Moved,
    Removed,
    /// Legacy compatibility variant; a later literal starts a fresh episode.
    Reintroduced,
    /// Continuity could not be proven uniquely.
    Ambiguous,
    RetentionGap,
    Observed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub point_id: String,
    pub lineage_id: String,
    pub kind: LifecycleKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_id: Option<String>,
}

#[derive(Default)]
struct LineageState {
    seen_present: bool,
    present: Option<(String, String, String)>, // path, selection id, content hash
}

/// Collapse chronological observations into deterministic lifecycle changes.
pub fn lifecycle_transitions(
    observations: &[LifecycleObservation],
    all_branches: bool,
    every_capture: bool,
) -> Vec<LifecycleEvent> {
    let mut lineages: BTreeMap<String, LineageState> = BTreeMap::new();
    let mut events = Vec::new();
    for observation in observations.iter().filter(|o| all_branches || o.on_current) {
        let state = lineages.entry(observation.lineage_id.clone()).or_default();
        let emit = |events: &mut Vec<LifecycleEvent>,
                    kind,
                    path: Option<String>,
                    selection_id: Option<String>| {
            events.push(LifecycleEvent {
                point_id: observation.point_id.clone(),
                lineage_id: observation.lineage_id.clone(),
                kind,
                path,
                selection_id,
            });
        };
        match &observation.state {
            LifecycleState::Pruned => {
                emit(&mut events, LifecycleKind::RetentionGap, None, None);
                state.present = None;
            }
            LifecycleState::Absent => {
                if let Some((path, selection_id, _)) = state.present.take() {
                    emit(
                        &mut events,
                        LifecycleKind::Removed,
                        Some(path),
                        Some(selection_id),
                    );
                }
            }
            LifecycleState::Present {
                path,
                selection_id,
                content_hash,
            } => {
                match &state.present {
                    None => emit(
                        &mut events,
                        if state.seen_present {
                            LifecycleKind::Reintroduced
                        } else {
                            LifecycleKind::Introduced
                        },
                        Some(path.clone()),
                        Some(selection_id.clone()),
                    ),
                    Some((old_path, _, old_hash)) => {
                        let moved = old_path != path;
                        let changed = old_hash != content_hash;
                        if moved {
                            emit(
                                &mut events,
                                LifecycleKind::Moved,
                                Some(path.clone()),
                                Some(selection_id.clone()),
                            );
                        }
                        if changed {
                            emit(
                                &mut events,
                                LifecycleKind::Changed,
                                Some(path.clone()),
                                Some(selection_id.clone()),
                            );
                        }
                        if !moved && !changed && every_capture {
                            emit(
                                &mut events,
                                LifecycleKind::Observed,
                                Some(path.clone()),
                                Some(selection_id.clone()),
                            );
                        }
                    }
                }
                state.seen_present = true;
                state.present = Some((path.clone(), selection_id.clone(), content_hash.clone()));
            }
        }
    }
    events
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchBudget {
    /// A partial `budget` object is valid: each omitted field falls back to
    /// its default (the `default_*` functions below) rather than failing
    /// deserialization.
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default = "default_max_materialized_bytes")]
    pub max_materialized_bytes: u64,
    #[serde(default = "default_max_elapsed_ms")]
    pub max_elapsed_ms: u64,
}

fn default_max_results() -> usize {
    1_000
}
fn default_max_materialized_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_max_elapsed_ms() -> u64 {
    5_000
}

impl Default for SearchBudget {
    fn default() -> Self {
        Self {
            max_results: default_max_results(),
            max_materialized_bytes: default_max_materialized_bytes(),
            max_elapsed_ms: default_max_elapsed_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchUsage {
    pub results: usize,
    pub materialized_bytes: u64,
    pub elapsed_ms: u64,
    /// Whole-document historical forks performed while resolving point reads.
    #[serde(default)]
    pub historical_forks: u64,
    /// Individual historical text/binary path lookups performed by the query.
    #[serde(default)]
    pub historical_path_reads: u64,
    /// Historical path lookups served from the process's bounded warm cache.
    #[serde(default)]
    pub historical_cache_hits: u64,
    /// Historical path lookups loaded from the persistent derived sidecar.
    #[serde(default)]
    pub historical_disk_cache_hits: u64,
    /// Path visits answered from the query's content-version memo: the
    /// content identity was already scanned this run, so no bytes were
    /// materialized, decompressed, or searched for the revisit.
    #[serde(default)]
    pub content_dedup_hits: u64,
    /// Captures reprocessed only to reconstruct lifecycle state for a cursor.
    #[serde(default)]
    pub cursor_replayed_captures: u64,
    /// Path visits the trigram pre-filter proved cannot contain the needle,
    /// so the content was neither read nor scanned. The core acceleration
    /// signal for a rare or absent literal: high here means the index did its
    /// job, zero means the filter did not apply (short needle or no index).
    #[serde(default)]
    pub trigram_skipped: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchStopReason {
    ResultLimit,
    ByteLimit,
    TimeLimit,
}

impl SearchBudget {
    pub fn stop_reason(&self, usage: &SearchUsage) -> Option<SearchStopReason> {
        if usage.results >= self.max_results {
            Some(SearchStopReason::ResultLimit)
        } else if usage.materialized_bytes >= self.max_materialized_bytes {
            Some(SearchStopReason::ByteLimit)
        } else if usage.elapsed_ms >= self.max_elapsed_ms {
            Some(SearchStopReason::TimeLimit)
        } else {
            None
        }
    }
}

/// Cursor is query-bound so it cannot accidentally resume a different grep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchCursor {
    pub query_fingerprint: String,
    /// Last fully processed capture. Legacy cursors resume after this point.
    pub after_capture_id: String,
    /// Partial-capture resume target. Absent means capture-boundary resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_capture_id: Option<String>,
    /// Number of records already consumed from the resume capture's stable
    /// ordered batch. Zero for capture-boundary cursors.
    #[serde(default)]
    pub record_index: usize,
    /// Reserved compatibility fields from the original cursor contract.
    #[serde(default)]
    pub path_index: usize,
    #[serde(default)]
    pub match_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalPathContent {
    Absent,
    Text(String),
    Binary { hash: String, bytes: u64 },
}

impl ProjectStore {
    /// Read one path at one retained point without materializing every tracked
    /// entry into a map. This is the search primitive timeline grep builds on.
    pub fn historical_path_content(
        &self,
        reference: &str,
        path: &str,
    ) -> Result<HistoricalPathContent> {
        let key = canonical_historical_path(path)?;
        let point = self.resolve(reference)?;
        let frontier = decode_frontier(&point.frontier)?;
        let mut view = HistoryView::open(&self.doc)?;
        view.path_at(&frontier, &key)
    }
}

impl TimelineReader {
    /// Read-only/degraded counterpart to [`ProjectStore::historical_path_content`].
    pub fn historical_path_content(
        &self,
        reference: &str,
        path: &str,
    ) -> Result<HistoricalPathContent> {
        let key = canonical_historical_path(path)?;
        let point = self.resolve(reference)?;
        let frontier = decode_frontier(&point.frontier)?;
        let mut view = HistoryView::open(self.doc())?;
        view.path_at(&frontier, &key)
    }
}

fn canonical_historical_path(path: &str) -> Result<String> {
    let scope = canonical_scope(&[path.to_owned()])?;
    Ok(scope.into_iter().next().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle_for(source: &str, needle: &str) -> SelectionHandle {
        let start = source.find(needle).unwrap();
        SelectionHandle::from_source(
            "frontier",
            Some("capture".into()),
            "src/lib.rs",
            SelectionExtent::Match,
            ByteRange {
                start,
                end: start + needle.len(),
            },
            source,
            "literal:needle",
            None,
        )
        .unwrap()
    }

    #[test]
    fn handle_ids_are_stable_and_sensitive() {
        let source = "before needle after";
        let handle = handle_for(source, "needle");
        let encoded = serde_json::to_string(&handle).unwrap();
        let decoded: SelectionHandle = serde_json::from_str(&encoded).unwrap();
        assert_eq!(handle.id(), decoded.id());

        let mut variants = Vec::new();
        let mut frontier = handle.clone();
        frontier.source_frontier.push('x');
        variants.push(frontier);
        let mut range = handle.clone();
        range.range.start -= 1;
        variants.push(range);
        let mut content = handle.clone();
        content.selected_text_sha256 = sha256(b"other");
        variants.push(content);
        let mut context = handle.clone();
        context.before_context_sha256 = sha256(b"different");
        variants.push(context);
        assert!(variants.iter().all(|variant| variant.id() != handle.id()));
    }

    #[test]
    fn exact_rebinding_is_unique_missing_or_ambiguous() {
        assert_eq!(overlapping_match_starts("ababa", "aba"), vec![0, 2]);
        let source = "before needle after";
        let handle = handle_for(source, "needle");
        let bound = rebind_exact(
            &handle,
            "needle",
            &[SelectionCandidate {
                path: "renamed.rs".into(),
                text: source.into(),
            }],
        )
        .unwrap();
        assert!(matches!(bound, RebindOutcome::Bound { .. }));

        assert_eq!(
            rebind_exact(&handle, "needle", &[]).unwrap(),
            RebindOutcome::Missing
        );
        let ambiguous = rebind_exact(
            &handle,
            "needle",
            &[SelectionCandidate {
                path: "dup.rs".into(),
                text: "needle needle".into(),
            }],
        )
        .unwrap();
        assert!(
            matches!(ambiguous, RebindOutcome::Ambiguous { candidates } if candidates.len() == 2)
        );
    }

    #[test]
    fn rust_parser_and_symbol_rebinding_fail_closed_on_duplicates() {
        let source = "const FAKE: &str = \"fn not_a_symbol() {}\";\nfn alpha() { println!(\"}\"); }\nfn beta<'a>(x: &'a str) { let _ = x; /* { */ }\n";
        let parser = RustPrototypeParser;
        let symbols = parser
            .parse_symbols(Path::new("src/lib.rs"), source)
            .unwrap();
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].identity.qualified_name, "alpha");
        assert_eq!(
            &source[symbols[0].range.start..symbols[0].range.end],
            "fn alpha() { println!(\"}\"); }"
        );

        let alpha = &symbols[0];
        let handle = SelectionHandle::from_source(
            "frontier",
            None,
            "src/lib.rs",
            SelectionExtent::Symbol,
            alpha.range,
            source,
            "symbol:alpha",
            Some(alpha.identity.clone()),
        )
        .unwrap();
        let duplicate = format!("{source}\n{}", &source[alpha.range.start..alpha.range.end]);
        let outcome = rebind_symbol(
            &handle,
            &[SelectionCandidate {
                path: "src/lib.rs".into(),
                text: duplicate,
            }],
            &parser,
        )
        .unwrap();
        assert!(
            matches!(outcome, RebindOutcome::Ambiguous { candidates } if candidates.len() == 2)
        );
    }

    #[test]
    fn lifecycle_tracks_episodes_branches_and_retention_gaps() {
        let present =
            |point: &str, lineage: &str, current: bool, path: &str, id: &str, hash: &str| {
                LifecycleObservation {
                    point_id: point.into(),
                    lineage_id: lineage.into(),
                    on_current: current,
                    state: LifecycleState::Present {
                        path: path.into(),
                        selection_id: id.into(),
                        content_hash: hash.into(),
                    },
                }
            };
        let observations = vec![
            present("a", "main", true, "old.rs", "s1", "h1"),
            present("b", "main", true, "old.rs", "s1", "h2"),
            present("c", "main", true, "new.rs", "s1", "h2"),
            LifecycleObservation {
                point_id: "d".into(),
                lineage_id: "main".into(),
                on_current: true,
                state: LifecycleState::Absent,
            },
            present("e", "main", true, "new.rs", "s2", "h3"),
            present("x", "abandoned", false, "old.rs", "sx", "hx"),
            LifecycleObservation {
                point_id: "gap".into(),
                lineage_id: "main".into(),
                on_current: true,
                state: LifecycleState::Pruned,
            },
        ];
        let current = lifecycle_transitions(&observations, false, false);
        assert_eq!(
            current.iter().map(|e| e.kind).collect::<Vec<_>>(),
            [
                LifecycleKind::Introduced,
                LifecycleKind::Changed,
                LifecycleKind::Moved,
                LifecycleKind::Removed,
                LifecycleKind::Reintroduced,
                LifecycleKind::RetentionGap,
            ]
        );
        let all = lifecycle_transitions(&observations, true, false);
        assert!(all.iter().any(|e| e.lineage_id == "abandoned"));
    }

    #[test]
    fn budgets_stop_in_stable_priority_order() {
        let budget = SearchBudget::default();
        assert_eq!(
            budget.stop_reason(&SearchUsage {
                results: budget.max_results,
                materialized_bytes: budget.max_materialized_bytes,
                elapsed_ms: budget.max_elapsed_ms,
                historical_forks: 0,
                historical_path_reads: 0,
                historical_cache_hits: 0,
                historical_disk_cache_hits: 0,
                content_dedup_hits: 0,
                cursor_replayed_captures: 0,
                trigram_skipped: 0,
            }),
            Some(SearchStopReason::ResultLimit)
        );
    }
    #[test]
    fn ranges_and_handles_reject_invalid_or_stale_bytes() {
        assert!(matches!(
            ByteRange::new(3, 2),
            Err(SelectionError::InvalidRange { .. })
        ));
        let source = "é needle";
        let h = handle_for(source, "needle");
        assert!(matches!(
            ByteRange { start: 1, end: 2 }.validate_in(source),
            Err(SelectionError::RangeOutsideText { .. })
        ));
        assert!(matches!(
            ByteRange { start: 0, end: 0 }.validate_in(source),
            Err(SelectionError::EmptySelection)
        ));
        assert!(matches!(
            h.validate_selected_text("other"),
            Err(SelectionError::SourceContentMismatch)
        ));
        let mut old = h.clone();
        old.version += 1;
        assert!(matches!(
            old.validate_selected_text("needle"),
            Err(SelectionError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            h.verified_contexts("changed"),
            Err(SelectionError::RangeOutsideText { .. })
        ));
    }

    #[test]
    fn exact_rebinding_distinguishes_raw_and_context_matches() {
        let source = "left needle right";
        let h = handle_for(source, "needle");
        let raw_only = rebind_exact(
            &h,
            "needle",
            &[SelectionCandidate {
                path: "x".into(),
                text: "prefix needle suffix".into(),
            }],
        )
        .unwrap();
        assert!(
            matches!(raw_only, RebindOutcome::Ambiguous { candidates } if candidates.len() == 1)
        );
        assert_eq!(
            rebind_exact(&h, "needle", &[]).unwrap(),
            RebindOutcome::Missing
        );
    }

    #[test]
    fn parser_rejects_bad_literals_bodies_and_paths() {
        let parser = RustPrototypeParser;
        assert!(matches!(
            parser.parse_symbols(Path::new("x.txt"), "fn x() {}"),
            Err(SymbolParseError::UnsupportedLanguage(_))
        ));
        for source in ["fn x() {", "const X = \"unterminated", "/* unterminated"] {
            assert!(matches!(
                parser.parse_symbols(Path::new("x.rs"), source),
                Err(SymbolParseError::InvalidSource(_))
            ));
        }
        let h = handle_for("fn x() {}", "fn x() {}");
        let mut semantic = h;
        semantic.semantic = Some(SemanticIdentity {
            language: "python".into(),
            kind: "function".into(),
            qualified_name: "x".into(),
            structural_fingerprint: "f".into(),
        });
        assert!(matches!(
            rebind_symbol(&semantic, &[], &parser),
            Err(SymbolParseError::UnsupportedLanguage(_))
        ));
    }

    #[test]
    fn lifecycle_options_filter_branches_and_emit_observations() {
        let mk = |point: &str, lineage: &str, current: bool, state| LifecycleObservation {
            point_id: point.into(),
            lineage_id: lineage.into(),
            on_current: current,
            state,
        };
        let p = |hash: &str| LifecycleState::Present {
            path: "a".into(),
            selection_id: "s".into(),
            content_hash: hash.into(),
        };
        let observations = vec![
            mk("a", "l", true, p("h")),
            mk("b", "l", true, p("h")),
            mk("c", "other", false, p("h")),
        ];
        assert!(lifecycle_transitions(&observations, false, true)
            .iter()
            .any(|e| e.kind == LifecycleKind::Observed));
        assert!(lifecycle_transitions(&observations, true, false)
            .iter()
            .any(|e| e.point_id == "c"));
    }
    #[test]
    fn budget_reports_each_limit_and_parser_handles_comments() {
        let b = SearchBudget::default();
        let usage = |results, bytes, elapsed| SearchUsage {
            results,
            materialized_bytes: bytes,
            elapsed_ms: elapsed,
            historical_forks: 0,
            historical_path_reads: 0,
            historical_cache_hits: 0,
            historical_disk_cache_hits: 0,
            content_dedup_hits: 0,
            cursor_replayed_captures: 0,
            trigram_skipped: 0,
        };
        assert_eq!(
            b.stop_reason(&usage(b.max_results, 0, 0)),
            Some(SearchStopReason::ResultLimit)
        );
        assert_eq!(
            b.stop_reason(&usage(0, b.max_materialized_bytes, 0)),
            Some(SearchStopReason::ByteLimit)
        );
        assert_eq!(
            b.stop_reason(&usage(0, 0, b.max_elapsed_ms)),
            Some(SearchStopReason::TimeLimit)
        );
        let parser = RustPrototypeParser;
        let symbols = parser
            .parse_symbols(Path::new("x.rs"), "// fn fake() {}\nfn real() {}\n")
            .unwrap();
        assert_eq!(symbols.len(), 1);
        assert_eq!(symbols[0].identity.qualified_name, "real");
    }
}

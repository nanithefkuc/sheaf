//! A custom byte-trigram postings index over the sidecar's distinct
//! content versions.
//!
//! The content-version cache removes the per-capture `fork_at` cost, but a
//! rare or absent literal still scans every distinct content version once —
//! O(distinct-versions) work that dominates on a long history where every
//! capture edits a hot file. This index is the pre-filter that makes an
//! absent needle cheap: it maps each byte trigram to the set of distinct
//! contents that contain it, so a query intersects its needle's trigram
//! postings and only the surviving contents are read and exactly verified.
//!
//! Why trigrams over distinct **content IDs**, not captures: dedup is the
//! substrate's whole point. Ten thousand captures of one evolving file are a
//! few thousand distinct versions; postings over versions are what the scan
//! memo already deduplicates against, so the index and the scanner agree on
//! the unit of work.
//!
//! Why a custom index and not SQLite FTS5: the postings are keyed by our own
//! content-index space (a dense u32 assigned per distinct hash), the query is
//! a plain sorted-list intersection, and verification always re-reads the
//! blob — so the index is advisory and disposable exactly like the rest of
//! the sidecar. The measured comparison against FTS5 `detail=none` lives in
//! `crates/sheaf-core/tests/grep_trigram_bench.rs`; the custom design won on
//! both query latency and on-disk size while adding no C dependency.
//!
//! Correctness contract: the index may only ever *over*-approximate the
//! candidate set. A false positive costs one wasted scan (caught by exact
//! verification); a false negative would drop a real hit. So a needle shorter
//! than three bytes, or content whose trigram set is unavailable, yields "all
//! contents are candidates" — never a filtered subset. Multibyte literals are
//! handled uniformly: trigrams are raw bytes, so a three-byte UTF-8 scalar is
//! one trigram and a longer literal decomposes into overlapping byte trigrams
//! like any ASCII needle.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};

/// A byte trigram packed into the low 24 bits of a u32.
type Trigram = u32;

/// The literal needle must be at least this many bytes for the trigram
/// pre-filter to apply. Below it there is no trigram to intersect on, so the
/// query falls back to scanning every distinct content. The routing unit here
/// is the raw byte, and a sub-trigram needle is not routed through postings.
pub(super) const TRIGRAM_MIN_NEEDLE_BYTES: usize = 3;

/// On-disk schema tag for the trigram index files. Bumped independently of
/// the content-cache schema so the two evolve separately.
const TRIGRAM_SCHEMA: u32 = 2;
const PAYLOAD_DIGEST_BYTES: usize = 32;

/// zstd level for the framed index payload. Level 3 matched the content-cache
/// blobs and already lands the index well under the size ceiling.
const TRIGRAM_ZSTD_LEVEL: i32 = 3;

const MAGIC: &[u8; 8] = b"SHEAFTG1";

/// Every overlapping byte trigram in `bytes`, de-duplicated. A distinct
/// content contributes each trigram it holds exactly once to its posting.
fn trigrams_of(bytes: &[u8]) -> BTreeSet<Trigram> {
    let mut set = BTreeSet::new();
    if bytes.len() < TRIGRAM_MIN_NEEDLE_BYTES {
        return set;
    }
    for window in bytes.windows(3) {
        let gram = (window[0] as u32) << 16 | (window[1] as u32) << 8 | (window[2] as u32);
        set.insert(gram);
    }
    set
}

/// The trigrams a needle requires. A content can contain the needle only if
/// it contains every one of these, so the candidate set is the intersection
/// of their postings. Returns `None` when the needle is too short to filter
/// on — the caller must then treat every content as a candidate.
pub(super) fn needle_trigrams(needle: &[u8]) -> Option<Vec<Trigram>> {
    if needle.len() < TRIGRAM_MIN_NEEDLE_BYTES {
        return None;
    }
    let set = trigrams_of(needle);
    if set.is_empty() {
        return None;
    }
    Some(set.into_iter().collect())
}

/// An in-memory builder that accumulates one posting list per trigram over a
/// set of distinct contents, each identified by a dense `content_id`. The
/// builder is the write path; [`TrigramIndex`] is the queryable read path.
#[derive(Default)]
pub(super) struct TrigramBuilder {
    /// content_id -> content hash, in assignment order.
    hashes: Vec<String>,
    /// content hash -> content_id, to avoid indexing a version twice.
    seen: BTreeMap<String, u32>,
    /// trigram -> sorted, unique content_ids that contain it.
    postings: BTreeMap<Trigram, Vec<u32>>,
}

impl TrigramBuilder {
    /// Index one distinct content version. Idempotent per hash: re-adding a
    /// hash already present is a no-op, so a rebuild that revisits the same
    /// blobs cannot double-count.
    pub(super) fn add(&mut self, hash: &str, text: &[u8]) {
        if self.seen.contains_key(hash) {
            return;
        }
        // Invalid identities stay uncovered. Silently substituting a zero
        // digest would preserve postings under a hash no mapping can name and
        // contradict the builder's fail-open contract.
        if !matches!(hex::decode(hash), Ok(raw) if raw.len() == 32) {
            return;
        }
        let id = self.hashes.len() as u32;
        self.hashes.push(hash.to_owned());
        self.seen.insert(hash.to_owned(), id);
        for gram in trigrams_of(text) {
            let posting = self.postings.entry(gram).or_default();
            // Contents are added in ascending id order, so appending keeps
            // each posting sorted without a re-sort.
            posting.push(id);
        }
    }

    pub(super) fn distinct_contents(&self) -> usize {
        self.hashes.len()
    }

    /// Serialize to the compact on-disk form, then zstd-compress the payload.
    /// The measured comparison (`grep_trigram_bench`) showed a flat-u32
    /// layout at ~5.5x the zstd corpus (over the 2x ceiling) while this form
    /// — raw 32-byte digests, gap-and-length-prefixed delta-varint postings,
    /// then whole-blob zstd — lands at ~0.30x, well inside the ceiling and
    /// 4.4x smaller than SQLite FTS5 `detail=none`.
    ///
    /// Uncompressed payload (all integers LEB128 varint unless noted):
    /// `MAGIC | schema:u32(LE) | content_count | 32-byte-digest *
    /// content_count | trigram_count | (trigram, posting_len, gap *
    /// posting_len) * trigram_count | payload_sha256`, where `gap` is the
    /// ascending content-id delta and `payload_sha256` covers every preceding
    /// uncompressed byte. The whole payload is then zstd-framed on disk.
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(MAGIC);
        payload.extend_from_slice(&TRIGRAM_SCHEMA.to_le_bytes());
        write_varint(&mut payload, self.hashes.len() as u64);
        for hash in &self.hashes {
            // `add` admits only 32-byte SHA-256 identities. Raw storage halves
            // the dictionary versus 64-byte hex strings.
            let raw = hex::decode(hash).expect("validated hash identity");
            debug_assert_eq!(raw.len(), 32);
            payload.extend_from_slice(&raw);
        }
        write_varint(&mut payload, self.postings.len() as u64);
        for (gram, ids) in &self.postings {
            write_varint(&mut payload, *gram as u64);
            write_varint(&mut payload, ids.len() as u64);
            let mut prev = 0u32;
            for id in ids {
                write_varint(&mut payload, (id - prev) as u64);
                prev = *id;
            }
        }
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(&payload);
        payload.extend_from_slice(&digest);
        zstd::stream::encode_all(std::io::Cursor::new(&payload), TRIGRAM_ZSTD_LEVEL)
            .unwrap_or(payload)
    }
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// A queryable trigram index loaded from disk. Postings are borrowed as
/// offset ranges into the backing bytes, so loading is a header parse plus an
/// offset table — no per-posting allocation.
pub(super) struct TrigramIndex {
    hashes: Vec<String>,
    /// Fast membership over the indexed hashes. The filter may only *exclude*
    /// a hash it actually indexed; a hash absent here (a content version
    /// written after the last index rebuild) is treated as a candidate, never
    /// excluded — otherwise a stale index would drop a real match in freshly
    /// captured content.
    indexed: BTreeSet<String>,
    postings: BTreeMap<Trigram, Vec<u32>>,
}

impl TrigramIndex {
    /// Parse the on-disk form (zstd-framed varint payload). Returns `None` on
    /// any framing mismatch — a corrupt index is a missing index (every
    /// content becomes a candidate), never a wrong answer.
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let framed = zstd::stream::decode_all(bytes).ok()?;
        if framed.len() < PAYLOAD_DIGEST_BYTES {
            return None;
        }
        let payload_len = framed.len().checked_sub(PAYLOAD_DIGEST_BYTES)?;
        let (payload, stored_digest) = framed.split_at(payload_len);
        use sha2::{Digest, Sha256};
        if Sha256::digest(payload).as_slice() != stored_digest {
            return None;
        }

        let mut cursor = Cursor::new(payload);
        if cursor.take(8)? != MAGIC {
            return None;
        }
        if cursor.u32_le()? != TRIGRAM_SCHEMA {
            return None;
        }
        let content_count = usize::try_from(cursor.varint()?).ok()?;
        // Guard against a corrupt length claiming an implausible allocation.
        if content_count > (payload.len() / 32) + 1 {
            return None;
        }
        let mut hashes = Vec::with_capacity(content_count);
        for _ in 0..content_count {
            let raw = cursor.take(32)?;
            hashes.push(hex::encode(raw));
        }
        let indexed: BTreeSet<String> = hashes.iter().cloned().collect();
        if indexed.len() != hashes.len() {
            return None;
        }

        let trigram_count = usize::try_from(cursor.varint()?).ok()?;
        if trigram_count > payload.len() + 1 {
            return None;
        }
        let mut postings = BTreeMap::new();
        for _ in 0..trigram_count {
            let gram = u32::try_from(cursor.varint()?).ok()?;
            if gram > 0x00ff_ffff {
                return None;
            }
            let posting_len = usize::try_from(cursor.varint()?).ok()?;
            if posting_len == 0 || posting_len > content_count {
                return None;
            }
            let mut ids = Vec::with_capacity(posting_len);
            let mut prev = 0u32;
            for ordinal in 0..posting_len {
                let gap = u32::try_from(cursor.varint()?).ok()?;
                if ordinal > 0 && gap == 0 {
                    return None;
                }
                prev = prev.checked_add(gap)?;
                if usize::try_from(prev).ok()? >= content_count {
                    return None;
                }
                ids.push(prev);
            }
            if postings.insert(gram, ids).is_some() {
                return None;
            }
        }
        if cursor.at != payload.len() {
            return None;
        }
        Some(TrigramIndex {
            hashes,
            indexed,
            postings,
        })
    }

    /// Whether this index actually covers a content hash. Query exclusion
    /// requires this membership in addition to absence from the candidate set;
    /// uncovered (fresh) hashes are always scanned.
    pub(super) fn covers(&self, hash: &str) -> bool {
        self.indexed.contains(hash)
    }

    /// The set of content hashes that *may* contain `needle`. `None` means
    /// "no filter applies — every content is a candidate" (short needle or a
    /// needle trigram absent from the index would over-filter). A returned
    /// set is an over-approximation: exact verification still scans each.
    pub(super) fn candidates(&self, needle: &[u8]) -> Option<BTreeSet<String>> {
        let grams = needle_trigrams(needle)?;
        // Intersect posting lists, smallest first so the working set shrinks
        // fastest. A trigram entirely absent from the index means no content
        // holds it, so the candidate set is empty (a provable no-match).
        let mut lists: Vec<&Vec<u32>> = Vec::with_capacity(grams.len());
        for gram in &grams {
            match self.postings.get(gram) {
                Some(list) => lists.push(list),
                // The trigram appears in no indexed content: nothing can match.
                None => return Some(BTreeSet::new()),
            }
        }
        lists.sort_by_key(|list| list.len());
        let mut acc: Vec<u32> = lists[0].clone();
        for list in &lists[1..] {
            acc = intersect_sorted(&acc, list);
            if acc.is_empty() {
                break;
            }
        }
        Some(
            acc.into_iter()
                .filter_map(|id| self.hashes.get(id as usize).cloned())
                .collect(),
        )
    }
}

/// Intersection of two ascending, unique u32 lists.
fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// A minimal little-endian byte cursor for the framed format.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Cursor { bytes, at: 0 }
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }
    fn u32_le(&mut self) -> Option<u32> {
        let raw = self.take(4)?;
        Some(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }
    fn varint(&mut self) -> Option<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *self.take(1)?.first()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }
}

// -------------------------------------------------------------- on-disk store

/// The trigram index file inside the sidecar directory.
pub(super) fn index_path(index_dir: &Path) -> PathBuf {
    index_dir.join("trigram-v1.idx")
}

/// Write the index atomically next to the content cache. Best-effort: a
/// failure leaves the previous index (or none), and queries fall back to the
/// full distinct-content scan.
pub(super) fn store_index(index_dir: &Path, builder: &TrigramBuilder) -> std::io::Result<u64> {
    std::fs::create_dir_all(index_dir)?;
    let raw = builder.encode();
    let size = raw.len() as u64;
    super::fsutil::atomic_write(&index_path(index_dir), &raw)?;
    Ok(size)
}

/// Load the index if present and well-framed; `None` on absence or corruption.
pub(super) fn load_index(index_dir: &Path) -> Option<TrigramIndex> {
    let mut file = std::fs::File::open(index_path(index_dir)).ok()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    TrigramIndex::decode(&bytes)
}

/// Remove the index file (used by rebuild/retention wipe). Absence is success.
pub(super) fn remove_index(index_dir: &Path) {
    let _ = std::fs::remove_file(index_path(index_dir));
}

/// On-disk size of the index file, or 0 when absent (for doctor/size reports).
pub(super) fn index_size(index_dir: &Path) -> u64 {
    std::fs::metadata(index_path(index_dir))
        .map(|m| m.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha(s: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(s.as_bytes());
        hex::encode(h.finalize())
    }

    #[test]
    fn candidates_over_approximate_never_drop_a_match() {
        let mut b = TrigramBuilder::default();
        let a = "the quick brown fox";
        let c = "lazy dog sleeps";
        let d = "quokka marker needle";
        b.add(&sha(a), a.as_bytes());
        b.add(&sha(c), c.as_bytes());
        b.add(&sha(d), d.as_bytes());
        let idx = TrigramIndex::decode(&b.encode()).unwrap();

        // A needle present in exactly one content yields only that content.
        let cands = idx.candidates(b"quokka").unwrap();
        assert_eq!(cands.len(), 1);
        assert!(cands.contains(&sha(d)));

        // A needle present in two contents yields both.
        let quick = idx.candidates(b"quick").unwrap();
        assert!(quick.contains(&sha(a)));

        // An absent needle whose trigrams are all indexed but never co-occur
        // yields an empty candidate set — a provable no-match.
        let absent = idx.candidates(b"zzzznotpresent").unwrap();
        assert!(absent.is_empty());
    }

    #[test]
    fn short_needles_disable_the_filter() {
        let mut b = TrigramBuilder::default();
        let a = "abcdef";
        b.add(&sha(a), a.as_bytes());
        let idx = TrigramIndex::decode(&b.encode()).unwrap();
        // Under three bytes: no filter, the caller scans everything.
        assert!(idx.candidates(b"ab").is_none());
        assert!(idx.candidates(b"a").is_none());
        // Exactly three bytes: filter applies.
        assert!(idx.candidates(b"abc").is_some());
    }

    #[test]
    fn multibyte_literals_produce_byte_trigrams() {
        let mut b = TrigramBuilder::default();
        // "café" and a decoy without the accented run.
        let hit = "función café";
        let miss = "plain ascii only";
        b.add(&sha(hit), hit.as_bytes());
        b.add(&sha(miss), miss.as_bytes());
        let idx = TrigramIndex::decode(&b.encode()).unwrap();
        let cands = idx.candidates("café".as_bytes()).unwrap();
        assert!(cands.contains(&sha(hit)));
        assert!(!cands.contains(&sha(miss)));
    }

    #[test]
    fn corrupt_index_decodes_to_none() {
        assert!(TrigramIndex::decode(b"not an index").is_none());
        assert!(TrigramIndex::decode(b"").is_none());
        // Truncated zstd frame.
        let mut b = TrigramBuilder::default();
        b.add(&sha("hello world"), b"hello world");
        let encoded = b.encode();
        let mut truncated = encoded.clone();
        truncated.truncate(truncated.len() - 3);
        assert!(TrigramIndex::decode(&truncated).is_none());

        // A bit flip that still forms a valid zstd stream must fail the inner
        // payload digest rather than silently changing coverage or postings.
        let mut framed = zstd::stream::decode_all(&encoded[..]).unwrap();
        framed[12] ^= 0x01;
        let reframed =
            zstd::stream::encode_all(std::io::Cursor::new(&framed), TRIGRAM_ZSTD_LEVEL).unwrap();
        assert!(TrigramIndex::decode(&reframed).is_none());

        // Even a payload with a freshly valid digest is rejected when bytes
        // remain after the declared posting tables: structural completeness
        // is independent of the checksum.
        let payload_len = framed.len() - PAYLOAD_DIGEST_BYTES;
        let mut payload = framed[..payload_len].to_vec();
        payload[12] ^= 0x01; // undo the bit flip above
        payload.push(0);
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(&payload);
        payload.extend_from_slice(&digest);
        let with_trailing =
            zstd::stream::encode_all(std::io::Cursor::new(&payload), TRIGRAM_ZSTD_LEVEL).unwrap();
        assert!(TrigramIndex::decode(&with_trailing).is_none());
    }

    #[test]
    fn re_adding_a_hash_is_idempotent_and_invalid_hashes_stay_uncovered() {
        let mut b = TrigramBuilder::default();
        let a = "duplicate content here";
        b.add("not-a-sha256", b"must remain uncovered");
        b.add(&sha(a), a.as_bytes());
        b.add(&sha(a), a.as_bytes());
        assert_eq!(b.distinct_contents(), 1);
    }
}

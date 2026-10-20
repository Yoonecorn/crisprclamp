use super::{ExpectedPairFilter, PairId};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

// ceil(-ln(0.001) / ln(2)^2) = 15 bits per pair. Ten probes yield
// (1 - exp(-10/15))^10 ~= 0.000745, below the 0.1% target.
// Round up to a power of two for division-free indexing (never fewer bits).
const BITS_PER_PAIR: usize = 15;
const PROBES: u32 = 10;
// Tiny arrays expose double-hashing correlations; keep at least 2048 bits
// so small libraries also satisfy the empirical 0.1% false-positive gate.
const MIN_WORDS: usize = 32;

/// Immutable pair pre-filter. Positive answers require the exact oracle.
/// Construction and queries use identical full-width ordered pair keys.
pub struct BloomFilter {
    words: Box<[u64]>,
    bit_count: u64,
    diagnostics: Option<Diagnostics>,
}
struct Diagnostics {
    path: PathBuf,
    queries: AtomicU64,
    rejections: AtomicU64,
}
// SplitMix64 finalizer with two seeds supplies double hashing. Wrapping
// arithmetic is intentional; the odd step prevents a zero hash increment.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn hashes(a: u32, b: u32) -> (u64, u64) {
    let key = (u64::from(a) << 32) | u64::from(b);
    (
        mix(key ^ 0x243f6a8885a308d3),
        mix(key ^ 0x13198a2e03707344) | 1,
    )
}
impl BloomFilter {
    fn word_count(pair_count: usize) -> usize {
        // Admission bounds cardinality to 50M rows. Checked arithmetic also
        // protects the sizing calculation on smaller address-width targets.
        pair_count
            .checked_mul(BITS_PER_PAIR)
            .expect("bounded pair cardinality")
            .div_ceil(64)
            .max(MIN_WORDS)
            .checked_next_power_of_two()
            .expect("bounded bit array")
    }
    pub fn byte_len_for_pairs(pair_count: usize) -> usize {
        Self::word_count(pair_count)
            .checked_mul(8)
            .expect("bounded byte array")
    }
    /// Disable pre-filtering while retaining the exact pair oracle.
    pub fn bypass() -> Self {
        Self {
            words: Box::new([]),
            bit_count: 0,
            diagnostics: None,
        }
    }
    pub fn from_pairs(pairs: &HashSet<PairId>) -> Self {
        let words = Self::word_count(pairs.len());
        let mut filter = Self {
            words: vec![0; words].into_boxed_slice(),
            bit_count: words as u64 * 64,
            diagnostics: std::env::var_os("PROJECT_C_BLOOM_PROFILE_PATH").map(|path| Diagnostics {
                path: PathBuf::from(path),
                queries: AtomicU64::new(0),
                rejections: AtomicU64::new(0),
            }),
        };
        for &(a, b) in pairs {
            let (mut hash, step) = hashes(a, b);
            for _ in 0..PROBES {
                let bit = hash & (filter.bit_count - 1);
                filter.words[(bit / 64) as usize] |= 1u64 << (bit % 64);
                hash = hash.wrapping_add(step);
            }
        }
        filter
    }
    pub fn byte_len(&self) -> usize {
        self.words.len() * 8
    }
    pub(super) fn write_diagnostics(&self) -> super::Result<()> {
        if let Some(d) = &self.diagnostics {
            let queries = d.queries.load(Ordering::Relaxed);
            let rejected = d.rejections.load(Ordering::Relaxed);
            let value = serde_json::json!({
                "filter_bytes": self.byte_len(), "hash_probes": PROBES,
                "primary_pair_queries": queries, "early_rejections": rejected,
                "exact_checks": queries - rejected,
                "rejection_percent": if queries == 0 { 0.0 } else { 100.0 * rejected as f64 / queries as f64 }
            });
            std::fs::write(&d.path, value.to_string())
                .map_err(|e| format!("diagnostic.bloom_write: {e}"))?;
        }
        Ok(())
    }
}
impl ExpectedPairFilter for BloomFilter {
    fn might_contain(&self, a_idx: u32, b_idx: u32) -> bool {
        if self.words.is_empty() {
            return true;
        }
        let (mut hash, step) = hashes(a_idx, b_idx);
        let mut possible = true;
        for _ in 0..PROBES {
            let bit = hash & (self.bit_count - 1);
            if self.words[(bit / 64) as usize] & (1u64 << (bit % 64)) == 0 {
                possible = false;
                break;
            }
            hash = hash.wrapping_add(step);
        }
        if let Some(d) = &self.diagnostics {
            d.queries.fetch_add(1, Ordering::Relaxed);
            if !possible {
                d.rejections.fetch_add(1, Ordering::Relaxed);
            }
        }
        possible
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bypass_accepts_every_pair_without_allocating_filter_words() {
        let filter = BloomFilter::bypass();
        assert_eq!(filter.byte_len(), 0);
        assert_eq!(filter.bit_count, 0);
        for pair in [(0, 0), (0, u32::MAX), (u32::MAX, 0), (u32::MAX, u32::MAX)] {
            assert!(filter.might_contain(pair.0, pair.1));
        }
    }
    #[test]
    fn exhaustive_declared_pairs_and_full_width_keys_have_no_false_negatives() {
        let mut pairs: HashSet<_> = (0u32..8192).map(|i| (i, i.wrapping_mul(7919))).collect();
        pairs.extend([(0, 0), (0, u32::MAX), (u32::MAX, 0), (u32::MAX, u32::MAX)]);
        let filter = BloomFilter::from_pairs(&pairs);
        for &(a, b) in &pairs {
            assert!(
                filter.might_contain(a, b),
                "STOP_FALSE_NEGATIVE_DETECTED: {a}/{b}"
            );
        }
        let rebuilt = BloomFilter::from_pairs(&pairs.iter().copied().collect());
        assert_eq!(filter.words, rebuilt.words);
    }
    #[test]
    fn random_non_library_pairs_false_positive_rate_within_point_one_percent() {
        for size in [1, 8, 64, 1024, 8192] {
            let pairs: HashSet<_> = (0..size).map(|i| (i, i * 7919)).collect();
            let filter = BloomFilter::from_pairs(&pairs);
            assert_eq!(
                filter.byte_len(),
                BloomFilter::byte_len_for_pairs(pairs.len())
            );
            let mut rng = 0x8a5cd789635d2dffu64;
            let mut samples = 0;
            let mut positives = 0;
            while samples < 100_000 {
                rng = mix(rng.wrapping_add(0x9e3779b97f4a7c15));
                let pair = ((rng >> 32) as u32, rng as u32);
                if pairs.contains(&pair) {
                    continue;
                }
                samples += 1;
                positives += usize::from(filter.might_contain(pair.0, pair.1));
            }
            eprintln!("Bloom n={size}: {positives}/{samples} false positives");
            assert!(
                positives <= samples / 1000,
                "FPR exceeds 0.1%: {positives}/{samples}"
            );
        }
    }
    #[test]
    fn empty_library_and_production_size_are_bounded() {
        let empty = BloomFilter::from_pairs(&HashSet::new());
        assert_eq!(empty.byte_len(), BloomFilter::byte_len_for_pairs(0));
        assert!(!empty.might_contain(0, 0));
        assert!(!empty.might_contain(u32::MAX, u32::MAX));
        let pairs: HashSet<_> = (0..1_044_484).map(|i| (i, i)).collect();
        let filter = BloomFilter::from_pairs(&pairs);
        assert_eq!(filter.byte_len(), 2_097_152);
        assert_eq!(
            filter.byte_len(),
            BloomFilter::byte_len_for_pairs(pairs.len())
        );
        let mut rng = 0x123456789abcdef0u64;
        let mut positives = 0;
        let mut samples = 0;
        while samples < 100_000 {
            rng = mix(rng.wrapping_add(0x9e3779b97f4a7c15));
            let pair = ((rng >> 32) as u32, rng as u32);
            if pairs.contains(&pair) {
                continue;
            }
            samples += 1;
            positives += usize::from(filter.might_contain(pair.0, pair.1));
        }
        eprintln!("Bloom n=1044484: {positives}/{samples} false positives");
        assert!(positives <= samples / 1000, "production FPR exceeds 0.1%");
        for &(a, b) in &pairs {
            assert!(filter.might_contain(a, b), "STOP_FALSE_NEGATIVE_DETECTED");
        }
    }
}

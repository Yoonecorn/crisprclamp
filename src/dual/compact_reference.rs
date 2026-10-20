//! Versioned descriptor for the frozen headerless ordered u32le pair stream.
//! Standard validates structure and ordered indices without cryptographic identity.
use super::*;
use serde::Deserialize;

const SCHEMA: &str = "cclamp.ordered-pairs-u32le.v2";
const LEGACY_SCHEMA: &str = "cclamp.ordered-pairs-u32le.v1";
const MAX_DESCRIPTOR_BYTES: u64 = 65_536;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    schema: String,
    reference: String,
    pair_count: u64,
    first_pair_ordinal: u64,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    guides_a_sha256: Option<String>,
    #[serde(default)]
    guides_b_sha256: Option<String>,
    self_pairs: SelfPairs,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SelfPairs {
    Allow,
    Reject,
}

pub(super) struct Metadata {
    pub(super) raw_path: PathBuf,
    pub(super) pair_count: usize,
    pub(super) first_pair_ordinal: u64,
    pub(super) reference_bytes: u64,
    self_pairs: SelfPairs,
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Parse a small strict descriptor. Raw data remains in its original immutable
/// location; both absolute and descriptor-relative paths are accepted.
pub(super) fn metadata(path: &Path) -> Result<Metadata> {
    let file = File::open(path).map_err(|e| format!("preflight.compact_descriptor_open: {e}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_DESCRIPTOR_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("preflight.compact_descriptor_read: {e}"))?;
    if bytes.len() as u64 > MAX_DESCRIPTOR_BYTES {
        return Err("preflight.compact_descriptor_size".into());
    }
    // Typed deserialization rejects duplicate and unknown fields, negative or
    // fractional integers, unsupported self policies, and trailing JSON data.
    let d: Descriptor =
        serde_json::from_slice(&bytes).map_err(|e| format!("preflight.compact_descriptor: {e}"))?;
    if d.schema != SCHEMA && d.schema != LEGACY_SCHEMA {
        return Err("preflight.compact_reference_version".into());
    }
    let identities = [&d.sha256, &d.guides_a_sha256, &d.guides_b_sha256];
    // Accept legacy descriptor syntax, but never claim its digests were checked.
    if d.reference.is_empty()
        || (d.schema == LEGACY_SCHEMA && identities.iter().any(|v| !v.as_deref().is_some_and(valid_sha)))
        || (d.schema == SCHEMA && identities.iter().any(|v| v.is_some()))
    {
        return Err("preflight.compact_descriptor_identity".into());
    }
    let pair_count = usize::try_from(d.pair_count).map_err(|_| "resource.dimension_overflow")?;
    if pair_count > MAX_MEMBERSHIP_ROWS {
        return Err("STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX".into());
    }
    d.first_pair_ordinal
        .checked_add(d.pair_count)
        .ok_or("resource.dimension_overflow")?;
    let reference_bytes = d
        .pair_count
        .checked_mul(8)
        .ok_or("resource.dimension_overflow")?;
    let raw = PathBuf::from(d.reference);
    let raw_path = if raw.is_absolute() {
        raw
    } else {
        path.parent().unwrap_or_else(|| Path::new(".")).join(raw)
    };
    let result = Metadata {
        raw_path,
        pair_count,
        first_pair_ordinal: d.first_pair_ordinal,
        reference_bytes,
        self_pairs: d.self_pairs,
    };
    validate_size(&result)?;
    Ok(result)
}

fn validate_size(meta: &Metadata) -> Result<()> {
    let size = fs::metadata(&meta.raw_path)
        .map_err(|e| format!("preflight.compact_reference_open: {e}"))?
        .len();
    if size != meta.reference_bytes {
        return Err("preflight.compact_reference_size".into());
    }
    Ok(())
}

/// Validate physical length without rereading or hashing file contents.
pub(super) fn validate(meta: &Metadata) -> Result<()> {
    validate_size(meta)
}

/// Allocation-free pair iteration in source row order. Duplicate pair detection
/// remains in the caller's existing exact membership insertion. "Self" means
/// equal guide sequence across roles, independently of role-specific indices.
pub(super) fn stream_pairs(
    meta: &Metadata,
    a: &[Guide],
    b: &[Guide],
    mut visit: impl FnMut(u64, u32, u32) -> Result<()>,
) -> Result<()> {
    validate_size(meta)?;
    let mut reader = BufReader::new(
        File::open(&meta.raw_path).map_err(|e| format!("preflight.compact_reference_open: {e}"))?,
    );
    let mut buffer = [0_u8; 65_536];
    let mut row = 0_usize;
    while row < meta.pair_count {
        let records = (meta.pair_count - row).min(buffer.len() / 8);
        let block = &mut buffer[..records * 8];
        reader
            .read_exact(block)
            .map_err(|e| format!("preflight.compact_reference_record: {e}"))?;
        for record in block.chunks_exact(8) {
            let ai = u32::from_le_bytes(record[..4].try_into().unwrap());
            let bi = u32::from_le_bytes(record[4..].try_into().unwrap());
            let ag = a.get(ai as usize).ok_or("preflight.compact_a_index")?;
            let bg = b.get(bi as usize).ok_or("preflight.compact_b_index")?;
            if matches!(meta.self_pairs, SelfPairs::Reject) && ag.sequence == bg.sequence {
                return Err("preflight.compact_self_pair".into());
            }
            let ordinal = meta
                .first_pair_ordinal
                .checked_add(u64::try_from(row).map_err(|_| "resource.dimension_overflow")?)
                .ok_or("resource.dimension_overflow")?;
            visit(ordinal, ai, bi)?;
            row = row.checked_add(1).ok_or("resource.dimension_overflow")?;
        }
    }
    let mut suffix = [0_u8; 1];
    if reader
        .read(&mut suffix)
        .map_err(|e| format!("preflight.compact_reference_read: {e}"))?
        != 0
    {
        return Err("preflight.compact_reference_suffix".into());
    }
    Ok(())
}

/// Compatibility adapter for historical JSONL and scientific provenance.
pub(super) fn stream(
    meta: &Metadata,
    a: &[Guide],
    b: &[Guide],
    mut visit: impl FnMut(Construct) -> Result<()>,
) -> Result<()> {
    stream_pairs(meta, a, b, |ordinal, ai, bi| {
        visit(Construct {
            id: format!("pcpair-{ordinal:020}"),
            a: ai as usize,
            b: bi as usize,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture {
        root: PathBuf,
    }
    impl Fixture {
        fn new(records: &[(u32, u32)]) -> Self {
            let root = env::temp_dir().join(format!(
                "cclamp-compact-ref-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            let raw: Vec<u8> = records
                .iter()
                .flat_map(|(a, b)| a.to_le_bytes().into_iter().chain(b.to_le_bytes()))
                .collect();
            fs::write(root.join("reference.bin"), raw).unwrap();
            fs::write(root.join("a.tsv"), b"a\tAAAA\nb\tCCCC\n").unwrap();
            fs::write(root.join("b.tsv"), b"a\tAAAA\nb\tCCCC\n").unwrap();
            let value = json!({"schema":SCHEMA,"reference":"reference.bin","pair_count":records.len(),
                "first_pair_ordinal":42,"self_pairs":"reject"});
            fs::write(
                root.join("descriptor.json"),
                serde_json::to_vec(&value).unwrap(),
            )
            .unwrap();
            Self { root }
        }
        fn meta(&self) -> Result<Metadata> {
            metadata(&self.root.join("descriptor.json"))
        }
        fn edit(&self, f: impl FnOnce(&mut Value)) {
            let path = self.root.join("descriptor.json");
            let mut v: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            f(&mut v);
            fs::write(path, serde_json::to_vec(&v).unwrap()).unwrap();
        }
        fn guides(&self) -> Vec<Guide> {
            load_guides(&self.root.join("a.tsv")).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn valid_order_identity_and_empty() {
        let f = Fixture::new(&[(1, 0), (0, 1)]);
        let m = f.meta().unwrap();
        validate(&m).unwrap();
        let g = f.guides();
        let mut rows = Vec::new();
        stream(&m, &g, &g, |c| {
            rows.push((c.id, c.a, c.b));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("pcpair-00000000000000000042".into(), 1, 0),
                ("pcpair-00000000000000000043".into(), 0, 1)
            ]
        );
        let empty = Fixture::new(&[]);
        let m = empty.meta().unwrap();
        stream_pairs(&m, &g, &g, |_, _, _| panic!("empty stream")).unwrap();
    }

    #[test]
    fn strict_descriptor_and_checked_dimensions() {
        for field in [
            "schema",
            "pair_count",
            "first_pair_ordinal",
            "self_pairs",
            "extra",
        ] {
            let f = Fixture::new(&[]);
            f.edit(|v| match field {
                "schema" => v[field] = json!("cclamp.ordered-pairs-u32le.v999"),
                "pair_count" => v[field] = json!(50_000_001),
                "first_pair_ordinal" => {
                    v[field] = json!(u64::MAX);
                    v["pair_count"] = json!(1);
                }
                "self_pairs" => v[field] = json!("unknown"),
                _ => v[field] = json!(true),
            });
            assert!(f.meta().is_err(), "{field}");
        }
        let f = Fixture::new(&[]);
        let path = f.root.join("descriptor.json");
        let mut bytes = fs::read(&path).unwrap();
        bytes.pop();
        bytes.extend_from_slice(b",\"pair_count\":0}");
        fs::write(path, bytes).unwrap();
        assert!(f.meta().is_err());
    }

    #[test]
    fn truncation_and_trailing_bytes_fail_without_hashes() {
        for raw in [vec![0_u8; 7], vec![0_u8; 9]] {
            let f = Fixture::new(&[(0, 1)]);
            fs::write(f.root.join("reference.bin"), raw).unwrap();
            assert!(f.meta().is_err());
        }
    }

    #[test]
    fn legacy_digest_syntax_is_accepted_but_not_used_as_identity() {
        let f = Fixture::new(&[(0, 1)]);
        f.edit(|v| {
            v["schema"] = json!(LEGACY_SCHEMA);
            for key in ["sha256", "guides_a_sha256", "guides_b_sha256"] {
                v[key] = json!("0".repeat(64));
            }
        });
        let m = f.meta().unwrap();
        validate(&m).unwrap();
        let g = f.guides();
        stream_pairs(&m, &g, &g, |_, _, _| Ok(())).unwrap();
        f.edit(|v| v["sha256"] = json!("invalid"));
        assert!(f.meta().is_err());
    }

    #[test]
    fn same_length_structural_changes_follow_current_reference_bytes() {
        let f = Fixture::new(&[(0, 1)]);
        let m = f.meta().unwrap();
        let changed: Vec<u8> = 1_u32.to_le_bytes().into_iter().chain(0_u32.to_le_bytes()).collect();
        fs::write(f.root.join("reference.bin"), changed).unwrap();
        let g = f.guides();
        let mut rows = Vec::new();
        stream_pairs(&m, &g, &g, |_, a, b| { rows.push((a,b)); Ok(()) }).unwrap();
        assert_eq!(rows, vec![(1,0)]);
    }

    #[test]
    fn invalid_indices_self_policy_and_duplicate_pair_signal() {
        for pair in [(2, 0), (0, 2), (0, 0)] {
            let f = Fixture::new(&[pair]);
            let g = f.guides();
            assert!(stream_pairs(&f.meta().unwrap(), &g, &g, |_, _, _| Ok(())).is_err());
        }
        let f = Fixture::new(&[(0, 0)]);
        f.edit(|v| v["self_pairs"] = json!("allow"));
        let g = f.guides();
        stream_pairs(&f.meta().unwrap(), &g, &g, |_, _, _| Ok(())).unwrap();
        let f = Fixture::new(&[(0, 1), (0, 1)]);
        let g = f.guides();
        let mut pairs = HashSet::new();
        let fault = stream_pairs(&f.meta().unwrap(), &g, &g, |_, a, b| {
            if !pairs.insert((a, b)) {
                return Err("preflight.duplicate_pair".into());
            }
            Ok(())
        })
        .unwrap_err();
        assert_eq!(fault, "preflight.duplicate_pair");
    }
}

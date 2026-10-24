use super::*;
use crate::discovery::{Discovery, DiscoveryMode};
use crate::evidence::{EvidenceMode, EvidenceSink};
use crate::resource::{Dimensions, Limits, OutputBudget, SparseDimensions};
use std::io::Read;

const PROFILE: &str = "project-c.c2b-json.v1";
const CONTRACT: &str = "project-c.c2a.v1";
const CONCORDANCE_CONTRACT: &str = "project-c.gse116198-concordance.v1";
pub(super) const CONCORDANCE_PROFILE: &str = "project-c.gse116198-concordance-json.v1";
const CONCORDANCE_QC: &str = "project-c.sample-qc.concordance.v1";
const CONCORDANCE_TERMINAL: &str = "project-c.terminal-totals.concordance.v1";
pub(super) const CONCORDANCE_EVIDENCE: &str = "project-c.evidence.concordance.v1";
const CONCORDANCE_PROVENANCE: &str = "project-c.scientific-provenance.concordance.v1";
pub(super) const RADIUS1_PROFILE: &str = "project-c.gse116198-concordance-radius1-json.v1";
pub(super) const RADIUS1_EVIDENCE: &str = "project-c.evidence.concordance-radius1.v1";
const RADIUS1_CONTRACT: &str = "project-c.gse116198-concordance-radius1.v1";
const RADIUS1_QC: &str = "project-c.sample-qc.concordance-radius1.v1";
const RADIUS1_TERMINAL: &str = "project-c.terminal-totals.concordance-radius1.v1";
const RADIUS1_PROVENANCE: &str = "project-c.scientific-provenance.concordance-radius1.v1";
fn radius1(cfg: &DualConfig) -> bool {
    cfg.barcode.as_ref().is_some_and(|b| b.radius == 1)
}
fn concordance_profile(cfg: &DualConfig) -> &'static str {
    if radius1(cfg) {
        RADIUS1_PROFILE
    } else {
        CONCORDANCE_PROFILE
    }
}
fn concordance_contract(cfg: &DualConfig) -> &'static str {
    if radius1(cfg) {
        RADIUS1_CONTRACT
    } else {
        CONCORDANCE_CONTRACT
    }
}
#[derive(Serialize)]
struct Radius1Qc {
    barcode_radius: u8,
    radius1_ambiguous: u64,
    radius1_rescued: u64,
}
const MAX_LINE: usize = 1_048_576;
const DUAL_LEAVES: [&str; 12] = [
    "mapped.expected",
    "recombinant.unexpected",
    "ambiguous.component",
    "ambiguous.layout",
    "unmapped.layout",
    "unmapped.A_known",
    "unmapped.B_known",
    "unmapped.neither",
    "invalid.alphabet",
    "invalid.extraction",
    "invalid.N",
    "invalid.quality",
];
const DISCOVERY_LEAVES: [usize; 5] = [1, 4, 5, 6, 7];

#[derive(Clone, Copy)]
enum Source {
    R1,
    R2,
}
struct ComponentConfig {
    source: Source,
    geometry: Geometry,
    orientation: Orientation,
}
struct DualConfig {
    #[allow(dead_code)] // Accepted CLI opt-out; dual/concordance always use CPU.
    no_gpu: bool,
    execution_profile: ExecutionProfile,
    sample: String,
    r1: PathBuf,
    r2: Option<PathBuf>,
    barcode: Option<BarcodeConfig>,
    guides_a: PathBuf,
    guides_b: PathBuf,
    constructs: PathBuf,
    compact_reference: Option<compact_reference::Metadata>,
    compact_output: bool,
    a: ComponentConfig,
    b: ComponentConfig,
    radius: u8,
    min_phred: Option<u8>,
    discovery_mode: DiscoveryMode,
    evidence: EvidenceMode,
    workers: WorkerConfig,
    limits: Limits,
    output: PathBuf,
}
type PairId = (u32, u32);
const MAX_MEMBERSHIP_ROWS: usize = 50_000_000;

/// Conservative filter hook. Exact membership remains a separate check.
pub trait ExpectedPairFilter: Sync + Send {
    fn might_contain(&self, a_idx: u32, b_idx: u32) -> bool;
}

mod bloom;
mod compact_reference;
mod compact_output;
pub use bloom::BloomFilter;

// Keep the immutable exact oracle and its conservative pre-filter together.
// Layout adjudication continues to use the exact set via Deref.
struct ExpectedPairs {
    exact: HashSet<PairId>,
    filter: BloomFilter,
}
impl ExpectedPairs {
    fn new(exact: HashSet<PairId>) -> Self {
        let filter = if std::env::var("PROJECT_C_BLOOM_BYPASS").as_deref() == Ok("1") {
            BloomFilter::bypass()
        } else {
            BloomFilter::from_pairs(&exact)
        };
        Self { exact, filter }
    }
}
impl std::ops::Deref for ExpectedPairs {
    type Target = HashSet<PairId>;
    fn deref(&self) -> &Self::Target {
        &self.exact
    }
}

struct Construct {
    id: String,
    a: usize,
    b: usize,
}
#[derive(Default)]
struct DualQc {
    concordant: u64,
    discordant_triplet: u64,
    unmapped_barcode: u64,
    radius1_rescued: u64,
    radius1_ambiguous: u64,
    components: [[u64; 4]; 2],
    extraction: [u64; 3],
    mapped_distance: [u64; 4],
    recombinant_distance: [u64; 4],
}

fn required(values: &HashMap<String, String>, key: &str) -> Result<String> {
    values
        .get(key)
        .cloned()
        .ok_or_else(|| format!("preflight.missing_option: {key}"))
}

fn component(
    values: &HashMap<String, String>,
    role: &str,
    paired: bool,
) -> Result<ComponentConfig> {
    let key = |suffix: &str| format!("--{role}-{suffix}");
    let source = match values
        .get(&key("source"))
        .map(String::as_str)
        .unwrap_or("R1")
    {
        "R1" => Source::R1,
        "R2" if paired => Source::R2,
        "R2" => return Err("preflight.missing_r2".into()),
        _ => return Err("preflight.invalid_source".into()),
    };
    let geometry = if let Some(anchor) = values.get(&key("anchor")) {
        if values.contains_key(&key("start")) {
            return Err("preflight.conflicting_geometry".into());
        }
        let sequence = anchor.to_ascii_uppercase().into_bytes();
        if sequence.is_empty()
            || !sequence
                .iter()
                .all(|b| matches!(b, b'A' | b'C' | b'G' | b'T'))
        {
            return Err("preflight.anchor_alphabet".into());
        }
        let search_start = required(values, &key("search-start"))?
            .parse::<usize>()
            .map_err(|_| "preflight.invalid_search_start")?;
        let search_end = required(values, &key("search-end"))?
            .parse::<usize>()
            .map_err(|_| "preflight.invalid_search_end")?;
        let displacement = required(values, &key("displacement"))?
            .parse::<isize>()
            .map_err(|_| "preflight.invalid_displacement")?;
        if search_start > search_end || search_end > MAX_LINE || sequence.len() > MAX_LINE {
            return Err("preflight.invalid_anchor_bounds".into());
        }
        Geometry::Anchor {
            sequence,
            search_start,
            search_end,
            displacement,
        }
    } else {
        if ["search-start", "search-end", "displacement"]
            .iter()
            .any(|s| values.contains_key(&key(s)))
        {
            return Err("preflight.incomplete_anchor_geometry".into());
        }
        Geometry::Fixed {
            start: required(values, &key("start"))?
                .parse::<usize>()
                .map_err(|_| "preflight.invalid_start")?,
        }
    };
    let orientation = match values
        .get(&key("orientation"))
        .map(String::as_str)
        .unwrap_or("forward")
    {
        "forward" => Orientation::Forward,
        "reverse_complement" => Orientation::ReverseComplement,
        _ => return Err("preflight.invalid_orientation".into()),
    };
    Ok(ComponentConfig {
        source,
        geometry,
        orientation,
    })
}

fn parse() -> Result<DualConfig> {
    let mut args = env::args_os();
    let _ = args.next();
    let mut values = HashMap::new();
    while let Some(flag) = args.next() {
        let flag = flag
            .into_string()
            .map_err(|_| "preflight.non_utf8_argument")?;
        if matches!(flag.as_str(), "--no-gpu" | "--experimental-metal") {
            if values.insert(flag.clone(), "true".into()).is_some() {
                return Err(format!("preflight.duplicate_option: {flag}"));
            }
            continue;
        }
        let accepted = [
            "--execution-profile",
            "--mode",
            "--sample",
            "--fastq",
            "--fastq-r2",
            "--fastq-r3",
            "--barcode-whitelist",
            "--barcode-window",
            "--barcode-radius",
            "--guides-a",
            "--guides-b",
            "--constructs",
            "--construct-format",
            "--output-format",
            "--radius",
            "--min-phred",
            "--discovery",
            "--discovery-k",
            "--exact-distinct-key-limit",
            "--evidence",
            "--evidence-seed",
            "--evidence-capacity",
            "--max-record-bytes",
            "--max-header-bytes",
            "--memory-budget-bytes",
            "--output-budget-bytes",
            "--output",
            "--workers",
            "--batch-records",
            "--work-queue-capacity",
            "--result-queue-capacity",
            "--a-source",
            "--b-source",
            "--a-start",
            "--b-start",
            "--a-anchor",
            "--b-anchor",
            "--a-search-start",
            "--b-search-start",
            "--a-search-end",
            "--b-search-end",
            "--a-displacement",
            "--b-displacement",
            "--a-orientation",
            "--b-orientation",
        ];
        if !accepted.contains(&flag.as_str()) {
            return Err(format!("preflight.unsupported_option: {flag}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("preflight.missing_value: {flag}"))?
            .into_string()
            .map_err(|_| "preflight.non_utf8_argument")?;
        if values.insert(flag.clone(), value).is_some() {
            return Err(format!("preflight.duplicate_option: {flag}"));
        }
    }
    if required(&values, "--mode")? != "dual" {
        return Err("preflight.unsupported_mode".into());
    }
    let no_gpu = metal_policy(&values)?;
    let execution_profile = ExecutionProfile::parse(&values)?;
    let sample = required(&values, "--sample")?;
    if sample.is_empty() {
        return Err("preflight.empty_sample_id".into());
    }
    let r1 = PathBuf::from(required(&values, "--fastq")?);
    let r2 = values.get("--fastq-r2").map(PathBuf::from);
    let a = component(&values, "a", r2.is_some())?;
    let b = component(&values, "b", r2.is_some())?;
    let radius = match values.get("--radius").map(String::as_str).unwrap_or("0") {
        "0" => 0,
        "1" => 1,
        _ => return Err("preflight.unsupported_radius".into()),
    };
    let min_phred = values
        .get("--min-phred")
        .map(|v| v.parse::<u8>().map_err(|_| "preflight.invalid_min_phred"))
        .transpose()?;
    if min_phred.is_some_and(|n| n > 93) {
        return Err("preflight.invalid_min_phred".into());
    }
    let discovery_mode_str = values
        .get("--discovery")
        .map(String::as_str)
        .unwrap_or("bounded");
    let discovery_mode = match discovery_mode_str {
        "bounded" => {
            if values.contains_key("--exact-distinct-key-limit") {
                return Err("preflight.conflicting_discovery_options".into());
            }
            let discovery_k = values.get("--discovery-k").map_or(Ok(100_u64), |v| {
                v.parse::<u64>()
                    .map_err(|_| "preflight.invalid_discovery_k")
            })?;
            DiscoveryMode::Bounded(
                usize::try_from(discovery_k).map_err(|_| "preflight.invalid_discovery_k")?,
            )
        }
        "exact" => {
            if values.contains_key("--discovery-k") {
                return Err("preflight.conflicting_discovery_options".into());
            }
            let limit_str = values
                .get("--exact-distinct-key-limit")
                .ok_or("preflight.missing_exact_limit")?;
            let limit = limit_str
                .parse::<u64>()
                .map_err(|_| "preflight.invalid_exact_limit")?;
            if limit == 0 {
                return Err("preflight.invalid_exact_limit".into());
            }
            DiscoveryMode::Exact(
                usize::try_from(limit).map_err(|_| "preflight.invalid_exact_limit")?,
            )
        }
        _ => return Err("preflight.unsupported_discovery".into()),
    };
    let evidence = EvidenceMode::parse(&values)?;
    let workers = WorkerConfig::parse(&values, &evidence)?;
    let barcode = if let Some(whitelist) = values.get("--barcode-whitelist") {
        if r2.is_none() || !matches!(a.source, Source::R1) || !matches!(b.source, Source::R2) {
            return Err("preflight.concordance_requires_r1_r2_roles".into());
        }
        let window = values
            .get("--barcode-window")
            .map(|v| {
                let (start, length) = v.split_once(',').ok_or("preflight.barcode_window")?;
                let start = start
                    .parse::<usize>()
                    .map_err(|_| "preflight.barcode_window")?;
                let length = length
                    .parse::<usize>()
                    .map_err(|_| "preflight.barcode_window")?;
                if length == 0 || start.checked_add(length).is_none_or(|end| end > MAX_LINE) {
                    return Err("preflight.barcode_window");
                }
                Ok((start, length))
            })
            .transpose()?;
        let barcode_radius = values
            .get("--barcode-radius")
            .map(|v| {
                v.parse::<u8>()
                    .map_err(|_| "preflight.barcode_radius: expected u8")
            })
            .transpose()?
            .unwrap_or(0);
        if barcode_radius > 1 {
            return Err("barcode radius > 1 not yet supported".into());
        }
        Some(BarcodeConfig {
            radius: barcode_radius,
            whitelist: PathBuf::from(whitelist),
            r3: PathBuf::from(required(&values, "--fastq-r3")?),
            window,
        })
    } else {
        if values.contains_key("--fastq-r3") || values.contains_key("--barcode-window") {
            return Err("preflight.barcode_option_without_whitelist".into());
        }
        None
    };
    let limits = Limits::parse(&values)?;
    let output = PathBuf::from(required(&values, "--output")?);
    if if execution_profile.audited() {
        output.exists()
    } else {
        fs::symlink_metadata(&output).is_ok()
    } {
        return Err("preflight.output_exists".into());
    }
    if output.file_name().is_none() {
        return Err("preflight.invalid_output".into());
    }
    let constructs = PathBuf::from(required(&values, "--constructs")?);
    let compact_reference = match values.get("--construct-format").map(String::as_str).unwrap_or("tsv") {
        "tsv" => None,
        "ordered-pairs-u32le-v1" => Some(compact_reference::metadata(&constructs)?),
        _ => return Err("preflight.unsupported_construct_format".into()),
    };
    let compact_output = match values.get("--output-format").map(String::as_str).unwrap_or("jsonl") {
        "jsonl" => false,
        "ordinal-u64le-v1" => true,
        _ => return Err("preflight.unsupported_output_format".into()),
    };
    if (compact_reference.is_some() || compact_output) && (execution_profile.audited() || barcode.is_some()) {
        return Err("preflight.compact_requires_standard_dual".into());
    }
    Ok(DualConfig {
        no_gpu,
        execution_profile,
        sample,
        r1,
        r2,
        barcode,
        guides_a: PathBuf::from(required(&values, "--guides-a")?),
        guides_b: PathBuf::from(required(&values, "--guides-b")?),
        constructs,
        compact_reference,
        compact_output,
        a,
        b,
        radius,
        min_phred,
        discovery_mode,
        evidence,
        workers,
        limits,
        output,
    })
}

impl DualConfig {
    fn stream_constructs(&self, a: &[Guide], b: &[Guide], visit: impl FnMut(Construct)->Result<()>) -> Result<()> {
        if let Some(meta) = &self.compact_reference {
            compact_reference::stream(meta, a, b, visit)
        } else { stream_constructs(&self.constructs, a, b, visit) }
    }
    fn stream_pairs(&self, a: &[Guide], b: &[Guide], mut visit: impl FnMut(u32,u32)->Result<()>) -> Result<()> {
        if let Some(meta) = &self.compact_reference {
            compact_reference::stream_pairs(meta, a, b, |_, ai, bi| visit(ai, bi))
        } else { stream_constructs(&self.constructs, a, b, |c| visit(c.a as u32,c.b as u32)) }
    }
}

/// One uniform, bounded ASCII byte window. Exact matching deliberately performs no normalization.
struct BarcodeConfig {
    radius: u8,
    whitelist: PathBuf,
    r3: PathBuf,
    window: Option<(usize, usize)>,
}
/// Two-bit keys preserve all bases, without collision-prone fingerprints.
fn compact_barcode(sequence: &[u8]) -> Option<[u8; 10]> {
    if sequence.len() > 40 {
        return None;
    }
    let mut key = [0u8; 10];
    for (i, &base) in sequence.iter().enumerate() {
        let bits = match base {
            b'A' => 0,
            b'C' => 1,
            b'G' => 2,
            b'T' => 3,
            _ => return None,
        };
        key[i / 4] |= bits << (2 * (i % 4));
    }
    Some(key)
}
fn packed_key(key: [u8; 10]) -> u128 {
    let mut bytes = [0u8; 16];
    bytes[..10].copy_from_slice(&key);
    u128::from_le_bytes(bytes)
}
enum NeighborIndex {
    Compact(Vec<u128>),
    Long(HashMap<Vec<u8>, u32>),
}
impl NeighborIndex {
    fn get(&self, window: &[u8]) -> Option<u32> {
        match self {
            Self::Compact(entries) => compact_barcode(window).and_then(|key| {
                let sequence = packed_key(key);
                let slot = entries.partition_point(|entry| (*entry >> 32) < sequence);
                entries
                    .get(slot)
                    .filter(|entry| (**entry >> 32) == sequence)
                    .map(|entry| *entry as u32)
            }),
            Self::Long(map) => map.get(window).copied(),
        }
    }
}
fn neighbor_insert<K: std::hash::Hash + Eq>(map: &mut HashMap<K, u32>, key: K, index: u32) {
    map.entry(key)
        .and_modify(|v| {
            if *v != index {
                *v = u32::MAX;
            }
        })
        .or_insert(index);
}
struct BarcodeOracle {
    neighbors: Option<NeighborIndex>,
    barcode_index: HashMap<Vec<u8>, u32>,
    barcode_to_pair: HashMap<u32, PairId>,
    start: usize,
    length: usize,
    bindings: Value,
}
#[derive(Debug, PartialEq, Eq)]
enum ConcordanceOutcome {
    Valid(PairId),
    DiscordantTriplet,
    UnmappedBarcode,
    GuideFailure,
    RescuedValid(PairId),
    RescuedDiscordant,
    AmbiguousBarcode,
}
impl BarcodeOracle {
    fn load(
        cfg: &BarcodeConfig,
        a: &[Guide],
        b: &[Guide],
        audited: bool,
        remaining_memory: u64,
    ) -> Result<Self> {
        let a_ids: HashMap<&str, u32> = a
            .iter()
            .enumerate()
            .map(|(i, g)| (g.id.as_str(), i as u32))
            .collect();
        let b_ids: HashMap<&str, u32> = b
            .iter()
            .enumerate()
            .map(|(i, g)| (g.id.as_str(), i as u32))
            .collect();
        let mut reader = BufReader::new(
            File::open(&cfg.whitelist).map_err(|e| format!("preflight.barcode_open: {e}"))?,
        );
        let mut barcode_index = HashMap::new();
        let mut barcode_to_pair = HashMap::new();
        let mut length = None;
        while let Some(line) = bounded_line(&mut reader, "preflight.barcode")? {
            let text = String::from_utf8(line).map_err(|_| "preflight.barcode_utf8")?;
            let text = text.trim_end_matches(['\r', '\n']);
            let fields: Vec<_> = text.split('\t').collect();
            if fields.len() != 3
                || fields[0].is_empty()
                || !fields[0]
                    .bytes()
                    .all(|c| matches!(c, b'A' | b'C' | b'G' | b'T'))
            {
                return Err("preflight.barcode_row".into());
            }
            let n = fields[0].len();
            if length.is_some_and(|v| v != n) {
                return Err("preflight.barcode_nonuniform_length".into());
            }
            length = Some(n);
            let pair = (
                *a_ids
                    .get(fields[1])
                    .ok_or("preflight.barcode_unknown_guide_a")?,
                *b_ids
                    .get(fields[2])
                    .ok_or("preflight.barcode_unknown_guide_b")?,
            );
            let index =
                u32::try_from(barcode_index.len()).map_err(|_| "resource.dimension_overflow")?;
            if barcode_index
                .insert(fields[0].as_bytes().to_vec(), index)
                .is_some()
            {
                return Err("preflight.barcode_duplicate".into());
            }
            barcode_to_pair.insert(index, pair);
        }
        let length = length.ok_or("preflight.barcode_empty")?;
        let (start, window_length) = cfg.window.unwrap_or((0, length));
        if window_length != length || start.checked_add(length).is_none_or(|n| n > MAX_LINE) {
            return Err("preflight.barcode_window_length".into());
        }
        let neighbors = if cfg.radius == 1 {
            let clock = Instant::now();
            let generated = barcode_index
                .len()
                .checked_mul(length)
                .and_then(|n| n.checked_mul(3))
                .ok_or("resource.dimension_overflow")?;
            // std HashMap's 7/8 load factor; reserve once, never rehash a giant table.
            let buckets = generated
                .checked_mul(8)
                .and_then(|n| n.checked_add(6))
                .map(|n| n / 7)
                .and_then(|n| n.max(4).checked_next_power_of_two())
                .ok_or("resource.dimension_overflow")?;
            let allocated = if length <= 40 {
                generated.checked_mul(16).and_then(|n| n.checked_add(64))
            } else {
                29usize
                    .checked_add(length)
                    .and_then(|slot| buckets.checked_mul(slot))
                    .and_then(|n| n.checked_add(64))
            }
            .ok_or("resource.dimension_overflow")? as u64;
            let charged = allocated
                .checked_mul(7)
                .and_then(|n| n.checked_add(3))
                .ok_or("resource.dimension_overflow")?
                / 4;
            if charged > remaining_memory {
                return Err(
                    "STOP_MEMORY_INFEASIBLE: neighbor index exceeds configured memory budget"
                        .into(),
                );
            }
            let (index, slots) = if length <= 40 {
                let mut entries = Vec::new();
                entries
                    .try_reserve_exact(generated)
                    .map_err(|_| "STOP_MEMORY_INFEASIBLE: neighbor allocation")?;
                for (sequence, &idx) in &barcode_index {
                    let key = compact_barcode(sequence).expect("validated DNA");
                    for position in 0..length {
                        for delta in 1..=3 {
                            let mut neighbor = key;
                            neighbor[position / 4] ^= delta << (2 * (position % 4));
                            entries.push((packed_key(neighbor) << 32) | idx as u128);
                        }
                    }
                }
                entries.sort_unstable();
                let mut slots = 0;
                for read in 0..entries.len() {
                    let entry = entries[read];
                    if slots > 0 && (entries[slots - 1] >> 32) == (entry >> 32) {
                        if entries[slots - 1] as u32 != entry as u32 {
                            entries[slots - 1] |= u32::MAX as u128;
                        }
                    } else {
                        entries[slots] = entry;
                        slots += 1;
                    }
                }
                entries.truncate(slots);
                (NeighborIndex::Compact(entries), slots)
            } else {
                let mut map = HashMap::new();
                map.try_reserve(generated)
                    .map_err(|_| "STOP_MEMORY_INFEASIBLE: neighbor allocation")?;
                for (sequence, &idx) in &barcode_index {
                    let mut neighbor = sequence.clone();
                    for position in 0..length {
                        for base in b"ACGT" {
                            if *base != sequence[position] {
                                neighbor[position] = *base;
                                neighbor_insert(&mut map, neighbor.clone(), idx);
                            }
                        }
                        neighbor[position] = sequence[position];
                    }
                }
                let slots = map.len();
                (NeighborIndex::Long(map), slots)
            };
            eprintln!(
                "barcode-radius=1 neighbor index: build_seconds={:.6} generated={} slots={} allocated_bytes_estimate={} charged_bytes={}",
                clock.elapsed().as_secs_f64(),
                generated,
                slots,
                allocated,
                charged
            );
            Some(index)
        } else {
            None
        };
        let mut bindings = json!({"r3":if audited { physical(&cfg.r3)? } else {json!({"path":cfg.r3.to_string_lossy()})},
            "barcode_whitelist":if audited {physical(&cfg.whitelist)?} else {json!({"path":cfg.whitelist.to_string_lossy()})},
            "extraction_window":{"start":start,"length":length},"matching_policy":{"kind":"exact","radius":0}});
        if cfg.radius == 1 {
            bindings["barcode_radius"] = json!(1);
            bindings["matching_policy"] =
                json!({"kind":"hamming","radius":1,"ambiguity":"unmapped","exact_priority":true});
        }
        Ok(Self {
            neighbors,
            barcode_index,
            barcode_to_pair,
            start,
            length,
            bindings,
        })
    }
    fn check(&self, components: &[Classification; 2], sequence: &[u8]) -> ConcordanceOutcome {
        if components.iter().any(|c| c.leaf != 0) {
            return ConcordanceOutcome::GuideFailure;
        }
        let pair = pair_id(
            components[0].guide.expect("unique A"),
            components[1].guide.expect("unique B"),
        );
        let Some(window) = sequence.get(self.start..self.start + self.length) else {
            return ConcordanceOutcome::UnmappedBarcode;
        };
        let (index, rescued) = if let Some(&index) = self.barcode_index.get(window) {
            (index, false)
        } else if let Some(index) = self.neighbors.as_ref().and_then(|n| n.get(window)) {
            if index == u32::MAX {
                return ConcordanceOutcome::AmbiguousBarcode;
            }
            (index, true)
        } else {
            return ConcordanceOutcome::UnmappedBarcode;
        };
        match (self.barcode_to_pair.get(&index) == Some(&pair), rescued) {
            (true, false) => ConcordanceOutcome::Valid(pair),
            (false, false) => ConcordanceOutcome::DiscordantTriplet,
            (true, true) => ConcordanceOutcome::RescuedValid(pair),
            (false, true) => ConcordanceOutcome::RescuedDiscordant,
        }
    }
    fn verify_bindings(&self, cfg: &BarcodeConfig) -> Result<()> {
        for (key, path) in [("r3", &cfg.r3), ("barcode_whitelist", &cfg.whitelist)] {
            if let Some(hash) = self.bindings[key]["sha256"].as_str() {
                if file_hash(path)? != hash {
                    return Err("input.concordance_binding_changed".into());
                }
            }
        }
        Ok(())
    }
}
fn check_barcode_name(r1: &FastqRecord, r3: &FastqRecord) -> Result<()> {
    let name1 = normalized_name(&r1.header, Source::R1)?;
    let mut fields = r3.header[1..]
        .split(|b| b.is_ascii_whitespace())
        .filter(|v| !v.is_empty());
    let token = fields.next().ok_or("input.invalid_header")?;
    if token.ends_with(b"/1") || token.ends_with(b"/2") {
        return Err("input.barcode_mate_marker".into());
    }
    if let Some(marker) = fields.next() {
        if marker.len() >= 2 && marker[1] == b':' && matches!(marker[0], b'1' | b'2') {
            return Err("input.barcode_mate_marker".into());
        }
    }
    let name3 = token.strip_suffix(b"/3").unwrap_or(token);
    if name1 != name3 {
        return Err("input.barcode_pair_name".into());
    }
    Ok(())
}
fn active_row(cfg: &DualConfig, mut value: Value) -> Value {
    if cfg.barcode.is_some() {
        let object = value.as_object_mut().expect("artifact row");
        let preserve = matches!(
            object["schema_id"].as_str(),
            Some("project-c.raw-counts.v1" | "project-c.guide-metadata.v1")
        );
        object.insert(
            "profile_id".into(),
            json!(if preserve {
                CONCORDANCE_PROFILE
            } else {
                concordance_profile(cfg)
            }),
        );
        let schema = object["schema_id"].as_str().expect("schema");
        let schema = match schema {
            "project-c.sample-qc.v1" => {
                if radius1(cfg) {
                    RADIUS1_QC
                } else {
                    CONCORDANCE_QC
                }
            }
            "project-c.terminal-totals.v1" => {
                if radius1(cfg) {
                    RADIUS1_TERMINAL
                } else {
                    CONCORDANCE_TERMINAL
                }
            }
            "project-c.scientific-provenance.v1" => {
                if radius1(cfg) {
                    RADIUS1_PROVENANCE
                } else {
                    CONCORDANCE_PROVENANCE
                }
            }
            other => other,
        }
        .to_string();
        object.insert("schema_id".into(), json!(schema));
    }
    value
}

// Count before allocating any membership or duplicate-validation sets.
fn construct_row_count(path: &Path) -> Result<usize> {
    let reader = File::open(path).map_err(|e| format!("preflight.construct_open: {e}"))?;
    count_rows_up_to(reader, MAX_MEMBERSHIP_ROWS)
}

fn count_rows_up_to(mut reader: impl Read, maximum: usize) -> Result<usize> {
    let mut buffer = [0u8; 65_536];
    let mut rows = 0usize;
    let mut trailing = false;
    loop {
        let n = reader
            .read(&mut buffer)
            .map_err(|e| format!("preflight.construct_read: {e}"))?;
        if n == 0 {
            break;
        }
        for &byte in &buffer[..n] {
            if byte == b'\n' {
                rows = rows.checked_add(1).ok_or("resource.dimension_overflow")?;
                trailing = false;
            } else {
                trailing = true;
            }
            if rows > maximum {
                return Err("STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX".into());
            }
        }
    }
    if trailing {
        rows = rows.checked_add(1).ok_or("resource.dimension_overflow")?;
    }
    if rows > maximum {
        return Err("STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX".into());
    }
    Ok(rows)
}

fn stream_constructs(
    path: &Path,
    a: &[Guide],
    b: &[Guide],
    visit: impl FnMut(Construct) -> Result<()>,
) -> Result<()> {
    stream_constructs_checked(path, a, b, None, visit)
}

fn stream_constructs_checked(
    path: &Path,
    a: &[Guide],
    b: &[Guide],
    mut ids: Option<&mut HashSet<String>>,
    mut visit: impl FnMut(Construct) -> Result<()>,
) -> Result<()> {
    let mut reader =
        BufReader::new(File::open(path).map_err(|e| format!("preflight.construct_open: {e}"))?);
    let ids_a: HashMap<&str, usize> = a
        .iter()
        .enumerate()
        .map(|(i, g)| (g.id.as_str(), i))
        .collect();
    let ids_b: HashMap<&str, usize> = b
        .iter()
        .enumerate()
        .map(|(i, g)| (g.id.as_str(), i))
        .collect();
    while let Some(mut line) = bounded_line(&mut reader, "preflight.construct_read")? {
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        let line = String::from_utf8(line).map_err(|_| "preflight.construct_utf8")?;
        let parts: Vec<&str> = line.splitn(4, '\t').collect();
        if parts.len() != 3 || parts.iter().any(|x| x.is_empty()) {
            return Err("preflight.construct_columns".into());
        }
        if let Some(ids) = ids.as_mut() {
            if !ids.insert(parts[0].to_owned()) {
                return Err("preflight.duplicate_construct_id".into());
            }
        }
        let &ai = ids_a.get(parts[1]).ok_or("preflight.unknown_a_guide")?;
        let &bi = ids_b.get(parts[2]).ok_or("preflight.unknown_b_guide")?;
        visit(Construct {
            id: parts[0].to_owned(),
            a: ai,
            b: bi,
        })?;
    }
    Ok(())
}

fn pair_id(a: usize, b: usize) -> PairId {
    // Guide cardinality is checked once, before constructing indices.
    (a as u32, b as u32)
}

fn load_membership(path: &Path, a: &[Guide], b: &[Guide]) -> Result<ExpectedPairs> {
    let mut ids = HashSet::new();
    let mut pairs = HashSet::new();
    stream_constructs_checked(path, a, b, Some(&mut ids), |c| {
        if !pairs.insert(pair_id(c.a, c.b)) {
            return Err("preflight.duplicate_pair".into());
        }
        Ok(())
    })?;
    Ok(ExpectedPairs::new(pairs))
}

// File sizes bound plain FASTQ record counts conservatively. Gzip has no safe
// compressed-size bound (including concatenated members); use the library cap.
fn input_records_bound(path: &Path) -> u64 {
    if path.extension().is_some_and(|x| x == "gz") {
        return u64::MAX;
    }
    fs::metadata(path).map_or(u64::MAX, |m| m.len() / 4 + 1)
}

fn construct_sizes(path: &Path) -> Result<(u64, u64)> {
    let mut reader =
        BufReader::new(File::open(path).map_err(|e| format!("preflight.construct_open: {e}"))?);
    let mut ids = 0u64;
    let mut maximum = 0u64;
    while let Some(line) = bounded_line(&mut reader, "preflight.construct_read")? {
        let id = line.iter().position(|b| *b == b'\t').unwrap_or(line.len());
        ids = ids
            .checked_add(id as u64)
            .ok_or("resource.dimension_overflow")?;
        maximum = maximum.max(line.len() as u64);
    }
    Ok((ids, maximum))
}

fn bounded_count_entry(
    counts: &mut HashMap<PairId, u64>,
    pair: PairId,
    cap: u64,
) -> Result<&mut u64> {
    if !counts.contains_key(&pair) && counts.len() as u64 >= cap {
        return Err("resource.sparse_count_cap".into());
    }
    Ok(counts.entry(pair).or_insert(0))
}

// Only expected pairs have per-pair scientific output rows. Unexpected pairs
// retain an independent checked total; discovery and evidence remain separate.
fn classified_count_entry<'a>(
    counts: &'a mut HashMap<PairId, u64>,
    unexpected: &'a mut u64,
    pair: PairId,
    leaf: usize,
    cap: u64,
) -> Result<&'a mut u64> {
    match leaf {
        0 => bounded_count_entry(counts, pair, cap),
        1 => Ok(unexpected),
        _ => Err("conservation.failure".into()),
    }
}

fn merge_unexpected_count(target: &mut u64, value: u64) -> Result<()> {
    #[cfg(debug_assertions)]
    if value != 0
        && env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|s| s == "worker_reduction_overflow")
    {
        *target = u64::MAX;
    }
    *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
    Ok(())
}

fn normalized_name(header: &[u8], side: Source) -> Result<Vec<u8>> {
    let mut fields = header[1..]
        .split(|b| b.is_ascii_whitespace())
        .filter(|f| !f.is_empty());
    let token = fields.next().ok_or("input.invalid_header")?;
    let marker = fields.next();
    if let Some(marker) = marker {
        let expected = match side {
            Source::R1 => b'1',
            Source::R2 => b'2',
        };
        if marker.len() >= 2
            && matches!(marker[0], b'1' | b'2')
            && marker[1] == b':'
            && marker[0] != expected
        {
            return Err("input.mate_marker".into());
        }
    }
    let (correct, wrong) = match side {
        Source::R1 => (b"/1".as_slice(), b"/2".as_slice()),
        Source::R2 => (b"/2".as_slice(), b"/1".as_slice()),
    };
    if token.ends_with(wrong) {
        return Err("input.mate_marker".into());
    }
    Ok(token.strip_suffix(correct).unwrap_or(token).to_vec())
}

fn check_pair(r1: &FastqRecord, r2: &FastqRecord) -> Result<()> {
    let name1 = normalized_name(&r1.header, Source::R1)?;
    let name2 = normalized_name(&r2.header, Source::R2)?;
    if name1 != name2 {
        return Err("input.pair_name".into());
    }
    Ok(())
}

fn reverse_complement(sequence: &str) -> String {
    sequence
        .bytes()
        .rev()
        .map(complement)
        .map(char::from)
        .collect()
}

fn layout_explanations(
    a_window: &str,
    b_window: &str,
    a: &[Guide],
    b: &[Guide],
    indices: [&HashMap<String, usize>; 2],
    expected: &HashSet<PairId>,
) -> usize {
    let transforms = [
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (false, true, true),
        (true, true, false),
        (true, false, true),
        (true, true, true),
    ];
    let reverse_a = reverse_complement(a_window);
    let reverse_b = reverse_complement(b_window);
    let mut explained = HashSet::new();
    for (swap, complement_a, complement_b) in transforms {
        let (left, right, reverse_left, reverse_right) = if swap {
            (b_window, a_window, reverse_b.as_str(), reverse_a.as_str())
        } else {
            (a_window, b_window, reverse_a.as_str(), reverse_b.as_str())
        };
        let left = if complement_a { reverse_left } else { left };
        let right = if complement_b { reverse_right } else { right };
        if left.len() != a[0].sequence.len() || right.len() != b[0].sequence.len() {
            continue;
        }
        // Admission rejects duplicate role sequences and duplicate construct pairs.
        // These existing indices therefore resolve exactly the construct identity
        // that the linear scan would insert, without traversing the library.
        if let (Some(&ai), Some(&bi)) = (indices[0].get(left), indices[1].get(right))
            && expected.contains(&pair_id(ai, bi))
        {
            explained.insert(pair_id(ai, bi));
            if explained.len() > 1 {
                return 2;
            }
        }
    }
    explained.len()
}

struct DualResult {
    leaf: usize,
    construct: Option<PairId>,
    distance: Option<usize>,
    extraction: Option<ExtractionFault>,
    components: [Classification; 2],
}

fn classify_unit(
    r1: &FastqRecord,
    r2: Option<&FastqRecord>,
    cfg: &DualConfig,
    guides: [&[Guide]; 2],
    indices: [&HashMap<String, usize>; 2],
    expected: &ExpectedPairs,
) -> DualResult {
    let read = |source: Source| match source {
        Source::R1 => r1,
        Source::R2 => r2.expect("preflight R2"),
    };
    let a_read = read(cfg.a.source);
    let b_read = read(cfg.b.source);
    let a = classify_component(
        &a_read.sequence,
        &a_read.quality,
        ComponentSpec {
            geometry: &cfg.a.geometry,
            orientation: cfg.a.orientation,
            radius: cfg.radius,
            min_phred: cfg.min_phred,
        },
        guides[0],
        indices[0],
    );
    let b = classify_component(
        &b_read.sequence,
        &b_read.quality,
        ComponentSpec {
            geometry: &cfg.b.geometry,
            orientation: cfg.b.orientation,
            radius: cfg.radius,
            min_phred: cfg.min_phred,
        },
        guides[1],
        indices[1],
    );
    let illegal = |sequence: &[u8]| {
        sequence
            .iter()
            .any(|base| !matches!(base.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N'))
    };
    let global_illegal =
        illegal(&r1.sequence) || r2.is_some_and(|record| illegal(&record.sequence));
    let mut result = DualResult {
        leaf: 0,
        construct: None,
        distance: None,
        extraction: None,
        components: [a, b],
    };
    if global_illegal {
        result.leaf = 8;
        return result;
    }
    let ca = &result.components[0];
    let cb = &result.components[1];
    let invalid = [ca.leaf, cb.leaf]
        .into_iter()
        .filter(|leaf| (3..=6).contains(leaf))
        .min();
    if let Some(leaf) = invalid {
        result.leaf = leaf + 5;
        if leaf == 4 {
            let rank = |reason: ExtractionFault| match reason {
                ExtractionFault::MissingAnchor => 0,
                ExtractionFault::MultipleAnchors => 1,
                ExtractionFault::OutOfBounds => 2,
            };
            result.extraction = [ca.extraction_fault, cb.extraction_fault]
                .into_iter()
                .flatten()
                .min_by_key(|reason| rank(*reason));
        }
        return result;
    }
    if ca.leaf == 1 || cb.leaf == 1 {
        result.leaf = 2;
        return result;
    }
    if let (Some(ai), Some(bi)) = (ca.guide, cb.guide)
        && expected.filter.might_contain(ai as u32, bi as u32)
        && expected.contains(&pair_id(ai, bi))
    {
        result.leaf = 0;
        result.construct = Some(pair_id(ai, bi));
        result.distance = Some(
            ca.distance.expect("unique distance") as usize * 2
                + cb.distance.expect("unique distance") as usize,
        );
        return result;
    }
    let layout = layout_explanations(
        ca.window.as_deref().expect("valid A window"),
        cb.window.as_deref().expect("valid B window"),
        guides[0],
        guides[1],
        indices,
        expected,
    );
    if layout > 1 {
        result.leaf = 3;
        return result;
    }
    if layout == 1 {
        result.leaf = 4;
        return result;
    }
    result.leaf = match (ca.guide, cb.guide) {
        (Some(ai), Some(bi)) => {
            result.construct = Some(pair_id(ai, bi));
            result.distance = Some(
                ca.distance.expect("unique distance") as usize * 2
                    + cb.distance.expect("unique distance") as usize,
            );
            1
        }
        (Some(_), None) => 5,
        (None, Some(_)) => 6,
        (None, None) => 7,
    };
    result
}

/// Explicit three-read stage: the legacy A/B classifier retains all geometry and
/// failure priorities, then the immutable barcode oracle gates accumulation.
fn classify_triplet(
    reads: [&FastqRecord; 3],
    cfg: &DualConfig,
    guides: [&[Guide]; 2],
    indices: [&HashMap<String, usize>; 2],
    expected: &ExpectedPairs,
    oracle: &BarcodeOracle,
) -> (DualResult, ConcordanceOutcome) {
    let result = classify_unit(reads[0], Some(reads[1]), cfg, guides, indices, expected);
    let outcome = if result.leaf >= 8 {
        // Whole-read illegal flanks must retain their existing invalid priority,
        // even if both selected guide windows independently match.
        ConcordanceOutcome::GuideFailure
    } else {
        oracle.check(&result.components, &reads[2].sequence)
    };
    (result, outcome)
}

fn component_state(leaf: usize) -> usize {
    match leaf {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 3,
    }
}
fn extraction_slot(reason: ExtractionFault) -> usize {
    match reason {
        ExtractionFault::MissingAnchor => 0,
        ExtractionFault::MultipleAnchors => 1,
        ExtractionFault::OutOfBounds => 2,
    }
}

fn geometry_json(geometry: &Geometry) -> Value {
    match geometry {
        Geometry::Fixed { start } => json!({"kind":"fixed","start":start}),
        Geometry::Anchor {
            sequence,
            search_start,
            search_end,
            displacement,
        } => json!({
            "kind":"anchor_relative","anchor":String::from_utf8(sequence.clone()).expect("ASCII anchor"),
            "search":[search_start,search_end],"displacement":displacement.to_string()}),
    }
}
fn component_json(component: &ComponentConfig) -> Value {
    json!({"source":match component.source { Source::R1=>"R1",Source::R2=>"R2"},
        "orientation":match component.orientation { Orientation::Forward=>"forward",Orientation::ReverseComplement=>"reverse_complement"},
        "geometry":geometry_json(&component.geometry)})
}
fn physical(path: &Path) -> Result<Value> {
    Ok(json!({"path":path.to_string_lossy(),
        "encoding":if path.extension().is_some_and(|x|x=="gz") {"gzip"} else {"plain"},
        "bytes":fs::metadata(path).map_err(|e|e.to_string())?.len(),"sha256":file_hash(path)?}))
}
fn checked_sum(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(0_u64, |a, b| {
        a.checked_add(*b).ok_or("arithmetic.overflow".into())
    })
}

// Dual-only streaming writer: same canonical serde_json bytes, budget charging,
// SHA-256 and durability behavior as jsonl, without collecting output rows.
struct DualJsonlWriter<'a> {
    writer: BufWriter<File>,
    budget: &'a mut OutputBudget,
    audited: bool,
    digest: Option<Sha256>,
    size: u64,
    rows: u64,
    prepaid: bool,
}
impl<'a> DualJsonlWriter<'a> {
    fn new(path: &Path, cfg: &DualConfig, budget: &'a mut OutputBudget) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| format!("publication.create: {e}"))?;
        Ok(Self {
            writer: BufWriter::new(file),
            budget,
            audited: cfg.execution_profile.audited(),
            digest: cfg.execution_profile.audited().then(Sha256::new),
            size: 0,
            rows: 0,
            prepaid: false,
        })
    }
    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let len = u64::try_from(bytes.len()).map_err(|_| "resource.output_budget")?;
        if !self.prepaid {
            self.budget.charge(len, true)?;
        }
        test_fault("output_write")?;
        self.writer
            .write_all(bytes)
            .map_err(|e| format!("publication.write: {e}"))?;
        if let Some(digest) = &mut self.digest {
            digest.update(bytes);
        }
        self.size = self.size.checked_add(len).ok_or("arithmetic.overflow")?;
        self.rows = self
            .rows
            .checked_add(bytes.iter().filter(|&&b| b == b'\n').count() as u64)
            .ok_or("arithmetic.overflow")?;
        Ok(())
    }
    fn value_line(&mut self, value: &Value) -> Result<()> {
        let mut bytes =
            serde_json::to_vec(value).map_err(|e| format!("serialization.json: {e}"))?;
        bytes.push(b'\n');
        self.bytes(&bytes)
    }
    fn finish(mut self) -> Result<(u64, String, u64)> {
        if !self.audited {
            test_fault("output_flush")?;
        }
        self.writer
            .flush()
            .map_err(|e| format!("publication.flush: {e}"))?;
        if self.audited {
            test_fault("output_sync")?;
            self.writer
                .into_inner()
                .map_err(|e| format!("publication.close: {e}"))?
                .sync_all()
                .map_err(|e| format!("publication.sync: {e}"))?;
        } else {
            let file = self
                .writer
                .into_inner()
                .map_err(|e| format!("publication.close: {e}"))?;
            test_fault("output_close")?;
            drop(file);
        }
        Ok((
            self.size,
            self.digest
                .map_or_else(String::new, |h| format!("{:x}", h.finalize())),
            self.rows,
        ))
    }
}

fn stream_expected(
    cfg: &DualConfig,
    a: &[Guide],
    b: &[Guide],
    emit: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    emit(b"[")?;
    let mut first = true;
    cfg.stream_constructs(a, b, |c| {
        if !first {
            emit(b",")?;
        }
        first = false;
        let bytes = serde_json::to_vec(&json!([c.id, a[c.a].id, b[c.b].id]))
            .map_err(|e| format!("serialization.json: {e}"))?;
        emit(&bytes)
    })?;
    emit(b"]")
}

fn stream_library(
    cfg: &DualConfig,
    a: &[Guide],
    b: &[Guide],
    library: &Value,
    emit: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    // serde_json's ordered object keys: expected_constructs precedes guides.
    emit(b"{\"expected_constructs\":")?;
    stream_expected(cfg, a, b, emit)?;
    emit(b",\"guides\":")?;
    emit(&serde_json::to_vec(library).map_err(|e| format!("serialization.json: {e}"))?)?;
    emit(b"}")
}

fn stream_provenance(
    cfg: &DualConfig,
    a: &[Guide],
    b: &[Guide],
    library: &Value,
    provenance: &Value,
    emit: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    emit(b"{")?;
    for (i, (key, value)) in provenance
        .as_object()
        .expect("provenance object")
        .iter()
        .enumerate()
    {
        if i != 0 {
            emit(b",")?;
        }
        emit(&serde_json::to_vec(key).map_err(|e| format!("serialization.json: {e}"))?)?;
        emit(b":")?;
        match key.as_str() {
            "expected_constructs" => stream_expected(cfg, a, b, emit)?,
            "normalized_library" => stream_library(cfg, a, b, library, emit)?,
            _ => emit(&serde_json::to_vec(value).map_err(|e| format!("serialization.json: {e}"))?)?,
        }
    }
    emit(b"}\n")
}

struct DualScience<'a> {
    a: &'a [Guide],
    b: &'a [Guide],
    counts: &'a mut HashMap<PairId, u64>,
    unexpected_count: u64,
    leaves: &'a [u64; 12],
    qc: &'a DualQc,
    total: u64,
    decoded_bytes: u64,
    decoded_hashes: Vec<String>,
    discovery: &'a Discovery,
    evidence_artifact: Option<Value>,
    barcode_bindings: Option<&'a Value>,
}

fn science(
    stage: &Path,
    cfg: &DualConfig,
    stats: DualScience<'_>,
    budget: &mut OutputBudget,
) -> Result<(String, usize)> {
    let DualScience {
        a,
        b,
        counts,
        unexpected_count,
        leaves,
        qc,
        total,
        decoded_bytes,
        decoded_hashes,
        discovery,
        evidence_artifact,
        barcode_bindings,
    } = stats;
    let mut matrix_sum = 0u64;
    let mut zero_rows = 0u64;
    let raw_metadata = if cfg.compact_output {
        let mut raw = compact_output::Writer::new(&stage.join("counts.u64le"), budget)?;
        cfg.stream_pairs(a, b, |ai, bi| {
            let count = counts.remove(&(ai,bi)).unwrap_or(0);
            matrix_sum = matrix_sum.checked_add(count).ok_or("arithmetic.overflow")?;
            if count == 0 { increment(&mut zero_rows)?; }
            raw.push(count)
        })?;
        raw.finish()?
    } else {
    let mut raw = DualJsonlWriter::new(&stage.join("raw-counts.jsonl"), cfg, budget)?;
    cfg.stream_constructs(a, b, |c| {
        let count = counts.remove(&pair_id(c.a, c.b)).unwrap_or(0);
        matrix_sum = matrix_sum.checked_add(count).ok_or("arithmetic.overflow")?;
        if count == 0 {
            increment(&mut zero_rows)?;
        }
        raw.value_line(&active_row(
            cfg,
            row(
                "project-c.raw-counts.v1",
                json!({"row_kind":"construct","row_id":c.id,"counts":[count]}),
            ),
        ))
    })?;
    raw.finish()?
    };
    if !counts.is_empty() || unexpected_count != leaves[1] {
        return Err("conservation.failure".into());
    }
    // Unexpected pairs already have exact per-read categorization and discovery.
    // Keep their established discovery artifact instead of appending matrix rows.
    counts.clear();
    if checked_sum(leaves)?
        .checked_add(qc.discordant_triplet)
        .and_then(|n| n.checked_add(qc.unmapped_barcode))
        .ok_or("arithmetic.overflow")?
        != total
        || matrix_sum != leaves[0]
        || checked_sum(&qc.mapped_distance)? != leaves[0]
        || checked_sum(&qc.recombinant_distance)? != leaves[1]
        || checked_sum(&qc.extraction)? != leaves[9]
        || checked_sum(&qc.components[0])? != total
        || checked_sum(&qc.components[1])? != total
    {
        return Err("conservation.failure".into());
    }
    for (index, leaf) in DISCOVERY_LEAVES.iter().enumerate() {
        if discovery.eligible(index) != leaves[*leaf] {
            return Err("conservation.discovery_eligible".into());
        }
    }
    let mut artifacts = BTreeMap::<String, Value>::new();
    let (bytes, sha256, rows) = raw_metadata;
    if cfg.compact_output {
        artifacts.insert("counts.u64le".into(), json!({"schema_id":"cclamp.ordinal-count-u64le.v1","bytes":bytes,"rows":rows}));
    } else {
    artifacts.insert("raw-counts.jsonl".into(), if cfg.execution_profile.audited() {
        json!({"schema_id":"project-c.raw-counts.v1","profile_id":if cfg.barcode.is_some() {CONCORDANCE_PROFILE} else {PROFILE},"bytes":bytes,"sha256":sha256})
    } else { json!({"schema_id":"project-c.raw-counts.v1","profile_id":if cfg.barcode.is_some() {CONCORDANCE_PROFILE} else {PROFILE},"bytes":bytes,"rows":rows}) });
    }
    if let Some(metadata) = evidence_artifact {
        artifacts.insert("evidence.jsonl".into(), metadata);
    }
    {
        let mut save = |name: &str, schema: &str, rows: Vec<Value>| -> Result<()> {
            if cfg.compact_output && matches!(name, "guide-metadata.jsonl" | "construct-metadata.jsonl") { return Ok(()); }
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|mut value| {
                    if cfg.barcode.is_some() {
                        if name == "sample-qc.jsonl" {
                            value["concordance_checked"] = json!(true);
                            value["concordant"] = json!(qc.concordant);
                            value["discordant_triplet"] = json!(qc.discordant_triplet);
                            value["unmapped_barcode"] = json!(qc.unmapped_barcode);
                            if radius1(cfg) {
                                let fields = serde_json::to_value(Radius1Qc {
                                    barcode_radius: 1,
                                    radius1_ambiguous: qc.radius1_ambiguous,
                                    radius1_rescued: qc.radius1_rescued,
                                })
                                .map_err(|e| e.to_string())
                                .expect("serializable QC fields");
                                value
                                    .as_object_mut()
                                    .expect("QC row")
                                    .extend(fields.as_object().expect("QC fields").clone());
                            }
                        }
                        if name == "terminal-totals.jsonl" {
                            value["accounting_partition"] = json!("post_concordance_filter");
                            value["semantic_contract_id"] = json!(concordance_contract(cfg));
                        }
                    }
                    active_row(cfg, value)
                })
                .collect();
            let schema = if cfg.barcode.is_some() {
                match schema {
                    "project-c.sample-qc.v1" => {
                        if radius1(cfg) {
                            RADIUS1_QC
                        } else {
                            CONCORDANCE_QC
                        }
                    }
                    "project-c.terminal-totals.v1" => {
                        if radius1(cfg) {
                            RADIUS1_TERMINAL
                        } else {
                            CONCORDANCE_TERMINAL
                        }
                    }
                    other => other,
                }
            } else {
                schema
            };
            let (bytes, sha256, row_count) = if name == "construct-metadata.jsonl" {
                let mut writer = DualJsonlWriter::new(&stage.join(name), cfg, budget)?;
                cfg.stream_constructs(a, b, |c| {
                    writer.value_line(&active_row(
                        cfg,
                        row(
                            schema,
                            json!({"construct_id":c.id,
                        "A_guide_id":a[c.a].id,"B_guide_id":b[c.b].id,
                        "gene_id_or_null":null,"annotations":{}}),
                        ),
                    ))
                })?;
                writer.finish()?
            } else {
                let (bytes, sha256) = jsonl(
                    &stage.join(name),
                    &rows,
                    budget,
                    true,
                    cfg.execution_profile,
                )?;
                (bytes, sha256, rows.len() as u64)
            };
            artifacts.insert(
                name.into(),
                if cfg.execution_profile.audited() {
                    json!({"schema_id":schema,"profile_id":if cfg.barcode.is_some() {if name == "guide-metadata.jsonl" {CONCORDANCE_PROFILE} else {concordance_profile(cfg)}} else {PROFILE},"bytes":bytes,"sha256":sha256})
                } else {
                    json!({"schema_id":schema,"profile_id":if cfg.barcode.is_some() {if name == "guide-metadata.jsonl" {CONCORDANCE_PROFILE} else {concordance_profile(cfg)}} else {PROFILE},"bytes":bytes,"rows":row_count})
                },
            );
            Ok(())
        };
        let mut guide_rows = Vec::new();
        for (role, guides) in [("A", a), ("B", b)].into_iter().filter(|_| !cfg.compact_output) {
            for guide in guides {
                guide_rows.push(row(
                    "project-c.guide-metadata.v1",
                    json!({"role":role,"guide_id":guide.id,
                "normalized_sequence":guide.sequence,"guide_length":guide.sequence.len(),
                "gene_id_or_null":null,"annotations":{}}),
                ));
            }
        }
        save(
            "guide-metadata.jsonl",
            "project-c.guide-metadata.v1",
            guide_rows,
        )?;
        save(
            "construct-metadata.jsonl",
            "project-c.construct-metadata.v1",
            Vec::new(),
        )?;
        let invalid = checked_sum(&leaves[8..])?;
        let ambiguous = checked_sum(&leaves[2..4])?;
        let unmapped = checked_sum(&leaves[4..8])?;
        let fraction = if total == 0 {
            json!({"undefined":"zero_denominator"})
        } else {
            json!({"numerator":leaves[0],"denominator":total})
        };
        let states =
            |c: &[u64; 4]| json!({"unique":c[0],"ambiguous":c[1],"unknown":c[2],"invalid":c[3]});
        let rescued = leaves[0]
            .checked_sub(qc.mapped_distance[0])
            .ok_or("conservation.failure")?;
        save(
            "sample-qc.jsonl",
            "project-c.sample-qc.v1",
            vec![row(
                "project-c.sample-qc.v1",
                json!({
                    "sample_id":cfg.sample,"total_records":total,"mapped":leaves[0],"recombinant":leaves[1],
                    "ambiguous":ambiguous,"unmapped":unmapped,"invalid":invalid,"matrix_sum":matrix_sum,
                    "zero_count_rows":zero_rows,"mapped_exact":qc.mapped_distance[0],"mapped_rescued_h1":rescued,
                    "mapped_distance_histogram":qc.mapped_distance,"recombinant_distance_histogram":qc.recombinant_distance,
                    "invalid_reason_counts":{"invalid.alphabet":leaves[8],"invalid.extraction":leaves[9],
                        "invalid.N":leaves[10],"invalid.quality":leaves[11]},
                    "invalid_reason_counts_additive":true,"invalid_reason_counts_denominator":total,
                    "extraction_subreason_counts":{"missing_anchor":qc.extraction[0],"multiple_anchors":qc.extraction[1],"out_of_bounds":qc.extraction[2]},
                    "component_state_counts":{"A":states(&qc.components[0]),"B":states(&qc.components[1])},
                    "component_state_additive_per_role":true,"component_state_denominator":total,
                    "structural_success":true,"mapped_fraction":fraction
                }),
            )],
        )?;
        let parents = [
            ("mapped", leaves[0]),
            ("recombinant", leaves[1]),
            ("ambiguous", ambiguous),
            ("unmapped", unmapped),
            ("invalid", invalid),
        ];
        let mut totals = Vec::new();
        for (name, count) in parents {
            totals.push(row("project-c.terminal-totals.v1",json!({
        "sample_id":cfg.sample,"mode":"dual","row_type":"parent","parent":name,"terminal_leaf":null,"count":count})));
        }
        for (i, leaf) in DUAL_LEAVES.iter().enumerate() {
            let parent = match i {
                0 => "mapped",
                1 => "recombinant",
                2..=3 => "ambiguous",
                4..=7 => "unmapped",
                _ => "invalid",
            };
            totals.push(row(
                "project-c.terminal-totals.v1",
                json!({"sample_id":cfg.sample,"mode":"dual",
            "row_type":"leaf","parent":parent,"terminal_leaf":leaf,"count":leaves[i]}),
            ));
        }
        save(
            "terminal-totals.jsonl",
            "project-c.terminal-totals.v1",
            totals,
        )?;
        save(
            "discovery.jsonl",
            "project-c.discovery.v1",
            discovery.rows(&cfg.sample, "dual")?,
        )?;
    }
    if cfg.compact_output {
        let metadata = json!({"schema_id":"cclamp.compact-result.v2","status":"COMPLETE",
            "sample_id":cfg.sample,"row_order":"exact reference row order","counts":artifacts["counts.u64le"],
            "matrix_sum":matrix_sum,"unexpected_pair_total":unexpected_count,"terminal_leaves":leaves,"total_records":total,
            "conservation":true,"zero_count_rows":zero_rows,
            "configuration":env::args().collect::<Vec<_>>(),"worker_count":cfg.workers.count,
            "construct_format":if cfg.compact_reference.is_some(){"ordered-pairs-u32le-v1"}else{"tsv"},
            "first_pair_ordinal":cfg.compact_reference.as_ref().map(|m| m.first_pair_ordinal),
            "inputs":std::iter::once(&cfg.r1).chain(cfg.r2.iter()).map(|p| json!({"path":p})).collect::<Vec<_>>(),
            "references":[{"path":cfg.guides_a},{"path":cfg.guides_b},{"path":cfg.constructs}],
            "raw_reference":cfg.compact_reference.as_ref().map(|m|json!({"path":m.raw_path,"bytes":m.reference_bytes})),
            "verification":"structural conservation; no cryptographic identity",
            "publication":"Standard atomic visibility after close; no crash durability claim"});
        let (bytes,_) = jsonl(&stage.join("compact-metadata.jsonl"), &[metadata], budget, true, cfg.execution_profile)?;
        artifacts.insert("compact-metadata.jsonl".into(), json!({"schema_id":"cclamp.compact-result.v2","bytes":bytes,"rows":1}));
    }
    if !cfg.execution_profile.audited() {
        return Ok((
            serde_json::to_string(&json!({"artifacts":artifacts})).map_err(|e| e.to_string())?,
            artifacts.len(),
        ));
    }
    let library = json!({"A":a.iter().map(|g|json!([g.id,g.sequence])).collect::<Vec<_>>(),
        "B":b.iter().map(|g|json!([g.id,g.sequence])).collect::<Vec<_>>()});
    let mut library_digest = Sha256::new();
    stream_library(cfg, a, b, &library, &mut |bytes| {
        library_digest.update(bytes);
        Ok(())
    })?;
    let library_hash = format!("{:x}", library_digest.finalize());
    let discovery_json = match cfg.discovery_mode {
        DiscoveryMode::Bounded(k) => json!({"mode":"bounded","K":k}),
        DiscoveryMode::Exact(k) => json!({"mode":"exact","limit":k}),
    };
    let mut config = json!({"mode":"dual","A":component_json(&cfg.a),"B":component_json(&cfg.b),
        "radius":cfg.radius,"quality":cfg.min_phred.map_or(json!("off"),|n|json!({"min_phred":n})),
        "discovery":discovery_json,"evidence":cfg.evidence.selector(),"paired":cfg.r2.is_some(),
        "resource_limits":cfg.limits.as_json()});
    if let Some(bindings) = barcode_bindings {
        config["barcode_concordance"] = bindings.clone();
    }
    let mut physical_inputs = vec![physical(&cfg.r1)?];
    if let Some(r2) = &cfg.r2 {
        physical_inputs.push(physical(r2)?);
    }
    if let Some(bindings) = barcode_bindings {
        physical_inputs.push(bindings["r3"].clone());
    }
    let provenance = active_row(
        cfg,
        row(
            "project-c.scientific-provenance.v1",
            json!({
                "semantic_contract_id":if cfg.barcode.is_some() {concordance_contract(cfg)} else {CONTRACT},"artifact_schema_profile_ids":artifacts,
                "sample_manifest":[cfg.sample],"normalized_library":null,"expected_constructs":null,
                "normalized_library_sha256":library_hash,
                "semantic_configuration":config,
                "semantic_configuration_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&config).map_err(|e|e.to_string())?)),
                "physical_inputs":physical_inputs,"canonical_decoded_input_digest_scheme":"project-c.record-stream.v1: four u64 big-endian lengths and logical line bytes per record",
                "canonical_decoded_input_sha256":decoded_hashes,"decoded_bytes":decoded_bytes,
                "software_build_identity":{"package":env!("CARGO_PKG_NAME"),"version":env!("CARGO_PKG_VERSION")},
                "requested_backend":"serial-scalar","executed_backend":"serial-scalar",
                "discovery_selector":discovery_json,"evidence_selector":cfg.evidence.selector(),"deterministic_artifacts":artifacts
            }),
        ),
    );
    let mut writer = DualJsonlWriter::new(&stage.join("scientific-provenance.jsonl"), cfg, budget)?;
    // Reserve the complete row before emitting any bytes, exactly as jsonl does.
    // The counting pass holds at most one declared row in memory.
    let mut size = 0u64;
    stream_provenance(cfg, a, b, &library, &provenance, &mut |bytes| {
        size = size
            .checked_add(bytes.len() as u64)
            .ok_or("arithmetic.overflow")?;
        Ok(())
    })?;
    writer.budget.charge(size, true)?;
    writer.prepaid = true;
    stream_provenance(cfg, a, b, &library, &provenance, &mut |bytes| {
        writer.bytes(bytes)
    })?;
    let (_, manifest_hash, _) = writer.finish()?;
    Ok((manifest_hash, artifacts.len() + 1))
}

fn execution(
    stage: &Path,
    cfg: &DualConfig,
    state: ExecutionState<'_>,
    budget: &mut OutputBudget,
) -> Result<()> {
    if !cfg.execution_profile.audited() {
        let mut config = json!({"mode":"dual","A":component_json(&cfg.a),"B":component_json(&cfg.b),
            "radius":cfg.radius,"quality":cfg.min_phred.map_or(json!("off"),|n|json!({"min_phred":n})),
            "discovery":match cfg.discovery_mode { DiscoveryMode::Bounded(k)=>json!({"mode":"bounded","K":k}),DiscoveryMode::Exact(k)=>json!({"mode":"exact","limit":k}) },
            "evidence":cfg.evidence.selector(),"paired":cfg.r2.is_some(),"resource_limits":cfg.limits.as_json()});
        if cfg.compact_reference.is_some() || cfg.compact_output {
            config["construct_format"] = json!(if cfg.compact_reference.is_some(){"ordered-pairs-u32le-v1"}else{"tsv"});
            config["output_format"] = json!(if cfg.compact_output{"ordinal-u64le-v1"}else{"jsonl"});
        }
        let mut inputs = vec![cfg.r1.clone()];
        if let Some(r2) = &cfg.r2 {
            inputs.push(r2.clone());
        }
        if let Some(barcode) = &cfg.barcode {
            inputs.push(barcode.r3.clone());
            config["barcode_concordance"] = json!({"r3":barcode.r3.to_string_lossy(),"barcode_whitelist":barcode.whitelist.to_string_lossy(),"requested_window":barcode.window,"matching_policy":{"kind":"exact","radius":0},"profile_id":CONCORDANCE_PROFILE});
            if barcode.radius == 1 {
                config["barcode_concordance"]["barcode_radius"] = json!(1);
                config["barcode_concordance"]["matching_policy"] = json!({"kind":"hamming","radius":1,"ambiguity":"unmapped","exact_priority":true});
                config["barcode_concordance"]["profile_id"] = json!(RADIUS1_PROFILE);
            }
        }
        return standard_execution(
            stage,
            &cfg.sample,
            config,
            if cfg.barcode.is_some() {
                concordance_contract(cfg)
            } else {
                CONTRACT
            },
            inputs,
            cfg.workers,
            state,
            budget,
        );
    }
    let mut input_paths = if let Some(r2) = &cfg.r2 {
        vec![
            cfg.r1.to_string_lossy().to_string(),
            r2.to_string_lossy().to_string(),
        ]
    } else {
        vec![cfg.r1.to_string_lossy().to_string()]
    };
    if let Some(barcode) = &cfg.barcode {
        input_paths.push(barcode.r3.to_string_lossy().to_string());
    }
    let record = active_row(
        cfg,
        row(
            "project-c.execution.v1",
            json!({
                "execution_profile":cfg.execution_profile.name(),"scientific_manifest_sha256":state.manifest_hash.map_or(json!({"unavailable":"run_incomplete"}),|h|json!(h)),
                "execution_id":format!("{}-{}",std::process::id(),state.started),
                "host_os_cpu_storage":{"unavailable":"not_collected_in_vertical_slice_3"},
                "input_paths":input_paths,"output_path":cfg.output.to_string_lossy(),
                "start_unix_ms":state.started,"end_unix_ms":unix_ms(),
                "elapsed_ms":state.elapsed_ms.to_string(),
                "worker_count":cfg.workers.count,"scheduling":if cfg.workers.count == 1 {"serial"} else {"bounded-worker-local"},
                "batch_records":cfg.workers.batch_records,"work_queue_capacity":cfg.workers.work_queue_capacity,
                "result_queue_capacity":cfg.workers.result_queue_capacity,"command_config":env::args().collect::<Vec<_>>(),
                "cpu_time":{"unavailable":"not_collected"},"peak_rss":{"unavailable":"not_collected"},
                "physical_input_bytes":{"unavailable":"not_collected"},
                "decoded_bytes":state.decoded_bytes,"processed_prefix_records":state.processed,
                "output_bytes":budget.used(),
                "output_cardinality":{"unavailable":"not_collected"},
                "peak_internal_outstanding_bytes":{"unavailable":"not_collected"},
                "peak_temporary_storage_bytes":{"unavailable":"not_collected"},
                "outcome":state.outcome,"fault":state.fault
            }),
        ),
    );
    jsonl(
        &stage.join("execution.jsonl"),
        &[record],
        budget,
        false,
        cfg.execution_profile,
    )?;
    Ok(())
}

struct DualWorkerState {
    counts: HashMap<PairId, u64>,
    unexpected_count: u64,
    leaves: [u64; 12],
    qc: DualQc,
    discovery: Discovery,
    total: u64,
}

impl DualWorkerState {
    fn new(discovery_mode: DiscoveryMode) -> Self {
        Self {
            counts: HashMap::new(),
            unexpected_count: 0,
            leaves: [0; 12],
            qc: DualQc::default(),
            total: 0,
            discovery: Discovery::new(
                discovery_mode,
                &[
                    "recombinant.unexpected",
                    "unmapped.layout",
                    "unmapped.A_known",
                    "unmapped.B_known",
                    "unmapped.neither",
                ],
            ),
        }
    }
}

fn parallel_dual(
    cfg: &DualConfig,
    r1: &mut Fastq,
    r2: &mut Option<Fastq>,
    r3: &mut Option<Fastq>,
    barcode: Option<&BarcodeOracle>,
    guides: [&[Guide]; 2],
    indices: [&HashMap<String, usize>; 2],
    expected: &ExpectedPairs,
    count_cap: u64,
    effective_workers: WorkerConfig,
) -> Result<DualWorkerState> {
    // Ordinary dual already admits both decoder and recycle queues. Keep
    // concordance's separately qualified legacy allocation path unchanged.
    let recycle = if cfg.barcode.is_none() {
        [r1.recycle_sender.clone(), r2.as_ref().and_then(|r| r.recycle_sender.clone())]
    } else {
        [None, None]
    };
    let make = || DualWorkerState::new(cfg.discovery_mode);
    let read = || -> Result<Option<(FastqRecord, Option<FastqRecord>, Option<FastqRecord>)>> {
        let first = r1.record()?;
        let second = if let Some(reader) = r2.as_mut() {
            reader.record()?
        } else {
            None
        };
        let third = r3
            .as_mut()
            .map(|reader| reader.record())
            .transpose()?
            .flatten();
        let Some(first) = first else {
            if third.is_some() {
                return Err("input.barcode_unequal_eof".into());
            }
            if second.is_some() {
                return Err("input.unequal_eof".into());
            }
            return Ok(None);
        };
        if r2.is_some() && second.is_none() {
            return Err("input.unequal_eof".into());
        }
        if let Some(second) = &second {
            let _pair = profile::start(profile::FASTQ_PAIR_COORDINATE_VALIDATE);
            check_pair(&first, second)?;
        }
        if r3.is_some() && third.is_none() {
            return Err("input.barcode_unequal_eof".into());
        }
        if let Some(third) = &third {
            check_barcode_name(&first, third)?;
        }
        Ok(Some((first, second, third)))
    };
    let process = |state: &mut DualWorkerState,
                   (first, second, third): (
        FastqRecord,
        Option<FastqRecord>,
        Option<FastqRecord>,
    )|
     -> Result<()> {
        let _worker = profile::start(profile::WORKER);
        let _classify = profile::start(profile::CLASSIFY);
        let (result, outcome) = if let (Some(oracle), Some(third)) = (barcode, third.as_ref()) {
            let (result, outcome) = classify_triplet(
                [&first, second.as_ref().expect("R2 active"), third],
                cfg,
                guides,
                indices,
                expected,
                oracle,
            );
            (result, Some(outcome))
        } else {
            (
                classify_unit(&first, second.as_ref(), cfg, guides, indices, expected),
                None,
            )
        };
        drop(_classify);
        increment(&mut state.total)?;
        for (role, component) in result.components.iter().enumerate() {
            increment(&mut state.qc.components[role][component_state(component.leaf)])?;
        }
        // Components are accounted before the concordance filter, just as in
        // the serial path. Barcode rejections never enter leaves/counts/discovery.
        if matches!(
            outcome,
            Some(ConcordanceOutcome::RescuedValid(_) | ConcordanceOutcome::RescuedDiscordant)
        ) {
            increment(&mut state.qc.radius1_rescued)?;
        }
        if matches!(outcome, Some(ConcordanceOutcome::AmbiguousBarcode)) {
            increment(&mut state.qc.radius1_ambiguous)?;
        }
        match outcome {
            Some(ConcordanceOutcome::DiscordantTriplet | ConcordanceOutcome::RescuedDiscordant) => {
                increment(&mut state.qc.discordant_triplet)?;
                return Ok(());
            }
            Some(ConcordanceOutcome::UnmappedBarcode | ConcordanceOutcome::AmbiguousBarcode) => {
                increment(&mut state.qc.unmapped_barcode)?;
                return Ok(());
            }
            Some(ConcordanceOutcome::Valid(_) | ConcordanceOutcome::RescuedValid(_)) => {
                increment(&mut state.qc.concordant)?;
            }
            _ => {}
        }
        increment(&mut state.leaves[result.leaf])?;
        if let Some(reason) = result.extraction {
            increment(&mut state.qc.extraction[extraction_slot(reason)])?;
        }
        if let Some(distance) = result.distance {
            match result.leaf {
                0 => increment(&mut state.qc.mapped_distance[distance])?,
                1 => increment(&mut state.qc.recombinant_distance[distance])?,
                _ => return Err("conservation.distance".into()),
            }
        }
        if let Some(i) = result.construct {
            let count = classified_count_entry(
                &mut state.counts,
                &mut state.unexpected_count,
                i,
                result.leaf,
                count_cap,
            )?;
            #[cfg(debug_assertions)]
            if *count == 0
                && env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                    .is_ok_and(|s| s == "worker_counter_overflow")
            {
                *count = u64::MAX;
            }
            increment(count)?;
        }
        if let Some(index) = DISCOVERY_LEAVES.iter().position(|&i| i == result.leaf) {
            let _discovery = profile::start(profile::DISCOVERY);
            let label = |leaf| match leaf {
                0 => "unique",
                1 => "ambiguous",
                2 => "unknown",
                _ => "invalid",
            };
            state.discovery.observe(
                index,
                &[
                    label(result.components[0].leaf),
                    label(result.components[1].leaf),
                ],
                &[
                    result.components[0].window.as_deref().unwrap_or("absent"),
                    result.components[1].window.as_deref().unwrap_or("absent"),
                ],
            )?;
        }
        // Return only successfully processed records. A full or disconnected
        // bounded recycler drops the record, matching Fastq::recycle. Never
        // block a worker against a full decoder/recycle pipeline.
        if let Some(sender) = &recycle[0] {
            let _ = sender.try_send(first);
        }
        if let (Some(sender), Some(record)) = (&recycle[1], second) {
            let _ = sender.try_send(record);
        }
        Ok(())
    };
    let states = worker::run_batches_partitioned(
        effective_workers,
        cfg.workers.batch_records,
        read,
        false,
        make,
        process,
    )?;
    eprintln!(
        "resource.worker_count_storage={}",
        json!(
            states
                .iter()
                .map(|state| json!({"expected_keys":state.counts.len(),
            "expected_capacity":state.counts.capacity(),"unexpected_count":state.unexpected_count}))
                .collect::<Vec<_>>()
        )
    );
    let _merge = profile::start(profile::MERGE);
    let mut states = states.into_iter();
    // Active concordance can retain many distinct pairs per worker. Reuse the
    // first worker's accumulator rather than holding a fifth map during reduction.
    let mut merged = if barcode.is_some() {
        states.next().ok_or("worker.missing_result")?
    } else {
        make()
    };
    for state in states {
        merged.total = merged
            .total
            .checked_add(state.total)
            .ok_or("arithmetic.overflow")?;
        merge_unexpected_count(&mut merged.unexpected_count, state.unexpected_count)?;
        for (pair, value) in state.counts {
            let target = bounded_count_entry(&mut merged.counts, pair, count_cap)?;
            #[cfg(debug_assertions)]
            if env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                .is_ok_and(|s| s == "worker_reduction_overflow")
            {
                *target = u64::MAX;
            }
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target, value) in merged.leaves.iter_mut().zip(state.leaves) {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target_role, source_role) in merged.qc.components.iter_mut().zip(state.qc.components) {
            for (target, value) in target_role.iter_mut().zip(source_role) {
                *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
            }
        }
        for (target, value) in merged.qc.extraction.iter_mut().zip(state.qc.extraction) {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target, value) in merged
            .qc
            .mapped_distance
            .iter_mut()
            .zip(state.qc.mapped_distance)
        {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target, value) in merged
            .qc
            .recombinant_distance
            .iter_mut()
            .zip(state.qc.recombinant_distance)
        {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target, value) in [
            (&mut merged.qc.concordant, state.qc.concordant),
            (
                &mut merged.qc.discordant_triplet,
                state.qc.discordant_triplet,
            ),
            (&mut merged.qc.unmapped_barcode, state.qc.unmapped_barcode),
            (&mut merged.qc.radius1_rescued, state.qc.radius1_rescued),
            (&mut merged.qc.radius1_ambiguous, state.qc.radius1_ambiguous),
        ] {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        merged.discovery.merge(state.discovery)?;
    }
    Ok(merged)
}

fn process(
    cfg: &DualConfig,
    stage: &Path,
    started: u64,
    clock: Instant,
    budget: &mut OutputBudget,
) -> Result<()> {
    let reference_clock = Instant::now();
    let counted_rows = cfg.compact_reference.as_ref().map_or_else(|| construct_row_count(&cfg.constructs), |m| usize::try_from(m.pair_count).map_err(|_| "resource.dimension_overflow".into()));
    // The owner's new safety gate precedes any library-sized allocation/admission.
    // Other count-pass faults are deferred to retain reference/guide fault priority.
    if counted_rows
        .as_ref()
        .is_err_and(|fault| fault == "STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX")
    {
        return Err(counted_rows.unwrap_err());
    }
    let mut reference_paths: Vec<&Path> = vec![&cfg.guides_a, &cfg.guides_b, &cfg.constructs];
    if let Some(barcode) = &cfg.barcode {
        reference_paths.push(&barcode.whitelist);
    }
    if let Some(meta) = &cfg.compact_reference { reference_paths.push(&meta.raw_path); }
    cfg.limits
        .preflight_dual_reference_files(&reference_paths)?;
    let a = load_guides(&cfg.guides_a)?;
    let b = load_guides(&cfg.guides_b)?;
    let construct_count = counted_rows?;
    if a.len() > u32::MAX as usize || b.len() > u32::MAX as usize {
        return Err("resource.dimension_overflow".into());
    }
    if let Some(meta) = &cfg.compact_reference {
        compact_reference::validate(meta)?;
    }
    let (id_bytes, max_construct_line) = if cfg.compact_reference.is_some() {
        let a_max = a.iter().map(|g| g.id.len() as u64).max().unwrap_or(0);
        let b_max = b.iter().map(|g| g.id.len() as u64).max().unwrap_or(0);
        (0, a_max.checked_add(b_max).and_then(|n| n.checked_add(30)).ok_or("resource.dimension_overflow")?)
    } else { construct_sizes(&cfg.constructs)? };
    let records_bound = input_records_bound(&cfg.r1);
    let cartesian = (a.len() as u64)
        .checked_mul(b.len() as u64)
        .ok_or("resource.dimension_overflow")?;
    let unexpected_pairs_bound = cartesian
        .saturating_sub(construct_count as u64)
        .min(records_bound);
    let count_cap = records_bound.min(construct_count as u64);
    let mut counts = HashMap::<PairId, u64>::new();
    let mut unexpected_count = 0_u64;
    let reference_bytes = reference_paths.iter().try_fold(0_u64, |sum, path| {
        sum.checked_add(
            fs::metadata(path)
                .map_err(|e| format!("preflight.reference_open: {e}"))?
                .len(),
        )
        .ok_or_else(|| "resource.dimension_overflow".to_string())
    })?;
    let discovery_k = match cfg.discovery_mode {
        DiscoveryMode::Bounded(k) => k,
        DiscoveryMode::Exact(k) => k,
    };
    let dimensions = Dimensions {
        parallel_extra: cfg
            .limits
            .plan_workers(
                cfg.workers,
                0, // Sparse map storage is charged by plan_dual; retain queue/state terms.
                5,
                discovery_k,
                a[0].sequence.len().max(b[0].sequence.len()),
            )?
            // Each active concordance work item owns three records. Charging
            // the complete worker addition three times also bounds tuple overhead.
            .checked_mul(if cfg.barcode.is_some() { 3 } else { 1 })
            .and_then(|bytes| {
                let filter_bytes = if std::env::var("PROJECT_C_BLOOM_BYPASS").as_deref() == Ok("1")
                {
                    0
                } else {
                    BloomFilter::byte_len_for_pairs(construct_count)
                };
                bytes.checked_add(filter_bytes as u64)
            })
            .ok_or("resource.dimension_overflow")?,
        guide_count: a
            .len()
            .checked_add(b.len())
            .ok_or("resource.dimension_overflow")?,
        construct_count,
        matrix_rows: construct_count,
        reference_bytes,
        max_window_bytes: a[0].sequence.len().max(b[0].sequence.len()),
        input_count: if cfg.barcode.is_some() {
            3
        } else if cfg.r2.is_some() {
            2
        } else {
            1
        },
        discovery_leaves: 5,
        k: discovery_k,
        evidence_capacity: cfg.evidence.capacity(),
        evidence_enabled: !matches!(cfg.evidence, EvidenceMode::Off),
        evidence_identity_bytes: cfg.evidence.identity_bytes(&cfg.sample)?,
    };
    let sparse_dimensions = SparseDimensions {
        input_records_bound: records_bound,
        worker_count: cfg.workers.count,
        id_bytes,
        max_encoded_row: max_construct_line
            .max(
                a.iter()
                    .chain(&b)
                    .map(|g| (g.id.len() + g.sequence.len()) as u64)
                    .max()
                    .unwrap_or(0),
            )
            .checked_mul(6)
            .and_then(|n| n.checked_add((cfg.sample.len() as u64).checked_mul(6)?))
            .and_then(|n| n.checked_add(4_096))
            .ok_or("resource.dimension_overflow")?,
        unexpected_pairs_bound,
        // Construct rows are streamed; guides and barcode indexes remain retained.
        retained_reference_bytes: reference_bytes
            .checked_sub(
                fs::metadata(&cfg.constructs)
                    .map_err(|e| format!("preflight.reference_open: {e}"))?
                    .len(),
            )
            .and_then(|n| n.checked_sub(cfg.compact_reference.as_ref().map_or(0, |m| m.reference_bytes)))
            .ok_or("resource.dimension_overflow")?,
        audited: cfg.execution_profile.audited(),
    };
    let bounded_plan: Option<crate::resource::BoundedDualPlan> = if cfg.barcode.is_none() {
        Some(
            cfg.limits
                .plan_bounded_dual_formats(dimensions, sparse_dimensions, cfg.workers, cfg.compact_reference.is_some(), cfg.compact_output)?,
        )
    } else {
        None
    };
    let baseline_memory = if let Some(plan) = &bounded_plan {
        eprintln!("resource.bounded_dual_plan={}", plan.components);
        plan.estimate_bytes
    } else {
        cfg.limits.plan_dual(dimensions, sparse_dimensions)?
    };
    let recycle_record_capacity = bounded_plan
        .as_ref()
        .map_or(4096, |plan| plan.recycle_record_capacity);
    let input_batch_records = bounded_plan.as_ref().map_or(FASTQ_BATCH_RECORDS, |p| p.input_batch_records);
    let effective_workers = bounded_plan
        .as_ref()
        .map_or(cfg.workers, |plan| plan.effective_workers);
    let expected = if let Some(meta) = &cfg.compact_reference {
        let mut pairs = HashSet::new();
        compact_reference::stream_pairs(meta, &a, &b, |_, ai, bi| {
            if !pairs.insert((ai, bi)) { return Err("preflight.duplicate_pair".into()); }
            Ok(())
        })?;
        ExpectedPairs::new(pairs)
    } else { load_membership(&cfg.constructs, &a, &b)? };
    let barcode = cfg
        .barcode
        .as_ref()
        .map(|config| {
            BarcodeOracle::load(
                config,
                &a,
                &b,
                cfg.execution_profile.audited(),
                cfg.limits.memory_budget_bytes - baseline_memory,
            )
        })
        .transpose()?;
    for (component, length) in [(&cfg.a, a[0].sequence.len()), (&cfg.b, b[0].sequence.len())] {
        if let Geometry::Fixed { start } = &component.geometry {
            start
                .checked_add(length)
                .ok_or("preflight.window_overflow")?;
        }
    }
    let ai: HashMap<String, usize> = a
        .iter()
        .enumerate()
        .map(|(i, g)| (g.sequence.clone(), i))
        .collect();
    let bi: HashMap<String, usize> = b
        .iter()
        .enumerate()
        .map(|(i, g)| (g.sequence.clone(), i))
        .collect();
    eprintln!("cclamp.reference_loading_seconds={:.9}", reference_clock.elapsed().as_secs_f64());
    let counting_clock = Instant::now();
    let mut r1 = Fastq::open(&cfg.r1, cfg.limits, cfg.execution_profile)?;
    let mut r2 = cfg
        .r2
        .as_ref()
        .map(|p| Fastq::open(p, cfg.limits, cfg.execution_profile))
        .transpose()?;
    let mut r3 = cfg
        .barcode
        .as_ref()
        .map(|config| Fastq::open(&config.r3, cfg.limits, cfg.execution_profile))
        .transpose()?;
    if let Some(reader) = r3.as_mut() {
        reader.spawn_worker();
    }
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|value| value == "ordinal_overflow") {
        r1.ordinal = u64::MAX;
    }
    r1.spawn_worker_with_geometry(input_batch_records, recycle_record_capacity);
    if let Some(r) = r2.as_mut() {
        r.spawn_worker_with_geometry(input_batch_records, recycle_record_capacity);
    }
    let mut leaves = [0_u64; 12];
    let mut qc = DualQc::default();
    let mut discovery = Discovery::new(
        cfg.discovery_mode,
        &[
            "recombinant.unexpected",
            "unmapped.layout",
            "unmapped.A_known",
            "unmapped.B_known",
            "unmapped.neither",
        ],
    );
    let mut evidence = EvidenceSink::new_with_concordance(
        cfg.evidence.clone(),
        &cfg.sample,
        stage,
        budget,
        cfg.execution_profile,
        barcode.as_ref().map(|oracle| oracle.bindings.clone()),
    )?;
    let mut total = 0_u64;
    let counting = (|| -> Result<()> {
        if cfg.workers.count > 1 {
            let state = parallel_dual(
                cfg,
                &mut r1,
                &mut r2,
                &mut r3,
                barcode.as_ref(),
                [&a, &b],
                [&ai, &bi],
                &expected,
                count_cap,
                effective_workers,
            )?;
            counts = state.counts;
            unexpected_count = state.unexpected_count;
            #[cfg(debug_assertions)]
            if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|s| s == "counter_overflow") {
                if let Some(count) = counts.values_mut().next() {
                    *count = u64::MAX;
                    increment(count)?;
                } else if unexpected_count != 0 {
                    unexpected_count = u64::MAX;
                    increment(&mut unexpected_count)?;
                }
            }
            leaves = state.leaves;
            qc = state.qc;
            discovery = state.discovery;
            total = state.total;
            return Ok(());
        }
        loop {
            let record1 = r1.record()?;
            let record2 = if let Some(reader) = r2.as_mut() {
                reader.record()?
            } else {
                None
            };
            let record3 = r3
                .as_mut()
                .map(|reader| reader.record())
                .transpose()?
                .flatten();
            let Some(record1) = record1 else {
                if record3.is_some() {
                    return Err("input.barcode_unequal_eof".into());
                }
                if record2.is_some() {
                    return Err("input.unequal_eof".into());
                }
                break;
            };
            if r2.is_some() && record2.is_none() {
                return Err("input.unequal_eof".into());
            }
            if let Some(record2) = &record2 {
                let _pair = profile::start(profile::FASTQ_PAIR_COORDINATE_VALIDATE);
                check_pair(&record1, record2)?;
            }
            if r3.is_some() && record3.is_none() {
                return Err("input.barcode_unequal_eof".into());
            }
            if let Some(record) = &record3 {
                check_barcode_name(&record1, record)?;
            }
            let _classify = profile::start(profile::CLASSIFY);
            let (result, outcome) = if let (Some(oracle), Some(record3)) = (&barcode, &record3) {
                let (result, outcome) = classify_triplet(
                    [&record1, record2.as_ref().expect("R2 active"), record3],
                    cfg,
                    [&a, &b],
                    [&ai, &bi],
                    &expected,
                    oracle,
                );
                (result, Some(outcome))
            } else {
                (
                    classify_unit(
                        &record1,
                        record2.as_ref(),
                        cfg,
                        [&a, &b],
                        [&ai, &bi],
                        &expected,
                    ),
                    None,
                )
            };
            drop(_classify);
            total = total.checked_add(1).ok_or("arithmetic.overflow")?;
            for (role, component) in result.components.iter().enumerate() {
                increment(&mut qc.components[role][component_state(component.leaf)])?;
            }
            if matches!(
                outcome,
                Some(ConcordanceOutcome::RescuedValid(_) | ConcordanceOutcome::RescuedDiscordant)
            ) {
                increment(&mut qc.radius1_rescued)?;
            }
            if matches!(outcome, Some(ConcordanceOutcome::AmbiguousBarcode)) {
                increment(&mut qc.radius1_ambiguous)?;
            }
            let rejection = match outcome {
                Some(
                    ConcordanceOutcome::DiscordantTriplet | ConcordanceOutcome::RescuedDiscordant,
                ) => {
                    increment(&mut qc.discordant_triplet)?;
                    Some("DiscordantTriplet")
                }
                Some(
                    ConcordanceOutcome::UnmappedBarcode | ConcordanceOutcome::AmbiguousBarcode,
                ) => {
                    increment(&mut qc.unmapped_barcode)?;
                    Some("UnmappedBarcode")
                }
                Some(ConcordanceOutcome::Valid(_) | ConcordanceOutcome::RescuedValid(_)) => {
                    increment(&mut qc.concordant)?;
                    None
                }
                _ => None,
            };
            if let Some(rejection) = rejection {
                if let Some(priority) = evidence.selected_priority(total)? {
                    evidence.observe(total, priority, json!({"terminal_leaf":null,"reason":rejection,
                        "concordance_outcome":rejection,
                        "extracted_windows":{"A":result.components[0].window,"B":result.components[1].window},
                        "components": result.components.iter().enumerate().map(|(i,c)| json!({"role":if i==0 {"A"} else {"B"},"source":if i==0 {"R1"} else {"R2"},"state":"unique","guide_id":if i==0 {&a[c.guide.unwrap()].id} else {&b[c.guide.unwrap()].id},"distance":c.distance})).collect::<Vec<_>>()}), budget)?;
                }
                r1.recycle(record1);
                if let (Some(reader), Some(record)) = (r2.as_mut(), record2) {
                    reader.recycle(record);
                }
                if let (Some(reader), Some(record)) = (r3.as_mut(), record3) {
                    reader.recycle(record);
                }
                continue;
            }
            increment(&mut leaves[result.leaf])?;
            if let Some(reason) = result.extraction {
                increment(&mut qc.extraction[extraction_slot(reason)])?;
            }
            if let Some(distance) = result.distance {
                match result.leaf {
                    0 => increment(&mut qc.mapped_distance[distance])?,
                    1 => increment(&mut qc.recombinant_distance[distance])?,
                    _ => return Err("conservation.distance".into()),
                }
            }
            if let Some(i) = result.construct {
                let count = classified_count_entry(
                    &mut counts,
                    &mut unexpected_count,
                    i,
                    result.leaf,
                    count_cap,
                )?;
                #[cfg(debug_assertions)]
                if *count == 0
                    && env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                        .is_ok_and(|s| s == "counter_overflow")
                {
                    *count = u64::MAX;
                }
                increment(count)?;
            }
            if let Some(index) = DISCOVERY_LEAVES.iter().position(|&i| i == result.leaf) {
                let _discovery = profile::start(profile::DISCOVERY);
                let state = |leaf| match leaf {
                    0 => "unique",
                    1 => "ambiguous",
                    2 => "unknown",
                    _ => "invalid",
                };
                let components = &result.components;
                discovery.observe(
                    index,
                    &[state(components[0].leaf), state(components[1].leaf)],
                    &[
                        components[0].window.as_deref().unwrap_or("absent"),
                        components[1].window.as_deref().unwrap_or("absent"),
                    ],
                )?;
            }
            if let Some(priority) = evidence.selected_priority(total)? {
                let _evidence = profile::start(profile::EVIDENCE);
                let component_event = |component: &Classification,
                                       guides: &[Guide],
                                       role: &str,
                                       source: Source| {
                    json!({"role":role,"state":match component.leaf {0=>"unique",1=>"ambiguous",2=>"unknown",_=>"invalid"},
                        "source":match source {Source::R1=>"R1",Source::R2=>"R2"},
                        "guide_id":component.guide.map(|i| &guides[i].id),"distance":component.distance})
                };
                let reason = match result.extraction {
                    Some(ExtractionFault::MissingAnchor) => Some("missing_anchor"),
                    Some(ExtractionFault::MultipleAnchors) => Some("multiple_anchors"),
                    Some(ExtractionFault::OutOfBounds) => Some("out_of_bounds"),
                    None if result.leaf == 3 || result.leaf == 4 => Some("layout"),
                    None if result.leaf == 1 => Some("unexpected_pair"),
                    None if result.leaf >= 8 => Some(DUAL_LEAVES[result.leaf]),
                    None => None,
                };
                evidence.observe(total, priority, json!({
                    "terminal_leaf":DUAL_LEAVES[result.leaf],"reason":reason,
                    "extracted_windows":{"A":result.components[0].window,"B":result.components[1].window},
                    "components":[component_event(&result.components[0], &a, "A",cfg.a.source),component_event(&result.components[1], &b, "B",cfg.b.source)]
                }), budget)?;
            }
            if let (Some(reader), Some(record)) = (r3.as_mut(), record3) {
                reader.recycle(record);
            }
            r1.recycle(record1);
            if let (Some(reader), Some(record)) = (r2.as_mut(), record2) {
                reader.recycle(record);
            }
        }
        Ok(())
    })();
    if let Err(fault) = counting {
        let decoded_bytes = r1
            .decoded_bytes
            .checked_add(r2.as_ref().map_or(0, |r| r.decoded_bytes))
            .and_then(|n| n.checked_add(r3.as_ref().map_or(0, |r| r.decoded_bytes)))
            .ok_or("arithmetic.overflow")?;
        let _ = execution(
            stage,
            cfg,
            ExecutionState {
                outcome: "FAILED",
                fault: Some(&fault),
                manifest_hash: None,
                started,
                elapsed_ms: clock.elapsed().as_millis(),
                processed: total,
                decoded_bytes,
            },
            budget,
        );
        return Err(fault);
    }
    let decoded_bytes = r1
        .decoded_bytes
        .checked_add(r2.as_ref().map_or(0, |r| r.decoded_bytes))
        .and_then(|n| n.checked_add(r3.as_ref().map_or(0, |r| r.decoded_bytes)))
        .ok_or("arithmetic.overflow")?;
    if let (Some(oracle), Some(config)) = (&barcode, &cfg.barcode) {
        oracle.verify_bindings(config)?;
    }
    eprintln!("cclamp.counting_seconds={:.9}", counting_clock.elapsed().as_secs_f64());
    expected.filter.write_diagnostics()?;
    let _evidence = profile::start(profile::EVIDENCE);
    let evidence_artifact = evidence.finish(budget)?;
    drop(_evidence);
    let _output = profile::start(profile::OUTPUT);
    let output_clock = Instant::now();
    let mut hashes = vec![
        r1.digest.take()
            .map_or_else(String::new, |h| format!("{:x}", h.finalize())),
    ];
    if let Some(mut r2) = r2 {
        hashes.push(
            r2.digest.take()
                .map_or_else(String::new, |h| format!("{:x}", h.finalize())),
        );
    }
    if let Some(mut reader) = r3 {
        hashes.push(
            reader
                .digest.take()
                .map_or_else(String::new, |h| format!("{:x}", h.finalize())),
        );
    }
    eprintln!(
        "resource.observed_count_storage={}",
        json!({
            "expected_keys":counts.len(),"expected_capacity":counts.capacity(),
            "unexpected_count":unexpected_count,"recycle_record_capacity":recycle_record_capacity
        })
    );
    let (manifest_hash, artifact_count) = science(
        stage,
        cfg,
        DualScience {
            a: &a,
            b: &b,
            counts: &mut counts,
            unexpected_count,
            leaves: &leaves,
            qc: &qc,
            total,
            decoded_bytes,
            decoded_hashes: hashes,
            discovery: &discovery,
            evidence_artifact,
            barcode_bindings: barcode.as_ref().map(|oracle| &oracle.bindings),
        },
        budget,
    )?;
    execution(
        stage,
        cfg,
        ExecutionState {
            outcome: "READY_TO_PUBLISH",
            fault: None,
            manifest_hash: Some(&manifest_hash),
            started,
            elapsed_ms: clock.elapsed().as_millis(),
            processed: total,
            decoded_bytes,
        },
        budget,
    )?;
    eprintln!("cclamp.native_output_seconds={:.9}", output_clock.elapsed().as_secs_f64());
    drop(_output);
    if !cfg.execution_profile.audited() {
        return publish_standard(stage, &cfg.output, budget);
    }
    let bundle_id = format!(
        "{:x}",
        Sha256::digest(format!("{}{}", cfg.sample, manifest_hash).as_bytes())
    );
    let completion = active_row(
        cfg,
        row(
            "project-c.completion.v1",
            json!({"execution_profile":cfg.execution_profile.name(),"status":"COMPLETE",
        "scientific_manifest_sha256":manifest_hash,"bundle_identity":bundle_id,
        "deterministic_artifact_count":artifact_count,
        "verification_durability_profile_id":"project-c.local-fsync-symlink.v1"}),
        ),
    );
    publish_bundle(stage, &cfg.output, &completion, budget)?;
    Ok(())
}

pub(super) fn run_cli(started: u64, clock: Instant) -> Result<()> {
    let cfg = parse()?;
    let parent = cfg.output.parent().unwrap_or(Path::new("."));
    if !parent.is_dir() {
        return Err("preflight.output_parent".into());
    }
    let stage = parent.join(format!(
        ".{}.stage.{}.{}",
        cfg.output.file_name().expect("name").to_string_lossy(),
        std::process::id(),
        started
    ));
    fs::create_dir(&stage).map_err(|e| format!("publication.stage: {e}"))?;
    let mut budget = OutputBudget::new(cfg.limits);
    let result = process(&cfg, &stage, started, clock, &mut budget);
    if !cfg.execution_profile.audited() {
        return result.map_err(|e| cleanup_standard(&stage, e));
    }
    if let Err(ref fault) = result
        && !stage.join("execution.jsonl").exists()
    {
        let _ = execution(
            &stage,
            &cfg,
            ExecutionState {
                outcome: "FAILED",
                fault: Some(fault),
                manifest_hash: None,
                started,
                elapsed_ms: clock.elapsed().as_millis(),
                processed: 0,
                decoded_bytes: 0,
            },
            &mut budget,
        );
    }
    result
}

#[cfg(test)]
mod layout_index_tests {
    use super::*;
    fn linear_oracle(
        a_window: &str,
        b_window: &str,
        a: &[Guide],
        b: &[Guide],
        constructs: &[Construct],
    ) -> usize {
        let transforms = [
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (false, true, true),
            (true, true, false),
            (true, false, true),
            (true, true, true),
        ];
        let mut explained = HashSet::new();
        for (swap, complement_a, complement_b) in transforms {
            let (left, right) = if swap {
                (b_window, a_window)
            } else {
                (a_window, b_window)
            };
            let left = if complement_a {
                reverse_complement(left)
            } else {
                left.to_owned()
            };
            let right = if complement_b {
                reverse_complement(right)
            } else {
                right.to_owned()
            };
            if left.len() != a[0].sequence.len() || right.len() != b[0].sequence.len() {
                continue;
            }
            for (i, construct) in constructs.iter().enumerate() {
                if left == a[construct.a].sequence && right == b[construct.b].sequence {
                    explained.insert(i);
                    if explained.len() > 1 {
                        return 2;
                    }
                }
            }
        }
        explained.len()
    }

    #[test]
    fn exhaustive_index_matches_rc5_construct_identity_explanations() {
        fn words(n: usize) -> Vec<String> {
            (0..4usize.pow(n as u32))
                .map(|mut v| {
                    (0..n)
                        .map(|_| {
                            let b = b"ACGT"[v % 4];
                            v /= 4;
                            b as char
                        })
                        .collect()
                })
                .collect()
        }
        // Equal and unequal lengths, palindromes, overlapping role sequences,
        // sparse pair sets, and identities reached by several transforms.
        let mut totals = [0usize; 3];
        for (al, bl) in [(2, 2), (1, 2), (2, 1)] {
            let a: Vec<Guide> = words(al)
                .into_iter()
                .enumerate()
                .map(|(i, sequence)| Guide {
                    id: format!("a{i}"),
                    sequence,
                    count: 0,
                })
                .collect();
            let b: Vec<Guide> = words(bl)
                .into_iter()
                .enumerate()
                .map(|(i, sequence)| Guide {
                    id: format!("b{i}"),
                    sequence,
                    count: 0,
                })
                .collect();
            let ai = a
                .iter()
                .enumerate()
                .map(|(i, g)| (g.sequence.clone(), i))
                .collect();
            let bi = b
                .iter()
                .enumerate()
                .map(|(i, g)| (g.sequence.clone(), i))
                .collect();
            for modulus in [1, 3, 7, 257] {
                let constructs: Vec<Construct> = a
                    .iter()
                    .enumerate()
                    .flat_map(|(i, _)| {
                        b.iter().enumerate().filter_map(move |(j, _)| {
                            ((i * 17 + j) % modulus == 0).then_some(Construct {
                                id: format!("{i}-{j}"),
                                a: i,
                                b: j,
                            })
                        })
                    })
                    .collect();
                let expected = constructs
                    .iter()
                    .enumerate()
                    .map(|(_, c)| pair_id(c.a, c.b))
                    .collect();
                let windows: Vec<String> = (1..=3).flat_map(words).collect();
                let mut bins = [0usize; 3];
                for left in &windows {
                    for right in &windows {
                        let reference = linear_oracle(left, right, &a, &b, &constructs);
                        assert_eq!(
                            layout_explanations(left, right, &a, &b, [&ai, &bi], &expected),
                            reference,
                            "{al}/{bl} modulus={modulus} {left}/{right}"
                        );
                        bins[reference] += 1;
                    }
                }
                assert!(bins[0] > 0);
                for i in 0..3 {
                    totals[i] += bins[i];
                }
            }
        }
        assert!(totals.iter().all(|n| *n > 0));
        assert_eq!(totals.iter().sum::<usize>(), 84_672);
        println!("RC5 differential zero/one/multiple cases: {totals:?}");
    }
}

#[cfg(test)]
mod sparse_tests {
    use super::*;

    #[test]
    fn membership_gate_accepts_boundary_and_rejects_next_row() {
        assert_eq!(count_rows_up_to(&b"a\r\nb\nlast"[..], 3).unwrap(), 3);
        assert_eq!(count_rows_up_to(&b""[..], 0).unwrap(), 0);
        assert_eq!(
            count_rows_up_to(&b"a\nb\n"[..], 1).unwrap_err(),
            "STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX"
        );
        assert_eq!(
            count_rows_up_to(
                std::io::repeat(b'\n').take(MAX_MEMBERSHIP_ROWS as u64),
                MAX_MEMBERSHIP_ROWS
            )
            .unwrap(),
            MAX_MEMBERSHIP_ROWS
        );
        assert_eq!(
            count_rows_up_to(
                std::io::repeat(b'\n').take(MAX_MEMBERSHIP_ROWS as u64 + 1),
                MAX_MEMBERSHIP_ROWS
            )
            .unwrap_err(),
            "STOP_LIBRARY_TOO_LARGE_FOR_MEMORY_MEMBERSHIP_INDEX"
        );
    }

    #[test]
    fn sparse_cap_checks_new_entries_and_allows_existing_counts() {
        let mut counts = HashMap::new();
        increment(bounded_count_entry(&mut counts, (0, 0), 1).unwrap()).unwrap();
        increment(bounded_count_entry(&mut counts, (0, 0), 1).unwrap()).unwrap();
        assert_eq!(counts[&(0, 0)], 2);
        assert_eq!(
            bounded_count_entry(&mut counts, (0, 1), 1).unwrap_err(),
            "resource.sparse_count_cap"
        );
        assert_eq!(counts.len(), 1);
        assert!(bounded_count_entry(&mut HashMap::new(), (0, 0), 0).is_err());
    }

    #[test]
    fn unexpected_pair_cardinality_does_not_allocate_count_entries() {
        let mut counts = HashMap::new();
        let mut unexpected = 0;
        for a in 0..64 {
            for b in 0..64 {
                for _ in 0..3 {
                    increment(
                        classified_count_entry(&mut counts, &mut unexpected, (a, b), 1, 0).unwrap(),
                    )
                    .unwrap();
                }
            }
        }
        assert_eq!(unexpected, 12_288);
        assert_eq!(counts.capacity(), 0);
        increment(classified_count_entry(&mut counts, &mut unexpected, (0, 0), 0, 1).unwrap())
            .unwrap();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[&(0, 0)], 1);
        assert!(classified_count_entry(&mut counts, &mut unexpected, (1, 1), 0, 1,).is_err());
        assert_eq!(unexpected, 12_288);
        assert!(classified_count_entry(&mut counts, &mut unexpected, (1, 1), 2, 1,).is_err());
    }

    #[test]
    fn unexpected_scalar_increment_and_reduction_remain_checked() {
        let mut counts = HashMap::new();
        let mut unexpected = u64::MAX;
        assert_eq!(
            increment(classified_count_entry(&mut counts, &mut unexpected, (0, 1), 1, 0,).unwrap())
                .unwrap_err(),
            "arithmetic.overflow"
        );
        assert_eq!(unexpected, u64::MAX);
        let mut merged = 7;
        merge_unexpected_count(&mut merged, 9).unwrap();
        assert_eq!(merged, 16);
        merged = u64::MAX - 1;
        assert_eq!(
            merge_unexpected_count(&mut merged, 2).unwrap_err(),
            "arithmetic.overflow"
        );
        assert_eq!(merged, u64::MAX - 1);
        assert_eq!(counts.capacity(), 0);
    }

    #[test]
    fn malformed_construct_columns_keep_bounded_split_storage() {
        let path = env::temp_dir().join(format!(
            "project-c-construct-columns-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bytes = vec![b'\t'; MAX_LINE];
        fs::write(&path, bytes).unwrap();
        let result = stream_constructs_checked(&path, &[], &[], None, |_| Ok(()));
        fs::remove_file(path).unwrap();
        assert_eq!(result.unwrap_err(), "preflight.construct_columns");
    }

    #[test]
    fn worker_count_storage_starts_empty_independently_of_membership() {
        let expected: HashSet<PairId> = (0..100_000).map(|i| (i, i)).collect();
        let mut worker = DualWorkerState::new(DiscoveryMode::Bounded(0));
        assert_eq!(worker.counts.capacity(), 0);
        // Count only two observed pairs, including one outside the library.
        for pair in [(2, 2), (2, 2), (2, 3)] {
            increment(worker.counts.entry(pair).or_insert(0)).unwrap();
        }
        assert_eq!(worker.counts.len(), 2);
        assert_eq!(worker.counts[&(2, 2)], 2);
        assert!(worker.counts.capacity() < 10);
        assert!(!expected.contains(&(2, 3)));
        let filter = BloomFilter::from_pairs(&expected);
        assert!(filter.might_contain(2, 2));
    }
}


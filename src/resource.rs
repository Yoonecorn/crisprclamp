use super::*;

mod bounded_dual;
pub(super) use bounded_dual::BoundedDualPlan;

const OUTPUT_RESERVE: u64 = 65_536;
const FIXED_BASE: u64 = 1_048_576;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub max_record_bytes: usize,
    pub max_header_bytes: usize,
    pub memory_budget_bytes: u64,
    pub output_budget_bytes: u64,
}

#[derive(Clone, Copy)]
pub(super) struct Dimensions {
    pub parallel_extra: u64,
    pub guide_count: usize,
    pub construct_count: usize,
    pub matrix_rows: usize,
    pub reference_bytes: u64,
    pub max_window_bytes: usize,
    pub input_count: usize,
    pub discovery_leaves: usize,
    pub k: usize,
    pub evidence_capacity: usize,
    pub evidence_enabled: bool,
    pub evidence_identity_bytes: usize,
}

// Dual-only dimensions leave the single-guide admission formula unchanged.
#[derive(Clone, Copy)]
pub(super) struct SparseDimensions {
    pub input_records_bound: u64,
    pub worker_count: usize,
    pub id_bytes: u64,
    pub max_encoded_row: u64,
    pub unexpected_pairs_bound: u64,
    pub retained_reference_bytes: u64,
    pub audited: bool,
}

fn table_capacity(entries: u64) -> Result<u64> {
    if entries == 0 {
        return Ok(0);
    }
    // Round up before choosing buckets; floor(L * 8 / 7) can undercount.
    add(mul(entries, 8)?, 6)?
        .checked_div(7)
        .and_then(u64::checked_next_power_of_two)
        .ok_or("resource.dimension_overflow".into())
}

fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or("resource.dimension_overflow".into())
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or("resource.dimension_overflow".into())
}
fn size(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| "resource.dimension_overflow".into())
}

impl Limits {
    pub(super) fn as_json(&self) -> Value {
        json!({"max_record_bytes":self.max_record_bytes,"max_header_bytes":self.max_header_bytes,
            "memory_budget_bytes":self.memory_budget_bytes,"output_budget_bytes":self.output_budget_bytes})
    }
    pub(super) fn parse(values: &HashMap<String, String>) -> Result<Self> {
        let value = |key: &str, default: u64| -> Result<u64> {
            values.get(key).map_or(Ok(default), |s| {
                s.parse::<u64>()
                    .map_err(|_| format!("preflight.invalid_limit: {key}"))
            })
        };
        let max_record = value("--max-record-bytes", 4_194_304)?;
        let max_header = value("--max-header-bytes", 4_096)?;
        let memory = value("--memory-budget-bytes", 536_870_912)?;
        let output = value("--output-budget-bytes", 1_073_741_824)?;
        let line_cap = MAX_LINE as u64;
        if !(4..=4 * line_cap + 8).contains(&max_record)
            || !(2..=line_cap).contains(&max_header)
            || max_header > max_record
        {
            return Err("preflight.invalid_record_limits".into());
        }
        if memory < FIXED_BASE || output < OUTPUT_RESERVE + 4_096 {
            return Err("preflight.invalid_budget".into());
        }
        Ok(Self {
            max_record_bytes: usize::try_from(max_record)
                .map_err(|_| "resource.dimension_overflow")?,
            max_header_bytes: usize::try_from(max_header)
                .map_err(|_| "resource.dimension_overflow")?,
            memory_budget_bytes: memory,
            output_budget_bytes: output,
        })
    }

    pub(super) fn preflight_reference_files(&self, paths: &[&Path]) -> Result<()> {
        self.preflight_reference_files_scaled(paths, 24)
    }

    pub(super) fn preflight_dual_reference_files(&self, paths: &[&Path]) -> Result<()> {
        self.preflight_reference_files_scaled(paths, 2)
    }

    fn preflight_reference_files_scaled(&self, paths: &[&Path], multiplier: u64) -> Result<()> {
        let mut bytes = 0_u64;
        for path in paths {
            bytes = add(
                bytes,
                fs::metadata(path)
                    .map_err(|e| format!("preflight.reference_open: {e}"))?
                    .len(),
            )?;
        }
        if mul(bytes, multiplier)? > self.memory_budget_bytes {
            return Err("resource.memory_budget".into());
        }
        Ok(())
    }

    pub(super) fn plan(&self, d: Dimensions) -> Result<u64> {
        self.plan_with_sparse(d, None)
    }

    pub(super) fn plan_gpu(&self, mut d: Dimensions, active: bool) -> Result<u64> {
        // Charge before admission while retaining the legacy validation order.
        d.parallel_extra = Self::gpu_charge(d.parallel_extra, active)?;
        self.plan(d)
    }

    fn gpu_charge(estimate: u64, active: bool) -> Result<u64> {
        add(estimate, if active { 10 * 1024 * 1024 } else { 0 })
    }

    pub(super) fn plan_dual(&self, d: Dimensions, sparse: SparseDimensions) -> Result<u64> {
        self.plan_with_sparse(d, Some(sparse))
    }

    fn plan_with_sparse(&self, d: Dimensions, sparse: Option<SparseDimensions>) -> Result<u64> {
        let guides = size(d.guide_count)?;
        let constructs = size(d.construct_count)?;
        let rows = size(d.matrix_rows)?;
        let inputs = size(d.input_count)?;
        let leaves = size(d.discovery_leaves)?;
        let k = size(d.k)?;
        let window = size(d.max_window_bytes)?;
        let record = size(self.max_record_bytes)?;
        let header = size(self.max_header_bytes)?;
        let reference = if let Some(s) = &sparse {
            // Match dual preflight: retained guide/whitelist storage uses the
            // compact reference multiplier; guide objects are charged separately.
            add(mul(s.retained_reference_bytes, 2)?, mul(guides, 256)?)?
        } else {
            add(
                mul(d.reference_bytes, 24)?,
                add(mul(guides, 256)?, mul(constructs, 384)?)?,
            )?
        };
        let counters = if let Some(s) = &sparse {
            let capacity = table_capacity(constructs)?;
            let membership = add(mul(capacity, 9)?, 16)?; // PairId + control
            // String payload (24B), control, and the measured ID allocations.
            let transient_ids = add(mul(capacity, 25)?, s.id_bytes)?;
            let u = add(
                s.input_records_bound.min(constructs),
                s.unexpected_pairs_bound,
            )?;
            let map = mul(u, 32)?; // 16-byte payload, HASH_SLACK = 2
            add(
                add(membership, transient_ids)?,
                add(mul(add(size(s.worker_count)?, 1)?, map)?, 16_384)?,
            )?
        } else {
            add(
                mul(add(guides, constructs)?, 32)?,
                add(mul(rows, 64)?, 16_384)?,
            )?
        };
        let input = mul(
            inputs,
            add(add(mul(record, 5)?, mul(header, 2)?)?, 262_144)?,
        )?;
        let key = add(256, add(mul(window, 16)?, 256)?)?;
        let entry = add(mul(key, 4)?, 1_024)?;
        let discovery = mul(mul(leaves, k)?, entry)?;
        let serialization = if let Some(s) = &sparse {
            // One encoded row, 8KiB BufWriter, existing 1MiB bounded-pass reserve.
            // Audited sizing/hashing passes get a calibrated additional 5MiB.
            add(
                add(s.max_encoded_row, 8_192)?,
                add(
                    add(mul(discovery, 2)?, mul(record, 2)?)?,
                    if s.audited { 6_291_456 } else { 1_048_576 },
                )?,
            )?
        } else {
            add(
                add(mul(d.reference_bytes, 12)?, mul(rows, 1_024)?)?,
                add(mul(discovery, 2)?, add(mul(record, 2)?, 1_048_576)?)?,
            )?
        };
        // Full mode has one formatted event in flight. Sampled mode retains E
        // events; twice E covers Vec growth and two covers the candidate and
        // JSON serialization buffer.
        let event = add(
            add(mul(record, 8)?, mul(d.reference_bytes, 4)?)?,
            add(size(d.evidence_identity_bytes)?, 4_096)?,
        )?;
        let evidence = if d.evidence_enabled {
            mul(add(mul(size(d.evidence_capacity)?, 2)?, 2)?, event)?
        } else {
            0
        };
        let peak = add(
            FIXED_BASE,
            add(
                reference,
                add(
                    counters,
                    add(input, add(discovery, add(serialization, evidence)?)?)?,
                )?,
            )?,
        )?;
        let estimate = add(peak, d.parallel_extra)?;
        let estimate = if sparse.is_some() {
            add(mul(estimate, 7)?, 3)? / 4 // ceil(1.75 * complete dual estimate)
        } else {
            estimate
        };
        if estimate > self.memory_budget_bytes {
            return Err("resource.memory_budget".into());
        }
        let minimum_output = add(4_096, mul(rows, 48)?)?;
        if minimum_output > self.output_budget_bytes - OUTPUT_RESERVE {
            return Err("resource.output_budget".into());
        }
        Ok(estimate)
    }

    pub(super) fn plan_workers(
        &self,
        options: WorkerConfig,
        rows: usize,
        leaves: usize,
        k: usize,
        window: usize,
    ) -> Result<u64> {
        if options.count == 1 {
            return Ok(0);
        }
        let batch_bytes = mul(
            size(options.batch_records)?,
            add(size(self.max_record_bytes)?, 128)?,
        )?;
        let queued_batches = mul(
            size(options.count)?,
            add(size(options.work_queue_capacity)?, 2)?,
        )?;
        // The serial plan already includes the reference, one input buffer, one
        // accumulator, and output serialization. Charge all parallel additions.
        let queued = mul(queued_batches, add(batch_bytes, 256)?)?;
        let entry = add(mul(size(window)?, 64)?, 2_048)?;
        let state = add(
            mul(size(rows)?, 64)?,
            add(mul(mul(size(leaves)?, size(k)?)?, entry)?, 2_162_688)?,
        )?;
        let states = mul(size(options.count)?, state)?;
        let results = mul(size(options.result_queue_capacity)?, 256)?;
        add(queued, add(states, results)?)
    }
}

pub(super) struct OutputBudget {
    limit: u64,
    used: u64,
}
impl OutputBudget {
    pub(super) fn new(limits: Limits) -> Self {
        Self {
            limit: limits.output_budget_bytes,
            used: 0,
        }
    }
    pub(super) fn charge(&mut self, bytes: u64, science: bool) -> Result<()> {
        let next = self
            .used
            .checked_add(bytes)
            .ok_or("resource.output_budget")?;
        let ceiling = if science {
            self.limit - OUTPUT_RESERVE
        } else {
            self.limit
        };
        if next > ceiling {
            return Err("resource.output_budget".into());
        }
        self.used = next;
        Ok(())
    }
    pub(super) fn used(&self) -> u64 {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn representative(workers: usize, audited: bool, budget: u64) -> Result<u64> {
        let limits = Limits {
            max_record_bytes: 1024,
            max_header_bytes: 256,
            memory_budget_bytes: budget,
            output_budget_bytes: 2_147_483_648,
        };
        let options = WorkerConfig {
            count: workers,
            batch_records: 1,
            work_queue_capacity: 2,
            result_queue_capacity: 1,
        };
        limits.plan_dual(
            Dimensions {
                parallel_extra: limits.plan_workers(options, 0, 5, 100, 19)?,
                guide_count: 2044,
                construct_count: 1_044_484,
                matrix_rows: 1_044_484,
                reference_bytes: 106_550_104,
                max_window_bytes: 19,
                input_count: 2,
                discovery_leaves: 5,
                k: 100,
                evidence_capacity: 0,
                evidence_enabled: false,
                evidence_identity_bytes: 0,
            },
            SparseDimensions {
                input_records_bound: 4_000_000,
                worker_count: workers,
                id_bytes: 52_706_584,
                max_encoded_row: 5_068,
                unexpected_pairs_bound: 0,
                retained_reference_bytes: 92_452,
                audited,
            },
        )
    }

    #[test]
    fn representative_sparse_admission_and_exact_budget_boundaries() {
        for (workers, audited) in [(1, false), (1, true), (4, false)] {
            let estimate = representative(workers, audited, u64::MAX).unwrap();
            assert_eq!(
                representative(workers, audited, estimate).unwrap(),
                estimate
            );
            assert_eq!(
                representative(workers, audited, estimate + 1).unwrap(),
                estimate
            );
            assert_eq!(
                representative(workers, audited, estimate - 1).unwrap_err(),
                "resource.memory_budget"
            );
            if workers == 1 {
                assert!(representative(workers, audited, 536_870_912).is_ok());
            }
            println!(
                "BOUNDARY workers={workers} audited={audited} estimate={estimate} default_admit={}",
                representative(workers, audited, 536_870_912).is_ok()
            );
        }
    }

    #[test]
    fn whitelist_sized_sparse_reference_admits_at_four_gib() {
        let plan = |budget| {
            Limits {
                max_record_bytes: 4_194_304,
                max_header_bytes: 4_096,
                memory_budget_bytes: budget,
                output_budget_bytes: 1_073_741_824,
            }
            .plan_dual(
                Dimensions {
                    parallel_extra: 0,
                    guide_count: 2044,
                    construct_count: 1_044_484,
                    matrix_rows: 1_044_484,
                    reference_bytes: 270_550_104,
                    max_window_bytes: 19,
                    input_count: 3,
                    discovery_leaves: 5,
                    k: 100,
                    evidence_capacity: 0,
                    evidence_enabled: false,
                    evidence_identity_bytes: 0,
                },
                SparseDimensions {
                    input_records_bound: 41_881_752,
                    worker_count: 1,
                    id_bytes: 52_706_584,
                    max_encoded_row: 5_068,
                    unexpected_pairs_bound: 0,
                    retained_reference_bytes: 164_092_452,
                    audited: false,
                },
            )
        };
        let estimate = plan(4_294_967_296).unwrap();
        // The former x24 charge, including the existing 1.75 safety factor,
        // would reject this whitelist-sized reference at the same budget.
        assert!(estimate + 164_092_452 * 22 * 7 / 4 > 4_294_967_296);
        assert_eq!(plan(estimate).unwrap(), estimate);
        assert_eq!(plan(estimate - 1).unwrap_err(), "resource.memory_budget");
    }

    #[test]
    fn sparse_capacity_rounding_and_overflow() {
        assert_eq!(table_capacity(0).unwrap(), 0);
        assert_eq!(table_capacity(7).unwrap(), 8);
        assert_eq!(table_capacity(8).unwrap(), 16);
        assert_eq!(table_capacity(1_044_484).unwrap(), 2_097_152);
        assert_eq!(table_capacity(50_000_000).unwrap(), 67_108_864);
        assert!(table_capacity(u64::MAX).is_err());
        assert!(representative(4, false, 1).is_err());
    }

    #[test]
    fn dual_reference_multiplier_and_single_reference_boundary() {
        let limits = Limits {
            max_record_bytes: 1024,
            max_header_bytes: 256,
            memory_budget_bytes: 536_870_912,
            output_budget_bytes: 1_073_741_824,
        };
        // Sparse files avoid creating a 106MB test fixture on disk.
        let path = env::temp_dir().join(format!("project-c-reference-{}.tsv", std::process::id()));
        let file = File::create(&path).unwrap();
        file.set_len(106_550_104).unwrap();
        assert!(limits.preflight_dual_reference_files(&[&path]).is_ok());
        assert_eq!(
            limits.preflight_reference_files(&[&path]).unwrap_err(),
            "resource.memory_budget"
        );
        let dual_boundary = Limits {
            memory_budget_bytes: 213_100_208,
            ..limits
        };
        assert!(
            dual_boundary
                .preflight_dual_reference_files(&[&path])
                .is_ok()
        );
        assert!(
            Limits {
                memory_budget_bytes: 213_100_207,
                ..limits
            }
            .preflight_dual_reference_files(&[&path])
            .is_err()
        );
        let single_boundary = Limits {
            memory_budget_bytes: 2_557_202_496,
            ..limits
        };
        assert!(single_boundary.preflight_reference_files(&[&path]).is_ok());
        assert!(
            Limits {
                memory_budget_bytes: 2_557_202_495,
                ..limits
            }
            .preflight_reference_files(&[&path])
            .is_err()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn single_guide_estimate_is_unchanged() {
        let limits = Limits {
            max_record_bytes: 1024,
            max_header_bytes: 256,
            memory_budget_bytes: u64::MAX,
            output_budget_bytes: 1_073_741_824,
        };
        let estimate = limits
            .plan(Dimensions {
                parallel_extra: 1234,
                guide_count: 7,
                construct_count: 0,
                matrix_rows: 7,
                reference_bytes: 123,
                max_window_bytes: 19,
                input_count: 1,
                discovery_leaves: 5,
                k: 100,
                evidence_capacity: 0,
                evidence_enabled: false,
                evidence_identity_bytes: 0,
            })
            .unwrap();
        // Frozen arithmetic of accepted RC3 for this single-guide shape.
        assert_eq!(estimate, 8_830_654);
    }

    #[test]
    fn gpu_overhead_is_exact_checked_and_admitted() {
        assert_eq!(Limits::gpu_charge(123, false).unwrap(), 123);
        assert_eq!(
            Limits::gpu_charge(123, true).unwrap(),
            123 + 10 * 1024 * 1024
        );
        assert!(Limits::gpu_charge(u64::MAX, true).is_err());
        let d = || Dimensions {
            parallel_extra: 1234,
            guide_count: 7,
            construct_count: 0,
            matrix_rows: 7,
            reference_bytes: 123,
            max_window_bytes: 19,
            input_count: 1,
            discovery_leaves: 5,
            k: 100,
            evidence_capacity: 0,
            evidence_enabled: false,
            evidence_identity_bytes: 0,
        };
        let limits = Limits {
            max_record_bytes: 1024,
            max_header_bytes: 256,
            memory_budget_bytes: u64::MAX,
            output_budget_bytes: 1_073_741_824,
        };
        let cpu = limits.plan_gpu(d(), false).unwrap();
        let gpu = limits.plan_gpu(d(), true).unwrap();
        assert_eq!(gpu - cpu, 10 * 1024 * 1024);
        assert_eq!(
            Limits {
                memory_budget_bytes: gpu,
                ..limits
            }
            .plan_gpu(d(), true)
            .unwrap(),
            gpu
        );
        assert_eq!(
            Limits {
                memory_budget_bytes: gpu - 1,
                ..limits
            }
            .plan_gpu(d(), true)
            .unwrap_err(),
            "resource.memory_budget"
        );
    }

    #[test]
    fn checked_dimension_and_output_budget() {
        assert!(mul(u64::MAX, 2).is_err());
        let limits = Limits {
            max_record_bytes: 1024,
            max_header_bytes: 100,
            memory_budget_bytes: 1_000_000_000,
            output_budget_bytes: 70_000,
        };
        let mut output = OutputBudget::new(limits);
        assert!(output.charge(4_000, true).is_ok());
        assert!(output.charge(1_000, true).is_err());
        assert!(output.charge(1_000, false).is_ok());
    }
}

use super::*;

#[derive(Debug)]
pub(crate) struct BoundedDualPlan {
    pub(crate) estimate_bytes: u64,
    pub(crate) recycle_record_capacity: usize,
    pub(crate) input_batch_records: usize,
    pub(crate) effective_workers: WorkerConfig,
    pub(crate) components: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn campaign(
        budget: u64,
        workers: usize,
        audited: bool,
        records: u64,
    ) -> Result<BoundedDualPlan> {
        let limits = Limits {
            max_record_bytes: 4096,
            max_header_bytes: 1024,
            memory_budget_bytes: budget,
            output_budget_bytes: 2_147_483_648,
        };
        let options = WorkerConfig {
            count: workers,
            batch_records: 512,
            work_queue_capacity: 2,
            result_queue_capacity: 1,
        };
        let d = Dimensions {
            parallel_extra: add(limits.plan_workers(options, 0, 5, 100, 20)?, 2_097_152)?,
            guide_count: 152_882,
            construct_count: 1_000_000,
            matrix_rows: 1_000_000,
            reference_bytes: 103_024_367,
            max_window_bytes: 20,
            input_count: 2,
            discovery_leaves: 5,
            k: 100,
            evidence_capacity: 0,
            evidence_enabled: false,
            evidence_identity_bytes: 0,
        };
        let s = SparseDimensions {
            input_records_bound: records,
            worker_count: workers,
            id_bytes: 27_000_000,
            max_encoded_row: 4786,
            unexpected_pairs_bound: 5_842_226_481,
            retained_reference_bytes: 8_310_210,
            audited,
        };
        limits.plan_bounded_dual(d, s, options)
    }

    #[test]
    fn all_campaign_depths_have_one_reference_bounded_envelope() {
        for audited in [false, true] {
            let first = campaign(44 << 30, 4, audited, 57_750_001).unwrap();
            assert_eq!(first.recycle_record_capacity, 4096);
            assert_eq!(first.effective_workers.batch_records, 512);
            assert_eq!(first.effective_workers.work_queue_capacity, 2);
            assert_eq!(
                first.estimate_bytes,
                if audited { 1_640_593_322 } else { 1_631_418_282 }
            );
            assert!(first.estimate_bytes < 44 << 30);
            for n in [
                1_000_000,
                2_000_000,
                5_000_000,
                10_000_000,
                100_000_000,
                500_000_000,
                1_000_000_000,
            ] {
                let p = campaign(44 << 30, 4, audited, 231 * n / 4 + 1).unwrap();
                assert_eq!(p.estimate_bytes, first.estimate_bytes);
                assert_eq!(p.components["expected_key_cap"], 1_000_000);
                assert_eq!(p.components["count_map_concurrency"], 5);
            }
            assert_eq!(
                campaign(44 << 30, 4, audited, u64::MAX)
                    .unwrap()
                    .estimate_bytes,
                first.estimate_bytes
            );
        }
    }

    #[test]
    fn minimum_queue_admission_is_exact_to_one_byte() {
        for audited in [false, true] {
            for workers in [1, 2, 4] {
                let p = campaign(44 << 30, workers, audited, u64::MAX).unwrap();
                let v = &p.components;
                let phase = &v["phase_bytes_before_margin"];
                let loading = phase["loading"].as_u64().unwrap();
                let counting = phase["counting"].as_u64().unwrap()
                    - v["counting"]["input_pipeline_bytes"].as_u64().unwrap()
                    + input_pipeline_bytes(2, 4, 1, 1, v["validated_record_bytes"].as_u64().unwrap()).unwrap();
                let output = phase["output"].as_u64().unwrap()
                    - v["output"]["input_pipeline_residual"].as_u64().unwrap()
                    + input_pipeline_bytes(2, 4, 1, 1, v["validated_record_bytes"].as_u64().unwrap()).unwrap();
                let minimum = margin(loading.max(counting).max(output)).unwrap();
                assert_eq!(
                    campaign(minimum - 1, workers, audited, u64::MAX).unwrap_err(),
                    "resource.memory_budget"
                );
                assert_eq!(
                    campaign(minimum, workers, audited, u64::MAX)
                        .unwrap()
                        .estimate_bytes,
                    minimum
                );
                assert!(campaign(minimum + 1, workers, audited, u64::MAX).is_ok());
            }
        }
    }

    #[test]
    fn phases_profiles_and_workers_are_charged_explicitly() {
        for workers in [1, 2, 4] {
            let std = campaign(44 << 30, workers, false, u64::MAX).unwrap();
            let audit = campaign(44 << 30, workers, true, u64::MAX).unwrap();
            assert_eq!(
                std.components["count_map_concurrency"],
                if workers == 1 { 1 } else { workers + 1 }
            );
            assert_eq!(std.components["output"]["audited_library"], 0);
            assert!(
                audit.components["output"]["audited_library"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert!(audit.estimate_bytes >= std.estimate_bytes);
            let phases = std.components["phase_bytes_before_margin"]
                .as_object()
                .unwrap();
            let values = phases
                .values()
                .map(|v| v.as_u64().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                std.estimate_bytes,
                margin(*values.iter().max().unwrap()).unwrap()
            );
            assert!(std.estimate_bytes < margin(values.iter().sum()).unwrap());
            assert_eq!(
                std.components["counting"]["worker_batches"]
                    .as_u64()
                    .unwrap()
                    == 0,
                workers == 1
            );
            let sum = |value: &Value| {
                value
                    .as_object()
                    .unwrap()
                    .values()
                    .map(|v| v.as_u64().unwrap())
                    .sum::<u64>()
            };
            let common = sum(&std.components["common"]);
            assert_eq!(
                common + sum(&std.components["loading"]),
                std.components["phase_bytes_before_margin"]["loading"]
                    .as_u64()
                    .unwrap()
            );
            assert_eq!(
                common + sum(&std.components["counting"]),
                std.components["phase_bytes_before_margin"]["counting"]
                    .as_u64()
                    .unwrap()
            );
            let charged_output = std.components["output"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| !matches!(k.as_str(), "guide_metadata_rows" | "audited_library"))
                .map(|(_, v)| v.as_u64().unwrap())
                .sum::<u64>();
            assert_eq!(
                common + charged_output,
                std.components["phase_bytes_before_margin"]["output"]
                    .as_u64()
                    .unwrap()
            );
        }
    }

    #[test]
    fn batch_capacity_and_record_capacity_are_distinct_checked_units() {
        for inputs in [1,2,3] {
            for forward in [1,2,4] {
                for batch in [1,7,512] {
                    for recycle in [1,2,4096] {
                        let record = 10_400;
                        let expected = inputs * (((forward + 2) * batch + recycle) * record
                            + (forward + 2 + recycle) * 256);
                        assert_eq!(input_pipeline_bytes(inputs,forward,batch,recycle,record).unwrap(),expected);
                    }
                }
            }
        }
        for (n,q,b,r,d) in [(0,4,512,1,10), (2,0,512,1,10), (2,4,0,1,10),
            (2,4,512,0,10), (2,4,512,1,0), (u64::MAX,4,512,1,10),
            (2,u64::MAX,512,1,10), (2,4,u64::MAX,1,10), (2,4,512,u64::MAX,10), (2,4,512,1,u64::MAX)] {
            assert_eq!(input_pipeline_bytes(n,q,b,r,d).unwrap_err(),"resource.dimension_overflow");
        }
        let full = campaign(2 << 30,4,false,u64::MAX).unwrap();
        assert_eq!(full.input_batch_records,512);
        assert_eq!(full.recycle_record_capacity,4096);
        assert_eq!(full.components["forward_channel_capacity_batches"],4);
        assert_eq!(full.components["queued_forward_records"],2048);
        assert_eq!(full.components["producer_owned_batch_records"],512);
        assert_eq!(full.components["consumer_pending_storage_records"],512);
        assert_eq!(full.components["counting"]["input_pipeline_bytes"],full.components["output"]["input_pipeline_residual"]);
    }

    #[test]
    fn physical_batch_is_largest_admissible_and_minimum_default_budget_is_exact() {
        let limits = Limits { max_record_bytes:4_194_304,max_header_bytes:4096,
            memory_budget_bytes:536_870_912,output_budget_bytes:1_073_741_824 };
        let w = WorkerConfig { count:1,batch_records:1,work_queue_capacity:1,result_queue_capacity:1 };
        let d = Dimensions { parallel_extra:256,guide_count:4,construct_count:4,matrix_rows:4,
            reference_bytes:100,max_window_bytes:4,input_count:2,discovery_leaves:5,k:100,
            evidence_capacity:0,evidence_enabled:false,evidence_identity_bytes:0 };
        let s = SparseDimensions { input_records_bound:u64::MAX,worker_count:1,id_bytes:8,
            max_encoded_row:4220,unexpected_pairs_bound:u64::MAX,retained_reference_bytes:100,audited:false };
        let p = limits.plan_bounded_dual(d,s,w).unwrap();
        assert!((1..512).contains(&p.input_batch_records));
        let v = &p.components;
        let field = v["validated_record_bytes"].as_u64().unwrap();
        let old = v["counting"]["input_pipeline_bytes"].as_u64().unwrap();
        let counting = v["phase_bytes_before_margin"]["counting"].as_u64().unwrap() - old;
        let output = v["phase_bytes_before_margin"]["output"].as_u64().unwrap() - old;
        let loading = v["phase_bytes_before_margin"]["loading"].as_u64().unwrap();
        let next = input_pipeline_bytes(2,4,p.input_batch_records as u64+1,1,field).unwrap();
        assert!(margin(loading.max(counting+next).max(output+next)).unwrap() > limits.memory_budget_bytes);
        let minimum_pipeline = input_pipeline_bytes(2,4,1,1,field).unwrap();
        let minimum = margin(loading.max(counting+minimum_pipeline).max(output+minimum_pipeline)).unwrap();
        let mut low = limits; low.memory_budget_bytes = minimum-1;
        assert_eq!(low.plan_bounded_dual(d,s,w).unwrap_err(),"resource.memory_budget");
        low.memory_budget_bytes = minimum;
        let admitted = low.plan_bounded_dual(d,s,w).unwrap();
        assert_eq!(admitted.estimate_bytes,minimum);
        assert_eq!(admitted.input_batch_records,1);
        assert_eq!(admitted.recycle_record_capacity,1);
    }

    #[test]
    fn rounded_tables_and_growth_have_checked_bounds() {
        assert_eq!(table_bytes(0, 16, true).unwrap(), 0);
        assert!(table_bytes(1, 16, false).unwrap() >= 76);
        assert!(table_bytes(4, 16, true).unwrap() >= 220);
        assert!(table_bytes(917_505, 16, true).unwrap() >= 53_477_392);
        assert!(table_bytes(u64::MAX, 16, true).is_err());
        assert!(table_bytes(1, u64::MAX, true).is_err());
        assert!(margin(u64::MAX).is_err());
    }

    #[test]
    fn default_serial_decoder_is_bounded_by_admitted_budget() {
        let limits = Limits {
            max_record_bytes: 4_194_304,
            max_header_bytes: 4096,
            memory_budget_bytes: 536_870_912,
            output_budget_bytes: 1_073_741_824,
        };
        let w = WorkerConfig {
            count: 1,
            batch_records: 1,
            work_queue_capacity: 2,
            result_queue_capacity: 1,
        };
        let d = Dimensions {
            parallel_extra: 256,
            guide_count: 4,
            construct_count: 4,
            matrix_rows: 4,
            reference_bytes: 100,
            max_window_bytes: 4,
            input_count: 2,
            discovery_leaves: 5,
            k: 100,
            evidence_capacity: 0,
            evidence_enabled: false,
            evidence_identity_bytes: 0,
        };
        let s = SparseDimensions {
            input_records_bound: u64::MAX,
            worker_count: 1,
            id_bytes: 8,
            max_encoded_row: 4220,
            unexpected_pairs_bound: u64::MAX,
            retained_reference_bytes: 100,
            audited: false,
        };
        let p = limits.plan_bounded_dual(d, s, w).unwrap();
        assert!(p.recycle_record_capacity < 4096 || p.input_batch_records < 512);
        assert!(p.estimate_bytes <= limits.memory_budget_bytes);
        let mut overflow = d;
        overflow.guide_count = usize::MAX;
        assert!(limits.plan_bounded_dual(overflow, s, w).is_err());
        let mut overflow = s;
        overflow.retained_reference_bytes = u64::MAX;
        assert!(limits.plan_bounded_dual(d, overflow, w).is_err());
    }

    #[test]
    fn low_budget_parallel_batches_are_bounded_and_output_edges_checked() {
        for count in [1, 2, 4] {
            let mut limits = Limits {
                max_record_bytes: 4_194_304,
                max_header_bytes: 4096,
                memory_budget_bytes: 536_870_912,
                output_budget_bytes: 1_073_741_824,
            };
            let w = WorkerConfig {
                count,
                batch_records: 4,
                work_queue_capacity: 2,
                result_queue_capacity: 1,
            };
            let d = Dimensions {
                parallel_extra: add(limits.plan_workers(w, 0, 5, 100, 4).unwrap(), 256).unwrap(),
                guide_count: 4,
                construct_count: 4,
                matrix_rows: 4,
                reference_bytes: 100,
                max_window_bytes: 4,
                input_count: 2,
                discovery_leaves: 5,
                k: 100,
                evidence_capacity: 0,
                evidence_enabled: false,
                evidence_identity_bytes: 0,
            };
            let s = SparseDimensions {
                input_records_bound: u64::MAX,
                worker_count: count,
                id_bytes: 8,
                max_encoded_row: 4220,
                unexpected_pairs_bound: u64::MAX,
                retained_reference_bytes: 100,
                audited: false,
            };
            let p = limits.plan_bounded_dual(d, s, w).unwrap();
            assert!(p.effective_workers.batch_records <= w.batch_records);
            assert!(p.effective_workers.work_queue_capacity <= w.work_queue_capacity);
            assert_eq!(p.effective_workers.count, count);
            assert!(p.input_batch_records < 512);
            let minimum = OUTPUT_RESERVE + 4096 + 4 * 48;
            limits.output_budget_bytes = minimum - 1;
            assert_eq!(
                limits.plan_bounded_dual(d, s, w).unwrap_err(),
                "resource.output_budget"
            );
            limits.output_budget_bytes = minimum;
            assert!(limits.plan_bounded_dual(d, s, w).is_ok());
            limits.output_budget_bytes = minimum + 1;
            assert!(limits.plan_bounded_dual(d, s, w).is_ok());
            limits.output_budget_bytes = 0;
            assert_eq!(
                limits.plan_bounded_dual(d, s, w).unwrap_err(),
                "resource.output_budget"
            );
            let mut bad = w;
            bad.batch_records = 0;
            assert_eq!(
                limits.plan_bounded_dual(d, s, bad).unwrap_err(),
                "resource.dimension_overflow"
            );
        }
    }

    #[test]
    fn dual_evidence_charges_windows_and_identities_without_retaining_records() {
        let plan = |record, capacity, identity| {
            let limits = Limits {
                max_record_bytes: record,
                max_header_bytes: 512,
                memory_budget_bytes: 536_870_912,
                output_budget_bytes: 1_073_741_824,
            };
            let w = WorkerConfig {
                count: 1,
                batch_records: 1,
                work_queue_capacity: 2,
                result_queue_capacity: 1,
            };
            let d = Dimensions {
                parallel_extra: 256,
                guide_count: 4,
                construct_count: 4,
                matrix_rows: 4,
                reference_bytes: 100,
                max_window_bytes: 4,
                input_count: 2,
                discovery_leaves: 5,
                k: 2,
                evidence_capacity: capacity,
                evidence_enabled: true,
                evidence_identity_bytes: identity,
            };
            let s = SparseDimensions {
                input_records_bound: u64::MAX,
                worker_count: 1,
                id_bytes: 8,
                max_encoded_row: 4200,
                unexpected_pairs_bound: u64::MAX,
                retained_reference_bytes: 100,
                audited: true,
            };
            limits.plan_bounded_dual(d, s, w)
        };
        for record in [4096, 4_194_304] {
            for capacity in [0, 1, 2] {
                let p = plan(record, capacity, 7).unwrap();
                assert_eq!(
                    p.components["counting"]["evidence"],
                    (2 * capacity + 2) * 99_616
                );
                assert!(p.estimate_bytes <= 536_870_912);
            }
        }
        assert!(plan(4096, 2, usize::MAX).is_err());
        assert!(plan(4096, usize::MAX, 7).is_err());
    }
}

// Bounds the pinned 64-bit SwissTable allocation, including a control-group and
// allocator-alignment reserve. Empty maps allocate nothing; small tables use at
// least four buckets. Growth keeps the old half-sized table alive temporarily.
fn table_bytes(entries: u64, payload: u64, growing: bool) -> Result<u64> {
    if entries == 0 {
        return Ok(0);
    }
    let buckets = table_capacity(entries)?.max(4);
    let allocation = |b| add(mul(b, add(payload, 1)?)?, 256);
    let current = allocation(buckets)?;
    add(
        current,
        if growing && buckets > 4 {
            allocation(buckets / 2)?
        } else {
            0
        },
    )
}

fn margin(bytes: u64) -> Result<u64> {
    Ok(add(mul(bytes, 7)?, 3)? / 4)
}

// Forward capacity is in batches; recycle capacity is in individual records.
// Charge full-capacity producer and pending batches, even when partial/consumed.
// valid_record includes each FastqRecord's Vec headers and retained field capacity.
fn input_pipeline_bytes(inputs: u64, forward: u64, batch: u64, recycle: u64, record: u64) -> Result<u64> {
    if inputs == 0 || forward == 0 || batch == 0 || recycle == 0 || record == 0 {
        return Err("resource.dimension_overflow".into());
    }
    let batch_slots = add(forward, 2)?;
    let records = add(mul(batch_slots, batch)?, recycle)?;
    // Vec/message/channel metadata in addition to the charged record slots.
    mul(inputs, add(mul(records, record)?, mul(add(batch_slots, recycle)?, 256)?)?)
}

impl Limits {
    // Ordinary dual only. Concordance keeps its separately qualified legacy
    // admission path until its whitelist/oracle allocations are independently
    // included in this model. Single-guide planning is unchanged.
    pub(crate) fn plan_bounded_dual(
        &self,
        d: Dimensions,
        s: SparseDimensions,
        workers: WorkerConfig,
    ) -> Result<BoundedDualPlan> {
        self.plan_bounded_dual_formats(d, s, workers, false, false)
    }

    pub(crate) fn plan_bounded_dual_formats(
        &self, d: Dimensions, s: SparseDimensions, workers: WorkerConfig,
        compact_reference: bool, compact_output: bool,
    ) -> Result<BoundedDualPlan> {
        if workers.count == 0
            || workers.count != s.worker_count
            || d.input_count == 0
            || workers.batch_records == 0
            || workers.work_queue_capacity == 0
            || workers.result_queue_capacity == 0
        {
            return Err("resource.dimension_overflow".into());
        }
        let record = size(self.max_record_bytes)?;
        let header = size(self.max_header_bytes)?;
        let inputs = size(d.input_count)?;
        let guides = size(d.guide_count)?;
        let constructs = size(d.construct_count)?;
        let count_cap = constructs.min(s.input_records_bound);
        let worker_count = size(workers.count)?;
        let maps = if workers.count == 1 {
            1
        } else {
            add(worker_count, 1)?
        };
        // Framing caps each field at MAX_LINE+2 including CRLF, even when
        // the aggregate record limit is larger. Valid sequence and quality
        // lengths are equal, hence each is also <= record/2. Retained capacities
        // may come from different records; charge their independent maxima.
        let field_limit = add(size(MAX_LINE)?, 2)?;
        let valid_record = add(add(mul((record / 2).min(field_limit), 4)?, mul(header, 2)?)?, 160)?;
        // Only the producer owns an unvalidated candidate. Queued/recycled
        // records passed the aggregate record limit and equal-quality check.
        let parser = add(add(mul(record.min(field_limit), 9)?, mul(header, 3)?)?, 512)?;
        let reference = add(mul(s.retained_reference_bytes, 2)?, mul(guides, 256)?)?;
        let old_workers =
            self.plan_workers(workers, 0, d.discovery_leaves, d.k, d.max_window_bytes)?;
        let bloom = d
            .parallel_extra
            .checked_sub(old_workers)
            .ok_or("resource.dimension_overflow")?;
        let compact_scratch = if compact_reference || compact_output { 262_144 } else { 0 };
        let common = add(add(add(add(FIXED_BASE, reference)?, bloom)?, 16_384)?, compact_scratch)?;
        let membership = table_bytes(constructs, 8, false)?;
        let loading_membership = table_bytes(constructs, 8, true)?;
        let loading_ids = if compact_reference { 0 } else { table_bytes(constructs, 24, true)? };
        let id_payload = if compact_reference { 0 } else { add(mul(s.id_bytes, 2)?, mul(constructs, 32)?)? };
        let reference_scratch = add(mul(size(MAX_LINE)?, 3)?, 4096)?;
        let loading = add(
            common,
            add(
                loading_membership,
                add(loading_ids, add(id_payload, reference_scratch)?)?,
            )?,
        )?;

        let window = size(d.max_window_bytes)?;
        let leaves = size(d.discovery_leaves)?;
        let k = size(d.k)?;
        let discovery = mul(
            mul(leaves, k)?,
            add(mul(add(256, add(mul(window, 16)?, 256)?)?, 4)?, 1_024)?,
        )?;
        // Ordinary dual events contain two extracted windows, guide IDs, and
        // fixed category/component fields. They do not retain FASTQ headers,
        // qualities, or the complete reference. These factors cover lossy UTF-8,
        // JSON escaping, Vec growth, selector identities and tree overhead.
        let event = add(
            add(mul(window, 64)?, mul(s.max_encoded_row, 8)?)?,
            add(mul(size(d.evidence_identity_bytes)?, 32)?, 65_536)?,
        )?;
        let evidence = if d.evidence_enabled {
            mul(add(mul(size(d.evidence_capacity)?, 2)?, 2)?, event)?
        } else {
            0
        };
        let worker_batch_bytes = |batch: u64, queue: u64| -> Result<u64> {
            if workers.count == 1 {
                return Ok(0);
            }
            // One active batch per worker, bounded queued batches per worker,
            // and one producer batch shared by the entire worker pool.
            let slots = add(mul(worker_count, add(queue, 1)?)?, 1)?;
            // The tuple also owns optional absent-mate slots and an ordinal.
            // Charge these per item even for a one-stream dual invocation.
            mul(
                slots,
                add(mul(batch, add(mul(inputs, valid_record)?, 256)?)?, 256)?,
            )
        };
        let worker_states = if workers.count == 1 {
            0
        } else {
            mul(
                worker_count,
                add(
                    mul(mul(leaves, k)?, add(mul(window, 64)?, 2_048)?)?,
                    2_162_688,
                )?,
            )?
        };
        let result_queue = if workers.count == 1 {
            0
        } else {
            mul(size(workers.result_queue_capacity)?, 256)?
        };
        let count_maps = mul(maps, table_bytes(count_cap, 16, true)?)?;
        let input_parser = mul(inputs, parser)?;
        let input_readers = mul(inputs, 2_359_296)?;
        let caller_records = mul(inputs, valid_record)?;
        let scalar_bytes = mul(maps, 8)?;
        let counting_base = [
            common,
            membership,
            count_maps,
            input_parser,
            input_readers,
            caller_records,
            worker_states,
            result_queue,
            discovery,
            evidence,
            scalar_bytes,
        ]
        .into_iter()
        .try_fold(0, add)?;

        let serialization_legacy = add(
            add(mul(s.max_encoded_row, 3)?, 8_192)?,
            add(
                add(mul(discovery, 2)?, mul(record, 2)?)?,
                if s.audited { 6_291_456 } else { 1_048_576 },
            )?,
        )?;
        let compact_buffer = if compact_output { 98_304 } else { 0 };
        let serialization = add(serialization_legacy, compact_buffer)?;
        let guide_rows = if compact_output { 0 } else { add(mul(guides, 4_096)?, mul(s.retained_reference_bytes, 4)?)? };
        let audited_library = if s.audited {
            add(mul(guides, 512)?, mul(s.retained_reference_bytes, 32)?)?
        } else {
            0
        };
        // Metadata rows and audited library serialization occur sequentially.
        let output_metadata = guide_rows.max(audited_library);
        let output_reader_residual = mul(
            inputs,
            add(add(mul(record, 2)?, mul(header, 2)?)?, 262_144)?,
        )?;
        let output_base = [
            common,
            membership,
            table_bytes(count_cap, 16, false)?,
            discovery,
            serialization,
            output_metadata,
            evidence,
            output_reader_residual,
            8,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let phases = |recycle: u64, input_batch: u64, batch: u64, work_queue: u64| -> Result<(u64, u64, u64)> {
            let input_pipeline = input_pipeline_bytes(inputs, size(FASTQ_FORWARD_BATCHES)?, input_batch, recycle, valid_record)?;
            let counting = add(add(counting_base, input_pipeline)?, worker_batch_bytes(batch, work_queue)?)?;
            // Conservative across EOF/error/cancellation: charge the entire input
            // pipeline during output too, although stop_worker releases channels,
            // pending Vec and joins producers before successful publication.
            let output = add(output_base, input_pipeline)?;
            Ok((counting, output, margin(loading.max(counting).max(output))?))
        };
        if phases(1, 1, 1, 1)?.2 > self.memory_budget_bytes {
            return Err("resource.memory_budget".into());
        }
        let largest = |maximum: u64, fits: &dyn Fn(u64) -> Result<bool>| -> Result<u64> {
            let mut low = 1;
            let mut high = maximum;
            while low < high {
                let middle = low + (high - low + 1) / 2;
                if fits(middle)? {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            Ok(low)
        };
        // Preserve handoff512 whenever the budget admits it, before enlarging
        // downstream worker storage or the record recycler.
        let input_batch = largest(size(FASTQ_BATCH_RECORDS)?, &|b| {
            Ok(phases(1, b, 1, 1)?.2 <= self.memory_budget_bytes)
        })?;
        let work_queue = largest(size(workers.work_queue_capacity)?, &|q| {
            Ok(phases(1, input_batch, 1, q)?.2 <= self.memory_budget_bytes)
        })?;
        let batch = largest(size(workers.batch_records)?, &|b| {
            Ok(phases(1, input_batch, b, work_queue)?.2 <= self.memory_budget_bytes)
        })?;
        let recycle = largest(size(FASTQ_RECYCLE_RECORDS)?, &|r| {
            Ok(phases(r, input_batch, batch, work_queue)?.2 <= self.memory_budget_bytes)
        })?;
        let (counting, output, estimate) = phases(recycle, input_batch, batch, work_queue)?;
        let pipeline = input_pipeline_bytes(inputs, size(FASTQ_FORWARD_BATCHES)?, input_batch, recycle, valid_record)?;
        let worker_batches = worker_batch_bytes(batch, work_queue)?;
        let effective_workers = WorkerConfig {
            batch_records: usize::try_from(batch).map_err(|_| "resource.dimension_overflow")?,
            work_queue_capacity: usize::try_from(work_queue)
                .map_err(|_| "resource.dimension_overflow")?,
            ..workers
        };
        let minimum_output = add(4_096, mul(size(d.matrix_rows)?, if compact_output { 8 } else { 48 })?)?;
        if minimum_output
            > self
                .output_budget_bytes
                .checked_sub(OUTPUT_RESERVE)
                .ok_or("resource.output_budget")?
        {
            return Err("resource.output_budget".into());
        }
        Ok(BoundedDualPlan {
            estimate_bytes: estimate,
            recycle_record_capacity: usize::try_from(recycle)
                .map_err(|_| "resource.dimension_overflow")?,
            input_batch_records: usize::try_from(input_batch).map_err(|_| "resource.dimension_overflow")?,
            effective_workers,
            components: json!({
                "model":if compact_reference || compact_output {"cclamp-compact-dual-v1"}else{"ordinary-dual-expected-map-batched-v2"},
                "compact_reference":compact_reference,"compact_output":compact_output,
                "compact_reference_streaming_bytes":if compact_reference {98_304}else{0},
                "compact_output_buffer_bytes":compact_buffer,
                "configured_memory_budget_bytes":self.memory_budget_bytes,
                "estimate_bytes":estimate,"safety_margin":"ceil(7*peak/4)",
                "expected_key_cap":count_cap,"unexpected_storage":"checked scalar",
                "potential_unexpected_pair_bound_not_retained":s.unexpected_pairs_bound,
                "count_map_concurrency":maps,"recycle_record_capacity":recycle,
                "forward_channel_capacity_batches":FASTQ_FORWARD_BATCHES,
                "maximum_records_per_batch":FASTQ_BATCH_RECORDS,"effective_input_batch_records":input_batch,
                "producer_owned_batch_records":input_batch,"consumer_pending_storage_records":input_batch,
                "queued_forward_records":mul(size(FASTQ_FORWARD_BATCHES)?,input_batch)?,
                "recycle_capacity_unit":"records","forward_capacity_unit":"batches",
                "requested_batch_records":workers.batch_records,"effective_batch_records":batch,
                "requested_work_queue_capacity":workers.work_queue_capacity,
                "effective_work_queue_capacity":work_queue,"worker_count":workers.count,
                "validated_record_bytes":valid_record,"parser_bytes_per_input":parser,
                "evidence_event_bound_bytes":event,
                "common":{"fixed":FIXED_BASE,"retained_reference":reference,
                    "bloom":bloom,"counter_reserve":16384,"compact_identity_scratch":compact_scratch},
                "loading":{"membership_growth":loading_membership,
                    "id_table_growth":loading_ids,"id_payload":id_payload,
                    "stream_scratch":reference_scratch},
                "counting":{"membership":membership,"count_maps_growth":count_maps,
                    "input_parser":input_parser,"input_readers":input_readers,
                    "caller_records":caller_records,"worker_batches":worker_batches,
                    "worker_states":worker_states,"result_queue":result_queue,
                    "discovery":discovery,"evidence":evidence,"unexpected_scalars":scalar_bytes,
                    "input_pipeline_bytes":pipeline},
                "output":{"merged_count_map":table_bytes(count_cap,16,false)?,
                    "membership":membership,"discovery":discovery,"evidence":evidence,
                    "reader_residual":output_reader_residual,"unexpected_scalar":8,
                    "guide_metadata_rows":guide_rows,"audited_library":audited_library,
                    "metadata_phase_max":output_metadata,"serialization":serialization,
                    "input_pipeline_residual":pipeline},
                "phase_bytes_before_margin":{"loading":loading,"counting":counting,"output":output},
                "scope":"engine allocations; separate audit executable is sequential and separately measured"
            }),
        })
    }
}

#[cfg(test)]
mod prospective_tests {
    use super::*;

    fn fixture() -> (Limits, Dimensions, SparseDimensions, WorkerConfig) {
        let l = Limits { max_record_bytes: 4096, max_header_bytes: 1024,
            memory_budget_bytes: 4 << 30, output_budget_bytes: 2 << 30 };
        let w = WorkerConfig { count: 4, batch_records: 512,
            work_queue_capacity: 2, result_queue_capacity: 1 };
        let d = Dimensions { parallel_extra: l.plan_workers(w, 0, 5, 100, 20).unwrap(),
            guide_count: 152_882, construct_count: 1_000_000, matrix_rows: 1_000_000,
            reference_bytes: 103_024_367, max_window_bytes: 20, input_count: 2,
            discovery_leaves: 5, k: 100, evidence_capacity: 0, evidence_enabled: false,
            evidence_identity_bytes: 0 };
        let s = SparseDimensions { input_records_bound: 1_000_000_000, worker_count: 4,
            id_bytes: 27_000_000, max_encoded_row: 4786,
            unexpected_pairs_bound: u64::MAX, retained_reference_bytes: 8_310_210,
            audited: false };
        (l, d, s, w)
    }

    #[test]
    fn prospective_depths_are_bounded_without_allocating_input_or_pair_maps() {
        let (l, d, mut s, w) = fixture();
        let full = l.plan_bounded_dual(d, s, w).unwrap().estimate_bytes;
        for records in [0, 1, 10, 1_000_000, 10_000_000, 100_000_000,
            500_000_000, 1_000_000_000, 2_000_000_000, 1_000_000_000_000, u64::MAX] {
            s.input_records_bound = records;
            let p = l.plan_bounded_dual(d, s, w).unwrap();
            assert_eq!(p.components["expected_key_cap"], records.min(1_000_000));
            assert!(p.estimate_bytes <= full);
            if records >= 1_000_000 { assert_eq!(p.estimate_bytes, full); }
            s.unexpected_pairs_bound = 0;
            assert_eq!(l.plan_bounded_dual(d, s, w).unwrap().estimate_bytes, p.estimate_bytes);
            s.unexpected_pairs_bound = u64::MAX;
        }
    }

    #[test]
    fn budget_does_not_become_estimate_or_silently_increase() {
        let (mut l, d, s, w) = fixture();
        let mut last = 0;
        for budget in [0, 1, (1 << 20) - 1, 1 << 20, 512 << 20, 1 << 30,
            2 << 30, 4 << 30, 44 << 30, u64::MAX] {
            l.memory_budget_bytes = budget;
            match l.plan_bounded_dual(d, s, w) {
                Ok(p) => {
                    assert!(p.estimate_bytes <= budget);
                    assert!(p.estimate_bytes >= last);
                    assert_eq!(p.components["configured_memory_budget_bytes"], budget);
                    assert_eq!(p.components["estimate_bytes"], p.estimate_bytes);
                    assert!(p.components.get("measured_rss_bytes").is_none());
                    assert!((1..=4096).contains(&p.recycle_record_capacity));
                    last = p.estimate_bytes;
                }
                Err(e) => assert_eq!(e, "resource.memory_budget"),
            }
            assert_eq!(l.memory_budget_bytes, budget);
        }
    }

    #[test]
    fn physical_queues_shrink_but_worker_count_and_logical_request_survive() {
        let (mut l, mut d, s, mut w) = fixture();
        l.max_record_bytes = 65536;
        l.memory_budget_bytes = 2 << 30;
        w.batch_records = 1024;
        w.work_queue_capacity = 64;
        w.result_queue_capacity = 4;
        d.parallel_extra = l.plan_workers(w, 0, 5, 100, 20).unwrap();
        let p = l.plan_bounded_dual(d, s, w).unwrap();
        assert_eq!(p.effective_workers.count, w.count);
        assert_eq!(p.effective_workers.result_queue_capacity, w.result_queue_capacity);
        assert_eq!(p.components["requested_batch_records"], 1024);
        assert_eq!(p.components["requested_work_queue_capacity"], 64);
        assert!(p.effective_workers.batch_records < 1024 || p.effective_workers.work_queue_capacity < 64);
        assert!(p.estimate_bytes <= l.memory_budget_bytes);
    }

    #[test]
    fn evidence_and_reference_growth_must_be_admitted_or_refused() {
        let (mut l, mut d, mut s, w) = fixture();
        d.evidence_enabled = true;
        d.evidence_capacity = 100;
        d.evidence_identity_bytes = 65536;
        let small = l.plan_bounded_dual(d, s, w).unwrap();
        d.evidence_capacity = 100_000;
        assert_eq!(l.plan_bounded_dual(d, s, w).unwrap_err(), "resource.memory_budget");
        d.evidence_capacity = 100;
        l.memory_budget_bytes = small.estimate_bytes;
        assert!(l.plan_bounded_dual(d, s, w).is_ok());
        s.retained_reference_bytes = 16 << 30;
        assert_eq!(l.plan_bounded_dual(d, s, w).unwrap_err(), "resource.memory_budget");
    }

    #[test]
    fn every_external_dimension_overflows_closed() {
        let (l, d, s, w) = fixture();
        for field in 0..11 {
            let (mut a, mut b) = (d, s);
            match field {
                0 => a.guide_count = usize::MAX,
                1 => a.construct_count = usize::MAX,
                2 => a.matrix_rows = usize::MAX,
                3 => a.input_count = usize::MAX,
                4 => a.discovery_leaves = usize::MAX,
                5 => a.k = usize::MAX,
                6 => a.max_window_bytes = usize::MAX,
                7 => { a.evidence_enabled = true; a.evidence_capacity = usize::MAX; },
                8 => b.retained_reference_bytes = u64::MAX,
                9 => b.id_bytes = u64::MAX,
                _ => b.max_encoded_row = u64::MAX,
            }
            assert_eq!(l.plan_bounded_dual(a, b, w).unwrap_err(), "resource.dimension_overflow", "field {field}");
        }
        for field in 0..3 {
            let mut a = w;
            match field { 0 => a.count = 0, 1 => a.batch_records = 0, _ => a.work_queue_capacity = 0 }
            assert_eq!(l.plan_bounded_dual(d, s, a).unwrap_err(), "resource.dimension_overflow");
        }
    }

    #[test]
    fn table_envelope_covers_actual_retained_and_rehash_capacity_edges() {
        for entries in [0, 1, 3, 4, 7, 8, 14, 15, 28, 29, 56, 57, 112, 113, 896, 897, 1792, 1793] {
            let mut map = HashMap::<(u32, u32), u64>::new();
            let mut previous = 0;
            for key in 0..entries {
                let before = map.capacity();
                map.insert((key as u32, 0), key);
                if map.capacity() != before { previous = before; }
            }
            // Capacity is usable payload slots; actual control/bucket bytes are
            // larger. This lower bound independently catches under-sized tables.
            let payload_bytes = map.capacity() as u64 * 16;
            assert!(table_bytes(entries, 16, false).unwrap() >= payload_bytes);
            assert!(table_bytes(entries, 16, true).unwrap() >= payload_bytes + previous as u64 * 16);
        }
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    #[test]
    fn compact_only_removes_real_id_and_metadata_allocations_and_checks_budget() {
        let l = Limits {max_record_bytes:4096,max_header_bytes:1024,memory_budget_bytes:24<<30,output_budget_bytes:64<<30};
        let w = WorkerConfig {count:4,batch_records:512,work_queue_capacity:2,result_queue_capacity:1};
        let d = Dimensions {parallel_extra:l.plan_workers(w,0,5,100,20).unwrap()+134217728,
            guide_count:152882,construct_count:50000000,matrix_rows:50000000,reference_bytes:400000000,
            max_window_bytes:20,input_count:2,discovery_leaves:5,k:100,evidence_capacity:0,evidence_enabled:false,evidence_identity_bytes:0};
        let s = SparseDimensions {input_records_bound:u64::MAX,worker_count:4,id_bytes:1350000000,max_encoded_row:4786,
            unexpected_pairs_bound:u64::MAX,retained_reference_bytes:8310210,audited:false};
        let old=l.plan_bounded_dual(d,s,w).unwrap();
        let new=l.plan_bounded_dual_formats(d,s,w,true,true).unwrap();
        assert_eq!(new.components["loading"]["id_table_growth"],0);
        assert_eq!(new.components["loading"]["id_payload"],0);
        assert_eq!(new.components["output"]["guide_metadata_rows"],0);
        assert_eq!(new.components["compact_output_buffer_bytes"],98304);
        assert_eq!(new.components["counting"]["count_maps_growth"],old.components["counting"]["count_maps_growth"]);
        assert_eq!(new.components["counting"]["membership"],old.components["counting"]["membership"]);
        assert!(new.estimate_bytes<=old.estimate_bytes+1048576);
        let mut low=l;low.memory_budget_bytes=1<<20;
        assert_eq!(low.plan_bounded_dual_formats(d,s,w,true,true).unwrap_err(),"resource.memory_budget");
        low=l;low.output_budget_bytes=400000000;
        assert_eq!(low.plan_bounded_dual_formats(d,s,w,true,true).unwrap_err(),"resource.output_budget");
        low.output_budget_bytes=400000000+65536+4096;
        assert!(low.plan_bounded_dual_formats(d,s,w,true,true).is_ok());
        assert_eq!(low.plan_bounded_dual(d,s,w).unwrap_err(),"resource.output_budget");
    }
}

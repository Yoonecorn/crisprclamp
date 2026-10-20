//! Explicit Standard-only strict index followed by bounded record-aligned workers.
//! Workers return checked counters only: there are no intermediate publications.
use super::*;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;

pub(super) fn scope(
    shards: usize,
    profile: ExecutionProfile,
    geometry: &Geometry,
    radius: u8,
    discovery: &DiscoveryMode,
    evidence: &EvidenceMode,
    workers: WorkerConfig,
    path: &Path,
) -> Result<()> {
    if shards == 0 {
        return Ok(());
    }
    if !(2..=12).contains(&shards) {
        return Err("preflight.invalid_shards".into());
    }
    if profile.audited()
        || !matches!(geometry, Geometry::Fixed { .. })
        || radius != 0
        || !matches!(discovery, DiscoveryMode::Bounded(0))
        || !matches!(evidence, EvidenceMode::Off)
        || workers.count != 1
    {
        return Err("preflight.sharding_scope: requires Standard, single-end fixed exact, discovery-k=0, evidence off, workers=1".into());
    }
    if !fs::metadata(path)
        .map_err(|e| format!("input.metadata: {e}"))?
        .is_file()
        || path.extension().is_some_and(|s| s == "gz")
    {
        return Err("preflight.sharding_scope: requires regular uncompressed FASTQ".into());
    }
    let mut file = File::open(path).map_err(|e| format!("input.open: {e}"))?;
    let mut magic = [0; 2];
    let n = file
        .read(&mut magic)
        .map_err(|e| format!("input.read: {e}"))?;
    if n == 2 && magic == [0x1f, 0x8b] {
        return Err("preflight.sharding_scope: gzip magic".into());
    }
    Ok(())
}

pub(super) fn admission(cfg: &Config, guides: usize) -> Result<u64> {
    if cfg.shards == 0 {
        return Ok(0);
    }
    // In addition to the complete conservative plan: all worker stacks, parser
    // capacities, buffered input, counters (including retained results), and
    // bounded handles/boundaries. No FASTQ copies or per-record index exist.
    let state = (cfg.limits.max_record_bytes as u64)
        .checked_mul(5)
        .and_then(|v| v.checked_add((cfg.limits.max_header_bytes as u64) * 2))
        .and_then(|v| v.checked_add(2_424_832))
        .and_then(|v| v.checked_add((guides as u64).checked_mul(32)?))
        .ok_or("resource.dimension_overflow")?;
    state
        .checked_mul(cfg.shards as u64)
        .and_then(|v| v.checked_add(4096))
        .and_then(|v| v.checked_add(h2::admission(cfg.shards).ok()?))
        .ok_or("resource.dimension_overflow".into())
}

fn identity(path: &Path) -> Result<(u64, u64, u64, i64, i64)> {
    let m = fs::metadata(path).map_err(|e| format!("input.metadata: {e}"))?;
    Ok((m.dev(), m.ino(), m.len(), m.mtime(), m.mtime_nsec()))
}
fn make(cfg: &Config, guides: usize) -> SingleWorkerState {
    SingleWorkerState {
        counts: vec![0; guides],
        leaves: [0; 7],
        details: QcDetails::default(),
        discovery: Discovery::new(cfg.discovery_mode, &["unmapped.no_match"]),
    }
}
fn add(target: &mut u64, value: u64) -> Result<()> {
    *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
    Ok(())
}
fn merge(target: &mut SingleWorkerState, state: SingleWorkerState) -> Result<()> {
    for (a, b) in target.counts.iter_mut().zip(state.counts) {
        add(a, b)?;
    }
    for (a, b) in target.leaves.iter_mut().zip(state.leaves) {
        add(a, b)?;
    }
    add(&mut target.details.mapped_exact, state.details.mapped_exact)?;
    add(
        &mut target.details.mapped_rescued_h1,
        state.details.mapped_rescued_h1,
    )?;
    for (a, b) in target
        .details
        .extraction
        .iter_mut()
        .zip(state.details.extraction)
    {
        add(a, b)?;
    }
    target.discovery.merge(state.discovery)?;
    Ok(())
}

pub(super) fn count(
    cfg: &Config,
    fastq: &mut Fastq,
    guides: &[Guide],
    index: &HashMap<String, usize>,
) -> Result<(SingleWorkerState, Value)> {
    let initial = identity(&cfg.fastq)?;
    let clock = Instant::now();
    let outcome = h2::index(
        fastq.reader.as_mut().unwrap(),
        cfg.limits,
        cfg.shards,
        initial.2,
        fastq.ordinal,
    )?;
    fastq.decoded_bytes = outcome.decoded;
    fastq.ordinal = outcome.ordinal;
    if let Some(fault) = outcome.error {
        return Err(fault);
    }
    let bounds = outcome.bounds;
    let index_s = clock.elapsed().as_secs_f64();
    if identity(&cfg.fastq)? != initial || fastq.decoded_bytes != initial.2 {
        return Err("input.sharding_changed".into());
    }
    let effective = bounds.len().saturating_sub(1);
    let clock = Instant::now();
    let results = thread::scope(|scope| {
        let mut children = Vec::new();
        for (shard, pair) in bounds.windows(2).enumerate() {
            let (start, ordinal) = pair[0];
            let (end, end_ordinal) = pair[1];
            #[cfg(debug_assertions)]
            if shard == 1 && test_fault("shard_spawn").is_err() {
                children.push(Err(io::Error::other("test_fault.shard_spawn")));
                continue;
            }
            let child = thread::Builder::new()
                .name(format!("project-c-shard-{shard}"))
                .stack_size(2_097_152)
                .spawn_scoped(scope, move || -> Result<SingleWorkerState> {
                    #[cfg(debug_assertions)]
                    if shard == 0
                        && env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                            .is_ok_and(|s| s.split(',').any(|v| v == "shard_panic"))
                    {
                        panic!("test shard panic");
                    }
                    test_fault("shard_child")?;
                    #[cfg(debug_assertions)]
                    if shard == 2 && test_fault("shard_positioned").is_err() {
                        return Err("input.test_fault".into());
                    }
                    let mut file =
                        File::open(&cfg.fastq).map_err(|e| format!("input.open: {e}"))?;
                    file.seek(SeekFrom::Start(start))
                        .map_err(|e| format!("input.seek: {e}"))?;
                    let mut reader = Fastq {
                        reader: Some(Box::new(BufReader::new(file.take(end - start)))),
                        receiver: None,
                        pending: Vec::new().into_iter(),
                        recycle_sender: None,
                        worker: None,
                        decoded_bytes: 0,
                        digest: None,
                        ordinal,
                        limits: cfg.limits,
                        recycled: None,
                        separator: Vec::new(),
                    };
                    let mut state = make(cfg, guides.len());
                    #[cfg(debug_assertions)]
                    if env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                        .is_ok_and(|s| s == "worker_counter_overflow")
                    {
                        state.counts[0] = u64::MAX;
                    }
                    while let Some(record) = reader.record()? {
                        let result =
                            classify(&record.sequence, &record.quality, cfg, guides, index);
                        increment(&mut state.leaves[result.leaf])?;
                        if let Some(reason) = result.extraction_fault {
                            let slot = match reason {
                                ExtractionFault::MissingAnchor => 0,
                                ExtractionFault::MultipleAnchors => 1,
                                ExtractionFault::OutOfBounds => 2,
                            };
                            increment(&mut state.details.extraction[slot])?;
                        }
                        if let Some(i) = result.guide {
                            increment(&mut state.counts[i])?;
                            increment(&mut state.details.mapped_exact)?;
                        }
                        if result.leaf == 2 {
                            state.discovery.observe(
                                0,
                                &["unknown"],
                                &[result.window.as_deref().expect("unknown window")],
                            )?;
                        }
                        reader.recycle(record);
                    }
                    if reader.ordinal != end_ordinal || reader.decoded_bytes != end - start {
                        return Err("conservation.shard_input".into());
                    }
                    Ok(state)
                });
            children.push(child);
        }
        // Join every child before selecting any error, in ascending input order.
        children
            .into_iter()
            .map(|child| match child {
                Ok(child) => child.join().unwrap_or_else(|_| Err("worker.panic".into())),
                Err(e) => Err(format!("worker.spawn: {e}")),
            })
            .collect::<Vec<_>>()
    });
    let processing_s = clock.elapsed().as_secs_f64();
    // Positioned input/counting faults outrank unpositioned spawn/panic faults,
    // as in the accepted worker contract. Disjoint ordered record intervals
    // make shard order sufficient to choose the earliest positioned fault.
    if let Some((_, fault)) = results
        .iter()
        .enumerate()
        .filter_map(|(shard, result)| result.as_ref().err().map(|fault| (shard, fault)))
        .min_by_key(|(shard, fault)| (fault.starts_with("worker."), *shard))
    {
        return Err(fault.clone());
    }
    let clock = Instant::now();
    let mut state = make(cfg, guides.len());
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|s| s == "worker_reduction_overflow") {
        state.counts[0] = u64::MAX;
    }
    for result in results {
        merge(&mut state, result?)?;
    }
    test_fault("shard_reduction")?;
    if identity(&cfg.fastq)? != initial {
        return Err("input.sharding_changed".into());
    }
    Ok((
        state,
        json!({"requested_shards":cfg.shards,"effective_shards":effective,
        "scope":"standard/single-end/fixed/exact/evidence-off/discovery-k=0/regular-uncompressed",
        "decision":"sharded","fallback_or_rejection_reason":null,
        "worker_memory_addition_bytes":admission(cfg,guides.len())?,
        "process_count":1,"worker_threads":effective,"boundaries":bounds,
        "index_seconds":index_s,"shard_processing_seconds":processing_s,"reduction_seconds":clock.elapsed().as_secs_f64(),
        "intermediate_temporary_bytes":0}),
    ))
}

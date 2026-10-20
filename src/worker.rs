use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, TrySendError};

// Input validation is ordered before classification at a counting-unit
// ordinal. The reader itself checks framing/decompression before pair EOF and
// mate identity, and R1 before R2 for equivalent framing faults.
#[derive(Debug)]
struct Fault {
    ordinal: Option<u64>,
    precedence: u8,
    worker: usize,
    message: String,
}

impl Fault {
    fn input(ordinal: u64, message: String) -> Self {
        let precedence = if message == "input.unequal_eof" {
            1
        } else if message == "input.mate_marker" || message == "input.pair_name" {
            2
        } else {
            0
        };
        Self {
            ordinal: Some(ordinal),
            precedence,
            worker: 0,
            message,
        }
    }
    fn worker(ordinal: u64, worker: usize, message: String) -> Self {
        Self {
            ordinal: Some(ordinal),
            precedence: 3,
            worker,
            message,
        }
    }
    fn unpositioned(worker: usize, message: &str) -> Self {
        Self {
            ordinal: None,
            precedence: 4,
            worker,
            message: message.into(),
        }
    }
    fn key(&self) -> (u64, u8, usize, &str) {
        (
            self.ordinal.unwrap_or(u64::MAX),
            self.precedence,
            self.worker,
            &self.message,
        )
    }
}

fn earliest(faults: impl IntoIterator<Item = Fault>) -> Option<Fault> {
    faults.into_iter().min_by(|a, b| a.key().cmp(&b.key()))
}

fn collect<S>(
    receiver: &Receiver<(usize, std::result::Result<S, Fault>)>,
    slots: &mut [Option<std::result::Result<S, Fault>>],
) {
    while let Ok((id, result)) = receiver.try_recv() {
        slots[id] = Some(result);
    }
}

#[cfg(debug_assertions)]
fn injected(point: &str, ordinal: u64) -> bool {
    env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|value| {
        value
            .split(',')
            .any(|selected| selected == point || selected == format!("{point}:{ordinal}"))
    })
}

pub(super) fn run_batches<R, S, Read, Make, Process>(
    options: WorkerConfig,
    read: Read,
    dedicated_io: bool,
    make: Make,
    process: Process,
) -> Result<Vec<S>>
where
    R: Send,
    S: Send,
    Read: FnMut() -> Result<Option<R>> + Send,
    Make: Fn() -> S + Sync,
    Process: Fn(&mut S, R) -> Result<()> + Sync,
{
    run_batches_partitioned(
        options,
        options.batch_records,
        read,
        dedicated_io,
        make,
        process,
    )
}

// Storage batches may shrink without changing the requested logical worker
// partition. Each worker sees exactly its accepted ordered record subsequence.
pub(super) fn run_batches_partitioned<R, S, Read, Make, Process>(
    options: WorkerConfig,
    logical_batch_records: usize,
    read: Read,
    dedicated_io: bool,
    make: Make,
    process: Process,
) -> Result<Vec<S>>
where
    R: Send,
    S: Send,
    Read: FnMut() -> Result<Option<R>> + Send,
    Make: Fn() -> S + Sync,
    Process: Fn(&mut S, R) -> Result<()> + Sync,
{
    if logical_batch_records == 0 || options.batch_records == 0 {
        return Err("resource.dimension_overflow".into());
    }
    thread::scope(|scope| {
        // Every dispatched record at or before this ordinal must finish before
        // selecting a fault. A producer fault at P lowers it to P-1; a worker
        // fault at W lowers it to W. This preserves earlier queued faults while
        // cancelling all work that cannot win.
        let stop_after = Arc::new(AtomicU64::new(u64::MAX));
        let (result_sender, result_receiver) = mpsc::sync_channel(options.result_queue_capacity);
        let mut senders: Vec<SyncSender<Vec<(u64, R)>>> = Vec::new();
        let mut handles = Vec::new();
        for id in 0..options.count {
            let (sender, receiver) =
                mpsc::sync_channel::<Vec<(u64, R)>>(options.work_queue_capacity);
            senders.push(sender);
            let result_sender = result_sender.clone();
            let stop_after = Arc::clone(&stop_after);
            let make = &make;
            let process = &process;
            handles.push(scope.spawn(move || {
                if dedicated_io {
                    hetero_qos("extract", true);
                }
                let mut state = make();
                let mut outcome = Ok(());
                loop {
                    let _wait = profile::start(profile::BACKPRESSURE);
                    let received = receiver.recv();
                    drop(_wait);
                    let Ok(batch) = received else {
                        break;
                    };
                    for (ordinal, record) in batch {
                        if ordinal > stop_after.load(Ordering::Acquire) {
                            break;
                        }
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            #[cfg(debug_assertions)]
                            if injected("worker_panic", ordinal) && id == 0 {
                                panic!("test worker panic");
                            }
                            #[cfg(debug_assertions)]
                            if injected("worker_error_at", ordinal) {
                                return Err("test_fault.worker_error".into());
                            }
                            process(&mut state, record)
                        }));
                        let fault = match result {
                            Ok(Ok(())) => None,
                            Ok(Err(message)) => Some(message),
                            Err(_) => Some("worker.panic".into()),
                        };
                        if let Some(message) = fault {
                            stop_after.fetch_min(ordinal, Ordering::AcqRel);
                            outcome = Err(Fault::worker(ordinal, id, message));
                            break;
                        }
                    }
                    if outcome.is_err() {
                        break;
                    }
                }
                let _ = result_sender.send((id, outcome.map(|()| state)));
            }));
        }
        drop(result_sender);
        let mut slots: Vec<Option<std::result::Result<S, Fault>>> =
            (0..options.count).map(|_| None).collect();
        // One bounded batch boundary: the IO thread owns framing and dispatch;
        // the coordinator drains final results while extraction workers run.
        // The same ordinal cancellation and earliest-fault arbitration apply.
        let produce = move |drain: &mut dyn FnMut()| {
            if dedicated_io {
                hetero_qos("io", false);
            }
            let mut read = read;
            let mut producer_fault = None;
            let mut last_ordinal = 0_u64;
            'produce: loop {
                drain();
                let mut batch = Vec::with_capacity(options.batch_records);
                let mut finished = false;
                let logical_remaining =
                    logical_batch_records as u64 - last_ordinal % logical_batch_records as u64;
                let stored_records = (options.batch_records as u64).min(logical_remaining);
                for _ in 0..stored_records {
                    let ordinal = match last_ordinal.checked_add(1) {
                        Some(value) => value,
                        None => {
                            producer_fault = Some(Fault::unpositioned(0, "arithmetic.overflow"));
                            finished = true;
                            break;
                        }
                    };
                    if ordinal > stop_after.load(Ordering::Acquire) {
                        finished = true;
                        break;
                    }
                    #[cfg(debug_assertions)]
                    let read_result = if injected("producer_fault_at", ordinal) {
                        Err("input.invalid_header".into())
                    } else {
                        read()
                    };
                    #[cfg(not(debug_assertions))]
                    let read_result = read();
                    match read_result {
                        Ok(Some(record)) => {
                            last_ordinal = ordinal;
                            let _assembly = profile::start(profile::COORDINATOR_BATCH_ASSEMBLY);
                            batch.push((ordinal, record));
                        }
                        Ok(None) => {
                            finished = true;
                            break;
                        }
                        Err(message) => {
                            stop_after.fetch_min(ordinal - 1, Ordering::AcqRel);
                            producer_fault = Some(Fault::input(ordinal, message));
                            finished = true;
                            break;
                        }
                    }
                }
                if !batch.is_empty() {
                    let id = (((batch[0].0 - 1) / logical_batch_records as u64)
                        % options.count as u64) as usize;
                    loop {
                        drain();
                        let _dispatch = profile::start(profile::DISPATCH);
                        match senders[id].try_send(batch) {
                            Ok(()) => break,
                            Err(TrySendError::Full(returned)) => {
                                drop(_dispatch);
                                let _wait = profile::start(profile::BACKPRESSURE);
                                batch = returned;
                                thread::yield_now();
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                break 'produce;
                            }
                        }
                    }
                }
                if finished {
                    break;
                }
            }
            drop(senders);
            producer_fault
        };
        // Dual callers retain their existing coordinator producer until separately
        // qualified. The single-guide serial path never calls this function.
        let (producer, inline_fault) = if dedicated_io {
            (Some(scope.spawn(move || produce(&mut || {}))), None)
        } else {
            (None, produce(&mut || collect(&result_receiver, &mut slots)))
        };
        while slots.iter().any(Option::is_none) {
            match result_receiver.recv() {
                Ok((id, result)) => slots[id] = Some(result),
                Err(_) => break,
            }
        }
        let producer_fault = match producer {
            Some(handle) => match handle.join() {
                Ok(fault) => fault,
                Err(_) => Some(Fault::unpositioned(0, "worker.panic")),
            },
            None => inline_fault,
        };
        let mut faults = producer_fault.into_iter().collect::<Vec<_>>();
        for (id, handle) in handles.into_iter().enumerate() {
            if handle.join().is_err() {
                faults.push(Fault::unpositioned(id, "worker.panic"));
            }
        }
        let mut states = Vec::with_capacity(options.count);
        for (id, slot) in slots.into_iter().enumerate() {
            match slot {
                Some(Ok(state)) => states.push(state),
                Some(Err(fault)) => faults.push(fault),
                None => faults.push(Fault::unpositioned(id, "worker.missing_result")),
            }
        }
        if let Some(fault) = earliest(faults) {
            return Err(fault.message);
        }
        Ok(states)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_selection_orders_positions_precedence_and_fallback() {
        let worker = Fault::worker(3, 1, "worker.earlier".into());
        let producer = Fault::input(8, "input.invalid_header".into());
        assert_eq!(
            earliest([producer, worker]).unwrap().message,
            "worker.earlier"
        );
        let producer = Fault::input(3, "input.invalid_header".into());
        let worker = Fault::worker(8, 1, "worker.later".into());
        assert_eq!(
            earliest([worker, producer]).unwrap().message,
            "input.invalid_header"
        );
        let producer = Fault::input(3, "input.unequal_eof".into());
        let worker = Fault::worker(3, 0, "arithmetic.overflow".into());
        assert_eq!(
            earliest([worker, producer]).unwrap().message,
            "input.unequal_eof"
        );
        let framing = Fault::input(3, "input.invalid_header".into());
        let pair = Fault::input(3, "input.pair_name".into());
        assert_eq!(
            earliest([pair, framing]).unwrap().message,
            "input.invalid_header"
        );
        assert_eq!(
            earliest([Fault::unpositioned(2, "b"), Fault::unpositioned(1, "a")])
                .unwrap()
                .message,
            "a"
        );
    }
}

// Eligible fixed-window extraction is kept here, immediately before batch
// lookup. The legacy classify_component and run_batches remain CPU-only and
// unchanged. The destination is the available shared Metal input allocation.
fn extract_gpu_key(
    record: &FastqRecord,
    cfg: &Config,
    key: &mut [u8; gpu::LENGTH],
) -> Classification {
    key.fill(0);
    if record
        .sequence
        .iter()
        .any(|b| !matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N'))
    {
        return Classification::leaf(3);
    }
    let Geometry::Fixed { start } = cfg.geometry else {
        unreachable!("GPU fixed eligibility")
    };
    let start = match window_start(&record.sequence, gpu::LENGTH, start) {
        Ok(start) => start,
        Err(fault) => return Classification::extraction(fault),
    };
    for (offset, byte) in key.iter_mut().enumerate() {
        *byte = match cfg.orientation {
            Orientation::Forward => record.sequence[start + offset].to_ascii_uppercase(),
            Orientation::ReverseComplement => complement(
                record.sequence[record.sequence.len() - 1 - start - offset].to_ascii_uppercase(),
            ),
        };
    }
    if key.contains(&b'N') {
        return Classification::leaf(5);
    }
    if let Some(min_phred) = cfg.min_phred {
        let low = match cfg.orientation {
            Orientation::Forward => record.quality[start..start + gpu::LENGTH]
                .iter()
                .any(|&q| q - 33 < min_phred),
            Orientation::ReverseComplement => record.quality
                [record.quality.len() - start - gpu::LENGTH..record.quality.len() - start]
                .iter()
                .any(|&q| q - 33 < min_phred),
        };
        if low {
            return Classification::leaf(6);
        }
    }
    Classification::leaf(2)
}

pub(super) fn run_gpu_single<S>(
    cfg: &Config,
    fastq: &mut Fastq,
    guides: &[Guide],
    gpu: &gpu::Pipeline,
    make: impl Fn() -> S,
    accumulate: impl Fn(&mut S, Classification) -> Result<()>,
) -> Result<Vec<S>> {
    run_gpu_with(
        cfg,
        fastq,
        guides.len(),
        make,
        accumulate,
        |fill, consume| gpu.run(fill, consume),
    )
}

// This adapter preserves the CPU batch-to-worker assignment for discovery and
// fault identity, even when a GPU dispatch groups many CPU batches together.
// Each row retains only ordinal + leaf (16 bytes): at most 2 MiB across both
// slots, within the existing multi-worker state reservation. Keys stay in the
// shared input allocation until accumulation; windows are materialized lazily.
// It also allows CPU-only testing of the exact production extraction boundary.
fn run_gpu_with<S>(
    cfg: &Config,
    fastq: &mut Fastq,
    guide_count: usize,
    make: impl Fn() -> S,
    accumulate: impl Fn(&mut S, Classification) -> Result<()>,
    run: impl FnOnce(
        &mut dyn FnMut(&mut [u8]) -> Result<Vec<(u64, u8)>>,
        &mut dyn FnMut(Vec<(u64, u8)>, &[u8], &[u32]) -> Result<()>,
    ) -> Result<()>,
) -> Result<Vec<S>> {
    use std::cell::{Cell, RefCell};
    let mut states: Vec<S> = (0..cfg.workers.count).map(|_| make()).collect();
    let mut ordinal = 0u64;
    let stopped = Cell::new(false);
    let faults = RefCell::new(Vec::new());
    let mut fill = |bytes: &mut [u8]| -> Result<Vec<(u64, u8)>> {
        hetero_qos("extract", true);
        let mut rows = Vec::with_capacity(bytes.len() / gpu::LENGTH);
        for key in bytes.chunks_exact_mut(gpu::LENGTH) {
            if stopped.get() {
                break;
            }
            let Some(next) = ordinal.checked_add(1) else {
                faults
                    .borrow_mut()
                    .push(Fault::unpositioned(0, "arithmetic.overflow"));
                stopped.set(true);
                break;
            };
            #[cfg(debug_assertions)]
            let record = if injected("producer_fault_at", next) {
                Err("input.invalid_header".into())
            } else {
                fastq.record()
            };
            #[cfg(not(debug_assertions))]
            let record = fastq.record();
            match record {
                Ok(Some(record)) => {
                    ordinal = next;
                    let classification =
                        extract_gpu_key(&record, cfg, key.try_into().expect("20-byte slot"));
                    rows.push((ordinal, classification.leaf as u8));
                }
                Ok(None) => {
                    stopped.set(true);
                    break;
                }
                Err(message) => {
                    faults.borrow_mut().push(Fault::input(next, message));
                    stopped.set(true);
                    break;
                }
            }
        }
        // A read fault does not discard preceding rows: dispatch the partial
        // batch and drain its ordered classifications before fault selection.
        Ok(rows)
    };
    let mut consume = |rows: Vec<(u64, u8)>, keys: &[u8], indices: &[u32]| -> Result<()> {
        if rows.len() != indices.len() || keys.len() != rows.len() * gpu::LENGTH {
            return Err("gpu.output_length".into());
        }
        for (((ordinal, leaf), key), &index) in rows
            .into_iter()
            .zip(keys.chunks_exact(gpu::LENGTH))
            .zip(indices)
        {
            let worker = ((ordinal - 1) / cfg.workers.batch_records as u64
                % cfg.workers.count as u64) as usize;
            let mut classification = if leaf == 4 {
                Classification::extraction(ExtractionFault::OutOfBounds)
            } else {
                Classification::leaf(leaf as usize)
            };
            if leaf == 2 {
                classification.window =
                    Some(String::from_utf8(key.to_vec()).expect("DNA key is ASCII"));
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                #[cfg(debug_assertions)]
                if injected("worker_panic", ordinal) && worker == 0 {
                    panic!("test worker panic");
                }
                #[cfg(debug_assertions)]
                if injected("worker_error_at", ordinal) {
                    return Err("test_fault.worker_error".into());
                }
                if classification.leaf == 2 && index != gpu::NO_MATCH {
                    if index as usize >= guide_count {
                        return Err("gpu.output_index".into());
                    }
                    classification.leaf = 0;
                    classification.guide = Some(index as usize);
                    classification.distance = Some(0);
                }
                accumulate(&mut states[worker], classification)
            }));
            let message = match result {
                Ok(Ok(())) => continue,
                Ok(Err(message)) => message,
                Err(_) => "worker.panic".into(),
            };
            faults
                .borrow_mut()
                .push(Fault::worker(ordinal, worker, message));
            stopped.set(true);
            // Earlier input faults win through the unchanged Fault ordering.
            return Err("gpu.classification_failed".into());
        }
        Ok(())
    };
    let outcome = run(&mut fill, &mut consume);
    if let Some(fault) = earliest(faults.into_inner()) {
        return Err(fault.message);
    }
    outcome?;
    Ok(states)
}

#[cfg(test)]
mod gpu_boundary_tests {
    use super::*;
    fn config() -> Config {
        Config {
            no_gpu: false,
            shards: 0,
            execution_profile: ExecutionProfile::CryptographicAudited,
            sample: "s".into(),
            fastq: PathBuf::new(),
            guides: PathBuf::new(),
            geometry: Geometry::Fixed { start: 1 },
            orientation: Orientation::Forward,
            radius: 0,
            min_phred: Some(20),
            discovery_mode: DiscoveryMode::Bounded(3),
            evidence: EvidenceMode::Off,
            workers: WorkerConfig {
                count: 4,
                batch_records: 3,
                work_queue_capacity: 1,
                result_queue_capacity: 1,
            },
            limits: Limits {
                max_record_bytes: 1024,
                max_header_bytes: 256,
                memory_budget_bytes: 1_073_741_824,
                output_budget_bytes: 1_073_741_824,
            },
            output: PathBuf::new(),
        }
    }
    #[test]
    fn extraction_priority_orientation_quality_and_miss_match_cpu() {
        let mut cfg = config();
        let guides = [Guide {
            id: "a".into(),
            sequence: "ACGT".repeat(5),
            count: 0,
        }];
        let index = HashMap::from([(guides[0].sequence.clone(), 0)]);
        for orientation in [Orientation::Forward, Orientation::ReverseComplement] {
            cfg.orientation = orientation;
            for sequence in [
                "xACGTACGTACGTACGTACGT",
                "ACGT",
                "NACGTACGTACGTACGTACGT",
                "ANCCTACGTACGTACGTACGT",
                "aACGTACGTACGTACGTACGT",
                "TTTTTTTTTTTTTTTTTTTTT",
            ] {
                for low_at in [None, Some(0), Some(1), Some(20)] {
                    let mut quality = vec![b'I'; sequence.len()];
                    if let Some(i) = low_at.filter(|&i| i < quality.len()) {
                        quality[i] = b'!';
                    }
                    let record = FastqRecord {
                        header: b"@r".to_vec(),
                        sequence: sequence.as_bytes().to_vec(),
                        quality,
                    };
                    let mut key = [0; gpu::LENGTH];
                    let mut actual = extract_gpu_key(&record, &cfg, &mut key);
                    // Emulated exact GPU table lookup after extraction.
                    if actual.leaf == 2 {
                        actual.window = Some(String::from_utf8(key.to_vec()).unwrap());
                    }
                    if actual.leaf == 2 && key == *b"ACGTACGTACGTACGTACGT" {
                        actual.leaf = 0;
                        actual.guide = Some(0);
                        actual.distance = Some(0);
                    }
                    let expected =
                        classify(&record.sequence, &record.quality, &cfg, &guides, &index);
                    assert_eq!(
                        (actual.leaf, actual.guide, actual.distance, actual.window),
                        (
                            expected.leaf,
                            expected.guide,
                            expected.distance,
                            expected.window
                        )
                    );
                    assert_eq!(
                        actual.extraction_fault.map(|_| 4),
                        expected.extraction_fault.map(|_| 4)
                    );
                }
            }
        }
    }
    #[test]
    fn gpu_adapter_partial_eof_retains_cpu_worker_partition_and_discovery() {
        let cfg = config();
        let path = env::temp_dir().join(format!("gpu-adapter-{}.fastq", std::process::id()));
        let mut text = String::new();
        for i in 0..17 {
            text.push_str(&format!(
                "@r{i}\n{}\n+\n{}\n",
                "T".repeat(21),
                "I".repeat(21)
            ));
        }
        fs::write(&path, text).unwrap();
        let mut fastq = Fastq::open(&path, cfg.limits, cfg.execution_profile).unwrap();
        let states = run_gpu_with(
            &cfg,
            &mut fastq,
            1,
            Vec::new,
            |state, classification| {
                assert_eq!(classification.leaf, 2);
                state.push(classification.window.unwrap());
                Ok(())
            },
            |fill, consume| {
                // Seven rows per emulated GPU dispatch, independent of the
                // CPU scheduling batch size of three. Final dispatch has three.
                loop {
                    let mut keys = [0; 7 * gpu::LENGTH];
                    let rows = fill(&mut keys)?;
                    if rows.is_empty() {
                        break;
                    }
                    let values = vec![gpu::NO_MATCH; rows.len()];
                    consume(rows, &keys[..values.len() * gpu::LENGTH], &values)?;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            states.iter().map(Vec::len).collect::<Vec<_>>(),
            [6, 5, 3, 3]
        );
        assert_eq!(states.iter().map(Vec::len).sum::<usize>(), 17);
        drop(fastq);
        fs::remove_file(path).unwrap();
    }
    #[test]
    fn storage_microbatches_preserve_logical_partition_and_earliest_fault() {
        for stored in [1, 2, 3, 5] {
            let options = WorkerConfig {
                count: 4,
                batch_records: stored,
                work_queue_capacity: 1,
                result_queue_capacity: 1,
            };
            let mut input = (1_u64..=43).into_iter();
            let states = run_batches_partitioned(
                options,
                5,
                || Ok(input.next()),
                false,
                Vec::new,
                |state, record| {
                    state.push(record);
                    Ok(())
                },
            )
            .unwrap();
            for (worker, records) in states.iter().enumerate() {
                assert_eq!(
                    *records,
                    (1_u64..=43)
                        .filter(|n| ((n - 1) / 5) % 4 == worker as u64)
                        .collect::<Vec<_>>()
                );
            }
            let mut ordinal = 0_u64;
            let fault = run_batches_partitioned(
                options,
                2,
                || {
                    ordinal += 1;
                    if ordinal == 3 {
                        Err("input.invalid_header".into())
                    } else {
                        Ok(Some(ordinal))
                    }
                },
                false,
                Vec::new,
                |state, record| {
                    state.push(record);
                    if state.len() > 1 {
                        Err("resource.discovery_limit".into())
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert!(fault.contains("resource.discovery_limit"), "{fault}");
        }
    }
}

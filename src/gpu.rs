use super::*;

pub const LENGTH: usize = 20;
pub const NO_MATCH: u32 = u32::MAX;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Slot {
    pub key: [u8; LENGTH],
    pub guide_index: u32,
    pub occupied: u32,
}
const _: () = assert!(std::mem::size_of::<Slot>() == 28);
const _: () = assert!(std::mem::align_of::<Slot>() == 4);
const _: () = assert!(std::mem::offset_of!(Slot, guide_index) == 20);
const _: () = assert!(std::mem::offset_of!(Slot, occupied) == 24);

#[cfg(any(target_os = "macos", test))]
pub fn fnv1a(key: &[u8; LENGTH]) -> u32 {
    key.iter().fold(2_166_136_261u32, |hash, &byte| {
        (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
    })
}

#[cfg(test)]
pub fn build_table(guides: &[String]) -> Result<Vec<Slot>> {
    let mut slots = vec![Slot::default(); table_capacity(guides.len())?];
    insert_guides(&mut slots, guides.iter().map(String::as_str))?;
    Ok(slots)
}

#[cfg(any(target_os = "macos", test))]
fn table_capacity(count: usize) -> Result<usize> {
    if count == 0 || count > MAX_GUIDES {
        return Err("gpu.invalid_guide_count".into());
    }
    count
        .checked_mul(2)
        .and_then(usize::checked_next_power_of_two)
        .ok_or("gpu.capacity_overflow".into())
}

#[cfg(any(target_os = "macos", test))]
fn insert_guides<'a>(slots: &mut [Slot], guides: impl Iterator<Item = &'a str>) -> Result<()> {
    let capacity = slots.len();
    for (index, guide) in guides.enumerate() {
        let key: [u8; LENGTH] = guide.as_bytes().try_into().map_err(|_| "gpu.invalid_key")?;
        let mut slot = fnv1a(&key) as usize & (capacity - 1);
        loop {
            if slots[slot].occupied == 0 {
                slots[slot] = Slot {
                    key,
                    guide_index: index as u32,
                    occupied: 1,
                };
                break;
            }
            if slots[slot].key == key {
                return Err("gpu.duplicate_key".into());
            }
            slot = (slot + 1) & (capacity - 1);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn lookup(table: &[Slot], key: &[u8; LENGTH]) -> u32 {
        let mask = table.len() - 1;
        let mut slot = fnv1a(key) as usize & mask;
        for _ in 0..table.len() {
            if table[slot].occupied == 0 {
                return NO_MATCH;
            }
            if table[slot].key == *key {
                return table[slot].guide_index;
            }
            slot = (slot + 1) & mask;
        }
        NO_MATCH
    }
    #[test]
    fn fnv_matches_known_twenty_byte_vector() {
        // Independently calculated FNV-1a vector, not a shader execution test.
        assert_eq!(fnv1a(b"AAAAAAAAAAAAAAAAAAAA"), 0x6161_9849);
    }
    #[test]
    fn collisions_wrap_and_misses_stop_at_empty_slots() {
        let mut guides = Vec::new();
        for n in 0..10_000 {
            let key = format!("{n:020}");
            let bytes: [u8; LENGTH] = key.as_bytes().try_into().unwrap();
            if fnv1a(&bytes) & 7 == 7 {
                guides.push(key);
            }
            if guides.len() == 3 {
                break;
            }
        }
        let table = build_table(&guides).unwrap();
        assert_eq!(table.len(), 8);
        assert_eq!(table[7].guide_index, 0);
        assert_eq!(table[0].guide_index, 1);
        assert_eq!(table[1].guide_index, 2);
        for (i, key) in guides.iter().enumerate() {
            assert_eq!(
                lookup(&table, &key.as_bytes().try_into().unwrap()),
                i as u32
            );
        }
        assert_eq!(lookup(&table, &[0; LENGTH]), NO_MATCH);
    }
    #[test]
    fn capacity_load_and_full_reference_indices() {
        let guides: Vec<String> = (0..77_441).map(|n| format!("{n:020}")).collect();
        let table = build_table(&guides).unwrap();
        assert_eq!(table.len(), 262_144);
        assert!(guides.len() * 2 <= table.len());
        for (i, key) in guides.iter().enumerate() {
            assert_eq!(
                lookup(&table, &key.as_bytes().try_into().unwrap()),
                i as u32
            );
        }
    }
    #[test]
    fn rejects_invalid_or_duplicate_keys() {
        assert!(build_table(&[]).is_err());
        assert!(build_table(&["A".into()]).is_err());
        assert!(build_table(&["A".repeat(20), "A".repeat(20)]).is_err());
    }
}

// The two shared input/output pairs total 3 MiB. Limit the table to 7 MiB,
// making the planner's fixed 10 MiB charge an upper bound on GPU allocations.
pub(super) const BATCH: usize = 65_536;
const MAX_GUIDES: usize = 131_072;
pub const GPU_CROSSOVER_READS: u64 = 400_000;
const EXPECTED_BYTES_PER_READ: u64 = 150;

fn estimate_read_count(file_size: Option<u64>) -> u64 {
    file_size.map_or(u64::MAX, |bytes| bytes / EXPECTED_BYTES_PER_READ)
}

#[derive(Debug, PartialEq, Eq)]
enum IneligibleReason {
    BelowCrossoverThreshold,
    UnsupportedConfiguration,
}

#[derive(Debug, PartialEq, Eq)]
enum GpuEligibility {
    Eligible,
    Ineligible { reason: IneligibleReason },
}

#[derive(Clone, Copy)]
pub(super) struct Eligibility {
    pub single: bool,
    pub length: usize,
    pub fixed: bool,
    pub barcode_radius: u8,
    pub radius: u8,
    pub no_gpu: bool,
    pub device_available: bool,
    pub workers: usize,
    pub shards: usize,
    pub guide_count: usize,
    pub estimated_read_count: u64,
}
impl Eligibility {
    pub fn allowed(self) -> bool {
        self.check() == GpuEligibility::Eligible
    }

    fn check(self) -> GpuEligibility {
        if self.estimated_read_count < GPU_CROSSOVER_READS {
            return GpuEligibility::Ineligible {
                reason: IneligibleReason::BelowCrossoverThreshold,
            };
        }
        if self.single
            && self.length == LENGTH
            && self.fixed
            && self.barcode_radius == 0
            && self.radius == 0
            && !self.no_gpu
            && self.device_available
            && self.workers > 1
            && self.shards == 0
            && self.guide_count > 0
            && self.guide_count <= MAX_GUIDES
        {
            GpuEligibility::Eligible
        } else {
            GpuEligibility::Ineligible {
                reason: IneligibleReason::UnsupportedConfiguration,
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) type Device = metal::Device;
#[cfg(not(target_os = "macos"))]
pub(super) struct Device;

pub(super) fn detect(cfg: &Config, length: usize, guide_count: usize) -> Option<Device> {
    if cfg.no_gpu {
        return None;
    }
    let eligibility = Eligibility {
        single: true,
        length,
        fixed: matches!(cfg.geometry, Geometry::Fixed { .. }),
        barcode_radius: 0,
        radius: cfg.radius,
        no_gpu: cfg.no_gpu,
        device_available: true,
        workers: cfg.workers.count,
        shards: cfg.shards,
        guide_count,
        // Only regular files have a useful size. Pipes, stdin paths and metadata
        // errors retain eligibility without reading or decompressing the input.
        estimated_read_count: estimate_read_count(
            fs::metadata(&cfg.fastq)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len()),
        ),
    };
    if eligibility.check()
        == (GpuEligibility::Ineligible {
            reason: IneligibleReason::BelowCrossoverThreshold,
        })
        && env::var("PROJECT_C_GPU_DIAGNOSTICS").is_ok_and(|value| value == "1")
    {
        eprintln!(
            "gpu_active=false reason=BelowCrossoverThreshold estimated_read_count={}",
            eligibility.estimated_read_count
        );
    }
    // Reject unsupported settings before querying Metal, particularly workers=1.
    if !eligibility.allowed() {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        metal::objc::rc::autoreleasepool(|| {
            metal::Device::system_default().filter(|device| device.has_unified_memory())
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(any(target_os = "macos", test))]
trait Backend {
    fn input(&mut self, slot: usize) -> &mut [u8];
    fn submit(&mut self, slot: usize, count: usize) -> Result<()>;
    fn complete(&mut self, slot: usize, count: usize) -> Result<(&[u8], &[u32])>;
}

// Backend-independent ownership protocol, also exercised by CPU-only tests.
// Metadata and GPU indices are consumed in submission order before slot reuse.
#[cfg(any(target_os = "macos", test))]
fn drive<M>(
    backend: &mut impl Backend,
    mut fill: impl FnMut(&mut [u8]) -> Result<Vec<M>>,
    mut consume: impl FnMut(Vec<M>, &[u8], &[u32]) -> Result<()>,
) -> Result<()> {
    let mut pending: [Option<Vec<M>>; 2] = [None, None];
    let mut slot = 0;
    loop {
        if let Some(rows) = pending[slot].take() {
            let (keys, indices) = backend.complete(slot, rows.len())?;
            consume(rows, keys, indices)?;
        }
        let rows = fill(backend.input(slot))?;
        if rows.is_empty() {
            break;
        }
        backend.submit(slot, rows.len())?;
        pending[slot] = Some(rows);
        slot ^= 1;
    }
    for slot in [slot, slot ^ 1] {
        if let Some(rows) = pending[slot].take() {
            let (keys, indices) = backend.complete(slot, rows.len())?;
            consume(rows, keys, indices)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) struct Pipeline {
    device: metal::Device,
    queue: metal::CommandQueue,
    pipeline: metal::ComputePipelineState,
    table: metal::Buffer,
    slot_count: u32,
}
#[cfg(not(target_os = "macos"))]
pub(super) struct Pipeline;

pub(super) fn initialize(device: Device, guides: &[Guide]) -> Option<Pipeline> {
    #[cfg(target_os = "macos")]
    {
        metal::objc::rc::autoreleasepool(|| Pipeline::new(device, guides).ok())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (device, guides);
        None
    }
}

#[cfg(target_os = "macos")]
struct Pair {
    input: metal::Buffer,
    output: metal::Buffer,
}

#[cfg(target_os = "macos")]
struct MetalBackend<'a> {
    gpu: &'a Pipeline,
    buffers: &'a mut [Pair; 2],
    submitted: mpsc::SyncSender<(usize, metal::CommandBuffer)>,
    completed: [mpsc::Receiver<metal::MTLCommandBufferStatus>; 2],
}

#[cfg(target_os = "macos")]
impl Backend for MetalBackend<'_> {
    fn input(&mut self, slot: usize) -> &mut [u8] {
        // SAFETY: drive consumes completion before every subsequent CPU write.
        // The returned borrow ends before submit; the other slot may be in flight.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.buffers[slot].input.contents().cast(),
                BATCH * LENGTH,
            )
        }
    }
    fn submit(&mut self, slot: usize, n: usize) -> Result<()> {
        use metal::*;
        metal::objc::rc::autoreleasepool(|| {
            let command = self.gpu.queue.new_command_buffer();
            let encoder = command.new_compute_command_encoder();
            encoder.set_compute_pipeline_state(&self.gpu.pipeline);
            encoder.set_buffer(0, Some(&self.gpu.table), 0);
            encoder.set_buffer(1, Some(&self.buffers[slot].input), 0);
            encoder.set_buffer(2, Some(&self.buffers[slot].output), 0);
            let count = n as u32;
            encoder.set_bytes(3, 4, (&self.gpu.slot_count as *const u32).cast());
            encoder.set_bytes(4, 4, (&count as *const u32).cast());
            encoder.dispatch_threads(
                MTLSize::new(n as u64, 1, 1),
                MTLSize::new(self.gpu.pipeline.thread_execution_width(), 1, 1),
            );
            encoder.end_encoding();
            let retained = command.to_owned();
            command.commit();
            self.submitted
                .send((slot, retained))
                .map_err(|_| "gpu.completion_disconnected".into())
        })
    }

    fn complete(&mut self, slot: usize, n: usize) -> Result<(&[u8], &[u32])> {
        let status = self.completed[slot]
            .recv()
            .map_err(|_| "gpu.completion_disconnected")?;
        if status != metal::MTLCommandBufferStatus::Completed {
            return Err(format!("gpu.execution_failed: {status:?}"));
        }
        // SAFETY: semaphore is sent only after Metal completion. The slot is
        // not written or submitted again until consume returns.
        Ok(unsafe {
            (
                std::slice::from_raw_parts(self.buffers[slot].input.contents().cast(), n * LENGTH),
                std::slice::from_raw_parts(self.buffers[slot].output.contents().cast(), n),
            )
        })
    }
}

#[cfg(target_os = "macos")]
impl Pipeline {
    fn new(device: Device, guides: &[Guide]) -> Result<Self> {
        use metal::*;
        let library =
            device.new_library_with_source(include_str!("lookup.metal"), &CompileOptions::new())?;
        let function = library.get_function("guide_lookup", None)?;
        let pipeline = device.new_compute_pipeline_state_with_function(&function)?;
        let capacity = table_capacity(guides.len())?;
        let table = device.new_buffer(
            (capacity * std::mem::size_of::<Slot>()) as u64,
            MTLResourceOptions::StorageModeShared,
        );
        // SAFETY: fresh Metal allocation is aligned for Slot; CPU exclusively
        // initializes it before any command sees the immutable table. Direct
        // construction avoids holding a second 7 MiB staging table at startup.
        let slots =
            unsafe { std::slice::from_raw_parts_mut(table.contents().cast::<Slot>(), capacity) };
        slots.fill(Slot::default());
        insert_guides(slots, guides.iter().map(|g| g.sequence.as_str()))?;
        let queue = device.new_command_queue();
        Ok(Self {
            device,
            queue,
            pipeline,
            table,
            slot_count: capacity as u32,
        })
    }

    pub(super) fn run<M>(
        &self,
        fill: impl FnMut(&mut [u8]) -> Result<Vec<M>>,
        consume: impl FnMut(Vec<M>, &[u8], &[u32]) -> Result<()>,
    ) -> Result<()> {
        use metal::*;
        let mut buffers = std::array::from_fn(|_| Pair {
            input: self.device.new_buffer(
                (BATCH * LENGTH) as u64,
                MTLResourceOptions::StorageModeShared,
            ),
            output: self
                .device
                .new_buffer((BATCH * 4) as u64, MTLResourceOptions::StorageModeShared),
        });
        completion_scope(
            |command: CommandBuffer| {
                command.wait_until_completed();
                command.status()
            },
            |submitted, completed| {
                let mut backend = MetalBackend {
                    gpu: self,
                    buffers: &mut buffers,
                    submitted,
                    completed,
                };
                drive(&mut backend, fill, consume)
            },
        )
    }
}

#[cfg(not(target_os = "macos"))]
impl Pipeline {
    pub(super) fn run<M>(
        &self,
        _: impl FnMut(&mut [u8]) -> Result<Vec<M>>,
        _: impl FnMut(Vec<M>, &[u8], &[u32]) -> Result<()>,
    ) -> Result<()> {
        Err("gpu.unavailable".into())
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    fn eligible() -> Eligibility {
        Eligibility {
            single: true,
            length: 20,
            fixed: true,
            barcode_radius: 0,
            radius: 0,
            no_gpu: false,
            device_available: true,
            workers: 4,
            shards: 0,
            guide_count: 77_441,
            estimated_read_count: u64::MAX,
        }
    }
    #[test]
    fn gpu_ineligible_below_crossover_threshold() {
        let e = eligible();
        let estimate = estimate_read_count(Some(100_000 * 150));
        assert_eq!(estimate, 100_000);
        assert_eq!(
            Eligibility {
                estimated_read_count: estimate,
                ..e
            }
            .check(),
            GpuEligibility::Ineligible {
                reason: IneligibleReason::BelowCrossoverThreshold
            }
        );
        assert!(
            !Eligibility {
                estimated_read_count: GPU_CROSSOVER_READS - 1,
                ..e
            }
            .allowed()
        );
        assert!(
            Eligibility {
                estimated_read_count: GPU_CROSSOVER_READS,
                ..e
            }
            .allowed()
        );
        assert_eq!(estimate_read_count(None), u64::MAX);
        assert!(
            Eligibility {
                estimated_read_count: estimate_read_count(None),
                ..e
            }
            .allowed()
        );
    }

    #[test]
    fn all_ineligible_conditions_select_cpu() {
        let e = eligible();
        assert!(e.allowed());
        for rejected in [
            Eligibility { single: false, ..e }, // dual and concordance
            Eligibility { length: 19, ..e },
            Eligibility { fixed: false, ..e },
            Eligibility {
                barcode_radius: 1,
                ..e
            },
            Eligibility { radius: 1, ..e },
            Eligibility { no_gpu: true, ..e },
            Eligibility {
                device_available: false,
                ..e
            },
            Eligibility { workers: 1, ..e },
            Eligibility { shards: 2, ..e },
            Eligibility {
                guide_count: MAX_GUIDES + 1,
                ..e
            },
        ] {
            assert!(!rejected.allowed());
        }
    }

    struct Mock {
        bytes: [Vec<u8>; 2],
        values: [Vec<u32>; 2],
        pending: [bool; 2],
        dispatched: Vec<usize>,
    }
    impl Mock {
        fn new() -> Self {
            Self {
                bytes: [vec![0; 3 * LENGTH], vec![0; 3 * LENGTH]],
                values: [Vec::new(), Vec::new()],
                pending: [false; 2],
                dispatched: Vec::new(),
            }
        }
    }
    impl Backend for Mock {
        fn input(&mut self, slot: usize) -> &mut [u8] {
            assert!(!self.pending[slot], "write while slot in flight");
            &mut self.bytes[slot]
        }
        fn submit(&mut self, slot: usize, count: usize) -> Result<()> {
            assert!(!self.pending[slot]);
            self.pending[slot] = true;
            self.dispatched.push(count);
            self.values[slot] = self.bytes[slot]
                .chunks_exact(LENGTH)
                .take(count)
                .map(|b| u32::from(b[0]))
                .collect();
            Ok(())
        }
        fn complete(&mut self, slot: usize, count: usize) -> Result<(&[u8], &[u32])> {
            assert!(self.pending[slot]);
            self.pending[slot] = false;
            assert_eq!(self.values[slot].len(), count);
            Ok((&self.bytes[slot][..count * LENGTH], &self.values[slot]))
        }
    }
    #[test]
    fn partial_exact_and_empty_eof_drain_preserves_order_and_reuse() {
        for total in [0, 1, 2, 3, 4, 6, 7, 9, 10, 30] {
            let mut backend = Mock::new();
            let mut next = 0;
            let mut observed = Vec::new();
            drive(
                &mut backend,
                |bytes| {
                    let mut rows = Vec::new();
                    for key in bytes.chunks_exact_mut(LENGTH) {
                        if next == total {
                            break;
                        }
                        key.fill(next as u8);
                        rows.push(next as u32);
                        next += 1;
                    }
                    Ok(rows)
                },
                |rows, keys, indices| {
                    assert_eq!(keys.len(), rows.len() * LENGTH);
                    assert_eq!(rows, indices);
                    observed.extend_from_slice(indices);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(observed, (0..total as u32).collect::<Vec<_>>());
            assert_eq!(backend.pending, [false; 2]);
            assert_eq!(backend.dispatched.iter().sum::<usize>(), total);
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn completion_scope<C: Send, S: Send, T>(
    wait: impl Fn(C) -> S + Send,
    run: impl FnOnce(mpsc::SyncSender<(usize, C)>, [mpsc::Receiver<S>; 2]) -> Result<T>,
) -> Result<T> {
    let (done_a, completed_a) = mpsc::sync_channel(1);
    let (done_b, completed_b) = mpsc::sync_channel(1);
    thread::scope(|scope| {
        let (submitted, work) = mpsc::sync_channel::<(usize, C)>(2);
        scope.spawn(move || {
            let done = [done_a, done_b];
            for (slot, command) in work {
                let status = wait(command);
                // Capacity one per pair suffices, even if the consumer exits.
                let _ = done[slot].send(status);
            }
        });
        // run owns submission and closes it on return/error/unwind. The scope
        // drains committed work and joins before the caller's buffers can drop.
        run(submitted, [completed_a, completed_b])
    })
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Allocation(Arc<AtomicUsize>);
    impl Drop for Allocation {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn submitted_work_joins_before_allocations_drop_on_success_error_and_panic() {
        for exit in [0, 1, 2] {
            let released = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let buffers = [
                    Arc::new(Allocation(released.clone())),
                    Arc::new(Allocation(released.clone())),
                ];
                let finished = completed.clone();
                let result = completion_scope(
                    move |command: Arc<Allocation>| {
                        assert_eq!(command.0.load(Ordering::SeqCst), 0);
                        finished.fetch_add(1, Ordering::SeqCst);
                    },
                    |submitted, receivers| {
                        submitted.send((0, buffers[0].clone())).unwrap();
                        submitted.send((1, buffers[1].clone())).unwrap();
                        match exit {
                            1 => Err("input.error".into()),
                            2 => panic!("consumer panic"),
                            _ => {
                                receivers[0].recv().unwrap();
                                receivers[1].recv().unwrap();
                                Ok(())
                            }
                        }
                    },
                );
                assert_eq!(completed.load(Ordering::SeqCst), 2);
                assert_eq!(released.load(Ordering::SeqCst), 0);
                result
            }));
            assert_eq!(completed.load(Ordering::SeqCst), 2);
            assert_eq!(released.load(Ordering::SeqCst), 2);
            match exit {
                0 => assert!(outcome.unwrap().is_ok()),
                1 => assert!(outcome.unwrap().is_err()),
                _ => assert!(outcome.is_err()),
            }
        }
    }
}

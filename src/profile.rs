//! Opt-in diagnostic timing. These counters never enter scientific artifacts.
use serde_json::json;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub(crate) const INPUT: usize = 0;
pub(crate) const GZIP: usize = 1;
pub(crate) const PARSE: usize = 2;
pub(crate) const CLASSIFY: usize = 3;
pub(crate) const DISCOVERY: usize = 4;
pub(crate) const WORKER: usize = 5;
pub(crate) const DISPATCH: usize = 6;
pub(crate) const BACKPRESSURE: usize = 7;
pub(crate) const MERGE: usize = 8;
pub(crate) const EVIDENCE: usize = 9;
pub(crate) const OUTPUT: usize = 10;
pub(crate) const FASTQ_LINE_FRAMING: usize = 11;
pub(crate) const FASTQ_HEADER_VALIDATE: usize = 12;
pub(crate) const FASTQ_SEQUENCE_VALIDATE: usize = 13;
pub(crate) const FASTQ_PLUS_LINE_VALIDATE: usize = 14;
pub(crate) const FASTQ_QUALITY_VALIDATE: usize = 15;
pub(crate) const FASTQ_RECORD_ASSEMBLY: usize = 16;
pub(crate) const FASTQ_PAIR_COORDINATE_VALIDATE: usize = 17;
pub(crate) const COORDINATOR_BATCH_ASSEMBLY: usize = 18;

pub(crate) const NUM_STAGES: usize = 19;
pub(crate) const NAMES: [&str; NUM_STAGES] = [
    "input_read",
    "gzip_decode",
    "fastq_parse_validate",
    "classify_extract",
    "discovery",
    "worker_processing",
    "dispatch",
    "queue_backpressure",
    "merge",
    "evidence",
    "output_write_publish",
    "fastq_line_framing",
    "fastq_header_validate",
    "fastq_sequence_validate",
    "fastq_plus_line_validate",
    "fastq_quality_validate",
    "fastq_record_assembly",
    "fastq_pair_coordinate_validate",
    "coordinator_batch_assembly",
];

struct Profile {
    path: PathBuf,
    started: Instant,
    nanos: [AtomicU64; NUM_STAGES],
    calls: [AtomicU64; NUM_STAGES],
}
static PROFILE: OnceLock<Option<Profile>> = OnceLock::new();

pub(crate) fn init() {
    let _ = PROFILE.set(
        std::env::var_os("PROJECT_C_STAGE_PROFILE_PATH").map(|path| Profile {
            path: PathBuf::from(path),
            started: Instant::now(),
            nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            calls: std::array::from_fn(|_| AtomicU64::new(0)),
        }),
    );
}

pub(crate) struct Timer(Option<(usize, Instant)>);
pub(crate) fn start(stage: usize) -> Timer {
    Timer(
        PROFILE
            .get()
            .and_then(Option::as_ref)
            .map(|_| (stage, Instant::now())),
    )
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((stage, started)) = self.0
            && let Some(Some(profile)) = PROFILE.get()
        {
            let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            profile.nanos[stage].fetch_add(elapsed, Ordering::Relaxed);
            profile.calls[stage].fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct TimedRead<R> {
    inner: R,
    stage: usize,
}
impl<R> TimedRead<R> {
    pub(crate) fn new(inner: R, stage: usize) -> Self {
        Self { inner, stage }
    }
}
impl<R: Read> Read for TimedRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let _timer = start(self.stage);
        self.inner.read(buf)
    }
}

pub(crate) fn finish() -> io::Result<()> {
    let Some(Some(profile)) = PROFILE.get() else {
        return Ok(());
    };
    let stages: Vec<_> = NAMES
        .iter()
        .enumerate()
        .map(|(i, name)| {
            json!({
                "stage": name,
                "aggregate_wall_ns": profile.nanos[i].load(Ordering::Relaxed),
                "calls": profile.calls[i].load(Ordering::Relaxed)
            })
        })
        .collect();
    let value = json!({
        "schema": "project-c.stage-profile.v1",
        "elapsed_wall_ns": profile.started.elapsed().as_nanos(),
        "stages": stages,
        "timing_model": "Aggregate inclusive wall durations; concurrent and nested stages overlap. No stage CPU attribution."
    });
    fs::write(&profile.path, serde_json::to_vec_pretty(&value)?)
}

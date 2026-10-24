#![recursion_limit = "256"]
use flate2::bufread::MultiGzDecoder;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

mod discovery;
mod dual;
mod evidence;
mod gpu;
mod h2;
mod profile;
mod resource;
mod sharding;
mod worker;
use discovery::{Discovery, DiscoveryMode};
use evidence::{EvidenceMode, EvidenceSink};
use resource::{Dimensions, Limits, OutputBudget};

const PROFILE: &str = "project-c.c1b-json.v1";
const CONTRACT: &str = "project-c.c1a.v1";
const MAX_LINE: usize = 1_048_576;
const LEAVES: [&str; 7] = [
    "mapped.unique",
    "ambiguous.match",
    "unmapped.no_match",
    "invalid.alphabet",
    "invalid.extraction",
    "invalid.N",
    "invalid.quality",
];
type Result<T> = std::result::Result<T, String>;

// Available only in test-profile binaries; release builds contain no fault hook.
fn test_fault(point: &str) -> Result<()> {
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT")
        .is_ok_and(|value| value.split(',').any(|selected| selected == point))
    {
        return Err(format!("test_fault.{point}"));
    }
    let _ = point;
    Ok(())
}

fn bounded_line<R: BufRead>(reader: &mut R, fault: &str) -> Result<Option<Vec<u8>>> {
    bounded_line_limit(reader, fault, MAX_LINE)
}

fn bounded_line_limit<R: BufRead>(
    reader: &mut R,
    fault: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    bounded_line_limit_into(reader, fault, max_bytes, Vec::new())
}

fn bounded_line_limit_into<R: BufRead>(
    reader: &mut R,
    fault: &str,
    max_bytes: usize,
    mut line: Vec<u8>,
) -> Result<Option<Vec<u8>>> {
    line.clear();
    loop {
        let chunk = reader.fill_buf().map_err(|e| format!("{fault}: {e}"))?;
        if chunk.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            break;
        }
        let n = chunk
            .iter()
            .position(|&b| b == b'\n')
            .map_or(chunk.len(), |p| p + 1);
        if line
            .len()
            .checked_add(n)
            .is_none_or(|x| x > max_bytes.min(MAX_LINE) + 2)
        {
            return Err(format!("{fault}.line_limit"));
        }
        line.extend_from_slice(&chunk[..n]);
        reader.consume(n);
        if line.last() == Some(&b'\n') {
            break;
        }
    }
    Ok(Some(line))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExecutionProfile {
    Standard,
    CryptographicAudited,
}
impl ExecutionProfile {
    fn parse(values: &HashMap<String, String>) -> Result<Self> {
        match values
            .get("--execution-profile")
            .map(String::as_str)
            .unwrap_or("standard")
        {
            "standard" => Ok(Self::Standard),
            "cryptographic-audited" => Ok(Self::CryptographicAudited),
            _ => Err("preflight.invalid_execution_profile".into()),
        }
    }
    fn audited(self) -> bool {
        self == Self::CryptographicAudited
    }
    fn name(self) -> &'static str {
        if self.audited() {
            "cryptographic-audited"
        } else {
            "standard"
        }
    }
}

struct Config {
    no_gpu: bool,
    shards: usize,
    execution_profile: ExecutionProfile,
    sample: String,
    fastq: PathBuf,
    guides: PathBuf,
    geometry: Geometry,
    orientation: Orientation,
    radius: u8,
    min_phred: Option<u8>,
    discovery_mode: DiscoveryMode,
    evidence: EvidenceMode,
    workers: WorkerConfig,
    limits: Limits,
    output: PathBuf,
}
#[derive(Clone, Copy, Debug)]
struct WorkerConfig {
    count: usize,
    batch_records: usize,
    work_queue_capacity: usize,
    result_queue_capacity: usize,
}
impl WorkerConfig {
    fn parse(values: &HashMap<String, String>, evidence: &EvidenceMode) -> Result<Self> {
        let get = |flag: &str, default: usize| -> Result<usize> {
            values.get(flag).map_or(Ok(default), |s| {
                s.parse::<usize>()
                    .map_err(|_| format!("preflight.invalid_worker_setting: {flag}"))
            })
        };
        let count = get("--workers", 1)?;
        let batch_records = get("--batch-records", 1)?;
        let work_queue_capacity = get("--work-queue-capacity", 2)?;
        let result_queue_capacity = get("--result-queue-capacity", 1)?;
        if ![1, 2, 4].contains(&count)
            || batch_records == 0
            || batch_records > 1024
            || work_queue_capacity == 0
            || work_queue_capacity > 64
            || result_queue_capacity == 0
            || result_queue_capacity > count
        {
            return Err("preflight.invalid_worker_setting".into());
        }
        if count > 1 && !matches!(evidence, EvidenceMode::Off) {
            return Err("preflight.parallel_evidence_unsupported".into());
        }
        Ok(Self {
            count,
            batch_records,
            work_queue_capacity,
            result_queue_capacity,
        })
    }
}
enum Geometry {
    Fixed {
        start: usize,
    },
    Anchor {
        sequence: Vec<u8>,
        search_start: usize,
        search_end: usize,
        displacement: isize,
    },
}
#[derive(Clone, Copy)]
enum Orientation {
    Forward,
    ReverseComplement,
}
struct Guide {
    id: String,
    sequence: String,
    count: u64,
}
const FASTQ_FORWARD_BATCHES: usize = 4;
const FASTQ_BATCH_RECORDS: usize = 512;
const FASTQ_RECYCLE_RECORDS: usize = 4096;

struct Fastq {
    reader: Option<Box<dyn BufRead + Send>>,
    receiver: Option<mpsc::Receiver<Result<FastqMessage>>>,
    pending: std::vec::IntoIter<FastqRecord>,
    recycle_sender: Option<mpsc::SyncSender<FastqRecord>>,
    worker: Option<thread::JoinHandle<()>>,
    decoded_bytes: u64,
    digest: Option<Sha256>,
    ordinal: u64,
    limits: Limits,
    recycled: Option<FastqRecord>,
    separator: Vec<u8>,
}

pub 

enum FastqMessage {
    Batch(Vec<FastqRecord>),
    Eof {
        decoded_bytes: u64,
        ordinal: u64,
        digest: Option<Sha256>,
        separator: Vec<u8>,
    },
}

struct FastqWorker {
    reader: Box<dyn BufRead + Send>,
    decoded_bytes: u64,
    digest: Option<Sha256>,
    ordinal: u64,
    limits: Limits,
    recycled: Option<FastqRecord>,
    separator: Vec<u8>,
    recycle_receiver: mpsc::Receiver<FastqRecord>,
    sender: mpsc::SyncSender<Result<FastqMessage>>,
    batch_records: usize,
}

impl FastqWorker {
    fn line(&mut self, max_bytes: usize, storage: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let _timer = profile::start(profile::FASTQ_LINE_FRAMING);
        let Some(mut line) =
            bounded_line_limit_into(&mut self.reader, "input", max_bytes, storage)?
        else {
            return Ok(None);
        };
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(u64::try_from(line.len()).map_err(|_| "arithmetic.overflow")?)
            .ok_or_else(|| "arithmetic.overflow".to_string())?;
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        if line.len() > max_bytes {
            return Err("input.line_limit".into());
        }
        Ok(Some(line))
    }
    
    fn record_inner(&mut self) -> Result<Option<FastqRecord>> {
        let _profile = profile::start(profile::PARSE);
        let cached = self.recycled.take().or_else(|| self.recycle_receiver.try_recv().ok());
        let (header, sequence, quality) = cached
            .map(|r| (r.header, r.sequence, r.quality))
            .unwrap_or_default();
        let separator = std::mem::take(&mut self.separator);
        let Some(header) = self.line(self.limits.max_header_bytes, header)? else {
            return Ok(None);
        };
        let sequence = self
            .line(self.limits.max_record_bytes, sequence)?
            .ok_or("input.incomplete_record")?;
        let separator = self
            .line(self.limits.max_record_bytes, separator)?
            .ok_or("input.incomplete_record")?;
        let quality = self
            .line(self.limits.max_record_bytes, quality)?
            .ok_or("input.incomplete_record")?;
        {
            let _timer = profile::start(profile::FASTQ_SEQUENCE_VALIDATE);
            let record_bytes = [&header, &sequence, &separator, &quality]
                .into_iter()
                .try_fold(0_usize, |sum, part| {
                    sum.checked_add(part.len()).ok_or("arithmetic.overflow".to_string())
                })?;
            if record_bytes > self.limits.max_record_bytes {
                return Err("input.record_limit".into());
            }
        }
        {
            let _timer = profile::start(profile::FASTQ_HEADER_VALIDATE);
            if header.first() != Some(&b'@')
                || header[1..]
                    .split(|b| b.is_ascii_whitespace())
                    .next()
                    .is_none_or(|token| token.is_empty())
            {
                return Err("input.invalid_header".into());
            }
        }
        {
            let _timer = profile::start(profile::FASTQ_PLUS_LINE_VALIDATE);
            if separator.first() != Some(&b'+') {
                return Err("input.invalid_separator".into());
            }
        }
        {
            let _timer = profile::start(profile::FASTQ_QUALITY_VALIDATE);
            if sequence.len() != quality.len() {
                return Err("input.quality_length".into());
            }
            if quality.iter().any(|&b| !(b'!'..=b'~').contains(&b)) {
                return Err("input.quality_encoding".into());
            }
        }
        let _assembly = profile::start(profile::FASTQ_RECORD_ASSEMBLY);
        self.ordinal = self.ordinal.checked_add(1).ok_or("arithmetic.overflow")?;
        if let Some(digest) = self.digest.as_mut() {
            for part in [&header, &sequence, &separator, &quality] {
                digest.update(
                    u64::try_from(part.len())
                        .map_err(|_| "arithmetic.overflow")?
                        .to_be_bytes(),
                );
                digest.update(part);
            }
        }
        self.separator = separator;
        Ok(Some(FastqRecord {
            header,
            sequence,
            quality,
        }))
    }
    
    fn run(mut self) {
        // Keep transfer overhead per batch while preserving the record/error order.
        // Four queued batches plus producer/consumer batches hold at most 3,072
        // records; the queued record bound remains 2,048 per input stream.
        let mut batch = Vec::with_capacity(self.batch_records);
        loop {
            match self.record_inner() {
                Ok(Some(record)) => {
                    batch.push(record);
                    if batch.len() == self.batch_records {
                        if self.sender.send(Ok(FastqMessage::Batch(batch))).is_err() {
                            break;
                        }
                        batch = Vec::with_capacity(self.batch_records);
                    }
                }
                Ok(None) => {
                    if !batch.is_empty()
                        && self.sender.send(Ok(FastqMessage::Batch(batch))).is_err()
                    {
                        break;
                    }
                    let _ = self.sender.send(Ok(FastqMessage::Eof {
                        decoded_bytes: self.decoded_bytes,
                        ordinal: self.ordinal,
                        digest: self.digest.take(),
                        separator: std::mem::take(&mut self.separator),
                    }));
                    break;
                }
                Err(e) => {
                    // Valid records preceding a parser fault must remain visible
                    // before that fault, including an incomplete last batch.
                    if !batch.is_empty()
                        && self.sender.send(Ok(FastqMessage::Batch(batch))).is_err()
                    {
                        break;
                    }
                    let _ = self.sender.send(Err(e));
                    break;
                }
            }
        }
    }
}

struct FastqRecord {
    header: Vec<u8>,
    sequence: Vec<u8>,
    quality: Vec<u8>,
}

#[cfg(test)]
mod allocation_reuse_tests {
    use super::*;
    #[test]
    fn bounded_prefetch_recycles_fields_and_joins_after_eof_or_fault() {
        for queue in [1, 2, 4096] {
            for malformed in [false, true] {
                let limits = Limits {
                    max_record_bytes: 1024, max_header_bytes: 128,
                    memory_budget_bytes: 536_870_912, output_budget_bytes: 1_073_741_824,
                };
                let fields = [
                    (vec![b'@';120],vec![b'A';400],vec![b'+';80],vec![b'I';400]),
                    (b"@short".to_vec(),b"TG".to_vec(),b"+".to_vec(),b"!!".to_vec()),
                    (b"@empty".to_vec(),vec![],b"+comment".to_vec(),vec![]),
                ];
                let mut bytes=Vec::new();
                for _ in 0..64 {
                    for (h,s,p,q) in &fields {
                        for part in [h,s,p,q] { bytes.extend(part);bytes.push(b'\n'); }
                    }
                }
                if malformed { bytes.extend(b"@bad\nA\n+\nII\n"); }
                let mut reader=Fastq {
                    reader:Some(Box::new(BufReader::with_capacity(3,Cursor::new(bytes)))),
                    receiver:None,pending:Vec::new().into_iter(),recycle_sender:None,worker:None,decoded_bytes:0,
                    digest:Some(Sha256::new()),ordinal:0,limits,recycled:None,separator:Vec::new(),
                };
                reader.spawn_worker_with_geometry(FASTQ_BATCH_RECORDS, queue);
                for _ in 0..64 {
                    for (h,s,_,q) in &fields {
                        let record=reader.record().unwrap().unwrap();
                        assert_eq!(&record.header,h);assert_eq!(&record.sequence,s);
                        assert_eq!(&record.quality,q);reader.recycle(record);
                    }
                }
                if malformed { assert_eq!(reader.record().err().unwrap(),"input.quality_length"); }
                else { assert!(reader.record().unwrap().is_none());assert!(reader.decoded_bytes>0); }
                assert!(reader.worker.is_none());
            }
        }
    }

    use std::io::Cursor;

    #[test]
    fn threaded_batches_preserve_prefix_fault_and_eof_digest() {
        for (batch, count) in [1, 7, 512].into_iter().flat_map(|b| [0, 1, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025].map(|n| (b, n))) {
            for malformed in [false, true] {
                let mut bytes = Vec::new();
                for ordinal in 0..count {
                    bytes.extend(format!("@r{ordinal}\nACGT\n+\nIIII\n").bytes());
                }
                if malformed {
                    bytes.extend(b"@bad\nACGT\nwrong\nIIII\n");
                }
                let make = || Fastq {
                    reader: Some(Box::new(BufReader::new(Cursor::new(bytes.clone())))),
                    receiver: None,
                    pending: Vec::new().into_iter(),
                    recycle_sender: None,
                    worker: None,
                    decoded_bytes: 0,
                    digest: Some(Sha256::new()),
                    ordinal: 0,
                    limits: Limits {
                        max_record_bytes: 1024,
                        max_header_bytes: 128,
                        memory_budget_bytes: 536_870_912,
                        output_budget_bytes: 1_073_741_824,
                    },
                    recycled: None,
                    separator: Vec::new(),
                };
                let mut serial = make();
                let mut threaded = make();
                threaded.spawn_worker_with_geometry(batch, 1);
                for _ in 0..count {
                    let a = serial.record().unwrap().unwrap();
                    let b = threaded.record().unwrap().unwrap();
                    assert_eq!(a.header, b.header);
                    assert_eq!(a.sequence, b.sequence);
                    assert_eq!(a.quality, b.quality);
                }
                assert_eq!(threaded.ordinal, count);
                if malformed {
                    assert_eq!(serial.record().err(), Some("input.invalid_separator".into()));
                    assert_eq!(threaded.record().err(), Some("input.invalid_separator".into()));
                    assert_eq!(threaded.ordinal, count);
                } else {
                    assert!(serial.record().unwrap().is_none());
                    assert!(threaded.record().unwrap().is_none());
                    assert!(threaded.record().unwrap().is_none());
                    assert!(threaded.receiver.is_none() && threaded.pending.len() == 0);
                    assert_eq!(threaded.decoded_bytes, serial.decoded_bytes);
                    assert_eq!(threaded.digest.clone().unwrap().finalize(), serial.digest.clone().unwrap().finalize());
                }
            }
        }
    }

    #[test]
    fn retained_field_capacity_envelope_covers_independent_line_cap_and_crlf_reuse() {
        let limits = Limits { max_record_bytes:4_194_304, max_header_bytes:MAX_LINE,
            memory_budget_bytes:536_870_912, output_budget_bytes:1_073_741_824 };
        let mut bytes = Vec::new();
        for (header, seq) in [(MAX_LINE,4),(8,MAX_LINE),(8,0)] {
            bytes.push(b'@'); bytes.extend(vec![b'h';header-1]); bytes.extend(b"\r\n");
            bytes.extend(vec![b'A';seq]); bytes.extend(b"\r\n+long separator\r\n");
            bytes.extend(vec![b'I';seq]); bytes.extend(b"\r\n");
        }
        let mut reader = Fastq { reader:Some(Box::new(BufReader::with_capacity(8192,Cursor::new(bytes)))),
            receiver:None,pending:Vec::new().into_iter(),recycle_sender:None,worker:None,
            decoded_bytes:0,digest:Some(Sha256::new()),ordinal:0,limits,recycled:None,separator:Vec::new() };
        for _ in 0..3 {
            let record = reader.record().unwrap().unwrap();
            let retained = record.header.capacity() + record.sequence.capacity() + record.quality.capacity()
                + std::mem::size_of::<FastqRecord>();
            let bound = 4 * (limits.max_record_bytes / 2).min(MAX_LINE+2) + 2 * limits.max_header_bytes + 160;
            assert!(retained <= bound, "retained {retained} > {bound}");
            reader.recycle(record);
        }
        assert!(reader.record().unwrap().is_none());
    }

    #[test]
    fn batch_pending_cancellation_and_full_recycler_terminate_and_release_storage() {
        use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
        struct Observed {
            bytes: Cursor<Vec<u8>>, consumed: Arc<AtomicUsize>, dropped: Arc<AtomicBool>,
        }
        impl std::io::Read for Observed {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> { self.bytes.read(out) }
        }
        impl BufRead for Observed {
            fn fill_buf(&mut self) -> std::io::Result<&[u8]> { self.bytes.fill_buf() }
            fn consume(&mut self, n: usize) { self.consumed.fetch_add(n, Ordering::SeqCst); self.bytes.consume(n); }
        }
        impl Drop for Observed { fn drop(&mut self) { self.dropped.store(true, Ordering::SeqCst); } }
        for batch in [1, 7, 512] {
            let consumed = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicBool::new(false));
            let line = b"@record\nACGT\n+\nIIII\n";
            let mut reader = Fastq {
                reader: Some(Box::new(Observed { bytes: Cursor::new(line.repeat(8192)), consumed: consumed.clone(), dropped: dropped.clone() })),
                receiver: None, pending: Vec::new().into_iter(), recycle_sender: None, worker: None,
                decoded_bytes: 0, digest: Some(Sha256::new()), ordinal: 0,
                limits: Limits { max_record_bytes:1024, max_header_bytes:128, memory_budget_bytes:536_870_912, output_budget_bytes:1_073_741_824 },
                recycled:None, separator:Vec::new(),
            };
            reader.spawn_worker_with_geometry(batch, 1);
            let record = reader.record().unwrap().unwrap();
            assert_eq!(reader.pending.len(), batch - 1);
            reader.recycle(record);
            // One pending batch, four queued batches and one producer batch.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while consumed.load(Ordering::SeqCst) < 6 * batch * line.len() {
                assert!(std::time::Instant::now() < deadline);
                thread::yield_now();
            }
            // Deterministically full recycler: full/disconnected returns never block.
            let (tx, rx) = mpsc::sync_channel(1);
            let make = || FastqRecord { header:vec![b'@';128], sequence:vec![b'A';400], quality:vec![b'I';400] };
            tx.try_send(make()).unwrap_or_else(|_| panic!("empty recycler"));
            let previous = reader.recycle_sender.replace(tx);
            drop(previous);
            reader.recycle(make()); // full
            drop(rx);
            reader.recycle(make()); // disconnected
            reader.stop_worker(); // must disconnect forward channel before joining
            assert!(dropped.load(Ordering::SeqCst));
            assert!(reader.worker.is_none() && reader.receiver.is_none() && reader.recycle_sender.is_none());
            assert_eq!(reader.pending.len(), 0);
            assert!(reader.record().unwrap().is_none());
        }
    }

    #[test]
    fn reused_fields_preserve_short_records_digest_and_fault_prefix() {
        for capacity in [1, 3, 8192] {
            let limits = Limits {
                max_record_bytes: 1024,
                max_header_bytes: 128,
                memory_budget_bytes: 536_870_912,
                output_budget_bytes: 1_073_741_824,
            };
            let fields = [
                (
                    vec![b'@'; 120],
                    vec![b'A'; 400],
                    vec![b'+'; 80],
                    vec![b'I'; 400],
                ),
                (
                    b"@short".to_vec(),
                    b"TG".to_vec(),
                    b"+".to_vec(),
                    b"!~".to_vec(),
                ),
                (b"@empty".to_vec(), vec![], b"+comment".to_vec(), vec![]),
            ];
            let mut bytes = Vec::new();
            let mut expected = Sha256::new();
            for (i, (h, s, p, q)) in fields.iter().enumerate() {
                for part in [h, s, p, q] {
                    bytes.extend(part);
                    bytes.extend(if i == 1 {
                        b"\r\n".as_slice()
                    } else {
                        b"\n".as_slice()
                    });
                    expected.update((part.len() as u64).to_be_bytes());
                    expected.update(part);
                }
            }
            let valid_bytes = bytes.len() as u64;
            bytes.extend(b"@bad\nA\n+\nII\n");
            let mut reader = Fastq {
                reader: Some(Box::new(BufReader::with_capacity(capacity, Cursor::new(bytes)))),
                receiver: None,
                pending: Vec::new().into_iter(),
                recycle_sender: None,
                worker: None,
                decoded_bytes: 0,
                digest: Some(Sha256::new()),
                ordinal: 0,
                limits,
                recycled: None,
                separator: Vec::new(),
            };
            for (h, s, _, q) in &fields {
                let record = reader.record().unwrap().unwrap();
                assert_eq!(&record.header, h);
                assert_eq!(&record.sequence, s);
                assert_eq!(&record.quality, q);
                assert!(record.header.capacity() <= 2 * (limits.max_header_bytes + 2));
                reader.recycle(record);
            }
            assert_eq!(reader.decoded_bytes, valid_bytes);
            assert_eq!(reader.ordinal, 3);
            assert_eq!(
                reader.digest.clone().unwrap().finalize(),
                expected.finalize()
            );
            assert_eq!(reader.record().err().unwrap(), "input.quality_length");
            assert_eq!(reader.ordinal, 3);
            assert_eq!(reader.decoded_bytes, valid_bytes + 12);
        }
    }
}

impl Drop for Fastq {
    fn drop(&mut self) {
        self.stop_worker();
    }
}

impl Fastq {
    fn spawn_worker(&mut self) {
        self.spawn_worker_with_geometry(FASTQ_BATCH_RECORDS, FASTQ_RECYCLE_RECORDS);
    }

    // Capacity is in recycled records. Forward messages remain batches of at most
    // 512 records, with smaller admitted physical batches under tight budgets.
    fn spawn_worker_with_geometry(&mut self, batch_records: usize, capacity: usize) {
        if self.worker.is_some() || self.reader.is_none() {
            return;
        }
        assert!((1..=FASTQ_RECYCLE_RECORDS).contains(&capacity));
        assert!((1..=FASTQ_BATCH_RECORDS).contains(&batch_records));
        let (tx, rx) = mpsc::sync_channel(FASTQ_FORWARD_BATCHES);
        let (recycle_tx, recycle_rx) = mpsc::sync_channel(capacity);
        let worker = FastqWorker {
            reader: self.reader.take().unwrap(),
            decoded_bytes: self.decoded_bytes,
            digest: self.digest.take(),
            ordinal: self.ordinal,
            limits: self.limits,
            recycled: self.recycled.take(),
            separator: std::mem::take(&mut self.separator),
            recycle_receiver: recycle_rx,
            sender: tx,
            batch_records,
        };
        self.worker = Some(thread::spawn(move || worker.run()));
        self.receiver = Some(rx);
        self.recycle_sender = Some(recycle_tx);
    }

    // Disconnect before joining: a producer may be blocked sending a full batch.
    // Clearing IntoIter also releases consumed positions' retained Vec storage.
    fn stop_worker(&mut self) {
        self.receiver.take();
        self.pending = Vec::new().into_iter();
        self.recycle_sender.take();
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }

    fn open(path: &Path, limits: Limits, execution_profile: ExecutionProfile) -> Result<Self> {
        let file = File::open(path).map_err(|e| format!("input.open: {}", e))?;
        let base = BufReader::new(profile::TimedRead::new(file, profile::INPUT));
        let reader: Box<dyn BufRead + Send> = if path.extension().is_some_and(|s| s == "gz") {
            Box::new(BufReader::new(profile::TimedRead::new(
                MultiGzDecoder::new(base),
                profile::GZIP,
            )))
        } else {
            Box::new(base)
        };
        Ok(Self {
            reader: Some(reader),
            receiver: None,
            pending: Vec::new().into_iter(),
            recycle_sender: None,
            worker: None,
            decoded_bytes: 0,
            digest: execution_profile.audited().then(Sha256::new),
            ordinal: 0,
            limits,
            recycled: None,
            separator: Vec::new(),
        })
    }
    
    fn recycle(&mut self, record: FastqRecord) {
        if let Some(sender) = &self.recycle_sender {
            let _ = sender.try_send(record);
        } else {
            self.recycled = Some(record);
        }
    }
    
    fn line(&mut self, max_bytes: usize, storage: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let _timer = profile::start(profile::FASTQ_LINE_FRAMING);
        let Some(mut line) =
            bounded_line_limit_into(self.reader.as_mut().unwrap(), "input", max_bytes, storage)?
        else {
            return Ok(None);
        };
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(u64::try_from(line.len()).map_err(|_| "arithmetic.overflow")?)
            .ok_or("arithmetic.overflow".to_string())?;
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        if line.len() > max_bytes {
            return Err("input.line_limit".into());
        }
        Ok(Some(line))
    }
    
    fn record(&mut self) -> Result<Option<FastqRecord>> {
        if let Some(rx) = &self.receiver {
            if let Some(record) = self.pending.next() {
                self.ordinal = self.ordinal.checked_add(1).ok_or_else(|| "arithmetic.overflow".to_string())?;
                return Ok(Some(record));
            }
            match rx.recv().map_err(|_| String::from("input.thread_panicked"))? {
                Ok(FastqMessage::Batch(batch)) => {
                    self.pending = batch.into_iter();
                    let record = self.pending.next().ok_or("input.empty_batch")?;
                    self.ordinal = self.ordinal.checked_add(1).ok_or_else(|| "arithmetic.overflow".to_string())?;
                    Ok(Some(record))
                }
                Ok(FastqMessage::Eof { decoded_bytes, ordinal, digest, separator }) => {
                    self.decoded_bytes = decoded_bytes;
                    self.ordinal = ordinal;
                    self.digest = digest;
                    self.separator = separator;
                    self.stop_worker();
                    Ok(None)
                }
                Err(e) => {
                    self.stop_worker();
                    Err(e)
                }
            }
        } else {
            // EOF/cancellation has already joined and released the producer.
            if self.reader.is_none() { return Ok(None); }
            let _profile = profile::start(profile::PARSE);
            let cached = self.recycled.take();
            let (header, sequence, quality) = cached
                .map(|r| (r.header, r.sequence, r.quality))
                .unwrap_or_default();
            let separator = std::mem::take(&mut self.separator);
            let Some(header) = self.line(self.limits.max_header_bytes, header)? else {
                return Ok(None);
            };
            let sequence = self
                .line(self.limits.max_record_bytes, sequence)?
                .ok_or("input.incomplete_record".to_string())?;
            let separator = self
                .line(self.limits.max_record_bytes, separator)?
                .ok_or("input.incomplete_record".to_string())?;
            let quality = self
                .line(self.limits.max_record_bytes, quality)?
                .ok_or("input.incomplete_record".to_string())?;
            {
                let _timer = profile::start(profile::FASTQ_SEQUENCE_VALIDATE);
                let record_bytes = [&header, &sequence, &separator, &quality]
                    .into_iter()
                    .try_fold(0_usize, |sum, part| {
                        sum.checked_add(part.len()).ok_or("arithmetic.overflow".to_string())
                    })?;
                if record_bytes > self.limits.max_record_bytes {
                    return Err("input.record_limit".into());
                }
            }
            {
                let _timer = profile::start(profile::FASTQ_HEADER_VALIDATE);
                if header.first() != Some(&b'@')
                    || header[1..]
                        .split(|b| b.is_ascii_whitespace())
                        .next()
                        .is_none_or(|token| token.is_empty())
                {
                    return Err("input.invalid_header".into());
                }
            }
            {
                let _timer = profile::start(profile::FASTQ_PLUS_LINE_VALIDATE);
                if separator.first() != Some(&b'+') {
                    return Err("input.invalid_separator".into());
                }
            }
            {
                let _timer = profile::start(profile::FASTQ_QUALITY_VALIDATE);
                if sequence.len() != quality.len() {
                    return Err("input.quality_length".into());
                }
                if quality.iter().any(|&b| !(b'!'..=b'~').contains(&b)) {
                    return Err("input.quality_encoding".into());
                }
            }
            let _assembly = profile::start(profile::FASTQ_RECORD_ASSEMBLY);
            self.ordinal = self.ordinal.checked_add(1).ok_or("arithmetic.overflow".to_string())?;
            if let Some(digest) = self.digest.as_mut() {
                for part in [&header, &sequence, &separator, &quality] {
                    digest.update(
                        u64::try_from(part.len())
                            .map_err(|_| "arithmetic.overflow".to_string())?
                            .to_be_bytes(),
                    );
                    digest.update(part);
                }
            }
            self.separator = separator;
            Ok(Some(FastqRecord {
                header,
                sequence,
                quality,
            }))
        }
    }
}


// CPU is the qualified default. This switch only permits the existing Metal
// eligibility checks; ineligible/unavailable requests retain CPU fallback.
fn metal_policy(values: &HashMap<String, String>) -> Result<bool> {
    if values.contains_key("--experimental-metal") && values.contains_key("--no-gpu") {
        return Err("preflight.conflicting_options: --experimental-metal --no-gpu".into());
    }
    Ok(!values.contains_key("--experimental-metal"))
}

const METAL_HELP: &str = "crisprclamp count-first engine
CPU is the default backend.
--experimental-metal  Explicit opt-in to the experimental Metal backend.
                      Eligible single-guide fixed-window 20mers, radius 0,
                      workers >1, no sharding, unified-memory macOS device,
                      and the existing estimated-read crossover are required.
                      Ineligible or unavailable Metal falls back to CPU.
                      Full-scale acceleration and qualification are not claimed.
--no-gpu              Retained CPU-only switch; conflicts with --experimental-metal.
--help, -h            Show this help.
Dual/concordance workflows use CPU, including explicit Metal requests.
";

fn parse() -> Result<Config> {
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
        if ![
            "--experimental-shards",
            "--execution-profile",
            "--sample",
            "--fastq",
            "--guides",
            "--start",
            "--anchor",
            "--search-start",
            "--search-end",
            "--displacement",
            "--orientation",
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
            "--workers",
            "--batch-records",
            "--work-queue-capacity",
            "--result-queue-capacity",
            "--output",
        ]
        .contains(&flag.as_str())
        {
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
    let get = |key: &str| {
        values
            .get(key)
            .cloned()
            .ok_or_else(|| format!("preflight.missing_option: {key}"))
    };
    let no_gpu = metal_policy(&values)?;
    let execution_profile = ExecutionProfile::parse(&values)?;
    let sample = get("--sample")?;
    if sample.is_empty() {
        return Err("preflight.empty_sample_id".into());
    }
    let fastq = PathBuf::from(get("--fastq")?);
    let guides = PathBuf::from(get("--guides")?);
    let geometry = if let Some(anchor) = values.get("--anchor") {
        if values.contains_key("--start") {
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
        let search_start = get("--search-start")?
            .parse::<usize>()
            .map_err(|_| "preflight.invalid_search_start")?;
        let search_end = get("--search-end")?
            .parse::<usize>()
            .map_err(|_| "preflight.invalid_search_end")?;
        let displacement = get("--displacement")?
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
        if values.contains_key("--search-start")
            || values.contains_key("--search-end")
            || values.contains_key("--displacement")
        {
            return Err("preflight.incomplete_anchor_geometry".into());
        }
        Geometry::Fixed {
            start: get("--start")?
                .parse::<usize>()
                .map_err(|_| "preflight.invalid_start")?,
        }
    };
    let orientation = match values
        .get("--orientation")
        .map(String::as_str)
        .unwrap_or("forward")
    {
        "forward" => Orientation::Forward,
        "reverse_complement" => Orientation::ReverseComplement,
        _ => return Err("preflight.invalid_orientation".into()),
    };
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
    let limits = Limits::parse(&values)?;
    let output = PathBuf::from(get("--output")?);
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
    let shards = values
        .get("--experimental-shards")
        .map(|v| v.parse::<usize>().map_err(|_| "preflight.invalid_shards"))
        .transpose()?
        .unwrap_or(0);
    if values.contains_key("--experimental-shards") && shards == 0 {
        return Err("preflight.invalid_shards".into());
    }
    sharding::scope(
        shards,
        execution_profile,
        &geometry,
        radius,
        &discovery_mode,
        &evidence,
        workers,
        &fastq,
    )?;
    Ok(Config {
        no_gpu,
        shards,
        execution_profile,
        sample,
        fastq,
        guides,
        geometry,
        orientation,
        radius,
        min_phred,
        discovery_mode,
        evidence,
        workers,
        limits,
        output,
    })
}

fn load_guides(path: &Path) -> Result<Vec<Guide>> {
    let file = File::open(path).map_err(|e| format!("preflight.guide_open: {e}"))?;
    let mut reader = BufReader::new(file);
    let mut guides = Vec::new();
    let mut ids = HashSet::new();
    let mut sequences = HashSet::new();
    let mut length = None;
    while let Some(mut line) = bounded_line(&mut reader, "preflight.guide_read")? {
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        let line = String::from_utf8(line).map_err(|_| "preflight.guide_utf8")?;
        let (id, sequence) = line.split_once('\t').ok_or("preflight.guide_columns")?;
        if id.is_empty() || sequence.is_empty() || sequence.contains('\t') {
            return Err("preflight.guide_columns".into());
        }
        if !ids.insert(id.to_owned()) {
            return Err("preflight.duplicate_id".into());
        }
        let sequence = sequence.to_ascii_uppercase();
        if !sequence
            .bytes()
            .all(|b| matches!(b, b'A' | b'C' | b'G' | b'T'))
        {
            return Err("preflight.guide_alphabet".into());
        }
        if !sequences.insert(sequence.clone()) {
            return Err("preflight.duplicate_sequence".into());
        }
        if length.is_some_and(|l| l != sequence.len()) {
            return Err("preflight.guide_length".into());
        }
        length = Some(sequence.len());
        guides.push(Guide {
            id: id.to_owned(),
            sequence,
            count: 0,
        });
    }
    if guides.is_empty() {
        return Err("preflight.empty_guides".into());
    }
    Ok(guides)
}

fn increment(x: &mut u64) -> Result<()> {
    *x = x.checked_add(1).ok_or("arithmetic.overflow")?;
    Ok(())
}
#[derive(Clone, Copy, Debug)]
enum ExtractionFault {
    MissingAnchor,
    #[allow(dead_code)] // Reserved legacy extraction subreason in the unchanged output schema.
    MultipleAnchors,
    OutOfBounds,
}
struct Classification {
    leaf: usize,
    guide: Option<usize>,
    distance: Option<u8>,
    extraction_fault: Option<ExtractionFault>,
    window: Option<String>,
}
#[derive(Default)]
struct QcDetails {
    mapped_exact: u64,
    mapped_rescued_h1: u64,
    extraction: [u64; 3],
}
impl Classification {
    fn leaf(leaf: usize) -> Self {
        Self {
            leaf,
            guide: None,
            distance: None,
            extraction_fault: None,
            window: None,
        }
    }
    fn extraction(fault: ExtractionFault) -> Self {
        Self {
            leaf: 4,
            guide: None,
            distance: None,
            extraction_fault: Some(fault),
            window: None,
        }
    }
    fn with_window(leaf: usize, window: String) -> Self {
        Self {
            leaf,
            guide: None,
            distance: None,
            extraction_fault: None,
            window: Some(window),
        }
    }
}

fn complement(base: u8) -> u8 {
    match base {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        b'N' => b'N',
        _ => unreachable!(),
    }
}

fn window_start(
    sequence: &[u8],
    length: usize,
    start: usize,
) -> std::result::Result<usize, ExtractionFault> {
    let end = start
        .checked_add(length)
        .ok_or(ExtractionFault::OutOfBounds)?;
    if end > sequence.len() {
        return Err(ExtractionFault::OutOfBounds);
    }
    Ok(start)
}

fn classify(
    sequence: &[u8],
    quality: &[u8],
    cfg: &Config,
    guides: &[Guide],
    index: &HashMap<String, usize>,
) -> Classification {
    classify_component(
        sequence,
        quality,
        ComponentSpec {
            geometry: &cfg.geometry,
            orientation: cfg.orientation,
            radius: cfg.radius,
            min_phred: cfg.min_phred,
        },
        guides,
        index,
    )
}

#[derive(Clone, Copy)]
struct ComponentSpec<'a> {
    geometry: &'a Geometry,
    orientation: Orientation,
    radius: u8,
    min_phred: Option<u8>,
}
fn classify_component(
    sequence: &[u8],
    quality: &[u8],
    spec: ComponentSpec<'_>,
    guides: &[Guide],
    index: &HashMap<String, usize>,
) -> Classification {
    if sequence
        .iter()
        .any(|b| !matches!(b.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T' | b'N'))
    {
        return Classification::leaf(3);
    }
    let oriented: Vec<u8> = match spec.orientation {
        Orientation::Forward => sequence.iter().map(u8::to_ascii_uppercase).collect(),
        Orientation::ReverseComplement => sequence
            .iter()
            .rev()
            .map(|b| complement(b.to_ascii_uppercase()))
            .collect(),
    };
    if let Geometry::Anchor {
        sequence: anchor,
        search_start,
        search_end,
        displacement,
    } = spec.geometry
    {
        let mut found = false;
        let mut resolved: Option<Classification> = None;
        let mut ambiguous = false;
        let mut ambiguity_window = None;
        let mut fallback = Classification::extraction(ExtractionFault::OutOfBounds);
        let mut fallback_rank = 0;
        for position in *search_start..=(*search_end).min(oriented.len()) {
            if oriented.get(position..position.saturating_add(anchor.len()))
                != Some(anchor.as_slice())
            {
                continue;
            }
            found = true;
            let start = (position as i128 + anchor.len() as i128)
                .checked_add(*displacement as i128)
                .and_then(|n| usize::try_from(n).ok());
            let Some(start) = start else { continue };
            let Ok(start) = window_start(&oriented, guides[0].sequence.len(), start) else {
                continue;
            };
            let candidate = classify_window(&oriented, quality, spec, guides, index, start);
            if (candidate.guide.is_some() || candidate.leaf == 1) && ambiguity_window.is_none() {
                ambiguity_window = candidate.window.clone();
            }
            if let Some(guide) = candidate.guide {
                if resolved
                    .as_ref()
                    .is_some_and(|old| old.guide != Some(guide))
                {
                    ambiguous = true;
                }
                // Prefer exact evidence for repeated occurrences of the same guide;
                // ties retain the earliest occurrence for deterministic evidence.
                if resolved
                    .as_ref()
                    .is_none_or(|old| candidate.distance < old.distance)
                {
                    resolved = Some(candidate);
                }
            } else if candidate.leaf == 1 {
                // Preserve all radius-one ties: another occurrence resolving one
                // member cannot turn a tied candidate into a unique assignment.
                ambiguous = true;
            } else {
                // Prefer complete rule-valid windows over validation failures;
                // the precedence is independent of anchor enumeration order.
                let rank = match candidate.leaf {
                    2 => 3,
                    6 => 2,
                    5 => 1,
                    _ => unreachable!(),
                };
                if rank > fallback_rank {
                    fallback_rank = rank;
                    fallback = candidate;
                }
            }
        }
        if !found {
            return Classification::extraction(ExtractionFault::MissingAnchor);
        }
        if ambiguous {
            return Classification::with_window(
                1,
                ambiguity_window.expect("valid candidate window"),
            );
        }
        return resolved.unwrap_or(fallback);
    }
    let Geometry::Fixed { start } = spec.geometry else {
        unreachable!()
    };
    let start = match window_start(&oriented, guides[0].sequence.len(), *start) {
        Ok(start) => start,
        Err(fault) => return Classification::extraction(fault),
    };
    classify_window(&oriented, quality, spec, guides, index, start)
}

fn classify_window(
    oriented: &[u8],
    quality: &[u8],
    spec: ComponentSpec<'_>,
    guides: &[Guide],
    index: &HashMap<String, usize>,
    start: usize,
) -> Classification {
    let end = start + guides[0].sequence.len(); // checked by window_start
    let window = &oriented[start..end];
    if window.contains(&b'N') {
        return Classification::leaf(5);
    }
    if let Some(min_phred) = spec.min_phred {
        let low = match spec.orientation {
            Orientation::Forward => quality[start..end].iter().any(|&q| q - 33 < min_phred),
            Orientation::ReverseComplement => quality[quality.len() - end..quality.len() - start]
                .iter()
                .any(|&q| q - 33 < min_phred),
        };
        if low {
            return Classification::leaf(6);
        }
    }
    let normalized = String::from_utf8(window.to_vec()).expect("DNA window is ASCII");
    if let Some(&guide) = index.get(&normalized) {
        return Classification {
            leaf: 0,
            guide: Some(guide),
            distance: Some(0),
            extraction_fault: None,
            window: Some(normalized),
        };
    }
    if spec.radius == 1 {
        let mut candidate = None;
        for (i, guide) in guides.iter().enumerate() {
            let mut mismatch = 0;
            for (a, b) in window.iter().zip(guide.sequence.bytes()) {
                if *a != b {
                    mismatch += 1;
                    if mismatch > 1 {
                        break;
                    }
                }
            }
            if mismatch == 1 {
                if candidate.is_some() {
                    return Classification::with_window(1, normalized);
                }
                candidate = Some(i);
            }
        }
        if let Some(guide) = candidate {
            return Classification {
                leaf: 0,
                guide: Some(guide),
                distance: Some(1),
                extraction_fault: None,
                window: Some(normalized),
            };
        }
    }
    Classification::with_window(2, normalized)
}

fn semantic_config(cfg: &Config, guide_length: usize) -> Value {
    let discovery_json = match cfg.discovery_mode {
        DiscoveryMode::Bounded(k) => json!({"mode":"bounded","K":k}),
        DiscoveryMode::Exact(k) => json!({"mode":"exact","limit":k}),
    };
    let mut config = json!({"mode":"single","orientation":match cfg.orientation {
        Orientation::Forward => "forward", Orientation::ReverseComplement => "reverse_complement"},
        "guide_length":guide_length,"radius":cfg.radius,
        "quality":cfg.min_phred.map_or(json!("off"), |n| json!({"min_phred":n})),
        "discovery":discovery_json,"evidence":cfg.evidence.selector(),
        "resource_limits":cfg.limits.as_json()});
    let fields = config.as_object_mut().expect("config object");
    match &cfg.geometry {
        Geometry::Fixed { start } => {
            fields.insert("geometry".into(), json!("fixed"));
            fields.insert("start".into(), json!(start));
        }
        Geometry::Anchor {
            sequence,
            search_start,
            search_end,
            displacement,
        } => {
            fields.insert("geometry".into(), json!("anchor_relative"));
            fields.insert(
                "anchor".into(),
                json!(String::from_utf8(sequence.clone()).expect("ASCII anchor")),
            );
            fields.insert("search".into(), json!([search_start, search_end]));
            fields.insert("displacement".into(), json!(displacement.to_string()));
        }
    }
    config
}

// Serde emits struct fields in declaration order. Keep these fields strictly
// alphabetical to match the accepted Value/BTreeMap JSONL bytes and audit hashes.
#[derive(Serialize)]
struct RawCountsRow<'a> {
    counts: [u64; 1],
    profile_id: &'a str,
    row_id: &'a str,
    row_kind: &'a str,
    schema_id: &'a str,
}

#[derive(Serialize)]
struct GuideMetadataRow<'a> {
    annotations: BTreeMap<&'a str, &'a str>,
    gene_id_or_null: Option<&'a str>,
    guide_id: &'a str,
    guide_length: usize,
    normalized_sequence: &'a str,
    profile_id: &'a str,
    role: &'a str,
    schema_id: &'a str,
}

fn row(schema: &str, mut fields: Value) -> Value {
    let object = fields.as_object_mut().expect("object row");
    object.insert("schema_id".into(), json!(schema));
    object.insert("profile_id".into(), json!(PROFILE));
    fields
}
fn jsonl(
    path: &Path,
    rows: impl IntoIterator<Item = impl Serialize>,
    budget: &mut OutputBudget,
    science: bool,
    execution_profile: ExecutionProfile,
) -> Result<(u64, String)> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("publication.create: {e}"))?;
    let mut writer = BufWriter::new(file);
    if path
        .file_name()
        .is_some_and(|name| name == "completion.jsonl")
    {
        test_fault("completion_write")?;
    }
    let mut digest = execution_profile.audited().then(Sha256::new);
    let mut size = 0_u64;
    let mut bytes = Vec::new();
    for row in rows {
        bytes.clear();
        serde_json::to_writer(&mut bytes, &row)
            .map_err(|e| format!("serialization.json: {e}"))?;
        bytes.push(b'\n');
        budget.charge(
            u64::try_from(bytes.len()).map_err(|_| "resource.output_budget")?,
            science,
        )?;
        if science {
            test_fault("output_write")?;
        }
        writer
            .write_all(&bytes)
            .map_err(|e| format!("publication.write: {e}"))?;
        if let Some(digest) = digest.as_mut() {
            digest.update(&bytes);
        }
        size = size
            .checked_add(u64::try_from(bytes.len()).map_err(|_| "arithmetic.overflow")?)
            .ok_or("arithmetic.overflow")?;
    }
    if !execution_profile.audited() {
        test_fault("output_flush")?;
    }
    writer
        .flush()
        .map_err(|e| format!("publication.flush: {e}"))?;
    if execution_profile.audited() {
        if path
            .file_name()
            .is_some_and(|name| name == "completion.jsonl")
        {
            test_fault("completion_sync")?;
        } else {
            test_fault("output_sync")?;
        }
        writer
            .into_inner()
            .map_err(|e| format!("publication.close: {e}"))?
            .sync_all()
            .map_err(|e| format!("publication.sync: {e}"))?;
    } else {
        let file = writer
            .into_inner()
            .map_err(|e| format!("publication.close: {e}"))?;
        test_fault("output_close")?;
        drop(file);
    }
    Ok((
        size,
        digest.map_or_else(String::new, |h| format!("{:x}", h.finalize())),
    ))
}

fn jsonl_encoded(
    path: &Path,
    rows: impl IntoIterator<Item = Result<impl AsRef<[u8]>>>,
    budget: &mut OutputBudget,
    science: bool,
    execution_profile: ExecutionProfile,
) -> Result<(u64, String)> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("publication.create: {e}"))?;
    let mut writer = BufWriter::new(file);
    if path
        .file_name()
        .is_some_and(|name| name == "completion.jsonl")
    {
        test_fault("completion_write")?;
    }
    let mut digest = execution_profile.audited().then(Sha256::new);
    let mut size = 0_u64;
    for row in rows {
        let row = row?;
        let bytes = row.as_ref();
        budget.charge(
            u64::try_from(bytes.len()).map_err(|_| "resource.output_budget")?,
            science,
        )?;
        if science {
            test_fault("output_write")?;
        }
        writer
            .write_all(&bytes)
            .map_err(|e| format!("publication.write: {e}"))?;
        if let Some(digest) = digest.as_mut() {
            digest.update(&bytes);
        }
        size = size
            .checked_add(u64::try_from(bytes.len()).map_err(|_| "arithmetic.overflow")?)
            .ok_or("arithmetic.overflow")?;
    }
    if !execution_profile.audited() {
        test_fault("output_flush")?;
    }
    writer
        .flush()
        .map_err(|e| format!("publication.flush: {e}"))?;
    if execution_profile.audited() {
        if path
            .file_name()
            .is_some_and(|name| name == "completion.jsonl")
        {
            test_fault("completion_sync")?;
        } else {
            test_fault("output_sync")?;
        }
        writer
            .into_inner()
            .map_err(|e| format!("publication.close: {e}"))?
            .sync_all()
            .map_err(|e| format!("publication.sync: {e}"))?;
    } else {
        let file = writer
            .into_inner()
            .map_err(|e| format!("publication.close: {e}"))?;
        test_fault("output_close")?;
        drop(file);
    }
    Ok((
        size,
        digest.map_or_else(String::new, |h| format!("{:x}", h.finalize())),
    ))
}
fn file_hash(path: &Path) -> Result<String> {
    test_fault("physical_input_hash")?;
    let mut file = File::open(path).map_err(|e| format!("input.open: {e}"))?;
    let mut hash = Sha256::new();
    io::copy(&mut file, &mut hash).map_err(|e| format!("input.read: {e}"))?;
    Ok(format!("{:x}", hash.finalize()))
}

fn standard_execution(
    stage: &Path,
    sample: &str,
    configuration: Value,
    semantic_contract: &str,
    inputs: Vec<PathBuf>,
    workers: WorkerConfig,
    state: ExecutionState<'_>,
    budget: &mut OutputBudget,
) -> Result<()> {
    let receipt: Value = state.manifest_hash.map_or(Ok(json!({})), |v| {
        serde_json::from_str(v).map_err(|e| e.to_string())
    })?;
    let artifacts = &receipt["artifacts"];
    let mut configuration = configuration;
    if let Some(length) = receipt.get("guide_length") {
        configuration["guide_length"] = length.clone();
    }
    let mut record = json!({"schema_id":"project-c.run-metadata.v1", "execution_profile":"standard",
        "execution_id":format!("{}-{}",std::process::id(),state.started),"sample_id":sample,
        "software_build_identity":{"package":env!("CARGO_PKG_NAME"),"version":env!("CARGO_PKG_VERSION")},
        "semantic_contract_id":semantic_contract,"semantic_configuration":configuration,"inputs":inputs.iter().enumerate().map(|(i,p)|json!({"role":if i==0{"R1"}else{"R2"},"path":p.to_string_lossy(),
            "encoding":if p.extension().is_some_and(|x|x=="gz"){"gzip"}else{"plain"},
            "bytes":fs::metadata(p).map_or(json!({"unavailable":"metadata_error"}),|m|json!(m.len()))})).collect::<Vec<_>>(),
        "worker_count":workers.count,"batch_records":workers.batch_records,"work_queue_capacity":workers.work_queue_capacity,
        "result_queue_capacity":workers.result_queue_capacity,"scheduling":if workers.count==1{"serial"}else{"bounded-worker-local"},
        "start_unix_ms":state.started,"end_unix_ms":unix_ms(),"elapsed_ms":state.elapsed_ms.to_string(),
        "decoded_bytes":state.decoded_bytes,"processed_prefix_records":state.processed,"output_bytes":budget.used(),
        "artifacts":artifacts,"outcome":state.outcome,"fault":state.fault});
    if let Some(sharding) = receipt.get("experimental_sharding") {
        record["experimental_sharding"] = sharding.clone();
        record["worker_count"] = sharding["effective_shards"].clone();
        record["scheduling"] = json!("strict-index-sharded");
    }
    test_fault("metadata_write")?;
    jsonl(
        &stage.join("run-metadata.jsonl"),
        &[record],
        budget,
        false,
        ExecutionProfile::Standard,
    )?;
    test_fault("metadata_close")?;
    Ok(())
}

// symlink is the contract's equivalent exclusive atomic publication primitive:
// it exposes the entire closed sibling directory and cannot replace any entry.
// The stage remains the owned backing directory after successful publication.
fn publish_standard(stage: &Path, output: &Path, budget: &mut OutputBudget) -> Result<()> {
    test_fault("before_publication")?;
    // Finish optional diagnostics before the acceptance point, never after it.
    profile::finish().map_err(|e| format!("profile.write: {e}"))?;
    let metadata: Value = serde_json::from_slice(
        &fs::read(stage.join("run-metadata.jsonl")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let completion = json!({"schema_id":"project-c.standard-completion.v1","status":"COMPLETE",
        "execution_profile":"standard","execution_id":metadata["execution_id"],
        "metadata_file":"run-metadata.jsonl","artifacts":metadata["artifacts"],"scientific_success":true,
        "publication_profile_id":"project-c.local-close-symlink.v1",
        "assurance":{"cryptographic_input_identity":false,"cryptographic_artifact_identity":false,"crash_durability":false,
            "strict_science":true,"resource_admission":true,"atomic_visibility":true}});
    test_fault("before_completion")?;
    jsonl(
        &stage.join("completion.jsonl"),
        &[completion],
        budget,
        false,
        ExecutionProfile::Standard,
    )?;
    test_fault("completion_close")?;
    test_fault("atomic_link")?;
    let target = stage.file_name().ok_or("publication.stage_name")?;
    symlink(target, output).map_err(|e| format!("publication.atomic_link: {e}"))?;
    Ok(())
}

fn cleanup_standard(stage: &Path, primary: String) -> String {
    let mut cleanup = Vec::new();
    if let Err(e) = fs::remove_file(stage.join("completion.jsonl")) {
        if e.kind() != io::ErrorKind::NotFound {
            cleanup.push(format!("publication.cleanup_completion: {e}"));
        }
    }
    if let Err(e) = test_fault("cleanup") {
        cleanup.push(e);
    } else if let Err(e) = fs::remove_dir_all(stage) {
        cleanup.push(format!("publication.cleanup_stage: {e}"));
    }
    if cleanup.is_empty() {
        primary
    } else {
        format!("{primary}; cleanup: {}", cleanup.join("; "))
    }
}

struct ExecutionState<'a> {
    outcome: &'a str,
    fault: Option<&'a str>,
    manifest_hash: Option<&'a str>,
    started: u64,
    elapsed_ms: u128,
    processed: u64,
    decoded_bytes: u64,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

fn execution(
    stage: &Path,
    cfg: &Config,
    state: ExecutionState<'_>,
    budget: &mut OutputBudget,
) -> Result<()> {
    if !cfg.execution_profile.audited() {
        return standard_execution(
            stage,
            &cfg.sample,
            semantic_config(cfg, 0),
            CONTRACT,
            vec![cfg.fastq.clone()],
            cfg.workers,
            state,
            budget,
        );
    }
    let record = row(
        "project-c.execution.v1",
        json!({
            "execution_profile":cfg.execution_profile.name(),
            "scientific_manifest_sha256": state.manifest_hash.map_or(json!({"unavailable":"run_incomplete"}), |h| json!(h)),
            "execution_id": format!("{}-{}", std::process::id(), state.started),
            "host_os_cpu_storage": {"unavailable":"not_collected_in_vertical_slice_1"},
            "input_path": cfg.fastq.to_string_lossy(), "output_path": cfg.output.to_string_lossy(),
            "start_unix_ms": state.started, "end_unix_ms": unix_ms(),
            "elapsed_ms": state.elapsed_ms.to_string(),
            "worker_count": cfg.workers.count, "scheduling": if cfg.workers.count == 1 {"serial"} else {"bounded-worker-local"},
            "batch_records":cfg.workers.batch_records,"work_queue_capacity":cfg.workers.work_queue_capacity,
            "result_queue_capacity":cfg.workers.result_queue_capacity,"command_config": env::args().collect::<Vec<_>>(),
            "cpu_time": {"unavailable":"not_collected"}, "peak_rss": {"unavailable":"not_collected"},
            "physical_input_bytes": fs::metadata(&cfg.fastq).map_or(json!({"unavailable":"metadata_error"}), |m| json!(m.len())),
            "decoded_bytes": state.decoded_bytes, "processed_prefix_records": state.processed,
            "output_bytes": budget.used(),
            "output_cardinality": {"unavailable":"not_collected"},
            "peak_internal_outstanding_bytes": {"unavailable":"not_collected"},
            "peak_temporary_storage_bytes": {"unavailable":"not_collected"},
            "outcome": state.outcome, "fault": state.fault
        }),
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

struct ScienceStats<'a> {
    leaves: &'a [u64; 7],
    details: &'a QcDetails,
    total: u64,
    decoded_bytes: u64,
    decoded_hash: &'a str,
    discovery: &'a Discovery,
    evidence_artifact: Option<Value>,
}

// Opt-in campaign probe only. QoS acceptance does not prove physical-core
// placement; production runs keep the scheduler's existing policy.
fn hetero_qos(role: &str, extract: bool) {
    #[cfg(target_os = "macos")]
    if env::var("PROJECT_C_HETERO_QOS").is_ok_and(|value| value == "1") {
        let requested = if extract {
            libc::qos_class_t::QOS_CLASS_USER_INITIATED
        } else {
            libc::qos_class_t::QOS_CLASS_UTILITY
        };
        let mut actual = libc::qos_class_t::QOS_CLASS_UNSPECIFIED;
        let mut priority = 0;
        // SAFETY: these APIs operate on the calling thread. The getter's out
        // parameters point to live stack values of the declared libc types.
        let (set, get) = unsafe {
            let set = libc::pthread_set_qos_class_self_np(requested, 0);
            let get = libc::pthread_get_qos_class_np(
                libc::pthread_self(), &mut actual, &mut priority,
            );
            (set, get)
        };
        eprintln!("hetero.qos role={role} requested={requested:?} actual={actual:?} set={set} get={get}");
    }
    #[cfg(not(target_os = "macos"))]
    let _ = (role, extract);
}

struct SingleWorkerState {
    counts: Vec<u64>,
    leaves: [u64; 7],
    details: QcDetails,
    discovery: Discovery,
}

fn parallel_single(
    cfg: &Config,
    fastq: &mut Fastq,
    guides: &[Guide],
    index: &HashMap<String, usize>,
    gpu: Option<&gpu::Pipeline>,
) -> Result<SingleWorkerState> {
    let make = || SingleWorkerState {
        counts: {
            #[allow(unused_mut)]
            let mut counts = vec![0; guides.len()];
            #[cfg(debug_assertions)]
            if env::var("PROJECT_C_PRIVATE_TEST_FAULT")
                .is_ok_and(|s| s == "worker_counter_overflow")
            {
                counts[0] = u64::MAX;
            }
            counts
        },
        leaves: [0; 7],
        details: QcDetails::default(),
        discovery: Discovery::worker_local(cfg.discovery_mode, &["unmapped.no_match"]),
    };
    let accumulate = |state: &mut SingleWorkerState, result: Classification| -> Result<()> {
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
            match result.distance {
                Some(0) => increment(&mut state.details.mapped_exact)?,
                Some(1) => increment(&mut state.details.mapped_rescued_h1)?,
                _ => return Err("conservation.distance".into()),
            }
        }
        if result.leaf == 2 {
            let _discovery = profile::start(profile::DISCOVERY);
            state.discovery.observe_worker(
                0,
                &["unknown"],
                &[result.window.as_deref().expect("unknown window")],
            )?;
        }
        Ok(())
    };
    let process = |state: &mut SingleWorkerState, record: FastqRecord| -> Result<()> {
        let _worker = profile::start(profile::WORKER);
        let _classify = profile::start(profile::CLASSIFY);
        let result = classify(&record.sequence, &record.quality, cfg, guides, index);
        drop(_classify);
        accumulate(state, result)
    };
    let states = if let Some(gpu) = gpu {
        worker::run_gpu_single(cfg, fastq, guides, gpu, make, accumulate)?
    } else {
        worker::run_batches(cfg.workers, || fastq.record(), true, make, process)?
    };
    let _merge = profile::start(profile::MERGE);
    let mut merged = make();
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|s| s == "worker_reduction_overflow") {
        merged.counts[0] = u64::MAX;
    }
    for state in states {
        for (target, value) in merged.counts.iter_mut().zip(state.counts) {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        for (target, value) in merged.leaves.iter_mut().zip(state.leaves) {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        merged.details.mapped_exact = merged
            .details
            .mapped_exact
            .checked_add(state.details.mapped_exact)
            .ok_or("arithmetic.overflow")?;
        merged.details.mapped_rescued_h1 = merged
            .details
            .mapped_rescued_h1
            .checked_add(state.details.mapped_rescued_h1)
            .ok_or("arithmetic.overflow")?;
        for (target, value) in merged
            .details
            .extraction
            .iter_mut()
            .zip(state.details.extraction)
        {
            *target = target.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        merged.discovery.merge(state.discovery)?;
    }
    Ok(merged)
}

// The two guide-sized artifacts construct borrowed rows only as they are written.
fn save_typed_rows(
    stage: &Path,
    name: &str,
    schema: &str,
    rows: impl ExactSizeIterator<Item = impl Serialize>,
    budget: &mut OutputBudget,
    execution_profile: ExecutionProfile,
    artifacts: &mut BTreeMap<String, Value>,
) -> Result<()> {
    let row_count = rows.len();
    let (bytes, sha256) = jsonl(&stage.join(name), rows, budget, true, execution_profile)?;
    artifacts.insert(
        name.into(),
        if execution_profile.audited() {
            json!({"schema_id":schema,"profile_id":PROFILE,"bytes":bytes,"sha256":sha256})
        } else {
            json!({"schema_id":schema,"profile_id":PROFILE,"bytes":bytes,"rows":row_count})
        },
    );
    Ok(())
}

struct PreparedMetadata {
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

fn prepare_metadata(guides: Vec<(String, String)>) -> Result<PreparedMetadata> {
    let mut prepared = PreparedMetadata {
        bytes: Vec::new(),
        ends: Vec::with_capacity(guides.len()),
    };
    for (id, sequence) in guides {
        let row = GuideMetadataRow {
            annotations: BTreeMap::new(),
            gene_id_or_null: None,
            guide_id: &id,
            guide_length: sequence.len(),
            normalized_sequence: &sequence,
            profile_id: PROFILE,
            role: "single",
            schema_id: "project-c.guide-metadata.v1",
        };
        serde_json::to_writer(&mut prepared.bytes, &row)
            .map_err(|e| format!("serialization.json: {e}"))?;
        prepared.bytes.push(b'\n');
        prepared.ends.push(prepared.bytes.len());
    }
    Ok(prepared)
}

fn science(
    stage: &Path,
    cfg: &Config,
    guides: &[Guide],
    stats: ScienceStats<'_>,
    prepared_metadata: Option<Result<PreparedMetadata>>,
    budget: &mut OutputBudget,
) -> Result<(String, usize)> {
    let ScienceStats {
        leaves,
        details,
        total,
        decoded_bytes,
        decoded_hash,
        discovery,
        evidence_artifact,
    } = stats;
    let mut artifacts: BTreeMap<String, Value> = BTreeMap::new();
    if let Some(metadata) = evidence_artifact {
        artifacts.insert("evidence.jsonl".into(), metadata);
    }
    save_typed_rows(
        stage,
        "raw-counts.jsonl",
        "project-c.raw-counts.v1",
        guides.iter().map(|g| RawCountsRow {
            counts: [g.count],
            profile_id: PROFILE,
            row_id: &g.id,
            row_kind: "guide",
            schema_id: "project-c.raw-counts.v1",
        }),
        budget,
        cfg.execution_profile,
        &mut artifacts,
    )?;
    if let Some(prepared) = prepared_metadata {
        // Charge and write in the original artifact/row order, after raw counts.
        // Preparation is speculative; errors surface only at the legacy boundary.
        let prepared = prepared?;
        let mut start = 0;
        let rows = prepared.ends.iter().map(|&end| {
            let row = &prepared.bytes[start..end];
            start = end;
            Ok(row)
        });
        let (bytes, sha256) = jsonl_encoded(
            &stage.join("guide-metadata.jsonl"), rows, budget, true, cfg.execution_profile,
        )?;
        artifacts.insert("guide-metadata.jsonl".into(), if cfg.execution_profile.audited() {
            json!({"schema_id":"project-c.guide-metadata.v1","profile_id":PROFILE,"bytes":bytes,"sha256":sha256})
        } else {
            json!({"schema_id":"project-c.guide-metadata.v1","profile_id":PROFILE,"bytes":bytes,"rows":prepared.ends.len()})
        });
    } else {
        save_typed_rows(
            stage,
            "guide-metadata.jsonl",
            "project-c.guide-metadata.v1",
            guides.iter().map(|g| GuideMetadataRow {
                annotations: BTreeMap::new(),
                gene_id_or_null: None,
                guide_id: &g.id,
                guide_length: g.sequence.len(),
                normalized_sequence: &g.sequence,
                profile_id: PROFILE,
                role: "single",
                schema_id: "project-c.guide-metadata.v1",
            }),
            budget,
            cfg.execution_profile,
            &mut artifacts,
        )?;
    }
    {
        let mut save = |name: &str, schema: &str, rows: Vec<Value>| -> Result<()> {
            let (bytes, sha256) = jsonl(
                &stage.join(name),
                &rows,
                budget,
                true,
                cfg.execution_profile,
            )?;
            artifacts.insert(
                name.into(),
                if cfg.execution_profile.audited() {
                    json!({"schema_id":schema,"profile_id":PROFILE,"bytes":bytes,"sha256":sha256})
                } else {
                    json!({"schema_id":schema,"profile_id":PROFILE,"bytes":bytes,"rows":rows.len()})
                },
            );
            Ok(())
        };
        save(
            "construct-metadata.jsonl",
            "project-c.construct-metadata.v1",
            Vec::new(),
        )?;
        let matrix_sum = guides.iter().try_fold(0_u64, |a, g| {
            a.checked_add(g.count).ok_or("arithmetic.overflow")
        })?;
        let leaf_sum = leaves
            .iter()
            .try_fold(0_u64, |a, b| a.checked_add(*b).ok_or("arithmetic.overflow"))?;
        if leaf_sum != total || matrix_sum != leaves[0] {
            return Err("conservation.failure".into());
        }
        if details.mapped_exact.checked_add(details.mapped_rescued_h1) != Some(leaves[0])
            || details
                .extraction
                .iter()
                .try_fold(0_u64, |a, n| a.checked_add(*n))
                != Some(leaves[4])
        {
            return Err("conservation.qc_failure".into());
        }
        if discovery.eligible(0) != leaves[2] {
            return Err("conservation.discovery_eligible".into());
        }
        let invalid = leaves[3..]
            .iter()
            .try_fold(0_u64, |a, b| a.checked_add(*b).ok_or("arithmetic.overflow"))?;
        let zero_rows = u64::try_from(guides.iter().filter(|g| g.count == 0).count())
            .map_err(|_| "arithmetic.overflow")?;
        let fraction = if total == 0 {
            json!({"undefined":"zero_denominator"})
        } else {
            json!({"numerator":leaves[0],"denominator":total})
        };
        save(
            "sample-qc.jsonl",
            "project-c.sample-qc.v1",
            vec![row(
                "project-c.sample-qc.v1",
                json!({
                    "sample_id":cfg.sample,"total_records":total,"mapped":leaves[0],"recombinant":0,
                    "ambiguous":leaves[1],"unmapped":leaves[2],"invalid":invalid,"matrix_sum":matrix_sum,
                    "zero_count_rows":zero_rows,"mapped_exact":details.mapped_exact,"mapped_rescued_h1":details.mapped_rescued_h1,
                    "mapped_distance_histogram":[details.mapped_exact,details.mapped_rescued_h1],"recombinant_distance_histogram":[],
                    "invalid_reason_counts":{"invalid.alphabet":leaves[3],"invalid.extraction":leaves[4],"invalid.N":leaves[5],"invalid.quality":leaves[6]},
                    "invalid_reason_counts_additive":true,"invalid_reason_counts_denominator":total,
                    "extraction_subreason_counts":{"missing_anchor":details.extraction[0],"multiple_anchors":details.extraction[1],"out_of_bounds":details.extraction[2]},
                    "component_state_counts":{"single":{"unique":leaves[0],"ambiguous":leaves[1],"unknown":leaves[2],"invalid":invalid}},
                    "component_state_additive_per_role":true,"component_state_denominator":total,
                    "structural_success":true,"mapped_fraction":fraction
                }),
            )],
        )?;
        let parent = |i: usize| match i {
            0 => "mapped",
            1 => "ambiguous",
            2 => "unmapped",
            _ => "invalid",
        };
        let mut totals = Vec::new();
        for (name, count) in [
            ("mapped", leaves[0]),
            ("recombinant", 0),
            ("ambiguous", leaves[1]),
            ("unmapped", leaves[2]),
            ("invalid", invalid),
        ] {
            totals.push(row("project-c.terminal-totals.v1", json!({
            "sample_id":cfg.sample,"mode":"single","row_type":"parent","parent":name,"terminal_leaf":null,"count":count})));
        }
        for (i, leaf) in LEAVES.iter().enumerate() {
            totals.push(row(
                "project-c.terminal-totals.v1",
                json!({
            "sample_id":cfg.sample,"mode":"single","row_type":"leaf","parent":parent(i),
            "terminal_leaf":leaf,"count":leaves[i]}),
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
            discovery.rows(&cfg.sample, "single")?,
        )?;
    }
    let config = semantic_config(cfg, guides[0].sequence.len());
    if !cfg.execution_profile.audited() {
        return Ok((
            serde_json::to_string(
                &json!({"artifacts":artifacts,"guide_length":guides[0].sequence.len()}),
            )
            .map_err(|e| e.to_string())?,
            artifacts.len(),
        ));
    }
    let library = guides
        .iter()
        .map(|g| json!([g.id, g.sequence]))
        .collect::<Vec<_>>();
    let physical = json!({"path":cfg.fastq.to_string_lossy(),"encoding":if cfg.fastq.extension().is_some_and(|x| x=="gz") {"gzip"} else {"plain"},
        "bytes":fs::metadata(&cfg.fastq).map_err(|e| e.to_string())?.len(),"sha256":file_hash(&cfg.fastq)?});
    let discovery_json = match cfg.discovery_mode {
        DiscoveryMode::Bounded(k) => json!({"mode":"bounded","K":k}),
        DiscoveryMode::Exact(k) => json!({"mode":"exact","limit":k}),
    };
    let provenance = row(
        "project-c.scientific-provenance.v1",
        json!({
            "semantic_contract_id":CONTRACT,"artifact_schema_profile_ids":artifacts,
            "sample_manifest":[cfg.sample],"normalized_library":library,
            "normalized_library_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&library).map_err(|e| e.to_string())?)),
            "semantic_configuration":config,
            "semantic_configuration_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&config).map_err(|e| e.to_string())?)),
            "physical_inputs":[physical],"canonical_decoded_input_digest_scheme":"project-c.record-stream.v1: four u64 big-endian lengths and logical line bytes per record",
            "canonical_decoded_input_sha256":decoded_hash,"decoded_bytes":decoded_bytes,
            "software_build_identity":{"package":env!("CARGO_PKG_NAME"),"version":env!("CARGO_PKG_VERSION")},
            "requested_backend":"serial-scalar","executed_backend":"serial-scalar",
            "discovery_selector":discovery_json,"evidence_selector":cfg.evidence.selector(), "deterministic_artifacts":artifacts
        }),
    );
    let (_, manifest_hash) = jsonl(
        &stage.join("scientific-provenance.jsonl"),
        &[provenance],
        budget,
        true,
        cfg.execution_profile,
    )?;
    Ok((manifest_hash, artifacts.len() + 1))
}

fn run(
    cfg: &Config,
    stage: &Path,
    started: u64,
    clock: Instant,
    budget: &mut OutputBudget,
) -> Result<()> {
    cfg.limits.preflight_reference_files(&[&cfg.guides])?;
    let mut guides = load_guides(&cfg.guides)?;
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|value| value == "counter_overflow") {
        guides[0].count = u64::MAX;
    }
    let reference_bytes = fs::metadata(&cfg.guides)
        .map_err(|e| format!("preflight.reference_open: {e}"))?
        .len();
    let discovery_k = match cfg.discovery_mode {
        DiscoveryMode::Bounded(k) => k,
        DiscoveryMode::Exact(k) => k,
    };
    let gpu_device = gpu::detect(cfg, guides[0].sequence.len(), guides.len());
    let aggregate_memory_estimate = cfg.limits.plan_gpu(Dimensions {
        parallel_extra: sharding::admission(cfg, guides.len())?
            .checked_add(if cfg.workers.count > 1 && cfg.shards == 0 {
                reference_bytes.checked_mul(16)
                    .and_then(|n| n.checked_add((guides.len() as u64).checked_mul(1024)?))
                    .ok_or("resource.dimension_overflow")?
            } else { 0 }).ok_or("resource.dimension_overflow")?
            .checked_add(cfg.limits.plan_workers(
                cfg.workers,
                guides.len(),
                1,
                discovery_k,
                guides[0].sequence.len(),
            )?)
            .ok_or("resource.dimension_overflow")?,
        guide_count: guides.len(),
        construct_count: 0,
        matrix_rows: guides.len(),
        reference_bytes,
        max_window_bytes: guides[0].sequence.len(),
        input_count: 1,
        discovery_leaves: 1,
        k: discovery_k,
        evidence_capacity: cfg.evidence.capacity(),
        evidence_enabled: !matches!(cfg.evidence, EvidenceMode::Off),
        evidence_identity_bytes: cfg.evidence.identity_bytes(&cfg.sample)?,
    }, gpu_device.is_some())?;
    if let Geometry::Fixed { start } = &cfg.geometry {
        start
            .checked_add(guides[0].sequence.len())
            .ok_or("preflight.window_overflow")?;
    }
    let gpu = gpu_device.and_then(|device| gpu::initialize(device, &guides));
    if gpu.is_some() && env::var("PROJECT_C_GPU_DIAGNOSTICS").is_ok_and(|v| v == "1") {
        eprintln!("gpu.active=1 gpu.batch_records={}", gpu::BATCH);
    }
    let index: HashMap<String, usize> = guides
        .iter()
        .enumerate()
        .map(|(i, g)| (g.sequence.clone(), i))
        .collect();
    let mut fastq = Fastq::open(&cfg.fastq, cfg.limits, cfg.execution_profile)?;
    #[cfg(debug_assertions)]
    if env::var("PROJECT_C_PRIVATE_TEST_FAULT").is_ok_and(|value| value == "ordinal_overflow") {
        fastq.ordinal = u64::MAX;
    }
    let mut leaves = [0_u64; 7];
    let mut details = QcDetails::default();
    let mut discovery = Discovery::new(cfg.discovery_mode, &["unmapped.no_match"]);
    let mut evidence = EvidenceSink::new(
        cfg.evidence.clone(),
        &cfg.sample,
        stage,
        budget,
        cfg.execution_profile,
    )?;
    let mut shard_receipt = None;
    let metadata_worker = if cfg.workers.count > 1 && cfg.shards == 0 {
        let metadata = guides.iter().map(|g| (g.id.clone(), g.sequence.clone())).collect();
        Some(thread::spawn(move || {
            hetero_qos("metadata", false);
            prepare_metadata(metadata)
        }))
    } else {
        None
    };
    let counting = (|| -> Result<()> {
        if cfg.shards > 0 {
            let (state, receipt) = sharding::count(cfg, &mut fastq, &guides, &index)?;
            for (guide, count) in guides.iter_mut().zip(state.counts) {
                guide.count = guide
                    .count
                    .checked_add(count)
                    .ok_or("arithmetic.overflow")?;
            }
            leaves = state.leaves;
            details = state.details;
            discovery = state.discovery;
            shard_receipt = Some(receipt);
            return Ok(());
        }
        if cfg.workers.count > 1 {
            let state = parallel_single(cfg, &mut fastq, &guides, &index, gpu.as_ref())?;
            for (guide, count) in guides.iter_mut().zip(state.counts) {
                guide.count = guide
                    .count
                    .checked_add(count)
                    .ok_or("arithmetic.overflow")?;
            }
            leaves = state.leaves;
            details = state.details;
            discovery = state.discovery;
            return Ok(());
        }
        while let Some(record) = fastq.record()? {
            let _classify = profile::start(profile::CLASSIFY);
            let result = classify(&record.sequence, &record.quality, cfg, &guides, &index);
            drop(_classify);
            increment(&mut leaves[result.leaf])?;
            if let Some(reason) = result.extraction_fault {
                let slot = match reason {
                    ExtractionFault::MissingAnchor => 0,
                    ExtractionFault::MultipleAnchors => 1,
                    ExtractionFault::OutOfBounds => 2,
                };
                increment(&mut details.extraction[slot])?;
            }
            if let Some(i) = result.guide {
                increment(&mut guides[i].count)?;
                match result.distance {
                    Some(0) => increment(&mut details.mapped_exact)?,
                    Some(1) => increment(&mut details.mapped_rescued_h1)?,
                    _ => return Err("conservation.distance".into()),
                }
            }
            if result.leaf == 2 {
                let _discovery = profile::start(profile::DISCOVERY);
                discovery.observe(
                    0,
                    &["unknown"],
                    &[result.window.as_deref().expect("unknown window")],
                )?;
            }
            if let Some(priority) = evidence.selected_priority(fastq.ordinal)? {
                let _evidence = profile::start(profile::EVIDENCE);
                let state = match result.leaf {
                    0 => "unique",
                    1 => "ambiguous",
                    2 => "unknown",
                    _ => "invalid",
                };
                let reason = match result.extraction_fault {
                    Some(ExtractionFault::MissingAnchor) => Some("missing_anchor"),
                    Some(ExtractionFault::MultipleAnchors) => Some("multiple_anchors"),
                    Some(ExtractionFault::OutOfBounds) => Some("out_of_bounds"),
                    None if result.leaf >= 3 => Some(LEAVES[result.leaf]),
                    None => None,
                };
                evidence.observe(fastq.ordinal, priority, json!({
                    "terminal_leaf":LEAVES[result.leaf],"reason":reason,
                    "extracted_windows":{"single":result.window},
                    "components":[{"role":"single","source":"R1","state":state,
                        "guide_id":result.guide.map(|i| &guides[i].id),"distance":result.distance}]
                }), budget)?;
            }
            fastq.recycle(record);
        }
        Ok(())
    })();
    let prepared_metadata = metadata_worker.map(|worker| {
        worker.join().unwrap_or_else(|_| Err("worker.panic".into()))
    });
    if let Err(fault) = counting {
        let _ = execution(
            stage,
            cfg,
            ExecutionState {
                outcome: "FAILED",
                fault: Some(&fault),
                manifest_hash: None,
                started,
                elapsed_ms: clock.elapsed().as_millis(),
                processed: fastq.ordinal,
                decoded_bytes: fastq.decoded_bytes,
            },
            budget,
        );
        return Err(fault);
    }
    let decoded_bytes = fastq.decoded_bytes;
    let total = fastq.ordinal;
    let decoded_hash = fastq
        .digest.take()
        .map_or_else(String::new, |h| format!("{:x}", h.finalize()));
    let _evidence = profile::start(profile::EVIDENCE);
    let evidence_artifact = evidence.finish(budget)?;
    drop(_evidence);
    let _output = profile::start(profile::OUTPUT);
    let (mut manifest_hash, artifact_count) = science(
        stage,
        cfg,
        &guides,
        ScienceStats {
            leaves: &leaves,
            details: &details,
            total,
            decoded_bytes,
            decoded_hash: &decoded_hash,
            discovery: &discovery,
            evidence_artifact,
        },
        prepared_metadata,
        budget,
    )?;
    if let Some(mut receipt) = shard_receipt {
        receipt["aggregate_memory_estimate_bytes"] = json!(aggregate_memory_estimate);
        let mut value: Value = serde_json::from_str(&manifest_hash).map_err(|e| e.to_string())?;
        value["experimental_sharding"] = receipt;
        manifest_hash = value.to_string();
    }
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
    if !cfg.execution_profile.audited() {
        return publish_standard(stage, &cfg.output, budget);
    }
    let bundle_id = format!(
        "{:x}",
        Sha256::digest(format!("{}{}", cfg.sample, manifest_hash).as_bytes())
    );
    let completion = row(
        "project-c.completion.v1",
        json!({
            "execution_profile":cfg.execution_profile.name(),"status":"COMPLETE","scientific_manifest_sha256":manifest_hash,"bundle_identity":bundle_id,
            "deterministic_artifact_count":artifact_count,"verification_durability_profile_id":"project-c.local-fsync-symlink.v1"
        }),
    );
    publish_bundle(stage, &cfg.output, &completion, budget)?;
    Ok(())
}

fn publish_bundle(
    stage: &Path,
    output: &Path,
    completion: &Value,
    budget: &mut OutputBudget,
) -> Result<()> {
    test_fault("before_publication")?;
    let parent = output.parent().unwrap_or(Path::new("."));
    let mut linked = false;
    let publish = (|| -> Result<()> {
        test_fault("before_completion")?;
        jsonl(
            &stage.join("completion.jsonl"),
            std::slice::from_ref(completion),
            budget,
            false,
            ExecutionProfile::CryptographicAudited,
        )?;
        test_fault("stage_sync")?;
        File::open(stage)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("publication.sync_directory: {e}"))?;
        let target = stage.file_name().ok_or("publication.stage_name")?;
        test_fault("atomic_link")?;
        symlink(target, output).map_err(|e| format!("publication.atomic_link: {e}"))?;
        linked = true;
        test_fault("parent_sync")?;
        File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("publication.sync_parent: {e}"))?;
        Ok(())
    })();
    if let Err(primary) = publish {
        let mut cleanup = Vec::new();
        if let Err(error) = test_fault("cleanup") {
            cleanup.push(error);
        }
        if linked {
            if let Err(error) = fs::remove_file(output) {
                cleanup.push(format!("publication.cleanup_link: {error}"));
            }
            if let Err(error) = File::open(parent).and_then(|f| f.sync_all()) {
                cleanup.push(format!("publication.cleanup_sync_parent: {error}"));
            }
        }
        let completion_path = stage.join("completion.jsonl");
        if completion_path.exists()
            && let Err(error) = fs::remove_file(completion_path)
        {
            cleanup.push(format!("publication.cleanup_completion: {error}"));
        }
        if let Err(error) = File::open(stage).and_then(|f| f.sync_all()) {
            cleanup.push(format!("publication.cleanup_sync_directory: {error}"));
        }
        return if cleanup.is_empty() {
            Err(primary)
        } else {
            Err(format!("{primary}; cleanup: {}", cleanup.join("; ")))
        };
    }
    Ok(())
}

fn main() {
    let args: Vec<_> = env::args_os().collect();
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
        print!("{METAL_HELP}");
        return;
    }
    profile::init();
    let clock = Instant::now();
    let started = unix_ms();
    let audited = env::args_os()
        .collect::<Vec<_>>()
        .windows(2)
        .any(|p| p[0] == "--execution-profile" && p[1] == "cryptographic-audited");
    let result = (|| -> Result<()> {
        if env::args_os().any(|arg| arg == "--mode") {
            return dual::run_cli(started, clock);
        }
        let cfg = parse()?;
        let parent = cfg.output.parent().unwrap_or(Path::new("."));
        if !parent.is_dir() {
            return Err("preflight.output_parent".into());
        }
        let stage = parent.join(format!(
            ".{}.stage.{}.{}",
            cfg.output.file_name().unwrap().to_string_lossy(),
            std::process::id(),
            started
        ));
        fs::create_dir(&stage).map_err(|e| format!("publication.stage: {e}"))?;
        let mut budget = OutputBudget::new(cfg.limits);
        let result = run(&cfg, &stage, started, clock, &mut budget);
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
    })();
    if audited && let Err(error) = profile::finish() {
        eprintln!("profile.write: {error}");
        std::process::exit(1);
    }
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_overflow_is_a_fault() {
        let mut value = u64::MAX;
        assert_eq!(increment(&mut value), Err("arithmetic.overflow".into()));
        assert_eq!(value, u64::MAX);
    }

    #[test]
    fn semantic_precedence_and_fixed_window() {
        let mut index = HashMap::new();
        index.insert("AAAA".into(), 0);
        let guides = vec![Guide {
            id: "g".into(),
            sequence: "AAAA".into(),
            count: 0,
        }];
        let mut cfg = Config {
            no_gpu: false,
            shards: 0,
            execution_profile: ExecutionProfile::CryptographicAudited,
            workers: WorkerConfig {
                count: 1,
                batch_records: 32,
                work_queue_capacity: 2,
                result_queue_capacity: 1,
            },
            sample: "s".into(),
            fastq: PathBuf::new(),
            guides: PathBuf::new(),
            geometry: Geometry::Fixed { start: 0 },
            orientation: Orientation::Forward,
            radius: 0,
            min_phred: None,
            discovery_mode: DiscoveryMode::Bounded(100),
            evidence: EvidenceMode::Off,
            limits: Limits::parse(&HashMap::new()).unwrap(),
            output: PathBuf::new(),
        };
        assert_eq!(classify(b"-AAA", b"!!!!", &cfg, &guides, &index).leaf, 3);
        cfg.geometry = Geometry::Fixed { start: 1 };
        assert_eq!(classify(b"AAAA", b"!!!!", &cfg, &guides, &index).leaf, 4);
        assert_eq!(
            classify(b"NAAAA", b"!!!!!", &cfg, &guides, &index).guide,
            Some(0)
        );
        cfg.geometry = Geometry::Fixed { start: 0 };
        assert_eq!(classify(b"AAAN", b"!!!!", &cfg, &guides, &index).leaf, 5);
        assert_eq!(classify(b"TTTT", b"!!!!", &cfg, &guides, &index).leaf, 2);
    }

    #[test]
    fn fixed_bounds_and_single_anchor_full_classification_compatibility() {
        assert_eq!(window_start(b"AAAA", 4, 0).unwrap(), 0);
        assert!(matches!(
            window_start(b"AAAA", 4, usize::MAX),
            Err(ExtractionFault::OutOfBounds)
        ));
        let guides = vec![
            Guide {
                id: "g".into(),
                sequence: "AAAA".into(),
                count: 0,
            },
            Guide {
                id: "h".into(),
                sequence: "AAAC".into(),
                count: 0,
            },
        ];
        let index = guides
            .iter()
            .enumerate()
            .map(|(i, g)| (g.sequence.clone(), i))
            .collect();
        let anchor = Geometry::Anchor {
            sequence: b"CACCG".to_vec(),
            search_start: 0,
            search_end: 0,
            displacement: 0,
        };
        let fixed = Geometry::Fixed { start: 5 };
        for radius in [0, 1] {
            for min_phred in [None, Some(20)] {
                for sequence in [b"CACCGAAAA", b"CACCGAAAT", b"CACCGNNNN", b"CACCGTTTT"] {
                    let spec = ComponentSpec {
                        geometry: &anchor,
                        orientation: Orientation::Forward,
                        radius,
                        min_phred,
                    };
                    let a = classify_component(sequence, b"IIIIIIIII", spec, &guides, &index);
                    let f = classify_component(
                        sequence,
                        b"IIIIIIIII",
                        ComponentSpec {
                            geometry: &fixed,
                            ..spec
                        },
                        &guides,
                        &index,
                    );
                    assert_eq!(
                        (a.leaf, a.guide, a.distance, a.window),
                        (f.leaf, f.guide, f.distance, f.window)
                    );
                }
            }
        }
    }

    #[test]
    fn reverse_quality_tracks_oriented_window() {
        let mut index = HashMap::new();
        index.insert("AAAA".into(), 0);
        let guides = vec![Guide {
            id: "g".into(),
            sequence: "AAAA".into(),
            count: 0,
        }];
        let cfg = Config {
            no_gpu: false,
            shards: 0,
            execution_profile: ExecutionProfile::CryptographicAudited,
            workers: WorkerConfig {
                count: 1,
                batch_records: 32,
                work_queue_capacity: 2,
                result_queue_capacity: 1,
            },
            sample: "s".into(),
            fastq: PathBuf::new(),
            guides: PathBuf::new(),
            geometry: Geometry::Fixed { start: 0 },
            orientation: Orientation::ReverseComplement,
            radius: 0,
            min_phred: Some(20),
            discovery_mode: DiscoveryMode::Bounded(100),
            evidence: EvidenceMode::Off,
            limits: Limits::parse(&HashMap::new()).unwrap(),
            output: PathBuf::new(),
        };
        assert_eq!(
            classify(b"NTTTT", b"!5555", &cfg, &guides, &index).guide,
            Some(0)
        );
        assert_eq!(classify(b"NTTTT", b"!555!", &cfg, &guides, &index).leaf, 6);
    }
}

#[cfg(test)]
mod metal_policy_tests {
    use super::*;

    #[test]
    fn cpu_default_requires_explicit_experimental_opt_in() {
        let mut values = HashMap::new();
        assert!(metal_policy(&values).unwrap());
        values.insert("--experimental-metal".into(), "true".into());
        assert!(!metal_policy(&values).unwrap());
        values.insert("--no-gpu".into(), "true".into());
        assert!(metal_policy(&values).is_err());
        values.remove("--experimental-metal");
        assert!(metal_policy(&values).unwrap());
    }
}

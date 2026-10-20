//! Four physical-line hypotheses, bounded end witnesses, and ordered prefix composition.
use super::{Limits, Result};
use std::io::BufRead;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
const MAX_LINE: u64 = super::MAX_LINE as u64;
pub(super) const CHUNK: usize = 8_388_608;
const STACK: usize = 2_097_152;
const K: usize = 12;
const TARGETS: usize = 11;
#[derive(Clone, Copy, Default, Debug)]
struct LineDescriptor {
    start: u64,
    len: u64,
    bad: u64,
    first: [u8; 2],
    last: [u8; 2],
}
impl LineDescriptor {
    fn from(b: &[u8], start: u64) -> Self {
        let mut d = LineDescriptor {
            start,
            len: b.len() as u64,
            bad: b.iter().filter(|&&x| !(33..=126).contains(&x)).count() as u64,
            ..LineDescriptor::default()
        };
        for i in 0..b.len().min(2) {
            d.first[i] = b[i];
            d.last[2 - b.len().min(2) + i] = b[b.len() - b.len().min(2) + i];
        }
        d
    }
    fn join(a: LineDescriptor, b: LineDescriptor) -> Result<LineDescriptor> {
        if a.len == 0 {
            return Ok(b);
        }
        if b.len == 0 {
            return Ok(a);
        }
        let mut x = LineDescriptor {
            start: a.start,
            len: a.len.checked_add(b.len).ok_or("arithmetic.overflow")?,
            bad: a.bad.checked_add(b.bad).ok_or("arithmetic.overflow")?,
            first: a.first,
            last: b.last,
        };
        if a.len == 1 {
            x.first[1] = b.first[0];
        }
        if b.len == 1 {
            x.last[0] = a.last[1];
        }
        Ok(x)
    }
    fn stripped(self) -> (u64, u64) {
        let mut n = self.len;
        let mut bad = self.bad;
        if n > 0 && self.last[1] == b'\n' {
            n -= 1;
            bad -= 1;
            if n > 0 && self.last[0] == b'\r' {
                n -= 1;
                bad -= 1;
            }
        }
        (n, bad)
    }
    fn end(self) -> u64 {
        self.start + self.len
    }
}
#[derive(Clone, Copy, Default, Debug)]
struct RecordFault {
    line: u64,
    decoded: u64,
    code: u8,
}
const ERRORS: [&str; 9] = [
    "",
    "input.line_limit",
    "input.record_limit",
    "input.invalid_header",
    "input.invalid_separator",
    "input.quality_length",
    "input.quality_encoding",
    "input.incomplete_record",
    "arithmetic.overflow",
];
#[derive(Clone, Copy, Default, Debug)]
struct RecordEnd {
    line: u64,
    byte: u64,
}
#[derive(Clone, Copy)]
struct PhaseSummary {
    fault: RecordFault,
    ends: [[RecordEnd; K]; TARGETS],
    sizes: [u8; TARGETS],
}
impl Default for PhaseSummary {
    fn default() -> Self {
        Self {
            fault: RecordFault::default(),
            ends: [[RecordEnd::default(); K]; TARGETS],
            sizes: [0; TARGETS],
        }
    }
}
#[derive(Clone)]
struct Summary {
    n: u64,
    first: [LineDescriptor; 8],
    last: [LineDescriptor; 8],
    tail: LineDescriptor,
    agg: [PhaseSummary; 4],
    begin: u64,
    finish: u64,
}
impl Summary {
    fn empty(at: u64) -> Self {
        Self {
            n: 0,
            first: [LineDescriptor::default(); 8],
            last: [LineDescriptor::default(); 8],
            tail: LineDescriptor {
                start: at,
                ..LineDescriptor::default()
            },
            agg: [PhaseSummary::default(); 4],
            begin: at,
            finish: at,
        }
    }
    fn get(&self, i: u64) -> Result<LineDescriptor> {
        if i >= self.n || (i >= 8 && i < self.n.saturating_sub(8)) {
            return Err("conservation.h2_state".into());
        }
        Ok(if i < 8 {
            self.first[i as usize]
        } else {
            self.last[(i % 8) as usize]
        })
    }
    fn push(&mut self, d: LineDescriptor, c: &IndexConfig) -> Result<()> {
        let i = self.n;
        if i < 8 {
            self.first[i as usize] = d;
        }
        self.last[(i % 8) as usize] = d;
        self.n = self.n.checked_add(1).ok_or("arithmetic.overflow")?;
        if self.n >= 12 {
            let st = self.n - 8;
            self.add_window(st, c)?;
        }
        Ok(())
    }
    fn add_window(&mut self, st: u64, c: &IndexConfig) -> Result<()> {
        let ds = [
            self.get(st)?,
            self.get(st + 1)?,
            self.get(st + 2)?,
            self.get(st + 3)?,
        ];
        let f = record(ds, st, c)?;
        let a = &mut self.agg[(st % 4) as usize];
        add_fault(a, f);
        add_end(
            a,
            RecordEnd {
                line: st,
                byte: ds[3].end(),
            },
            c,
        );
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct IndexConfig {
    record: u64,
    header: u64,
    s: usize,
    length: u64,
    ordinal: u64,
}
fn line_fault(d: LineDescriptor, phase: usize, c: &IndexConfig) -> Option<(u64, u8)> {
    let limit = if phase == 0 { c.header } else { c.record };
    if d.len > limit.min(MAX_LINE) + 2 {
        return Some((d.start, 1));
    }
    if d.stripped().0 > limit {
        return Some((d.end(), 1));
    }
    None
}
fn record(ds: [LineDescriptor; 4], st: u64, c: &IndexConfig) -> Result<RecordFault> {
    for (p, d) in ds.iter().enumerate() {
        if let Some((decoded, code)) = line_fault(*d, p, c) {
            return Ok(RecordFault {
                line: st,
                decoded,
                code,
            });
        }
    }
    let lens = ds.map(|d| d.stripped().0);
    let decoded = ds[3].end();
    let code = if lens
        .iter()
        .try_fold(0u64, |a, b| a.checked_add(*b).ok_or("arithmetic.overflow"))?
        > c.record
    {
        2
    } else if lens[0] < 2 || ds[0].first[0] != b'@' || ds[0].first[1].is_ascii_whitespace() {
        3
    } else if lens[2] == 0 || ds[2].first[0] != b'+' {
        4
    } else if lens[1] != lens[3] {
        5
    } else if ds[3].stripped().1 > 0 {
        6
    } else if c.ordinal.checked_add(st / 4 + 1).is_none() {
        8
    } else {
        0
    };
    Ok(RecordFault {
        line: st,
        decoded,
        code,
    })
}
fn add_fault(a: &mut PhaseSummary, f: RecordFault) {
    if f.code != 0 && (a.fault.code == 0 || f.line < a.fault.line) {
        a.fault = f;
    }
}
fn add_end(a: &mut PhaseSummary, e: RecordEnd, c: &IndexConfig) {
    for j in 0..c.s - 1 {
        if e.byte < (c.length / c.s as u64) * (j as u64 + 1) {
            continue;
        }
        let n = a.sizes[j] as usize;
        let mut at = 0;
        while at < n && a.ends[j][at].line < e.line {
            at += 1;
        }
        if at < n && a.ends[j][at].line == e.line {
            continue;
        }
        if at >= K {
            continue;
        }
        for p in (at..n.min(K - 1)).rev() {
            a.ends[j][p + 1] = a.ends[j][p];
        }
        a.ends[j][at] = e;
        a.sizes[j] = (n + 1).min(K) as u8;
    }
}
fn scan(bytes: &[u8], at: u64, c: &IndexConfig) -> Result<Summary> {
    at.checked_add(bytes.len() as u64)
        .ok_or("arithmetic.overflow")?;
    let mut out = Summary::empty(at);
    let mut cursor = 0;
    while cursor < bytes.len() {
        let n = bytes[cursor..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len() - cursor, |n| n + 1);
        let d = LineDescriptor::from(&bytes[cursor..cursor + n], at + cursor as u64);
        out.tail = LineDescriptor::join(out.tail, d)?;
        cursor += n;
        if bytes[cursor - 1] == b'\n' {
            out.push(out.tail, c)?;
            out.tail = LineDescriptor {
                start: at + cursor as u64,
                ..LineDescriptor::default()
            };
        }
    }
    out.finish = at
        .checked_add(bytes.len() as u64)
        .ok_or("arithmetic.overflow")?;
    Ok(out)
}
fn merge(a: Summary, b: Summary, c: &IndexConfig) -> Result<Summary> {
    if a.finish != b.begin {
        return Err("conservation.h2_interval".into());
    }
    let mut out = Summary::empty(a.begin);
    out.finish = b.finish;
    out.n = a.n.checked_add(b.n).ok_or("arithmetic.overflow")?;
    let modified = if b.n > 0 {
        Some(LineDescriptor::join(a.tail, b.get(0)?)?)
    } else {
        None
    };
    let get = |i: u64| -> Result<LineDescriptor> {
        if i < a.n {
            a.get(i)
        } else if i == a.n {
            match modified {
                Some(d) => Ok(d),
                None => b.get(i - a.n),
            }
        } else {
            b.get(i - a.n)
        }
    };
    for i in 0..out.n.min(8) {
        out.first[i as usize] = get(i)?;
    }
    for i in out.n.saturating_sub(8)..out.n {
        out.last[(i % 8) as usize] = get(i)?;
    }
    out.tail = if b.n == 0 {
        LineDescriptor::join(a.tail, b.tail)?
    } else {
        b.tail
    };
    out.agg = a.agg;
    for p in 0..4 {
        let dest = (p + (a.n % 4) as usize) % 4;
        let mut f = b.agg[p].fault;
        f.line = f.line.checked_add(a.n).ok_or("arithmetic.overflow")?;
        add_fault(&mut out.agg[dest], f);
        for j in 0..c.s - 1 {
            for z in 0..b.agg[p].sizes[j] as usize {
                let mut e = b.agg[p].ends[j][z];
                e.line = e.line.checked_add(a.n).ok_or("arithmetic.overflow")?; // merge sorted bounded candidates even if left list is full
                let ag = &mut out.agg[dest];
                let n = ag.sizes[j] as usize;
                let mut pos = 0;
                while pos < n && ag.ends[j][pos].line < e.line {
                    pos += 1;
                }
                if pos < n && ag.ends[j][pos].line == e.line {
                    continue;
                }
                if pos >= K {
                    continue;
                }
                for k in (pos..n.min(K - 1)).rev() {
                    ag.ends[j][k + 1] = ag.ends[j][k];
                }
                ag.ends[j][pos] = e;
                ag.sizes[j] = (n + 1).min(K) as u8;
            }
        }
    }
    // Recompute newly interior windows excluded from child aggregates.
    if out.n >= 12 {
        let lo = a.n.saturating_sub(7).max(4);
        let hi = a.n.saturating_add(3).min(out.n - 8);
        if lo <= hi {
            for st in lo..=hi {
                let ds = [get(st)?, get(st + 1)?, get(st + 2)?, get(st + 3)?];
                let f = record(ds, st, c)?;
                let ag = &mut out.agg[(st % 4) as usize];
                add_fault(ag, f);
                add_end(
                    ag,
                    RecordEnd {
                        line: st,
                        byte: ds[3].end(),
                    },
                    c,
                );
            }
        }
    }
    Ok(out)
}
fn finish(mut x: Summary, c: &IndexConfig, io_error: Option<String>) -> Result<Outcome> {
    let mut terminal = io_error;
    // Only actual EOF returns an unterminated nonempty line.
    if terminal.is_none() && x.tail.len > 0 {
        let d = x.tail;
        x.push(d, c)?;
        x.tail = LineDescriptor {
            start: d.end(),
            ..LineDescriptor::default()
        };
    }
    let mut ag = x.agg[0];
    // Evaluate complete edge records excluded from interior aggregation.
    for st in (0..x.n).step_by(4) {
        if st.checked_add(4).is_none_or(|end| end > x.n) {
            break;
        }
        if st < 4 || st + 4 > x.n.saturating_sub(4) {
            let ds = [x.get(st)?, x.get(st + 1)?, x.get(st + 2)?, x.get(st + 3)?];
            add_fault(&mut ag, record(ds, st, c)?);
            add_end(
                &mut ag,
                RecordEnd {
                    line: st,
                    byte: ds[3].end(),
                },
                c,
            );
        } else {
            break;
        }
    }
    // trailing edge, independent of the leading edge loop
    let lo = x.n.saturating_sub(8);
    for st in lo..x.n {
        if st % 4 == 0 && st.checked_add(4).is_some_and(|end| end <= x.n) {
            let ds = [x.get(st)?, x.get(st + 1)?, x.get(st + 2)?, x.get(st + 3)?];
            add_fault(&mut ag, record(ds, st, c)?);
            add_end(
                &mut ag,
                RecordEnd {
                    line: st,
                    byte: ds[3].end(),
                },
                c,
            );
        }
    }
    let st = (x.n / 4) * 4;
    let mut pending = RecordFault::default();
    for i in st..x.n {
        if let Some((decoded, code)) = line_fault(x.get(i)?, (i - st) as usize, c) {
            pending = RecordFault {
                line: st,
                decoded,
                code,
            };
            break;
        }
    }
    if pending.code != 0 {
        terminal = None;
        add_fault(&mut ag, pending);
    } else if terminal.is_some() {
        let phase = (x.n % 4) as usize;
        let limit = if phase == 0 { c.header } else { c.record };
        if x.tail.len > limit.min(MAX_LINE) + 2 {
            terminal = None;
            add_fault(
                &mut ag,
                RecordFault {
                    line: st,
                    decoded: x.tail.start,
                    code: 1,
                },
            );
        }
    } else if x.n % 4 != 0 {
        add_fault(
            &mut ag,
            RecordFault {
                line: st,
                decoded: x.finish,
                code: 7,
            },
        );
    }
    let (error, decoded, ordinal) = if ag.fault.code != 0 {
        (
            Some(ERRORS[ag.fault.code as usize].to_string()),
            ag.fault.decoded,
            c.ordinal
                .checked_add(ag.fault.line / 4)
                .ok_or("arithmetic.overflow")?,
        )
    } else if let Some(e) = terminal {
        (
            Some(e),
            x.tail.start,
            c.ordinal
                .checked_add(x.n / 4)
                .ok_or("arithmetic.overflow")?,
        )
    } else {
        (
            None,
            x.finish,
            c.ordinal
                .checked_add(x.n / 4)
                .ok_or("arithmetic.overflow")?,
        )
    };
    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(c.s + 1)
        .map_err(|_| "resource.memory_budget")?;
    bounds.push((0, 0));
    if error.is_none() {
        for j in 0..c.s - 1 {
            let prev = bounds.last().map_or(c.ordinal, |b| b.1);
            if let Some(e) = ag.ends[j][..ag.sizes[j] as usize]
                .iter()
                .find(|e| c.ordinal.saturating_add(e.line / 4 + 1) > prev)
            {
                bounds.push((
                    e.byte,
                    c.ordinal
                        .checked_add(e.line / 4 + 1)
                        .ok_or("arithmetic.overflow")?,
                ));
            } else {
                break;
            }
        }
        let end = (decoded, ordinal);
        if bounds.last() == Some(&end) {
            bounds.pop();
        }
        bounds.push(end);
    }
    Ok(Outcome {
        error,
        decoded,
        ordinal,
        bounds,
    })
}

pub(super) struct Outcome {
    pub(super) error: Option<String>,
    pub(super) decoded: u64,
    pub(super) ordinal: u64,
    pub(super) bounds: Vec<(u64, u64)>,
}

// Admission includes simultaneous downstream additions in the caller, even
// though the index pool has joined before downstream threads are launched.
pub(super) fn admission(workers: usize) -> Result<u64> {
    if workers == 0 {
        return Ok(0);
    }
    let summary = std::mem::size_of::<Summary>() as u64;
    (CHUNK as u64)
        .checked_add(STACK as u64)
        .and_then(|n| n.checked_add(summary.checked_mul(4)?))
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| n.checked_mul(workers as u64))
        .and_then(|n| n.checked_add(1_048_576))
        .ok_or("resource.dimension_overflow".into())
}
struct Job {
    number: usize,
    start: u64,
    bytes: Vec<u8>,
}
struct Completed {
    number: usize,
    bytes: Vec<u8>,
    summary: Result<Summary>,
    fault: Option<String>,
}
enum Message {
    Chunk(Completed),
    Exit(usize, String),
}
fn infrastructure(current: &mut Option<(usize, String)>, number: usize, fault: String) {
    let key = (fault.starts_with("worker.panic"), number);
    if current
        .as_ref()
        .is_none_or(|(n, f)| key < (f.starts_with("worker.panic"), *n))
    {
        *current = Some((number, fault));
    }
}
fn collect(
    done: Completed,
    pending: &mut [Option<Result<Summary>>; K],
    free: &mut Vec<Vec<u8>>,
    root: &mut Summary,
    next: &mut usize,
    sent: usize,
    c: &IndexConfig,
    broken: &mut bool,
    infra: &mut Option<(usize, String)>,
) -> Result<()> {
    if done.number < *next || done.number >= sent || done.number - *next >= c.s {
        return Err("conservation.h2_window".into());
    }
    if let Some(fault) = done.fault {
        infrastructure(infra, done.number, fault);
    }
    let slot = done.number % c.s;
    if pending[slot].is_some() {
        return Err("conservation.h2_duplicate".into());
    }
    if free.len() >= c.s {
        return Err("conservation.h2_buffers".into());
    }
    free.push(done.bytes);
    pending[slot] = Some(done.summary);
    while let Some(item) = pending[*next % c.s].take() {
        match item {
            Ok(sum) if !*broken => match merge(root.clone(), sum, c) {
                Ok(sum) => *root = sum,
                Err(f) => {
                    infrastructure(infra, *next, f);
                    *broken = true;
                }
            },
            Ok(_) => {}
            Err(f) => {
                infrastructure(infra, *next, f);
                *broken = true;
            }
        }
        *next = next.checked_add(1).ok_or("arithmetic.overflow")?;
    }
    Ok(())
}

pub(super) fn index<R: BufRead + ?Sized>(
    reader: &mut R,
    limits: Limits,
    workers: usize,
    length: u64,
    ordinal: u64,
) -> Result<Outcome> {
    #[cfg(not(debug_assertions))]
    let chunk = CHUNK;
    #[cfg(debug_assertions)]
    let chunk = std::env::var("PROJECT_C_PRIVATE_H2_CHUNK_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0 && *v <= CHUNK)
        .map_or(CHUNK, |v| v);
    index_with_chunk(reader, limits, workers, length, ordinal, chunk)
}
fn index_with_chunk<R: BufRead + ?Sized>(
    reader: &mut R,
    limits: Limits,
    workers: usize,
    length: u64,
    ordinal: u64,
    chunk: usize,
) -> Result<Outcome> {
    if !(2..=K).contains(&workers) {
        return Err("preflight.invalid_shards".into());
    }
    if chunk == 0 || chunk > CHUNK {
        return Err("conservation.h2_chunk".into());
    }
    let c = IndexConfig {
        record: limits.max_record_bytes as u64,
        header: limits.max_header_bytes as u64,
        s: workers,
        length,
        ordinal,
    };
    let mut free = Vec::new();
    free.try_reserve_exact(workers)
        .map_err(|_| "resource.memory_budget")?;
    for _ in 0..workers {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(chunk)
            .map_err(|_| "resource.memory_budget")?;
        free.push(bytes);
    }
    let (work_tx, work_rx) = mpsc::sync_channel::<Job>(workers);
    let (result_tx, result_rx) = mpsc::sync_channel::<Message>(workers);
    let receiver = Arc::new(Mutex::new(work_rx));
    let mut handles = Vec::new();
    handles
        .try_reserve_exact(workers)
        .map_err(|_| "resource.memory_budget")?;
    let mut infra = None;
    for worker in 0..workers {
        #[cfg(debug_assertions)]
        if worker == 1 && super::test_fault("h2_spawn").is_err() {
            infrastructure(
                &mut infra,
                worker,
                "worker.spawn: test_fault.h2_spawn".into(),
            );
            continue;
        }
        let receiver = receiver.clone();
        let sender = result_tx.clone();
        let launched = thread::Builder::new()
            .name(format!("project-c-h2-{worker}"))
            .stack_size(STACK)
            .spawn(move || -> Result<()> {
                let execution =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
                        loop {
                            let received = {
                                let locked = receiver.lock().map_err(|_| "worker.panic")?;
                                locked.recv()
                            };
                            let job = match received {
                                Ok(job) => job,
                                Err(_) => break,
                            };
                            #[cfg(debug_assertions)]
                            if job.number == 1 && super::test_fault("h2_thread_panic").is_err() {
                                std::panic::panic_any("test_fault.h2_thread_panic");
                            }
                            let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                || -> Result<Summary> {
                                    #[cfg(debug_assertions)]
                                    if job.number == 1 && super::test_fault("h2_panic").is_err() {
                                        std::panic::panic_any("test_fault.h2_panic");
                                    }
                                    scan(&job.bytes, job.start, &c)
                                },
                            ));
                            let (summary, fault) = match parsed {
                                Ok(value) => (value, None),
                                Err(_) => {
                                    // One bounded retry of the same H2 descriptor kernel, on this index
                                    // worker, preserves positioned-error selection after a caught panic.
                                    // The panic remains fatal even if summaries complete. No FASTQ parser
                                    // fallback and no new worker are involved.
                                    let retry =
                                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                            || scan(&job.bytes, job.start, &c),
                                        ));
                                    (
                                        match retry {
                                            Ok(value) => value,
                                            Err(_) => Err("worker.panic".into()),
                                        },
                                        Some("worker.panic".into()),
                                    )
                                }
                            };
                            if sender
                                .send(Message::Chunk(Completed {
                                    number: job.number,
                                    bytes: job.bytes,
                                    summary,
                                    fault,
                                }))
                                .is_err()
                            {
                                break;
                            }
                            #[cfg(debug_assertions)]
                            if job.number == 1 && super::test_fault("h2_exit_after_result").is_err()
                            {
                                std::panic::panic_any("test_fault.h2_exit_after_result");
                            }
                        }
                        Ok(())
                    }));
                let result = match execution {
                    Ok(value) => value,
                    Err(_) => Err("worker.panic".into()),
                };
                if let Err(fault) = &result {
                    let _ = sender.send(Message::Exit(worker, fault.clone()));
                }
                result
            });
        match launched {
            Ok(handle) => handles.push((worker, handle)),
            Err(e) => infrastructure(&mut infra, worker, format!("worker.spawn: {e}")),
        }
    }
    drop(result_tx);
    let mut root = Summary::empty(0);
    let mut pending: [Option<Result<Summary>>; K] = std::array::from_fn(|_| None);
    let mut sent = 0usize;
    let mut next = 0usize;
    let mut at = 0u64;
    let mut broken = false;
    let mut halt = false;
    let mut source = None;
    let mut source_ended = false;
    // The closure may return early, but channel teardown and joins below always run.
    let delivery = (|| -> Result<()> {
        if handles.is_empty() {
            return Ok(());
        }
        loop {
            while sent - next >= workers || free.is_empty() {
                let message = result_rx.recv().map_err(|_| "worker.panic")?;
                let done = match message {
                    Message::Chunk(done) => done,
                    Message::Exit(worker, fault) => {
                        infrastructure(&mut infra, worker, fault);
                        halt = true;
                        return Ok(());
                    }
                };
                collect(
                    done,
                    &mut pending,
                    &mut free,
                    &mut root,
                    &mut next,
                    sent,
                    &c,
                    &mut broken,
                    &mut infra,
                )?;
                if broken {
                    return Ok(());
                }
            }
            let Some(mut bytes) = free.pop() else {
                return Err("conservation.h2_buffers".into());
            };
            bytes.clear();
            while bytes.len() < chunk {
                #[cfg(debug_assertions)]
                if std::env::var("PROJECT_C_PRIVATE_H2_READ_FAIL_AT")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|limit| at + bytes.len() as u64 >= limit)
                {
                    source_ended = true;
                    source = Some("input: test_fault.h2_source".into());
                    break;
                }
                let input = match reader.fill_buf() {
                    Ok(input) => input,
                    Err(e) => {
                        source_ended = true;
                        source = Some(format!("input: {e}"));
                        break;
                    }
                };
                if input.is_empty() {
                    source_ended = true;
                    break;
                }
                let take = (chunk - bytes.len()).min(input.len());
                #[cfg(debug_assertions)]
                let take = std::env::var("PROJECT_C_PRIVATE_H2_READ_FAIL_AT")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map_or(take, |limit| {
                        take.min(limit.saturating_sub(at + bytes.len() as u64) as usize)
                    });
                bytes.extend_from_slice(&input[..take]);
                reader.consume(take);
            }
            let used = bytes.len();
            if used > 0 {
                let start = at;
                at = at.checked_add(used as u64).ok_or("arithmetic.overflow")?;
                work_tx
                    .send(Job {
                        number: sent,
                        start,
                        bytes,
                    })
                    .map_err(|_| "worker.panic")?;
                sent = sent.checked_add(1).ok_or("arithmetic.overflow")?;
            } else {
                free.push(bytes);
            }
            if used < chunk || source.is_some() {
                break;
            }
        }
        Ok(())
    })();
    if let Err(f) = delivery {
        infrastructure(&mut infra, next, f);
        halt = true;
    }
    drop(work_tx);
    // Drain every in-flight result before joining, so bounded senders cannot block.
    for message in result_rx {
        let done = match message {
            Message::Chunk(done) => done,
            Message::Exit(worker, fault) => {
                infrastructure(&mut infra, worker, fault);
                halt = true;
                continue;
            }
        };
        if let Err(f) = collect(
            done,
            &mut pending,
            &mut free,
            &mut root,
            &mut next,
            sent,
            &c,
            &mut broken,
            &mut infra,
        ) {
            infrastructure(&mut infra, next, f);
            halt = true;
        }
    }
    for (worker, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(f)) => infrastructure(&mut infra, worker, f),
            Err(_) => infrastructure(&mut infra, worker, "worker.panic".into()),
        }
    }
    #[cfg(debug_assertions)]
    if let Ok(path) = std::env::var("PROJECT_C_PRIVATE_H2_JOIN_LOG") {
        std::fs::write(path, b"all-index-workers-joined\n").map_err(|e| format!("input: {e}"))?;
    }
    // Preserve an observed EOF/read error once every source chunk has composed.
    // For a lost chunk, evaluate only the trusted prefix with an infrastructure
    // terminal, never an invented EOF at the gap.
    let complete = !broken && next == sent && root.finish == at && source_ended;
    let terminal = if broken || (halt && !complete) {
        infra.as_ref().map(|(_, f)| f.clone()).or(source)
    } else {
        source
    };
    let mut outcome = finish(root, &c, terminal)?;
    if outcome.error.is_none() {
        if let Some((_, f)) = infra {
            outcome.error = Some(f);
        }
    }
    #[cfg(debug_assertions)]
    if let Ok(path) = std::env::var("PROJECT_C_PRIVATE_H2_ACCOUNTING_PATH") {
        let bytes=serde_json::to_vec(&serde_json::json!({"error":outcome.error,"decoded":outcome.decoded,"ordinal":outcome.ordinal,"bounds":outcome.bounds,"summary_bytes":std::mem::size_of::<Summary>(),"admission":admission(workers)?})).map_err(|e|format!("input: {e}"))?;
        std::fs::write(path, bytes).map_err(|e| format!("input: {e}"))?;
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};
    fn limits() -> Limits {
        Limits {
            max_record_bytes: 1024,
            max_header_bytes: 128,
            memory_budget_bytes: 536870912,
            output_budget_bytes: 1073741824,
        }
    }
    fn oracle(reader: Box<dyn BufRead + Send>, l: Limits) -> (Option<String>, u64, u64) {
        let mut f = super::super::Fastq {
            reader: Some(reader),
            receiver: None,
            pending: Vec::new().into_iter(),
            recycle_sender: None,
            worker: None,
            decoded_bytes: 0,
            digest: None,
            ordinal: 0,
            limits: l,
            recycled: None,
            separator: Vec::new(),
        };
        let error = loop {
            match f.record() {
                Ok(Some(r)) => f.recycle(r),
                Ok(None) => break None,
                Err(e) => break Some(e),
            }
        };
        (error, f.decoded_bytes, f.ordinal)
    }
    #[test]
    fn actual_index_parser_accounting_matches_rc3() {
        let cases: [&[u8]; 12] = [
            b"",
            b"@r\r\nA\r\n+\r\nI\r\n",
            b"@r\nA\n+\nI",
            b"@r\nA\n+\nI\r",
            b"@r",
            b"@r\nA",
            b"@r\nA\n+",
            b"@r\n\n+\n\n",
            b"bad\nA\nwrong\nII\n",
            b"bad\nA\nwrong\n",
            b"@r\nA\n+\n\x7fI\n",
            b"@r\n@\n+comment\n@\n",
        ];
        for data in cases {
            let expected = oracle(Box::new(Cursor::new(data.to_vec())), limits());
            let actual = index(&mut Cursor::new(data), limits(), 2, data.len() as u64, 0).unwrap();
            assert_eq!((actual.error, actual.decoded, actual.ordinal), expected);
        }
    }
    struct Fails {
        bytes: Cursor<Vec<u8>>,
        left: usize,
    }
    impl Read for Fails {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            if self.left == 0 {
                return Err(std::io::Error::other("h2 source probe"));
            }
            let n = b.len().min(self.left);
            let n = self.bytes.read(&mut b[..n])?;
            self.left -= n;
            Ok(n)
        }
    }
    #[test]
    fn ordered_source_error_precedence_matches_rc3() {
        for data in [
            b"bad\nA\nwrong\nI\n".to_vec(),
            [
                b"@r\nA\n+\nI\n".as_slice(),
                b"@h\n",
                &vec![b'A'; 1100],
                b"\n+\nI\n",
            ]
            .concat(),
        ] {
            for cut in 0..=data.len() {
                let make = || {
                    BufReader::with_capacity(
                        8192,
                        Fails {
                            bytes: Cursor::new(data.clone()),
                            left: cut,
                        },
                    )
                };
                let expected = oracle(Box::new(make()), limits());
                let actual = index(&mut make(), limits(), 2, data.len() as u64, 0).unwrap();
                assert_eq!(
                    (actual.error, actual.decoded, actual.ordinal),
                    expected,
                    "cut {cut}"
                );
            }
        }
    }
    #[test]
    fn injected_ordinal_origin_and_overflow_match_rc3() {
        let empty = index(&mut std::io::Cursor::new(b""), limits(), 2, 0, u64::MAX).unwrap();
        assert_eq!(empty.bounds, vec![(0, 0), (0, u64::MAX)]);
        let full = index(
            &mut std::io::Cursor::new(b"@r\nA\n+\nI\n"),
            limits(),
            2,
            9,
            u64::MAX,
        )
        .unwrap();
        assert_eq!(full.error.as_deref(), Some("arithmetic.overflow"));
        assert_eq!(full.decoded, 9);
        assert_eq!(full.ordinal, u64::MAX);
    }
    #[test]
    fn summary_and_resource_bounds_do_not_depend_on_record_count() {
        assert_eq!(std::mem::size_of::<Summary>(), 9176);
        assert_eq!(admission(12).unwrap(), 127367296);
        let c = IndexConfig {
            record: 1024,
            header: 128,
            s: 12,
            length: 1000000,
            ordinal: 0,
        };
        assert!(scan(b"x", u64::MAX, &c).is_err());
        assert!(merge(Summary::empty(0), Summary::empty(1), &c).is_err());
    }
}

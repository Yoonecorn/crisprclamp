//! Headerless ordinal-ordered counts, compatible with pc-ordinal-count-u64le-v1.
//! Publication belongs to the caller's existing private-stage protocol.
use super::*;

pub(super) const BUFFER_BYTES: usize = 96 * 1024;

pub(super) struct Writer<'a> {
    file: File,
    budget: &'a mut OutputBudget,
    buffer: Vec<u8>,
    bytes: u64,
    rows: u64,
}

impl<'a> Writer<'a> {
    pub(super) fn new(path: &Path, budget: &'a mut OutputBudget) -> Result<Self> {
        test_fault("output_open")?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| format!("publication.create: {e}"))?;
        Ok(Self {
            file,
            budget,
            buffer: Vec::with_capacity(BUFFER_BYTES),
            bytes: 0,
            rows: 0,
        })
    }

    pub(super) fn push(&mut self, value: u64) -> Result<()> {
        let rows = self.rows.checked_add(1).ok_or("arithmetic.overflow")?;
        let bytes = self.bytes.checked_add(8).ok_or("arithmetic.overflow")?;
        self.budget.charge(8, true)?;
        self.buffer.extend_from_slice(&value.to_le_bytes());
        self.rows = rows;
        self.bytes = bytes;
        if self.buffer.len() == BUFFER_BYTES {
            self.flush_buffer()?;
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            test_fault("output_write")?;
            self.file
                .write_all(&self.buffer)
                .map_err(|e| format!("publication.write: {e}"))?;
            self.buffer.clear();
        }
        Ok(())
    }

    /// Standard uses close-before-publication, without audited fsync semantics.
    pub(super) fn finish(mut self) -> Result<(u64, String, u64)> {
        self.flush_buffer()?;
        test_fault("output_flush")?;
        self.file
            .flush()
            .map_err(|e| format!("publication.flush: {e}"))?;
        test_fault("output_close")?;
        drop(self.file);
        Ok((self.bytes, String::new(), self.rows))
    }
}

#[derive(Debug)]
pub(super) struct VectorValidation {
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) sum: u64,
}

/// Independent bounded structural reader: exact length and checked sum.
/// This does not publish a vector or assume the caller's count sum is correct.
pub(super) fn validate_vector(
    path: &Path,
    rows: u64,
) -> Result<VectorValidation> {
    let bytes = rows.checked_mul(8).ok_or("arithmetic.overflow")?;
    let mut file = File::open(path).map_err(|e| format!("compact.output_open: {e}"))?;
    let actual_bytes = file
        .metadata()
        .map_err(|e| format!("compact.output_metadata: {e}"))?
        .len();
    if actual_bytes != bytes {
        return Err("compact.output_length".into());
    }
    let mut buffer = [0u8; BUFFER_BYTES];
    let mut remaining = bytes;
    let mut sum = 0u64;
    while remaining != 0 {
        let take = usize::try_from(remaining.min(BUFFER_BYTES as u64))
            .map_err(|_| "arithmetic.overflow")?;
        file.read_exact(&mut buffer[..take])
            .map_err(|e| format!("compact.output_read: {e}"))?;
        for encoded in buffer[..take].chunks_exact(8) {
            let value = u64::from_le_bytes(encoded.try_into().expect("eight-byte count"));
            sum = sum.checked_add(value).ok_or("arithmetic.overflow")?;
        }
        remaining = remaining.checked_sub(take as u64).ok_or("arithmetic.overflow")?;
    }
    let mut suffix = [0u8; 1];
    if file.read(&mut suffix).map_err(|e| format!("compact.output_read: {e}"))? != 0 {
        return Err("compact.output_length".into());
    }
    Ok(VectorValidation { rows, bytes, sum })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = env::temp_dir().join(format!(
                "cclamp-compact-output-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn path(&self) -> PathBuf { self.0.join("counts.u64le") }
    }
    impl Drop for Fixture {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }
    fn budget() -> OutputBudget {
        OutputBudget::new(Limits {
            max_record_bytes: 4096,
            max_header_bytes: 1024,
            memory_budget_bytes: 536_870_912,
            output_budget_bytes: 1_073_741_824,
        })
    }

    #[test]
    fn roundtrip_preserves_order_zeros_and_little_endian_across_blocks() {
        let fixture = Fixture::new();
        let mut budget = budget();
        let mut writer = Writer::new(&fixture.path(), &mut budget).unwrap();
        let count = BUFFER_BYTES / 8 + 3;
        let mut expected = Vec::new();
        for i in 0..count {
            let value = if i % 3 == 0 { 0 } else { i as u64 * 0x0102_0304 };
            writer.push(value).unwrap();
            assert!(writer.buffer.len() < BUFFER_BYTES);
            assert_eq!(writer.buffer.capacity(), BUFFER_BYTES);
            expected.extend_from_slice(&value.to_le_bytes());
        }
        let (bytes, digest, rows) = writer.finish().unwrap();
        assert_eq!(bytes, expected.len() as u64);
        assert_eq!(rows, count as u64);
        assert!(digest.is_empty());
        assert_eq!(fs::read(fixture.path()).unwrap(), expected);
        assert_eq!(budget.used(), bytes);
        let validated = validate_vector(&fixture.path(), rows).unwrap();
        assert_eq!(validated.rows, rows);
        assert_eq!(validated.bytes, bytes);
        assert_eq!(validated.sum, expected.chunks_exact(8).map(|b| u64::from_le_bytes(b.try_into().unwrap())).sum::<u64>());
    }

    #[test]
    fn empty_vector_has_no_digest_and_zero_sum() {
        let fixture = Fixture::new();
        let mut budget = budget();
        let (bytes, digest, rows) = Writer::new(&fixture.path(), &mut budget).unwrap().finish().unwrap();
        assert_eq!((bytes, rows), (0, 0));
        assert!(digest.is_empty());
        assert_eq!(validate_vector(&fixture.path(), 0).unwrap().sum, 0);
        assert!(Writer::new(&fixture.path(), &mut budget).is_err());
    }

    #[test]
    fn validator_rejects_truncation_trailing_bytes_and_sum_overflow() {
        let fixture = Fixture::new();
        for bytes in [vec![0; 7], vec![0; 9], vec![0; 16]] {
            fs::write(fixture.path(), &bytes).unwrap();
            assert_eq!(validate_vector(&fixture.path(), 1).unwrap_err(), "compact.output_length");
        }
        let bytes = 7u64.to_le_bytes();
        fs::write(fixture.path(), bytes).unwrap();
        assert_eq!(validate_vector(&fixture.path(), 1).unwrap().sum, 7);
        let values: Vec<u8> = [u64::MAX, 1].into_iter().flat_map(u64::to_le_bytes).collect();
        fs::write(fixture.path(), &values).unwrap();
        assert_eq!(validate_vector(&fixture.path(), 2).unwrap_err(), "arithmetic.overflow");
        assert_eq!(validate_vector(&fixture.path(), u64::MAX).unwrap_err(), "arithmetic.overflow");
    }

    #[test]
    fn writer_checks_counters_and_output_budget_before_append() {
        let fixture = Fixture::new();
        let mut budget = budget();
        let mut writer = Writer::new(&fixture.path(), &mut budget).unwrap();
        writer.rows = u64::MAX;
        assert_eq!(writer.push(1).unwrap_err(), "arithmetic.overflow");
        assert!(writer.buffer.is_empty());
        writer.rows = 0;
        writer.bytes = u64::MAX;
        assert_eq!(writer.push(1).unwrap_err(), "arithmetic.overflow");
        assert!(writer.buffer.is_empty());
        drop(writer);
        let mut tiny = OutputBudget::new(Limits { max_record_bytes: 4096, max_header_bytes: 1024,
            memory_budget_bytes: 536_870_912, output_budget_bytes: 65_540 });
        let path = fixture.0.join("limited.u64le");
        let mut writer = Writer::new(&path, &mut tiny).unwrap();
        assert_eq!(writer.push(1).unwrap_err(), "resource.output_budget");
        assert!(writer.buffer.is_empty());
    }
}

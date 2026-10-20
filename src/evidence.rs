use super::*;

const SCHEMA: &str = "project-c.evidence.v1";

#[derive(Clone)]
pub(super) enum EvidenceMode {
    Off,
    Full,
    Sampled { seed: String, capacity: usize },
}

impl EvidenceMode {
    pub(super) fn parse(values: &HashMap<String, String>) -> Result<Self> {
        let mode = values
            .get("--evidence")
            .map(String::as_str)
            .unwrap_or("off");
        match mode {
            "off" | "full"
                if values.contains_key("--evidence-seed")
                    || values.contains_key("--evidence-capacity") =>
            {
                Err("preflight.unexpected_evidence_option".into())
            }
            "off" => Ok(Self::Off),
            "full" => Ok(Self::Full),
            "sampled" => {
                let seed = values
                    .get("--evidence-seed")
                    .ok_or("preflight.missing_evidence_seed")?
                    .clone();
                u32::try_from(seed.len()).map_err(|_| "preflight.evidence_seed_length")?;
                let capacity = values
                    .get("--evidence-capacity")
                    .ok_or("preflight.missing_evidence_capacity")?
                    .parse::<u64>()
                    .map_err(|_| "preflight.invalid_evidence_capacity")?;
                Ok(Self::Sampled {
                    seed,
                    capacity: usize::try_from(capacity)
                        .map_err(|_| "preflight.invalid_evidence_capacity")?,
                })
            }
            _ => Err("preflight.unsupported_evidence".into()),
        }
    }
    pub(super) fn selector(&self) -> Value {
        match self {
            Self::Off => json!("off"),
            Self::Full => json!("full"),
            Self::Sampled { seed, capacity } => json!({"mode":"sampled","seed":seed,"E":capacity}),
        }
    }
    pub(super) fn capacity(&self) -> usize {
        match self {
            Self::Sampled { capacity, .. } => *capacity,
            _ => 0,
        }
    }
    pub(super) fn identity_bytes(&self, sample: &str) -> Result<usize> {
        u32::try_from(sample.len()).map_err(|_| "preflight.sample_id_length")?;
        let seed = match self {
            Self::Sampled { seed, .. } => seed.len(),
            _ => 0,
        };
        sample
            .len()
            .checked_add(seed)
            .ok_or("resource.dimension_overflow".into())
    }
}

struct Retained {
    priority: [u8; 32],
    ordinal: u64,
    event: Value,
}

pub(super) struct EvidenceSink {
    mode: EvidenceMode,
    sample: String,
    retained: Vec<Retained>,
    writer: Option<BufWriter<File>>,
    digest: Option<Sha256>,
    execution_profile: ExecutionProfile,
    bytes: u64,
    events: u64,
    concordance: Option<Value>,
}

fn priority(seed: &str, sample: &str, ordinal: u64) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"project-c.evidence.v1");
    for text in [seed, sample] {
        hash.update(
            u32::try_from(text.len())
                .map_err(|_| "resource.dimension_overflow")?
                .to_be_bytes(),
        );
        hash.update(text.as_bytes());
    }
    hash.update(ordinal.to_be_bytes());
    Ok(hash.finalize().into())
}

impl EvidenceSink {
    pub(super) fn new(
        mode: EvidenceMode,
        sample: &str,
        stage: &Path,
        budget: &mut OutputBudget,
        execution_profile: ExecutionProfile,
    ) -> Result<Self> {
        Self::new_with_concordance(mode, sample, stage, budget, execution_profile, None)
    }
    pub(super) fn new_with_concordance(
        mode: EvidenceMode,
        sample: &str,
        stage: &Path,
        budget: &mut OutputBudget,
        execution_profile: ExecutionProfile,
        concordance: Option<Value>,
    ) -> Result<Self> {
        let writer = if matches!(mode, EvidenceMode::Off) {
            None
        } else {
            Some(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(stage.join("evidence.jsonl"))
                    .map_err(|e| format!("evidence.create: {e}"))?,
            ))
        };
        let mut sink = Self {
            mode,
            sample: sample.into(),
            retained: Vec::new(),
            writer,
            digest: execution_profile.audited().then(Sha256::new),
            execution_profile,
            bytes: 0,
            events: 0,
            concordance,
        };
        if !matches!(sink.mode, EvidenceMode::Off) {
            sink.write(&row(SCHEMA, json!({"record_type":"selector","sample_id":sample,
                "selector":sink.mode.selector(),"complete":matches!(sink.mode, EvidenceMode::Full)})), budget)?;
        }
        Ok(sink)
    }
    pub(super) fn selected_priority(&self, ordinal: u64) -> Result<Option<Option<[u8; 32]>>> {
        match &self.mode {
            EvidenceMode::Off => Ok(None),
            EvidenceMode::Full => Ok(Some(None)),
            EvidenceMode::Sampled { seed, capacity } => {
                if *capacity == 0 {
                    return Ok(None);
                }
                let p = priority(seed, &self.sample, ordinal)?;
                if self.retained.len() < *capacity
                    || self
                        .retained
                        .iter()
                        .any(|r| (p, ordinal) < (r.priority, r.ordinal))
                {
                    Ok(Some(Some(p)))
                } else {
                    Ok(None)
                }
            }
        }
    }
    pub(super) fn observe(
        &mut self,
        ordinal: u64,
        priority: Option<[u8; 32]>,
        mut event: Value,
        budget: &mut OutputBudget,
    ) -> Result<()> {
        let object = event.as_object_mut().ok_or("evidence.invalid_event")?;
        object.insert("record_type".into(), json!("event"));
        object.insert("sample_id".into(), json!(self.sample));
        object.insert("ordinal".into(), json!(ordinal));
        object.insert("schema_id".into(), json!(SCHEMA));
        object.insert("profile_id".into(), json!(PROFILE));
        match self.mode.capacity() {
            0 if matches!(self.mode, EvidenceMode::Full) => self.write(&event, budget),
            _ if matches!(self.mode, EvidenceMode::Off) => unreachable!(),
            capacity => {
                let priority = priority.ok_or("evidence.missing_priority")?;
                let priority_hex: String =
                    priority.iter().map(|byte| format!("{byte:02x}")).collect();
                object.insert("priority_sha256".into(), json!(priority_hex));
                if self.retained.len() == capacity {
                    let worst = self
                        .retained
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, r)| (r.priority, r.ordinal))
                        .map(|(i, _)| i)
                        .ok_or("evidence.empty_capacity")?;
                    self.retained.swap_remove(worst);
                }
                self.retained.push(Retained {
                    priority,
                    ordinal,
                    event,
                });
                Ok(())
            }
        }
    }
    fn write(&mut self, value: &Value, budget: &mut OutputBudget) -> Result<()> {
        let mut active;
        let value = if let Some(bindings) = &self.concordance {
            active = value.clone();
            active["schema_id"] = json!(if bindings["barcode_radius"] == 1 {
                dual::RADIUS1_EVIDENCE
            } else {
                dual::CONCORDANCE_EVIDENCE
            });
            active["profile_id"] = json!(if bindings["barcode_radius"] == 1 {
                dual::RADIUS1_PROFILE
            } else {
                dual::CONCORDANCE_PROFILE
            });
            if active["record_type"] == "selector" {
                active["barcode_concordance"] = bindings.clone();
            }
            &active
        } else {
            value
        };
        let mut bytes =
            serde_json::to_vec(value).map_err(|e| format!("evidence.serialize: {e}"))?;
        bytes.push(b'\n');
        let length = u64::try_from(bytes.len()).map_err(|_| "resource.output_budget")?;
        budget.charge(length, true)?;
        test_fault("evidence_write")?;
        self.writer
            .as_mut()
            .ok_or("evidence.missing_writer")?
            .write_all(&bytes)
            .map_err(|e| format!("evidence.write: {e}"))?;
        if let Some(digest) = self.digest.as_mut() {
            digest.update(&bytes);
        }
        self.bytes = self
            .bytes
            .checked_add(length)
            .ok_or("arithmetic.overflow")?;
        self.events = self
            .events
            .checked_add(u64::from(value["record_type"] == "event"))
            .ok_or("arithmetic.overflow")?;
        Ok(())
    }
    pub(super) fn finish(mut self, budget: &mut OutputBudget) -> Result<Option<Value>> {
        if matches!(self.mode, EvidenceMode::Off) {
            return Ok(None);
        }
        self.retained.sort_by_key(|r| r.ordinal);
        for retained in std::mem::take(&mut self.retained) {
            self.write(&retained.event, budget)?;
        }
        let mut writer = self.writer.take().ok_or("evidence.missing_writer")?;
        if !self.execution_profile.audited() {
            test_fault("evidence_flush")?;
        }
        writer.flush().map_err(|e| format!("evidence.flush: {e}"))?;
        if self.execution_profile.audited() {
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| format!("evidence.sync: {e}"))?;
            drop(writer);
        } else {
            let file = writer
                .into_inner()
                .map_err(|e| format!("evidence.close: {e}"))?;
            test_fault("evidence_close")?;
            drop(file);
        }
        let mut metadata = if let Some(digest) = self.digest {
            json!({"schema_id":SCHEMA,"profile_id":PROFILE,"bytes":self.bytes,"sha256":format!("{:x}",digest.finalize()),"events":self.events})
        } else {
            json!({"schema_id":SCHEMA,"profile_id":PROFILE,"bytes":self.bytes,"events":self.events,"rows":self.events.checked_add(1).ok_or("arithmetic.overflow")?})
        };
        if let Some(bindings) = &self.concordance {
            metadata["schema_id"] = json!(if bindings["barcode_radius"] == 1 {
                dual::RADIUS1_EVIDENCE
            } else {
                dual::CONCORDANCE_EVIDENCE
            });
            metadata["profile_id"] = json!(if bindings["barcode_radius"] == 1 {
                dual::RADIUS1_PROFILE
            } else {
                dual::CONCORDANCE_PROFILE
            });
        }
        Ok(Some(metadata))
    }
}

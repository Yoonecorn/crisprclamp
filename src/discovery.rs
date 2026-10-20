use super::*;

const DOMAIN: &[u8] = b"project-c.discovery-key.v1";

#[derive(Clone)]
struct Retained {
    count: u64,
    states: Vec<String>,
    windows: Vec<String>,
}

struct LeafSample {
    leaf: &'static str,
    eligible: u64,
    retained: BTreeMap<([u8; 32], Vec<u8>), Retained>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DiscoveryMode {
    Bounded(usize),
    Exact(usize),
}

pub(super) struct Discovery {
    mode: DiscoveryMode,
    leaves: Vec<LeafSample>,
    reusable_key: Option<Vec<u8>>,
}

fn field(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| "resource.discovery_key_length")?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode(leaf: &str, states: &[&str], windows: &[&str]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    field(&mut bytes, leaf)?;
    bytes.extend_from_slice(
        &u32::try_from(states.len())
            .map_err(|_| "resource.discovery_key_length")?
            .to_be_bytes(),
    );
    for state in states {
        field(&mut bytes, state)?;
    }
    bytes.extend_from_slice(
        &u32::try_from(windows.len())
            .map_err(|_| "resource.discovery_key_length")?
            .to_be_bytes(),
    );
    for window in windows {
        field(&mut bytes, window)?;
    }
    Ok(bytes)
}

fn encode_into(bytes: &mut Vec<u8>, leaf: &str, states: &[&str], windows: &[&str]) -> Result<()> {
    bytes.clear();
    field(bytes, leaf)?;
    bytes.extend_from_slice(
        &u32::try_from(states.len())
            .map_err(|_| "resource.discovery_key_length")?
            .to_be_bytes(),
    );
    for state in states {
        field(bytes, state)?;
    }
    bytes.extend_from_slice(
        &u32::try_from(windows.len())
            .map_err(|_| "resource.discovery_key_length")?
            .to_be_bytes(),
    );
    for window in windows {
        field(bytes, window)?;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::new();
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

impl Discovery {
    pub(super) fn merge(&mut self, other: Self) -> Result<()> {
        let (my_k, exact) = match self.mode {
            DiscoveryMode::Bounded(k) => (k, false),
            DiscoveryMode::Exact(k) => (k, true),
        };
        let other_k = match other.mode {
            DiscoveryMode::Bounded(k) => k,
            DiscoveryMode::Exact(k) => k,
        };
        if my_k != other_k
            || exact != matches!(other.mode, DiscoveryMode::Exact(_))
            || self.leaves.len() != other.leaves.len()
        {
            return Err("conservation.discovery_merge".into());
        }
        for (target, source) in self.leaves.iter_mut().zip(other.leaves) {
            if target.leaf != source.leaf {
                return Err("conservation.discovery_merge".into());
            }
            target.eligible = target
                .eligible
                .checked_add(source.eligible)
                .ok_or("arithmetic.overflow")?;
            for (key, value) in source.retained {
                if let Some(existing) = target.retained.get_mut(&key) {
                    existing.count = existing
                        .count
                        .checked_add(value.count)
                        .ok_or("arithmetic.overflow")?;
                } else if exact {
                    target.retained.insert(key, value);
                } else if my_k != 0 {
                    if target.retained.len() == my_k {
                        let worst = target
                            .retained
                            .last_key_value()
                            .expect("nonzero K")
                            .0
                            .clone();
                        if key >= worst {
                            continue;
                        }
                        target.retained.remove(&worst);
                    }
                    target.retained.insert(key, value);
                }
            }
        }
        if exact {
            let total_keys: usize = self.leaves.iter().map(|l| l.retained.len()).sum();
            if total_keys > my_k {
                return Err("resource.discovery_limit".into());
            }
        }
        Ok(())
    }
    pub(super) fn new(mode: DiscoveryMode, names: &[&'static str]) -> Self {
        Self {
            mode,
            reusable_key: None,
            leaves: names
                .iter()
                .map(|leaf| LeafSample {
                    leaf,
                    eligible: 0,
                    retained: BTreeMap::new(),
                })
                .collect(),
        }
    }

    // Worker-local scratch avoids allocating a key for each rejected unknown.
    // The synchronous discovery path retains the legacy implementation.
    pub(super) fn worker_local(mode: DiscoveryMode, names: &[&'static str]) -> Self {
        let mut discovery = Self::new(mode, names);
        discovery.reusable_key = Some(Vec::new());
        discovery
    }

    pub(super) fn observe(
        &mut self,
        leaf_index: usize,
        states: &[&str],
        windows: &[&str],
    ) -> Result<()> {
        let (k, exact) = match self.mode {
            DiscoveryMode::Bounded(k) => (k, false),
            DiscoveryMode::Exact(k) => (k, true),
        };
        let encoded = {
            let leaf = self
                .leaves
                .get_mut(leaf_index)
                .ok_or("conservation.discovery_leaf")?;
            encode(leaf.leaf, states, windows)?
        };
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(&encoded);
        let digest: [u8; 32] = hash.finalize().into();
        let priority = (digest, encoded);

        if exact {
            let total_keys: usize = self.leaves.iter().map(|l| l.retained.len()).sum();
            let leaf = self.leaves.get_mut(leaf_index).unwrap();
            increment(&mut leaf.eligible)?;
            if let Some(retained) = leaf.retained.get_mut(&priority) {
                return increment(&mut retained.count);
            }
            if total_keys == k {
                return Err("resource.discovery_limit".into());
            }
            leaf.retained.insert(
                priority,
                Retained {
                    count: 1,
                    states: states.iter().map(|s| (*s).to_owned()).collect(),
                    windows: windows.iter().map(|w| (*w).to_owned()).collect(),
                },
            );
            return Ok(());
        }

        let leaf = self.leaves.get_mut(leaf_index).unwrap();
        increment(&mut leaf.eligible)?;
        if k == 0 {
            return Ok(());
        }
        if let Some(retained) = leaf.retained.get_mut(&priority) {
            return increment(&mut retained.count);
        }
        if leaf.retained.len() == k {
            let worst = leaf.retained.last_key_value().expect("nonzero K").0.clone();
            if priority >= worst {
                return Ok(());
            }
            leaf.retained.remove(&worst);
        }
        leaf.retained.insert(
            priority,
            Retained {
                count: 1,
                states: states.iter().map(|s| (*s).to_owned()).collect(),
                windows: windows.iter().map(|w| (*w).to_owned()).collect(),
            },
        );
        Ok(())
    }

    pub(super) fn observe_worker(
        &mut self,
        leaf_index: usize,
        states: &[&str],
        windows: &[&str],
    ) -> Result<()> {
        let (k, exact) = match self.mode {
            DiscoveryMode::Bounded(k) => (k, false),
            DiscoveryMode::Exact(k) => (k, true),
        };
        let encoded = {
            let leaf = self
                .leaves
                .get_mut(leaf_index)
                .ok_or("conservation.discovery_leaf")?;
            if let Some(bytes) = self.reusable_key.as_mut() {
                encode_into(bytes, leaf.leaf, states, windows)?;
                std::mem::take(bytes)
            } else {
                encode(leaf.leaf, states, windows)?
            }
        };
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(&encoded);
        let digest: [u8; 32] = hash.finalize().into();
        let priority = (digest, encoded);

        if exact {
            let total_keys: usize = self.leaves.iter().map(|l| l.retained.len()).sum();
            let leaf = self.leaves.get_mut(leaf_index).unwrap();
            increment(&mut leaf.eligible)?;
            if let Some(retained) = leaf.retained.get_mut(&priority) {
                return increment(&mut retained.count);
            }
            if total_keys == k {
                return Err("resource.discovery_limit".into());
            }
            leaf.retained.insert(
                priority,
                Retained {
                    count: 1,
                    states: states.iter().map(|s| (*s).to_owned()).collect(),
                    windows: windows.iter().map(|w| (*w).to_owned()).collect(),
                },
            );
            return Ok(());
        }

        let leaf = self.leaves.get_mut(leaf_index).unwrap();
        increment(&mut leaf.eligible)?;
        if k == 0 {
            if let Some(bytes) = self.reusable_key.as_mut() {
                *bytes = priority.1;
            }
            return Ok(());
        }
        if let Some(retained) = leaf.retained.get_mut(&priority) {
            if let Some(bytes) = self.reusable_key.as_mut() {
                *bytes = priority.1;
            }
            return increment(&mut retained.count);
        }
        if leaf.retained.len() == k {
            if self.reusable_key.is_some() {
                if &priority >= leaf.retained.last_key_value().expect("nonzero K").0 {
                    self.reusable_key = Some(priority.1);
                    return Ok(());
                }
                let (worst, _) = leaf.retained.pop_last().expect("nonzero K");
                self.reusable_key = Some(worst.1);
            } else {
                let worst = leaf.retained.last_key_value().expect("nonzero K").0.clone();
                if priority >= worst {
                    return Ok(());
                }
                leaf.retained.remove(&worst);
            }
        }
        leaf.retained.insert(
            priority,
            Retained {
                count: 1,
                states: states.iter().map(|s| (*s).to_owned()).collect(),
                windows: windows.iter().map(|w| (*w).to_owned()).collect(),
            },
        );
        Ok(())
    }

    pub(super) fn eligible(&self, index: usize) -> u64 {
        self.leaves[index].eligible
    }

    pub(super) fn rows(&self, sample: &str, mode: &str) -> Result<Vec<Value>> {
        let mut output = Vec::new();
        let (k, exact) = match self.mode {
            DiscoveryMode::Bounded(k) => (k, false),
            DiscoveryMode::Exact(k) => (k, true),
        };
        for leaf in &self.leaves {
            let retained_mass = leaf.retained.values().try_fold(0_u64, |sum, item| {
                sum.checked_add(item.count).ok_or("arithmetic.overflow")
            })?;
            let tail = leaf
                .eligible
                .checked_sub(retained_mass)
                .ok_or("conservation.discovery_mass")?;
            let mut items: Vec<_> = leaf.retained.iter().collect();
            items.sort_by(|(a, left), (b, right)| {
                right.count.cmp(&left.count).then_with(|| a.1.cmp(&b.1))
            });
            let rows = items.into_iter().map(|((digest, encoded), value)| {
                let priority = hex(digest);
                json!({"typed_key":{"terminal_leaf":leaf.leaf,"role_states":value.states,"windows":value.windows},
                    "exact_count":value.count,"priority":priority,"priority_sha256":priority,
                    "encoded_key_hex":hex(encoded)})
            }).collect::<Vec<_>>();
            let (discovery_mode, tail_keys) = if exact {
                ("exact", json!(0))
            } else {
                ("bounded", json!("unknown"))
            };
            output.push(row("project-c.discovery.v1", json!({
                "sample_id":sample,"mode":mode,"eligible_leaf":leaf.leaf,"discovery_mode":discovery_mode,
                "K_or_limit":k,"eligible_records":leaf.eligible,
                "retained_record_mass":retained_mass,"tail_record_mass":tail,
                "tail_distinct_keys":tail_keys,"rows":rows
            })));
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_encoding_and_priority() {
        let encoded = encode("unmapped.no_match", &["unknown"], &["TTTT"]).unwrap();
        assert_eq!(
            hex(&encoded),
            "00000011756e6d61707065642e6e6f5f6d617463680000000100000007756e6b6e6f776e000000010000000454545454"
        );
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(&encoded);
        assert_eq!(
            hex(&hash.finalize()),
            "567f3a664002c8bd563631aa7cbad47454c8a23c289d360e8ebfd680375b581d"
        );
    }

    #[test]
    fn partition_merge_and_checked_overflow() {
        let mut left = Discovery::new(DiscoveryMode::Bounded(2), &["unmapped.no_match"]);
        let mut right = Discovery::new(DiscoveryMode::Bounded(2), &["unmapped.no_match"]);
        let mut serial = Discovery::new(DiscoveryMode::Bounded(2), &["unmapped.no_match"]);
        for window in ["AAAA", "CCCC", "AAAA", "GGGG", "TTTT", "GGGG"] {
            serial.observe(0, &["unknown"], &[window]).unwrap();
        }
        for window in ["AAAA", "CCCC", "AAAA"] {
            left.observe(0, &["unknown"], &[window]).unwrap();
        }
        for window in ["GGGG", "TTTT", "GGGG"] {
            right.observe(0, &["unknown"], &[window]).unwrap();
        }
        left.merge(right).unwrap();
        assert_eq!(
            left.rows("s", "single").unwrap(),
            serial.rows("s", "single").unwrap()
        );
        left.leaves[0].eligible = u64::MAX;
        assert_eq!(
            left.observe(0, &["unknown"], &["AAAA"]).unwrap_err(),
            "arithmetic.overflow"
        );
        let mut other = Discovery::new(DiscoveryMode::Bounded(2), &["unmapped.no_match"]);
        other.observe(0, &["unknown"], &["AAAA"]).unwrap();
        assert_eq!(left.merge(other).unwrap_err(), "arithmetic.overflow");
    }
}

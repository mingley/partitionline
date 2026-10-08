//! Bounded DeleteRecords0–2 and explicitly enabled ordinary RF1 retention.
//!
//! The retention router adds API21 only with rolling storage. Its confirmed high
//! watermark is the exclusive locally synchronized ordinary log end; there is
//! no replicated or transactional safety claim. A successful deletion durably
//! advances the logical start before reclaiming whole segments. A containing
//! batch is kept intact, while Fetch rejects offsets below the logical start.
//! Age/size sweeps are explicit bounded actor operations, with disabled defaults.

use crate::{catalog::Catalog, fetch, metadata, produce, protocol};

/// Separately selected read/write/retention profile; legacy profiles stay intact.
pub static DATA_API_VERSIONS: [protocol::ApiVersion; 8] = [
    fetch::DATA_API_VERSIONS[0],
    fetch::DATA_API_VERSIONS[1],
    fetch::DATA_API_VERSIONS[2],
    fetch::DATA_API_VERSIONS[3],
    fetch::DATA_API_VERSIONS[4],
    fetch::DATA_API_VERSIONS[5],
    fetch::DATA_API_VERSIONS[6],
    protocol::ApiVersion {
        api_key: 21,
        min_version: 0,
        max_version: 2,
    },
];

/// Explicit sweep policy and positive work ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Largest-record CreateTime age; None disables age retention.
    /// Expiration is strict: now minus the largest timestamp must exceed this.
    /// Entirely negative/unknown record times are preserved by age retention;
    /// unlike Apache, this policy does not infer age from mutable file mtime.
    pub retention_ms: Option<u64>,
    /// Retained Kafka record payload bytes; journal/index headers are excluded.
    /// None disables size retention. Physical disk budgets remain separate.
    pub retention_bytes: Option<u64>,
    /// Maximum whole prefix segments removed per store, 1..=1024.
    /// Candidate scans also respect the backend's total segment/work bounds.
    pub max_segments_per_store: usize,
    /// Maximum historical store slots visited per explicit sweep, 1..=4096.
    /// Deleted catalog identities are visited but never changed by this policy.
    pub max_sweep_stores: usize,
}
impl Config {
    /// Validate policy thresholds and bounded work; zero age/size is valid.
    pub fn validate(self) -> Result<(), Error> {
        if self.retention_ms.is_some_and(|n| n > i64::MAX as u64)
            || self.retention_bytes.is_some_and(|n| n > 1 << 40)
            || !(1..=1024).contains(&self.max_segments_per_store)
            || !(1..=4096).contains(&self.max_sweep_stores)
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            retention_ms: None,
            retention_bytes: None,
            max_segments_per_store: 1024,
            max_sweep_stores: 64,
        }
    }
}

/// Completed explicit sweep work, including idempotent unchanged stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SweepReport {
    /// Historical slots visited within the configured work ceiling.
    pub stores_visited: usize,
    /// Active identities whose logical start advanced or files were reclaimed.
    pub stores_changed: usize,
    /// Removed data/index files, excluding atomic manifest replacement.
    pub reclaimed_files: usize,
    /// Removed data/index file bytes, excluding atomic manifest replacement.
    pub reclaimed_bytes: u64,
}

/// Structural and admission failures produce no partial wire response.
#[derive(Debug)]
pub enum Error {
    /// Invalid policy or work limits, or retention was not explicitly enabled.
    InvalidConfig,
    /// Complete bounded header/body parsing failed.
    Protocol(protocol::Error),
    /// This module accepts only API21 versions0..=2.
    UnsupportedVersion,
    /// Aggregate topic/partition count exceeds its byte or configured bound.
    RequestCount,
    /// Response size/reservation or checked aggregate accounting failed.
    ResourceLimit,
    /// Caller canceled or the actor is stopping before the next storage step.
    Canceled,
    /// An explicit sweep failed storage safety, publication or cleanup.
    Storage(crate::partition::Error),
}
impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "retention storage: {error}"),
            other => write!(f, "retention: {other:?}"),
        }
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy)]
struct Target<'a> {
    name: &'a str,
    index: i32,
    offset: i64,
    sequence: usize,
}
struct Reader<'a> {
    bytes: &'a [u8],
    tags: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let result = self.bytes.get(..n).ok_or(protocol::Error::Truncated)?;
        self.bytes = &self.bytes[n..];
        Ok(result)
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| protocol::Error::Truncated)?,
        ))
    }
    fn varint(&mut self) -> Result<u32, Error> {
        let mut value = 0;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.take(1)?[0];
            if shift == 28 && byte > 15 {
                return Err(protocol::Error::InvalidVarint.into());
            }
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(protocol::Error::InvalidVarint.into())
    }
    fn count(&mut self, flex: bool, maximum: usize, minimum: usize) -> Result<usize, Error> {
        let count = if flex {
            usize::try_from(self.varint()?)
                .ok()
                .and_then(|n| n.checked_sub(1))
        } else {
            usize::try_from(self.i32()?).ok()
        }
        .ok_or(protocol::Error::InvalidLength)?;
        if count > maximum || count > self.bytes.len() / minimum {
            return Err(Error::RequestCount);
        }
        Ok(count)
    }
    fn string(&mut self, flex: bool) -> Result<&'a str, Error> {
        let count = if flex {
            usize::try_from(self.varint()?)
                .ok()
                .and_then(|n| n.checked_sub(1))
        } else {
            usize::try_from(self.i16()?).ok()
        }
        .filter(|n| *n <= 32767)
        .ok_or(protocol::Error::InvalidLength)?;
        Ok(std::str::from_utf8(self.take(count)?).map_err(|_| protocol::Error::InvalidUtf8)?)
    }
    fn tags(&mut self) -> Result<(), Error> {
        let count = usize::try_from(self.varint()?).map_err(|_| Error::RequestCount)?;
        if count > self.tags || count > self.bytes.len() / 2 {
            return Err(protocol::Error::TooManyTags.into());
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|n| tag <= n) {
                return Err(protocol::Error::InvalidTagOrder.into());
            }
            previous = Some(tag);
            let length = usize::try_from(self.varint()?).map_err(|_| Error::RequestCount)?;
            self.take(length)?;
        }
        Ok(())
    }
}
fn parse<'a>(
    body: &'a [u8],
    flex: bool,
    common: &metadata::Config,
    max_partitions: usize,
) -> Result<Vec<Target<'a>>, Error> {
    let mut reader = Reader {
        bytes: body,
        tags: common.protocol_limits.max_tagged_fields(),
    };
    let count = reader.count(flex, common.max_topics, if flex { 3 } else { 6 })?;
    let mut targets = Vec::new();
    if count != 0 {
        // Reserve one bounded upper bound; many one-partition topics must not
        // cause repeated copying while growing a flattened request.
        targets
            .try_reserve_exact(max_partitions.min(reader.bytes.len() / if flex { 13 } else { 12 }))
            .map_err(|_| Error::ResourceLimit)?;
    }
    for _ in 0..count {
        let name = reader.string(flex)?;
        let count = reader.count(flex, max_partitions, if flex { 13 } else { 12 })?;
        if targets
            .len()
            .checked_add(count)
            .is_none_or(|n| n > max_partitions)
        {
            return Err(Error::RequestCount);
        }
        for _ in 0..count {
            let index = reader.i32()?;
            let offset = reader.i64()?;
            if flex {
                reader.tags()?;
            }
            targets.push(Target {
                name,
                index,
                offset,
                sequence: targets.len(),
            });
        }
        if flex {
            reader.tags()?;
        }
    }
    let _timeout = reader.i32()?;
    if flex {
        reader.tags()?;
    }
    if !reader.bytes.is_empty() {
        return Err(protocol::Error::TrailingBytes.into());
    }
    // KafkaApis builds a TopicPartition map: the last supplied offset wins.
    // Response ordering is unspecified upstream. This broker sorts by name/index.
    targets
        .sort_unstable_by(|a, b| (a.name, a.index, a.sequence).cmp(&(b.name, b.index, b.sequence)));
    let mut count = 0;
    for position in 0..targets.len() {
        let current = targets[position];
        if count != 0
            && (targets[count - 1].name, targets[count - 1].index) == (current.name, current.index)
        {
            targets[count - 1] = current;
        } else {
            targets[count] = current;
            count += 1;
        }
    }
    targets.truncate(count);
    Ok(targets)
}
fn varint_size(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 128 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}
fn response_size(targets: &[Target<'_>], flex: bool) -> Result<usize, Error> {
    let topic_count = targets
        .iter()
        .enumerate()
        .filter(|(i, t)| *i == 0 || targets[*i - 1].name != t.name)
        .count();
    let mut size = 8usize
        .checked_add(if flex {
            2 + varint_size(topic_count + 1)
        } else {
            4
        })
        .ok_or(Error::ResourceLimit)?;
    let mut at = 0;
    while at < targets.len() {
        let end = at
            + targets[at..]
                .iter()
                .take_while(|t| t.name == targets[at].name)
                .count();
        let count = end - at;
        size = size
            .checked_add(targets[at].name.len())
            .and_then(|n| {
                n.checked_add(if flex {
                    varint_size(targets[at].name.len() + 1) + varint_size(count + 1) + 1
                } else {
                    6
                })
            })
            .and_then(|n| {
                count
                    .checked_mul(if flex { 15 } else { 14 })
                    .and_then(|p| n.checked_add(p))
            })
            .ok_or(Error::ResourceLimit)?;
        at = end;
    }
    Ok(size)
}
struct Writer {
    bytes: Vec<u8>,
    planned: usize,
}
impl Writer {
    fn new(planned: usize, maximum: usize) -> Result<Self, Error> {
        if planned > maximum {
            return Err(Error::ResourceLimit);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(planned)
            .map_err(|_| Error::ResourceLimit)?;
        Ok(Self { bytes, planned })
    }
    fn bytes(&mut self, value: &[u8]) -> Result<(), Error> {
        if self
            .bytes
            .len()
            .checked_add(value.len())
            .is_none_or(|n| n > self.planned)
        {
            return Err(Error::ResourceLimit);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    fn varint(&mut self, mut value: u32) -> Result<(), Error> {
        loop {
            let byte = (value & 127) as u8;
            value >>= 7;
            self.bytes(&[if value == 0 { byte } else { byte | 128 }])?;
            if value == 0 {
                return Ok(());
            }
        }
    }
    fn count(&mut self, count: usize, flex: bool) -> Result<(), Error> {
        if flex {
            self.varint(
                u32::try_from(count)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Error::ResourceLimit)?,
            )
        } else {
            self.bytes(
                &i32::try_from(count)
                    .map_err(|_| Error::ResourceLimit)?
                    .to_be_bytes(),
            )
        }
    }
    fn string(&mut self, name: &str, flex: bool) -> Result<(), Error> {
        if flex {
            self.count(name.len(), true)?;
        } else {
            self.bytes(
                &i16::try_from(name.len())
                    .map_err(|_| Error::ResourceLimit)?
                    .to_be_bytes(),
            )?;
        }
        self.bytes(name.as_bytes())
    }
}

pub(crate) fn process(
    catalog: &Catalog,
    store: &mut produce::Store,
    input: &[u8],
    common: &metadata::Config,
    config: Config,
    active: impl Fn() -> Result<(), Error>,
) -> Result<Vec<u8>, Error> {
    config.validate()?;
    active()?;
    let version = i16::from_be_bytes(
        input
            .get(2..4)
            .ok_or(protocol::Error::Truncated)?
            .try_into()
            .map_err(|_| protocol::Error::Truncated)?,
    );
    let flex = version >= 2;
    let (header, body) =
        protocol::RequestHeader::parse(input, if flex { 2 } else { 1 }, common.protocol_limits)?;
    if header.api_key != 21 || !(0..=2).contains(&version) {
        return Err(Error::UnsupportedVersion);
    }
    let targets = parse(body, flex, common, store.max_partitions())?;
    let mut out = Writer::new(response_size(&targets, flex)?, common.max_response_bytes)?;
    let topic_count = targets
        .iter()
        .enumerate()
        .filter(|(i, t)| *i == 0 || targets[*i - 1].name != t.name)
        .count();
    out.bytes(&header.correlation_id.to_be_bytes())?;
    if flex {
        out.bytes(&[0])?;
    }
    // ThrottleTimeMs is present even in version0.
    out.bytes(&0i32.to_be_bytes())?;
    out.count(topic_count, flex)?;
    let mut at = 0;
    while at < targets.len() {
        let end = at
            + targets[at..]
                .iter()
                .take_while(|t| t.name == targets[at].name)
                .count();
        out.string(targets[at].name, flex)?;
        out.count(end - at, flex)?;
        for target in &targets[at..end] {
            active()?;
            let result = match catalog.by_name(target.name) {
                None => Err(3i16),
                Some(topic)
                    if target.index < 0 || target.index as u32 >= topic.partition_count() =>
                {
                    Err(3)
                }
                Some(topic)
                    if matches!(
                        topic.name(),
                        "__consumer_offsets" | "__transaction_state" | "__share_group_state"
                    ) =>
                {
                    Err(17)
                }
                Some(topic) => store.delete_records(topic.id(), target.index, target.offset),
            };
            let (low_watermark, error) = match result {
                Ok(low_watermark) => (low_watermark, 0),
                Err(error) => (-1, error),
            };
            out.bytes(&target.index.to_be_bytes())?;
            out.bytes(&low_watermark.to_be_bytes())?;
            out.bytes(&error.to_be_bytes())?;
            if flex {
                out.bytes(&[0])?;
            }
        }
        if flex {
            out.bytes(&[0])?;
        }
        at = end;
    }
    if flex {
        out.bytes(&[0])?;
    }
    active()?;
    if out.bytes.len() != out.planned {
        return Err(Error::ResourceLimit);
    }
    Ok(out.bytes)
}

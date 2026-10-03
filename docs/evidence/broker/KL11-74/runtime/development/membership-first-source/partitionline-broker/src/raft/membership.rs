//! Bounded directory-aware voter configurations for the explicit dynamic profile.
//!
//! Kafka's one-at-a-time voter changes use the newly appended set before commit.
//! This module describes that set and a strict custom local record; it is not a
//! Kafka control-batch codec. Configuration epochs count accepted voter records,
//! while positions use the existing normalized term and inclusive one-based index.

use super::election::LogPosition;

const MAGIC: &[u8; 8] = b"PLVOTR01";
const HEADER: usize = 40;
/// Maximum active voters, matching the existing election resource ceiling.
pub const MAX_VOTERS: usize = 64;
/// Maximum listener endpoints retained per configured voter.
pub const MAX_ENDPOINTS: usize = 4;
/// Maximum encoded configuration, before any peer-sized allocation.
pub const MAX_CONFIGURATION_BYTES: usize = 132 * 1024;

/// Exact configured replica identity; directories are mandatory in this profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    /// Nonnegative Kafka node ID.
    pub id: u32,
    /// Nonzero directory UUID bytes in network order.
    pub directory: [u8; 16],
}
impl Key {
    /// Reject IDs outside Kafka's signed domain and missing directory identity.
    pub fn new(id: u32, directory: [u8; 16]) -> Result<Self, Error> {
        if id > i32::MAX as u32 || directory == [0; 16] {
            return Err(Error::InvalidIdentity);
        }
        Ok(Self { id, directory })
    }
}

/// Bounded declared listener address; this type performs no hostname resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    listener: String,
    host: String,
    port: u16,
}
impl Endpoint {
    /// Validate an uppercase listener name, bounded UTF-8 host and positive port.
    ///
    /// The local profile admits at most249 bytes per name/host; broader listener
    /// syntax is deliberately outside this profile rather than silently normalized.
    pub fn new(listener: String, host: String, port: u16) -> Result<Self, Error> {
        if listener.is_empty()
            || listener.len() > 249
            || !listener
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b"_.-".contains(&b))
            || host.is_empty()
            || host.len() > 249
            || host.contains('\0')
            || port == 0
        {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self {
            listener,
            host,
            port,
        })
    }
    /// Exact normalized listener name.
    pub fn listener(&self) -> &str {
        &self.listener
    }
    /// Declared bounded host, with no implied DNS or reachability proof.
    pub fn host(&self) -> &str {
        &self.host
    }
    /// Positive configured TCP port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// One configured voter and the feature range supplied by a correlated probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voter {
    key: Key,
    endpoints: Vec<Endpoint>,
    kraft_min: i16,
    kraft_max: i16,
}
impl Voter {
    /// Validate identity, one to four distinct listeners and a nonnegative range.
    pub fn new(
        key: Key,
        mut endpoints: Vec<Endpoint>,
        kraft_min: i16,
        kraft_max: i16,
    ) -> Result<Self, Error> {
        Key::new(key.id, key.directory)?;
        if endpoints.is_empty() || endpoints.len() > MAX_ENDPOINTS {
            return Err(Error::InvalidEndpoint);
        }
        endpoints.sort_unstable_by(|a, b| a.listener.cmp(&b.listener));
        if endpoints
            .windows(2)
            .any(|pair| pair[0].listener == pair[1].listener)
            || kraft_min < 0
            || kraft_max < kraft_min
        {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self {
            key,
            endpoints,
            kraft_min,
            kraft_max,
        })
    }
    /// Directory-fenced voter identity.
    pub fn key(&self) -> Key {
        self.key
    }
    /// Sorted bounded listener endpoints.
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }
    /// Advertised inclusive minimum kraft.version.
    pub fn kraft_min(&self) -> i16 {
        self.kraft_min
    }
    /// Advertised inclusive maximum kraft.version.
    pub fn kraft_max(&self) -> i16 {
        self.kraft_max
    }
    /// Whether a requested finalized feature is in this advertised range.
    pub fn supports(&self, version: i16) -> bool {
        self.kraft_min <= version && version <= self.kraft_max
    }
    /// Find the exact configured default listener required for addition.
    pub fn endpoint(&self, listener: &str) -> Option<&Endpoint> {
        self.endpoints.iter().find(|e| e.listener == listener)
    }
}

/// A canonical complete configuration, including its authoritative log position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Voters {
    epoch: u64,
    position: LogPosition,
    feature: i16,
    voters: Vec<Voter>,
}
impl Voters {
    /// Construct the explicit kraft.version1 profile and sorted unique voter IDs.
    /// Epoch0/position0 is the immutable bootstrap; later epochs require a log entry.
    pub fn new(
        epoch: u64,
        position: LogPosition,
        feature: i16,
        mut voters: Vec<Voter>,
    ) -> Result<Self, Error> {
        if feature != 1 {
            return Err(Error::UnsupportedFeature);
        }
        if voters.is_empty()
            || voters.len() > MAX_VOTERS
            || (epoch == 0) != (position == LogPosition::default())
            || (epoch > 0 && (position.term == 0 || position.index == 0))
        {
            return Err(Error::InvalidConfiguration);
        }
        voters.sort_unstable_by_key(|v| v.key.id);
        if voters
            .windows(2)
            .any(|pair| pair[0].key.id == pair[1].key.id)
        {
            return Err(Error::DuplicateVoter);
        }
        if voters.iter().any(|v| !v.supports(feature)) {
            return Err(Error::UnsupportedFeature);
        }
        let result = Self {
            epoch,
            position,
            feature,
            voters,
        };
        if result.encoded_size()? > MAX_CONFIGURATION_BYTES {
            return Err(Error::Bounds);
        }
        Ok(result)
    }
    /// Local configuration record count, separate from the election term.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Inclusive authoritative position; zero is the immutable bootstrap.
    pub fn position(&self) -> LogPosition {
        self.position
    }
    /// Explicit finalized kraft.version, currently only1.
    pub fn feature(&self) -> i16 {
        self.feature
    }
    /// Canonical sorted voter descriptors.
    pub fn voters(&self) -> &[Voter] {
        &self.voters
    }
    /// Strict majority of this set, including after an uncommitted change.
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
    /// Find a voter by ID; callers must still compare its directory identity.
    pub fn by_id(&self, id: u32) -> Option<&Voter> {
        self.voters
            .binary_search_by_key(&id, |v| v.key.id)
            .ok()
            .map(|p| &self.voters[p])
    }
    /// Test the complete ID and directory identity.
    pub fn contains(&self, key: Key) -> bool {
        self.by_id(key.id).is_some_and(|v| v.key == key)
    }
    /// Append exactly one new ID, preserving all existing descriptors.
    pub fn add(&self, voter: Voter, position: LogPosition) -> Result<Self, Error> {
        if self.by_id(voter.key.id).is_some() {
            return Err(Error::DuplicateVoter);
        }
        if self.voters.len() == MAX_VOTERS {
            return Err(Error::Bounds);
        }
        let mut voters = self.voters.clone();
        voters.push(voter);
        self.successor(position, voters)
    }
    /// Remove one exact identity, preserving at least one remaining voter.
    pub fn remove(&self, key: Key, position: LogPosition) -> Result<Self, Error> {
        if !self.contains(key) {
            return Err(Error::VoterNotFound);
        }
        if self.voters.len() == 1 {
            return Err(Error::LastVoter);
        }
        let voters = self
            .voters
            .iter()
            .filter(|v| v.key != key)
            .cloned()
            .collect();
        self.successor(position, voters)
    }
    fn successor(&self, position: LogPosition, voters: Vec<Voter>) -> Result<Self, Error> {
        if position.index <= self.position.index || position.term < self.position.term {
            return Err(Error::InvalidConfiguration);
        }
        Self::new(
            self.epoch.checked_add(1).ok_or(Error::Bounds)?,
            position,
            self.feature,
            voters,
        )
    }
    /// Check exactly one add/remove; arbitrary set jumps and descriptor edits reject.
    pub fn validate_successor(&self, next: &Self) -> Result<(), Error> {
        if next.epoch != self.epoch.checked_add(1).ok_or(Error::Bounds)?
            || next.position.index <= self.position.index
            || next.position.term < self.position.term
            || next.feature != self.feature
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut removed = 0;
        let mut added = 0;
        for old in &self.voters {
            match next.by_id(old.key.id) {
                Some(new) if new == old => {}
                Some(_) => return Err(Error::InvalidConfiguration),
                None => removed += 1,
            }
        }
        for new in &next.voters {
            if self.by_id(new.key.id).is_none() {
                added += 1;
            }
        }
        if (removed, added) != (1, 0) && (removed, added) != (0, 1) {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
    fn encoded_size(&self) -> Result<usize, Error> {
        self.voters.iter().try_fold(HEADER, |bytes, voter| {
            voter.endpoints.iter().try_fold(
                bytes.checked_add(28).ok_or(Error::Bounds)?,
                |bytes, endpoint| {
                    bytes
                        .checked_add(6 + endpoint.listener.len() + endpoint.host.len())
                        .ok_or(Error::Bounds)
                },
            )
        })
    }
    /// Encode the strict bounded custom local Voters record, excluding WAL framing.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let size = self.encoded_size()?;
        if size > MAX_CONFIGURATION_BYTES {
            return Err(Error::Bounds);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| Error::Allocation)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        bytes.extend_from_slice(&self.position.term.to_be_bytes());
        bytes.extend_from_slice(&self.position.index.to_be_bytes());
        bytes.extend_from_slice(&self.feature.to_be_bytes());
        bytes.extend_from_slice(&(self.voters.len() as u16).to_be_bytes());
        bytes.extend_from_slice(&[0; 4]);
        for voter in &self.voters {
            bytes.extend_from_slice(&voter.key.id.to_be_bytes());
            bytes.extend_from_slice(&voter.key.directory);
            bytes.extend_from_slice(&voter.kraft_min.to_be_bytes());
            bytes.extend_from_slice(&voter.kraft_max.to_be_bytes());
            bytes.extend_from_slice(&(voter.endpoints.len() as u16).to_be_bytes());
            bytes.extend_from_slice(&[0; 2]);
            for endpoint in &voter.endpoints {
                bytes.extend_from_slice(&(endpoint.listener.len() as u16).to_be_bytes());
                bytes.extend_from_slice(&(endpoint.host.len() as u16).to_be_bytes());
                bytes.extend_from_slice(&endpoint.port.to_be_bytes());
                bytes.extend_from_slice(endpoint.listener.as_bytes());
                bytes.extend_from_slice(endpoint.host.as_bytes());
            }
        }
        Ok(bytes)
    }
    /// Decode bounded counts/UTF-8/exact completion before exposing any configuration.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_CONFIGURATION_BYTES {
            return Err(Error::Bounds);
        }
        let mut cursor = Cursor { bytes, offset: 0 };
        if cursor.take(8)? != MAGIC {
            return Err(Error::InvalidConfiguration);
        }
        let epoch = cursor.u64()?;
        let position = LogPosition::new(cursor.u64()?, cursor.u64()?)
            .map_err(|_| Error::InvalidConfiguration)?;
        let feature = cursor.i16()?;
        let count = usize::from(cursor.u16()?);
        cursor.zero(4)?;
        if !(1..=MAX_VOTERS).contains(&count) {
            return Err(Error::Bounds);
        }
        let mut voters = Vec::new();
        voters
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
        for _ in 0..count {
            let id = cursor.u32()?;
            let directory = cursor
                .take(16)?
                .try_into()
                .map_err(|_| Error::InvalidIdentity)?;
            let key = Key::new(id, directory)?;
            let min = cursor.i16()?;
            let max = cursor.i16()?;
            let count = usize::from(cursor.u16()?);
            cursor.zero(2)?;
            if !(1..=MAX_ENDPOINTS).contains(&count) {
                return Err(Error::Bounds);
            }
            let mut endpoints = Vec::new();
            endpoints
                .try_reserve_exact(count)
                .map_err(|_| Error::Allocation)?;
            for _ in 0..count {
                let listener_size = usize::from(cursor.u16()?);
                let host_size = usize::from(cursor.u16()?);
                let port = cursor.u16()?;
                if !(1..=249).contains(&listener_size) || !(1..=249).contains(&host_size) {
                    return Err(Error::Bounds);
                }
                let listener = cursor.text(listener_size)?;
                let host = cursor.text(host_size)?;
                endpoints.push(Endpoint::new(listener, host, port)?);
            }
            voters.push(Voter::new(key, endpoints, min, max)?);
        }
        if cursor.offset != bytes.len() {
            return Err(Error::InvalidConfiguration);
        }
        let decoded = Self::new(epoch, position, feature, voters)?;
        if decoded.encode()? != bytes {
            return Err(Error::InvalidConfiguration);
        }
        Ok(decoded)
    }
}

/// Bounded membership validation or allocation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Negative-domain ID or missing directory UUID.
    InvalidIdentity,
    /// Missing/duplicate/out-of-profile listener, host, port or range.
    InvalidEndpoint,
    /// Malformed, noncanonical or unsafe configuration transition.
    InvalidConfiguration,
    /// Current profile does not support this feature range or level.
    UnsupportedFeature,
    /// An existing ID cannot be added again, even with a changed directory.
    DuplicateVoter,
    /// Removal requires an exact existing ID/directory pair.
    VoterNotFound,
    /// At least one voter must remain.
    LastVoter,
    /// Positive count/byte/index ceilings exhausted.
    Bounds,
    /// Fallible reservation failed before publishing a result.
    Allocation,
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for Error {}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8], Error> {
        let end = self.offset.checked_add(size).ok_or(Error::Bounds)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(Error::InvalidConfiguration)?;
        self.offset = end;
        Ok(result)
    }
    fn zero(&mut self, size: usize) -> Result<(), Error> {
        if self.take(size)?.iter().any(|byte| *byte != 0) {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| Error::InvalidConfiguration)?,
        ))
    }
    fn i16(&mut self) -> Result<i16, Error> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| Error::InvalidConfiguration)?,
        ))
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| Error::InvalidConfiguration)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| Error::InvalidConfiguration)?,
        ))
    }
    fn text(&mut self, size: usize) -> Result<String, Error> {
        let input = std::str::from_utf8(self.take(size)?).map_err(|_| Error::InvalidEndpoint)?;
        let mut output = String::new();
        output
            .try_reserve_exact(size)
            .map_err(|_| Error::Allocation)?;
        output.push_str(input);
        Ok(output)
    }
}

/// Explicit dynamic-profile bootstrap; immutable group genesis is not the live set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootstrap {
    local: Voter,
    genesis: Voters,
    listener: String,
}
impl Bootstrap {
    /// Validate a local directory identity and genesis at epoch0/position0.
    ///
    /// The local replica may initially be an observer or have the same ID with a
    /// different directory; neither qualifies it to vote. All configured genesis
    /// voters and the local descriptor must provide the required default listener.
    pub fn new(local: Voter, genesis: Voters, listener: String) -> Result<Self, Error> {
        if genesis.epoch != 0
            || genesis.position != LogPosition::default()
            || !local.supports(genesis.feature)
            || local.endpoint(&listener).is_none()
            || genesis
                .voters
                .iter()
                .any(|v| v.endpoint(&listener).is_none())
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            local,
            genesis,
            listener,
        })
    }
    /// Exact local descriptor for correlated typed feature negotiation.
    pub fn local(&self) -> &Voter {
        &self.local
    }
    /// Immutable canonical original configuration shared by every group replica.
    pub fn genesis(&self) -> &Voters {
        &self.genesis
    }
    /// Required configured listener, with no reachability implication.
    pub fn listener(&self) -> &str {
        &self.listener
    }
}

/// Correlated local feature-discovery request; this is not an ApiVersions frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureRequest {
    /// Current directory-qualified leader.
    pub leader: Key,
    /// Exact proposed replica.
    pub peer: Key,
    /// Current normalized leader term.
    pub term: u64,
    /// Positive source-generated correlation sequence.
    pub sequence: u64,
    /// Active configuration epoch when this probe was prepared.
    pub configuration_epoch: u64,
}
/// Actual declared capabilities returned by the proposed replica's owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureResponse {
    /// Echoed complete request; delayed/changed correlations never authorize addition.
    pub request: FeatureRequest,
    /// Actual responding owner's directory/endpoints and supported feature range.
    pub voter: Voter,
}

/// Monotonic durable-contact observations, separate from a raw upstream component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    /// Greatest confirmed durable end; stale/regressing receipts must not lower it.
    pub matched: u64,
    /// Latest accepted current-term contact timestamp.
    pub last_fetch_ms: Option<u64>,
    /// Leader end observed at that accepted contact.
    pub last_fetch_leader_end: u64,
    /// Latest time the peer caught up to a current or previous observed leader end.
    pub last_caught_up_ms: Option<u64>,
}
impl Progress {
    /// Apply a fenced successful durable receipt, preserving monotonic progress.
    ///
    /// The recurrence follows Apache's current/prior leader-end timestamps. The
    /// enclosing owner supplies exact term/directory/sequence/sent-end checks;
    /// raw permissive component offsets alone are insufficient safety evidence.
    pub fn observe(&mut self, end: u64, leader_end: u64, now_ms: u64) -> Result<(), Error> {
        if end < self.matched
            || end > leader_end
            || self.last_fetch_ms.is_some_and(|old| now_ms < old)
        {
            return Err(Error::InvalidConfiguration);
        }
        if end >= leader_end {
            self.last_caught_up_ms = Some(self.last_caught_up_ms.unwrap_or(0).max(now_ms));
        } else if self.last_fetch_leader_end > 0 && end >= self.last_fetch_leader_end {
            if let Some(previous) = self.last_fetch_ms {
                self.last_caught_up_ms = Some(self.last_caught_up_ms.unwrap_or(0).max(previous));
            }
        }
        self.matched = end;
        self.last_fetch_leader_end = leader_end;
        self.last_fetch_ms = Some(now_ms);
        Ok(())
    }
    /// Apache's addition predicate: positive prior catch-up and fetch within one hour.
    ///
    /// Exact equality to today's leader end is deliberately not required. The
    /// owner must reject stale/reordered/forged observations before storing them.
    pub fn caught_up_for_addition(&self, now_ms: u64) -> bool {
        self.last_caught_up_ms.is_some_and(|t| t > 0)
            && self
                .last_fetch_ms
                .is_some_and(|t| t > 0 && t <= now_ms && now_ms - t < 3_600_000)
    }
}

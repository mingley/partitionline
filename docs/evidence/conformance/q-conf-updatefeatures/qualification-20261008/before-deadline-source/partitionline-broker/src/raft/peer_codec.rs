//! Bounded private fixed-route peer frames. This is not a Kafka codec or authentication.

use super::{
    election::{LogPosition, VoteRequest, VoteResponse},
    membership::{Endpoint, FeatureRequest, FeatureResponse, Key, Voter, MAX_CONFIGURATION_BYTES},
    replication::{
        Context, DynamicRequest, DynamicResponse, DynamicSnapshotRequest, DynamicSnapshotResponse,
        DynamicVoteRequest, DynamicVoteResponse, Record, RecordKind, Request, Response,
        SnapshotRequest, SnapshotResponse,
    },
    snapshot::Descriptor,
};

const MAGIC: &[u8; 8] = b"PLPEER01";
const MAX_TERM: u64 = i32::MAX as u64 + 1;
const MIN_FRAME: usize = 28;
const MAX_RECORDS: usize = 4096;
const MAX_RECORD: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Group {
    pub(super) cluster: String,
    pub(super) topic: String,
    pub(super) partition: u32,
    pub(super) genesis: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hello {
    pub(super) source: Key,
    pub(super) target: Key,
    pub(super) group: Group,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Message {
    Hello(Hello),
    HelloAck(Hello),
    Vote(DynamicVoteRequest),
    VoteReply(DynamicVoteResponse),
    Feature(FeatureRequest),
    FeatureReply(FeatureResponse),
    Append(DynamicRequest),
    AppendReply(DynamicResponse),
    Begin(DynamicSnapshotRequest),
    Begun(DynamicSnapshotRequest),
    Chunk {
        offer: DynamicSnapshotRequest,
        offset: u64,
        bytes: Vec<u8>,
    },
    Chunked {
        offer: DynamicSnapshotRequest,
        offset: u64,
        length: u32,
    },
    Finish(DynamicSnapshotRequest),
    Finished(DynamicSnapshotResponse),
    Failure {
        request_kind: u8,
        code: u8,
    },
}
impl Message {
    pub(super) fn kind(&self) -> u8 {
        match self {
            Self::Hello(_) => 1,
            Self::HelloAck(_) => 2,
            Self::Vote(_) => 10,
            Self::VoteReply(_) => 11,
            Self::Feature(_) => 12,
            Self::FeatureReply(_) => 13,
            Self::Append(_) => 20,
            Self::AppendReply(_) => 21,
            Self::Begin(_) => 30,
            Self::Begun(_) => 31,
            Self::Chunk { .. } => 32,
            Self::Chunked { .. } => 33,
            Self::Finish(_) => 34,
            Self::Finished(_) => 35,
            Self::Failure { .. } => 40,
        }
    }
    pub(super) fn context(&self) -> Option<Context> {
        match self {
            Self::Vote(q) => Some(q.context),
            Self::VoteReply(q) => Some(q.request.context),
            Self::Feature(q) => Some(Context {
                leader: q.leader,
                peer: q.peer,
                configuration_epoch: q.configuration_epoch,
            }),
            Self::FeatureReply(q) => Some(Context {
                leader: q.request.leader,
                peer: q.request.peer,
                configuration_epoch: q.request.configuration_epoch,
            }),
            Self::Append(q) => Some(q.context),
            Self::AppendReply(q) => Some(q.context),
            Self::Begin(q) | Self::Begun(q) | Self::Finish(q) => Some(q.context),
            Self::Chunk { offer, .. } | Self::Chunked { offer, .. } => Some(offer.context),
            Self::Finished(q) => Some(q.context),
            _ => None,
        }
    }
    pub(super) fn request(&self) -> bool {
        matches!(
            self,
            Self::Vote(_)
                | Self::Feature(_)
                | Self::Append(_)
                | Self::Begin(_)
                | Self::Chunk { .. }
                | Self::Finish(_)
        )
    }
    pub(super) fn matches_reply(&self, response: &Self) -> bool {
        match (self, response) {
            (Self::Vote(q), Self::VoteReply(r)) => r.request == *q,
            (Self::Feature(q), Self::FeatureReply(r)) => r.request == *q,
            (Self::Append(q), Self::AppendReply(r)) => {
                q.context == r.context
                    && q.request.sequence == r.response.sequence
                    && q.request.leader == r.response.leader
                    && q.request.peer == r.response.peer
            }
            (Self::Begin(q), Self::Begun(r)) => q == r,
            (
                Self::Chunk {
                    offer,
                    offset,
                    bytes,
                },
                Self::Chunked {
                    offer: r,
                    offset: o,
                    length,
                },
            ) => offer == r && offset == o && bytes.len() == *length as usize,
            (Self::Finish(q), Self::Finished(r)) => {
                q.context == r.context
                    && q.request.descriptor == r.response.descriptor
                    && q.request.sequence == r.response.response.sequence
                    && q.request.leader == r.response.response.leader
                    && q.request.peer == r.response.response.peer
            }
            _ => false,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Frame {
    pub(super) rpc: u64,
    pub(super) message: Message,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Error {
    Bounds,
    Corrupt,
    Allocation,
}

struct Writer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Writer {
    fn new(limit: usize) -> Result<Self, Error> {
        if !(MIN_FRAME..=4 * 1024 * 1024).contains(&limit) {
            return Err(Error::Bounds);
        }
        Ok(Self {
            bytes: Vec::new(),
            limit,
        })
    }
    fn put(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(Error::Bounds)?;
        if size > self.limit {
            return Err(Error::Bounds);
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(|_| Error::Allocation)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn u8(&mut self, n: u8) -> Result<(), Error> {
        self.put(&[n])
    }
    fn u16(&mut self, n: u16) -> Result<(), Error> {
        self.put(&n.to_be_bytes())
    }
    fn u32(&mut self, n: u32) -> Result<(), Error> {
        self.put(&n.to_be_bytes())
    }
    fn u64(&mut self, n: u64) -> Result<(), Error> {
        self.put(&n.to_be_bytes())
    }
    fn string(&mut self, s: &str) -> Result<(), Error> {
        if s.is_empty() || s.len() > 249 {
            return Err(Error::Bounds);
        }
        self.u16(s.len() as u16)?;
        self.put(s.as_bytes())
    }
    fn key(&mut self, key: Key) -> Result<(), Error> {
        self.u32(key.id)?;
        self.put(&key.directory)
    }
    fn pos(&mut self, p: LogPosition) -> Result<(), Error> {
        self.u64(p.term)?;
        self.u64(p.index)
    }
    fn context(&mut self, c: Context) -> Result<(), Error> {
        self.key(c.leader)?;
        self.key(c.peer)?;
        self.u64(c.configuration_epoch)
    }
    fn voter(&mut self, v: &Voter) -> Result<(), Error> {
        self.key(v.key())?;
        self.put(&v.kraft_min().to_be_bytes())?;
        self.put(&v.kraft_max().to_be_bytes())?;
        self.u16(v.endpoints().len() as u16)?;
        for e in v.endpoints() {
            self.string(e.listener())?;
            self.string(e.host())?;
            self.u16(e.port())?;
        }
        Ok(())
    }
    fn vote(&mut self, q: DynamicVoteRequest) -> Result<(), Error> {
        self.context(q.context)?;
        self.u64(q.sequence)?;
        self.u64(q.request.term)?;
        self.u32(q.request.candidate)?;
        self.pos(q.request.log)
    }
    fn feature(&mut self, q: FeatureRequest) -> Result<(), Error> {
        self.key(q.leader)?;
        self.key(q.peer)?;
        self.u64(q.term)?;
        self.u64(q.sequence)?;
        self.u64(q.configuration_epoch)
    }
    fn response(&mut self, r: Response) -> Result<(), Error> {
        self.u32(r.peer)?;
        self.u32(r.leader)?;
        self.u64(r.sequence)?;
        self.u64(r.term)?;
        self.u8(u8::from(r.success))?;
        self.put(&[0; 7])?;
        self.pos(r.matched)?;
        self.u64(r.conflict_index)
    }
    fn descriptor(&mut self, d: Descriptor) -> Result<(), Error> {
        self.put(&d.generation)?;
        self.pos(d.base)?;
        self.u64(d.records as u64)?;
        self.u64(d.payload_bytes)?;
        self.u64(d.bytes)?;
        self.u32(d.checksum)
    }
    fn offer(&mut self, q: DynamicSnapshotRequest) -> Result<(), Error> {
        self.context(q.context)?;
        self.u32(q.request.leader)?;
        self.u32(q.request.peer)?;
        self.u64(q.request.sequence)?;
        self.u64(q.request.term)?;
        self.u64(q.request.leader_commit)?;
        self.descriptor(q.request.descriptor)
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8], Error> {
        let end = self.at.checked_add(size).ok_or(Error::Bounds)?;
        let out = self.bytes.get(self.at..end).ok_or(Error::Corrupt)?;
        self.at = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| Error::Corrupt)?,
        ))
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Corrupt)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Corrupt)?,
        ))
    }
    fn zero(&mut self, size: usize) -> Result<(), Error> {
        if self.take(size)?.iter().any(|b| *b != 0) {
            Err(Error::Corrupt)
        } else {
            Ok(())
        }
    }
    fn string(&mut self) -> Result<String, Error> {
        let size = self.u16()? as usize;
        if !(1..=249).contains(&size) {
            return Err(Error::Bounds);
        }
        let s = std::str::from_utf8(self.take(size)?).map_err(|_| Error::Corrupt)?;
        let mut out = String::new();
        out.try_reserve_exact(size).map_err(|_| Error::Allocation)?;
        out.push_str(s);
        Ok(out)
    }
    fn key(&mut self) -> Result<Key, Error> {
        let id = self.u32()?;
        let dir = self.take(16)?.try_into().map_err(|_| Error::Corrupt)?;
        Key::new(id, dir).map_err(|_| Error::Corrupt)
    }
    fn term(&mut self) -> Result<u64, Error> {
        let n = self.u64()?;
        if (1..=MAX_TERM).contains(&n) {
            Ok(n)
        } else {
            Err(Error::Corrupt)
        }
    }
    fn positive(&mut self) -> Result<u64, Error> {
        let n = self.u64()?;
        if n == 0 {
            Err(Error::Corrupt)
        } else {
            Ok(n)
        }
    }
    fn boolean(&mut self) -> Result<bool, Error> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Corrupt),
        }
    }
    fn pos(&mut self) -> Result<LogPosition, Error> {
        LogPosition::new(self.u64()?, self.u64()?).map_err(|_| Error::Corrupt)
    }
    fn context(&mut self) -> Result<Context, Error> {
        Ok(Context {
            leader: self.key()?,
            peer: self.key()?,
            configuration_epoch: self.u64()?,
        })
    }
    fn voter(&mut self) -> Result<Voter, Error> {
        let key = self.key()?;
        let min = self.u16()? as i16;
        let max = self.u16()? as i16;
        let count = self.u16()? as usize;
        if !(1..=4).contains(&count) {
            return Err(Error::Bounds);
        }
        let mut endpoints = Vec::new();
        endpoints
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
        for _ in 0..count {
            let listener = self.string()?;
            let host = self.string()?;
            let port = self.u16()?;
            if endpoints
                .last()
                .is_some_and(|e: &Endpoint| e.listener() >= listener.as_str())
            {
                return Err(Error::Corrupt);
            }
            endpoints.push(Endpoint::new(listener, host, port).map_err(|_| Error::Corrupt)?);
        }
        Voter::new(key, endpoints, min, max).map_err(|_| Error::Corrupt)
    }
    fn vote(&mut self) -> Result<DynamicVoteRequest, Error> {
        let context = self.context()?;
        let sequence = self.positive()?;
        let term = self.term()?;
        let candidate = self.u32()?;
        let log = self.pos()?;
        if candidate != context.leader.id || log.term >= term {
            return Err(Error::Corrupt);
        }
        Ok(DynamicVoteRequest {
            context,
            sequence,
            request: VoteRequest {
                term,
                candidate,
                log,
            },
        })
    }
    fn feature(&mut self) -> Result<FeatureRequest, Error> {
        Ok(FeatureRequest {
            leader: self.key()?,
            peer: self.key()?,
            term: self.term()?,
            sequence: self.positive()?,
            configuration_epoch: self.u64()?,
        })
    }
    fn response(&mut self) -> Result<Response, Error> {
        let peer = self.u32()?;
        let leader = self.u32()?;
        let sequence = self.positive()?;
        let term = self.term()?;
        let success = self.boolean()?;
        self.zero(7)?;
        let matched = self.pos()?;
        let conflict_index = self.u64()?;
        Ok(Response {
            peer,
            leader,
            sequence,
            term,
            success,
            matched,
            conflict_index,
        })
    }
    fn descriptor(&mut self) -> Result<Descriptor, Error> {
        let generation = self.take(16)?.try_into().map_err(|_| Error::Corrupt)?;
        let base = self.pos()?;
        let records = usize::try_from(self.u64()?).map_err(|_| Error::Bounds)?;
        let payload_bytes = self.u64()?;
        let bytes = self.u64()?;
        let checksum = self.u32()?;
        if generation == [0; 16]
            || records > 4096
            || base.index != records as u64
            || payload_bytes > 64 * 1024 * 1024
            || bytes > 128 * 1024 * 1024
        {
            return Err(Error::Bounds);
        }
        Ok(Descriptor {
            generation,
            base,
            records,
            payload_bytes,
            bytes,
            checksum,
        })
    }
    fn offer(&mut self) -> Result<DynamicSnapshotRequest, Error> {
        let context = self.context()?;
        let leader = self.u32()?;
        let peer = self.u32()?;
        let sequence = self.positive()?;
        let term = self.term()?;
        let leader_commit = self.u64()?;
        let descriptor = self.descriptor()?;
        if context.leader.id != leader
            || context.peer.id != peer
            || descriptor.base.term > term
            || descriptor.base.index > leader_commit
        {
            return Err(Error::Corrupt);
        }
        Ok(DynamicSnapshotRequest {
            context,
            request: SnapshotRequest {
                leader,
                peer,
                sequence,
                term,
                leader_commit,
                descriptor,
            },
        })
    }
}
fn copy(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| Error::Allocation)?;
    out.extend_from_slice(bytes);
    Ok(out)
}

pub(super) fn encode(frame: &Frame, limit: usize) -> Result<Vec<u8>, Error> {
    let mut w = Writer::new(limit.checked_sub(4).ok_or(Error::Bounds)?)?;
    let kind = frame.message.kind();
    if (kind == 1 || kind == 2) != (frame.rpc == 0) {
        return Err(Error::Corrupt);
    }
    w.put(MAGIC)?;
    w.u16(1)?;
    w.u8(kind)?;
    w.u8(0)?;
    w.u64(frame.rpc)?;
    w.put(&[0; 4])?;
    match &frame.message {
        Message::Hello(h) | Message::HelloAck(h) => {
            if h.group.genesis.len() > MAX_CONFIGURATION_BYTES {
                return Err(Error::Bounds);
            }
            w.key(h.source)?;
            w.key(h.target)?;
            w.string(&h.group.cluster)?;
            w.string(&h.group.topic)?;
            w.u32(h.group.partition)?;
            w.u32(h.group.genesis.len() as u32)?;
            w.put(&h.group.genesis)?;
        }
        Message::Vote(q) => w.vote(*q)?,
        Message::VoteReply(r) => {
            w.vote(r.request)?;
            w.u64(r.response.term)?;
            w.u32(r.response.voter)?;
            w.u32(r.response.candidate)?;
            w.u8(u8::from(r.response.granted))?;
        }
        Message::Feature(q) => w.feature(*q)?,
        Message::FeatureReply(r) => {
            w.feature(r.request)?;
            w.voter(&r.voter)?;
        }
        Message::Append(q) => {
            if q.request.entries.len() > MAX_RECORDS {
                return Err(Error::Bounds);
            }
            w.context(q.context)?;
            w.u32(q.request.leader)?;
            w.u32(q.request.peer)?;
            w.u64(q.request.sequence)?;
            w.u64(q.request.term)?;
            w.pos(q.request.previous)?;
            w.u64(q.request.leader_commit)?;
            w.u32(q.request.entries.len() as u32)?;
            w.put(&[0; 4])?;
            for e in &q.request.entries {
                if e.payload.len() > MAX_RECORD {
                    return Err(Error::Bounds);
                }
                w.u64(e.term)?;
                w.u64(e.index)?;
                w.u8(match e.kind {
                    RecordKind::Data => 0,
                    RecordKind::Barrier => 1,
                    RecordKind::Voters => 2,
                })?;
                w.put(&[0; 3])?;
                w.u32(e.payload.len() as u32)?;
                w.put(&e.payload)?;
            }
        }
        Message::AppendReply(r) => {
            w.context(r.context)?;
            w.response(r.response)?;
        }
        Message::Begin(q) | Message::Begun(q) | Message::Finish(q) => w.offer(*q)?,
        Message::Chunk {
            offer,
            offset,
            bytes,
        } => {
            w.offer(*offer)?;
            w.u64(*offset)?;
            w.u32(bytes.len().try_into().map_err(|_| Error::Bounds)?)?;
            w.put(bytes)?;
        }
        Message::Chunked {
            offer,
            offset,
            length,
        } => {
            w.offer(*offer)?;
            w.u64(*offset)?;
            w.u32(*length)?;
        }
        Message::Finished(r) => {
            w.context(r.context)?;
            w.response(r.response.response)?;
            w.descriptor(r.response.descriptor)?;
        }
        Message::Failure { request_kind, code } => {
            if !(1..=8).contains(code) {
                return Err(Error::Corrupt);
            }
            w.u8(*request_kind)?;
            w.u8(*code)?;
        }
    }
    let crc = crc32c::crc32c(&w.bytes);
    let mut out = w.bytes;
    out.try_reserve_exact(4).map_err(|_| Error::Allocation)?;
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}
pub(super) fn decode(bytes: &[u8], limit: usize, max_records: usize) -> Result<Frame, Error> {
    if !(MIN_FRAME..=limit).contains(&bytes.len()) || limit > 4 * 1024 * 1024 {
        return Err(Error::Bounds);
    }
    let body_end = bytes.len() - 4;
    let crc = u32::from_be_bytes(bytes[body_end..].try_into().map_err(|_| Error::Corrupt)?);
    if crc32c::crc32c(&bytes[..body_end]) != crc {
        return Err(Error::Corrupt);
    }
    let mut r = Reader {
        bytes: &bytes[..body_end],
        at: 0,
    };
    if r.take(8)? != MAGIC || r.u16()? != 1 {
        return Err(Error::Corrupt);
    }
    let kind = r.u8()?;
    r.zero(1)?;
    let rpc = r.u64()?;
    r.zero(4)?;
    if (kind == 1 || kind == 2) != (rpc == 0) {
        return Err(Error::Corrupt);
    }
    let message = match kind {
        1 | 2 => {
            let source = r.key()?;
            let target = r.key()?;
            let cluster = r.string()?;
            let topic = r.string()?;
            let partition = r.u32()?;
            let size = r.u32()? as usize;
            if size > MAX_CONFIGURATION_BYTES {
                return Err(Error::Bounds);
            }
            let genesis = copy(r.take(size)?)?;
            let h = Hello {
                source,
                target,
                group: Group {
                    cluster,
                    topic,
                    partition,
                    genesis,
                },
            };
            if kind == 1 {
                Message::Hello(h)
            } else {
                Message::HelloAck(h)
            }
        }
        10 => Message::Vote(r.vote()?),
        11 => {
            let request = r.vote()?;
            let response = VoteResponse {
                term: r.term()?,
                voter: r.u32()?,
                candidate: r.u32()?,
                granted: r.boolean()?,
            };
            Message::VoteReply(DynamicVoteResponse { request, response })
        }
        12 => Message::Feature(r.feature()?),
        13 => {
            let request = r.feature()?;
            let voter = r.voter()?;
            Message::FeatureReply(FeatureResponse { request, voter })
        }
        20 => {
            let context = r.context()?;
            let leader = r.u32()?;
            let peer = r.u32()?;
            let sequence = r.positive()?;
            let term = r.term()?;
            let previous = r.pos()?;
            let leader_commit = r.u64()?;
            let count = r.u32()? as usize;
            r.zero(4)?;
            if count > MAX_RECORDS
                || count > max_records
                || context.leader.id != leader
                || context.peer.id != peer
                || previous.term > term
            {
                return Err(Error::Corrupt);
            }
            let start = r.at;
            for _ in 0..count {
                r.term()?;
                r.positive()?;
                let kind = r.u8()?;
                r.zero(3)?;
                let size = r.u32()? as usize;
                if kind > 2 || size > MAX_RECORD || (kind == 1) != (size == 0) {
                    return Err(Error::Corrupt);
                }
                r.take(size)?;
            }
            r.at = start;
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(count)
                .map_err(|_| Error::Allocation)?;
            for _ in 0..count {
                let term = r.term()?;
                let index = r.positive()?;
                let k = r.u8()?;
                r.zero(3)?;
                let size = r.u32()? as usize;
                let kind = match k {
                    0 => RecordKind::Data,
                    1 => RecordKind::Barrier,
                    2 => RecordKind::Voters,
                    _ => return Err(Error::Corrupt),
                };
                entries.push(Record {
                    term,
                    index,
                    kind,
                    payload: copy(r.take(size)?)?,
                });
            }
            Message::Append(DynamicRequest {
                context,
                request: Request {
                    leader,
                    peer,
                    sequence,
                    term,
                    previous,
                    leader_commit,
                    entries,
                },
            })
        }
        21 => {
            let context = r.context()?;
            let response = r.response()?;
            Message::AppendReply(DynamicResponse { context, response })
        }
        30 => Message::Begin(r.offer()?),
        31 => Message::Begun(r.offer()?),
        32 => {
            let offer = r.offer()?;
            let offset = r.u64()?;
            let size = r.u32()? as usize;
            Message::Chunk {
                offer,
                offset,
                bytes: copy(r.take(size)?)?,
            }
        }
        33 => Message::Chunked {
            offer: r.offer()?,
            offset: r.u64()?,
            length: r.u32()?,
        },
        34 => Message::Finish(r.offer()?),
        35 => {
            let context = r.context()?;
            let response = r.response()?;
            let descriptor = r.descriptor()?;
            Message::Finished(DynamicSnapshotResponse {
                context,
                response: SnapshotResponse {
                    response,
                    descriptor,
                },
            })
        }
        40 => {
            let request_kind = r.u8()?;
            let code = r.u8()?;
            if !(1..=8).contains(&code) {
                return Err(Error::Corrupt);
            }
            Message::Failure { request_kind, code }
        }
        _ => return Err(Error::Corrupt),
    };
    if r.at != r.bytes.len() {
        return Err(Error::Corrupt);
    }
    Ok(Frame { rpc, message })
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;
    fn sample() -> Frame {
        Frame {
            rpc: 9,
            message: Message::Vote(DynamicVoteRequest {
                context: Context {
                    leader: Key {
                        id: 1,
                        directory: [1; 16],
                    },
                    peer: Key {
                        id: 2,
                        directory: [2; 16],
                    },
                    configuration_epoch: 3,
                },
                sequence: 17,
                request: VoteRequest {
                    term: 4,
                    candidate: 1,
                    log: LogPosition { term: 3, index: 5 },
                },
            }),
        }
    }
    #[test]
    fn every_prefix_crc_version_reserved_unknown_and_trailing_mutation_rejects() -> Result {
        let frame = sample();
        let bytes = encode(&frame, 1024).map_err(|e| format!("{e:?}"))?;
        assert_eq!(decode(&bytes, 1024, 4096), Ok(frame));
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n], 1024, 4096).is_err());
        }
        for offset in [0, 8, 9, 10, 11, 20, 24] {
            let mut bad = bytes.clone();
            bad[offset] ^= 0x80;
            let end = bad.len() - 4;
            let crc = crc32c::crc32c(&bad[..end]);
            bad[end..].copy_from_slice(&crc.to_be_bytes());
            assert!(decode(&bad, 1024, 4096).is_err());
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(decode(&bad, 1024, 4096).is_err());
        assert!(encode(&sample(), 28).is_err());
        Ok(())
    }
    #[test]
    fn response_echo_must_match_original_request_and_directory() -> Result {
        let Message::Vote(q) = sample().message else {
            return Err("wrong sample".into());
        };
        let reply = Message::VoteReply(DynamicVoteResponse {
            request: q,
            response: VoteResponse {
                term: 4,
                voter: 2,
                candidate: 1,
                granted: true,
            },
        });
        assert!(Message::Vote(q).matches_reply(&reply));
        let mut other = q;
        other.context.peer.directory = [9; 16];
        assert!(!Message::Vote(other).matches_reply(&reply));
        Ok(())
    }
}

//! Persistent single-node Kafka metadata and topic administration.
//!
//! The composed [`Router`] advertises exactly Metadata0..13, ApiVersions0..4,
//! CreateTopics2..4 and DeleteTopics1..6. Create overrides are rejected with
//! INVALID_CONFIG; automatic creation is disabled. Logical leaders/ISR are this
//! configured node. This is not replicated storage, authorization or production
//! qualification. The catalog journal is the custom format documented in
//! [`catalog`], not an Apache metadata log.
//!
//! One bounded blocking actor owns the catalog. Canceled queued requests are
//! skipped; cancellation during append/sync is ambiguous and cannot retract a
//! durable operation. Look up the result after reconnecting, or reopen after an
//! ambiguous storage failure. Each topic operation is atomic, not the whole
//! multi-topic request. Successful mutations are published only after sync.
//! [`Router::shutdown`] rejects admission, drains pending work and joins; its
//! canceled future can be retried. Drop requests stop without blocking to join.

use crate::{
    catalog::{self, Catalog, Topic, TopicId},
    journal,
    protocol::{self, ApiVersionsHandler, RequestHeader},
    transport::Handler,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};

const ZERO: [u8; 16] = [0; 16];
const TOPIC_OPS: i32 = 8 | 16 | 32 | 64 | 128 | 256 | 1024 | 2048;
const CLUSTER_OPS: i32 = 32 | 128 | 256 | 512 | 1024 | 2048 | 4096;

/// Validated actor, parser, response and standalone-node configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Nonnegative node ID returned as controller, leader and sole replica.
    pub broker_id: i32,
    /// Client-reachable host, independent of the transport bind address.
    pub advertised_host: String,
    /// Client-reachable port; zero is invalid.
    pub advertised_port: u16,
    /// Stable operator-provided cluster ID, reused when reopening the catalog.
    pub cluster_id: String,
    /// Default logical partitions for CreateTopics v4's -1 sentinel.
    pub default_partitions: u32,
    /// Persistent catalog budgets, also applied to validate-only admission.
    pub catalog_limits: catalog::Limits,
    /// Bounded request bytes and per-block tagged-field count.
    pub protocol_limits: protocol::Limits,
    /// Maximum topics or assignments in any request array, 1..=4096.
    pub max_topics: usize,
    /// Maximum queued requests, 1..=1024; full admission closes that connection.
    /// Queue count times maximum request bytes must not exceed512MiB.
    pub max_queued_requests: usize,
    /// Response payload cap, 64bytes..=64MiB; align with transport configuration.
    pub max_response_bytes: usize,
}
impl Config {
    /// Construct defaults around an explicit stable cluster/node endpoint.
    pub fn new(
        broker_id: i32,
        advertised_host: String,
        advertised_port: u16,
        cluster_id: String,
    ) -> Self {
        Self {
            broker_id,
            advertised_host,
            advertised_port,
            cluster_id,
            default_partitions: 1,
            catalog_limits: catalog::Limits::default(),
            protocol_limits: protocol::Limits::default(),
            max_topics: 1024,
            max_queued_requests: 64,
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if self.broker_id < 0
            || self.advertised_port == 0
            || self.advertised_host.is_empty()
            || self.advertised_host.len() > 253
            || !self.advertised_host.is_ascii()
            || self
                .advertised_host
                .bytes()
                .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
            || self.cluster_id.is_empty()
            || self.cluster_id.len() > 32767
            || self.default_partitions == 0
            || self.default_partitions > self.catalog_limits.max_partitions_per_topic()
            || !(1..=4096).contains(&self.max_topics)
            || !(1..=1024).contains(&self.max_queued_requests)
            || self
                .max_queued_requests
                .saturating_mul(self.protocol_limits.max_request_bytes())
                > 512 * 1024 * 1024
            || !(64..=64 * 1024 * 1024).contains(&self.max_response_bytes)
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

/// Structural/admission failures close only the affected transport connection.
#[derive(Debug)]
pub enum Error {
    /// Invalid local endpoint or resource bounds.
    InvalidConfig,
    /// Bounded protocol parsing failed before mutation.
    Protocol(protocol::Error),
    /// API/version is outside this router's implemented range.
    UnsupportedVersion {
        /// API key.
        api_key: i16,
        /// Requested version.
        version: i16,
    },
    /// A request array exceeds its local count or remaining-byte bound.
    RequestCount,
    /// A boolean is not encoded as zero or one.
    InvalidBoolean,
    /// Metadata v12+ supplies neither a topic name nor a nonzero identity.
    /// The complete bounded body was parsed, but no response is fabricated.
    InvalidTarget,
    /// Bounded response reservation failed or would exceed the configured cap.
    ResponseLimit,
    /// Bounded actor queue is full; no operation was admitted.
    QueueFull,
    /// Actor is stopping or stopped; no new operation was admitted.
    Stopped,
    /// Actor task failed to join.
    ActorFailed,
    /// Startup replay/storage failure; no router was returned.
    Catalog(catalog::Error),
    /// Catalog is poisoned after an ambiguous storage failure; reopen it.
    StoragePoisoned,
    /// A canceled in-flight client no longer has a response receiver.
    Canceled,
}
impl From<protocol::Error> for Error {
    fn from(value: protocol::Error) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metadata router: {self:?}")
    }
}
impl std::error::Error for Error {}

struct Job {
    request: Vec<u8>,
    admitted: Instant,
    reply: oneshot::Sender<Result<Vec<u8>, Error>>,
}
enum Command {
    Request(Job),
    Stop,
}

/// Composed bounded Kafka handler and exclusive asynchronous catalog owner.
///
/// Do not open another catalog owner for the same file. This process-local
/// actor does not add cross-process locks to the journal. Fresh topic IDs are
/// RFC4122 version4 values from getrandom, excluding reserved IDs and Apache
/// Uuid.randomUuid's leading-base64url-minus case, with at most32 attempts.
/// Shutdown must finish before reopening the same file.
#[derive(Debug)]
pub struct Router {
    config: Config,
    sender: mpsc::Sender<Command>,
    stopping: Arc<AtomicBool>,
    failed: AtomicBool,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl Router {
    /// Open/replay on a blocking actor; cancellation before startup drops it.
    pub async fn open(
        path: impl Into<PathBuf>,
        config: Config,
    ) -> Result<(Self, journal::Recovery), Error> {
        config.validate()?;
        let path = path.into();
        let (sender, receiver) = mpsc::channel(config.max_queued_requests);
        let (ready_tx, ready_rx) = oneshot::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stopping);
        let worker_config = config.clone();
        let task = tokio::task::spawn_blocking(move || {
            match Catalog::open(path, worker_config.catalog_limits) {
                Ok((catalog, recovery)) => {
                    if ready_tx.send(Ok(recovery)).is_ok() {
                        actor(catalog, receiver, &worker_config, &worker_stop);
                    }
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(Error::Catalog(error)));
                }
            }
        });
        let recovery = ready_rx.await.map_err(|_| Error::ActorFailed)??;
        Ok((
            Self {
                config,
                sender,
                stopping,
                failed: AtomicBool::new(false),
                task: Mutex::new(Some(task)),
            },
            recovery,
        ))
    }
    /// Configuration validated at startup.
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// Request stop, drain pending replies, and join without blocking Tokio.
    ///
    /// Awaiting by mutable reference retains the join handle when this future
    /// is canceled. A later shutdown call completes the same join. An operation
    /// already inside append/sync may complete before the actor observes stop.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.request_stop();
        let mut task = self.task.lock().await;
        if let Some(handle) = task.as_mut() {
            if handle.await.is_err() {
                self.failed.store(true, Ordering::Release);
            }
        }
        let _ = task.take();
        if self.failed.load(Ordering::Acquire) {
            Err(Error::ActorFailed)
        } else {
            Ok(())
        }
    }
    fn request_stop(&self) {
        self.stopping.store(true, Ordering::Release);
        let _ = self.sender.try_send(Command::Stop);
    }
    /// Dispatch one transport-bounded payload; no frame prefix is included.
    pub async fn respond(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let (key, _) = prefix(&request, &self.config)?;
        if key == 18 {
            return ApiVersionsHandler::composed(self.config.protocol_limits)
                .respond(&request)
                .map_err(Into::into);
        }
        let (reply, receiver) = oneshot::channel();
        let job = Job {
            request,
            admitted: Instant::now(),
            reply,
        };
        self.sender
            .try_send(Command::Request(job))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Stopped,
            })?;
        receiver.await.map_err(|_| Error::ActorFailed)?
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        self.request_stop();
    }
}
impl Handler for Router {
    type Error = Error;
    async fn handle(&self, request: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        self.respond(request).await.map(Some)
    }
}
fn actor(
    mut catalog: Catalog,
    mut receiver: mpsc::Receiver<Command>,
    config: &Config,
    stopping: &AtomicBool,
) {
    while let Some(command) = receiver.blocking_recv() {
        if stopping.load(Ordering::Acquire) {
            receiver.close();
            reject(command);
            break;
        }
        match command {
            Command::Stop => {
                receiver.close();
                break;
            }
            Command::Request(job) => {
                if job.reply.is_closed() {
                    continue;
                }
                let result = process(
                    &mut catalog,
                    &job.request,
                    config,
                    job.admitted,
                    &job.reply,
                    stopping,
                );
                let _ = job.reply.send(result);
            }
        }
    }
    receiver.close();
    while let Some(command) = receiver.blocking_recv() {
        reject(command);
    }
}
fn reject(command: Command) {
    if let Command::Request(job) = command {
        let _ = job.reply.send(Err(Error::Stopped));
    }
}

fn prefix(request: &[u8], config: &Config) -> Result<(i16, i16), Error> {
    if request.len() > config.protocol_limits.max_request_bytes() {
        return Err(protocol::Error::RequestTooLarge.into());
    }
    let mut reader = Reader::new(request, config);
    let key = reader.i16()?;
    let version = reader.i16()?;
    let supported = match key {
        3 => (0..=13).contains(&version),
        18 => true,
        19 => (2..=4).contains(&version),
        20 => (1..=6).contains(&version),
        _ => return Err(protocol::Error::UnimplementedApi(key).into()),
    };
    if !supported {
        return Err(Error::UnsupportedVersion {
            api_key: key,
            version,
        });
    }
    Ok((key, version))
}
fn process(
    catalog: &mut Catalog,
    request: &[u8],
    config: &Config,
    admitted: Instant,
    reply: &oneshot::Sender<Result<Vec<u8>, Error>>,
    stopping: &AtomicBool,
) -> Result<Vec<u8>, Error> {
    if catalog.is_poisoned() {
        return Err(Error::StoragePoisoned);
    }
    let (key, version) = prefix(request, config)?;
    let flexible = (key == 3 && version >= 9) || (key == 20 && version >= 4);
    let (header, body) = RequestHeader::parse(
        request,
        if flexible { 2 } else { 1 },
        config.protocol_limits,
    )?;
    let mut reader = Reader::new(body, config);
    match key {
        3 => {
            let parsed = read_metadata(&mut reader, version, flexible)?;
            reader.finish()?;
            metadata(catalog, config, header, parsed, flexible)
        }
        19 => {
            let parsed = read_create(&mut reader)?;
            reader.finish()?;
            create(catalog, config, header, parsed, admitted, reply, stopping)
        }
        20 => {
            let parsed = read_delete(&mut reader, version, flexible)?;
            reader.finish()?;
            delete(catalog, config, header, parsed, admitted, reply, stopping)
        }
        _ => Err(protocol::Error::UnimplementedApi(key).into()),
    }
}

struct Reader<'a> {
    remaining: &'a [u8],
    max_topics: usize,
    max_tags: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], config: &Config) -> Self {
        Self {
            remaining: bytes,
            max_topics: config.max_topics,
            max_tags: config.protocol_limits.max_tagged_fields(),
        }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let value = self
            .remaining
            .get(..length)
            .ok_or(protocol::Error::Truncated)?;
        self.remaining = &self.remaining[length..];
        Ok(value)
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
    fn uuid(&mut self) -> Result<[u8; 16], Error> {
        self.take(16)?
            .try_into()
            .map_err(|_| protocol::Error::Truncated.into())
    }
    fn boolean(&mut self) -> Result<bool, Error> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::InvalidBoolean),
        }
    }
    fn varint(&mut self) -> Result<u32, Error> {
        let mut value = 0u32;
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
    fn string(&mut self, flexible: bool, nullable: bool) -> Result<Option<&'a str>, Error> {
        let length = if flexible {
            i64::from(self.varint()?) - 1
        } else {
            i64::from(self.i16()?)
        };
        if length == -1 && nullable {
            return Ok(None);
        }
        if !(0..=32767).contains(&length) {
            return Err(protocol::Error::InvalidLength.into());
        }
        let value = std::str::from_utf8(self.take(length as usize)?)
            .map_err(|_| protocol::Error::InvalidUtf8)?;
        Ok(Some(value))
    }
    fn required_string(&mut self, flexible: bool) -> Result<&'a str, Error> {
        self.string(flexible, false)?
            .ok_or_else(|| protocol::Error::InvalidLength.into())
    }
    fn count(
        &mut self,
        flexible: bool,
        nullable: bool,
        minimum: usize,
    ) -> Result<Option<usize>, Error> {
        let count = if flexible {
            i64::from(self.varint()?) - 1
        } else {
            i64::from(self.i32()?)
        };
        if count == -1 && nullable {
            return Ok(None);
        }
        if count < 0 || count > self.max_topics as i64 {
            return Err(Error::RequestCount);
        }
        let count = count as usize;
        if count > self.remaining.len() / minimum.max(1) {
            return Err(Error::RequestCount);
        }
        Ok(Some(count))
    }
    fn array(&mut self, flexible: bool, minimum: usize) -> Result<usize, Error> {
        self.count(flexible, false, minimum)?
            .ok_or(Error::RequestCount)
    }
    fn tags(&mut self) -> Result<(), Error> {
        let count = self.varint()? as usize;
        if count > self.max_tags {
            return Err(protocol::Error::TooManyTags.into());
        }
        let mut previous = None;
        for _ in 0..count {
            let tag = self.varint()?;
            if previous.is_some_and(|v| tag <= v) {
                return Err(protocol::Error::InvalidTagOrder.into());
            }
            previous = Some(tag);
            let length = self.varint()? as usize;
            self.take(length)?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), Error> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(protocol::Error::TrailingBytes.into())
        }
    }
}
fn bounded_vec<T>(count: usize) -> Result<Vec<T>, Error> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| Error::ResponseLimit)?;
    Ok(values)
}
struct Target<'a> {
    name: Option<&'a str>,
    id: [u8; 16],
}
struct MetadataRequest<'a> {
    topics: Option<Vec<Target<'a>>>,
    cluster_ops: bool,
    topic_ops: bool,
}
fn read_metadata<'a>(
    reader: &mut Reader<'a>,
    version: i16,
    flex: bool,
) -> Result<MetadataRequest<'a>, Error> {
    let count = reader.count(flex, version >= 1, if version >= 10 { 17 } else { 2 })?;
    let topics = if let Some(count) = count {
        let mut values = bounded_vec(count)?;
        for _ in 0..count {
            let id = if version >= 10 { reader.uuid()? } else { ZERO };
            let name = reader.string(flex, version >= 10)?;
            if flex {
                reader.tags()?;
            }
            values.push(Target { name, id });
        }
        if version == 0 && count == 0 {
            None
        } else {
            Some(values)
        }
    } else {
        None
    };
    if version >= 4 {
        let _ = reader.boolean()?;
    }
    let cluster_ops = if (8..=10).contains(&version) {
        reader.boolean()?
    } else {
        false
    };
    let topic_ops = if version >= 8 {
        reader.boolean()?
    } else {
        false
    };
    if flex {
        reader.tags()?;
    }
    Ok(MetadataRequest {
        topics,
        cluster_ops,
        topic_ops,
    })
}
struct Assignment {
    index: i32,
    brokers: Vec<i32>,
}
struct CreateTopic<'a> {
    name: &'a str,
    partitions: i32,
    replicas: i16,
    assignments: Vec<Assignment>,
    config_count: usize,
}
struct CreateRequest<'a> {
    topics: Vec<CreateTopic<'a>>,
    timeout: i32,
    validate: bool,
}
fn read_create<'a>(reader: &mut Reader<'a>) -> Result<CreateRequest<'a>, Error> {
    let count = reader.array(false, 16)?;
    let mut topics = bounded_vec(count)?;
    for _ in 0..count {
        let name = reader.required_string(false)?;
        let partitions = reader.i32()?;
        let replicas = reader.i16()?;
        let count = reader.array(false, 8)?;
        let mut assignments = bounded_vec(count)?;
        for _ in 0..count {
            let index = reader.i32()?;
            let count = reader.array(false, 4)?;
            let mut brokers = bounded_vec(count)?;
            for _ in 0..count {
                brokers.push(reader.i32()?);
            }
            assignments.push(Assignment { index, brokers });
        }
        let config_count = reader.array(false, 4)?;
        for _ in 0..config_count {
            let _ = reader.required_string(false)?;
            let _ = reader.string(false, true)?;
        }
        topics.push(CreateTopic {
            name,
            partitions,
            replicas,
            assignments,
            config_count,
        });
    }
    Ok(CreateRequest {
        topics,
        timeout: reader.i32()?,
        validate: reader.boolean()?,
    })
}
struct DeleteRequest<'a> {
    topics: Vec<Target<'a>>,
    timeout: i32,
}
fn read_delete<'a>(
    reader: &mut Reader<'a>,
    version: i16,
    flex: bool,
) -> Result<DeleteRequest<'a>, Error> {
    let count = reader.array(
        flex,
        if version >= 6 {
            18
        } else if flex {
            1
        } else {
            2
        },
    )?;
    let mut topics = bounded_vec(count)?;
    for _ in 0..count {
        let name = reader.string(flex, version >= 6)?;
        let id = if version >= 6 { reader.uuid()? } else { ZERO };
        if flex && version >= 6 {
            reader.tags()?;
        }
        topics.push(Target { name, id });
    }
    let timeout = reader.i32()?;
    if flex {
        reader.tags()?;
    }
    Ok(DeleteRequest { topics, timeout })
}

struct Writer {
    bytes: Vec<u8>,
    cap: usize,
}
impl Writer {
    fn new(cap: usize) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(cap)
            .map_err(|_| Error::ResponseLimit)?;
        Ok(Self { bytes, cap })
    }
    fn put(&mut self, value: &[u8]) -> Result<(), Error> {
        if value.len() > self.cap.saturating_sub(self.bytes.len()) {
            return Err(Error::ResponseLimit);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    fn byte(&mut self, value: u8) -> Result<(), Error> {
        self.put(&[value])
    }
    fn i16(&mut self, value: i16) -> Result<(), Error> {
        self.put(&value.to_be_bytes())
    }
    fn i32(&mut self, value: i32) -> Result<(), Error> {
        self.put(&value.to_be_bytes())
    }
    fn varint(&mut self, mut value: u32) -> Result<(), Error> {
        while value >= 128 {
            self.byte((value as u8) | 128)?;
            value >>= 7;
        }
        self.byte(value as u8)
    }
    fn count(&mut self, count: usize, flex: bool) -> Result<(), Error> {
        if flex {
            let count = u32::try_from(count).map_err(|_| Error::ResponseLimit)?;
            self.varint(count.checked_add(1).ok_or(Error::ResponseLimit)?)
        } else {
            self.i32(i32::try_from(count).map_err(|_| Error::ResponseLimit)?)
        }
    }
    fn string(&mut self, value: Option<&str>, flex: bool, nullable: bool) -> Result<(), Error> {
        if let Some(value) = value {
            if value.len() > 32767 {
                return Err(Error::ResponseLimit);
            }
            if flex {
                self.varint((value.len() + 1) as u32)?;
            } else {
                self.i16(value.len() as i16)?;
            }
            self.put(value.as_bytes())
        } else if nullable {
            if flex {
                self.byte(0)
            } else {
                self.i16(-1)
            }
        } else {
            Err(Error::ResponseLimit)
        }
    }
    fn tags(&mut self, flex: bool) -> Result<(), Error> {
        if flex {
            self.byte(0)
        } else {
            Ok(())
        }
    }
    fn header(&mut self, correlation: i32, flex: bool) -> Result<(), Error> {
        self.i32(correlation)?;
        self.tags(flex)
    }
}

struct MetadataTopic<'a> {
    name: Option<&'a str>,
    id: [u8; 16],
    error: i16,
    topic: Option<&'a Topic>,
}
fn metadata(
    catalog: &Catalog,
    config: &Config,
    header: RequestHeader<'_>,
    request: MetadataRequest<'_>,
    flex: bool,
) -> Result<Vec<u8>, Error> {
    let version = header.api_version;
    if version >= 12
        && request.topics.as_ref().is_some_and(|topics| {
            topics
                .iter()
                .any(|target| target.name.is_none() && target.id == ZERO)
        })
    {
        return Err(Error::InvalidTarget);
    }
    let semantic_error = request.topics.as_ref().is_some_and(|topics| {
        version < 12 && topics.iter().any(|t| t.name.is_none() || t.id != ZERO)
    });
    let mut results = bounded_vec(
        request
            .topics
            .as_ref()
            .map_or(catalog.topic_count(), Vec::len),
    )?;
    if let Some(targets) = &request.topics {
        let use_id = !semantic_error && targets.iter().any(|t| t.id != ZERO);
        for target in targets {
            if use_id && target.id == ZERO {
                continue;
            }
            if !semantic_error
                && results.iter().any(|result: &MetadataTopic<'_>| {
                    if use_id {
                        result.id == target.id
                    } else {
                        result.name == target.name
                    }
                })
            {
                continue;
            }
            if semantic_error {
                results.push(MetadataTopic {
                    name: Some(target.name.unwrap_or("")),
                    id: target.id,
                    error: 42,
                    topic: None,
                });
                continue;
            }
            let topic = if use_id {
                TopicId::new(target.id)
                    .ok()
                    .and_then(|id| catalog.by_id(id))
            } else {
                target.name.and_then(|name| catalog.by_name(name))
            };
            if let Some(topic) = topic {
                results.push(MetadataTopic {
                    name: Some(topic.name()),
                    id: topic.id().bytes(),
                    error: 0,
                    topic: Some(topic),
                });
            } else {
                let error = if use_id {
                    100
                } else if target
                    .name
                    .is_some_and(|name| catalog::validate_topic_name(name).is_err())
                {
                    17
                } else {
                    3
                };
                results.push(MetadataTopic {
                    name: if use_id { None } else { target.name },
                    id: if use_id { target.id } else { ZERO },
                    error,
                    topic: None,
                });
            }
        }
    } else {
        for topic in catalog.topics() {
            results.push(MetadataTopic {
                name: Some(topic.name()),
                id: topic.id().bytes(),
                error: 0,
                topic: Some(topic),
            });
        }
    }
    let mut writer = Writer::new(config.max_response_bytes)?;
    writer.header(header.correlation_id, flex)?;
    if version >= 3 {
        writer.i32(0)?;
    }
    writer.count(if semantic_error { 0 } else { 1 }, flex)?;
    if !semantic_error {
        writer.i32(config.broker_id)?;
        writer.string(Some(&config.advertised_host), flex, false)?;
        writer.i32(i32::from(config.advertised_port))?;
        if version >= 1 {
            writer.string(None, flex, true)?;
        }
        writer.tags(flex)?;
    }
    if version >= 2 {
        writer.string(
            if semantic_error {
                None
            } else {
                Some(&config.cluster_id)
            },
            flex,
            true,
        )?;
    }
    if version >= 1 {
        writer.i32(if semantic_error { -1 } else { config.broker_id })?;
    }
    writer.count(results.len(), flex)?;
    for result in results {
        writer.i16(result.error)?;
        writer.string(result.name, flex, version >= 12)?;
        if version >= 10 {
            writer.put(&result.id)?;
        }
        if version >= 1 {
            writer.byte(u8::from(
                !semantic_error && result.name.is_some_and(is_internal),
            ))?;
        }
        let partitions = result.topic.map_or(0, Topic::partition_count) as usize;
        writer.count(partitions, flex)?;
        for partition in 0..partitions {
            writer.i16(0)?;
            writer.i32(partition as i32)?;
            writer.i32(config.broker_id)?;
            if version >= 7 {
                writer.i32(0)?;
            }
            writer.count(1, flex)?;
            writer.i32(config.broker_id)?;
            writer.count(1, flex)?;
            writer.i32(config.broker_id)?;
            if version >= 5 {
                writer.count(0, flex)?;
            }
            writer.tags(flex)?;
        }
        if version >= 8 {
            writer.i32(
                if request.topic_ops && result.error != 100 && !semantic_error {
                    TOPIC_OPS
                } else {
                    i32::MIN
                },
            )?;
        }
        writer.tags(flex)?;
    }
    if (8..=10).contains(&version) {
        writer.i32(if request.cluster_ops && !semantic_error {
            CLUSTER_OPS
        } else {
            i32::MIN
        })?;
    }
    if version >= 13 {
        writer.i16(if semantic_error { 42 } else { 0 })?;
    }
    writer.tags(flex)?;
    Ok(writer.bytes)
}
fn is_internal(name: &str) -> bool {
    matches!(
        name,
        "__consumer_offsets" | "__transaction_state" | "__share_group_state"
    )
}

#[derive(Clone, Copy)]
struct ApiError {
    code: i16,
    message: Option<&'static str>,
}
const OK: ApiError = ApiError {
    code: 0,
    message: None,
};
const TIMEOUT: ApiError = ApiError {
    code: 7,
    message: None,
};
fn api_error(code: i16, message: &'static str) -> ApiError {
    ApiError {
        code,
        message: Some(message),
    }
}
fn catalog_error(error: &catalog::Error) -> ApiError {
    match error {
        catalog::Error::InvalidName => api_error(17, "Invalid topic name."),
        catalog::Error::ReservedName => api_error(
            42,
            "Creation of internal topic __cluster_metadata is prohibited.",
        ),
        catalog::Error::DuplicateName => api_error(36, "Topic already exists."),
        catalog::Error::NameCollision => {
            api_error(17, "Topic name collides with an existing topic.")
        }
        catalog::Error::InvalidPartitionCount => api_error(
            37,
            "Number of partitions was set to an invalid non-positive value.",
        ),
        catalog::Error::UnknownIdentity => ApiError {
            code: 100,
            message: None,
        },
        catalog::Error::Journal(journal::Error::FileBudgetExceeded)
        | catalog::Error::TopicBudgetExceeded
        | catalog::Error::IdentityBudgetExceeded
        | catalog::Error::PartitionBudgetExceeded
        | catalog::Error::OperationBudgetExceeded
        | catalog::Error::ReplayBudgetExceeded => {
            api_error(89, "Local catalog resource budget exceeded.")
        }
        _ => api_error(
            56,
            "Local catalog storage operation failed; outcome may be ambiguous.",
        ),
    }
}
fn timed_out(admitted: Instant, timeout: i32) -> bool {
    timeout <= 0 || admitted.elapsed() >= Duration::from_millis(timeout as u64)
}
fn check_active(
    reply: &oneshot::Sender<Result<Vec<u8>, Error>>,
    stopping: &AtomicBool,
) -> Result<(), Error> {
    if stopping.load(Ordering::Acquire) {
        Err(Error::Stopped)
    } else if reply.is_closed() {
        Err(Error::Canceled)
    } else {
        Ok(())
    }
}
fn create_validation(
    catalog: &Catalog,
    config: &Config,
    version: i16,
    topic: &CreateTopic<'_>,
) -> Result<u32, ApiError> {
    if topic.name == "__cluster_metadata" {
        return Err(api_error(
            42,
            "Creation of internal topic __cluster_metadata is prohibited.",
        ));
    }
    if catalog::validate_topic_name(topic.name).is_err() {
        return Err(api_error(17, "Invalid topic name."));
    }
    if catalog.by_name(topic.name).is_some() {
        return Err(api_error(36, "Topic already exists."));
    }
    if catalog.topics().any(|live| {
        live.name().len() == topic.name.len()
            && live
                .name()
                .bytes()
                .zip(topic.name.bytes())
                .all(|(a, b)| if a == b'.' { b'_' } else { a } == if b == b'.' { b'_' } else { b })
    }) {
        return Err(api_error(17, "Topic name collides with an existing topic."));
    }
    if topic.config_count != 0 {
        return Err(api_error(
            40,
            "Topic configuration overrides are not supported by this broker.",
        ));
    }
    let partitions = if !topic.assignments.is_empty() {
        if topic.replicas != -1 {
            return Err(api_error(42,"A manual partition assignment was specified, but replication factor was not set to -1."));
        }
        if topic.partitions != -1 {
            return Err(api_error(
                42,
                "A manual partition assignment was specified, but numPartitions was not set to -1.",
            ));
        }
        for (position, assignment) in topic.assignments.iter().enumerate() {
            if assignment.index < 0
                || assignment.index as usize >= topic.assignments.len()
                || topic.assignments[..position]
                    .iter()
                    .any(|other| other.index == assignment.index)
            {
                return Err(api_error(
                    39,
                    "partitions should be a consecutive 0-based integer sequence",
                ));
            }
            if assignment.brokers.as_slice() != [config.broker_id] {
                return Err(api_error(
                    39,
                    "Manual assignments must contain only this broker.",
                ));
            }
        }
        topic.assignments.len() as u32
    } else {
        let replicas = if topic.replicas == -1 && version >= 4 {
            1
        } else {
            topic.replicas
        };
        if replicas != 1 {
            return Err(api_error(
                38,
                "Replication factor must be one on this single-node broker.",
            ));
        }
        if topic.partitions == -1 && version >= 4 {
            config.default_partitions
        } else {
            u32::try_from(topic.partitions)
                .ok()
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    api_error(
                        37,
                        "Number of partitions was set to an invalid non-positive value.",
                    )
                })?
        }
    };
    let limits = config.catalog_limits;
    let payload = 34 + topic.name.len() as u64;
    if partitions > limits.max_partitions_per_topic()
        || catalog.total_partitions() + u64::from(partitions) > limits.max_total_partitions()
        || catalog.topic_count() >= limits.max_live_topics()
        || catalog.identity_count() >= limits.max_identities()
        || catalog.operation_count() >= limits.max_operations()
        || catalog.replay_bytes() + payload > limits.max_replay_bytes()
        || catalog.journal_bytes() + 32 + payload > limits.journal_limits().max_file_bytes()
    {
        return Err(api_error(89, "Local catalog resource budget exceeded."));
    }
    Ok(partitions)
}
fn fresh_id(catalog: &Catalog) -> Result<TopicId, ApiError> {
    for _ in 0..32 {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)
            .map_err(|_| api_error(56, "Operating system topic identity allocation failed."))?;
        bytes[6] = (bytes[6] & 15) | 64;
        bytes[8] = (bytes[8] & 63) | 128;
        // Apache Uuid.randomUuid also excludes base64url's leading '-'.
        if bytes[0] >> 2 == 62 {
            continue;
        }
        if let Ok(id) = TopicId::new(bytes) {
            if catalog.by_id(id).is_none() && !catalog.is_tombstoned(id) {
                return Ok(id);
            }
        }
    }
    Err(api_error(
        56,
        "Topic identity collision retry budget exhausted.",
    ))
}
fn create(
    catalog: &mut Catalog,
    config: &Config,
    header: RequestHeader<'_>,
    request: CreateRequest<'_>,
    admitted: Instant,
    reply: &oneshot::Sender<Result<Vec<u8>, Error>>,
    stopping: &AtomicBool,
) -> Result<Vec<u8>, Error> {
    let mut writer = Writer::new(config.max_response_bytes)?;
    let mut results = bounded_vec(request.topics.len())?;
    // Reserve a conservative complete response before any durable operation.
    let needed = request.topics.iter().try_fold(12usize, |sum, topic| {
        sum.checked_add(topic.name.len() + 260)
            .ok_or(Error::ResponseLimit)
    })?;
    if needed > config.max_response_bytes {
        return Err(Error::ResponseLimit);
    }
    for (position, topic) in request.topics.iter().enumerate() {
        check_active(reply, stopping)?;
        if request.topics[..position]
            .iter()
            .any(|other| other.name == topic.name)
        {
            continue;
        }
        let duplicate = request.topics[position + 1..]
            .iter()
            .any(|other| other.name == topic.name);
        let error = if duplicate {
            api_error(42, "Duplicate topic name.")
        } else {
            match create_validation(catalog, config, header.api_version, topic) {
                Err(error) => error,
                Ok(_) if timed_out(admitted, request.timeout) => TIMEOUT,
                Ok(_) if request.validate => OK,
                Ok(partitions) => match fresh_id(catalog) {
                    Err(error) => error,
                    Ok(id) => {
                        check_active(reply, stopping)?;
                        match catalog.create(topic.name, id, partitions) {
                            Ok(_) => {
                                if timed_out(admitted, request.timeout) {
                                    TIMEOUT
                                } else {
                                    OK
                                }
                            }
                            Err(error) => catalog_error(&error),
                        }
                    }
                },
            }
        };
        results.push((topic.name, error));
        if catalog.is_poisoned() {
            return Err(Error::StoragePoisoned);
        }
    }
    writer.header(header.correlation_id, false)?;
    writer.i32(0)?;
    writer.count(results.len(), false)?;
    for (name, error) in results {
        writer.string(Some(name), false, false)?;
        writer.i16(error.code)?;
        writer.string(error.message, false, true)?;
    }
    Ok(writer.bytes)
}
struct DeleteResult<'a> {
    name: Option<&'a str>,
    id: [u8; 16],
    error: ApiError,
}
fn delete(
    catalog: &mut Catalog,
    config: &Config,
    header: RequestHeader<'_>,
    request: DeleteRequest<'_>,
    admitted: Instant,
    reply: &oneshot::Sender<Result<Vec<u8>, Error>>,
    stopping: &AtomicBool,
) -> Result<Vec<u8>, Error> {
    let flex = header.api_version >= 4;
    let mut writer = Writer::new(config.max_response_bytes)?;
    let mut results = bounded_vec(request.topics.len())?;
    // Names resolved from IDs are copied before mutation, bounded by249 bytes.
    let mut names = bounded_vec(request.topics.len())?;
    for target in &request.topics {
        let resolved = if target.id != ZERO && target.name.is_none() {
            TopicId::new(target.id)
                .ok()
                .and_then(|id| catalog.by_id(id))
                .map(Topic::name)
        } else {
            target.name
        };
        let mut name = String::new();
        if let Some(value) = resolved {
            name.try_reserve_exact(value.len())
                .map_err(|_| Error::ResponseLimit)?;
            name.push_str(value);
        }
        names.push(resolved.map(|_| name));
    }
    let needed = request.topics.iter().try_fold(12usize, |sum, target| {
        sum.checked_add(target.name.map_or(249, str::len) + 280)
            .ok_or(Error::ResponseLimit)
    })?;
    if needed > config.max_response_bytes {
        return Err(Error::ResponseLimit);
    }
    for (position, target) in request.topics.iter().enumerate() {
        check_active(reply, stopping)?;
        let neither = target.name.is_none() && target.id == ZERO;
        let both = target.name.is_some() && target.id != ZERO;
        if neither || both {
            results.push(DeleteResult {
                name: target.name,
                id: target.id,
                error: api_error(
                    42,
                    if neither {
                        "Neither topic name nor id were specified."
                    } else {
                        "You may not specify both topic name and topic id."
                    },
                ),
            });
            continue;
        }
        let same = |other: &Target<'_>| {
            if target.name.is_some() {
                other.name == target.name && other.id == ZERO
            } else {
                other.name.is_none() && other.id == target.id
            }
        };
        if request.topics[..position].iter().any(same) {
            continue;
        }
        if request.topics[position + 1..].iter().any(same) {
            results.push(DeleteResult {
                name: target.name,
                id: target.id,
                error: api_error(
                    42,
                    if target.name.is_some() {
                        "Duplicate topic name."
                    } else {
                        "Duplicate topic id."
                    },
                ),
            });
            continue;
        }
        let topic = if let Some(name) = target.name {
            catalog.by_name(name)
        } else {
            TopicId::new(target.id)
                .ok()
                .and_then(|id| catalog.by_id(id))
        };
        let Some(topic) = topic else {
            results.push(DeleteResult {
                name: target.name,
                id: target.id,
                error: ApiError {
                    code: if target.name.is_some() { 3 } else { 100 },
                    message: None,
                },
            });
            continue;
        };
        let id = topic.id();
        let bytes = id.bytes();
        let alias = request.topics.iter().any(|other| {
            if target.name.is_some() {
                other.name.is_none() && other.id == bytes
            } else {
                other.id == ZERO
                    && other.name.is_some_and(|name| {
                        request
                            .topics
                            .iter()
                            .filter(|candidate| {
                                candidate.id == ZERO && candidate.name == Some(name)
                            })
                            .count()
                            == 1
                            && catalog.by_name(name).is_some_and(|live| live.id() == id)
                    })
            }
        });
        if alias {
            if target.name.is_some() {
                results.push(DeleteResult {
                    name: target.name,
                    id: bytes,
                    error: api_error(
                        42,
                        "The provided topic name maps to an ID that was already supplied.",
                    ),
                });
            }
            continue;
        }
        let error = if timed_out(admitted, request.timeout) {
            TIMEOUT
        } else {
            check_active(reply, stopping)?;
            match catalog.delete(id) {
                Ok(_) => {
                    if timed_out(admitted, request.timeout) {
                        TIMEOUT
                    } else {
                        OK
                    }
                }
                Err(error) => catalog_error(&error),
            }
        };
        results.push(DeleteResult {
            name: names[position].as_deref(),
            id: bytes,
            error,
        });
        if catalog.is_poisoned() {
            return Err(Error::StoragePoisoned);
        }
    }
    writer.header(header.correlation_id, flex)?;
    writer.i32(0)?;
    writer.count(results.len(), flex)?;
    for result in results {
        writer.string(result.name, flex, header.api_version >= 6)?;
        if header.api_version >= 6 {
            writer.put(&result.id)?;
        }
        writer.i16(result.error.code)?;
        if header.api_version >= 5 {
            writer.string(result.error.message, flex, true)?;
        }
        writer.tags(flex)?;
    }
    writer.tags(flex)?;
    Ok(writer.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error as StdError, sync::atomic::AtomicU64};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    fn parked_actor() -> (Arc<Router>, std::sync::mpsc::Sender<()>, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "partitionline-actor-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut config = Config::new(0, "127.0.0.1".into(), 19095, "actor-test".into());
        config.max_queued_requests = 1;
        let (sender, receiver) = mpsc::channel(1);
        let (release, gate) = std::sync::mpsc::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stopping);
        let worker_config = config.clone();
        let worker_path = path.clone();
        let task = tokio::task::spawn_blocking(move || {
            if gate.recv().is_err() {
                return;
            }
            if let Ok((catalog, _)) = Catalog::open(worker_path, worker_config.catalog_limits) {
                actor(catalog, receiver, &worker_config, &worker_stop);
            }
        });
        (
            Arc::new(Router {
                config,
                sender,
                stopping,
                failed: AtomicBool::new(false),
                task: Mutex::new(Some(task)),
            }),
            release,
            path,
        )
    }
    fn request(key: i16) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&(if key == 19 { 4i16 } else { 1i16 }).to_be_bytes());
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&(-1i16).to_be_bytes());
        if key == 19 {
            bytes.extend_from_slice(&1i32.to_be_bytes());
            bytes.extend_from_slice(&3i16.to_be_bytes());
            bytes.extend_from_slice(b"new");
            bytes.extend_from_slice(&1i32.to_be_bytes());
            bytes.extend_from_slice(&1i16.to_be_bytes());
            bytes.extend_from_slice(&0i32.to_be_bytes());
            bytes.extend_from_slice(&0i32.to_be_bytes());
            bytes.extend_from_slice(&60000i32.to_be_bytes());
            bytes.push(0);
        } else {
            bytes.extend_from_slice(&(-1i32).to_be_bytes());
        }
        bytes
    }
    async fn full(router: &Router) -> Result<(), Box<dyn StdError>> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while router.sender.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
    #[tokio::test]
    async fn queued_cancel_and_full_admission_never_mutate() -> Result<(), Box<dyn StdError>> {
        let (router, release, path) = parked_actor();
        let waiting = tokio::spawn({
            let router = Arc::clone(&router);
            async move { router.respond(request(19)).await }
        });
        full(&router).await?;
        assert!(matches!(
            router.respond(request(3)).await,
            Err(Error::QueueFull)
        ));
        waiting.abort();
        assert!(waiting.await.is_err());
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(2), async {
            while router.sender.capacity() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        router.respond(request(3)).await?;
        router.shutdown().await?;
        let verified = tokio::task::spawn_blocking({
            let path = path.clone();
            move || {
                Catalog::open(path, catalog::Limits::default())
                    .map(|(catalog, _)| (catalog.operation_count(), catalog.topic_count()))
            }
        })
        .await??;
        assert_eq!(verified, (0, 0));
        std::fs::remove_file(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn canceled_shutdown_retains_join_and_pending_work_is_rejected(
    ) -> Result<(), Box<dyn StdError>> {
        let (router, release, path) = parked_actor();
        let waiting = tokio::spawn({
            let router = Arc::clone(&router);
            async move { router.respond(request(19)).await }
        });
        full(&router).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(2), router.shutdown())
                .await
                .is_err()
        );
        assert!(router.task.lock().await.is_some());
        release.send(())?;
        router.shutdown().await?;
        assert!(matches!(waiting.await?, Err(Error::Stopped)));
        assert!(router.task.lock().await.is_none());
        router.shutdown().await?;
        let verified = tokio::task::spawn_blocking({
            let path = path.clone();
            move || {
                Catalog::open(path, catalog::Limits::default())
                    .map(|(catalog, _)| catalog.operation_count())
            }
        })
        .await??;
        assert_eq!(verified, 0);
        std::fs::remove_file(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn committed_operation_with_lost_receipt_replays() -> Result<(), Box<dyn StdError>> {
        let path = std::env::temp_dir().join(format!(
            "partitionline-lost-receipt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::task::spawn_blocking({
            let path = path.clone();
            move || -> Result<(), Error> {
                let config = Config::new(0, "127.0.0.1".into(), 19095, "actor-test".into());
                let (mut catalog, _) =
                    Catalog::open(&path, config.catalog_limits).map_err(Error::Catalog)?;
                let (reply, receiver) = oneshot::channel();
                let stop = AtomicBool::new(false);
                let response = process(
                    &mut catalog,
                    &request(19),
                    &config,
                    Instant::now(),
                    &reply,
                    &stop,
                )?;
                let identity = catalog.by_name("new").ok_or(Error::ActorFailed)?.id();
                drop(receiver);
                assert!(reply.send(Ok(response)).is_err());
                drop(catalog);
                let (reopened, _) =
                    Catalog::open(&path, config.catalog_limits).map_err(Error::Catalog)?;
                assert_eq!(reopened.by_name("new").map(Topic::id), Some(identity));
                assert_eq!(reopened.operation_count(), 1);
                Ok(())
            }
        })
        .await??;
        std::fs::remove_file(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn failed_actor_join_remains_failed_on_retry() -> Result<(), Box<dyn StdError>> {
        let (router, release, path) = parked_actor();
        drop(release);
        if let Some(old) = router.task.lock().await.take() {
            old.await?;
        }
        let failed = tokio::task::spawn_blocking(|| {
            std::panic::resume_unwind(Box::new("injected actor failure"))
        });
        *router.task.lock().await = Some(failed);
        assert!(matches!(router.shutdown().await, Err(Error::ActorFailed)));
        assert!(matches!(router.shutdown().await, Err(Error::ActorFailed)));
        assert!(!path.exists());
        Ok(())
    }
}

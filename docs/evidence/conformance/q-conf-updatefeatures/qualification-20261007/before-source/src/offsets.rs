//! Caller-driven offset operations with explicit topic names or UUIDs.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::admin::AdminConfig;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::protocol::api::{negotiate_api_versions, ApiVersionsResponse};
use crate::protocol::api_keys::{
    pick_version, FIND_COORDINATOR, METADATA, OFFSET_COMMIT, OFFSET_FETCH,
};
use crate::protocol::group::{
    decode_find_coordinator_response_coordinators, decode_offset_commit_response_data,
    decode_offset_fetch_response_data, encode_find_coordinator_request_typed,
    encode_offset_commit_request_data, encode_offset_fetch_request_data,
    validate_offset_commit_request_data, validate_offset_fetch_request_data,
    validate_offset_fetch_response_data, OffsetCommitRequestData, OffsetCommitResponseData,
    OffsetFetchRequestData, OffsetFetchResponseData, OffsetLimits, OffsetTopicIdentity,
    COORDINATOR_GROUP,
};
use crate::protocol::group::{decode_offset_metadata_bindings, encode_offset_metadata_names};
use crate::protocol::sasl;

const MAX_ATTEMPTS: u32 = 8;
const MAX_GROUPS: usize = 256;
const MAX_CONTROL_BYTES: usize = 1024 * 1024;

/// Wire identity policy, admission limits and total operation timeout.
#[derive(Clone, Debug, Default)]
pub struct OffsetOptions {
    /// Require UUIDs and v10, including when fetching all topics with `None`.
    /// False selects name-based v8/v9. Identities are never converted implicitly.
    pub topic_ids: bool,
    /// Total deadline. None uses the connection configuration's request timeout.
    pub timeout: Option<Duration>,
    /// Request, response and combined-result limits.
    pub limits: OffsetLimits,
}

/// Typed offset operations without group membership or background tasks.
///
/// Every operation negotiates on the actual coordinator and owns at most two
/// sockets. Cancellation drops them. OffsetCommit supplies the caller's group
/// generation or member epoch; this client does not join or heartbeat a group.
pub struct OffsetClient {
    cfg: AdminConfig,
    stats: Option<std::sync::Arc<crate::metrics::AdminTracker>>,
}

fn retryable(err: &Error) -> bool {
    matches!(err, Error::Io(_) | Error::Timeout)
        || err.broker_code().is_some_and(error::coordinator_retriable)
}

impl OffsetClient {
    pub(crate) fn set_stats(&mut self, stats: std::sync::Arc<crate::metrics::AdminTracker>) {
        self.stats = Some(stats);
    }
    pub(crate) async fn resolve_names(
        &self,
        names: Option<&[String]>,
        api: i16,
        deadline: Deadline,
        limits: OffsetLimits,
    ) -> Result<HashMap<String, [u8; 16]>> {
        // Preflight the borrowed names before any discovery or metadata I/O.
        drop(encode_offset_metadata_names(names, 10, limits)?);
        let mut last = Error::Timeout;
        let mut bytes = 0;
        for attempt in 0..3 {
            let result = async {
                let (mut conn, versions, _) = self.bootstrap(api, true, false, deadline).await?;
                let version = versions
                    .api_version(METADATA)
                    .and_then(|range| pick_version(range.min_version, range.max_version, 10, 13))
                    .ok_or_else(|| {
                        Error::Unsupported("offset UUID resolution requires Metadata v10-13".into())
                    })?;
                let encoded = encode_offset_metadata_names(names, version, limits)?;
                let body = conn
                    .roundtrip_deadline(
                        METADATA,
                        version,
                        |buf| {
                            buf.extend_from_slice(&encoded);
                            Ok(())
                        },
                        deadline,
                    )
                    .await?;
                charge_bytes(
                    &mut bytes,
                    body.len(),
                    limits
                        .wire_bytes
                        .checked_mul(3)
                        .ok_or_else(|| Error::protocol("offset metadata budget overflow"))?,
                )?;
                let bindings = decode_offset_metadata_bindings(&body, version, limits)?;
                if names.is_some_and(|names| names.iter().any(|name| !bindings.contains_key(name)))
                {
                    return Err(Error::protocol("offset metadata omitted a requested topic"));
                }
                deadline.check_expired()?;
                Ok(bindings)
            }
            .await;
            match result {
                Ok(bindings) => return Ok(bindings),
                Err(err) if retryable(&err) => last = err,
                Err(err) => return Err(err),
            }
            if attempt < 2 {
                self.backoff(&last, attempt, deadline).await?;
            }
        }
        Err(last)
    }
    /// Validate connection settings without network I/O.
    pub fn new(mut cfg: AdminConfig) -> Result<Self> {
        if cfg.bootstrap.len() > 16 {
            return Err(Error::protocol("too many offset bootstrap servers"));
        }
        if cfg.client_id.len() > i16::MAX as usize {
            return Err(Error::protocol("offset client ID exceeds header limit"));
        }
        cfg.bootstrap = crate::net::parse_and_validate_addresses(&cfg.bootstrap)?;
        Ok(Self { cfg, stats: None })
    }

    fn deadline(&self, options: &OffsetOptions) -> Result<Deadline> {
        let target = Instant::now()
            .checked_add(options.timeout.unwrap_or(self.cfg.request_timeout))
            .ok_or(Error::Timeout)?;
        let deadline = Deadline::from_std(target);
        deadline.check_expired()?;
        options.limits.validate()?;
        Ok(deadline)
    }

    /// Commit exact topic identities and retain every partition error.
    ///
    /// UUID-only intent requires v10; it cannot fall back to a name. Errors
    /// 14/15/16 and transport failures retry at most eight times under the original
    /// deadline. A lost response may follow a successful commit. Empty topics
    /// are a local no-op after validation.
    pub async fn commit(
        &mut self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetCommitResponseData> {
        let deadline = self.deadline(options)?;
        let version = if options.topic_ids { 10 } else { 9 };
        validate_offset_commit_request_data(request, version, options.limits)?;
        validate_commit(request)?;
        deadline.check_expired()?;
        if request.topics.is_empty() {
            return Ok(OffsetCommitResponseData::default());
        }
        deadline
            .run(self.commit_until(request, options, deadline))
            .await
    }

    /// Fetch one or more groups, preserving null/all-topic versus empty topic
    /// selection, nullable metadata, leader epochs and raw response identities.
    ///
    /// Groups sharing a coordinator are batched. Results follow request group
    /// order; topic and partition order remain the broker's order. Unknown UUIDs
    /// in an all-topic response remain UUIDs. Duplicate requested groups or
    /// partitions, unexpected response identities and incomplete replies fail.
    pub async fn fetch(
        &mut self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetFetchResponseData> {
        let deadline = self.deadline(options)?;
        let version = if options.topic_ids { 10 } else { 9 };
        validate_offset_fetch_request_data(request, version, options.limits)?;
        validate_fetch(request)?;
        deadline.check_expired()?;
        if request.groups.is_empty() {
            return Ok(OffsetFetchResponseData::default());
        }
        deadline
            .run(self.fetch_until(request, options, deadline))
            .await
    }

    async fn backoff(&self, err: &Error, attempt: u32, deadline: Deadline) -> Result<()> {
        let (base, max) = if err.broker_code().is_some() {
            (self.cfg.retry_backoff, self.cfg.retry_backoff_max)
        } else {
            (self.cfg.reconnect_backoff, self.cfg.reconnect_backoff_max)
        };
        crate::config::sleep_retry_backoff(base, max, attempt, deadline.target().into_std()).await;
        deadline.check_expired()
    }

    async fn commit_until(
        &self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
    ) -> Result<OffsetCommitResponseData> {
        let mut control_bytes = 0;
        let mut response_bytes = 0;
        let mut last = Error::Timeout;
        for attempt in 0..MAX_ATTEMPTS {
            match self
                .commit_once(
                    request,
                    options,
                    deadline,
                    &mut control_bytes,
                    &mut response_bytes,
                )
                .await
            {
                Ok(response) => return Ok(response),
                Err(err) if retryable(&err) => last = err,
                Err(err) => return Err(err),
            }
            if attempt + 1 < MAX_ATTEMPTS {
                self.backoff(&last, attempt, deadline).await?;
            }
        }
        Err(last)
    }

    async fn fetch_until(
        &self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
    ) -> Result<OffsetFetchResponseData> {
        let mut control_bytes = 0;
        let mut response_bytes = 0;
        let mut last = Error::Timeout;
        for attempt in 0..MAX_ATTEMPTS {
            match self
                .fetch_once(
                    request,
                    options,
                    deadline,
                    &mut control_bytes,
                    &mut response_bytes,
                )
                .await
            {
                Ok(response) => return Ok(response),
                Err(err) if retryable(&err) => last = err,
                Err(err) => return Err(err),
            }
            if attempt + 1 < MAX_ATTEMPTS {
                self.backoff(&last, attempt, deadline).await?;
            }
        }
        Err(last)
    }

    fn version(versions: &ApiVersionsResponse, api: i16, ids: bool, member: bool) -> Result<i16> {
        let (min, max) = if ids {
            (10, 10)
        } else {
            (if member { 9 } else { 8 }, 9)
        };
        versions
            .api_version(api)
            .and_then(|range| pick_version(range.min_version, range.max_version, min, max))
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "broker does not support offset API{api} v{min}-{max}"
                ))
            })
    }

    async fn open(
        &self,
        address: &str,
        api: i16,
        ids: bool,
        member: bool,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse, i16)> {
        let cfg = &self.cfg;
        let mut conn = BrokerConn::connect_tls(
            address,
            &cfg.client_id,
            cfg.connect_timeout.min(deadline.remaining()?),
            cfg.tls.as_ref(),
        )
        .await?;
        if let Some(stats) = &self.stats {
            conn.set_stats(std::sync::Arc::clone(stats));
        }
        let versions = negotiate_api_versions(&mut conn, deadline.remaining()?).await?;
        let version = Self::version(&versions, api, ids, member)?;
        sasl::apply_api_keys(&mut conn, &versions.api_keys);
        sasl::authenticate(
            &mut conn,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            deadline.remaining()?,
        )
        .await?;
        Ok((conn, versions, version))
    }

    async fn bootstrap(
        &self,
        api: i16,
        ids: bool,
        member: bool,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse, i16)> {
        let mut last = Error::Timeout;
        for address in &self.cfg.bootstrap {
            match self.open(address, api, ids, member, deadline).await {
                Ok(conn) => return Ok(conn),
                Err(err) if retryable(&err) => last = err,
                Err(err) => return Err(err),
            }
        }
        Err(last)
    }

    async fn coordinator(
        conn: &mut BrokerConn,
        versions: &ApiVersionsResponse,
        group: &str,
        deadline: Deadline,
        control_bytes: &mut usize,
    ) -> Result<String> {
        let version = versions
            .api_version(FIND_COORDINATOR)
            .and_then(|range| pick_version(range.min_version, range.max_version, 1, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support FindCoordinator v1-6".into())
            })?;
        let body = conn
            .roundtrip_deadline(
                FIND_COORDINATOR,
                version,
                |buf| encode_find_coordinator_request_typed(buf, version, group, COORDINATOR_GROUP),
                deadline,
            )
            .await?;
        charge_bytes(control_bytes, body.len(), MAX_CONTROL_BYTES)?;
        if body.len() > 64 * 1024 || (version >= 4 && body.get(4) != Some(&2)) {
            return Err(Error::protocol(
                "offset coordinator response exceeds bounds",
            ));
        }
        let mut raw = body.as_ref();
        let (mut coordinators, _) =
            decode_find_coordinator_response_coordinators(&mut raw, version)?;
        if !raw.is_empty() || coordinators.len() != 1 {
            return Err(Error::protocol("invalid offset coordinator response"));
        }
        let coordinator = coordinators
            .pop()
            .ok_or_else(|| Error::protocol("missing offset coordinator"))?;
        if version >= 4 && coordinator.key != group {
            return Err(Error::protocol("offset coordinator key differs"));
        }
        if coordinator.error_code != 0 {
            return Err(Error::broker(coordinator.error_code, "FindCoordinator"));
        }
        if coordinator.host.is_empty() || !(1..=65535).contains(&coordinator.port) {
            return Err(Error::protocol("invalid offset coordinator address"));
        }
        Ok(if coordinator.host.contains(':') {
            format!("[{}]:{}", coordinator.host, coordinator.port)
        } else {
            format!("{}:{}", coordinator.host, coordinator.port)
        })
    }

    async fn commit_once(
        &self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
        control_bytes: &mut usize,
        response_bytes: &mut usize,
    ) -> Result<OffsetCommitResponseData> {
        let (mut bootstrap, versions, bootstrap_version) = self
            .bootstrap(OFFSET_COMMIT, options.topic_ids, false, deadline)
            .await?;
        let address = Self::coordinator(
            &mut bootstrap,
            &versions,
            &request.group_id,
            deadline,
            control_bytes,
        )
        .await?;
        let (mut conn, version) = if address == bootstrap.addr() {
            (bootstrap, bootstrap_version)
        } else {
            let (conn, _, version) = self
                .open(&address, OFFSET_COMMIT, options.topic_ids, false, deadline)
                .await?;
            (conn, version)
        };
        let encoded = encode_offset_commit_request_data(request, version, options.limits)?;
        deadline.check_expired()?;
        let body = conn
            .roundtrip_deadline(
                OFFSET_COMMIT,
                version,
                |buf| {
                    buf.extend_from_slice(&encoded);
                    Ok(())
                },
                deadline,
            )
            .await?;
        charge_bytes(response_bytes, body.len(), response_limit(options.limits)?)?;
        let response = decode_offset_commit_response_data(&body, version, options.limits)?;
        validate_commit_response(request, &response)?;
        if let Some(code) = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| error::coordinator_retriable(*code))
        {
            return Err(Error::broker(code, "OffsetCommit"));
        }
        deadline.check_expired()?;
        Ok(response)
    }

    async fn fetch_once(
        &self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
        control_bytes: &mut usize,
        response_bytes: &mut usize,
    ) -> Result<OffsetFetchResponseData> {
        let member = request
            .groups
            .iter()
            .any(|group| group.member_id.is_some() || group.member_epoch != -1);
        let (mut bootstrap, versions, _) = self
            .bootstrap(OFFSET_FETCH, options.topic_ids, member, deadline)
            .await?;
        let mut routes = BTreeMap::<String, Vec<usize>>::new();
        for (index, group) in request.groups.iter().enumerate() {
            let address = Self::coordinator(
                &mut bootstrap,
                &versions,
                &group.group_id,
                deadline,
                control_bytes,
            )
            .await?;
            routes.entry(address).or_default().push(index);
        }
        let mut slots = vec![None; request.groups.len()];
        let mut throttle_time_ms = 0;
        for (address, indexes) in routes {
            let (mut conn, _, version) = self
                .open(&address, OFFSET_FETCH, options.topic_ids, member, deadline)
                .await?;
            let groups = indexes
                .iter()
                .map(|index| {
                    request
                        .groups
                        .get(*index)
                        .cloned()
                        .ok_or_else(|| Error::protocol("missing offset request group"))
                })
                .collect::<Result<Vec<_>>>()?;
            let batched = OffsetFetchRequestData {
                groups,
                require_stable: request.require_stable,
            };
            let encoded = encode_offset_fetch_request_data(&batched, version, options.limits)?;
            deadline.check_expired()?;
            let body = conn
                .roundtrip_deadline(
                    OFFSET_FETCH,
                    version,
                    |buf| {
                        buf.extend_from_slice(&encoded);
                        Ok(())
                    },
                    deadline,
                )
                .await?;
            charge_bytes(response_bytes, body.len(), response_limit(options.limits)?)?;
            let response = decode_offset_fetch_response_data(&body, version, options.limits)?;
            validate_fetch_response(&batched, &response)?;
            if let Some(code) = response
                .groups
                .iter()
                .flat_map(|group| {
                    std::iter::once(group.error_code).chain(
                        group
                            .topics
                            .iter()
                            .flat_map(|topic| &topic.partitions)
                            .map(|partition| partition.error_code),
                    )
                })
                .find(|code| error::coordinator_retriable(*code))
            {
                return Err(Error::broker(code, "OffsetFetch"));
            }
            throttle_time_ms = throttle_time_ms.max(response.throttle_time_ms);
            for group in response.groups {
                let index = request
                    .groups
                    .iter()
                    .position(|requested| requested.group_id == group.group_id)
                    .ok_or_else(|| Error::protocol("unexpected offset group"))?;
                *slots
                    .get_mut(index)
                    .ok_or_else(|| Error::protocol("missing offset result slot"))? = Some(group);
            }
            // Bound accumulated results before accepting another coordinator.
            let groups = slots
                .iter_mut()
                .filter_map(Option::take)
                .collect::<Vec<_>>();
            let retained = OffsetFetchResponseData {
                throttle_time_ms,
                groups,
            };
            validate_offset_fetch_response_data(
                &retained,
                if options.topic_ids { 10 } else { 9 },
                options.limits,
            )?;
            for group in retained.groups {
                let index = request
                    .groups
                    .iter()
                    .position(|requested| requested.group_id == group.group_id)
                    .ok_or_else(|| Error::protocol("unexpected retained offset group"))?;
                *slots
                    .get_mut(index)
                    .ok_or_else(|| Error::protocol("missing retained offset slot"))? = Some(group);
            }
            deadline.check_expired()?;
        }
        let groups = slots
            .into_iter()
            .map(|slot| slot.ok_or_else(|| Error::protocol("missing offset result group")))
            .collect::<Result<Vec<_>>>()?;
        Ok(OffsetFetchResponseData {
            throttle_time_ms,
            groups,
        })
    }
}

fn charge_bytes(used: &mut usize, amount: usize, limit: usize) -> Result<()> {
    *used = used
        .checked_add(amount)
        .filter(|next| *next <= limit)
        .ok_or_else(|| Error::protocol("offset cumulative bytes exceed limit"))?;
    Ok(())
}

pub(crate) fn collect_bounded_commits(
    offsets: impl IntoIterator<
        Item = (
            impl Into<crate::TopicPartition>,
            impl Into<crate::OffsetAndMetadata>,
        ),
    >,
) -> Result<Vec<(crate::TopicPartition, crate::OffsetAndMetadata)>> {
    let limits = OffsetLimits::default();
    let mut out = Vec::new();
    let mut strings = 0;
    for (tp, md) in offsets {
        if out.len() >= limits.array_elements {
            return Err(Error::protocol("too many committed partitions"));
        }
        let tp = tp.into();
        let md = md.into();
        if tp.topic.len() > limits.string_bytes || md.metadata.len() > limits.string_bytes {
            return Err(Error::protocol(
                "offset topic/metadata string exceeds limit",
            ));
        }
        charge_bytes(&mut strings, tp.topic.len(), limits.total_string_bytes)?;
        charge_bytes(&mut strings, md.metadata.len(), limits.total_string_bytes)?;
        out.push((tp, md));
    }
    Ok(out)
}

pub(crate) fn collect_bounded_partitions(
    partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
) -> Result<Vec<crate::TopicPartition>> {
    let limits = OffsetLimits::default();
    let mut out = Vec::new();
    let mut strings = 0;
    for partition in partitions {
        if out.len() >= limits.array_elements {
            return Err(Error::protocol("too many offset partitions"));
        }
        let partition = partition.into();
        if partition.topic.len() > limits.string_bytes {
            return Err(Error::protocol("offset topic string exceeds limit"));
        }
        charge_bytes(
            &mut strings,
            partition.topic.len(),
            limits.total_string_bytes,
        )?;
        out.push(partition);
    }
    Ok(out)
}

fn response_limit(limits: OffsetLimits) -> Result<usize> {
    limits
        .wire_bytes
        .checked_mul(
            usize::try_from(MAX_ATTEMPTS)
                .map_err(|_| Error::protocol("offset attempt count overflow"))?,
        )
        .ok_or_else(|| Error::protocol("offset response limit overflow"))
}

fn validate_commit(request: &OffsetCommitRequestData) -> Result<()> {
    let mut identities = HashSet::new();
    for topic in &request.topics {
        if !identities.insert(&topic.identity) {
            return Err(Error::protocol("duplicate offset commit topic"));
        }
        let mut partitions = HashSet::new();
        for partition in &topic.partitions {
            if !partitions.insert(partition.partition_index) {
                return Err(Error::protocol("duplicate offset commit partition"));
            }
        }
    }
    Ok(())
}

fn validate_fetch(request: &OffsetFetchRequestData) -> Result<()> {
    if request.groups.len() > MAX_GROUPS {
        return Err(Error::protocol("too many offset groups"));
    }
    let mut groups = HashSet::new();
    for group in &request.groups {
        if !groups.insert(&group.group_id) {
            return Err(Error::protocol("duplicate offset fetch group"));
        }
        if let Some(topics) = &group.topics {
            let mut identities = HashSet::new();
            for topic in topics {
                if !identities.insert(&topic.identity) {
                    return Err(Error::protocol("duplicate offset fetch topic"));
                }
                let mut partitions = HashSet::new();
                for partition in &topic.partition_indexes {
                    if !partitions.insert(partition) {
                        return Err(Error::protocol("duplicate offset fetch partition"));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_commit_response(
    request: &OffsetCommitRequestData,
    response: &OffsetCommitResponseData,
) -> Result<()> {
    let wanted = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(move |partition| (&topic.identity, partition.partition_index))
        })
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut topics = HashSet::new();
    for topic in &response.topics {
        if !topics.insert(&topic.identity)
            || !request
                .topics
                .iter()
                .any(|requested| requested.identity == topic.identity)
        {
            return Err(Error::protocol("unexpected or duplicate committed topic"));
        }
        for partition in &topic.partitions {
            let key = (&topic.identity, partition.partition_index);
            if !wanted.contains(&key) || !seen.insert(key) {
                return Err(Error::protocol(
                    "unexpected or duplicate committed partition",
                ));
            }
        }
    }
    if wanted != seen || topics.len() != request.topics.len() {
        return Err(Error::protocol("incomplete offset commit response"));
    }
    Ok(())
}

fn validate_fetch_response(
    request: &OffsetFetchRequestData,
    response: &OffsetFetchResponseData,
) -> Result<()> {
    let wanted = request
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<HashMap<_, _>>();
    let mut groups = HashSet::new();
    for group in &response.groups {
        let requested = wanted
            .get(group.group_id.as_str())
            .ok_or_else(|| Error::protocol("unexpected fetched group"))?;
        if !groups.insert(group.group_id.as_str()) {
            return Err(Error::protocol("duplicate fetched group"));
        }
        // A group error can legitimately omit topic results.
        if group.error_code != 0 {
            continue;
        }
        let mut topics = HashSet::<&OffsetTopicIdentity>::new();
        for topic in &group.topics {
            if !topics.insert(&topic.identity) {
                return Err(Error::protocol("duplicate fetched topic"));
            }
            let expected = match &requested.topics {
                None => None,
                Some(wanted) => Some(
                    wanted
                        .iter()
                        .find(|wanted| wanted.identity == topic.identity)
                        .ok_or_else(|| Error::protocol("unexpected fetched topic identity"))?,
                ),
            };
            let mut partitions = HashSet::new();
            for partition in &topic.partitions {
                if !partitions.insert(partition.partition_index)
                    || expected.is_some_and(|expected| {
                        !expected
                            .partition_indexes
                            .contains(&partition.partition_index)
                    })
                {
                    return Err(Error::protocol("unexpected or duplicate fetched partition"));
                }
            }
            if expected.is_some_and(|expected| partitions.len() != expected.partition_indexes.len())
            {
                return Err(Error::protocol("incomplete fetched partitions"));
            }
        }
        if requested
            .topics
            .as_ref()
            .is_some_and(|wanted| topics.len() != wanted.len())
        {
            return Err(Error::protocol("incomplete fetched topics"));
        }
    }
    if groups.len() != request.groups.len() {
        return Err(Error::protocol("incomplete offset fetch groups"));
    }
    Ok(())
}

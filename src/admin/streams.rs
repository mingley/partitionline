use super::{Admin, AdminConfig};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::protocol::api::{negotiate_api_versions, ApiVersionsResponse};
use crate::protocol::api_keys::{pick_version, FIND_COORDINATOR, STREAMS_GROUP_DESCRIBE};
use crate::protocol::group::{
    decode_find_coordinator_response_coordinators, encode_find_coordinator_request_typed,
    COORDINATOR_GROUP,
};
use crate::protocol::{sasl, streams};

/// Admission policy and total deadline for Streams group descriptions.
#[derive(Debug, Clone, Default)]
pub struct DescribeStreamsGroupsOptions {
    /// Request the broker's authorized-operation bitfield.
    pub include_authorized_operations: bool,
    /// Explicitly allow v0; its wire version cannot identify release stability.
    pub allow_unstable: bool,
    /// Override the Admin request timeout for the entire operation.
    pub timeout: Option<Duration>,
    /// Limits apply to requests, responses, retained descriptions and expanded
    /// duplicates. They cover requested storage, rather than allocator RSS.
    pub limits: streams::Limits,
}

/// One result in the caller's group order, including repeated IDs.
#[derive(Debug, Clone)]
pub struct StreamsGroupDescription {
    /// The requested group ID.
    pub group_id: String,
    /// A complete typed API89 response entry, including any terminal broker
    /// error. Discovery, transport and capability failures retain their own
    /// origin here instead of fabricating a Streams protocol response.
    pub description: std::result::Result<streams::DescribedStreamsGroup, Arc<Error>>,
}

impl StreamsGroupDescription {
    /// The protocol family queried by this operation.
    pub fn group_type(&self) -> &'static str {
        "Streams"
    }
}

const MAX_ATTEMPTS: u32 = 8;
const MAX_NODES: usize = 256;
const MAX_CONTROL_BYTES: usize = 1024 * 1024;

async fn open_streams_describe_socket(
    cfg: &AdminConfig,
    stats: &Arc<crate::metrics::AdminTracker>,
    address: &str,
    deadline: Deadline,
) -> Result<(BrokerConn, ApiVersionsResponse)> {
    let opened = async {
        let mut conn = BrokerConn::connect_tls(
            address,
            &cfg.client_id,
            cfg.connect_timeout.min(deadline.remaining()?),
            cfg.tls.as_ref(),
        )
        .await?;
        conn.set_stats(Arc::clone(stats));
        let versions = negotiate_api_versions(&mut conn, deadline.remaining()?).await?;
        if versions
            .api_version(STREAMS_GROUP_DESCRIBE)
            .and_then(|api| pick_version(api.min_version, api.max_version, 0, 0))
            .is_none()
        {
            return Err(Error::Unsupported(
                "broker does not support StreamsGroupDescribe v0".into(),
            ));
        }
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
        Ok((conn, versions))
    }
    .await;
    // A failed handshake drops its socket in this call. Yield so the broker
    // observes that close before the error returns. A successful dial keeps
    // the socket and does not wait.
    if opened.is_err() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    opened
}

async fn open_streams_describe_known(
    cfg: &AdminConfig,
    stats: &Arc<crate::metrics::AdminTracker>,
    address: &str,
    keys: &[crate::protocol::api::ApiVersion],
    deadline: Deadline,
) -> Result<(BrokerConn, ApiVersionsResponse)> {
    let opened = async {
        let mut conn = BrokerConn::connect_tls(
            address,
            &cfg.client_id,
            cfg.connect_timeout.min(deadline.remaining()?),
            cfg.tls.as_ref(),
        )
        .await?;
        conn.set_stats(Arc::clone(stats));
        let versions = ApiVersionsResponse {
            error_code: 0,
            api_keys: keys.to_vec(),
            throttle_time_ms: 0,
            supported_features: Vec::new(),
            finalized_features_epoch: None,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
        };
        if versions
            .api_version(STREAMS_GROUP_DESCRIBE)
            .and_then(|api| pick_version(api.min_version, api.max_version, 0, 0))
            .is_none()
        {
            return Err(Error::Unsupported(
                "broker does not support StreamsGroupDescribe v0".into(),
            ));
        }
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
        Ok((conn, versions))
    }
    .await;
    if opened.is_err() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    opened
}

fn streams_retryable(err: &Error) -> bool {
    matches!(err, Error::Io(_) | Error::Timeout)
        || err.broker_code().is_some_and(error::coordinator_retriable)
}

impl Admin {
    /// Describe Streams groups using API89 v0 on each actual GROUP coordinator.
    ///
    /// Explicit opt-in is required before I/O. Each operation owns at most two
    /// additional sockets and no background tasks. Connections authenticate
    /// using the Admin settings and negotiate their own API versions. Successful
    /// groups survive retries of other groups; only errors14/15/16 and retriable
    /// transport failures retry, up to eight attempts under one deadline.
    ///
    /// Returned entries preserve request order/duplicates and every wire field.
    /// Local limits, malformed responses or the total deadline fail the whole
    /// operation. Cancellation drops its sockets while leaving Admin reusable.
    /// This operation does not join a group or run Streams tasks.
    pub async fn describe_streams_groups(
        &mut self,
        group_ids: &[&str],
        options: &DescribeStreamsGroupsOptions,
    ) -> Result<Vec<StreamsGroupDescription>> {
        let target = Instant::now()
            .checked_add(options.timeout.unwrap_or(self.cfg.request_timeout))
            .ok_or(Error::Timeout)?;
        let deadline = Deadline::from_std(target);
        deadline.check_expired()?;
        if !options.allow_unstable {
            return Err(Error::Unsupported(
                "Streams v0 requires explicit allow_unstable".into(),
            ));
        }
        streams::validate_describe_ids(group_ids, options.limits)?;
        // Bound the routing/result slots before allocating either structure.
        let slots = group_ids
            .len()
            .checked_mul(
                std::mem::size_of::<StreamsGroupDescription>()
                    + std::mem::size_of::<Option<StreamsGroupDescription>>(),
            )
            .ok_or_else(|| Error::protocol("Streams routing storage overflow"))?;
        if slots > options.limits.decoded_bytes {
            return Err(Error::protocol("Streams routing storage exceeds limit"));
        }
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        let mut positions = Vec::with_capacity(group_ids.len());
        let mut indexes = HashMap::new();
        for id in group_ids {
            let index = match indexes.get(*id) {
                Some(index) => *index,
                None => {
                    let index = ids.len();
                    ids.push((*id).to_owned());
                    let _previous = indexes.insert(*id, index);
                    index
                }
            };
            positions.push(index);
        }
        deadline.check_expired()?;
        let unique = self.describe_streams_until(&ids, options, deadline).await?;
        let references = positions
            .iter()
            .filter_map(|&index| {
                unique
                    .get(index)
                    .and_then(|group| group.description.as_ref().ok())
            })
            .collect::<Vec<_>>();
        streams::validate_described_groups(&references, options.limits)?;
        let output = positions
            .into_iter()
            .map(|index| {
                unique
                    .get(index)
                    .cloned()
                    .ok_or_else(|| Error::protocol("missing Streams result slot"))
            })
            .collect::<Result<Vec<_>>>()?;
        deadline.check_expired()?;
        Ok(output)
    }

    /// Race Streams describe handshakes and return once the losers have stopped.
    ///
    /// A later describe continues with coordinator lookup, which hides a
    /// handshake that was only aborted.
    #[doc(hidden)]
    pub async fn test_race_streams_bootstrap(&mut self, extra: String) -> Result<()> {
        self.cfg.bootstrap.push(extra);
        let deadline = Deadline::from_std(
            Instant::now()
                .checked_add(self.cfg.request_timeout)
                .ok_or(Error::Timeout)?,
        );
        let _opened = self.streams_bootstrap(deadline).await?;
        Ok(())
    }

    /// Point later Streams describe dials at `address`.
    #[doc(hidden)]
    pub fn test_point_bootstrap(&mut self, address: String) {
        self.cfg.bootstrap = vec![address];
        self.cfg.retry_backoff = Duration::ZERO;
        self.cfg.retry_backoff_max = Duration::ZERO;
        self.cfg.reconnect_backoff = Duration::ZERO;
        self.cfg.reconnect_backoff_max = Duration::ZERO;
        self.cfg.connect_timeout = Duration::from_millis(200);
        self.cfg.request_timeout = Duration::from_millis(200);
    }

    async fn open_streams_describe(
        &self,
        address: &str,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        if let Some(keys) = self.peer_versions.get(address).cloned() {
            return open_streams_describe_known(&self.cfg, &self.stats, address, &keys, deadline)
                .await;
        }
        open_streams_describe_socket(&self.cfg, &self.stats, address, deadline).await
    }

    async fn streams_bootstrap(
        &self,
        deadline: Deadline,
    ) -> Result<(
        BrokerConn,
        ApiVersionsResponse,
        Vec<(String, Vec<crate::protocol::api::ApiVersion>)>,
    )> {
        if self.cfg.bootstrap.len() > 16 {
            return Err(Error::protocol("too many Streams bootstrap servers"));
        }
        if self.cfg.bootstrap.len() <= 1 {
            let mut last = Error::Timeout;
            for address in &self.cfg.bootstrap {
                match self.open_streams_describe(address, deadline).await {
                    Ok((conn, versions)) => return Ok((conn, versions, Vec::new())),
                    Err(err) if streams_retryable(&err) => last = err,
                    Err(err) => return Err(err),
                }
            }
            return Err(last);
        }
        let mut inflight = tokio::task::JoinSet::new();
        for (index, address) in self.cfg.bootstrap.iter().enumerate() {
            let cfg = self.cfg.clone();
            let stats = Arc::clone(&self.stats);
            let address = address.clone();
            let known = self
                .peer_versions
                .get(&address)
                .cloned()
                .unwrap_or_default();
            let _ = inflight.spawn(async move {
                let opened = if known.is_empty() {
                    open_streams_describe_socket(&cfg, &stats, &address, deadline).await
                } else {
                    open_streams_describe_known(&cfg, &stats, &address, &known, deadline).await
                };
                (index, opened)
            });
        }
        let mut last = Error::protocol("all bootstrap servers failed");
        let mut finished = Vec::new();
        while !inflight.is_empty() {
            let Some(joined) = inflight.join_next().await else {
                break;
            };
            match joined {
                Ok((index, Ok(opened))) => {
                    finished.push((index, opened));
                    let gather = deadline
                        .remaining()
                        .unwrap_or(Duration::ZERO)
                        .min(Duration::from_millis(5));
                    let gather_done = tokio::time::sleep(gather);
                    tokio::pin!(gather_done);
                    loop {
                        tokio::select! {
                            biased;
                            joined = inflight.join_next(), if !inflight.is_empty() => {
                                match joined {
                                    Some(Ok((index, Ok(opened)))) => finished.push((index, opened)),
                                    Some(Ok((_, Err(error)))) => last = error,
                                    _ => {}
                                }
                            }
                            () = &mut gather_done => break,
                        }
                    }
                    break;
                }
                Ok((_, Err(error))) => last = error,
                Err(error) => last = Error::protocol(format!("streams bootstrap: {error}")),
            }
        }
        Admin::finish_aborted_indexed_handshakes(inflight).await;
        finished.sort_by_key(|(index, _)| *index);
        let mut rest = finished.into_iter();
        let Some((_, (conn, versions))) = rest.next() else {
            return Err(last);
        };
        let mut learned = Vec::new();
        for (_, (mut spare, spare_versions)) in rest {
            if !spare_versions.api_keys.is_empty() {
                learned.push((spare.addr().to_string(), spare_versions.api_keys));
            }
            Admin::retire_admin_socket(&mut spare).await;
        }
        Ok((conn, versions, learned))
    }

    async fn streams_coordinator(
        conn: &mut BrokerConn,
        id: &str,
        version: i16,
        deadline: Deadline,
        control_bytes: &mut usize,
    ) -> Result<String> {
        let body = conn
            .roundtrip_deadline(
                FIND_COORDINATOR,
                version,
                |buf| encode_find_coordinator_request_typed(buf, version, id, COORDINATOR_GROUP),
                deadline,
            )
            .await?;
        *control_bytes = control_bytes
            .checked_add(body.len())
            .ok_or_else(|| Error::protocol("Streams control bytes overflow"))?;
        if body.len() > 64 * 1024
            || *control_bytes > MAX_CONTROL_BYTES
            || (version >= 4 && body.get(4) != Some(&2))
        {
            return Err(Error::protocol(
                "Streams coordinator response exceeds bounds",
            ));
        }
        let mut cursor = body.as_ref();
        let (mut coordinators, _) =
            decode_find_coordinator_response_coordinators(&mut cursor, version)?;
        if !cursor.is_empty() || coordinators.len() != 1 {
            return Err(Error::protocol("invalid Streams coordinator response"));
        }
        let coordinator = coordinators
            .pop()
            .ok_or_else(|| Error::protocol("missing Streams coordinator"))?;
        if version >= 4 && coordinator.key != id {
            return Err(Error::protocol("Streams coordinator key differs"));
        }
        if coordinator.error_code != 0 {
            return Err(Error::broker(coordinator.error_code, "FindCoordinator"));
        }
        crate::cluster::dial_address(&coordinator.host, coordinator.port)
            .ok_or_else(|| Error::protocol("invalid Streams coordinator address"))
    }

    async fn describe_streams_until(
        &mut self,
        ids: &[String],
        options: &DescribeStreamsGroupsOptions,
        deadline: Deadline,
    ) -> Result<Vec<StreamsGroupDescription>> {
        let finished = async {
            let mut results: Vec<Option<StreamsGroupDescription>> =
                (0..ids.len()).map(|_| None).collect();
            let mut last: Vec<Option<Arc<Error>>> = (0..ids.len()).map(|_| None).collect();
            let mut control_bytes = 0usize;
            let mut response_bytes = 0usize;
            for attempt in 0..MAX_ATTEMPTS {
                let (mut bootstrap, versions, siblings) =
                    match self.streams_bootstrap(deadline).await {
                        Ok(opened) => opened,
                        Err(err) if streams_retryable(&err) => {
                            let err = Arc::new(err);
                            for (result, previous) in results.iter().zip(&mut last) {
                                if result.is_none() {
                                    *previous = Some(Arc::clone(&err));
                                }
                            }
                            self.streams_describe_backoff(attempt, deadline, true)
                                .await?;
                            continue;
                        }
                        Err(err) => return Err(err),
                    };
                for (addr, keys) in siblings {
                    if !keys.is_empty() && !self.peer_versions.contains_key(&addr) {
                        let _stored = self.peer_versions.insert(addr, keys);
                    }
                }
                let version = versions
                    .api_version(FIND_COORDINATOR)
                    .and_then(|api| pick_version(api.min_version, api.max_version, 1, 6))
                    .ok_or_else(|| {
                        Error::Unsupported(
                            "broker does not support GROUP FindCoordinator v1-6".into(),
                        )
                    })?;
                let mut routes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
                let mut transport_retry = false;
                for (index, id) in ids.iter().enumerate() {
                    if results.get(index).is_some_and(Option::is_some) {
                        continue;
                    }
                    match Self::streams_coordinator(
                        &mut bootstrap,
                        id,
                        version,
                        deadline,
                        &mut control_bytes,
                    )
                    .await
                    {
                        Ok(address) => {
                            if !routes.contains_key(&address) && routes.len() >= MAX_NODES {
                                return Err(Error::protocol("too many Streams coordinators"));
                            }
                            routes.entry(address).or_default().push(index);
                        }
                        Err(err) if streams_retryable(&err) => {
                            transport_retry |= err.broker_code().is_none();
                            *last
                                .get_mut(index)
                                .ok_or_else(|| Error::protocol("missing Streams retry slot"))? =
                                Some(Arc::new(err));
                        }
                        Err(err @ Error::Broker { .. }) => {
                            *results
                                .get_mut(index)
                                .ok_or_else(|| Error::protocol("missing Streams result slot"))? =
                                Some(StreamsGroupDescription {
                                    group_id: id.clone(),
                                    description: Err(Arc::new(err)),
                                });
                        }
                        Err(err) => return Err(err),
                    }
                }
                let bootstrap_addr = bootstrap.addr().to_string();
                let mut bootstrap = Some(bootstrap);
                for (address, indexes) in routes {
                    let queried = if address == bootstrap_addr {
                        match bootstrap.take() {
                            Some(conn) => {
                                Self::streams_describe_conn(conn, ids, &indexes, options, deadline)
                                    .await
                            }
                            None => {
                                self.streams_describe_node(
                                    &address, ids, &indexes, options, deadline,
                                )
                                .await
                            }
                        }
                    } else {
                        self.streams_describe_node(&address, ids, &indexes, options, deadline)
                            .await
                    };
                    let address_for_retry = address.clone();
                    match queried {
                        Ok(response) => {
                            response_bytes =
                                response_bytes.checked_add(response.0).ok_or_else(|| {
                                    Error::protocol("Streams response bytes overflow")
                                })?;
                            if response_bytes > options.limits.wire_bytes.saturating_mul(8) {
                                return Err(Error::protocol(
                                    "Streams cumulative responses exceed limit",
                                ));
                            }
                            let retained = results
                                .iter()
                                .filter_map(|group| {
                                    group
                                        .as_ref()
                                        .and_then(|group| group.description.as_ref().ok())
                                })
                                .chain(response.1.iter())
                                .collect::<Vec<_>>();
                            streams::validate_described_groups(&retained, options.limits)?;
                            for (index, group) in indexes.iter().copied().zip(response.1) {
                                if error::coordinator_retriable(group.error_code) {
                                    *last.get_mut(index).ok_or_else(|| {
                                        Error::protocol("missing Streams retry slot")
                                    })? = Some(Arc::new(Error::broker(
                                        group.error_code,
                                        "StreamsGroupDescribe",
                                    )));
                                } else {
                                    *results.get_mut(index).ok_or_else(|| {
                                        Error::protocol("missing Streams result slot")
                                    })? = Some(StreamsGroupDescription {
                                        group_id: group.group_id.clone(),
                                        description: Ok(group),
                                    });
                                }
                            }
                        }
                        Err(err) if streams_retryable(&err) => {
                            transport_retry |= err.broker_code().is_none();
                            if err.broker_code().is_none() {
                                let _removed = self.peer_versions.remove(&address_for_retry);
                            }
                            let err = Arc::new(err);
                            for index in indexes {
                                *last.get_mut(index).ok_or_else(|| {
                                    Error::protocol("missing Streams retry slot")
                                })? = Some(Arc::clone(&err));
                            }
                        }
                        Err(err @ Error::Unsupported(_)) => {
                            let err = Arc::new(err);
                            for index in indexes {
                                *results.get_mut(index).ok_or_else(|| {
                                    Error::protocol("missing Streams result slot")
                                })? = Some(StreamsGroupDescription {
                                    group_id: ids
                                        .get(index)
                                        .ok_or_else(|| Error::protocol("missing Streams ID"))?
                                        .clone(),
                                    description: Err(Arc::clone(&err)),
                                });
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
                if results.iter().all(Option::is_some) {
                    break;
                }
                self.streams_describe_backoff(attempt, deadline, transport_retry)
                    .await?;
            }
            results
                .into_iter()
                .zip(last)
                .zip(ids)
                .map(|((result, last), id)| {
                    result
                        .or_else(|| {
                            last.map(|err| StreamsGroupDescription {
                                group_id: id.clone(),
                                description: Err(err),
                            })
                        })
                        .ok_or_else(|| Error::protocol("missing Streams group result"))
                })
                .collect()
        }
        .await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finished
    }

    async fn streams_describe_backoff(
        &self,
        attempt: u32,
        deadline: Deadline,
        transport: bool,
    ) -> Result<()> {
        if attempt + 1 < MAX_ATTEMPTS {
            let cfg: &AdminConfig = &self.cfg;
            let (base, max) = if transport {
                (cfg.reconnect_backoff, cfg.reconnect_backoff_max)
            } else {
                (cfg.retry_backoff, cfg.retry_backoff_max)
            };
            crate::config::sleep_retry_backoff(base, max, attempt, deadline.target().into_std())
                .await;
        }
        deadline.check_expired()
    }

    async fn streams_describe_node(
        &mut self,
        address: &str,
        ids: &[String],
        indexes: &[usize],
        options: &DescribeStreamsGroupsOptions,
        deadline: Deadline,
    ) -> Result<(usize, Vec<streams::DescribedStreamsGroup>)> {
        let (conn, versions) = self.open_streams_describe(address, deadline).await?;
        let keys = versions.api_keys.clone();
        let described = Self::streams_describe_conn(conn, ids, indexes, options, deadline).await?;
        if !keys.is_empty() && !self.peer_versions.contains_key(address) {
            let _stored = self.peer_versions.insert(address.to_string(), keys);
        }
        Ok(described)
    }

    async fn streams_describe_conn(
        mut conn: BrokerConn,
        ids: &[String],
        indexes: &[usize],
        options: &DescribeStreamsGroupsOptions,
        deadline: Deadline,
    ) -> Result<(usize, Vec<streams::DescribedStreamsGroup>)> {
        let subset = indexes
            .iter()
            .map(|&index| {
                ids.get(index)
                    .cloned()
                    .ok_or_else(|| Error::protocol("missing Streams ID"))
            })
            .collect::<Result<Vec<_>>>()?;
        let request = streams::StreamsGroupDescribeRequest {
            group_ids: subset,
            include_authorized_operations: options.include_authorized_operations,
        };
        let encoded = streams::encode_streams_group_describe_request(&request, 0, options.limits)?;
        let raw = conn
            .roundtrip_deadline(
                STREAMS_GROUP_DESCRIBE,
                0,
                |buf| {
                    buf.extend_from_slice(&encoded);
                    Ok(())
                },
                deadline,
            )
            .await?;
        let response = streams::decode_streams_group_describe_response(&raw, 0, options.limits)?;
        let mut groups = HashMap::new();
        for group in response.groups {
            if !request.group_ids.contains(&group.group_id)
                || groups.insert(group.group_id.clone(), group).is_some()
            {
                return Err(Error::protocol(
                    "unexpected or duplicate Streams response group",
                ));
            }
        }
        let ordered = request
            .group_ids
            .into_iter()
            .map(|id| {
                groups
                    .remove(&id)
                    .ok_or_else(|| Error::protocol("missing Streams response group"))
            })
            .collect::<Result<Vec<_>>>()?;
        deadline.check_expired()?;
        Ok((raw.len(), ordered))
    }
}

#[cfg(test)]
mod tests {
    use super::open_streams_describe_socket;
    use crate::net::Deadline;
    use crate::AdminConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::io::AsyncReadExt;

    #[tokio::test(flavor = "current_thread")]
    async fn a_streams_bootstrap_race_lets_the_other_broker_leave() {
        use super::Admin;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::STREAMS_GROUP_DESCRIBE;

        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dummy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first.local_addr().unwrap().to_string();
        let second_addr = second.local_addr().unwrap().to_string();
        let dummy_addr = dummy.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let spawn_peer = |listener: tokio::net::TcpListener,
                          open: Arc<AtomicUsize>,
                          accepted: Arc<AtomicUsize>| {
            tokio::spawn(async move {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let _accepted = accepted.fetch_add(1, Ordering::SeqCst);
                let _started = open.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 8];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open.fetch_sub(1, Ordering::SeqCst);
            })
        };
        let first_peer = spawn_peer(first, Arc::clone(&open), Arc::clone(&accepted));
        let second_peer = spawn_peer(second, Arc::clone(&open), Arc::clone(&accepted));
        let dummy_peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = dummy.accept().await else {
                return;
            };
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        });
        let held =
            crate::net::BrokerConn::connect(&dummy_addr, "streams-hold", Duration::from_secs(2))
                .await
                .unwrap();
        let mut admin = Admin::test_holding(held);
        admin.cfg = AdminConfig::bootstrap([first_addr.clone(), second_addr.clone()]);
        let keys = vec![ApiVersion {
            api_key: STREAMS_GROUP_DESCRIBE,
            min_version: 0,
            max_version: 0,
        }];
        let _stored = admin.peer_versions.insert(first_addr, keys.clone());
        let _stored = admin.peer_versions.insert(second_addr, keys);
        let opened = admin
            .streams_bootstrap(Deadline::from_timeout(Duration::from_secs(2)))
            .await;
        assert!(opened.is_ok(), "the streams bootstrap race did not finish");
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "a bootstrap was not dialed"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            1,
            "a streams bootstrap race left the unused broker blocked in a read"
        );
        drop(opened);
        drop(admin);
        first_peer.abort();
        second_peer.abort();
        dummy_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remembered_sasl_timeout_closes_the_socket() {
        use super::open_streams_describe_known;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{
            SASL_AUTHENTICATE, SASL_HANDSHAKE, STREAMS_GROUP_DESCRIBE,
        };
        use std::time::{Duration, Instant};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut cfg = AdminConfig::bootstrap([addr.clone()]);
        cfg.connect_timeout = Duration::from_millis(200);
        cfg.sasl_plain = Some(("user".into(), "pass".into()));
        let keys = [
            ApiVersion {
                api_key: SASL_HANDSHAKE,
                min_version: 0,
                max_version: 1,
            },
            ApiVersion {
                api_key: SASL_AUTHENTICATE,
                min_version: 0,
                max_version: 2,
            },
            ApiVersion {
                api_key: STREAMS_GROUP_DESCRIBE,
                min_version: 0,
                max_version: 0,
            },
        ];
        let stats = Arc::new(crate::metrics::AdminTracker::default());
        let result = open_streams_describe_known(
            &cfg,
            &stats,
            &addr,
            &keys,
            Deadline::from_std(Instant::now() + Duration::from_millis(200)),
        )
        .await;
        assert!(matches!(result, Err(crate::Error::Timeout)));
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "describe sasl left the socket open"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streams_describe_handshake_timeout_closes_the_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let open_flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 64];
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut cfg = AdminConfig::bootstrap([addr.clone()]);
        cfg.connect_timeout = Duration::from_millis(200);
        let stats = Arc::new(crate::metrics::AdminTracker::default());
        let result = open_streams_describe_socket(
            &cfg,
            &stats,
            &addr,
            Deadline::from_std(Instant::now() + Duration::from_millis(200)),
        )
        .await;
        assert!(
            matches!(result, Err(crate::Error::Timeout)),
            "a silent Streams broker should time out the handshake"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "Streams describe handshake left the socket blocked in a broker read"
        );
        peer.abort();
    }
}

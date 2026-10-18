//! Caller-driven offset operations with explicit topic names or UUIDs.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::admin::AdminConfig;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::protocol::api::{negotiate_api_versions, ApiVersion, ApiVersionsResponse};
use crate::protocol::api_keys::{
    pick_version, FIND_COORDINATOR, METADATA, OFFSET_COMMIT, OFFSET_FETCH,
};
use crate::protocol::group::{
    decode_find_coordinator_response_coordinators, decode_offset_commit_response_data,
    decode_offset_fetch_response_data, encode_find_coordinator_request_keys,
    encode_find_coordinator_request_typed, encode_offset_commit_request_into,
    encode_offset_fetch_request_into, validate_offset_commit_request_data,
    validate_offset_fetch_request_data, validate_offset_fetch_response_data,
    OffsetCommitRequestData, OffsetCommitResponseData, OffsetFetchRequestData,
    OffsetFetchResponseData, OffsetLimits, OffsetTopicIdentity, COORDINATOR_GROUP,
};
use crate::protocol::group::{
    decode_offset_metadata_bindings, encode_offset_metadata_names_into,
    validate_offset_metadata_names,
};
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
/// The first contact with a broker negotiates ApiVersions. A later dial to
/// that same address reuses the learned versions. Each operation still owns
/// at most two sockets, including overlapped coordinator handshakes.
/// OffsetFetch responses are checked in coordinator order before the next
/// fetch is sent. Cancellation drops the sockets. OffsetCommit supplies the
/// caller's group generation or member epoch; this client does not join or
/// heartbeat a group.
pub struct OffsetClient {
    cfg: AdminConfig,
    stats: Option<std::sync::Arc<crate::metrics::AdminTracker>>,
    /// ApiVersions learned by broker address. A redial skips the handshake.
    known_versions: HashMap<String, Vec<ApiVersion>>,
}

type OffsetDial = (String, Result<(BrokerConn, Vec<ApiVersion>, i16)>);

struct OffsetSocketRequest<'a> {
    cfg: &'a AdminConfig,
    stats: Option<&'a std::sync::Arc<crate::metrics::AdminTracker>>,
    api: i16,
    ids: bool,
    member: bool,
    known: &'a [ApiVersion],
}

fn remembered_api_versions(keys: &[ApiVersion]) -> ApiVersionsResponse {
    ApiVersionsResponse {
        error_code: 0,
        api_keys: keys.to_vec(),
        throttle_time_ms: 0,
        supported_features: Vec::new(),
        finalized_features_epoch: None,
        finalized_features: Vec::new(),
        zk_migration_ready: false,
    }
}

async fn open_offset_socket(
    address: &str,
    request: OffsetSocketRequest<'_>,
    deadline: Deadline,
) -> Result<(BrokerConn, ApiVersionsResponse, i16)> {
    let opened = async {
        let mut conn = BrokerConn::connect_tls(
            address,
            &request.cfg.client_id,
            request.cfg.connect_timeout.min(deadline.remaining()?),
            request.cfg.tls.as_ref(),
        )
        .await?;
        if let Some(stats) = request.stats {
            conn.set_stats(std::sync::Arc::clone(stats));
        }
        let versions = if request.known.is_empty() {
            negotiate_api_versions(&mut conn, deadline.remaining()?).await?
        } else {
            remembered_api_versions(request.known)
        };
        let version = OffsetClient::version(&versions, request.api, request.ids, request.member)?;
        sasl::apply_api_keys(&mut conn, &versions.api_keys);
        sasl::authenticate(
            &mut conn,
            request.cfg.sasl_plain.as_ref(),
            request.cfg.sasl_scram.as_ref(),
            request.cfg.sasl_scram_sha512.as_ref(),
            request.cfg.sasl_oauthbearer.as_deref(),
            request.cfg.sasl_oauthbearer_oidc.as_ref(),
            deadline.remaining()?,
        )
        .await?;
        Ok((conn, versions, version))
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

fn retryable(err: &Error) -> bool {
    matches!(err, Error::Io(_) | Error::Timeout)
        || err.broker_code().is_some_and(error::coordinator_retriable)
}

/// Shut an offset coordinator down before the next group is dialed.
///
/// Dropping the connection aborts its idle watch, so the broker stays in its
/// read until that watch is scheduled.
async fn retire_offset_conn(mut conn: BrokerConn) {
    conn.close();
    conn.release_if_closed().await;
}

async fn retire_ready_offsets(ready: &mut HashMap<String, Result<(BrokerConn, i16)>>) {
    let addresses = ready.keys().cloned().collect::<Vec<_>>();
    for address in addresses {
        if let Some(Ok((conn, _))) = ready.remove(&address) {
            retire_offset_conn(conn).await;
        }
    }
}

async fn retire_dialing_offsets(dialing: &mut tokio::task::JoinSet<OffsetDial>) {
    dialing.abort_all();
    while let Some(joined) = dialing.join_next().await {
        if let Ok((_, Ok((conn, _, _)))) = joined {
            retire_offset_conn(conn).await;
        }
    }
    // A cancelled dial drops its socket when the task is polled. The idle
    // watch then has to run before that broker sees the close.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

impl OffsetClient {
    pub(crate) fn set_stats(&mut self, stats: std::sync::Arc<crate::metrics::AdminTracker>) {
        self.stats = Some(stats);
    }
    pub(crate) async fn resolve_names(
        &mut self,
        names: Option<&[String]>,
        api: i16,
        deadline: Deadline,
        limits: OffsetLimits,
    ) -> Result<HashMap<String, [u8; 16]>> {
        // Preflight the borrowed names before any discovery or metadata I/O.
        validate_offset_metadata_names(names, 10, limits)?;
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
                let body = conn
                    .roundtrip_deadline(
                        METADATA,
                        version,
                        |buf| encode_offset_metadata_names_into(buf, names, version, limits),
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
    /// Race bootstrap handshakes and return as soon as the losers have stopped.
    ///
    /// Later offset RPCs await again after this race, which hides a handshake
    /// that was only aborted.
    #[doc(hidden)]
    pub async fn test_race_bootstrap(&mut self) -> Result<()> {
        let deadline = Deadline::from_std(
            Instant::now()
                .checked_add(self.cfg.request_timeout)
                .ok_or(Error::Timeout)?,
        );
        let _opened = self.bootstrap(OFFSET_FETCH, false, false, deadline).await?;
        Ok(())
    }

    /// Cancel handshakes that lost the bootstrap race and wait until they stop.
    ///
    /// Abort only schedules cancellation. On a current-thread runtime a read
    /// stays inside the broker until this caller yields.
    async fn finish_aborted_handshakes(
        mut set: tokio::task::JoinSet<Result<(BrokerConn, ApiVersionsResponse, i16)>>,
    ) {
        set.abort_all();
        let mut extra = 0u32;
        while !set.is_empty() {
            tokio::select! {
                biased;
                joined = set.join_next() => {
                    let Some(joined) = joined else {
                        break;
                    };
                    if let Ok(Ok((conn, _, _))) = joined {
                        retire_offset_conn(conn).await;
                    }
                }
                () = tokio::task::yield_now() => {
                    extra = extra.saturating_add(1);
                    if extra >= 8 {
                        break;
                    }
                }
            }
        }
        // A cancelled offset handshake drops its socket when the task is polled.
        // The idle watch then has to run before that broker sees the close.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    async fn finish_aborted_offset_dials(mut set: tokio::task::JoinSet<OffsetDial>) {
        set.abort_all();
        let mut extra = 0u32;
        while !set.is_empty() {
            tokio::select! {
                biased;
                joined = set.join_next() => {
                    let Some(joined) = joined else {
                        break;
                    };
                    if let Ok((_, Ok((conn, _, _)))) = joined {
                        retire_offset_conn(conn).await;
                    }
                }
                () = tokio::task::yield_now() => {
                    extra = extra.saturating_add(1);
                    if extra >= 8 {
                        break;
                    }
                }
            }
        }
        // A cancelled offset dial drops its socket when the task is polled.
        // The idle watch then has to run before that broker sees the close.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
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
        Ok(Self {
            cfg,
            stats: None,
            known_versions: HashMap::new(),
        })
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
        validate_commit(request).await?;
        deadline.check_expired()?;
        if request.topics.is_empty() {
            return Ok(OffsetCommitResponseData::default());
        }
        self.commit_until(request, options, deadline).await
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
        self.fetch_until(request, options, deadline).await
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
        &mut self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
    ) -> Result<OffsetCommitResponseData> {
        let finished = async {
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
        .await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finished
    }

    async fn fetch_until(
        &mut self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
    ) -> Result<OffsetFetchResponseData> {
        let finished = async {
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
        .await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        finished
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

    fn remember_versions(&mut self, address: &str, keys: &[ApiVersion]) {
        if keys.is_empty() || self.known_versions.contains_key(address) {
            return;
        }
        let _stored = self
            .known_versions
            .insert(address.to_string(), keys.to_vec());
    }

    async fn open(
        &mut self,
        address: &str,
        api: i16,
        ids: bool,
        member: bool,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse, i16)> {
        let known = self
            .known_versions
            .get(address)
            .cloned()
            .unwrap_or_default();
        let opened = open_offset_socket(
            address,
            OffsetSocketRequest {
                cfg: &self.cfg,
                stats: self.stats.as_ref(),
                api,
                ids,
                member,
                known: &known,
            },
            deadline,
        )
        .await?;
        self.remember_versions(address, &opened.1.api_keys);
        Ok(opened)
    }

    async fn bootstrap(
        &mut self,
        api: i16,
        ids: bool,
        member: bool,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse, i16)> {
        if self.cfg.bootstrap.len() <= 1 {
            let mut last = Error::Timeout;
            let addresses = self.cfg.bootstrap.clone();
            for address in &addresses {
                match self.open(address, api, ids, member, deadline).await {
                    Ok(conn) => return Ok(conn),
                    Err(err) if retryable(&err) => last = err,
                    Err(err) => return Err(err),
                }
            }
            return Err(last);
        }
        let mut inflight = tokio::task::JoinSet::new();
        for address in &self.cfg.bootstrap {
            let cfg = self.cfg.clone();
            let stats = self.stats.clone();
            let address = address.clone();
            let known = self
                .known_versions
                .get(&address)
                .cloned()
                .unwrap_or_default();
            let _ = inflight.spawn(async move {
                open_offset_socket(
                    &address,
                    OffsetSocketRequest {
                        cfg: &cfg,
                        stats: stats.as_ref(),
                        api,
                        ids,
                        member,
                        known: &known,
                    },
                    deadline,
                )
                .await
            });
        }
        let mut last = Error::protocol("all bootstrap servers failed");
        let mut finished = Vec::new();
        while !inflight.is_empty() {
            let Some(joined) = inflight.join_next().await else {
                break;
            };
            match joined {
                Ok(Ok(opened)) => {
                    finished.push(opened);
                    // Other bootstraps that finish with this one keep their
                    // versions, so a coordinator on one of them is not negotiated again.
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
                                    Some(Ok(Ok(opened))) => finished.push(opened),
                                    Some(Ok(Err(error))) => last = error,
                                    _ => {}
                                }
                            }
                            () = &mut gather_done => break,
                        }
                    }
                    break;
                }
                Ok(Err(error)) => last = error,
                Err(error) => last = Error::protocol(format!("offset bootstrap: {error}")),
            }
        }
        Self::finish_aborted_handshakes(inflight).await;
        for spare in &finished {
            self.remember_versions(spare.0.addr(), &spare.1.api_keys);
        }
        let mut rest = finished.into_iter();
        let Some(kept) = rest.next() else {
            return Err(last);
        };
        for (conn, _, _) in rest {
            retire_offset_conn(conn).await;
        }
        Ok(kept)
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
        crate::cluster::dial_address(&coordinator.host, coordinator.port)
            .ok_or_else(|| Error::protocol("invalid offset coordinator address"))
    }

    async fn route_groups(
        conn: &mut BrokerConn,
        versions: &ApiVersionsResponse,
        groups: &[crate::protocol::group::OffsetFetchGroupData],
        deadline: Deadline,
        control_bytes: &mut usize,
    ) -> Result<BTreeMap<String, Vec<usize>>> {
        let version = versions
            .api_version(FIND_COORDINATOR)
            .and_then(|range| pick_version(range.min_version, range.max_version, 1, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support FindCoordinator v1-6".into())
            })?;
        if groups.len() <= 1 || version < 4 {
            let mut routes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
            for (index, group) in groups.iter().enumerate() {
                let address =
                    Self::coordinator(conn, versions, &group.group_id, deadline, control_bytes)
                        .await?;
                routes.entry(address).or_default().push(index);
            }
            return Ok(routes);
        }
        let keys: Vec<&str> = groups.iter().map(|group| group.group_id.as_str()).collect();
        let body = conn
            .roundtrip_deadline(
                FIND_COORDINATOR,
                version,
                |buf| encode_find_coordinator_request_keys(buf, version, &keys, COORDINATOR_GROUP),
                deadline,
            )
            .await?;
        charge_bytes(control_bytes, body.len(), MAX_CONTROL_BYTES)?;
        if body.len() > 64 * 1024 {
            return Err(Error::protocol(
                "offset coordinator response exceeds bounds",
            ));
        }
        let mut raw = body.as_ref();
        let (coordinators, _) = decode_find_coordinator_response_coordinators(&mut raw, version)?;
        if !raw.is_empty() || coordinators.len() != groups.len() {
            return Err(Error::protocol("invalid offset coordinator response"));
        }
        let mut routes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, group) in groups.iter().enumerate() {
            let coordinator = coordinators
                .iter()
                .find(|item| item.key == group.group_id)
                .ok_or_else(|| Error::protocol("offset coordinator key differs"))?;
            if coordinator.error_code != 0 {
                return Err(Error::broker(coordinator.error_code, "FindCoordinator"));
            }
            let address = crate::cluster::dial_address(&coordinator.host, coordinator.port)
                .ok_or_else(|| Error::protocol("invalid offset coordinator address"))?;
            routes.entry(address).or_default().push(index);
        }
        Ok(routes)
    }

    async fn commit_once(
        &mut self,
        request: &OffsetCommitRequestData,
        options: &OffsetOptions,
        deadline: Deadline,
        control_bytes: &mut usize,
        response_bytes: &mut usize,
    ) -> Result<OffsetCommitResponseData> {
        let (mut bootstrap, versions, bootstrap_version) = self
            .bootstrap(OFFSET_COMMIT, options.topic_ids, false, deadline)
            .await?;
        let address = match Self::coordinator(
            &mut bootstrap,
            &versions,
            &request.group_id,
            deadline,
            control_bytes,
        )
        .await
        {
            Ok(address) => address,
            Err(error) => {
                retire_offset_conn(bootstrap).await;
                return Err(error);
            }
        };
        let (mut conn, version) = if address == bootstrap.addr() {
            (bootstrap, bootstrap_version)
        } else {
            let opened = self
                .open(&address, OFFSET_COMMIT, options.topic_ids, false, deadline)
                .await;
            retire_offset_conn(bootstrap).await;
            match opened {
                Ok((conn, _, version)) => (conn, version),
                Err(error) => return Err(error),
            }
        };
        if let Err(error) = deadline.check_expired() {
            retire_offset_conn(conn).await;
            return Err(error);
        }
        let body = match conn
            .roundtrip_deadline(
                OFFSET_COMMIT,
                version,
                |buf| encode_offset_commit_request_into(buf, request, version, options.limits),
                deadline,
            )
            .await
        {
            Ok(body) => body,
            Err(error) => {
                retire_offset_conn(conn).await;
                return Err(error);
            }
        };
        let limit = match response_limit(options.limits) {
            Ok(limit) => limit,
            Err(error) => {
                retire_offset_conn(conn).await;
                return Err(error);
            }
        };
        if let Err(error) = charge_bytes(response_bytes, body.len(), limit) {
            retire_offset_conn(conn).await;
            return Err(error);
        }
        let response = match decode_offset_commit_response_data(&body, version, options.limits) {
            Ok(response) => response,
            Err(error) => {
                retire_offset_conn(conn).await;
                return Err(error);
            }
        };
        if let Err(error) = validate_commit_response(request, &response).await {
            retire_offset_conn(conn).await;
            return Err(error);
        }
        if let Some(code) = response
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.error_code)
            .find(|code| error::coordinator_retriable(*code))
        {
            retire_offset_conn(conn).await;
            return Err(Error::broker(code, "OffsetCommit"));
        }
        if let Err(error) = deadline.check_expired() {
            retire_offset_conn(conn).await;
            return Err(error);
        }
        retire_offset_conn(conn).await;
        Ok(response)
    }

    async fn fetch_once(
        &mut self,
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
        let routes = match Self::route_groups(
            &mut bootstrap,
            &versions,
            &request.groups,
            deadline,
            control_bytes,
        )
        .await
        {
            Ok(routes) => routes,
            Err(error) => {
                retire_offset_conn(bootstrap).await;
                return Err(error);
            }
        };
        let order: Vec<(String, Vec<usize>)> = routes.into_iter().collect();
        let bootstrap_addr = bootstrap.addr().to_string();
        let bootstrap_version =
            match Self::version(&versions, OFFSET_FETCH, options.topic_ids, member) {
                Ok(version) => version,
                Err(error) => {
                    retire_offset_conn(bootstrap).await;
                    return Err(error);
                }
            };
        let mut next_dial = 0usize;
        let mut dialing = tokio::task::JoinSet::<OffsetDial>::new();
        let mut ready: HashMap<String, Result<(BrokerConn, i16)>> = HashMap::new();
        if order.iter().any(|(address, _)| address == &bootstrap_addr) {
            let _prev = ready.insert(bootstrap_addr, Ok((bootstrap, bootstrap_version)));
        } else {
            retire_offset_conn(bootstrap).await;
        }
        let mut in_use = 0usize;
        let cfg = self.cfg.clone();
        let stats = self.stats.clone();
        let topic_ids = options.topic_ids;
        let known_versions = self.known_versions.clone();
        let start_dial = |dialing: &mut tokio::task::JoinSet<OffsetDial>, index: usize| {
            let Some((address, _)) = order.get(index) else {
                return;
            };
            let address = address.clone();
            let cfg = cfg.clone();
            let stats = stats.clone();
            let known = known_versions.get(&address).cloned().unwrap_or_default();
            let _ = dialing.spawn(async move {
                let result = open_offset_socket(
                    &address,
                    OffsetSocketRequest {
                        cfg: &cfg,
                        stats: stats.as_ref(),
                        api: OFFSET_FETCH,
                        ids: topic_ids,
                        member,
                        known: &known,
                    },
                    deadline,
                )
                .await
                .map(|(conn, versions, version)| (conn, versions.api_keys, version));
                (address, result)
            });
        };
        while next_dial < order.len() && dialing.len() + ready.len() + in_use < 2 {
            if order
                .get(next_dial)
                .is_some_and(|(address, _)| ready.contains_key(address))
            {
                next_dial += 1;
                continue;
            }
            start_dial(&mut dialing, next_dial);
            next_dial += 1;
        }
        // Handshake errors, decode errors, and timeouts leave through this
        // exit, after coordinator dials that already started have stopped.
        let outcome = async {
            let mut slots = vec![None; request.groups.len()];
            let mut throttle_time_ms = 0;
            for pos in 0..order.len() {
                let Some((address, indexes)) = order.get(pos) else {
                    break;
                };
                let address = address.clone();
                let indexes = indexes.clone();
                while !ready.contains_key(&address) {
                    let Some(joined) = dialing.join_next().await else {
                        retire_ready_offsets(&mut ready).await;
                        return Err(Error::protocol("missing offset coordinator socket"));
                    };
                    let Ok((dialed, result)) = joined else {
                        retire_ready_offsets(&mut ready).await;
                        return Err(Error::protocol("offset coordinator handshake failed"));
                    };
                    let result = match result {
                        Ok((conn, keys, version)) => {
                            self.remember_versions(&dialed, &keys);
                            Ok((conn, version))
                        }
                        Err(error) => Err(error),
                    };
                    let _prev = ready.insert(dialed, result);
                }
                let removed = match ready.remove(&address) {
                    Some(result) => result,
                    None => {
                        retire_ready_offsets(&mut ready).await;
                        return Err(Error::protocol("missing offset coordinator socket"));
                    }
                };
                let (mut conn, version) = match removed {
                    Ok(conn) => conn,
                    Err(error) => {
                        retire_ready_offsets(&mut ready).await;
                        return Err(error);
                    }
                };
                in_use = 1;
                while next_dial < order.len() && dialing.len() + ready.len() + in_use < 2 {
                    if order
                        .get(next_dial)
                        .is_some_and(|(address, _)| ready.contains_key(address))
                    {
                        next_dial += 1;
                        continue;
                    }
                    start_dial(&mut dialing, next_dial);
                    next_dial += 1;
                }
                let fetched = async {
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
                    deadline.check_expired()?;
                    let body = match conn
                        .roundtrip_deadline(
                            OFFSET_FETCH,
                            version,
                            |buf| {
                                encode_offset_fetch_request_into(
                                    buf,
                                    &batched,
                                    version,
                                    options.limits,
                                )
                            },
                            deadline,
                        )
                        .await
                    {
                        Ok(body) => body,
                        Err(error) => return Err(error),
                    };
                    charge_bytes(response_bytes, body.len(), response_limit(options.limits)?)?;
                    let response =
                        decode_offset_fetch_response_data(&body, version, options.limits)?;
                    validate_fetch_response(&batched, &response).await?;
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
                            .ok_or_else(|| Error::protocol("missing offset result slot"))? =
                            Some(group);
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
                            .ok_or_else(|| Error::protocol("missing retained offset slot"))? =
                            Some(group);
                    }
                    deadline.check_expired()?;
                    Ok(())
                }
                .await;
                retire_offset_conn(conn).await;
                if let Err(error) = fetched {
                    retire_ready_offsets(&mut ready).await;
                    retire_dialing_offsets(&mut dialing).await;
                    return Err(error);
                }
                in_use = 0;
                while next_dial < order.len() && dialing.len() + ready.len() + in_use < 2 {
                    if order
                        .get(next_dial)
                        .is_some_and(|(address, _)| ready.contains_key(address))
                    {
                        next_dial += 1;
                        continue;
                    }
                    start_dial(&mut dialing, next_dial);
                    next_dial += 1;
                }
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
        .await;
        Self::finish_aborted_offset_dials(dialing).await;
        outcome
    }

    /// One offset-fetch attempt, without the retry loop.
    ///
    /// Retries await again after this attempt, which hides a coordinator
    /// handshake that was only aborted.
    #[doc(hidden)]
    pub async fn test_fetch_once(
        &mut self,
        request: &OffsetFetchRequestData,
        options: &OffsetOptions,
    ) -> Result<OffsetFetchResponseData> {
        let deadline = self.deadline(options)?;
        let version = if options.topic_ids { 10 } else { 9 };
        validate_offset_fetch_request_data(request, version, options.limits)?;
        validate_fetch(request)?;
        let mut control_bytes = 0;
        let mut response_bytes = 0;
        self.fetch_once(
            request,
            options,
            deadline,
            &mut control_bytes,
            &mut response_bytes,
        )
        .await
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
    // Topic names stay on the commits. The slot list only remembers which
    // commit owns each name.
    let mut seen: Vec<(usize, HashSet<i32>)> = Vec::new();
    for (tp, md) in offsets {
        if out.len() >= limits.array_elements {
            return Err(Error::protocol("too many committed partitions"));
        }
        let tp = tp.into();
        let md = md.into();
        if tp.partition < 0 || md.offset < 0 {
            return Err(Error::protocol(
                "committed partition and offset must not be negative",
            ));
        }
        crate::protocol::group::Topic::validate(&tp.topic)?;
        out.push((tp, md));
        let index = out.len().saturating_sub(1);
        if !claim_commit_topic(&mut seen, &out, index) {
            return Err(Error::protocol("duplicate committed partition"));
        }
        let Some((tp, md)) = out.get(index) else {
            return Err(Error::protocol("duplicate committed partition"));
        };
        if tp.topic.len() > limits.string_bytes || md.metadata.len() > limits.string_bytes {
            return Err(Error::protocol(
                "offset topic/metadata string exceeds limit",
            ));
        }
        charge_bytes(&mut strings, tp.topic.len(), limits.total_string_bytes)?;
        charge_bytes(&mut strings, md.metadata.len(), limits.total_string_bytes)?;
    }
    Ok(out)
}

pub(crate) fn collect_bounded_partitions(
    partitions: impl IntoIterator<Item = impl Into<crate::TopicPartition>>,
) -> Result<Vec<crate::TopicPartition>> {
    let limits = OffsetLimits::default();
    let mut out = Vec::new();
    let mut strings = 0;
    let mut seen: Vec<(usize, HashSet<i32>)> = Vec::new();
    for partition in partitions {
        if out.len() >= limits.array_elements {
            return Err(Error::protocol("too many offset partitions"));
        }
        let partition = partition.into();
        if partition.partition < 0 {
            return Err(Error::protocol(
                "offset partition index must not be negative",
            ));
        }
        crate::protocol::group::Topic::validate(&partition.topic)?;
        out.push(partition);
        let index = out.len().saturating_sub(1);
        if !claim_listed_topic(&mut seen, &out, index) {
            return Err(Error::protocol("duplicate offset partition"));
        }
        let Some(partition) = out.get(index) else {
            return Err(Error::protocol("duplicate offset partition"));
        };
        if partition.topic.len() > limits.string_bytes {
            return Err(Error::protocol("offset topic string exceeds limit"));
        }
        charge_bytes(
            &mut strings,
            partition.topic.len(),
            limits.total_string_bytes,
        )?;
    }
    Ok(out)
}

/// Reject a repeated partition. `index` names the commit that already stores the topic.
fn claim_commit_topic(
    seen: &mut Vec<(usize, HashSet<i32>)>,
    commits: &[(crate::TopicPartition, crate::OffsetAndMetadata)],
    index: usize,
) -> bool {
    let Some(partition) = commits.get(index).map(|(tp, _)| tp.partition) else {
        return false;
    };
    claim_named(seen, index, partition, |slot| {
        commits.get(slot).map(|(tp, _)| tp.topic.as_str())
    })
}

/// Reject a repeated partition. `index` names the partition that already stores the topic.
fn claim_listed_topic(
    seen: &mut Vec<(usize, HashSet<i32>)>,
    partitions: &[crate::TopicPartition],
    index: usize,
) -> bool {
    let Some(partition) = partitions.get(index).map(|tp| tp.partition) else {
        return false;
    };
    claim_named(seen, index, partition, |slot| {
        partitions.get(slot).map(|tp| tp.topic.as_str())
    })
}

fn claim_named<'a>(
    seen: &mut Vec<(usize, HashSet<i32>)>,
    index: usize,
    partition: i32,
    name_at: impl Fn(usize) -> Option<&'a str>,
) -> bool {
    let found = seen
        .iter()
        .position(|(slot, _)| name_at(*slot) == name_at(index) && name_at(index).is_some());
    if let Some(found) = found {
        if let Some((_, parts)) = seen.get_mut(found) {
            return parts.insert(partition);
        }
    }
    let mut parts = HashSet::new();
    let _ = parts.insert(partition);
    seen.push((index, parts));
    true
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

async fn validate_commit(request: &OffsetCommitRequestData) -> Result<()> {
    let mut identities = HashSet::new();
    let mut walked = 0usize;
    for topic in &request.topics {
        if !identities.insert(&topic.identity) {
            return Err(Error::protocol("duplicate offset commit topic"));
        }
        walked = walked.saturating_add(1);
        if walked == 8 * 1024 {
            walked = 0;
            tokio::task::yield_now().await;
        }
        let mut partitions = HashSet::new();
        for partition in &topic.partitions {
            if !partitions.insert(partition.partition_index) {
                return Err(Error::protocol("duplicate offset commit partition"));
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
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

async fn validate_commit_response(
    request: &OffsetCommitRequestData,
    response: &OffsetCommitResponseData,
) -> Result<()> {
    let mut requested = HashSet::new();
    let mut wanted = HashSet::new();
    let mut walked = 0usize;
    for topic in &request.topics {
        let _ = requested.insert(&topic.identity);
        for partition in &topic.partitions {
            let _ = wanted.insert((&topic.identity, partition.partition_index));
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    let mut seen = HashSet::new();
    let mut topics = HashSet::new();
    walked = 0;
    for topic in &response.topics {
        if !topics.insert(&topic.identity) || !requested.contains(&topic.identity) {
            return Err(Error::protocol("unexpected or duplicate committed topic"));
        }
        for partition in &topic.partitions {
            let key = (&topic.identity, partition.partition_index);
            if !wanted.contains(&key) || !seen.insert(key) {
                return Err(Error::protocol(
                    "unexpected or duplicate committed partition",
                ));
            }
            walked = walked.saturating_add(1);
            if walked == 8 * 1024 {
                walked = 0;
                tokio::task::yield_now().await;
            }
        }
    }
    if wanted != seen || topics.len() != request.topics.len() {
        return Err(Error::protocol("incomplete offset commit response"));
    }
    Ok(())
}

async fn validate_fetch_response(
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
            let mut expected_indexes = HashSet::new();
            if let Some(expected) = expected {
                let mut walked = 0usize;
                for index in &expected.partition_indexes {
                    let _ = expected_indexes.insert(*index);
                    walked = walked.saturating_add(1);
                    if walked == 8 * 1024 {
                        walked = 0;
                        tokio::task::yield_now().await;
                    }
                }
            }
            let mut partitions = HashSet::new();
            let mut walked = 0usize;
            for partition in &topic.partitions {
                let unexpected =
                    expected.is_some() && !expected_indexes.contains(&partition.partition_index);
                if !partitions.insert(partition.partition_index) || unexpected {
                    return Err(Error::protocol("unexpected or duplicate fetched partition"));
                }
                walked = walked.saturating_add(1);
                if walked == 8 * 1024 {
                    walked = 0;
                    tokio::task::yield_now().await;
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

#[cfg(test)]
mod tests {
    use super::{collect_bounded_commits, collect_bounded_partitions};
    use crate::consumer::{OffsetAndMetadata, TopicPartition};
    use crate::protocol::group::{
        OffsetCommitPartitionData, OffsetCommitPartitionResponseData, OffsetCommitRequestData,
        OffsetCommitResponseData, OffsetCommitTopicData, OffsetCommitTopicResponseData,
        OffsetTopicIdentity,
    };

    fn commit_pair(count: i32) -> (OffsetCommitRequestData, OffsetCommitResponseData) {
        let partitions: Vec<OffsetCommitPartitionData> = (0..count)
            .map(|index| OffsetCommitPartitionData {
                partition_index: index,
                committed_offset: 0,
                committed_leader_epoch: -1,
                committed_metadata: None,
            })
            .collect();
        let outcomes: Vec<OffsetCommitPartitionResponseData> = (0..count)
            .map(|index| OffsetCommitPartitionResponseData {
                partition_index: index,
                error_code: 0,
            })
            .collect();
        let request = OffsetCommitRequestData {
            group_id: "g".into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: vec![OffsetCommitTopicData {
                identity: OffsetTopicIdentity::Name("orders".into()),
                partitions,
            }],
        };
        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponseData {
                identity: OffsetTopicIdentity::Name("orders".into()),
                partitions: outcomes,
            }],
        };
        (request, response)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn validating_many_commits_does_not_hold_the_caller() {
        // Matching each response topic against the request list is the same scan
        // that held assignment grouping for 1.2s. Checking 1_000_000 partitions
        // without a pause held the caller 2.4s.
        let (request, response) = commit_pair(1_000_000);
        let started = std::time::Instant::now();
        tokio::select! {
            result = super::validate_commit_response(&request, &response) => {
                result.unwrap_or_else(|error| panic!("{error}"));
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "validating offset commits held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn validating_a_commit_keeps_the_requested_partitions() {
        let (request, response) = commit_pair(2);
        super::validate_commit(&request)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        super::validate_commit_response(&request, &response)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let mut extra = response;
        extra.topics.first_mut().expect("topic").partitions.push(
            OffsetCommitPartitionResponseData {
                partition_index: 9,
                error_code: 0,
            },
        );
        let error = super::validate_commit_response(&request, &extra)
            .await
            .expect_err("extra partition");
        assert!(
            error
                .to_string()
                .contains("unexpected or duplicate committed partition"),
            "{error}"
        );
    }

    fn offset_fetch_pair(
        count: i32,
    ) -> (
        crate::protocol::group::OffsetFetchRequestData,
        crate::protocol::group::OffsetFetchResponseData,
    ) {
        use crate::protocol::group::{
            OffsetFetchGroupData, OffsetFetchGroupResponseData, OffsetFetchPartitionData,
            OffsetFetchRequestData, OffsetFetchResponseData, OffsetFetchTopicData,
            OffsetFetchTopicResponseData, OffsetTopicIdentity,
        };
        let indexes: Vec<i32> = (0..count).collect();
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchGroupData {
                group_id: "g".into(),
                member_id: None,
                member_epoch: -1,
                topics: Some(vec![OffsetFetchTopicData {
                    identity: OffsetTopicIdentity::Name("orders".into()),
                    partition_indexes: indexes.clone(),
                }]),
            }],
            require_stable: false,
        };
        let response = OffsetFetchResponseData {
            throttle_time_ms: 0,
            groups: vec![OffsetFetchGroupResponseData {
                group_id: "g".into(),
                topics: vec![OffsetFetchTopicResponseData {
                    identity: OffsetTopicIdentity::Name("orders".into()),
                    partitions: indexes
                        .into_iter()
                        .map(|index| OffsetFetchPartitionData {
                            partition_index: index,
                            committed_offset: 1,
                            committed_leader_epoch: -1,
                            metadata: None,
                            error_code: 0,
                        })
                        .collect(),
                }],
                error_code: 0,
            }],
        };
        (request, response)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn validating_many_fetched_offsets_does_not_hold_the_caller() {
        // 20_000 partitions: scanning the request list for each one held the
        // caller 758ms. A longer indexed walk still has to pause.
        use super::validate_fetch_response;
        let (request, response) = offset_fetch_pair(500_000);
        let started = std::time::Instant::now();
        tokio::select! {
            _ = validate_fetch_response(&request, &response) => {}
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(80),
            "validating fetched offsets held the caller for {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn validating_a_small_offset_fetch_accepts_the_request() {
        use super::validate_fetch_response;
        let (request, response) = offset_fetch_pair(3);
        validate_fetch_response(&request, &response).await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finishing_an_offset_fetch_lets_the_broker_leave() {
        use super::retire_offset_conn;
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "offset-done", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        retire_offset_conn(conn).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "finishing an offset fetch left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_offset_dial_lets_the_bootstrap_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::{
            encode_find_coordinator_response_coordinators, CoordinatorResult, OffsetFetchGroupData,
            OffsetFetchRequestData,
        };
        use crate::AdminConfig;
        use bytes::BytesMut;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut body = BytesMut::new();
                        let encoded = encode_find_coordinator_response_coordinators(
                            &mut body,
                            4,
                            &[
                                CoordinatorResult {
                                    key: "a".to_string(),
                                    node_id: 1,
                                    host: "127.0.0.1".to_string(),
                                    port: 1,
                                    error_code: 0,
                                    error_message: None,
                                },
                                CoordinatorResult {
                                    key: "group".to_string(),
                                    node_id: 2,
                                    host: "127.0.0.1".to_string(),
                                    port: i32::from(addr.port()),
                                    error_code: 0,
                                    error_message: None,
                                },
                            ],
                        );
                        if encoded.is_ok() {
                            let mut response = Vec::new();
                            let len = i32::try_from(5 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.push(0);
                            response.extend_from_slice(&body);
                            let _wrote = socket.write_all(&response).await;
                        }
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut client = OffsetClient::new(AdminConfig::bootstrap([addr.to_string()])).unwrap();
        let _stored = client.known_versions.insert(
            addr.to_string(),
            vec![
                ApiVersion {
                    api_key: FIND_COORDINATOR,
                    min_version: 4,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: OFFSET_FETCH,
                    min_version: 8,
                    max_version: 8,
                },
            ],
        );
        let request = OffsetFetchRequestData {
            groups: vec![
                OffsetFetchGroupData {
                    group_id: "a".to_string(),
                    ..OffsetFetchGroupData::default()
                },
                OffsetFetchGroupData {
                    group_id: "group".to_string(),
                    ..OffsetFetchGroupData::default()
                },
            ],
            require_stable: false,
        };
        let mut control = 0usize;
        let mut response_bytes = 0usize;
        let _fetched = client
            .fetch_once(
                &request,
                &super::OffsetOptions::default(),
                Deadline::from_timeout(Duration::from_secs(2)),
                &mut control,
                &mut response_bytes,
            )
            .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a failed offset dial left the bootstrap broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_offset_fetch_lets_the_other_coordinator_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::{
            encode_find_coordinator_response_coordinators, CoordinatorResult, OffsetFetchGroupData,
            OffsetFetchRequestData,
        };
        use crate::AdminConfig;
        use bytes::BytesMut;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let bootstrap_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let left_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let right_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_addr = bootstrap_listener.local_addr().unwrap();
        let left_addr = left_listener.local_addr().unwrap();
        let right_addr = right_listener.local_addr().unwrap();
        let (slow_listener, fast_listener, slow_addr, fast_addr) =
            if left_addr.to_string() <= right_addr.to_string() {
                (left_listener, right_listener, left_addr, right_addr)
            } else {
                (right_listener, left_listener, right_addr, left_addr)
            };
        let open = Arc::new(AtomicUsize::new(0));
        let bootstrap_flag = Arc::clone(&open);
        let bootstrap = tokio::spawn(async move {
            let Ok((mut socket, _)) = bootstrap_listener.accept().await else {
                return;
            };
            let _started = bootstrap_flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut body = BytesMut::new();
                        let encoded = encode_find_coordinator_response_coordinators(
                            &mut body,
                            4,
                            &[
                                CoordinatorResult {
                                    key: "a".to_string(),
                                    node_id: 1,
                                    host: "127.0.0.1".to_string(),
                                    port: i32::from(slow_addr.port()),
                                    error_code: 0,
                                    error_message: None,
                                },
                                CoordinatorResult {
                                    key: "b".to_string(),
                                    node_id: 2,
                                    host: "127.0.0.1".to_string(),
                                    port: i32::from(fast_addr.port()),
                                    error_code: 0,
                                    error_message: None,
                                },
                            ],
                        );
                        if encoded.is_ok() {
                            let mut response = Vec::new();
                            let len = i32::try_from(5 + body.len()).unwrap_or(0);
                            response.extend_from_slice(&len.to_be_bytes());
                            response.extend_from_slice(&correlation.to_be_bytes());
                            response.push(0);
                            response.extend_from_slice(&body);
                            let _wrote = socket.write_all(&response).await;
                        }
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = bootstrap_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let slow_flag = Arc::clone(&open);
        let slow = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let Ok((mut socket, _)) = slow_listener.accept().await else {
                return;
            };
            let _started = slow_flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::new();
                        response.extend_from_slice(&5i32.to_be_bytes());
                        response.extend_from_slice(&correlation.to_be_bytes());
                        response.push(0);
                        let _wrote = socket.write_all(&response).await;
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = slow_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let fast_flag = Arc::clone(&open);
        let fast = tokio::spawn(async move {
            let Ok((mut socket, _)) = fast_listener.accept().await else {
                return;
            };
            let _started = fast_flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = fast_flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut client =
            OffsetClient::new(AdminConfig::bootstrap([bootstrap_addr.to_string()])).unwrap();
        let lookup = vec![
            ApiVersion {
                api_key: FIND_COORDINATOR,
                min_version: 4,
                max_version: 4,
            },
            ApiVersion {
                api_key: OFFSET_FETCH,
                min_version: 8,
                max_version: 8,
            },
        ];
        let fetch = vec![ApiVersion {
            api_key: OFFSET_FETCH,
            min_version: 8,
            max_version: 8,
        }];
        let _stored = client
            .known_versions
            .insert(bootstrap_addr.to_string(), lookup);
        let _stored = client
            .known_versions
            .insert(slow_addr.to_string(), fetch.clone());
        let _stored = client.known_versions.insert(fast_addr.to_string(), fetch);
        let request = OffsetFetchRequestData {
            groups: vec![
                OffsetFetchGroupData {
                    group_id: "a".to_string(),
                    ..OffsetFetchGroupData::default()
                },
                OffsetFetchGroupData {
                    group_id: "b".to_string(),
                    ..OffsetFetchGroupData::default()
                },
            ],
            require_stable: false,
        };
        let mut control = 0usize;
        let mut response_bytes = 0usize;
        let _fetched = client
            .fetch_once(
                &request,
                &super::OffsetOptions::default(),
                Deadline::from_timeout(Duration::from_secs(2)),
                &mut control,
                &mut response_bytes,
            )
            .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a failed offset fetch left another coordinator blocked in a read"
        );
        bootstrap.abort();
        slow.abort();
        fast.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_bad_offset_fetch_lets_the_broker_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::encode_find_coordinator_response;
        use crate::protocol::group::{OffsetFetchGroupData, OffsetFetchRequestData};
        use crate::AdminConfig;
        use bytes::BytesMut;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut find_body = BytesMut::new();
            encode_find_coordinator_response(
                &mut find_body,
                1,
                1,
                "127.0.0.1",
                i32::from(addr.port()),
                "group",
            )
            .unwrap();
            for flexible in [false, true] {
                let mut size_buf = [0u8; 4];
                if socket.read_exact(&mut size_buf).await.is_err() {
                    return;
                }
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_err() {
                    return;
                }
                let Some(bytes) = request.get(4..8) else {
                    return;
                };
                let correlation = i32::from_be_bytes([
                    bytes.first().copied().unwrap_or(0),
                    bytes.get(1).copied().unwrap_or(0),
                    bytes.get(2).copied().unwrap_or(0),
                    bytes.get(3).copied().unwrap_or(0),
                ]);
                let mut response = Vec::new();
                if flexible {
                    response.extend_from_slice(&5i32.to_be_bytes());
                    response.extend_from_slice(&correlation.to_be_bytes());
                    response.push(0);
                } else {
                    let len = i32::try_from(4 + find_body.len()).unwrap_or(0);
                    response.extend_from_slice(&len.to_be_bytes());
                    response.extend_from_slice(&correlation.to_be_bytes());
                    response.extend_from_slice(&find_body);
                }
                if socket.write_all(&response).await.is_err() {
                    return;
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut client = OffsetClient::new(AdminConfig::bootstrap([addr.to_string()])).unwrap();
        let _stored = client.known_versions.insert(
            addr.to_string(),
            vec![
                ApiVersion {
                    api_key: FIND_COORDINATOR,
                    min_version: 1,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: OFFSET_FETCH,
                    min_version: 8,
                    max_version: 8,
                },
            ],
        );
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchGroupData {
                group_id: "group".to_string(),
                ..OffsetFetchGroupData::default()
            }],
            require_stable: false,
        };
        let mut control = 0usize;
        let mut response_bytes = 0usize;
        let _fetched = client
            .fetch_once(
                &request,
                &super::OffsetOptions::default(),
                Deadline::from_timeout(Duration::from_secs(2)),
                &mut control,
                &mut response_bytes,
            )
            .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a bad offset fetch left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_bad_offset_lookup_lets_the_broker_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_FETCH};
        use crate::protocol::group::{OffsetFetchGroupData, OffsetFetchRequestData};
        use crate::AdminConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_ok() {
                let size = i32::from_be_bytes(size_buf).max(0) as usize;
                let mut request = vec![0u8; size];
                if socket.read_exact(&mut request).await.is_ok() {
                    if let Some(bytes) = request.get(4..8) {
                        let correlation = i32::from_be_bytes([
                            bytes.first().copied().unwrap_or(0),
                            bytes.get(1).copied().unwrap_or(0),
                            bytes.get(2).copied().unwrap_or(0),
                            bytes.get(3).copied().unwrap_or(0),
                        ]);
                        let mut response = Vec::with_capacity(8);
                        response.extend_from_slice(&4i32.to_be_bytes());
                        response.extend_from_slice(&correlation.to_be_bytes());
                        let _wrote = socket.write_all(&response).await;
                    }
                }
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let mut client = OffsetClient::new(AdminConfig::bootstrap([addr.clone()])).unwrap();
        let _stored = client.known_versions.insert(
            addr,
            vec![
                ApiVersion {
                    api_key: FIND_COORDINATOR,
                    min_version: 1,
                    max_version: 1,
                },
                ApiVersion {
                    api_key: OFFSET_FETCH,
                    min_version: 8,
                    max_version: 8,
                },
            ],
        );
        let request = OffsetFetchRequestData {
            groups: vec![OffsetFetchGroupData {
                group_id: "group".to_string(),
                ..OffsetFetchGroupData::default()
            }],
            require_stable: false,
        };
        let mut control = 0usize;
        let mut response_bytes = 0usize;
        let _fetched = client
            .fetch_once(
                &request,
                &super::OffsetOptions::default(),
                Deadline::from_timeout(Duration::from_secs(2)),
                &mut control,
                &mut response_bytes,
            )
            .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "a bad offset lookup left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_offset_bootstrap_race_lets_the_other_broker_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use crate::AdminConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first.local_addr().unwrap().to_string();
        let second_addr = second.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let spawn_peer = |listener: tokio::net::TcpListener,
                          open: Arc<AtomicUsize>,
                          accepted: Arc<AtomicUsize>| {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
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
        let mut client = OffsetClient::new(AdminConfig::bootstrap([
            first_addr.clone(),
            second_addr.clone(),
        ]))
        .unwrap();
        let keys = vec![ApiVersion {
            api_key: OFFSET_FETCH,
            min_version: 8,
            max_version: 8,
        }];
        let _stored = client.known_versions.insert(first_addr, keys.clone());
        let _stored = client.known_versions.insert(second_addr, keys);
        let opened = client
            .bootstrap(
                OFFSET_FETCH,
                false,
                false,
                Deadline::from_timeout(Duration::from_secs(2)),
            )
            .await;
        assert!(opened.is_ok(), "the offset bootstrap race did not finish");
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "a bootstrap was not dialed"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            1,
            "an offset bootstrap race left the unused broker blocked in a read"
        );
        drop(opened);
        first_peer.abort();
        second_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_offset_commit_lets_the_bootstrap_broker_leave() {
        use super::OffsetClient;
        use crate::net::Deadline;
        use crate::protocol::api::ApiVersion;
        use crate::protocol::api_keys::{FIND_COORDINATOR, OFFSET_COMMIT};
        use crate::protocol::group::{
            encode_find_coordinator_response, encode_offset_commit_response_data,
            OffsetCommitRequestData, OffsetCommitResponseData, OffsetLimits,
        };
        use crate::AdminConfig;
        use bytes::BytesMut;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        async fn answer(
            mut socket: TcpStream,
            body: Vec<u8>,
            flexible: bool,
            open: Arc<AtomicUsize>,
        ) {
            let _started = open.fetch_add(1, Ordering::SeqCst);
            let mut size_buf = [0u8; 4];
            if socket.read_exact(&mut size_buf).await.is_err() {
                return;
            }
            let size = i32::from_be_bytes(size_buf).max(0) as usize;
            let mut request = vec![0u8; size];
            if socket.read_exact(&mut request).await.is_err() {
                return;
            }
            let Some(bytes) = request.get(4..8) else {
                return;
            };
            let correlation = i32::from_be_bytes([
                bytes.first().copied().unwrap_or(0),
                bytes.get(1).copied().unwrap_or(0),
                bytes.get(2).copied().unwrap_or(0),
                bytes.get(3).copied().unwrap_or(0),
            ]);
            let mut response = Vec::new();
            let header = if flexible { 5 } else { 4 };
            let len = i32::try_from(header + body.len()).unwrap_or(0);
            response.extend_from_slice(&len.to_be_bytes());
            response.extend_from_slice(&correlation.to_be_bytes());
            if flexible {
                response.push(0);
            }
            response.extend_from_slice(&body);
            if socket.write_all(&response).await.is_err() {
                return;
            }
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = open.fetch_sub(1, Ordering::SeqCst);
        }

        let bootstrap_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coordinator_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap_addr = bootstrap_listener.local_addr().unwrap();
        let coordinator_addr = coordinator_listener.local_addr().unwrap();
        let open = Arc::new(AtomicUsize::new(0));
        let bootstrap_flag = Arc::clone(&open);
        let coordinator_flag = Arc::clone(&open);
        let mut find_body = BytesMut::new();
        encode_find_coordinator_response(
            &mut find_body,
            1,
            2,
            "127.0.0.1",
            i32::from(coordinator_addr.port()),
            "g",
        )
        .unwrap();
        let commit_body = encode_offset_commit_response_data(
            &OffsetCommitResponseData {
                throttle_time_ms: 0,
                topics: Vec::new(),
            },
            8,
            OffsetLimits::default(),
        )
        .unwrap();
        let bootstrap_peer = tokio::spawn(async move {
            let Ok((socket, _)) = bootstrap_listener.accept().await else {
                return;
            };
            answer(socket, find_body.to_vec(), false, bootstrap_flag).await;
        });
        let coordinator_peer = tokio::spawn(async move {
            let Ok((socket, _)) = coordinator_listener.accept().await else {
                return;
            };
            answer(socket, commit_body, true, coordinator_flag).await;
        });
        let mut client =
            OffsetClient::new(AdminConfig::bootstrap([bootstrap_addr.to_string()])).unwrap();
        let keys = vec![
            ApiVersion {
                api_key: FIND_COORDINATOR,
                min_version: 1,
                max_version: 1,
            },
            ApiVersion {
                api_key: OFFSET_COMMIT,
                min_version: 8,
                max_version: 8,
            },
        ];
        let _stored = client
            .known_versions
            .insert(bootstrap_addr.to_string(), keys.clone());
        let _stored = client
            .known_versions
            .insert(coordinator_addr.to_string(), keys);
        let request = OffsetCommitRequestData {
            group_id: "g".to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: Vec::new(),
        };
        let mut control = 0usize;
        let mut response_bytes = 0usize;
        let _committed = client
            .commit_once(
                &request,
                &super::OffsetOptions::default(),
                Deadline::from_timeout(Duration::from_secs(2)),
                &mut control,
                &mut response_bytes,
            )
            .await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "an offset commit left a broker blocked in a read"
        );
        bootstrap_peer.abort();
        coordinator_peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn offset_handshake_timeout_closes_the_socket() {
        use super::{open_offset_socket, OffsetSocketRequest};
        use crate::net::Deadline;
        use crate::protocol::api_keys::OFFSET_FETCH;
        use crate::AdminConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use tokio::io::AsyncReadExt;

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
        let known = [];
        let result = open_offset_socket(
            &addr,
            OffsetSocketRequest {
                cfg: &cfg,
                stats: None,
                api: OFFSET_FETCH,
                ids: false,
                member: false,
                known: &known,
            },
            Deadline::from_std(Instant::now() + Duration::from_millis(200)),
        )
        .await;
        assert!(
            matches!(result, Err(crate::Error::Timeout)),
            "a silent offset broker should time out the handshake"
        );
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "offset handshake left the socket blocked in a broker read"
        );
        peer.abort();
    }

    #[test]
    fn a_negative_commit_is_rejected() {
        assert!(collect_bounded_commits([(
            TopicPartition::new("t", -1),
            OffsetAndMetadata::new(0)
        )])
        .is_err());
        assert!(collect_bounded_commits([(
            TopicPartition::new("t", 0),
            OffsetAndMetadata::new(-1)
        )])
        .is_err());
        assert!(collect_bounded_commits([(
            TopicPartition::new("t", 0),
            OffsetAndMetadata::new(0)
        )])
        .is_ok());
        assert!(collect_bounded_partitions([TopicPartition::new("t", -1)]).is_err());
        assert!(collect_bounded_partitions([TopicPartition::new("t", 0)]).is_ok());
        assert!(collect_bounded_commits([
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(1)),
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(2)),
        ])
        .is_err());
        assert!(collect_bounded_commits([
            (TopicPartition::new("t", 0), OffsetAndMetadata::new(1)),
            (TopicPartition::new("t", 1), OffsetAndMetadata::new(2)),
        ])
        .is_ok());
        assert!(collect_bounded_partitions([
            TopicPartition::new("t", 0),
            TopicPartition::new("t", 0),
        ])
        .is_err());
        let invalid = collect_bounded_commits([(
            TopicPartition::new("bad name", 0),
            OffsetAndMetadata::new(1),
        )])
        .unwrap_err()
        .to_string();
        assert!(invalid.contains("Topic name is invalid"), "{invalid}");
        assert!(collect_bounded_partitions([TopicPartition::new("", 0)]).is_err());
        assert!(collect_bounded_partitions([TopicPartition::new("orders", 0)]).is_ok());
        let orders = "orders-topic-name-is-long".to_string();
        let orders_ptr = orders.as_ptr();
        let payments = "payments-topic-name-is-long".to_string();
        let payments_ptr = payments.as_ptr();
        let collected = collect_bounded_commits([
            (TopicPartition::new(orders, 0), OffsetAndMetadata::new(1)),
            (
                TopicPartition::new("orders-topic-name-is-long", 3),
                OffsetAndMetadata::new(4),
            ),
            (TopicPartition::new(payments, 1), OffsetAndMetadata::new(5)),
        ])
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(collected.len(), 3);
        assert_eq!(
            collected
                .first()
                .map(|(tp, md)| (tp.topic.as_ptr(), tp.partition, md.offset)),
            Some((orders_ptr, 0, 1))
        );
        assert_eq!(
            collected.get(1).map(|(tp, _)| tp.topic.as_str()),
            Some("orders-topic-name-is-long")
        );
        assert_eq!(
            collected
                .get(2)
                .map(|(tp, md)| (tp.topic.as_ptr(), tp.partition, md.offset)),
            Some((payments_ptr, 1, 5))
        );
        let listed = "orders-topic-name-is-long".to_string();
        let listed_ptr = listed.as_ptr();
        let partitions = collect_bounded_partitions([
            TopicPartition::new(listed, 0),
            TopicPartition::new("orders-topic-name-is-long", 2),
        ])
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            partitions
                .first()
                .map(|tp| (tp.topic.as_ptr(), tp.partition)),
            Some((listed_ptr, 0))
        );
        assert_eq!(partitions.get(1).map(|tp| tp.partition), Some(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_a_finished_offset_handshake_lets_the_broker_leave() {
        use super::OffsetClient;
        use crate::net::BrokerConn;
        use crate::protocol::api::ApiVersionsResponse;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "offset-handshake", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let versions = ApiVersionsResponse {
            error_code: 0,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
            supported_features: Vec::new(),
            finalized_features_epoch: None,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
        };
        let mut set =
            tokio::task::JoinSet::<crate::Result<(BrokerConn, ApiVersionsResponse, i16)>>::new();
        let _ = set.spawn(async move { Ok((conn, versions, 8i16)) });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        OffsetClient::finish_aborted_handshakes(set).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "stopping a finished offset handshake left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_aborted_offset_handshake_lets_the_broker_leave() {
        use super::OffsetClient;
        use crate::net::BrokerConn;
        use crate::protocol::api::ApiVersionsResponse;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "abort-offset", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut set =
            tokio::task::JoinSet::<crate::Result<(BrokerConn, ApiVersionsResponse, i16)>>::new();
        let _spawned = set.spawn(async move {
            let _hold = conn;
            std::future::pending().await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        OffsetClient::finish_aborted_handshakes(set).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "an aborted offset handshake left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_aborted_offset_dial_lets_the_broker_leave() {
        use super::{OffsetClient, OffsetDial};
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "abort-offset-dial", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut set = tokio::task::JoinSet::<OffsetDial>::new();
        let _spawned = set.spawn(async move {
            let _hold = conn;
            std::future::pending().await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        OffsetClient::finish_aborted_offset_dials(set).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "an aborted offset dial left the broker blocked in a read"
        );
        peer.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retiring_an_offset_dial_lets_the_broker_leave() {
        use super::{retire_dialing_offsets, OffsetDial};
        use crate::net::BrokerConn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let open = Arc::new(AtomicUsize::new(0));
        let flag = Arc::clone(&open);
        let peer = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let _started = flag.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 8];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            let _left = flag.fetch_sub(1, Ordering::SeqCst);
        });
        let conn = BrokerConn::connect(&addr, "retire-offset-dial", Duration::from_secs(2))
            .await
            .unwrap();
        while open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let mut dialing = tokio::task::JoinSet::<OffsetDial>::new();
        let _spawned = dialing.spawn(async move {
            let _hold = conn;
            std::future::pending().await
        });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        retire_dialing_offsets(&mut dialing).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            0,
            "retiring an offset dial left the broker blocked in a read"
        );
        peer.abort();
    }
}

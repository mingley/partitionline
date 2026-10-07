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
        let unique = deadline
            .run(self.describe_streams_until(&ids, options, deadline))
            .await?;
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

    async fn open_streams_describe(
        &self,
        address: &str,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        let cfg = &self.cfg;
        let mut conn = BrokerConn::connect_tls(
            address,
            &cfg.client_id,
            cfg.connect_timeout.min(deadline.remaining()?),
            cfg.tls.as_ref(),
        )
        .await?;
        conn.set_stats(Arc::clone(&self.stats));
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

    async fn streams_bootstrap(
        &self,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        if self.cfg.bootstrap.len() > 16 {
            return Err(Error::protocol("too many Streams bootstrap servers"));
        }
        let mut last = Error::Timeout;
        for address in &self.cfg.bootstrap {
            match self.open_streams_describe(address, deadline).await {
                Ok(opened) => return Ok(opened),
                Err(err) if streams_retryable(&err) => last = err,
                Err(err) => return Err(err),
            }
        }
        Err(last)
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
        if coordinator.host.is_empty() || !(1..=65535).contains(&coordinator.port) {
            return Err(Error::protocol("invalid Streams coordinator address"));
        }
        Ok(if coordinator.host.contains(':') {
            format!("[{}]:{}", coordinator.host, coordinator.port)
        } else {
            format!("{}:{}", coordinator.host, coordinator.port)
        })
    }

    async fn describe_streams_until(
        &self,
        ids: &[String],
        options: &DescribeStreamsGroupsOptions,
        deadline: Deadline,
    ) -> Result<Vec<StreamsGroupDescription>> {
        let mut results: Vec<Option<StreamsGroupDescription>> =
            (0..ids.len()).map(|_| None).collect();
        let mut last: Vec<Option<Arc<Error>>> = (0..ids.len()).map(|_| None).collect();
        let mut control_bytes = 0usize;
        let mut response_bytes = 0usize;
        for attempt in 0..MAX_ATTEMPTS {
            let (mut bootstrap, versions) = match self.streams_bootstrap(deadline).await {
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
            let version = versions
                .api_version(FIND_COORDINATOR)
                .and_then(|api| pick_version(api.min_version, api.max_version, 1, 6))
                .ok_or_else(|| {
                    Error::Unsupported("broker does not support GROUP FindCoordinator v1-6".into())
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
            drop(bootstrap);
            for (address, indexes) in routes {
                let queried = self
                    .streams_describe_node(&address, ids, &indexes, options, deadline)
                    .await;
                match queried {
                    Ok(response) => {
                        response_bytes = response_bytes
                            .checked_add(response.0)
                            .ok_or_else(|| Error::protocol("Streams response bytes overflow"))?;
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
                        let err = Arc::new(err);
                        for index in indexes {
                            *last
                                .get_mut(index)
                                .ok_or_else(|| Error::protocol("missing Streams retry slot"))? =
                                Some(Arc::clone(&err));
                        }
                    }
                    Err(err @ Error::Unsupported(_)) => {
                        let err = Arc::new(err);
                        for index in indexes {
                            *results
                                .get_mut(index)
                                .ok_or_else(|| Error::protocol("missing Streams result slot"))? =
                                Some(StreamsGroupDescription {
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
        &self,
        address: &str,
        ids: &[String],
        indexes: &[usize],
        options: &DescribeStreamsGroupsOptions,
        deadline: Deadline,
    ) -> Result<(usize, Vec<streams::DescribedStreamsGroup>)> {
        let (mut conn, _) = self.open_streams_describe(address, deadline).await?;
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

//! Caller-driven Streams group heartbeats. The caller runs and reconciles tasks.

use std::time::Duration;

use crate::admin::AdminConfig;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, Deadline};
use crate::protocol::api::{negotiate_api_versions, ApiVersion, ApiVersionsResponse};
use crate::protocol::api_keys::{pick_version, FIND_COORDINATOR, STREAMS_GROUP_HEARTBEAT};
use crate::protocol::group::{
    decode_find_coordinator_response_coordinators, encode_find_coordinator_request_typed,
    COORDINATOR_GROUP,
};
use crate::protocol::sasl;
pub use crate::protocol::streams::{
    Limits as StreamsLimits, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse,
};

const MAX_BOOTSTRAP_SERVERS: usize = 16;

async fn open_streams_heartbeat(
    cfg: &AdminConfig,
    address: &str,
    deadline: Deadline,
) -> Result<(BrokerConn, ApiVersionsResponse)> {
    let mut conn = BrokerConn::connect_tls(
        address,
        &cfg.client_id,
        cfg.connect_timeout.min(deadline.remaining()?),
        cfg.tls.as_ref(),
    )
    .await?;
    let versions = negotiate_api_versions(&mut conn, deadline.remaining()?).await?;
    if versions
        .api_version(STREAMS_GROUP_HEARTBEAT)
        .and_then(|api| pick_version(api.min_version, api.max_version, 0, 0))
        .is_none()
    {
        return Err(Error::Unsupported(
            "broker does not support StreamsGroupHeartbeat v0".into(),
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

/// Versions learned for the bootstrap lookup and the coordinator socket.
#[derive(Clone)]
struct LearnedStreams {
    bootstrap_addr: String,
    bootstrap_keys: Vec<ApiVersion>,
    find_version: i16,
    coord_addr: String,
    coord_keys: Vec<ApiVersion>,
}

async fn open_known_streams(
    cfg: &AdminConfig,
    address: &str,
    keys: &[ApiVersion],
    deadline: Deadline,
) -> Result<BrokerConn> {
    let mut conn = BrokerConn::connect_tls(
        address,
        &cfg.client_id,
        cfg.connect_timeout.min(deadline.remaining()?),
        cfg.tls.as_ref(),
    )
    .await?;
    sasl::apply_api_keys(&mut conn, keys);
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
    Ok(conn)
}

fn coordinator_address(body: &[u8], version: i16, group_id: &str) -> Result<String> {
    if body.len() > MAX_COORDINATOR_BODY || (version >= 4 && body.get(4) != Some(&2)) {
        return Err(Error::protocol(
            "Streams coordinator response exceeds bounds",
        ));
    }
    let mut cursor = body;
    let (mut coords, _) = decode_find_coordinator_response_coordinators(&mut cursor, version)?;
    if !cursor.is_empty() || coords.len() != 1 {
        return Err(Error::protocol("invalid Streams coordinator response"));
    }
    let coord = coords
        .pop()
        .ok_or_else(|| Error::protocol("missing Streams coordinator"))?;
    if version >= 4 && coord.key != group_id {
        return Err(Error::protocol("Streams coordinator key differs"));
    }
    if coord.error_code != 0 {
        return Err(Error::broker(coord.error_code, "FindCoordinator"));
    }
    crate::cluster::dial_address(&coord.host, coord.port)
        .ok_or_else(|| Error::protocol("invalid Streams coordinator address"))
}
const MAX_ATTEMPTS: u32 = 8;
const MAX_COORDINATOR_BODY: usize = 64 * 1024;

/// Connection settings and admission policy for [`StreamsClient`].
#[derive(Clone, Default)]
pub struct StreamsConfig {
    /// Bootstrap addresses, client identity, TLS/SASL and timeout/backoff settings.
    /// No Admin operations are required by the Streams client.
    pub connection: AdminConfig,
    /// Explicitly opt into Streams v0. Its version does not identify whether a
    /// broker release treats it as stable. Default false refuses all network I/O.
    pub allow_unstable: bool,
    /// Limits applied to the complete heartbeat request and response.
    pub limits: StreamsLimits,
}

impl StreamsConfig {
    /// Use the supplied bootstrap servers with the default admission policy.
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            connection: AdminConfig::bootstrap(servers),
            ..Self::default()
        }
    }
}

/// One caller-driven Streams heartbeat at a time.
///
/// There is no heartbeat task, automatic epoch advancement, assignment mutation
/// or Streams execution engine. The caller keeps a stable member ID, sends join
/// epoch0/update epochs/leave epochs-1 or-2, and reconciles the returned tasks.
/// A successful operation retains one coordinator connection for subsequent
/// heartbeats. Each operation owns at most two connections; failure/cancellation
/// drops them. Retries stop after eight attempts or the original deadline.
pub struct StreamsClient {
    cfg: StreamsConfig,
    cached: Option<(String, BrokerConn)>,
    learned: Option<LearnedStreams>,
}

impl StreamsClient {
    /// Validate local settings. Connections are opened by [`Self::heartbeat`].
    pub fn new(mut cfg: StreamsConfig) -> Result<Self> {
        if !cfg.allow_unstable {
            return Err(Error::Unsupported(
                "Streams v0 requires explicit allow_unstable".into(),
            ));
        }
        cfg.limits.validate()?;
        if cfg.connection.bootstrap.len() > MAX_BOOTSTRAP_SERVERS {
            return Err(Error::protocol("too many Streams bootstrap servers"));
        }
        if cfg.connection.client_id.len() > i16::MAX as usize {
            return Err(Error::protocol("Streams client ID exceeds header limit"));
        }
        cfg.connection.bootstrap =
            crate::net::parse_and_validate_addresses(&cfg.connection.bootstrap)?;
        Ok(Self {
            cfg,
            cached: None,
            learned: None,
        })
    }

    /// Send a typed heartbeat using the configured total request timeout.
    ///
    /// Terminal member, authorization and topology errors are returned in the
    /// typed response, including all assignment/status fields. Only coordinator
    /// errors14/15/16 and retriable transport errors trigger automatic retries.
    /// A lost reply or cancellation may have followed an accepted heartbeat;
    /// the client does not infer that the broker left membership unchanged.
    pub async fn heartbeat(
        &mut self,
        request: &StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse> {
        self.heartbeat_timeout(request, self.cfg.connection.request_timeout)
            .await
    }

    /// [`Self::heartbeat`] with one deadline covering validation, encoding,
    /// bootstrap negotiation, authentication, discovery, reconnects and retries.
    pub async fn heartbeat_timeout(
        &mut self,
        request: &StreamsGroupHeartbeatRequest,
        timeout: Duration,
    ) -> Result<StreamsGroupHeartbeatResponse> {
        let now = std::time::Instant::now();
        let target = now.checked_add(timeout).ok_or(Error::Timeout)?;
        let deadline = Deadline::from_std(target);
        deadline.check_expired()?;
        let body = crate::protocol::streams::encode_streams_group_heartbeat_request(
            request,
            0,
            self.cfg.limits,
        )?;
        deadline.check_expired()?;
        let cached_group = self.cached.as_ref().map(|(group, _)| group.clone());
        let cached = self.cached.take().and_then(|(group, conn)| {
            (group == request.group_id
                && !conn.idle_expired(self.cfg.connection.connections_max_idle))
            .then_some(conn)
        });
        if cached_group.as_deref() != Some(request.group_id.as_str()) {
            self.learned = None;
        }
        let learned = self.learned.clone();
        let (response, conn, learned) = self
            .heartbeat_until(request, &body, deadline, cached, learned)
            .await?;
        let group = request.group_id.clone();
        deadline.check_expired()?;
        self.cached = Some((group, conn));
        self.learned = Some(learned);
        Ok(response)
    }

    /// Close the cached socket. This does not send a membership leave or change
    /// the epoch; send an explicit leave heartbeat when that is intended.
    pub fn close(&mut self) {
        self.cached = None;
    }

    async fn heartbeat_until(
        &self,
        request: &StreamsGroupHeartbeatRequest,
        body: &[u8],
        deadline: Deadline,
        mut cached: Option<BrokerConn>,
        mut learned: Option<LearnedStreams>,
    ) -> Result<(StreamsGroupHeartbeatResponse, BrokerConn, LearnedStreams)> {
        let mut last = Error::Timeout;
        for attempt in 0..MAX_ATTEMPTS {
            match self
                .heartbeat_once(request, body, deadline, cached.take(), learned.clone())
                .await
            {
                Ok((response, _conn, known))
                    if error::coordinator_retriable(response.error_code) =>
                {
                    learned = Some(known);
                    last = Error::broker(response.error_code, "StreamsGroupHeartbeat");
                }
                Ok(response) => return Ok(response),
                Err(err) if err.is_retriable() => {
                    last = err;
                }
                Err(err) => return Err(err),
            }
            if attempt + 1 < MAX_ATTEMPTS {
                let cfg = &self.cfg.connection;
                let (base, max) = if last.broker_code().is_none() {
                    (cfg.reconnect_backoff, cfg.reconnect_backoff_max)
                } else {
                    (cfg.retry_backoff, cfg.retry_backoff_max)
                };
                crate::config::sleep_retry_backoff(
                    base,
                    max,
                    attempt,
                    deadline.target().into_std(),
                )
                .await;
                deadline.check_expired()?;
            }
        }
        Err(last)
    }

    async fn open(
        &self,
        address: &str,
        deadline: Deadline,
    ) -> Result<(BrokerConn, ApiVersionsResponse)> {
        open_streams_heartbeat(&self.cfg.connection, address, deadline).await
    }

    async fn open_any(&self, deadline: Deadline) -> Result<(BrokerConn, ApiVersionsResponse)> {
        let addresses = &self.cfg.connection.bootstrap;
        if addresses.len() <= 1 {
            let mut last = Error::Timeout;
            for address in addresses {
                match self.open(address, deadline).await {
                    Ok(opened) => return Ok(opened),
                    Err(err) if err.is_retriable() => last = err,
                    Err(err) => return Err(err),
                }
            }
            return Err(last);
        }
        let mut inflight = tokio::task::JoinSet::new();
        for address in addresses {
            let cfg = self.cfg.connection.clone();
            let address = address.clone();
            let _ = inflight
                .spawn(async move { open_streams_heartbeat(&cfg, &address, deadline).await });
        }
        let mut last = Error::protocol("all bootstrap servers failed");
        while let Some(joined) = inflight.join_next().await {
            match joined {
                Ok(Ok(opened)) => {
                    inflight.abort_all();
                    return Ok(opened);
                }
                Ok(Err(error)) => last = error,
                Err(error) => last = Error::protocol(format!("streams bootstrap: {error}")),
            }
        }
        Err(last)
    }

    async fn discover(
        &self,
        request: &StreamsGroupHeartbeatRequest,
        deadline: Deadline,
    ) -> Result<(BrokerConn, LearnedStreams)> {
        let (mut bootstrap, versions) = self.open_any(deadline).await?;
        let version = versions
            .api_version(FIND_COORDINATOR)
            .and_then(|api| pick_version(api.min_version, api.max_version, 1, 6))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support FindCoordinator v1-6".into())
            })?;
        let body = bootstrap
            .roundtrip_deadline(
                FIND_COORDINATOR,
                version,
                |buf| {
                    encode_find_coordinator_request_typed(
                        buf,
                        version,
                        &request.group_id,
                        COORDINATOR_GROUP,
                    )
                },
                deadline,
            )
            .await?;
        let address = coordinator_address(&body, version, &request.group_id)?;
        let bootstrap_addr = bootstrap.addr().to_string();
        let bootstrap_keys = versions.api_keys.clone();
        if address == bootstrap.addr() {
            return Ok((
                bootstrap,
                LearnedStreams {
                    bootstrap_addr,
                    bootstrap_keys: bootstrap_keys.clone(),
                    find_version: version,
                    coord_addr: address,
                    coord_keys: bootstrap_keys,
                },
            ));
        }
        let (conn, coord_versions) = self.open(&address, deadline).await?;
        Ok((
            conn,
            LearnedStreams {
                bootstrap_addr,
                bootstrap_keys,
                find_version: version,
                coord_addr: address,
                coord_keys: coord_versions.api_keys,
            },
        ))
    }

    async fn rediscover(
        &self,
        request: &StreamsGroupHeartbeatRequest,
        deadline: Deadline,
        known: &LearnedStreams,
    ) -> Result<(BrokerConn, LearnedStreams)> {
        let mut hop = open_known_streams(
            &self.cfg.connection,
            &known.bootstrap_addr,
            &known.bootstrap_keys,
            deadline,
        )
        .await?;
        let body = hop
            .roundtrip_deadline(
                FIND_COORDINATOR,
                known.find_version,
                |buf| {
                    encode_find_coordinator_request_typed(
                        buf,
                        known.find_version,
                        &request.group_id,
                        COORDINATOR_GROUP,
                    )
                },
                deadline,
            )
            .await?;
        let address = coordinator_address(&body, known.find_version, &request.group_id)?;
        if address == known.coord_addr {
            if address == hop.addr() {
                return Ok((hop, known.clone()));
            }
            let conn =
                open_known_streams(&self.cfg.connection, &address, &known.coord_keys, deadline)
                    .await?;
            return Ok((conn, known.clone()));
        }
        let (conn, coord_versions) = self.open(&address, deadline).await?;
        Ok((
            conn,
            LearnedStreams {
                bootstrap_addr: known.bootstrap_addr.clone(),
                bootstrap_keys: known.bootstrap_keys.clone(),
                find_version: known.find_version,
                coord_addr: address,
                coord_keys: coord_versions.api_keys,
            },
        ))
    }

    async fn heartbeat_once(
        &self,
        request: &StreamsGroupHeartbeatRequest,
        encoded: &[u8],
        deadline: Deadline,
        cached: Option<BrokerConn>,
        learned: Option<LearnedStreams>,
    ) -> Result<(StreamsGroupHeartbeatResponse, BrokerConn, LearnedStreams)> {
        let (mut conn, learned) = match cached {
            Some(conn) => (
                conn,
                learned.ok_or_else(|| Error::protocol("missing Streams versions"))?,
            ),
            None => match learned {
                Some(known) => self.rediscover(request, deadline, &known).await?,
                None => self.discover(request, deadline).await?,
            },
        };
        let response = conn
            .roundtrip_deadline(
                STREAMS_GROUP_HEARTBEAT,
                0,
                |buf| {
                    buf.extend_from_slice(encoded);
                    Ok(())
                },
                deadline,
            )
            .await?;
        let decoded = crate::protocol::streams::decode_streams_group_heartbeat_response(
            &response,
            0,
            self.cfg.limits,
        )?;
        deadline.check_expired()?;
        Ok((decoded, conn, learned))
    }
}

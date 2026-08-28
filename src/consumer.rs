#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion, MetadataResponse,
};
use crate::protocol::api_keys::{
    pick_version, API_VERSIONS, FETCH, LIST_OFFSETS, METADATA, OFFSET_FOR_LEADER_EPOCH,
};
use crate::protocol::epoch::{
    decode_offset_for_leader_epoch_response, encode_offset_for_leader_epoch_request,
};
use crate::protocol::fetch::{
    decode_fetch_response, encode_fetch_request, FetchPartition, FetchTopic,
};
use crate::protocol::offsets::{decode_list_offsets_response, encode_list_offsets_request};
use crate::protocol::records::Header;
use crate::protocol::sasl;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub sasl_plain: Option<(String, String)>,
    pub sasl_scram: Option<(String, String)>,
    pub sasl_scram_sha512: Option<(String, String)>,
    pub sasl_oauthbearer: Option<String>,
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
    pub tls: Option<crate::net::TlsConfig>,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    pub isolation_level: i8,
    /// Client rack for fetch-from-follower (KIP-392). Empty means leader only.
    pub rack: Option<String>,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            sasl_plain: None,
            sasl_scram: None,
            sasl_scram_sha512: None,
            sasl_oauthbearer: None,
            sasl_oauthbearer_oidc: None,
            tls: None,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 16_777_216,
            isolation_level: 0,
            rack: None,
        }
    }
}

impl ConsumerConfig {
    /// Bootstrap brokers, for example `["127.0.0.1:9092"]`.
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// Kafka `client.id`.
    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    /// `fetch.max.wait.ms`.
    #[must_use]
    pub fn max_wait_ms(mut self, ms: i32) -> Self {
        self.max_wait_ms = ms;
        self
    }

    /// `fetch.min.bytes`.
    #[must_use]
    pub fn min_bytes(mut self, n: i32) -> Self {
        self.min_bytes = n;
        self
    }

    /// Cap for the Fetch request and each partition (`fetch.max.bytes` /
    /// `max.partition.fetch.bytes`). Default 16 MiB.
    #[must_use]
    pub fn max_bytes(mut self, n: i32) -> Self {
        self.max_bytes = n;
        self
    }

    /// `isolation.level`.
    #[must_use]
    pub fn isolation(mut self, level: crate::IsolationLevel) -> Self {
        self.isolation_level = level.as_i8();
        self
    }

    /// `client.rack` for fetch-from-follower (KIP-392).
    #[must_use]
    pub fn rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// SASL. Replaces any previously set mechanism.
    #[must_use]
    pub fn sasl(mut self, sasl: crate::Sasl) -> Self {
        sasl.apply_to(
            &mut self.sasl_plain,
            &mut self.sasl_scram,
            &mut self.sasl_scram_sha512,
            &mut self.sasl_oauthbearer,
            &mut self.sasl_oauthbearer_oidc,
        );
        self
    }

    /// rustls. No OpenSSL.
    #[must_use]
    pub fn tls(mut self, tls: crate::net::TlsConfig) -> Self {
        crate::config::apply_tls(&mut self.tls, tls);
        self
    }

    /// Per-request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

#[derive(Debug, Clone)]
pub struct FetchedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

pub struct Consumer {
    cfg: ConsumerConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    fetch_version: i16,
    metadata_version: i16,
    metadata: Option<MetadataResponse>,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
    assigned: Vec<(String, i32, i64)>,
    preferred: HashMap<(String, i32), i32>,
}

impl Consumer {
    /// Connect with default config to one bootstrap server.
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ConsumerConfig::bootstrap([bootstrap.into()])).await
    }

    /// Connect using `cfg`. Negotiates ApiVersions and optional SASL/TLS.
    pub async fn new(cfg: ConsumerConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let mut conn = BrokerConn::connect_tls_any(
            &cfg.bootstrap,
            &cfg.client_id,
            cfg.connect_timeout,
            cfg.tls.as_ref(),
        )
        .await?;
        let body = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                cfg.request_timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), 3)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        let mut versions = HashMap::new();
        for api in resp.api_keys {
            let _prev = versions.insert(api.api_key, api);
        }
        sasl::authenticate(
            &mut conn,
            cfg.sasl_plain.as_ref(),
            cfg.sasl_scram.as_ref(),
            cfg.sasl_scram_sha512.as_ref(),
            cfg.sasl_oauthbearer.as_deref(),
            cfg.sasl_oauthbearer_oidc.as_ref(),
            cfg.request_timeout,
        )
        .await?;
        let fetch_version = versions
            .get(&FETCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 4, 11))
            .ok_or_else(|| Error::Unsupported("broker does not support Fetch v4-11".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 12))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        Ok(Self {
            cfg,
            conn,
            versions,
            fetch_version,
            metadata_version,
            metadata: None,
            cluster: Cluster::default(),
            conns: HashMap::new(),
            assigned: Vec::new(),
            preferred: HashMap::new(),
        })
    }

    pub async fn assign(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let topic = topic.into();
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        self.assigned
            .retain(|(t, p, _)| !(t == &topic && *p == partition));
        self.assigned.push((topic, partition, offset));
        Ok(())
    }

    /// Assign every partition of `topic` at `offset` (from metadata).
    pub async fn assign_topic(&mut self, topic: impl Into<String>, offset: i64) -> Result<()> {
        let topic = topic.into();
        self.refresh_metadata(Some(std::slice::from_ref(&topic)))
            .await?;
        let (error_code, parts): (i16, Vec<i32>) = {
            let tmd = self
                .metadata
                .as_ref()
                .and_then(|md| {
                    md.topics
                        .iter()
                        .find(|t| t.name.as_deref() == Some(topic.as_str()))
                })
                .ok_or_else(|| Error::UnknownTopic(topic.clone()))?;
            (
                tmd.error_code,
                tmd.partitions.iter().map(|p| p.partition_index).collect(),
            )
        };
        if error_code != 0 {
            return Err(Error::broker(error_code, topic));
        }
        if parts.is_empty() {
            return Err(Error::UnknownTopic(topic));
        }
        self.assigned.retain(|(t, _, _)| t != &topic);
        for p in parts {
            self.assigned.push((topic.clone(), p, offset));
        }
        Ok(())
    }

    pub fn assignment(&self) -> &[(String, i32, i64)] {
        &self.assigned
    }

    pub(crate) fn clear_assignment(&mut self) {
        self.assigned.clear();
    }

    /// Replace the assignment. One Metadata refresh for the topic set.
    pub(crate) async fn assign_all(&mut self, starts: &[(String, i32, i64)]) -> Result<()> {
        self.clear_assignment();
        if starts.is_empty() {
            return Ok(());
        }
        let mut topics: Vec<String> = Vec::new();
        for (topic, _, _) in starts {
            if !topics.iter().any(|t| t == topic) {
                topics.push(topic.clone());
            }
        }
        self.refresh_metadata(Some(&topics)).await?;
        self.assigned.extend(starts.iter().cloned());
        Ok(())
    }

    pub(crate) async fn partition_ids(&mut self, topic: &str) -> Result<Vec<i32>> {
        let topics = [topic.to_string()];
        self.refresh_metadata(Some(&topics)).await?;
        let tmd = self
            .metadata
            .as_ref()
            .and_then(|md| md.topics.iter().find(|t| t.name.as_deref() == Some(topic)))
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        if tmd.error_code != 0 {
            return Err(Error::broker(tmd.error_code, topic.to_string()));
        }
        Ok(tmd.partitions.iter().map(|p| p.partition_index).collect())
    }

    pub fn advance(&mut self, topic: &str, partition: i32, next_offset: i64) {
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t == topic && *p == partition)
        {
            slot.2 = next_offset;
        }
    }

    async fn reconnect_bootstrap(&mut self) -> Result<()> {
        self.conns.clear();
        let addr = self.conn.addr().to_string();
        let mut conn = BrokerConn::connect_tls(
            &addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        let body = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                self.cfg.request_timeout,
            )
            .await?;
        let resp = decode_api_versions_response(&mut body.clone(), 3)?;
        if resp.error_code != 0 {
            return Err(Error::broker(resp.error_code, "ApiVersions"));
        }
        sasl::authenticate(
            &mut conn,
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            self.cfg.request_timeout,
        )
        .await?;
        self.conn = conn;
        Ok(())
    }

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        let version = self.metadata_version;
        let timeout = self.cfg.request_timeout;
        let body = match self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, false),
                timeout,
            )
            .await
        {
            Ok(b) => b,
            Err(e) if e.is_retriable() => {
                self.reconnect_bootstrap().await?;
                self.conn
                    .roundtrip(
                        METADATA,
                        version,
                        |buf| encode_metadata_request(buf, version, topics, false),
                        timeout,
                    )
                    .await?
            }
            Err(e) => return Err(e),
        };
        let md = decode_metadata_response(&mut body.clone(), version)?;
        self.cluster.apply(&md);
        self.metadata = Some(md);
        Ok(())
    }

    pub(crate) async fn ensure_topic_metadata(&mut self, topic: &str) -> Result<()> {
        if self.cluster.partition_count(topic).is_some() {
            return Ok(());
        }
        self.refresh_topic_metadata(topic).await
    }

    pub(crate) async fn refresh_topics(&mut self, topics: &[String]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        self.refresh_metadata(Some(topics)).await
    }

    pub(crate) async fn refresh_topic_metadata(&mut self, topic: &str) -> Result<()> {
        let topics = [topic.to_string()];
        self.refresh_metadata(Some(&topics)).await
    }

    pub(crate) fn topic_id_names(&self) -> HashMap<[u8; 16], String> {
        let mut out = HashMap::new();
        let Some(md) = &self.metadata else {
            return out;
        };
        for t in &md.topics {
            if t.topic_id == [0u8; 16] {
                continue;
            }
            if let Some(name) = &t.name {
                let _ = out.insert(t.topic_id, name.clone());
            }
        }
        out
    }

    pub(crate) fn leader_of(&self, topic: &str, partition: i32) -> Result<(i32, String)> {
        self.cluster.leader(topic, partition)
    }

    pub(crate) fn invalidate_topic(&mut self, topic: &str) {
        self.cluster.invalidate_topic(topic);
    }

    pub(crate) fn drop_node(&mut self, node: i32) {
        let _ = self.conns.remove(&node);
    }

    pub(crate) async fn roundtrip_node(
        &mut self,
        node: i32,
        api_key: i16,
        api_version: i16,
        encode_body: impl Fn(&mut bytes::BytesMut) -> Result<()>,
        timeout: Duration,
    ) -> Result<Bytes> {
        self.connect_node(node).await?;
        let conn = self
            .conns
            .get_mut(&node)
            .ok_or_else(|| Error::protocol("missing node conn"))?;
        conn.roundtrip(api_key, api_version, encode_body, timeout)
            .await
    }

    async fn connect_node(&mut self, node: i32) -> Result<()> {
        if self.conns.contains_key(&node) {
            return Ok(());
        }
        let addr = self
            .cluster
            .brokers
            .get(&node)
            .cloned()
            .ok_or_else(|| Error::protocol(format!("unknown broker {node}")))?;
        let mut conn = BrokerConn::connect_tls(
            &addr,
            &self.cfg.client_id,
            self.cfg.connect_timeout,
            self.cfg.tls.as_ref(),
        )
        .await?;
        let _versions = conn
            .roundtrip(
                API_VERSIONS,
                3,
                |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
                self.cfg.request_timeout,
            )
            .await?;
        sasl::authenticate(
            &mut conn,
            self.cfg.sasl_plain.as_ref(),
            self.cfg.sasl_scram.as_ref(),
            self.cfg.sasl_scram_sha512.as_ref(),
            self.cfg.sasl_oauthbearer.as_deref(),
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            self.cfg.request_timeout,
        )
        .await?;
        let _prev = self.conns.insert(node, conn);
        Ok(())
    }

    async fn recover_leader_epoch(&mut self, topic: &str, partition: i32) -> Result<()> {
        let version = self
            .versions
            .get(&OFFSET_FOR_LEADER_EPOCH)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support OffsetForLeaderEpoch".into())
            })?;
        // Preferred replica may have returned the fence; OffsetForLeaderEpoch is leader-only.
        // Refresh Metadata first so `current_leader_epoch` is not the value that just fenced us.
        let _ = self.preferred.remove(&(topic.to_string(), partition));
        let deadline = Instant::now() + self.cfg.request_timeout;
        {
            let topics = [topic.to_string()];
            self.refresh_metadata(Some(&topics)).await?;
        }
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let current = self.cluster.leader_epoch(topic, partition);
            let timeout = self.cfg.request_timeout;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing epoch conn"))?;
                conn.roundtrip(
                    OFFSET_FOR_LEADER_EPOCH,
                    version,
                    |buf| {
                        encode_offset_for_leader_epoch_request(
                            buf, version, topic, partition, current, current,
                        )
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (err, epoch, end_offset) =
                decode_offset_for_leader_epoch_response(&mut body.clone(), version)?;
            if err == 0 {
                self.cluster.set_leader_epoch(topic, partition, epoch);
                let assigned = self
                    .assigned
                    .iter()
                    .find(|(t, p, _)| t == topic && *p == partition)
                    .map(|(_, _, o)| *o);
                if let Some(off) = assigned {
                    if off > end_offset {
                        self.advance(topic, partition, end_offset);
                    }
                }
                return Ok(());
            }
            let e = Error::broker(err, format!("OffsetForLeaderEpoch {topic}-{partition}"));
            let fence = err == error::FENCED_LEADER_EPOCH || err == error::UNKNOWN_LEADER_EPOCH;
            if e.is_retriable() || fence {
                // NOT_LEADER_OR_FOLLOWER (6) / fence: Metadata, then the new leader/epoch.
                self.cluster.invalidate_topic(topic);
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Err(e);
        }
    }

    /// Fetch one round from every assigned partition. Empty if nothing is assigned.
    pub async fn fetch(&mut self) -> Result<Vec<FetchedRecord>> {
        if self.assigned.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + self.cfg.request_timeout;
        loop {
            if self.cluster.leaders.is_empty() {
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
            }
            let mut by_leader: HashMap<i32, HashMap<String, Vec<FetchPartition>>> = HashMap::new();
            let mut missing_leader = false;
            for (topic, part, offset) in &self.assigned {
                let node = if self.cfg.rack.is_some() {
                    self.preferred.get(&(topic.clone(), *part)).copied()
                } else {
                    None
                };
                let node = match node {
                    Some(n) => Some(n),
                    None => self.cluster.leader(topic, *part).ok().map(|(n, _)| n),
                };
                match node {
                    Some(node) => {
                        by_leader
                            .entry(node)
                            .or_default()
                            .entry(topic.clone())
                            .or_default()
                            .push(FetchPartition {
                                partition: *part,
                                current_leader_epoch: self.cluster.leader_epoch(topic, *part),
                                fetch_offset: *offset,
                                partition_max_bytes: self.cfg.max_bytes,
                            });
                    }
                    None => {
                        missing_leader = true;
                        break;
                    }
                }
            }
            if missing_leader {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                for (t, _, _) in &self.assigned {
                    self.cluster.invalidate_topic(t);
                }
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            let max_wait = self.cfg.max_wait_ms;
            let min_bytes = self.cfg.min_bytes;
            let max_bytes = self.cfg.max_bytes;
            let isolation_level = self.cfg.isolation_level;
            let timeout = self.cfg.request_timeout;
            let fetch_version = self.fetch_version;
            let rack = self.cfg.rack.clone();
            let mut out = Vec::new();
            let mut retry = false;
            let leaders: Vec<i32> = by_leader.keys().copied().collect();
            for node in leaders {
                let Some(by_topic) = by_leader.remove(&node) else {
                    continue;
                };
                let topics: Vec<FetchTopic> = by_topic
                    .into_iter()
                    .map(|(topic, partitions)| FetchTopic { topic, partitions })
                    .collect();
                self.connect_node(node).await?;
                let body = {
                    let conn = self
                        .conns
                        .get_mut(&node)
                        .ok_or_else(|| Error::protocol("missing fetch conn"))?;
                    conn.roundtrip(
                        FETCH,
                        fetch_version,
                        |buf| {
                            encode_fetch_request(
                                buf,
                                max_wait,
                                min_bytes,
                                max_bytes,
                                isolation_level,
                                &topics,
                                rack.as_deref(),
                            )
                        },
                        timeout,
                    )
                    .await
                };
                let mut body = match body {
                    Ok(b) => b,
                    Err(e) if e.is_retriable() => {
                        let _ = self.conns.remove(&node);
                        retry = true;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                let fetched = decode_fetch_response(&mut body)?;
                for topic in fetched {
                    for part in topic.partitions {
                        if part.preferred_read_replica >= 0
                            && self.cfg.rack.is_some()
                            && part.preferred_read_replica != node
                        {
                            let _prev = self.preferred.insert(
                                (topic.topic.clone(), part.partition),
                                part.preferred_read_replica,
                            );
                            retry = true;
                            continue;
                        }
                        if part.error_code == error::OFFSET_OUT_OF_RANGE {
                            self.advance(&topic.topic, part.partition, part.log_start_offset);
                            continue;
                        }
                        if part.error_code == error::FENCED_LEADER_EPOCH
                            || part.error_code == error::UNKNOWN_LEADER_EPOCH
                        {
                            self.recover_leader_epoch(&topic.topic, part.partition)
                                .await?;
                            retry = true;
                            continue;
                        }
                        if part.error_code != 0 {
                            let e = Error::broker(
                                part.error_code,
                                format!("{}-{}", topic.topic, part.partition),
                            );
                            if e.is_retriable() {
                                self.cluster.invalidate_topic(&topic.topic);
                                let _ = self.conns.remove(&node);
                                retry = true;
                                continue;
                            }
                            return Err(e);
                        }
                        let mut next = None;
                        let isolation = self.cfg.isolation_level;
                        for batch in part.records {
                            if batch.attributes & crate::protocol::records::ATTR_CONTROL != 0 {
                                if let Some(last) = batch.records.last() {
                                    next = Some(last.offset + 1);
                                }
                                continue;
                            }
                            for rec in batch.records {
                                let offset = rec.offset;
                                if isolation == 1 && offset >= part.last_stable_offset {
                                    break;
                                }
                                next = Some(offset + 1);
                                if isolation == 1 {
                                    let aborted =
                                        part.aborted_transactions.iter().any(|(pid, first)| {
                                            batch.producer_id == *pid && offset >= *first
                                        });
                                    if aborted {
                                        continue;
                                    }
                                }
                                out.push(FetchedRecord {
                                    topic: topic.topic.clone(),
                                    partition: part.partition,
                                    offset,
                                    timestamp: rec.timestamp,
                                    key: rec.key,
                                    value: rec.value,
                                    headers: rec.headers,
                                });
                            }
                        }
                        if let Some(n) = next {
                            self.advance(&topic.topic, part.partition, n);
                        }
                    }
                }
            }
            if retry {
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics: Vec<String> = self.assigned.iter().map(|(t, _, _)| t.clone()).collect();
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok(out);
        }
    }

    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    #[expect(
        dead_code,
        reason = "callers that already hold a Consumer use this to hop FindCoordinator"
    )]
    pub(crate) fn conn_mut(&mut self) -> &mut BrokerConn {
        &mut self.conn
    }

    /// ListOffsets timestamp: `EARLIEST_TIMESTAMP` (-2), `LATEST_TIMESTAMP` (-1), or ms.
    pub async fn list_offsets(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let topic = topic.into();
        let deadline = Instant::now() + self.cfg.request_timeout;
        loop {
            if self.cluster.leader(&topic, partition).is_err() {
                let topics = [topic.clone()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(&topic, partition)?;
            self.connect_node(node).await?;
            let version = self
                .versions
                .get(&LIST_OFFSETS)
                .and_then(|v| pick_version(v.min_version, v.max_version, 1, 5))
                .ok_or_else(|| Error::Unsupported("broker does not support ListOffsets".into()))?;
            let isolation = self.cfg.isolation_level;
            let timeout = self.cfg.request_timeout;
            let current_leader_epoch = self.cluster.leader_epoch(&topic, partition);
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing list_offsets conn"))?;
                conn.roundtrip(
                    LIST_OFFSETS,
                    version,
                    |buf| {
                        encode_list_offsets_request(
                            buf,
                            version,
                            isolation,
                            &topic,
                            partition,
                            current_leader_epoch,
                            timestamp,
                        )
                    },
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            match decode_list_offsets_response(&mut body.clone(), version) {
                Ok((_err, _ts, offset)) => return Ok(offset),
                Err(e)
                    if matches!(
                        &e,
                        Error::Broker {
                            code: error::FENCED_LEADER_EPOCH | error::UNKNOWN_LEADER_EPOCH,
                            ..
                        }
                    ) =>
                {
                    self.recover_leader_epoch(&topic, partition).await?;
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) if e.is_retriable() => {
                    // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                    self.cluster.invalidate_topic(&topic);
                    let _ = self.conns.remove(&node);
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let topics = [topic.clone()];
                    self.refresh_metadata(Some(&topics)).await?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Set the next fetch offset for an assigned partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        if let Some(slot) = self
            .assigned
            .iter_mut()
            .find(|(t, p, _)| t == topic && *p == partition)
        {
            slot.2 = offset;
            return Ok(());
        }
        Err(Error::protocol(format!(
            "seek of unassigned {topic}-{partition}"
        )))
    }

    /// Seek every assigned partition to the log start (`ListOffsets` earliest).
    pub async fn seek_to_beginning(&mut self) -> Result<()> {
        self.seek_assigned(crate::EARLIEST_TIMESTAMP).await
    }

    /// Seek every assigned partition to the high watermark (`ListOffsets` latest).
    pub async fn seek_to_end(&mut self) -> Result<()> {
        self.seek_assigned(crate::LATEST_TIMESTAMP).await
    }

    async fn seek_assigned(&mut self, timestamp: i64) -> Result<()> {
        let assigned: Vec<(String, i32)> = self
            .assigned
            .iter()
            .map(|(t, p, _)| (t.clone(), *p))
            .collect();
        for (topic, partition) in assigned {
            let offset = self
                .list_offsets(topic.clone(), partition, timestamp)
                .await?;
            self.seek(&topic, partition, offset)?;
        }
        Ok(())
    }
}

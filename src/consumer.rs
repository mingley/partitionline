#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::cluster::Cluster;
use crate::error::{Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion, MetadataResponse,
};
use crate::protocol::api_keys::{pick_version, API_VERSIONS, FETCH, LIST_OFFSETS, METADATA};
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
    pub tls: Option<crate::net::TlsConfig>,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    pub isolation_level: i8,
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
            tls: None,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
        }
    }
}

impl ConsumerConfig {
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
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
}

impl Consumer {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(ConsumerConfig::bootstrap([bootstrap.into()])).await
    }

    pub async fn new(cfg: ConsumerConfig) -> Result<Self> {
        if cfg.bootstrap.is_empty() {
            return Err(Error::protocol("no bootstrap servers"));
        }
        let addr = cfg
            .bootstrap
            .first()
            .ok_or_else(|| Error::protocol("no bootstrap servers"))?
            .clone();
        let mut conn =
            BrokerConn::connect_tls(&addr, &cfg.client_id, cfg.connect_timeout, cfg.tls.as_ref())
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

    async fn refresh_metadata(&mut self, topics: Option<&[String]>) -> Result<()> {
        let version = self.metadata_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                METADATA,
                version,
                |buf| encode_metadata_request(buf, version, topics, false),
                timeout,
            )
            .await?;
        let md = decode_metadata_response(&mut body.clone(), version)?;
        self.cluster.apply(&md);
        self.metadata = Some(md);
        Ok(())
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
            self.cfg.request_timeout,
        )
        .await?;
        let _prev = self.conns.insert(node, conn);
        Ok(())
    }

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
                match self.cluster.leader(topic, *part) {
                    Ok((node, _)) => {
                        by_leader
                            .entry(node)
                            .or_default()
                            .entry(topic.clone())
                            .or_default()
                            .push(FetchPartition {
                                partition: *part,
                                fetch_offset: *offset,
                                partition_max_bytes: self.cfg.max_bytes,
                            });
                    }
                    Err(_) => {
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
                        retry = true;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                let fetched = decode_fetch_response(&mut body.clone())?;
                for topic in fetched {
                    for part in topic.partitions {
                        if part.error_code == crate::error::OFFSET_OUT_OF_RANGE {
                            self.advance(&topic.topic, part.partition, part.log_start_offset);
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
        let conn = self
            .conns
            .get_mut(&node)
            .ok_or_else(|| Error::protocol("missing list_offsets conn"))?;
        let body = conn
            .roundtrip(
                LIST_OFFSETS,
                version,
                |buf| {
                    encode_list_offsets_request(
                        buf, version, isolation, &topic, partition, timestamp,
                    )
                },
                timeout,
            )
            .await?;
        let (_err, _ts, offset) = decode_list_offsets_response(&mut body.clone(), version)?;
        Ok(offset)
    }

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
}

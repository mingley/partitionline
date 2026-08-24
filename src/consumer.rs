use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion, MetadataResponse,
};
use crate::protocol::api_keys::{pick_version, API_VERSIONS, FETCH, METADATA};
use crate::protocol::fetch::{
    decode_fetch_response, encode_fetch_request, FetchPartition, FetchTopic,
};
use crate::protocol::records::Header;
use crate::protocol::sasl;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub sasl_plain: Option<(String, String)>,
    pub tls: Option<crate::net::TlsConfig>,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec!["127.0.0.1:9092".into()],
            client_id: "partitionline".into(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            sasl_plain: None,
            tls: None,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
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
        let addr = cfg.bootstrap[0].clone();
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
            versions.insert(api.api_key, api);
        }
        if let Some((u, p)) = &cfg.sasl_plain {
            sasl::authenticate_plain(&mut conn, u, p, cfg.request_timeout).await?;
        }
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

    pub fn assignment(&self) -> &[(String, i32, i64)] {
        &self.assigned
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
        self.metadata = Some(decode_metadata_response(&mut body.clone(), version)?);
        Ok(())
    }

    pub async fn fetch(&mut self) -> Result<Vec<FetchedRecord>> {
        if self.assigned.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_topic: HashMap<String, Vec<FetchPartition>> = HashMap::new();
        for (topic, part, offset) in &self.assigned {
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(FetchPartition {
                    partition: *part,
                    fetch_offset: *offset,
                    partition_max_bytes: self.cfg.max_bytes,
                });
        }
        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(topic, partitions)| FetchTopic { topic, partitions })
            .collect();
        let max_wait = self.cfg.max_wait_ms;
        let min_bytes = self.cfg.min_bytes;
        let max_bytes = self.cfg.max_bytes;
        let timeout = self.cfg.request_timeout;
        let version = self.fetch_version;
        let _ = version;
        let body = self
            .conn
            .roundtrip(
                FETCH,
                11,
                |buf| encode_fetch_request(buf, max_wait, min_bytes, max_bytes, &topics),
                timeout,
            )
            .await?;
        let fetched = decode_fetch_response(&mut body.clone())?;
        let mut out = Vec::new();
        for topic in fetched {
            for part in topic.partitions {
                if part.error_code != 0 {
                    return Err(Error::broker(
                        part.error_code,
                        format!("{}-{}", topic.topic, part.partition),
                    ));
                }
                let mut next = None;
                for batch in part.records {
                    for rec in batch.records {
                        let offset = rec.offset;
                        out.push(FetchedRecord {
                            topic: topic.topic.clone(),
                            partition: part.partition,
                            offset,
                            timestamp: rec.timestamp,
                            key: rec.key,
                            value: rec.value,
                            headers: rec.headers,
                        });
                        next = Some(offset + 1);
                    }
                }
                if let Some(n) = next {
                    self.advance(&topic.topic, part.partition, n);
                }
            }
        }
        Ok(out)
    }

    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    pub(crate) fn conn_mut(&mut self) -> &mut BrokerConn {
        &mut self.conn
    }
}

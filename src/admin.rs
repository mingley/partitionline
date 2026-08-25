#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::protocol::admin::{
    decode_create_topics_response, decode_delete_topics_response, decode_describe_configs_response,
    encode_create_topics_request, encode_delete_topics_request, encode_describe_configs_request,
    CreatableTopic, CreateTopicsRequest, DescribeConfigsResource, DescribeConfigsResult,
    TopicConfig, TopicResult, RESOURCE_BROKER, RESOURCE_TOPIC,
};
use crate::protocol::api::{decode_api_versions_response, encode_api_versions_request, ApiVersion};
use crate::protocol::api_keys::{
    pick_version, API_VERSIONS, CREATE_TOPICS, DELETE_TOPICS, DESCRIBE_CONFIGS,
};
use crate::protocol::sasl;

pub use crate::protocol::admin::{
    ConfigEntry, ConfigSynonym, RESOURCE_BROKER as CONFIG_RESOURCE_BROKER,
    RESOURCE_TOPIC as CONFIG_RESOURCE_TOPIC,
};

#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub bootstrap: Vec<String>,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub sasl_plain: Option<(String, String)>,
    pub sasl_scram: Option<(String, String)>,
    pub sasl_scram_sha512: Option<(String, String)>,
    pub sasl_oauthbearer: Option<String>,
    pub tls: Option<TlsConfig>,
}

impl Default for AdminConfig {
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
        }
    }
}

impl AdminConfig {
    pub fn bootstrap<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            bootstrap: servers.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<(String, Option<String>)>,
}

impl NewTopic {
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: Vec::new(),
        }
    }

    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), Some(value.into())));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    pub resource_type: i8,
    pub name: String,
    pub keys: Option<Vec<String>>,
}

impl ConfigResource {
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            resource_type: RESOURCE_TOPIC,
            name: name.into(),
            keys: None,
        }
    }

    pub fn broker(id: i32) -> Self {
        Self {
            resource_type: RESOURCE_BROKER,
            name: id.to_string(),
            keys: None,
        }
    }

    pub fn keys(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }
}

pub struct Admin {
    cfg: AdminConfig,
    conn: BrokerConn,
    versions: HashMap<i16, ApiVersion>,
    create_version: i16,
    delete_version: i16,
    describe_version: i16,
}

impl Admin {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(AdminConfig::bootstrap([bootstrap.into()])).await
    }

    pub async fn new(cfg: AdminConfig) -> Result<Self> {
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
        let create_version = versions
            .get(&CREATE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 4))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support CreateTopics v0-4".into())
            })?;
        let delete_version = versions
            .get(&DELETE_TOPICS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 3))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DeleteTopics v0-3".into())
            })?;
        let describe_version = versions
            .get(&DESCRIBE_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support DescribeConfigs v0-1".into())
            })?;
        Ok(Self {
            cfg,
            conn,
            versions,
            create_version,
            delete_version,
            describe_version,
        })
    }

    pub fn versions(&self) -> &HashMap<i16, ApiVersion> {
        &self.versions
    }

    pub async fn create_topics(
        &mut self,
        topics: &[NewTopic],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let req = CreateTopicsRequest {
            topics: topics
                .iter()
                .map(|t| CreatableTopic {
                    name: t.name.clone(),
                    num_partitions: t.num_partitions,
                    replication_factor: t.replication_factor,
                    assignments: Vec::new(),
                    configs: t
                        .configs
                        .iter()
                        .map(|(n, v)| TopicConfig {
                            name: n.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                })
                .collect(),
            timeout_ms,
            validate_only,
        };
        let version = self.create_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                CREATE_TOPICS,
                version,
                |buf| encode_create_topics_request(buf, version, &req),
                timeout,
            )
            .await?;
        decode_create_topics_response(&mut body.clone(), version)
    }

    pub async fn delete_topics(
        &mut self,
        names: &[impl AsRef<str>],
        timeout_ms: i32,
    ) -> Result<Vec<TopicResult>> {
        let names: Vec<String> = names.iter().map(|n| n.as_ref().to_string()).collect();
        let version = self.delete_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DELETE_TOPICS,
                version,
                |buf| encode_delete_topics_request(buf, &names, timeout_ms),
                timeout,
            )
            .await?;
        decode_delete_topics_response(&mut body.clone(), version)
    }

    pub async fn describe_configs(
        &mut self,
        resources: &[ConfigResource],
        include_synonyms: bool,
    ) -> Result<Vec<DescribeConfigsResult>> {
        let req: Vec<DescribeConfigsResource> = resources
            .iter()
            .map(|r| DescribeConfigsResource {
                resource_type: r.resource_type,
                name: r.name.clone(),
                keys: r.keys.clone(),
            })
            .collect();
        let version = self.describe_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_CONFIGS,
                version,
                |buf| encode_describe_configs_request(buf, version, &req, include_synonyms),
                timeout,
            )
            .await?;
        decode_describe_configs_response(&mut body.clone(), version)
    }
}

#![expect(
    missing_docs,
    reason = "public client types are named for their Kafka role; crate rustdoc covers connect/send/fetch/admin"
)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::cluster::Cluster;
use crate::error::{self, Error, Result};
use crate::net::{BrokerConn, TlsConfig};
use crate::protocol::acl::{
    decode_create_acls_response, decode_delete_acls_response, decode_describe_acls_response,
    encode_create_acls_request, encode_delete_acls_request, encode_describe_acls_request,
};
use crate::protocol::admin::{
    decode_alter_configs_response, decode_create_partitions_response,
    decode_create_topics_response, decode_delete_records_response, decode_delete_topics_response,
    decode_describe_cluster_response, decode_describe_configs_response,
    decode_incremental_alter_configs_response, encode_alter_configs_request,
    encode_create_partitions_request, encode_create_topics_request, encode_delete_records_request,
    encode_delete_topics_request, encode_describe_cluster_request, encode_describe_configs_request,
    encode_incremental_alter_configs_request, CreatableTopic, CreateTopicsRequest,
    DescribeConfigsResource, DescribeConfigsResult, TopicConfig, TopicResult, RESOURCE_BROKER,
    RESOURCE_TOPIC,
};
use crate::protocol::api::{
    decode_api_versions_response, decode_metadata_response, encode_api_versions_request,
    encode_metadata_request, ApiVersion,
};
use crate::protocol::api_keys::{
    pick_version, ALTER_CONFIGS, API_VERSIONS, CREATE_ACLS, CREATE_PARTITIONS, CREATE_TOPICS,
    DELETE_ACLS, DELETE_RECORDS, DELETE_TOPICS, DESCRIBE_ACLS, DESCRIBE_CLUSTER, DESCRIBE_CONFIGS,
    INCREMENTAL_ALTER_CONFIGS, METADATA,
};
use crate::protocol::sasl;

pub use crate::protocol::acl::AclBinding;
pub use crate::protocol::admin::{
    AlterConfig, ClusterDescription, ConfigEntry, ConfigSynonym, ALTER_CONFIG_DELETE,
    ALTER_CONFIG_SET, RESOURCE_BROKER as CONFIG_RESOURCE_BROKER,
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
    pub sasl_oauthbearer_oidc: Option<crate::OidcConfig>,
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
            sasl_oauthbearer_oidc: None,
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
    partitions_version: i16,
    alter_version: i16,
    legacy_alter_version: i16,
    delete_records_version: i16,
    describe_cluster_version: i16,
    create_acls_version: i16,
    describe_acls_version: i16,
    delete_acls_version: i16,
    metadata_version: i16,
    cluster: Cluster,
    conns: HashMap<i32, BrokerConn>,
}

impl Admin {
    pub async fn connect(bootstrap: impl Into<String>) -> Result<Self> {
        Self::new(AdminConfig::bootstrap([bootstrap.into()])).await
    }

    pub async fn new(cfg: AdminConfig) -> Result<Self> {
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
        let partitions_version = versions
            .get(&CREATE_PARTITIONS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support CreatePartitions".into()))?;
        let alter_version = versions
            .get(&INCREMENTAL_ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| {
                Error::Unsupported("broker does not support IncrementalAlterConfigs".into())
            })?;
        let legacy_alter_version = versions
            .get(&ALTER_CONFIGS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support AlterConfigs".into()))?;
        let delete_records_version = versions
            .get(&DELETE_RECORDS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteRecords".into()))?;
        let describe_cluster_version = versions
            .get(&DESCRIBE_CLUSTER)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeCluster".into()))?;
        let create_acls_version = versions
            .get(&CREATE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support CreateAcls".into()))?;
        let describe_acls_version = versions
            .get(&DESCRIBE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DescribeAcls".into()))?;
        let delete_acls_version = versions
            .get(&DELETE_ACLS)
            .and_then(|v| pick_version(v.min_version, v.max_version, 0, 0))
            .ok_or_else(|| Error::Unsupported("broker does not support DeleteAcls".into()))?;
        let metadata_version = versions
            .get(&METADATA)
            .and_then(|v| pick_version(v.min_version, v.max_version, 1, 12))
            .ok_or_else(|| Error::Unsupported("broker does not support Metadata".into()))?;
        Ok(Self {
            cfg,
            conn,
            versions,
            create_version,
            delete_version,
            describe_version,
            partitions_version,
            alter_version,
            legacy_alter_version,
            delete_records_version,
            describe_cluster_version,
            create_acls_version,
            describe_acls_version,
            delete_acls_version,
            metadata_version,
            cluster: Cluster::default(),
            conns: HashMap::new(),
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
        let deadline = Instant::now() + timeout;
        loop {
            if self.cluster.controller().is_err() {
                self.refresh_metadata(None).await?;
            }
            let node = self.cluster.controller()?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing create_topics conn"))?;
                conn.roundtrip(
                    CREATE_TOPICS,
                    version,
                    |buf| encode_create_topics_request(buf, version, &req),
                    timeout,
                )
                .await
            };
            let body = match body {
                Ok(b) => b,
                Err(e) if e.is_retriable() => {
                    let _ = self.conns.remove(&node);
                    self.cluster.invalidate_controller();
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let results = decode_create_topics_response(&mut body.clone(), version)?;
            if results
                .iter()
                .any(|r| r.error_code == error::NOT_CONTROLLER)
            {
                // NOT_CONTROLLER (41): Metadata, then the new controller.
                self.cluster.invalidate_controller();
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                self.refresh_metadata(None).await?;
                continue;
            }
            return Ok(results);
        }
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

    pub async fn create_partitions(
        &mut self,
        topics: &[(String, i32)],
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Vec<TopicResult>> {
        let topics = topics.to_vec();
        let version = self.partitions_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                CREATE_PARTITIONS,
                version,
                |buf| encode_create_partitions_request(buf, &topics, timeout_ms, validate_only),
                timeout,
            )
            .await?;
        decode_create_partitions_response(&mut body.clone())
    }

    pub async fn incremental_alter_configs(
        &mut self,
        resource_type: i8,
        name: &str,
        configs: &[AlterConfig],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.alter_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                INCREMENTAL_ALTER_CONFIGS,
                version,
                |buf| {
                    encode_incremental_alter_configs_request(
                        buf,
                        resource_type,
                        name,
                        configs,
                        validate_only,
                    )
                },
                timeout,
            )
            .await?;
        decode_incremental_alter_configs_response(&mut body.clone())
    }

    pub async fn create_acls(&mut self, acls: &[AclBinding]) -> Result<Vec<i16>> {
        let version = self.create_acls_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                CREATE_ACLS,
                version,
                |buf| encode_create_acls_request(buf, acls),
                timeout,
            )
            .await?;
        decode_create_acls_response(&mut body.clone())
    }

    pub async fn describe_acls(&mut self, resource_type: i8) -> Result<Vec<AclBinding>> {
        let version = self.describe_acls_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_ACLS,
                version,
                |buf| encode_describe_acls_request(buf, resource_type),
                timeout,
            )
            .await?;
        decode_describe_acls_response(&mut body.clone())
    }

    pub async fn alter_configs(
        &mut self,
        resource_type: i8,
        name: &str,
        configs: &[(String, Option<String>)],
        validate_only: bool,
    ) -> Result<i16> {
        let version = self.legacy_alter_version;
        let timeout = self.cfg.request_timeout;
        let configs: Vec<TopicConfig> = configs
            .iter()
            .map(|(n, v)| TopicConfig {
                name: n.clone(),
                value: v.clone(),
            })
            .collect();
        let body = self
            .conn
            .roundtrip(
                ALTER_CONFIGS,
                version,
                |buf| {
                    encode_alter_configs_request(
                        buf,
                        version,
                        resource_type,
                        name,
                        &configs,
                        validate_only,
                    )
                },
                timeout,
            )
            .await?;
        decode_alter_configs_response(&mut body.clone(), version)
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
            self.cfg.sasl_oauthbearer_oidc.as_ref(),
            self.cfg.request_timeout,
        )
        .await?;
        let _prev = self.conns.insert(node, conn);
        Ok(())
    }

    pub async fn delete_records(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        timeout_ms: i32,
    ) -> Result<(i64, i16)> {
        let version = self.delete_records_version;
        let timeout = self.cfg.request_timeout;
        let deadline = Instant::now() + timeout;
        loop {
            if self.cluster.leader(topic, partition).is_err() {
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
            }
            let (node, _) = self.cluster.leader(topic, partition)?;
            self.connect_node(node).await?;
            let body = {
                let conn = self
                    .conns
                    .get_mut(&node)
                    .ok_or_else(|| Error::protocol("missing delete_records conn"))?;
                conn.roundtrip(
                    DELETE_RECORDS,
                    version,
                    |buf| encode_delete_records_request(buf, topic, partition, offset, timeout_ms),
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
            let (_p, low, err) = decode_delete_records_response(&mut body.clone(), version)?;
            if err == 0 {
                return Ok((low, err));
            }
            let e = Error::broker(err, format!("{topic}-{partition}"));
            if e.is_retriable() {
                // NOT_LEADER_OR_FOLLOWER (6) and friends: Metadata, then the new leader.
                self.cluster.invalidate_topic(topic);
                let _ = self.conns.remove(&node);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                let topics = [topic.to_string()];
                self.refresh_metadata(Some(&topics)).await?;
                continue;
            }
            return Ok((low, err));
        }
    }

    pub async fn describe_cluster(&mut self) -> Result<ClusterDescription> {
        let version = self.describe_cluster_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DESCRIBE_CLUSTER,
                version,
                |buf| encode_describe_cluster_request(buf, false),
                timeout,
            )
            .await?;
        decode_describe_cluster_response(&mut body.clone())
    }

    pub async fn delete_acls(&mut self, resource_type: i8) -> Result<i16> {
        let version = self.delete_acls_version;
        let timeout = self.cfg.request_timeout;
        let body = self
            .conn
            .roundtrip(
                DELETE_ACLS,
                version,
                |buf| encode_delete_acls_request(buf, resource_type),
                timeout,
            )
            .await?;
        decode_delete_acls_response(&mut body.clone())
    }
}

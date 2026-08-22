//! Bootstrap, ApiVersions, and Metadata using kafka-protocol types.

use std::collections::HashMap;

use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::broker::{parse_bootstrap, Broker};
use crate::error::{Error, Result};

/// Versions we encode. Intersected with the broker's ApiVersions table.
#[derive(Debug, Clone, Copy)]
pub struct Negotiated {
    /// ApiVersions body version (always 3 after handshake).
    pub api_versions: i16,
    /// Metadata request version.
    pub metadata: i16,
    /// Produce request version.
    pub produce: i16,
    /// Fetch request version.
    pub fetch: i16,
}

impl Default for Negotiated {
    fn default() -> Self {
        Self {
            api_versions: 3,
            metadata: 12,
            produce: 8,
            fetch: 11,
        }
    }
}

/// One partition's leader from Metadata.
#[derive(Debug, Clone)]
pub struct PartitionMeta {
    /// Partition index.
    pub index: i32,
    /// Leader broker id, or -1.
    pub leader: i32,
}

/// Topic metadata we cache.
#[derive(Debug, Clone)]
pub struct TopicMeta {
    /// Topic name.
    pub name: String,
    /// Partitions by index.
    pub partitions: HashMap<i32, PartitionMeta>,
}

/// Broker advertised listener.
#[derive(Debug, Clone)]
pub struct Node {
    /// Kafka node id.
    pub id: i32,
    /// Host.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// Cluster view plus live connections.
pub struct Client {
    bootstrap: Vec<(String, u16)>,
    versions: HashMap<i16, (i16, i16)>,
    /// Negotiated encode versions.
    pub negotiated: Negotiated,
    nodes: HashMap<i32, Node>,
    topics: HashMap<String, TopicMeta>,
    conns: HashMap<i32, Broker>,
    seed: Option<Broker>,
}

impl Client {
    /// Connect to the first reachable bootstrap and run ApiVersions + Metadata.
    pub async fn connect(bootstrap: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self> {
        let bootstrap: Vec<(String, u16)> = bootstrap
            .into_iter()
            .map(|s| parse_bootstrap(s.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        if bootstrap.is_empty() {
            return Err(Error::NoBootstrap);
        }
        let mut client = Self {
            bootstrap,
            versions: HashMap::new(),
            negotiated: Negotiated::default(),
            nodes: HashMap::new(),
            topics: HashMap::new(),
            conns: HashMap::new(),
            seed: None,
        };
        client.rebootstrap().await?;
        client.refresh_metadata(None).await?;
        Ok(client)
    }

    async fn rebootstrap(&mut self) -> Result<()> {
        let mut last = Error::NoBootstrap;
        for (host, port) in self.bootstrap.clone() {
            match Broker::connect(&host, port).await {
                Ok(mut b) => match b.api_versions().await {
                    Ok(resp) => {
                        Error::check(resp.error_code)?;
                        self.versions.clear();
                        for v in resp.api_keys {
                            self.versions
                                .insert(v.api_key, (v.min_version, v.max_version));
                        }
                        self.negotiated = Negotiated {
                            api_versions: 3,
                            metadata: self.pick(ApiKey::Metadata, 12, 9),
                            produce: self.pick(ApiKey::Produce, 8, 3),
                            fetch: self.pick(ApiKey::Fetch, 11, 4),
                        };
                        self.seed = Some(b);
                        return Ok(());
                    }
                    Err(e) => last = e,
                },
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    fn pick(&self, key: ApiKey, prefer: i16, min: i16) -> i16 {
        match self.versions.get(&(key as i16)) {
            Some(&(bmin, bmax)) => prefer.min(bmax).max(bmin.max(min)),
            None => prefer,
        }
    }

    fn seed(&mut self) -> Result<&mut Broker> {
        self.seed.as_mut().ok_or(Error::NoBootstrap)
    }

    /// Metadata for `topics` (`None` = all topics).
    pub async fn refresh_metadata(
        &mut self,
        topics: Option<&[String]>,
    ) -> Result<MetadataResponse> {
        let req = match topics {
            None => MetadataRequest::default()
                .with_topics(None)
                .with_allow_auto_topic_creation(false),
            Some(names) => {
                let ts = names
                    .iter()
                    .map(|n| {
                        MetadataRequestTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string(n.clone()))))
                    })
                    .collect();
                MetadataRequest::default()
                    .with_topics(Some(ts))
                    .with_allow_auto_topic_creation(true)
            }
        };
        let ver = self.negotiated.metadata;
        let resp: MetadataResponse = self.seed()?.call(ApiKey::Metadata, ver, &req).await?;
        if resp.error_code != 0 {
            Error::check(resp.error_code)?;
        }
        self.nodes.clear();
        for b in &resp.brokers {
            if b.port < 0 || b.port > u16::MAX as i32 {
                continue;
            }
            self.nodes.insert(
                b.node_id.0,
                Node {
                    id: b.node_id.0,
                    host: b.host.to_string(),
                    port: b.port as u16,
                },
            );
        }
        self.topics.clear();
        for t in &resp.topics {
            Error::check(t.error_code)?;
            let name = match &t.name {
                Some(n) => n.0.as_str().to_string(),
                None => continue,
            };
            let mut partitions = HashMap::new();
            for p in &t.partitions {
                Error::check(p.error_code)?;
                partitions.insert(
                    p.partition_index,
                    PartitionMeta {
                        index: p.partition_index,
                        leader: p.leader_id.0,
                    },
                );
            }
            self.topics
                .insert(name.clone(), TopicMeta { name, partitions });
        }
        Ok(resp)
    }

    /// Leader node id for a topic partition.
    pub fn leader_id(&self, topic: &str, partition: i32) -> Result<i32> {
        let t = self
            .topics
            .get(topic)
            .ok_or_else(|| Error::UnknownPartition {
                topic: topic.to_string(),
                partition,
            })?;
        let p = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::UnknownPartition {
                topic: topic.to_string(),
                partition,
            })?;
        if p.leader < 0 {
            return Err(Error::UnknownPartition {
                topic: topic.to_string(),
                partition,
            });
        }
        Ok(p.leader)
    }

    /// Connection to `node_id`, opening if needed.
    pub async fn broker(&mut self, node_id: i32) -> Result<&mut Broker> {
        if !self.conns.contains_key(&node_id) {
            let node = self.nodes.get(&node_id).ok_or(Error::NoBootstrap)?.clone();
            let b = Broker::connect(&node.host, node.port).await?;
            self.conns.insert(node_id, b);
        }
        Ok(self.conns.get_mut(&node_id).expect("just inserted"))
    }

    /// Partition count for a cached topic.
    pub fn partition_count(&self, topic: &str) -> Option<i32> {
        self.topics.get(topic).map(|t| t.partitions.len() as i32)
    }
}

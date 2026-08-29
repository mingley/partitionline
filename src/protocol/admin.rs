//! Admin Kafka protocol codecs: topics, configs, groups, quotas,
//! transactions, telemetry, log dirs, and delegation tokens.
//!
//! ACL codecs live in [`super::acl`].

use std::fmt;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Kafka SCRAM mechanism id (KIP-554 / `ScramMechanism`).
pub const SCRAM_UNKNOWN: i8 = 0;
/// Kafka SCRAM mechanism id (KIP-554 / `ScramMechanism`).
pub const SCRAM_SHA_256: i8 = 1;
/// Kafka SCRAM mechanism id (KIP-554 / `ScramMechanism`).
pub const SCRAM_SHA_512: i8 = 2;

/// Kafka SCRAM mechanism (`ScramMechanism` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum ScramMechanism {
    /// Java `UNKNOWN`.
    Unknown = 0,
    /// SCRAM-SHA-256.
    Sha256 = 1,
    /// SCRAM-SHA-512.
    Sha512 = 2,
}

impl From<ScramMechanism> for i8 {
    fn from(mech: ScramMechanism) -> Self {
        mech as i8
    }
}

impl ScramMechanism {
    /// Java `ScramMechanism.fromType` (out of range is [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            1 => Self::Sha256,
            2 => Self::Sha512,
            _ => Self::Unknown,
        }
    }

    /// Java `ScramMechanism.fromMechanismName` (exact SASL name; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn from_mechanism_name(name: &str) -> Self {
        match name {
            "UNKNOWN" => Self::Unknown,
            "SCRAM-SHA-256" => Self::Sha256,
            "SCRAM-SHA-512" => Self::Sha512,
            _ => Self::Unknown,
        }
    }

    /// Java `ScramMechanism.mechanismName` (`toString` with `_` → `-`).
    #[must_use]
    pub const fn mechanism_name(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// Config resource type for a topic (`DescribeConfigs` / `AlterConfigs`).
pub const RESOURCE_TOPIC: i8 = 2;
/// Config resource type for a broker.
pub const RESOURCE_BROKER: i8 = 4;
/// Config resource type for broker logger (KIP-1142 ListConfigResources).
pub const RESOURCE_BROKER_LOGGER: i8 = 8;
/// Config resource type for client metrics (KIP-714 / KIP-1142).
pub const RESOURCE_CLIENT_METRICS: i8 = 16;
/// Config resource type for consumer groups (KIP-1142).
pub const RESOURCE_GROUP: i8 = 32;
/// Config source: unknown (Java `ConfigSource.UNKNOWN`; alterConfigs entries).
pub const CONFIG_SOURCE_UNKNOWN: i8 = 0;
/// Config source: dynamic topic config (Java `DYNAMIC_TOPIC_CONFIG`).
pub const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
/// Config source: dynamic broker config.
pub const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
/// Config source: dynamic default broker config.
pub const CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER: i8 = 3;
/// Config source: static broker config (`server.properties`).
pub const CONFIG_SOURCE_STATIC_BROKER: i8 = 4;
/// Config source: default (Java `DEFAULT_CONFIG`).
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// Config source: dynamic broker logger config.
pub const CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER: i8 = 6;
/// Config source: dynamic client metrics config.
pub const CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS: i8 = 7;
/// Config source: dynamic group config.
pub const CONFIG_SOURCE_DYNAMIC_GROUP: i8 = 8;
/// DescribeConfigs v3+ ConfigType: unknown (Java `ConfigType.UNKNOWN`).
pub const CONFIG_TYPE_UNKNOWN: i8 = 0;
/// DescribeConfigs v3+ ConfigType: boolean.
pub const CONFIG_TYPE_BOOLEAN: i8 = 1;
/// DescribeConfigs v3+ ConfigType: string.
pub const CONFIG_TYPE_STRING: i8 = 2;
/// DescribeConfigs v3+ ConfigType: int.
pub const CONFIG_TYPE_INT: i8 = 3;
/// DescribeConfigs v3+ ConfigType: short.
pub const CONFIG_TYPE_SHORT: i8 = 4;
/// DescribeConfigs v3+ ConfigType: long.
pub const CONFIG_TYPE_LONG: i8 = 5;
/// DescribeConfigs v3+ ConfigType: double.
pub const CONFIG_TYPE_DOUBLE: i8 = 6;
/// DescribeConfigs v3+ ConfigType: list.
pub const CONFIG_TYPE_LIST: i8 = 7;
/// DescribeConfigs v3+ ConfigType: class.
pub const CONFIG_TYPE_CLASS: i8 = 8;
/// DescribeConfigs v3+ ConfigType: password.
pub const CONFIG_TYPE_PASSWORD: i8 = 9;

/// Java `ConfigEntry.ConfigSource` (DescribeConfigs ConfigSource on the wire).
///
/// Wire ids match `DescribeConfigsResponse.ConfigSource.id`, not Java enum
/// ordinals (`DYNAMIC_TOPIC_CONFIG` is `1`, not `0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum ConfigSource {
    /// Java `UNKNOWN` (wire `0`).
    Unknown = CONFIG_SOURCE_UNKNOWN,
    /// Java `DYNAMIC_TOPIC_CONFIG` (wire `1`).
    DynamicTopic = CONFIG_SOURCE_DYNAMIC_TOPIC,
    /// Java `DYNAMIC_BROKER_CONFIG` (wire `2`).
    DynamicBroker = CONFIG_SOURCE_DYNAMIC_BROKER,
    /// Java `DYNAMIC_DEFAULT_BROKER_CONFIG` (wire `3`).
    DynamicDefaultBroker = CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
    /// Java `STATIC_BROKER_CONFIG` (wire `4`).
    StaticBroker = CONFIG_SOURCE_STATIC_BROKER,
    /// Java `DEFAULT_CONFIG` (wire `5`).
    Default = CONFIG_SOURCE_DEFAULT,
    /// Java `DYNAMIC_BROKER_LOGGER_CONFIG` (wire `6`).
    DynamicBrokerLogger = CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
    /// Java `DYNAMIC_CLIENT_METRICS_CONFIG` (wire `7`).
    DynamicClientMetrics = CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS,
    /// Java `DYNAMIC_GROUP_CONFIG` (wire `8`).
    DynamicGroup = CONFIG_SOURCE_DYNAMIC_GROUP,
}

impl ConfigSource {
    /// Java `DescribeConfigsResponse.ConfigSource.forId` (out of range is
    /// [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            CONFIG_SOURCE_UNKNOWN => Self::Unknown,
            CONFIG_SOURCE_DYNAMIC_TOPIC => Self::DynamicTopic,
            CONFIG_SOURCE_DYNAMIC_BROKER => Self::DynamicBroker,
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER => Self::DynamicDefaultBroker,
            CONFIG_SOURCE_STATIC_BROKER => Self::StaticBroker,
            CONFIG_SOURCE_DEFAULT => Self::Default,
            CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER => Self::DynamicBrokerLogger,
            CONFIG_SOURCE_DYNAMIC_CLIENT_METRICS => Self::DynamicClientMetrics,
            CONFIG_SOURCE_DYNAMIC_GROUP => Self::DynamicGroup,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigSource> for i8 {
    fn from(source: ConfigSource) -> Self {
        source as i8
    }
}

/// Java `ConfigEntry.ConfigType` (DescribeConfigs ConfigType on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum ConfigType {
    /// Java `UNKNOWN` (wire `0`; below v3).
    Unknown = CONFIG_TYPE_UNKNOWN,
    /// Java `BOOLEAN`.
    Boolean = CONFIG_TYPE_BOOLEAN,
    /// Java `STRING`.
    String = CONFIG_TYPE_STRING,
    /// Java `INT`.
    Int = CONFIG_TYPE_INT,
    /// Java `SHORT`.
    Short = CONFIG_TYPE_SHORT,
    /// Java `LONG`.
    Long = CONFIG_TYPE_LONG,
    /// Java `DOUBLE`.
    Double = CONFIG_TYPE_DOUBLE,
    /// Java `LIST`.
    List = CONFIG_TYPE_LIST,
    /// Java `CLASS`.
    Class = CONFIG_TYPE_CLASS,
    /// Java `PASSWORD`.
    Password = CONFIG_TYPE_PASSWORD,
}

impl ConfigType {
    /// Java `DescribeConfigsResponse.ConfigType.forId` (out of range is
    /// [`Self::Unknown`]).
    #[must_use]
    pub const fn from_id(id: i8) -> Self {
        match id {
            CONFIG_TYPE_UNKNOWN => Self::Unknown,
            CONFIG_TYPE_BOOLEAN => Self::Boolean,
            CONFIG_TYPE_STRING => Self::String,
            CONFIG_TYPE_INT => Self::Int,
            CONFIG_TYPE_SHORT => Self::Short,
            CONFIG_TYPE_LONG => Self::Long,
            CONFIG_TYPE_DOUBLE => Self::Double,
            CONFIG_TYPE_LIST => Self::List,
            CONFIG_TYPE_CLASS => Self::Class,
            CONFIG_TYPE_PASSWORD => Self::Password,
            _ => Self::Unknown,
        }
    }
}

impl From<ConfigType> for i8 {
    fn from(ty: ConfigType) -> Self {
        ty as i8
    }
}

/// Manual replica assignment for one CreateTopics partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAssignment {
    /// Partition index.
    pub partition_index: i32,
    /// Replica broker ids.
    pub broker_ids: Vec<i32>,
}

/// Topic config key/value for CreateTopics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfig {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Config value, or `None` when unset.
    pub value: Option<String>,
}

/// One topic in a CreateTopics request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partition count, or `-1` to use cluster default.
    pub num_partitions: i32,
    /// Replication factor, or `-1` to use cluster default.
    pub replication_factor: i16,
    /// Manual replica assignments. Empty means broker default.
    pub assignments: Vec<ReplicaAssignment>,
    /// Topic configs to set at create time.
    pub configs: Vec<TopicConfig>,
}

/// CreateTopics request body (classic v0–4; flexible v5–v7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsRequest {
    /// Topics in this request or response.
    pub topics: Vec<CreatableTopic>,
    /// Broker-side operation timeout in milliseconds.
    pub timeout_ms: i32,
    /// When true, validate without applying the change.
    pub validate_only: bool,
}

/// Per-topic result of CreateTopics / DeleteTopics.
///
/// Java `CreateTopicsResult.TopicMetadataAndConfig` plus the per-topic
/// error. DeleteTopics leaves [`Self::num_partitions`],
/// [`Self::replication_factor`], and [`Self::configs`] omitted (`-1` /
/// empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicResult {
    /// Topic name.
    pub name: String,
    /// Per-topic error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Topic UUID from CreateTopics v7 / DeleteTopics v6. Zero when omitted.
    pub topic_id: [u8; 16],
    /// Partition count from CreateTopics v5+, or `-1` when omitted.
    pub num_partitions: i32,
    /// Replication factor from CreateTopics v5+, or `-1` when omitted.
    pub replication_factor: i16,
    /// Topic configs from CreateTopics v5+ (KIP-525).
    pub configs: Vec<CreatedTopicConfig>,
}

impl TopicResult {
    /// Name plus per-topic error. CreateTopics v5+ fields are omitted.
    #[must_use]
    pub fn new(name: impl Into<String>, error_code: i16, error_message: Option<String>) -> Self {
        Self {
            name: name.into(),
            error_code,
            error_message,
            topic_id: [0; 16],
            num_partitions: -1,
            replication_factor: -1,
            configs: Vec::new(),
        }
    }

    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Per-topic error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Java `CreateTopicsResult.numPartitions` (`-1` when omitted).
    #[must_use]
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Java `CreateTopicsResult.replicationFactor` (`-1` when omitted).
    #[must_use]
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// CreateTopics v5+ Configs (KIP-525). Empty on DeleteTopics and
    /// on CreateTopics below v5.
    #[must_use]
    pub fn configs(&self) -> &[CreatedTopicConfig] {
        &self.configs
    }

    /// Java `CreateTopicsResult.config`.
    ///
    /// Builds [`Config`] from CreateTopics v5+ Configs (KIP-525). Type and
    /// documentation on each entry are unknown (Java `null`). Empty when
    /// the broker omitted Configs or the topic failed.
    #[must_use]
    pub fn config(&self) -> Config {
        Config::new(self.configs.iter().map(CreatedTopicConfig::to_config_entry))
    }
}

/// One config entry on a CreateTopics v5+ response (KIP-525).
///
/// [`Debug`] redacts [`Self::value`] when [`Self::is_sensitive`] is set
/// (Java `ConfigEntry.toString`).
#[derive(Clone, PartialEq, Eq)]
pub struct CreatedTopicConfig {
    /// Config key.
    pub name: String,
    /// Config value, or `None` when unset / sensitive.
    pub value: Option<String>,
    /// True when the broker will not change this key.
    pub read_only: bool,
    /// Kafka config source (`CONFIG_SOURCE_DYNAMIC_TOPIC`, …).
    pub config_source: i8,
    /// True when the value is redacted.
    pub is_sensitive: bool,
}

impl CreatedTopicConfig {
    /// Config key.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Config value, or `None` when unset / sensitive on the wire.
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Java `ConfigEntry.isReadOnly()`.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Java `ConfigEntry.source()`.
    #[must_use]
    pub fn source(&self) -> ConfigSource {
        ConfigSource::from_id(self.config_source)
    }

    /// Java `ConfigEntry.isSensitive()`.
    #[must_use]
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    fn to_config_entry(&self) -> ConfigEntry {
        ConfigEntry {
            name: self.name.clone(),
            value: self.value.clone(),
            read_only: self.read_only,
            source: self.config_source,
            is_sensitive: self.is_sensitive,
            synonyms: Vec::new(),
            config_type: CONFIG_TYPE_UNKNOWN,
            documentation: None,
        }
    }
}

impl fmt::Debug for CreatedTopicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java `ConfigEntry.toString` prints `Redacted` when `isSensitive`.
        let value = if self.is_sensitive {
            Some("Redacted")
        } else {
            self.value.as_deref()
        };
        f.debug_struct("CreatedTopicConfig")
            .field("name", &self.name)
            .field("value", &value)
            .field("read_only", &self.read_only)
            .field("config_source", &self.config_source)
            .field("is_sensitive", &self.is_sensitive)
            .finish()
    }
}

/// One resource in a DescribeConfigs request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResource {
    /// Kafka resource type (`CONFIG_RESOURCE_TOPIC`, …).
    pub resource_type: i8,
    /// Resource name (topic, broker id, …).
    pub name: String,
    /// Keys to return, or `None` for every key on the resource.
    pub keys: Option<Vec<String>>,
}

/// A config key that is an alias or parent of a [`ConfigEntry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    /// Config key.
    pub name: String,
    /// Config value, or `None` when unset.
    pub value: Option<String>,
    /// Kafka config source (`CONFIG_SOURCE_DYNAMIC_TOPIC`, …).
    pub source: i8,
}

impl ConfigSynonym {
    /// Java `ConfigSynonym.name()`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `ConfigSynonym.value()` (`None` is Java `null`).
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Java `ConfigSynonym.source()`.
    #[must_use]
    pub fn source(&self) -> ConfigSource {
        ConfigSource::from_id(self.source)
    }
}

/// One key from DescribeConfigs.
///
/// [`Debug`] redacts [`Self::value`] when [`Self::is_sensitive`] is set
/// (Java `ConfigEntry.toString`).
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Config key.
    pub name: String,
    /// Config value, or `None` when unset.
    pub value: Option<String>,
    /// When true, the broker will not apply a write to this key.
    pub read_only: bool,
    /// Kafka config source (`CONFIG_SOURCE_DYNAMIC_TOPIC`, …).
    pub source: i8,
    /// When true, the value is redacted by the broker.
    pub is_sensitive: bool,
    /// Synonyms / parents of this key.
    pub synonyms: Vec<ConfigSynonym>,
    /// Kafka config type (`CONFIG_TYPE_STRING`, …). `0` below v3.
    pub config_type: i8,
    /// Broker documentation, when present (v3+ IncludeDocumentation).
    pub documentation: Option<String>,
}

impl ConfigEntry {
    /// Java `ConfigEntry(String, String)` for [`Config`]. `value` `None` is
    /// Java `null`. Source is [`ConfigSource::Unknown`]; type is
    /// [`ConfigType::Unknown`].
    #[must_use]
    pub fn new(name: impl Into<String>, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            value,
            read_only: false,
            source: CONFIG_SOURCE_UNKNOWN,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: CONFIG_TYPE_UNKNOWN,
            documentation: None,
        }
    }

    /// Java `ConfigEntry.source()`.
    #[must_use]
    pub fn source(&self) -> ConfigSource {
        ConfigSource::from_id(self.source)
    }

    /// Java `ConfigEntry.type()`.
    #[must_use]
    pub fn config_type(&self) -> ConfigType {
        ConfigType::from_id(self.config_type)
    }

    /// Java `ConfigEntry.isDefault()`.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.source == CONFIG_SOURCE_DEFAULT
    }

    /// Java `ConfigEntry.name()`.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Java `ConfigEntry.value()` (`None` is Java `null`).
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Java `ConfigEntry.isSensitive()`.
    #[must_use]
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    /// Java `ConfigEntry.isReadOnly()`.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Java `ConfigEntry.synonyms()`.
    #[must_use]
    pub fn synonyms(&self) -> &[ConfigSynonym] {
        &self.synonyms
    }

    /// Java `ConfigEntry.documentation()` (`None` is Java `null`).
    #[must_use]
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
    }
}

impl fmt::Debug for ConfigEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java `ConfigEntry.toString` prints `Redacted` when `isSensitive`.
        let value = if self.is_sensitive {
            Some("Redacted")
        } else {
            self.value.as_deref()
        };
        f.debug_struct("ConfigEntry")
            .field("name", &self.name)
            .field("value", &value)
            .field("read_only", &self.read_only)
            .field("source", &self.source)
            .field("is_sensitive", &self.is_sensitive)
            .field("synonyms", &self.synonyms)
            .field("config_type", &self.config_type)
            .field("documentation", &self.documentation)
            .finish()
    }
}

/// Java `org.apache.kafka.clients.admin.Config`: entries for one resource.
///
/// [`Self::new`] is Java `Config(Collection)`. [`Self::entries`] /
/// [`Self::get`] are Java `entries()` / `get(String)`. Use with
/// [`crate::Admin::alter_configs_with`] (`alterConfigs(Map)` one resource)
/// and [`DescribeConfigsResult::config`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    entries: Vec<ConfigEntry>,
}

impl Config {
    /// Java `Config(Collection)`.
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = ConfigEntry>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
        }
    }

    /// Java `Config.entries()`.
    #[must_use]
    pub fn entries(&self) -> &[ConfigEntry] {
        &self.entries
    }

    /// Java `Config.get(String)`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.iter().find(|e| e.name == name)
    }
}

/// Per-resource result of DescribeConfigs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    /// Per-resource error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Kafka resource type (`CONFIG_RESOURCE_TOPIC`, …).
    pub resource_type: i8,
    /// Resource name.
    pub name: String,
    /// Config keys on this resource.
    pub entries: Vec<ConfigEntry>,
}

impl DescribeConfigsResult {
    /// Java `Config` for this resource (`describeConfigs` result value).
    #[must_use]
    pub fn config(&self) -> Config {
        Config::new(self.entries.iter().cloned())
    }

    /// Per-resource error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Resource name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Config keys on this resource.
    #[must_use]
    pub fn entries(&self) -> &[ConfigEntry] {
        &self.entries
    }
}

fn put_i32_array(buf: &mut BytesMut, flexible: bool, items: &[i32]) -> crate::error::Result<()> {
    buf::put_array_len(buf, flexible, Some(items.len()))?;
    for v in items {
        buf.put_i32(*v);
    }
    Ok(())
}

fn get_i32_array<B: Buf>(buf: &mut B, flexible: bool) -> Result<Vec<i32>> {
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(out)
}

fn get_string_array<B: Buf>(buf: &mut B, flexible: bool) -> Result<Option<Vec<String>>> {
    let n = buf::get_array_len(buf, flexible)?;
    let Some(n) = n else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            buf::get_string(buf, flexible)?
                .ok_or_else(|| Error::protocol("null string in array"))?,
        );
    }
    Ok(Some(out))
}

fn put_string_array(
    buf: &mut BytesMut,
    flexible: bool,
    items: Option<&[String]>,
) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(items) => {
            buf::put_array_len(buf, flexible, Some(items.len()))?;
            for s in items {
                buf::put_string(buf, flexible, Some(s))?;
            }
        }
    }
    Ok(())
}

/// `true` when CreateTopics `version` is flexible.
///
/// v0–v4 are classic. v5 is the first flexible version (KIP-525 configs on
/// the response). v6 is the same layout (KIP-599 THROTTLING_QUOTA_EXCEEDED).
/// v7 adds TopicId UUID on the response (KIP-516). Kafka 4.0
/// `validVersions` is `2-7` (v0–v1 removed). This crate speaks 0–7.
/// v8+ is not spoken.
fn create_topics_flexible(version: i16) -> Result<bool> {
    match version {
        0..=4 => Ok(false),
        5..=7 => Ok(true),
        other => Err(Error::protocol(format!(
            "CreateTopics version {other} is not implemented"
        ))),
    }
}

/// CreateTopics v0–7 (classic through v4; flexible from v5).
pub fn encode_create_topics_request(
    buf: &mut BytesMut,
    version: i16,
    req: &CreateTopicsRequest,
) -> crate::error::Result<()> {
    let flexible = create_topics_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(req.topics.len()))?;
    for t in &req.topics {
        buf::put_string(buf, flexible, Some(&t.name))?;
        buf.put_i32(t.num_partitions);
        buf.put_i16(t.replication_factor);
        buf::put_array_len(buf, flexible, Some(t.assignments.len()))?;
        for a in &t.assignments {
            buf.put_i32(a.partition_index);
            put_i32_array(buf, flexible, &a.broker_ids)?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        buf::put_array_len(buf, flexible, Some(t.configs.len()))?;
        for c in &t.configs {
            buf::put_string(buf, flexible, Some(&c.name))?;
            buf::put_string(buf, flexible, c.value.as_deref())?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_i32(req.timeout_ms);
    if version >= 1 {
        buf.put_u8(u8::from(req.validate_only));
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreateTopics request.
pub fn decode_create_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<CreateTopicsRequest> {
    let flexible = create_topics_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let num_partitions = buf::get_i32(buf)?;
        let replication_factor = buf::get_i16(buf)?;
        let an = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut assignments = Vec::with_capacity(an);
        for _ in 0..an {
            let partition_index = buf::get_i32(buf)?;
            let broker_ids = get_i32_array(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            assignments.push(ReplicaAssignment {
                partition_index,
                broker_ids,
            });
        }
        let cn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut configs = Vec::with_capacity(cn);
        for _ in 0..cn {
            let name = buf::get_string(buf, flexible)?.unwrap_or_default();
            let value = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            configs.push(TopicConfig { name, value });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(CreatableTopic {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        });
    }
    let timeout_ms = buf::get_i32(buf)?;
    let validate_only = if version >= 1 {
        buf::get_bool(buf)?
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(CreateTopicsRequest {
        topics,
        timeout_ms,
        validate_only,
    })
}

/// Encode a CreateTopics response.
pub fn encode_create_topics_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    let flexible = create_topics_flexible(version)?;
    if version >= 2 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf::put_string(buf, flexible, Some(&r.name))?;
        if version >= 7 {
            buf.extend_from_slice(&r.topic_id);
        }
        buf.put_i16(r.error_code);
        if version >= 1 {
            buf::put_string(buf, flexible, r.error_message.as_deref())?;
        }
        if version >= 5 {
            buf.put_i32(r.num_partitions);
            buf.put_i16(r.replication_factor);
            buf::put_array_len(buf, true, Some(r.configs.len()))?;
            for c in &r.configs {
                buf::put_compact_string(buf, Some(&c.name))?;
                buf::put_compact_string(buf, c.value.as_deref())?;
                buf.put_u8(u8::from(c.read_only));
                buf.put_i8(c.config_source);
                buf.put_u8(u8::from(c.is_sensitive));
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreateTopics response.
pub fn decode_create_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TopicResult>> {
    let flexible = create_topics_flexible(version)?;
    if version >= 2 {
        let _throttle = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let topic_id = if version >= 7 {
            buf::get_uuid(buf)?
        } else {
            [0u8; 16]
        };
        let error_code = buf::get_i16(buf)?;
        let error_message = if version >= 1 {
            buf::get_string(buf, flexible)?
        } else {
            None
        };
        let (num_partitions, replication_factor, configs) = if version >= 5 {
            let num_partitions = buf::get_i32(buf)?;
            let replication_factor = buf::get_i16(buf)?;
            let cn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut configs = Vec::with_capacity(cn);
            for _ in 0..cn {
                let cname = buf::get_compact_string(buf)?.unwrap_or_default();
                let value = buf::get_compact_string(buf)?;
                let read_only = buf::get_bool(buf)?;
                let config_source = buf::get_i8(buf)?;
                let is_sensitive = buf::get_bool(buf)?;
                buf::skip_tagged_fields(buf)?;
                configs.push(CreatedTopicConfig {
                    name: cname,
                    value,
                    read_only,
                    config_source,
                    is_sensitive,
                });
            }
            buf::skip_tagged_fields(buf)?;
            (num_partitions, replication_factor, configs)
        } else {
            (-1, -1, Vec::new())
        };
        out.push(TopicResult {
            name,
            error_code,
            error_message,
            topic_id,
            num_partitions,
            replication_factor,
            configs,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// `true` when DeleteTopics `version` is flexible.
///
/// v0–v3 are classic TopicNames. v4 is the first flexible version.
/// v5 is the same request (ErrorMessage on the response, KIP-599).
/// v6 replaces TopicNames with Topics of Name + TopicId (KIP-516).
/// Kafka 4.0 `validVersions` is `1-6` (v0 removed). This crate speaks
/// 0–6. v7+ is not spoken.
fn delete_topics_flexible(version: i16) -> Result<bool> {
    match version {
        0..=3 => Ok(false),
        4..=6 => Ok(true),
        other => Err(Error::protocol(format!(
            "DeleteTopics version {other} is not implemented"
        ))),
    }
}

/// One topic in a DeleteTopics v6 Topics array (KIP-516).
///
/// Java `deleteTopics(Collection<String>)` sends [`Self::by_name`]
/// (Name set, TopicId zero). Java `deleteTopics(TopicCollection.ofTopicIds)`
/// sends [`Self::by_id`] (Name null, TopicId set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicState {
    /// Topic name, or `None` when deleting by TopicId.
    pub name: Option<String>,
    /// Topic UUID. Zero when deleting by name.
    pub topic_id: [u8; 16],
}

impl DeleteTopicState {
    /// Name-based delete (TopicId zero).
    #[must_use]
    pub fn by_name(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            topic_id: [0; 16],
        }
    }

    /// Id-based delete (Name null).
    #[must_use]
    pub fn by_id(topic_id: [u8; 16]) -> Self {
        Self {
            name: None,
            topic_id,
        }
    }
}

/// DeleteTopics v0–6 (classic through v3; flexible from v4).
///
/// Name-based: v6 sends Topics of Name + zero TopicId (Java
/// `deleteTopics(Collection<String>)`). For TopicId deletes, use
/// [`encode_delete_topics_states_request`].
pub fn encode_delete_topics_request(
    buf: &mut BytesMut,
    version: i16,
    names: &[String],
    timeout_ms: i32,
) -> crate::error::Result<()> {
    let topics: Vec<DeleteTopicState> = names
        .iter()
        .map(|name| DeleteTopicState::by_name(name.clone()))
        .collect();
    encode_delete_topics_states_request(buf, version, &topics, timeout_ms)
}

/// DeleteTopics Topics of N (v6 Name + TopicId; v0–5 TopicNames).
///
/// Java `deleteTopics(TopicCollection.ofTopicIds)` sends Name null and
/// TopicId set. Brokers below v6 have no TopicId field: Name-only.
pub fn encode_delete_topics_states_request(
    buf: &mut BytesMut,
    version: i16,
    topics: &[DeleteTopicState],
    timeout_ms: i32,
) -> crate::error::Result<()> {
    let flexible = delete_topics_flexible(version)?;
    if version >= 6 {
        buf::put_array_len(buf, true, Some(topics.len()))?;
        for t in topics {
            buf::put_compact_string(buf, t.name.as_deref())?;
            buf.extend_from_slice(&t.topic_id);
            buf::put_empty_tagged_fields(buf);
        }
    } else {
        buf::put_array_len(buf, flexible, Some(topics.len()))?;
        for t in topics {
            buf::put_string(buf, flexible, t.name.as_deref())?;
        }
    }
    buf.put_i32(timeout_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteTopics request: `(names, timeout_ms)`.
///
/// v6 Topics entries with a null Name are skipped (id-based deletes).
/// See [`decode_delete_topics_states_request`] for every Topics entry.
pub fn decode_delete_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, i32)> {
    let (topics, timeout_ms) = decode_delete_topics_states_request(buf, version)?;
    let names = topics
        .into_iter()
        .filter_map(|t| t.name.filter(|n| !n.is_empty()))
        .collect();
    Ok((names, timeout_ms))
}

/// Decode DeleteTopics: every topic (Name and/or TopicId) plus TimeoutMs.
pub fn decode_delete_topics_states_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<DeleteTopicState>, i32)> {
    let flexible = delete_topics_flexible(version)?;
    let mut topics = Vec::new();
    if version >= 6 {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        topics.reserve(n);
        for _ in 0..n {
            let name = buf::get_compact_string(buf)?;
            let topic_id = buf::get_uuid(buf)?;
            buf::skip_tagged_fields(buf)?;
            topics.push(DeleteTopicState { name, topic_id });
        }
    } else {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        topics.reserve(n);
        for _ in 0..n {
            topics.push(DeleteTopicState {
                name: buf::get_string(buf, flexible)?,
                topic_id: [0; 16],
            });
        }
    }
    let timeout_ms = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, timeout_ms))
}

/// Encode a DeleteTopics response.
pub fn encode_delete_topics_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    let flexible = delete_topics_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        if version >= 6 {
            buf::put_compact_string(
                buf,
                if r.name.is_empty() {
                    None
                } else {
                    Some(r.name.as_str())
                },
            )?;
            buf.extend_from_slice(&r.topic_id);
        } else {
            buf::put_string(buf, flexible, Some(&r.name))?;
        }
        buf.put_i16(r.error_code);
        if version >= 5 {
            buf::put_string(buf, true, r.error_message.as_deref())?;
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteTopics response.
pub fn decode_delete_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TopicResult>> {
    let flexible = delete_topics_flexible(version)?;
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let (name, topic_id) = if version >= 6 {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let topic_id = buf::get_uuid(buf)?;
            (name, topic_id)
        } else {
            (
                buf::get_string(buf, flexible)?.unwrap_or_default(),
                [0u8; 16],
            )
        };
        let error_code = buf::get_i16(buf)?;
        let error_message = if version >= 5 {
            buf::get_string(buf, true)?
        } else {
            None
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(TopicResult {
            name,
            error_code,
            error_message,
            topic_id,
            num_partitions: -1,
            replication_factor: -1,
            configs: Vec::new(),
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// `true` when DescribeConfigs `version` is flexible.
///
/// v0–v3 are classic. v1 adds IncludeSynonyms / ConfigSource / Synonyms.
/// v2 is the same layout as v1 (quota throttle timing). v3 adds
/// IncludeDocumentation, ConfigType, and Documentation (KIP-226).
/// v4 is the first flexible version. Kafka 4.0 `validVersions` is
/// `1-4` (v0 removed). This crate speaks 0–4. v5+ is not spoken.
fn describe_configs_flexible(version: i16) -> Result<bool> {
    match version {
        0..=3 => Ok(false),
        4 => Ok(true),
        other => Err(Error::protocol(format!(
            "DescribeConfigs version {other} is not implemented"
        ))),
    }
}

/// DescribeConfigs v0–4 (classic through v3; flexible from v4).
pub fn encode_describe_configs_request(
    buf: &mut BytesMut,
    version: i16,
    resources: &[DescribeConfigsResource],
    include_synonyms: bool,
    include_documentation: bool,
) -> crate::error::Result<()> {
    let flexible = describe_configs_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(resources.len()))?;
    for r in resources {
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        put_string_array(buf, flexible, r.keys.as_deref())?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if version >= 1 {
        buf.put_u8(u8::from(include_synonyms));
    }
    if version >= 3 {
        buf.put_u8(u8::from(include_documentation));
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeConfigs request: `(resources, include_synonyms, include_documentation)`.
pub fn decode_describe_configs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<DescribeConfigsResource>, bool, bool)> {
    let flexible = describe_configs_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let keys = get_string_array(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        resources.push(DescribeConfigsResource {
            resource_type,
            name,
            keys,
        });
    }
    let include_synonyms = if version >= 1 {
        buf::get_bool(buf)?
    } else {
        false
    };
    let include_documentation = if version >= 3 {
        buf::get_bool(buf)?
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((resources, include_synonyms, include_documentation))
}

/// Encode a DescribeConfigs response.
pub fn encode_describe_configs_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[DescribeConfigsResult],
) -> crate::error::Result<()> {
    let flexible = describe_configs_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        buf::put_array_len(buf, flexible, Some(r.entries.len()))?;
        for e in &r.entries {
            buf::put_string(buf, flexible, Some(&e.name))?;
            buf::put_string(buf, flexible, e.value.as_deref())?;
            buf.put_u8(u8::from(e.read_only));
            if version == 0 {
                buf.put_u8(u8::from(e.source == CONFIG_SOURCE_DEFAULT));
            } else {
                buf.put_i8(e.source);
            }
            buf.put_u8(u8::from(e.is_sensitive));
            if version >= 1 {
                buf::put_array_len(buf, flexible, Some(e.synonyms.len()))?;
                for s in &e.synonyms {
                    buf::put_string(buf, flexible, Some(&s.name))?;
                    buf::put_string(buf, flexible, s.value.as_deref())?;
                    buf.put_i8(s.source);
                    if flexible {
                        buf::put_empty_tagged_fields(buf);
                    }
                }
            }
            if version >= 3 {
                buf.put_i8(e.config_type);
                buf::put_string(buf, flexible, e.documentation.as_deref())?;
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeConfigs response.
pub fn decode_describe_configs_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DescribeConfigsResult>> {
    let flexible = describe_configs_flexible(version)?;
    let _throttle = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let en = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut entries = Vec::with_capacity(en);
        for _ in 0..en {
            let ename = buf::get_string(buf, flexible)?.unwrap_or_default();
            let value = buf::get_string(buf, flexible)?;
            let read_only = buf::get_bool(buf)?;
            let source = if version == 0 {
                if buf::get_bool(buf)? {
                    CONFIG_SOURCE_DEFAULT
                } else {
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                }
            } else {
                buf::get_i8(buf)?
            };
            let is_sensitive = buf::get_bool(buf)?;
            let mut synonyms = Vec::new();
            if version >= 1 {
                let sn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                synonyms.reserve(sn);
                for _ in 0..sn {
                    let sname = buf::get_string(buf, flexible)?.unwrap_or_default();
                    let svalue = buf::get_string(buf, flexible)?;
                    let ssource = buf::get_i8(buf)?;
                    if flexible {
                        buf::skip_tagged_fields(buf)?;
                    }
                    synonyms.push(ConfigSynonym {
                        name: sname,
                        value: svalue,
                        source: ssource,
                    });
                }
            }
            let (config_type, documentation) = if version >= 3 {
                (buf::get_i8(buf)?, buf::get_string(buf, flexible)?)
            } else {
                (CONFIG_TYPE_UNKNOWN, None)
            };
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            entries.push(ConfigEntry {
                name: ename,
                value,
                read_only,
                source,
                is_sensitive,
                synonyms,
                config_type,
                documentation,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(DescribeConfigsResult {
            error_code,
            error_message,
            resource_type,
            name,
            entries,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// Incremental AlterConfigs op: set a key (Java `AlterConfigOp.OpType.SET`).
pub const ALTER_CONFIG_SET: i8 = 0;
/// Incremental AlterConfigs op: delete a key (Java `AlterConfigOp.OpType.DELETE`).
pub const ALTER_CONFIG_DELETE: i8 = 1;
/// Incremental AlterConfigs op: append to a list (Java `AlterConfigOp.OpType.APPEND`).
pub const ALTER_CONFIG_APPEND: i8 = 2;
/// Incremental AlterConfigs op: subtract from a list (Java `AlterConfigOp.OpType.SUBTRACT`).
pub const ALTER_CONFIG_SUBTRACT: i8 = 3;

/// Java `AlterConfigOp.OpType` (IncrementalAlterConfigs ConfigOperation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum AlterConfigOpType {
    /// Java `SET` (wire `0`).
    Set = ALTER_CONFIG_SET,
    /// Java `DELETE` (wire `1`).
    Delete = ALTER_CONFIG_DELETE,
    /// Java `APPEND` (wire `2`).
    Append = ALTER_CONFIG_APPEND,
    /// Java `SUBTRACT` (wire `3`).
    Subtract = ALTER_CONFIG_SUBTRACT,
}

impl AlterConfigOpType {
    /// Java `AlterConfigOp.OpType.forId` (`None` when the id is unknown).
    #[must_use]
    pub const fn from_id(id: i8) -> Option<Self> {
        match id {
            ALTER_CONFIG_SET => Some(Self::Set),
            ALTER_CONFIG_DELETE => Some(Self::Delete),
            ALTER_CONFIG_APPEND => Some(Self::Append),
            ALTER_CONFIG_SUBTRACT => Some(Self::Subtract),
            _ => None,
        }
    }
}

impl From<AlterConfigOpType> for i8 {
    fn from(op: AlterConfigOpType) -> Self {
        op as i8
    }
}

/// `true` when CreatePartitions `version` is flexible.
///
/// v0–v1 are classic (v1 is quota-throttle timing only). v2 is the
/// first flexible version. v3 is the same layout (KIP-599
/// THROTTLING_QUOTA_EXCEEDED). Kafka 4.0 `validVersions` is `0-3`.
/// This crate speaks 0–3. v4+ is not spoken.
fn create_partitions_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        2..=3 => Ok(true),
        other => Err(Error::protocol(format!(
            "CreatePartitions version {other} is not implemented"
        ))),
    }
}

/// One CreatePartitions topic (name, new total count, replica assignments).
///
/// `assignments = None` is a null Assignments array: the broker assigns
/// replicas (Java `NewPartitions.increaseTo(int)`). `Some` is Java
/// `increaseTo(int, List<List<Integer>>)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopic {
    /// Topic name.
    pub name: String,
    /// New total partition count (not a delta).
    pub count: i32,
    /// Replica assignments for the new partitions, or `None` (null).
    pub assignments: Option<Vec<Vec<i32>>>,
}

impl CreatePartitionsTopic {
    /// Topic `name` at `count` partitions; broker assigns replicas.
    #[must_use]
    pub fn new(name: impl Into<String>, count: i32) -> Self {
        Self {
            name: name.into(),
            count,
            assignments: None,
        }
    }
}

/// Topics, TimeoutMs, and ValidateOnly from a CreatePartitions request.
type CreatePartitionsDecoded = (Vec<CreatePartitionsTopic>, i32, bool);

/// CreatePartitions v0–3 (classic through v1; flexible from v2).
pub fn encode_create_partitions_request(
    buf: &mut BytesMut,
    version: i16,
    topics: &[CreatePartitionsTopic],
    timeout_ms: i32,
    validate_only: bool,
) -> crate::error::Result<()> {
    let flexible = create_partitions_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.name))?;
        buf.put_i32(t.count);
        match &t.assignments {
            None => buf::put_array_len(buf, flexible, None)?,
            Some(assignments) => {
                buf::put_array_len(buf, flexible, Some(assignments.len()))?;
                for brokers in assignments {
                    put_i32_array(buf, flexible, brokers)?;
                    if flexible {
                        buf::put_empty_tagged_fields(buf);
                    }
                }
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_i32(timeout_ms);
    buf.put_u8(u8::from(validate_only));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreatePartitions request: topics, TimeoutMs, and
/// `validate_only`.
pub fn decode_create_partitions_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<CreatePartitionsDecoded> {
    let flexible = create_partitions_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let count = buf::get_i32(buf)?;
        let assignments = match buf::get_array_len(buf, flexible)? {
            None => None,
            Some(an) => {
                let mut assignments = Vec::with_capacity(an);
                for _ in 0..an {
                    let brokers = get_i32_array(buf, flexible)?;
                    if flexible {
                        buf::skip_tagged_fields(buf)?;
                    }
                    assignments.push(brokers);
                }
                Some(assignments)
            }
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(CreatePartitionsTopic {
            name,
            count,
            assignments,
        });
    }
    let timeout_ms = buf::get_i32(buf)?;
    let validate_only = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, timeout_ms, validate_only))
}

/// Encode a CreatePartitions response.
pub fn encode_create_partitions_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[TopicResult],
) -> crate::error::Result<()> {
    let flexible = create_partitions_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf::put_string(buf, flexible, Some(&r.name))?;
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreatePartitions response.
pub fn decode_create_partitions_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<TopicResult>> {
    let flexible = create_partitions_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(TopicResult::new(name, error_code, error_message));
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// One incremental config change (`AlterConfigOp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfig {
    /// Config key.
    pub name: String,
    /// `ALTER_CONFIG_SET`, `ALTER_CONFIG_DELETE`, `ALTER_CONFIG_APPEND`,
    /// or `ALTER_CONFIG_SUBTRACT`.
    pub op: i8,
    /// New value. `None` for delete.
    pub value: Option<String>,
}

impl AlterConfig {
    /// Set `name` to `value` (`ALTER_CONFIG_SET`).
    #[must_use]
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: ALTER_CONFIG_SET,
            value: Some(value.into()),
        }
    }

    /// Delete `name` (`ALTER_CONFIG_DELETE`).
    #[must_use]
    pub fn delete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: ALTER_CONFIG_DELETE,
            value: None,
        }
    }

    /// Append comma-separated `value` to a LIST config (`ALTER_CONFIG_APPEND`).
    ///
    /// Java `AlterConfigOp.OpType.APPEND`. Duplicates already in the
    /// current value are not added again.
    #[must_use]
    pub fn append(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: ALTER_CONFIG_APPEND,
            value: Some(value.into()),
        }
    }

    /// Remove comma-separated `value` from a LIST config
    /// (`ALTER_CONFIG_SUBTRACT`).
    ///
    /// Java `AlterConfigOp.OpType.SUBTRACT`. Removing every entry leaves
    /// an empty list; it does not revert to the default.
    #[must_use]
    pub fn subtract(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: ALTER_CONFIG_SUBTRACT,
            value: Some(value.into()),
        }
    }

    /// Java `AlterConfigOp(ConfigEntry, OpType)`.
    #[must_use]
    pub fn from_entry(entry: &ConfigEntry, op: AlterConfigOpType) -> Self {
        Self {
            name: entry.name.clone(),
            op: i8::from(op),
            value: entry.value.clone(),
        }
    }

    /// Java `AlterConfigOp.opType()`.
    #[must_use]
    pub fn op_type(&self) -> Option<AlterConfigOpType> {
        AlterConfigOpType::from_id(self.op)
    }

    /// Java `AlterConfigOp.configEntry()` (name and value; source unknown).
    #[must_use]
    pub fn config_entry(&self) -> ConfigEntry {
        ConfigEntry::new(self.name.clone(), self.value.clone())
    }
}

/// Java `AlterConfigOp`. Same wire as [`AlterConfig`].
pub type AlterConfigOp = AlterConfig;

/// One IncrementalAlterConfigs resource (Resources array element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterableResource {
    /// Resource type (`RESOURCE_TOPIC`, …).
    pub resource_type: i8,
    /// Resource name.
    pub name: String,
    /// Incremental ops for this resource.
    pub configs: Vec<AlterConfig>,
}

/// Per-resource IncrementalAlterConfigs result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResourceResult {
    /// Resource error, or `0`.
    pub error_code: i16,
    /// Resource error message.
    pub error_message: Option<String>,
    /// Resource type (`RESOURCE_TOPIC`, …).
    pub resource_type: i8,
    /// Resource name.
    pub name: String,
}

impl AlterConfigsResourceResult {
    /// Resource error, or `0`.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Resource error message.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Resource name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// `true` when IncrementalAlterConfigs `version` is flexible.
///
/// v0 is classic. v1 is the first flexible version. Kafka 4.0
/// `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken.
fn incremental_alter_configs_flexible(version: i16) -> Result<bool> {
    match version {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::protocol(format!(
            "IncrementalAlterConfigs version {other} is not implemented"
        ))),
    }
}

/// IncrementalAlterConfigs v0–1 (classic at v0; flexible from v1).
pub fn encode_incremental_alter_configs_request(
    buf: &mut BytesMut,
    version: i16,
    resource_type: i8,
    name: &str,
    configs: &[AlterConfig],
    validate_only: bool,
) -> crate::error::Result<()> {
    encode_incremental_alter_configs_resources_request(
        buf,
        version,
        &[AlterableResource {
            resource_type,
            name: name.to_string(),
            configs: configs.to_vec(),
        }],
        validate_only,
    )
}

/// IncrementalAlterConfigs Resources of N (Java `incrementalAlterConfigs(Map)`).
pub fn encode_incremental_alter_configs_resources_request(
    buf: &mut BytesMut,
    version: i16,
    resources: &[AlterableResource],
    validate_only: bool,
) -> crate::error::Result<()> {
    let flexible = incremental_alter_configs_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(resources.len()))?;
    for r in resources {
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        buf::put_array_len(buf, flexible, Some(r.configs.len()))?;
        for c in &r.configs {
            buf::put_string(buf, flexible, Some(&c.name))?;
            buf.put_i8(c.op);
            buf::put_string(buf, flexible, c.value.as_deref())?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_u8(u8::from(validate_only));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an IncrementalAlterConfigs request (first resource).
pub fn decode_incremental_alter_configs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, String, Vec<AlterConfig>, bool)> {
    let (resources, validate_only) =
        decode_incremental_alter_configs_resources_request(buf, version)?;
    let first = resources.into_iter().next();
    match first {
        Some(r) => Ok((r.resource_type, r.name, r.configs, validate_only)),
        None => Ok((0, String::new(), Vec::new(), validate_only)),
    }
}

/// Decode IncrementalAlterConfigs: every resource plus ValidateOnly.
pub fn decode_incremental_alter_configs_resources_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<AlterableResource>, bool)> {
    let flexible = incremental_alter_configs_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let cn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut configs = Vec::with_capacity(cn);
        for _ in 0..cn {
            let cname = buf::get_string(buf, flexible)?.unwrap_or_default();
            let op = buf::get_i8(buf)?;
            let value = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            configs.push(AlterConfig {
                name: cname,
                op,
                value,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        resources.push(AlterableResource {
            resource_type,
            name,
            configs,
        });
    }
    let validate_only = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((resources, validate_only))
}

/// Encode an IncrementalAlterConfigs response (one resource).
pub fn encode_incremental_alter_configs_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    name: &str,
) -> crate::error::Result<()> {
    encode_incremental_alter_configs_resource_results(
        buf,
        version,
        &[AlterConfigsResourceResult {
            error_code,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: name.to_string(),
        }],
    )
}

/// Encode IncrementalAlterConfigs Responses of N.
pub fn encode_incremental_alter_configs_resource_results(
    buf: &mut BytesMut,
    version: i16,
    results: &[AlterConfigsResourceResult],
) -> crate::error::Result<()> {
    let flexible = incremental_alter_configs_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an IncrementalAlterConfigs response (first resource error).
pub fn decode_incremental_alter_configs_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let results = decode_incremental_alter_configs_resource_results(buf, version)?;
    Ok(results.first().map(|r| r.error_code).unwrap_or(0))
}

/// Decode IncrementalAlterConfigs: every resource result.
pub fn decode_incremental_alter_configs_resource_results<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<AlterConfigsResourceResult>> {
    let flexible = incremental_alter_configs_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(AlterConfigsResourceResult {
            error_code,
            error_message,
            resource_type,
            name,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// `true` when AlterConfigs `version` is flexible.
///
/// v0–v1 are classic (v1 response adds ThrottleTimeMs). v2 is the first
/// flexible version. Kafka 4.0 `validVersions` is `0-2`. This crate
/// speaks 0–2. v3+ is not spoken.
fn alter_configs_flexible(version: i16) -> Result<bool> {
    match version {
        0 | 1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "AlterConfigs version {other} is not implemented"
        ))),
    }
}

/// One AlterConfigs resource (Resources array element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResource {
    /// Resource type (`RESOURCE_TOPIC`, …).
    pub resource_type: i8,
    /// Resource name.
    pub name: String,
    /// Replacement configs for this resource.
    pub configs: Vec<TopicConfig>,
}

/// Encode an AlterConfigs request (v0–2; classic through v1; flexible from v2).
pub fn encode_alter_configs_request(
    buf: &mut BytesMut,
    version: i16,
    resource_type: i8,
    name: &str,
    configs: &[TopicConfig],
    validate_only: bool,
) -> crate::error::Result<()> {
    encode_alter_configs_resources_request(
        buf,
        version,
        &[AlterConfigsResource {
            resource_type,
            name: name.to_string(),
            configs: configs.to_vec(),
        }],
        validate_only,
    )
}

/// AlterConfigs Resources of N (Java `alterConfigs(Map)`).
pub fn encode_alter_configs_resources_request(
    buf: &mut BytesMut,
    version: i16,
    resources: &[AlterConfigsResource],
    validate_only: bool,
) -> crate::error::Result<()> {
    let flexible = alter_configs_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(resources.len()))?;
    for r in resources {
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        buf::put_array_len(buf, flexible, Some(r.configs.len()))?;
        for c in &r.configs {
            buf::put_string(buf, flexible, Some(&c.name))?;
            buf::put_string(buf, flexible, c.value.as_deref())?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_u8(u8::from(validate_only));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterConfigs request (first resource).
pub fn decode_alter_configs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, String, Vec<TopicConfig>, bool)> {
    let (resources, validate_only) = decode_alter_configs_resources_request(buf, version)?;
    match resources.into_iter().next() {
        Some(r) => Ok((r.resource_type, r.name, r.configs, validate_only)),
        None => Ok((0, String::new(), Vec::new(), validate_only)),
    }
}

/// Decode AlterConfigs: every resource plus ValidateOnly.
pub fn decode_alter_configs_resources_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<AlterConfigsResource>, bool)> {
    let flexible = alter_configs_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let cn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut configs = Vec::with_capacity(cn);
        for _ in 0..cn {
            let cname = buf::get_string(buf, flexible)?.unwrap_or_default();
            let value = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            configs.push(TopicConfig { name: cname, value });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        resources.push(AlterConfigsResource {
            resource_type,
            name,
            configs,
        });
    }
    let validate_only = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((resources, validate_only))
}

/// Encode an AlterConfigs response (one resource).
///
/// ThrottleTimeMs is encoded from v1 (KIP-219). v2 adds compact arrays/
/// strings plus tagged fields.
pub fn encode_alter_configs_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    name: &str,
) -> crate::error::Result<()> {
    encode_alter_configs_resource_results(
        buf,
        version,
        &[AlterConfigsResourceResult {
            error_code,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: name.to_string(),
        }],
    )
}

/// Encode AlterConfigs Responses of N.
pub fn encode_alter_configs_resource_results(
    buf: &mut BytesMut,
    version: i16,
    results: &[AlterConfigsResourceResult],
) -> crate::error::Result<()> {
    let flexible = alter_configs_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        buf.put_i8(r.resource_type);
        buf::put_string(buf, flexible, Some(&r.name))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterConfigs response (first resource error).
pub fn decode_alter_configs_response<B: Buf>(buf: &mut B, version: i16) -> Result<i16> {
    let results = decode_alter_configs_resource_results(buf, version)?;
    Ok(results.first().map(|r| r.error_code).unwrap_or(0))
}

/// Decode AlterConfigs: every resource result.
pub fn decode_alter_configs_resource_results<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<AlterConfigsResourceResult>> {
    let flexible = alter_configs_flexible(version)?;
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let resource_type = buf::get_i8(buf)?;
        let name = buf::get_string(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        out.push(AlterConfigsResourceResult {
            error_code,
            error_message,
            resource_type,
            name,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// `true` when DeleteRecords `version` is flexible.
///
/// v0–v1 are classic (v1 response adds ThrottleTimeMs). v2 is the first
/// flexible version. Kafka 4.0 `validVersions` is `0-2`. This crate
/// speaks 0–2. v3+ is not spoken.
fn delete_records_flexible(version: i16) -> Result<bool> {
    match version {
        0 | 1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "DeleteRecords version {other} is not implemented"
        ))),
    }
}

/// One partition in a DeleteRecords request (v0–2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartition {
    /// Partition index.
    pub partition: i32,
    /// Delete records with offset strictly below this value.
    pub offset: i64,
}

/// Topic + partitions for DeleteRecords (v0–2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions and before-offsets.
    pub partitions: Vec<DeleteRecordsPartition>,
}

/// One partition in a DeleteRecords response (v0–2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedRecordsPartition {
    /// Partition index.
    pub partition: i32,
    /// New log start offset after the delete.
    pub low_watermark: i64,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

/// Topic + partition results from DeleteRecords (v0–2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedRecordsTopic {
    /// Topic name.
    pub topic: String,
    /// Per-partition low watermarks and errors.
    pub partitions: Vec<DeletedRecordsPartition>,
}

/// Encode a DeleteRecords request (v0–2; classic through v1; flexible from v2).
pub fn encode_delete_records_request(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    offset: i64,
    timeout_ms: i32,
) -> crate::error::Result<()> {
    encode_delete_records_topics_request(
        buf,
        version,
        &[DeleteRecordsTopic {
            topic: topic.to_string(),
            partitions: vec![DeleteRecordsPartition { partition, offset }],
        }],
        timeout_ms,
    )
}

/// Encode DeleteRecords v0–2 with a Topics array of N (Java `deleteRecords(Map)`).
pub fn encode_delete_records_topics_request(
    buf: &mut BytesMut,
    version: i16,
    topics: &[DeleteRecordsTopic],
    timeout_ms: i32,
) -> crate::error::Result<()> {
    let flexible = delete_records_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.offset);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_i32(timeout_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteRecords request (first topic/partition).
pub fn decode_delete_records_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i32, i64, i32)> {
    let (topics, timeout_ms) = decode_delete_records_topics_request(buf, version)?;
    let mut topic = String::new();
    let mut partition = 0i32;
    let mut offset = 0i64;
    if let Some(t) = topics.into_iter().next() {
        topic = t.topic;
        if let Some(p) = t.partitions.into_iter().next() {
            partition = p.partition;
            offset = p.offset;
        }
    }
    Ok((topic, partition, offset, timeout_ms))
}

/// Decode DeleteRecords v0–2: every topic/partition plus TimeoutMs.
pub fn decode_delete_records_topics_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<DeleteRecordsTopic>, i32)> {
    let flexible = delete_records_flexible(version)?;
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let offset = buf::get_i64(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(DeleteRecordsPartition { partition, offset });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(DeleteRecordsTopic { topic, partitions });
    }
    let timeout_ms = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((topics, timeout_ms))
}

/// Encode a DeleteRecords response (one topic/partition).
///
/// ThrottleTimeMs is encoded from v1 (KIP-219). v2 adds compact arrays/
/// strings plus tagged fields.
pub fn encode_delete_records_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    low_watermark: i64,
    error_code: i16,
) -> crate::error::Result<()> {
    encode_delete_records_topics_response(
        buf,
        version,
        &[DeletedRecordsTopic {
            topic: topic.to_string(),
            partitions: vec![DeletedRecordsPartition {
                partition,
                low_watermark,
                error_code,
            }],
        }],
    )
}

/// Encode DeleteRecords v0–2 for every topic/partition.
pub fn encode_delete_records_topics_response(
    buf: &mut BytesMut,
    version: i16,
    topics: &[DeletedRecordsTopic],
) -> crate::error::Result<()> {
    let flexible = delete_records_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(topics.len()))?;
    for t in topics {
        buf::put_string(buf, flexible, Some(&t.topic))?;
        buf::put_array_len(buf, flexible, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition);
            buf.put_i64(p.low_watermark);
            buf.put_i16(p.error_code);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteRecords response (first partition).
pub fn decode_delete_records_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i32, i64, i16)> {
    let topics = decode_delete_records_topics_response(buf, version)?;
    let mut partition = 0i32;
    let mut low_watermark = 0i64;
    let mut error_code = 0i16;
    if let Some(t) = topics.into_iter().next() {
        if let Some(p) = t.partitions.into_iter().next() {
            partition = p.partition;
            low_watermark = p.low_watermark;
            error_code = p.error_code;
        }
    }
    Ok((partition, low_watermark, error_code))
}

/// Decode DeleteRecords v0–2: every topic/partition.
///
/// Throttle is v1+. Does not fail on a non-zero partition ErrorCode;
/// callers decide.
pub fn decode_delete_records_topics_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DeletedRecordsTopic>> {
    let flexible = delete_records_flexible(version)?;
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition = buf::get_i32(buf)?;
            let low_watermark = buf::get_i64(buf)?;
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(DeletedRecordsPartition {
                partition,
                low_watermark,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topics.push(DeletedRecordsTopic { topic, partitions });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(topics)
}

/// DescribeCluster endpoint type: brokers (KIP-919).
pub const ENDPOINT_TYPE_BROKERS: i8 = 1;
/// DescribeCluster endpoint type: controllers (KIP-919).
pub const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;

/// DescribeCluster `EndpointType` (KIP-919). `1` = brokers, `2` = controllers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum EndpointType {
    /// Broker listener (`1`).
    Brokers = 1,
    /// Controller listener (`2`).
    Controllers = 2,
}

impl From<EndpointType> for i8 {
    fn from(ty: EndpointType) -> Self {
        ty as i8
    }
}

/// One broker in a DescribeCluster response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterBroker {
    /// Broker id.
    pub node_id: i32,
    /// Hostname or IP.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack id, or `None`.
    pub rack: Option<String>,
    /// Whether the broker is fenced (v2 / KIP-1073). `false` on v0–v1.
    pub is_fenced: bool,
}

impl DescribeClusterBroker {
    /// Construct [`Self`].
    pub fn new(
        node_id: i32,
        host: impl Into<String>,
        port: i32,
        rack: Option<String>,
        is_fenced: bool,
    ) -> Self {
        Self {
            node_id,
            host: host.into(),
            port,
            rack,
            is_fenced,
        }
    }

    /// Java `Node.id`.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Java `Node.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    /// Java `Node.port`.
    #[must_use]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Java `Node.rack`.
    #[must_use]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Java `Node.hasRack`.
    #[must_use]
    pub fn has_rack(&self) -> bool {
        self.rack.is_some()
    }

    /// Whether the broker is fenced (DescribeCluster v2).
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.is_fenced
    }
}

impl From<super::api::Broker> for DescribeClusterBroker {
    fn from(b: super::api::Broker) -> Self {
        Self {
            node_id: b.node_id,
            host: b.host,
            port: b.port,
            rack: b.rack,
            is_fenced: false,
        }
    }
}

/// DescribeCluster response: cluster id, controller, and brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    /// Top-level error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Cluster id from DescribeCluster, when known.
    pub cluster_id: Option<String>,
    /// Controller broker id, or `-1`.
    pub controller_id: i32,
    /// Endpoint type described (KIP-919). `1` = brokers, `2` = controllers.
    /// v0 decode fills [`ENDPOINT_TYPE_BROKERS`].
    pub endpoint_type: i8,
    /// 32-bit authorized-operations bitfield, or
    /// [`AUTHORIZED_OPERATIONS_OMITTED`].
    pub cluster_authorized_operations: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<DescribeClusterBroker>,
}

impl ClusterDescription {
    /// Top-level error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `DescribeClusterResult.clusterId`.
    #[must_use]
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Java `DescribeClusterResult.controller` as broker id (`-1` if none).
    #[must_use]
    pub fn controller_id(&self) -> i32 {
        self.controller_id
    }

    /// Java `DescribeClusterResult.nodes`.
    #[must_use]
    pub fn brokers(&self) -> &[DescribeClusterBroker] {
        &self.brokers
    }

    /// Java `DescribeClusterResult.nodes`.
    #[must_use]
    pub fn nodes(&self) -> &[DescribeClusterBroker] {
        self.brokers()
    }

    /// Java `DescribeClusterResult.controller`. Empty when
    /// [`Self::controller_id`] is `-1` or the id is not in [`Self::nodes`].
    #[must_use]
    pub fn controller(&self) -> Option<&DescribeClusterBroker> {
        if self.controller_id < 0 {
            return None;
        }
        self.brokers
            .iter()
            .find(|b| b.node_id == self.controller_id)
    }

    /// 32-bit authorized-operations bitfield, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn cluster_authorized_operations(&self) -> i32 {
        self.cluster_authorized_operations
    }

    /// Java `DescribeClusterResult.authorizedOperations` as the
    /// DescribeCluster bitfield, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        self.cluster_authorized_operations()
    }

    /// Endpoint type described (KIP-919).
    #[must_use]
    pub fn endpoint_type(&self) -> i8 {
        self.endpoint_type
    }
}

/// Check that DescribeCluster `version` is spoken (0–2).
///
/// Flexible from v0. v1 adds EndpointType (KIP-919). v2 adds
/// IncludeFencedBrokers / IsFenced (KIP-1073). Kafka 4.0 `validVersions`
/// is `0-2`. This crate speaks 0–2. v3+ is not spoken.
fn describe_cluster_spoken(version: i16) -> Result<i16> {
    match version {
        0..=2 => Ok(version),
        other => Err(Error::protocol(format!(
            "DescribeCluster version {other} is not implemented"
        ))),
    }
}

/// Encode a DescribeCluster request (v0–2; flexible from v0).
pub fn encode_describe_cluster_request(
    buf: &mut BytesMut,
    version: i16,
    include_authorized_operations: bool,
    endpoint_type: i8,
    include_fenced_brokers: bool,
) -> crate::error::Result<()> {
    let _ = describe_cluster_spoken(version)?;
    buf.put_u8(u8::from(include_authorized_operations));
    if version >= 1 {
        buf.put_i8(endpoint_type);
    }
    if version >= 2 {
        buf.put_u8(u8::from(include_fenced_brokers));
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeCluster request.
///
/// v0 fills `endpoint_type` = [`ENDPOINT_TYPE_BROKERS`] and
/// `include_fenced_brokers` = `false`.
pub fn decode_describe_cluster_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(bool, i8, bool)> {
    let _ = describe_cluster_spoken(version)?;
    let include = buf::get_bool(buf)?;
    let endpoint_type = if version >= 1 {
        buf::get_i8(buf)?
    } else {
        ENDPOINT_TYPE_BROKERS
    };
    let include_fenced = if version >= 2 {
        buf::get_bool(buf)?
    } else {
        false
    };
    buf::skip_tagged_fields(buf)?;
    Ok((include, endpoint_type, include_fenced))
}

/// Encode a DescribeCluster response (v0–2; flexible from v0).
pub fn encode_describe_cluster_response(
    buf: &mut BytesMut,
    version: i16,
    desc: &ClusterDescription,
) -> crate::error::Result<()> {
    let _ = describe_cluster_spoken(version)?;
    buf.put_i32(0);
    buf.put_i16(desc.error_code);
    buf::put_compact_string(buf, desc.error_message.as_deref())?;
    if version >= 1 {
        buf.put_i8(desc.endpoint_type);
    }
    buf::put_compact_string(buf, Some(desc.cluster_id.as_deref().unwrap_or("")))?;
    buf.put_i32(desc.controller_id);
    buf::put_array_len(buf, true, Some(desc.brokers.len()))?;
    for b in &desc.brokers {
        buf.put_i32(b.node_id);
        buf::put_compact_string(buf, Some(&b.host))?;
        buf.put_i32(b.port);
        buf::put_compact_string(buf, b.rack.as_deref())?;
        if version >= 2 {
            buf.put_u8(u8::from(b.is_fenced));
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_i32(desc.cluster_authorized_operations);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// One partition in AlterPartitionReassignments v0 (flexible).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignablePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Replica broker ids, or `None` to cancel the reassignment.
    pub replicas: Option<Vec<i32>>,
}

/// One topic in AlterPartitionReassignments v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignableTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<ReassignablePartition>,
}

/// Per-partition result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

/// Per-topic result of AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentTopicResult {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<ReassignmentPartitionResult>,
}

/// AlterPartitionReassignments v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Per-item results.
    pub results: Vec<ReassignmentTopicResult>,
}

fn put_compact_nullable_i32_array(
    buf: &mut BytesMut,
    items: Option<&[i32]>,
) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, true, None)?,
        Some(items) => {
            buf::put_array_len(buf, true, Some(items.len()))?;
            for v in items {
                buf.put_i32(*v);
            }
        }
    }
    Ok(())
}

fn get_compact_nullable_i32_array<B: Buf>(buf: &mut B) -> Result<Option<Vec<i32>>> {
    let Some(n) = buf::get_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(Some(out))
}

/// AlterPartitionReassignments v0 (flexible from v0; KIP-455).
pub fn encode_alter_partition_reassignments_request(
    buf: &mut BytesMut,
    timeout_ms: i32,
    topics: &[ReassignableTopic],
) -> crate::error::Result<()> {
    buf.put_i32(timeout_ms);
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            put_compact_nullable_i32_array(buf, p.replicas.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterPartitionReassignments request.
pub fn decode_alter_partition_reassignments_request<B: Buf>(
    buf: &mut B,
) -> Result<(i32, Vec<ReassignableTopic>)> {
    let timeout_ms = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let replicas = get_compact_nullable_i32_array(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(ReassignablePartition {
                partition_index,
                replicas,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(ReassignableTopic { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, topics))
}

/// Encode an AlterPartitionReassignments response.
pub fn encode_alter_partition_reassignments_response(
    buf: &mut BytesMut,
    resp: &AlterPartitionReassignmentsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for t in &resp.results {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterPartitionReassignments response.
pub fn decode_alter_partition_reassignments_response<B: Buf>(
    buf: &mut B,
) -> Result<AlterPartitionReassignmentsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let part_err = buf::get_i16(buf)?;
            let part_msg = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(ReassignmentPartitionResult {
                partition_index,
                error_code: part_err,
                error_message: part_msg,
            });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(ReassignmentTopicResult { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlterPartitionReassignmentsResponse {
        error_code,
        error_message,
        results,
    })
}

/// One topic in ListPartitionReassignments v0 (flexible; topics nullable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListReassignmentTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partition indexes.
    pub partition_indexes: Vec<i32>,
}

/// One ongoing partition reassignment in the List response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingPartitionReassignment {
    /// Partition index.
    pub partition_index: i32,
    /// Replica broker ids.
    pub replicas: Vec<i32>,
    /// Replicas being added.
    pub adding_replicas: Vec<i32>,
    /// Replicas being removed.
    pub removing_replicas: Vec<i32>,
}

/// One topic in ListPartitionReassignments response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingTopicReassignment {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<OngoingPartitionReassignment>,
}

/// ListPartitionReassignments v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Topics in this request or response.
    pub topics: Vec<OngoingTopicReassignment>,
}

fn put_compact_i32_array(buf: &mut BytesMut, items: &[i32]) -> crate::error::Result<()> {
    put_compact_nullable_i32_array(buf, Some(items))
}

fn get_compact_i32_array<B: Buf>(buf: &mut B) -> Result<Vec<i32>> {
    Ok(get_compact_nullable_i32_array(buf)?.unwrap_or_default())
}

/// ListPartitionReassignments v0 (flexible from v0; KIP-455).
///
/// `topics = None` lists every ongoing reassignment.
pub fn encode_list_partition_reassignments_request(
    buf: &mut BytesMut,
    timeout_ms: i32,
    topics: Option<&[ListReassignmentTopic]>,
) -> crate::error::Result<()> {
    buf.put_i32(timeout_ms);
    match topics {
        None => buf::put_array_len(buf, true, None)?,
        Some(topics) => {
            buf::put_array_len(buf, true, Some(topics.len()))?;
            for t in topics {
                buf::put_compact_string(buf, Some(&t.name))?;
                put_compact_i32_array(buf, &t.partition_indexes)?;
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListPartitionReassignments request.
pub fn decode_list_partition_reassignments_request<B: Buf>(
    buf: &mut B,
) -> Result<(i32, Option<Vec<ListReassignmentTopic>>)> {
    let timeout_ms = buf::get_i32(buf)?;
    let topics = match buf::get_array_len(buf, true)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_compact_string(buf)?.unwrap_or_default();
                let partition_indexes = get_compact_i32_array(buf)?;
                buf::skip_tagged_fields(buf)?;
                topics.push(ListReassignmentTopic {
                    name,
                    partition_indexes,
                });
            }
            Some(topics)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, topics))
}

/// Encode a ListPartitionReassignments response.
pub fn encode_list_partition_reassignments_response(
    buf: &mut BytesMut,
    resp: &ListPartitionReassignmentsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            put_compact_i32_array(buf, &p.replicas)?;
            put_compact_i32_array(buf, &p.adding_replicas)?;
            put_compact_i32_array(buf, &p.removing_replicas)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListPartitionReassignments response.
pub fn decode_list_partition_reassignments_response<B: Buf>(
    buf: &mut B,
) -> Result<ListPartitionReassignmentsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let replicas = get_compact_i32_array(buf)?;
            let adding_replicas = get_compact_i32_array(buf)?;
            let removing_replicas = get_compact_i32_array(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(OngoingPartitionReassignment {
                partition_index,
                replicas,
                adding_replicas,
                removing_replicas,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(OngoingTopicReassignment { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListPartitionReassignmentsResponse {
        error_code,
        error_message,
        topics,
    })
}

/// UpdateFeatures upgrade type: upgrade only (v1+ default).
pub const UPGRADE_TYPE_UPGRADE: i8 = 1;
/// UpdateFeatures upgrade type: safe (lossless) downgrade.
pub const UPGRADE_TYPE_SAFE_DOWNGRADE: i8 = 2;
/// UpdateFeatures upgrade type: unsafe (lossy) downgrade.
pub const UPGRADE_TYPE_UNSAFE_DOWNGRADE: i8 = 3;

/// UpdateFeatures `UpgradeType` (v1+). `1` = upgrade, `2` = safe downgrade,
/// `3` = unsafe downgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum UpgradeType {
    /// Upgrade only (`1`).
    Upgrade = 1,
    /// Safe lossless downgrade (`2`).
    SafeDowngrade = 2,
    /// Unsafe lossy downgrade (`3`).
    UnsafeDowngrade = 3,
}

impl From<UpgradeType> for i8 {
    fn from(ty: UpgradeType) -> Self {
        ty as i8
    }
}

fn upgrade_type_from_allow_downgrade(allow_downgrade: bool) -> i8 {
    if allow_downgrade {
        UPGRADE_TYPE_SAFE_DOWNGRADE
    } else {
        UPGRADE_TYPE_UPGRADE
    }
}

/// One finalized-feature update in UpdateFeatures (flexible; KIP-584).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateKey {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Maximum feature version to set.
    pub max_version_level: i16,
    /// When true, the broker may lower the feature level (v0 `AllowDowngrade`).
    pub allow_downgrade: bool,
    /// Upgrade type (v1+ `UpgradeType`). `1` upgrade, `2` safe downgrade,
    /// `3` unsafe downgrade.
    pub upgrade_type: i8,
}

impl FeatureUpdateKey {
    /// Construct [`Self`]. v1+ `upgrade_type` follows `allow_downgrade`
    /// (Java `FeatureUpdate(short, boolean)`).
    pub fn new(name: impl Into<String>, max_version_level: i16, allow_downgrade: bool) -> Self {
        Self {
            name: name.into(),
            max_version_level,
            allow_downgrade,
            upgrade_type: upgrade_type_from_allow_downgrade(allow_downgrade),
        }
    }
}

/// Per-feature result of UpdateFeatures v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatableFeatureResult {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

/// UpdateFeatures response (top-level error after throttle).
///
/// v2 omits per-feature Results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Per-item results.
    pub results: Vec<UpdatableFeatureResult>,
}

/// Check that UpdateFeatures `version` is spoken (0–2).
///
/// Flexible from v0. v0 encodes `AllowDowngrade`. v1+ replaces it with
/// `UpgradeType` and adds top-level `ValidateOnly`. v2 omits per-feature
/// Results. Kafka 4.0 `validVersions` is `0-2`. This crate speaks 0–2.
/// v3+ is not spoken.
fn update_features_spoken(version: i16) -> Result<i16> {
    match version {
        0..=2 => Ok(version),
        other => Err(Error::protocol(format!(
            "UpdateFeatures version {other} is not implemented"
        ))),
    }
}

/// Encode an UpdateFeatures request (v0–2; flexible from v0).
pub fn encode_update_features_request(
    buf: &mut BytesMut,
    version: i16,
    timeout_ms: i32,
    updates: &[FeatureUpdateKey],
    validate_only: bool,
) -> crate::error::Result<()> {
    let _ = update_features_spoken(version)?;
    buf.put_i32(timeout_ms);
    buf::put_array_len(buf, true, Some(updates.len()))?;
    for u in updates {
        buf::put_compact_string(buf, Some(&u.name))?;
        buf.put_i16(u.max_version_level);
        if version == 0 {
            buf.put_u8(u8::from(u.allow_downgrade));
        } else {
            buf.put_i8(u.upgrade_type);
        }
        buf::put_empty_tagged_fields(buf);
    }
    if version >= 1 {
        buf.put_u8(u8::from(validate_only));
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an UpdateFeatures request.
///
/// Returns `(timeout_ms, updates, validate_only)`. v0 fills `validate_only`
/// = `false` and `upgrade_type` from `AllowDowngrade`. v1+ fills
/// `allow_downgrade` from `UpgradeType != 1`.
pub fn decode_update_features_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i32, Vec<FeatureUpdateKey>, bool)> {
    let _ = update_features_spoken(version)?;
    let timeout_ms = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut updates = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let max_version_level = buf::get_i16(buf)?;
        let (allow_downgrade, upgrade_type) = if version == 0 {
            let allow = buf::get_bool(buf)?;
            (allow, upgrade_type_from_allow_downgrade(allow))
        } else {
            let upgrade_type = buf::get_i8(buf)?;
            (upgrade_type != UPGRADE_TYPE_UPGRADE, upgrade_type)
        };
        buf::skip_tagged_fields(buf)?;
        updates.push(FeatureUpdateKey {
            name,
            max_version_level,
            allow_downgrade,
            upgrade_type,
        });
    }
    let validate_only = if version >= 1 {
        buf::get_bool(buf)?
    } else {
        false
    };
    buf::skip_tagged_fields(buf)?;
    Ok((timeout_ms, updates, validate_only))
}

/// Encode an UpdateFeatures response (v0–2; flexible from v0).
///
/// v2 omits Results (Kafka 4.0 JSON `versions: "0-1"`).
pub fn encode_update_features_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &UpdateFeaturesResponse,
) -> crate::error::Result<()> {
    let _ = update_features_spoken(version)?;
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    if version < 2 {
        buf::put_array_len(buf, true, Some(resp.results.len()))?;
        for r in &resp.results {
            buf::put_compact_string(buf, Some(&r.name))?;
            buf.put_i16(r.error_code);
            buf::put_compact_string(buf, r.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an UpdateFeatures response.
///
/// v2 fills `results` empty (no Results array on the wire).
pub fn decode_update_features_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<UpdateFeaturesResponse> {
    let _ = update_features_spoken(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let mut results = Vec::new();
    if version < 2 {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        results.reserve(n);
        for _ in 0..n {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let feat_err = buf::get_i16(buf)?;
            let feat_msg = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            results.push(UpdatableFeatureResult {
                name,
                error_code: feat_err,
                error_message: feat_msg,
            });
        }
    }
    buf::skip_tagged_fields(buf)?;
    Ok(UpdateFeaturesResponse {
        error_code,
        error_message,
        results,
    })
}

/// One SCRAM credential to remove (AlterUserScramCredentials v0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// SCRAM mechanism id (`SCRAM_SHA_256` / `SCRAM_SHA_512`).
    pub mechanism: i8,
}

/// One SCRAM credential to insert or replace (AlterUserScramCredentials v0).
///
/// `salt` / `salted_password` are caller-supplied bytes. This type does not
/// hash a password. `Debug` redacts those fields.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// SCRAM mechanism id (`SCRAM_SHA_256` / `SCRAM_SHA_512`).
    pub mechanism: i8,
    /// SCRAM iteration count.
    pub iterations: i32,
    /// SCRAM salt.
    pub salt: Vec<u8>,
    /// SCRAM salted password.
    pub salted_password: Vec<u8>,
}

impl fmt::Debug for ScramCredentialUpsertion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramCredentialUpsertion")
            .field("name", &self.name)
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("salted_password", &"<redacted>")
            .finish()
    }
}

/// Per-user result of AlterUserScramCredentials v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResult {
    /// SCRAM user name.
    pub user: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

/// AlterUserScramCredentials v0 (flexible from v0; KIP-554).
///
/// Official Apache JSON (`apiKey: 51`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: compact
/// `Deletions` of `{Name, Mechanism, tagged}`, compact `Upsertions` of
/// `{Name, Mechanism, Iterations, Salt, SaltedPassword, tagged}`, then
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32, compact
/// `Results` of `{User, ErrorCode, ErrorMessage, tagged}`, tagged. There
/// is no top-level `error_code` — 41 is on each result, after throttle,
/// the compact results length, and that result's compact `User`.
pub fn encode_alter_user_scram_credentials_request(
    buf: &mut BytesMut,
    deletions: &[ScramCredentialDeletion],
    upsertions: &[ScramCredentialUpsertion],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(deletions.len()))?;
    for d in deletions {
        buf::put_compact_string(buf, Some(&d.name))?;
        buf.put_i8(d.mechanism);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_array_len(buf, true, Some(upsertions.len()))?;
    for u in upsertions {
        buf::put_compact_string(buf, Some(&u.name))?;
        buf.put_i8(u.mechanism);
        buf.put_i32(u.iterations);
        buf::put_compact_bytes(buf, Some(&u.salt))?;
        buf::put_compact_bytes(buf, Some(&u.salted_password))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterUserScramCredentials request.
pub fn decode_alter_user_scram_credentials_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<ScramCredentialDeletion>, Vec<ScramCredentialUpsertion>)> {
    let n_del = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut deletions = Vec::with_capacity(n_del);
    for _ in 0..n_del {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mechanism = buf::get_i8(buf)?;
        buf::skip_tagged_fields(buf)?;
        deletions.push(ScramCredentialDeletion { name, mechanism });
    }
    let n_up = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut upsertions = Vec::with_capacity(n_up);
    for _ in 0..n_up {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mechanism = buf::get_i8(buf)?;
        let iterations = buf::get_i32(buf)?;
        let salt = buf::get_compact_bytes(buf)?.unwrap_or_default();
        let salted_password = buf::get_compact_bytes(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        upsertions.push(ScramCredentialUpsertion {
            name,
            mechanism,
            iterations,
            salt,
            salted_password,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((deletions, upsertions))
}

/// Encode an AlterUserScramCredentials response.
pub fn encode_alter_user_scram_credentials_response(
    buf: &mut BytesMut,
    results: &[AlterUserScramCredentialsResult],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(results.len()))?;
    for r in results {
        buf::put_compact_string(buf, Some(&r.user))?;
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterUserScramCredentials response.
pub fn decode_alter_user_scram_credentials_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<AlterUserScramCredentialsResult>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = buf::get_compact_string(buf)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        results.push(AlterUserScramCredentialsResult {
            user,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(results)
}

/// One SCRAM mechanism + iteration count (DescribeUserScramCredentials v0).
///
/// Fixture metadata only. This is not a credential store and does not
/// carry salt or salted password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    /// SCRAM mechanism id (`SCRAM_SHA_256` / `SCRAM_SHA_512`).
    pub mechanism: i8,
    /// SCRAM iteration count.
    pub iterations: i32,
}

impl ScramCredentialInfo {
    /// Java `ScramCredentialInfo(mechanism, iterations)`.
    #[must_use]
    pub fn new(mechanism: impl Into<i8>, iterations: i32) -> Self {
        Self {
            mechanism: mechanism.into(),
            iterations,
        }
    }

    /// Java `ScramCredentialInfo.mechanism`.
    #[must_use]
    pub fn mechanism(&self) -> ScramMechanism {
        ScramMechanism::from_id(self.mechanism)
    }

    /// Java `ScramCredentialInfo.iterations`.
    #[must_use]
    pub fn iterations(&self) -> i32 {
        self.iterations
    }
}

/// Per-user result of DescribeUserScramCredentials v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResult {
    /// SCRAM user name.
    pub user: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// SCRAM credentials for this user.
    pub credential_infos: Vec<ScramCredentialInfo>,
}

impl DescribeUserScramCredentialsResult {
    /// Java `UserScramCredentialsDescription.name`.
    #[must_use]
    pub fn user(&self) -> &str {
        self.user.as_str()
    }

    /// Per-user error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Java `UserScramCredentialsDescription.credentialInfos`.
    #[must_use]
    pub fn credential_infos(&self) -> &[ScramCredentialInfo] {
        &self.credential_infos
    }
}

/// DescribeUserScramCredentials v0 body (api 50).
///
/// Official Apache JSON (`apiKey: 50`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` and `controller`) and
/// kafka-protocol 0.18.0: this crate targets v0. Request: nullable
/// compact `Users` of `{Name compact, tagged}`, tagged. Null or empty
/// means describe all users. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact nullable `ErrorMessage`, compact `Results`
/// of `{User, ErrorCode, ErrorMessage, CredentialInfos[] of
/// {Mechanism INT8, Iterations INT32, tagged}, tagged}`, tagged.
/// Measured: **41 is the top-level ErrorCode at bytes 4–5**, after
/// throttle. Not a first-result field (AlterUserScramCredentials puts
/// 41 after compact User at bytes 11–12). Fixture users only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Per-item results.
    pub results: Vec<DescribeUserScramCredentialsResult>,
}

/// Encode a DescribeUserScramCredentials request.
pub fn encode_describe_user_scram_credentials_request(
    buf: &mut BytesMut,
    users: Option<&[String]>,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, users.map(|u| u.len()))?;
    if let Some(users) = users {
        for name in users {
            buf::put_compact_string(buf, Some(name))?;
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeUserScramCredentials request.
pub fn decode_describe_user_scram_credentials_request<B: Buf>(
    buf: &mut B,
) -> Result<Option<Vec<String>>> {
    let n = buf::get_array_len(buf, true)?;
    let users = match n {
        None => None,
        Some(n) => {
            let mut users = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_compact_string(buf)?.unwrap_or_default();
                buf::skip_tagged_fields(buf)?;
                users.push(name);
            }
            Some(users)
        }
    };
    buf::skip_tagged_fields(buf)?;
    Ok(users)
}

/// Encode a DescribeUserScramCredentials response.
pub fn encode_describe_user_scram_credentials_response(
    buf: &mut BytesMut,
    resp: &DescribeUserScramCredentialsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.results.len()))?;
    for r in &resp.results {
        buf::put_compact_string(buf, Some(&r.user))?;
        buf.put_i16(r.error_code);
        buf::put_compact_string(buf, r.error_message.as_deref())?;
        buf::put_array_len(buf, true, Some(r.credential_infos.len()))?;
        for c in &r.credential_infos {
            buf.put_i8(c.mechanism);
            buf.put_i32(c.iterations);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeUserScramCredentials response.
pub fn decode_describe_user_scram_credentials_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeUserScramCredentialsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = buf::get_compact_string(buf)?.unwrap_or_default();
        let user_error = buf::get_i16(buf)?;
        let user_message = buf::get_compact_string(buf)?;
        let cn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut credential_infos = Vec::with_capacity(cn);
        for _ in 0..cn {
            let mechanism = buf::get_i8(buf)?;
            let iterations = buf::get_i32(buf)?;
            buf::skip_tagged_fields(buf)?;
            credential_infos.push(ScramCredentialInfo {
                mechanism,
                iterations,
            });
        }
        buf::skip_tagged_fields(buf)?;
        results.push(DescribeUserScramCredentialsResult {
            user,
            error_code: user_error,
            error_message: user_message,
            credential_infos,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeUserScramCredentialsResponse {
        error_code,
        error_message,
        results,
    })
}

/// One quota entity component (type + optional name). Null name is the default.
///
/// Java `ClientQuotaEntity` is a type-to-name map; this is one map entry.
/// [`Self::USER`] / [`Self::CLIENT_ID`] / [`Self::IP`] match the Java
/// constants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntity {
    /// Quota entity type (for example `client-id`).
    pub entity_type: String,
    /// Topic, resource, group, or feature name.
    pub name: Option<String>,
}

impl ClientQuotaEntity {
    /// Java `ClientQuotaEntity.USER`.
    pub const USER: &'static str = "user";
    /// Java `ClientQuotaEntity.CLIENT_ID`.
    pub const CLIENT_ID: &'static str = "client-id";
    /// Java `ClientQuotaEntity.IP`.
    pub const IP: &'static str = "ip";

    /// Construct [`Self`].
    #[must_use]
    pub fn new(entity_type: impl Into<String>, name: Option<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            name,
        }
    }

    /// Java `ClientQuotaEntity.isValidEntityType`.
    #[must_use]
    pub fn is_valid_entity_type(entity_type: &str) -> bool {
        entity_type == Self::USER || entity_type == Self::CLIENT_ID || entity_type == Self::IP
    }

    /// Entity type string (Java map key in `ClientQuotaEntity.entries`).
    #[must_use]
    pub fn entity_type(&self) -> &str {
        self.entity_type.as_str()
    }

    /// Entity name, or `None` for the built-in default (Java null map value).
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// MatchType 0: exact entity name (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_EXACT: i8 = 0;
/// MatchType 1: default entity (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_DEFAULT: i8 = 1;
/// MatchType 2: any specified name (DescribeClientQuotas, KIP-219).
pub const QUOTA_MATCH_ANY: i8 = 2;

/// One filter component in DescribeClientQuotas (api 48).
///
/// [`Self::of_entity`] / [`Self::of_default_entity`] / [`Self::of_entity_type`]
/// are Java `ClientQuotaFilterComponent` factories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilterComponent {
    /// Quota entity type (for example `client-id`).
    pub entity_type: String,
    /// Quota filter match type (`QUOTA_MATCH_*`).
    pub match_type: i8,
    /// Quota filter match value.
    pub match_value: Option<String>,
}

impl ClientQuotaFilterComponent {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(
        entity_type: impl Into<String>,
        match_type: i8,
        match_value: Option<String>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type,
            match_value,
        }
    }

    /// Java `ClientQuotaFilterComponent.ofEntity`.
    #[must_use]
    pub fn of_entity(entity_type: impl Into<String>, entity_name: impl Into<String>) -> Self {
        Self::new(entity_type, QUOTA_MATCH_EXACT, Some(entity_name.into()))
    }

    /// Java `ClientQuotaFilterComponent.ofDefaultEntity`.
    #[must_use]
    pub fn of_default_entity(entity_type: impl Into<String>) -> Self {
        Self::new(entity_type, QUOTA_MATCH_DEFAULT, None)
    }

    /// Java `ClientQuotaFilterComponent.ofEntityType` (any specified name).
    #[must_use]
    pub fn of_entity_type(entity_type: impl Into<String>) -> Self {
        Self::new(entity_type, QUOTA_MATCH_ANY, None)
    }

    /// Java `ClientQuotaFilterComponent.entityType`.
    #[must_use]
    pub fn entity_type(&self) -> &str {
        self.entity_type.as_str()
    }

    /// Java `ClientQuotaFilterComponent.match`.
    ///
    /// Present inner is an exact name ([`Self::of_entity`]). Empty inner is
    /// the default entity ([`Self::of_default_entity`]). Outer `None` is any
    /// specified name ([`Self::of_entity_type`]).
    #[must_use]
    pub fn matched(&self) -> Option<Option<&str>> {
        match self.match_type {
            QUOTA_MATCH_EXACT => Some(self.match_value.as_deref()),
            QUOTA_MATCH_DEFAULT => Some(None),
            _ => None,
        }
    }
}

/// Java `ClientQuotaFilter` for [`crate::Admin::describe_client_quotas`].
///
/// [`Self::all`] is empty components and `strict = false`.
/// [`Self::contains`] is those components and `strict = false`.
/// [`Self::contains_only`] is those components and `strict = true`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilter {
    components: Vec<ClientQuotaFilterComponent>,
    strict: bool,
}

impl ClientQuotaFilter {
    /// Java `ClientQuotaFilter.all()`.
    #[must_use]
    pub fn all() -> Self {
        Self {
            components: Vec::new(),
            strict: false,
        }
    }

    /// Java `ClientQuotaFilter.contains(Collection)`.
    #[must_use]
    pub fn contains(components: impl IntoIterator<Item = ClientQuotaFilterComponent>) -> Self {
        Self {
            components: components.into_iter().collect(),
            strict: false,
        }
    }

    /// Java `ClientQuotaFilter.containsOnly(Collection)`.
    #[must_use]
    pub fn contains_only(components: impl IntoIterator<Item = ClientQuotaFilterComponent>) -> Self {
        Self {
            components: components.into_iter().collect(),
            strict: true,
        }
    }

    /// Filter components (Java `ClientQuotaFilter.components()`).
    #[must_use]
    pub fn components(&self) -> &[ClientQuotaFilterComponent] {
        &self.components
    }

    /// Strict match (Java `ClientQuotaFilter.strict()`).
    #[must_use]
    pub fn strict(&self) -> bool {
        self.strict
    }
}

/// One quota key/value in a DescribeClientQuotas entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaValue {
    /// Quota or config key.
    pub key: String,
    /// Quota value.
    pub value: f64,
}

impl ClientQuotaValue {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    /// Quota key.
    #[must_use]
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Quota value.
    #[must_use]
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// One described quota entity plus its values.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    /// Quota entity entries.
    pub entity: Vec<ClientQuotaEntity>,
    /// Quota key/value pairs.
    pub values: Vec<ClientQuotaValue>,
}

impl ClientQuotaEntry {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(entity: Vec<ClientQuotaEntity>, values: Vec<ClientQuotaValue>) -> Self {
        Self { entity, values }
    }

    /// Entity type/name pairs (Java `ClientQuotaEntity.entries` as a list).
    #[must_use]
    pub fn entity(&self) -> &[ClientQuotaEntity] {
        &self.entity
    }

    /// Quota key/value pairs.
    #[must_use]
    pub fn values(&self) -> &[ClientQuotaValue] {
        &self.values
    }
}

/// DescribeClientQuotas v1 response body (top-level ErrorCode).
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Matching quota entries.
    pub entries: Option<Vec<ClientQuotaEntry>>,
}

/// One quota key to set or remove (AlterClientQuotas).
///
/// `value` is ignored when `remove` is true. This is a fixture op, not a
/// live cluster quota store.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaOp {
    /// Quota or config key.
    pub key: String,
    /// Quota value.
    pub value: f64,
    /// When true, delete this quota key.
    pub remove: bool,
}

impl ClientQuotaOp {
    /// Quota op that sets `key` to `value`.
    #[must_use]
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            remove: false,
        }
    }

    /// Quota op that deletes `key`.
    #[must_use]
    pub fn remove(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: 0.0,
            remove: true,
        }
    }

    /// Java `ClientQuotaAlteration.Op.key`.
    #[must_use]
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Java `ClientQuotaAlteration.Op.value` (`None` clears the key).
    #[must_use]
    pub fn value(&self) -> Option<f64> {
        if self.remove {
            None
        } else {
            Some(self.value)
        }
    }
}

/// One entity plus its ops in AlterClientQuotas.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    /// Quota entity entries.
    pub entity: Vec<ClientQuotaEntity>,
    /// Quota alterations.
    pub ops: Vec<ClientQuotaOp>,
}

impl ClientQuotaAlteration {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(entity: Vec<ClientQuotaEntity>, ops: Vec<ClientQuotaOp>) -> Self {
        Self { entity, ops }
    }

    /// Java `ClientQuotaAlteration.entity` (one type/name pair per entry).
    #[must_use]
    pub fn entity(&self) -> &[ClientQuotaEntity] {
        &self.entity
    }

    /// Java `ClientQuotaAlteration.ops`.
    #[must_use]
    pub fn ops(&self) -> &[ClientQuotaOp] {
        &self.ops
    }
}

/// Per-entry result of AlterClientQuotas. Error sits on the entry;
/// there is no top-level response error_code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaAlterationResult {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Quota entity entries.
    pub entity: Vec<ClientQuotaEntity>,
}

impl ClientQuotaAlterationResult {
    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Quota entity entries.
    #[must_use]
    pub fn entity(&self) -> &[ClientQuotaEntity] {
        &self.entity
    }
}

/// `true` when AlterClientQuotas `version` is flexible.
///
/// v0 is classic. v1 is the first flexible version. Kafka 4.0
/// `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken.
fn alter_client_quotas_flexible(version: i16) -> Result<bool> {
    match version {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::protocol(format!(
            "AlterClientQuotas version {other} is not implemented"
        ))),
    }
}

/// AlterClientQuotas v0–1 (classic at v0; flexible from v1; KIP-546 / KIP-599).
///
/// Official Apache JSON (`apiKey: 49`, `validVersions: "0-1"`,
/// `flexibleVersions: "1+"`) and kafka-protocol 0.18.0
/// (`AlterClientQuotasRequest` / `AlterClientQuotasResponse`).
/// Kafka 4.0 max is 1; this crate speaks 0–1.
/// Request: `Entries` of `{Entity [{EntityType, EntityName nullable,
/// tagged (v1+)}], Ops [{Key, Value FLOAT64, Remove BOOLEAN, tagged
/// (v1+)}], tagged (v1+)}`, `ValidateOnly` BOOLEAN, tagged (v1+). No
/// timeout field. Response: `ThrottleTimeMs` INT32, `Entries` of
/// `{ErrorCode INT16, ErrorMessage nullable, Entity, tagged (v1+)}`,
/// tagged (v1+). There is no top-level `error_code` — 41 is the first
/// entry ErrorCode, after throttle and the entries length (bytes 5–6
/// on leftover-empty v1 compact; classic v0 places that ErrorCode
/// later).
pub fn encode_alter_client_quotas_request(
    buf: &mut BytesMut,
    version: i16,
    entries: &[ClientQuotaAlteration],
    validate_only: bool,
) -> crate::error::Result<()> {
    let flexible = alter_client_quotas_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(entries.len()))?;
    for e in entries {
        buf::put_array_len(buf, flexible, Some(e.entity.len()))?;
        for ent in &e.entity {
            buf::put_string(buf, flexible, Some(&ent.entity_type))?;
            buf::put_string(buf, flexible, ent.name.as_deref())?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        buf::put_array_len(buf, flexible, Some(e.ops.len()))?;
        for op in &e.ops {
            buf::put_string(buf, flexible, Some(&op.key))?;
            buf.put_f64(op.value);
            buf.put_u8(u8::from(op.remove));
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_u8(u8::from(validate_only));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterClientQuotas request.
pub fn decode_alter_client_quotas_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<ClientQuotaAlteration>, bool)> {
    let flexible = alter_client_quotas_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let en = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut entity = Vec::with_capacity(en);
        for _ in 0..en {
            let entity_type = buf::get_string(buf, flexible)?.unwrap_or_default();
            let name = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            entity.push(ClientQuotaEntity { entity_type, name });
        }
        let on = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut ops = Vec::with_capacity(on);
        for _ in 0..on {
            let key = buf::get_string(buf, flexible)?.unwrap_or_default();
            let value = buf::get_f64(buf)?;
            let remove = buf::get_bool(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            ops.push(ClientQuotaOp { key, value, remove });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        entries.push(ClientQuotaAlteration { entity, ops });
    }
    let validate_only = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((entries, validate_only))
}

/// Encode an AlterClientQuotas response (v0–1).
pub fn encode_alter_client_quotas_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[ClientQuotaAlterationResult],
) -> crate::error::Result<()> {
    let flexible = alter_client_quotas_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf.put_i16(r.error_code);
        buf::put_string(buf, flexible, r.error_message.as_deref())?;
        buf::put_array_len(buf, flexible, Some(r.entity.len()))?;
        for ent in &r.entity {
            buf::put_string(buf, flexible, Some(&ent.entity_type))?;
            buf::put_string(buf, flexible, ent.name.as_deref())?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterClientQuotas response.
pub fn decode_alter_client_quotas_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<ClientQuotaAlterationResult>> {
    let flexible = alter_client_quotas_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let en = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut entity = Vec::with_capacity(en);
        for _ in 0..en {
            let entity_type = buf::get_string(buf, flexible)?.unwrap_or_default();
            let name = buf::get_string(buf, flexible)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            entity.push(ClientQuotaEntity { entity_type, name });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        results.push(ClientQuotaAlterationResult {
            error_code,
            error_message,
            entity,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(results)
}

/// `true` when DescribeClientQuotas `version` is flexible.
///
/// v0 is classic. v1 is the first flexible version. Kafka 4.0
/// `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken.
fn describe_client_quotas_flexible(version: i16) -> Result<bool> {
    match version {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::protocol(format!(
            "DescribeClientQuotas version {other} is not implemented"
        ))),
    }
}

/// DescribeClientQuotas v0–1 (classic at v0; flexible from v1; KIP-219).
///
/// Official Apache JSON (`apiKey: 48`, `validVersions: "0-1"`,
/// `flexibleVersions: "1+"`, listeners `broker` only) and
/// kafka-protocol 0.18.0. Request: `Components` of `{EntityType,
/// MatchType INT8 (0 exact / 1 default / 2 any), Match nullable,
/// tagged (v1+)}`, `Strict` BOOLEAN, tagged (v1+). Response:
/// `ThrottleTimeMs` INT32, **top-level `ErrorCode` INT16**, nullable
/// `ErrorMessage`, nullable `Entries` of `{Entity [{EntityType,
/// EntityName nullable, tagged (v1+)}], Values [{Key, Value FLOAT64,
/// tagged (v1+)}], tagged (v1+)}`, tagged (v1+). Measured
/// independently from kafka-protocol 0.18.0 (`client` encodes the
/// request; `broker` encodes the response): **the top-level ErrorCode
/// is the INT16 at bytes 4–5**, after throttle — not a first-result
/// field (AlterClientQuotas puts the first-entry code at bytes 5–6
/// on v1). This is not a controller hop.
pub fn encode_describe_client_quotas_request(
    buf: &mut BytesMut,
    version: i16,
    components: &[ClientQuotaFilterComponent],
    strict: bool,
) -> crate::error::Result<()> {
    let flexible = describe_client_quotas_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(components.len()))?;
    for c in components {
        buf::put_string(buf, flexible, Some(&c.entity_type))?;
        buf.put_i8(c.match_type);
        buf::put_string(buf, flexible, c.match_value.as_deref())?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_u8(u8::from(strict));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeClientQuotas request.
pub fn decode_describe_client_quotas_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<ClientQuotaFilterComponent>, bool)> {
    let flexible = describe_client_quotas_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut components = Vec::with_capacity(n);
    for _ in 0..n {
        let entity_type = buf::get_string(buf, flexible)?.unwrap_or_default();
        let match_type = buf::get_i8(buf)?;
        let match_value = buf::get_string(buf, flexible)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        components.push(ClientQuotaFilterComponent {
            entity_type,
            match_type,
            match_value,
        });
    }
    let strict = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((components, strict))
}

/// Encode a DescribeClientQuotas response (v0–1).
pub fn encode_describe_client_quotas_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &DescribeClientQuotasResponse,
) -> crate::error::Result<()> {
    let flexible = describe_client_quotas_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_string(buf, flexible, resp.error_message.as_deref())?;
    match &resp.entries {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(entries) => {
            buf::put_array_len(buf, flexible, Some(entries.len()))?;
            for e in entries {
                buf::put_array_len(buf, flexible, Some(e.entity.len()))?;
                for ent in &e.entity {
                    buf::put_string(buf, flexible, Some(&ent.entity_type))?;
                    buf::put_string(buf, flexible, ent.name.as_deref())?;
                    if flexible {
                        buf::put_empty_tagged_fields(buf);
                    }
                }
                buf::put_array_len(buf, flexible, Some(e.values.len()))?;
                for v in &e.values {
                    buf::put_string(buf, flexible, Some(&v.key))?;
                    buf.put_f64(v.value);
                    if flexible {
                        buf::put_empty_tagged_fields(buf);
                    }
                }
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeClientQuotas response.
pub fn decode_describe_client_quotas_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeClientQuotasResponse> {
    let flexible = describe_client_quotas_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_string(buf, flexible)?;
    let entries = match buf::get_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let en = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                let mut entity = Vec::with_capacity(en);
                for _ in 0..en {
                    let entity_type = buf::get_string(buf, flexible)?.unwrap_or_default();
                    let name = buf::get_string(buf, flexible)?;
                    if flexible {
                        buf::skip_tagged_fields(buf)?;
                    }
                    entity.push(ClientQuotaEntity { entity_type, name });
                }
                let vn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                let mut values = Vec::with_capacity(vn);
                for _ in 0..vn {
                    let key = buf::get_string(buf, flexible)?.unwrap_or_default();
                    let value = buf::get_f64(buf)?;
                    if flexible {
                        buf::skip_tagged_fields(buf)?;
                    }
                    values.push(ClientQuotaValue { key, value });
                }
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                entries.push(ClientQuotaEntry { entity, values });
            }
            Some(entries)
        }
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribeClientQuotasResponse {
        error_code,
        error_message,
        entries,
    })
}

/// One active producer in DescribeProducers (api 61, KIP-360).
///
/// Java `ProducerState`. [`Self::coordinator_epoch`] /
/// [`Self::current_txn_start_offset`] are `None` when the wire value is
/// negative (Java `OptionalInt` / `OptionalLong`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProducer {
    /// Producer id, or `-1`.
    pub producer_id: i64,
    /// Producer epoch, or `-1`.
    pub producer_epoch: i32,
    /// Last Produce sequence number.
    pub last_sequence: i32,
    /// Last Produce timestamp (milliseconds).
    pub last_timestamp: i64,
    /// Transaction coordinator epoch.
    pub coordinator_epoch: i32,
    /// Start offset of the current transaction, or `-1`.
    pub current_txn_start_offset: i64,
}

impl ActiveProducer {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(
        producer_id: i64,
        producer_epoch: i32,
        last_sequence: i32,
        last_timestamp: i64,
        coordinator_epoch: i32,
        current_txn_start_offset: i64,
    ) -> Self {
        Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        }
    }

    /// Java `ProducerState.producerId`.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `ProducerState.producerEpoch`.
    #[must_use]
    pub fn producer_epoch(&self) -> i32 {
        self.producer_epoch
    }

    /// Java `ProducerState.lastSequence`.
    #[must_use]
    pub fn last_sequence(&self) -> i32 {
        self.last_sequence
    }

    /// Java `ProducerState.lastTimestamp`.
    #[must_use]
    pub fn last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    /// Java `ProducerState.coordinatorEpoch` (`None` when the wire value is negative).
    #[must_use]
    pub fn coordinator_epoch(&self) -> Option<i32> {
        (self.coordinator_epoch >= 0).then_some(self.coordinator_epoch)
    }

    /// Java `ProducerState.currentTransactionStartOffset` (`None` when the wire value is negative).
    #[must_use]
    pub fn current_txn_start_offset(&self) -> Option<i64> {
        (self.current_txn_start_offset >= 0).then_some(self.current_txn_start_offset)
    }
}

/// Per-partition DescribeProducers result. ErrorCode sits here, not
/// at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Active producers on this partition.
    pub active_producers: Vec<ActiveProducer>,
}

impl DescribeProducersPartition {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(
        partition_index: i32,
        error_code: i16,
        error_message: Option<String>,
        active_producers: Vec<ActiveProducer>,
    ) -> Self {
        Self {
            partition_index,
            error_code,
            error_message,
            active_producers,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Java `DescribeProducersResult.PartitionProducerState.activeProducers`.
    #[must_use]
    pub fn active_producers(&self) -> &[ActiveProducer] {
        &self.active_producers
    }
}

/// One topic in a DescribeProducers v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<DescribeProducersPartition>,
}

impl DescribeProducersTopic {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(name: impl Into<String>, partitions: Vec<DescribeProducersPartition>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Per-partition producer state.
    #[must_use]
    pub fn partitions(&self) -> &[DescribeProducersPartition] {
        &self.partitions
    }
}

/// DescribeProducers v0 response body. There is no top-level ErrorCode
/// after throttle; the first-partition code is later in the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResponse {
    /// Topics in this request or response.
    pub topics: Vec<DescribeProducersTopic>,
}

impl DescribeProducersResponse {
    /// Construct [`Self`].
    #[must_use]
    pub fn new(topics: Vec<DescribeProducersTopic>) -> Self {
        Self { topics }
    }

    /// Per-topic producer state.
    #[must_use]
    pub fn topics(&self) -> &[DescribeProducersTopic] {
        &self.topics
    }
}

/// One topic in a DescribeProducers request (Topics array element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopicRequest {
    /// Topic name.
    pub name: String,
    /// Partition indexes to describe.
    pub partition_indexes: Vec<i32>,
}

/// DescribeProducers v0 (flexible from v0; KIP-360).
///
/// Official Apache JSON (`apiKey: 61`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` only) and
/// kafka-protocol 0.18.0 (`DescribeProducersRequest` /
/// `DescribeProducersResponse`, `VERSIONS` min=max=0). This crate
/// targets v0, the only version. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Request: compact `Topics` of `{Name, compact PartitionIndexes
/// INT32[], tagged}`, tagged. Response: `ThrottleTimeMs` INT32,
/// compact `Topics` of `{Name, compact Partitions of
/// {PartitionIndex INT32, ErrorCode INT16, compact nullable
/// ErrorMessage, compact ActiveProducers of {ProducerId INT64,
/// ProducerEpoch INT32, LastSequence INT32, LastTimestamp INT64,
/// CoordinatorEpoch INT32, CurrentTxnStartOffset INT64, tagged},
/// tagged}, tagged}`, tagged. **ErrorCode is per-partition**, not
/// top-level. Measured independently on leftover-empty fixture topic
/// `"t"` partition `0`: the first-partition ErrorCode is the INT16
/// at **bytes 12–13**, after throttle, compact topics len, compact
/// name `"t"`, compact partitions len, and PartitionIndex — not
/// bytes 4–5. This is a partition-leader hop, not a controller hop
/// and not a transaction-coordinator hop.
pub fn encode_describe_producers_request(
    buf: &mut BytesMut,
    topic: &str,
    partitions: &[i32],
) -> crate::error::Result<()> {
    encode_describe_producers_topics_request(
        buf,
        &[DescribeProducersTopicRequest {
            name: topic.to_string(),
            partition_indexes: partitions.to_vec(),
        }],
    )
}

/// DescribeProducers Topics of N (Java `describeProducers(Collection)`).
pub fn encode_describe_producers_topics_request(
    buf: &mut BytesMut,
    topics: &[DescribeProducersTopicRequest],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.name))?;
        buf::put_array_len(buf, true, Some(t.partition_indexes.len()))?;
        for p in &t.partition_indexes {
            buf.put_i32(*p);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeProducers request (first topic).
pub fn decode_describe_producers_request<B: Buf>(buf: &mut B) -> Result<(String, Vec<i32>)> {
    let topics = decode_describe_producers_topics_request(buf)?;
    match topics.into_iter().next() {
        Some(t) => Ok((t.name, t.partition_indexes)),
        None => Ok((String::new(), Vec::new())),
    }
}

/// Decode DescribeProducers: every topic plus partition indexes.
pub fn decode_describe_producers_topics_request<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribeProducersTopicRequest>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partition_indexes = Vec::with_capacity(pn);
        for _ in 0..pn {
            partition_indexes.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribeProducersTopicRequest {
            name,
            partition_indexes,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(topics)
}

/// Encode a DescribeProducers response.
pub fn encode_describe_producers_response(
    buf: &mut BytesMut,
    resp: &DescribeProducersResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for topic in &resp.topics {
        buf::put_compact_string(buf, Some(&topic.name))?;
        buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
        for p in &topic.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_array_len(buf, true, Some(p.active_producers.len()))?;
            for prod in &p.active_producers {
                buf.put_i64(prod.producer_id);
                buf.put_i32(prod.producer_epoch);
                buf.put_i32(prod.last_sequence);
                buf.put_i64(prod.last_timestamp);
                buf.put_i32(prod.coordinator_epoch);
                buf.put_i64(prod.current_txn_start_offset);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeProducers response.
pub fn decode_describe_producers_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeProducersResponse> {
    let _th = buf::get_i32(buf)?;
    let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(tn);
    for _ in 0..tn {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_compact_string(buf)?;
            let an = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut active_producers = Vec::with_capacity(an);
            for _ in 0..an {
                let producer_id = buf::get_i64(buf)?;
                let producer_epoch = buf::get_i32(buf)?;
                let last_sequence = buf::get_i32(buf)?;
                let last_timestamp = buf::get_i64(buf)?;
                let coordinator_epoch = buf::get_i32(buf)?;
                let current_txn_start_offset = buf::get_i64(buf)?;
                buf::skip_tagged_fields(buf)?;
                active_producers.push(ActiveProducer {
                    producer_id,
                    producer_epoch,
                    last_sequence,
                    last_timestamp,
                    coordinator_epoch,
                    current_txn_start_offset,
                });
            }
            buf::skip_tagged_fields(buf)?;
            partitions.push(DescribeProducersPartition {
                partition_index,
                error_code,
                error_message,
                active_producers,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribeProducersTopic { name, partitions });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeProducersResponse { topics })
}

/// AllocateProducerIds v0 response (top-level error after throttle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateProducerIdsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// First producer id in the allocated block.
    pub producer_id_start: i64,
    /// Number of producer ids in the allocated block.
    pub producer_id_len: i32,
}

/// AllocateProducerIds v0 (flexible from v0; KIP-730).
///
/// Official Apache JSON (`apiKey: 67`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: this crate
/// targets v0, the only version a client encodes (`VERSIONS` min=max=0).
/// v0 is flexible. Request: `BrokerId` INT32, `BrokerEpoch` INT64,
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32,
/// `ErrorCode` INT16, `ProducerIdStart` INT64, `ProducerIdLen` INT32,
/// tagged. There is a top-level `error_code` — 41 is that INT16 after
/// throttle (bytes 4–5). Fixture broker id/epoch only; not a live
/// cluster PID allocator.
pub fn encode_allocate_producer_ids_request(
    buf: &mut BytesMut,
    broker_id: i32,
    broker_epoch: i64,
) -> crate::error::Result<()> {
    buf.put_i32(broker_id);
    buf.put_i64(broker_epoch);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AllocateProducerIds request.
pub fn decode_allocate_producer_ids_request<B: Buf>(buf: &mut B) -> Result<(i32, i64)> {
    let broker_id = buf::get_i32(buf)?;
    let broker_epoch = buf::get_i64(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((broker_id, broker_epoch))
}

/// Encode an AllocateProducerIds response.
pub fn encode_allocate_producer_ids_response(
    buf: &mut BytesMut,
    resp: &AllocateProducerIdsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf.put_i64(resp.producer_id_start);
    buf.put_i32(resp.producer_id_len);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AllocateProducerIds response.
pub fn decode_allocate_producer_ids_response<B: Buf>(
    buf: &mut B,
) -> Result<AllocateProducerIdsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let producer_id_start = buf::get_i64(buf)?;
    let producer_id_len = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(AllocateProducerIdsResponse {
        error_code,
        producer_id_start,
        producer_id_len,
    })
}

/// One topic in a DescribeTransactions v0 transaction state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl TransactionTopic {
    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Partition indexes in this transaction.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One transactional.id result from DescribeTransactions (api 65) v0.
///
/// Java `TransactionDescription` (without `coordinatorId`, which Java
/// fills from the hop node). [`Self::transaction_start_time_ms`] is
/// `None` when the wire value is negative (Java `OptionalLong`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionState {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Kafka `transactional.id`.
    pub transactional_id: String,
    /// Transaction state name from the broker.
    pub transaction_state: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Transaction start time in milliseconds since the Unix epoch.
    pub transaction_start_time_ms: i64,
    /// Producer id, or `-1`.
    pub producer_id: i64,
    /// Producer epoch, or `-1`.
    pub producer_epoch: i16,
    /// Topics in this request or response.
    pub topics: Vec<TransactionTopic>,
}

impl TransactionState {
    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Kafka `transactional.id`.
    #[must_use]
    pub fn transactional_id(&self) -> &str {
        self.transactional_id.as_str()
    }

    /// Java `TransactionDescription.state` as the broker string.
    ///
    /// This is not a parsed Java `TransactionState` enum (`TransactionState`
    /// is this describe result).
    #[must_use]
    pub fn state(&self) -> &str {
        self.transaction_state.as_str()
    }

    /// Java `TransactionDescription.producerId`.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `TransactionDescription.producerEpoch`.
    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Java `TransactionDescription.transactionTimeoutMs`.
    #[must_use]
    pub fn transaction_timeout_ms(&self) -> i32 {
        self.transaction_timeout_ms
    }

    /// Java `TransactionDescription.transactionStartTimeMs` (`None` when the wire value is negative).
    #[must_use]
    pub fn transaction_start_time_ms(&self) -> Option<i64> {
        (self.transaction_start_time_ms >= 0).then_some(self.transaction_start_time_ms)
    }

    /// Topics and partitions in this transaction.
    #[must_use]
    pub fn topics(&self) -> &[TransactionTopic] {
        &self.topics
    }
}

/// DescribeTransactions v0 (flexible from v0; KIP-664).
///
/// Official Apache JSON (`apiKey: 65`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`) and kafka-protocol 0.18.0: this crate
/// targets v0, the only version a client encodes (`VERSIONS` min=max=0).
/// v0 is flexible. Request: compact `TransactionalIds` `[]string`,
/// tagged. No timeout field. Response: `ThrottleTimeMs` INT32, compact
/// `TransactionStates` of `{ErrorCode INT16, TransactionalId compact,
/// TransactionState compact, TransactionTimeoutMs INT32,
/// TransactionStartTimeMs INT64, ProducerId INT64, ProducerEpoch INT16,
/// Topics compact [{Topic compact, Partitions compact []INT32, tagged}],
/// tagged}`, tagged. There is no top-level `error_code` — 16 is the
/// first result ErrorCode, after throttle and the compact states length
/// (bytes 5–6 for a one-result fixture). Fixture transactional ids only.
pub fn encode_describe_transactions_request(
    buf: &mut BytesMut,
    transactional_ids: &[String],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(transactional_ids.len()))?;
    for id in transactional_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeTransactions request.
pub fn decode_describe_transactions_request<B: Buf>(buf: &mut B) -> Result<Vec<String>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ids)
}

/// Encode a DescribeTransactions response.
pub fn encode_describe_transactions_response(
    buf: &mut BytesMut,
    states: &[TransactionState],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(states.len()))?;
    for s in states {
        buf.put_i16(s.error_code);
        buf::put_compact_string(buf, Some(&s.transactional_id))?;
        buf::put_compact_string(buf, Some(&s.transaction_state))?;
        buf.put_i32(s.transaction_timeout_ms);
        buf.put_i64(s.transaction_start_time_ms);
        buf.put_i64(s.producer_id);
        buf.put_i16(s.producer_epoch);
        buf::put_array_len(buf, true, Some(s.topics.len()))?;
        for t in &s.topics {
            buf::put_compact_string(buf, Some(&t.name))?;
            buf::put_array_len(buf, true, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(*p);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeTransactions response.
pub fn decode_describe_transactions_response<B: Buf>(buf: &mut B) -> Result<Vec<TransactionState>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut states = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let transactional_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let transaction_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let transaction_timeout_ms = buf::get_i32(buf)?;
        let transaction_start_time_ms = buf::get_i64(buf)?;
        let producer_id = buf::get_i64(buf)?;
        let producer_epoch = buf::get_i16(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_compact_string(buf)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(TransactionTopic { name, partitions });
        }
        buf::skip_tagged_fields(buf)?;
        states.push(TransactionState {
            error_code,
            transactional_id,
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(states)
}

/// `true` when ListTransactions `version` is flexible (all spoken versions).
///
/// v0 is StateFilters / ProducerIdFilters plus tagged fields. v1 adds
/// DurationFilter INT64 (KIP-994; `< 0` means no duration filter). Kafka
/// 4.0 `validVersions` is `0-1`. This crate speaks 0–1. v2
/// (TransactionalIdPattern) is not spoken.
fn list_transactions_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(true),
        other => Err(Error::protocol(format!(
            "ListTransactions version {other} is not implemented"
        ))),
    }
}

/// One transactional.id listing from ListTransactions (api 66) v0–v1.
///
/// Java `TransactionListing`. This is not [`TransactionState`]
/// (DescribeTransactions api 65).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    /// Kafka `transactional.id`.
    pub transactional_id: String,
    /// Producer id, or `-1`.
    pub producer_id: i64,
    /// Transaction state name from the broker.
    pub transaction_state: String,
}

impl TransactionListing {
    /// Java `TransactionListing.transactionalId`.
    #[must_use]
    pub fn transactional_id(&self) -> &str {
        self.transactional_id.as_str()
    }

    /// Java `TransactionListing.producerId`.
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Java `TransactionListing.state` as the broker string.
    #[must_use]
    pub fn state(&self) -> &str {
        self.transaction_state.as_str()
    }
}

/// ListTransactions v0–v1 body (api 66).
///
/// Official Apache JSON (`apiKey: 66`, `validVersions: "0-1"`,
/// `flexibleVersions: "0+"`). This crate speaks 0–1. v1 is DurationFilter
/// (KIP-994). v2 TransactionalIdPattern is not spoken. v0–v1 are flexible.
/// Request: compact `StateFilters` `[]string`, compact
/// `ProducerIdFilters` `[]INT64`, `DurationFilter` INT64 on v1, tagged.
/// Response: `ThrottleTimeMs` INT32, top-level `ErrorCode` INT16,
/// compact `UnknownStateFilters` `[]string`, compact
/// `TransactionStates` of `{TransactionalId compact, ProducerId INT64,
/// TransactionState compact, tagged}`, tagged. v1 response matches v0.
/// Measured: **16 is the top-level ErrorCode at bytes 4–5**, after
/// throttle. Not a first-result field (DescribeTransactions puts 16
/// at bytes 5–6). Fixture transactional ids only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Transaction state filters the broker did not recognize.
    pub unknown_state_filters: Vec<String>,
    /// Matching transactions.
    pub transaction_states: Vec<TransactionListing>,
}

impl ListTransactionsResponse {
    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Transaction state filters the broker did not recognize.
    #[must_use]
    pub fn unknown_state_filters(&self) -> &[String] {
        &self.unknown_state_filters
    }

    /// Matching transactions.
    #[must_use]
    pub fn transaction_states(&self) -> &[TransactionListing] {
        &self.transaction_states
    }
}

/// Encode a ListTransactions v0–v1 request.
pub fn encode_list_transactions_request(
    buf: &mut BytesMut,
    version: i16,
    state_filters: &[String],
    producer_id_filters: &[i64],
    duration_ms: i64,
) -> crate::error::Result<()> {
    let _flexible = list_transactions_flexible(version)?;
    buf::put_array_len(buf, true, Some(state_filters.len()))?;
    for state in state_filters {
        buf::put_compact_string(buf, Some(state))?;
    }
    buf::put_array_len(buf, true, Some(producer_id_filters.len()))?;
    for id in producer_id_filters {
        buf.put_i64(*id);
    }
    if version >= 1 {
        buf.put_i64(duration_ms);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListTransactions request: `(states, producer_ids, duration_ms)`.
///
/// `duration_ms` is `-1` on v0 (no DurationFilter).
pub fn decode_list_transactions_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, Vec<i64>, i64)> {
    let _flexible = list_transactions_flexible(version)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut state_filters = Vec::with_capacity(n);
    for _ in 0..n {
        state_filters.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut producer_id_filters = Vec::with_capacity(pn);
    for _ in 0..pn {
        producer_id_filters.push(buf::get_i64(buf)?);
    }
    let duration_ms = if version >= 1 { buf::get_i64(buf)? } else { -1 };
    buf::skip_tagged_fields(buf)?;
    Ok((state_filters, producer_id_filters, duration_ms))
}

/// Encode a ListTransactions v0–v1 response.
pub fn encode_list_transactions_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ListTransactionsResponse,
) -> crate::error::Result<()> {
    let _flexible = list_transactions_flexible(version)?;
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.unknown_state_filters.len()))?;
    for state in &resp.unknown_state_filters {
        buf::put_compact_string(buf, Some(state))?;
    }
    buf::put_array_len(buf, true, Some(resp.transaction_states.len()))?;
    for t in &resp.transaction_states {
        buf::put_compact_string(buf, Some(&t.transactional_id))?;
        buf.put_i64(t.producer_id);
        buf::put_compact_string(buf, Some(&t.transaction_state))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListTransactions response.
pub fn decode_list_transactions_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ListTransactionsResponse> {
    let _flexible = list_transactions_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let un = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut unknown_state_filters = Vec::with_capacity(un);
    for _ in 0..un {
        unknown_state_filters.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut transaction_states = Vec::with_capacity(n);
    for _ in 0..n {
        let transactional_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let producer_id = buf::get_i64(buf)?;
        let transaction_state = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        transaction_states.push(TransactionListing {
            transactional_id,
            producer_id,
            transaction_state,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListTransactionsResponse {
        error_code,
        unknown_state_filters,
        transaction_states,
    })
}

/// UnregisterBroker v0 body (api 64, KIP-500).
///
/// Official Apache JSON (`apiKey: 64`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, listeners `broker` and `controller`) and
/// kafka-protocol 0.18.0: this crate targets v0, the only version a
/// client encodes (`VERSIONS` min=max=0). v0 is flexible. Request:
/// `BrokerId` INT32, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact nullable `ErrorMessage`, tagged.
/// Measured independently from kafka-protocol 0.18.0 (`client` encodes
/// the request; `broker` encodes the response): **41 is the top-level
/// ErrorCode at bytes 4–5**, after throttle. Not a first-result field
/// (AlterUserScramCredentials puts 41 after compact User at bytes
/// 11–12; DescribeTransactions puts the first-result code at bytes
/// 5–6). Fixture broker id only; not a live KRaft unregistration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl UnregisterBrokerResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, error_message: Option<String>) -> Self {
        Self {
            error_code,
            error_message,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// Encode an UnregisterBroker request.
pub fn encode_unregister_broker_request(
    buf: &mut BytesMut,
    broker_id: i32,
) -> crate::error::Result<()> {
    buf.put_i32(broker_id);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an UnregisterBroker request.
pub fn decode_unregister_broker_request<B: Buf>(buf: &mut B) -> Result<i32> {
    let broker_id = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(broker_id)
}

/// Encode an UnregisterBroker response.
pub fn encode_unregister_broker_response(
    buf: &mut BytesMut,
    resp: &UnregisterBrokerResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an UnregisterBroker response.
pub fn decode_unregister_broker_response<B: Buf>(buf: &mut B) -> Result<UnregisterBrokerResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(UnregisterBrokerResponse {
        error_code,
        error_message,
    })
}

/// Decode a DescribeCluster response (v0–2; flexible from v0).
///
/// v0 fills `endpoint_type` = [`ENDPOINT_TYPE_BROKERS`]. v0–v1 fill
/// `is_fenced` = `false`.
pub fn decode_describe_cluster_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ClusterDescription> {
    let _ = describe_cluster_spoken(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let endpoint_type = if version >= 1 {
        buf::get_i8(buf)?
    } else {
        ENDPOINT_TYPE_BROKERS
    };
    let cluster_id = buf::get_compact_string(buf)?;
    let controller_id = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut brokers = Vec::with_capacity(n);
    for _ in 0..n {
        let node_id = buf::get_i32(buf)?;
        let host = buf::get_compact_string(buf)?.unwrap_or_default();
        let port = buf::get_i32(buf)?;
        let rack = buf::get_compact_string(buf)?;
        let is_fenced = if version >= 2 {
            buf::get_bool(buf)?
        } else {
            false
        };
        buf::skip_tagged_fields(buf)?;
        brokers.push(DescribeClusterBroker {
            node_id,
            host,
            port,
            rack,
            is_fenced,
        });
    }
    let cluster_authorized_operations = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ClusterDescription {
        error_code,
        error_message,
        cluster_id,
        controller_id,
        endpoint_type,
        cluster_authorized_operations,
        brokers,
    })
}

/// Omitted authorized-operations bitfield (`INT32` min). Official default.
pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// One assigned topic in ConsumerGroupDescribe (api 69) Assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupTopicPartitions {
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl ConsumerGroupTopicPartitions {
    /// Construct [`Self`].
    pub fn new(topic_id: [u8; 16], topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic id (UUID), or zeros.
    #[must_use]
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partition indexes in this assignment.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// Current or target assignment for one described member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerGroupAssignment {
    /// Assigned topic partitions.
    pub topic_partitions: Vec<ConsumerGroupTopicPartitions>,
}

impl ConsumerGroupAssignment {
    /// Construct [`Self`].
    pub fn new(topic_partitions: Vec<ConsumerGroupTopicPartitions>) -> Self {
        Self { topic_partitions }
    }

    /// Assigned topic partitions.
    #[must_use]
    pub fn topic_partitions(&self) -> &[ConsumerGroupTopicPartitions] {
        &self.topic_partitions
    }
}

/// One member in a ConsumerGroupDescribe group.
///
/// Java `MemberDescription` for a [`GroupType::Consumer`] group.
/// `member_type` is v1+ (`-1` unknown, `0` classic, `1` consumer).
/// v0 decode fills `-1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMember {
    /// Group member id.
    pub member_id: String,
    /// Kafka `group.instance.id`, when static membership is set.
    pub instance_id: Option<String>,
    /// Client rack, when known.
    pub rack_id: Option<String>,
    /// Member epoch (KIP-848 / share).
    pub member_epoch: i32,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Client host as seen by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex, when present.
    pub subscribed_topic_regex: Option<String>,
    /// Assigned partitions for this member.
    pub assignment: ConsumerGroupAssignment,
    /// Target assignment (KIP-848).
    pub target_assignment: ConsumerGroupAssignment,
    /// Member type byte from the broker.
    pub member_type: i8,
}

impl ConsumerGroupMember {
    /// Construct [`Self`].
    pub fn new(
        member_id: impl Into<String>,
        member_epoch: i32,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            member_epoch,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: Vec::new(),
            subscribed_topic_regex: None,
            assignment: ConsumerGroupAssignment::default(),
            target_assignment: ConsumerGroupAssignment::default(),
            member_type: -1,
        }
    }

    /// Java `MemberDescription.consumerId`.
    #[must_use]
    pub fn member_id(&self) -> &str {
        self.member_id.as_str()
    }

    /// Java `MemberDescription.groupInstanceId`.
    #[must_use]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    /// Java `MemberDescription.clientId`.
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.client_id.as_str()
    }

    /// Java `MemberDescription.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.client_host.as_str()
    }

    /// Java `MemberDescription.assignment` (api 69 topic partitions).
    #[must_use]
    pub fn assignment(&self) -> &ConsumerGroupAssignment {
        &self.assignment
    }

    /// Java `MemberDescription.targetAssignment` (always present for api 69).
    #[must_use]
    pub fn target_assignment(&self) -> Option<&ConsumerGroupAssignment> {
        Some(&self.target_assignment)
    }

    /// Java `MemberDescription.memberEpoch` (always present for api 69).
    #[must_use]
    pub fn member_epoch(&self) -> Option<i32> {
        Some(self.member_epoch)
    }

    /// Java `MemberDescription.upgraded`.
    ///
    /// [`DescribeConsumerGroupsHandler`](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java)
    /// maps `MemberType` `-1` to empty, `1` to `true`, and any other
    /// value to `false`.
    #[must_use]
    pub fn upgraded(&self) -> Option<bool> {
        (self.member_type != -1).then_some(self.member_type == 1)
    }
}

/// One described group in ConsumerGroupDescribe (api 69).
///
/// ErrorCode sits here, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedConsumerGroup {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Kafka `group.id`.
    pub group_id: String,
    /// Group state name from the broker.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Assignor name.
    pub assignor_name: String,
    /// Group members.
    pub members: Vec<ConsumerGroupMember>,
    /// Bitfield of authorized operations, or `AUTHORIZED_OPERATIONS_OMITTED`.
    pub authorized_operations: i32,
}

impl DescribedConsumerGroup {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Group state name from the broker.
    #[must_use]
    pub fn group_state(&self) -> &str {
        self.group_state.as_str()
    }

    /// Java `ConsumerGroupDescription.groupEpoch`.
    #[must_use]
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Java `ConsumerGroupDescription.targetAssignmentEpoch`.
    #[must_use]
    pub fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Java `ConsumerGroupDescription.partitionAssignor`.
    #[must_use]
    pub fn assignor_name(&self) -> &str {
        self.assignor_name.as_str()
    }

    /// Java `ConsumerGroupDescription.members`.
    #[must_use]
    pub fn members(&self) -> &[ConsumerGroupMember] {
        &self.members
    }

    /// Bitfield of authorized operations, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }
}

/// Reject ConsumerGroupDescribe versions this crate does not speak.
///
/// Flexible from v0. Kafka 4.0 `validVersions` is `0-1`. This crate
/// speaks 0–1. Request layout is the same on v0 and v1. Response v1
/// adds `MemberType` INT8 (KIP-1099). v2+ is not spoken.
fn consumer_group_describe_spoken(version: i16) -> Result<()> {
    match version {
        0..=1 => Ok(()),
        other => Err(Error::protocol(format!(
            "ConsumerGroupDescribe version {other} is not implemented"
        ))),
    }
}

/// ConsumerGroupDescribe v0–1 (flexible from v0; KIP-848 / KIP-1099).
///
/// Official Apache JSON (`apiKey: 69`, `validVersions: "0-1"`,
/// `flexibleVersions: "0+"`, request listeners `broker`) and
/// kafka-protocol 0.18.0 (`ConsumerGroupDescribeRequest` /
/// `ConsumerGroupDescribeResponse`, `VERSIONS` min=0 max=1). Request
/// encode used `features = ["client"]`; response encode used `broker`.
/// v0 is the same request as v1. Response v1 adds `MemberType`.
/// Request: compact `GroupIds`, `IncludeAuthorizedOperations` BOOLEAN,
/// tagged. Response: `ThrottleTimeMs` INT32, compact `Groups` of
/// `{ErrorCode INT16, compact nullable ErrorMessage, GroupId, GroupState,
/// GroupEpoch INT32, AssignmentEpoch INT32, AssignorName, compact
/// Members of {MemberId, compact nullable InstanceId, compact nullable
/// RackId, MemberEpoch INT32, ClientId, ClientHost, compact
/// SubscribedTopicNames, compact nullable SubscribedTopicRegex,
/// Assignment, TargetAssignment, MemberType INT8 (v1+), tagged},
/// AuthorizedOperations INT32, tagged}`, tagged. Assignment is compact
/// TopicPartitions of `{TopicId UUID, TopicName, compact Partitions
/// INT32[], tagged}`. **ErrorCode is per-group**, the first field of
/// each DescribedGroup — not a top-level code after throttle. Measured
/// independently on leftover-empty fixture group `"g"`: the first-group
/// ErrorCode is the INT16 at **bytes 5–6**, after throttle and the
/// compact groups length — not bytes 4–5 (DescribeClientQuotas) or
/// 12–13 (DescribeProducers first partition). Official response
/// supported errors include `NOT_COORDINATOR`. This is a
/// group-coordinator hop, not a controller hop and not a
/// partition-leader hop.
pub fn encode_consumer_group_describe_request(
    buf: &mut BytesMut,
    version: i16,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    consumer_group_describe_spoken(version)?;
    buf::put_array_len(buf, true, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_compact_string(buf, Some(id))?;
    }
    buf.put_u8(u8::from(include_authorized_operations));
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ConsumerGroupDescribe request.
pub fn decode_consumer_group_describe_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, bool)> {
    consumer_group_describe_spoken(version)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let include_authorized_operations = buf::get_bool(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((group_ids, include_authorized_operations))
}

fn encode_consumer_group_assignment(
    buf: &mut BytesMut,
    assignment: &ConsumerGroupAssignment,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(assignment.topic_partitions.len()))?;
    for tp in &assignment.topic_partitions {
        buf.extend_from_slice(&tp.topic_id);
        buf::put_compact_string(buf, Some(&tp.topic_name))?;
        buf::put_array_len(buf, true, Some(tp.partitions.len()))?;
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_consumer_group_assignment<B: Buf>(buf: &mut B) -> Result<ConsumerGroupAssignment> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topic_partitions = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        topic_partitions.push(ConsumerGroupTopicPartitions {
            topic_id,
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupAssignment { topic_partitions })
}

fn encode_consumer_group_member(
    buf: &mut BytesMut,
    version: i16,
    member: &ConsumerGroupMember,
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(&member.member_id))?;
    buf::put_compact_string(buf, member.instance_id.as_deref())?;
    buf::put_compact_string(buf, member.rack_id.as_deref())?;
    buf.put_i32(member.member_epoch);
    buf::put_compact_string(buf, Some(&member.client_id))?;
    buf::put_compact_string(buf, Some(&member.client_host))?;
    buf::put_array_len(buf, true, Some(member.subscribed_topic_names.len()))?;
    for name in &member.subscribed_topic_names {
        buf::put_compact_string(buf, Some(name))?;
    }
    buf::put_compact_string(buf, member.subscribed_topic_regex.as_deref())?;
    encode_consumer_group_assignment(buf, &member.assignment)?;
    encode_consumer_group_assignment(buf, &member.target_assignment)?;
    if version >= 1 {
        buf.put_i8(member.member_type);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

fn decode_consumer_group_member<B: Buf>(buf: &mut B, version: i16) -> Result<ConsumerGroupMember> {
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let instance_id = buf::get_compact_string(buf)?;
    let rack_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let client_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let client_host = buf::get_compact_string(buf)?.unwrap_or_default();
    let sn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut subscribed_topic_names = Vec::with_capacity(sn);
    for _ in 0..sn {
        subscribed_topic_names.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    let subscribed_topic_regex = buf::get_compact_string(buf)?;
    let assignment = decode_consumer_group_assignment(buf)?;
    let target_assignment = decode_consumer_group_assignment(buf)?;
    let member_type = if version >= 1 { buf::get_i8(buf)? } else { -1 };
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupMember {
        member_id,
        instance_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        subscribed_topic_regex,
        assignment,
        target_assignment,
        member_type,
    })
}

/// Encode a ConsumerGroupDescribe response (v0–1). MemberType is v1+.
pub fn encode_consumer_group_describe_response(
    buf: &mut BytesMut,
    version: i16,
    groups: &[DescribedConsumerGroup],
) -> crate::error::Result<()> {
    consumer_group_describe_spoken(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_compact_string(buf, Some(&g.group_state))?;
        buf.put_i32(g.group_epoch);
        buf.put_i32(g.assignment_epoch);
        buf::put_compact_string(buf, Some(&g.assignor_name))?;
        buf::put_array_len(buf, true, Some(g.members.len()))?;
        for m in &g.members {
            encode_consumer_group_member(buf, version, m)?;
        }
        buf.put_i32(g.authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ConsumerGroupDescribe response.
pub fn decode_consumer_group_describe_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DescribedConsumerGroup>> {
    consumer_group_describe_spoken(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_state = buf::get_compact_string(buf)?.unwrap_or_default();
        let group_epoch = buf::get_i32(buf)?;
        let assignment_epoch = buf::get_i32(buf)?;
        let assignor_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_consumer_group_member(buf, version)?);
        }
        let authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedConsumerGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One member in a classic DescribeGroups (api 15) group.
///
/// Java `MemberDescription` for a [`GroupType::Classic`] group.
/// `group_instance_id` is v4+ (nullable). Metadata and assignment are
/// protocol bytes, not a parsed member store. Java deserializes
/// assignment with `ConsumerProtocol.deserializeAssignment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    /// Group member id.
    pub member_id: String,
    /// Kafka `group.instance.id`, when static membership is set.
    pub group_instance_id: Option<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Client host as seen by the broker.
    pub client_host: String,
    /// Classic JoinGroup member metadata bytes.
    pub member_metadata: Vec<u8>,
    /// Classic SyncGroup assignment bytes.
    pub member_assignment: Vec<u8>,
}

impl DescribedGroupMember {
    /// Construct [`Self`].
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            group_instance_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            member_metadata: Vec::new(),
            member_assignment: Vec::new(),
        }
    }

    /// Java `MemberDescription.consumerId`.
    #[must_use]
    pub fn member_id(&self) -> &str {
        self.member_id.as_str()
    }

    /// Java `MemberDescription.groupInstanceId`.
    #[must_use]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Java `MemberDescription.clientId`.
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.client_id.as_str()
    }

    /// Java `MemberDescription.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.client_host.as_str()
    }

    /// Classic SyncGroup assignment bytes (Java `MemberDescription.assignment`).
    #[must_use]
    pub fn assignment(&self) -> &[u8] {
        &self.member_assignment
    }

    /// Java `MemberDescription.targetAssignment` (empty for CLASSIC groups).
    #[must_use]
    pub fn target_assignment(&self) -> Option<&[u8]> {
        None
    }

    /// Java `MemberDescription.memberEpoch` (empty for CLASSIC groups).
    #[must_use]
    pub fn member_epoch(&self) -> Option<i32> {
        None
    }

    /// Java `MemberDescription.upgraded` (empty for CLASSIC groups).
    #[must_use]
    pub fn upgraded(&self) -> Option<bool> {
        None
    }
}

/// One described group in DescribeGroups (api 15).
///
/// ErrorCode sits here, not at the top of the response body.
/// `error_message` is v6+ (nullable). `authorized_operations` is v3+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Kafka `group.id`.
    pub group_id: String,
    /// Group state name from the broker.
    pub group_state: String,
    /// Group protocol type (for example `consumer`).
    pub protocol_type: String,
    /// Classic group protocol data.
    pub protocol_data: String,
    /// Group members.
    pub members: Vec<DescribedGroupMember>,
    /// Bitfield of authorized operations, or `AUTHORIZED_OPERATIONS_OMITTED`.
    pub authorized_operations: i32,
}

impl DescribedGroup {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Group state name from the broker.
    #[must_use]
    pub fn group_state(&self) -> &str {
        self.group_state.as_str()
    }

    /// Group protocol type (for example `consumer`).
    #[must_use]
    pub fn protocol_type(&self) -> &str {
        self.protocol_type.as_str()
    }

    /// Java `ConsumerGroupDescription.partitionAssignor` (classic `ProtocolData`).
    #[must_use]
    pub fn protocol_data(&self) -> &str {
        self.protocol_data.as_str()
    }

    /// Java `ConsumerGroupDescription.members`.
    #[must_use]
    pub fn members(&self) -> &[DescribedGroupMember] {
        &self.members
    }

    /// Bitfield of authorized operations, or [`AUTHORIZED_OPERATIONS_OMITTED`].
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Java `ConsumerGroupDescription.isSimpleConsumerGroup` (empty protocol type).
    #[must_use]
    pub fn is_simple_consumer_group(&self) -> bool {
        self.protocol_type.is_empty()
    }
}

/// `true` when DescribeGroups `version` is flexible.
///
/// v0–v4 are classic. v5 is the first flexible version. v6 adds
/// ErrorMessage and GROUP_ID_NOT_FOUND (KIP-1043). Kafka 4.0
/// `validVersions` is `0-6`. This crate speaks 0–6. v7+ is not spoken.
fn describe_groups_flexible(version: i16) -> Result<bool> {
    match version {
        0..=4 => Ok(false),
        5..=6 => Ok(true),
        other => Err(Error::protocol(format!(
            "DescribeGroups version {other} is not implemented"
        ))),
    }
}

/// DescribeGroups v0–6 (classic through v4; flexible from v5; KIP-1043).
///
/// Official Apache JSON (`apiKey: 15`, request `listeners: ["broker"]`,
/// `validVersions: "0-6"`, `flexibleVersions: "5+"`) and
/// kafka-protocol 0.18.0 (`DescribeGroupsRequest` /
/// `DescribeGroupsResponse`, `VERSIONS` min=0 max=6). Request encode
/// used `features = ["client"]`; response encode used `broker`.
/// Request: `Groups` (classic STRING[] through v4; compact v5+),
/// `IncludeAuthorizedOperations` BOOLEAN (v3+), tagged (v5+).
/// Response: `ThrottleTimeMs` INT32 (v1+), `Groups` of `{ErrorCode INT16,
/// nullable ErrorMessage (v6+), GroupId, GroupState, ProtocolType,
/// ProtocolData, Members of {MemberId, nullable GroupInstanceId (v4+),
/// ClientId, ClientHost, MemberMetadata BYTES, MemberAssignment BYTES,
/// tagged (v5+)}, AuthorizedOperations INT32 (v3+), tagged (v5+)}`,
/// tagged (v5+). **ErrorCode is per-group**, the first field of each
/// DescribedGroup — not a top-level code after throttle. Measured
/// independently on leftover-empty fixture group `"g"` at **v6**: the
/// first-group ErrorCode is the INT16 at **bytes 5–6**, after throttle
/// and the compact groups length — not bytes 4–5 (DescribeClientQuotas
/// / v0 first-group after a classic array length) or 12–13
/// (DescribeProducers first partition). Official listed per-group
/// errors include `COORDINATOR_LOAD_IN_PROGRESS` (14),
/// `COORDINATOR_NOT_AVAILABLE` (15), `NOT_COORDINATOR` (16),
/// `AUTHORIZATION_FAILED` (29); version 6 also returns
/// `GROUP_ID_NOT_FOUND`. This is a group-coordinator hop, not a
/// controller hop and not a partition-leader hop.
pub fn encode_describe_groups_request(
    buf: &mut BytesMut,
    version: i16,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    let flexible = describe_groups_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_string(buf, flexible, Some(id))?;
    }
    if version >= 3 {
        buf.put_u8(u8::from(include_authorized_operations));
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeGroups request.
///
/// v0–v2 fill `include_authorized_operations` = `false`.
pub fn decode_describe_groups_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, bool)> {
    let flexible = describe_groups_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_string(buf, flexible)?.unwrap_or_default());
    }
    let include_authorized_operations = if version >= 3 {
        buf::get_bool(buf)?
    } else {
        false
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_ids, include_authorized_operations))
}

fn encode_described_group_member(
    buf: &mut BytesMut,
    version: i16,
    member: &DescribedGroupMember,
) -> crate::error::Result<()> {
    let flexible = describe_groups_flexible(version)?;
    buf::put_string(buf, flexible, Some(&member.member_id))?;
    if version >= 4 {
        buf::put_string(buf, flexible, member.group_instance_id.as_deref())?;
    }
    buf::put_string(buf, flexible, Some(&member.client_id))?;
    buf::put_string(buf, flexible, Some(&member.client_host))?;
    buf::put_bytes(buf, flexible, Some(&member.member_metadata))?;
    buf::put_bytes(buf, flexible, Some(&member.member_assignment))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn decode_described_group_member<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribedGroupMember> {
    let flexible = describe_groups_flexible(version)?;
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let group_instance_id = if version >= 4 {
        buf::get_string(buf, flexible)?
    } else {
        None
    };
    let client_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let client_host = buf::get_string(buf, flexible)?.unwrap_or_default();
    let member_metadata = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    let member_assignment = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribedGroupMember {
        member_id,
        group_instance_id,
        client_id,
        client_host,
        member_metadata,
        member_assignment,
    })
}

/// Encode a DescribeGroups response (v0–6).
pub fn encode_describe_groups_response(
    buf: &mut BytesMut,
    version: i16,
    groups: &[DescribedGroup],
) -> crate::error::Result<()> {
    let flexible = describe_groups_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        if version >= 6 {
            buf::put_string(buf, flexible, g.error_message.as_deref())?;
        }
        buf::put_string(buf, flexible, Some(&g.group_id))?;
        buf::put_string(buf, flexible, Some(&g.group_state))?;
        buf::put_string(buf, flexible, Some(&g.protocol_type))?;
        buf::put_string(buf, flexible, Some(&g.protocol_data))?;
        buf::put_array_len(buf, flexible, Some(g.members.len()))?;
        for m in &g.members {
            encode_described_group_member(buf, version, m)?;
        }
        if version >= 3 {
            buf.put_i32(g.authorized_operations);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeGroups response.
///
/// v0 has no throttle. v0–v2 fill `authorized_operations` =
/// [`AUTHORIZED_OPERATIONS_OMITTED`]. v0–v5 fill `error_message` =
/// `None`. v0–v3 fill each member `group_instance_id` = `None`.
pub fn decode_describe_groups_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DescribedGroup>> {
    let flexible = describe_groups_flexible(version)?;
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = if version >= 6 {
            buf::get_string(buf, flexible)?
        } else {
            None
        };
        let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let group_state = buf::get_string(buf, flexible)?.unwrap_or_default();
        let protocol_type = buf::get_string(buf, flexible)?.unwrap_or_default();
        let protocol_data = buf::get_string(buf, flexible)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_described_group_member(buf, version)?);
        }
        let authorized_operations = if version >= 3 {
            buf::get_i32(buf)?
        } else {
            AUTHORIZED_OPERATIONS_OMITTED
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        groups.push(DescribedGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(groups)
}

/// Java `org.apache.kafka.common.GroupType` (ListGroups TypesFilter).
///
/// Wire strings match Java `toString` (`Classic`, `Consumer`, `Share`).
/// [`Self::parse`] is case-insensitive. Kafka 4.1 `Streams` is not spoken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupType {
    /// Java `UNKNOWN`.
    Unknown,
    /// Java `CONSUMER` (KIP-848).
    Consumer,
    /// Java `CLASSIC`.
    Classic,
    /// Java `SHARE` (KIP-932).
    Share,
}

impl GroupType {
    /// Java `GroupType.toString()`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Consumer => "Consumer",
            Self::Classic => "Classic",
            Self::Share => "Share",
        }
    }

    /// Java `GroupType.parse` (case-insensitive; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn parse(name: &str) -> Self {
        if name.eq_ignore_ascii_case("consumer") {
            Self::Consumer
        } else if name.eq_ignore_ascii_case("classic") {
            Self::Classic
        } else if name.eq_ignore_ascii_case("share") {
            Self::Share
        } else {
            Self::Unknown
        }
    }
}

impl fmt::Display for GroupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<GroupType> for String {
    fn from(ty: GroupType) -> Self {
        ty.as_str().to_string()
    }
}

/// Java `org.apache.kafka.common.GroupState` (ListGroups StatesFilter).
///
/// Wire strings match Java `toString` (`Stable`, `PreparingRebalance`, …).
/// [`Self::parse`] is case-insensitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupState {
    /// Java `UNKNOWN`.
    Unknown,
    /// Java `PREPARING_REBALANCE`.
    PreparingRebalance,
    /// Java `COMPLETING_REBALANCE`.
    CompletingRebalance,
    /// Java `STABLE`.
    Stable,
    /// Java `DEAD`.
    Dead,
    /// Java `EMPTY`.
    Empty,
    /// Java `ASSIGNING` (KIP-848 consumer groups).
    Assigning,
    /// Java `RECONCILING` (KIP-848 consumer groups).
    Reconciling,
}

impl GroupState {
    /// Java `GroupState.toString()`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
        }
    }

    /// Java `GroupState.parse` (case-insensitive; unknown is [`Self::Unknown`]).
    #[must_use]
    pub fn parse(name: &str) -> Self {
        if name.eq_ignore_ascii_case("preparingrebalance") {
            Self::PreparingRebalance
        } else if name.eq_ignore_ascii_case("completingrebalance") {
            Self::CompletingRebalance
        } else if name.eq_ignore_ascii_case("stable") {
            Self::Stable
        } else if name.eq_ignore_ascii_case("dead") {
            Self::Dead
        } else if name.eq_ignore_ascii_case("empty") {
            Self::Empty
        } else if name.eq_ignore_ascii_case("assigning") {
            Self::Assigning
        } else if name.eq_ignore_ascii_case("reconciling") {
            Self::Reconciling
        } else {
            Self::Unknown
        }
    }

    /// Java `GroupState.groupStatesForType`. [`GroupType::Unknown`] is empty
    /// (Java throws).
    #[must_use]
    pub fn group_states_for_type(ty: GroupType) -> &'static [Self] {
        match ty {
            GroupType::Classic => &[
                Self::PreparingRebalance,
                Self::CompletingRebalance,
                Self::Stable,
                Self::Dead,
                Self::Empty,
            ],
            GroupType::Consumer => &[
                Self::PreparingRebalance,
                Self::CompletingRebalance,
                Self::Stable,
                Self::Dead,
                Self::Empty,
                Self::Assigning,
                Self::Reconciling,
            ],
            GroupType::Share => &[Self::Stable, Self::Dead, Self::Empty],
            GroupType::Unknown => &[],
        }
    }
}

impl fmt::Display for GroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<GroupState> for String {
    fn from(state: GroupState) -> Self {
        state.as_str().to_string()
    }
}

/// One listed group in ListGroups (api 16).
///
/// Java `GroupListing`. There is no per-group ErrorCode. The response
/// error sits at the top of the body (after throttle on v1+).
/// `group_state` is v4+; `group_type` is v5+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    /// Kafka `group.id`.
    pub group_id: String,
    /// Group protocol type (for example `consumer`).
    pub protocol_type: String,
    /// Group state name from the broker.
    pub group_state: String,
    /// Group type (`classic`, `consumer`, `share`, …).
    pub group_type: String,
}

impl ListedGroup {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            protocol_type: String::new(),
            group_state: String::new(),
            group_type: String::new(),
        }
    }

    /// Java `ListedGroup.groupType` as [`GroupType`].
    #[must_use]
    pub fn group_type(&self) -> GroupType {
        GroupType::parse(&self.group_type)
    }

    /// Java `GroupListing.groupState` as [`GroupState`].
    #[must_use]
    pub fn group_state(&self) -> GroupState {
        GroupState::parse(&self.group_state)
    }

    /// Java `GroupListing.groupId`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Java `GroupListing.protocol`.
    #[must_use]
    pub fn protocol(&self) -> &str {
        self.protocol_type.as_str()
    }

    /// Java `GroupListing.isSimpleConsumerGroup` (CLASSIC type and empty protocol).
    #[must_use]
    pub fn is_simple_consumer_group(&self) -> bool {
        self.group_type() == GroupType::Classic && self.protocol_type.is_empty()
    }
}

/// `true` when ListGroups `version` is flexible.
///
/// v0–v2 are classic. v3 is the first flexible version. v4 adds
/// StatesFilter / GroupState (KIP-518). v5 adds TypesFilter / GroupType
/// (KIP-848). Kafka 4.0 `validVersions` is `0-5`. This crate speaks
/// 0–5. v6+ is not spoken.
fn list_groups_flexible(version: i16) -> Result<bool> {
    match version {
        0..=2 => Ok(false),
        3..=5 => Ok(true),
        other => Err(Error::protocol(format!(
            "ListGroups version {other} is not implemented"
        ))),
    }
}

/// ListGroups v0–5 (classic through v2; flexible from v3; KIP-518 / KIP-848).
///
/// Official Apache JSON (`apiKey: 16`, request `listeners: ["broker"]`,
/// `validVersions: "0-5"`, `flexibleVersions: "3+"`) and
/// kafka-protocol 0.18.0 (`ListGroupsRequest` /
/// `ListGroupsResponse`, `VERSIONS` min=0 max=5). Request encode used
/// `features = ["client"]`; response encode used `broker`. Official
/// listed errors (`ListGroupsRequest.java`):
/// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
/// (15), `AUTHORIZATION_FAILED` (29). `NOT_COORDINATOR` (16) is **not**
/// listed. Request: empty through v2; tagged only at v3; `StatesFilter`
/// (v4+); `TypesFilter` (v5+). Response: `ThrottleTimeMs` INT32 (v1+),
/// top-level `ErrorCode` INT16, `Groups` of `{GroupId, ProtocolType,
/// GroupState (v4+), GroupType (v5+), tagged (v3+)}`, tagged (v3+).
/// **ErrorCode is top-level**, after throttle on v1+ — not a first-group
/// field. Measured independently on leftover-empty fixture group `"g"`
/// at **v5**: the top-level ErrorCode is the INT16 at **bytes 4–5** —
/// not bytes 5–6 (DescribeGroups / ConsumerGroupDescribe first-group)
/// or 12–13 (DescribeProducers first partition). This is broker-only:
/// no FindCoordinator hop, no controller hop, no partition-leader hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Groups in this response.
    pub groups: Vec<ListedGroup>,
}

/// Encode a ListGroups request (v0–5).
///
/// v0–v2 write an empty body. v3 writes tagged fields only. v4+ sends
/// `states_filter`. v5 sends `types_filter`.
pub fn encode_list_groups_request(
    buf: &mut BytesMut,
    version: i16,
    states_filter: &[String],
    types_filter: &[String],
) -> crate::error::Result<()> {
    let flexible = list_groups_flexible(version)?;
    if version >= 4 {
        buf::put_array_len(buf, flexible, Some(states_filter.len()))?;
        for state in states_filter {
            buf::put_string(buf, flexible, Some(state))?;
        }
    }
    if version >= 5 {
        buf::put_array_len(buf, flexible, Some(types_filter.len()))?;
        for ty in types_filter {
            buf::put_string(buf, flexible, Some(ty))?;
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ListGroups request.
///
/// v0–v3 fill empty `states_filter`. v0–v4 fill empty `types_filter`.
pub fn decode_list_groups_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, Vec<String>)> {
    let flexible = list_groups_flexible(version)?;
    let states_filter = if version >= 4 {
        let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut states_filter = Vec::with_capacity(n);
        for _ in 0..n {
            states_filter.push(buf::get_string(buf, flexible)?.unwrap_or_default());
        }
        states_filter
    } else {
        Vec::new()
    };
    let types_filter = if version >= 5 {
        let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut types_filter = Vec::with_capacity(tn);
        for _ in 0..tn {
            types_filter.push(buf::get_string(buf, flexible)?.unwrap_or_default());
        }
        types_filter
    } else {
        Vec::new()
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((states_filter, types_filter))
}

/// Encode a ListGroups response (v0–5).
pub fn encode_list_groups_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ListGroupsResponse,
) -> crate::error::Result<()> {
    let flexible = list_groups_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, flexible, Some(resp.groups.len()))?;
    for g in &resp.groups {
        buf::put_string(buf, flexible, Some(&g.group_id))?;
        buf::put_string(buf, flexible, Some(&g.protocol_type))?;
        if version >= 4 {
            buf::put_string(buf, flexible, Some(&g.group_state))?;
        }
        if version >= 5 {
            buf::put_string(buf, flexible, Some(&g.group_type))?;
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ListGroups response.
///
/// v0 has no throttle. v0–v3 fill `group_state` = `""`. v0–v4 fill
/// `group_type` = `""`.
pub fn decode_list_groups_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ListGroupsResponse> {
    let flexible = list_groups_flexible(version)?;
    if version >= 1 {
        let _th = buf::get_i32(buf)?;
    }
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let protocol_type = buf::get_string(buf, flexible)?.unwrap_or_default();
        let group_state = if version >= 4 {
            buf::get_string(buf, flexible)?.unwrap_or_default()
        } else {
            String::new()
        };
        let group_type = if version >= 5 {
            buf::get_string(buf, flexible)?.unwrap_or_default()
        } else {
            String::new()
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        groups.push(ListedGroup {
            group_id,
            protocol_type,
            group_state,
            group_type,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ListGroupsResponse { error_code, groups })
}

/// One deletion result in DeleteGroups (api 42).
///
/// ErrorCode sits here after GroupId, not at the top of the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletableGroupResult {
    /// Kafka `group.id`.
    pub group_id: String,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl DeletableGroupResult {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            group_id: group_id.into(),
            error_code,
        }
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// `true` when DeleteGroups `version` is flexible.
///
/// v0–v1 are classic (same request/response layout). v2 is the first
/// flexible version. Kafka 4.0 `validVersions` is `0-2`. This crate
/// speaks 0–2. v3+ is not spoken.
fn delete_groups_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "DeleteGroups version {other} is not implemented"
        ))),
    }
}

/// DeleteGroups v0–2 (classic through v1; flexible from v2).
///
/// Official Apache JSON (`apiKey: 42`, request `listeners: ["broker"]`,
/// `validVersions: "0-2"`, `flexibleVersions: "2+"`) and
/// kafka-protocol 0.18.0 (`DeleteGroupsRequest` /
/// `DeleteGroupsResponse`, `VERSIONS` min=0 max=2). Request encode
/// used `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`DeleteGroupsResponse.java`):
/// `COORDINATOR_LOAD_IN_PROGRESS` (14), `COORDINATOR_NOT_AVAILABLE`
/// (15), `NOT_COORDINATOR` (16), `INVALID_GROUP_ID` (24),
/// `GROUP_AUTHORIZATION_FAILED` (30), `NON_EMPTY_GROUP` (68),
/// `GROUP_ID_NOT_FOUND` (69). Request: `GroupsNames` (classic STRING[]
/// through v1; compact v2+), tagged (v2+). Response: `ThrottleTimeMs`
/// INT32 (v0+), `Results` of `{GroupId, ErrorCode INT16, tagged
/// (v2+)}`, tagged (v2+). **ErrorCode is per-group**, the second field
/// of each DeletableGroupResult after GroupId — not a top-level code
/// after throttle. Measured independently on leftover-empty fixture
/// group `"g"` at **v2**: the first-group ErrorCode is the INT16 at
/// **bytes 7–8**, after throttle, the compact results length, and
/// compact GroupId `"g"` — not bytes 4–5 (ListGroups /
/// DescribeClientQuotas top-level) or 5–6 (DescribeGroups /
/// ConsumerGroupDescribe first-group first field) or 12–13
/// (DescribeProducers first partition). Because `NOT_COORDINATOR` (16)
/// is listed, this is a group-coordinator hop, not a controller hop
/// and not a partition-leader hop.
pub fn encode_delete_groups_request(
    buf: &mut BytesMut,
    version: i16,
    group_ids: &[String],
) -> crate::error::Result<()> {
    let flexible = delete_groups_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_string(buf, flexible, Some(id))?;
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteGroups request.
pub fn decode_delete_groups_request<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<String>> {
    let flexible = delete_groups_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_string(buf, flexible)?.unwrap_or_default());
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(group_ids)
}

/// Encode a DeleteGroups response (v0–2). ThrottleTimeMs is present on
/// every spoken version.
pub fn encode_delete_groups_response(
    buf: &mut BytesMut,
    version: i16,
    results: &[DeletableGroupResult],
) -> crate::error::Result<()> {
    let flexible = delete_groups_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(results.len()))?;
    for r in results {
        buf::put_string(buf, flexible, Some(&r.group_id))?;
        buf.put_i16(r.error_code);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DeleteGroups response.
pub fn decode_delete_groups_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DeletableGroupResult>> {
    let flexible = delete_groups_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let error_code = buf::get_i16(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        results.push(DeletableGroupResult {
            group_id,
            error_code,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(results)
}

/// One assigned topic in ShareGroupDescribe (api 77) Assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupTopicPartitions {
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl ShareGroupTopicPartitions {
    /// Construct [`Self`].
    pub fn new(topic_id: [u8; 16], topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic id (UUID), or zeros.
    #[must_use]
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partition indexes in this assignment.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// Current assignment for one described share-group member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupAssignment {
    /// Assigned topic partitions.
    pub topic_partitions: Vec<ShareGroupTopicPartitions>,
}

impl ShareGroupAssignment {
    /// Construct [`Self`].
    pub fn new(topic_partitions: Vec<ShareGroupTopicPartitions>) -> Self {
        Self { topic_partitions }
    }

    /// Java `ShareMemberAssignment.topicPartitions`.
    #[must_use]
    pub fn topic_partitions(&self) -> &[ShareGroupTopicPartitions] {
        &self.topic_partitions
    }
}

/// One member in a ShareGroupDescribe v0–v1 group.
///
/// Java `ShareMemberDescription`. Official member fields are MemberId,
/// RackId, MemberEpoch, ClientId, ClientHost, SubscribedTopicNames,
/// Assignment. There is no InstanceId, SubscribedTopicRegex,
/// TargetAssignment, or MemberType.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMember {
    /// Group member id.
    pub member_id: String,
    /// Client rack, when known.
    pub rack_id: Option<String>,
    /// Member epoch (KIP-848 / share).
    pub member_epoch: i32,
    /// Kafka `client.id`.
    pub client_id: String,
    /// Client host as seen by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Assigned partitions for this member.
    pub assignment: ShareGroupAssignment,
}

impl ShareGroupMember {
    /// Construct [`Self`].
    pub fn new(
        member_id: impl Into<String>,
        member_epoch: i32,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            rack_id: None,
            member_epoch,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: Vec::new(),
            assignment: ShareGroupAssignment::default(),
        }
    }

    /// Java `ShareMemberDescription.consumerId`.
    #[must_use]
    pub fn member_id(&self) -> &str {
        self.member_id.as_str()
    }

    /// Java `ShareMemberDescription.clientId`.
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.client_id.as_str()
    }

    /// Java `ShareMemberDescription.host`.
    #[must_use]
    pub fn host(&self) -> &str {
        self.client_host.as_str()
    }

    /// Java `ShareMemberDescription.assignment`.
    #[must_use]
    pub fn assignment(&self) -> &ShareGroupAssignment {
        &self.assignment
    }
}

/// One described group in ShareGroupDescribe (api 77) v0–v1.
///
/// Java `ShareGroupDescription` (without `coordinator`, which Java fills
/// from the hop node). ErrorCode sits here, not at the top of the
/// response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroup {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Kafka `group.id`.
    pub group_id: String,
    /// Group state name from the broker.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Assignor name.
    pub assignor_name: String,
    /// Group members.
    pub members: Vec<ShareGroupMember>,
    /// Bitfield of authorized operations, or `AUTHORIZED_OPERATIONS_OMITTED`.
    pub authorized_operations: i32,
}

impl DescribedShareGroup {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            group_id: group_id.into(),
            group_state: String::new(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: String::new(),
            members: Vec::new(),
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Java `ShareGroupDescription.groupId`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Group state name from the broker.
    #[must_use]
    pub fn group_state(&self) -> &str {
        self.group_state.as_str()
    }

    /// Group epoch.
    #[must_use]
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Assignment epoch.
    #[must_use]
    pub fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Assignor name.
    #[must_use]
    pub fn assignor_name(&self) -> &str {
        self.assignor_name.as_str()
    }

    /// Java `ShareGroupDescription.members`.
    #[must_use]
    pub fn members(&self) -> &[ShareGroupMember] {
        &self.members
    }

    /// Java `ShareGroupDescription.authorizedOperations`.
    #[must_use]
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }
}

/// Whether `version` of ShareGroupDescribe is flexible.
///
/// Kafka 4.0 JSON (`apiKey: 77`, `validVersions: "0"`,
/// `flexibleVersions: "0+"`, `latestVersionUnstable: true`) and Kafka
/// 4.1 JSON (`validVersions: "1"` — v0 removed) use the same request
/// and response fields. This crate speaks 0–1.
fn share_group_describe_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(true),
        other => Err(Error::protocol(format!(
            "ShareGroupDescribe version {other} is not implemented"
        ))),
    }
}

/// Encode a ShareGroupDescribe request (`version` 0–1).
///
/// Official Apache JSON (`apiKey: 77`, request `listeners: ["broker"]`,
/// Kafka 4.0 `validVersions: "0"` / Kafka 4.1 `"1"`,
/// `flexibleVersions: "0+"`). kafka-protocol 0.18.0
/// (`ShareGroupDescribeRequest` / `ShareGroupDescribeResponse`)
/// `VERSIONS` min=1 max=1 names only the 4.1-stable version; Kafka 4.0
/// still advertises v0. This crate speaks 0–1. Request and response
/// fields are identical on v0 and v1. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`ShareGroupDescribeResponse.json` /
/// `ShareGroupDescribeResponse.java`): `GROUP_AUTHORIZATION_FAILED`,
/// `TOPIC_AUTHORIZATION_FAILED` (commented as v1+ in 4.1 JSON; not a
/// wire field), `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `INVALID_GROUP_ID`,
/// `GROUP_ID_NOT_FOUND`, `INVALID_REQUEST`.
/// Request: compact `GroupIds`, `IncludeAuthorizedOperations` BOOLEAN,
/// tagged. Response: `ThrottleTimeMs` INT32, compact `Groups` of
/// `{ErrorCode INT16, compact nullable ErrorMessage, GroupId,
/// GroupState, GroupEpoch INT32, AssignmentEpoch INT32, AssignorName,
/// compact Members of {MemberId, compact nullable RackId, MemberEpoch
/// INT32, ClientId, ClientHost, compact SubscribedTopicNames,
/// Assignment, tagged}, AuthorizedOperations INT32, tagged}`, tagged.
/// Assignment is compact TopicPartitions of `{TopicId UUID, TopicName,
/// compact Partitions INT32[], tagged}`. **ErrorCode is per-group**,
/// the first field of each DescribedGroup — not a top-level code after
/// throttle. Measured independently from kafka-protocol 0.18.0
/// (`broker` encodes the response) on leftover-empty fixture group
/// `"g"`: the first-group ErrorCode is the INT16 at **bytes 5–6**,
/// after throttle and the compact groups length — not bytes 4–5
/// (ListGroups / DescribeClientQuotas top-level), 7–8 (DeleteGroups
/// after GroupId), or 12–13 (DescribeProducers first partition).
/// Official Java `DescribeShareGroupsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the share-state
/// key `"groupId:topicId:partition"` (v6), which this API does not use.
/// Because `NOT_COORDINATOR` (16) is listed, this is a
/// share-group coordinator hop (group coordinator), not a controller
/// hop and not a partition-leader hop.
pub fn encode_share_group_describe_request(
    buf: &mut BytesMut,
    version: i16,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> crate::error::Result<()> {
    let flexible = share_group_describe_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(group_ids.len()))?;
    for id in group_ids {
        buf::put_string(buf, flexible, Some(id))?;
    }
    buf.put_u8(u8::from(include_authorized_operations));
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareGroupDescribe request (`version` 0–1).
pub fn decode_share_group_describe_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Vec<String>, bool)> {
    let flexible = share_group_describe_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut group_ids = Vec::with_capacity(n);
    for _ in 0..n {
        group_ids.push(buf::get_string(buf, flexible)?.unwrap_or_default());
    }
    let include_authorized_operations = buf::get_bool(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((group_ids, include_authorized_operations))
}

fn encode_share_group_assignment(
    buf: &mut BytesMut,
    assignment: &ShareGroupAssignment,
    flexible: bool,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, flexible, Some(assignment.topic_partitions.len()))?;
    for tp in &assignment.topic_partitions {
        buf.extend_from_slice(&tp.topic_id);
        buf::put_string(buf, flexible, Some(&tp.topic_name))?;
        buf::put_array_len(buf, flexible, Some(tp.partitions.len()))?;
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn decode_share_group_assignment<B: Buf>(
    buf: &mut B,
    flexible: bool,
) -> Result<ShareGroupAssignment> {
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut topic_partitions = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let topic_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        topic_partitions.push(ShareGroupTopicPartitions {
            topic_id,
            topic_name,
            partitions,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ShareGroupAssignment { topic_partitions })
}

fn encode_share_group_member(
    buf: &mut BytesMut,
    member: &ShareGroupMember,
    flexible: bool,
) -> crate::error::Result<()> {
    buf::put_string(buf, flexible, Some(&member.member_id))?;
    buf::put_string(buf, flexible, member.rack_id.as_deref())?;
    buf.put_i32(member.member_epoch);
    buf::put_string(buf, flexible, Some(&member.client_id))?;
    buf::put_string(buf, flexible, Some(&member.client_host))?;
    buf::put_array_len(buf, flexible, Some(member.subscribed_topic_names.len()))?;
    for name in &member.subscribed_topic_names {
        buf::put_string(buf, flexible, Some(name))?;
    }
    encode_share_group_assignment(buf, &member.assignment, flexible)?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

fn decode_share_group_member<B: Buf>(buf: &mut B, flexible: bool) -> Result<ShareGroupMember> {
    let member_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let rack_id = buf::get_string(buf, flexible)?;
    let member_epoch = buf::get_i32(buf)?;
    let client_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let client_host = buf::get_string(buf, flexible)?.unwrap_or_default();
    let sn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut subscribed_topic_names = Vec::with_capacity(sn);
    for _ in 0..sn {
        subscribed_topic_names.push(buf::get_string(buf, flexible)?.unwrap_or_default());
    }
    let assignment = decode_share_group_assignment(buf, flexible)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ShareGroupMember {
        member_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        assignment,
    })
}

/// Encode a ShareGroupDescribe response (`version` 0–1).
pub fn encode_share_group_describe_response(
    buf: &mut BytesMut,
    version: i16,
    groups: &[DescribedShareGroup],
) -> crate::error::Result<()> {
    let flexible = share_group_describe_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(groups.len()))?;
    for g in groups {
        buf.put_i16(g.error_code);
        buf::put_string(buf, flexible, g.error_message.as_deref())?;
        buf::put_string(buf, flexible, Some(&g.group_id))?;
        buf::put_string(buf, flexible, Some(&g.group_state))?;
        buf.put_i32(g.group_epoch);
        buf.put_i32(g.assignment_epoch);
        buf::put_string(buf, flexible, Some(&g.assignor_name))?;
        buf::put_array_len(buf, flexible, Some(g.members.len()))?;
        for m in &g.members {
            encode_share_group_member(buf, m, flexible)?;
        }
        buf.put_i32(g.authorized_operations);
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a ShareGroupDescribe response (`version` 0–1).
pub fn decode_share_group_describe_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<Vec<DescribedShareGroup>> {
    let flexible = share_group_describe_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_string(buf, flexible)?;
        let group_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let group_state = buf::get_string(buf, flexible)?.unwrap_or_default();
        let group_epoch = buf::get_i32(buf)?;
        let assignment_epoch = buf::get_i32(buf)?;
        let assignor_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let mn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut members = Vec::with_capacity(mn);
        for _ in 0..mn {
            members.push(decode_share_group_member(buf, flexible)?);
        }
        let authorized_operations = buf::get_i32(buf)?;
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        groups.push(DescribedShareGroup {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(groups)
}

/// One requested topic in DescribeShareGroupOffsets (api 90).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl DescribeShareGroupOffsetsTopic {
    /// Construct [`Self`].
    pub fn new(topic_name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partition indexes to describe (`[]` is none of this topic).
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One requested group in DescribeShareGroupOffsets (api 90) v0.
///
/// `topics = None` is official nullable Topics (all topic-partitions).
/// kafka-protocol 0.18.0 `Default` is `Some([])`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsGroup {
    /// Kafka `group.id`.
    pub group_id: String,
    /// Topics in this request or response.
    pub topics: Option<Vec<DescribeShareGroupOffsetsTopic>>,
}

impl DescribeShareGroupOffsetsGroup {
    /// Construct [`Self`] with empty Topics (`Some([])`, not null).
    ///
    /// Official nullable Topics `None` lists every topic-partition; use
    /// [`Self::all`].
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Some(Vec::new()),
        }
    }

    /// Official nullable Topics `None` (list every topic-partition).
    ///
    /// Kafka 4.1 `ListShareGroupOffsetsSpec` with a null `topicPartitions`
    /// collection. Kafka 4.0 `Admin.java` omits this RPC.
    #[must_use]
    pub fn all(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: None,
        }
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Requested topics (`None` is official null Topics).
    #[must_use]
    pub fn topics(&self) -> Option<&[DescribeShareGroupOffsetsTopic]> {
        self.topics.as_deref()
    }
}

/// One partition in a described share-group offsets topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsetsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Start offset.
    pub start_offset: i64,
    /// Leader epoch, or `-1`.
    pub leader_epoch: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl DescribedShareGroupOffsetsPartition {
    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Start offset.
    #[must_use]
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Leader epoch, or `-1`.
    #[must_use]
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// One topic in a described share-group offsets group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Partitions in this topic.
    pub partitions: Vec<DescribedShareGroupOffsetsPartition>,
}

impl DescribedShareGroupOffsetsTopic {
    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Topic id (UUID), or zeros.
    #[must_use]
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Per-partition offsets.
    #[must_use]
    pub fn partitions(&self) -> &[DescribedShareGroupOffsetsPartition] {
        &self.partitions
    }
}

/// One described group in DescribeShareGroupOffsets (api 90) v0.
///
/// Group-level ErrorCode sits here after GroupId and Topics, not at the
/// top of the response body and not on the first partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupOffsets {
    /// Kafka `group.id`.
    pub group_id: String,
    /// Topics in this request or response.
    pub topics: Vec<DescribedShareGroupOffsetsTopic>,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl DescribedShareGroupOffsets {
    /// Construct [`Self`].
    pub fn new(group_id: impl Into<String>, error_code: i16) -> Self {
        Self {
            group_id: group_id.into(),
            topics: Vec::new(),
            error_code,
            error_message: None,
        }
    }

    /// Kafka `group.id`.
    #[must_use]
    pub fn group_id(&self) -> &str {
        self.group_id.as_str()
    }

    /// Topics in this response.
    #[must_use]
    pub fn topics(&self) -> &[DescribedShareGroupOffsetsTopic] {
        &self.topics
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// DescribeShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 90`, request `listeners: ["broker"]`,
/// `validVersions: "0-1"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`DescribeShareGroupOffsetsRequest` /
/// `DescribeShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Official trunk v1 adds `Lag` INT64 (KIP-1226); 0.18.0 does not
/// speak it and this crate does not invent it. Request encode used
/// `features = ["client"]`; response encode used `broker`.
/// Official listed errors (`DescribeShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `INVALID_REQUEST`, `UNKNOWN_SERVER_ERROR`.
/// Request: compact `Groups` of `{GroupId, compact nullable Topics of
/// {TopicName, compact Partitions INT32[], tagged}, tagged}`, tagged.
/// Response: `ThrottleTimeMs` INT32, compact `Groups` of `{GroupId,
/// compact Topics of {TopicName, TopicId UUID, compact Partitions of
/// {PartitionIndex INT32, StartOffset INT64, LeaderEpoch INT32,
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}, tagged},
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}`, tagged.
/// **Group-level ErrorCode is per-group**, after GroupId and Topics —
/// not a top-level code after throttle and not the first-partition
/// code. Measured independently from kafka-protocol 0.18.0 (`broker`
/// encodes the response) on leftover-empty fixture group `"g"`
/// (empty Topics): the first-group ErrorCode is the INT16 at
/// **bytes 8–9**, after throttle, the compact groups length, compact
/// GroupId `"g"`, and the compact empty Topics length — not bytes
/// 4–5 (ListGroups / DescribeClientQuotas top-level), 5–6
/// (ShareGroupDescribe / DescribeGroups first-group first field),
/// 7–8 (DeleteGroups after GroupId), or 12–13 (DescribeProducers
/// first partition). First-partition ErrorCode, when a leftover-empty
/// topic `"t"` partition `0` is present, is at **byte 43** and is not
/// the hop code. Official Java `ListShareGroupOffsetsHandler` (the
/// AdminClient handler for this RPC; there is no class named
/// `DescribeShareGroupOffsetsHandler`) looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_describe_share_group_offsets_request(
    buf: &mut BytesMut,
    groups: &[DescribeShareGroupOffsetsGroup],
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_array_len(buf, true, g.topics.as_ref().map(Vec::len))?;
        if let Some(topics) = &g.topics {
            for t in topics {
                buf::put_compact_string(buf, Some(&t.topic_name))?;
                buf::put_array_len(buf, true, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeShareGroupOffsets request.
pub fn decode_describe_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribeShareGroupOffsetsGroup>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let topics = match buf::get_array_len(buf, true)? {
            None => None,
            Some(tn) => {
                let mut topics = Vec::with_capacity(tn);
                for _ in 0..tn {
                    let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
                    let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
                    let mut partitions = Vec::with_capacity(pn);
                    for _ in 0..pn {
                        partitions.push(buf::get_i32(buf)?);
                    }
                    buf::skip_tagged_fields(buf)?;
                    topics.push(DescribeShareGroupOffsetsTopic {
                        topic_name,
                        partitions,
                    });
                }
                Some(topics)
            }
        };
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribeShareGroupOffsetsGroup { group_id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// Encode a DescribeShareGroupOffsets response.
pub fn encode_describe_share_group_offsets_response(
    buf: &mut BytesMut,
    groups: &[DescribedShareGroupOffsets],
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(groups.len()))?;
    for g in groups {
        buf::put_compact_string(buf, Some(&g.group_id))?;
        buf::put_array_len(buf, true, Some(g.topics.len()))?;
        for t in &g.topics {
            buf::put_compact_string(buf, Some(&t.topic_name))?;
            buf.extend_from_slice(&t.topic_id);
            buf::put_array_len(buf, true, Some(t.partitions.len()))?;
            for p in &t.partitions {
                buf.put_i32(p.partition_index);
                buf.put_i64(p.start_offset);
                buf.put_i32(p.leader_epoch);
                buf.put_i16(p.error_code);
                buf::put_compact_string(buf, p.error_message.as_deref())?;
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf.put_i16(g.error_code);
        buf::put_compact_string(buf, g.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeShareGroupOffsets response.
pub fn decode_describe_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<Vec<DescribedShareGroupOffsets>> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut groups = Vec::with_capacity(n);
    for _ in 0..n {
        let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let start_offset = buf::get_i64(buf)?;
                let leader_epoch = buf::get_i32(buf)?;
                let error_code = buf::get_i16(buf)?;
                let error_message = buf::get_compact_string(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(DescribedShareGroupOffsetsPartition {
                    partition_index,
                    start_offset,
                    leader_epoch,
                    error_code,
                    error_message,
                });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(DescribedShareGroupOffsetsTopic {
                topic_name,
                topic_id,
                partitions,
            });
        }
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        groups.push(DescribedShareGroupOffsets {
            group_id,
            topics,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(groups)
}

/// One requested partition in AlterShareGroupOffsets (api 91).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Start offset.
    pub start_offset: i64,
}

impl AlterShareGroupOffsetsPartition {
    /// Construct [`Self`].
    pub fn new(partition_index: i32, start_offset: i64) -> Self {
        Self {
            partition_index,
            start_offset,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Start offset.
    #[must_use]
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }
}

/// One requested topic in AlterShareGroupOffsets (api 91) v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partitions in this topic.
    pub partitions: Vec<AlterShareGroupOffsetsPartition>,
}

impl AlterShareGroupOffsetsTopic {
    /// Construct [`Self`].
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<AlterShareGroupOffsetsPartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partitions to alter.
    #[must_use]
    pub fn partitions(&self) -> &[AlterShareGroupOffsetsPartition] {
        &self.partitions
    }
}

/// One partition in an altered share-group offsets topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsetsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl AlteredShareGroupOffsetsPartition {
    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// One topic in an AlterShareGroupOffsets (api 91) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Partitions in this topic.
    pub partitions: Vec<AlteredShareGroupOffsetsPartition>,
}

impl AlteredShareGroupOffsetsTopic {
    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Topic id (UUID), or zeros.
    #[must_use]
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Per-partition results.
    #[must_use]
    pub fn partitions(&self) -> &[AlteredShareGroupOffsetsPartition] {
        &self.partitions
    }
}

/// AlterShareGroupOffsets (api 91) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-group field
/// and not the first-partition code. This API has a single GroupId on
/// the request and no Groups array on the response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredShareGroupOffsets {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Topics in this request or response.
    pub topics: Vec<AlteredShareGroupOffsetsTopic>,
}

impl AlteredShareGroupOffsets {
    /// Construct [`Self`].
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            topics: Vec::new(),
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Per-topic results.
    #[must_use]
    pub fn topics(&self) -> &[AlteredShareGroupOffsetsTopic] {
        &self.topics
    }
}

/// AlterShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 91`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`AlterShareGroupOffsetsRequest` /
/// `AlterShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Official listed errors
/// (`AlterShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `NON_EMPTY_GROUP`, `KAFKA_STORAGE_ERROR`, `INVALID_REQUEST`,
/// `UNKNOWN_SERVER_ERROR`.
/// Request: compact `GroupId`, compact `Topics` of `{TopicName,
/// compact Partitions of {PartitionIndex INT32, StartOffset INT64,
/// tagged}, tagged}`, tagged.
/// Response: `ThrottleTimeMs` INT32, `ErrorCode` INT16, compact
/// nullable `ErrorMessage`, compact `Responses` of `{TopicName,
/// TopicId UUID, compact Partitions of {PartitionIndex INT32,
/// ErrorCode INT16, compact nullable ErrorMessage, tagged}, tagged}`,
/// tagged.
/// **ErrorCode is top-level**, after throttle — not a first-group
/// field and not the first-partition code. Measured independently
/// from kafka-protocol 0.18.0 (`broker` encodes the response) on
/// leftover-empty fixture group `"g"` (empty Responses): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**, after throttle
/// — not bytes 5–6 (ShareGroupDescribe / DescribeGroups first-group
/// first field), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 9 bytes, so those later offsets are not present. First-
/// partition ErrorCode, when a leftover-empty topic `"t"` partition
/// `0` is present, is at **bytes 31–32** and is not the hop code.
/// Official Java `AlterShareGroupOffsetsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_alter_share_group_offsets_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[AlterShareGroupOffsetsTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i64(p.start_offset);
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterShareGroupOffsets request.
pub fn decode_alter_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, Vec<AlterShareGroupOffsetsTopic>)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let start_offset = buf::get_i64(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(AlterShareGroupOffsetsPartition {
                partition_index,
                start_offset,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(AlterShareGroupOffsetsTopic {
            topic_name,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((group_id, topics))
}

/// Encode an AlterShareGroupOffsets response.
pub fn encode_alter_share_group_offsets_response(
    buf: &mut BytesMut,
    resp: &AlteredShareGroupOffsets,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf.extend_from_slice(&t.topic_id);
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i32(p.partition_index);
            buf.put_i16(p.error_code);
            buf::put_compact_string(buf, p.error_message.as_deref())?;
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AlterShareGroupOffsets response.
pub fn decode_alter_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<AlteredShareGroupOffsets> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            let error_message = buf::get_compact_string(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(AlteredShareGroupOffsetsPartition {
                partition_index,
                error_code,
                error_message,
            });
        }
        buf::skip_tagged_fields(buf)?;
        topics.push(AlteredShareGroupOffsetsTopic {
            topic_name,
            topic_id,
            partitions,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AlteredShareGroupOffsets {
        error_code,
        error_message,
        topics,
    })
}

/// One requested topic in DeleteShareGroupOffsets (api 92).
///
/// Official request topics are topic names only — no partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
}

impl DeleteShareGroupOffsetsTopic {
    /// Construct [`Self`].
    pub fn new(topic_name: impl Into<String>) -> Self {
        Self {
            topic_name: topic_name.into(),
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }
}

/// One topic in a DeleteShareGroupOffsets (api 92) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedShareGroupOffsetsTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
}

impl DeletedShareGroupOffsetsTopic {
    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Topic id (UUID), or zeros.
    #[must_use]
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }
}

/// DeleteShareGroupOffsets (api 92) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-group field
/// and not the first-topic code. This API has a single GroupId on the
/// request and no Groups array on the response. Official request topics
/// have no partitions; the response ErrorCode after TopicName/TopicId
/// is topic-level, not partition-level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedShareGroupOffsets {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message, when present.
    pub error_message: Option<String>,
    /// Topics in this request or response.
    pub topics: Vec<DeletedShareGroupOffsetsTopic>,
}

impl DeletedShareGroupOffsets {
    /// Construct [`Self`].
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            error_message: None,
            topics: Vec::new(),
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Broker error message, when present.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Per-topic results.
    #[must_use]
    pub fn topics(&self) -> &[DeletedShareGroupOffsetsTopic] {
        &self.topics
    }
}

/// DeleteShareGroupOffsets v0 (flexible from v0; KIP-932).
///
/// Official Apache JSON (`apiKey: 92`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`) and kafka-protocol
/// 0.18.0 (`DeleteShareGroupOffsetsRequest` /
/// `DeleteShareGroupOffsetsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Official listed errors
/// (`DeleteShareGroupOffsetsResponse.json`):
/// `GROUP_AUTHORIZATION_FAILED`, `TOPIC_AUTHORIZATION_FAILED`,
/// `NOT_COORDINATOR` (16), `COORDINATOR_NOT_AVAILABLE`,
/// `COORDINATOR_LOAD_IN_PROGRESS`, `GROUP_ID_NOT_FOUND`,
/// `NON_EMPTY_GROUP`, `KAFKA_STORAGE_ERROR`, `INVALID_REQUEST`,
/// `UNKNOWN_SERVER_ERROR`, `UNKNOWN_TOPIC_OR_PARTITION`.
/// Request: compact `GroupId`, compact `Topics` of `{TopicName,
/// tagged}`, tagged. No partitions on the request.
/// Response: `ThrottleTimeMs` INT32, `ErrorCode` INT16, compact
/// nullable `ErrorMessage`, compact `Responses` of `{TopicName,
/// TopicId UUID, ErrorCode INT16, compact nullable ErrorMessage,
/// tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-group
/// field and not the first-topic code. Measured independently
/// from kafka-protocol 0.18.0 (`broker` encodes the response) on
/// leftover-empty fixture group `"g"` (empty Responses): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**, after throttle
/// — not bytes 5–6 (ShareGroupDescribe / DescribeGroups first-group
/// first field), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 9 bytes, so those later offsets are not present. First-
/// topic ErrorCode, when a leftover-empty topic `"t"` is present, is
/// at **bytes 26–27** and is not the hop code. This API has no
/// first-partition ErrorCode (official request topics are names only).
/// Official Java `DeleteShareGroupOffsetsHandler` looks up
/// `CoordinatorType.GROUP` (`FindCoordinator` `key_type=0`). Official
/// FindCoordinator JSON names SHARE (`key_type=2`) for the
/// share-state key `"groupId:topicId:partition"` (v6), which this
/// API does not use. Because `NOT_COORDINATOR` (16) is listed, this
/// is a share-group coordinator hop (group coordinator), not a
/// controller hop and not a partition-leader hop.
pub fn encode_delete_share_group_offsets_request(
    buf: &mut BytesMut,
    group_id: &str,
    topics: &[DeleteShareGroupOffsetsTopic],
) -> crate::error::Result<()> {
    buf::put_compact_string(buf, Some(group_id))?;
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for t in topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DeleteShareGroupOffsets request.
pub fn decode_delete_share_group_offsets_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, Vec<DeleteShareGroupOffsetsTopic>)> {
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        topics.push(DeleteShareGroupOffsetsTopic { topic_name });
    }
    buf::skip_tagged_fields(buf)?;
    Ok((group_id, topics))
}

/// Encode a DeleteShareGroupOffsets response.
pub fn encode_delete_share_group_offsets_response(
    buf: &mut BytesMut,
    resp: &DeletedShareGroupOffsets,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf::put_compact_string(buf, Some(&t.topic_name))?;
        buf.extend_from_slice(&t.topic_id);
        buf.put_i16(t.error_code);
        buf::put_compact_string(buf, t.error_message.as_deref())?;
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DeleteShareGroupOffsets response.
pub fn decode_delete_share_group_offsets_response<B: Buf>(
    buf: &mut B,
) -> Result<DeletedShareGroupOffsets> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let topic_id = buf::get_uuid(buf)?;
        let error_code = buf::get_i16(buf)?;
        let error_message = buf::get_compact_string(buf)?;
        buf::skip_tagged_fields(buf)?;
        topics.push(DeletedShareGroupOffsetsTopic {
            topic_name,
            topic_id,
            error_code,
            error_message,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(DeletedShareGroupOffsets {
        error_code,
        error_message,
        topics,
    })
}

/// Cursor for DescribeTopicPartitions (api 75) pagination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionCursor {
    /// Topic name.
    pub topic_name: String,
    /// Partition index.
    pub partition_index: i32,
}

impl TopicPartitionCursor {
    /// Construct [`Self`].
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }
}

/// One partition in a DescribeTopicPartitions (api 75) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartition {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker id, or `-1`.
    pub leader_id: i32,
    /// Leader epoch, or `-1`.
    pub leader_epoch: i32,
    /// Replica broker ids.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker ids.
    pub isr_nodes: Vec<i32>,
    /// Eligible leader replicas (KIP-966), when present.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// Last known eligible leader replicas, when present.
    pub last_known_elr: Option<Vec<i32>>,
    /// Offline replica broker ids.
    pub offline_replicas: Vec<i32>,
}

impl DescribedTopicPartition {
    /// Construct [`Self`].
    pub fn new(error_code: i16) -> Self {
        Self {
            error_code,
            partition_index: 0,
            leader_id: 0,
            leader_epoch: -1,
            replica_nodes: Vec::new(),
            isr_nodes: Vec::new(),
            eligible_leader_replicas: None,
            last_known_elr: None,
            offline_replicas: Vec::new(),
        }
    }
}

/// One topic in a DescribeTopicPartitions (api 75) v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartitions {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Topic, resource, group, or feature name.
    pub name: Option<String>,
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// When true, this is an internal topic.
    pub is_internal: bool,
    /// Partitions in this topic.
    pub partitions: Vec<DescribedTopicPartition>,
    /// Bitfield of authorized topic operations.
    pub topic_authorized_operations: i32,
}

impl DescribedTopicPartitions {
    /// Construct [`Self`].
    pub fn new(name: impl Into<String>, error_code: i16) -> Self {
        Self {
            error_code,
            name: Some(name.into()),
            topic_id: [0; 16],
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }
    }
}

/// DescribeTopicPartitions (api 75) v0 response body.
///
/// **There is no top-level ErrorCode.** The first ErrorCode is the
/// first-topic INT16. A first-partition ErrorCode exists only when a
/// partition is present and is later in the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResponse {
    /// Topics in this request or response.
    pub topics: Vec<DescribedTopicPartitions>,
    /// Cursor for the next page, when truncated.
    pub next_cursor: Option<TopicPartitionCursor>,
}

impl DescribeTopicPartitionsResponse {
    /// Construct [`Self`].
    pub fn new(topics: Vec<DescribedTopicPartitions>) -> Self {
        Self {
            topics,
            next_cursor: None,
        }
    }
}

fn put_compact_i32s(buf: &mut BytesMut, items: &[i32]) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(items.len()))?;
    for v in items {
        buf.put_i32(*v);
    }
    Ok(())
}

fn get_compact_i32s<B: Buf>(buf: &mut B) -> Result<Vec<i32>> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(out)
}

fn put_compact_nullable_i32s(
    buf: &mut BytesMut,
    items: Option<&[i32]>,
) -> crate::error::Result<()> {
    match items {
        None => buf::put_array_len(buf, true, None),
        Some(items) => put_compact_i32s(buf, items),
    }
}

fn get_compact_nullable_i32s<B: Buf>(buf: &mut B) -> Result<Option<Vec<i32>>> {
    let Some(n) = buf::get_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(buf::get_i32(buf)?);
    }
    Ok(Some(out))
}

fn put_nullable_cursor(
    buf: &mut BytesMut,
    cursor: Option<&TopicPartitionCursor>,
) -> crate::error::Result<()> {
    match cursor {
        None => buf.put_i8(-1),
        Some(c) => {
            buf.put_i8(1);
            buf::put_compact_string(buf, Some(&c.topic_name))?;
            buf.put_i32(c.partition_index);
            buf::put_empty_tagged_fields(buf);
        }
    }
    Ok(())
}

fn get_nullable_cursor<B: Buf>(buf: &mut B) -> Result<Option<TopicPartitionCursor>> {
    let marker = buf::get_i8(buf)?;
    if marker < 0 {
        return Ok(None);
    }
    let topic_name = buf::get_compact_string(buf)?.unwrap_or_default();
    let partition_index = buf::get_i32(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(Some(TopicPartitionCursor {
        topic_name,
        partition_index,
    }))
}

/// DescribeTopicPartitions v0 (flexible from v0; KIP-966).
///
/// Official Apache JSON (`apiKey: 75`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `DescribeTopicPartitionsRequestHandler` answers from the broker
/// `MetadataCache` (topic describe / `TOPIC_AUTHORIZATION_FAILED` /
/// `INVALID_REQUEST`); it does not look up a coordinator. Official
/// Java `DescribeTopicPartitionsRequest.getErrorResponse` writes the
/// exception code onto each topic. `NOT_COORDINATOR` (16) is **not**
/// listed. kafka-protocol 0.18.0 (`DescribeTopicPartitionsRequest` /
/// `DescribeTopicPartitionsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: compact `Topics` of `{Name, tagged}`,
/// `ResponsePartitionLimit` INT32 (default 2000), nullable `Cursor`
/// `{TopicName, PartitionIndex INT32, tagged}` (`0xff` null / `0x01`
/// present), tagged. Response: `ThrottleTimeMs` INT32, compact
/// `Topics` of `{ErrorCode INT16, compact nullable Name, TopicId UUID,
/// IsInternal BOOLEAN, compact Partitions of {ErrorCode INT16,
/// PartitionIndex INT32, LeaderId INT32, LeaderEpoch INT32, compact
/// ReplicaNodes, compact IsrNodes, compact nullable
/// EligibleLeaderReplicas, compact nullable LastKnownElr, compact
/// OfflineReplicas, tagged}, TopicAuthorizedOperations INT32,
/// tagged}`, nullable `NextCursor`, tagged.
/// **ErrorCode is first-topic**, the first field of the first topic
/// after throttle and the compact topics length — not a top-level
/// code after throttle. Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// topic `"t"` (empty Partitions): the first-topic ErrorCode is the
/// INT16 at **bytes 5–6** — not bytes 4–5 (DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level), 7–8 (DeleteGroups
/// after GroupId), 8–9 (DescribeShareGroupOffsets first-group after
/// GroupId and Topics), or 12–13 (DescribeProducers first partition).
/// This offset happens to match ShareGroupDescribe / DescribeGroups
/// first-group first field (also bytes 5–6); it was measured on this
/// API's official first-topic field, not copied. First-partition
/// ErrorCode, when leftover-empty partition `0` is present, is at
/// **bytes 27–28** and is not the first ErrorCode. Because 16 is not
/// listed, this is broker-only: no FindCoordinator, no controller
/// hop, no partition-leader hop.
pub fn encode_describe_topic_partitions_request(
    buf: &mut BytesMut,
    topics: &[String],
    response_partition_limit: i32,
    cursor: Option<&TopicPartitionCursor>,
) -> crate::error::Result<()> {
    buf::put_array_len(buf, true, Some(topics.len()))?;
    for name in topics {
        buf::put_compact_string(buf, Some(name))?;
        buf::put_empty_tagged_fields(buf);
    }
    buf.put_i32(response_partition_limit);
    put_nullable_cursor(buf, cursor)?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeTopicPartitions request.
pub fn decode_describe_topic_partitions_request<B: Buf>(
    buf: &mut B,
) -> Result<(Vec<String>, i32, Option<TopicPartitionCursor>)> {
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = buf::get_compact_string(buf)?.unwrap_or_default();
        buf::skip_tagged_fields(buf)?;
        topics.push(name);
    }
    let response_partition_limit = buf::get_i32(buf)?;
    let cursor = get_nullable_cursor(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok((topics, response_partition_limit, cursor))
}

/// Encode a DescribeTopicPartitions response.
pub fn encode_describe_topic_partitions_response(
    buf: &mut BytesMut,
    resp: &DescribeTopicPartitionsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, true, Some(resp.topics.len()))?;
    for t in &resp.topics {
        buf.put_i16(t.error_code);
        buf::put_compact_string(buf, t.name.as_deref())?;
        buf.extend_from_slice(&t.topic_id);
        buf.put_u8(u8::from(t.is_internal));
        buf::put_array_len(buf, true, Some(t.partitions.len()))?;
        for p in &t.partitions {
            buf.put_i16(p.error_code);
            buf.put_i32(p.partition_index);
            buf.put_i32(p.leader_id);
            buf.put_i32(p.leader_epoch);
            put_compact_i32s(buf, &p.replica_nodes)?;
            put_compact_i32s(buf, &p.isr_nodes)?;
            put_compact_nullable_i32s(buf, p.eligible_leader_replicas.as_deref())?;
            put_compact_nullable_i32s(buf, p.last_known_elr.as_deref())?;
            put_compact_i32s(buf, &p.offline_replicas)?;
            buf::put_empty_tagged_fields(buf);
        }
        buf.put_i32(t.topic_authorized_operations);
        buf::put_empty_tagged_fields(buf);
    }
    put_nullable_cursor(buf, resp.next_cursor.as_ref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a DescribeTopicPartitions response.
pub fn decode_describe_topic_partitions_response<B: Buf>(
    buf: &mut B,
) -> Result<DescribeTopicPartitionsResponse> {
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = buf::get_i16(buf)?;
        let name = buf::get_compact_string(buf)?;
        let topic_id = buf::get_uuid(buf)?;
        let is_internal = buf::get_bool(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let error_code = buf::get_i16(buf)?;
            let partition_index = buf::get_i32(buf)?;
            let leader_id = buf::get_i32(buf)?;
            let leader_epoch = buf::get_i32(buf)?;
            let replica_nodes = get_compact_i32s(buf)?;
            let isr_nodes = get_compact_i32s(buf)?;
            let eligible_leader_replicas = get_compact_nullable_i32s(buf)?;
            let last_known_elr = get_compact_nullable_i32s(buf)?;
            let offline_replicas = get_compact_i32s(buf)?;
            buf::skip_tagged_fields(buf)?;
            partitions.push(DescribedTopicPartition {
                error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                eligible_leader_replicas,
                last_known_elr,
                offline_replicas,
            });
        }
        let topic_authorized_operations = buf::get_i32(buf)?;
        buf::skip_tagged_fields(buf)?;
        topics.push(DescribedTopicPartitions {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        });
    }
    let next_cursor = get_nullable_cursor(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(DescribeTopicPartitionsResponse {
        topics,
        next_cursor,
    })
}

/// One listed resource in ListConfigResources (api 74).
///
/// There is no per-resource ErrorCode. The response error sits at the
/// top of the body, after throttle. `resource_type` is v1+; v0 decode
/// fills `RESOURCE_CLIENT_METRICS` (16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResource {
    /// Config resource name.
    pub resource_name: String,
    /// Config resource type (`RESOURCE_*`).
    pub resource_type: i8,
}

impl ListedConfigResource {
    /// Construct [`Self`].
    pub fn new(resource_name: impl Into<String>, resource_type: i8) -> Self {
        Self {
            resource_name: resource_name.into(),
            resource_type,
        }
    }
}

/// ListConfigResources (api 74) response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-resource
/// field and not a first-config field. Resources have no ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Matching config resources.
    pub config_resources: Vec<ListedConfigResource>,
}

impl ListConfigResourcesResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, config_resources: Vec<ListedConfigResource>) -> Self {
        Self {
            error_code,
            config_resources,
        }
    }
}

/// Reject ListConfigResources versions this crate does not speak.
///
/// Flexible from v0. Kafka 4.0 api 74 is ListClientMetricsResources
/// `validVersions` `0`. Kafka 4.1 renames the key and adds v1
/// ResourceTypes / ResourceType (KIP-1142). This crate speaks 0–1.
/// v2+ is not spoken.
fn list_config_resources_spoken(version: i16) -> Result<()> {
    match version {
        0..=1 => Ok(()),
        other => Err(Error::protocol(format!(
            "ListConfigResources version {other} is not implemented"
        ))),
    }
}

/// ListConfigResources v0–1 (flexible from v0; KIP-1142, formerly
/// ListClientMetricsResources).
///
/// Official Apache JSON (`apiKey: 74`, request `listeners: ["broker"]`,
/// `validVersions: "0-1"`, `flexibleVersions: "0+"`). Official JSON
/// lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleListConfigResources` answers from the connected
/// broker (`DESCRIBE_CONFIGS` on `CLUSTER`, then
/// `groupConfigManager` / `clientMetricsManager` / `metadataCache`).
/// Official Java `ListConfigResourcesRequest.getErrorResponse` writes
/// the exception onto the top-level `ErrorCode`. Handler-observed
/// codes: `CLUSTER_AUTHORIZATION_FAILED` (31), `UNSUPPORTED_VERSION`
/// (35). `NOT_COORDINATOR` (16) is **not** listed. kafka-protocol
/// 0.18.0 (`ListConfigResourcesRequest` /
/// `ListConfigResourcesResponse`, `VERSIONS` min=0 max=1). This crate
/// speaks 0–1. v0 is Kafka 4.0 ListClientMetricsResources (empty
/// request; response names only). v1 adds ResourceTypes / ResourceType.
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: compact `ResourceTypes` of INT8 (v1+), tagged.
/// Response: `ThrottleTimeMs` INT32, top-level `ErrorCode` INT16,
/// compact `ConfigResources` of `{compact ResourceName, ResourceType
/// INT8 (v1+), tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-resource
/// field. Resources have no ErrorCode. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture resource `"r"` type `CLIENT_METRICS` (16): the top-
/// level ErrorCode is the INT16 at **bytes 4–5** — not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe first-topic / first-
/// group), 7–8 (DeleteGroups after GroupId), 8–9
/// (DescribeShareGroupOffsets first-group after GroupId and Topics),
/// or 12–13 (DescribeProducers first partition). The leftover-empty
/// body is 12 bytes, so those later offsets are not a first ErrorCode
/// here. This offset happens to match DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level INT16; it was
/// measured on this API's official top-level field, not copied.
/// Because 16 is not listed, this is broker-only: no FindCoordinator,
/// no controller hop, no partition-leader hop.
pub fn encode_list_config_resources_request(
    buf: &mut BytesMut,
    version: i16,
    resource_types: &[i8],
) -> crate::error::Result<()> {
    list_config_resources_spoken(version)?;
    if version >= 1 {
        buf::put_array_len(buf, true, Some(resource_types.len()))?;
        for ty in resource_types {
            buf.put_i8(*ty);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListConfigResources request.
pub fn decode_list_config_resources_request<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<i8>> {
    list_config_resources_spoken(version)?;
    let resource_types = if version >= 1 {
        let n = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut resource_types = Vec::with_capacity(n);
        for _ in 0..n {
            resource_types.push(buf::get_i8(buf)?);
        }
        resource_types
    } else {
        Vec::new()
    };
    buf::skip_tagged_fields(buf)?;
    Ok(resource_types)
}

/// Encode a ListConfigResources response (v0–1). ResourceType is v1+.
pub fn encode_list_config_resources_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ListConfigResourcesResponse,
) -> crate::error::Result<()> {
    list_config_resources_spoken(version)?;
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.config_resources.len()))?;
    for r in &resp.config_resources {
        buf::put_compact_string(buf, Some(&r.resource_name))?;
        if version >= 1 {
            buf.put_i8(r.resource_type);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a ListConfigResources response.
pub fn decode_list_config_resources_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ListConfigResourcesResponse> {
    list_config_resources_spoken(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut config_resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_name = buf::get_compact_string(buf)?.unwrap_or_default();
        let resource_type = if version >= 1 {
            buf::get_i8(buf)?
        } else {
            RESOURCE_CLIENT_METRICS
        };
        buf::skip_tagged_fields(buf)?;
        config_resources.push(ListedConfigResource {
            resource_name,
            resource_type,
        });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(ListConfigResourcesResponse {
        error_code,
        config_resources,
    })
}

/// GetTelemetrySubscriptions (api 71) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-subscription
/// field and not a first-metric field. SubscriptionId is an INT32.
/// RequestedMetrics are compact strings with no ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsResponse {
    /// Top-level error code (`0` is success).
    pub error_code: i16,
    /// KIP-714 client instance UUID (Java `clientInstanceId`).
    pub client_instance_id: [u8; 16],
    /// Subscription generation.
    pub subscription_id: i32,
    /// Compression codecs the broker accepts for PushTelemetry.
    pub accepted_compression_types: Vec<i8>,
    /// How often to PushTelemetry, in milliseconds.
    pub push_interval_ms: i32,
    /// Max PushTelemetry payload size.
    pub telemetry_max_bytes: i32,
    /// When true, metric values are deltas since the last push.
    pub delta_temporality: bool,
    /// Metric names the broker wants.
    pub requested_metrics: Vec<String>,
}

impl GetTelemetrySubscriptionsResponse {
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor mirrors official GetTelemetrySubscriptionsResponse fields"
    )]
    /// Construct [`Self`].
    pub fn new(
        error_code: i16,
        client_instance_id: [u8; 16],
        subscription_id: i32,
        accepted_compression_types: Vec<i8>,
        push_interval_ms: i32,
        telemetry_max_bytes: i32,
        delta_temporality: bool,
        requested_metrics: Vec<String>,
    ) -> Self {
        Self {
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Subscription generation.
    #[must_use]
    pub fn subscription_id(&self) -> i32 {
        self.subscription_id
    }

    /// Compression codecs the broker accepts for PushTelemetry.
    #[must_use]
    pub fn accepted_compression_types(&self) -> &[i8] {
        &self.accepted_compression_types
    }

    /// How often to PushTelemetry, in milliseconds.
    #[must_use]
    pub fn push_interval_ms(&self) -> i32 {
        self.push_interval_ms
    }

    /// Max PushTelemetry payload size.
    #[must_use]
    pub fn telemetry_max_bytes(&self) -> i32 {
        self.telemetry_max_bytes
    }

    /// When true, metric values are deltas since the last push.
    #[must_use]
    pub fn delta_temporality(&self) -> bool {
        self.delta_temporality
    }

    /// Metric names the broker wants.
    #[must_use]
    pub fn requested_metrics(&self) -> &[String] {
        &self.requested_metrics
    }
}

/// GetTelemetrySubscriptions v0 (flexible from v0; KIP-714).
///
/// Official Apache JSON (`apiKey: 71`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `KafkaApis.handleGetTelemetrySubscriptionsRequest` answers from the
/// connected broker (`clientMetricsManager.processGetTelemetrySubscriptionRequest`).
/// Official Java `GetTelemetrySubscriptionsRequest.getErrorResponse`
/// writes the exception onto the top-level `ErrorCode`. Handler-observed
/// codes: `INVALID_REQUEST` (42) on catch-all, `UNSUPPORTED_VERSION`
/// (35) on the older ZooKeeper path, `THROTTLING_QUOTA_EXCEEDED` (89)
/// when a get arrives before the push interval.
/// `NOT_COORDINATOR` (16) is **not** listed. kafka-protocol 0.18.0
/// (`GetTelemetrySubscriptionsRequest` /
/// `GetTelemetrySubscriptionsResponse`, `VERSIONS` min=0 max=0). This
/// crate targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `ClientInstanceId` UUID, tagged. Response:
/// `ThrottleTimeMs` INT32, top-level `ErrorCode` INT16,
/// `ClientInstanceId` UUID, `SubscriptionId` INT32, compact
/// `AcceptedCompressionTypes` of INT8, `PushIntervalMs` INT32,
/// `TelemetryMaxBytes` INT32, `DeltaTemporality` BOOLEAN, compact
/// `RequestedMetrics` of compact STRING, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-subscription
/// field and not a first-metric field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture ClientInstanceId `[0x11; 16]`, SubscriptionId `1`,
/// accepted compression `[1]`, PushIntervalMs `1000`, TelemetryMaxBytes
/// `100`, DeltaTemporality `true`, RequestedMetrics `["m"]`, error
/// `UNSUPPORTED_VERSION` (35): the top-level ErrorCode is the INT16 at
/// **bytes 4–5** — not bytes 5–6 (DescribeTopicPartitions /
/// ShareGroupDescribe first-topic / first-group), 7–8 (DeleteGroups
/// after GroupId), 8–9 (DescribeShareGroupOffsets first-group after
/// GroupId and Topics), 12–13 (DescribeProducers first partition), or
/// 27–28 (DescribeTopicPartitions first-partition). i16=35 hits only
/// at byte 4. This offset happens to match ListConfigResources /
/// DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
/// top-level INT16; it was measured on this API's official top-level
/// field, not copied. Because 16 is not listed, this is broker-only:
/// no FindCoordinator, no controller hop, no partition-leader hop.
pub fn encode_get_telemetry_subscriptions_request(
    buf: &mut BytesMut,
    client_instance_id: &[u8; 16],
) -> crate::error::Result<()> {
    buf.extend_from_slice(client_instance_id);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a GetTelemetrySubscriptions request.
pub fn decode_get_telemetry_subscriptions_request<B: Buf>(buf: &mut B) -> Result<[u8; 16]> {
    let client_instance_id = buf::get_uuid(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(client_instance_id)
}

/// Encode a GetTelemetrySubscriptions response.
pub fn encode_get_telemetry_subscriptions_response(
    buf: &mut BytesMut,
    resp: &GetTelemetrySubscriptionsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf.extend_from_slice(&resp.client_instance_id);
    buf.put_i32(resp.subscription_id);
    buf::put_array_len(buf, true, Some(resp.accepted_compression_types.len()))?;
    for ty in &resp.accepted_compression_types {
        buf.put_i8(*ty);
    }
    buf.put_i32(resp.push_interval_ms);
    buf.put_i32(resp.telemetry_max_bytes);
    buf.put_u8(u8::from(resp.delta_temporality));
    buf::put_array_len(buf, true, Some(resp.requested_metrics.len()))?;
    for m in &resp.requested_metrics {
        buf::put_compact_string(buf, Some(m))?;
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a GetTelemetrySubscriptions response.
pub fn decode_get_telemetry_subscriptions_response<B: Buf>(
    buf: &mut B,
) -> Result<GetTelemetrySubscriptionsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let client_instance_id = buf::get_uuid(buf)?;
    let subscription_id = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut accepted_compression_types = Vec::with_capacity(n);
    for _ in 0..n {
        accepted_compression_types.push(buf::get_i8(buf)?);
    }
    let push_interval_ms = buf::get_i32(buf)?;
    let telemetry_max_bytes = buf::get_i32(buf)?;
    let delta_temporality = buf::get_bool(buf)?;
    let mn = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut requested_metrics = Vec::with_capacity(mn);
    for _ in 0..mn {
        requested_metrics.push(buf::get_compact_string(buf)?.unwrap_or_default());
    }
    buf::skip_tagged_fields(buf)?;
    Ok(GetTelemetrySubscriptionsResponse {
        error_code,
        client_instance_id,
        subscription_id,
        accepted_compression_types,
        push_interval_ms,
        telemetry_max_bytes,
        delta_temporality,
        requested_metrics,
    })
}

/// PushTelemetry (api 72) v0 request body.
///
/// Official Apache JSON (`apiKey: 72`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Metrics are compact
/// BYTES (OTLP MetricsData). There is no per-metric ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryRequest {
    /// KIP-714 client instance UUID.
    pub client_instance_id: [u8; 16],
    /// Telemetry subscription generation.
    pub subscription_id: i32,
    /// When true, this is the last PushTelemetry for the process.
    pub terminating: bool,
    /// Compression codec for the metrics payload.
    pub compression_type: i8,
    /// Encoded client metrics payload.
    pub metrics: Vec<u8>,
}

impl PushTelemetryRequest {
    /// Construct [`Self`].
    pub fn new(
        client_instance_id: [u8; 16],
        subscription_id: i32,
        terminating: bool,
        compression_type: i8,
        metrics: Vec<u8>,
    ) -> Self {
        Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        }
    }
}

/// PushTelemetry (api 72) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-metric
/// field and not a first-payload field. The response has no Metrics
/// array. Official JSON lists only `ThrottleTimeMs` and `ErrorCode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl PushTelemetryResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16) -> Self {
        Self { error_code }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// PushTelemetry v0 (flexible from v0; KIP-714).
///
/// Official Apache JSON (`apiKey: 72`, request `listeners: ["broker"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `KafkaApis.handlePushTelemetryRequest` answers from the connected
/// broker (`clientMetricsManager.processPushTelemetryRequest`).
/// Official Java `PushTelemetryRequest.getErrorResponse` writes the
/// exception onto the top-level `ErrorCode`. Handler-observed codes:
/// `INVALID_REQUEST` (42) on catch-all / reserved ClientInstanceId /
/// already-terminating, `UNKNOWN_SUBSCRIPTION_ID` (117) on subscription
/// mismatch, `THROTTLING_QUOTA_EXCEEDED` (89) when a push arrives
/// before the interval, `UNSUPPORTED_COMPRESSION_TYPE` (76) on unknown
/// compression, `TELEMETRY_TOO_LARGE` (118) when metrics exceed
/// `client.telemetry.max.bytes`, `INVALID_RECORD` (87) on plugin
/// export failure. `NOT_COORDINATOR` (16) is **not** listed.
/// kafka-protocol 0.18.0 (`PushTelemetryRequest` /
/// `PushTelemetryResponse`, `VERSIONS` min=0 max=0). This crate
/// targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `ClientInstanceId` UUID, `SubscriptionId` INT32,
/// `Terminating` BOOLEAN, `CompressionType` INT8, compact `Metrics`
/// BYTES, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-metric
/// field and not a first-payload field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on leftover-
/// empty fixture throttle `0`, error `INVALID_REQUEST` (42): the
/// top-level ErrorCode is the INT16 at **bytes 4–5**. The leftover-
/// empty body is 7 bytes, so there is no first-metric / first-payload
/// ErrorCode and no INT16 at bytes 7–8 (DeleteGroups), 8–9
/// (DescribeShareGroupOffsets), 12–13 (DescribeProducers), or 27–28
/// (DescribeTopicPartitions first-partition). i16=42 hits only at
/// byte 4. This offset happens to match GetTelemetrySubscriptions /
/// ListConfigResources / DeleteShareGroupOffsets /
/// AlterShareGroupOffsets / ListGroups top-level INT16; it was
/// measured on this API's official top-level field, not copied.
/// Because 16 is not listed, this is broker-only: no
/// FindCoordinator, no controller hop, no partition-leader hop.
pub fn encode_push_telemetry_request(
    buf: &mut BytesMut,
    req: &PushTelemetryRequest,
) -> crate::error::Result<()> {
    buf.extend_from_slice(&req.client_instance_id);
    buf.put_i32(req.subscription_id);
    buf.put_u8(u8::from(req.terminating));
    buf.put_i8(req.compression_type);
    buf::put_compact_bytes(buf, Some(&req.metrics))?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a PushTelemetry request.
pub fn decode_push_telemetry_request<B: Buf>(buf: &mut B) -> Result<PushTelemetryRequest> {
    let client_instance_id = buf::get_uuid(buf)?;
    let subscription_id = buf::get_i32(buf)?;
    let terminating = buf::get_bool(buf)?;
    let compression_type = buf::get_i8(buf)?;
    let metrics = buf::get_compact_bytes(buf)?.unwrap_or_default();
    buf::skip_tagged_fields(buf)?;
    Ok(PushTelemetryRequest {
        client_instance_id,
        subscription_id,
        terminating,
        compression_type,
        metrics,
    })
}

/// Encode a PushTelemetry response.
pub fn encode_push_telemetry_response(
    buf: &mut BytesMut,
    resp: &PushTelemetryResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a PushTelemetry response.
pub fn decode_push_telemetry_response<B: Buf>(buf: &mut B) -> Result<PushTelemetryResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(PushTelemetryResponse { error_code })
}

/// One partition in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsPartition {
    /// Partition index.
    pub partition_index: i32,
}

impl AssignReplicasToDirsPartition {
    /// Construct [`Self`].
    pub fn new(partition_index: i32) -> Self {
        Self { partition_index }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }
}

/// One topic in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsTopic {
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Partitions in this topic.
    pub partitions: Vec<AssignReplicasToDirsPartition>,
}

impl AssignReplicasToDirsTopic {
    /// Construct [`Self`].
    pub fn new(
        topic_id: impl Into<[u8; 16]>,
        partitions: Vec<AssignReplicasToDirsPartition>,
    ) -> Self {
        Self {
            topic_id: topic_id.into(),
            partitions,
        }
    }

    /// Partitions in this topic.
    #[must_use]
    pub fn partitions(&self) -> &[AssignReplicasToDirsPartition] {
        &self.partitions
    }
}

/// One directory in an AssignReplicasToDirs (api 73) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsDirectory {
    /// Directory or topic UUID.
    pub id: [u8; 16],
    /// Topics in this request or response.
    pub topics: Vec<AssignReplicasToDirsTopic>,
}

impl AssignReplicasToDirsDirectory {
    /// Construct [`Self`].
    pub fn new(id: impl Into<[u8; 16]>, topics: Vec<AssignReplicasToDirsTopic>) -> Self {
        Self {
            id: id.into(),
            topics,
        }
    }

    /// Topics in this directory.
    #[must_use]
    pub fn topics(&self) -> &[AssignReplicasToDirsTopic] {
        &self.topics
    }
}

/// AssignReplicasToDirs (api 73) v0 request body.
///
/// Official Apache JSON (`apiKey: 73`, request `listeners: ["controller"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// no `errorCodes`. Request has no ErrorCode field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsRequest {
    /// Broker id.
    pub broker_id: i32,
    /// Broker epoch.
    pub broker_epoch: i64,
    /// Log directories.
    pub directories: Vec<AssignReplicasToDirsDirectory>,
}

impl AssignReplicasToDirsRequest {
    /// Construct [`Self`].
    pub fn new(
        broker_id: i32,
        broker_epoch: i64,
        directories: Vec<AssignReplicasToDirsDirectory>,
    ) -> Self {
        Self {
            broker_id,
            broker_epoch,
            directories,
        }
    }

    /// Broker id.
    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Broker epoch.
    #[must_use]
    pub fn broker_epoch(&self) -> i64 {
        self.broker_epoch
    }

    /// Log directories.
    #[must_use]
    pub fn directories(&self) -> &[AssignReplicasToDirsDirectory] {
        &self.directories
    }
}

/// One partition in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl AssignReplicasToDirsResponsePartition {
    /// Construct [`Self`].
    pub fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// One topic in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseTopic {
    /// Topic id (UUID), or zeros.
    pub topic_id: [u8; 16],
    /// Partitions in this topic.
    pub partitions: Vec<AssignReplicasToDirsResponsePartition>,
}

impl AssignReplicasToDirsResponseTopic {
    /// Construct [`Self`].
    pub fn new(
        topic_id: impl Into<[u8; 16]>,
        partitions: Vec<AssignReplicasToDirsResponsePartition>,
    ) -> Self {
        Self {
            topic_id: topic_id.into(),
            partitions,
        }
    }

    /// Partitions in this topic.
    #[must_use]
    pub fn partitions(&self) -> &[AssignReplicasToDirsResponsePartition] {
        &self.partitions
    }
}

/// One directory in an AssignReplicasToDirs (api 73) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseDirectory {
    /// Directory or topic UUID.
    pub id: [u8; 16],
    /// Topics in this request or response.
    pub topics: Vec<AssignReplicasToDirsResponseTopic>,
}

impl AssignReplicasToDirsResponseDirectory {
    /// Construct [`Self`].
    pub fn new(id: impl Into<[u8; 16]>, topics: Vec<AssignReplicasToDirsResponseTopic>) -> Self {
        Self {
            id: id.into(),
            topics,
        }
    }

    /// Topics in this directory.
    #[must_use]
    pub fn topics(&self) -> &[AssignReplicasToDirsResponseTopic] {
        &self.topics
    }
}

/// AssignReplicasToDirs (api 73) v0 response body.
///
/// **ErrorCode is top-level**, after throttle — not a first-directory
/// field and not a first-partition field. Official JSON then lists
/// compact `Directories` with a nested per-partition ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Log directories.
    pub directories: Vec<AssignReplicasToDirsResponseDirectory>,
}

impl AssignReplicasToDirsResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, directories: Vec<AssignReplicasToDirsResponseDirectory>) -> Self {
        Self {
            error_code,
            directories,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Log directories.
    #[must_use]
    pub fn directories(&self) -> &[AssignReplicasToDirsResponseDirectory] {
        &self.directories
    }
}

/// AssignReplicasToDirs v0 (flexible from v0; KIP-858).
///
/// Official Apache JSON (`apiKey: 73`, request `listeners: ["controller"]`,
/// `validVersions: "0"`, `flexibleVersions: "0+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `AssignReplicasToDirsRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto the top-level `ErrorCode`.
/// Official Java `QuorumController.assignReplicasToDirs` is an
/// `appendWriteEvent`; `ControllerWriteEvent.run` throws
/// `ControllerExceptions.newWrongControllerException` (a
/// `NotControllerException`) when the node is not the active
/// controller, which `getErrorResponse` writes as `NOT_CONTROLLER`
/// (41) on the top-level ErrorCode. Official Java
/// `ReplicationControlManager.handleAssignReplicasToDirs` does not
/// write 41 or `NOT_COORDINATOR` (16); handler-observed per-partition
/// codes are `UNKNOWN_TOPIC_ID`, `UNKNOWN_TOPIC_OR_PARTITION`, and
/// `NOT_LEADER_OR_FOLLOWER` (6) when the broker is not a replica.
/// kafka-protocol 0.18.0 (`AssignReplicasToDirsRequest` /
/// `AssignReplicasToDirsResponse`, `VERSIONS` min=0 max=0). This crate
/// targets v0, the version a client encodes (`VERSIONS.max`).
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: `BrokerId` INT32, `BrokerEpoch` INT64, compact
/// `Directories` of `{Id UUID, Topics compact [{TopicId UUID,
/// Partitions compact [{PartitionIndex INT32, tagged}], tagged}],
/// tagged}`, tagged. Response: `ThrottleTimeMs` INT32, top-level
/// `ErrorCode` INT16, compact `Directories` of `{Id UUID, Topics
/// compact [{TopicId UUID, Partitions compact [{PartitionIndex INT32,
/// ErrorCode INT16, tagged}], tagged}], tagged}`, tagged.
/// **ErrorCode is top-level**, after throttle — not a first-directory
/// field (directory `Id` is a UUID) and not a first-partition field.
/// Measured independently from kafka-protocol 0.18.0 (`broker` encodes
/// the response) on leftover-empty fixture throttle `0`, error
/// `NOT_CONTROLLER` (41), empty `Directories`: the top-level ErrorCode
/// is the INT16 at **bytes 4–5**. The leftover-empty body is 8 bytes
/// (throttle + INT16 + compact empty array + tagged), so there is no
/// first-directory ErrorCode and no first-partition ErrorCode and no
/// INT16 at bytes 7–8 (DeleteGroups), 8–9
/// (DescribeShareGroupOffsets), 12–13 (DescribeProducers), or 27–28
/// (DescribeTopicPartitions first-partition). i16=41 hits only at
/// byte 4. A one-directory / one-topic / one-partition fixture still
/// has 41 only at bytes 4–5; the first-partition ErrorCode is at
/// bytes 45–46. This offset happens to match PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources / ListGroups
/// top-level INT16; it was measured on this API's official top-level
/// field, not copied. Because 41 is the hop written by
/// `getErrorResponse` on `NotControllerException` and listeners are
/// controller only, this is a controller hop via Metadata
/// `controller_id`. No FindCoordinator, no `key_type`, no
/// partition-leader hop on 6 (6 is a per-partition handler code and
/// is not listed in the official JSON).
pub fn encode_assign_replicas_to_dirs_request(
    buf: &mut BytesMut,
    req: &AssignReplicasToDirsRequest,
) -> crate::error::Result<()> {
    buf.put_i32(req.broker_id);
    buf.put_i64(req.broker_epoch);
    buf::put_array_len(buf, true, Some(req.directories.len()))?;
    for dir in &req.directories {
        buf.extend_from_slice(&dir.id);
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf.extend_from_slice(&topic.topic_id);
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AssignReplicasToDirs request.
pub fn decode_assign_replicas_to_dirs_request<B: Buf>(
    buf: &mut B,
) -> Result<AssignReplicasToDirsRequest> {
    let broker_id = buf::get_i32(buf)?;
    let broker_epoch = buf::get_i64(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut directories = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_uuid(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(AssignReplicasToDirsPartition { partition_index });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(AssignReplicasToDirsTopic {
                topic_id,
                partitions,
            });
        }
        buf::skip_tagged_fields(buf)?;
        directories.push(AssignReplicasToDirsDirectory { id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AssignReplicasToDirsRequest {
        broker_id,
        broker_epoch,
        directories,
    })
}

/// Encode an AssignReplicasToDirs response.
pub fn encode_assign_replicas_to_dirs_response(
    buf: &mut BytesMut,
    resp: &AssignReplicasToDirsResponse,
) -> crate::error::Result<()> {
    buf.put_i32(0);
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, true, Some(resp.directories.len()))?;
    for dir in &resp.directories {
        buf.extend_from_slice(&dir.id);
        buf::put_array_len(buf, true, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf.extend_from_slice(&topic.topic_id);
            buf::put_array_len(buf, true, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf.put_i16(part.error_code);
                buf::put_empty_tagged_fields(buf);
            }
            buf::put_empty_tagged_fields(buf);
        }
        buf::put_empty_tagged_fields(buf);
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode an AssignReplicasToDirs response.
pub fn decode_assign_replicas_to_dirs_response<B: Buf>(
    buf: &mut B,
) -> Result<AssignReplicasToDirsResponse> {
    let _th = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, true)?.unwrap_or(0);
    let mut directories = Vec::with_capacity(n);
    for _ in 0..n {
        let id = buf::get_uuid(buf)?;
        let tn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let topic_id = buf::get_uuid(buf)?;
            let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let error_code = buf::get_i16(buf)?;
                buf::skip_tagged_fields(buf)?;
                partitions.push(AssignReplicasToDirsResponsePartition {
                    partition_index,
                    error_code,
                });
            }
            buf::skip_tagged_fields(buf)?;
            topics.push(AssignReplicasToDirsResponseTopic {
                topic_id,
                partitions,
            });
        }
        buf::skip_tagged_fields(buf)?;
        directories.push(AssignReplicasToDirsResponseDirectory { id, topics });
    }
    buf::skip_tagged_fields(buf)?;
    Ok(AssignReplicasToDirsResponse {
        error_code,
        directories,
    })
}

/// One topic in an AlterReplicaLogDirs (api 34) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl AlterReplicaLogDirsTopic {
    /// Construct [`Self`].
    pub fn new(name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Partitions in this topic.
    #[must_use]
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// One directory in an AlterReplicaLogDirs (api 34) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsDirectory {
    /// Log directory path.
    pub path: String,
    /// Topics in this request or response.
    pub topics: Vec<AlterReplicaLogDirsTopic>,
}

impl AlterReplicaLogDirsDirectory {
    /// Construct [`Self`].
    pub fn new(path: impl Into<String>, topics: Vec<AlterReplicaLogDirsTopic>) -> Self {
        Self {
            path: path.into(),
            topics,
        }
    }

    /// Log directory path.
    #[must_use]
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Topics in this directory.
    #[must_use]
    pub fn topics(&self) -> &[AlterReplicaLogDirsTopic] {
        &self.topics
    }
}

/// AlterReplicaLogDirs (api 34) v1–v2 request body.
///
/// Official Apache JSON (`apiKey: 34`, request `listeners: ["broker"]`,
/// `validVersions: "1-2"`, `flexibleVersions: "2+"`). Official JSON lists
/// no `errorCodes`. Request has no ErrorCode field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsRequest {
    /// Log directories in this request.
    pub dirs: Vec<AlterReplicaLogDirsDirectory>,
}

impl AlterReplicaLogDirsRequest {
    /// Construct [`Self`].
    pub fn new(dirs: Vec<AlterReplicaLogDirsDirectory>) -> Self {
        Self { dirs }
    }

    /// Log directories in this request.
    #[must_use]
    pub fn dirs(&self) -> &[AlterReplicaLogDirsDirectory] {
        &self.dirs
    }
}

/// One partition in an AlterReplicaLogDirs (api 34) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
}

impl AlterReplicaLogDirsResponsePartition {
    /// Construct [`Self`].
    pub fn new(partition_index: i32, error_code: i16) -> Self {
        Self {
            partition_index,
            error_code,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }
}

/// One topic in an AlterReplicaLogDirs (api 34) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponseTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partitions in this topic.
    pub partitions: Vec<AlterReplicaLogDirsResponsePartition>,
}

impl AlterReplicaLogDirsResponseTopic {
    /// Construct [`Self`].
    pub fn new(
        topic_name: impl Into<String>,
        partitions: Vec<AlterReplicaLogDirsResponsePartition>,
    ) -> Self {
        Self {
            topic_name: topic_name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn topic_name(&self) -> &str {
        self.topic_name.as_str()
    }

    /// Partitions in this topic.
    #[must_use]
    pub fn partitions(&self) -> &[AlterReplicaLogDirsResponsePartition] {
        &self.partitions
    }
}

/// AlterReplicaLogDirs (api 34) v1–v2 response body.
///
/// **ErrorCode is first-partition**, not top-level and not a
/// first-directory field. Official JSON has no top-level ErrorCode;
/// throttle is followed by compact `Results` of `{TopicName,
/// Partitions of {PartitionIndex, ErrorCode}}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponse {
    /// Per-item results.
    pub results: Vec<AlterReplicaLogDirsResponseTopic>,
}

impl AlterReplicaLogDirsResponse {
    /// Construct [`Self`].
    pub fn new(results: Vec<AlterReplicaLogDirsResponseTopic>) -> Self {
        Self { results }
    }

    /// Per-item results.
    #[must_use]
    pub fn results(&self) -> &[AlterReplicaLogDirsResponseTopic] {
        &self.results
    }
}

/// `true` when AlterReplicaLogDirs `version` is flexible.
///
/// v1 is classic. v2 is the first flexible version. Kafka 4.0
/// `validVersions` is `1-2` (v0 removed). This crate speaks 1–2.
/// v0 and v3+ are not spoken.
fn alter_replica_log_dirs_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "AlterReplicaLogDirs version {other} is not implemented"
        ))),
    }
}

/// AlterReplicaLogDirs v1–2 (classic at v1; flexible from v2; KIP-113).
///
/// Official Apache JSON (`apiKey: 34`, request `listeners: ["broker"]`,
/// `validVersions: "1-2"`, `flexibleVersions: "2+"`). Official JSON lists
/// **no** `errorCodes`. Official Java
/// `AlterReplicaLogDirsRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto each **partition** ErrorCode
/// (no top-level field). Official Java
/// `KafkaApis.handleAlterReplicaLogDirsRequest` answers from the
/// connected broker (`replicaManager.alterReplicaLogDirs`); it does
/// not look up a controller or a coordinator. Auth failure uses
/// `getErrorResponse` with `CLUSTER_AUTHORIZATION_FAILED` (31).
/// Official `ReplicaManager.alterReplicaLogDirs` handler-observed
/// codes: `INVALID_TOPIC_EXCEPTION` (17) on a too-long topic name,
/// `KAFKA_STORAGE_ERROR` (56) when the destination dir or partition
/// is offline, `INVALID_REPLICA_ASSIGNMENT` (39) when the dir is
/// cordoned, `LOG_DIR_NOT_FOUND` (57), `REPLICA_NOT_AVAILABLE` (9)
/// when `getPartitionOrException` throws
/// `NotLeaderOrFollowerException` (retained as 9 for compatibility;
/// 6 is **not** written). `NOT_COORDINATOR` (16) is **not** listed.
/// `NOT_CONTROLLER` (41) is **not** listed. kafka-protocol 0.18.0
/// (`AlterReplicaLogDirsRequest` / `AlterReplicaLogDirsResponse`,
/// `VERSIONS` min=1 max=2). Kafka 4.0 max is 2; this crate speaks 1–2.
/// v0 was removed in Kafka 4.0. v3+ is not spoken. Request encode
/// used `features = ["client"]`; response encode used `broker`. Same
/// fields on v1 and v2. Request: `Dirs` of `{Path STRING, Topics
/// [{Name STRING, Partitions INT32[], tagged (v2+)}], tagged (v2+)}`,
/// tagged (v2+). Response: `ThrottleTimeMs` INT32, `Results` of
/// `{TopicName STRING, Partitions [{PartitionIndex INT32, ErrorCode
/// INT16, tagged (v2+)}], tagged (v2+)}`, tagged (v2+).
/// **ErrorCode is first-partition**, after throttle, results length,
/// topic name, partitions length, and PartitionIndex — not a
/// top-level field and not a first-directory field (request
/// directories are paths; the response has no directory array).
/// Measured independently from kafka-protocol 0.18.0 (`broker`
/// encodes the response) on leftover-empty fixture throttle `0`,
/// empty `Results` at **v2**: the leftover-empty body is **6 bytes**
/// (throttle + compact empty array + tagged) and has **no
/// ErrorCode**. On leftover-empty fixture topic `"t"` partition `0`,
/// error `CLUSTER_AUTHORIZATION_FAILED` (31) at **v2**: the
/// first-partition ErrorCode is the INT16 at **bytes 12–13**. Classic
/// **v1** places that ErrorCode later (bytes 19–20 on the same
/// fixture). i16=31 hits only at byte 12 on v2. There is no top-level
/// ErrorCode and no INT16 at bytes 4–5 (AssignReplicasToDirs /
/// PushTelemetry / GetTelemetrySubscriptions / ListConfigResources),
/// 5–6 (DescribeTopicPartitions / ShareGroupDescribe), 7–8
/// (DeleteGroups), 8–9 (DescribeShareGroupOffsets), 27–28
/// (DescribeTopicPartitions first-partition), or 45–46
/// (AssignReplicasToDirs first-partition). This offset happens to
/// match DescribeProducers first-partition INT16; it was measured
/// on this API's official first-partition field, not copied.
/// Because 41 is not listed, 16 is not listed, and 6 is converted
/// to 9 as a per-partition handler code (not a client hop), this is
/// broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop.
pub fn encode_alter_replica_log_dirs_request(
    buf: &mut BytesMut,
    version: i16,
    req: &AlterReplicaLogDirsRequest,
) -> crate::error::Result<()> {
    let flexible = alter_replica_log_dirs_flexible(version)?;
    buf::put_array_len(buf, flexible, Some(req.dirs.len()))?;
    for dir in &req.dirs {
        buf::put_string(buf, flexible, Some(&dir.path))?;
        buf::put_array_len(buf, flexible, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf::put_string(buf, flexible, Some(&topic.name))?;
            buf::put_array_len(buf, flexible, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(*part);
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterReplicaLogDirs request.
pub fn decode_alter_replica_log_dirs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<AlterReplicaLogDirsRequest> {
    let flexible = alter_replica_log_dirs_flexible(version)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut dirs = Vec::with_capacity(n);
    for _ in 0..n {
        let path = buf::get_string(buf, flexible)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_string(buf, flexible)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                partitions.push(buf::get_i32(buf)?);
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            topics.push(AlterReplicaLogDirsTopic { name, partitions });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        dirs.push(AlterReplicaLogDirsDirectory { path, topics });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(AlterReplicaLogDirsRequest { dirs })
}

/// Encode an AlterReplicaLogDirs response (v1–2).
pub fn encode_alter_replica_log_dirs_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &AlterReplicaLogDirsResponse,
) -> crate::error::Result<()> {
    let flexible = alter_replica_log_dirs_flexible(version)?;
    buf.put_i32(0);
    buf::put_array_len(buf, flexible, Some(resp.results.len()))?;
    for topic in &resp.results {
        buf::put_string(buf, flexible, Some(&topic.topic_name))?;
        buf::put_array_len(buf, flexible, Some(topic.partitions.len()))?;
        for part in &topic.partitions {
            buf.put_i32(part.partition_index);
            buf.put_i16(part.error_code);
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an AlterReplicaLogDirs response.
pub fn decode_alter_replica_log_dirs_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<AlterReplicaLogDirsResponse> {
    let flexible = alter_replica_log_dirs_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            let partition_index = buf::get_i32(buf)?;
            let error_code = buf::get_i16(buf)?;
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            partitions.push(AlterReplicaLogDirsResponsePartition {
                partition_index,
                error_code,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        results.push(AlterReplicaLogDirsResponseTopic {
            topic_name,
            partitions,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(AlterReplicaLogDirsResponse { results })
}

/// One topic in a DescribeLogDirs (api 35) request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribableLogDirTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<i32>,
}

impl DescribableLogDirTopic {
    /// Construct [`Self`].
    pub fn new(name: impl Into<String>, partitions: Vec<i32>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }
}

/// DescribeLogDirs (api 35) v1–v4 request body.
///
/// Official Apache JSON (`apiKey: 35`, request `listeners: ["broker"]`,
/// `validVersions: "1-4"`, `flexibleVersions: "2+"`). Official JSON
/// lists no `errorCodes`. Request has no ErrorCode field. `Topics` is
/// nullable: null means all topics. v5 is a named STATUS hole and is
/// not spoken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsRequest {
    /// Topics in this request or response.
    pub topics: Option<Vec<DescribableLogDirTopic>>,
}

impl DescribeLogDirsRequest {
    /// Construct [`Self`].
    pub fn new(topics: Option<Vec<DescribableLogDirTopic>>) -> Self {
        Self { topics }
    }
}

/// Java `DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES` (`-1`).
pub const UNKNOWN_VOLUME_BYTES: i64 = -1;

/// One partition in a DescribeLogDirs (api 35) response.
///
/// Java `ReplicaInfo`. Official JSON has no partition ErrorCode. Fields
/// are PartitionIndex, PartitionSize, OffsetLag, IsFutureKey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Partition log size in bytes.
    pub partition_size: i64,
    /// Offset lag on this replica.
    pub offset_lag: i64,
    /// When true, this replica is a future log directory.
    pub is_future_key: bool,
}

impl DescribeLogDirsPartition {
    /// Construct [`Self`].
    pub fn new(
        partition_index: i32,
        partition_size: i64,
        offset_lag: i64,
        is_future_key: bool,
    ) -> Self {
        Self {
            partition_index,
            partition_size,
            offset_lag,
            is_future_key,
        }
    }

    /// Partition index.
    #[must_use]
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Java `ReplicaInfo.size`.
    #[must_use]
    pub fn size(&self) -> i64 {
        self.partition_size
    }

    /// Java `ReplicaInfo.offsetLag`.
    #[must_use]
    pub fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Java `ReplicaInfo.isFuture`.
    #[must_use]
    pub fn is_future(&self) -> bool {
        self.is_future_key
    }
}

/// One topic in a DescribeLogDirs (api 35) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsTopic {
    /// Topic, resource, group, or feature name.
    pub name: String,
    /// Partitions in this topic.
    pub partitions: Vec<DescribeLogDirsPartition>,
}

impl DescribeLogDirsTopic {
    /// Construct [`Self`].
    pub fn new(name: impl Into<String>, partitions: Vec<DescribeLogDirsPartition>) -> Self {
        Self {
            name: name.into(),
            partitions,
        }
    }

    /// Topic name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Per-partition replica info in this directory.
    #[must_use]
    pub fn partitions(&self) -> &[DescribeLogDirsPartition] {
        &self.partitions
    }
}

/// One directory in a DescribeLogDirs (api 35) response.
///
/// Java `LogDirDescription`. First-directory ErrorCode is this struct's
/// `error_code`, not a first-partition field. `total_bytes` /
/// `usable_bytes` are v4 (official JSON default `-1`; decode fills `-1`
/// on v1–v3). [`Self::total_bytes`] / [`Self::usable_bytes`] are `None`
/// when the wire value is [`UNKNOWN_VOLUME_BYTES`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResult {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Log directory path.
    pub log_dir: String,
    /// Topics in this request or response.
    pub topics: Vec<DescribeLogDirsTopic>,
    /// Total bytes on this log directory.
    pub total_bytes: i64,
    /// Usable bytes on this log directory.
    pub usable_bytes: i64,
}

impl DescribeLogDirsResult {
    /// Construct [`Self`].
    pub fn new(
        error_code: i16,
        log_dir: impl Into<String>,
        topics: Vec<DescribeLogDirsTopic>,
        total_bytes: i64,
        usable_bytes: i64,
    ) -> Self {
        Self {
            error_code,
            log_dir: log_dir.into(),
            topics,
            total_bytes,
            usable_bytes,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Log directory path.
    #[must_use]
    pub fn log_dir(&self) -> &str {
        self.log_dir.as_str()
    }

    /// Topics in this log directory.
    #[must_use]
    pub fn topics(&self) -> &[DescribeLogDirsTopic] {
        &self.topics
    }

    /// Java `LogDirDescription.totalBytes` (`None` when [`UNKNOWN_VOLUME_BYTES`]).
    #[must_use]
    pub fn total_bytes(&self) -> Option<i64> {
        (self.total_bytes != UNKNOWN_VOLUME_BYTES).then_some(self.total_bytes)
    }

    /// Java `LogDirDescription.usableBytes` (`None` when [`UNKNOWN_VOLUME_BYTES`]).
    #[must_use]
    pub fn usable_bytes(&self) -> Option<i64> {
        (self.usable_bytes != UNKNOWN_VOLUME_BYTES).then_some(self.usable_bytes)
    }
}

/// DescribeLogDirs (api 35) v1–v4 response body.
///
/// **ErrorCode is top-level**, after throttle. Official JSON adds
/// top-level ErrorCode at versions `3+`. Each result also has a
/// first-directory ErrorCode. There is no first-partition ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Per-item results.
    pub results: Vec<DescribeLogDirsResult>,
}

impl DescribeLogDirsResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, results: Vec<DescribeLogDirsResult>) -> Self {
        Self {
            error_code,
            results,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Per-directory results.
    #[must_use]
    pub fn results(&self) -> &[DescribeLogDirsResult] {
        &self.results
    }
}

/// `true` when DescribeLogDirs `version` is flexible.
///
/// v1 is classic. v2–v4 are flexible. Kafka 4.0 `validVersions` is
/// `1-4` (v0 removed). This crate speaks 1–4. v0 and v5+ are not
/// spoken. v5 is a named STATUS hole.
fn describe_log_dirs_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2..=4 => Ok(true),
        other => Err(Error::protocol(format!(
            "DescribeLogDirs version {other} is not implemented"
        ))),
    }
}

/// DescribeLogDirs v1–4 (classic at v1; flexible from v2; KIP-113 /
/// KIP-784 / KIP-827).
///
/// Official Apache JSON (`apiKey: 35`, request `listeners: ["broker"]`,
/// `validVersions: "1-4"`, `flexibleVersions: "2+"`). Official JSON lists
/// **no** `errorCodes`. Official Java `KafkaApis.handleDescribeLogDirsRequest`
/// answers from the connected broker (`replicaManager.describeLogDirs`);
/// it does not look up a controller or a coordinator. Auth failure
/// writes `CLUSTER_AUTHORIZATION_FAILED` (31) onto the **top-level**
/// ErrorCode (KIP-784, v3+). Official `ReplicaManager.describeLogDirs`
/// writes `KAFKA_STORAGE_ERROR` (56) onto a **first-directory**
/// ErrorCode when that dir is offline, or `Errors.forException(t).code()`
/// for other throwables. `NOT_COORDINATOR` (16) is **not** listed.
/// `NOT_CONTROLLER` (41) is **not** listed. `NOT_LEADER_OR_FOLLOWER`
/// (6) is **not** a client hop. kafka-protocol 0.18.0
/// (`DescribeLogDirsRequest` / `DescribeLogDirsResponse`, `VERSIONS`
/// min=1 max=4). Kafka 4.0 max is 4; this crate speaks 1–4. v0 was
/// removed in Kafka 4.0. v5 is a named STATUS hole and is not spoken.
/// Request encode used `features = ["client"]`; response encode used
/// `broker`. Request: nullable `Topics` of `{Topic STRING, Partitions
/// INT32[], tagged (v2+)}`, tagged (v2+). Same request fields on
/// v1–v4. Response: `ThrottleTimeMs` INT32, **top-level `ErrorCode`
/// INT16 (v3+)**, `Results` of `{ErrorCode INT16, LogDir STRING,
/// Topics [{Name STRING, Partitions [{PartitionIndex INT32,
/// PartitionSize INT64, OffsetLag INT64, IsFutureKey BOOLEAN, tagged
/// (v2+)}], tagged (v2+)}], TotalBytes INT64 (v4+), UsableBytes INT64
/// (v4+), tagged (v2+)}`, tagged (v2+). v4 directory fields are
/// TotalBytes and UsableBytes (official JSON default `-1`; decode
/// fills `-1` on v1–v3). v1–v2 omit the top-level ErrorCode (decode
/// fills `0`). **ErrorCode is top-level on v3+**, after throttle —
/// not a first-directory field and not a first-partition field.
/// Measured independently from kafka-protocol 0.18.0 (`broker`
/// encodes the response) on leftover-empty fixture throttle `0`,
/// empty `Results`, error `CLUSTER_AUTHORIZATION_FAILED` (31) at
/// **v4**: the leftover-empty body is **8 bytes** (throttle +
/// top-level INT16 + compact empty array + tagged) and the top-level
/// ErrorCode is the INT16 at **bytes 4–5**. i16=31 hits only at byte
/// 4. v3 empty-Results matches v4 (TotalBytes/UsableBytes are
/// per-directory). v2 leftover-empty empty-Results is **6 bytes** and
/// has no top-level ErrorCode. Classic **v1** empty-Results is **8
/// bytes** (throttle + INT32 0) with no ErrorCode. On leftover-empty
/// fixture directory `"/d"` topic `"t"` partition `0`, top-level 31
/// and first-directory 0 at **v4**: the first-directory ErrorCode is
/// the INT16 at **bytes 7–8**. There is no first-partition ErrorCode.
/// Do not assume bytes 4–5 from AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official top-level field (versions 3+).
/// Not bytes 5–6 (DescribeTopicPartitions / ShareGroupDescribe), 7–8
/// as the hop/auth code (DeleteGroups after GroupId; 7–8 here is
/// first-directory, not the top-level hop field), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop.
pub fn encode_describe_log_dirs_request(
    buf: &mut BytesMut,
    version: i16,
    req: &DescribeLogDirsRequest,
) -> crate::error::Result<()> {
    let flexible = describe_log_dirs_flexible(version)?;
    match &req.topics {
        None => buf::put_array_len(buf, flexible, None)?,
        Some(topics) => {
            buf::put_array_len(buf, flexible, Some(topics.len()))?;
            for topic in topics {
                buf::put_string(buf, flexible, Some(&topic.name))?;
                buf::put_array_len(buf, flexible, Some(topic.partitions.len()))?;
                for part in &topic.partitions {
                    buf.put_i32(*part);
                }
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeLogDirs request.
pub fn decode_describe_log_dirs_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeLogDirsRequest> {
    let flexible = describe_log_dirs_flexible(version)?;
    let topics = match buf::get_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = buf::get_string(buf, flexible)?.unwrap_or_default();
                let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
                let mut partitions = Vec::with_capacity(pn);
                for _ in 0..pn {
                    partitions.push(buf::get_i32(buf)?);
                }
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                topics.push(DescribableLogDirTopic { name, partitions });
            }
            Some(topics)
        }
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribeLogDirsRequest { topics })
}

/// Encode a DescribeLogDirs response (v1–4). Top-level ErrorCode is
/// v3+. TotalBytes / UsableBytes are v4+.
pub fn encode_describe_log_dirs_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &DescribeLogDirsResponse,
) -> crate::error::Result<()> {
    let flexible = describe_log_dirs_flexible(version)?;
    buf.put_i32(0);
    if version >= 3 {
        buf.put_i16(resp.error_code);
    }
    buf::put_array_len(buf, flexible, Some(resp.results.len()))?;
    for dir in &resp.results {
        buf.put_i16(dir.error_code);
        buf::put_string(buf, flexible, Some(&dir.log_dir))?;
        buf::put_array_len(buf, flexible, Some(dir.topics.len()))?;
        for topic in &dir.topics {
            buf::put_string(buf, flexible, Some(&topic.name))?;
            buf::put_array_len(buf, flexible, Some(topic.partitions.len()))?;
            for part in &topic.partitions {
                buf.put_i32(part.partition_index);
                buf.put_i64(part.partition_size);
                buf.put_i64(part.offset_lag);
                buf.put_u8(u8::from(part.is_future_key));
                if flexible {
                    buf::put_empty_tagged_fields(buf);
                }
            }
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if version >= 4 {
            buf.put_i64(dir.total_bytes);
            buf.put_i64(dir.usable_bytes);
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeLogDirs response.
pub fn decode_describe_log_dirs_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeLogDirsResponse> {
    let flexible = describe_log_dirs_flexible(version)?;
    let _th = buf::get_i32(buf)?;
    let error_code = if version >= 3 { buf::get_i16(buf)? } else { 0 };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let dir_error = buf::get_i16(buf)?;
        let log_dir = buf::get_string(buf, flexible)?.unwrap_or_default();
        let tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut topics = Vec::with_capacity(tn);
        for _ in 0..tn {
            let name = buf::get_string(buf, flexible)?.unwrap_or_default();
            let pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                let partition_index = buf::get_i32(buf)?;
                let partition_size = buf::get_i64(buf)?;
                let offset_lag = buf::get_i64(buf)?;
                let is_future_key = buf::get_bool(buf)?;
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                partitions.push(DescribeLogDirsPartition {
                    partition_index,
                    partition_size,
                    offset_lag,
                    is_future_key,
                });
            }
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            topics.push(DescribeLogDirsTopic { name, partitions });
        }
        let (total_bytes, usable_bytes) = if version >= 4 {
            (buf::get_i64(buf)?, buf::get_i64(buf)?)
        } else {
            (-1, -1)
        };
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        results.push(DescribeLogDirsResult {
            error_code: dir_error,
            log_dir,
            topics,
            total_bytes,
            usable_bytes,
        });
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribeLogDirsResponse {
        error_code,
        results,
    })
}

/// Java `KafkaPrincipal.toString` (`type:name`).
fn kafka_principal_as_string(principal_type: &str, principal_name: &str) -> String {
    format!("{principal_type}:{principal_name}")
}

/// Java `DelegationToken.hmacAsBase64String` (standard Base64 with padding).
fn encode_hmac_as_base64(hmac: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hmac)
}

/// One renewer principal in a CreateDelegationToken (api 38) request.
///
/// Official JSON `CreatableRenewers` has PrincipalType and
/// PrincipalName only. There is no per-renewer ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableRenewer {
    /// Principal type (for example `User`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

impl CreatableRenewer {
    /// Construct [`Self`].
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Java `KafkaPrincipal.getPrincipalType`.
    #[must_use]
    pub fn principal_type(&self) -> &str {
        self.principal_type.as_str()
    }

    /// Java `KafkaPrincipal.getName`.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal_name.as_str()
    }
}

impl fmt::Display for CreatableRenewer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.principal_name)
    }
}

/// CreateDelegationToken (api 38) v1–v3 request body.
///
/// Official Apache JSON (`apiKey: 38`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-3"`, `flexibleVersions: "2+"`).
/// Official JSON lists no `errorCodes`. Request has no ErrorCode
/// field. `OwnerPrincipalType` / `OwnerPrincipalName` are nullable
/// (v3+); null means the token request principal. `MaxLifetimeMs`
/// `-1` uses the server default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenRequest {
    /// Token owner principal type (v3+; decode fills `None` on v1–v2).
    pub owner_principal_type: Option<String>,
    /// Token owner principal name (v3+; decode fills `None` on v1–v2).
    pub owner_principal_name: Option<String>,
    /// Principals allowed to renew the token.
    pub renewers: Vec<CreatableRenewer>,
    /// Maximum token lifetime in milliseconds.
    pub max_lifetime_ms: i64,
}

impl CreateDelegationTokenRequest {
    /// Construct [`Self`].
    pub fn new(
        owner_principal_type: Option<String>,
        owner_principal_name: Option<String>,
        renewers: Vec<CreatableRenewer>,
        max_lifetime_ms: i64,
    ) -> Self {
        Self {
            owner_principal_type,
            owner_principal_name,
            renewers,
            max_lifetime_ms,
        }
    }
}

impl Default for CreateDelegationTokenRequest {
    /// Java `CreateDelegationTokenOptions`: request principal, empty
    /// renewers, `max_lifetime_ms = -1` (broker default).
    fn default() -> Self {
        Self::new(None, None, Vec::new(), -1)
    }
}

impl CreateDelegationTokenRequest {
    /// Java `CreateDelegationTokenOptions` owner type (`None` is the
    /// request principal).
    #[must_use]
    pub fn owner_principal_type(&self) -> Option<&str> {
        self.owner_principal_type.as_deref()
    }

    /// Java `CreateDelegationTokenOptions` owner name (`None` is the
    /// request principal).
    #[must_use]
    pub fn owner_principal_name(&self) -> Option<&str> {
        self.owner_principal_name.as_deref()
    }

    /// Java `CreateDelegationTokenOptions.renewers`.
    #[must_use]
    pub fn renewers(&self) -> &[CreatableRenewer] {
        &self.renewers
    }

    /// Java `CreateDelegationTokenOptions.maxLifeTimeMs` (`-1` is broker
    /// default).
    #[must_use]
    pub fn max_lifetime_ms(&self) -> i64 {
        self.max_lifetime_ms
    }
}

/// CreateDelegationToken (api 38) v1–v3 response body.
///
/// Java `DelegationToken` plus `TokenInformation` (no `renewers` on
/// create). **ErrorCode is top-level**, first field — not after throttle.
/// Official JSON places `ThrottleTimeMs` last. This is a single token,
/// not a token array: there is no first-token ErrorCode and no
/// first-renewer ErrorCode (renewers are request-only).
///
/// [`Debug`] redacts [`Self::hmac`] (Java `DelegationToken.toString`
/// prints `hmac=[*******]`).
#[derive(Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Principal type (for example `User`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
    /// Token requester principal type (v3+; decode fills empty on v1–v2).
    pub token_requester_principal_type: String,
    /// Token requester principal name (v3+; decode fills empty on v1–v2).
    pub token_requester_principal_name: String,
    /// Issue time in milliseconds since the Unix epoch.
    pub issue_timestamp_ms: i64,
    /// Expiry time in milliseconds since the Unix epoch.
    pub expiry_timestamp_ms: i64,
    /// Maximum expiry in milliseconds since the Unix epoch.
    pub max_timestamp_ms: i64,
    /// Delegation token id.
    pub token_id: String,
    /// Token HMAC bytes.
    pub hmac: Vec<u8>,
}

impl CreateDelegationTokenResponse {
    #[expect(
        clippy::too_many_arguments,
        reason = "wire type follows the Kafka spec field-for-field"
    )]
    /// Construct [`Self`].
    pub fn new(
        error_code: i16,
        principal_type: impl Into<String>,
        principal_name: impl Into<String>,
        token_requester_principal_type: impl Into<String>,
        token_requester_principal_name: impl Into<String>,
        issue_timestamp_ms: i64,
        expiry_timestamp_ms: i64,
        max_timestamp_ms: i64,
        token_id: impl Into<String>,
        hmac: Vec<u8>,
    ) -> Self {
        Self {
            error_code,
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
            token_requester_principal_type: token_requester_principal_type.into(),
            token_requester_principal_name: token_requester_principal_name.into(),
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id: token_id.into(),
            hmac,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `TokenInformation.owner` principal type.
    #[must_use]
    pub fn principal_type(&self) -> &str {
        self.principal_type.as_str()
    }

    /// Java `TokenInformation.owner` principal name.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal_name.as_str()
    }

    /// Java `TokenInformation.ownerAsString`.
    #[must_use]
    pub fn owner_as_string(&self) -> String {
        kafka_principal_as_string(&self.principal_type, &self.principal_name)
    }

    /// Java `TokenInformation.tokenRequester` principal type.
    #[must_use]
    pub fn token_requester_principal_type(&self) -> &str {
        self.token_requester_principal_type.as_str()
    }

    /// Java `TokenInformation.tokenRequester` principal name.
    #[must_use]
    pub fn token_requester_principal_name(&self) -> &str {
        self.token_requester_principal_name.as_str()
    }

    /// Java `TokenInformation.tokenRequesterAsString`.
    #[must_use]
    pub fn token_requester_as_string(&self) -> String {
        kafka_principal_as_string(
            &self.token_requester_principal_type,
            &self.token_requester_principal_name,
        )
    }

    /// Java `TokenInformation.issueTimestamp`.
    #[must_use]
    pub fn issue_timestamp(&self) -> i64 {
        self.issue_timestamp_ms
    }

    /// Java `TokenInformation.expiryTimestamp`.
    #[must_use]
    pub fn expiry_timestamp(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Java `TokenInformation.maxTimestamp`.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Java `TokenInformation.tokenId`.
    #[must_use]
    pub fn token_id(&self) -> &str {
        self.token_id.as_str()
    }

    /// Java `DelegationToken.hmac`.
    #[must_use]
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Java `DelegationToken.hmacAsBase64String`.
    #[must_use]
    pub fn hmac_as_base64_string(&self) -> String {
        encode_hmac_as_base64(&self.hmac)
    }
}

impl fmt::Debug for CreateDelegationTokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateDelegationTokenResponse")
            .field("error_code", &self.error_code)
            .field("principal_type", &self.principal_type)
            .field("principal_name", &self.principal_name)
            .field(
                "token_requester_principal_type",
                &self.token_requester_principal_type,
            )
            .field(
                "token_requester_principal_name",
                &self.token_requester_principal_name,
            )
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &"[*******]")
            .finish()
    }
}
///
/// v1 is classic. v2–v3 are flexible. Kafka 4.0 `validVersions` is
/// `1-3` (v0 removed). This crate speaks 1–3. v0 and v4+ are not
/// spoken.
fn create_delegation_token_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2..=3 => Ok(true),
        other => Err(Error::protocol(format!(
            "CreateDelegationToken version {other} is not implemented"
        ))),
    }
}

/// CreateDelegationToken v1–3 (classic at v1; flexible from v2;
/// KIP-48 / KIP-373).
///
/// Official Apache JSON (`apiKey: 38`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-3"`, `flexibleVersions: "2+"`).
/// Official JSON lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleCreateTokenRequest` validates the connection
/// (`allowTokenRequests` → `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`
/// (64)), owner `CREATE_TOKENS`
/// (`DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65)), and renewer
/// principal type (`INVALID_PRINCIPAL_TYPE` (67)), then
/// `forwardToController` — broker-side envelope forwarding, not a
/// client hop. Official Java `CreateDelegationTokenRequest.getErrorResponse`
/// writes `Errors.forException(e).code()` onto the **top-level**
/// ErrorCode. Official Java `KafkaAdminClient.createDelegationToken`
/// uses `LeastLoadedNodeProvider` (any broker). `NOT_COORDINATOR`
/// (16) is **not** listed. `NOT_CONTROLLER` (41) is **not** listed.
/// `NOT_LEADER_OR_FOLLOWER` (6) is **not** a client hop.
/// kafka-protocol 0.18.0 (`CreateDelegationTokenRequest` /
/// `CreateDelegationTokenResponse`, `VERSIONS` min=1 max=3). Kafka
/// 4.0 max is 3; this crate speaks 1–3. v0 was removed in Kafka 4.0.
/// v4+ is not spoken. Request encode used `features = ["client"]`;
/// response encode used `broker`. Request: compact nullable
/// `OwnerPrincipalType` / `OwnerPrincipalName` (v3+), `Renewers` of
/// `{PrincipalType STRING, PrincipalName STRING, tagged (v2+)}`,
/// `MaxLifetimeMs` INT64, tagged (v2+). Response: **top-level
/// `ErrorCode` INT16 first**, `PrincipalType`, `PrincipalName`,
/// `TokenRequesterPrincipalType` / `TokenRequesterPrincipalName`
/// (v3+), `IssueTimestampMs` INT64, `ExpiryTimestampMs` INT64,
/// `MaxTimestampMs` INT64, `TokenId`, `Hmac` BYTES, `ThrottleTimeMs`
/// INT32 last, tagged (v2+). v1–v2 omit owner and requester fields
/// (decode fills `None` / empty). **ErrorCode is top-level**, first
/// field — not after throttle, not a first-renewer field, and not a
/// first-token field. Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// throttle `0`, empty principals / token / hmac, error
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) at **v3**: the
/// leftover-empty body is **37 bytes** and the top-level ErrorCode is
/// the INT16 at **bytes 0–1**. i16=64 hits only at byte 0. v2
/// leftover-empty is **35 bytes** (no requester strings). Classic
/// **v1** leftover-empty is **40 bytes**. There is no first-renewer
/// ErrorCode and no first-token ErrorCode. Do not assume bytes 4–5
/// from DescribeLogDirs / AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official first field. Not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe), 7–8 (DeleteGroups
/// after GroupId; DescribeLogDirs first-directory), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop. This is not a token store.
pub fn encode_create_delegation_token_request(
    buf: &mut BytesMut,
    version: i16,
    req: &CreateDelegationTokenRequest,
) -> crate::error::Result<()> {
    let flexible = create_delegation_token_flexible(version)?;
    if version >= 3 {
        buf::put_string(buf, flexible, req.owner_principal_type.as_deref())?;
        buf::put_string(buf, flexible, req.owner_principal_name.as_deref())?;
    }
    buf::put_array_len(buf, flexible, Some(req.renewers.len()))?;
    for renewer in &req.renewers {
        buf::put_string(buf, flexible, Some(&renewer.principal_type))?;
        buf::put_string(buf, flexible, Some(&renewer.principal_name))?;
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_i64(req.max_lifetime_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreateDelegationToken request.
pub fn decode_create_delegation_token_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<CreateDelegationTokenRequest> {
    let flexible = create_delegation_token_flexible(version)?;
    let (owner_principal_type, owner_principal_name) = if version >= 3 {
        (
            buf::get_string(buf, flexible)?,
            buf::get_string(buf, flexible)?,
        )
    } else {
        (None, None)
    };
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut renewers = Vec::with_capacity(n);
    for _ in 0..n {
        let principal_type = buf::get_string(buf, flexible)?.unwrap_or_default();
        let principal_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        renewers.push(CreatableRenewer {
            principal_type,
            principal_name,
        });
    }
    let max_lifetime_ms = buf::get_i64(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(CreateDelegationTokenRequest {
        owner_principal_type,
        owner_principal_name,
        renewers,
        max_lifetime_ms,
    })
}

/// Encode a CreateDelegationToken response (v1–3). Requester
/// principal fields are v3+.
pub fn encode_create_delegation_token_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &CreateDelegationTokenResponse,
) -> crate::error::Result<()> {
    let flexible = create_delegation_token_flexible(version)?;
    buf.put_i16(resp.error_code);
    buf::put_string(buf, flexible, Some(&resp.principal_type))?;
    buf::put_string(buf, flexible, Some(&resp.principal_name))?;
    if version >= 3 {
        buf::put_string(buf, flexible, Some(&resp.token_requester_principal_type))?;
        buf::put_string(buf, flexible, Some(&resp.token_requester_principal_name))?;
    }
    buf.put_i64(resp.issue_timestamp_ms);
    buf.put_i64(resp.expiry_timestamp_ms);
    buf.put_i64(resp.max_timestamp_ms);
    buf::put_string(buf, flexible, Some(&resp.token_id))?;
    buf::put_bytes(buf, flexible, Some(&resp.hmac))?;
    buf.put_i32(0);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a CreateDelegationToken response.
pub fn decode_create_delegation_token_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<CreateDelegationTokenResponse> {
    let flexible = create_delegation_token_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let principal_type = buf::get_string(buf, flexible)?.unwrap_or_default();
    let principal_name = buf::get_string(buf, flexible)?.unwrap_or_default();
    let (token_requester_principal_type, token_requester_principal_name) = if version >= 3 {
        (
            buf::get_string(buf, flexible)?.unwrap_or_default(),
            buf::get_string(buf, flexible)?.unwrap_or_default(),
        )
    } else {
        (String::new(), String::new())
    };
    let issue_timestamp_ms = buf::get_i64(buf)?;
    let expiry_timestamp_ms = buf::get_i64(buf)?;
    let max_timestamp_ms = buf::get_i64(buf)?;
    let token_id = buf::get_string(buf, flexible)?.unwrap_or_default();
    let hmac = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    let _th = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(CreateDelegationTokenResponse {
        error_code,
        principal_type,
        principal_name,
        token_requester_principal_type,
        token_requester_principal_name,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
    })
}

/// RenewDelegationToken (api 39) v1–v2 request body.
///
/// Official Apache JSON (`apiKey: 39`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-2"`, `flexibleVersions: "2+"`).
/// Official JSON lists no `errorCodes`. Request has no ErrorCode
/// field. `Hmac` is the token HMAC (non-null BYTES). `RenewPeriodMs`
/// `-1` uses the server default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewDelegationTokenRequest {
    /// Token HMAC bytes.
    pub hmac: Vec<u8>,
    /// Renewal period in milliseconds.
    pub renew_period_ms: i64,
}

impl RenewDelegationTokenRequest {
    /// Construct [`Self`].
    pub fn new(hmac: Vec<u8>, renew_period_ms: i64) -> Self {
        Self {
            hmac,
            renew_period_ms,
        }
    }

    /// Java `renewDelegationToken` HMAC bytes.
    #[must_use]
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Java `RenewDelegationTokenOptions.renewTimePeriodMs` (`-1` is broker
    /// default).
    #[must_use]
    pub fn renew_period_ms(&self) -> i64 {
        self.renew_period_ms
    }
}

/// RenewDelegationToken (api 39) v1–v2 response body.
///
/// **ErrorCode is top-level**, first field — not after throttle.
/// Official JSON places `ThrottleTimeMs` last. This is a single token
/// expiry, not a token array: there is no first-token ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewDelegationTokenResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Expiry time in milliseconds since the Unix epoch.
    pub expiry_timestamp_ms: i64,
}

impl RenewDelegationTokenResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, expiry_timestamp_ms: i64) -> Self {
        Self {
            error_code,
            expiry_timestamp_ms,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `RenewDelegationTokenResult.expiryTimestamp`.
    #[must_use]
    pub fn expiry_timestamp(&self) -> i64 {
        self.expiry_timestamp_ms
    }
}

/// `true` when RenewDelegationToken `version` is flexible.
///
/// v1 is classic. v2 is flexible. Kafka 4.0 `validVersions` is `1-2`
/// (v0 removed). This crate speaks 1–2. v0 and v3+ are not spoken.
fn renew_delegation_token_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "RenewDelegationToken version {other} is not implemented"
        ))),
    }
}

/// RenewDelegationToken v1–2 (classic at v1; flexible from v2;
/// KIP-48 / KIP-373).
///
/// Official Apache JSON (`apiKey: 39`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-2"`, `flexibleVersions: "2+"`).
/// Official JSON lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleRenewTokenRequest` validates the connection
/// (`allowTokenRequests` → `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`
/// (64)), then `forwardToController` — broker-side envelope
/// forwarding, not a client hop. Official Java
/// `RenewDelegationTokenResponseData` writes `Errors.forException` /
/// handler `setErrorCode` onto the **top-level** ErrorCode. Official
/// Java `KafkaAdminClient.renewDelegationToken` uses
/// `LeastLoadedNodeProvider` (any broker). `NOT_COORDINATOR` (16) is
/// **not** listed. `NOT_CONTROLLER` (41) is **not** listed.
/// `NOT_LEADER_OR_FOLLOWER` (6) is **not** a client hop.
/// kafka-protocol 0.18.0 (`RenewDelegationTokenRequest` /
/// `RenewDelegationTokenResponse`, `VERSIONS` min=1 max=2). Kafka
/// 4.0 max is 2; this crate speaks 1–2. v0 was removed in Kafka 4.0.
/// v3+ is not spoken. Same fields on v1 and v2. Request encode used
/// `features = ["client"]`; response encode used `broker`. Request:
/// `Hmac` BYTES, `RenewPeriodMs` INT64, tagged (v2+). Response:
/// **top-level `ErrorCode` INT16 first**, `ExpiryTimestampMs` INT64,
/// `ThrottleTimeMs` INT32 last, tagged (v2+). **ErrorCode is
/// top-level**, first field — not after throttle and not a
/// first-token field. Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// throttle `0`, expiry `0`, error
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) at **v2**: the
/// leftover-empty body is **15 bytes** and the top-level ErrorCode is
/// the INT16 at **bytes 0–1**. i16=64 hits only at byte 0. Classic
/// **v1** leftover-empty is **14 bytes**. There is no first-token
/// ErrorCode. Do not assume bytes 0–1 from CreateDelegationToken
/// (different response, 37-byte empty-token body at v3): this offset
/// was measured on this API's official first field. Not bytes 4–5
/// from DescribeLogDirs / AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official first field. Not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe), 7–8 (DeleteGroups
/// after GroupId; DescribeLogDirs first-directory), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop. This is not a token store. Do not
/// copy CreateDelegationToken just because it is the previous slice.
pub fn encode_renew_delegation_token_request(
    buf: &mut BytesMut,
    version: i16,
    req: &RenewDelegationTokenRequest,
) -> crate::error::Result<()> {
    let flexible = renew_delegation_token_flexible(version)?;
    buf::put_bytes(buf, flexible, Some(&req.hmac))?;
    buf.put_i64(req.renew_period_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a RenewDelegationToken request.
pub fn decode_renew_delegation_token_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<RenewDelegationTokenRequest> {
    let flexible = renew_delegation_token_flexible(version)?;
    let hmac = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    let renew_period_ms = buf::get_i64(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(RenewDelegationTokenRequest {
        hmac,
        renew_period_ms,
    })
}

/// Encode a RenewDelegationToken response (v1–2).
pub fn encode_renew_delegation_token_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &RenewDelegationTokenResponse,
) -> crate::error::Result<()> {
    let flexible = renew_delegation_token_flexible(version)?;
    buf.put_i16(resp.error_code);
    buf.put_i64(resp.expiry_timestamp_ms);
    buf.put_i32(0);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a RenewDelegationToken response.
pub fn decode_renew_delegation_token_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<RenewDelegationTokenResponse> {
    let flexible = renew_delegation_token_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let expiry_timestamp_ms = buf::get_i64(buf)?;
    let _th = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(RenewDelegationTokenResponse {
        error_code,
        expiry_timestamp_ms,
    })
}

/// ExpireDelegationToken (api 40) v1–v2 request body.
///
/// Official Apache JSON (`apiKey: 40`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-2"`, `flexibleVersions: "2+"`).
/// Official JSON lists no `errorCodes`. Request has no ErrorCode
/// field. `Hmac` is the token HMAC (non-null BYTES).
/// `ExpiryTimePeriodMs` `-1` expires immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireDelegationTokenRequest {
    /// Token HMAC bytes.
    pub hmac: Vec<u8>,
    /// Remaining lifetime in milliseconds (`-1` expires immediately).
    pub expiry_time_period_ms: i64,
}

impl ExpireDelegationTokenRequest {
    /// Construct [`Self`].
    pub fn new(hmac: Vec<u8>, expiry_time_period_ms: i64) -> Self {
        Self {
            hmac,
            expiry_time_period_ms,
        }
    }

    /// Java `expireDelegationToken` HMAC bytes.
    #[must_use]
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Java `ExpireDelegationTokenOptions.expiryTimePeriodMs` (`-1` expires
    /// immediately).
    #[must_use]
    pub fn expiry_time_period_ms(&self) -> i64 {
        self.expiry_time_period_ms
    }
}

/// ExpireDelegationToken (api 40) v1–v2 response body.
///
/// **ErrorCode is top-level**, first field — not after throttle.
/// Official JSON places `ThrottleTimeMs` last. This is a single token
/// expiry, not a token array: there is no first-token ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireDelegationTokenResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Expiry time in milliseconds since the Unix epoch.
    pub expiry_timestamp_ms: i64,
}

impl ExpireDelegationTokenResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, expiry_timestamp_ms: i64) -> Self {
        Self {
            error_code,
            expiry_timestamp_ms,
        }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `ExpireDelegationTokenResult.expiryTimestamp`.
    #[must_use]
    pub fn expiry_timestamp(&self) -> i64 {
        self.expiry_timestamp_ms
    }
}

/// `true` when ExpireDelegationToken `version` is flexible.
///
/// v1 is classic. v2 is flexible. Kafka 4.0 `validVersions` is `1-2`
/// (v0 removed). This crate speaks 1–2. v0 and v3+ are not spoken.
fn expire_delegation_token_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "ExpireDelegationToken version {other} is not implemented"
        ))),
    }
}

/// ExpireDelegationToken v1–2 (classic at v1; flexible from v2;
/// KIP-48 / KIP-373).
///
/// Official Apache JSON (`apiKey: 40`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-2"`, `flexibleVersions: "2+"`).
/// Official JSON lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleExpireTokenRequest` validates the connection
/// (`allowTokenRequests` → `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`
/// (64)), then `forwardToController` — broker-side envelope
/// forwarding, not a client hop. Official Java
/// `ExpireDelegationTokenRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto the **top-level** ErrorCode.
/// Official Java `KafkaAdminClient.expireDelegationToken` uses
/// `LeastLoadedNodeProvider` (any broker). `NOT_COORDINATOR` (16) is
/// **not** listed. `NOT_CONTROLLER` (41) is **not** listed.
/// `NOT_LEADER_OR_FOLLOWER` (6) is **not** a client hop.
/// kafka-protocol 0.18.0 (`ExpireDelegationTokenRequest` /
/// `ExpireDelegationTokenResponse`, `VERSIONS` min=1 max=2). Kafka
/// 4.0 max is 2; this crate speaks 1–2. v0 was removed in Kafka 4.0.
/// v3+ is not spoken. Same fields on v1 and v2. Request encode used
/// `features = ["client"]`; response encode used `broker`. Request:
/// `Hmac` BYTES, `ExpiryTimePeriodMs` INT64, tagged (v2+). Response:
/// **top-level `ErrorCode` INT16 first**, `ExpiryTimestampMs` INT64,
/// `ThrottleTimeMs` INT32 last, tagged (v2+). **ErrorCode is
/// top-level**, first field — not after throttle and not a
/// first-token field. Measured independently from kafka-protocol
/// 0.18.0 (`broker` encodes the response) on leftover-empty fixture
/// throttle `0`, expiry `0`, error
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) at **v2**: the
/// leftover-empty body is **15 bytes** and the top-level ErrorCode is
/// the INT16 at **bytes 0–1**. i16=64 hits only at byte 0. Classic
/// **v1** leftover-empty is **14 bytes**. There is no first-token
/// ErrorCode. Do not assume bytes 0–1 from CreateDelegationToken
/// (different response, 37-byte empty-token body at v3) or
/// RenewDelegationToken (sibling API, independently measured): this
/// offset was measured on this API's official first field. Not bytes
/// 4–5 from DescribeLogDirs / AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official first field. Not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe), 7–8 (DeleteGroups
/// after GroupId; DescribeLogDirs first-directory), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop. This is not a token store. Do not
/// copy RenewDelegationToken just because it is the previous slice.
pub fn encode_expire_delegation_token_request(
    buf: &mut BytesMut,
    version: i16,
    req: &ExpireDelegationTokenRequest,
) -> crate::error::Result<()> {
    let flexible = expire_delegation_token_flexible(version)?;
    buf::put_bytes(buf, flexible, Some(&req.hmac))?;
    buf.put_i64(req.expiry_time_period_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an ExpireDelegationToken request.
pub fn decode_expire_delegation_token_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ExpireDelegationTokenRequest> {
    let flexible = expire_delegation_token_flexible(version)?;
    let hmac = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    let expiry_time_period_ms = buf::get_i64(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ExpireDelegationTokenRequest {
        hmac,
        expiry_time_period_ms,
    })
}

/// Encode an ExpireDelegationToken response (v1–2).
pub fn encode_expire_delegation_token_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ExpireDelegationTokenResponse,
) -> crate::error::Result<()> {
    let flexible = expire_delegation_token_flexible(version)?;
    buf.put_i16(resp.error_code);
    buf.put_i64(resp.expiry_timestamp_ms);
    buf.put_i32(0);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode an ExpireDelegationToken response.
pub fn decode_expire_delegation_token_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ExpireDelegationTokenResponse> {
    let flexible = expire_delegation_token_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let expiry_timestamp_ms = buf::get_i64(buf)?;
    let _th = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(ExpireDelegationTokenResponse {
        error_code,
        expiry_timestamp_ms,
    })
}

/// One owner principal in a DescribeDelegationToken (api 41) request.
///
/// Official JSON `Owners` has PrincipalType and PrincipalName only.
/// There is no per-owner ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenOwner {
    /// Principal type (for example `User`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

impl DescribeDelegationTokenOwner {
    /// Construct [`Self`].
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Java `KafkaPrincipal.getPrincipalType`.
    #[must_use]
    pub fn principal_type(&self) -> &str {
        self.principal_type.as_str()
    }

    /// Java `KafkaPrincipal.getName`.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal_name.as_str()
    }
}

impl fmt::Display for DescribeDelegationTokenOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.principal_name)
    }
}

/// DescribeDelegationToken (api 41) v1–v3 request body.
///
/// Official Apache JSON (`apiKey: 41`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-3"`, `flexibleVersions: "2+"`).
/// Official JSON lists no `errorCodes`. Request has no ErrorCode
/// field. `Owners` is a nullable array: null describes all tokens the
/// caller may see; empty describes none. Request layout is the same
/// on v1–v3 (classic at v1; compact plus tagged from v2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenRequest {
    /// Owner filter, or `None` for every token.
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
}

impl DescribeDelegationTokenRequest {
    /// Construct [`Self`].
    pub fn new(owners: Option<Vec<DescribeDelegationTokenOwner>>) -> Self {
        Self { owners }
    }

    /// Java `DescribeDelegationTokenOptions.owners` (`None` is every
    /// visible token; empty describes none).
    #[must_use]
    pub fn owners(&self) -> Option<&[DescribeDelegationTokenOwner]> {
        self.owners.as_deref()
    }
}

impl Default for DescribeDelegationTokenRequest {
    /// Java `DescribeDelegationTokenOptions`: `owners` null (every
    /// visible token).
    fn default() -> Self {
        Self::new(None)
    }
}

/// One renewer principal on a described delegation token.
///
/// Official JSON `Renewers` has PrincipalType and PrincipalName only.
/// There is no per-renewer ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedDelegationTokenRenewer {
    /// Principal type (for example `User`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

impl DescribedDelegationTokenRenewer {
    /// Construct [`Self`].
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Java `KafkaPrincipal.getPrincipalType`.
    #[must_use]
    pub fn principal_type(&self) -> &str {
        self.principal_type.as_str()
    }

    /// Java `KafkaPrincipal.getName`.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal_name.as_str()
    }
}

impl fmt::Display for DescribedDelegationTokenRenewer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.principal_name)
    }
}

/// One token in a DescribeDelegationToken (api 41) v1–v3 response.
///
/// Java `DelegationToken` plus `TokenInformation`. Official JSON `Tokens`
/// has no per-token ErrorCode. v3 adds TokenRequesterPrincipalType /
/// TokenRequesterPrincipalName (decode fills empty on v1–v2).
///
/// [`Debug`] redacts [`Self::hmac`] (Java `DelegationToken.toString`
/// prints `hmac=[*******]`).
#[derive(Clone, PartialEq, Eq)]
pub struct DescribedDelegationToken {
    /// Principal type (for example `User`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
    /// Token requester principal type (v3+; decode fills empty on v1–v2).
    pub token_requester_principal_type: String,
    /// Token requester principal name (v3+; decode fills empty on v1–v2).
    pub token_requester_principal_name: String,
    /// Issue time in milliseconds since the Unix epoch.
    pub issue_timestamp: i64,
    /// Expiry time in milliseconds since the Unix epoch.
    pub expiry_timestamp: i64,
    /// Maximum expiry in milliseconds since the Unix epoch.
    pub max_timestamp: i64,
    /// Delegation token id.
    pub token_id: String,
    /// Token HMAC bytes.
    pub hmac: Vec<u8>,
    /// Principals allowed to renew the token.
    pub renewers: Vec<DescribedDelegationTokenRenewer>,
}

impl DescribedDelegationToken {
    #[expect(
        clippy::too_many_arguments,
        reason = "wire type follows the Kafka spec field-for-field"
    )]
    /// Construct [`Self`].
    pub fn new(
        principal_type: impl Into<String>,
        principal_name: impl Into<String>,
        token_requester_principal_type: impl Into<String>,
        token_requester_principal_name: impl Into<String>,
        issue_timestamp: i64,
        expiry_timestamp: i64,
        max_timestamp: i64,
        token_id: impl Into<String>,
        hmac: Vec<u8>,
        renewers: Vec<DescribedDelegationTokenRenewer>,
    ) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
            token_requester_principal_type: token_requester_principal_type.into(),
            token_requester_principal_name: token_requester_principal_name.into(),
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id: token_id.into(),
            hmac,
            renewers,
        }
    }

    /// Java `TokenInformation.owner` principal type.
    #[must_use]
    pub fn principal_type(&self) -> &str {
        self.principal_type.as_str()
    }

    /// Java `TokenInformation.owner` principal name.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal_name.as_str()
    }

    /// Java `TokenInformation.ownerAsString`.
    #[must_use]
    pub fn owner_as_string(&self) -> String {
        kafka_principal_as_string(&self.principal_type, &self.principal_name)
    }

    /// Java `TokenInformation.tokenRequester` principal type.
    #[must_use]
    pub fn token_requester_principal_type(&self) -> &str {
        self.token_requester_principal_type.as_str()
    }

    /// Java `TokenInformation.tokenRequester` principal name.
    #[must_use]
    pub fn token_requester_principal_name(&self) -> &str {
        self.token_requester_principal_name.as_str()
    }

    /// Java `TokenInformation.tokenRequesterAsString`.
    #[must_use]
    pub fn token_requester_as_string(&self) -> String {
        kafka_principal_as_string(
            &self.token_requester_principal_type,
            &self.token_requester_principal_name,
        )
    }

    /// Java `TokenInformation.issueTimestamp`.
    #[must_use]
    pub fn issue_timestamp(&self) -> i64 {
        self.issue_timestamp
    }

    /// Java `TokenInformation.expiryTimestamp`.
    #[must_use]
    pub fn expiry_timestamp(&self) -> i64 {
        self.expiry_timestamp
    }

    /// Java `TokenInformation.maxTimestamp`.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Java `TokenInformation.tokenId`.
    #[must_use]
    pub fn token_id(&self) -> &str {
        self.token_id.as_str()
    }

    /// Java `DelegationToken.hmac`.
    #[must_use]
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Java `DelegationToken.hmacAsBase64String`.
    #[must_use]
    pub fn hmac_as_base64_string(&self) -> String {
        encode_hmac_as_base64(&self.hmac)
    }

    /// Java `TokenInformation.renewers`.
    #[must_use]
    pub fn renewers(&self) -> &[DescribedDelegationTokenRenewer] {
        &self.renewers
    }
}

impl fmt::Debug for DescribedDelegationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DescribedDelegationToken")
            .field("principal_type", &self.principal_type)
            .field("principal_name", &self.principal_name)
            .field(
                "token_requester_principal_type",
                &self.token_requester_principal_type,
            )
            .field(
                "token_requester_principal_name",
                &self.token_requester_principal_name,
            )
            .field("issue_timestamp", &self.issue_timestamp)
            .field("expiry_timestamp", &self.expiry_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("token_id", &self.token_id)
            .field("hmac", &"[*******]")
            .field("renewers", &self.renewers)
            .finish()
    }
}
///
/// **ErrorCode is top-level**, first field — not after throttle and
/// not a first-token field. Official JSON places `Tokens` next and
/// `ThrottleTimeMs` last. Empty Tokens is not a first-token ErrorCode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenResponse {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Delegation tokens.
    pub tokens: Vec<DescribedDelegationToken>,
}

impl DescribeDelegationTokenResponse {
    /// Construct [`Self`].
    pub fn new(error_code: i16, tokens: Vec<DescribedDelegationToken>) -> Self {
        Self { error_code, tokens }
    }

    /// Kafka error code (`0` is success).
    #[must_use]
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Java `describeDelegationToken` tokens (`DelegationToken` list).
    #[must_use]
    pub fn tokens(&self) -> &[DescribedDelegationToken] {
        &self.tokens
    }
}

/// `true` when DescribeDelegationToken `version` is flexible.
///
/// v1 is classic. v2–v3 are flexible. Kafka 4.0 `validVersions` is
/// `1-3` (v0 removed). This crate speaks 1–3. v0 and v4+ are not
/// spoken.
fn describe_delegation_token_flexible(version: i16) -> Result<bool> {
    match version {
        1 => Ok(false),
        2..=3 => Ok(true),
        other => Err(Error::protocol(format!(
            "DescribeDelegationToken version {other} is not implemented"
        ))),
    }
}

/// DescribeDelegationToken v1–3 (classic at v1; flexible from v2;
/// TokenRequester v3; KIP-48 / KIP-373).
///
/// Official Apache JSON (`apiKey: 41`, request `listeners: ["broker",
/// "controller"]`, `validVersions: "1-3"`, `flexibleVersions: "2+"`).
/// Official JSON lists **no** `errorCodes`. Official Java
/// `KafkaApis.handleDescribeTokensRequest` answers on the connected
/// broker: `allowTokenRequests` →
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64), disabled manager →
/// `DELEGATION_TOKEN_AUTH_DISABLED`, empty owners → empty Tokens,
/// otherwise `tokenManager.getTokens`. It does **not** call
/// `forwardToController`. Official Java
/// `DescribeDelegationTokenRequest.getErrorResponse` writes
/// `Errors.forException(e).code()` onto the **top-level** ErrorCode.
/// Official Java `KafkaAdminClient.describeDelegationToken` uses
/// `LeastLoadedNodeProvider` (any broker). Official `ApiKeys` marks
/// Create/Renew/Expire forwardable; DescribeDelegationToken is **not**
/// forwardable. `NOT_COORDINATOR` (16) is **not** listed.
/// `NOT_CONTROLLER` (41) is **not** listed. apiKey 41 and error code
/// 41 collide numerically; the apiKey is not a hop.
/// `NOT_LEADER_OR_FOLLOWER` (6) is **not** a client hop.
/// kafka-protocol 0.18.0 (`DescribeDelegationTokenRequest` /
/// `DescribeDelegationTokenResponse`, `VERSIONS` min=1 max=3). Kafka
/// 4.0 max is 3; this crate speaks 1–3. v0 was removed in Kafka 4.0.
/// v4+ is not spoken. Request encode used `features = ["client"]`;
/// response encode used `broker`. Request: nullable `Owners` of
/// `{PrincipalType STRING, PrincipalName STRING, tagged (v2+)}`,
/// tagged (v2+). Response: **top-level `ErrorCode` INT16 first**,
/// `Tokens` of principals / requester (v3+) / timestamps / TokenId /
/// Hmac / Renewers, `ThrottleTimeMs` INT32 last, tagged (v2+).
/// **ErrorCode is top-level**, first field — not after throttle and
/// not a first-token field. Measured independently from
/// kafka-protocol 0.18.0 (`broker` encodes the response) on
/// leftover-empty fixture throttle `0`, empty Tokens, error
/// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) at **v3**: the
/// leftover-empty body is **8 bytes** and the top-level ErrorCode is
/// the INT16 at **bytes 0–1**. i16=64 hits only at byte 0. v2
/// leftover-empty empty-Tokens is also **8 bytes**. Classic **v1**
/// leftover-empty is **10 bytes**. There is no first-token ErrorCode.
/// Do not assume bytes 0–1 from CreateDelegationToken (37-byte
/// empty-token body), RenewDelegationToken, or ExpireDelegationToken
/// (15-byte leftover-empty body): this offset was measured on this
/// API's official first field. Not bytes 4–5 from DescribeLogDirs /
/// AssignReplicasToDirs / PushTelemetry /
/// GetTelemetrySubscriptions / ListConfigResources: this offset was
/// measured on this API's official first field. Not bytes 5–6
/// (DescribeTopicPartitions / ShareGroupDescribe), 7–8 (DeleteGroups
/// after GroupId; DescribeLogDirs first-directory), 8–9
/// (DescribeShareGroupOffsets), 12–13 (AlterReplicaLogDirs /
/// DescribeProducers first-partition), 27–28, or 45–46. Because 41
/// is not listed, 16 is not listed, and 6 is not a client hop, this
/// is broker-only: no FindCoordinator, no `key_type`, no controller
/// hop, no partition-leader hop. This is not a token store. Do not
/// copy ExpireDelegationToken just because it is the previous slice.
pub fn encode_describe_delegation_token_request(
    buf: &mut BytesMut,
    version: i16,
    req: &DescribeDelegationTokenRequest,
) -> crate::error::Result<()> {
    let flexible = describe_delegation_token_flexible(version)?;
    buf::put_array_len(buf, flexible, req.owners.as_ref().map(Vec::len))?;
    if let Some(owners) = &req.owners {
        for owner in owners {
            buf::put_string(buf, flexible, Some(&owner.principal_type))?;
            buf::put_string(buf, flexible, Some(&owner.principal_name))?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeDelegationToken request.
pub fn decode_describe_delegation_token_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeDelegationTokenRequest> {
    let flexible = describe_delegation_token_flexible(version)?;
    let owners = match buf::get_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut owners = Vec::with_capacity(n);
            for _ in 0..n {
                let principal_type = buf::get_string(buf, flexible)?.unwrap_or_default();
                let principal_name = buf::get_string(buf, flexible)?.unwrap_or_default();
                if flexible {
                    buf::skip_tagged_fields(buf)?;
                }
                owners.push(DescribeDelegationTokenOwner {
                    principal_type,
                    principal_name,
                });
            }
            Some(owners)
        }
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribeDelegationTokenRequest { owners })
}

/// Encode a DescribeDelegationToken response (v1–3). Requester
/// principal fields are v3+.
pub fn encode_describe_delegation_token_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &DescribeDelegationTokenResponse,
) -> crate::error::Result<()> {
    let flexible = describe_delegation_token_flexible(version)?;
    buf.put_i16(resp.error_code);
    buf::put_array_len(buf, flexible, Some(resp.tokens.len()))?;
    for token in &resp.tokens {
        buf::put_string(buf, flexible, Some(&token.principal_type))?;
        buf::put_string(buf, flexible, Some(&token.principal_name))?;
        if version >= 3 {
            buf::put_string(buf, flexible, Some(&token.token_requester_principal_type))?;
            buf::put_string(buf, flexible, Some(&token.token_requester_principal_name))?;
        }
        buf.put_i64(token.issue_timestamp);
        buf.put_i64(token.expiry_timestamp);
        buf.put_i64(token.max_timestamp);
        buf::put_string(buf, flexible, Some(&token.token_id))?;
        buf::put_bytes(buf, flexible, Some(&token.hmac))?;
        buf::put_array_len(buf, flexible, Some(token.renewers.len()))?;
        for renewer in &token.renewers {
            buf::put_string(buf, flexible, Some(&renewer.principal_type))?;
            buf::put_string(buf, flexible, Some(&renewer.principal_name))?;
            if flexible {
                buf::put_empty_tagged_fields(buf);
            }
        }
        if flexible {
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf.put_i32(0);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode a DescribeDelegationToken response.
pub fn decode_describe_delegation_token_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<DescribeDelegationTokenResponse> {
    let flexible = describe_delegation_token_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut tokens = Vec::with_capacity(n);
    for _ in 0..n {
        let principal_type = buf::get_string(buf, flexible)?.unwrap_or_default();
        let principal_name = buf::get_string(buf, flexible)?.unwrap_or_default();
        let (token_requester_principal_type, token_requester_principal_name) = if version >= 3 {
            (
                buf::get_string(buf, flexible)?.unwrap_or_default(),
                buf::get_string(buf, flexible)?.unwrap_or_default(),
            )
        } else {
            (String::new(), String::new())
        };
        let issue_timestamp = buf::get_i64(buf)?;
        let expiry_timestamp = buf::get_i64(buf)?;
        let max_timestamp = buf::get_i64(buf)?;
        let token_id = buf::get_string(buf, flexible)?.unwrap_or_default();
        let hmac = buf::get_bytes(buf, flexible)?.unwrap_or_default();
        let rn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
        let mut renewers = Vec::with_capacity(rn);
        for _ in 0..rn {
            let principal_type = buf::get_string(buf, flexible)?.unwrap_or_default();
            let principal_name = buf::get_string(buf, flexible)?.unwrap_or_default();
            if flexible {
                buf::skip_tagged_fields(buf)?;
            }
            renewers.push(DescribedDelegationTokenRenewer {
                principal_type,
                principal_name,
            });
        }
        if flexible {
            buf::skip_tagged_fields(buf)?;
        }
        tokens.push(DescribedDelegationToken {
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id,
            hmac,
            renewers,
        });
    }
    let _th = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(DescribeDelegationTokenResponse { error_code, tokens })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topics_v3_roundtrip() {
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "orders".into(),
                num_partitions: 6,
                replication_factor: 1,
                assignments: vec![ReplicaAssignment {
                    partition_index: 0,
                    broker_ids: vec![1],
                }],
                configs: vec![TopicConfig {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                }],
            }],
            timeout_ms: 10_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 3, &req).unwrap();
        let decoded = decode_create_topics_request(&mut &buf[..], 3).unwrap();
        assert_eq!(decoded, req);

        let results = vec![TopicResult::new("orders", 0, None)];
        buf.clear();
        encode_create_topics_response(&mut buf, 3, &results).unwrap();
        assert_eq!(
            decode_create_topics_response(&mut &buf[..], 3).unwrap(),
            results
        );
    }

    #[test]
    fn create_topics_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult::new("t", crate::error::NOT_CONTROLLER, None)];
        let mut buf = BytesMut::new();
        encode_create_topics_response(&mut buf, 4, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_response(&mut cur, 4).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v4 NOT_CONTROLLER must be leftover-empty"
        );
    }

    fn sample_create_topics_req() -> CreateTopicsRequest {
        CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "orders".into(),
                num_partitions: 6,
                replication_factor: 1,
                assignments: vec![ReplicaAssignment {
                    partition_index: 0,
                    broker_ids: vec![1],
                }],
                configs: vec![TopicConfig {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                }],
            }],
            timeout_ms: 10_000,
            validate_only: false,
        }
    }

    #[test]
    fn create_topics_v5_roundtrip_is_leftover_empty() {
        let req = sample_create_topics_req();
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 5, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_request(&mut cur, 5).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v5 request must consume compact fields and tagged fields"
        );
        let mut v4 = BytesMut::new();
        encode_create_topics_request(&mut v4, 4, &req).unwrap();
        assert_ne!(&buf[..], &v4[..], "CreateTopics v5 must not be classic v4");

        let results = vec![TopicResult {
            name: "orders".into(),
            error_code: 0,
            error_message: None,
            topic_id: [0; 16],
            num_partitions: 6,
            replication_factor: 1,
            configs: vec![CreatedTopicConfig {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
            }],
        }];
        buf.clear();
        encode_create_topics_response(&mut buf, 5, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_response(&mut cur, 5).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v5 response must be leftover-empty"
        );
        assert!(
            encode_create_topics_request(&mut BytesMut::new(), 8, &req).is_err(),
            "CreateTopics v8+ is not spoken"
        );
    }

    #[test]
    fn create_topics_v5_compact_layout_matches_independent_encode() {
        // Compact 1 topic "t", 1 partition, rf 1, empty assignments/configs,
        // timeout 5000, validateOnly false, empty tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00,
            0x13, 0x88, 0x00, 0x00,
        ];
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t".into(),
                num_partitions: 1,
                replication_factor: 1,
                assignments: Vec::new(),
                configs: Vec::new(),
            }],
            timeout_ms: 5_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 5, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v6 = BytesMut::new();
        encode_create_topics_request(&mut v6, 6, &req).unwrap();
        assert_eq!(&buf[..], &v6[..], "CreateTopics v6 request matches v5");
        let mut v7 = BytesMut::new();
        encode_create_topics_request(&mut v7, 7, &req).unwrap();
        assert_eq!(&buf[..], &v7[..], "CreateTopics v7 request matches v5");
    }

    #[test]
    fn create_topics_assignments_roundtrip() {
        // Compact 1 topic "t", NumPartitions -1, RF -1, one assignment
        // partition 0 brokers [1, 2], empty configs, timeout 5000.
        const V5: &[u8] = &[
            0x02, 0x02, 0x74, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x13, 0x88, 0x00, 0x00,
        ];
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t".into(),
                num_partitions: -1,
                replication_factor: -1,
                assignments: vec![ReplicaAssignment {
                    partition_index: 0,
                    broker_ids: vec![1, 2],
                }],
                configs: Vec::new(),
            }],
            timeout_ms: 5_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 5, &req).unwrap();
        assert_eq!(&buf[..], V5);
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_request(&mut cur, 5).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v5 assignments request must be leftover-empty"
        );
        buf.clear();
        encode_create_topics_request(&mut buf, 0, &req).unwrap();
        let mut cur = &buf[..];
        let decoded0 = decode_create_topics_request(&mut cur, 0).unwrap();
        assert_eq!(decoded0.topics, req.topics);
        assert_eq!(decoded0.timeout_ms, req.timeout_ms);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v0 assignments request must be leftover-empty"
        );
    }

    #[test]
    fn create_topics_broker_defaults_roundtrip() {
        // Compact 1 topic "t", NumPartitions -1, RF -1, empty assignments
        // and configs, timeout 5000, validateOnly false.
        const V5: &[u8] = &[
            0x02, 0x02, 0x74, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x01, 0x00, 0x00, 0x00,
            0x13, 0x88, 0x00, 0x00,
        ];
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t".into(),
                num_partitions: -1,
                replication_factor: -1,
                assignments: Vec::new(),
                configs: Vec::new(),
            }],
            timeout_ms: 5_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        encode_create_topics_request(&mut buf, 5, &req).unwrap();
        assert_eq!(&buf[..], V5);
        let mut cur = &buf[..];
        assert_eq!(decode_create_topics_request(&mut cur, 5).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v5 broker-defaults request must be leftover-empty"
        );
        buf.clear();
        encode_create_topics_request(&mut buf, 4, &req).unwrap();
        let mut cur = &buf[..];
        let decoded4 = decode_create_topics_request(&mut cur, 4).unwrap();
        assert_eq!(decoded4, req);
        assert!(
            !cur.has_remaining(),
            "CreateTopics v4 broker-defaults request must be leftover-empty"
        );
    }

    #[test]
    fn create_topics_v7_response_includes_topic_id() {
        let mut id = [0u8; 16];
        id[15] = 7;
        let results = vec![TopicResult {
            name: "t".into(),
            error_code: 0,
            error_message: None,
            topic_id: id,
            num_partitions: 1,
            replication_factor: 1,
            configs: Vec::new(),
        }];
        let mut v5 = BytesMut::new();
        encode_create_topics_response(&mut v5, 5, &results).unwrap();
        let mut v7 = BytesMut::new();
        encode_create_topics_response(&mut v7, 7, &results).unwrap();
        assert_ne!(
            &v5[..],
            &v7[..],
            "CreateTopics v7 response must include TopicId"
        );
        let mut cur = &v7[..];
        let got = decode_create_topics_response(&mut cur, 7).unwrap();
        assert_eq!(got, results);
        assert_eq!(got[0].name(), "t");
        assert_eq!(got[0].error_code(), 0);
        assert!(got[0].error_message().is_none());
        assert_eq!(got[0].num_partitions(), 1);
        assert_eq!(got[0].replication_factor(), 1);
        assert!(got[0].configs().is_empty());
        assert!(got[0].config().entries().is_empty());
        assert!(
            !cur.has_remaining(),
            "CreateTopics v7 response must be leftover-empty"
        );
        let mut v6 = BytesMut::new();
        encode_create_topics_response(&mut v6, 6, &results).unwrap();
        assert_eq!(&v5[..], &v6[..], "CreateTopics v6 response matches v5");
    }

    #[test]
    fn delete_topics_v3_roundtrip() {
        let names = vec!["orders".into(), "t".into()];
        let mut buf = BytesMut::new();
        encode_delete_topics_request(&mut buf, 3, &names, 5000).unwrap();
        let mut cur = &buf[..];
        let (decoded, timeout) = decode_delete_topics_request(&mut cur, 3).unwrap();
        assert_eq!(decoded, names);
        assert_eq!(timeout, 5000);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v3 request must be leftover-empty"
        );

        let results = vec![
            TopicResult::new("orders", 0, None),
            TopicResult::new("t", 3, None),
        ];
        buf.clear();
        encode_delete_topics_response(&mut buf, 3, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 3).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v3 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_topics_v4_roundtrip_is_leftover_empty() {
        let names = vec!["orders".into()];
        let mut buf = BytesMut::new();
        encode_delete_topics_request(&mut buf, 4, &names, 5000).unwrap();
        let mut cur = &buf[..];
        let (decoded, timeout) = decode_delete_topics_request(&mut cur, 4).unwrap();
        assert_eq!(decoded, names);
        assert_eq!(timeout, 5000);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v4 request must consume compact fields and tagged fields"
        );
        let mut v3 = BytesMut::new();
        encode_delete_topics_request(&mut v3, 3, &names, 5000).unwrap();
        assert_ne!(&buf[..], &v3[..], "DeleteTopics v4 must not be classic v3");

        let results = vec![TopicResult::new("orders", 0, None)];
        buf.clear();
        encode_delete_topics_response(&mut buf, 4, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 4).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v4 response must be leftover-empty"
        );
        assert!(
            encode_delete_topics_request(&mut BytesMut::new(), 7, &names, 5000).is_err(),
            "DeleteTopics v7+ is not spoken"
        );
    }

    #[test]
    fn delete_topics_v4_compact_layout_matches_independent_encode() {
        // Compact 1 topic "t", timeout 5000, empty tagged fields.
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x00, 0x00, 0x13, 0x88, 0x00];
        let names = vec!["t".into()];
        let mut buf = BytesMut::new();
        encode_delete_topics_request(&mut buf, 4, &names, 5_000).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut v5 = BytesMut::new();
        encode_delete_topics_request(&mut v5, 5, &names, 5_000).unwrap();
        assert_eq!(&buf[..], &v5[..], "DeleteTopics v5 request matches v4");
        let mut cur = &v5[..];
        let (decoded5, timeout5) = decode_delete_topics_request(&mut cur, 5).unwrap();
        assert_eq!(decoded5, names);
        assert_eq!(timeout5, 5_000);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v5 request must be leftover-empty"
        );
        let mut v6 = BytesMut::new();
        encode_delete_topics_request(&mut v6, 6, &names, 5_000).unwrap();
        assert_ne!(
            &buf[..],
            &v6[..],
            "DeleteTopics v6 request is Topics + TopicId"
        );
        // Compact Topics[1] { Name "t", TopicId 16 zeros, tagged } + timeout + tagged.
        const V6: &[u8] = &[
            0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x88, 0x00,
        ];
        assert_eq!(&v6[..], V6);
        let mut cur = &v6[..];
        let (decoded, timeout) = decode_delete_topics_request(&mut cur, 6).unwrap();
        assert_eq!(decoded, names);
        assert_eq!(timeout, 5_000);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v6 request must be leftover-empty"
        );

        // Compact Topics[1] { Name null, TopicId "t" padded, tagged } + timeout + tagged.
        const V6_ID: &[u8] = &[
            0x02, 0x00, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x88, 0x00,
        ];
        let mut id = [0u8; 16];
        id[0] = b't';
        let by_id = [DeleteTopicState::by_id(id)];
        let mut id_buf = BytesMut::new();
        encode_delete_topics_states_request(&mut id_buf, 6, &by_id, 5_000).unwrap();
        assert_eq!(&id_buf[..], V6_ID);
        let mut cur = &id_buf[..];
        let (got, timeout) = decode_delete_topics_states_request(&mut cur, 6).unwrap();
        assert_eq!(got, by_id);
        assert_eq!(timeout, 5_000);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v6 TopicId request must be leftover-empty"
        );
        let mut cur = &id_buf[..];
        let (names_only, _) = decode_delete_topics_request(&mut cur, 6).unwrap();
        assert!(
            names_only.is_empty(),
            "name-only decode skips null-Name TopicId deletes"
        );
    }

    #[test]
    fn delete_topics_v5_response_includes_error_message() {
        let results = vec![TopicResult::new("t", 3, Some("Unknown topic.".into()))];
        let mut v4 = BytesMut::new();
        encode_delete_topics_response(&mut v4, 4, &results).unwrap();
        let mut v5 = BytesMut::new();
        encode_delete_topics_response(&mut v5, 5, &results).unwrap();
        assert_ne!(
            &v4[..],
            &v5[..],
            "DeleteTopics v5 response must include ErrorMessage"
        );
        let mut cur = &v5[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 5).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v5 response must be leftover-empty"
        );
        let mut cur = &v4[..];
        let got4 = decode_delete_topics_response(&mut cur, 4).unwrap();
        assert_eq!(got4[0].error_code, 3);
        assert_eq!(got4[0].error_message, None);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v4 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_topics_v6_response_includes_topic_id() {
        let mut id = [0u8; 16];
        id[15] = 6;
        let results = vec![TopicResult {
            name: "t".into(),
            error_code: 0,
            error_message: None,
            topic_id: id,
            num_partitions: -1,
            replication_factor: -1,
            configs: Vec::new(),
        }];
        let mut v5 = BytesMut::new();
        encode_delete_topics_response(&mut v5, 5, &results).unwrap();
        let mut v6 = BytesMut::new();
        encode_delete_topics_response(&mut v6, 6, &results).unwrap();
        assert_ne!(
            &v5[..],
            &v6[..],
            "DeleteTopics v6 response must include TopicId"
        );
        let mut cur = &v6[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 6).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v6 response must be leftover-empty"
        );
        let mut v5_again = BytesMut::new();
        encode_delete_topics_response(&mut v5_again, 5, &results).unwrap();
        assert_eq!(&v5[..], &v5_again[..], "DeleteTopics v5 omits TopicId");
    }

    #[test]
    fn delete_topics_flexible_not_controller_is_leftover_empty() {
        let results = vec![TopicResult::new(
            "t",
            crate::error::NOT_CONTROLLER,
            Some("Not controller".into()),
        )];
        for version in [4i16, 5, 6] {
            let mut buf = BytesMut::new();
            encode_delete_topics_response(&mut buf, version, &results).unwrap();
            let mut cur = &buf[..];
            let got = decode_delete_topics_response(&mut cur, version).unwrap();
            assert_eq!(got[0].error_code, crate::error::NOT_CONTROLLER);
            if version >= 5 {
                assert_eq!(got[0].error_message.as_deref(), Some("Not controller"));
            } else {
                assert_eq!(got[0].error_message, None);
            }
            assert!(
                !cur.has_remaining(),
                "DeleteTopics v{version} NOT_CONTROLLER must be leftover-empty"
            );
        }
    }

    #[test]
    fn create_partitions_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult::new("t", crate::error::NOT_CONTROLLER, None)];
        let mut buf = BytesMut::new();
        encode_create_partitions_response(&mut buf, 1, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_partitions_response(&mut cur, 1).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v1 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn create_partitions_v1_request_matches_v0() {
        let topics = vec![CreatePartitionsTopic::new("t", 3)];
        let mut v0 = BytesMut::new();
        encode_create_partitions_request(&mut v0, 0, &topics, 5_000, false).unwrap();
        let mut v1 = BytesMut::new();
        encode_create_partitions_request(&mut v1, 1, &topics, 5_000, false).unwrap();
        assert_eq!(&v0[..], &v1[..], "CreatePartitions v1 request matches v0");
        let mut cur = &v1[..];
        let (decoded, timeout_ms, validate) =
            decode_create_partitions_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, topics);
        assert_eq!(timeout_ms, 5_000);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v1 request must be leftover-empty"
        );
    }

    #[test]
    fn create_partitions_v2_compact_layout_matches_independent_encode() {
        // Compact 1 topic "t", count 3, null assignments, timeout 5000,
        // validateOnly false, empty tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x13, 0x88, 0x00,
            0x00,
        ];
        let topics = vec![CreatePartitionsTopic::new("t", 3)];
        let mut buf = BytesMut::new();
        encode_create_partitions_request(&mut buf, 2, &topics, 5_000, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (decoded, timeout_ms, validate) =
            decode_create_partitions_request(&mut cur, 2).unwrap();
        assert_eq!(decoded, topics);
        assert_eq!(timeout_ms, 5_000);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v2 request must consume compact fields and tagged fields"
        );
        let mut v1 = BytesMut::new();
        encode_create_partitions_request(&mut v1, 1, &topics, 5_000, false).unwrap();
        assert_ne!(
            &buf[..],
            &v1[..],
            "CreatePartitions v2 must not be classic v1"
        );
        let mut v3 = BytesMut::new();
        encode_create_partitions_request(&mut v3, 3, &topics, 5_000, false).unwrap();
        assert_eq!(&buf[..], &v3[..], "CreatePartitions v3 request matches v2");
        assert!(
            encode_create_partitions_request(&mut BytesMut::new(), 4, &topics, 5_000, false)
                .is_err(),
            "CreatePartitions v4+ is not spoken"
        );

        let results = vec![TopicResult::new("t", 0, None)];
        buf.clear();
        encode_create_partitions_response(&mut buf, 2, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_partitions_response(&mut cur, 2).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v2 response must be leftover-empty"
        );
        let mut v3r = BytesMut::new();
        encode_create_partitions_response(&mut v3r, 3, &results).unwrap();
        assert_eq!(
            &buf[..],
            &v3r[..],
            "CreatePartitions v3 response matches v2"
        );
        let mut v1r = BytesMut::new();
        encode_create_partitions_response(&mut v1r, 1, &results).unwrap();
        assert_ne!(
            &buf[..],
            &v1r[..],
            "CreatePartitions v2 response must not be classic v1"
        );
    }

    #[test]
    fn create_partitions_assignments_roundtrip() {
        let topics = vec![CreatePartitionsTopic {
            name: "t".into(),
            count: 3,
            assignments: Some(vec![vec![1, 2]]),
        }];
        const V2: &[u8] = &[
            0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x03, 0x02, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x13, 0x88, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_create_partitions_request(&mut buf, 2, &topics, 5_000, false).unwrap();
        assert_eq!(&buf[..], V2);
        let mut cur = &buf[..];
        let (decoded, timeout_ms, validate) =
            decode_create_partitions_request(&mut cur, 2).unwrap();
        assert_eq!(decoded, topics);
        assert_eq!(timeout_ms, 5_000);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v2 assignments request must be leftover-empty"
        );
        buf.clear();
        encode_create_partitions_request(&mut buf, 0, &topics, 5_000, false).unwrap();
        let mut cur = &buf[..];
        let (decoded0, _, _) = decode_create_partitions_request(&mut cur, 0).unwrap();
        assert_eq!(decoded0, topics);
        assert!(
            !cur.has_remaining(),
            "CreatePartitions v0 assignments request must be leftover-empty"
        );
    }

    #[test]
    fn create_partitions_flexible_not_controller_is_leftover_empty() {
        let results = vec![TopicResult::new(
            "t",
            crate::error::NOT_CONTROLLER,
            Some("Not controller".into()),
        )];
        for version in [2i16, 3] {
            let mut buf = BytesMut::new();
            encode_create_partitions_response(&mut buf, version, &results).unwrap();
            let mut cur = &buf[..];
            let got = decode_create_partitions_response(&mut cur, version).unwrap();
            assert_eq!(got, results);
            assert!(
                !cur.has_remaining(),
                "CreatePartitions v{version} NOT_CONTROLLER must be leftover-empty"
            );
        }
    }

    #[test]
    fn incremental_alter_configs_not_controller_is_not_at_byte_four() {
        for version in [0i16, 1] {
            let mut buf = BytesMut::new();
            encode_incremental_alter_configs_response(
                &mut buf,
                version,
                crate::error::NOT_CONTROLLER,
                "t",
            )
            .unwrap();
            let b4 = buf.get(4).copied().unwrap();
            let b5 = buf.get(5).copied().unwrap();
            assert_ne!(
                i16::from_be_bytes([b4, b5]),
                crate::error::NOT_CONTROLLER,
                "throttle + resource-array length must not look like error 41 at v{version}"
            );
            let mut cur = &buf[..];
            assert_eq!(
                decode_incremental_alter_configs_response(&mut cur, version).unwrap(),
                crate::error::NOT_CONTROLLER
            );
            assert!(
                !cur.has_remaining(),
                "IncrementalAlterConfigs v{version} NOT_CONTROLLER must be leftover-empty"
            );
        }
    }

    #[test]
    fn incremental_alter_configs_v1_compact_layout_matches_independent_encode() {
        // Compact 1 resource type=2 name "t", 1 config "k"=SET "v",
        // validateOnly false, empty tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x02, 0x74, 0x02, 0x02, 0x6b, 0x00, 0x02, 0x76, 0x00, 0x00, 0x00, 0x00,
        ];
        let configs = [AlterConfig::set("k", "v")];
        let mut buf = BytesMut::new();
        encode_incremental_alter_configs_request(&mut buf, 1, RESOURCE_TOPIC, "t", &configs, false)
            .unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (rt, name, decoded, validate) =
            decode_incremental_alter_configs_request(&mut cur, 1).unwrap();
        assert_eq!(rt, RESOURCE_TOPIC);
        assert_eq!(name, "t");
        assert_eq!(decoded, configs);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v1 request must consume compact fields and tagged fields"
        );
        let mut v0 = BytesMut::new();
        encode_incremental_alter_configs_request(&mut v0, 0, RESOURCE_TOPIC, "t", &configs, false)
            .unwrap();
        assert_ne!(
            &buf[..],
            &v0[..],
            "IncrementalAlterConfigs v1 must not be classic v0"
        );
        let mut cur = &v0[..];
        let (rt0, name0, decoded0, validate0) =
            decode_incremental_alter_configs_request(&mut cur, 0).unwrap();
        assert_eq!(rt0, RESOURCE_TOPIC);
        assert_eq!(name0, "t");
        assert_eq!(decoded0, configs);
        assert!(!validate0);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v0 request must be leftover-empty"
        );
        assert!(
            encode_incremental_alter_configs_request(
                &mut BytesMut::new(),
                2,
                RESOURCE_TOPIC,
                "t",
                &configs,
                false
            )
            .is_err(),
            "IncrementalAlterConfigs v2+ is not spoken"
        );

        buf.clear();
        encode_incremental_alter_configs_response(&mut buf, 1, 0, "t").unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_incremental_alter_configs_response(&mut cur, 1).unwrap(),
            0
        );
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v1 response must be leftover-empty"
        );
        let mut v0r = BytesMut::new();
        encode_incremental_alter_configs_response(&mut v0r, 0, 0, "t").unwrap();
        assert_ne!(
            &buf[..],
            &v0r[..],
            "IncrementalAlterConfigs v1 response must not be classic v0"
        );
    }

    #[test]
    fn incremental_alter_configs_append_subtract_ops_roundtrip() {
        assert_eq!(ALTER_CONFIG_APPEND, 2);
        assert_eq!(ALTER_CONFIG_SUBTRACT, 3);
        const APPEND: &[u8] = &[
            0x02, 0x02, 0x02, 0x74, 0x02, 0x02, 0x6b, 0x02, 0x02, 0x76, 0x00, 0x00, 0x00, 0x00,
        ];
        const SUBTRACT: &[u8] = &[
            0x02, 0x02, 0x02, 0x74, 0x02, 0x02, 0x6b, 0x03, 0x02, 0x76, 0x00, 0x00, 0x00, 0x00,
        ];
        let append = [AlterConfig::append("k", "v")];
        let mut buf = BytesMut::new();
        encode_incremental_alter_configs_request(&mut buf, 1, RESOURCE_TOPIC, "t", &append, false)
            .unwrap();
        assert_eq!(&buf[..], APPEND);
        let mut cur = &buf[..];
        let (_, _, decoded, _) = decode_incremental_alter_configs_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, append);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs APPEND request must be leftover-empty"
        );
        let subtract = [AlterConfig::subtract("k", "v")];
        buf.clear();
        encode_incremental_alter_configs_request(
            &mut buf,
            1,
            RESOURCE_TOPIC,
            "t",
            &subtract,
            false,
        )
        .unwrap();
        assert_eq!(&buf[..], SUBTRACT);
        let mut cur = &buf[..];
        let (_, _, decoded, _) = decode_incremental_alter_configs_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, subtract);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs SUBTRACT request must be leftover-empty"
        );
    }

    #[test]
    fn incremental_alter_configs_v1_resources_of_two_matches_independent_encode() {
        const REQ: &[u8] = &[
            0x03, 0x02, 0x02, 0x61, 0x02, 0x02, 0x6b, 0x00, 0x02, 0x31, 0x00, 0x00, 0x02, 0x02,
            0x62, 0x02, 0x02, 0x6b, 0x00, 0x02, 0x32, 0x00, 0x00, 0x00, 0x00,
        ];
        let resources = [
            AlterableResource {
                resource_type: RESOURCE_TOPIC,
                name: "a".into(),
                configs: vec![AlterConfig::set("k", "1")],
            },
            AlterableResource {
                resource_type: RESOURCE_TOPIC,
                name: "b".into(),
                configs: vec![AlterConfig::set("k", "2")],
            },
        ];
        let mut buf = BytesMut::new();
        encode_incremental_alter_configs_resources_request(&mut buf, 1, &resources, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (got, validate) =
            decode_incremental_alter_configs_resources_request(&mut cur, 1).unwrap();
        assert_eq!(got, resources);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v1 Resources of 2 must be leftover-empty"
        );

        let mut v0 = BytesMut::new();
        encode_incremental_alter_configs_resources_request(&mut v0, 0, &resources, false).unwrap();
        const V0: &[u8] = &[
            0x00, 0x00, 0x00, 0x02, 0x02, 0x00, 0x01, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x6b, 0x00, 0x00, 0x01, 0x31, 0x02, 0x00, 0x01, 0x62, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x01, 0x6b, 0x00, 0x00, 0x01, 0x32, 0x00,
        ];
        assert_eq!(&v0[..], V0);
        let mut cur = &v0[..];
        let (got0, validate0) =
            decode_incremental_alter_configs_resources_request(&mut cur, 0).unwrap();
        assert_eq!(got0, resources);
        assert!(!validate0);
        assert!(
            !cur.has_remaining(),
            "IncrementalAlterConfigs v0 Resources of 2 must be leftover-empty"
        );
    }

    #[test]
    fn delete_topics_not_controller_is_not_at_byte_four() {
        let results = vec![TopicResult::new("t", crate::error::NOT_CONTROLLER, None)];
        let mut buf = BytesMut::new();
        encode_delete_topics_response(&mut buf, 3, &results).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "throttle + topic-array length must not look like error 41"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_delete_topics_response(&mut cur, 3).unwrap(), results);
        assert!(
            !cur.has_remaining(),
            "DeleteTopics v3 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_configs_v1_roundtrip() {
        let resources = vec![DescribeConfigsResource {
            resource_type: RESOURCE_TOPIC,
            name: "orders".into(),
            keys: Some(vec!["cleanup.policy".into()]),
        }];
        let mut buf = BytesMut::new();
        encode_describe_configs_request(&mut buf, 1, &resources, true, false).unwrap();
        let mut cur = &buf[..];
        let (decoded, syn, docs) = decode_describe_configs_request(&mut cur, 1).unwrap();
        assert_eq!(decoded, resources);
        assert!(syn);
        assert!(!docs);
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v1 request must be leftover-empty"
        );

        let results = vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: "orders".into(),
            entries: vec![ConfigEntry {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                read_only: false,
                source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: vec![ConfigSynonym {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    source: CONFIG_SOURCE_DEFAULT,
                }],
                config_type: CONFIG_TYPE_UNKNOWN,
                documentation: None,
            }],
        }];
        buf.clear();
        encode_describe_configs_response(&mut buf, 1, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_configs_response(&mut cur, 1).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v1 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_configs_v0_is_default() {
        let results = vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_BROKER,
            name: "1".into(),
            entries: vec![ConfigEntry {
                name: "log.retention.hours".into(),
                value: Some("168".into()),
                read_only: true,
                source: CONFIG_SOURCE_DEFAULT,
                is_sensitive: false,
                synonyms: vec![],
                config_type: CONFIG_TYPE_UNKNOWN,
                documentation: None,
            }],
        }];
        let mut buf = BytesMut::new();
        encode_describe_configs_response(&mut buf, 0, &results).unwrap();
        let mut cur = &buf[..];
        let decoded = decode_describe_configs_response(&mut cur, 0).unwrap();
        assert_eq!(decoded[0].entries[0].source, CONFIG_SOURCE_DEFAULT);
        assert!(decoded[0].entries[0].synonyms.is_empty());
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v0 response must be leftover-empty"
        );
    }

    fn sample_describe_configs_resources() -> Vec<DescribeConfigsResource> {
        vec![DescribeConfigsResource {
            resource_type: RESOURCE_TOPIC,
            name: "t".into(),
            keys: None,
        }]
    }

    fn sample_describe_configs_results() -> Vec<DescribeConfigsResult> {
        vec![DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: "t".into(),
            entries: vec![ConfigEntry {
                name: "cleanup.policy".into(),
                value: Some("delete".into()),
                read_only: false,
                source: CONFIG_SOURCE_DEFAULT,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: CONFIG_TYPE_STRING,
                documentation: Some("docs".into()),
            }],
        }]
    }

    #[test]
    fn describe_configs_v2_request_matches_v1() {
        let resources = sample_describe_configs_resources();
        let mut v1 = BytesMut::new();
        encode_describe_configs_request(&mut v1, 1, &resources, false, false).unwrap();
        let mut v2 = BytesMut::new();
        encode_describe_configs_request(&mut v2, 2, &resources, false, false).unwrap();
        assert_eq!(&v1[..], &v2[..], "DescribeConfigs v2 request matches v1");
        let mut cur = &v2[..];
        let (decoded, syn, docs) = decode_describe_configs_request(&mut cur, 2).unwrap();
        assert_eq!(decoded, resources);
        assert!(!syn);
        assert!(!docs);
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v2 request must be leftover-empty"
        );
    }

    #[test]
    fn describe_configs_v3_adds_include_documentation_and_config_type() {
        let resources = sample_describe_configs_resources();
        let mut v2 = BytesMut::new();
        encode_describe_configs_request(&mut v2, 2, &resources, false, true).unwrap();
        let mut v3 = BytesMut::new();
        encode_describe_configs_request(&mut v3, 3, &resources, false, true).unwrap();
        assert_ne!(
            &v2[..],
            &v3[..],
            "DescribeConfigs v3 request must include IncludeDocumentation"
        );
        let mut cur = &v3[..];
        let (decoded, syn, docs) = decode_describe_configs_request(&mut cur, 3).unwrap();
        assert_eq!(decoded, resources);
        assert!(!syn);
        assert!(docs);
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v3 request must be leftover-empty"
        );

        let results = sample_describe_configs_results();
        let mut v2r = BytesMut::new();
        encode_describe_configs_response(&mut v2r, 2, &results).unwrap();
        let mut v3r = BytesMut::new();
        encode_describe_configs_response(&mut v3r, 3, &results).unwrap();
        assert_ne!(
            &v2r[..],
            &v3r[..],
            "DescribeConfigs v3 response must include ConfigType and Documentation"
        );
        let mut cur = &v3r[..];
        assert_eq!(
            decode_describe_configs_response(&mut cur, 3).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v3 response must be leftover-empty"
        );
        let mut cur = &v2r[..];
        let got2 = decode_describe_configs_response(&mut cur, 2).unwrap();
        assert_eq!(got2[0].entries[0].config_type, CONFIG_TYPE_UNKNOWN);
        assert_eq!(got2[0].entries[0].documentation, None);
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v2 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_configs_v4_compact_layout_matches_independent_encode() {
        // Compact 1 resource type=2 name "t", null keys, IncludeSynonyms
        // false, IncludeDocumentation false, empty tagged fields.
        const REQ: &[u8] = &[0x02, 0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00];
        let resources = sample_describe_configs_resources();
        let mut buf = BytesMut::new();
        encode_describe_configs_request(&mut buf, 4, &resources, false, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (decoded, syn, docs) = decode_describe_configs_request(&mut cur, 4).unwrap();
        assert_eq!(decoded, resources);
        assert!(!syn);
        assert!(!docs);
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v4 request must consume compact fields and tagged fields"
        );
        let mut v3 = BytesMut::new();
        encode_describe_configs_request(&mut v3, 3, &resources, false, false).unwrap();
        assert_ne!(
            &buf[..],
            &v3[..],
            "DescribeConfigs v4 must not be classic v3"
        );
        assert!(
            encode_describe_configs_request(&mut BytesMut::new(), 5, &resources, false, false)
                .is_err(),
            "DescribeConfigs v5+ is not spoken"
        );

        let results = sample_describe_configs_results();
        buf.clear();
        encode_describe_configs_response(&mut buf, 4, &results).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_configs_response(&mut cur, 4).unwrap(),
            results
        );
        assert!(
            !cur.has_remaining(),
            "DescribeConfigs v4 response must be leftover-empty"
        );
        let mut v3r = BytesMut::new();
        encode_describe_configs_response(&mut v3r, 3, &results).unwrap();
        assert_ne!(
            &buf[..],
            &v3r[..],
            "DescribeConfigs v4 response must not be classic v3"
        );
    }

    #[test]
    fn alter_configs_v1_roundtrip() {
        let configs = [TopicConfig {
            name: "retention.ms".into(),
            value: Some("1".into()),
        }];
        let mut buf = BytesMut::new();
        encode_alter_configs_request(&mut buf, 1, RESOURCE_TOPIC, "t", &configs, false).unwrap();
        let mut cur = &buf[..];
        let (rt, name, decoded, validate) = decode_alter_configs_request(&mut cur, 1).unwrap();
        assert_eq!(rt, RESOURCE_TOPIC);
        assert_eq!(name, "t");
        assert_eq!(decoded, configs);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v1 request must be leftover-empty"
        );
        buf.clear();
        encode_alter_configs_response(&mut buf, 1, 0, "t").unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_alter_configs_response(&mut cur, 1).unwrap(), 0);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v1 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_configs_v2_compact_layout_matches_independent_encode() {
        // Compact 1 resource type=2 name "t", 1 config "k"="v",
        // validateOnly false, empty tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x02, 0x74, 0x02, 0x02, 0x6b, 0x02, 0x76, 0x00, 0x00, 0x00, 0x00,
        ];
        let configs = [TopicConfig {
            name: "k".into(),
            value: Some("v".into()),
        }];
        let mut buf = BytesMut::new();
        encode_alter_configs_request(&mut buf, 2, RESOURCE_TOPIC, "t", &configs, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (rt, name, decoded, validate) = decode_alter_configs_request(&mut cur, 2).unwrap();
        assert_eq!(rt, RESOURCE_TOPIC);
        assert_eq!(name, "t");
        assert_eq!(decoded, configs);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v2 request must consume compact fields and tagged fields"
        );
        let mut v1 = BytesMut::new();
        encode_alter_configs_request(&mut v1, 1, RESOURCE_TOPIC, "t", &configs, false).unwrap();
        assert_ne!(&buf[..], &v1[..], "AlterConfigs v2 must not be classic v1");
        assert!(
            encode_alter_configs_request(
                &mut BytesMut::new(),
                3,
                RESOURCE_TOPIC,
                "t",
                &configs,
                false
            )
            .is_err(),
            "AlterConfigs v3+ is not spoken"
        );

        buf.clear();
        encode_alter_configs_response(&mut buf, 2, 0, "t").unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_alter_configs_response(&mut cur, 2).unwrap(), 0);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v2 response must be leftover-empty"
        );
        let mut v1r = BytesMut::new();
        encode_alter_configs_response(&mut v1r, 1, 0, "t").unwrap();
        assert_ne!(
            &buf[..],
            &v1r[..],
            "AlterConfigs v2 response must not be classic v1"
        );
        let mut v0r = BytesMut::new();
        encode_alter_configs_response(&mut v0r, 0, 0, "t").unwrap();
        assert_ne!(
            &v1r[..],
            &v0r[..],
            "AlterConfigs v1 response must include ThrottleTimeMs"
        );
        let mut cur = &v0r[..];
        assert_eq!(decode_alter_configs_response(&mut cur, 0).unwrap(), 0);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_configs_v2_resources_of_two_matches_independent_encode() {
        const REQ: &[u8] = &[
            0x03, 0x02, 0x02, 0x61, 0x02, 0x02, 0x6b, 0x02, 0x31, 0x00, 0x00, 0x02, 0x02, 0x62,
            0x02, 0x02, 0x6b, 0x02, 0x32, 0x00, 0x00, 0x00, 0x00,
        ];
        let resources = [
            AlterConfigsResource {
                resource_type: RESOURCE_TOPIC,
                name: "a".into(),
                configs: vec![TopicConfig {
                    name: "k".into(),
                    value: Some("1".into()),
                }],
            },
            AlterConfigsResource {
                resource_type: RESOURCE_TOPIC,
                name: "b".into(),
                configs: vec![TopicConfig {
                    name: "k".into(),
                    value: Some("2".into()),
                }],
            },
        ];
        let mut buf = BytesMut::new();
        encode_alter_configs_resources_request(&mut buf, 2, &resources, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (got, validate) = decode_alter_configs_resources_request(&mut cur, 2).unwrap();
        assert_eq!(got, resources);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v2 Resources of 2 must be leftover-empty"
        );

        let mut v1 = BytesMut::new();
        encode_alter_configs_resources_request(&mut v1, 1, &resources, false).unwrap();
        const V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x02, 0x02, 0x00, 0x01, 0x61, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x6b, 0x00, 0x01, 0x31, 0x02, 0x00, 0x01, 0x62, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x6b, 0x00, 0x01, 0x32, 0x00,
        ];
        assert_eq!(&v1[..], V1);
        let mut cur = &v1[..];
        let (got1, validate1) = decode_alter_configs_resources_request(&mut cur, 1).unwrap();
        assert_eq!(got1, resources);
        assert!(!validate1);
        assert!(
            !cur.has_remaining(),
            "AlterConfigs v1 Resources of 2 must be leftover-empty"
        );
    }

    #[test]
    fn delete_records_v1_roundtrip() {
        let mut buf = BytesMut::new();
        encode_delete_records_request(&mut buf, 1, "t", 0, 5, 1000).unwrap();
        let mut cur = &buf[..];
        let (topic, part, off, timeout) = decode_delete_records_request(&mut cur, 1).unwrap();
        assert_eq!((topic.as_str(), part, off, timeout), ("t", 0, 5, 1000));
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v1 request must be leftover-empty"
        );
        buf.clear();
        encode_delete_records_response(&mut buf, 1, "t", 0, 5, 0).unwrap();
        let mut cur = &buf[..];
        let (p, low, err) = decode_delete_records_response(&mut cur, 1).unwrap();
        assert_eq!((p, low, err), (0, 5, 0));
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v1 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_records_v2_compact_layout_matches_independent_encode() {
        // Compact 1 topic "t", 1 partition 0, offset 5, timeout 1000,
        // empty tagged fields on partition, topic, and top-level.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_delete_records_request(&mut buf, 2, "t", 0, 5, 1000).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let (topic, part, off, timeout) = decode_delete_records_request(&mut cur, 2).unwrap();
        assert_eq!((topic.as_str(), part, off, timeout), ("t", 0, 5, 1000));
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v2 request must consume compact fields and tagged fields"
        );
        let mut v1 = BytesMut::new();
        encode_delete_records_request(&mut v1, 1, "t", 0, 5, 1000).unwrap();
        assert_ne!(&buf[..], &v1[..], "DeleteRecords v2 must not be classic v1");
        assert!(
            encode_delete_records_request(&mut BytesMut::new(), 3, "t", 0, 5, 1000).is_err(),
            "DeleteRecords v3+ is not spoken"
        );

        buf.clear();
        encode_delete_records_response(&mut buf, 2, "t", 0, 5, 0).unwrap();
        let mut cur = &buf[..];
        let (p, low, err) = decode_delete_records_response(&mut cur, 2).unwrap();
        assert_eq!((p, low, err), (0, 5, 0));
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v2 response must be leftover-empty"
        );
        let mut v1r = BytesMut::new();
        encode_delete_records_response(&mut v1r, 1, "t", 0, 5, 0).unwrap();
        assert_ne!(
            &buf[..],
            &v1r[..],
            "DeleteRecords v2 response must not be classic v1"
        );
        let mut v0r = BytesMut::new();
        encode_delete_records_response(&mut v0r, 0, "t", 0, 5, 0).unwrap();
        assert_ne!(
            &v1r[..],
            &v0r[..],
            "DeleteRecords v1 response must include ThrottleTimeMs"
        );
        let mut cur = &v0r[..];
        let (p0, low0, err0) = decode_delete_records_response(&mut cur, 0).unwrap();
        assert_eq!((p0, low0, err0), (0, 5, 0));
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v0 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_records_v2_topics_array_of_two_partitions() {
        let topics = vec![DeleteRecordsTopic {
            topic: "t".into(),
            partitions: vec![
                DeleteRecordsPartition {
                    partition: 0,
                    offset: 5,
                },
                DeleteRecordsPartition {
                    partition: 1,
                    offset: 9,
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_delete_records_topics_request(&mut buf, 2, &topics, 1000).unwrap();
        let mut cur = buf.as_ref();
        let (got, timeout) = decode_delete_records_topics_request(&mut cur, 2).unwrap();
        assert_eq!(timeout, 1000);
        assert_eq!(got, topics);
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v2 Topics-of-2 leftover-empty"
        );
        // Compact 1 topic "t", 2 partitions, timeout 1000, tagged fields.
        const REQ: &[u8] = &[
            0x02, 0x02, 0x74, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x09, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x00,
        ];
        assert_eq!(&buf[..], REQ);

        let one = vec![DeleteRecordsTopic {
            topic: "t".into(),
            partitions: vec![DeleteRecordsPartition {
                partition: 0,
                offset: 5,
            }],
        }];
        let mut via_topics = BytesMut::new();
        encode_delete_records_topics_request(&mut via_topics, 2, &one, 1000).unwrap();
        let mut via_one = BytesMut::new();
        encode_delete_records_request(&mut via_one, 2, "t", 0, 5, 1000).unwrap();
        assert_eq!(
            via_topics.as_ref(),
            via_one.as_ref(),
            "Topics of 1 must match encode_delete_records_request"
        );

        let resp = vec![DeletedRecordsTopic {
            topic: "t".into(),
            partitions: vec![
                DeletedRecordsPartition {
                    partition: 0,
                    low_watermark: 5,
                    error_code: 0,
                },
                DeletedRecordsPartition {
                    partition: 1,
                    low_watermark: 9,
                    error_code: 0,
                },
            ],
        }];
        buf.clear();
        encode_delete_records_topics_response(&mut buf, 2, &resp).unwrap();
        let mut cur = buf.as_ref();
        let decoded = decode_delete_records_topics_response(&mut cur, 2).unwrap();
        assert_eq!(decoded, resp);
        assert!(
            !cur.has_remaining(),
            "DeleteRecords v2 Topics-of-2 response leftover-empty"
        );
    }

    #[test]
    fn describe_cluster_v0_roundtrip() {
        let desc = ClusterDescription {
            error_code: 0,
            error_message: None,
            cluster_id: Some("mock".into()),
            controller_id: 1,
            endpoint_type: ENDPOINT_TYPE_BROKERS,
            cluster_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
            brokers: vec![DescribeClusterBroker::new(
                1,
                "127.0.0.1",
                9092,
                None,
                false,
            )],
        };
        let mut req = BytesMut::new();
        encode_describe_cluster_request(&mut req, 0, false, ENDPOINT_TYPE_BROKERS, false).unwrap();
        let mut cur = &req[..];
        let (include, endpoint, fenced) = decode_describe_cluster_request(&mut cur, 0).unwrap();
        assert!(!include);
        assert_eq!(endpoint, ENDPOINT_TYPE_BROKERS);
        assert!(!fenced);
        assert!(
            !cur.has_remaining(),
            "DescribeCluster v0 request must be leftover-empty"
        );
        let mut buf = BytesMut::new();
        encode_describe_cluster_response(&mut buf, 0, &desc).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_cluster_response(&mut cur, 0).unwrap(), desc);
        assert!(
            !cur.has_remaining(),
            "DescribeCluster v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_cluster_v2_compact_layout_matches_independent_encode() {
        // IncludeClusterAuthorizedOperations false, EndpointType brokers,
        // IncludeFencedBrokers false, empty tagged fields.
        const REQ_V2: &[u8] = &[0x00, 0x01, 0x00, 0x00];
        const REQ_V1: &[u8] = &[0x00, 0x01, 0x00];
        const REQ_V0: &[u8] = &[0x00, 0x00];
        let mut buf = BytesMut::new();
        encode_describe_cluster_request(&mut buf, 2, false, ENDPOINT_TYPE_BROKERS, false).unwrap();
        assert_eq!(&buf[..], REQ_V2);
        let mut cur = &buf[..];
        let (include, endpoint, fenced) = decode_describe_cluster_request(&mut cur, 2).unwrap();
        assert!(!include);
        assert_eq!(endpoint, ENDPOINT_TYPE_BROKERS);
        assert!(!fenced);
        assert!(
            !cur.has_remaining(),
            "DescribeCluster v2 request must consume compact fields and tagged fields"
        );
        let mut v1 = BytesMut::new();
        encode_describe_cluster_request(&mut v1, 1, false, ENDPOINT_TYPE_BROKERS, false).unwrap();
        assert_eq!(&v1[..], REQ_V1);
        assert_ne!(&buf[..], &v1[..], "DescribeCluster v2 must include fenced");
        let mut v0 = BytesMut::new();
        encode_describe_cluster_request(&mut v0, 0, false, ENDPOINT_TYPE_BROKERS, false).unwrap();
        assert_eq!(&v0[..], REQ_V0);
        assert_ne!(
            &v1[..],
            &v0[..],
            "DescribeCluster v1 must include EndpointType"
        );
        assert!(
            encode_describe_cluster_request(
                &mut BytesMut::new(),
                3,
                false,
                ENDPOINT_TYPE_BROKERS,
                false
            )
            .is_err(),
            "DescribeCluster v3+ is not spoken"
        );

        buf.clear();
        encode_describe_cluster_request(&mut buf, 2, true, ENDPOINT_TYPE_CONTROLLERS, true)
            .unwrap();
        assert_eq!(&buf[..], &[0x01, 0x02, 0x01, 0x00]);
        let mut cur = &buf[..];
        let (include, endpoint, fenced) = decode_describe_cluster_request(&mut cur, 2).unwrap();
        assert!(include);
        assert_eq!(endpoint, ENDPOINT_TYPE_CONTROLLERS);
        assert!(fenced);

        let desc = ClusterDescription {
            error_code: 0,
            error_message: None,
            cluster_id: Some("m".into()),
            controller_id: 1,
            endpoint_type: ENDPOINT_TYPE_BROKERS,
            cluster_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
            brokers: vec![DescribeClusterBroker::new(1, "h", 9092, None, false)],
        };
        // Throttle 0, error 0, null message, EndpointType 1, cluster "m",
        // controller 1, 1 broker id=1 host "h" port 9092 rack null unfenced,
        // authorized ops omitted, empty tagged fields.
        const RESP_V2: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x6d, 0x00, 0x00, 0x00, 0x01,
            0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x68, 0x00, 0x00, 0x23, 0x84, 0x00, 0x00, 0x00,
            0x80, 0x00, 0x00, 0x00, 0x00,
        ];
        buf.clear();
        encode_describe_cluster_response(&mut buf, 2, &desc).unwrap();
        assert_eq!(&buf[..], RESP_V2);
        let mut cur = &buf[..];
        assert_eq!(decode_describe_cluster_response(&mut cur, 2).unwrap(), desc);
        assert!(
            !cur.has_remaining(),
            "DescribeCluster v2 response must be leftover-empty"
        );
        let mut v1r = BytesMut::new();
        encode_describe_cluster_response(&mut v1r, 1, &desc).unwrap();
        assert_ne!(
            &buf[..],
            &v1r[..],
            "DescribeCluster v2 response must include IsFenced"
        );
        let mut v0r = BytesMut::new();
        encode_describe_cluster_response(&mut v0r, 0, &desc).unwrap();
        assert_ne!(
            &v1r[..],
            &v0r[..],
            "DescribeCluster v1 response must include EndpointType"
        );
        let mut cur = &v0r[..];
        let got0 = decode_describe_cluster_response(&mut cur, 0).unwrap();
        assert_eq!(got0.endpoint_type, ENDPOINT_TYPE_BROKERS);
        assert!(!got0.brokers[0].is_fenced);
        assert!(
            encode_describe_cluster_response(&mut BytesMut::new(), 3, &desc).is_err(),
            "DescribeCluster v3+ is not spoken"
        );
    }

    #[test]
    fn alter_partition_reassignments_v0_roundtrip_is_leftover_empty() {
        let topics = vec![ReassignableTopic {
            name: "t".into(),
            partitions: vec![
                ReassignablePartition {
                    partition_index: 0,
                    replicas: Some(vec![1, 2]),
                },
                ReassignablePartition {
                    partition_index: 1,
                    replicas: None,
                },
            ],
        }];
        let mut buf = BytesMut::new();
        encode_alter_partition_reassignments_request(&mut buf, 10_000, &topics).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_alter_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got, topics);
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 request must be leftover-empty"
        );

        let resp = AlterPartitionReassignmentsResponse {
            error_code: 0,
            error_message: None,
            results: vec![ReassignmentTopicResult {
                name: "t".into(),
                partitions: vec![
                    ReassignmentPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                        error_message: None,
                    },
                    ReassignmentPartitionResult {
                        partition_index: 1,
                        error_code: 0,
                        error_message: None,
                    },
                ],
            }],
        };
        buf.clear();
        encode_alter_partition_reassignments_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_partition_reassignments_not_controller_is_at_byte_four() {
        let resp = AlterPartitionReassignmentsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_alter_partition_reassignments_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterPartitionReassignments v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn list_partition_reassignments_v0_roundtrip_is_leftover_empty() {
        let topics = vec![ListReassignmentTopic {
            name: "t".into(),
            partition_indexes: vec![0, 1],
        }];
        let mut buf = BytesMut::new();
        encode_list_partition_reassignments_request(&mut buf, 10_000, Some(&topics)).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_list_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got.as_deref(), Some(topics.as_slice()));
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 request must be leftover-empty"
        );

        buf.clear();
        encode_list_partition_reassignments_request(&mut buf, 5_000, None).unwrap();
        let mut cur = &buf[..];
        let (timeout, got) = decode_list_partition_reassignments_request(&mut cur).unwrap();
        assert_eq!(timeout, 5_000);
        assert_eq!(got, None);
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 null-topics request must be leftover-empty"
        );

        let resp = ListPartitionReassignmentsResponse {
            error_code: 0,
            error_message: None,
            topics: vec![OngoingTopicReassignment {
                name: "t".into(),
                partitions: vec![OngoingPartitionReassignment {
                    partition_index: 0,
                    replicas: vec![1, 2, 3],
                    adding_replicas: vec![3],
                    removing_replicas: vec![1],
                }],
            }],
        };
        buf.clear();
        encode_list_partition_reassignments_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 response must be leftover-empty"
        );
    }

    #[test]
    fn list_partition_reassignments_not_controller_is_at_byte_four() {
        // Official v0 body: throttle_time_ms INT32, then error_code INT16.
        // That is the same field order as AlterPartitionReassignments v0,
        // verified from the Apache JSON (not copied from Alter).
        let resp = ListPartitionReassignmentsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            topics: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_list_partition_reassignments_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_partition_reassignments_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListPartitionReassignments v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn update_features_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client+broker).
        // Apache JSON: timeout INT32, compact FeatureUpdates
        // {Feature compact string, MaxVersionLevel INT16, AllowDowngrade
        // BOOLEAN, tagged}, tagged. Response: throttle INT32, error INT16,
        // compact-nullable ErrorMessage, compact Results (v0-1), tagged.
        const REQ: &[u8] = &[
            0x00, 0x00, 0x27, 0x10, 0x03, 0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61,
            0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x11, 0x00, 0x00, 0x0e, 0x67,
            0x72, 0x6f, 0x75, 0x70, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x01,
            0x01, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x01, 0x00,
        ];
        let updates = vec![
            FeatureUpdateKey::new("metadata.version", 17, false),
            FeatureUpdateKey::new("group.version", 1, true),
        ];
        let mut buf = BytesMut::new();
        encode_update_features_request(&mut buf, 0, 10_000, &updates, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = UpdateFeaturesResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        buf.clear();
        encode_update_features_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn update_features_v0_roundtrip_is_leftover_empty() {
        let updates = vec![
            FeatureUpdateKey::new("metadata.version", 17, false),
            FeatureUpdateKey::new("group.version", 1, true),
        ];
        let mut buf = BytesMut::new();
        encode_update_features_request(&mut buf, 0, 10_000, &updates, false).unwrap();
        let mut cur = &buf[..];
        let (timeout, got, validate) = decode_update_features_request(&mut cur, 0).unwrap();
        assert_eq!(timeout, 10_000);
        assert_eq!(got, updates);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 request must be leftover-empty"
        );

        let resp = UpdateFeaturesResponse {
            error_code: 0,
            error_message: None,
            results: vec![
                UpdatableFeatureResult {
                    name: "metadata.version".into(),
                    error_code: 0,
                    error_message: None,
                },
                UpdatableFeatureResult {
                    name: "group.version".into(),
                    error_code: 0,
                    error_message: None,
                },
            ],
        };
        buf.clear();
        encode_update_features_response(&mut buf, 0, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_update_features_response(&mut cur, 0).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 response must be leftover-empty"
        );
    }

    #[test]
    fn update_features_not_controller_is_at_byte_four() {
        // Official v0 body: throttle_time_ms INT32, then error_code INT16.
        // Verified from Apache UpdateFeaturesResponse.json and
        // kafka-protocol 0.18.0 (not copied from List/Alter).
        let resp = UpdateFeaturesResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_update_features_response(&mut buf, 0, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_update_features_response(&mut cur, 0).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn update_features_v2_compact_layout_matches_independent_encode() {
        // timeout 1000, 1 update "f" max 1, UpgradeType upgrade, ValidateOnly
        // false, empty tagged fields on the feature and top-level.
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 0x66, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00,
        ];
        const REQ_V0: &[u8] = &[
            0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 0x66, 0x00, 0x01, 0x00, 0x00, 0x00,
        ];
        let updates = vec![FeatureUpdateKey::new("f", 1, false)];
        let mut buf = BytesMut::new();
        encode_update_features_request(&mut buf, 2, 1000, &updates, false).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        let mut v1 = BytesMut::new();
        encode_update_features_request(&mut v1, 1, 1000, &updates, false).unwrap();
        assert_eq!(&v1[..], REQ_V1, "UpdateFeatures v2 request matches v1");
        let mut v0 = BytesMut::new();
        encode_update_features_request(&mut v0, 0, 1000, &updates, false).unwrap();
        assert_eq!(&v0[..], REQ_V0);
        assert_ne!(
            &buf[..],
            &v0[..],
            "UpdateFeatures v1+ must send UpgradeType"
        );
        let mut cur = &buf[..];
        let (timeout, got, validate) = decode_update_features_request(&mut cur, 2).unwrap();
        assert_eq!(timeout, 1000);
        assert_eq!(got, updates);
        assert!(!validate);
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v2 request must consume compact fields and tagged fields"
        );
        buf.clear();
        encode_update_features_request(&mut buf, 2, 1000, &updates, true).unwrap();
        assert_eq!(
            &buf[..],
            &[0x00, 0x00, 0x03, 0xe8, 0x02, 0x02, 0x66, 0x00, 0x01, 0x01, 0x00, 0x01, 0x00]
        );
        assert!(
            encode_update_features_request(&mut BytesMut::new(), 3, 1000, &updates, false).is_err(),
            "UpdateFeatures v3+ is not spoken"
        );

        let resp = UpdateFeaturesResponse {
            error_code: 0,
            error_message: None,
            results: vec![UpdatableFeatureResult {
                name: "f".into(),
                error_code: 0,
                error_message: None,
            }],
        };
        // v2 omits Results: throttle 0, error 0, null message, tagged.
        const RESP_V2: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        buf.clear();
        encode_update_features_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V2);
        let mut cur = &buf[..];
        let got = decode_update_features_response(&mut cur, 2).unwrap();
        assert_eq!(got.error_code, 0);
        assert!(got.results.is_empty(), "UpdateFeatures v2 omits Results");
        assert!(
            !cur.has_remaining(),
            "UpdateFeatures v2 response must be leftover-empty"
        );
        let mut v1r = BytesMut::new();
        encode_update_features_response(&mut v1r, 1, &resp).unwrap();
        assert_ne!(
            &buf[..],
            &v1r[..],
            "UpdateFeatures v2 response must omit Results"
        );
        let mut cur = &v1r[..];
        assert_eq!(decode_update_features_response(&mut cur, 1).unwrap(), resp);
        assert!(
            encode_update_features_response(&mut BytesMut::new(), 3, &resp).is_err(),
            "UpdateFeatures v3+ is not spoken"
        );
    }

    #[test]
    fn alter_user_scram_credentials_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 51 v0 is
        // flexible, no timeout, no top-level response error.
        const REQ: &[u8] = &[
            0x02, 0x08, 0x6f, 0x6c, 0x64, 0x75, 0x73, 0x65, 0x72, 0x02, 0x00, 0x02, 0x06, 0x61,
            0x6c, 0x69, 0x63, 0x65, 0x01, 0x00, 0x00, 0x10, 0x00, 0x0b, 0x64, 0x75, 0x6d, 0x6d,
            0x79, 0x2d, 0x73, 0x61, 0x6c, 0x74, 0x0d, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x2d, 0x73,
            0x61, 0x6c, 0x74, 0x65, 0x64, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x29, 0x0f,
            0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72,
            0x00, 0x00,
        ];
        let deletions = vec![ScramCredentialDeletion {
            name: "olduser".into(),
            mechanism: SCRAM_SHA_512,
        }];
        let upsertions = vec![ScramCredentialUpsertion {
            name: "alice".into(),
            mechanism: SCRAM_SHA_256,
            iterations: 4096,
            salt: b"dummy-salt".to_vec(),
            salted_password: b"dummy-salted".to_vec(),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_request(&mut buf, &deletions, &upsertions).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        }];
        buf.clear();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn alter_user_scram_credentials_v0_roundtrip_is_leftover_empty() {
        let deletions = vec![ScramCredentialDeletion {
            name: "olduser".into(),
            mechanism: SCRAM_SHA_512,
        }];
        let upsertions = vec![ScramCredentialUpsertion {
            name: "alice".into(),
            mechanism: SCRAM_SHA_256,
            iterations: 4096,
            salt: b"dummy-salt".to_vec(),
            salted_password: b"dummy-salted".to_vec(),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_request(&mut buf, &deletions, &upsertions).unwrap();
        let mut cur = &buf[..];
        let (got_del, got_up) = decode_alter_user_scram_credentials_request(&mut cur).unwrap();
        assert_eq!(got_del, deletions);
        assert_eq!(got_up, upsertions);
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 request must be leftover-empty"
        );

        let resp = vec![
            AlterUserScramCredentialsResult {
                user: "olduser".into(),
                error_code: 0,
                error_message: None,
            },
            AlterUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
            },
        ];
        buf.clear();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_user_scram_credentials_not_controller_is_after_user() {
        // Official v0 body: throttle INT32, compact Results[], then each
        // result is compact User then ErrorCode INT16. Verified from Apache
        // AlterUserScramCredentialsResponse.json and kafka-protocol 0.18.0.
        // Not copied from UpdateFeatures (top-level 41 at bytes 4-5).
        let resp = vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        }];
        let mut buf = BytesMut::new();
        encode_alter_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 has no top-level error; bytes 4-5 are compact array/user, not 41"
        );
        let b11 = buf.get(11).copied().unwrap();
        let b12 = buf.get(12).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b11, b12]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is the first result ErrorCode after throttle, results len, and User"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterUserScramCredentials v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn alter_client_quotas_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 49
        // validVersions 0-1, flexibleVersions 1+. This crate speaks 0–1;
        // this fixture is v1.
        // v0 is classic (not copied from AlterUserScramCredentials).
        const REQ: &[u8] = &[
            0x02, 0x02, 0x05, 0x75, 0x73, 0x65, 0x72, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00,
            0x02, 0x13, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x62, 0x79, 0x74,
            0x65, 0x5f, 0x72, 0x61, 0x74, 0x65, 0x40, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f,
            0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x02, 0x05, 0x75, 0x73, 0x65, 0x72,
            0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00,
        ];
        let entries = vec![ClientQuotaAlteration::new(
            vec![ClientQuotaEntity::new("user", Some("alice".into()))],
            vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
        )];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_request(&mut buf, 1, &entries, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![ClientQuotaAlterationResult {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        }];
        buf.clear();
        encode_alter_client_quotas_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn alter_client_quotas_v1_roundtrip_is_leftover_empty() {
        let entries = vec![
            ClientQuotaAlteration::new(
                vec![ClientQuotaEntity::new("user", Some("alice".into()))],
                vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
            ),
            ClientQuotaAlteration::new(
                vec![ClientQuotaEntity::new("user", Some("carol".into()))],
                vec![ClientQuotaOp::remove("producer_byte_rate")],
            ),
        ];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_request(&mut buf, 1, &entries, true).unwrap();
        let mut cur = &buf[..];
        let (got, validate_only) = decode_alter_client_quotas_request(&mut cur, 1).unwrap();
        assert_eq!(got, entries);
        assert!(validate_only);
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 request must be leftover-empty"
        );

        let resp = vec![
            ClientQuotaAlterationResult {
                error_code: 0,
                error_message: None,
                entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
            },
            ClientQuotaAlterationResult {
                error_code: 0,
                error_message: None,
                entity: vec![ClientQuotaEntity::new("user", Some("carol".into()))],
            },
        ];
        buf.clear();
        encode_alter_client_quotas_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_client_quotas_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_client_quotas_not_controller_is_at_byte_five() {
        // Official v1 body: throttle INT32, compact Entries[], then each
        // entry is ErrorCode INT16, ErrorMessage, Entity. Verified from
        // Apache AlterClientQuotasResponse.json and kafka-protocol 0.18.0.
        // Not copied from UpdateFeatures (top-level 41 at bytes 4-5) or
        // AlterUserScramCredentials (41 after compact User at 11-12).
        // ErrorCode is before Entity, not after.
        let resp = vec![ClientQuotaAlterationResult {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        }];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_response(&mut buf, 1, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v1 has no top-level error; bytes 4-5 are compact entries len + high byte, not 41"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_CONTROLLER,
            "v1 41 is the first entry ErrorCode after throttle and compact entries len"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_client_quotas_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v1 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn alter_client_quotas_v0_is_classic() {
        const REQ_V0: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x75, 0x73, 0x65, 0x72,
            0x00, 0x05, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x01, 0x00, 0x12, 0x70,
            0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x5f, 0x72,
            0x61, 0x74, 0x65, 0x40, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_V0_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x29, 0x00, 0x0e, 0x4e, 0x6f,
            0x74, 0x20, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x04, 0x75, 0x73, 0x65, 0x72, 0x00, 0x05, 0x61, 0x6c, 0x69, 0x63,
            0x65,
        ];
        let entries = vec![ClientQuotaAlteration::new(
            vec![ClientQuotaEntity::new("user", Some("alice".into()))],
            vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
        )];
        let resp = vec![ClientQuotaAlterationResult {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entity: vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        }];
        let mut buf = BytesMut::new();
        encode_alter_client_quotas_request(&mut buf, 0, &entries, false).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        let mut cur = &buf[..];
        let (got, validate_only) = decode_alter_client_quotas_request(&mut cur, 0).unwrap();
        assert_eq!(got, entries);
        assert!(!validate_only);
        assert!(
            !cur.has_remaining(),
            "AlterClientQuotas v0 request leftover-empty"
        );
        buf.clear();
        encode_alter_client_quotas_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_41);
        let b8 = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b8, b9]),
            crate::error::NOT_CONTROLLER,
            "v0 first-entry ErrorCode is after throttle and classic array len (bytes 8-9)"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_client_quotas_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(!cur.has_remaining());
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);
    }

    #[test]
    fn alter_client_quotas_v2_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_alter_client_quotas_request(&mut buf, 2, &[], false).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2+ is not spoken, got {err}"
        );
    }

    #[test]
    fn config_new_get_matches_java() {
        let entry = ConfigEntry::new("cleanup.policy", Some("compact".into()));
        let config = Config::new([entry.clone()]);
        assert_eq!(config.entries().len(), 1);
        assert_eq!(config.get("cleanup.policy"), Some(&entry));
        assert_eq!(config.get("retention.ms"), None);
        assert_eq!(entry.source, CONFIG_SOURCE_UNKNOWN);
        assert_eq!(entry.source(), ConfigSource::Unknown);
        assert_eq!(entry.config_type, CONFIG_TYPE_UNKNOWN);
        assert_eq!(entry.config_type(), ConfigType::Unknown);
        assert!(!entry.is_default());
        assert_eq!(entry.name(), "cleanup.policy");
        assert_eq!(entry.value(), Some("compact"));
        assert!(!entry.is_sensitive());
        assert!(!entry.is_read_only());
        let result = DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_TOPIC,
            name: "orders".into(),
            entries: vec![entry.clone()],
        };
        assert_eq!(result.config().get("cleanup.policy"), Some(&entry));
        assert_eq!(result.name(), "orders");
        assert_eq!(result.error_code(), 0);
        assert_eq!(result.entries().len(), 1);
        let mut secret = ConfigEntry::new("ssl.keystore.password", Some("s3cret".into()));
        secret.is_sensitive = true;
        let debug = format!("{secret:?}");
        assert!(
            debug.contains("Redacted"),
            "sensitive ConfigEntry Debug must match Java toString redaction: {debug}"
        );
        assert!(
            !debug.contains("s3cret"),
            "sensitive ConfigEntry Debug must not leak the value: {debug}"
        );
        assert_eq!(secret.value(), Some("s3cret"));
        let created = CreatedTopicConfig {
            name: "ssl.keystore.password".into(),
            value: Some("s3cret".into()),
            read_only: false,
            config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
            is_sensitive: true,
        };
        assert_eq!(created.name(), "ssl.keystore.password");
        assert_eq!(created.value(), Some("s3cret"));
        assert!(!created.is_read_only());
        assert_eq!(created.source(), ConfigSource::DynamicTopic);
        assert!(created.is_sensitive());
        let created_debug = format!("{created:?}");
        assert!(
            created_debug.contains("Redacted"),
            "sensitive CreatedTopicConfig Debug must match Java toString redaction: {created_debug}"
        );
        assert!(
            !created_debug.contains("s3cret"),
            "sensitive CreatedTopicConfig Debug must not leak the value: {created_debug}"
        );
        let topic = TopicResult {
            name: "orders".into(),
            error_code: 0,
            error_message: None,
            topic_id: [0; 16],
            num_partitions: 3,
            replication_factor: 1,
            configs: vec![CreatedTopicConfig {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
            }],
        };
        assert_eq!(
            topic
                .config()
                .get("cleanup.policy")
                .and_then(ConfigEntry::value),
            Some("compact")
        );
        assert_eq!(
            topic
                .config()
                .get("cleanup.policy")
                .map(ConfigEntry::config_type),
            Some(ConfigType::Unknown),
            "CreateTopics Config type is unknown (Java null)"
        );
    }

    #[test]
    fn client_quota_filter_matches_java_factories() {
        let all = ClientQuotaFilter::all();
        assert!(all.components().is_empty());
        assert!(!all.strict());
        let c = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
        assert_eq!(
            c,
            ClientQuotaFilterComponent::new(
                ClientQuotaEntity::USER,
                QUOTA_MATCH_EXACT,
                Some("alice".into())
            )
        );
        assert_eq!(c.entity_type(), ClientQuotaEntity::USER);
        assert_eq!(c.matched(), Some(Some("alice")));
        let contains = ClientQuotaFilter::contains([c.clone()]);
        assert_eq!(contains.components(), std::slice::from_ref(&c));
        assert!(!contains.strict());
        let only = ClientQuotaFilter::contains_only([c.clone()]);
        assert_eq!(only.components(), std::slice::from_ref(&c));
        assert!(only.strict());
        let default = ClientQuotaFilterComponent::of_default_entity(ClientQuotaEntity::CLIENT_ID);
        assert_eq!(default.match_type, QUOTA_MATCH_DEFAULT);
        assert!(default.match_value.is_none());
        assert_eq!(default.matched(), Some(None));
        let any = ClientQuotaFilterComponent::of_entity_type(ClientQuotaEntity::IP);
        assert_eq!(any.match_type, QUOTA_MATCH_ANY);
        assert!(any.match_value.is_none());
        assert_eq!(any.matched(), None);
        assert!(ClientQuotaEntity::is_valid_entity_type(
            ClientQuotaEntity::USER
        ));
        assert!(ClientQuotaEntity::is_valid_entity_type(
            ClientQuotaEntity::CLIENT_ID
        ));
        assert!(ClientQuotaEntity::is_valid_entity_type(
            ClientQuotaEntity::IP
        ));
        assert!(!ClientQuotaEntity::is_valid_entity_type("group"));
        let entity = ClientQuotaEntity::new(ClientQuotaEntity::USER, Some("alice".into()));
        assert_eq!(entity.entity_type(), ClientQuotaEntity::USER);
        assert_eq!(entity.name(), Some("alice"));
        let set = ClientQuotaOp::set("producer_byte_rate", 1024.0);
        assert_eq!(set.key(), "producer_byte_rate");
        assert_eq!(set.value(), Some(1024.0));
        let del = ClientQuotaOp::remove("producer_byte_rate");
        assert_eq!(del.key(), "producer_byte_rate");
        assert!(del.value().is_none());
        let alteration = ClientQuotaAlteration::new(vec![entity.clone()], vec![set.clone()]);
        assert_eq!(alteration.entity(), std::slice::from_ref(&entity));
        assert_eq!(alteration.ops(), std::slice::from_ref(&set));
        let value = ClientQuotaValue::new("producer_byte_rate", 1024.0);
        assert_eq!(value.key(), "producer_byte_rate");
        assert_eq!(value.value(), 1024.0);
        let entry = ClientQuotaEntry::new(vec![entity.clone()], vec![value.clone()]);
        assert_eq!(entry.entity(), std::slice::from_ref(&entity));
        assert_eq!(entry.values(), std::slice::from_ref(&value));
        let result = ClientQuotaAlterationResult {
            error_code: 0,
            error_message: None,
            entity: vec![entity.clone()],
        };
        assert_eq!(result.error_code(), 0);
        assert!(result.error_message().is_none());
        assert_eq!(result.entity(), std::slice::from_ref(&entity));
        const REQ: &[u8] = &[
            0x02, 0x05, 0x75, 0x73, 0x65, 0x72, 0x00, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00,
            0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, 1, std::slice::from_ref(&c), false)
            .unwrap();
        assert_eq!(&buf[..], REQ, "ofEntity must encode MatchType exact");
    }

    #[test]
    fn producer_and_transaction_getters_match_java() {
        let active = ActiveProducer::new(1000, 1, 7, 1_700_000_000_000, 0, -1);
        assert_eq!(active.producer_id(), 1000);
        assert_eq!(active.producer_epoch(), 1);
        assert_eq!(active.last_sequence(), 7);
        assert_eq!(active.last_timestamp(), 1_700_000_000_000);
        assert_eq!(active.coordinator_epoch(), Some(0));
        assert!(active.current_txn_start_offset().is_none());
        let unset = ActiveProducer::new(1, 0, 0, 0, -1, 42);
        assert!(unset.coordinator_epoch().is_none());
        assert_eq!(unset.current_txn_start_offset(), Some(42));
        let part = DescribeProducersPartition::new(0, 0, None, vec![active.clone()]);
        assert_eq!(part.partition_index(), 0);
        assert_eq!(part.error_code(), 0);
        assert!(part.error_message().is_none());
        assert_eq!(part.active_producers(), std::slice::from_ref(&active));
        let topic = DescribeProducersTopic::new("t", vec![part.clone()]);
        assert_eq!(topic.name(), "t");
        assert_eq!(topic.partitions(), std::slice::from_ref(&part));
        let resp = DescribeProducersResponse::new(vec![topic.clone()]);
        assert_eq!(resp.topics(), std::slice::from_ref(&topic));
        let txn_topic = TransactionTopic {
            name: "orders".into(),
            partitions: vec![0, 1],
        };
        assert_eq!(txn_topic.name(), "orders");
        assert_eq!(txn_topic.partitions(), &[0, 1]);
        let described = TransactionState {
            error_code: 0,
            transactional_id: "tx".into(),
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: 60_000,
            transaction_start_time_ms: 1_700_000_000_000,
            producer_id: 1001,
            producer_epoch: 3,
            topics: vec![txn_topic.clone()],
        };
        assert_eq!(described.error_code(), 0);
        assert_eq!(described.transactional_id(), "tx");
        assert_eq!(described.state(), "Ongoing");
        assert_eq!(described.producer_id(), 1001);
        assert_eq!(described.producer_epoch(), 3);
        assert_eq!(described.transaction_timeout_ms(), 60_000);
        assert_eq!(
            described.transaction_start_time_ms(),
            Some(1_700_000_000_000)
        );
        assert_eq!(described.topics(), std::slice::from_ref(&txn_topic));
        let empty_start = TransactionState {
            transaction_start_time_ms: -1,
            ..described.clone()
        };
        assert!(empty_start.transaction_start_time_ms().is_none());
        let listing = TransactionListing {
            transactional_id: "tx".into(),
            producer_id: 1001,
            transaction_state: "Ongoing".into(),
        };
        assert_eq!(listing.transactional_id(), "tx");
        assert_eq!(listing.producer_id(), 1001);
        assert_eq!(listing.state(), "Ongoing");
        let listed = ListTransactionsResponse {
            error_code: 0,
            unknown_state_filters: vec!["Nope".into()],
            transaction_states: vec![listing.clone()],
        };
        assert_eq!(listed.error_code(), 0);
        assert_eq!(listed.unknown_state_filters(), &["Nope".to_string()]);
        assert_eq!(listed.transaction_states(), std::slice::from_ref(&listing));
    }

    #[test]
    fn member_and_log_dir_getters_match_java() {
        let assigned = ConsumerGroupTopicPartitions::new([0; 16], "t", vec![0, 1]);
        assert_eq!(assigned.topic_id(), [0; 16]);
        assert_eq!(assigned.topic_name(), "t");
        assert_eq!(assigned.partitions(), &[0, 1]);
        let assignment = ConsumerGroupAssignment::new(vec![assigned.clone()]);
        assert_eq!(
            assignment.topic_partitions(),
            std::slice::from_ref(&assigned)
        );
        let mut member = ConsumerGroupMember::new("m1", 7, "c", "h");
        member.instance_id = Some("i1".into());
        member.assignment = assignment.clone();
        member.target_assignment = assignment.clone();
        member.member_type = 1;
        assert_eq!(member.member_id(), "m1");
        assert_eq!(member.group_instance_id(), Some("i1"));
        assert_eq!(member.client_id(), "c");
        assert_eq!(member.host(), "h");
        assert_eq!(member.assignment(), &assignment);
        assert_eq!(member.target_assignment(), Some(&assignment));
        assert_eq!(member.member_epoch(), Some(7));
        assert_eq!(member.upgraded(), Some(true));
        member.member_type = 0;
        assert_eq!(member.upgraded(), Some(false));
        member.member_type = -1;
        assert!(member.upgraded().is_none());
        let described = DescribedConsumerGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 2,
            assignment_epoch: 3,
            assignor_name: "uniform".into(),
            members: vec![member.clone()],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        };
        assert_eq!(described.error_code(), 0);
        assert!(described.error_message().is_none());
        assert_eq!(described.group_id(), "g");
        assert_eq!(described.group_state(), "Stable");
        assert_eq!(described.group_epoch(), 2);
        assert_eq!(described.assignment_epoch(), 3);
        assert_eq!(described.assignor_name(), "uniform");
        assert_eq!(described.members(), std::slice::from_ref(&member));
        assert_eq!(
            described.authorized_operations(),
            AUTHORIZED_OPERATIONS_OMITTED
        );
        let mut classic_member = DescribedGroupMember::new("m", "c", "h");
        classic_member.group_instance_id = Some("i1".into());
        classic_member.member_assignment = vec![1, 2, 3];
        assert_eq!(classic_member.member_id(), "m");
        assert_eq!(classic_member.group_instance_id(), Some("i1"));
        assert_eq!(classic_member.client_id(), "c");
        assert_eq!(classic_member.host(), "h");
        assert_eq!(classic_member.assignment(), &[1, 2, 3]);
        assert!(classic_member.target_assignment().is_none());
        assert!(classic_member.member_epoch().is_none());
        assert!(classic_member.upgraded().is_none());
        let mut classic = DescribedGroup::new("g", 0);
        classic.group_state = "Stable".into();
        classic.protocol_type = "consumer".into();
        classic.protocol_data = "range".into();
        classic.members = vec![classic_member.clone()];
        assert_eq!(classic.error_code(), 0);
        assert_eq!(classic.group_id(), "g");
        assert_eq!(classic.group_state(), "Stable");
        assert_eq!(classic.protocol_type(), "consumer");
        assert_eq!(classic.protocol_data(), "range");
        assert_eq!(classic.members(), std::slice::from_ref(&classic_member));
        assert!(!classic.is_simple_consumer_group());
        let simple = DescribedGroup::new("s", 0);
        assert!(simple.is_simple_consumer_group());
        let replica = DescribeLogDirsPartition::new(0, 10, 3, true);
        assert_eq!(replica.partition_index(), 0);
        assert_eq!(replica.size(), 10);
        assert_eq!(replica.offset_lag(), 3);
        assert!(replica.is_future());
        let topic = DescribeLogDirsTopic::new("t", vec![replica.clone()]);
        assert_eq!(topic.name(), "t");
        assert_eq!(topic.partitions(), std::slice::from_ref(&replica));
        let dir = DescribeLogDirsResult::new(
            0,
            "/d",
            vec![topic.clone()],
            UNKNOWN_VOLUME_BYTES,
            UNKNOWN_VOLUME_BYTES,
        );
        assert_eq!(dir.error_code(), 0);
        assert_eq!(dir.log_dir(), "/d");
        assert_eq!(dir.topics(), std::slice::from_ref(&topic));
        assert!(dir.total_bytes().is_none());
        assert!(dir.usable_bytes().is_none());
        let sized = DescribeLogDirsResult::new(0, "/d", Vec::new(), 100, 40);
        assert_eq!(sized.total_bytes(), Some(100));
        assert_eq!(sized.usable_bytes(), Some(40));
        let resp = DescribeLogDirsResponse::new(0, vec![dir.clone()]);
        assert_eq!(resp.error_code(), 0);
        assert_eq!(resp.results(), std::slice::from_ref(&dir));
    }

    #[test]
    fn share_member_and_group_getters_match_java() {
        let assigned = ShareGroupTopicPartitions::new([0; 16], "t", vec![0, 1]);
        assert_eq!(assigned.topic_id(), [0; 16]);
        assert_eq!(assigned.topic_name(), "t");
        assert_eq!(assigned.partitions(), &[0, 1]);
        let assignment = ShareGroupAssignment::new(vec![assigned.clone()]);
        assert_eq!(
            assignment.topic_partitions(),
            std::slice::from_ref(&assigned)
        );
        let mut member = ShareGroupMember::new("m1", 3, "c", "h");
        member.assignment = assignment.clone();
        assert_eq!(member.member_id(), "m1");
        assert_eq!(member.client_id(), "c");
        assert_eq!(member.host(), "h");
        assert_eq!(member.assignment(), &assignment);
        let described = DescribedShareGroup {
            error_code: 0,
            error_message: None,
            group_id: "sg".into(),
            group_state: "Stable".into(),
            group_epoch: 2,
            assignment_epoch: 3,
            assignor_name: "uniform".into(),
            members: vec![member.clone()],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        };
        assert_eq!(described.error_code(), 0);
        assert!(described.error_message().is_none());
        assert_eq!(described.group_id(), "sg");
        assert_eq!(described.group_state(), "Stable");
        assert_eq!(described.group_epoch(), 2);
        assert_eq!(described.assignment_epoch(), 3);
        assert_eq!(described.assignor_name(), "uniform");
        assert_eq!(described.members(), std::slice::from_ref(&member));
        assert_eq!(
            described.authorized_operations(),
            AUTHORIZED_OPERATIONS_OMITTED
        );
    }

    #[test]
    fn share_offset_getters_match_wire() {
        let req_topic = DescribeShareGroupOffsetsTopic::new("t", vec![0, 1]);
        assert_eq!(req_topic.topic_name(), "t");
        assert_eq!(req_topic.partitions(), &[0, 1]);
        let empty = DescribeShareGroupOffsetsGroup::new("g");
        assert_eq!(empty.group_id(), "g");
        assert_eq!(empty.topics(), Some(&[][..]));
        let all = DescribeShareGroupOffsetsGroup::all("g2");
        assert_eq!(all.group_id(), "g2");
        assert!(all.topics().is_none());
        let part = DescribedShareGroupOffsetsPartition {
            partition_index: 0,
            start_offset: 7,
            leader_epoch: 3,
            error_code: 0,
            error_message: None,
        };
        assert_eq!(part.partition_index(), 0);
        assert_eq!(part.start_offset(), 7);
        assert_eq!(part.leader_epoch(), 3);
        assert_eq!(part.error_code(), 0);
        assert!(part.error_message().is_none());
        let topic = DescribedShareGroupOffsetsTopic {
            topic_name: "t".into(),
            topic_id: [1; 16],
            partitions: vec![part.clone()],
        };
        assert_eq!(topic.topic_name(), "t");
        assert_eq!(topic.topic_id(), [1; 16]);
        assert_eq!(topic.partitions(), std::slice::from_ref(&part));
        let described = DescribedShareGroupOffsets {
            group_id: "sg".into(),
            topics: vec![topic.clone()],
            error_code: 0,
            error_message: None,
        };
        assert_eq!(described.group_id(), "sg");
        assert_eq!(described.topics(), std::slice::from_ref(&topic));
        assert_eq!(described.error_code(), 0);
        let alter_part = AlterShareGroupOffsetsPartition::new(0, 9);
        assert_eq!(alter_part.partition_index(), 0);
        assert_eq!(alter_part.start_offset(), 9);
        let alter_topic = AlterShareGroupOffsetsTopic::new("t", vec![alter_part.clone()]);
        assert_eq!(alter_topic.topic_name(), "t");
        assert_eq!(alter_topic.partitions(), std::slice::from_ref(&alter_part));
        let altered_part = AlteredShareGroupOffsetsPartition {
            partition_index: 0,
            error_code: 0,
            error_message: None,
        };
        assert_eq!(altered_part.partition_index(), 0);
        assert_eq!(altered_part.error_code(), 0);
        let altered_topic = AlteredShareGroupOffsetsTopic {
            topic_name: "t".into(),
            topic_id: [2; 16],
            partitions: vec![altered_part.clone()],
        };
        assert_eq!(altered_topic.topic_name(), "t");
        assert_eq!(altered_topic.topic_id(), [2; 16]);
        assert_eq!(
            altered_topic.partitions(),
            std::slice::from_ref(&altered_part)
        );
        let altered = AlteredShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![altered_topic.clone()],
        };
        assert_eq!(altered.error_code(), 0);
        assert!(altered.error_message().is_none());
        assert_eq!(altered.topics(), std::slice::from_ref(&altered_topic));
        let del_req = DeleteShareGroupOffsetsTopic::new("t");
        assert_eq!(del_req.topic_name(), "t");
        let del_topic = DeletedShareGroupOffsetsTopic {
            topic_name: "t".into(),
            topic_id: [3; 16],
            error_code: 0,
            error_message: None,
        };
        assert_eq!(del_topic.topic_name(), "t");
        assert_eq!(del_topic.topic_id(), [3; 16]);
        assert_eq!(del_topic.error_code(), 0);
        let deleted = DeletedShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![del_topic.clone()],
        };
        assert_eq!(deleted.error_code(), 0);
        assert_eq!(deleted.topics(), std::slice::from_ref(&del_topic));
    }

    #[test]
    fn describe_client_quotas_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 48
        // validVersions 0-1, flexibleVersions 1+, listeners broker only.
        // This crate speaks 0–1; this fixture is v1. Not copied from AlterClientQuotas
        // (no top-level error; first-entry 41 at bytes 5-6).
        const REQ: &[u8] = &[
            0x02, 0x05, 0x75, 0x73, 0x65, 0x72, 0x00, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00,
            0x00, 0x00,
        ];
        const RESP_ERR: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x00, 0x00,
        ];
        let components = vec![ClientQuotaFilterComponent::new(
            "user",
            QUOTA_MATCH_EXACT,
            Some("alice".into()),
        )];
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, 1, &components, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeClientQuotasResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entries: None,
        };
        buf.clear();
        encode_describe_client_quotas_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_ERR);
    }

    #[test]
    fn describe_client_quotas_v1_roundtrip_is_leftover_empty() {
        let components = vec![
            ClientQuotaFilterComponent::new("user", QUOTA_MATCH_EXACT, Some("alice".into())),
            ClientQuotaFilterComponent::new("client-id", QUOTA_MATCH_ANY, None),
        ];
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, 1, &components, true).unwrap();
        let mut cur = &buf[..];
        let (got, strict) = decode_describe_client_quotas_request(&mut cur, 1).unwrap();
        assert_eq!(got, components);
        assert!(strict);
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 request must be leftover-empty"
        );

        let resp = DescribeClientQuotasResponse {
            error_code: 0,
            error_message: None,
            entries: Some(vec![ClientQuotaEntry::new(
                vec![ClientQuotaEntity::new("user", Some("alice".into()))],
                vec![ClientQuotaValue::new("producer_byte_rate", 1024.0)],
            )]),
        };
        buf.clear();
        encode_describe_client_quotas_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_client_quotas_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_client_quotas_error_code_is_at_bytes_4_5() {
        // Official v1 body: throttle INT32, then top-level ErrorCode
        // INT16, ErrorMessage, nullable Entries. Measured independently
        // from Apache DescribeClientQuotasResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features = ["broker"]`).
        // Do not assume bytes 4-5 from UnregisterBroker /
        // AllocateProducerIds (same offset, different fields after) or
        // AlterClientQuotas (first-entry code at bytes 5-6).
        let resp = DescribeClientQuotasResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entries: None,
        };
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_response(&mut buf, 1, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v1 throttle then top-level error must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_CONTROLLER,
            "v1 ErrorCode is not a first-result field at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_client_quotas_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn describe_client_quotas_v0_is_classic() {
        const REQ_V0: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x75, 0x73, 0x65, 0x72, 0x00, 0x00, 0x05, 0x61,
            0x6c, 0x69, 0x63, 0x65, 0x00,
        ];
        const RESP_V0_ERR: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x0e, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f,
            0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0xff, 0xff, 0xff, 0xff,
        ];
        let components = vec![ClientQuotaFilterComponent::new(
            "user",
            QUOTA_MATCH_EXACT,
            Some("alice".into()),
        )];
        let resp = DescribeClientQuotasResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            entries: None,
        };
        let mut buf = BytesMut::new();
        encode_describe_client_quotas_request(&mut buf, 0, &components, false).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        let mut cur = &buf[..];
        let (got, strict) = decode_describe_client_quotas_request(&mut cur, 0).unwrap();
        assert_eq!(got, components);
        assert!(!strict);
        assert!(
            !cur.has_remaining(),
            "DescribeClientQuotas v0 request leftover-empty"
        );
        buf.clear();
        encode_describe_client_quotas_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_ERR);
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 top-level ErrorCode is still the INT16 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_client_quotas_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(!cur.has_remaining());
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);
    }

    #[test]
    fn describe_client_quotas_v2_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_describe_client_quotas_request(&mut buf, 2, &[], false).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2+ is not spoken, got {err}"
        );
    }

    #[test]
    fn allocate_producer_ids_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 67
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AlterClientQuotas / UpdateFeatures / OffsetDelete.
        const REQ: &[u8] = &[
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00,
        ];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_request(&mut buf, 7, 42).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AllocateProducerIdsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            producer_id_start: 0,
            producer_id_len: 0,
        };
        buf.clear();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn allocate_producer_ids_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_request(&mut buf, 7, 42).unwrap();
        let mut cur = &buf[..];
        let (broker_id, broker_epoch) = decode_allocate_producer_ids_request(&mut cur).unwrap();
        assert_eq!(broker_id, 7);
        assert_eq!(broker_epoch, 42);
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 request must be leftover-empty"
        );

        let resp = AllocateProducerIdsResponse {
            error_code: 0,
            producer_id_start: 1000,
            producer_id_len: 1000,
        };
        buf.clear();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_allocate_producer_ids_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 response must be leftover-empty"
        );
    }

    #[test]
    fn allocate_producer_ids_not_controller_is_at_byte_four() {
        // Official v0 body: throttle INT32, then top-level ErrorCode INT16,
        // ProducerIdStart INT64, ProducerIdLen INT32, tagged. Measured from
        // Apache AllocateProducerIdsResponse.json and an independent
        // kafka-protocol 0.18.0 broker encode. Not copied from
        // UpdateFeatures (same byte offset, different fields after 41),
        // AlterClientQuotas (41 at bytes 5-6), AlterUserScramCredentials
        // (41 after compact User), or OffsetDelete (error before throttle).
        let resp = AllocateProducerIdsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            producer_id_start: 0,
            producer_id_len: 0,
        };
        let mut buf = BytesMut::new();
        encode_allocate_producer_ids_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_allocate_producer_ids_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AllocateProducerIds v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_transactions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 65
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AllocateProducerIds / AlterClientQuotas /
        // OffsetDelete.
        const REQ: &[u8] = &[0x02, 0x05, 0x74, 0x78, 0x2d, 0x31, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x05, 0x74, 0x78, 0x2d, 0x31, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];
        let ids = vec!["tx-1".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_transactions_request(&mut buf, &ids).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![TransactionState {
            error_code: crate::error::NOT_COORDINATOR,
            transactional_id: "tx-1".into(),
            transaction_state: String::new(),
            transaction_timeout_ms: 0,
            transaction_start_time_ms: 0,
            producer_id: 0,
            producer_epoch: 0,
            topics: Vec::new(),
        }];
        buf.clear();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_transactions_v0_roundtrip_is_leftover_empty() {
        let ids = vec!["tx-1".to_string(), "tx-2".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_transactions_request(&mut buf, &ids).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_transactions_request(&mut cur).unwrap(), ids);
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 request must be leftover-empty"
        );

        let resp = vec![TransactionState {
            error_code: 0,
            transactional_id: "tx-1".into(),
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: 60_000,
            transaction_start_time_ms: 1_700_000_000_000,
            producer_id: 1001,
            producer_epoch: 3,
            topics: vec![TransactionTopic {
                name: "orders".into(),
                partitions: vec![0, 1],
            }],
        }];
        buf.clear();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_transactions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_transactions_not_coordinator_is_at_byte_five() {
        // Official v0 body: throttle INT32, compact TransactionStates[],
        // then each state starts with ErrorCode INT16. Measured from
        // Apache DescribeTransactionsResponse.json and an independent
        // kafka-protocol 0.18.0 broker encode. Not copied from
        // AllocateProducerIds (top-level 41 at bytes 4-5),
        // AlterClientQuotas (41 at bytes 5-6, different fields after),
        // or OffsetDelete (error before throttle).
        let resp = vec![TransactionState {
            error_code: crate::error::NOT_COORDINATOR,
            transactional_id: "tx-1".into(),
            transaction_state: String::new(),
            transaction_timeout_ms: 0,
            transaction_start_time_ms: 0,
            producer_id: 0,
            producer_epoch: 0,
            topics: Vec::new(),
        }];
        let mut buf = BytesMut::new();
        encode_describe_transactions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 has no top-level error; bytes 4-5 are compact states len + high byte, not 16"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 16 is the first result ErrorCode after throttle and compact states len"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_transactions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTransactions v0 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn list_transactions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache Kafka 4.0 JSON
        // api 66 validVersions 0-1, flexibleVersions 0+. kafka-protocol
        // 0.18.0 advertised 0-2 (TransactionalIdPattern); this crate
        // speaks 0–1. v0 has no DurationFilter.
        // Not copied from DescribeTransactions (no top-level error;
        // 16 at bytes 5-6) or AllocateProducerIds (different fields
        // after the top-level INT16).
        const REQ: &[u8] = &[
            0x02, 0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x03, 0xe9, 0x00,
        ];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x01, 0x01, 0x00];
        let states = vec!["Ongoing".to_string()];
        let pids = vec![1001_i64];
        let mut buf = BytesMut::new();
        encode_list_transactions_request(&mut buf, 0, &states, &pids, -1).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListTransactionsResponse {
            error_code: crate::error::NOT_COORDINATOR,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
        };
        buf.clear();
        encode_list_transactions_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn list_transactions_v0_roundtrip_is_leftover_empty() {
        let states = vec!["Ongoing".to_string(), "PrepareCommit".to_string()];
        let pids = vec![1001_i64, 1002];
        let mut buf = BytesMut::new();
        encode_list_transactions_request(&mut buf, 0, &states, &pids, 5000).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_request(&mut cur, 0).unwrap(),
            (states, pids, -1)
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 request must be leftover-empty"
        );

        let resp = ListTransactionsResponse {
            error_code: 0,
            unknown_state_filters: vec!["UnknownState".into()],
            transaction_states: vec![TransactionListing {
                transactional_id: "tx-1".into(),
                producer_id: 1001,
                transaction_state: "Ongoing".into(),
            }],
        };
        buf.clear();
        encode_list_transactions_response(&mut buf, 0, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn list_transactions_v1_duration_filter_is_i64_before_tags() {
        // v0 compact body plus DurationFilter INT64 (KIP-994) before
        // tagged fields. duration -1 is Java's no-filter default.
        const V0: &[u8] = &[
            0x02, 0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x03, 0xe9, 0x00,
        ];
        const V1: &[u8] = &[
            0x02, 0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x03, 0xe9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        let states = vec!["Ongoing".to_string()];
        let pids = vec![1001_i64];
        let mut v0 = BytesMut::new();
        encode_list_transactions_request(&mut v0, 0, &states, &pids, -1).unwrap();
        assert_eq!(&v0[..], V0);
        let mut v1 = BytesMut::new();
        encode_list_transactions_request(&mut v1, 1, &states, &pids, -1).unwrap();
        assert_eq!(&v1[..], V1);
        assert_ne!(
            &v0[..],
            &v1[..],
            "ListTransactions v1 must send DurationFilter"
        );
        let mut cur = &v1[..];
        assert_eq!(
            decode_list_transactions_request(&mut cur, 1).unwrap(),
            (states.clone(), pids.clone(), -1)
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v1 request must be leftover-empty"
        );
        assert!(
            encode_list_transactions_request(&mut BytesMut::new(), 2, &states, &pids, -1).is_err(),
            "ListTransactions v2 TransactionalIdPattern is not spoken"
        );

        let resp = ListTransactionsResponse {
            error_code: crate::error::NOT_COORDINATOR,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
        };
        let mut r0 = BytesMut::new();
        encode_list_transactions_response(&mut r0, 0, &resp).unwrap();
        let mut r1 = BytesMut::new();
        encode_list_transactions_response(&mut r1, 1, &resp).unwrap();
        assert_eq!(&r0[..], &r1[..], "v1 response layout matches v0");
        let mut cur = &r1[..];
        assert_eq!(
            decode_list_transactions_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v1 response must be leftover-empty"
        );
        assert!(
            encode_list_transactions_response(&mut BytesMut::new(), 2, &resp).is_err(),
            "ListTransactions v2 is not spoken"
        );
    }

    #[test]
    fn list_transactions_v1_roundtrip_is_leftover_empty() {
        let states = vec!["Ongoing".to_string(), "PrepareCommit".to_string()];
        let pids = vec![1001_i64, 1002];
        let mut buf = BytesMut::new();
        encode_list_transactions_request(&mut buf, 1, &states, &pids, 5000).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_request(&mut cur, 1).unwrap(),
            (states, pids, 5000)
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v1 request must be leftover-empty"
        );

        let resp = ListTransactionsResponse {
            error_code: 0,
            unknown_state_filters: vec!["UnknownState".into()],
            transaction_states: vec![TransactionListing {
                transactional_id: "tx-1".into(),
                producer_id: 1001,
                transaction_state: "Ongoing".into(),
            }],
        };
        buf.clear();
        encode_list_transactions_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v1 response must be leftover-empty"
        );
    }

    #[test]
    fn list_transactions_not_coordinator_is_at_bytes_4_5() {
        // Official v0–v1 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured from Apache ListTransactionsResponse.json
        // and an independent kafka-protocol 0.18.0 broker encode.
        // Not copied from DescribeTransactions (16 at bytes 5-6, first
        // result after compact TransactionStates length).
        let resp = ListTransactionsResponse {
            error_code: crate::error::NOT_COORDINATOR,
            unknown_state_filters: Vec::new(),
            transaction_states: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_list_transactions_response(&mut buf, 0, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 throttle then top-level error must be 16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 16 is not a first-result ErrorCode at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v0 NOT_COORDINATOR must be leftover-empty"
        );
        buf.clear();
        encode_list_transactions_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_transactions_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListTransactions v1 NOT_COORDINATOR must be leftover-empty"
        );
    }

    #[test]
    fn describe_user_scram_credentials_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 50
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AlterUserScramCredentials (no top-level error;
        // 41 after compact User at 11-12) or ListTransactions (16 at
        // bytes 4-5, different fields after the INT16).
        const REQ: &[u8] = &[0x02, 0x06, 0x61, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x01, 0x00,
        ];
        let users = vec!["alice".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_request(&mut buf, Some(&users)).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeUserScramCredentialsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        buf.clear();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn describe_user_scram_credentials_v0_roundtrip_is_leftover_empty() {
        let users = vec!["alice".to_string(), "bob".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_request(&mut buf, Some(&users)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_request(&mut cur).unwrap(),
            Some(users)
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 request must be leftover-empty"
        );

        buf.clear();
        encode_describe_user_scram_credentials_request(&mut buf, None).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_request(&mut cur).unwrap(),
            None
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 null Users request must be leftover-empty"
        );

        let resp = DescribeUserScramCredentialsResponse {
            error_code: 0,
            error_message: None,
            results: vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
                credential_infos: vec![ScramCredentialInfo {
                    mechanism: SCRAM_SHA_256,
                    iterations: 4096,
                }],
            }],
        };
        buf.clear();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_user_scram_credentials_not_controller_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured from Apache DescribeUserScramCredentialsResponse.json
        // and an independent kafka-protocol 0.18.0 broker encode.
        // Not copied from AlterUserScramCredentials (41 after compact
        // User at 11-12) or ListTransactions (same byte offset, different
        // fields after the INT16).
        let resp = DescribeUserScramCredentialsResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
            results: Vec::new(),
        };
        let mut buf = BytesMut::new();
        encode_describe_user_scram_credentials_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let b11 = buf.get(11).copied().unwrap();
        let b12 = buf.get(12).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b11, b12]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is not a first-result ErrorCode after compact User"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_user_scram_credentials_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeUserScramCredentials v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn unregister_broker_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes the
        // request; broker encodes the response). Apache JSON api 64
        // validVersions 0, flexibleVersions 0+. This crate targets v0.
        // Not copied from AllocateProducerIds (BrokerId then BrokerEpoch)
        // or DescribeUserScramCredentials (same 41 offset, Results after
        // ErrorMessage).
        const REQ: &[u8] = &[0x00, 0x00, 0x00, 0x07, 0x00];
        const RESP_41: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x0f, 0x4e, 0x6f, 0x74, 0x20, 0x63, 0x6f, 0x6e,
            0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_unregister_broker_request(&mut buf, 7).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = UnregisterBrokerResponse::new(
            crate::error::NOT_CONTROLLER,
            Some("Not controller".into()),
        );
        assert_eq!(resp.error_code(), crate::error::NOT_CONTROLLER);
        assert_eq!(resp.error_message(), Some("Not controller"));
        buf.clear();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn unregister_broker_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_unregister_broker_request(&mut buf, 7).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_request(&mut cur).unwrap(), 7);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 request must be leftover-empty"
        );

        let resp = UnregisterBrokerResponse {
            error_code: 0,
            error_message: None,
        };
        buf.clear();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 response must be leftover-empty"
        );
    }

    #[test]
    fn unregister_broker_not_controller_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16. Measured independently from Apache
        // UnregisterBrokerResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`). Not copied from
        // AlterUserScramCredentials (41 after compact User at 11-12),
        // DescribeTransactions (first-result code at bytes 5-6), or
        // DescribeUserScramCredentials (same offset, Results after the
        // INT16 + ErrorMessage).
        let resp = UnregisterBrokerResponse {
            error_code: crate::error::NOT_CONTROLLER,
            error_message: Some("Not controller".into()),
        };
        let mut buf = BytesMut::new();
        encode_unregister_broker_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 throttle then top-level error must be 41 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_CONTROLLER,
            "v0 41 is not a first-result ErrorCode at bytes 5-6"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_unregister_broker_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "UnregisterBroker v0 NOT_CONTROLLER must be leftover-empty"
        );
    }

    #[test]
    fn describe_producers_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 61
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0. Not copied from DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5) or DeleteRecords.
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_6: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
            0x00, 0x01, 0x00, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_describe_producers_request(&mut buf, "t", &[0]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                crate::error::NOT_LEADER_OR_FOLLOWER,
                None,
                vec![],
            )],
        )]);
        buf.clear();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_6);
    }

    #[test]
    fn describe_producers_v0_topics_of_two_matches_independent_encode() {
        const REQ: &[u8] = &[
            0x03, 0x02, 0x61, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x62, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ];
        let topics = [
            DescribeProducersTopicRequest {
                name: "a".into(),
                partition_indexes: vec![0],
            },
            DescribeProducersTopicRequest {
                name: "b".into(),
                partition_indexes: vec![0],
            },
        ];
        let mut buf = BytesMut::new();
        encode_describe_producers_topics_request(&mut buf, &topics).unwrap();
        assert_eq!(&buf[..], REQ);
        let mut cur = &buf[..];
        let got = decode_describe_producers_topics_request(&mut cur).unwrap();
        assert_eq!(got, topics);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 Topics of 2 must be leftover-empty"
        );
    }

    #[test]
    fn describe_producers_v0_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_describe_producers_request(&mut buf, "t", &[0]).unwrap();
        let mut cur = &buf[..];
        let (topic, parts) = decode_describe_producers_request(&mut cur).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(parts, vec![0]);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 request must be leftover-empty"
        );

        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                0,
                None,
                vec![ActiveProducer::new(1000, 1, 7, 1_700_000_000_000, 0, -1)],
            )],
        )]);
        buf.clear();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_producers_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_producers_first_partition_error_code_is_at_bytes_12_13() {
        // Official v0 body: throttle INT32, compact Topics of {Name,
        // compact Partitions of {PartitionIndex, ErrorCode, ...}}.
        // Measured independently from Apache DescribeProducersResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture topic "t" partition 0.
        // Do not assume bytes 4-5 from DescribeClientQuotas /
        // UnregisterBroker / ListTransactions (top-level ErrorCode).
        let resp = DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
            "t",
            vec![DescribeProducersPartition::new(
                0,
                crate::error::NOT_LEADER_OR_FOLLOWER,
                None,
                vec![],
            )],
        )]);
        let mut buf = BytesMut::new();
        encode_describe_producers_response(&mut buf, &resp).unwrap();
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 first-partition ErrorCode must be the INT16 at bytes 12-13"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_describe_producers_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeProducers v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_describe_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 69
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate speaks 0–1; this fixture is v1. Not copied from DescribeClientQuotas
        // (top-level ErrorCode at bytes 4-5) or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_request(&mut buf, 1, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedConsumerGroup::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_consumer_group_describe_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn consumer_group_describe_v1_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_request(&mut buf, 1, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_consumer_group_describe_request(&mut cur, 1).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 request must be leftover-empty"
        );

        let mut member = ConsumerGroupMember::new("m1", 1, "c", "h");
        member.subscribed_topic_names = vec!["t".into()];
        member.assignment = ConsumerGroupAssignment::new(vec![ConsumerGroupTopicPartitions::new(
            [0; 16],
            "t",
            vec![0],
        )]);
        member.target_assignment = member.assignment.clone();
        member.member_type = 1;
        let resp = vec![DescribedConsumerGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignor_name: "uniform".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_consumer_group_describe_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_describe_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 response must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_describe_first_group_error_code_is_at_bytes_5_6() {
        // Official v1 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // ConsumerGroupDescribeResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g". Do not assume bytes 4-5 from
        // DescribeClientQuotas or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedConsumerGroup::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_response(&mut buf, 1, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v1 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_describe_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_describe_v0_matches_v1_request_and_empty_member_response() {
        // Official JSON: request v1 is the same as v0. Empty-member
        // responses are also the same: MemberType lives on each member.
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let resp = vec![DescribedConsumerGroup::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_consumer_group_describe_request(&mut buf, 0, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ, "v0 request matches v1");
        buf.clear();
        encode_consumer_group_describe_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16, "v0 empty-member response matches v1");
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);
    }

    #[test]
    fn consumer_group_describe_v0_omits_member_type() {
        let mut member = ConsumerGroupMember::new("m1", 1, "c", "h");
        member.member_type = 1;
        let resp = vec![DescribedConsumerGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignor_name: "uniform".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        let mut v0 = BytesMut::new();
        encode_consumer_group_describe_response(&mut v0, 0, &resp).unwrap();
        let mut v1 = BytesMut::new();
        encode_consumer_group_describe_response(&mut v1, 1, &resp).unwrap();
        assert_eq!(
            v1.len(),
            v0.len() + 1,
            "v1 adds MemberType INT8 after TargetAssignment"
        );
        let mut cur = &v0[..];
        let got = decode_consumer_group_describe_response(&mut cur, 0).unwrap();
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupDescribe v0 response must be leftover-empty"
        );
        assert_eq!(
            got[0].members[0].member_type, -1,
            "v0 has no MemberType; decode fills -1"
        );
        let mut cur = &v1[..];
        let got = decode_consumer_group_describe_response(&mut cur, 1).unwrap();
        assert_eq!(got[0].members[0].member_type, 1);
        assert!(!cur.has_remaining());
    }

    #[test]
    fn consumer_group_describe_v2_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_consumer_group_describe_request(&mut buf, 2, &[], false).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2+ is not spoken, got {err}"
        );
    }

    #[test]
    fn describe_groups_v6_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 15
        // validVersions 0-6, flexibleVersions 5+, listeners broker only.
        // This crate speaks 0–6; this fixture is v6. Not copied from
        // DescribeClientQuotas (top-level ErrorCode at bytes 4-5),
        // ConsumerGroupDescribe (first-group ErrorCode at bytes 5-6),
        // or DescribeProducers (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x01, 0x01, 0x01,
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, 6, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedGroup::new("g", crate::error::NOT_COORDINATOR)];
        buf.clear();
        encode_describe_groups_response(&mut buf, 6, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_groups_v2_request_omits_include_authorized_operations() {
        // Official JSON: IncludeAuthorizedOperations is v3+. v0–v2 are
        // the same request: classic Groups only.
        const REQ_V2: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67];
        const REQ_V3: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67, 0x01];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, 2, &ids, true).unwrap();
        assert_eq!(&buf[..], REQ_V2);
        let mut cur = &buf[..];
        let (got, include) = decode_describe_groups_request(&mut cur, 2).unwrap();
        assert_eq!(got, ids);
        assert!(
            !include,
            "v2 has no IncludeAuthorizedOperations; decode fills false"
        );
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v2 request leftover-empty"
        );
        buf.clear();
        encode_describe_groups_request(&mut buf, 3, &ids, true).unwrap();
        assert_eq!(&buf[..], REQ_V3);
        assert_ne!(REQ_V2, REQ_V3, "v3 must send IncludeAuthorizedOperations");
    }

    #[test]
    fn describe_groups_v0_and_v4_and_v5_fixtures() {
        const REQ_V0: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67];
        const RESP_V0_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x01, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_V4_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x01, 0x67, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00,
        ];
        const RESP_V5_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x02, 0x67, 0x01, 0x01, 0x01, 0x01, 0x80,
            0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_V6_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x01, 0x01, 0x01,
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let resp = vec![DescribedGroup::new("g", crate::error::NOT_COORDINATOR)];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, 0, &ids, true).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        buf.clear();
        encode_describe_groups_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_16);
        buf.clear();
        encode_describe_groups_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V4_16);
        buf.clear();
        encode_describe_groups_response(&mut buf, 5, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V5_16);
        assert_ne!(
            RESP_V5_16, RESP_V6_16,
            "v5 must not send ErrorMessage (v6+)"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 6, 0, 6), Some(6));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 5, 0, 6), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 6), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(7, 7, 0, 6), None);
    }

    #[test]
    fn describe_groups_v4_sends_group_instance_id_v3_does_not() {
        let mut member = DescribedGroupMember::new("m", "c", "h");
        member.group_instance_id = Some("i1".into());
        let resp = vec![DescribedGroup {
            error_code: 0,
            error_message: Some("x".into()),
            group_id: "g".into(),
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: vec![member.clone()],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        let mut buf = BytesMut::new();
        encode_describe_groups_response(&mut buf, 3, &resp).unwrap();
        let mut cur = &buf[..];
        let got = decode_describe_groups_response(&mut cur, 3).unwrap();
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v3 response leftover-empty"
        );
        assert!(
            got[0].members[0].group_instance_id.is_none(),
            "v3 must not send GroupInstanceId"
        );
        assert!(
            got[0].error_message.is_none(),
            "v3 must not send ErrorMessage"
        );
        buf.clear();
        encode_describe_groups_response(&mut buf, 4, &resp).unwrap();
        let mut cur = &buf[..];
        let got = decode_describe_groups_response(&mut cur, 4).unwrap();
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v4 response leftover-empty"
        );
        assert_eq!(got[0].members[0].group_instance_id.as_deref(), Some("i1"));
        assert!(
            got[0].error_message.is_none(),
            "v4 must not send ErrorMessage"
        );
    }

    #[test]
    fn describe_groups_v6_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_describe_groups_request(&mut buf, 6, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_describe_groups_request(&mut cur, 6).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 request must be leftover-empty"
        );

        let mut member = DescribedGroupMember::new("m1", "c", "h");
        member.group_instance_id = Some("i1".into());
        member.member_metadata = vec![0x01];
        member.member_assignment = vec![0x02];
        let resp = vec![DescribedGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            protocol_type: "consumer".into(),
            protocol_data: "range".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_describe_groups_response(&mut buf, 6, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_groups_response(&mut cur, 6).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_groups_v7_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_describe_groups_request(&mut buf, 7, &[], false).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v7+ is not spoken, got {err}"
        );
    }

    #[test]
    fn describe_groups_first_group_error_code_is_at_bytes_5_6() {
        // Official v6 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // DescribeGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from
        // DescribeClientQuotas, bytes 5-6 from ConsumerGroupDescribe,
        // or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedGroup::new("g", crate::error::NOT_COORDINATOR)];
        let mut buf = BytesMut::new();
        encode_describe_groups_response(&mut buf, 6, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v6 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v6 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v6 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_describe_groups_response(&mut cur, 6).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DescribeGroups v6 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn list_groups_v5_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 16
        // validVersions 0-5, flexibleVersions 3+, listeners broker only.
        // This crate speaks 0–5; this fixture is v5. Not copied from
        // DescribeGroups (first-group ErrorCode at bytes 5-6),
        // DescribeClientQuotas (top-level ErrorCode at bytes 4-5,
        // different fields after), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[
            0x02, 0x07, 0x53, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x02, 0x08, 0x63, 0x6c, 0x61, 0x73,
            0x73, 0x69, 0x63, 0x00,
        ];
        const RESP_15: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x02, 0x02, 0x67, 0x01, 0x01, 0x01, 0x00, 0x00,
        ];
        let states = vec!["Stable".to_string()];
        let types = vec!["classic".to_string()];
        let mut buf = BytesMut::new();
        encode_list_groups_request(&mut buf, 5, &states, &types).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListGroupsResponse {
            error_code: crate::error::COORDINATOR_NOT_AVAILABLE,
            groups: vec![ListedGroup::new("g")],
        };
        buf.clear();
        encode_list_groups_response(&mut buf, 5, &resp).unwrap();
        assert_eq!(&buf[..], RESP_15);
    }

    #[test]
    fn list_groups_v0_v3_v4_omit_later_fields() {
        const REQ_V0: &[u8] = &[];
        const REQ_V3: &[u8] = &[0x00];
        const REQ_V4: &[u8] = &[0x02, 0x07, 0x53, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x00];
        const RESP_V0_15: &[u8] = &[
            0x00, 0x0f, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67, 0x00, 0x00,
        ];
        const RESP_V3_15: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x02, 0x02, 0x67, 0x01, 0x00, 0x00,
        ];
        const RESP_V4_15: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x02, 0x02, 0x67, 0x01, 0x01, 0x00, 0x00,
        ];
        const RESP_V5_15: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x02, 0x02, 0x67, 0x01, 0x01, 0x01, 0x00, 0x00,
        ];
        let states = vec!["Stable".to_string()];
        let types = vec!["classic".to_string()];
        let resp = ListGroupsResponse {
            error_code: crate::error::COORDINATOR_NOT_AVAILABLE,
            groups: vec![ListedGroup::new("g")],
        };
        let mut buf = BytesMut::new();
        encode_list_groups_request(&mut buf, 0, &states, &types).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        let mut cur = &buf[..];
        let (got_s, got_t) = decode_list_groups_request(&mut cur, 0).unwrap();
        assert!(got_s.is_empty() && got_t.is_empty());
        assert!(!cur.has_remaining(), "ListGroups v0 request leftover-empty");
        buf.clear();
        encode_list_groups_request(&mut buf, 3, &states, &types).unwrap();
        assert_eq!(&buf[..], REQ_V3);
        buf.clear();
        encode_list_groups_request(&mut buf, 4, &states, &types).unwrap();
        assert_eq!(&buf[..], REQ_V4);
        let mut cur = &buf[..];
        let (got_s, got_t) = decode_list_groups_request(&mut cur, 4).unwrap();
        assert_eq!(got_s, states);
        assert!(got_t.is_empty(), "v4 must not send TypesFilter");
        assert!(!cur.has_remaining(), "ListGroups v4 request leftover-empty");
        buf.clear();
        encode_list_groups_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_15);
        buf.clear();
        encode_list_groups_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V3_15);
        buf.clear();
        encode_list_groups_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V4_15);
        assert_ne!(RESP_V4_15, RESP_V5_15, "v4 must not send GroupType");
        assert_eq!(crate::protocol::api_keys::pick_version(0, 5, 0, 5), Some(5));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 4, 0, 5), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 5), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(6, 6, 0, 5), None);
    }

    #[test]
    fn list_groups_v5_roundtrip_is_leftover_empty() {
        let states = vec!["Stable".to_string(), "Empty".to_string()];
        let types = vec!["classic".to_string(), "consumer".to_string()];
        let mut buf = BytesMut::new();
        encode_list_groups_request(&mut buf, 5, &states, &types).unwrap();
        let mut cur = &buf[..];
        let (got_states, got_types) = decode_list_groups_request(&mut cur, 5).unwrap();
        assert_eq!(got_states, states);
        assert_eq!(got_types, types);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 request must be leftover-empty"
        );

        let resp = ListGroupsResponse {
            error_code: 0,
            groups: vec![ListedGroup {
                group_id: "g".into(),
                protocol_type: "consumer".into(),
                group_state: "Stable".into(),
                group_type: "classic".into(),
            }],
        };
        buf.clear();
        encode_list_groups_response(&mut buf, 5, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_list_groups_response(&mut cur, 5).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 response must be leftover-empty"
        );
    }

    #[test]
    fn list_groups_v6_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_list_groups_request(&mut buf, 6, &[], &[]).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v6+ is not spoken, got {err}"
        );
    }

    #[test]
    fn list_groups_error_code_is_at_bytes_4_5() {
        // Official v5 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact Groups. Measured independently from
        // Apache ListGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 5-6 from
        // DescribeGroups / ConsumerGroupDescribe first-group, or
        // bytes 12-13 from DescribeProducers first partition. Official
        // listed errors include COORDINATOR_NOT_AVAILABLE (15), not
        // NOT_COORDINATOR (16).
        let resp = ListGroupsResponse {
            error_code: crate::error::COORDINATOR_NOT_AVAILABLE,
            groups: vec![ListedGroup::new("g")],
        };
        let mut buf = BytesMut::new();
        encode_list_groups_response(&mut buf, 5, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 ErrorCode is not a first-group field at bytes 5-6"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::COORDINATOR_NOT_AVAILABLE,
            "v5 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_list_groups_response(&mut cur, 5).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "ListGroups v5 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn delete_groups_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 42
        // validVersions 0-2, flexibleVersions 2+, listeners broker only.
        // This crate speaks 0–2; this fixture is v2. Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5), DescribeGroups
        // / ConsumerGroupDescribe (first-group ErrorCode at bytes 5-6),
        // or DescribeProducers (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x67, 0x00, 0x10, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_delete_groups_request(&mut buf, 2, &ids).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DeletableGroupResult::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_delete_groups_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn delete_groups_v0_is_classic_v1_matches_v0() {
        const REQ_V0: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67];
        const RESP_V0_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x67, 0x00, 0x10,
        ];
        const REQ_V2: &[u8] = &[0x02, 0x02, 0x67, 0x00];
        let ids = vec!["g".to_string()];
        let resp = vec![DeletableGroupResult::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_delete_groups_request(&mut buf, 0, &ids).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_request(&mut cur, 0).unwrap(), ids);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v0 request leftover-empty"
        );
        buf.clear();
        encode_delete_groups_request(&mut buf, 1, &ids).unwrap();
        assert_eq!(&buf[..], REQ_V0, "v1 request matches v0");
        buf.clear();
        encode_delete_groups_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_16);
        buf.clear();
        encode_delete_groups_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_16, "v1 response matches v0");
        assert_ne!(REQ_V0, REQ_V2, "v2 must use compact arrays");
        assert_eq!(crate::protocol::api_keys::pick_version(0, 2, 0, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 2), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 2), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(3, 3, 0, 2), None);
    }

    #[test]
    fn delete_groups_v2_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_delete_groups_request(&mut buf, 2, &ids).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_request(&mut cur, 2).unwrap(), ids);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 request must be leftover-empty"
        );

        let resp = vec![
            DeletableGroupResult::new("g", 0),
            DeletableGroupResult::new("g2", crate::error::NOT_COORDINATOR),
        ];
        buf.clear();
        encode_delete_groups_response(&mut buf, 2, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_response(&mut cur, 2).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_groups_v3_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_delete_groups_request(&mut buf, 3, &[]).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3+ is not spoken, got {err}"
        );
    }

    #[test]
    fn delete_groups_first_group_error_code_is_at_bytes_7_8() {
        // Official v2 body: throttle INT32, compact Results of
        // {GroupId, ErrorCode, tagged}. Measured independently from
        // Apache DeleteGroupsResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from ListGroups /
        // DescribeClientQuotas, bytes 5-6 from DescribeGroups /
        // ConsumerGroupDescribe first-group, or bytes 12-13 from
        // DescribeProducers first partition.
        let resp = vec![DeletableGroupResult::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_delete_groups_response(&mut buf, 2, &resp).unwrap();
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v2 first-group ErrorCode must be the INT16 at bytes 7-8"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v2 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v2 ErrorCode is not a first-group first field at bytes 5-6"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_delete_groups_response(&mut cur, 2).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "DeleteGroups v2 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn share_group_describe_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 77
        // Kafka 4.0 validVersions "0" / Kafka 4.1 "1", flexibleVersions
        // 0+, listeners broker only. This crate speaks 0–1. Not copied
        // from ListGroups (top-level ErrorCode at bytes 4-5),
        // DescribeGroups / ConsumerGroupDescribe (first-group ErrorCode
        // at bytes 5-6 on a different member layout), DeleteGroups
        // (after GroupId at bytes 7-8), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00, 0x02, 0x67, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let ids = vec!["g".to_string()];
        let mut buf = BytesMut::new();
        encode_share_group_describe_request(&mut buf, 1, &ids, false).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedShareGroup::new("g", crate::error::NOT_COORDINATOR)];
        buf.clear();
        encode_share_group_describe_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn share_group_describe_v1_roundtrip_is_leftover_empty() {
        let ids = vec!["g".to_string(), "g2".to_string()];
        let mut buf = BytesMut::new();
        encode_share_group_describe_request(&mut buf, 1, &ids, true).unwrap();
        let mut cur = &buf[..];
        let (got, include) = decode_share_group_describe_request(&mut cur, 1).unwrap();
        assert_eq!(got, ids);
        assert!(include);
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 request must be leftover-empty"
        );

        let mut member = ShareGroupMember::new("m1", 1, "c", "h");
        member.subscribed_topic_names = vec!["t".into()];
        member.assignment =
            ShareGroupAssignment::new(vec![ShareGroupTopicPartitions::new([0; 16], "t", vec![0])]);
        let resp = vec![DescribedShareGroup {
            error_code: 0,
            error_message: None,
            group_id: "g".into(),
            group_state: "Stable".into(),
            group_epoch: 1,
            assignment_epoch: 1,
            assignor_name: "uniform".into(),
            members: vec![member],
            authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }];
        buf.clear();
        encode_share_group_describe_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_describe_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 response must be leftover-empty"
        );
    }

    #[test]
    fn share_group_describe_first_group_error_code_is_at_bytes_5_6() {
        // Official v1 body: throttle INT32, compact Groups of
        // {ErrorCode, ...}. Measured independently from Apache
        // ShareGroupDescribeResponse.json and a kafka-protocol 0.18.0
        // broker encode (`features = ["broker"]`) on leftover-empty
        // fixture group "g". Do not assume bytes 4-5 from ListGroups /
        // DescribeClientQuotas, bytes 5-6 from DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedShareGroup::new("g", crate::error::NOT_COORDINATOR)];
        let mut buf = BytesMut::new();
        encode_share_group_describe_response(&mut buf, 1, &resp).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v1 first-group ErrorCode must be the INT16 at bytes 5-6"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5b = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5b]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::NOT_COORDINATOR,
            "v1 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_share_group_describe_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ShareGroupDescribe v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn share_group_describe_v0_matches_v1_and_does_not_speak_v2() {
        // Official Kafka 4.0 JSON: validVersions "0", flexibleVersions "0+",
        // latestVersionUnstable. Official Kafka 4.1 JSON: validVersions "1"
        // (v0 removed). Same request/response fields. This crate speaks 0–1.
        let ids = vec!["g".to_string()];
        let mut v0 = BytesMut::new();
        encode_share_group_describe_request(&mut v0, 0, &ids, false).unwrap();
        let mut v1 = BytesMut::new();
        encode_share_group_describe_request(&mut v1, 1, &ids, false).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        let mut cur = v0.as_ref();
        let (got, include) = decode_share_group_describe_request(&mut cur, 0).unwrap();
        assert_eq!(got, ids);
        assert!(!include);
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let err =
            encode_share_group_describe_request(&mut BytesMut::new(), 2, &ids, false).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_share_group_describe_request(&mut empty, 2).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);

        let resp = vec![DescribedShareGroup::new("g", crate::error::NOT_COORDINATOR)];
        v0.clear();
        encode_share_group_describe_response(&mut v0, 0, &resp).unwrap();
        v1.clear();
        encode_share_group_describe_response(&mut v1, 1, &resp).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 response bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(
            decode_share_group_describe_response(&mut cur, 0).unwrap(),
            resp
        );
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_share_group_describe_response(&mut v0, 2, &resp).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }

    #[test]
    fn describe_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 90
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5), ShareGroupDescribe
        // / DescribeGroups / ConsumerGroupDescribe (first-group ErrorCode
        // at bytes 5-6), DeleteGroups (after GroupId at bytes 7-8), or
        // DescribeProducers (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x67, 0x01, 0x00, 0x00];
        const RESP_16: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x67, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00,
        ];
        let groups = vec![DescribeShareGroupOffsetsGroup::new("g")];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_request(&mut buf, &groups).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = vec![DescribedShareGroupOffsets::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        buf.clear();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn describe_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let groups = vec![
            DescribeShareGroupOffsetsGroup {
                group_id: "g".into(),
                topics: Some(vec![DescribeShareGroupOffsetsTopic::new("t", vec![0, 1])]),
            },
            DescribeShareGroupOffsetsGroup::all("g2"),
        ];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_request(&mut buf, &groups).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_request(&mut cur).unwrap(),
            groups
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = vec![DescribedShareGroupOffsets {
            group_id: "g".into(),
            topics: vec![DescribedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![DescribedShareGroupOffsetsPartition {
                    partition_index: 0,
                    start_offset: 7,
                    leader_epoch: 3,
                    error_code: 0,
                    error_message: None,
                }],
            }],
            error_code: 0,
            error_message: None,
        }];
        buf.clear();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_share_group_offsets_first_group_error_code_is_at_bytes_8_9() {
        // Official v0 body: throttle INT32, compact Groups of
        // {GroupId, Topics, ErrorCode, ...}. Measured independently
        // from Apache DescribeShareGroupOffsetsResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features = ["broker"]`)
        // on leftover-empty fixture group "g" (empty Topics). Do not
        // assume bytes 4-5 from ListGroups / DescribeClientQuotas,
        // bytes 5-6 from ShareGroupDescribe / DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, or bytes 12-13 from DescribeProducers.
        let resp = vec![DescribedShareGroupOffsets::new(
            "g",
            crate::error::NOT_COORDINATOR,
        )];
        let mut buf = BytesMut::new();
        encode_describe_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b8 = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b8, b9]),
            crate::error::NOT_COORDINATOR,
            "v0 first-group ErrorCode must be the INT16 at bytes 8-9"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8b = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8b]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn alter_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 91
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListGroups (top-level ErrorCode at bytes 4-5, different
        // fields after the INT16), ShareGroupDescribe / DescribeGroups
        // / ConsumerGroupDescribe (first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x67, 0x01, 0x00];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_request(&mut buf, "g", &[]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AlteredShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn alter_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let topics = vec![
            AlterShareGroupOffsetsTopic::new(
                "t",
                vec![
                    AlterShareGroupOffsetsPartition::new(0, 7),
                    AlterShareGroupOffsetsPartition::new(1, 9),
                ],
            ),
            AlterShareGroupOffsetsTopic::new("t2", vec![]),
        ];
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_request(&mut buf, "g", &topics).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_request(&mut cur).unwrap(),
            ("g".into(), topics)
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = AlteredShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![AlteredShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![AlteredShareGroupOffsetsPartition {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                }],
            }],
        };
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn alter_share_group_offsets_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact nullable ErrorMessage, then compact
        // Responses. Measured independently from Apache
        // AlterShareGroupOffsetsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g" (empty Responses). Do not assume
        // bytes 4-5 from ListGroups / DescribeClientQuotas, bytes 5-6
        // from ShareGroupDescribe / DescribeGroups /
        // ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, bytes 8-9 from DescribeShareGroupOffsets first-
        // group, or bytes 12-13 from DescribeProducers. The hop code
        // is this top-level INT16, not the first-partition ErrorCode
        // (bytes 31-32 when leftover-empty topic "t" partition 0 is
        // present).
        let resp = AlteredShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        let mut buf = BytesMut::new();
        encode_alter_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 10,
            "leftover-empty fixture is shorter than DescribeShareGroupOffsets bytes 8-9"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );

        let with_part = AlteredShareGroupOffsets {
            error_code: crate::error::NOT_COORDINATOR,
            error_message: None,
            topics: vec![AlteredShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                partitions: vec![AlteredShareGroupOffsetsPartition {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                }],
            }],
        };
        buf.clear();
        encode_alter_share_group_offsets_response(&mut buf, &with_part).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 hop ErrorCode stays the top-level INT16 at bytes 4-5"
        );
        let b31 = buf.get(31).copied().unwrap();
        let b32 = buf.get(32).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b31, b32]),
            0,
            "v0 first-partition ErrorCode is the INT16 at bytes 31-32 and is not the hop code"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_share_group_offsets_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "AlterShareGroupOffsets v0 first-partition body must be leftover-empty"
        );
    }

    #[test]
    fn delete_share_group_offsets_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 92
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // AlterShareGroupOffsets / ListGroups (top-level ErrorCode at
        // bytes 4-5, different fields after the INT16),
        // ShareGroupDescribe / DescribeGroups / ConsumerGroupDescribe
        // (first-group ErrorCode at bytes 5-6), DeleteGroups (after
        // GroupId at bytes 7-8), DescribeShareGroupOffsets (first-group
        // after GroupId and Topics at bytes 8-9), or DescribeProducers
        // (first-partition ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x67, 0x01, 0x00];
        const RESP_16: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00];
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_request(&mut buf, "g", &[]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DeletedShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_16);
    }

    #[test]
    fn delete_share_group_offsets_v0_roundtrip_is_leftover_empty() {
        let topics = vec![
            DeleteShareGroupOffsetsTopic::new("t"),
            DeleteShareGroupOffsetsTopic::new("t2"),
        ];
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_request(&mut buf, "g", &topics).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_request(&mut cur).unwrap(),
            ("g".into(), topics)
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 request must be leftover-empty"
        );

        let resp = DeletedShareGroupOffsets {
            error_code: 0,
            error_message: None,
            topics: vec![DeletedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                error_code: 0,
                error_message: None,
            }],
        };
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 response must be leftover-empty"
        );
    }

    #[test]
    fn delete_share_group_offsets_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact nullable ErrorMessage, then compact
        // Responses. Measured independently from Apache
        // DeleteShareGroupOffsetsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture group "g" (empty Responses). Do not assume
        // bytes 4-5 from AlterShareGroupOffsets / ListGroups /
        // DescribeClientQuotas, bytes 5-6 from ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The hop code is this top-level INT16, not
        // the first-topic ErrorCode (bytes 26-27 when leftover-empty
        // topic "t" is present). This API has no first-partition
        // ErrorCode.
        let resp = DeletedShareGroupOffsets::new(crate::error::NOT_COORDINATOR);
        let mut buf = BytesMut::new();
        encode_delete_share_group_offsets_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5, b6]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at ShareGroupDescribe first-group bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::NOT_COORDINATOR,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.len() < 10,
            "leftover-empty fixture is shorter than DescribeShareGroupOffsets bytes 8-9"
        );
        assert!(
            buf.len() < 14,
            "leftover-empty fixture is shorter than DescribeProducers bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 ErrorCode body must be leftover-empty"
        );

        let with_topic = DeletedShareGroupOffsets {
            error_code: crate::error::NOT_COORDINATOR,
            error_message: None,
            topics: vec![DeletedShareGroupOffsetsTopic {
                topic_name: "t".into(),
                topic_id: [0; 16],
                error_code: 0,
                error_message: None,
            }],
        };
        buf.clear();
        encode_delete_share_group_offsets_response(&mut buf, &with_topic).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_COORDINATOR,
            "v0 hop ErrorCode stays the top-level INT16 at bytes 4-5"
        );
        let b26 = buf.get(26).copied().unwrap();
        let b27 = buf.get(27).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b26, b27]),
            0,
            "v0 first-topic ErrorCode is the INT16 at bytes 26-27 and is not the hop code"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_delete_share_group_offsets_response(&mut cur).unwrap(),
            with_topic
        );
        assert!(
            !cur.has_remaining(),
            "DeleteShareGroupOffsets v0 first-topic body must be leftover-empty"
        );
    }

    #[test]
    fn describe_topic_partitions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 75
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5), ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe (first-group ErrorCode
        // at bytes 5-6), DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x00, 0x00, 0x00, 0x07, 0xd0, 0xff, 0x00];
        const RESP_29: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x1d, 0x02, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x80,
            0x00, 0x00, 0x00, 0x00, 0xff, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_request(&mut buf, &["t".into()], 2000, None).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions::new(
            "t",
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        )]);
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_29);
    }

    #[test]
    fn describe_topic_partitions_v0_roundtrip_is_leftover_empty() {
        let topics = vec!["t".into(), "t2".into()];
        let cursor = TopicPartitionCursor::new("t", 3);
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_request(&mut buf, &topics, 2000, Some(&cursor)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_request(&mut cur).unwrap(),
            (topics, 2000, Some(cursor.clone()))
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 request must be leftover-empty"
        );

        let resp = DescribeTopicPartitionsResponse {
            topics: vec![DescribedTopicPartitions {
                error_code: 0,
                name: Some("t".into()),
                topic_id: [0; 16],
                is_internal: false,
                partitions: vec![DescribedTopicPartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    eligible_leader_replicas: None,
                    last_known_elr: None,
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
            }],
            next_cursor: Some(cursor),
        };
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn describe_topic_partitions_first_topic_error_code_is_at_bytes_5_6() {
        // Official v0 body: throttle INT32, compact Topics, then each
        // topic starts with ErrorCode INT16. There is no top-level
        // ErrorCode. Measured independently from Apache
        // DescribeTopicPartitionsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture topic "t" (empty Partitions). Do not assume
        // bytes 4-5 from DeleteShareGroupOffsets / AlterShareGroupOffsets
        // / ListGroups, bytes 5-6 from ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The first ErrorCode is this first-topic
        // INT16. First-partition ErrorCode (bytes 27-28 when leftover-
        // empty partition 0 is present) is not the first ErrorCode.
        let resp = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions::new(
            "t",
            crate::error::TOPIC_AUTHORIZATION_FAILED,
        )]);
        let mut buf = BytesMut::new();
        encode_describe_topic_partitions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not a top-level field at bytes 4-5"
        );
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 first-topic ErrorCode must be the INT16 at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 ErrorCode body must be leftover-empty"
        );

        let with_part = DescribeTopicPartitionsResponse::new(vec![DescribedTopicPartitions {
            error_code: crate::error::TOPIC_AUTHORIZATION_FAILED,
            name: Some("t".into()),
            topic_id: [0; 16],
            is_internal: false,
            partitions: vec![DescribedTopicPartition::new(
                crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            )],
            topic_authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
        }]);
        buf.clear();
        encode_describe_topic_partitions_response(&mut buf, &with_part).unwrap();
        let b5 = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b5, b6]),
            crate::error::TOPIC_AUTHORIZATION_FAILED,
            "v0 first ErrorCode stays the first-topic INT16 at bytes 5-6"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b27, b28]),
            crate::error::UNKNOWN_TOPIC_OR_PARTITION,
            "v0 first-partition ErrorCode is the INT16 at bytes 27-28 and is not the first ErrorCode"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_topic_partitions_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "DescribeTopicPartitions v0 first-partition body must be leftover-empty"
        );
    }

    #[test]
    fn list_config_resources_v1_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 74
        // validVersions 0-1, flexibleVersions 0+, listeners broker only.
        // This crate speaks 0–1; this fixture is v1. Not copied from
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5, different fields after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), or DescribeProducers (first-partition
        // ErrorCode at bytes 12-13).
        const REQ: &[u8] = &[0x02, 0x10, 0x00];
        const RESP_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x02, 0x02, 0x72, 0x10, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_list_config_resources_request(&mut buf, 1, &[RESOURCE_CLIENT_METRICS]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = ListConfigResourcesResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS)],
        );
        buf.clear();
        encode_list_config_resources_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
    }

    #[test]
    fn list_config_resources_v1_roundtrip_is_leftover_empty() {
        let types = vec![RESOURCE_CLIENT_METRICS, RESOURCE_TOPIC];
        let mut buf = BytesMut::new();
        encode_list_config_resources_request(&mut buf, 1, &types).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_request(&mut cur, 1).unwrap(),
            types
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 request must be leftover-empty"
        );

        let resp = ListConfigResourcesResponse::new(
            0,
            vec![
                ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS),
                ListedConfigResource::new("t", RESOURCE_TOPIC),
            ],
        );
        buf.clear();
        encode_list_config_resources_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 response must be leftover-empty"
        );
    }

    #[test]
    fn list_config_resources_top_level_error_code_is_at_bytes_4_5() {
        // Official v1 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact ConfigResources. There is no first-
        // resource ErrorCode. Measured independently from Apache
        // ListConfigResourcesResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture resource "r" type CLIENT_METRICS (16). Do not
        // assume bytes 4-5 from DeleteShareGroupOffsets /
        // AlterShareGroupOffsets / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, or bytes 12-13 from
        // DescribeProducers. The leftover-empty body is 12 bytes, so
        // bytes 12-13 are not present. Official JSON lists no
        // errorCodes; official handler writes
        // CLUSTER_AUTHORIZATION_FAILED (31) via getErrorResponse.
        let resp = ListConfigResourcesResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS)],
        );
        let mut buf = BytesMut::new();
        encode_list_config_resources_response(&mut buf, 1, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not a first-resource field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v1 leftover-empty body is 12 bytes; DescribeProducers bytes 12-13 are not present"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ListConfigResources v1 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn list_config_resources_v0_omits_resource_types() {
        const REQ_V0: &[u8] = &[0x00];
        const RESP_V0_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x02, 0x02, 0x72, 0x00, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_list_config_resources_request(&mut buf, 0, &[RESOURCE_TOPIC]).unwrap();
        assert_eq!(&buf[..], REQ_V0, "v0 request is tagged fields only");
        let mut cur = &buf[..];
        assert_eq!(
            decode_list_config_resources_request(&mut cur, 0).unwrap(),
            Vec::<i8>::new(),
            "v0 has no ResourceTypes; decode fills empty"
        );
        assert!(!cur.has_remaining());
        let resp = ListConfigResourcesResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![ListedConfigResource::new("r", RESOURCE_TOPIC)],
        );
        buf.clear();
        encode_list_config_resources_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V0_31);
        let mut cur = &buf[..];
        let got = decode_list_config_resources_response(&mut cur, 0).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(got.error_code, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(got.config_resources[0].resource_name, "r");
        assert_eq!(
            got.config_resources[0].resource_type, RESOURCE_CLIENT_METRICS,
            "v0 has no ResourceType; decode fills CLIENT_METRICS (16)"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);
    }

    #[test]
    fn list_config_resources_v2_is_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_list_config_resources_request(&mut buf, 2, &[]).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2+ is not spoken, got {err}"
        );
    }

    #[test]
    fn get_telemetry_subscriptions_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 71
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // ListConfigResources / DeleteShareGroupOffsets /
        // AlterShareGroupOffsets / ListGroups (top-level ErrorCode at
        // bytes 4-5, different fields after), DescribeTopicPartitions /
        // ShareGroupDescribe / DescribeGroups (first-topic / first-
        // group ErrorCode at bytes 5-6), DeleteGroups (after GroupId
        // at bytes 7-8), DescribeShareGroupOffsets (first-group after
        // GroupId and Topics at bytes 8-9), DescribeProducers (first-
        // partition ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x00,
        ];
        const RESP_35: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x00, 0x00, 0x00, 0x01, 0x02, 0x01,
            0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x00, 0x64, 0x01, 0x02, 0x02, 0x6d, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_request(&mut buf, &[0x11; 16]).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = GetTelemetrySubscriptionsResponse::new(
            crate::error::UNSUPPORTED_VERSION,
            [0x11; 16],
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        buf.clear();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_35);
    }

    #[test]
    fn get_telemetry_subscriptions_v0_roundtrip_is_leftover_empty() {
        let id = [0x11u8; 16];
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_request(&mut buf, &id).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_request(&mut cur).unwrap(),
            id
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 request must be leftover-empty"
        );

        let resp = GetTelemetrySubscriptionsResponse::new(
            0,
            id,
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        buf.clear();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 response must be leftover-empty"
        );
    }

    #[test]
    fn get_telemetry_subscriptions_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then ClientInstanceId UUID, SubscriptionId INT32,
        // compact AcceptedCompressionTypes, PushIntervalMs,
        // TelemetryMaxBytes, DeltaTemporality, compact RequestedMetrics.
        // There is no first-subscription ErrorCode and no first-metric
        // ErrorCode. Measured independently from Apache
        // GetTelemetrySubscriptionsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture ClientInstanceId [0x11; 16], SubscriptionId 1,
        // accepted compression [1], PushIntervalMs 1000,
        // TelemetryMaxBytes 100, DeltaTemporality true, RequestedMetrics
        // ["m"]. Do not assume bytes 4-5 from ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // handler writes UNSUPPORTED_VERSION (35) via getErrorResponse
        // on the older ZooKeeper path.
        let resp = GetTelemetrySubscriptionsResponse::new(
            crate::error::UNSUPPORTED_VERSION,
            [0x11; 16],
            1,
            vec![1],
            1000,
            100,
            true,
            vec!["m".into()],
        );
        let mut buf = BytesMut::new();
        encode_get_telemetry_subscriptions_response(&mut buf, &resp).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not a first-subscription field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::UNSUPPORTED_VERSION,
            "v0 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_get_telemetry_subscriptions_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "GetTelemetrySubscriptions v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn push_telemetry_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 72
        // validVersions 0, flexibleVersions 0+, listeners broker only.
        // This crate targets v0 (VERSIONS.max). Not copied from
        // GetTelemetrySubscriptions / ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups
        // (top-level ErrorCode at bytes 4-5, different fields after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
            0x11, 0x11, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02, 0x6d, 0x00,
        ];
        // INVALID_REQUEST (42). Leftover-empty body is 7 bytes.
        const RESP_42: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00];
        let req = PushTelemetryRequest::new([0x11; 16], 1, false, 0, b"m".to_vec());
        let mut buf = BytesMut::new();
        encode_push_telemetry_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = PushTelemetryResponse::new(42);
        buf.clear();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_42);
    }

    #[test]
    fn push_telemetry_v0_roundtrip_is_leftover_empty() {
        let req = PushTelemetryRequest::new([0x11; 16], 1, false, 0, b"m".to_vec());
        let mut buf = BytesMut::new();
        encode_push_telemetry_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_request(&mut cur).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 request must be leftover-empty"
        );

        let resp = PushTelemetryResponse::new(0);
        buf.clear();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 response must be leftover-empty"
        );
    }

    #[test]
    fn push_telemetry_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then tagged. There is no first-metric ErrorCode and
        // no first-payload ErrorCode. Metrics live on the request.
        // Measured independently from Apache PushTelemetryResponse.json
        // and a kafka-protocol 0.18.0 broker encode
        // (`features = ["broker"]`) on leftover-empty fixture throttle
        // 0, error INVALID_REQUEST (42). Do not assume bytes 4-5 from
        // GetTelemetrySubscriptions / ListConfigResources /
        // DeleteShareGroupOffsets / AlterShareGroupOffsets / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // handler writes INVALID_REQUEST (42) via getErrorResponse on
        // catch-all / reserved ClientInstanceId.
        let resp = PushTelemetryResponse::new(42);
        let mut buf = BytesMut::new();
        encode_push_telemetry_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            7,
            "v0 leftover-empty ErrorCode body is throttle + INT16 + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            42,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            42,
            "v0 ErrorCode is not a first-metric / first-payload field at bytes 5-6"
        );
        assert!(
            buf.get(7).is_none(),
            "v0 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v0 ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v0 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(decode_push_telemetry_response(&mut cur).unwrap(), resp);
        assert!(
            !cur.has_remaining(),
            "PushTelemetry v0 ErrorCode body must be leftover-empty"
        );
    }

    #[test]
    fn assign_replicas_to_dirs_v0_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 73
        // validVersions 0, flexibleVersions 0+, listeners controller
        // only. This crate targets v0 (VERSIONS.max). Not copied from
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups (top-level ErrorCode at
        // bytes 4-5, no Directories array after),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00,
        ];
        // NOT_CONTROLLER (41). Leftover-empty body is 8 bytes.
        const RESP_41: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x01, 0x00];
        let req = AssignReplicasToDirsRequest::new(7, -1, vec![]);
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_request(&mut buf, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AssignReplicasToDirsResponse::new(crate::error::NOT_CONTROLLER, vec![]);
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(&buf[..], RESP_41);
    }

    #[test]
    fn assign_replicas_to_dirs_v0_roundtrip_is_leftover_empty() {
        let req = AssignReplicasToDirsRequest::new(
            7,
            -1,
            vec![AssignReplicasToDirsDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsPartition::new(0)],
                )],
            )],
        );
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_request(&mut buf, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_request(&mut cur).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 request must be leftover-empty"
        );

        let resp = AssignReplicasToDirsResponse::new(
            0,
            vec![AssignReplicasToDirsResponseDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsResponseTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsResponsePartition::new(0, 0)],
                )],
            )],
        );
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 response must be leftover-empty"
        );
    }

    #[test]
    fn assign_replicas_to_dirs_top_level_error_code_is_at_bytes_4_5() {
        // Official v0 body: throttle INT32, then top-level ErrorCode
        // INT16, then compact Directories, then tagged. There is no
        // first-directory ErrorCode (directory Id is a UUID) and the
        // leftover-empty fixture has no first-partition ErrorCode.
        // Measured independently from Apache
        // AssignReplicasToDirsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture throttle 0, error NOT_CONTROLLER (41). Do not
        // assume bytes 4-5 from PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, or bytes 27-28 from DescribeTopicPartitions
        // first-partition. Official JSON lists no errorCodes; official
        // getErrorResponse writes NOT_CONTROLLER (41) via
        // NotControllerException from ControllerWriteEvent when the
        // node is not the active controller.
        let resp = AssignReplicasToDirsResponse::new(crate::error::NOT_CONTROLLER, vec![]);
        let mut buf = BytesMut::new();
        encode_assign_replicas_to_dirs_response(&mut buf, &resp).unwrap();
        assert_eq!(
            buf.len(),
            8,
            "v0 leftover-empty ErrorCode body is throttle + INT16 + empty dirs + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::NOT_CONTROLLER,
            "v0 ErrorCode is not a first-directory / first-partition field at bytes 5-6"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 leftover-empty ErrorCode is not at DeleteGroups after-GroupId bytes 7-8 as a 2-byte field past the body"
        );
        assert!(
            buf.get(8).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeProducers first-partition bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v0 leftover-empty ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 ErrorCode body must be leftover-empty"
        );

        // One-directory fixture: first-partition ErrorCode is at
        // bytes 45-46, not the hop code.
        let with_part = AssignReplicasToDirsResponse::new(
            crate::error::NOT_CONTROLLER,
            vec![AssignReplicasToDirsResponseDirectory::new(
                [0x11; 16],
                vec![AssignReplicasToDirsResponseTopic::new(
                    [0x22; 16],
                    vec![AssignReplicasToDirsResponsePartition::new(
                        0,
                        crate::error::NOT_LEADER_OR_FOLLOWER,
                    )],
                )],
            )],
        );
        buf.clear();
        encode_assign_replicas_to_dirs_response(&mut buf, &with_part).unwrap();
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::NOT_CONTROLLER,
            "v0 top-level 41 stays at bytes 4-5 when Directories are present"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b45, b46]),
            crate::error::NOT_LEADER_OR_FOLLOWER,
            "v0 first-partition ErrorCode is at bytes 45-46, not a hop"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_assign_replicas_to_dirs_response(&mut cur).unwrap(),
            with_part
        );
        assert!(
            !cur.has_remaining(),
            "AssignReplicasToDirs v0 one-directory body must be leftover-empty"
        );
    }

    #[test]
    fn alter_replica_log_dirs_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 34
        // validVersions 1-2, flexibleVersions 2+, listeners broker only.
        // This crate speaks 1–2. Not copied from
        // AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), DescribeProducers (first-partition
        // ErrorCode at bytes 12-13), or DescribeTopicPartitions
        // first-partition (bytes 27-28).
        const REQ: &[u8] = &[
            0x02, 0x03, 0x2f, 0x64, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        // CLUSTER_AUTHORIZATION_FAILED (31). One-partition body is 17
        // bytes. Leftover-empty (empty Results) is 6 bytes and has no
        // ErrorCode.
        const RESP_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1f,
            0x00, 0x00, 0x00,
        ];
        const RESP_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let req = AlterReplicaLogDirsRequest::new(vec![AlterReplicaLogDirsDirectory::new(
            "/d",
            vec![AlterReplicaLogDirsTopic::new("t", vec![0])],
        )]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_request(&mut buf, 2, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(
                0,
                crate::error::CLUSTER_AUTHORIZATION_FAILED,
            )],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
        buf.clear();
        encode_alter_replica_log_dirs_response(
            &mut buf,
            2,
            &AlterReplicaLogDirsResponse::new(vec![]),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_EMPTY);
    }

    #[test]
    fn alter_replica_log_dirs_v1_is_classic() {
        // Same fields as v2; classic INT32 array lengths, INT16 STRING,
        // no tagged fields. Apache JSON validVersions 1-2, v0 removed.
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x2f, 0x64, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
            0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ];
        const REQ_V2: &[u8] = &[
            0x02, 0x03, 0x2f, 0x64, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        const RESP_V1_31: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1f,
        ];
        const RESP_V1_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let req = AlterReplicaLogDirsRequest::new(vec![AlterReplicaLogDirsDirectory::new(
            "/d",
            vec![AlterReplicaLogDirsTopic::new("t", vec![0])],
        )]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_request(&mut buf, 1, &req).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_request(&mut cur, 1).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v1 request leftover-empty"
        );
        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(
                0,
                crate::error::CLUSTER_AUTHORIZATION_FAILED,
            )],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V1_31);
        assert_eq!(
            buf.len(),
            21,
            "v1 one-partition body is throttle + classic results + topic t + partition 0 + INT16"
        );
        let b19 = buf.get(19).copied().unwrap();
        let b20 = buf.get(20).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b19, b20]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 first-partition ErrorCode must be the INT16 at bytes 19-20"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v1 ErrorCode is not at the v2 first-partition bytes 12-13"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v1 response leftover-empty"
        );
        buf.clear();
        encode_alter_replica_log_dirs_response(
            &mut buf,
            1,
            &AlterReplicaLogDirsResponse::new(vec![]),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_V1_EMPTY);
        assert_ne!(REQ_V1, REQ_V2, "v2 must use compact arrays");
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 2), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 2), None);
        assert_eq!(crate::protocol::api_keys::pick_version(3, 3, 1, 2), None);
    }

    #[test]
    fn alter_replica_log_dirs_v0_and_v3_are_not_spoken() {
        let mut buf = BytesMut::new();
        let err = encode_alter_replica_log_dirs_request(
            &mut buf,
            0,
            &AlterReplicaLogDirsRequest::new(vec![]),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_alter_replica_log_dirs_request(
            &mut buf,
            3,
            &AlterReplicaLogDirsRequest::new(vec![]),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3+ is not spoken, got {err}"
        );
    }

    #[test]
    fn alter_replica_log_dirs_v2_roundtrip_is_leftover_empty() {
        let req = AlterReplicaLogDirsRequest::new(vec![AlterReplicaLogDirsDirectory::new(
            "/d",
            vec![AlterReplicaLogDirsTopic::new("t", vec![0])],
        )]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_request(&mut buf, 2, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_request(&mut cur, 2).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 request must be leftover-empty"
        );

        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(0, 0)],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, 2, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 response must be leftover-empty"
        );

        buf.clear();
        encode_alter_replica_log_dirs_request(
            &mut buf,
            2,
            &AlterReplicaLogDirsRequest::new(vec![]),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_request(&mut cur, 2).unwrap(),
            AlterReplicaLogDirsRequest::new(vec![])
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 empty request must be leftover-empty"
        );
    }

    #[test]
    fn alter_replica_log_dirs_first_partition_error_code_is_at_bytes_12_13() {
        // Official v2 body: throttle INT32, then compact Results of
        // {TopicName, Partitions of {PartitionIndex INT32, ErrorCode
        // INT16, tagged}}. There is no top-level ErrorCode and no
        // first-directory ErrorCode (the response has no directory
        // array). Leftover-empty empty Results has no ErrorCode.
        // Measured independently from Apache
        // AlterReplicaLogDirsResponse.json and a kafka-protocol
        // 0.18.0 broker encode (`features = ["broker"]`) on leftover-
        // empty fixture throttle 0, topic "t", partition 0, error
        // CLUSTER_AUTHORIZATION_FAILED (31). Do not assume bytes 4-5
        // from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // DescribeProducers, bytes 27-28 from DescribeTopicPartitions
        // first-partition, or bytes 45-46 from AssignReplicasToDirs
        // first-partition. Official JSON lists no errorCodes; official
        // getErrorResponse writes CLUSTER_AUTHORIZATION_FAILED (31)
        // onto each partition when KafkaApis authorization fails.
        let empty = AlterReplicaLogDirsResponse::new(vec![]);
        let mut buf = BytesMut::new();
        encode_alter_replica_log_dirs_response(&mut buf, 2, &empty).unwrap();
        assert_eq!(
            buf.len(),
            6,
            "v2 leftover-empty empty-Results body is throttle + compact empty + tagged"
        );
        assert!(
            buf.get(4).zip(buf.get(5)).is_some(),
            "v2 leftover-empty body reaches bytes 4-5 as compact empty + tagged, not ErrorCode"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 leftover-empty has no top-level ErrorCode at bytes 4-5"
        );
        assert!(
            buf.get(12).is_none(),
            "v2 leftover-empty empty-Results has no first-partition ErrorCode"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur, 2).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 empty-Results body must be leftover-empty"
        );

        let resp = AlterReplicaLogDirsResponse::new(vec![AlterReplicaLogDirsResponseTopic::new(
            "t",
            vec![AlterReplicaLogDirsResponsePartition::new(
                0,
                crate::error::CLUSTER_AUTHORIZATION_FAILED,
            )],
        )]);
        buf.clear();
        encode_alter_replica_log_dirs_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(
            buf.len(),
            17,
            "v2 one-partition ErrorCode body is throttle + results + topic t + partition 0 + INT16 + tagged"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b12, b13]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 first-partition ErrorCode must be the INT16 at bytes 12-13"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not a top-level field at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not a first-topic / first-directory field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not at DeleteGroups after-GroupId bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        assert!(
            buf.get(27).is_none(),
            "v2 one-partition body is shorter than DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_none(),
            "v2 one-partition body is shorter than AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 12, "i16=31 must hit only at byte 12");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 12");
        let mut cur = &buf[..];
        assert_eq!(
            decode_alter_replica_log_dirs_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "AlterReplicaLogDirs v2 one-partition body must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_v4_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 35
        // listeners broker only. This crate speaks 1–4. v5 is a named
        // STATUS hole. Not copied from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        const REQ: &[u8] = &[0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const REQ_EMPTY: &[u8] = &[0x01, 0x00];
        const REQ_NULL: &[u8] = &[0x00, 0x00];
        // CLUSTER_AUTHORIZATION_FAILED (31). Empty-Results body is 8
        // bytes. Top-level ErrorCode is at bytes 4-5.
        const RESP_31: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x01, 0x00];
        const RESP_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        let req =
            DescribeLogDirsRequest::new(Some(vec![DescribableLogDirTopic::new("t", vec![0])]));
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_request(&mut buf, 4, &req).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 4, &DescribeLogDirsRequest::new(Some(vec![])))
            .unwrap();
        assert_eq!(&buf[..], REQ_EMPTY);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 4, &DescribeLogDirsRequest::new(None)).unwrap();
        assert_eq!(&buf[..], REQ_NULL);
        let resp = DescribeLogDirsResponse::new(crate::error::CLUSTER_AUTHORIZATION_FAILED, vec![]);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(&buf[..], RESP_31);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 4, &DescribeLogDirsResponse::new(0, vec![]))
            .unwrap();
        assert_eq!(&buf[..], RESP_EMPTY);
    }

    #[test]
    fn describe_log_dirs_v1_is_classic_v2_omits_error_code() {
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00,
        ];
        const REQ_V1_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00];
        const REQ_V1_NULL: &[u8] = &[0xff, 0xff, 0xff, 0xff];
        const REQ_V2: &[u8] = &[0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_V1_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_V2_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x01, 0x00];
        const RESP_V3_31: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x1f, 0x01, 0x00];
        let req =
            DescribeLogDirsRequest::new(Some(vec![DescribableLogDirTopic::new("t", vec![0])]));
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_request(&mut buf, 1, &req).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_request(&mut cur, 1).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v1 request leftover-empty"
        );
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 1, &DescribeLogDirsRequest::new(Some(vec![])))
            .unwrap();
        assert_eq!(&buf[..], REQ_V1_EMPTY);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 1, &DescribeLogDirsRequest::new(None)).unwrap();
        assert_eq!(&buf[..], REQ_V1_NULL);
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 2, &req).unwrap();
        assert_eq!(&buf[..], REQ_V2, "v2 request matches v4 compact");
        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 3, &req).unwrap();
        assert_eq!(&buf[..], REQ_V2, "v3 request matches v2");
        let resp31 =
            DescribeLogDirsResponse::new(crate::error::CLUSTER_AUTHORIZATION_FAILED, vec![]);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 1, &resp31).unwrap();
        assert_eq!(&buf[..], RESP_V1_EMPTY, "v1 omits top-level ErrorCode");
        let mut cur = &buf[..];
        let got = decode_describe_log_dirs_response(&mut cur, 1).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(got.error_code, 0, "v1 decode fills ErrorCode 0");
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 2, &resp31).unwrap();
        assert_eq!(&buf[..], RESP_V2_EMPTY, "v2 omits top-level ErrorCode");
        assert_eq!(
            buf.len(),
            6,
            "v2 leftover-empty empty-Results is throttle + compact empty + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v2 has no top-level ErrorCode at bytes 4-5"
        );
        let mut cur = &buf[..];
        let got = decode_describe_log_dirs_response(&mut cur, 2).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(got.error_code, 0, "v2 decode fills ErrorCode 0");
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 3, &resp31).unwrap();
        assert_eq!(&buf[..], RESP_V3_31, "v3 empty-Results matches v4");
        assert_ne!(REQ_V1, REQ_V2, "v2 must use compact arrays");
        assert_eq!(crate::protocol::api_keys::pick_version(1, 4, 1, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 4), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 3, 1, 4), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 4), None);
        assert_eq!(crate::protocol::api_keys::pick_version(5, 5, 1, 4), None);
    }

    #[test]
    fn describe_log_dirs_v3_omits_total_bytes() {
        let resp = DescribeLogDirsResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                99,
                88,
            )],
        );
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(
            buf.len(),
            41,
            "v3 one-directory omits TotalBytes/UsableBytes (v4 is 57)"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v3 top-level ErrorCode is at bytes 4-5"
        );
        let mut cur = &buf[..];
        let got = decode_describe_log_dirs_response(&mut cur, 3).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(got.error_code, crate::error::CLUSTER_AUTHORIZATION_FAILED);
        assert_eq!(got.results[0].log_dir, "/d");
        assert_eq!(
            got.results[0].total_bytes, -1,
            "v3 has no TotalBytes; decode fills -1"
        );
        assert_eq!(got.results[0].usable_bytes, -1);
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(
            buf.len(),
            39,
            "v2 one-directory omits top-level ErrorCode and TotalBytes"
        );
        let mut cur = &buf[..];
        let got = decode_describe_log_dirs_response(&mut cur, 2).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(got.error_code, 0, "v2 decode fills ErrorCode 0");
        assert_eq!(got.results[0].total_bytes, -1);
    }

    #[test]
    fn describe_log_dirs_v4_roundtrip_is_leftover_empty() {
        let req =
            DescribeLogDirsRequest::new(Some(vec![DescribableLogDirTopic::new("t", vec![0])]));
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_request(&mut buf, 4, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(decode_describe_log_dirs_request(&mut cur, 4).unwrap(), req);
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 request must be leftover-empty"
        );

        let resp = DescribeLogDirsResponse::new(
            0,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 4, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_response(&mut cur, 4).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 response must be leftover-empty"
        );

        buf.clear();
        encode_describe_log_dirs_request(&mut buf, 4, &DescribeLogDirsRequest::new(None)).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_request(&mut cur, 4).unwrap(),
            DescribeLogDirsRequest::new(None)
        );
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 null-Topics request must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_top_level_error_code_is_at_bytes_4_5() {
        // Official v4 body: throttle INT32, then top-level ErrorCode
        // INT16 (versions 3+), then compact Results of {ErrorCode
        // INT16, LogDir, Topics, TotalBytes, UsableBytes, tagged}.
        // Measured independently from Apache DescribeLogDirsResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, empty
        // Results, error CLUSTER_AUTHORIZATION_FAILED (31). Do not
        // assume bytes 4-5 from AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // AlterReplicaLogDirs / DescribeProducers first-partition,
        // bytes 27-28 from DescribeTopicPartitions first-partition, or
        // bytes 45-46 from AssignReplicasToDirs first-partition.
        // Official JSON lists no errorCodes; official handler writes
        // CLUSTER_AUTHORIZATION_FAILED (31) onto the top-level field
        // when KafkaApis authorization fails.
        let empty =
            DescribeLogDirsResponse::new(crate::error::CLUSTER_AUTHORIZATION_FAILED, vec![]);
        let mut buf = BytesMut::new();
        encode_describe_log_dirs_response(&mut buf, 4, &empty).unwrap();
        assert_eq!(
            buf.len(),
            8,
            "v4 leftover-empty empty-Results body is throttle + top-level INT16 + compact empty + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 top-level ErrorCode must be the INT16 at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-directory field at bytes 5-6"
        );
        assert!(
            buf.get(8).is_none(),
            "v4 leftover-empty empty-Results has no first-directory ErrorCode at bytes 7-8"
        );
        assert!(
            buf.get(12).is_none(),
            "v4 leftover-empty empty-Results is shorter than AlterReplicaLogDirs first-partition bytes 12-13"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 4, "i16=31 must hit only at byte 4");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 4");
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_response(&mut cur, 4).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 empty-Results body must be leftover-empty"
        );

        let resp = DescribeLogDirsResponse::new(
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(
            buf.len(),
            57,
            "v4 one-directory body is throttle + top-level INT16 + results + dir + topic t + partition 0 + TotalBytes + UsableBytes + tagged"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b4, b5]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 top-level ErrorCode stays at bytes 4-5 when Results are present"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b7, b8]),
            0,
            "v4 first-directory ErrorCode is at bytes 7-8, not the hop/auth code"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-directory field at bytes 5-6"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not a first-partition field at bytes 12-13"
        );
        assert!(
            buf.get(27).is_some(),
            "v4 one-directory body reaches DescribeTopicPartitions first-partition bytes 27-28 as OffsetLag, not ErrorCode"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_some(),
            "v4 one-directory body reaches AssignReplicasToDirs first-partition bytes 45-46 as UsableBytes, not ErrorCode"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b45, b46]),
            crate::error::CLUSTER_AUTHORIZATION_FAILED,
            "v4 ErrorCode is not at AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi]) == crate::error::CLUSTER_AUTHORIZATION_FAILED {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 4, "i16=31 must hit only at byte 4");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=31 hits only at byte 4");
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_response(&mut cur, 4).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeLogDirs v4 one-directory body must be leftover-empty"
        );
    }

    #[test]
    fn describe_log_dirs_does_not_speak_v5() {
        // kafka-protocol 0.18.0 VERSIONS.max = 4. Kafka 4.0
        // validVersions is 1-4. This crate speaks 1–4. v5 is a named
        // STATUS hole.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 5, 1, 4), Some(4));
        assert_eq!(crate::protocol::api_keys::pick_version(5, 5, 1, 4), None);
        let mut buf = BytesMut::new();
        let err = encode_describe_log_dirs_request(&mut buf, 5, &DescribeLogDirsRequest::new(None))
            .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v5 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_describe_log_dirs_request(&mut buf, 0, &DescribeLogDirsRequest::new(None))
            .unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        let resp = DescribeLogDirsResponse::new(
            0,
            vec![DescribeLogDirsResult::new(
                0,
                "/d",
                vec![DescribeLogDirsTopic::new(
                    "t",
                    vec![DescribeLogDirsPartition::new(0, 0, 0, false)],
                )],
                -1,
                -1,
            )],
        );
        buf.clear();
        encode_describe_log_dirs_response(&mut buf, 4, &resp).unwrap();
        assert_eq!(
            buf.len(),
            57,
            "v4 one-directory leftover-empty has no extra directory field after UsableBytes"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_log_dirs_response(&mut cur, 4).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "v4 body must be leftover-empty; a later-version directory field would leave leftover"
        );
    }

    #[test]
    fn create_delegation_token_v3_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 38
        // listeners broker + controller. This crate speaks 1–3.
        // v3 fixtures below. Not copied from DescribeLogDirs /
        // AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        // kafka-protocol Default owner is Some(""); null owner is 0x00.
        const REQ_DEFAULT: &[u8] = &[
            0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const REQ_NULL: &[u8] = &[
            0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        const REQ_ONE: &[u8] = &[
            0x00, 0x00, 0x02, 0x05, 0x55, 0x73, 0x65, 0x72, 0x02, 0x72, 0x00, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Leftover-empty
        // body is 37 bytes. Top-level ErrorCode is at bytes 0-1.
        const RESP_64: &[u8] = &[
            0x00, 0x40, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_OK: &[u8] = &[
            0x00, 0x00, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let req_default =
            CreateDelegationTokenRequest::new(Some(String::new()), Some(String::new()), vec![], 0);
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, 3, &req_default).unwrap();
        assert_eq!(&buf[..], REQ_DEFAULT);
        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            3,
            &CreateDelegationTokenRequest::new(None, None, vec![], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_NULL);
        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            3,
            &CreateDelegationTokenRequest::new(
                None,
                None,
                vec![CreatableRenewer::new("User", "r")],
                -1,
            ),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_ONE);
        let resp = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "",
            "",
            "",
            "",
            0,
            0,
            0,
            "",
            vec![],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(&buf[..], RESP_64);
        buf.clear();
        encode_create_delegation_token_response(
            &mut buf,
            3,
            &CreateDelegationTokenResponse::new(0, "", "", "", "", 0, 0, 0, "", vec![]),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_OK);
    }

    #[test]
    fn create_delegation_token_v3_roundtrip_is_leftover_empty() {
        let req = CreateDelegationTokenRequest::new(
            Some("User".into()),
            Some("alice".into()),
            vec![CreatableRenewer::new("User", "r")],
            -1,
        );
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, 3, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur, 3).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 request must be leftover-empty"
        );

        let resp = CreateDelegationTokenResponse::new(
            0,
            "User",
            "u",
            "User",
            "u",
            0,
            0,
            0,
            "tid",
            vec![0xaa],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, 3, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur, 3).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 response must be leftover-empty"
        );

        buf.clear();
        encode_create_delegation_token_request(
            &mut buf,
            3,
            &CreateDelegationTokenRequest::new(None, None, vec![], -1),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur, 3).unwrap(),
            CreateDelegationTokenRequest::new(None, None, vec![], -1)
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 null-owner request must be leftover-empty"
        );
    }

    #[test]
    fn create_delegation_token_top_level_error_code_is_at_bytes_0_1() {
        // Official v3 body: top-level ErrorCode INT16 first, then
        // compact principals, timestamps, compact TokenId, compact
        // Hmac, ThrottleTimeMs INT32 last. Measured independently
        // from Apache CreateDelegationTokenResponse.json and a
        // kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, empty
        // principals / token / hmac, error
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Do not assume
        // bytes 4-5 from DescribeLogDirs / AssignReplicasToDirs /
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // AlterReplicaLogDirs / DescribeProducers first-partition,
        // bytes 27-28 from DescribeTopicPartitions first-partition, or
        // bytes 45-46 from AssignReplicasToDirs first-partition.
        // Official JSON lists no errorCodes; official handler writes
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64) onto the
        // top-level field when allowTokenRequests fails.
        let empty = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "",
            "",
            "",
            "",
            0,
            0,
            0,
            "",
            vec![],
        );
        let mut buf = BytesMut::new();
        encode_create_delegation_token_response(&mut buf, 3, &empty).unwrap();
        assert_eq!(
            buf.len(),
            37,
            "v3 leftover-empty empty-token body is top-level INT16 + empty principals + timestamps + empty token/hmac + throttle + tagged"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode must be the INT16 at bytes 0-1"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not a first-renewer / first-topic field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DeleteGroups / first-directory bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not a first-partition field at bytes 12-13"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur, 3).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 empty-token body must be leftover-empty"
        );

        let resp = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "User",
            "u",
            "User",
            "u",
            0,
            0,
            0,
            "tid",
            vec![0xaa],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(
            buf.len(),
            51,
            "v3 one-token body is top-level INT16 + User/u principals + timestamps + tid + hmac + throttle + tagged"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode stays at bytes 0-1 when token fields are present"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        assert!(
            buf.get(27).is_some(),
            "v3 one-token body reaches DescribeTopicPartitions first-partition bytes 27-28 as timestamp bytes, not ErrorCode"
        );
        let b27 = buf.get(27).copied().unwrap();
        let b28 = buf.get(28).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b27, b28]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at DescribeTopicPartitions first-partition bytes 27-28"
        );
        assert!(
            buf.get(45).is_some(),
            "v3 one-token body reaches AssignReplicasToDirs first-partition bytes 45-46 as hmac/throttle, not ErrorCode"
        );
        let b45 = buf.get(45).copied().unwrap();
        let b46 = buf.get(46).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b45, b46]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not at AssignReplicasToDirs first-partition bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_response(&mut cur, 3).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "CreateDelegationToken v3 one-token body must be leftover-empty"
        );
    }

    #[test]
    fn create_delegation_token_v1_is_classic_v2_omits_owner() {
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V1_ONE: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x55, 0x73, 0x65, 0x72, 0x00, 0x01, 0x72, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V2: &[u8] = &[0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        const REQ_V2_ONE: &[u8] = &[
            0x02, 0x05, 0x55, 0x73, 0x65, 0x72, 0x02, 0x72, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x00,
        ];
        const RESP_V1_64: &[u8] = &[
            0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const RESP_V2_64: &[u8] = &[
            0x00, 0x40, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let owned = CreateDelegationTokenRequest::new(
            Some("User".into()),
            Some("alice".into()),
            vec![],
            -1,
        );
        let empty = CreateDelegationTokenRequest::new(None, None, vec![], -1);
        let one = CreateDelegationTokenRequest::new(
            None,
            None,
            vec![CreatableRenewer::new("User", "r")],
            -1,
        );
        let mut buf = BytesMut::new();
        encode_create_delegation_token_request(&mut buf, 1, &owned).unwrap();
        assert_eq!(&buf[..], REQ_V1, "v1 omits owner");
        assert_eq!(
            buf.len(),
            12,
            "v1 leftover-empty empty-renewers is 12 bytes"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur, 1).unwrap(),
            empty,
            "v1 decode fills owner None"
        );
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        buf.clear();
        encode_create_delegation_token_request(&mut buf, 1, &empty).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        buf.clear();
        encode_create_delegation_token_request(&mut buf, 1, &one).unwrap();
        assert_eq!(&buf[..], REQ_V1_ONE);
        buf.clear();
        encode_create_delegation_token_request(&mut buf, 2, &owned).unwrap();
        assert_eq!(&buf[..], REQ_V2, "v2 omits owner");
        assert_eq!(
            buf.len(),
            10,
            "v2 leftover-empty empty-renewers is 10 bytes"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur, 2).unwrap(),
            empty,
            "v2 decode fills owner None"
        );
        assert!(!cur.has_remaining(), "v2 request leftover-empty");
        buf.clear();
        encode_create_delegation_token_request(&mut buf, 2, &one).unwrap();
        assert_eq!(&buf[..], REQ_V2_ONE);
        let resp = CreateDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "",
            "",
            "User",
            "u",
            0,
            0,
            0,
            "",
            vec![],
        );
        buf.clear();
        encode_create_delegation_token_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V1_64, "v1 omits requester");
        assert_eq!(buf.len(), 40, "v1 leftover-empty empty-token is 40 bytes");
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v1 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v1 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v1 i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        let got = decode_create_delegation_token_response(&mut cur, 1).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(
            got.token_requester_principal_type, "",
            "v1 decode fills requester empty"
        );
        assert_eq!(got.token_requester_principal_name, "");
        buf.clear();
        encode_create_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V2_64, "v2 omits requester");
        assert_eq!(buf.len(), 35, "v2 leftover-empty empty-token is 35 bytes");
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v2 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v2 i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        let got = decode_create_delegation_token_response(&mut cur, 2).unwrap();
        assert!(!cur.has_remaining());
        assert_eq!(
            got.token_requester_principal_type, "",
            "v2 decode fills requester empty"
        );
        assert_eq!(got.token_requester_principal_name, "");
        assert_eq!(crate::protocol::api_keys::pick_version(1, 3, 1, 3), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 3), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 3), Some(1));
        assert_ne!(REQ_V1, REQ_V2, "v2 must use compact arrays");
    }

    #[test]
    fn create_delegation_token_does_not_speak_v0() {
        // kafka-protocol 0.18.0 VERSIONS.min = 1, VERSIONS.max = 3.
        // Kafka 4.0 validVersions is 1-3. This crate speaks 1–3.
        // Official 3.9.1 lists deprecated v0; that version is not
        // encoded. Official trunk removed v0. v4+ is not spoken.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 3, 1, 3), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 3), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 3), None);
        assert_eq!(crate::protocol::api_keys::pick_version(4, 4, 1, 3), None);
        let req = CreateDelegationTokenRequest::new(None, None, vec![], -1);
        let mut buf = BytesMut::new();
        let err = encode_create_delegation_token_request(&mut buf, 0, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_create_delegation_token_request(&mut buf, 4, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v4 is not spoken, got {err}"
        );
        buf.clear();
        encode_create_delegation_token_request(&mut buf, 3, &req).unwrap();
        assert_eq!(
            buf.len(),
            12,
            "v3 leftover-empty null-owner request has no extra field after MaxLifetimeMs"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_create_delegation_token_request(&mut cur, 3).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "v3 request must be leftover-empty; a later-version field would leave leftover"
        );
    }

    #[test]
    fn renew_delegation_token_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 39
        // listeners broker + controller. This crate speaks 1–2.
        // Not copied from CreateDelegationToken
        // (top-level ErrorCode at bytes 0-1 on a 37-byte empty-token
        // body), DescribeLogDirs / AssignReplicasToDirs / PushTelemetry
        // / GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        const REQ_EMPTY: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const REQ_NULL_PERIOD: &[u8] =
            &[0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        const REQ_ONE: &[u8] = &[
            0x02, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Leftover-empty
        // body is 15 bytes. Top-level ErrorCode is at bytes 0-1.
        const RESP_64: &[u8] = &[
            0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        const RESP_OK: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        let mut buf = BytesMut::new();
        encode_renew_delegation_token_request(
            &mut buf,
            2,
            &RenewDelegationTokenRequest::new(vec![], 0),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_EMPTY);
        buf.clear();
        encode_renew_delegation_token_request(
            &mut buf,
            2,
            &RenewDelegationTokenRequest::new(vec![], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_NULL_PERIOD);
        buf.clear();
        encode_renew_delegation_token_request(
            &mut buf,
            2,
            &RenewDelegationTokenRequest::new(vec![0xaa], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_ONE);
        let resp = RenewDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        buf.clear();
        encode_renew_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_64);
        buf.clear();
        encode_renew_delegation_token_response(
            &mut buf,
            2,
            &RenewDelegationTokenResponse::new(0, 0),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_OK);
    }

    #[test]
    fn renew_delegation_token_v2_roundtrip_is_leftover_empty() {
        let req = RenewDelegationTokenRequest::new(vec![0xaa, 0xbb], -1);
        let mut buf = BytesMut::new();
        encode_renew_delegation_token_request(&mut buf, 2, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_request(&mut cur, 2).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "RenewDelegationToken v2 request must be leftover-empty"
        );

        let resp = RenewDelegationTokenResponse::new(0, 1_700_000_000_000);
        buf.clear();
        encode_renew_delegation_token_response(&mut buf, 2, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "RenewDelegationToken v2 response must be leftover-empty"
        );

        buf.clear();
        encode_renew_delegation_token_request(
            &mut buf,
            2,
            &RenewDelegationTokenRequest::new(vec![], -1),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_request(&mut cur, 2).unwrap(),
            RenewDelegationTokenRequest::new(vec![], -1)
        );
        assert!(
            !cur.has_remaining(),
            "RenewDelegationToken v2 empty-hmac request must be leftover-empty"
        );
    }

    #[test]
    fn renew_delegation_token_top_level_error_code_is_at_bytes_0_1() {
        // Official v2 body: top-level ErrorCode INT16 first, then
        // ExpiryTimestampMs INT64, ThrottleTimeMs INT32 last. Measured
        // independently from Apache RenewDelegationTokenResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, expiry 0,
        // error DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Do not
        // assume bytes 0-1 from CreateDelegationToken (different
        // response, 37-byte empty-token body), bytes 4-5 from
        // DescribeLogDirs / AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups,
        // bytes 5-6 from DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups / ConsumerGroupDescribe, bytes 7-8 from
        // DeleteGroups after GroupId, bytes 8-9 from
        // DescribeShareGroupOffsets first-group, bytes 12-13 from
        // AlterReplicaLogDirs / DescribeProducers first-partition,
        // bytes 27-28 from DescribeTopicPartitions first-partition, or
        // bytes 45-46 from AssignReplicasToDirs first-partition.
        // Official JSON lists no errorCodes; official handler writes
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64) onto the
        // top-level field when allowTokenRequests fails.
        let empty = RenewDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        let mut buf = BytesMut::new();
        encode_renew_delegation_token_response(&mut buf, 2, &empty).unwrap();
        assert_eq!(
            buf.len(),
            15,
            "v2 leftover-empty body is top-level INT16 + expiry INT64 + throttle INT32 + tagged"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode must be the INT16 at bytes 0-1"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not after throttle at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not a first-renewer / first-topic field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not at DeleteGroups / first-directory bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not a first-partition field at bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v2 leftover-empty body is 15 bytes and does not reach bytes 27-28"
        );
        assert!(
            buf.get(45).is_none(),
            "v2 leftover-empty body is 15 bytes and does not reach bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_response(&mut cur, 2).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "RenewDelegationToken v2 empty-expiry body must be leftover-empty"
        );

        let resp = RenewDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            1_700_000_000_000,
        );
        buf.clear();
        encode_renew_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(
            buf.len(),
            15,
            "v2 body stays 15 bytes when expiry is non-zero"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode stays at bytes 0-1 when expiry is present"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not after throttle at bytes 4-5"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "RenewDelegationToken v2 non-zero-expiry body must be leftover-empty"
        );
    }

    #[test]
    fn renew_delegation_token_v1_is_classic() {
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V1_ONE: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V2: &[u8] = &[0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        const RESP_V1_64: &[u8] = &[
            0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let req = RenewDelegationTokenRequest::new(vec![], -1);
        let one = RenewDelegationTokenRequest::new(vec![0xaa], -1);
        let mut buf = BytesMut::new();
        encode_renew_delegation_token_request(&mut buf, 1, &req).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        assert_eq!(buf.len(), 12, "v1 leftover-empty empty-hmac is 12 bytes");
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_request(&mut cur, 1).unwrap(),
            req
        );
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        buf.clear();
        encode_renew_delegation_token_request(&mut buf, 1, &one).unwrap();
        assert_eq!(&buf[..], REQ_V1_ONE);
        buf.clear();
        encode_renew_delegation_token_request(&mut buf, 2, &req).unwrap();
        assert_eq!(&buf[..], REQ_V2, "v2 must use compact bytes");
        assert_eq!(buf.len(), 10, "v2 leftover-empty empty-hmac is 10 bytes");
        let resp = RenewDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        buf.clear();
        encode_renew_delegation_token_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V1_64);
        assert_eq!(buf.len(), 14, "v1 leftover-empty is 14 bytes");
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v1 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v1 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v1 i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(!cur.has_remaining(), "v1 response leftover-empty");
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 2), Some(1));
        assert_ne!(REQ_V1, REQ_V2, "v2 must use compact bytes");
    }

    #[test]
    fn renew_delegation_token_does_not_speak_v0() {
        // kafka-protocol 0.18.0 VERSIONS.min = 1, VERSIONS.max = 2.
        // Kafka 4.0 validVersions is 1-2. This crate speaks 1–2.
        // Official 3.9.1 lists deprecated v0; that version is not
        // encoded. Official trunk removed v0. v3+ is not spoken.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 2), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 2), None);
        assert_eq!(crate::protocol::api_keys::pick_version(3, 3, 1, 2), None);
        let req = RenewDelegationTokenRequest::new(vec![], -1);
        let mut buf = BytesMut::new();
        let err = encode_renew_delegation_token_request(&mut buf, 0, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_renew_delegation_token_request(&mut buf, 3, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3 is not spoken, got {err}"
        );
        buf.clear();
        encode_renew_delegation_token_request(&mut buf, 2, &req).unwrap();
        assert_eq!(
            buf.len(),
            10,
            "v2 leftover-empty empty-hmac request has no extra field after RenewPeriodMs"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_renew_delegation_token_request(&mut cur, 2).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "v2 request must be leftover-empty; a later-version field would leave leftover"
        );
    }

    #[test]
    fn expire_delegation_token_v2_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 40
        // listeners broker + controller. This crate speaks 1–2.
        // Not copied from RenewDelegationToken
        // (sibling API; independently measured leftover-empty 15-byte
        // body) or CreateDelegationToken (top-level ErrorCode at
        // bytes 0-1 on a 37-byte empty-token body), DescribeLogDirs /
        // AssignReplicasToDirs / PushTelemetry /
        // GetTelemetrySubscriptions / ListConfigResources / ListGroups
        // (top-level ErrorCode at bytes 4-5),
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // (first-topic / first-group ErrorCode at bytes 5-6),
        // DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        const REQ_EMPTY: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const REQ_NULL_PERIOD: &[u8] =
            &[0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        const REQ_ONE: &[u8] = &[
            0x02, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Leftover-empty
        // body is 15 bytes. Top-level ErrorCode is at bytes 0-1.
        const RESP_64: &[u8] = &[
            0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        const RESP_OK: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
        ];
        let mut buf = BytesMut::new();
        encode_expire_delegation_token_request(
            &mut buf,
            2,
            &ExpireDelegationTokenRequest::new(vec![], 0),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_EMPTY);
        buf.clear();
        encode_expire_delegation_token_request(
            &mut buf,
            2,
            &ExpireDelegationTokenRequest::new(vec![], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_NULL_PERIOD);
        buf.clear();
        encode_expire_delegation_token_request(
            &mut buf,
            2,
            &ExpireDelegationTokenRequest::new(vec![0xaa], -1),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_ONE);
        let resp = ExpireDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        buf.clear();
        encode_expire_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(&buf[..], RESP_64);
        buf.clear();
        encode_expire_delegation_token_response(
            &mut buf,
            2,
            &ExpireDelegationTokenResponse::new(0, 0),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_OK);
    }

    #[test]
    fn expire_delegation_token_v2_roundtrip_is_leftover_empty() {
        let req = ExpireDelegationTokenRequest::new(vec![0xaa, 0xbb], -1);
        let mut buf = BytesMut::new();
        encode_expire_delegation_token_request(&mut buf, 2, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_request(&mut cur, 2).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "ExpireDelegationToken v2 request must be leftover-empty"
        );

        let resp = ExpireDelegationTokenResponse::new(0, 1_700_000_000_000);
        buf.clear();
        encode_expire_delegation_token_response(&mut buf, 2, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ExpireDelegationToken v2 response must be leftover-empty"
        );

        buf.clear();
        encode_expire_delegation_token_request(
            &mut buf,
            2,
            &ExpireDelegationTokenRequest::new(vec![], -1),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_request(&mut cur, 2).unwrap(),
            ExpireDelegationTokenRequest::new(vec![], -1)
        );
        assert!(
            !cur.has_remaining(),
            "ExpireDelegationToken v2 empty-hmac request must be leftover-empty"
        );
    }

    #[test]
    fn expire_delegation_token_top_level_error_code_is_at_bytes_0_1() {
        // Official v2 body: top-level ErrorCode INT16 first, then
        // ExpiryTimestampMs INT64, ThrottleTimeMs INT32 last. Measured
        // independently from Apache ExpireDelegationTokenResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, expiry 0,
        // error DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Do not
        // assume bytes 0-1 from CreateDelegationToken (different
        // response, 37-byte empty-token body) or RenewDelegationToken
        // (sibling API, independently measured). Do not assume bytes
        // 4-5 from DescribeLogDirs / AssignReplicasToDirs /
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // / ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, bytes 8-9 from DescribeShareGroupOffsets
        // first-group, bytes 12-13 from AlterReplicaLogDirs /
        // DescribeProducers first-partition, bytes 27-28 from
        // DescribeTopicPartitions first-partition, or bytes 45-46 from
        // AssignReplicasToDirs first-partition. Official JSON lists
        // no errorCodes; official handler writes
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64) onto the
        // top-level field when allowTokenRequests fails.
        let empty = ExpireDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        let mut buf = BytesMut::new();
        encode_expire_delegation_token_response(&mut buf, 2, &empty).unwrap();
        assert_eq!(
            buf.len(),
            15,
            "v2 leftover-empty body is top-level INT16 + expiry INT64 + throttle INT32 + tagged"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode must be the INT16 at bytes 0-1"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not after throttle at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not a first-renewer / first-topic field at bytes 5-6"
        );
        let b7 = buf.get(7).copied().unwrap();
        let b8 = buf.get(8).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b7, b8]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not at DeleteGroups / first-directory bytes 7-8"
        );
        let b8b = buf.get(8).copied().unwrap();
        let b9 = buf.get(9).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b8b, b9]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not at DescribeShareGroupOffsets first-group bytes 8-9"
        );
        let b12 = buf.get(12).copied().unwrap();
        let b13 = buf.get(13).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b12, b13]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not a first-partition field at bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v2 leftover-empty body is 15 bytes and does not reach bytes 27-28"
        );
        assert!(
            buf.get(45).is_none(),
            "v2 leftover-empty body is 15 bytes and does not reach bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_response(&mut cur, 2).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "ExpireDelegationToken v2 empty-expiry body must be leftover-empty"
        );

        let resp = ExpireDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            1_700_000_000_000,
        );
        buf.clear();
        encode_expire_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(
            buf.len(),
            15,
            "v2 body stays 15 bytes when expiry is non-zero"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode stays at bytes 0-1 when expiry is present"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 ErrorCode is not after throttle at bytes 4-5"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_response(&mut cur, 2).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ExpireDelegationToken v2 non-zero-expiry body must be leftover-empty"
        );
    }

    #[test]
    fn expire_delegation_token_v1_is_classic() {
        const REQ_V1: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V1_ONE: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0xaa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        const REQ_V2: &[u8] = &[0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        const RESP_V1_64: &[u8] = &[
            0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let req = ExpireDelegationTokenRequest::new(vec![], -1);
        let one = ExpireDelegationTokenRequest::new(vec![0xaa], -1);
        let mut buf = BytesMut::new();
        encode_expire_delegation_token_request(&mut buf, 1, &req).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        assert_eq!(buf.len(), 12, "v1 leftover-empty empty-hmac is 12 bytes");
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_request(&mut cur, 1).unwrap(),
            req
        );
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        buf.clear();
        encode_expire_delegation_token_request(&mut buf, 1, &one).unwrap();
        assert_eq!(&buf[..], REQ_V1_ONE);
        buf.clear();
        encode_expire_delegation_token_request(&mut buf, 2, &req).unwrap();
        assert_eq!(&buf[..], REQ_V2, "v2 must use compact bytes");
        assert_eq!(buf.len(), 10, "v2 leftover-empty empty-hmac is 10 bytes");
        let resp = ExpireDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            0,
        );
        buf.clear();
        encode_expire_delegation_token_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(&buf[..], RESP_V1_64);
        assert_eq!(buf.len(), 14, "v1 leftover-empty is 14 bytes");
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v1 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v1 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v1 i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(!cur.has_remaining(), "v1 response leftover-empty");
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 2), Some(1));
        assert_ne!(REQ_V1, REQ_V2, "v2 must use compact bytes");
    }

    #[test]
    fn expire_delegation_token_does_not_speak_v0() {
        // kafka-protocol 0.18.0 VERSIONS.min = 1, VERSIONS.max = 2.
        // Kafka 4.0 validVersions is 1-2. This crate speaks 1–2.
        // Official 3.9.1 lists deprecated v0; that version is not
        // encoded. Official trunk removed v0. v3+ is not spoken.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 2), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 2), None);
        assert_eq!(crate::protocol::api_keys::pick_version(3, 3, 1, 2), None);
        let req = ExpireDelegationTokenRequest::new(vec![], -1);
        let mut buf = BytesMut::new();
        let err = encode_expire_delegation_token_request(&mut buf, 0, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_expire_delegation_token_request(&mut buf, 3, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3 is not spoken, got {err}"
        );
        buf.clear();
        encode_expire_delegation_token_request(&mut buf, 2, &req).unwrap();
        assert_eq!(
            buf.len(),
            10,
            "v2 leftover-empty empty-hmac request has no extra field after ExpiryTimePeriodMs"
        );
        let mut cur = &buf[..];
        assert_eq!(
            decode_expire_delegation_token_request(&mut cur, 2).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "v2 request must be leftover-empty; a later-version field would leave leftover"
        );
    }

    #[test]
    fn describe_delegation_token_v3_matches_kafka_protocol_0_18() {
        // Independent encode from kafka-protocol 0.18.0 (client encodes
        // the request; broker encodes the response). Apache JSON api 41
        // listeners broker + controller. This crate speaks 1–3.
        // Not copied from ExpireDelegationToken
        // (sibling API; independently measured leftover-empty 15-byte
        // body), RenewDelegationToken, or CreateDelegationToken
        // (top-level ErrorCode at bytes 0-1 on a 37-byte empty-token
        // body), DescribeLogDirs / AssignReplicasToDirs /
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups (top-level ErrorCode at
        // bytes 4-5), DescribeTopicPartitions / ShareGroupDescribe /
        // DescribeGroups (first-topic / first-group ErrorCode at
        // bytes 5-6), DeleteGroups (after GroupId at bytes 7-8),
        // DescribeShareGroupOffsets (first-group after GroupId and
        // Topics at bytes 8-9), AlterReplicaLogDirs / DescribeProducers
        // (first-partition ErrorCode at bytes 12-13), or
        // DescribeTopicPartitions first-partition (bytes 27-28).
        // kafka-protocol Default owners is Some([]); null owners is 0x00.
        const REQ_DEFAULT: &[u8] = &[0x01, 0x00];
        const REQ_NULL: &[u8] = &[0x00, 0x00];
        const REQ_ONE: &[u8] = &[0x02, 0x05, 0x55, 0x73, 0x65, 0x72, 0x02, 0x72, 0x00, 0x00];
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Leftover-empty
        // (empty Tokens) body is 8 bytes. Top-level ErrorCode is at
        // bytes 0-1.
        const RESP_64: &[u8] = &[0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_OK: &[u8] = &[0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];
        let req_default = DescribeDelegationTokenRequest::new(Some(vec![]));
        let mut buf = BytesMut::new();
        encode_describe_delegation_token_request(&mut buf, 3, &req_default).unwrap();
        assert_eq!(&buf[..], REQ_DEFAULT);
        buf.clear();
        encode_describe_delegation_token_request(
            &mut buf,
            3,
            &DescribeDelegationTokenRequest::new(None),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_NULL);
        buf.clear();
        encode_describe_delegation_token_request(
            &mut buf,
            3,
            &DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
                "User", "r",
            )])),
        )
        .unwrap();
        assert_eq!(&buf[..], REQ_ONE);
        let resp = DescribeDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            vec![],
        );
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(&buf[..], RESP_64);
        buf.clear();
        encode_describe_delegation_token_response(
            &mut buf,
            3,
            &DescribeDelegationTokenResponse::new(0, vec![]),
        )
        .unwrap();
        assert_eq!(&buf[..], RESP_OK);
    }

    #[test]
    fn describe_delegation_token_v3_roundtrip_is_leftover_empty() {
        let req =
            DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
                "User", "alice",
            )]));
        let mut buf = BytesMut::new();
        encode_describe_delegation_token_request(&mut buf, 3, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 3).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "DescribeDelegationToken v3 request must be leftover-empty"
        );

        let resp = DescribeDelegationTokenResponse::new(
            0,
            vec![DescribedDelegationToken::new(
                "User",
                "u",
                "User",
                "u",
                0,
                0,
                0,
                "tid",
                vec![0xaa],
                vec![DescribedDelegationTokenRenewer::new("User", "r")],
            )],
        );
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 3, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_delegation_token_response(&mut cur, 3).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeDelegationToken v3 response must be leftover-empty"
        );

        buf.clear();
        encode_describe_delegation_token_request(
            &mut buf,
            3,
            &DescribeDelegationTokenRequest::new(None),
        )
        .unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 3).unwrap(),
            DescribeDelegationTokenRequest::new(None)
        );
        assert!(
            !cur.has_remaining(),
            "DescribeDelegationToken v3 null-owners request must be leftover-empty"
        );
    }

    #[test]
    fn describe_delegation_token_top_level_error_code_is_at_bytes_0_1() {
        // Official v3 body: top-level ErrorCode INT16 first, then
        // compact Tokens, ThrottleTimeMs INT32 last. Measured
        // independently from Apache DescribeDelegationTokenResponse.json
        // and a kafka-protocol 0.18.0 broker encode (`features =
        // ["broker"]`) on leftover-empty fixture throttle 0, empty
        // Tokens, error DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). Do
        // not assume bytes 0-1 from CreateDelegationToken (different
        // response, 37-byte empty-token body), RenewDelegationToken,
        // or ExpireDelegationToken (sibling API, independently
        // measured 15-byte leftover-empty body). Do not assume bytes
        // 4-5 from DescribeLogDirs / AssignReplicasToDirs /
        // PushTelemetry / GetTelemetrySubscriptions /
        // ListConfigResources / ListGroups, bytes 5-6 from
        // DescribeTopicPartitions / ShareGroupDescribe / DescribeGroups
        // / ConsumerGroupDescribe, bytes 7-8 from DeleteGroups after
        // GroupId, bytes 8-9 from DescribeShareGroupOffsets
        // first-group, bytes 12-13 from AlterReplicaLogDirs /
        // DescribeProducers first-partition, bytes 27-28 from
        // DescribeTopicPartitions first-partition, or bytes 45-46 from
        // AssignReplicasToDirs first-partition. Official JSON lists
        // no errorCodes; official handler writes
        // DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64) onto the
        // top-level field when allowTokenRequests fails.
        let empty = DescribeDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            vec![],
        );
        let mut buf = BytesMut::new();
        encode_describe_delegation_token_response(&mut buf, 3, &empty).unwrap();
        assert_eq!(
            buf.len(),
            8,
            "v3 leftover-empty body is top-level INT16 + empty Tokens + throttle INT32 + tagged"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode must be the INT16 at bytes 0-1"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        let b5b = buf.get(5).copied().unwrap();
        let b6 = buf.get(6).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b5b, b6]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not a first-renewer / first-topic field at bytes 5-6"
        );
        assert!(
            buf.get(8).is_none(),
            "v3 leftover-empty body is 8 bytes and does not reach DeleteGroups / first-directory bytes 7-8 as a pair, nor bytes 8-9"
        );
        assert!(
            buf.get(12).is_none(),
            "v3 leftover-empty body is 8 bytes and does not reach bytes 12-13"
        );
        assert!(
            buf.get(27).is_none(),
            "v3 leftover-empty body is 8 bytes and does not reach bytes 27-28"
        );
        assert!(
            buf.get(45).is_none(),
            "v3 leftover-empty body is 8 bytes and does not reach bytes 45-46"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_delegation_token_response(&mut cur, 3).unwrap(),
            empty
        );
        assert!(
            !cur.has_remaining(),
            "DescribeDelegationToken v3 empty-tokens body must be leftover-empty"
        );

        let resp = DescribeDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            vec![DescribedDelegationToken::new(
                "",
                "",
                "",
                "",
                0,
                0,
                0,
                "",
                vec![],
                vec![],
            )],
        );
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 3, &resp).unwrap();
        assert_eq!(
            buf.len(),
            40,
            "v3 one-default-token body stays leftover-empty at 40 bytes"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 top-level ErrorCode stays at bytes 0-1 when a token is present"
        );
        let b4 = buf.get(4).copied().unwrap();
        let b5 = buf.get(5).copied().unwrap();
        assert_ne!(
            i16::from_be_bytes([b4, b5]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v3 ErrorCode is not after throttle at bytes 4-5"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "i16=64 hits only at byte 0");
        let mut cur = &buf[..];
        assert_eq!(
            decode_describe_delegation_token_response(&mut cur, 3).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "DescribeDelegationToken v3 one-token body must be leftover-empty"
        );
    }

    #[test]
    fn describe_delegation_token_v1_is_classic_v2_omits_requester() {
        const REQ_V1_EMPTY: &[u8] = &[0x00, 0x00, 0x00, 0x00];
        const REQ_V1_NULL: &[u8] = &[0xff, 0xff, 0xff, 0xff];
        const REQ_V1_ONE: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x55, 0x73, 0x65, 0x72, 0x00, 0x01, 0x72,
        ];
        const REQ_V2_EMPTY: &[u8] = &[0x01, 0x00];
        const REQ_V2_NULL: &[u8] = &[0x00, 0x00];
        const REQ_V2_ONE: &[u8] = &[0x02, 0x05, 0x55, 0x73, 0x65, 0x72, 0x02, 0x72, 0x00, 0x00];
        const RESP_V1_64: &[u8] = &[0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        const RESP_V2_64: &[u8] = &[0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];
        let empty = DescribeDelegationTokenRequest::new(Some(vec![]));
        let null = DescribeDelegationTokenRequest::new(None);
        let one =
            DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
                "User", "r",
            )]));
        let mut buf = BytesMut::new();
        encode_describe_delegation_token_request(&mut buf, 1, &empty).unwrap();
        assert_eq!(buf.as_ref(), REQ_V1_EMPTY);
        assert_eq!(buf.len(), 4, "v1 leftover-empty empty-owners is 4 bytes");
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 1).unwrap(),
            empty
        );
        assert!(!cur.has_remaining(), "v1 empty-owners leftover-empty");
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 1, &null).unwrap();
        assert_eq!(buf.as_ref(), REQ_V1_NULL);
        assert_eq!(buf.len(), 4, "v1 leftover-empty null-owners is 4 bytes");
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 1).unwrap(),
            null
        );
        assert!(!cur.has_remaining(), "v1 null-owners leftover-empty");
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 1, &one).unwrap();
        assert_eq!(buf.as_ref(), REQ_V1_ONE);
        assert_eq!(buf.len(), 13, "v1 leftover-empty one-owner is 13 bytes");
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 1).unwrap(),
            one
        );
        assert!(!cur.has_remaining(), "v1 one-owner leftover-empty");
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 2, &empty).unwrap();
        assert_eq!(buf.as_ref(), REQ_V2_EMPTY);
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 2).unwrap(),
            empty
        );
        assert!(!cur.has_remaining(), "v2 empty-owners leftover-empty");
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 2, &null).unwrap();
        assert_eq!(buf.as_ref(), REQ_V2_NULL);
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 2).unwrap(),
            null
        );
        assert!(!cur.has_remaining(), "v2 null-owners leftover-empty");
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 2, &one).unwrap();
        assert_eq!(buf.as_ref(), REQ_V2_ONE);
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 2).unwrap(),
            one
        );
        assert!(!cur.has_remaining(), "v2 one-owner leftover-empty");

        let empty_resp = DescribeDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            vec![],
        );
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 1, &empty_resp).unwrap();
        assert_eq!(buf.as_ref(), RESP_V1_64);
        assert_eq!(
            buf.len(),
            10,
            "v1 leftover-empty empty-Tokens error 64 is 10 bytes"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v1 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v1 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v1 i16=64 hits only at byte 0");
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_response(&mut cur, 1).unwrap(),
            empty_resp
        );
        assert!(!cur.has_remaining(), "v1 empty-Tokens leftover-empty");
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 2, &empty_resp).unwrap();
        assert_eq!(buf.as_ref(), RESP_V2_64);
        assert_eq!(
            buf.len(),
            8,
            "v2 leftover-empty empty-Tokens error 64 is 8 bytes"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode is the INT16 at bytes 0-1"
        );
        let mut hits = 0u32;
        if buf.len() >= 2 {
            let end = buf.len().saturating_sub(1);
            let mut i = 0usize;
            while i < end {
                let lo = buf.get(i).copied().unwrap();
                let hi = buf.get(i.saturating_add(1)).copied().unwrap();
                if i16::from_be_bytes([lo, hi])
                    == crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                {
                    hits = hits.saturating_add(1);
                    assert_eq!(i, 0, "v2 i16=64 must hit only at byte 0");
                }
                i = i.saturating_add(1);
            }
        }
        assert_eq!(hits, 1, "v2 i16=64 hits only at byte 0");
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_response(&mut cur, 2).unwrap(),
            empty_resp
        );
        assert!(!cur.has_remaining(), "v2 empty-Tokens leftover-empty");

        let token =
            DescribedDelegationToken::new("", "", "User", "alice", 0, 0, 0, "", vec![], vec![]);
        let resp = DescribeDelegationTokenResponse::new(
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            vec![token],
        );
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 1, &resp).unwrap();
        assert_eq!(
            buf.len(),
            48,
            "v1 leftover-empty one-default-token is 48 bytes"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v1 top-level ErrorCode stays at bytes 0-1 when a token is present"
        );
        let mut cur = buf.as_ref();
        let got = decode_describe_delegation_token_response(&mut cur, 1).unwrap();
        assert!(!cur.has_remaining(), "v1 one-token leftover-empty");
        assert_eq!(
            got.tokens[0].token_requester_principal_type, "",
            "v1 decode fills requester empty"
        );
        assert_eq!(got.tokens[0].token_requester_principal_name, "");
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 2, &resp).unwrap();
        assert_eq!(
            buf.len(),
            38,
            "v2 leftover-empty one-default-token is 38 bytes"
        );
        let b0 = buf.first().copied().unwrap();
        let b1 = buf.get(1).copied().unwrap();
        assert_eq!(
            i16::from_be_bytes([b0, b1]),
            crate::error::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "v2 top-level ErrorCode stays at bytes 0-1 when a token is present"
        );
        let mut cur = buf.as_ref();
        let got = decode_describe_delegation_token_response(&mut cur, 2).unwrap();
        assert!(!cur.has_remaining(), "v2 one-token leftover-empty");
        assert_eq!(
            got.tokens[0].token_requester_principal_type, "",
            "v2 decode fills requester empty"
        );
        assert_eq!(got.tokens[0].token_requester_principal_name, "");
        buf.clear();
        encode_describe_delegation_token_response(&mut buf, 3, &resp).unwrap();
        let mut cur = buf.as_ref();
        let got = decode_describe_delegation_token_response(&mut cur, 3).unwrap();
        assert!(!cur.has_remaining(), "v3 one-token leftover-empty");
        assert_eq!(got.tokens[0].token_requester_principal_type, "User");
        assert_eq!(got.tokens[0].token_requester_principal_name, "alice");
    }

    #[test]
    fn describe_delegation_token_does_not_speak_v0() {
        // kafka-protocol 0.18.0 VERSIONS.min = 1, VERSIONS.max = 3.
        // Kafka 4.0 validVersions is 1-3. This crate speaks 1–3.
        // Official 3.9.1 lists deprecated v0; that version is not
        // encoded. Official trunk removed v0. v4+ is not spoken.
        assert_eq!(crate::protocol::api_keys::pick_version(1, 3, 1, 3), Some(3));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 2, 1, 3), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(1, 1, 1, 3), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 1, 3), None);
        assert_eq!(crate::protocol::api_keys::pick_version(4, 4, 1, 3), None);
        let req = DescribeDelegationTokenRequest::new(None);
        let mut buf = BytesMut::new();
        let err = encode_describe_delegation_token_request(&mut buf, 0, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v0 is not spoken, got {err}"
        );
        buf.clear();
        let err = encode_describe_delegation_token_request(&mut buf, 4, &req).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v4 is not spoken, got {err}"
        );
        buf.clear();
        encode_describe_delegation_token_request(&mut buf, 3, &req).unwrap();
        assert_eq!(
            buf.len(),
            2,
            "v3 leftover-empty null-owners request has no extra field after Owners"
        );
        let mut cur = buf.as_ref();
        assert_eq!(
            decode_describe_delegation_token_request(&mut cur, 3).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "v3 request must be leftover-empty; a later-version field would leave leftover"
        );
    }
}

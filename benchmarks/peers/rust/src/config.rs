use rdkafka::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bootstrap: String,
    pub topic: String,
    pub count: u64,
    pub warmup: u64,
    pub payload_bytes: usize,
    pub partitions: i32,
    pub acks: i32,
    pub linger_ms: u64,
    pub batch_size_bytes: u64,
    pub batch_num_messages: u64,
    pub max_in_flight: u64,
    pub queue_max_messages: u64,
    pub queue_max_kbytes: u64,
    pub delivery_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub run_timeout_ms: u64,
    pub consume_timeout_ms: u64,
    pub record_seed: u64,
    pub latency_samples: u64,
    pub idempotence: bool,
    pub compression: String,
    pub isolation_level: String,
    pub payload_mode: String,
    pub key_mode: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.bootstrap.is_empty() || self.topic.is_empty() || self.topic.len() > 249 {
            return Err("nonempty bootstrap and topic (<=249 bytes) required".into());
        }
        if !(1..=1_000_000_000).contains(&self.count)
            || self.warmup > 1_000_000_000
            || self.payload_bytes > 10_000_000
            || !(1..=10000).contains(&self.partitions)
            || ![-1, 0, 1].contains(&self.acks)
        {
            return Err("invalid bounded workload or acks".into());
        }
        if [
            self.batch_size_bytes,
            self.batch_num_messages,
            self.max_in_flight,
            self.queue_max_messages,
            self.queue_max_kbytes,
            self.delivery_timeout_ms,
            self.flush_timeout_ms,
            self.run_timeout_ms,
            self.consume_timeout_ms,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 1_000_000_000)
            || self.linger_ms > 1_000_000_000
        {
            return Err("positive bounded queue, batch, and deadline limits required".into());
        }
        if self.idempotence && (self.acks != -1 || self.max_in_flight > 5) {
            return Err(
                "idempotence requires acks=-1 and max_in_flight<=5; no implicit adjustment".into(),
            );
        }
        if !["none", "gzip", "snappy", "lz4", "zstd"].contains(&self.compression.as_str())
            || !["read_uncommitted", "read_committed"].contains(&self.isolation_level.as_str())
            || !["seeded", "constant-x"].contains(&self.payload_mode.as_str())
            || !["id", "none"].contains(&self.key_mode.as_str())
            || (self.key_mode == "none" && self.payload_mode != "constant-x")
            || !["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
                .contains(&self.security_protocol.as_str())
            || !["", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
                .contains(&self.sasl_mechanism.as_str())
        {
            return Err(
                "unsupported payload/key/security/compression/isolation configuration".into(),
            );
        }
        Ok(())
    }

    pub fn client(&self, consumer: bool) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("client.id", "rust-rdkafka-comparison-peer")
            .set("socket.nagle.disable", "true")
            .set("allow.auto.create.topics", "false")
            .set("log_level", "0")
            .set("security.protocol", &self.security_protocol);
        for (var, key) in [
            ("TLS_CA_PEM", "ssl.ca.location"),
            ("TLS_CLIENT_CERT_PEM", "ssl.certificate.location"),
            ("TLS_CLIENT_KEY_PEM", "ssl.key.location"),
            ("SASL_USERNAME", "sasl.username"),
            ("SASL_PASSWORD", "sasl.password"),
        ] {
            if let Ok(value) = std::env::var(var) {
                config.set(key, value);
            }
        }
        if !self.sasl_mechanism.is_empty() {
            config.set("sasl.mechanism", &self.sasl_mechanism);
        }
        if consumer {
            config
                .set("group.id", "rust-rdkafka-direct-audit")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .set("isolation.level", &self.isolation_level);
        } else {
            config
                .set("request.required.acks", self.acks.to_string())
                .set("enable.idempotence", self.idempotence.to_string())
                .set("queue.buffering.max.ms", self.linger_ms.to_string())
                .set("batch.size", self.batch_size_bytes.to_string())
                .set("batch.num.messages", self.batch_num_messages.to_string())
                .set(
                    "max.in.flight.requests.per.connection",
                    self.max_in_flight.to_string(),
                )
                .set(
                    "queue.buffering.max.messages",
                    self.queue_max_messages.to_string(),
                )
                .set(
                    "queue.buffering.max.kbytes",
                    self.queue_max_kbytes.to_string(),
                )
                .set("message.timeout.ms", self.delivery_timeout_ms.to_string())
                .set("compression.codec", &self.compression)
                .set("partitioner", "consistent_random")
                .set("sticky.partitioning.linger.ms", "0");
        }
        config
    }
}

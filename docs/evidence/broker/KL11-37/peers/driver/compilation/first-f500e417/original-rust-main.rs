//! Public Rust OAuth acquisition, metadata and ordinary manual record peer.

use partitionline::{
    Acks, Admin, AdminConfig, Compression, Consumer, ConsumerConfig, Error, FetchedRecord,
    IsolationLevel, OidcConfig, ProduceRecord, Producer, ProducerConfig, Sasl, TimestampType,
    TlsConfig,
};
use serde_json::{json, Value};
use std::{
    fs,
    io::{self, BufRead, Write},
    path::Path,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, &'static str>;
const OPERATION: Duration = Duration::from_secs(8);
const STAMP: i64 = 1_700_000_000_123;

fn sdk_category(error: Error) -> &'static str {
    match error {
        Error::Io(_) => "io",
        Error::Timeout => "timeout",
        Error::Protocol(_) => "client-protocol",
        Error::Broker { code: 58, .. } => "broker-authentication",
        Error::Broker { .. } => "broker-other",
        Error::UnknownTopic(_) => "unknown-topic",
        Error::NoLeader { .. } => "no-leader",
        Error::Unsupported(_) => "unsupported",
        Error::Closed => "closed",
        _ => "other-client-category",
    }
}

struct Config {
    bootstrap: String,
    token_url: String,
    client_id: String,
    secret: String,
    ca: Vec<u8>,
    topic: String,
    runtime: Duration,
}

fn bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let info = fs::symlink_metadata(path).map_err(|_| "file metadata")?;
    if !info.is_file() || info.len() > maximum as u64 {
        return Err("bounded regular file");
    }
    let bytes = fs::read(path).map_err(|_| "file read")?;
    if bytes.len() > maximum {
        return Err("file changed beyond bound");
    }
    Ok(bytes)
}

fn text(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && text.len() <= 2048)
        .map(str::to_owned)
        .ok_or("bounded configuration field")
}

impl Config {
    fn read(path: &Path) -> Result<Self> {
        let value: Value =
            serde_json::from_slice(&bounded_file(path, 8192)?).map_err(|_| "configuration JSON")?;
        let object = value.as_object().ok_or("configuration object")?;
        const KEYS: [&str; 7] = [
            "bootstrap",
            "token_url",
            "client_id",
            "client_secret_file",
            "ca_pem",
            "topic",
            "max_runtime_seconds",
        ];
        if object.keys().any(|key| !KEYS.contains(&key.as_str())) {
            return Err("unknown configuration key");
        }
        let topic = text(&value, "topic")?;
        let token_url = text(&value, "token_url")?;
        let client_id = text(&value, "client_id")?;
        if !token_url.starts_with("https://localhost:")
            || topic != "oidc-probe"
            || client_id != "partitionline-probe"
        {
            return Err("explicit synthetic HTTPS profile");
        }
        let seconds = value
            .get("max_runtime_seconds")
            .and_then(Value::as_u64)
            .filter(|seconds| (1..=180).contains(seconds))
            .ok_or("runtime ceiling")?;
        let secret = String::from_utf8(bounded_file(
            Path::new(&text(&value, "client_secret_file")?),
            4096,
        )?)
        .map_err(|_| "credential text")?;
        if secret.is_empty() || secret.contains('\0') {
            return Err("credential form");
        }
        Ok(Self {
            bootstrap: text(&value, "bootstrap")?,
            token_url,
            client_id,
            secret,
            ca: bounded_file(Path::new(&text(&value, "ca_pem")?), 16384)?,
            topic,
            runtime: Duration::from_secs(seconds),
        })
    }

    fn tls(&self) -> TlsConfig {
        TlsConfig::default()
            .ca_pem(self.ca.clone())
            .server_name("localhost")
    }

    fn sasl(&self) -> Sasl {
        Sasl::oidc(
            OidcConfig::new(&self.token_url, &self.client_id, self.secret.clone()).tls(self.tls()),
        )
    }
}

fn emit(event: Value) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    writeln!(output, "{event}").map_err(|_| "safe receipt write")?;
    output.flush().map_err(|_| "safe receipt flush")
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(
            *DIGITS.get(usize::from(byte >> 4)).unwrap_or(&b'?'),
        ));
        output.push(char::from(
            *DIGITS.get(usize::from(byte & 15)).unwrap_or(&b'?'),
        ));
    }
    output
}

fn record(record: &FetchedRecord) -> Result<Value> {
    if record.key.as_ref().is_some_and(|key| key.len() > 256)
        || record.value.as_ref().is_some_and(|value| value.len() > 256)
        || record.headers.len() > 8
        || record.headers.iter().any(|header| {
            header.key.len() > 64 || header.value.as_ref().is_some_and(|value| value.len() > 256)
        })
    {
        return Err("public record field bound");
    }
    Ok(json!({"offset":record.offset,"timestamp":record.timestamp,
        "timestamp_type":if record.timestamp_type==TimestampType::CreateTime {"CREATE_TIME"} else {"OTHER"},
        "key_hex":record.key.as_ref().map(|key|hex(key)),
        "value_hex":record.value.as_ref().map(|value|hex(value)),
        "headers":record.headers.iter().map(|header|json!({"name":header.key,
            "value_hex":header.value.as_ref().map(|value|hex(value))})).collect::<Vec<_>>()}))
}

struct Peer {
    config: Config,
    admin: Option<Admin>,
    producer: Option<Producer>,
    consumer: Option<Consumer>,
}

impl Peer {
    async fn query(&mut self) -> Result<Value> {
        if self.admin.is_none() {
            self.admin = Some(
                Admin::new(
                    AdminConfig::bootstrap([self.config.bootstrap.clone()])
                        .sasl(self.config.sasl())
                        .tls(self.config.tls())
                        .request_timeout(OPERATION)
                        .connect_timeout(OPERATION),
                )
                .await
                .map_err(sdk_category)?,
            );
        }
        let admin = self.admin.as_mut().ok_or("Admin owner")?;
        let rows = admin
            .describe_topics(std::slice::from_ref(&self.config.topic))
            .await
            .map_err(sdk_category)?;
        if rows.len() != 1
            || rows.iter().any(|row| {
                row.error_code != 0 || row.partitions.len() != 1 || row.topic_id == [0; 16]
            })
        {
            return Err("public metadata identity mismatch");
        }
        let versions: Vec<_> = admin.versions().iter().map(|(&api, version)| {
            json!({"api":api,"min":version.min_version,"max":version.max_version})
        }).collect();
        Ok(
            json!({"topic":self.config.topic,"partition_count":1,"api_versions":versions,
            "public_operation":"Admin.describe_topics"}),
        )
    }

    async fn write(&mut self, phase: &str) -> Result<Value> {
        if self.producer.is_none() {
            self.producer = Some(
                Producer::new(
                    ProducerConfig::bootstrap([self.config.bootstrap.clone()])
                        .sasl(self.config.sasl())
                        .tls(self.config.tls())
                        .acks(Acks::All)
                        .idempotent(false)
                        .compression(Compression::None)
                        .connections(1)
                        .max_in_flight(1)
                        .buffer_memory(1 << 20)
                        .max_request_size(65536)
                        .batch_bytes(128)
                        .batch_records(1)
                        .linger(Duration::ZERO)
                        .request_timeout(OPERATION)
                        .connect_timeout(OPERATION)
                        .delivery_timeout(Duration::from_secs(10))
                        .max_block(OPERATION),
                )
                .await
                .map_err(sdk_category)?,
            );
        }
        let key = format!("oauth-rust-{phase}");
        let value = "public-rust-oidc";
        let receipt = self
            .producer
            .as_ref()
            .ok_or("Producer owner")?
            .send(
                ProduceRecord::to(self.config.topic.as_str())
                    .partition(0)
                    .timestamp(STAMP)
                    .key(key.clone())
                    .value(value)
                    .header("peer", "rust")
                    .null_header("d")
                    .header("d", ""),
            )
            .await
            .map_err(sdk_category)?;
        Ok(
            json!({"topic":receipt.topic,"partition":receipt.partition,"offset":receipt.offset,
            "timestamp":STAMP,"key_hex":hex(key.as_bytes()),"value_hex":hex(value.as_bytes()),
            "public_operation":"Producer.send"}),
        )
    }

    async fn read(&mut self) -> Result<Value> {
        if self.consumer.is_none() {
            self.consumer = Some(
                Consumer::new(
                    ConsumerConfig::bootstrap([self.config.bootstrap.clone()])
                        .sasl(self.config.sasl())
                        .tls(self.config.tls())
                        .auto_commit(false)
                        .allow_auto_create_topics(false)
                        .isolation(IsolationLevel::ReadUncommitted)
                        .fetch_max_bytes(65536)
                        .max_partition_fetch_bytes(4096)
                        .max_poll_records(64)
                        .buffer_memory(1 << 20)
                        .max_wait_ms(20)
                        .request_timeout(OPERATION)
                        .connect_timeout(OPERATION),
                )
                .await
                .map_err(sdk_category)?,
            );
        }
        let consumer = self.consumer.as_mut().ok_or("Consumer owner")?;
        let start = consumer
            .list_offsets(&self.config.topic, 0, -2)
            .await
            .map_err(sdk_category)?;
        let end = consumer
            .list_offsets(&self.config.topic, 0, -1)
            .await
            .map_err(sdk_category)?;
        if start != 0 || end < start || end - start > 128 {
            return Err("ordinary public history ceiling");
        }
        consumer
            .assign_many([((self.config.topic.clone(), 0), start)])
            .await
            .map_err(sdk_category)?;
        let deadline = Instant::now() + OPERATION;
        let mut next = start;
        let mut history = Vec::new();
        while next < end && Instant::now() < deadline {
            let fetched = consumer
                .fetch_timeout(OPERATION)
                .await
                .map_err(sdk_category)?;
            for fetched_record in &fetched {
                if history.len() >= 128
                    || fetched_record.offset != next
                    || next >= end
                    || fetched_record.topic != self.config.topic
                    || fetched_record.partition != 0
                {
                    return Err("complete ordered public history");
                }
                history.push(record(fetched_record)?);
                next += 1;
            }
        }
        if next != end {
            return Err("incomplete public history");
        }
        Ok(json!({"log_start":start,"log_end":end,"records":history,
            "public_operations":["Consumer.list_offsets","Consumer.assign_many","Consumer.fetch_timeout"]}))
    }

    async fn close(&mut self, phase: &str) -> Result<()> {
        // Take and attempt every owner, even when an earlier close fails.
        let producer = match self.producer.take() {
            Some(owner) => Some(owner.close_timeout(OPERATION).await.is_ok()),
            None => None,
        };
        let consumer = match self.consumer.take() {
            Some(owner) => Some(owner.close_timeout(OPERATION).await.is_ok()),
            None => None,
        };
        let admin = match self.admin.take() {
            Some(owner) => Some(owner.close_timeout(OPERATION).await.is_ok()),
            None => None,
        };
        emit(
            json!({"event":"closed","phase":phase,"producer_ok":producer,
            "consumer_ok":consumer,"admin_ok":admin}),
        )?;
        if [producer, consumer, admin].contains(&Some(false)) {
            return Err("SDK owner close failed");
        }
        Ok(())
    }
}

async fn run() -> Result<()> {
    let arguments: Vec<_> = std::env::args_os().collect();
    if arguments.len() != 2 {
        return Err("one configuration path required");
    }
    let config = Config::read(Path::new(arguments.get(1).ok_or("config argument")?))?;
    let deadline = Instant::now() + config.runtime;
    let mut peer = Peer {
        config,
        admin: None,
        producer: None,
        consumer: None,
    };
    emit(json!({"event":"ready","sdk":"partitionline","provider":"OidcConfig/Sasl::oidc"}))?;
    let input = io::stdin();
    let mut lines = input.lock().lines();
    let result = async {
        for ordinal in 0..25 {
            if Instant::now() >= deadline {
                return Err("operator runtime ceiling");
            }
            // The external driver owns the finite process deadline while stdin
            // is idle; Tokio worker threads still service genuine SDK I/O.
            let Some(line) = lines.next() else {
                return Err("unexpected operator EOF");
            };
            let line = line.map_err(|_| "operator input")?;
            if line.len() > 96 {
                return Err("command byte ceiling");
            }
            let mut fields = line.split_ascii_whitespace();
            let operation = fields.next().ok_or("operator command")?;
            let phase = fields.next().ok_or("operator phase")?;
            if phase.is_empty()
                || phase.len() > 48
                || fields.next().is_some()
                || !phase.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'_'
                        || byte == b'-'
                })
            {
                return Err("bounded synthetic phase");
            }
            if operation == "close" {
                return Ok(());
            }
            if operation == "recreate" {
                peer.close(phase).await?;
            }
            let event = match operation {
                "query" | "recreate" => ("query", peer.query().await),
                "write" => ("produce", peer.write(phase).await),
                "read" => ("fetch", peer.read().await),
                _ => return Err("unknown public operation"),
            };
            let mut receipt = json!({"event":event.0,"phase":phase,"ordinal":ordinal,
                "passed":event.1.is_ok()});
            if let Err(category) = &event.1 {
                if let Some(output) = receipt.as_object_mut() {
                    output.insert("error_category".to_owned(), json!(category));
                }
            }
            if let Ok(fields) = event.1 {
                if let (Some(output), Some(fields)) = (receipt.as_object_mut(), fields.as_object())
                {
                    output.extend(fields.clone());
                }
            }
            // SDK error values are intentionally discarded: arbitrary server
            // and token-provider response text never enters the receipt.
            emit(receipt)?;
        }
        Err("operator command ceiling")
    }
    .await;
    let closed = peer.close("shutdown").await;
    emit(json!({"event":"shutdown","commands_ok":result.is_ok(),"owners_ok":closed.is_ok()}))?;
    result.and(closed)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(label) = run().await {
        let _result = emit(json!({"event":"fatal","label":label}));
        std::process::exit(1);
    }
}

//! One seeded nontransactional produce/consume trial (KL03-19).
//!
//! Produces `DATA_PLANE_RECORDS` idempotent `acks=all` records on a pinned
//! partition, waits at `DATA_PLANE_FAULT_AFTER` so the campaign can inject a
//! cluster fault, then a manual consumer reads acked payloads. Writes one
//! history JSON for `scripts/check-record-history.py`.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use partitionline::{
    Acks, Admin, AdminConfig, AutoOffsetReset, Consumer, ConsumerConfig, Error, NewTopic,
    ProduceRecord, Producer, ProducerConfig, TopicPartition,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Trial::from_env()?;
    let history = cfg.run().await?;
    if let Some(parent) = cfg.history_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::protocol(e.to_string()))?;
    }
    tokio::fs::write(&cfg.history_path, history)
        .await
        .map_err(|e| Error::protocol(e.to_string()))?;
    Ok(())
}

struct Trial {
    bootstrap: String,
    topic: String,
    seed: u32,
    records: u32,
    fault_after: u32,
    partition: i32,
    ready_path: Option<PathBuf>,
    go_path: Option<PathBuf>,
    history_path: PathBuf,
    trial_deadline: Duration,
    request_timeout: Duration,
    delivery_timeout: Duration,
}

struct Attempt {
    id: String,
    payload: String,
    status: &'static str,
    partition: i32,
    offset: Option<i64>,
}

impl Trial {
    fn from_env() -> partitionline::Result<Self> {
        let records = env_u32("DATA_PLANE_RECORDS", 3)?;
        let fault_after = env_u32("DATA_PLANE_FAULT_AFTER", 1)?;
        if fault_after < 1 || fault_after >= records {
            return Err(Error::protocol(
                "DATA_PLANE_FAULT_AFTER must be in [1, records)",
            ));
        }
        Ok(Self {
            bootstrap: env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:19092".into()),
            topic: env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-dp-fault".into()),
            seed: env_u32("DATA_PLANE_SEED", 0)?,
            records,
            fault_after,
            partition: env_i32("DATA_PLANE_PARTITION", 0)?,
            ready_path: env::var_os("DATA_PLANE_READY").map(PathBuf::from),
            go_path: env::var_os("DATA_PLANE_GO").map(PathBuf::from),
            history_path: env::var_os("DATA_PLANE_HISTORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data-plane-history.json")),
            trial_deadline: Duration::from_millis(u64::from(env_u32(
                "DATA_PLANE_TRIAL_MS",
                45_000,
            )?)),
            request_timeout: Duration::from_millis(u64::from(env_u32(
                "DATA_PLANE_REQUEST_MS",
                8_000,
            )?)),
            delivery_timeout: Duration::from_millis(u64::from(env_u32(
                "DATA_PLANE_DELIVERY_MS",
                20_000,
            )?)),
        })
    }

    async fn run(&self) -> partitionline::Result<String> {
        let deadline = Instant::now() + self.trial_deadline;
        self.ensure_topic().await?;
        let producer = self.connect_producer().await?;
        let mut consumer = self.connect_consumer().await?;
        consumer
            .assign_partitions((0..3).map(|p| TopicPartition::new(self.topic.clone(), p)))
            .await?;

        let mut attempted = Vec::new();
        for index in 0..self.records {
            if index == self.fault_after {
                self.signal_and_wait(deadline).await?;
            }
            attempted.push(self.produce_one(&producer, index).await);
        }
        producer.close().await?;

        let acked: Vec<&Attempt> = attempted.iter().filter(|a| a.status == "acked").collect();
        let consumed = self.consume_acked(&mut consumer, &acked, deadline).await?;
        Ok(render_history(self, &attempted, &consumed))
    }

    async fn ensure_topic(&self) -> partitionline::Result<()> {
        let mut admin = Admin::new(AdminConfig::bootstrap(
            self.bootstrap.split(',').map(str::trim),
        ))
        .await?;
        let created = admin
            .create_topics(
                &[NewTopic::new(self.topic.clone(), 3, 3).config("min.insync.replicas", "2")],
                30_000,
                false,
            )
            .await?;
        admin.close().await?;
        for result in created {
            if result.error_code != 0 && result.error_code != 36 {
                return Err(Error::broker(result.error_code, result.name));
            }
        }
        Ok(())
    }

    async fn connect_producer(&self) -> partitionline::Result<Producer> {
        let mut pcfg = ProducerConfig::bootstrap(self.bootstrap.split(',').map(str::trim));
        pcfg.linger = Duration::ZERO;
        pcfg.batch_records = 1;
        let producer = Producer::new(
            pcfg.acks(Acks::All)
                .idempotent(true)
                .request_timeout(self.request_timeout)
                .delivery_timeout(self.delivery_timeout)
                .max_block(self.delivery_timeout),
        )
        .await?;
        Ok(producer)
    }

    async fn connect_consumer(&self) -> partitionline::Result<Consumer> {
        let mut ccfg = ConsumerConfig::bootstrap(self.bootstrap.split(',').map(str::trim));
        ccfg.max_wait_ms = 1_000;
        ccfg.request_timeout = self.request_timeout;
        ccfg.auto_offset_reset = AutoOffsetReset::Latest;
        Consumer::new(ccfg).await
    }

    async fn produce_one(&self, producer: &Producer, index: u32) -> Attempt {
        let id = record_id(self.seed, index);
        let payload = format!("payload-{id}");
        let rec = ProduceRecord::to(self.topic.clone())
            .partition(self.partition)
            .key(id.as_bytes().to_vec())
            .value(payload.as_bytes().to_vec());
        match producer.send(rec).await {
            Ok(md) => Attempt {
                id,
                payload,
                status: "acked",
                partition: md.partition,
                offset: Some(md.offset),
            },
            Err(Error::Timeout) => Attempt {
                id,
                payload,
                status: "ambiguous",
                partition: self.partition,
                offset: None,
            },
            Err(_) => Attempt {
                id,
                payload,
                status: "failed",
                partition: self.partition,
                offset: None,
            },
        }
    }

    async fn signal_and_wait(&self, deadline: Instant) -> partitionline::Result<()> {
        if let Some(path) = &self.ready_path {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::protocol(e.to_string()))?;
            }
            tokio::fs::write(path, b"ready\n")
                .await
                .map_err(|e| Error::protocol(e.to_string()))?;
        }
        let Some(go) = &self.go_path else {
            return Ok(());
        };
        while Instant::now() < deadline {
            if tokio::fs::try_exists(go)
                .await
                .map_err(|e| Error::protocol(e.to_string()))?
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(Error::Timeout)
    }

    async fn consume_acked(
        &self,
        consumer: &mut Consumer,
        acked: &[&Attempt],
        deadline: Instant,
    ) -> partitionline::Result<Vec<Attempt>> {
        let mut wanted: Vec<String> = acked.iter().map(|a| a.id.clone()).collect();
        let mut found = Vec::new();
        while Instant::now() < deadline && !wanted.is_empty() {
            let recs = match consumer.fetch_timeout(Duration::from_millis(1_000)).await {
                Ok(recs) => recs,
                Err(Error::Timeout) => continue,
                Err(err) if err.is_retriable() => continue,
                Err(err) => return Err(err),
            };
            for rec in recs.iter() {
                let Some(value) = rec.value.as_deref() else {
                    continue;
                };
                let Ok(text) = std::str::from_utf8(value) else {
                    continue;
                };
                let Some(id) = text.strip_prefix("payload-") else {
                    continue;
                };
                if let Some(pos) = wanted.iter().position(|w| w == id) {
                    let _removed = wanted.remove(pos);
                    found.push(Attempt {
                        id: id.to_string(),
                        payload: text.to_string(),
                        status: "acked",
                        partition: rec.partition,
                        offset: Some(rec.offset),
                    });
                }
            }
        }
        Ok(found)
    }
}

fn record_id(seed: u32, index: u32) -> String {
    format!("s{seed:04}-r{index}")
}

fn env_u32(name: &str, default: u32) -> partitionline::Result<u32> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| Error::protocol(format!("invalid {name}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(Error::protocol(format!("invalid {name}"))),
    }
}

fn env_i32(name: &str, default: i32) -> partitionline::Result<i32> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| Error::protocol(format!("invalid {name}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(Error::protocol(format!("invalid {name}"))),
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn render_record(rec: &Attempt) -> String {
    let mut out = String::from("{\"id\":");
    out.push_str(&json_escape(&rec.id));
    out.push_str(",\"topic\":");
    // topic filled by caller context in render_history
    out.push_str("TOPIC_PLACEHOLDER");
    out.push_str(",\"partition\":");
    out.push_str(&rec.partition.to_string());
    if let Some(offset) = rec.offset {
        out.push_str(",\"offset\":");
        out.push_str(&offset.to_string());
    }
    out.push_str(",\"key\":");
    out.push_str(&json_escape(&rec.id));
    out.push_str(",\"payload\":");
    out.push_str(&json_escape(&rec.payload));
    out.push_str(",\"status\":");
    out.push_str(&json_escape(rec.status));
    out.push('}');
    out
}

fn render_history(trial: &Trial, attempted: &[Attempt], consumed: &[Attempt]) -> String {
    let topic = json_escape(&trial.topic);
    let render = |recs: &[Attempt]| {
        recs.iter()
            .map(|r| render_record(r).replace("TOPIC_PLACEHOLDER", &topic))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"id\":\"s{:04}\",\"config\":{{\"acks\":-1,\"idempotent\":true,\"transactional\":false,\"isolation_level\":\"read_uncommitted\",\"delivery\":\"partition\"}},\"attempted\":[{}],\"consumed\":[{}]}}\n",
        trial.seed,
        render(attempted),
        render(consumed)
    )
}

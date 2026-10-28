//! Verifiable-producer adapter: Apache system-test CLI + JSON event contract.
//!
//! Pinned to Apache Kafka 3.9.1 `VerifiableProducer.java` (see
//! `examples/common/verifiable.rs` for the pin). Produces increasing
//! integers (`"i"`, or `"<prefix>.i"` with `--value-prefix`) and prints one
//! JSON event per send to stdout: `startup_complete`,
//! `producer_send_success` (`key`, `value`, `topic`, `partition`, `offset`),
//! `producer_send_error` (`key`, `value`, `topic`, `exception`, `message`),
//! `shutdown_complete`, and the final `tool_data` summary (`sent`, `acked`,
//! `target_throughput`, `avg_throughput`).
//!
//! Behavioral notes (vs the Java tool):
//!
//! - Sends are sequential (await each ack) instead of async-callback, so the
//!   event stream is deterministic. IDs, values, partitions and offsets are
//!   preserved exactly.
//! - Java hardcodes `retries=0`; partitionline has no retries-count knob, so
//!   attempts are bounded by `delivery.timeout.ms` instead (configurable via
//!   `--producer.config`). Persistent broker errors surface as
//!   `producer_send_error` after the deadline.
//! - `--throughput 0` means unthrottled (Java would divide by zero).
//! - `tool_data.avg_throughput` is `0.0` when the run took 0ms (Java prints
//!   non-standard `Infinity`, which is not valid JSON).
//! - `producer_send_error.exception` is the partitionline error category
//!   (`Timeout`, `Broker`, `Io`, ...) rather than a Java class name;
//!   `message` is the human-readable detail.
//! - `--producer.config` honors the Java quirk that the file wins over CLI
//!   for keys settable both ways (`bootstrap.servers`, `acks`).
//! - Unknown config keys warn on stderr (Java logs the same warning);
//!   invalid values of known keys exit 1 (Java `ConfigException`).
//! - SIGINT/SIGTERM stops after the in-flight send, then closes and prints
//!   `shutdown_complete` + `tool_data` (Java shutdown hook).

#[path = "common/verifiable.rs"]
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    exit_usage, parse_i64, parse_properties, parse_raw, resolve_bootstrap, split_bootstrap,
    wants_help, Event,
};
use partitionline::{Acks, ProduceRecord, Producer, ProducerConfig};

const HELP: &str = "\
usage: verifiable_producer --topic TOPIC (--bootstrap-server HOST1:PORT1[,...] | --broker-list HOST1:PORT1[,...]) [--max-messages N] [--throughput N] [--acks 0|1|-1] [--producer.config FILE] [--message-create-time MS] [--value-prefix N] [--repeating-keys N]

This tool produces increasing integers to the specified topic and prints JSON
metadata to stdout on each \"send\" request, making externally visible which
messages have been acked and which have not.
Pinned contract: Apache Kafka 3.9.1 VerifiableProducer.

  --topic TOPIC            Produce messages to this topic (required).
  --bootstrap-server LIST  Comma-separated broker list (required unless
                           --broker-list is given).
  --broker-list LIST       Deprecated alias of --bootstrap-server.
  --max-messages N         Produce this many messages (-1 = until killed).
  --throughput N           Throttle to ~N messages/sec (-1 = unthrottled;
                           0 = unthrottled, adapter-defined).
  --acks 0|1|-1            Acks required per message (default -1).
  --producer.config FILE   Producer config properties file (file wins over
                           CLI for bootstrap.servers/acks, as in Java).
  --message-create-time MS First message create-time in ms since epoch;
                           each message advances it by the elapsed runtime.
  --value-prefix N         Prefix every value as \"N.i\".
  --repeating-keys N       Cycle record keys 0..N (then back to 0).

Supported producer.config keys: bootstrap.servers, client.id, acks (0/1/-1/
all), linger.ms, batch.size, buffer.memory, max.request.size,
compression.type (none/gzip/snappy/lz4), request.timeout.ms,
delivery.timeout.ms, max.block.ms, retry.backoff.ms, metadata.max.age.ms,
reconnect.backoff.ms, connections.max.idle.ms.
Unknown keys warn on stderr; `retries` has no effect (bounded by
delivery.timeout.ms instead of Java retries=0).";

/// Short stable category for `producer_send_error.exception`.
fn error_category(err: &partitionline::Error) -> &'static str {
    match err {
        partitionline::Error::Io(_) => "Io",
        partitionline::Error::Protocol(_) => "Protocol",
        partitionline::Error::Broker { .. } => "Broker",
        partitionline::Error::UnknownTopic(_) => "UnknownTopic",
        partitionline::Error::NoLeader { .. } => "NoLeader",
        partitionline::Error::Unsupported(_) => "Unsupported",
        partitionline::Error::Closed => "Closed",
        partitionline::Error::Timeout => "Timeout",
        partitionline::Error::QueueFull => "QueueFull",
        partitionline::Error::RecordTooLarge { .. } => "RecordTooLarge",
        partitionline::Error::MaxPollInterval => "MaxPollInterval",
        partitionline::Error::Wakeup => "Wakeup",
    }
}

/// Apply one `--producer.config` property (Java `putAll` ordering: caller
/// applies the file after CLI so the file wins).
fn apply_property(
    cfg: &mut ProducerConfig,
    bootstrap: &mut Vec<String>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    match key {
        "bootstrap.servers" => *bootstrap = split_bootstrap(value).map_err(|e| format!("{key}: {e}"))?,
        "client.id" => cfg.client_id = value.to_string(),
        "acks" => {
            cfg.acks = match value.trim() {
                "0" => 0,
                "1" => 1,
                "-1" | "all" => -1,
                other => return Err(format!("{key}: invalid acks value: '{other}'")),
            };
        }
        "linger.ms" => cfg.linger = Duration::from_millis(parse_ms(value, key)?),
        "batch.size" => cfg.batch_bytes = parse_usize(value, key)?,
        "buffer.memory" => cfg.buffer_memory = parse_usize(value, key)?,
        "max.request.size" => cfg.max_request_size = parse_usize(value, key)?,
        "compression.type" => {
            cfg.compression = partitionline::Compression::from_name(value.trim())
                .map_err(|e| format!("{key}: {e}"))?;
        }
        "request.timeout.ms" => cfg.request_timeout = Duration::from_millis(parse_ms(value, key)?),
        "delivery.timeout.ms" => cfg.delivery_timeout = Duration::from_millis(parse_ms(value, key)?),
        "max.block.ms" => cfg.max_block = Duration::from_millis(parse_ms(value, key)?),
        "retry.backoff.ms" => cfg.retry_backoff = Duration::from_millis(parse_ms(value, key)?),
        "metadata.max.age.ms" => cfg.metadata_max_age = Duration::from_millis(parse_ms(value, key)?),
        "reconnect.backoff.ms" => {
            cfg.reconnect_backoff = Duration::from_millis(parse_ms(value, key)?);
        }
        "connections.max.idle.ms" => {
            cfg.connections_max_idle = Duration::from_millis(parse_ms(value, key)?);
        }
        "retries" => eprintln!(
            "verifiable_producer: warning: '{key}' has no effect (attempts are bounded by delivery.timeout.ms, not Java retries=0)"
        ),
        _ => eprintln!("verifiable_producer: warning: ignoring unsupported producer property: '{key}'"),
    }
    Ok(())
}

/// Parse a millisecond config value (negative rejected, like Java range checks).
fn parse_ms(value: &str, key: &str) -> Result<u64, String> {
    let ms = parse_i64(value, key)?;
    u64::try_from(ms).map_err(|_| format!("{key}: must be >= 0, got '{value}'"))
}

/// Parse a byte-count config value.
fn parse_usize(value: &str, key: &str) -> Result<usize, String> {
    let n = parse_i64(value, key)?;
    usize::try_from(n).map_err(|_| format!("{key}: must be >= 0, got '{value}'"))
}

/// Wait for SIGINT (and SIGTERM on Unix), then set `stop`.
async fn watch_stop_signal(stop: Arc<AtomicBool>) {
    #[cfg(unix)]
    {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                () = async { drop(tokio::signal::ctrl_c().await); } => {},
                _ = sig.recv() => {},
            }
        } else {
            drop(tokio::signal::ctrl_c().await);
        }
    }
    #[cfg(not(unix))]
    drop(tokio::signal::ctrl_c().await);
    stop.store(true, Ordering::SeqCst);
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() || wants_help(&argv) {
        println!("{HELP}");
        return;
    }
    let args = match parse_raw(
        &argv,
        &[
            "topic",
            "bootstrap-server",
            "broker-list",
            "max-messages",
            "throughput",
            "acks",
            "producer.config",
            "message-create-time",
            "value-prefix",
            "repeating-keys",
        ],
        &[],
    ) {
        Ok(a) => a,
        Err(e) => exit_usage("verifiable_producer", HELP, &e),
    };

    let topic = args.get("topic").unwrap_or_else(|| {
        exit_usage(
            "verifiable_producer",
            HELP,
            "the following arguments are required: --topic",
        )
    });
    let mut bootstrap =
        resolve_bootstrap(&args).unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e));
    let max_messages = args
        .get("max-messages")
        .map(|v| parse_i64(v, "--max-messages"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e))
        .unwrap_or(-1);
    let throughput = args
        .get("throughput")
        .map(|v| parse_i64(v, "--throughput"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e))
        .unwrap_or(-1);
    let mut acks: i16 = match args.get("acks").unwrap_or("-1") {
        "0" => 0,
        "1" => 1,
        "-1" => -1,
        other => exit_usage(
            "verifiable_producer",
            HELP,
            &format!("argument --acks: invalid choice: '{other}' (choose from 0, 1, -1)"),
        ),
    };
    let value_prefix: Option<i64> = args
        .get("value-prefix")
        .map(|v| parse_i64(v, "--value-prefix"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e));
    let repeating_keys: Option<i64> = args
        .get("repeating-keys")
        .map(|v| parse_i64(v, "--repeating-keys"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e));
    let mut create_time: Option<u64> = match args.get("message-create-time") {
        None => None,
        Some(v) => {
            let ms = parse_i64(v, "--message-create-time")
                .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e));
            if ms == -1 {
                None
            } else {
                Some(u64::try_from(ms).unwrap_or_else(|_| {
                    exit_usage(
                        "verifiable_producer",
                        HELP,
                        "--message-create-time must be -1 or >= 0",
                    )
                }))
            }
        }
    };

    let mut cfg = ProducerConfig::bootstrap(bootstrap.clone());
    if let Some(path) = args.get("producer.config") {
        let text = tokio::fs::read_to_string(path).await.unwrap_or_else(|e| {
            exit_usage(
                "verifiable_producer",
                HELP,
                &format!("--producer.config: {e}"),
            )
        });
        // Java applies the file after CLI (`putAll`), so the file wins.
        for (key, value) in parse_properties(&text) {
            apply_property(&mut cfg, &mut bootstrap, &key, &value)
                .unwrap_or_else(|e| exit_usage("verifiable_producer", HELP, &e));
        }
        cfg.bootstrap = bootstrap.clone();
        acks = cfg.acks;
    }
    cfg = cfg.acks(match acks {
        0 => Acks::None,
        1 => Acks::Leader,
        _ => Acks::All,
    });

    let producer = Producer::new(cfg).await.unwrap_or_else(|e| {
        eprintln!("verifiable_producer: error: cannot create producer: {e}");
        std::process::exit(1);
    });

    let stop = Arc::new(AtomicBool::new(false));
    drop(tokio::spawn(watch_stop_signal(stop.clone())));

    Event::new("startup_complete").emit();
    let start = Instant::now();
    let start_ms = common::now_ms();
    let mut num_sent: u64 = 0;
    let mut num_acked: u64 = 0;
    let mut key_counter: i64 = 0;
    let mut i: i64 = 0;
    loop {
        if max_messages >= 0 && i >= max_messages {
            break;
        }
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let key: Option<String> = repeating_keys.map(|n| {
            let k = key_counter.to_string();
            key_counter += 1;
            if key_counter == n {
                key_counter = 0;
            }
            k
        });
        let value = match value_prefix {
            Some(p) => format!("{p}.{i}"),
            None => i.to_string(),
        };
        let mut record = ProduceRecord::to(topic).value(value.clone());
        if let Some(k) = key.as_deref() {
            record = record.key(k.to_string());
        }
        if let Some(ct) = create_time {
            record = record.timestamp(i64::try_from(ct).unwrap_or(i64::MAX));
            // Java: createTime += now - startTime after every send.
            create_time = Some(ct.saturating_add(common::now_ms().saturating_sub(start_ms)));
        }
        num_sent += 1;
        match producer.send(record).await {
            Ok(md) => {
                num_acked += 1;
                let mut ev = Event::new("producer_send_success");
                ev.opt_str_field("key", key.as_deref());
                ev.str_field("value", &value);
                ev.str_field("topic", &md.topic);
                ev.int_field("partition", i64::from(md.partition));
                ev.int_field("offset", md.offset);
                ev.emit();
            }
            Err(e) => {
                let mut ev = Event::new("producer_send_error");
                ev.opt_str_field("key", key.as_deref());
                ev.str_field("value", &value);
                ev.str_field("topic", topic);
                ev.str_field("exception", error_category(&e));
                ev.str_field("message", &e.to_string());
                ev.emit();
            }
        }
        // ThroughputThrottler shape: (i+1) messages in >= (i+1)*1000/throughput ms.
        if throughput > 0 {
            let target_ms = num_sent
                .saturating_mul(1000)
                .checked_div(u64::try_from(throughput).unwrap_or(1))
                .unwrap_or(0);
            let deadline = start + Duration::from_millis(target_ms);
            let now = Instant::now();
            if deadline > now {
                tokio::time::sleep(deadline - now).await;
            }
        }
        i += 1;
    }

    if let Err(e) = producer.close().await {
        eprintln!("verifiable_producer: error: close failed: {e}");
        std::process::exit(1);
    }
    Event::new("shutdown_complete").emit();
    let elapsed_ms =
        u64::try_from(start.elapsed().as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
    let avg = if elapsed_ms == 0 {
        0.0
    } else {
        1000.0 * (num_acked as f64) / (elapsed_ms as f64)
    };
    let mut tool = Event::new("tool_data");
    tool.raw_field("sent", &num_sent.to_string());
    tool.raw_field("acked", &num_acked.to_string());
    tool.raw_field("target_throughput", &throughput.to_string());
    tool.raw_field("avg_throughput", &format!("{avg:?}"));
    tool.emit();
}

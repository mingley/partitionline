//! Verifiable-consumer adapter: Apache system-test CLI + JSON event contract.
//!
//! Pinned to Apache Kafka 3.9.1 `VerifiableConsumer.java` (see
//! `examples/common/verifiable.rs` for the pin). Subscribes to one topic and
//! prints consumer events to stdout: `startup_complete`,
//! `partitions_revoked` / `partitions_assigned` (`partitions` as
//! `{topic, partition}` pairs), `records_consumed` (`count` plus per-partition
//! `{topic, partition, count, minOffset, maxOffset}` summaries),
//! `record_data` per record with `--verbose` (`key`, `value`, `topic`,
//! `partition`, `offset`), `offsets_committed` (`offsets` as
//! `{topic, partition, offset}`, `error` omitted on success, `success`), and
//! `shutdown_complete`.
//!
//! Behavioral notes (vs the Java tool):
//!
//! - Eager Range/Sticky events report full revoke/assign sets; cooperative
//!   events report deltas. Closing revokes the owned assignment before shutdown.
//!   Core callbacks fire for assignment changes; identical-set rejoins and empty
//!   initial assignments are not qualified by the event scenarios.
//! - Committed offsets are `maxOffset + 1` of each consumed poll (manual sync
//!   commit by default); with `--enable-autocommit` no `offsets_committed`
//!   events print, as in Java.
//! - Empty polls print nothing: Java blocks in `poll(MAX)` so it only ever
//!   observes non-empty polls (or wakeup); skipping empties reproduces the
//!   observable stream.
//! - `records_consumed.count` is the full poll size even when the tail is
//!   trimmed by `--max-messages` (Java `records.count()`), while summaries
//!   cover only counted records.
//! - Record keys/values decode as strict UTF-8 (Java `StringDeserializer`);
//!   malformed bytes are fatal to the run (stderr + close +
//!   `shutdown_complete`), like Java's `SerializationException` out of poll.
//! - A failed poll is fatal (stderr + close + `shutdown_complete`, exit 0,
//!   as Java logs and exits normally); a failed commit prints
//!   `offsets_committed` with `success: false` and the loop continues.
//! - `--assignment-strategy` accepts the `RangeAssignor` (default), sticky,
//!   and cooperative-sticky Java class names; anything else (e.g.
//!   `RoundRobinAssignor`) fails closed with exit 1.
//! - `--group-remote-assignor` must be the default `uniform` (the KIP-848
//!   join performs server-side assignment and sends no assignor name).
//! - `--consumer.config` honors the Java ordering: CLI wins over the file.
//! - Unknown config keys warn on stderr; invalid values of known keys exit 1.
//! - SIGINT/SIGTERM wakes the consumer, which closes and prints
//!   `shutdown_complete` (Java shutdown hook).

#[path = "common/verifiable.rs"]
mod common;

use common::{
    exit_usage, parse_i64, parse_properties, parse_raw, push_json_str, resolve_bootstrap,
    split_bootstrap, wants_help, Event,
};
use partitionline::{
    AutoOffsetReset, ConsumerConfig, ConsumerGroup, ConsumerRecords, Error, TopicPartition,
};

const HELP: &str = "\
usage: verifiable_consumer --topic TOPIC --group-id GROUP_ID (--bootstrap-server HOST1:PORT1[,...] | --broker-list HOST1:PORT1[,...]) [options]

This tool consumes messages from a specific topic and emits consumer events
(e.g. group rebalances, received messages, and offsets committed) as JSON
objects to STDOUT.
Pinned contract: Apache Kafka 3.9.1 VerifiableConsumer.

  --topic TOPIC              Consume messages from this topic (required).
  --group-id GROUP_ID        Group id shared by group members (required).
  --bootstrap-server LIST    Comma-separated broker list (required unless
                             --broker-list is given).
  --broker-list LIST         Deprecated alias of --bootstrap-server.
  --group-protocol P         Group protocol: classic (default) or consumer
                             (KIP-848); anything else behaves as classic.
  --group-remote-assignor A  Remote assignor for consumer protocol; must be
                             uniform (adapter sends no assignor name).
  --group-instance-id ID     Static-membership instance id.
  --max-messages N           Consume this many messages (-1 = until killed).
  --session-timeout MS       Consumer session timeout (default 30000).
  --verbose                  Log every consumed record (record_data events).
  --enable-autocommit        Commit via auto-commit (no offsets_committed).
  --reset-policy P           earliest (default), latest, or none.
  --assignment-strategy S    Java assignor class (classic only): Range
                             (default), Sticky, CooperativeSticky. Others
                             (e.g. RoundRobin) are rejected.
  --consumer.config FILE     Consumer config properties file (CLI wins over
                             the file, as in Java).

Supported consumer.config keys: bootstrap.servers, group.id, client.id,
enable.auto.commit, auto.offset.reset (earliest/latest/none),
session.timeout.ms, max.poll.records, fetch.max.wait.ms,
heartbeat.interval.ms, group.instance.id, partition.assignment.strategy,
request.timeout.ms.
Unknown keys warn on stderr.";

/// Classic-protocol assignor selected by `--assignment-strategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Assignor {
    Range,
    Sticky,
    CooperativeSticky,
}

/// Map a Java assignor class name to a supported join (suffix match).
fn parse_assignor(value: &str) -> Result<Assignor, String> {
    // CooperativeSticky ends with "StickyAssignor": check it first.
    if value.ends_with("CooperativeStickyAssignor") {
        Ok(Assignor::CooperativeSticky)
    } else if value.ends_with("StickyAssignor") {
        Ok(Assignor::Sticky)
    } else if value.ends_with("RangeAssignor") {
        Ok(Assignor::Range)
    } else {
        Err(format!(
            "argument --assignment-strategy: unsupported assignor '{value}' (choose a RangeAssignor, StickyAssignor, or CooperativeStickyAssignor class name)"
        ))
    }
}

/// Parse `--reset-policy` exactly (Java `ValidString`, case-sensitive).
fn parse_reset(value: &str) -> Result<AutoOffsetReset, String> {
    match value {
        "earliest" => Ok(AutoOffsetReset::Earliest),
        "latest" => Ok(AutoOffsetReset::Latest),
        "none" => Ok(AutoOffsetReset::None),
        other => Err(format!(
            "argument --reset-policy: invalid choice: '{other}' (choose from earliest, latest, none)"
        )),
    }
}

/// `--consumer.config` intents the caller folds with CLI precedence.
#[derive(Default)]
struct FileOverrides {
    bootstrap: Option<Vec<String>>,
    group_id: Option<String>,
    reset: Option<AutoOffsetReset>,
    autocommit: Option<bool>,
    strategy: Option<String>,
}

/// Apply one `--consumer.config` property (Java ordering: caller applies the
/// file first so CLI wins).
fn apply_property(
    cfg: &mut ConsumerConfig,
    overrides: &mut FileOverrides,
    key: &str,
    value: &str,
) -> Result<(), String> {
    match key {
        "bootstrap.servers" => {
            overrides.bootstrap = Some(split_bootstrap(value).map_err(|e| format!("{key}: {e}"))?);
        }
        "group.id" => overrides.group_id = Some(value.to_string()),
        "client.id" => cfg.client_id = value.to_string(),
        "enable.auto.commit" => {
            overrides.autocommit = Some(parse_bool(value, key)?);
        }
        "auto.offset.reset" => {
            overrides.reset = Some(parse_reset(value).map_err(|_| {
                format!("{key}: invalid choice: '{value}' (choose from earliest, latest, none)")
            })?)
        }
        "session.timeout.ms" => {
            cfg.session_timeout_ms = ms_i32(value, key)?;
        }
        "max.poll.records" => {
            let n = parse_i64(value, key)?;
            cfg.max_poll_records = Some(
                usize::try_from(n).map_err(|_| format!("{key}: must be >= 0, got '{value}'"))?,
            );
        }
        "fetch.max.wait.ms" => {
            let ms = parse_i64(value, key)?;
            cfg.max_wait_ms =
                i32::try_from(ms).map_err(|_| format!("{key}: out of range: '{value}'"))?;
        }
        "heartbeat.interval.ms" => cfg.heartbeat_interval = ms_duration(value, key)?,
        "group.instance.id" => cfg.group_instance_id = Some(value.to_string()),
        "partition.assignment.strategy" => overrides.strategy = Some(value.to_string()),
        "request.timeout.ms" => cfg.request_timeout = ms_duration(value, key)?,
        _ => eprintln!(
            "verifiable_consumer: warning: ignoring unsupported consumer property: '{key}'"
        ),
    }
    Ok(())
}

/// Parse a Java boolean config value.
fn parse_bool(value: &str, key: &str) -> Result<bool, String> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("{key}: invalid boolean value: '{other}'")),
    }
}

/// Parse a millisecond config value.
fn ms_duration(value: &str, key: &str) -> Result<std::time::Duration, String> {
    let ms = parse_i64(value, key)?;
    let ms = u64::try_from(ms).map_err(|_| format!("{key}: must be >= 0, got '{value}'"))?;
    Ok(std::time::Duration::from_millis(ms))
}

/// Parse a millisecond config value as `i32` (Java `session.timeout.ms` shape).
fn ms_i32(value: &str, key: &str) -> Result<i32, String> {
    let ms = parse_i64(value, key)?;
    i32::try_from(ms).map_err(|_| format!("{key}: out of range: '{value}'"))
}

/// Render `{topic, partition}` pairs as a JSON array.
fn partitions_json(partitions: &[TopicPartition]) -> String {
    let mut out = String::from("[");
    for (n, tp) in partitions.iter().enumerate() {
        if n > 0 {
            out.push(',');
        }
        out.push_str("{\"topic\":");
        push_json_str(&mut out, tp.topic());
        out.push_str(",\"partition\":");
        out.push_str(&tp.partition().to_string());
        out.push('}');
    }
    out.push(']');
    out
}

/// Join the group with the selected protocol/assignor.
async fn join_group(
    cfg: ConsumerConfig,
    group_id: &str,
    topic: &str,
    consumer_protocol: bool,
    assignor: Assignor,
) -> Result<ConsumerGroup, Error> {
    if consumer_protocol {
        ConsumerGroup::join_consumer(cfg, group_id, topic).await
    } else {
        match assignor {
            Assignor::Range => ConsumerGroup::join(cfg, group_id, topic).await,
            Assignor::Sticky => ConsumerGroup::join_sticky(cfg, group_id, topic).await,
            Assignor::CooperativeSticky => {
                ConsumerGroup::join_cooperative_sticky(cfg, group_id, topic).await
            }
        }
    }
}

/// One decoded record inside a trimmed per-partition batch.
struct BatchRecord {
    key: Option<String>,
    value: String,
    offset: i64,
}

/// One trimmed per-partition summary plus its commit offset.
struct PartitionBatch {
    topic: String,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    records: Vec<BatchRecord>,
}

/// Split one poll into trimmed per-partition batches (Java `onRecordsReceived`
/// trims each partition list to the remaining `--max-messages` budget).
fn batch_poll(
    recs: &ConsumerRecords,
    remaining: Option<usize>,
) -> Result<Vec<PartitionBatch>, String> {
    let mut batches = Vec::new();
    let mut budget = remaining;
    for tp in recs.partitions() {
        let mut records: Vec<_> = recs.records(tp.clone()).collect();
        if let Some(left) = budget {
            records.truncate(left);
        }
        if records.is_empty() {
            continue;
        }
        let mut decoded = Vec::with_capacity(records.len());
        for rec in &records {
            let key = rec
                .key()
                .map(|k| {
                    std::str::from_utf8(k).map(str::to_string).map_err(|_| {
                        "record key is not valid UTF-8 (StringDeserializer)".to_string()
                    })
                })
                .transpose()?;
            let bytes = rec.value().unwrap_or(b"");
            let value = std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|_| "record value is not valid UTF-8 (StringDeserializer)".to_string())?;
            decoded.push(BatchRecord {
                key,
                value,
                offset: rec.offset,
            });
        }
        if let Some(left) = budget.as_mut() {
            *left -= decoded.len();
        }
        let min_offset = decoded.first().map(|r| r.offset).unwrap_or(0);
        let max_offset = decoded.last().map(|r| r.offset).unwrap_or(0);
        batches.push(PartitionBatch {
            topic: tp.topic().to_string(),
            partition: tp.partition(),
            min_offset,
            max_offset,
            records: decoded,
        });
        if budget == Some(0) {
            break;
        }
    }
    Ok(batches)
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
            "group-protocol",
            "group-remote-assignor",
            "group-id",
            "group-instance-id",
            "max-messages",
            "session-timeout",
            "reset-policy",
            "assignment-strategy",
            "consumer.config",
        ],
        &["verbose", "enable-autocommit"],
    ) {
        Ok(a) => a,
        Err(e) => exit_usage("verifiable_consumer", HELP, &e),
    };

    let topic = args.get("topic").unwrap_or_else(|| {
        exit_usage(
            "verifiable_consumer",
            HELP,
            "the following arguments are required: --topic",
        )
    });
    // Java requires CLI --group-id and CLI connection flags (argparse
    // required group) and overwrites the file's group.id/bootstrap.servers
    // with them; the file values below are validated but then discarded.
    let cli_group_id = args.get("group-id").map(str::to_string).unwrap_or_else(|| {
        exit_usage(
            "verifiable_consumer",
            HELP,
            "the following arguments are required: --group-id",
        )
    });
    let cli_bootstrap =
        resolve_bootstrap(&args).unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e));
    let max_messages = args
        .get("max-messages")
        .map(|v| parse_i64(v, "--max-messages"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e))
        .unwrap_or(-1);
    let cli_session_timeout = args
        .get("session-timeout")
        .map(|v| ms_i32(v, "--session-timeout"))
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e));
    let cli_reset = args
        .get("reset-policy")
        .map(parse_reset)
        .transpose()
        .unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e));
    let cli_strategy = args.get("assignment-strategy").map(str::to_string);
    let cli_autocommit = args.get("enable-autocommit").is_some();
    let verbose = args.get("verbose").is_some();
    let group_protocol = args.get("group-protocol").unwrap_or("classic");
    // Java: CONSUMER (case-insensitive) selects KIP-848, anything else is classic.
    let consumer_protocol = group_protocol.eq_ignore_ascii_case("consumer");
    let remote_assignor = args.get("group-remote-assignor").unwrap_or("uniform");
    if !remote_assignor.eq_ignore_ascii_case("uniform") {
        exit_usage(
            "verifiable_consumer",
            HELP,
            &format!(
                "argument --group-remote-assignor: unsupported assignor '{remote_assignor}' (adapter performs server-side assignment; use uniform)"
            ),
        );
    }

    // Java ordering: file first, CLI wins.
    let mut cfg = ConsumerConfig::bootstrap(["127.0.0.1:9"]);
    let mut overrides = FileOverrides::default();
    if let Some(path) = args.get("consumer.config") {
        let text = tokio::fs::read_to_string(path).await.unwrap_or_else(|e| {
            exit_usage(
                "verifiable_consumer",
                HELP,
                &format!("--consumer.config: {e}"),
            )
        });
        for (key, value) in parse_properties(&text) {
            apply_property(&mut cfg, &mut overrides, &key, &value)
                .unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e));
        }
    }
    // CLI wins over the file (Java `createFromArgs` puts CLI last). The file's
    // bootstrap.servers/group.id are validated above, then discarded: Java
    // overwrites them with the required CLI values unseen.
    drop(overrides.bootstrap.take());
    drop(overrides.group_id.take());
    cfg.bootstrap = cli_bootstrap;
    let group_id = cli_group_id;
    if let Some(ms) = cli_session_timeout {
        cfg.session_timeout_ms = ms;
    }
    if let Some(reset) = cli_reset.or(overrides.reset) {
        cfg.auto_offset_reset = reset;
    }
    if cli_autocommit || overrides.autocommit == Some(true) {
        cfg.enable_auto_commit = true;
    }
    if let Some(id) = args.get("group-instance-id") {
        cfg.group_instance_id = Some(id.to_string());
    }
    let strategy_src = cli_strategy.or(overrides.strategy);
    let assignor = match strategy_src.as_deref() {
        None => Assignor::Range,
        Some(s) => {
            parse_assignor(s).unwrap_or_else(|e| exit_usage("verifiable_consumer", HELP, &e))
        }
    };
    let use_autocommit = cfg.enable_auto_commit;

    // The core callback reports partition deltas. Java's eager protocol
    // revokes the full old assignment before assigning the full new one;
    // cooperative/consumer protocols report only revoked/new partitions.
    let eager = !consumer_protocol && assignor != Assignor::CooperativeSticky;
    let assignment = parking_lot::Mutex::new(Vec::<TopicPartition>::new());
    cfg = cfg.on_rebalance(
        move |revoked: &[TopicPartition], added: &[TopicPartition]| {
            let mut current = assignment.lock();
            let old = current.clone();
            current.retain(|tp| !revoked.contains(tp));
            for tp in added {
                if !current.contains(tp) {
                    current.push(tp.clone());
                }
            }
            current.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));
            let reported_revoked = if eager { old.as_slice() } else { revoked };
            if !reported_revoked.is_empty() {
                let mut event = Event::new("partitions_revoked");
                event.raw_field("partitions", &partitions_json(reported_revoked));
                event.emit();
            }
            // Java invokes onPartitionsAssigned even when the new set is empty.
            let reported_assigned = if eager { current.as_slice() } else { added };
            let mut event = Event::new("partitions_assigned");
            event.raw_field("partitions", &partitions_json(reported_assigned));
            event.emit();
        },
    );

    Event::new("startup_complete").emit();
    let mut group = join_group(cfg, &group_id, topic, consumer_protocol, assignor)
        .await
        .unwrap_or_else(|e| {
            eprintln!("verifiable_consumer: error: cannot join group: {e}");
            std::process::exit(1);
        });

    // Signal task wakes the consumer so an in-flight poll returns Wakeup.
    let wakeup = group.wakeup_handle();
    drop(tokio::spawn(async move {
        drop(tokio::signal::ctrl_c().await);
        wakeup.wakeup();
    }));
    #[cfg(unix)]
    {
        let wakeup = group.wakeup_handle();
        drop(tokio::spawn(async move {
            if let Ok(mut sig) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                let _terminated = sig.recv().await;
                wakeup.wakeup();
            }
        }));
    }

    let mut consumed: usize = 0;
    loop {
        if max_messages >= 0 && consumed >= usize::try_from(max_messages).unwrap_or(usize::MAX) {
            break;
        }
        let recs = match group.poll().await {
            Ok(r) => r,
            Err(Error::Wakeup) => break,
            Err(e) => {
                eprintln!("verifiable_consumer: Error during processing, terminating consumer process: {e}");
                break;
            }
        };
        if recs.is_empty() {
            continue;
        }
        let remaining = if max_messages >= 0 {
            usize::try_from(max_messages)
                .ok()
                .and_then(|m| m.checked_sub(consumed))
        } else {
            None
        };
        let batches = match batch_poll(&recs, remaining) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("verifiable_consumer: Error during processing, terminating consumer process: {e}");
                break;
            }
        };
        if verbose {
            for batch in &batches {
                for rec in &batch.records {
                    let mut ev = Event::new("record_data");
                    ev.opt_str_field("key", rec.key.as_deref());
                    ev.str_field("value", &rec.value);
                    ev.str_field("topic", &batch.topic);
                    ev.int_field("partition", i64::from(batch.partition));
                    ev.int_field("offset", rec.offset);
                    ev.emit();
                }
            }
        }
        let mut summaries = String::from("[");
        let mut first = true;
        let mut commit_offsets = Vec::new();
        for batch in &batches {
            if !first {
                summaries.push(',');
            }
            first = false;
            summaries.push_str("{\"topic\":");
            push_json_str(&mut summaries, &batch.topic);
            summaries.push_str(",\"partition\":");
            summaries.push_str(&batch.partition.to_string());
            summaries.push_str(",\"count\":");
            summaries.push_str(&batch.records.len().to_string());
            summaries.push_str(",\"minOffset\":");
            summaries.push_str(&batch.min_offset.to_string());
            summaries.push_str(",\"maxOffset\":");
            summaries.push_str(&batch.max_offset.to_string());
            summaries.push('}');
            consumed += batch.records.len();
            commit_offsets.push((
                TopicPartition::new(batch.topic.clone(), batch.partition),
                batch.max_offset + 1,
            ));
        }
        summaries.push(']');
        let mut ev = Event::new("records_consumed");
        ev.raw_field("count", &recs.count().to_string());
        ev.raw_field("partitions", &summaries);
        ev.emit();

        if !use_autocommit {
            // Java reports the attempted offsets even when the commit fails.
            let mut offsets = String::from("[");
            for (n, (tp, off)) in commit_offsets.iter().enumerate() {
                if n > 0 {
                    offsets.push(',');
                }
                offsets.push_str("{\"topic\":");
                push_json_str(&mut offsets, tp.topic());
                offsets.push_str(",\"partition\":");
                offsets.push_str(&tp.partition().to_string());
                offsets.push_str(",\"offset\":");
                offsets.push_str(&off.to_string());
                offsets.push('}');
            }
            offsets.push(']');
            match group.commit_offsets(commit_offsets.clone()).await {
                Ok(()) => {
                    let mut ev = Event::new("offsets_committed");
                    ev.raw_field("offsets", &offsets);
                    ev.raw_field("success", "true");
                    ev.emit();
                }
                Err(e) => {
                    let mut ev = Event::new("offsets_committed");
                    ev.raw_field("offsets", &offsets);
                    ev.str_field("error", &e.to_string());
                    ev.raw_field("success", "false");
                    ev.emit();
                }
            }
        }
    }

    // Java ConsumerCoordinator.onLeavePrepare revokes the owned assignment
    // before close. The core close callback is intentionally not synthesized.
    let closing = group.assignment();
    if !closing.is_empty() {
        let mut event = Event::new("partitions_revoked");
        event.raw_field("partitions", &partitions_json(&closing));
        event.emit();
    }
    if let Err(e) = group.close().await {
        eprintln!("verifiable_consumer: error: close failed: {e}");
        std::process::exit(1);
    }
    Event::new("shutdown_complete").emit();
}

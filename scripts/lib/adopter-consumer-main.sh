#!/usr/bin/env bash
# Shared adopter operator-surface consumer main.rs emitter.
# Sourced by ci-crate-consumer.sh and verify-crates-io-consumer.sh so the
# packed-crate proof and the crates.io (or path) proof cannot drift apart.
#
# Usage (from a consumer script):
#   # shellcheck source=scripts/lib/adopter-consumer-main.sh
#   source "$ROOT/scripts/lib/adopter-consumer-main.sh"
#   pl_write_adopter_consumer_main "$cons/src/main.rs" "$name" "ci-crate-consumer"

pl_write_adopter_consumer_main() {
  local out="$1"
  local crate_name="$2"
  local label="${3:-adopter-consumer}"
  cat >"$out" <<EOF
use ${crate_name}::{
    Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup, ProduceRecord, Producer,
    ProducerConfig, Sasl, ShareGroup, TlsConfig,
};

#[tokio::main]
async fn main() {
    // Compile-only smoke: construct configs / records without connecting.
    let _ = ProducerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ConsumerConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = AdminConfig::bootstrap(["127.0.0.1:9092"]);
    let _ = ProduceRecord::to("${label}").value(&b"x"[..]);
    let _ = Sasl::plain("ci", "ci");
    let _ = TlsConfig::default();
    // Keep operator types referenced so a published crate cannot drop them.
    let _ = std::any::type_name::<Producer>();
    let _ = std::any::type_name::<Consumer>();
    let _ = std::any::type_name::<ConsumerGroup>();
    let _ = std::any::type_name::<ShareGroup>();
    let _ = std::any::type_name::<Admin>();
    println!("${label}: ok");
}
EOF
}

# KL07-04: runnable packed-package tutorial. The matching guide block is
# compared byte-for-byte during the downstream rehearsal.
# Usage: pl_write_quickstart_consumer_main <fresh-crate>/src/main.rs
pl_write_quickstart_consumer_main() {
  local out="$1"
  cat >"$out" <<'RUST'
use partitionline::{
    Acks, ConsumerConfig, ConsumerGroup, Error, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic =
        std::env::var("KAFKA_TOPIC").map_err(|_| Error::protocol("KAFKA_TOPIC is required"))?;
    let group_id =
        std::env::var("KAFKA_GROUP").map_err(|_| Error::protocol("KAFKA_GROUP is required"))?;
    let payload = b"hello from packed partitionline";
    let producer = Producer::new(
        ProducerConfig::bootstrap([bootstrap.clone()])
            .acks(Acks::All)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await?;
    // send resolves after the broker ack. try_send would only admit to a queue.
    let md = producer
        .send(
            ProduceRecord::to(topic.clone())
                .partition(0)
                .value(&payload[..]),
        )
        .await?;
    println!("ack partition={} offset={}", md.partition, md.offset);
    producer.close().await?;
    let cfg = ConsumerConfig::bootstrap([bootstrap])
        .auto_commit(false)
        .max_wait_ms(100)
        .request_timeout(Duration::from_secs(5));
    // A fresh broker may still be loading its group coordinator. Retry the
    // join, never the already acknowledged produce, within the outer deadline.
    let join_until = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut group = loop {
        match ConsumerGroup::join_topics(cfg.clone(), group_id.clone(), [topic.clone()]).await {
            Ok(group) => break group,
            Err(err) if err.is_retriable() && tokio::time::Instant::now() < join_until => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => return Err(err),
        }
    };
    let outcome = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            let records = group.poll_timeout(Duration::from_secs(2)).await?;
            if records.is_empty() {
                continue;
            }
            for record in &records {
                // Application processing succeeds before any offset is committed.
                if record.topic != topic
                    || record.partition != md.partition
                    || record.offset != md.offset
                    || record.value.as_deref() != Some(&payload[..])
                {
                    return Err(Error::protocol(
                        "unexpected record in isolated tutorial topic",
                    ));
                }
                println!(
                    "processed partition={} offset={} bytes={}",
                    record.partition,
                    record.offset,
                    payload.len()
                );
            }
            group
                .commit_with_metadata_timeout(records.next_offsets(), Duration::from_secs(5))
                .await?;
            let committed = group.committed_timeout(Duration::from_secs(5)).await?;
            let expected_next = md.offset + 1;
            if !committed.iter().any(|(tp, offset)| {
                tp.topic == topic
                    && tp.partition == md.partition
                    && offset.offset() == expected_next
            }) {
                return Err(Error::protocol(
                    "committed position did not match processed record",
                ));
            }
            println!("committed next_offset={expected_next}");
            return Ok(());
        }
    }
    .await;
    // Explicitly leave on processing errors as well; auto commit is disabled.
    group.leave().await?;
    outcome?;
    println!("closed producer and left group");
    Ok(())
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), run())
        .await
        .map_err(|_| Error::Timeout)?
}
RUST
}

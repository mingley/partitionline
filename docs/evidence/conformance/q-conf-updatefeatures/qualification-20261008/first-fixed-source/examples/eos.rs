//! Copy one capped batch and commit Kafka output/offsets in one transaction.
//!
//! Broker on KAFKA_BOOTSTRAP; source KAFKA_TOPIC (default partitionline),
//! destination KAFKA_OUTPUT_TOPIC (partitionline-out), group KAFKA_GROUP
//! (partitionline-eos). KAFKA_TRANSACTIONAL_ID defaults to partitionline-eos
//! and must identify one logical worker exclusively. Auto-commit is off.
//! This bounded recipe covers Kafka output/offsets, not external side effects.
use partitionline::{
    error, ConsumerConfig, ConsumerGroup, ConsumerRecords, Error, IsolationLevel, ProduceRecord,
    Producer, ProducerConfig,
};
use std::time::Duration;

async fn run() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let source = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let dest = std::env::var("KAFKA_OUTPUT_TOPIC").unwrap_or_else(|_| "partitionline-out".into());
    let group_id = std::env::var("KAFKA_GROUP").unwrap_or_else(|_| "partitionline-eos".into());
    let transactional_id =
        std::env::var("KAFKA_TRANSACTIONAL_ID").unwrap_or_else(|_| "partitionline-eos".into());
    let producer_config = ProducerConfig::bootstrap([bootstrap.clone()])
        .transactional_id(transactional_id)
        .transaction_timeout(Duration::from_secs(30))
        .linger(Duration::ZERO)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(2))
        .max_block(Duration::from_secs(2))
        .delivery_timeout(Duration::from_secs(5));
    let producer = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match Producer::new(producer_config.clone()).await {
                Ok(producer) => return Ok(producer),
                Err(error) if coordinator_starting(&error) => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)??;
    let consumer_config = ConsumerConfig::bootstrap([bootstrap])
        .isolation(IsolationLevel::ReadCommitted)
        .auto_commit(false)
        .max_poll_records(64)
        .max_wait_ms(100)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(2));
    let joined = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ConsumerGroup::join_topics(
                consumer_config.clone(),
                group_id.clone(),
                [source.clone()],
            )
            .await
            {
                Ok(group) => return Ok(group),
                Err(error) if coordinator_starting(&error) => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)
    .and_then(std::convert::identity);
    let mut group = match joined {
        Ok(group) => group,
        Err(error) => {
            if let Err(close) = producer.close_timeout(Duration::from_secs(2)).await {
                eprintln!("producer cleanup: {close}");
            }
            return Err(error);
        }
    };
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let recs = group.poll_timeout(Duration::from_secs(2)).await?;
            if recs.is_empty() {
                continue;
            }
            copy_batch(&producer, &group, &recs, &dest).await?;
            for rec in &recs {
                println!("{}-{}@{} -> {dest}", rec.topic, rec.partition, rec.offset);
            }
            return Ok(());
        }
    })
    .await
    .map_err(|_| Error::Timeout)
    .and_then(std::convert::identity);
    let leave = tokio::time::timeout(Duration::from_secs(3), group.leave())
        .await
        .map_err(|_| Error::Timeout)
        .and_then(std::convert::identity);
    let close = producer.close_timeout(Duration::from_secs(3)).await;
    if let Err(error) = &leave {
        eprintln!("group cleanup: {error}");
    }
    if let Err(error) = &close {
        eprintln!("producer cleanup: {error}");
    }
    outcome?;
    leave?;
    close?;
    println!("one batch committed; producer closed and group left");
    Ok(())
}

fn coordinator_starting(error: &Error) -> bool {
    matches!(
        error.broker_code(),
        Some(
            error::COORDINATOR_NOT_AVAILABLE
                | error::COORDINATOR_LOAD_IN_PROGRESS
                | error::NOT_COORDINATOR
        )
    )
}

async fn abort_before_restart(producer: &Producer, original: &Error) {
    if matches!(
        original.broker_code(),
        Some(error::PRODUCER_FENCED | error::INVALID_PRODUCER_ID_MAPPING)
    ) {
        return; // Fatal identity: abort/re-init cannot recover this instance.
    }
    match tokio::time::timeout(Duration::from_secs(3), producer.abort_transaction()).await {
        Ok(Ok(())) => eprintln!("transaction aborted; restart from stored group offsets"),
        Ok(Err(error)) => eprintln!("abort failed; stop this instance: {error}"),
        Err(_) => eprintln!("abort timed out; outcome ambiguous; stop this instance"),
    }
}

async fn copy_batch(
    producer: &Producer,
    group: &ConsumerGroup,
    recs: &ConsumerRecords,
    dest: &str,
) -> partitionline::Result<()> {
    producer.begin_transaction().await?;
    let staged = async {
        for rec in recs {
            let mut out = ProduceRecord::to(dest);
            if let Some(key) = rec.key.clone() {
                out = out.key(key);
            }
            if let Some(value) = rec.value.clone() {
                out = out.value(value);
            }
            drop(producer.send(out).await?);
        }
        producer
            .send_offsets_for_group(&group.group_metadata(), recs.next_offsets())
            .await
    }
    .await;
    if let Err(error) = staged {
        abort_before_restart(producer, &error).await;
        return Err(error);
    }
    // Retry the same transaction's commit, never re-send this batch. A lost
    // commit response can mean committed; switching to abort would be unsafe.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match producer.commit_transaction().await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retriable() => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(error) => {
                    if matches!(
                        error.broker_code(),
                        Some(
                            error::UNKNOWN_PRODUCER_ID
                                | error::TRANSACTION_ABORTABLE
                                | error::INVALID_TXN_STATE
                                | error::CONCURRENT_TRANSACTIONS
                        )
                    ) {
                        abort_before_restart(producer, &error).await;
                    }
                    return Err(error);
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Timeout)?
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    tokio::time::timeout(Duration::from_secs(45), run())
        .await
        .map_err(|_| Error::Timeout)?
}

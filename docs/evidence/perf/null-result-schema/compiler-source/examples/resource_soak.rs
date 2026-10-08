//! Sample producer overload and slow consumer processing against an owned peer or topic.
#[path = "common/resource_peer.rs"]
mod peer;

use bytes::Bytes;
use partitionline::{
    Consumer, ConsumerConfig, Error, ProduceRecord, Producer, ProducerConfig, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

#[derive(Clone, Copy, Default)]
struct FetchSample {
    buffered: usize,
    application_bytes: usize,
    delivered: u64,
    delivered_bytes: u64,
    errors: u64,
}

fn setting<T: std::str::FromStr>(name: &str) -> Result<T> {
    std::env::var(name)
        .map_err(|_| Error::protocol(format!("missing {name}")))?
        .parse()
        .map_err(|_| Error::protocol(format!("invalid {name}")))
}
fn now_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::protocol("clock before epoch"))?
        .as_millis())
}

async fn consume(
    mut consumer: Consumer,
    slow: Duration,
    sender: watch::Sender<FetchSample>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut sample = FetchSample::default();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            records = consumer.fetch() => {
                match records {
                    Ok(records) => {
                        sample.buffered=consumer.buffered_bytes();
                        sample.application_bytes=records.iter().map(|r| r.value.as_ref().map_or(0,Bytes::len)
                            +r.key.as_ref().map_or(0,Bytes::len)
                            +r.headers.iter().map(|h| h.key.len()+h.value.as_ref().map_or(0,Bytes::len)).sum::<usize>()).sum();
                        sample.delivered=sample.delivered.saturating_add(u64::try_from(records.len()).unwrap_or(u64::MAX));
                        sample.delivered_bytes=sample.delivered_bytes.saturating_add(u64::try_from(sample.application_bytes).unwrap_or(u64::MAX));
                        let _previous=sender.send_replace(sample);
                        tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(slow) => {} }
                        drop(records);
                        sample.application_bytes=0;
                    }
                    Err(_) => {
                        sample.errors=sample.errors.saturating_add(1);
                        tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
                    }
                }
                let _previous=sender.send_replace(sample);
            }
        }
    }
    // Clearing assignment discards pending records without committing them.
    consumer
        .assign_many(Vec::<(partitionline::TopicPartition, i64)>::new())
        .await?;
    sample.buffered = consumer.buffered_bytes();
    sample.application_bytes = 0;
    let _previous = sender.send_replace(sample);
    consumer.close_timeout(Duration::from_secs(2)).await
}

fn sample(
    producer: &Producer,
    fetch: FetchSample,
    offered: u64,
    rejected: u64,
    connections: &AtomicUsize,
    elapsed: u128,
    final_sample: bool,
) -> Result<()> {
    let metrics = producer.metrics();
    let pending = metrics
        .records_queued
        .saturating_sub(metrics.records_acked)
        .saturating_sub(metrics.produce_errors);
    println!("{{\"kind\":\"sample\",\"utc_ms\":{},\"elapsed_ms\":{elapsed},\"offered\":{offered},\"accepted\":{},\"completed\":{},\"failed\":{rejected},\"ambiguous\":{},\"pending\":{pending},\"producer_queue_bytes\":{},\"consumer_buffered_bytes\":{},\"application_decoded_bytes\":{},\"delivered\":{},\"delivered_bytes\":{},\"fetch_errors\":{},\"runtime_tasks\":{},\"peer_connections\":{},\"final\":{final_sample}}}",
        now_ms()?,metrics.records_queued,metrics.records_acked,metrics.produce_errors,metrics.bytes_buffered,
        fetch.buffered,fetch.application_bytes,fetch.delivered,fetch.delivered_bytes,fetch.errors,
        tokio::runtime::Handle::current().metrics().num_alive_tasks(),connections.load(Ordering::SeqCst));
    Ok(())
}

async fn run(address: String, connections: Arc<AtomicUsize>, external: bool) -> Result<()> {
    let duration: u64 = setting("SOAK_DURATION_MS")?;
    let rate: u64 = setting("SOAK_RATE")?;
    let slow: u64 = setting("SOAK_SLOW_MS")?;
    let base: u64 = setting("SOAK_ID_BASE")?;
    let topic = std::env::var("SOAK_TOPIC").map_err(|_| Error::protocol("missing topic"))?;
    let stop_file =
        std::env::var("SOAK_STOP_FILE").map_err(|_| Error::protocol("missing stop file"))?;
    if !(100..=86_400_000).contains(&duration)
        || !(1..=1_000_000).contains(&rate)
        || slow > 60_000
        || !topic.starts_with("pl-soak-")
        || topic.len() > 200
        || !topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::protocol("invalid resource workload"));
    }
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([address.clone()])
            .allow_auto_create_topics(false)
            .buffer_memory(4096)
            .max_poll_records(1)
            .max_bytes(4096)
            .max_partition_fetch_bytes(4096)
            .request_timeout(Duration::from_secs(2))
            .auto_commit(false),
    )
    .await?;
    consumer.assign_topic(&topic, 0).await?;
    if consumer.assignment().len() != 1 {
        return Err(Error::protocol("resource profile requires one partition"));
    }
    if external {
        let assignment = consumer.assignment();
        let ends = consumer.end_offsets(assignment.clone()).await?;
        for (partition, offset) in ends {
            consumer.seek(partition.topic(), partition.partition(), offset)?;
        }
    }
    let mut config = ProducerConfig::bootstrap([address])
        .buffer_memory(4096)
        .linger(Duration::from_millis(10))
        .max_in_flight(1)
        .request_timeout(Duration::from_millis(250))
        .delivery_timeout(Duration::from_millis(750));
    config.enable_idempotence = external;
    config = config.allow_auto_create_topics(false);
    let producer = Producer::new(config).await?;
    let _partitions = producer.partitions_for(topic.clone()).await?;
    let baseline_tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let (stats, latest) = watch::channel(FetchSample::default());
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(consume(
        consumer,
        Duration::from_millis(slow),
        stats,
        stopped,
    ));
    let started = Instant::now();
    let mut offered = 0_u64;
    let mut rejected = 0_u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_sample = 0;
    let mut interrupted = false;
    let result: Result<()> = async {
        loop {
            let _tick = ticker.tick().await;
            let elapsed = started.elapsed().as_millis();
            if tokio::fs::try_exists(&stop_file).await? {
                interrupted = true;
                break;
            }
            // Admission remains finite even if scheduling falls behind the offered rate.
            let target = u64::try_from(
                elapsed
                    .min(u128::from(duration))
                    .saturating_mul(u128::from(rate))
                    / 1000,
            )
            .unwrap_or(u64::MAX);
            for _ in 0..target.saturating_sub(offered).min(10_000) {
                let id = base
                    .checked_add(offered)
                    .ok_or_else(|| Error::protocol("ID overflow"))?;
                let mut value = vec![b'x'; 256];
                for (target, byte) in value.iter_mut().zip(id.to_be_bytes()) {
                    *target = byte;
                }
                offered += 1;
                if producer
                    .try_send(
                        ProduceRecord::to(topic.clone())
                            .partition(0)
                            .value(Bytes::from(value)),
                    )
                    .is_err()
                {
                    rejected += 1;
                }
            }
            if elapsed >= next_sample {
                sample(
                    &producer,
                    *latest.borrow(),
                    offered,
                    rejected,
                    &connections,
                    elapsed,
                    false,
                )?;
                next_sample = elapsed + 50;
            }
            if elapsed >= u128::from(duration) {
                break;
            }
        }
        Ok(())
    }
    .await;
    let load_elapsed = started.elapsed().as_millis();
    let _sent = stop.send(true);
    let joined = task
        .await
        .map_err(|e| Error::protocol(format!("consumer join: {e}")))?;
    let observer = producer.clone();
    let closed = producer.close_timeout(Duration::from_secs(2)).await;
    // Worker failure counters and reservations are observed after close has joined workers.
    sample(
        &observer,
        *latest.borrow(),
        offered,
        rejected,
        &connections,
        started.elapsed().as_millis(),
        true,
    )?;
    drop(observer);
    tokio::task::yield_now().await;
    println!("{{\"kind\":\"closure\",\"utc_ms\":{},\"load_elapsed_ms\":{load_elapsed},\"interrupted\":{interrupted},\"producer_close_ok\":{},\"consumer_joined\":{},\"runtime_tasks_before_client_close\":{baseline_tasks},\"runtime_tasks_after_client_close\":{}}}",now_ms()?,closed.is_ok(),joined.is_ok(),tokio::runtime::Handle::current().metrics().num_alive_tasks());
    result?;
    joined?;
    if !matches!(closed, Ok(()) | Err(Error::Timeout)) {
        closed?;
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let external = std::env::var("SOAK_MODE").as_deref() == Ok("controlled");
    let topic = std::env::var("SOAK_TOPIC").map_err(|_| Error::protocol("missing topic"))?;
    let peer = if external {
        None
    } else {
        Some(peer::Peer::start(topic, Duration::from_millis(setting("SOAK_PEER_DELAY_MS")?)).await?)
    };
    let address = match &peer {
        Some(peer) => peer.address.clone(),
        None => {
            std::env::var("SOAK_BOOTSTRAP").map_err(|_| Error::protocol("missing bootstrap"))?
        }
    };
    let active = peer.as_ref().map_or_else(
        || Arc::new(AtomicUsize::new(0)),
        |peer| Arc::clone(&peer.active),
    );
    let result = tokio::time::timeout(
        Duration::from_millis(setting::<u64>("SOAK_DURATION_MS")?) + Duration::from_secs(15),
        run(address, active, external),
    )
    .await;
    if let Some(peer) = peer {
        peer.close().await?;
    }
    println!("{{\"kind\":\"peer_closed\",\"owned\":{},\"connections\":0,\"joined\":true,\"port_reusable\":{},\"runtime_tasks\":{}}}",!external,!external,tokio::runtime::Handle::current().metrics().num_alive_tasks());
    result.map_err(|_| Error::Timeout)?
}

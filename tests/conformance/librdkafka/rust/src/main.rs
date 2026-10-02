//! Adaptation of pinned librdkafka 0125 immediate-flush behavioral assertion.
//! Original thresholds and enqueue/flush schedule are preserved.
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use serde_json::{json, Value};
use std::{
    env,
    fs::OpenOptions,
    io::Write,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

type Delivery = JoinHandle<(usize, partitionline::Result<partitionline::RecordMetadata>)>;
fn event(out: &mut std::fs::File, value: Value) {
    writeln!(out, "{value}").expect("event write");
    out.flush().expect("event flush");
}
fn key_value(id: usize) -> (String, String) {
    (format!("m-{id:03}"), format!("{id:03}:{}", "x".repeat(46)))
}
async fn enqueue(
    producer: &Producer,
    topic: &str,
    base: usize,
    out: &mut std::fs::File,
) -> Vec<Delivery> {
    event(
        out,
        json!({"event":"enqueue_phase","phase":base/50,"count":50,"linger_ms":10000}),
    );
    let mut handles = Vec::new();
    for id in base..base + 50 {
        let p = producer.clone();
        let (key, value) = key_value(id);
        let record = ProduceRecord::to(topic.to_owned())
            .partition(0)
            .key(key.into_bytes())
            .value(value.into_bytes());
        handles.push(tokio::spawn(async move { (id, p.send(record).await) }));
        // Preserve the C helper's sequential enqueue order across task scheduling.
        let deadline = Instant::now() + Duration::from_secs(5);
        while producer.metrics().records_queued < (id + 1) as u64 {
            assert!(Instant::now() < deadline, "enqueue order barrier failed");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    // C helper's nowait calls return after local enqueue. Match that boundary.
    let deadline = Instant::now() + Duration::from_secs(5);
    while producer.metrics().records_queued < (base + 50) as u64 {
        assert!(Instant::now() < deadline, "enqueue barrier failed");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    handles
}
async fn wait_without_flush(producer: &Producer, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        // Matches the upstream poll(1000) observation cadence, through metrics.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        if producer.metrics().records_acked >= target {
            break;
        }
        assert!(Instant::now() < deadline, "delivery barrier failed");
    }
}
async fn delivery_events(handles: Vec<Delivery>, out: &mut std::fs::File) -> usize {
    let mut failures = 0;
    for handle in handles {
        let (id, result) = handle.await.expect("delivery task");
        let (key, value) = key_value(id);
        match result {
            Ok(meta) => event(
                out,
                json!({"event":"delivery","id":key,"payload":value,"partition":meta.partition,"offset":meta.offset,"error_code":0}),
            ),
            Err(error) => {
                failures += 1;
                event(
                    out,
                    json!({"event":"delivery","id":key,"payload":value,"error":error.to_string()}),
                );
            }
        }
    }
    failures
}
fn timing(out: &mut std::fs::File, name: &str, start: Instant, lower: f64, upper: f64) -> usize {
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let passed = elapsed >= lower && elapsed <= upper;
    event(
        out,
        json!({"event":"timing_assertion","name":name,"elapsed_ms":elapsed,"lower_ms":lower,"upper_ms":upper,"passed":passed}),
    );
    usize::from(!passed)
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = env::var("KAFKA_TOPIC")?;
    let bootstrap = env::var("KAFKA_BOOTSTRAP")?;
    let variant = env::var("VARIANT").unwrap_or_else(|_| "normal".into());
    assert!(variant == "normal" || variant == "omit-flush");
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(env::var("EVENTS_PATH")?)?;
    event(
        &mut out,
        json!({"event":"start","peer":"partitionline","topic":topic,"variant":variant}),
    );
    let mut config = ProducerConfig::bootstrap([bootstrap]);
    config.linger = Duration::from_millis(10000);
    config.acks = 1;
    config.enable_idempotence = false;
    config.connections = 1;
    config.max_in_flight = 1;
    config.batch_records = 32768;
    config.batch_bytes = 1000000;
    config.buffer_memory = 33554432;
    config.delivery_timeout = Duration::from_secs(30);
    let producer = Producer::new(config).await?;
    assert_eq!(producer.partitions_for(topic.clone()).await?.len(), 1);
    let first = enqueue(&producer, &topic, 0, &mut out).await;
    let start = Instant::now();
    wait_without_flush(&producer, 50).await;
    let mut failures = timing(&mut out, "NO_FLUSH", start, 10000.0, 15000.0);
    failures += delivery_events(first, &mut out).await;
    let second = enqueue(&producer, &topic, 50, &mut out).await;
    event(
        &mut out,
        json!({"event":"flush_call","timeout_ms":2000,"omitted":variant=="omit-flush"}),
    );
    let start = Instant::now();
    let flush = if variant == "omit-flush" {
        wait_without_flush(&producer, 100).await;
        Ok(())
    } else {
        producer.flush_timeout(Duration::from_millis(2000)).await
    };
    event(
        &mut out,
        json!({"event":"assertion","assertion":"flush(timeout2000) returns success","passed":flush.is_ok(),"error":flush.as_ref().err().map(ToString::to_string)}),
    );
    failures += usize::from(flush.is_err());
    failures += timing(&mut out, "FLUSH", start, 0.0, 2500.0);
    failures += delivery_events(second, &mut out).await;
    let metrics = producer.metrics();
    event(
        &mut out,
        json!({"event":"result","assertion_failures":failures,"delivery_callbacks":metrics.records_acked,"callback_errors":metrics.produce_errors}),
    );
    let close = producer.close().await;
    if let Err(error) = close {
        return Err(Box::new(error) as Box<dyn std::error::Error>);
    }
    if failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}

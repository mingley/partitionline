//! Open-loop regression coverage against deterministic virtual peers and a mock broker.

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures and assertions fail immediately on unexpected errors"
)]

mod common;

#[path = "../examples/bench_latency.rs"]
#[expect(
    dead_code,
    reason = "include the production example to test its actual scheduling and reporting code"
)]
mod latency;

use std::time::Duration;

use bytes::Bytes;
use latency::open_loop::{summarize, Config, FixedRateSchedule, Sample, SAMPLE_FLOOR};
use partitionline::{ProduceRecord, Producer, ProducerConfig};

fn config(count: u64) -> Config {
    Config {
        count,
        rate_per_second: 1_000,
        max_pending: 128,
        sample_floor: SAMPLE_FLOOR,
        buffer_memory: 1_048_576,
        max_block: Duration::from_secs(1),
        delivery_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
    }
}

#[test]
fn fixed_rate_is_absolute_and_uses_rational_nanoseconds() {
    let schedule = FixedRateSchedule::new(3).unwrap();
    assert_eq!(schedule.arrival(0).unwrap(), Duration::ZERO);
    assert_eq!(
        schedule.arrival(1).unwrap(),
        Duration::from_nanos(333_333_333)
    );
    assert_eq!(schedule.arrival(3).unwrap(), Duration::from_secs(1));
    assert!(FixedRateSchedule::new(0).is_err());
    assert!(FixedRateSchedule::new(1_000_000_001).is_err());
    assert!(FixedRateSchedule::new(1)
        .unwrap()
        .arrival(u64::MAX)
        .is_err());
}

struct DelayedFakePeer {
    next_response_ns: u64,
    accepted: u64,
    pending: std::collections::VecDeque<Sample>,
    completed: Vec<Sample>,
}

impl DelayedFakePeer {
    fn offer(&mut self, mut sample: Sample, now_ns: u64) {
        sample
            .observe_enqueue(self.accepted, self.accepted + 1, now_ns, now_ns)
            .unwrap();
        self.accepted += 1;
        self.pending.push_back(sample);
    }

    fn poll_response(&mut self, now_ns: u64) {
        if now_ns < self.next_response_ns {
            return;
        }
        if let Some(mut sample) = self.pending.pop_front() {
            sample.acknowledged_ns = Some(now_ns);
            sample.completed_ns = now_ns;
            sample.outcome = "acknowledged";
            self.completed.push(sample);
            self.next_response_ns = now_ns + 1_000_000;
        }
    }
}

#[test]
fn paused_virtual_peer_keeps_all_arrivals_and_exposes_end_to_end_delay() {
    let schedule = FixedRateSchedule::new(1_000).unwrap();
    let mut peer = DelayedFakePeer {
        next_response_ns: 11_000_000,
        accepted: 0,
        pending: std::collections::VecDeque::new(),
        completed: Vec::new(),
    };
    let mut next_id = 0;
    // Advance a virtual clock in 1 ms ticks. The peer cannot respond during
    // its 10 ms pause, but every scheduled offer still enters its queue.
    for tick in 0..=20 {
        let now_ns = tick * 1_000_000;
        while next_id < 10 {
            let intended_ns = u64::try_from(schedule.arrival(next_id).unwrap().as_nanos()).unwrap();
            if intended_ns > now_ns {
                break;
            }
            peer.offer(Sample::offered(next_id, intended_ns, now_ns), now_ns);
            next_id += 1;
        }
        if tick == 10 {
            assert_eq!(peer.accepted, 10);
            assert_eq!(peer.pending.len(), 10);
            assert!(peer.completed.is_empty());
        }
        peer.poll_response(now_ns);
    }
    assert_eq!(peer.completed.len(), 10);
    assert!(peer.pending.is_empty());
    for sample in &peer.completed {
        assert_eq!(sample.end_to_end_ns(), Some(11_000_000));
    }
    println!("virtual_peer_pause: offered=10 accepted=10 acknowledged=10 pause_ns=10000000 end_to_end_each_ns=11000000");
}

#[test]
fn runtime_pause_catches_up_without_rewriting_intended_arrival() {
    let schedule = FixedRateSchedule::new(1_000).unwrap();
    // The client itself resumes at 10 ms: every overdue arrival keeps its
    // original appointment, even though acceptance is delayed until resume.
    for id in 0..10 {
        let intended = u64::try_from(schedule.arrival(id).unwrap().as_nanos()).unwrap();
        let mut sample = Sample::offered(id, intended, 10_000_000);
        sample
            .observe_enqueue(id, id + 1, 10_000_000, 10_000_100)
            .unwrap();
        sample.acknowledged_ns = Some(11_000_000);
        assert_eq!(sample.end_to_end_ns(), Some(11_000_000 - intended));
        assert_eq!(sample.enqueue_to_ack_upper_ns(), Some(1_000_000));
        assert_eq!(
            sample.offered_ns - sample.intended_ns,
            10_000_000 - intended
        );
    }
    println!("virtual_runtime_pause: intended_ns=0..9000000 resumed_ns=10000000 end_to_end_ns=11000000..2000000 enqueue_to_ack_upper_ns=1000000");
}

#[test]
fn enqueue_acceptance_requires_an_observed_single_record_counter_change() {
    let mut sample = Sample::offered(0, 0, 0);
    sample.observe_enqueue(7, 7, 10, 20).unwrap();
    assert_eq!(sample.enqueue_upper_ns, None);
    sample.observe_enqueue(7, 8, 30, 40).unwrap();
    assert_eq!(sample.enqueue_lower_ns, Some(30));
    assert_eq!(sample.enqueue_upper_ns, Some(40));
    assert!(sample.observe_enqueue(8, 9, 50, 60).is_err());
    assert!(Sample::offered(1, 0, 0)
        .observe_enqueue(7, 9, 10, 20)
        .is_err());
}

#[test]
fn all_percentiles_are_suppressed_below_floor_and_correct_at_floor() {
    let below = summarize((1..10_000).map(|us| us * 1_000), SAMPLE_FLOOR);
    for name in ["p50_us", "p95_us", "p99_us", "p99_9_us"] {
        assert!(below.contains(&format!("\"{name}\":null")), "{below}");
    }
    let at = summarize((1..=10_000).map(|us| us * 1_000), SAMPLE_FLOOR);
    for (name, value) in [
        ("p50_us", 5_000),
        ("p95_us", 9_500),
        ("p99_us", 9_900),
        ("p99_9_us", 9_990),
    ] {
        assert!(at.contains(&format!("\"{name}\":{value}")), "{at}");
    }
    assert!(summarize((1..=10_000).map(|us| us * 1_000), 20_000).contains("\"p99_us\":null"));
    assert!(summarize(std::iter::empty(), SAMPLE_FLOOR).contains("\"mean_us\":null"));
}

#[test]
fn invalid_load_configuration_fails_before_offering() {
    let mut cfg = config(0);
    assert!(cfg.validate().is_err());
    cfg.count = u64::MAX;
    assert!(cfg.validate().is_err());
    cfg.count = 1;
    cfg.max_pending = 0;
    assert!(cfg.validate().is_err());
    cfg.max_pending = 1;
    cfg.sample_floor = 9_999;
    assert!(cfg.validate().is_err());
    cfg.sample_floor = SAMPLE_FLOOR;
    cfg.max_block = Duration::ZERO;
    assert!(cfg.validate().is_err());
}

async fn warmed_producer(mock: &common::Mock, cfg: &Config) -> Producer {
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.batch_records = 1;
    pcfg.max_in_flight = 1;
    pcfg.buffer_memory = cfg.buffer_memory;
    pcfg.max_block = cfg.max_block;
    pcfg.delivery_timeout = cfg.delivery_timeout;
    pcfg.request_timeout = cfg.request_timeout;
    let producer = Producer::new(pcfg).await.unwrap();
    let _metadata = producer
        .send(
            ProduceRecord::to("t")
                .partition(0)
                .value(Bytes::from_static(b"warmup")),
        )
        .await
        .unwrap();
    producer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_mock_broker_does_not_stop_arrival_clock() {
    let mock = common::Mock::start().await;
    let cfg = config(40);
    let producer = warmed_producer(&mock, &cfg).await;
    mock.set_produce_delay_times(Duration::from_millis(100), 1);
    let report = latency::open_loop::run(&producer, "t", &Bytes::from_static(b"value"), &cfg)
        .await
        .unwrap();
    assert_eq!(report.samples.len(), 40);
    assert!(!report.failed());
    assert!(report.max_pending > 1);
    let first_ack = report
        .samples
        .iter()
        .filter_map(|s| s.acknowledged_ns)
        .min()
        .unwrap();
    let arrivals_during_stall = report
        .samples
        .iter()
        .filter(|s| s.offered_ns < first_ack)
        .count();
    assert!(
        arrivals_during_stall > 1,
        "a closed-loop driver would offer only one record during the stall"
    );
    for sample in &report.samples {
        assert!(sample.enqueue_lower_ns.is_some());
        assert!(sample.enqueue_upper_ns >= sample.enqueue_lower_ns);
        assert!(sample.acknowledged_ns >= sample.enqueue_upper_ns);
    }
    println!("mock_stall: offered=40 acknowledged=40 arrivals_before_first_ack={arrivals_during_stall} first_ack_ns={first_ack} max_pending={}", report.max_pending);
    report.print(&cfg, 5, 1, 0, 1);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn saturation_retains_rejected_offers_and_failed_disposition() {
    let mock = common::Mock::start().await;
    let mut cfg = config(20);
    cfg.max_pending = 2;
    let producer = warmed_producer(&mock, &cfg).await;
    mock.set_produce_delay_times(Duration::from_millis(100), 1);
    let report = latency::open_loop::run(&producer, "t", &Bytes::from_static(b"value"), &cfg)
        .await
        .unwrap();
    assert_eq!(report.samples.len(), 20);
    assert!(report.failed());
    assert_eq!(report.max_pending, 2);
    let rejected = report
        .samples
        .iter()
        .filter(|s| s.outcome == "rejected")
        .count();
    assert!(rejected > 0);
    for sample in report.samples.iter().filter(|s| s.outcome == "rejected") {
        assert_eq!(sample.enqueue_upper_ns, None);
        assert_eq!(sample.acknowledged_ns, None);
    }
    println!("mock_saturation: offered=20 rejected={rejected} max_pending=2");
    report.print(&cfg, 5, 1, 0, 1);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn delivery_timeouts_are_retained_separately_from_rejections() {
    let mock = common::Mock::start().await;
    let mut cfg = config(8);
    cfg.delivery_timeout = Duration::from_millis(25);
    cfg.request_timeout = Duration::from_millis(25);
    let producer = warmed_producer(&mock, &cfg).await;
    mock.set_produce_delay(Duration::from_millis(100));
    let report = latency::open_loop::run(&producer, "t", &Bytes::from_static(b"value"), &cfg)
        .await
        .unwrap();
    assert_eq!(report.samples.len(), 8);
    assert!(report.failed());
    assert!(report.samples.iter().any(|s| s.outcome == "timed_out"));
    assert!(report.samples.iter().all(|s| s.outcome != "rejected"));
    println!(
        "mock_timeouts: offered=8 timed_out={}",
        report
            .samples
            .iter()
            .filter(|s| s.outcome == "timed_out")
            .count()
    );
    report.print(&cfg, 5, 1, 0, 1);
    let _close = producer.close().await;
}

#[tokio::test]
async fn producer_buffer_backpressure_times_out_before_acceptance_without_hiding_offers() {
    let mock = common::Mock::start().await;
    let mut cfg = config(8);
    cfg.buffer_memory = 256;
    cfg.max_block = Duration::from_millis(25);
    let producer = warmed_producer(&mock, &cfg).await;
    mock.set_produce_delay_times(Duration::from_millis(100), 1);
    let report = latency::open_loop::run(&producer, "t", &Bytes::from(vec![b'x'; 100]), &cfg)
        .await
        .unwrap();
    assert_eq!(report.samples.len(), 8);
    assert!(report.failed());
    assert!(report
        .samples
        .iter()
        .any(|s| s.outcome == "timed_out" && s.enqueue_upper_ns.is_none()));
    assert!(report
        .samples
        .iter()
        .any(|s| s.outcome == "acknowledged" && s.enqueue_upper_ns.is_some()));
    assert!(report.samples.iter().all(|s| s.outcome != "rejected"));
    println!(
        "mock_buffer_backpressure: offered=8 unaccepted_timed_out={}",
        report
            .samples
            .iter()
            .filter(|s| s.outcome == "timed_out" && s.enqueue_upper_ns.is_none())
            .count()
    );
    report.print(&cfg, 100, 1, 0, 1);
    producer.close().await.unwrap();
}

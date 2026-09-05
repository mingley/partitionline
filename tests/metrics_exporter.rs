//! KL-07 Partial: optional Prometheus text exporter without a prom crate.
//!
//! Proves `examples/metrics/prom_format.rs` renders tip `ProducerMetrics` /
//! `ConsumerMetrics` snapshots as Prometheus exposition text. No live broker.
//! Does **not** add prometheus as a core dependency. Does **not** close full
//! KL-07 (no two-user diagnosis). Does **not** lift Suite HOLD.

#[path = "../examples/metrics/prom_format.rs"]
mod prom_format;

use partitionline::{ConsumerMetrics, LatencyStats, ProducerMetrics};
use prom_format::{format_consumer_prometheus, format_producer_prometheus};

#[test]
fn producer_prometheus_text_includes_acked_and_ack_p99() {
    let pm = ProducerMetrics {
        records_acked: 7,
        ack_latency: LatencyStats {
            count: 7,
            p99_nanos: 2_500_000_000,
            ..LatencyStats::default()
        },
        ..ProducerMetrics::default()
    };
    let text = format_producer_prometheus(&pm);
    assert!(
        text.contains("# TYPE partitionline_produce_records_acked counter"),
        "missing produce counter TYPE:\n{text}"
    );
    assert!(
        text.contains("partitionline_produce_records_acked 7\n"),
        "missing acked sample:\n{text}"
    );
    assert!(
        text.contains("partitionline_produce_ack_p99_seconds 2.5\n"),
        "missing ack p99 seconds:\n{text}"
    );
}

#[test]
fn consumer_prometheus_text_includes_fetch_rounds_and_p99() {
    let cm = ConsumerMetrics {
        fetch_rounds: 3,
        fetch_latency: LatencyStats {
            count: 3,
            p99_nanos: 500_000_000,
            ..LatencyStats::default()
        },
        ..ConsumerMetrics::default()
    };
    let text = format_consumer_prometheus(&cm);
    assert!(
        text.contains("# TYPE partitionline_fetch_rounds counter"),
        "missing fetch counter TYPE:\n{text}"
    );
    assert!(
        text.contains("partitionline_fetch_rounds 3\n"),
        "missing fetch rounds sample:\n{text}"
    );
    assert!(
        text.contains("partitionline_fetch_p99_seconds 0.5\n"),
        "missing fetch p99 seconds:\n{text}"
    );
}

#[test]
fn default_snapshots_render_zero_gauges_without_prom_crate() {
    let text = format!(
        "{}{}",
        format_producer_prometheus(&ProducerMetrics::default()),
        format_consumer_prometheus(&ConsumerMetrics::default())
    );
    assert!(text.contains("partitionline_produce_records_acked 0\n"));
    assert!(text.contains("partitionline_fetch_rounds 0\n"));
    assert!(!text.contains("prometheus_client"));
}

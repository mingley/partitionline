//! Minimal Prometheus text exposition for tip metrics snapshots.
//!
//! Shared by `examples/metrics` and `tests/metrics_exporter.rs`. No prometheus
//! crate — KL-07 asks for optional exporters as examples, not core deps.

use partitionline::{ConsumerMetrics, ProducerMetrics};
use std::fmt::Write;

/// Render a subset of [`ProducerMetrics`] as Prometheus text (UTF-8).
pub(crate) fn format_producer_prometheus(pm: &ProducerMetrics) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP partitionline_produce_records_acked Produce records acked"
    );
    let _ = writeln!(out, "# TYPE partitionline_produce_records_acked counter");
    let _ = writeln!(
        out,
        "partitionline_produce_records_acked {}",
        pm.records_acked
    );
    let _ = writeln!(
        out,
        "# HELP partitionline_produce_ack_p99_seconds Produce ack p99 latency"
    );
    let _ = writeln!(out, "# TYPE partitionline_produce_ack_p99_seconds gauge");
    let _ = writeln!(
        out,
        "partitionline_produce_ack_p99_seconds {}",
        (pm.ack_latency.p99_nanos as f64) / 1_000_000_000.0
    );
    out
}

/// Render a subset of [`ConsumerMetrics`] as Prometheus text (UTF-8).
pub(crate) fn format_consumer_prometheus(cm: &ConsumerMetrics) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP partitionline_fetch_rounds Fetch rounds completed"
    );
    let _ = writeln!(out, "# TYPE partitionline_fetch_rounds counter");
    let _ = writeln!(out, "partitionline_fetch_rounds {}", cm.fetch_rounds);
    let _ = writeln!(
        out,
        "# HELP partitionline_fetch_p99_seconds Fetch round p99 latency"
    );
    let _ = writeln!(out, "# TYPE partitionline_fetch_p99_seconds gauge");
    let _ = writeln!(
        out,
        "partitionline_fetch_p99_seconds {}",
        (cm.fetch_latency.p99_nanos as f64) / 1_000_000_000.0
    );
    out
}

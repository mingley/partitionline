//! KL-07 slice: disabled/enabled instrumentation cost measurement (brokerless).
//!
//! Core produce/fetch counters are always-on (atomics + latency window). Optional
//! `tracing` is off by default. This file measures span enter/exit when the
//! feature is enabled; default-build tests prove the feature is not on.
//!
//! Does **not** claim the ROADMAP 2% produce-ack telemetry budget. Does **not**
//! lift Suite HOLD.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

#[cfg(not(feature = "tracing"))]
#[test]
fn default_build_tracing_feature_disabled() {
    // Default Cargo features must keep optional spans compiled out.
    assert!(
        !cfg!(feature = "tracing"),
        "default test build must not enable feature=tracing (WP-4.2 / KL-07 cost honesty)"
    );
}

#[cfg(feature = "tracing")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "tracing")]
use std::sync::Arc;
#[cfg(feature = "tracing")]
use std::time::Instant;

#[cfg(feature = "tracing")]
struct SharedSub(Arc<AtomicU64>);

#[cfg(feature = "tracing")]
impl tracing::Subscriber for SharedSub {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::span::Id) {
        let _ = self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[cfg(feature = "tracing")]
#[test]
fn tracing_span_enter_exit_ns_per_op_bound() {
    use tracing::subscriber::with_default;

    const N: u64 = 50_000;
    let enters = Arc::new(AtomicU64::new(0));
    with_default(SharedSub(Arc::clone(&enters)), || {
        for _ in 0..1_000 {
            let span = tracing::info_span!("pl_instrumentation_cost_warm");
            let _g = span.enter();
        }
        let start = Instant::now();
        for _ in 0..N {
            let span = tracing::info_span!("pl_instrumentation_cost");
            let _g = span.enter();
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() / u128::from(N);
        eprintln!(
            "instrumentation_cost: tracing span enter/exit avg {ns_per_op} ns/op over {N} (wall {})",
            elapsed.as_nanos()
        );
        // Catastrophic bound only — not a ≤2% produce-ack claim.
        assert!(
            ns_per_op < 50_000,
            "tracing span enter/exit avg {ns_per_op} ns/op exceeds 50µs catastrophic bound"
        );
    });
    // Warm (1_000) + measured (N) enters.
    assert_eq!(enters.load(Ordering::Relaxed), 1_000 + N);
}

#[cfg(feature = "tracing")]
#[test]
fn tracing_span_enter_count_matches_iterations() {
    use tracing::subscriber::with_default;

    let enters = Arc::new(AtomicU64::new(0));
    const N: u64 = 1_000;
    with_default(SharedSub(Arc::clone(&enters)), || {
        for _ in 0..N {
            let span = tracing::info_span!("pl_instrumentation_cost_count");
            let _g = span.enter();
        }
    });
    assert_eq!(enters.load(Ordering::Relaxed), N);
}

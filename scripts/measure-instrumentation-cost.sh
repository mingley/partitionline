#!/usr/bin/env bash
# KL-07: measure always-on metrics + optional tracing instrumentation cost.
#
# Brokerless. Prints ns/op from lib + integration tests. Does **not** claim the
# ROADMAP 2% produce-ack telemetry budget. Does **not** lift Suite HOLD.
#
# Usage:
#   bash scripts/measure-instrumentation-cost.sh
#   bash scripts/measure-instrumentation-cost.sh --self-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

self_test() {
  fail() { echo "measure-instrumentation-cost --self-test: FAIL — $*" >&2; exit 1; }
  grep -qF 'Instrumentation cost (measured)' docs/guide.md \
    || fail "guide must document Instrumentation cost (measured)"
  grep -qF 'latency_tracker_record_ns_per_op_bound' src/metrics.rs \
    || fail "lib must keep latency_tracker_record_ns_per_op_bound"
  grep -qF 'default_build_tracing_feature_disabled' tests/instrumentation_cost.rs \
    || fail "tests/instrumentation_cost.rs must keep default_build_tracing_feature_disabled"
  grep -qF 'tracing_span_enter_exit_ns_per_op_bound' tests/instrumentation_cost.rs \
    || fail "tests/instrumentation_cost.rs must keep tracing_span_enter_exit_ns_per_op_bound"
  grep -qF 'measure-instrumentation-cost.sh' docs/ROADMAP.md \
    || fail "ROADMAP must reference this script"
  echo "measure-instrumentation-cost --self-test: ok"
  exit 0
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
fi

echo "== always-on metrics microbench (default features) =="
cargo test --lib latency_tracker_record_ns_per_op_bound -- --nocapture
cargo test --lib latency_tracker_snapshot_ns_per_op_bound -- --nocapture
cargo test --lib produce_counter_atomics_ns_per_op_bound -- --nocapture
cargo test --test instrumentation_cost default_build_tracing_feature_disabled -- --nocapture

echo "== optional tracing span cost (feature=tracing) =="
cargo test --features tracing --test instrumentation_cost tracing_span_enter_exit_ns_per_op_bound -- --nocapture
cargo test --features tracing --test instrumentation_cost tracing_span_enter_count_matches_iterations -- --nocapture

echo "measure-instrumentation-cost: ok (brokerless; not a 2% produce-ack claim; not Suite HOLD lift)"

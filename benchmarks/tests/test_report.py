"""Unit tests for the fail-closed benchmark result format and validator."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

# Allow importing benchmark-report.py from scripts/
import importlib.util

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "benchmark-report.py"

_spec = importlib.util.spec_from_file_location("benchmark_report", SCRIPT_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load benchmark_report from {SCRIPT_PATH}")
benchmark_report = importlib.util.module_from_spec(_spec)
sys.modules["benchmark_report"] = benchmark_report
_spec.loader.exec_module(benchmark_report)

BenchmarkValidator = benchmark_report.BenchmarkValidator


def build_valid_fixture() -> dict:
    """Build a complete, compliant benchmark result fixture for test assertions."""
    return {
        "schema_version": "1.0.0",
        "contract_version": "1.0.0",
        "suite_hold": {
            "status": "active",
            "policy": "Preserve Suite HOLD. Unsigned samples do not lift Suite HOLD.",
            "note": "Historical signoff rules stay; Lab A / Kernel Integrity required for qualification.",
        },
        "scenario": {
            "scenario_id": "matched-bulk-acks-all-6p-uncompressed",
            "profile": "bulk",
            "tier": "required",
            "peer": "partitionline",
            "cell_disposition": "executed",
            "equal_semantics": {
                "durability": {
                    "replication_factor": 1,
                    "min_insync_replicas": 1,
                },
                "acks": -1,
                "idempotence": True,
                "isolation": "read_uncommitted",
                "security": {
                    "protocol": "PLAINTEXT",
                    "mechanism": "NONE",
                },
            },
        },
        "provenance": {
            "source": {
                "git_commit": "e0ac7ff84752b0d87fa05a4ecb1a8d56221d8a1c",
                "git_branch": "mingley/kl04-02-benchmark-result-schema",
                "repo_url": "https://github.com/mingley/partitionline",
                "clean": True,
                "tree_hash": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
            },
            "binary": {
                "name": "bench_produce",
                "path": "target/release/examples/bench_produce",
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
            "config": {
                "path": "benchmarks/configs/matched-bulk.json",
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "effective_settings": {
                    "acks": -1,
                    "linger_ms": 5.0,
                    "batch_size_bytes": 1048576,
                    "max_in_flight": 5,
                    "idempotence": True,
                },
            },
            "toolchains": {
                "compiler": "rustc 1.80.0 (051478957 2024-07-21)",
                "runtime": "tokio 1.38.0",
                "build_tool": "cargo 1.80.0",
            },
            "broker": {
                "image": "apache/kafka:3.9.1",
                "version": "3.9.1",
                "mode": "kraft",
                "broker_commit": "bed63ae",
                "cluster_id": "4L622SOERtGEwb-FirstId",
                "node_count": 1,
                "endpoints": ["127.0.0.1:9092"],
            },
            "host": {
                "hostname": "lab-a-node-01",
                "os": "Linux 6.8.0-40-generic",
                "os_family": "linux",
                "kernel_version": "6.8.0-40-generic #40-Ubuntu SMP PREEMPT_DYNAMIC",
                "arch": "x86_64",
                "cpu": {
                    "model": "AMD EPYC 9654 96-Core Processor",
                    "physical_cores": 96,
                    "logical_cores": 192,
                    "frequency_mhz": 2400.0,
                },
                "memory": {
                    "total_bytes": 270282981376,
                    "unit": "bytes",
                },
            },
            "topology": {
                "environment": "loopback",
                "rtt_ms": 0.05,
                "rtt_unit": "milliseconds",
                "client_nodes": 1,
                "broker_nodes": 1,
                "network_interface": "lo",
            },
            "timestamps": {
                "start_time_utc": "2026-09-21T12:00:00Z",
                "end_time_utc": "2026-09-21T12:01:15Z",
                "duration_seconds": 75.0,
                "duration_unit": "seconds",
            },
            "seeds": {
                "payload_seed": "0x5EED0001",
                "key_seed": "0x5EED0002",
                "partition_seed": "0x5EED0003",
                "repetition_seed": "0x5EED0004",
            },
            "artifacts": [
                {
                    "path": "benchmarks/runs/run-01/raw_events.bin",
                    "type": "raw_events",
                    "sha256": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
                    "size_bytes": 800000000,
                },
                {
                    "path": "benchmarks/runs/run-01/high_watermarks.json",
                    "type": "offset_audit",
                    "sha256": "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
                    "size_bytes": 1024,
                },
            ],
        },
        "execution": {
            "phase": "steady_state",
            "warmup_completed": True,
            "warmup_records": 10000,
            "warmup_duration_seconds": 15.0,
            "steady_state_duration_seconds": 60.0,
            "repetition_index": 1,
            "total_repetitions": 5,
            "pairing_order": "A-B-B-A-A",
            "coordinated_omission_avoidance": {
                "enabled": True,
                "schedule_type": "open_loop_fixed_rate",
            },
        },
        "outcomes": {
            "offered": 8000000,
            "accepted": 8000000,
            "acknowledged": 8000000,
            "consumed": 0,
            "rejected": 0,
            "timed_out": 0,
            "unknown": 0,
        },
        "measurements": {
            "throughput": {
                "records_per_second": 133333.33,
                "records_per_second_unit": "records/s",
                "megabytes_per_second": 12.72,
                "megabytes_per_second_unit": "MB/s",
                "total_bytes_transferred": 800000000,
                "total_bytes_unit": "bytes",
            },
            "latency": {
                "sample_count": 8000000,
                "unit": "microseconds",
                "p50": 320.0,
                "p90": 450.0,
                "p95": 520.0,
                "p99": 680.0,
                "p99_9": 1150.0,
                "min": 180.0,
                "max": 2500.0,
                "mean": 345.0,
                "stddev": 85.0,
                "confidence_interval_95": {
                    "lower": 675.0,
                    "upper": 685.0,
                    "unit": "microseconds",
                },
                "raw_histogram": {
                    "bucket_unit": "microseconds",
                    "buckets": [
                        {"min_us": 0.0, "max_us": 500.0, "count": 7200000},
                        {"min_us": 500.0, "max_us": 1000.0, "count": 780000},
                        {"min_us": 1000.0, "max_us": 3000.0, "count": 20000},
                    ],
                },
            },
            "client_resources": {
                "cpu_utilization_pct": 85.5,
                "cpu_unit": "percent",
                "user_cpu_seconds": 42.0,
                "system_cpu_seconds": 9.3,
                "cpu_seconds_unit": "seconds",
                "allocations": {
                    "total_allocated_bytes": 1048576000,
                    "allocation_count": 520000,
                    "unit": "bytes",
                },
                "rss": {
                    "peak_rss_bytes": 157286400,
                    "average_rss_bytes": 125829120,
                    "unit": "bytes",
                },
                "threads_count": 8,
            },
            "broker_resources": {
                "cpu_utilization_pct": 145.2,
                "cpu_unit": "percent",
                "peak_rss_bytes": 1073741824,
                "rss_unit": "bytes",
                "disk_write_bytes": 840000000,
                "disk_write_unit": "bytes",
            },
            "errors": [],
        },
        "integrity": {
            "verified": True,
            "record_ids": {
                "start_id": 1,
                "end_id": 8000000,
                "expected_count": 8000000,
                "verified_count": 8000000,
                "missing_ids_count": 0,
                "duplicate_ids_count": 0,
                "checksum_algorithm": "crc32c",
                "payload_checksum_matches": True,
            },
            "high_watermark_audit": {
                "partitions": [
                    {"partition": 0, "start_offset": 0, "end_offset": 1333333, "offset_delta": 1333333},
                    {"partition": 1, "start_offset": 0, "end_offset": 1333333, "offset_delta": 1333333},
                    {"partition": 2, "start_offset": 0, "end_offset": 1333334, "offset_delta": 1333334},
                    {"partition": 3, "start_offset": 0, "end_offset": 1333333, "offset_delta": 1333333},
                    {"partition": 4, "start_offset": 0, "end_offset": 1333333, "offset_delta": 1333333},
                    {"partition": 5, "start_offset": 0, "end_offset": 1333334, "offset_delta": 1333334},
                ],
                "total_offset_delta": 8000000,
                "matches_acknowledged": True,
            },
            "idempotence_sequence_verified": True,
            "integrity_failure": False,
        },
        "repetition_history": {
            "total_attempts": 1,
            "failed_attempts": 0,
            "attempts": [
                {
                    "attempt_number": 1,
                    "repetition_index": 1,
                    "status": "passed_measurement",
                    "integrity_failure": False,
                    "error_message": None,
                    "timestamp_utc": "2026-09-21T12:00:00Z",
                }
            ],
        },
    }


class BenchmarkReportValidationTests(unittest.TestCase):
    """Test suite asserting fail-closed verification of benchmark result documents."""

    def setUp(self):
        self.schema_path = REPO_ROOT / "benchmarks" / "result-schema.json"
        self.validator = BenchmarkValidator(schema_path=self.schema_path)

    def test_schema_file_exists_and_valid_json(self):
        """Ensure benchmarks/result-schema.json is present and valid JSON."""
        self.assertTrue(self.schema_path.is_file(), "result-schema.json must exist")
        data = json.loads(self.schema_path.read_text(encoding="utf-8"))
        self.assertEqual(data.get("title"), "Partitionline Benchmark Result and Provenance Schema")
        self.assertIn("required", data)
        self.assertIn("provenance", data["required"])

    def test_valid_fixture_passes_validation(self):
        """Ensure a fully conforming result passes validation without errors."""
        fixture = build_valid_fixture()
        is_valid, errors, summary = self.validator.validate(fixture)
        self.assertTrue(is_valid, f"Expected valid fixture to pass, got errors: {errors}")
        self.assertEqual(len(errors), 0)
        self.assertEqual(summary["suite_hold"], "active")
        self.assertEqual(summary["total_acknowledged"], 8000000)
        self.assertTrue(summary["integrity_verified"])
        self.assertFalse(summary["has_retained_integrity_failure"])

    def test_no_empty_schema_success(self):
        """Assert empty dict or incomplete object is rejected (fail-closed)."""
        is_valid, errors, _ = self.validator.validate({})
        self.assertFalse(is_valid, "Empty schema must not pass validation")
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("Missing required top-level section" in e for e in errors))

    def test_reject_excellent_throughput_with_missing_record_ids(self):
        """The validator must reject a fixture with excellent throughput but missing record IDs."""
        fixture = build_valid_fixture()
        fixture["measurements"]["throughput"]["records_per_second"] = 500000.0

        # Case A: missing record IDs count > 0
        f_missing = copy.deepcopy(fixture)
        f_missing["integrity"]["record_ids"]["missing_ids_count"] = 42
        is_valid, errors, _ = self.validator.validate(f_missing)
        self.assertFalse(is_valid)
        self.assertTrue(any("missing 42 record IDs" in e for e in errors))

        # Case B: verified count < expected
        f_unverified = copy.deepcopy(fixture)
        f_unverified["integrity"]["record_ids"]["verified_count"] = 7999958
        is_valid, errors, _ = self.validator.validate(f_unverified)
        self.assertFalse(is_valid)
        self.assertTrue(any("verified record IDs" in e for e in errors))

        # Case C: record_ids block completely omitted
        f_no_rec = copy.deepcopy(fixture)
        del f_no_rec["integrity"]["record_ids"]
        is_valid, errors, _ = self.validator.validate(f_no_rec)
        self.assertFalse(is_valid)
        self.assertTrue(any("Missing required integrity.record_ids" in e for e in errors))

    def test_reject_mismatched_acks_scenario_vs_config(self):
        """Validator must reject when scenario equal_semantics acks differ from client config."""
        fixture = build_valid_fixture()
        # Scenario says acks=-1, but client config sets acks=1
        fixture["provenance"]["config"]["effective_settings"]["acks"] = 1
        is_valid, errors, _ = self.validator.validate(fixture)
        self.assertFalse(is_valid)
        self.assertTrue(any("Mismatched acks" in e for e in errors))

    def test_reject_mismatched_acks_high_watermark_audit(self):
        """Validator must reject when broker high-watermark delta does not match acked records."""
        fixture = build_valid_fixture()
        # Acknowledged says 8,000,000, but high-watermark delta is only 7,999,990
        fixture["integrity"]["high_watermark_audit"]["total_offset_delta"] = 7999990
        fixture["integrity"]["high_watermark_audit"]["matches_acknowledged"] = False
        is_valid, errors, _ = self.validator.validate(fixture)
        self.assertFalse(is_valid)
        self.assertTrue(any("high-watermark offset delta (7999990)" in e for e in errors))

    def test_reject_incompatible_units(self):
        """Reject incompatible units for latency, throughput, cpu, rss, or lag."""
        units_to_test = [
            ("measurements.throughput.records_per_second_unit", "items/sec", "records/s"),
            ("measurements.throughput.megabytes_per_second_unit", "GB/s", "MB/s"),
            ("measurements.latency.unit", "ms", "microseconds"),
            ("measurements.latency.unit", "seconds", "microseconds"),
            ("measurements.client_resources.cpu_unit", "cores", "percent"),
            ("measurements.client_resources.rss.unit", "MB", "bytes"),
            ("measurements.broker_resources.cpu_unit", "cores", "percent"),
            ("measurements.broker_resources.rss_unit", "MB", "bytes"),
        ]

        for path, bad_unit, expected in units_to_test:
            fixture = build_valid_fixture()
            keys = path.split(".")
            curr = fixture
            for k in keys[:-1]:
                curr = curr[k]
            curr[keys[-1]] = bad_unit

            is_valid, errors, _ = self.validator.validate(fixture)
            self.assertFalse(is_valid, f"Expected failure for {path} = {bad_unit}")
            self.assertTrue(
                any(f"must be '{expected}'" in e for e in errors),
                f"Missing expected error message for {path}: {errors}",
            )

    def test_reject_missing_required_measurements(self):
        """Reject results missing client/broker CPU, allocations, RSS, latency, throughput, or outcomes."""
        measurements_to_delete = [
            ("measurements.client_resources.cpu_utilization_pct", "cpu_utilization_pct"),
            ("measurements.client_resources.allocations", "allocations"),
            ("measurements.client_resources.rss", "rss"),
            ("measurements.broker_resources.cpu_utilization_pct", "cpu_utilization_pct"),
            ("measurements.broker_resources.peak_rss_bytes", "peak_rss_bytes"),
            ("measurements.latency.raw_histogram", "raw_histogram"),
            ("measurements.latency.p99", "p99"),
            ("measurements.throughput.records_per_second", "records_per_second"),
            ("outcomes.acknowledged", "acknowledged"),
        ]

        for path, key_name in measurements_to_delete:
            fixture = build_valid_fixture()
            keys = path.split(".")
            curr = fixture
            for k in keys[:-1]:
                curr = curr[k]
            del curr[keys[-1]]

            is_valid, errors, _ = self.validator.validate(fixture)
            self.assertFalse(is_valid, f"Expected failure when deleting {path}")
            self.assertTrue(
                any(key_name in e for e in errors),
                f"Expected error mentioning '{key_name}' in {errors}",
            )

    def test_reject_missing_provenance_hashes(self):
        """Reject results with missing or invalid source, binary, config, or artifact hashes."""
        # Invalid git commit
        f1 = build_valid_fixture()
        f1["provenance"]["source"]["git_commit"] = ""
        self.assertFalse(self.validator.validate(f1)[0])

        # Invalid binary sha256
        f2 = build_valid_fixture()
        f2["provenance"]["binary"]["sha256"] = "not-a-sha"
        self.assertFalse(self.validator.validate(f2)[0])

        # Missing toolchains
        f3 = build_valid_fixture()
        f3["provenance"]["toolchains"]["compiler"] = ""
        self.assertFalse(self.validator.validate(f3)[0])

        # Missing broker image
        f4 = build_valid_fixture()
        f4["provenance"]["broker"]["image"] = ""
        self.assertFalse(self.validator.validate(f4)[0])

        # Missing host CPU model
        f5 = build_valid_fixture()
        f5["provenance"]["host"]["cpu"]["model"] = ""
        self.assertFalse(self.validator.validate(f5)[0])

        # Missing artifacts
        f6 = build_valid_fixture()
        f6["provenance"]["artifacts"] = []
        self.assertFalse(self.validator.validate(f6)[0])

    def test_all_seven_outcomes_strictly_required(self):
        """All 7 outcome categories must be explicitly present and non-negative."""
        required = ["offered", "accepted", "acknowledged", "consumed", "rejected", "timed_out", "unknown"]
        for outcome_key in required:
            f = build_valid_fixture()
            del f["outcomes"][outcome_key]
            is_valid, errors, _ = self.validator.validate(f)
            self.assertFalse(is_valid, f"Expected rejection when '{outcome_key}' is missing")
            self.assertTrue(any(f"Missing required outcome category: '{outcome_key}'" in e for e in errors))

    def test_outcome_accounting_semantics(self):
        """Accepted cannot exceed offered; acknowledged cannot exceed accepted."""
        # Acknowledged exceeds accepted
        f1 = build_valid_fixture()
        f1["outcomes"]["acknowledged"] = 9000000
        f1["outcomes"]["accepted"] = 8000000
        is_valid, errors, _ = self.validator.validate(f1)
        self.assertFalse(is_valid)
        self.assertTrue(any("acknowledged (9000000) cannot exceed accepted" in e for e in errors))

        # Fire and forget with acknowledgments
        f2 = build_valid_fixture()
        f2["scenario"]["equal_semantics"]["acks"] = 0
        f2["provenance"]["config"]["effective_settings"]["acks"] = 0
        f2["outcomes"]["acknowledged"] = 500
        is_valid2, errors2, _ = self.validator.validate(f2)
        self.assertFalse(is_valid2)
        self.assertTrue(any("acks=0 (fire-and-forget)" in e for e in errors2))

    def test_idempotent_max_in_flight_protocol_bound(self):
        """With idempotence=true, max.in.flight must be bounded <= 5 per Kafka protocol."""
        f = build_valid_fixture()
        f["provenance"]["config"]["effective_settings"]["max_in_flight"] = 10
        is_valid, errors, _ = self.validator.validate(f)
        self.assertFalse(is_valid)
        self.assertTrue(any("max.in.flight (10) exceeds 5 while idempotence=true" in e for e in errors))

    def test_payload_checksum_mismatch_rejected(self):
        """Integrity failure when payload checksum does not match."""
        f = build_valid_fixture()
        f["integrity"]["record_ids"]["payload_checksum_matches"] = False
        is_valid, errors, _ = self.validator.validate(f)
        self.assertFalse(is_valid)
        self.assertTrue(any("payload_checksum_matches is false" in e for e in errors))

    def test_consumer_lag_validation(self):
        """Validate consumer lag structure and units when present."""
        f = build_valid_fixture()
        f["measurements"]["consumer_lag"] = {
            "records_lag": 150,
            "lag_unit": "records",
            "partition_lags": [{"partition": 0, "lag": 50}, {"partition": 1, "lag": 100}],
        }
        is_valid, errors, _ = self.validator.validate(f)
        self.assertTrue(is_valid, f"Expected valid consumer lag to pass: {errors}")

        # Wrong lag unit
        f_bad = copy.deepcopy(f)
        f_bad["measurements"]["consumer_lag"]["lag_unit"] = "bytes"
        is_valid_bad, errors_bad, _ = self.validator.validate(f_bad)
        self.assertFalse(is_valid_bad)
        self.assertTrue(any("Consumer lag unit mismatch" in e for e in errors_bad))

    def test_suite_hold_active_enforced(self):
        """Suite HOLD must remain active; a result file cannot claim cell disposition passed."""
        # Status lifted
        f_lifted = build_valid_fixture()
        f_lifted["suite_hold"]["status"] = "lifted"
        is_valid, errors, _ = self.validator.validate(f_lifted)
        self.assertFalse(is_valid)
        self.assertTrue(any("Suite HOLD violation" in e for e in errors))

        # Cell disposition passed
        f_passed = build_valid_fixture()
        f_passed["scenario"]["cell_disposition"] = "passed"
        is_valid, errors, _ = self.validator.validate(f_passed)
        self.assertFalse(is_valid)
        self.assertTrue(any("Disposition violation" in e for e in errors))

    def test_preserve_failed_attempts_no_rerun_erasure(self):
        """Preserve failed attempts; a later success does not erase an earlier integrity failure."""
        fixture = build_valid_fixture()
        # Repetition history records Attempt #1 failed with integrity failure, Attempt #2 succeeded
        fixture["repetition_history"]["total_attempts"] = 2
        fixture["repetition_history"]["failed_attempts"] = 1
        fixture["repetition_history"]["attempts"] = [
            {
                "attempt_number": 1,
                "repetition_index": 1,
                "status": "failed_integrity",
                "integrity_failure": True,
                "error_message": "Sequence gap at record 4105; broker acked out of order",
                "timestamp_utc": "2026-09-21T11:45:00Z",
            },
            {
                "attempt_number": 2,
                "repetition_index": 1,
                "status": "passed_measurement",
                "integrity_failure": False,
                "error_message": None,
                "timestamp_utc": "2026-09-21T12:00:00Z",
            },
        ]

        # In standard mode, the validator records that a retained integrity failure exists
        is_valid, errors, summary = self.validator.validate(fixture, require_clean=False)
        self.assertTrue(is_valid)
        self.assertTrue(summary["has_retained_integrity_failure"])

        # If someone tries to erase the failed attempt count (failed_attempts=0)
        f_erased = copy.deepcopy(fixture)
        f_erased["repetition_history"]["failed_attempts"] = 0
        is_valid_erased, errors_erased, _ = self.validator.validate(f_erased)
        self.assertFalse(is_valid_erased)
        self.assertTrue(any("Rerun accounting violation" in e for e in errors_erased))

        # Under strict qualification (require_clean=True), earlier integrity failure blocks qualification
        is_valid_clean, errors_clean, _ = self.validator.validate(fixture, require_clean=True)
        self.assertFalse(is_valid_clean)
        self.assertTrue(any("earlier integrity failure" in e for e in errors_clean))

    def test_cli_execution(self):
        """Test invoking scripts/benchmark-report.py as CLI subprocess."""
        script_path = REPO_ROOT / "scripts" / "benchmark-report.py"

        fixture = build_valid_fixture()
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
            json.dump(fixture, tf)
            valid_file = tf.name

        try:
            # Valid execution (exit 0)
            res = subprocess.run(
                [sys.executable, str(script_path), valid_file],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(res.returncode, 0, f"CLI stderr: {res.stderr}")
            self.assertIn("PARTITIONLINE BENCHMARK RESULT", res.stdout)
            self.assertIn("VALIDATION PASSED", res.stdout)

            # JSON flag
            res_json = subprocess.run(
                [sys.executable, str(script_path), "--json", valid_file],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(res_json.returncode, 0)
            doc = json.loads(res_json.stdout)
            self.assertTrue(doc["valid"])
            self.assertEqual(doc["error_count"], 0)

            # Invalid execution: break acks
            invalid_fixture = copy.deepcopy(fixture)
            invalid_fixture["provenance"]["config"]["effective_settings"]["acks"] = 1
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf2:
                json.dump(invalid_fixture, tf2)
                invalid_file = tf2.name

            try:
                res_bad = subprocess.run(
                    [sys.executable, str(script_path), invalid_file],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(res_bad.returncode, 1)
                self.assertIn("Mismatched acks", res_bad.stdout)
                self.assertIn("VALIDATION FAILED", res_bad.stdout)
            finally:
                Path(invalid_file).unlink(missing_ok=True)
        finally:
            Path(valid_file).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()

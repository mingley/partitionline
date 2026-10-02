"""Benchmarks require complete ID/hash histories even when aggregate totals match."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("bench_record_history", ROOT / "scripts/bench-record-history.py")
BENCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BENCH
SPEC.loader.exec_module(BENCH)


def fixture(acks=1):
    seed, size, count = 0x5EED0001, 100, 3
    config = {"kind": "config", "schema_version": 1, "role": "producer", "topic": "test", "acks": acks, "idempotent": False, "transactional": False, "seed": seed, "payload_bytes": size, "partitions": 1, "count": count}
    rows = [{"kind": "record", "id": f"{seed:016x}:{i}", "topic": "test", "partition": 0, "offset": None, "key": f"plbench-{seed:016x}-0".encode().hex(), "payload_hash": hashlib.sha256(BENCH.payload(seed, i, size)).hexdigest(), "payload_bytes": size, "phase": "measure", "status": "accepted"} for i in range(count)]
    producer = [config, *rows, {"kind": "summary", "role": "producer", "completed": True, "run_disposition": "executed", "accepted_total": count, "acknowledged_total": count if acks else 0, "locally_completed_total": 0 if acks else count, "warmup_records": 0, "measured_records": count, "produce_errors": 0, "queue_full_attempts": 0}]
    consumer = [{**config, "role": "consumer", "isolation_level": "read_uncommitted", "end_offsets": [{"partition": 0, "offset": count}]}, *[{**r, "offset": i, "phase": "consume"} for i, r in enumerate(rows)], {"kind": "summary", "role": "consumer", "completed": True, "run_disposition": "executed", "consumed": count, "verified": count, "verify_mismatches": 0, "unique_ids": count}]
    return producer, consumer


def write_rows(path, rows):
    Path(path).write_text("".join(json.dumps(row) + "\n" for row in rows))


class BenchHistoryTests(unittest.TestCase):
    def verify(self, producer, consumer):
        with tempfile.TemporaryDirectory() as temporary:
            p, c = Path(temporary) / "producer.jsonl", Path(temporary) / "consumer.jsonl"
            write_rows(p, producer)
            write_rows(c, consumer)
            return BENCH.verify(p, c)

    def test_independent_generator_matches_rust_golden(self):
        self.assertEqual(hashlib.sha256(BENCH.payload(0x5eed0001, 17, 100)).hexdigest(), "8f6fe2334395dee27d62e4c6123c0f9149f886cb49467965a72dc3cb60439a8c")

    def test_complete_history_passes_without_qualification(self):
        _, result = self.verify(*fixture())
        self.assertTrue(result["valid"])
        self.assertFalse(result["qualification"])
        self.assertEqual(result["suite_hold"], "active")

    def test_matching_count_missing_duplicate_swap_fails(self):
        p, c = fixture()
        c[2] = {**c[1], "offset": 1}
        c[-1]["unique_ids"] = 2
        _, result = self.verify(p, c)
        self.assertFalse(result["valid"])
        self.assertTrue(result["performance_claims_invalidated"])
        self.assertIn("SYNTHETIC_SWAP", {v["type"] for v in result["violations"]})

    def test_matching_count_payload_corruption_fails(self):
        p, c = fixture()
        c[2]["payload_hash"] = "0" * 64
        _, result = self.verify(p, c)
        self.assertIn("PAYLOAD_CORRUPTION", {v["type"] for v in result["violations"]})
        self.assertFalse(result["valid"])

    def test_per_key_reordering_with_increasing_offsets_fails(self):
        p, c = fixture()
        c[1], c[2] = c[2], c[1]
        c[1]["offset"], c[2]["offset"] = 0, 1
        _, result = self.verify(p, c)
        self.assertIn("KEY_ORDERING_VIOLATION", {v["type"] for v in result["violations"]})

    def test_control_offsets_are_gaps_not_application_records(self):
        p, c = fixture()
        p[0]["transactional"] = True
        c[0]["end_offsets"][0]["offset"] = 6
        for i, row in enumerate(c[1:-1]):
            row["offset"] = i * 2
        _, result = self.verify(p, c)
        self.assertTrue(result["valid"])
        self.assertEqual(result["consumed_count"], 3)

    def test_exposed_control_marker_is_a_correctness_failure(self):
        p, c = fixture()
        c[1]["is_control_record"] = True
        _, result = self.verify(p, c)
        self.assertIn("CONTROL_RECORD_EXPOSURE", {v["type"] for v in result["violations"]})

    def test_acks0_local_completion_is_never_promoted_to_acknowledged(self):
        history, result = self.verify(*fixture(0))
        self.assertTrue(result["valid"])
        self.assertFalse(result["acknowledged_throughput"])
        self.assertTrue(all(r["status"] == "accepted" for r in history["attempted"]))

    def test_acks0_requires_full_ids_for_benchmark_receipt_gate(self):
        p, c = fixture(0)
        del c[2]
        c[-1].update(consumed=2, verified=2, unique_ids=2)
        _, result = self.verify(p, c)
        self.assertFalse(result["valid"])
        self.assertIn("BENCHMARK_ID_SET_MISMATCH", {v["type"] for v in result["violations"]})

    def test_acks0_acknowledgment_mislabel_fails_closed(self):
        p, c = fixture(0)
        p[-1]["acknowledged_total"] = 3
        with self.assertRaises(BENCH.InvalidJournal):
            self.verify(p, c)

    def test_partial_and_failed_histories_fail_closed(self):
        for key, value in (("completed", False), ("run_disposition", "failed")):
            p, c = fixture()
            p[-1][key] = value
            with self.assertRaises(BENCH.InvalidJournal):
                self.verify(p, c)
        p, c = fixture()
        with self.assertRaises(BENCH.InvalidJournal):
            self.verify(p[:-1], c)

    def test_zero_or_malformed_count_and_missing_hash_fail_closed(self):
        for count in (0, "3", True):
            p, c = fixture()
            p[0]["count"] = count
            with self.assertRaises(BENCH.InvalidJournal):
                self.verify(p, c)
        p, c = fixture()
        del c[1]["payload_hash"]
        with self.assertRaises(BENCH.InvalidJournal):
            self.verify(p, c)

    def test_producer_hash_is_checked_against_independent_generator(self):
        p, c = fixture()
        p[1]["payload_hash"] = c[1]["payload_hash"] = "0" * 64
        with self.assertRaises(BENCH.InvalidJournal):
            self.verify(p, c)

    def test_failed_verdict_is_retained_and_cannot_be_overwritten(self):
        p, c = fixture()
        c[2] = {**c[1], "offset": 1}
        c[-1]["unique_ids"] = 2
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_rows(root / "p.jsonl", p)
            write_rows(root / "c.jsonl", c)
            raw = (root / "c.jsonl").read_bytes()
            args = [sys.executable, str(ROOT / "scripts/bench-record-history.py"), "--producer", str(root / "p.jsonl"), "--consumer", str(root / "c.jsonl"), "--output", str(root / "verdict.json")]
            first = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(first.returncode, 1, first.stderr)
            before = (root / "verdict.json").read_bytes()
            second = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(second.returncode, 2)
            self.assertEqual(before, (root / "verdict.json").read_bytes())
            self.assertEqual(raw, (root / "c.jsonl").read_bytes())

    def test_harness_rejects_invalid_settings_before_broker_mutation(self):
        for key, value in (("COUNT", "0"), ("COUNT", "garbage"), ("RUNS", "0"), ("PARTITIONS", "0"), ("PAYLOAD_BYTES", "23"), ("ACKS", "0"), ("WARMUP_SECS", "1")):
            env = os.environ | {"COUNT": "3", "RUNS": "1", "PARTITIONS": "1", "ACKS": "1", "WARMUP_SECS": "0", key: value}
            result = subprocess.run(["bash", str(ROOT / "scripts/lab-a-integrity.sh")], env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2, (key, value, result.stdout, result.stderr))

    def test_harness_preserves_matching_count_failure_and_raw_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "bin"
            binary.mkdir()
            p, c = fixture()
            c[2] = {**c[1], "offset": 1}
            c[-1]["unique_ids"] = 2
            write_rows(root / "p.jsonl", p)
            write_rows(root / "c.jsonl", c)
            scripts = {
                "kafka-topics.sh": "#!/bin/sh\nexit 0\n",
                "kafka-get-offsets.sh": '#!/bin/sh\nif [ -f "$MOCK_PRODUCED" ]; then echo test:0:3; else echo test:0:0; fi\n',
                "produce": '#!/bin/sh\ncp "$MOCK_P" "$RECORD_HISTORY"\ntouch "$MOCK_PRODUCED"\necho \'{"acked":3,"acks":1,"run_disposition":"executed"}\'\n',
                "fetch": '#!/bin/sh\ncp "$MOCK_C" "$RECORD_HISTORY"\necho \'{"consumed":3,"run_disposition":"executed"}\'\n',
            }
            for name, source in scripts.items():
                (binary / name).write_text(source)
                (binary / name).chmod(0o755)
            artifacts = root / "artifacts"
            env = os.environ | {"PATH": str(binary) + os.pathsep + os.environ["PATH"], "KAFKA_HOME": str(root), "BROKER_BACKEND": "native", "KAFKA_BOOTSTRAP": "127.0.0.1:29999", "TOPIC": "test", "COUNT": "3", "RUNS": "1", "PARTITIONS": "1", "PAYLOAD_BYTES": "100", "ACKS": "1", "WARMUP_SECS": "0", "SKIP_TOPIC_RESET": "1", "ARTIFACT_DIR": str(artifacts), "BENCH_PRODUCE_BINARY": str(binary / "produce"), "BENCH_FETCH_BINARY": str(binary / "fetch"), "MOCK_P": str(root / "p.jsonl"), "MOCK_C": str(root / "c.jsonl"), "MOCK_PRODUCED": str(root / "produced")}
            result = subprocess.run(["bash", str(ROOT / "scripts/lab-a-integrity.sh")], env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            verdict = json.loads((artifacts / "run-1/verdict.json").read_text())
            self.assertFalse(verdict["valid"])
            self.assertTrue(verdict["performance_claims_invalidated"])
            self.assertEqual((artifacts / "run-1/consumer.jsonl").read_bytes(), (root / "c.jsonl").read_bytes())
            self.assertTrue((artifacts / "SHA256SUMS").exists())
            status = json.loads((artifacts / "run-status.json").read_text())
            self.assertFalse(status["integrity_verified"])
            before = (artifacts / "run-1/verdict.json").read_bytes()
            rerun = subprocess.run(["bash", str(ROOT / "scripts/lab-a-integrity.sh")], env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(rerun.returncode, 0)
            self.assertEqual(before, (artifacts / "run-1/verdict.json").read_bytes())


if __name__ == "__main__":
    unittest.main()

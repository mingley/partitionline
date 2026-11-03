import copy
import importlib.util
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace
from pathlib import Path

spec = importlib.util.spec_from_file_location("drain_harness", Path(__file__).with_name("run.py"))
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def inputs(case_name="one_record"):
    case = harness.CASES[case_name]
    count, warmup = int(case["COUNT"]), int(harness.COMMON_ENV["WARMUP"])
    report = {
        "run_disposition": "executed", "acks": 1, "accepted": count, "acked": count,
        "warmup_records": warmup, "accepted_total": count + warmup,
        "acknowledged_total": count + warmup, "integrity_verified": False,
        "performance_claims_valid": False, "elapsed_s": 1.0,
        "effective_settings": {
            "acks": 1, "linger_ms": int(case["LINGER_MS"]), "batch_size_bytes": 1048576,
            "batch_records": int(case["BATCH_RECORDS"]), "max_in_flight": int(case["MAX_IN_FLIGHT"]),
            "idempotence": False, "connections": 1, "payload_bytes": 100, "compression": "none",
            "explicit_partitions": int(case["PARTITIONS"]), "requested_records": count,
            "payload_mode": "seeded", "key_mode": "id", "record_seed": 1592590337,
            "buffer_memory_bytes": 33554432, "max_request_size": 1048576,
            "delivery_timeout_ms": 30000, "run_timeout_ms": 600000,
            "warmup_requested_records": warmup, "warmup_min_duration_ms": 0, "measure_duration_ms": 0,
        },
    }
    broker = {
        "label": "client-ceiling", "accepted_records": count + warmup,
        "validation_failures": {k: 0 for k in ("framing", "crc", "count", "sequence", "transactional")},
        "injected_errors": 0, "leader_mismatches": 0,
        "end_offsets": {f"drain-census/{p}": warmup // int(case["PARTITIONS"]) + int(p < warmup % int(case["PARTITIONS"]))
                        + count // int(case["PARTITIONS"]) + int(p < count % int(case["PARTITIONS"]))
                        for p in range(int(case["PARTITIONS"]))},
    }
    census = {"kind": "full_drain_census", "counts": [10, 0, 10, 6, 4, 11, 2, 4, 19, 5, 1, 2, 1, 0, 0, 0]}
    return case, report, broker, census


class ResultValidation(unittest.TestCase):
    def test_valid_timing_and_diagnostic_are_separate(self):
        case, report, broker, census = inputs()
        self.assertEqual(harness.verify_attempt(report, broker, case, False, None), [])
        self.assertEqual(harness.verify_attempt(report, broker, case, True, census), [])
        self.assertTrue(harness.verify_attempt(report, broker, case, False, census))
        self.assertTrue(harness.verify_attempt(report, broker, case, True, None))

    def test_rejects_ack_or_settings_mismatch(self):
        case, report, broker, _ = inputs("bulk")
        for field in ("acked", "accepted", "warmup_records", "acknowledged_total"):
            changed = copy.deepcopy(report)
            changed[field] += 1
            self.assertTrue(harness.verify_attempt(changed, broker, case, False, None), field)
        changed = copy.deepcopy(report)
        changed["effective_settings"]["batch_records"] += 1
        self.assertTrue(harness.verify_attempt(changed, broker, case, False, None))

    def test_rejects_missing_counts_and_broker_errors(self):
        case, report, broker, _ = inputs()
        for field in ("accepted_records", "validation_failures", "end_offsets"):
            changed = copy.deepcopy(broker)
            del changed[field]
            self.assertTrue(harness.verify_attempt(report, changed, case, False, None), field)
        for kind in broker["validation_failures"]:
            changed = copy.deepcopy(broker)
            changed["validation_failures"][kind] = 1
            self.assertTrue(harness.verify_attempt(report, changed, case, False, None), kind)
        changed = copy.deepcopy(broker)
        changed["end_offsets"]["drain-census/0"] -= 1
        self.assertTrue(harness.verify_attempt(report, changed, case, False, None))
        bulk, bulk_report, bulk_broker, _ = inputs("bulk")
        changed = copy.deepcopy(bulk_broker)
        changed["end_offsets"]["drain-census/0"] += 1
        changed["end_offsets"]["drain-census/1"] -= 1
        self.assertEqual(sum(changed["end_offsets"].values()), sum(bulk_broker["end_offsets"].values()))
        self.assertTrue(harness.verify_attempt(bulk_report, changed, bulk, False, None))

    def test_rejects_false_claims_and_bad_census(self):
        case, report, broker, census = inputs()
        for field in ("performance_claims_valid", "integrity_verified"):
            changed = copy.deepcopy(report)
            changed[field] = True
            self.assertTrue(harness.verify_attempt(changed, broker, case, False, None), field)
        for index in (0, 3, 4, 6, 10):
            changed = copy.deepcopy(census)
            changed["counts"][index] += 1
            self.assertTrue(harness.verify_attempt(report, broker, case, True, changed), index)

    def test_paired_order_is_reproducible_and_complete(self):
        plan = harness.trial_order()
        self.assertEqual(plan, harness.trial_order())
        for name in harness.CASES:
            for pair in range(5):
                self.assertEqual({arm for case, p, arm in plan if case == name and p == pair}, {"baseline", "candidate"})
        self.assertEqual(len(plan), len(harness.CASES) * 10)

    def test_rejects_missing_or_failed_test_controls(self):
        complete = "\n".join(f"test producer::tests::{name} ... ok" for name in harness.REQUIRED_TESTS)
        self.assertEqual(harness.missing_test_successes(complete), [])
        partial = complete.replace(" ... ok", " ... FAILED", 1)
        self.assertEqual(harness.missing_test_successes(partial), [harness.REQUIRED_TESTS[0]])
        self.assertEqual(harness.missing_test_successes("0 tests"), list(harness.REQUIRED_TESTS))

    def test_disk_floor_and_running_process_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            with mock.patch.object(harness.shutil, "disk_usage", return_value=SimpleNamespace(free=harness.STOP_FREE_BYTES)):
                harness.check_free([path])
                with self.assertRaises(RuntimeError):
                    harness.check_free([path], reserve=1)
            with mock.patch.object(harness.shutil, "disk_usage", return_value=SimpleNamespace(free=harness.STOP_FREE_BYTES - 1)):
                with self.assertRaises(RuntimeError):
                    harness.check_free([path])
            task = mock.Mock()
            task.finished.return_value = False
            with mock.patch.object(harness, "check_free", side_effect=RuntimeError("floor")):
                self.assertEqual(harness.wait_monitored(task, 30, [path], {}), "disk_stop")

    def test_phase_boundary_rejects_late_acknowledgements(self):
        case, _, _, _ = inputs()
        warmup = int(harness.COMMON_ENV["WARMUP"])
        total = warmup + int(case["COUNT"])
        phases = [
            {"kind": "phase_end", "phase": "warmup", "accepted_total": warmup, "acknowledged_total": warmup, "produce_errors": 0},
            {"kind": "phase_end", "phase": "measure", "accepted_total": total, "acknowledged_total": total, "produce_errors": 0},
        ]
        self.assertEqual(harness.phase_errors(phases, case), [])
        late = copy.deepcopy(phases)
        late[1]["acknowledged_total"] -= 1
        self.assertTrue(harness.phase_errors(late, case))
        self.assertTrue(harness.phase_errors(phases[:1], case))

    def test_exact_owned_paths_and_helper_pin(self):
        self.assertEqual(harness.digest(harness.ASSETS / "candidate-command-ownership.py"), harness.HELPER_SHA)
        self.assertTrue(callable(harness.load_owner().owned_command))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "workspace"
            temp = root / "temp"
            workspace.mkdir()
            temp.mkdir()
            out = temp / "full-drain-results"
            out.mkdir()
            work = temp / "full-drain-work"
            env = {"GITHUB_WORKSPACE": str(workspace), "RUNNER_TEMP": str(temp)}
            harness.owned_paths(workspace, work, out, env)
            with self.assertRaises(RuntimeError):
                harness.owned_paths(workspace, temp / "wrong", out, env)
            with self.assertRaises(RuntimeError):
                harness.owned_paths(root, work, out, env)
            work.mkdir()
            with self.assertRaises(RuntimeError):
                harness.owned_paths(workspace, work, out, env)

    def test_instrumentation_requires_unique_anchors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "examples").mkdir()
            (root / "src/producer.rs").write_text(harness.HELPER + harness.BASE_BODY + "}\n")
            (root / "examples/bench_produce.rs").write_text('        let mut pending_record = None;\n        if failure.is_some() {\n            break;\n')
            harness.instrument(root)
            producer = (root / "src/producer.rs").read_text()
            example = (root / "examples/bench_produce.rs").read_text()
            self.assertEqual(producer.count("    pl_full_drain_observe(spare, pending, n);"), 1)
            self.assertEqual(example.count("__pl_full_drain_census_reset()"), 1)
            self.assertEqual(example.count("__pl_full_drain_census_snapshot()"), 1)
            with self.assertRaises(RuntimeError):
                harness.instrument(root)


if __name__ == "__main__":
    unittest.main()

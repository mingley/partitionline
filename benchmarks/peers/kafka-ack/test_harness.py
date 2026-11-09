"""Synthetic controls for benchmark sampling and run orchestration.

These tests do not read /proc or contact Kafka.
"""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("socket_sample", HERE / "socket_sample.py")
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)
spec = importlib.util.spec_from_file_location("check_sample", HERE / "check_sample.py")
check_sample = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_sample)


class SamplerTests(unittest.TestCase):
    def test_permission_error_preserves_partial_data_and_stops(self):
        with tempfile.TemporaryDirectory() as name:
            output = Path(name) / "sockets.json"
            with mock.patch.object(sys, "argv", ["sampler", "42", "19092", str(output)]), \
                 mock.patch.object(sampler.Path, "stat", side_effect=[None, None]), \
                 mock.patch.object(sampler, "sockets", side_effect=[2, PermissionError("synthetic denial")]) as read, \
                 mock.patch.object(sampler.time, "sleep"), \
                 contextlib.redirect_stderr(io.StringIO()):
                status = sampler.main()
            data = json.loads(output.read_text())
            self.assertEqual(status, 1)
            self.assertEqual(read.call_count, 2, "the denied read must not be retried")
            self.assertEqual(data["successful_samples"], 1)
            self.assertEqual(data["counts"], {"2": 1})
            self.assertEqual(data["max_established"], 2)
            self.assertIs(data["complete"], False)
            self.assertEqual(data["error"]["type"], "PermissionError")
            self.assertIn("synthetic denial", data["error"]["message"])
            (Path(name) / "socket-sampler-exit.txt").write_text("1\n")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                check_sample.load_complete_sample(name)

    def test_permission_error_checking_pid_preserves_prior_sample(self):
        with tempfile.TemporaryDirectory() as name:
            output = Path(name) / "sockets.json"
            with mock.patch.object(sys, "argv", ["sampler", "42", "19092", str(output)]), \
                 mock.patch.object(sampler.Path, "stat", side_effect=[None, PermissionError("synthetic stat denial")]) as stat, \
                 mock.patch.object(sampler, "sockets", return_value=1) as read, \
                 mock.patch.object(sampler.time, "sleep"), \
                 contextlib.redirect_stderr(io.StringIO()):
                status = sampler.main()
            data = json.loads(output.read_text())
            self.assertEqual(status, 1)
            self.assertEqual(stat.call_count, 2)
            self.assertEqual(read.call_count, 1)
            self.assertEqual(data["successful_samples"], 1)
            self.assertIs(data["complete"], False)
            self.assertEqual(data["error"]["type"], "PermissionError")

    def test_successful_samples_pass_and_summary_tamper_fails(self):
        with tempfile.TemporaryDirectory() as name:
            output = Path(name) / "sockets.json"
            with mock.patch.object(sys, "argv", ["sampler", "42", "19092", str(output)]), \
                 mock.patch.object(sampler.Path, "stat", side_effect=[None, None, FileNotFoundError()]), \
                 mock.patch.object(sampler, "sockets", side_effect=[0, 1]), \
                 mock.patch.object(sampler.time, "sleep"):
                status = sampler.main()
            self.assertEqual(status, 0)
            (Path(name) / "socket-sampler-exit.txt").write_text("0\n")
            data = check_sample.load_complete_sample(name)
            self.assertIs(data["complete"], True)
            (Path(name) / "socket-sampler-exit.txt").write_text("1\n")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                check_sample.load_complete_sample(name)
            (Path(name) / "socket-sampler-exit.txt").write_text("0\n")
            data["counts"] = {"1": 2}
            output.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "summary differs"):
                check_sample.load_complete_sample(name)


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nset -eu\n" + body)
    path.chmod(0o755)


class RunOneTests(unittest.TestCase):
    def run_case(self, driver_status, sampler_status):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            bins = root / "bin"
            evidence = root / "evidence"
            evidence.mkdir()
            executable(root / "target/release/examples/bench_kafka_paired", 'exit "$FAKE_DRIVER_STATUS"\n')
            executable(bins / "timeout", 'shift\nexec "$@"\n')
            executable(bins / "taskset", '[ "$1" = "-c" ]\nshift 2\nexec "$@"\n')
            executable(bins / "docker", r'''
case "$*" in
  *--describe*) echo "PartitionCount: 1 ReplicationFactor: 1 min.insync.replicas=1" ;;
  *--time*) echo "test:0:2" ;;
esac
''')
            executable(bins / "java", 'echo yes > "$AUDIT_MARKER"\nprintf "{}\\n" > "8"\n')
            executable(bins / "python3", r'''
script=$1
shift
case "$script" in
  *socket_sample.py)
    printf '{"complete":false}\n' > "$3"
    exit "$FAKE_SAMPLER_STATUS"
    ;;
  *fences.py)
    out=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--output" ]; then out=$2; shift 2; else shift; fi
    done
    printf '0\t0\t2\n' > "$out"
    printf '{}\n'
    ;;
  *verify.py) echo yes > "$VERIFY_MARKER"; printf '{}\n' ;;
  *check_sample.py) [ "$FAKE_SAMPLER_STATUS" -eq 0 ] ;;
  *) exit 90 ;;
esac
''')
            env = dict(os.environ, PATH=str(bins) + os.pathsep + os.environ["PATH"],
                       ACK_EVIDENCE=str(evidence), GITHUB_RUN_ID="17", GITHUB_RUN_ATTEMPT="1",
                       KAFKA_BOOTSTRAP="127.0.0.1:19092", CLIENT_CPU="0", SAMPLER_CPU="0",
                       RUNNER_TEMP=str(root), WARMUP="1", COUNT="1", PAYLOAD_BYTES="100",
                       FAKE_DRIVER_STATUS=str(driver_status), FAKE_SAMPLER_STATUS=str(sampler_status),
                       AUDIT_MARKER=str(root / "audit"), VERIFY_MARKER=str(root / "verify"))
            result = subprocess.run(["bash", str(HERE / "run-one.sh"), "1", "rust"],
                                    cwd=root, env=env, capture_output=True, text=True, timeout=15)
            out = evidence / "pair-1-rust"
            return (result.returncode, (root / "audit").exists(), (root / "verify").exists(),
                    (out / "driver-exit.txt").read_text().strip(),
                    (out / "socket-sampler-exit.txt").read_text().strip(),
                    (out / "failure-offsets.txt").exists(), result.stderr)

    def test_sampler_error_still_performs_readback_but_fails_arm(self):
        status, audit, verify, driver, sample, _, stderr = self.run_case(0, 1)
        self.assertNotEqual(status, 0, stderr)
        self.assertEqual((audit, verify, driver, sample), (True, True, "0", "1"))

    def test_driver_failure_does_not_claim_readback(self):
        status, audit, verify, driver, sample, offsets, stderr = self.run_case(7, 0)
        self.assertNotEqual(status, 0, stderr)
        self.assertEqual((audit, verify, driver, sample, offsets), (False, False, "7", "0", True))

    def test_success_reaches_gate(self):
        status, audit, verify, driver, sample, _, stderr = self.run_case(0, 0)
        self.assertEqual(status, 0, stderr)
        self.assertEqual((audit, verify, driver, sample), (True, True, "0", "0"))


class AggregateTests(unittest.TestCase):
    def run_aggregate(self, directory):
        return subprocess.run([sys.executable, str(HERE / "aggregate.py"), str(directory)],
                              capture_output=True, text=True, timeout=10)

    def test_incomplete_evidence_never_yields_ratios(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            sample = dict(complete=True, error=None, successful_samples=2,
                          samples=[[1, 0], [2, 1]], counts={"0": 1, "1": 1}, max_established=1)
            verdict = dict(verified_acknowledgements=20000,
                           measure=dict(records_per_s=1000, p50_us=10, p99_us=30))
            for pair in range(1, 6):
                for arm in ("rust", "c"):
                    directory = root / f"pair-{pair}-{arm}"
                    directory.mkdir()
                    (directory / "verdict.json").write_text(json.dumps(verdict))
                    (directory / "sockets.json").write_text(json.dumps(sample))
                    (directory / "driver-exit.txt").write_text("0\n")
                    (directory / "socket-sampler-exit.txt").write_text("0\n")
            good = self.run_aggregate(root)
            self.assertEqual(good.returncode, 0, good.stderr)
            self.assertIsNotNone(json.loads(good.stdout)["median_ratios_c_over_rust"])
            bad_dir = root / "pair-3-rust"
            sample.update(samples=[[1, 0], [2, 2]], counts={"0": 1, "2": 1}, max_established=2)
            (bad_dir / "sockets.json").write_text(json.dumps(sample))
            mismatch = self.run_aggregate(root)
            self.assertNotEqual(mismatch.returncode, 0, mismatch.stderr)
            self.assertIsNone(json.loads(mismatch.stdout)["median_ratios_c_over_rust"])
            sample["complete"] = False
            sample["error"] = {"type": "PermissionError", "message": "synthetic"}
            (bad_dir / "sockets.json").write_text(json.dumps(sample))
            (bad_dir / "socket-sampler-exit.txt").write_text("1\n")
            bad = self.run_aggregate(root)
            self.assertNotEqual(bad.returncode, 0, bad.stderr)
            report = json.loads(bad.stdout)
            self.assertIsNone(report["median_ratios_c_over_rust"])
            self.assertTrue(any("pair 3 rust" in issue for issue in report["issues"]))


if __name__ == "__main__":
    unittest.main()

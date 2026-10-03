"""Real Python guard controls only; synthetic metadata, never SDK or protocol vectors."""
import sys
sys.dont_write_bytecode = True
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import origin_audit as source
import importlib.util
spec = importlib.util.spec_from_file_location("bounded_runner", Path(__file__).with_name("run-oracle.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def make_small(root):
    root.mkdir(mode=0o700)
    directory = root / "nested"
    directory.mkdir(mode=0o700)
    rows = {}
    for name, data, git_mode, mode in [("a.txt", b"public synthetic control", "100644", 0o600),
                                      ("nested/b.sh", b"synthetic executable metadata", "100755", 0o700)]:
        p = root / name
        p.write_bytes(data)
        p.chmod(mode)
        rows[name] = {"bytes": len(data), "full_permission_mode": mode,
                      "git_blob_sha1": hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest(),
                      "mode": git_mode, "sha256": hashlib.sha256(data).hexdigest()}
    return rows


class SourceControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="streams-prelaunch-control-")
        self.base = Path(self.temp.name)
        self.root = self.base / "source"
        self.rows = make_small(self.root)
        self.owner = self.root.stat()

    def tearDown(self):
        # Deletes only synthetic, independently allocated test fixtures; never
        # source snapshots, frozen inputs or runtime/evidence outputs.
        self.temp.cleanup()

    def observed(self):
        return source.observe(self.root, self.rows, self.owner.st_uid, self.owner.st_gid)

    def test_synthetic_metadata_positive_full_paths_and_modes(self):
        rows, dirs, result = self.observed()
        self.assertTrue(result["passed"])
        self.assertEqual(rows, self.rows)
        self.assertEqual(set(dirs), {".", "nested"})
        self.assertEqual(result["file_identity_checks"], 2)

    def test_bytes_change_fails_git_sha_and_preserves_observed_map(self):
        (self.root / "a.txt").write_bytes(b"changed same public control")
        rows, _, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertNotEqual(rows["a.txt"]["sha256"], self.rows["a.txt"]["sha256"])
        self.assertNotEqual(rows["a.txt"]["git_blob_sha1"], self.rows["a.txt"]["git_blob_sha1"])

    def test_added_and_missing_paths_fail_exact_set(self):
        (self.root / "a.txt").unlink()
        (self.root / "extra").write_bytes(b"synthetic extra")
        rows, _, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertIsNone(rows["a.txt"]["sha256"])
        self.assertIn("extra", rows)

    def test_read_permission_mutation_rejected_even_without_exec_change(self):
        (self.root / "a.txt").chmod(0o640)
        rows, _, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertEqual(rows["a.txt"]["full_permission_mode"], 0o640)

    def test_special_permission_bits_are_not_discarded(self):
        (self.root / "nested/b.sh").chmod(0o1700)
        rows, _, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertEqual(rows["nested/b.sh"]["full_permission_mode"], 0o1700)

    def test_directory_mode_and_extra_empty_directory_fail(self):
        (self.root / "nested").chmod(0o750)
        (self.root / "extra-dir").mkdir(mode=0o700)
        _, dirs, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertEqual(dirs["nested"]["full_permission_mode"], 0o750)
        self.assertIn("extra-dir", dirs)

    def test_symlink_rejected_without_reading_its_target(self):
        (self.root / "a.txt").unlink()
        (self.root / "a.txt").symlink_to("/definitely-not-a-readable-control-target")
        rows, _, result = self.observed()
        self.assertFalse(result["passed"])
        self.assertIsNone(rows["a.txt"]["sha256"])

    def test_numeric_owner_check_fails_closed(self):
        _, _, result = source.observe(self.root, self.rows, self.owner.st_uid + 1, self.owner.st_gid)
        self.assertFalse(result["passed"])
        self.assertEqual(result["file_owner_checks"], 2)

    def fake_origin(self):
        raw = source.compact(self.rows)
        gzip_path = self.base / "origin.gz"
        gzip_path.write_bytes(gzip.compress(raw, mtime=0))
        return SimpleNamespace(source=self.root, rows=self.rows, raw=raw, gzip_path=gzip_path,
                               root_uid=self.owner.st_uid, root_gid=self.owner.st_gid,
                               unchanged_origin=lambda: None)

    def test_full_raw_identical_after_audit_is_exact_byte_verified_hardlink(self):
        output = self.base / "output"
        output.mkdir()
        origin = self.fake_origin()
        before = source.audit(origin, output, "before", lambda size: None)
        after = source.audit(origin, output, "after", lambda size: None)
        self.assertTrue(before["passed"] and after["passed"])
        self.assertEqual((output / "source-before.json").read_bytes(), (output / "source-after.json").read_bytes())
        self.assertEqual((output / "source-before.json").stat().st_ino, (output / "source-after.json").stat().st_ino)
        self.assertEqual((output / "source-before.json.gz").stat().st_ino, (output / "source-after.json.gz").stat().st_ino)
        self.assertEqual(len(after["exact_byte_verified_hardlinks"]), 3)
        self.assertEqual((output / "source-before.json").stat().st_mode & 0o7777, 0o600)

    def test_failure_after_map_remains_complete_separate_and_does_not_replace_before(self):
        output = self.base / "output"
        output.mkdir()
        origin = self.fake_origin()
        source.audit(origin, output, "before", lambda size: None)
        original = (output / "source-before.json").read_bytes()
        (self.root / "a.txt").write_bytes(b"synthetic mutation")
        after = source.audit(origin, output, "after", lambda size: None)
        self.assertFalse(after["passed"])
        self.assertEqual(len(json.loads((output / "source-after.json").read_bytes())), 2)
        self.assertNotEqual((output / "source-before.json").stat().st_ino, (output / "source-after.json").stat().st_ino)
        self.assertEqual((output / "source-before.json").read_bytes(), original)

    def test_full_write_reservation_checked_before_file_created(self):
        path = self.base / "refused"
        def refuse(size):
            raise RuntimeError("synthetic reserved floor refusal")
        with self.assertRaises(RuntimeError):
            source.write_owned(path, b"control", refuse, 100)
        self.assertFalse(path.exists())

    def test_compact_raw_size_bound_fails_before_write(self):
        path = self.base / "refused"
        with self.assertRaises(ValueError):
            source.write_owned(path, b"control", lambda size: None, 1)
        self.assertFalse(path.exists())


class GuardControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="streams-bound-control-")
        self.output = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_350mib_floor_includes_pending_write(self):
        with patch.object(runner.shutil, "disk_usage", return_value=SimpleNamespace(free=runner.FLOOR + 9)):
            with self.assertRaises(RuntimeError):
                runner.guard(self.output, 10)

    def test_owned96mib_unique_limit_includes_reservation(self):
        with patch.object(runner, "MAX_OUTPUT", 8):
            with self.assertRaises(RuntimeError):
                runner.guard(self.output, 9)

    def test_physical_hardlinks_count_once_but_both_paths_remain(self):
        p = self.output / "before"
        p.write_bytes(b"metadata control")
        os.link(p, self.output / "after")
        usage = runner.usage(self.output)
        self.assertEqual(usage["unique_bytes"], len(b"metadata control"))
        self.assertEqual(usage["paths"], 2)

    def test_scoped_fixture_cohort_bound(self):
        directory = self.output / "generation-1"
        directory.mkdir()
        (directory / "synthetic.bin").write_bytes(b"control")
        with patch.object(runner, "MAX_FIXTURES_BYTES", 1):
            with self.assertRaises(RuntimeError):
                runner.guard(self.output)

    def test_metadata_reservation_refused_before_create(self):
        with patch.object(runner, "MAX_OUTPUT", 1):
            with self.assertRaises(RuntimeError):
                runner.write_json(self.output, "refused.json", {"control": True})
        self.assertFalse((self.output / "refused.json").exists())

    def test_metadata_serialization_bound_before_create(self):
        with self.assertRaises(RuntimeError):
            runner.write_json(self.output, "refused.json", {"control": True}, maximum=1)
        self.assertFalse((self.output / "refused.json").exists())

    def test_synthetic_python_stdout_overflow_is_failed_and_joined(self):
        result = runner.command(self.output, "synthetic-python-overflow",
                                [sys.executable, "-c", "import sys;sys.stdout.write('x'*200000);sys.stdout.flush()"],
                                {"PATH": "/usr/bin:/bin"})
        self.assertIsNotNone(result["bound_failure"])
        self.assertTrue(result["joined"])
        self.assertLessEqual(result["streams"]["stdout"]["size"], runner.MAX_STREAM)

    def test_synthetic_python_hang_is_deadline_failed_and_joined(self):
        with patch.object(runner, "MAX_PROCESS_SECONDS", 0.1):
            result = runner.command(self.output, "synthetic-python-timeout",
                                    [sys.executable, "-c", "import time;time.sleep(30)"], {"PATH": "/usr/bin:/bin"})
        self.assertIsNotNone(result["bound_failure"])
        self.assertTrue(result["joined"])
        self.assertNotEqual(result["exit_status"], 0)


class ActualOriginSetupControls(unittest.TestCase):
    def test_frozen_guaranteed_blocks_and_actual_origin_are_retained(self):
        base = Path(__file__).parents[1]
        self.assertEqual(source.sha(base / "run-oracle.py"), "39c5d71c8ff2c1e99bfa5f1b9b7c61a6ebc623a254d3a45eb225239cf7a98171")
        self.assertEqual(source.sha(base / "runtime-preparation-04.json"), "d2b20f5771cd7a791bbe3d541634f8027f8c9a5c22e6660b8ce68ea7a6a5276b")
        self.assertGreater(source.FILES, 50000)
        self.assertGreater(2 * 23380322, 32 * 1024 * 1024)
        self.assertGreaterEqual(source.MAX_FILES, source.FILES)
        self.assertEqual(source.sha(runner.ORIGIN_RECEIPT), source.ORIGIN_SHA)

    def test_anchored_actual_origin_fullmap_and_measured_forecast(self):
        origin = source.Origin(runner.ORIGIN_RECEIPT, Path("/workspace/work/client-capabilities-source-04f6bc29"),
                               runner.git_entries(Path("/workspace/partitionline")))
        self.assertEqual(len(origin.rows), 73938)
        self.assertEqual(sum(row["bytes"] for row in origin.rows.values()), 713363027)
        # Measure the actual WORK output filesystem, rather than a system/tmp
        # mount which can report a different free-space budget.
        measured = runner.forecast(origin, Path(__file__).parent)
        self.assertLessEqual(measured["worst_case_new_output_allocated_bytes"], 96 * 1024 * 1024)
        self.assertEqual(measured["source_directory_count"], 10308)
        self.assertEqual(measured["minimum_remaining_free_bytes"], 350 * 1024 * 1024)
        (Path(__file__).with_name("measured-disk-forecast-08.json")).write_text(json.dumps(measured, indent=2) + "\n")

if __name__ == "__main__":
    unittest.main(verbosity=2)

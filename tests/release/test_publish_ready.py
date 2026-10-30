#!/usr/bin/env python3
"""Check that test failures stop the real release gate before later stages."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class PublishReadyTests(unittest.TestCase):
    def run_gate(self, failed_test):
        with tempfile.TemporaryDirectory(prefix="partitionline-release-gate-") as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            shutil.copy2(ROOT / "scripts/ci-publish-ready.sh", scripts)
            (scripts / "rehearse-partial-release.sh").write_text("exit 0\n")
            # Stop the successful-test control at the next real stage. No
            # registry, credentials, build, package or publication is involved.
            (scripts / "ci-deny.sh").write_text("exit 77\n")
            commands = root / "commands"
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "with open(os.environ['PL_GATE_COMMANDS'], 'a') as log:\n"
                "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "if sys.argv[1:2] == ['test']:\n"
                "    cell = 'all' if '--all-features' in sys.argv else 'default'\n"
                "    if cell == os.environ['PL_GATE_FAILED_TEST']:\n"
                "        sys.exit(23)\n"
            )
            cargo.chmod(0o755)
            env = os.environ.copy()
            env.update(
                PATH=str(binary) + os.pathsep + env.get("PATH", ""),
                PL_GATE_COMMANDS=str(commands),
                PL_GATE_FAILED_TEST=failed_test,
            )
            result = subprocess.run(
                ["bash", str(scripts / "ci-publish-ready.sh")],
                env=env,
                capture_output=True,
                text=True,
                timeout=10,
            )
            calls = [json.loads(line) for line in commands.read_text().splitlines()]
            return result, calls

    def test_default_failure_stops_before_other_tests_or_publication(self):
        result, calls = self.run_gate("default")
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertEqual(calls[-1], ["test", "--locked", "--all-targets"])
        self.assertEqual(len(calls), 3)

    def test_all_feature_failure_stops_before_packaging_or_publication(self):
        result, calls = self.run_gate("all")
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertEqual(calls[-2:], [
            ["test", "--locked", "--all-targets"],
            ["test", "--locked", "--all-targets", "--all-features"],
        ])
        self.assertEqual(len(calls), 4)

    def test_both_suites_pass_before_next_stage(self):
        result, calls = self.run_gate("none")
        self.assertEqual(result.returncode, 77, result.stdout + result.stderr)
        self.assertEqual(calls[-2:], [
            ["test", "--locked", "--all-targets"],
            ["test", "--locked", "--all-targets", "--all-features"],
        ])


if __name__ == "__main__":
    unittest.main()

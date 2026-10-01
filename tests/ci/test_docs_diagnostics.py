"""The docs wrapper retains Cargo failures and their exact exit status."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class DocsDiagnosticsTests(unittest.TestCase):
    def invoke(self, failing_phase, long_log=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text(
                "#!/usr/bin/env bash\n"
                'if [[ "$1" == -V ]]; then echo cargo-test; exit 0; fi\n'
                'if [[ "$1" == "$PL_TEST_FAIL_PHASE" ]]; then\n'
                '  if [[ "$PL_TEST_LONG_LOG" == 1 ]]; then\n'
                '    for i in {1..600}; do echo "dependency progress $i 100%"; done; fi\n'
                '  printf "diagnostic 100%%\\nsecond line\\n"; exit 42; fi\n'
                'echo "test result: ok. 4 passed; 0 failed"\n'
            )
            cargo.chmod(0o755)
            rustc = binary / "rustc"
            rustc.write_text("#!/usr/bin/env bash\necho rustc-test\n")
            rustc.chmod(0o755)
            report = root / "report"
            environment = os.environ | {
                "PATH": str(binary) + os.pathsep + os.environ["PATH"],
                "OFFLINE": "1",
                "PL_DOCS_REPORT_DIR": str(report),
                "PL_TEST_FAIL_PHASE": failing_phase,
                "PL_TEST_LONG_LOG": "1" if long_log else "0",
            }
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/ci-docs.sh")],
                env=environment, text=True, capture_output=True, check=False,
            )
            logs = {path.name: path.read_text() for path in report.iterdir()}
            return result, logs

    def test_rustdoc_failure_retained_and_doctests_not_run(self):
        result, logs = self.invoke("doc")
        self.assertEqual(result.returncode, 42, result.stdout + result.stderr)
        self.assertIn("diagnostic 100%\nsecond line\n", logs["rustdoc.log"])
        self.assertNotIn("doctests.log", logs)
        self.assertIn("rustdoc exited 42%0Adiagnostic 100%25%0Asecond line", result.stdout)

    def test_doctest_failure_retained(self):
        result, logs = self.invoke("test")
        self.assertEqual(result.returncode, 42, result.stdout + result.stderr)
        self.assertIn("diagnostic 100%\nsecond line\n", logs["doctests.log"])
        self.assertIn("doctests exited 42", result.stdout)

    def test_success_keeps_both_phase_logs(self):
        result, logs = self.invoke("none")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(set(logs), {"identity.log", "rustdoc.log", "doctests.log"})
        self.assertNotIn("::error", result.stdout)

    def test_long_compile_log_annotation_keeps_failure_tail(self):
        result, logs = self.invoke("doc", long_log=True)
        annotation = next(line for line in result.stdout.splitlines() if line.startswith("::error"))
        self.assertLess(len(annotation.encode()), 6500)
        self.assertIn("diagnostic 100%25%0Asecond line", annotation)
        self.assertIn("dependency progress 1 100%", logs["rustdoc.log"])
        self.assertEqual(result.returncode, 42)


if __name__ == "__main__":
    unittest.main()

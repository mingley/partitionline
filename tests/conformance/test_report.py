"""
Unit tests for the conformance report validator and aggregator (scripts/conformance-report.py).

Satisfies card KL01-02:
  - Reject missing/duplicate cases, unknown statuses, wrong source/peer revisions and absent artifacts.
  - Required failed/not-run/unsupported/blocked cases cannot produce a success exit code.
  - Preserve every attempt; rerun success cannot erase a first failure or change the case denominator.
  - Statuses that stay in the denominator (failed, not_run, unsupported, blocked) must make the
    report command exit non-zero.
  - not_applicable with a reason may be excluded only if the registry already marks denominator false.
  - independent_pass requires an artifact (and artifact exists on disk).
  - Tests use offline temporary fixtures (no network access).
"""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "conformance-report.py"
REGISTRY_PATH = REPO_ROOT / "tests" / "conformance" / "cases.json"

# Dynamically import scripts/conformance-report.py
spec = importlib.util.spec_from_file_location("conformance_report", str(SCRIPT_PATH))
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load module from {SCRIPT_PATH}")
conformance_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conformance_report)


class ConformanceReportTestBase(unittest.TestCase):
    """Base class providing helpers for creating registry and report fixtures."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)

        # Load real registry for base structure
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            self.real_registry = json.load(f)

    def create_artifact(self, filename: str = "oracle_artifact.bin") -> Path:
        """Create a real temp artifact file on disk."""
        art = self.temp_path / filename
        art.write_bytes(b"\x00\x01\x02\x03KAFKA_ORACLE_PROOF")
        return art

    def create_minimal_registry(self) -> Dict[str, Any]:
        """Create a small isolated registry fixture."""
        return {
            "schema_version": 1,
            "audited_source": "cb7e97d3b92a8555aea34d59266a2990c206395f",
            "disposition_enum": list(conformance_report.VALID_DISPOSITIONS),
            "cases": [
                {
                    "id": "case-produce-1",
                    "api_family": "Produce",
                    "source_pin": "cb7e97d3b92a8555aea34d59266a2990c206395f",
                    "peer_version_pin": "3.9.1",
                    "denominator": True,
                    "disposition": "independent_pass",
                    "reason": "Test produce case 1",
                },
                {
                    "id": "case-fetch-1",
                    "api_family": "Fetch",
                    "source_pin": "cb7e97d3b92a8555aea34d59266a2990c206395f",
                    "peer_version_pin": "4.1.0",
                    "denominator": True,
                    "disposition": "independent_pass",
                    "reason": "Test fetch case 1",
                },
                {
                    "id": "case-internal-1",
                    "api_family": "Metadata",
                    "source_pin": "cb7e97d3b92a8555aea34d59266a2990c206395f",
                    "peer_version_pin": "3.9.1",
                    "denominator": False,
                    "disposition": "not_applicable",
                    "reason": "Broker-internal RPC excluded from SDK scope",
                },
            ],
        }

    def run_cli(
        self,
        args: List[str],
        stdin_data: str | None = None,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess:
        """Invoke scripts/conformance-report.py as a subprocess."""
        cmd = [sys.executable, str(SCRIPT_PATH)] + args
        return subprocess.run(
            cmd,
            input=stdin_data,
            cwd=str(cwd or REPO_ROOT),
            capture_output=True,
            text=True,
        )


class TestConformanceReportValidation(ConformanceReportTestBase):
    """Tests covering fail-closed input validation rules."""

    def test_empty_object_input_fails_nonzero(self):
        """Empty {} input must not exit 0."""
        # Via Python API
        registry = self.create_minimal_registry()
        with self.assertRaises(conformance_report.ConformanceValidationError):
            conformance_report.validate_and_aggregate_reports(
                registry, [("empty_test", {})]
            )

        # Via CLI stdin
        res = self.run_cli(["-r", str(REGISTRY_PATH)], stdin_data="{}")
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("Validation error", res.stderr)

    def test_empty_list_input_fails_nonzero(self):
        """Empty list [] input must not exit 0."""
        registry = self.create_minimal_registry()
        with self.assertRaises(conformance_report.ConformanceValidationError):
            conformance_report.validate_and_aggregate_reports(
                registry, [("empty_list", [])]
            )

    def test_missing_cases_rejected_fail_closed(self):
        """Reject report with missing cases from the registry."""
        registry = self.create_minimal_registry()
        # Only provide 1 case out of 3
        art = self.create_artifact()
        partial_report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "status": "independent_pass",
                    "artifact": str(art),
                }
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(
                registry, [("partial", partial_report)]
            )
        self.assertIn("Missing 2 required conformance case(s)", str(ctx.exception))

    def test_unknown_case_id_rejected(self):
        """Reject unknown case ID not present in registry."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()
        report = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
                {"id": "unregistered-case-999", "status": "independent_pass", "artifact": str(art)},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("report", report)])
        self.assertIn("unknown case id 'unregistered-case-999'", str(ctx.exception))

    def test_duplicate_cases_in_same_report_rejected(self):
        """Reject duplicate case entries without distinct attempt numbers."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()
        dup_report = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("dup_report", dup_report)])
        self.assertIn("duplicate case entry 'case-produce-1'", str(ctx.exception))

    def test_duplicate_attempt_number_rejected(self):
        """Reject identical attempt numbers for the same case."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()
        dup_attempt_report = {
            "cases": [
                {"id": "case-produce-1", "attempt": 1, "status": "failed"},
                {"id": "case-produce-1", "attempt": 1, "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("dup", dup_attempt_report)])
        self.assertIn("duplicate attempt 1 for case 'case-produce-1'", str(ctx.exception))

    def test_unknown_status_rejected(self):
        """Reject unknown status/disposition strings."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()
        report = {
            "cases": [
                {"id": "case-produce-1", "status": "passed_mcp_heuristic", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", report)])
        self.assertIn("unknown status 'passed_mcp_heuristic'", str(ctx.exception))

    def test_wrong_source_revision_rejected(self):
        """Reject case or report with wrong source revision/pin."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        # Case-level mismatch
        report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "status": "independent_pass",
                    "artifact": str(art),
                    "source_pin": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                },
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", report)])
        self.assertIn("wrong source revision", str(ctx.exception))

        # Top-level mismatch
        top_report = {
            "source_sha": "0000000000000000000000000000000000000000",
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ],
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", top_report)])
        self.assertIn("Wrong top-level source revision", str(ctx.exception))

    def test_wrong_peer_revision_rejected(self):
        """Reject case specifying peer revision differing from registry pin."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()
        report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "status": "independent_pass",
                    "artifact": str(art),
                    "peer_version": "2.8.0",  # Registry pinned to 3.9.1
                },
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", report)])
        self.assertIn("wrong peer revision", str(ctx.exception))

    def test_independent_pass_requires_artifact(self):
        """independent_pass requires an artifact."""
        registry = self.create_minimal_registry()
        report_no_art = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass"},  # No artifact!
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": "dummy"},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(
                registry, [("rep", report_no_art)], check_artifacts=False
            )
        self.assertIn("absent artifact", str(ctx.exception))

    def test_absent_artifact_file_on_disk_rejected(self):
        """Reject independent_pass when specified artifact file does not exist on disk."""
        registry = self.create_minimal_registry()
        non_existent_art = self.temp_path / "does_not_exist.bin"
        report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "status": "independent_pass",
                    "artifact": str(non_existent_art),
                },
                {"id": "case-fetch-1", "status": "failed"},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(
                registry, [("rep", report)], check_artifacts=True
            )
        self.assertIn("artifact absent on disk", str(ctx.exception))

    def test_not_applicable_only_if_registry_marks_denominator_false(self):
        """not_applicable with a reason may be excluded only if registry already marks denominator false."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        # Try to mark case-produce-1 (denominator: true in registry) as not_applicable
        report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "status": "not_applicable",
                    "reason": "Trying to skip produce",
                },
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", report)])
        self.assertIn("registry specifies denominator=true", str(ctx.exception))

    def test_not_applicable_without_reason_rejected(self):
        """not_applicable requires an explicit non-empty reason."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        report = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "   "},
            ]
        }
        with self.assertRaises(conformance_report.ConformanceValidationError) as ctx:
            conformance_report.validate_and_aggregate_reports(registry, [("rep", report)])
        self.assertIn("without an explicit reason", str(ctx.exception))


class TestConformanceReportDenominatorAndExitCodes(ConformanceReportTestBase):
    """Tests covering denominator rules and fail-closed exit codes."""

    def test_required_failing_statuses_cause_failure(self):
        """
        Required failed/not-run/unsupported/blocked cases cannot produce a success exit code.
        Statuses that stay in the denominator (failed, not_run, unsupported, blocked) must
        make the report command exit non-zero.
        """
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        for failing_status in ("failed", "not_run", "unsupported", "blocked"):
            with self.subTest(status=failing_status):
                report = {
                    "cases": [
                        {"id": "case-produce-1", "status": failing_status},
                        {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                        {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
                    ]
                }
                summary = conformance_report.validate_and_aggregate_reports(
                    registry, [("rep", report)], check_artifacts=True
                )
                self.assertFalse(summary["success"])
                self.assertEqual(summary["exit_code"], 1)
                self.assertEqual(summary["denominator_cases"], 2)
                self.assertEqual(summary["excluded_cases"], 1)

    def test_rerun_success_cannot_erase_first_failure(self):
        """
        Preserve every attempt; rerun success cannot erase a first failure or
        change the case denominator.
        A second attempt that succeeds must not delete the first failed attempt from the
        preserved attempt log or shrink the denominator.
        """
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        # Case-produce-1 failed on attempt 1, passed on attempt 2
        report = {
            "cases": [
                {
                    "id": "case-produce-1",
                    "attempts": [
                        {"attempt": 1, "status": "failed", "reason": "transient timeout"},
                        {"attempt": 2, "status": "independent_pass", "artifact": str(art)},
                    ],
                },
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        summary = conformance_report.validate_and_aggregate_reports(
            registry, [("rep", report)], check_artifacts=True
        )

        # 1. First failure preserved in log
        attempts = summary["cases"]["case-produce-1"]["attempts"]
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0]["status"], "failed")
        self.assertEqual(attempts[1]["status"], "independent_pass")
        self.assertEqual(summary["total_attempts"], 4)

        # 2. Denominator is NOT shrunk or changed
        self.assertEqual(summary["denominator_cases"], 2)
        self.assertEqual(summary["excluded_cases"], 1)

        # 3. Flaky case detected, failure not erased, exit_code is 1 (non-zero)
        self.assertIn("case-produce-1", summary["flaky_cases"])
        self.assertIn("case-produce-1", summary["failing_cases"])
        self.assertFalse(summary["success"])
        self.assertEqual(summary["exit_code"], 1)

    def test_multi_report_rerun_preserves_first_failure(self):
        """Rerun across two separate report files preserves the first failure."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        run1 = {
            "cases": [
                {"id": "case-produce-1", "status": "failed"},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        run2 = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }

        summary = conformance_report.validate_and_aggregate_reports(
            registry, [("run1.json", run1), ("run2.json", run2)], check_artifacts=True
        )

        self.assertEqual(len(summary["preserved_attempt_log"]), 6)
        prod_attempts = summary["cases"]["case-produce-1"]["attempts"]
        self.assertEqual(len(prod_attempts), 2)
        self.assertEqual(prod_attempts[0]["status"], "failed")
        self.assertEqual(prod_attempts[1]["status"], "independent_pass")
        self.assertFalse(summary["success"])
        self.assertEqual(summary["exit_code"], 1)

    def test_clean_success_passes_exit_0(self):
        """All denominator cases passing on first attempt produces exit 0."""
        registry = self.create_minimal_registry()
        art = self.create_artifact()

        report = {
            "cases": [
                {"id": "case-produce-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-fetch-1", "status": "independent_pass", "artifact": str(art)},
                {"id": "case-internal-1", "status": "not_applicable", "reason": "Excluded"},
            ]
        }
        summary = conformance_report.validate_and_aggregate_reports(
            registry, [("rep", report)], check_artifacts=True
        )

        self.assertTrue(summary["success"])
        self.assertEqual(summary["exit_code"], 0)
        self.assertEqual(summary["denominator_cases"], 2)
        self.assertEqual(summary["excluded_cases"], 1)
        self.assertEqual(len(summary["failing_cases"]), 0)


class TestConformanceReportCLI(ConformanceReportTestBase):
    """Subprocess integration tests exercising CLI invocation and real registry."""

    def test_cli_on_empty_input_fails_nonzero(self):
        """Empty {} input via CLI must not exit 0."""
        res = self.run_cli(["-r", str(REGISTRY_PATH)], stdin_data="{}")
        self.assertEqual(res.returncode, 2)
        self.assertIn("Validation error", res.stderr)

    def test_cli_on_intentionally_incomplete_report_fails_nonzero(self):
        """Also invoke the script on an intentionally incomplete report and require nonzero exit."""
        art = self.create_artifact()
        incomplete_report_file = self.temp_path / "incomplete_report.json"
        with open(incomplete_report_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cases": [
                        {
                            "id": "matrix-cell-produce-3-9-1",
                            "status": "independent_pass",
                            "artifact": str(art),
                        }
                    ]
                },
                f,
            )

        res = self.run_cli(["-r", str(REGISTRY_PATH), str(incomplete_report_file)])
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("Missing", res.stderr)
        self.assertIn("conformance case", res.stderr)

    def test_cli_on_cases_json_fails_nonzero(self):
        """
        Invoking report script on the baseline registry cases.json itself
        must fail closed (exit 1) because A01-A05 and targets are failed/not-run/unsupported/blocked.
        """
        res = self.run_cli(["-r", str(REGISTRY_PATH), str(REGISTRY_PATH)])
        self.assertEqual(res.returncode, 1)
        self.assertIn("FAIL (incomplete or non-passing cases)", res.stdout)
        self.assertIn("audit-regression-a01-offset-filter", res.stdout)
        self.assertIn("Failing Cases (5)", res.stdout)

    def test_cli_synthetic_full_green_report_exits_0(self):
        """
        Synthetic complete report satisfying all 99 cases in tests/conformance/cases.json
        exits 0 and writes expected summary.
        """
        art = self.create_artifact()
        full_cases = []
        for rc in self.real_registry["cases"]:
            cid = rc["id"]
            if rc.get("denominator", True) is False:
                full_cases.append({
                    "id": cid,
                    "status": "not_applicable",
                    "reason": rc.get("reason", "Excluded"),
                })
            else:
                full_cases.append({
                    "id": cid,
                    "status": "independent_pass",
                    "artifact": str(art),
                    "source_pin": rc.get("source_pin"),
                    "peer_pin": rc.get("peer_version_pin"),
                })

        report_file = self.temp_path / "full_green_report.json"
        summary_out_file = self.temp_path / "summary_output.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump({"cases": full_cases}, f)

        res = self.run_cli([
            "-r", str(REGISTRY_PATH),
            "-o", str(summary_out_file),
            str(report_file),
        ])
        self.assertEqual(res.returncode, 0, f"Expected exit 0, got {res.returncode}. Stderr: {res.stderr}")
        self.assertIn("SUCCESS", res.stdout)

        # Verify summary output file
        self.assertTrue(summary_out_file.is_file())
        with open(summary_out_file, "r", encoding="utf-8") as f:
            summary_data = json.load(f)

        self.assertEqual(summary_data["total_cases"], 99)
        self.assertEqual(summary_data["denominator_cases"], 79)
        self.assertEqual(summary_data["excluded_cases"], 20)
        self.assertEqual(summary_data["exit_code"], 0)
        self.assertTrue(summary_data["success"])


if __name__ == "__main__":
    unittest.main()

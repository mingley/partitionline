"""
Unit tests for the protocol schema and coverage drift checker (scripts/check-protocol-coverage.py).

Satisfies card KL01-11:
  - Deterministic comparison of pinned Apache schema/API inventory with codec, runtime, and test coverage.
  - Acceptance: do not count a key name or helper type as an implemented client operation.
  - Report version gaps, missing runtime wiring, and excluded broker-internal APIs separately.
  - One synthetic new API or version fails the check until classified.
  - No codec-generator rewrite.
  - Standard-library tests only (no external dependencies).
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
SCRIPT_PATH = REPO_ROOT / "scripts" / "check-protocol-coverage.py"
CASES_PATH = REPO_ROOT / "tests" / "conformance" / "cases.json"
FEATURES_PATH = REPO_ROOT / "tests" / "conformance" / "features.json"
API_KEYS_PATH = REPO_ROOT / "src" / "protocol" / "api_keys.rs"

# Dynamically import scripts/check-protocol-coverage.py
spec = importlib.util.spec_from_file_location("check_protocol_coverage", str(SCRIPT_PATH))
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load module from {SCRIPT_PATH}")
cpc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpc)


class TestProtocolCoverageChecker(unittest.TestCase):
    """Test suite verifying protocol coverage comparison, drift detection, and classification."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)

    def run_cli(
        self,
        args: List[str],
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess:
        """Invoke scripts/check-protocol-coverage.py as a subprocess."""
        cmd = [sys.executable, str(SCRIPT_PATH)] + args
        return subprocess.run(
            cmd,
            cwd=str(cwd or REPO_ROOT),
            capture_output=True,
            text=True,
        )

    def test_default_frozen_pins_pass_with_zero_drift(self):
        """
        Verify that running against default frozen pins (3.9.1, 4.1.0, 4.1.2, 4.2.1, 4.3.1)
        passes cleanly with exit code 0 and zero unclassified drift.
        """
        results = cpc.evaluate_protocol_coverage(
            api_keys_path=API_KEYS_PATH,
            features_path=FEATURES_PATH,
            cases_path=CASES_PATH,
        )
        self.assertEqual(results["summary"]["exit_code"], 0)
        self.assertFalse(results["summary"]["drift_detected"])
        self.assertEqual(len(results["unclassified_drift"]), 0)
        self.assertEqual(results["summary"]["total_pinned_apis"], 88)
        self.assertEqual(results["summary"]["total_catalog_keys"], 91)
        self.assertEqual(results["summary"]["implemented_client_apis_count"], 62)
        self.assertEqual(results["gap_counts"]["missing_runtime_wiring_apis"], 4)
        self.assertEqual(results["gap_counts"]["excluded_broker_internal_apis"], 22)
        self.assertEqual(results["gap_counts"]["unclassified_drift"], 0)

    def test_deterministic_results(self):
        """
        Ensure evaluation is deterministic: two consecutive runs produce identical results.
        """
        run1 = cpc.evaluate_protocol_coverage()
        run2 = cpc.evaluate_protocol_coverage()
        self.assertEqual(json.dumps(run1, sort_keys=True), json.dumps(run2, sort_keys=True))

    def test_key_names_and_helpers_not_counted_as_implemented(self):
        """
        Acceptance rule: Do not count a key name or helper type as an implemented client operation.
        Verify that key names in api_keys.rs that lack client runtime methods
        (ElectLeaders 43, DescribeQuorum 55, AddRaftVoter 80, RemoveRaftVoter 81)
        and broker-internal APIs (4, 5, 6, 7, 52, etc.) are strictly NOT in
        implemented_client_operations.
        """
        results = cpc.evaluate_protocol_coverage()
        implemented_keys = {op["api_key"] for op in results["implemented_client_operations"]}

        # Extended admin keys that are names in the catalog only (no client runtime method)
        for missing_key in (43, 55, 80, 81):
            self.assertNotIn(
                missing_key,
                implemented_keys,
                f"Key {missing_key} is only a catalog name or unimplemented and must not be counted as implemented",
            )

        # Broker-internal / clusterAction keys must not be counted as implemented client operations
        for internal_key in (4, 5, 6, 7, 27, 52, 53, 54, 56, 58, 59, 62, 63, 70, 82, 83, 84, 85, 86, 87):
            self.assertNotIn(
                internal_key,
                implemented_keys,
                f"Broker-internal key {internal_key} must not be counted as an implemented client operation",
            )

        # Verify missing admin keys appear under missing_runtime_wiring.unimplemented_apis
        unimpl_keys = {ma["api_key"] for ma in results["missing_runtime_wiring"]["unimplemented_apis"]}
        self.assertEqual(unimpl_keys, {43, 55, 80, 81})

    def test_synthetic_unclassified_api_fails_check(self):
        """
        Acceptance rule: One synthetic new API fails the check until classified.
        """
        synthetic_inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
        synthetic_inventory[99] = {
            "name": "SyntheticAdminOperation",
            "versions": {"4.3.1": [0, 1]},
        }

        # Run evaluation with synthetic unclassified API
        results = cpc.evaluate_protocol_coverage(inventory=synthetic_inventory)
        self.assertEqual(results["summary"]["exit_code"], 1)
        self.assertTrue(results["summary"]["drift_detected"])
        self.assertEqual(len(results["unclassified_drift"]), 1)
        drift_item = results["unclassified_drift"][0]
        self.assertEqual(drift_item["type"], "unclassified_api")
        self.assertEqual(drift_item["api_key"], 99)
        self.assertIn("SyntheticAdminOperation", drift_item["description"])

    def test_classified_synthetic_api_passes_check(self):
        """
        Once classified, a synthetic new API passes the check cleanly.
        """
        synthetic_inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
        synthetic_inventory[99] = {
            "name": "SyntheticAdminOperation",
            "versions": {"4.3.1": [0, 1]},
        }

        # Classify as missing runtime wiring
        results = cpc.evaluate_protocol_coverage(
            inventory=synthetic_inventory,
            extra_classified_apis={99: "missing_runtime"},
        )
        self.assertEqual(results["summary"]["exit_code"], 0)
        self.assertFalse(results["summary"]["drift_detected"])
        self.assertEqual(len(results["unclassified_drift"]), 0)

        # Classify as excluded broker-internal
        results_internal = cpc.evaluate_protocol_coverage(
            inventory=synthetic_inventory,
            extra_classified_apis={99: "excluded_broker_internal"},
        )
        self.assertEqual(results_internal["summary"]["exit_code"], 0)
        self.assertFalse(results_internal["summary"]["drift_detected"])
        self.assertEqual(len(results_internal["unclassified_drift"]), 0)

    def test_synthetic_unclassified_version_fails_check(self):
        """
        Acceptance rule: One synthetic new version fails the check until classified.
        """
        synthetic_inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
        # Produce validVersions is 3-13 on 4.3.1; inject v14
        synthetic_inventory[0]["versions"]["4.3.1"] = [3, 14]

        results = cpc.evaluate_protocol_coverage(inventory=synthetic_inventory)
        self.assertEqual(results["summary"]["exit_code"], 1)
        self.assertTrue(results["summary"]["drift_detected"])
        self.assertEqual(len(results["unclassified_drift"]), 1)
        drift_item = results["unclassified_drift"][0]
        self.assertEqual(drift_item["type"], "unclassified_version")
        self.assertEqual(drift_item["api_key"], 0)
        self.assertEqual(drift_item["version"], 14)

    def test_classified_synthetic_version_passes_check(self):
        """
        Once classified, a synthetic new version passes the check cleanly.
        """
        synthetic_inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
        synthetic_inventory[0]["versions"]["4.3.1"] = [3, 14]

        results = cpc.evaluate_protocol_coverage(
            inventory=synthetic_inventory,
            extra_classified_versions={(0, 14): "Classified experimental Produce v14"},
        )
        self.assertEqual(results["summary"]["exit_code"], 0)
        self.assertFalse(results["summary"]["drift_detected"])
        self.assertEqual(len(results["unclassified_drift"]), 0)
        self.assertTrue(any(g["api_key"] == 0 and g["version"] == 14 for g in results["version_gaps"]))

    def test_separate_reporting_categories(self):
        """
        Acceptance rule: Report version gaps, missing runtime wiring, and excluded broker-internal APIs separately.
        """
        results = cpc.evaluate_protocol_coverage()

        # 1. Version gaps
        self.assertIn("version_gaps", results)
        version_gaps = results["version_gaps"]
        self.assertGreaterEqual(len(version_gaps), 40)
        # Check key expected version gaps
        self.assertTrue(any(g["api_key"] == 0 and g["version"] == 13 for g in version_gaps))  # Produce v13
        self.assertTrue(any(g["api_key"] == 1 and g["version"] == 18 for g in version_gaps))  # Fetch v18
        self.assertTrue(any(g["api_key"] == 2 and g["version"] == 11 for g in version_gaps))  # ListOffsets v11
        self.assertTrue(any(g["api_key"] == 78 and g["version"] == 2 for g in version_gaps)) # ShareFetch v2
        self.assertTrue(any(g["api_key"] == 35 and g["version"] == 5 for g in version_gaps)) # DescribeLogDirs v5

        # 2. Missing runtime wiring
        self.assertIn("missing_runtime_wiring", results)
        unimpl_apis = results["missing_runtime_wiring"]["unimplemented_apis"]
        self.assertEqual(len(unimpl_apis), 4)
        for item in unimpl_apis:
            self.assertFalse(item["has_runtime_operation"])
            self.assertIn("ElectLeaders" if item["api_key"] == 43 else "Raft" if item["api_key"] in (80, 81) else "DescribeQuorum", item["name"])

        # 3. Excluded broker-internal APIs
        self.assertIn("excluded_broker_internal", results)
        internal_apis = results["excluded_broker_internal"]["broker_internal_apis"]
        self.assertEqual(len(internal_apis), 22)
        internal_keys = {item["api_key"] for item in internal_apis}
        self.assertIn(4, internal_keys)  # LeaderAndIsr
        self.assertIn(5, internal_keys)  # StopReplica
        self.assertIn(6, internal_keys)  # UpdateMetadata
        self.assertIn(7, internal_keys)  # ControlledShutdown
        self.assertIn(27, internal_keys) # WriteTxnMarkers
        self.assertIn(52, internal_keys) # Vote
        self.assertIn(62, internal_keys) # BrokerRegistration

    def test_do_not_claim_uncovered_apis_implemented(self):
        """
        Verify that uncovered APIs and features are not claimed as implemented:
        ZSTD, GSSAPI, ElectLeaders, DescribeQuorum, AddRaftVoter, RemoveRaftVoter.
        """
        results = cpc.evaluate_protocol_coverage()
        implemented_keys = {op["api_key"] for op in results["implemented_client_operations"]}

        # Unimplemented admin APIs
        self.assertNotIn(43, implemented_keys)
        self.assertNotIn(55, implemented_keys)
        self.assertNotIn(80, implemented_keys)
        self.assertNotIn(81, implemented_keys)

        # Missing features in missing_features list
        missing_features = {f["feature_id"] for f in results["missing_runtime_wiring"]["missing_features"]}
        self.assertIn("codecs.zstd.decode", missing_features)
        self.assertIn("codecs.zstd.encode", missing_features)
        self.assertIn("auth.sasl_gssapi", missing_features)
        self.assertIn("manual_consumer.incremental_fetch_runtime", missing_features)
        self.assertIn("share.v2_runtime", missing_features)

    def test_cli_execution_clean_exit_0(self):
        """
        CLI invocation against repo frozen pins produces exit 0 and human report.
        """
        res = self.run_cli([])
        self.assertEqual(res.returncode, 0, f"Stderr: {res.stderr}")
        self.assertIn("PROTOCOL SCHEMA & COVERAGE DRIFT REPORT", res.stdout)
        self.assertIn("Final Status: PASS", res.stdout)
        self.assertIn("Unclassified Drift Items:   0", res.stdout)

    def test_cli_json_output_and_file_writing(self):
        """
        CLI with --json and -o writes structured JSON report.
        """
        out_file = self.temp_path / "coverage_report.json"
        res = self.run_cli(["--json", "-o", str(out_file)])
        self.assertEqual(res.returncode, 0)
        self.assertTrue(out_file.is_file())

        with open(out_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(data["summary"]["exit_code"], 0)
        self.assertEqual(data["gap_counts"]["unclassified_drift"], 0)
        self.assertEqual(data["summary"]["status"], "PASS (all APIs and versions classified)")

    def test_cli_diff_only(self):
        """
        CLI with --diff-only suppresses the list of implemented APIs.
        """
        res = self.run_cli(["--diff-only"])
        self.assertEqual(res.returncode, 0)
        self.assertNotIn("Implemented Client APIs (62):", res.stdout)
        self.assertIn("Version Gaps (40):", res.stdout)

    def test_cli_self_test_flag(self):
        """
        CLI --self-test flag executes built-in self-test suite.
        """
        res = self.run_cli(["--self-test"])
        self.assertEqual(res.returncode, 0)
        self.assertIn("Protocol coverage self-tests: ALL PASSED", res.stdout)

    def test_cli_synthetic_inventory_file_fails_exit_1(self):
        """
        CLI with custom inventory containing synthetic new API fails with exit 1.
        """
        synthetic_inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
        synthetic_inventory[99] = {
            "name": "SyntheticNewApi",
            "versions": {"4.3.1": [0, 1]},
        }
        inv_file = self.temp_path / "synthetic_inventory.json"
        with open(inv_file, "w", encoding="utf-8") as f:
            json.dump(synthetic_inventory, f)

        res = self.run_cli(["-i", str(inv_file)])
        self.assertEqual(res.returncode, 1)
        self.assertIn("UNCLASSIFIED DRIFT DETECTED", res.stdout)
        self.assertIn("Unclassified API key 99", res.stdout)

    def test_cli_nonexistent_inventory_file_exits_2(self):
        """
        CLI with nonexistent inventory file exits with error code 2.
        """
        res = self.run_cli(["-i", "/nonexistent/path/inv.json"])
        self.assertEqual(res.returncode, 2)
        self.assertIn("Error: inventory file not found", res.stderr)


if __name__ == "__main__":
    unittest.main()

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
from unittest.mock import patch
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
        self.assertEqual(results["summary"]["total_pinned_apis"], 93)
        self.assertEqual(results["summary"]["total_catalog_keys"], 91)
        self.assertEqual(results["summary"]["implemented_client_apis_count"], 71)
        self.assertEqual(results["gap_counts"]["missing_runtime_wiring_apis"], 2)
        self.assertEqual(results["gap_counts"]["excluded_broker_internal_apis"], 20)
        self.assertEqual(results["gap_counts"]["unclassified_drift"], 0)

    def test_share_v2_coverage_matches_official_sdk_ranges_and_qualified_runtime(self):
        repo = FEATURES_PATH.parents[2]
        sdk = json.loads((repo / "docs/evidence/client/KL05-14/three-sdk/manifest.json").read_text())
        for peer, release in sdk["sdk_releases"].items():
            self.assertEqual(len(release["ranges"]), 4)
            for line in release["ranges"]:
                marker, message, first, last = line.split()
                self.assertEqual(marker, "RANGE")
                key = 79 if message.startswith("ShareAcknowledge") else 78
                self.assertEqual(cpc.PINNED_APACHE_APIS[key]["versions"][peer], [int(first), int(last)])
        for key in (78, 79):
            self.assertEqual(cpc.CLIENT_SPOKEN_VERSIONS[key], [0, 1, 2])
        results = cpc.evaluate_protocol_coverage()
        for key in (78, 79):
            gaps = [g for g in results["version_gaps"] if g["api_key"] == key and g["version"] == 2]
            self.assertEqual(gaps, [])
        qualification = json.loads((repo / "docs/evidence/client/KL05-15/whole-compat-12f43986/qualification.json").read_text())
        for peer, cell in qualification["cells"].items():
            report = json.loads((repo / "docs/evidence/client/KL05-15/whole-compat-12f43986" / cell["report"]).read_text())
            self.assertEqual(report["source_sha"], qualification["source_sha"])
            self.assertEqual(report["runtime"]["source_sha"], qualification["source_sha"])
            self.assertEqual(report["runtime"]["scenarios"]["share"], {
                "records": 16, "duplicates": 0, "missing": 0, "corrupt": 0, "status": "passed",
            })
            self.assertEqual(report["runtime"]["share_accepted"], 16)
            for key in (78, 79):
                self.assertEqual(report["runtime"]["api_ranges"][str(key)], cpc.PINNED_APACHE_APIS[key]["versions"][peer])

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
        Both client-facing quorum keys now have runtime methods; broker-internal
        keys (4, 5, 6, 7, 52, etc.) remain excluded from implemented operations.
        """
        results = cpc.evaluate_protocol_coverage()
        implemented_keys = {op["api_key"] for op in results["implemented_client_operations"]}

        # Both client-facing quorum operations now have proven runtime wiring.
        self.assertTrue({80, 81}.issubset(implemented_keys))

        # Broker-internal / clusterAction keys must not be counted as implemented client operations
        for internal_key in (4, 5, 6, 7, 52, 53, 54, 56, 58, 59, 62, 63, 70, 82, 83, 84, 85, 86, 87):
            self.assertNotIn(
                internal_key,
                implemented_keys,
                f"Broker-internal key {internal_key} must not be counted as an implemented client operation",
            )

        # All cataloged client-facing admin keys now have runtime wiring.
        unimpl_keys = {ma["api_key"] for ma in results["missing_runtime_wiring"]["unimplemented_apis"]}
        self.assertEqual(unimpl_keys, {88, 89})

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
        synthetic_inventory[0]["versions"]["synthetic"] = [3, 14]

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
        synthetic_inventory[0]["versions"]["synthetic"] = [3, 14]

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
        self.assertEqual(len(version_gaps), 43)
        # Check key expected version gaps
        self.assertTrue(any(g["api_key"] == 0 and g["version"] == 13 for g in version_gaps))  # Produce v13 vs Apache3.9.1 max11
        self.assertFalse(any(g["api_key"] == 1 and g["version"] == 18 and g.get("direction") == "upstream_cap" for g in version_gaps))  # Fetch18 is implemented; old peer difference stays classified
        self.assertFalse(any(g["api_key"] == 2 and g["version"] == 11 for g in version_gaps))  # ListOffsets v11 implemented
        self.assertFalse(any(g["api_key"] == 78 and g["version"] == 2 for g in version_gaps)) # ShareFetch v2 qualified in KL05-15
        self.assertFalse(any(g["api_key"] == 35 and g["version"] == 5 for g in version_gaps)) # DescribeLogDirs v5 implemented

        # 2. Missing runtime wiring
        self.assertIn("missing_runtime_wiring", results)
        unimpl_apis = results["missing_runtime_wiring"]["unimplemented_apis"]
        self.assertEqual(len(unimpl_apis), 2)
        for item in unimpl_apis:
            self.assertFalse(item["has_runtime_operation"])
            self.assertIn("StreamsGroup", item["name"])

        # 3. Excluded broker-internal APIs
        self.assertIn("excluded_broker_internal", results)
        internal_apis = results["excluded_broker_internal"]["broker_internal_apis"]
        self.assertEqual(len(internal_apis), 20)
        internal_keys = {item["api_key"] for item in internal_apis}
        self.assertIn(4, internal_keys)  # LeaderAndIsr
        self.assertIn(5, internal_keys)  # StopReplica
        self.assertIn(6, internal_keys)  # UpdateMetadata
        self.assertIn(7, internal_keys)  # ControlledShutdown
        self.assertNotIn(27, internal_keys) # Public abort is required
        self.assertIn(52, internal_keys) # Vote
        self.assertIn(62, internal_keys) # BrokerRegistration

    def test_do_not_claim_uncovered_apis_implemented(self):
        """
        Verify that uncovered APIs and features are not claimed as implemented:
        ZSTD, GSSAPI.
        """
        results = cpc.evaluate_protocol_coverage()
        implemented_keys = {op["api_key"] for op in results["implemented_client_operations"]}

        # Unimplemented admin APIs
        self.assertIn(43, implemented_keys)  # Runtime wiring proven by KL05-17.
        self.assertIn(55, implemented_keys)  # Runtime wiring proven by KL05-19.
        self.assertIn(80, implemented_keys)  # Runtime wiring proven by KL05-20.
        self.assertIn(81, implemented_keys)  # Runtime wiring proven by KL05-21.

        # Missing features in missing_features list
        missing_features = {f["feature_id"] for f in results["missing_runtime_wiring"]["missing_features"]}
        self.assertIn("codecs.zstd.decode", missing_features)
        self.assertIn("codecs.zstd.encode", missing_features)
        self.assertIn("auth.sasl_gssapi", missing_features)
        partial_features = {f["feature_id"] for f in results["missing_runtime_wiring"]["partial_features"]}
        self.assertNotIn("manual_consumer.incremental_fetch_runtime", partial_features)
        self.assertNotIn("manual_consumer.incremental_fetch_runtime", missing_features)
        incremental = next(f for f in json.loads(FEATURES_PATH.read_text()) if f["id"] == "manual_consumer.incremental_fetch_runtime")
        self.assertEqual(incremental["disposition"], "present")
        self.assertNotIn("share.v2_runtime", missing_features)
        self.assertNotIn("share.v2_runtime", partial_features)
        share = next(f for f in json.loads(FEATURES_PATH.read_text()) if f["id"] == "share.v2_runtime")
        self.assertEqual(share["disposition"], "present")
        self.assertTrue((FEATURES_PATH.parents[2] / share["evidence"]).is_file())

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
        self.assertNotIn("Implemented Client APIs (71):", res.stdout)
        self.assertIn("Version Gaps (43):", res.stdout)

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

    def test_all_93_current_keys_match_retained_source_contracts(self):
        self.assertEqual(set(cpc.PINNED_APACHE_APIS), set(range(93)))
        for pin in cpc.CURRENT_PINS:
            self.assertEqual({k for k, v in cpc.PINNED_APACHE_APIS.items()
                              if pin in v["current_contracts"]}, set(range(93)))

    def test_omitting_any_current_key_fails_closed(self):
        for key in range(93):
            with self.subTest(key=key):
                inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
                del inventory[key]
                report = cpc.evaluate_protocol_coverage(inventory=inventory)
                self.assertEqual(report["summary"]["exit_code"], 1)
                self.assertTrue(any(d["type"] == "missing_inventory_key" and
                                    d["api_key"] == key for d in report["unclassified_drift"]))

    def test_current_write_markers_and_share_lag_ranges(self):
        for key, expected in ((27, [[1, 1], [1, 2], [1, 2]]),
                              (90, [[0, 0], [0, 1], [0, 1]])):
            for pin, versions in zip(cpc.CURRENT_PINS, expected):
                self.assertEqual(cpc.PINNED_APACHE_APIS[key]["versions"][pin], versions)
        self.assertEqual(cpc.CLIENT_SPOKEN_VERSIONS[27], [0, 1, 2])
        self.assertEqual(cpc.CLIENT_SPOKEN_VERSIONS[90], [0, 1])

    def test_forging_current_version_range_cannot_be_classified_away(self):
        for key in (27, 90):
            with self.subTest(key=key):
                inventory = copy.deepcopy(cpc.PINNED_APACHE_APIS)
                inventory[key]["versions"]["4.3.1"] = [0, 0]
                report = cpc.evaluate_protocol_coverage(
                    inventory=inventory, extra_classified_versions={(key, 0): "pretend classified"})
                self.assertEqual(report["summary"]["exit_code"], 1)
                self.assertTrue(any(d["type"] == "pinned_inventory_mismatch" and
                                    d["api_key"] == key for d in report["unclassified_drift"]))

    def test_client_cap_regression_for_27_or_90_fails(self):
        for key in (27, 90):
            with self.subTest(key=key), patch.dict(cpc.CLIENT_SPOKEN_VERSIONS,
                                                  {key: [0, 1] if key == 27 else [0]}):
                report = cpc.evaluate_protocol_coverage()
                self.assertEqual(report["summary"]["exit_code"], 1)
                self.assertTrue(any(d["type"] == "unclassified_version" and d["api_key"] == key
                                    for d in report["unclassified_drift"]))

    def test_removed_reserved_keys_cannot_be_advertised(self):
        baseline = json.loads((REPO_ROOT / "tests/conformance/broker/features.json").read_text())
        for key in (4, 5, 6, 7):
            with self.subTest(key=key):
                value = copy.deepcopy(baseline)
                next(r for r in value["features"] if r.get("api_key") == key)["implementation"] = "implemented"
                value["implemented_api_versions"].append({"api_key": key, "min_version": 0, "max_version": 0})
                path = self.temp_path / "advertisement.json"
                path.write_text(json.dumps(value))
                report = cpc.evaluate_protocol_coverage(broker_features_path=path)
                self.assertEqual(report["summary"]["exit_code"], 1)
                self.assertTrue(any(d["type"] == "invalid_broker_advertisement" and
                                    d["api_key"] == key for d in report["unclassified_drift"]))

    def test_public_cluster_action_apis_have_dual_use(self):
        report = cpc.evaluate_protocol_coverage()
        operations = {r["api_key"]: r for r in report["implemented_client_operations"]}
        excluded = {r["api_key"] for r in report["excluded_broker_internal"]["broker_internal_apis"]}
        for key in (27, 57):
            self.assertIn(key, operations)
            self.assertTrue(operations[key]["internal_use"])
            self.assertNotIn(key, excluded)
            self.assertTrue(operations[key]["entrypoint"].startswith("partitionline::admin::Admin::"))

    def test_blanket_public_admin_exclusion_is_a_failure(self):
        for key in (27, 57):
            with self.subTest(key=key):
                report = cpc.evaluate_protocol_coverage(extra_classified_apis={key: "excluded_broker_internal"})
                self.assertEqual(report["summary"]["exit_code"], 1)
                self.assertTrue(any(d["type"] == "public_api_excluded" and d["api_key"] == key
                                    for d in report["unclassified_drift"]))

    def test_constants_and_codec_ranges_do_not_establish_streams_runtime(self):
        catalog = API_KEYS_PATH.read_text()
        catalog += "\npub const STREAMS_GROUP_HEARTBEAT: i16 = 88;\npub const STREAMS_GROUP_DESCRIBE: i16 = 89;\n"
        catalog = catalog.replace('        DESCRIBE_SHARE_GROUP_OFFSETS =>',
                                  '        88 => Some("STREAMS_GROUP_HEARTBEAT"),\n'
                                  '        89 => Some("STREAMS_GROUP_DESCRIBE"),\n'
                                  '        DESCRIBE_SHARE_GROUP_OFFSETS =>', 1)
        path = self.temp_path / "api_keys.rs"
        path.write_text(catalog)
        with patch.dict(cpc.CLIENT_SPOKEN_VERSIONS, {88: [0], 89: [0]}):
            report = cpc.evaluate_protocol_coverage(api_keys_path=path)
        self.assertEqual(report["summary"]["exit_code"], 1)
        self.assertTrue({88, 89}.isdisjoint({r["api_key"] for r in report["implemented_client_operations"]}))
        self.assertEqual({d["api_key"] for d in report["unclassified_drift"]
                          if d["type"] == "runtime_unverified"}, {88, 89})

    def test_missing_callable_feature_cannot_be_blessed_by_version_table(self):
        rows = json.loads(FEATURES_PATH.read_text())
        rows = [r for r in rows if r["id"] != "full_admin.abort_transaction"]
        path = self.temp_path / "features.json"
        path.write_text(json.dumps(rows))
        report = cpc.evaluate_protocol_coverage(features_path=path)
        self.assertEqual(report["summary"]["exit_code"], 1)
        self.assertNotIn(27, {r["api_key"] for r in report["implemented_client_operations"]})

    def test_wire_helper_entrypoint_cannot_be_called_runtime(self):
        rows = json.loads(FEATURES_PATH.read_text())
        next(r for r in rows if r["id"] == "full_admin.abort_transaction")["entrypoint"] = (
            "partitionline::protocol::txn::encode_write_txn_markers_request")
        path = self.temp_path / "features.json"
        path.write_text(json.dumps(rows))
        self.assertEqual(cpc.evaluate_protocol_coverage(features_path=path)["summary"]["exit_code"], 1)

    def test_streams_stability_metadata_is_not_lost(self):
        for key in (88, 89):
            for pin in cpc.CURRENT_PINS:
                row = cpc.PINNED_APACHE_APIS[key]["current_contracts"][pin]
                self.assertEqual(row["listeners"], ["broker"])
                self.assertFalse(row["cluster_action"])
                self.assertEqual(row["latest_version_unstable"], pin == "4.1.2")
                self.assertEqual(row["stable_valid_versions"], "none" if pin == "4.1.2" else "0")

    def test_removed_contracts_have_no_usable_versions(self):
        for key in (4, 5, 6, 7):
            for pin in cpc.CURRENT_PINS:
                self.assertIsNone(cpc.PINNED_APACHE_APIS[key]["versions"][pin])
                row = cpc.PINNED_APACHE_APIS[key]["current_contracts"][pin]
                self.assertEqual(row["disposition"], "removed_api_key_reserved")
                self.assertEqual(row["listeners"], [])
                self.assertEqual(row["headers"], [])

    def test_matrix_corruption_fails_against_retained_sources(self):
        matrix = json.loads((REPO_ROOT / "tests/conformance/broker/api-matrix.json").read_text())
        next(r for r in matrix["releases"][2]["inventory"] if r["api_key"] == 27)["request"]["valid_versions"] = "1"
        path = self.temp_path / "api-matrix.json"
        path.write_text(json.dumps(matrix))
        (self.temp_path / "upstream").symlink_to(REPO_ROOT / "tests/conformance/broker/upstream")
        with self.assertRaises(cpc.ProtocolCoverageError):
            cpc.load_current_inventory(path)

    def test_classification_pass_is_not_complete_functionality(self):
        report = cpc.evaluate_protocol_coverage()
        self.assertEqual(report["summary"]["exit_code"], 0)
        self.assertFalse(report["full_current_protocol_complete"])
        self.assertEqual({r["api_key"] for r in report["missing_runtime_wiring"]["unimplemented_apis"]}, {88, 89})
        caps = {(r["api_key"], r["version"]) for r in report["version_gaps"] if r["direction"] == "upstream_cap"}
        self.assertTrue({(8, 10), (9, 10), (22, 6), (45, 1), (66, 2), (80, 1), (24, 4), (24, 5)} <= caps)

    def test_new_current_public_cases_preserve_unqualified_status(self):
        rows = json.loads(CASES_PATH.read_text())["cases"]
        current = [r for r in rows if r["id"].startswith("current-public-api-")]
        self.assertEqual(len(current), 31)
        for row in current:
            self.assertTrue(row["denominator"])
            self.assertIn(row["disposition"], ("not_run", "unsupported"))
            self.assertNotIn("artifacts", row)
        old = next(r for r in rows if r["id"] == "api-broker-internal-027-write-txn-markers")
        self.assertEqual(old["disposition"], "not_applicable")
        self.assertEqual(old["immutable_source_pin"], "cb7e97d3b92a8555aea34d59266a2990c206395f")


if __name__ == "__main__":
    unittest.main()

"""Pinned positive baselines and corrupted source/matrix counterexamples."""
import copy
import gzip
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = ROOT / "tests/conformance/broker"
SPEC = importlib.util.spec_from_file_location("broker_matrix", ROOT / "scripts/check-broker-api-matrix.py")
MATRIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MATRIX)
RETAIN_SPEC = importlib.util.spec_from_file_location("broker_retain", DIRECTORY / "upstream/retain-sources.py")
RETAIN = importlib.util.module_from_spec(RETAIN_SPEC)
RETAIN_SPEC.loader.exec_module(RETAIN)


def tar_bytes(entries):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, content, kind in entries:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.size = len(content) if kind == tarfile.REGTYPE else 0
            if kind == tarfile.SYMTYPE:
                member.linkname = "/tmp/forbidden"
            archive.addfile(member, io.BytesIO(content) if member.size else None)
    return gzip.compress(raw.getvalue(), mtime=0)


class BrokerApiMatrix(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.matrix = MATRIX.load_json(DIRECTORY / "api-matrix.json")
        cls.features = MATRIX.load_json(DIRECTORY / "features.json")
        cls.sources = {release["version"]: MATRIX.read_archive(
            DIRECTORY / "upstream" / release["retained_archive"], release["retained_archive_sha256"])
            for release in cls.matrix["releases"]}

    def verify_mutation(self, mutate_matrix=None, mutate_features=None, mutate_archive=None):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "upstream").mkdir()
            for release in self.matrix["releases"]:
                shutil.copyfile(DIRECTORY / "upstream" / release["retained_archive"],
                                directory / "upstream" / release["retained_archive"])
            matrix, features = copy.deepcopy(self.matrix), copy.deepcopy(self.features)
            if mutate_matrix:
                mutate_matrix(matrix)
            if mutate_features:
                mutate_features(features)
            if mutate_archive:
                mutate_archive(directory / "upstream", matrix)
            matrix_path, features_path = directory / "api-matrix.json", directory / "features.json"
            matrix_path.write_text(json.dumps(matrix), encoding="utf-8")
            features_path.write_text(json.dumps(features), encoding="utf-8")
            return MATRIX.verify(matrix_path, features_path)

    def mutate_spec(self, filename, mutate):
        sources = dict(self.sources["4.3.1"])
        path = MATRIX.MESSAGE_DIR + filename + ".json"
        spec = MATRIX.parse_json(sources[path], commented=True)
        mutate(spec)
        sources[path] = json.dumps(spec).encode("utf-8")
        return MATRIX.derive_inventory(sources)

    def test_real_retained_sources_verify(self):
        report = MATRIX.verify(DIRECTORY / "api-matrix.json", DIRECTORY / "features.json")
        self.assertEqual(report["verdict"], "passed")
        self.assertFalse(report["implementation_claim"])
        self.assertEqual([row["api_keys"] for row in report["releases"]], [93, 93, 93])
        self.assertEqual([row["header_version_pairs"] for row in report["releases"]], [292, 300, 302])

    def test_complete_keys_include_controller_and_new_group_apis(self):
        for release in self.matrix["releases"]:
            rows = release["inventory"]
            self.assertEqual([row["api_key"] for row in rows], list(range(93)))
            self.assertEqual(rows[52]["name"], "VOTE")
            self.assertEqual(rows[52]["listeners"], ["controller"])
            self.assertEqual(rows[62]["name"], "BROKER_REGISTRATION")
            self.assertEqual(rows[62]["listeners"], ["controller"])
            self.assertEqual(rows[92]["name"], "DELETE_SHARE_GROUP_OFFSETS")

    def test_api_versions_response_header_exception_and_classic_client_id(self):
        for release in self.matrix["releases"]:
            row = release["inventory"][18]
            self.assertEqual([(v["api_version"], v["request_header_version"], v["response_header_version"])
                              for v in row["headers"]], [(0, 1, 0), (1, 1, 0), (2, 1, 0), (3, 2, 0), (4, 2, 0)])
            self.assertIn("classic_nullable_string_int16_length", release["header_rules"]["request_client_id_encoding"])

    def test_removed_keys_reserve_ids_without_versions_or_listener(self):
        for release in self.matrix["releases"]:
            removed = [row for row in release["inventory"] if row["disposition"] != "active"]
            self.assertEqual([row["api_key"] for row in removed], [4, 5, 6, 7])
            for row in removed:
                self.assertEqual(row["request"]["valid_versions"], "none")
                self.assertEqual(row["response"]["valid_versions"], "none")
                self.assertEqual(row["headers"], [])
                self.assertEqual(row["listeners"], [])
        for key in range(4, 8):
            row = self.features["features"][key]
            self.assertEqual(row["upstream_applicability"], "not_applicable_removed_upstream")
            self.assertEqual(row["implementation"], "missing")

    def test_unstable_versions_and_release_deltas_are_preserved(self):
        first, second, third = [release["inventory"] for release in self.matrix["releases"]]
        self.assertEqual(first[88]["stable_valid_versions"], "none")
        self.assertEqual(second[88]["stable_valid_versions"], "0")
        self.assertEqual(first[90]["request"]["valid_versions"], "0")
        self.assertEqual(second[90]["request"]["valid_versions"], "0-1")
        self.assertEqual(first[78]["request"]["valid_versions"], "1")
        self.assertEqual(second[78]["request"]["valid_versions"], "1-2")
        self.assertEqual(third[78]["request"]["valid_versions"], "1-2")
        self.assertEqual(third[22]["stable_valid_versions"], "0-5")

    def test_produce_workaround_is_separate_from_schema_support(self):
        for release in self.matrix["releases"]:
            row = release["inventory"][0]
            self.assertEqual(row["request"]["valid_versions"], "3-13")
            self.assertEqual(row["upstream_negotiation"]["schema_minimum"], 3)
            self.assertEqual(row["upstream_negotiation"]["broker_listener_advertised_minimum"], 0)
            self.assertEqual(row["headers"][0]["api_version"], 3)
        self.assertEqual(self.features["implemented_api_versions"], MATRIX.IMPLEMENTED)

    def test_retention_is_byte_preserving_and_deterministic(self):
        for release in self.matrix["releases"]:
            sources = self.sources[release["version"]]
            self.assertIn(b"Apache License", sources["LICENSE"])
            self.assertIn(b"Apache Kafka", sources["NOTICE"])
            self.assertEqual(MATRIX.digest(RETAIN.canonical_archive(sources)), release["retained_archive_sha256"])

    def test_comment_scanner_preserves_urls_and_escaped_quotes(self):
        data = r'''// license http://apache.org
        {"url":"https://example.invalid/a//b", "quoted":"say \"//not comment\"", /* block */ "n":1}
        // tail'''
        parsed = MATRIX.parse_json(data, commented=True)
        self.assertEqual(parsed["url"], "https://example.invalid/a//b")
        self.assertEqual(parsed["quoted"], 'say "//not comment"')
        self.assertEqual(parsed["n"], 1)

    def test_duplicate_json_properties_and_unterminated_comments_fail(self):
        for data in ('{"apiKey":1,"apiKey":2}', '{"a":{"x":1,"x":2}}', '{/* missing', '{"x":"missing}'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                MATRIX.parse_json(data, commented=True)

    def test_original_pin_and_reviewed_subset_hash_fail_closed(self):
        for field in ("commit", "source_archive_sha256", "retained_archive_sha256", "tag", "source_archive_url"):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "pin mismatch"):
                self.verify_mutation(lambda m: m["releases"][0].__setitem__(field, "corrupted"))

    def test_missing_or_duplicate_release_fails(self):
        for mutate in (lambda m: m["releases"].pop(), lambda m: m["releases"].append(m["releases"][0])):
            with self.assertRaisesRegex(ValueError, "release pins"):
                self.verify_mutation(mutate)

    def test_archive_corruption_fails_even_with_unchanged_matrix(self):
        def mutate(directory, matrix):
            path = directory / matrix["releases"][0]["retained_archive"]
            raw = bytearray(path.read_bytes())
            raw[-10] ^= 1
            path.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "archive checksum mismatch"):
            self.verify_mutation(mutate_archive=mutate)

    def test_rewritten_source_and_self_reported_hashes_cannot_bless_a_pin(self):
        def mutate(directory, matrix):
            release = matrix["releases"][0]
            path = directory / release["retained_archive"]
            sources = dict(self.sources[release["version"]])
            source = MATRIX.MESSAGE_DIR + "ProduceRequest.json"
            sources[source] = sources[source].replace(b'"3-13"', b'"0-13"', 1)
            raw = RETAIN.canonical_archive(sources)
            path.write_bytes(raw)
            release["retained_archive_sha256"] = MATRIX.digest(raw)
            release["files_sha256"][source] = MATRIX.digest(sources[source])
        with self.assertRaisesRegex(ValueError, "reviewed retained-source pin mismatch"):
            self.verify_mutation(mutate_archive=mutate)

    def test_missing_or_changed_per_file_hash_fails(self):
        for mutate in (lambda hashes: hashes.pop("LICENSE"), lambda hashes: hashes.__setitem__("LICENSE", "0" * 64)):
            with self.assertRaisesRegex(ValueError, "per-file checksum map mismatch"):
                self.verify_mutation(lambda m: mutate(m["releases"][0]["files_sha256"]))

    def test_matrix_key_duplicates_missing_ranges_and_flags_fail(self):
        mutations = (
            lambda rows: rows.pop(),
            lambda rows: rows.append(rows[0]),
            lambda rows: rows[0].__setitem__("api_key", 1),
            lambda rows: rows[0]["request"].__setitem__("valid_versions", "0-13"),
            lambda rows: rows[19].__setitem__("forwardable", False),
            lambda rows: rows[52].__setitem__("cluster_action", False),
            lambda rows: rows[62].__setitem__("listeners", ["broker"]),
            lambda rows: rows[18]["headers"][4].__setitem__("response_header_version", 1),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate), self.assertRaisesRegex(ValueError, "matrix/source inventory mismatch"):
                self.verify_mutation(lambda m: mutate(m["releases"][0]["inventory"]))

    def test_feature_claims_or_retired_dispositions_fail(self):
        mutations = (
            lambda f: f["implemented_api_versions"].append({"api_key": 18, "min_version": 0, "max_version": 4}),
            lambda f: f["features"][0].__setitem__("implementation", "implemented"),
            lambda f: f["features"][0].__setitem__("qualification", "passed"),
            lambda f: f["features"][4].__setitem__("upstream_applicability", "required"),
            lambda f: f["features"].append(f["features"][0]),
            lambda f: f["features"].pop(0),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.verify_mutation(mutate_features=mutate)

    def test_source_missing_response_and_duplicate_request_fail(self):
        sources = dict(self.sources["4.3.1"])
        sources.pop(MATRIX.MESSAGE_DIR + "FetchResponse.json")
        with self.assertRaisesRegex(ValueError, "missing request/response pair"):
            MATRIX.derive_inventory(sources)
        sources = dict(self.sources["4.3.1"])
        sources[MATRIX.MESSAGE_DIR + "AnotherRequest.json"] = sources[MATRIX.MESSAGE_DIR + "FetchRequest.json"]
        with self.assertRaisesRegex(ValueError, "duplicate request"):
            MATRIX.derive_inventory(sources)

    def test_source_missing_key_and_unknown_key_fail(self):
        sources = dict(self.sources["4.3.1"])
        for kind in ("Request", "Response"):
            sources.pop(MATRIX.MESSAGE_DIR + "Fetch" + kind + ".json")
        with self.assertRaisesRegex(ValueError, "missing API keys"):
            MATRIX.derive_inventory(sources)
        with self.assertRaisesRegex(ValueError, "unknown API key"):
            self.mutate_spec("FetchRequest", lambda spec: spec.__setitem__("apiKey", 93))

    def test_source_pair_disagreement_fails(self):
        for field, value, error in (
            ("validVersions", "4-17", "valid range mismatch"),
            ("flexibleVersions", "0+", "flexible range mismatch"),
            ("name", "OtherResponse", "name mismatch"),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, error):
                self.mutate_spec("FetchResponse", lambda spec: spec.__setitem__(field, value))

    def test_source_bad_listener_and_header_client_id_fail(self):
        with self.assertRaisesRegex(ValueError, "invalid listeners"):
            self.mutate_spec("FetchRequest", lambda spec: spec.__setitem__("listeners", ["admin"]))
        with self.assertRaisesRegex(ValueError, "ClientId must remain classic"):
            self.mutate_spec("RequestHeader", lambda spec: spec["fields"][-1].__setitem__("flexibleVersions", "2+"))

    def test_source_api_keys_missing_duplicate_and_changed_generator_fail(self):
        for transform, expected in (
            (lambda s: s.replace(b"    FETCH(ApiMessageType.FETCH),", b""), "missing ApiKeys enum"),
            (lambda s: s.replace(b"    FETCH(ApiMessageType.FETCH),", b"    FETCH(ApiMessageType.FETCH),\n    FETCH(ApiMessageType.FETCH),"), "duplicate ApiKeys"),
        ):
            sources = dict(self.sources["4.3.1"])
            sources[MATRIX.API_KEYS] = transform(sources[MATRIX.API_KEYS])
            with self.assertRaisesRegex(ValueError, expected):
                MATRIX.derive_inventory(sources)
        sources = dict(self.sources["4.3.1"])
        path = MATRIX.GENERATOR_DIR + "ApiMessageTypeGenerator.java"
        sources[path] = sources[path].replace(b'apiKey == 18', b'apiKey == 17')
        with self.assertRaisesRegex(ValueError, "generator header/unstable rule"):
            MATRIX.derive_inventory(sources)

    def test_archive_duplicate_member_symlink_and_traversal_rejected(self):
        variants = (
            [("LICENSE", b"a", tarfile.REGTYPE), ("LICENSE", b"b", tarfile.REGTYPE)],
            [("LICENSE", b"", tarfile.SYMTYPE)],
            [("../escaped", b"x", tarfile.REGTYPE)],
            [("/tmp/escaped", b"x", tarfile.REGTYPE)],
            [("clients//x", b"x", tarfile.REGTYPE)],
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "unsafe.tar.gz"
            for entries in variants:
                with self.subTest(entries=entries), self.assertRaises(ValueError):
                    path.write_bytes(tar_bytes(entries))
                    MATRIX.read_archive(path)

    def test_archive_limits_and_missing_license_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bounded.tar.gz"
            path.write_bytes(tar_bytes([("LICENSE", b"abcd", tarfile.REGTYPE)]))
            for limit, size, error in (
                ("MAX_ARCHIVE_BYTES", 1, "archive too large"),
                ("MAX_TAR_BYTES", 1, "decompression bound"),
                ("MAX_MEMBER_BYTES", 1, "member exceeds bound"),
                ("MAX_MEMBERS", 0, "too many retained members"),
            ):
                with self.subTest(limit=limit), patch.object(MATRIX, limit, size), self.assertRaisesRegex(ValueError, error):
                    MATRIX.read_archive(path)
            with self.assertRaisesRegex(ValueError, "license/generator source missing"):
                MATRIX.read_archive(path)

    def test_cli_failure_has_nonzero_status_and_retains_failed_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            matrix = copy.deepcopy(self.matrix)
            matrix["releases"][0]["commit"] = "0" * 40
            path, report = directory / "matrix.json", directory / "report.json"
            path.write_text(json.dumps(matrix), encoding="utf-8")
            process = subprocess.run([sys.executable, "-B", str(ROOT / "scripts/check-broker-api-matrix.py"),
                                      "--matrix", str(path), "--features", str(DIRECTORY / "features.json"),
                                      "--report", str(report)], capture_output=True, text=True)
            self.assertEqual(process.returncode, 1)
            result = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result["verdict"], "failed")
            self.assertIn("immutable source pin mismatch", result["error"])


    def implementation_mutation(self, mutate_registry=None, mutate_files=None, mutate_report=None, mutate_metadata_report=None,
                                mutate_produce_report=None, mutate_controller_report=None, mutate_fetch_report=None,
                                mutate_retention_report=None):
        """Synthetic report exercises validation; Rust tests prove real behavior."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            registry = MATRIX.load_json(ROOT / MATRIX.REGISTRY_PATH)
            for label in {**MATRIX.SOURCE_PATHS, **MATRIX.PROFILE_SOURCE_PATHS}:
                target = root / registry[label]
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / registry[label], target)
            shutil.copytree(ROOT / registry["fixture_root"], root / registry["fixture_root"])
            shutil.copytree(ROOT / registry["metadata_fixture_root"], root / registry["metadata_fixture_root"])
            for label in ("produce", "controller", "fetch", "retention"):
                shutil.copytree(ROOT / registry[label + "_fixture_root"], root / registry[label + "_fixture_root"])
            report = {"schema_version": 1, "implemented_api_versions": copy.deepcopy(MATRIX.IMPLEMENTED),
                      "protocol_source_sha256": registry["protocol_source_sha256"],
                      "test_source_sha256": registry["test_source_sha256"], "case_results": []}
            for version in MATRIX.TARGETS:
                golden = MATRIX.load_json(root / registry["fixture_root"] / version / "goldens.json")
                report["case_results"].extend({"release": version, "case": case["name"],
                                               "response_hex": case["response_hex"]} for case in golden["cases"])
            metadata_report = {"schema_version": 1, "implemented_api_versions": copy.deepcopy(MATRIX.IMPLEMENTED),
                               "metadata_source_sha256": registry["metadata_source_sha256"],
                               "metadata_test_source_sha256": registry["metadata_test_source_sha256"], "case_results": []}
            for version in MATRIX.TARGETS:
                golden = MATRIX.load_json(root / registry["metadata_fixture_root"] / version / "goldens.json")
                metadata_report["case_results"].extend({"release": version, "case": case["name"],
                                                        "response_hex": case["response_hex"]} for case in golden["cases"])
            produce_report = {"schema_version": 1, "data_api_versions": copy.deepcopy(MATRIX.DATA_APIS),
                              "case_results": []}
            for release in MATRIX.TARGETS:
                golden = MATRIX.load_json(root / registry["produce_fixture_root"] / release / "goldens.json")
                produce_report["case_results"].extend({"release": release, "case": case["name"],
                    "outcome": case["expected_outcome"], "response_hex": case["response_hex"]} for case in golden["cases"])
            fetch_report = {"schema_version": 1, "read_write_api_versions": copy.deepcopy(MATRIX.READ_WRITE_APIS),
                            "case_results": [], "api_versions_cases": []}
            for release in MATRIX.TARGETS:
                golden = MATRIX.load_json(root / registry["fetch_fixture_root"] / release / "goldens.json")
                fetch_report["case_results"].extend({"release": release, "case": case["name"],
                    "outcome": case["expected_outcome"], "response_hex": case["response_hex"]} for case in golden["cases"])
                for version in range(5):
                    fetch_report["api_versions_cases"].append({"release": release, "version": version,
                        "correlation_id": 7, "header_version": 0,
                        "response_hex": self.synthetic_api_versions(version, 7, MATRIX.READ_WRITE_APIS)})
            controller_report = {"schema_version": 1, "profile": "fixed-controller-v0",
                                 "qualification": "not_run", "independent_fixture_cases": 201,
                                 "implemented_api_versions": copy.deepcopy(MATRIX.CONTROLLER_APIS),
                                 "case_results": []}
            retention_report = {"schema_version": 1, "retention_api_versions": copy.deepcopy(MATRIX.RETENTION_APIS),
                                "case_results": [], "api_versions_cases": []}
            for release in MATRIX.TARGETS:
                golden = MATRIX.load_json(root / registry["retention_fixture_root"] / release / "goldens.json")
                retention_report["case_results"].extend({"release": release, "case": case["name"],
                    "outcome": case["expected_outcome"], "response_hex": case["response_hex"]} for case in golden["cases"])
                metadata = MATRIX.load_json(root / registry["metadata_fixture_root"] / release / "goldens.json")
                for version in range(5):
                    case = next(row for row in metadata["cases"] if row["name"] == f"api-versions-v{version}")
                    retention_report["api_versions_cases"].append({"release": release, "api_version": version,
                        "correlation_id": 7, "request_hex": case["request_hex"],
                        "response_hex": self.synthetic_api_versions(version, 7, MATRIX.RETENTION_APIS)})
            for label in ("controller_source", "controller_test_source", "election_source", "raft_module_source"):
                controller_report[label + "_sha256"] = registry[label + "_sha256"]
            controller = MATRIX.load_json(root / registry["controller_fixture_root"] / "manifest.json")
            for release in controller["releases"]:
                controller_report["case_results"].extend({"release": release["release"], "case": case["name"],
                    "response_hex": (root / registry["controller_fixture_root"] / release["release"] / case["response_file"]).read_bytes().hex()
                    if case["expected_disposition"] == "response" else None} for case in release["cases"])
            if mutate_produce_report:
                mutate_produce_report(produce_report)
            if mutate_controller_report:
                mutate_controller_report(controller_report)
            if mutate_fetch_report:
                mutate_fetch_report(fetch_report)
            if mutate_retention_report:
                mutate_retention_report(retention_report)
            if mutate_metadata_report:
                mutate_metadata_report(metadata_report)
            if mutate_registry:
                mutate_registry(registry)
            if mutate_files:
                mutate_files(root, registry)
            if mutate_report:
                mutate_report(report)
            registry_path, report_path = root / "registry.json", root / "report.json"
            registry_path.write_text(json.dumps(registry), encoding="utf-8")
            report_path.write_text(json.dumps(report), encoding="utf-8")
            metadata_path = root / "metadata-report.json"
            metadata_path.write_text(json.dumps(metadata_report), encoding="utf-8")
            inventories = {row["version"]: row["inventory"] for row in self.matrix["releases"]}
            produce_path, controller_path = root / "produce-report.json", root / "controller-report.json"
            produce_path.write_text(json.dumps(produce_report), encoding="utf-8")
            controller_path.write_text(json.dumps(controller_report), encoding="utf-8")
            fetch_path = root / "fetch-report.json"
            fetch_path.write_text(json.dumps(fetch_report), encoding="utf-8")
            retention_path = root / "retention-report.json"
            retention_path.write_text(json.dumps(retention_report), encoding="utf-8")
            return MATRIX.verify_implementation(registry_path, root, inventories, report_path, metadata_path,
                                                produce_path, controller_path, read_write_handler_report=fetch_path,
                                                retention_handler_report=retention_path)

    @staticmethod
    def synthetic_api_versions(version, correlation, apis):
        """Synthetic decoder controls only; actual compiled/TCP receipts qualify behavior."""
        data = correlation.to_bytes(4, "big", signed=True) + b"\x00\x00"
        data += bytes([len(apis) + 1]) if version >= 3 else len(apis).to_bytes(4, "big")
        for api in apis:
            data += b"".join(api[field].to_bytes(2, "big", signed=True)
                             for field in ("api_key", "min_version", "max_version"))
            if version >= 3:
                data += b"\x00"
        if version >= 1:
            data += b"\x00" * 4
        if version >= 3:
            data += b"\x00"
        return data.hex()

    def test_checked_registry_and_synthetic_report_gate(self):
        registry, report = self.implementation_mutation()
        self.assertEqual(registry["implemented_api_versions"], MATRIX.IMPLEMENTED)
        self.assertTrue(report["compiled_handler_report_checked"])
        self.assertEqual(report["golden_cases"], 99)
        self.assertTrue(report["compiled_metadata_report_checked"])
        self.assertGreaterEqual(report["metadata_golden_cases"], 84)
        self.assertEqual(report["qualification"], "not_run")

    def test_registry_forged_extra_or_out_of_range_claims_fail(self):
        claims = ([], [{"api_key": 18, "min_version": 0, "max_version": 5}],
                  [{"api_key": 18, "min_version": -1, "max_version": 4}],
                  [{"api_key": 3, "min_version": 0, "max_version": 4}],
                  MATRIX.IMPLEMENTED * 2,
                  [dict(MATRIX.IMPLEMENTED[0], min_version=False)],
                  [dict(MATRIX.IMPLEMENTED[0], max_version=4.0)])
        for claim in claims:
            with self.subTest(claim=claim), self.assertRaisesRegex(ValueError, "implementation claim"):
                self.implementation_mutation(lambda r: r.__setitem__("implemented_api_versions", claim))

    def test_registry_cannot_claim_qualification_or_unchecked_source(self):
        for field, value in (("qualification", "passed"), ("implementation_gate", "none"),
                             ("protocol_source", "../outside"), ("test_source_sha256", "0" * 64),
                             ("fixture_root", "../outside"), ("standalone_api_versions", MATRIX.IMPLEMENTED)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.implementation_mutation(lambda r: r.__setitem__(field, value))
        with self.assertRaisesRegex(ValueError, "source checksum"):
            self.implementation_mutation(mutate_files=lambda root, r: (root / r["protocol_source"]).write_text("forged"))

    def test_registry_missing_extra_and_changed_fixtures_fail(self):
        mutations = (
            lambda root, r: (root / r["fixture_root"] / "4.3.1/v0-named.response.bin").unlink(),
            lambda root, r: (root / r["fixture_root"] / "extra.bin").write_bytes(b"extra"),
            lambda root, r: (root / r["fixture_root"] / "4.3.1/v0-named.response.bin").write_bytes(b"wrong"),
        )
        for mutate in mutations:
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_files=mutate)

    def test_self_rehashed_golden_rewrite_still_fails_independent_pin(self):
        def mutate(root, registry):
            path = root / registry["fixture_root"] / "4.3.1/goldens.json"
            golden = MATRIX.load_json(path)
            golden["cases"][0]["response_hex"] = "00"
            path.write_text(json.dumps(golden), encoding="utf-8")
            registry["fixtures_sha256"]["4.3.1/goldens.json"] = MATRIX.digest(path.read_bytes())
        with self.assertRaisesRegex(ValueError, "independent golden manifest"):
            self.implementation_mutation(mutate_files=mutate)

    def test_compiled_handler_report_corruption_fails(self):
        mutations = (
            lambda r: r.__setitem__("protocol_source_sha256", "0" * 64),
            lambda r: r["implemented_api_versions"][0].__setitem__("max_version", 5),
            lambda r: r["case_results"].pop(),
            lambda r: r["case_results"].__setitem__(0, r["case_results"][1]),
            lambda r: r["case_results"][0].__setitem__("response_hex", "00"),
            lambda r: r["case_results"][11].pop("response_hex"),
            lambda r: r["case_results"][0].__setitem__("case", "forged"),
        )
        for mutate in mutations:
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_report=mutate)

    def test_metadata_missing_changed_and_self_rehashed_fixtures_fail(self):
        def remove(root, registry):
            target = next(path for path in (root / registry["metadata_fixture_root"] / "4.3.1").glob("*.response.bin"))
            target.unlink()
        def corrupt(root, registry):
            target = next(path for path in (root / registry["metadata_fixture_root"] / "4.3.1").glob("*.request.bin"))
            target.write_bytes(b"corrupt")
        def rewrite(root, registry):
            target = root / registry["metadata_fixture_root"] / "4.3.1/goldens.json"
            golden = MATRIX.load_json(target)
            golden["cases"].pop()
            target.write_text(json.dumps(golden), encoding="utf-8")
            registry["metadata_fixtures_sha256"]["4.3.1/goldens.json"] = MATRIX.digest(target.read_bytes())
        def index(root, registry):
            target = root / registry["metadata_fixture_root"] / "4.3.1/cases.tsv"
            target.write_text("forged\t3\t13\tfixture\n", encoding="utf-8")
            registry["metadata_fixtures_sha256"]["4.3.1/cases.tsv"] = MATRIX.digest(target.read_bytes())
        for mutate in (remove, corrupt, rewrite, index):
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_files=mutate)

    def test_metadata_source_and_report_cannot_forge_runtime_coverage(self):
        for field, value in (("metadata_source", "../outside"), ("metadata_test_source_sha256", "0" * 64),
                             ("metadata_fixture_root", "../outside")):
            with self.assertRaises(ValueError):
                self.implementation_mutation(lambda registry: registry.__setitem__(field, value))
        for mutate in (
            lambda report: report.__setitem__("metadata_source_sha256", "0" * 64),
            lambda report: report.__setitem__("schema_version", True),
            lambda report: report["case_results"].pop(),
            lambda report: report["case_results"].__setitem__(0, report["case_results"][1]),
            lambda report: report["case_results"][0].__setitem__("response_hex", "00"),
            lambda report: report["case_results"][0].__setitem__("release", "unreviewed"),
            lambda report: report["implemented_api_versions"][2].__setitem__("max_version", 7),
            lambda report: next(row for row in report["case_results"] if row["response_hex"] is None).__setitem__("response_hex", "00"),
        ):
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_metadata_report=mutate)


    def test_feature_versions_and_local_gate_cannot_be_forged(self):
        for mutate in (
            lambda f: f["features"][18].__setitem__("implemented_versions", "0-5"),
            lambda f: f["features"][18].__setitem__("qualification", "passed"),
            lambda f: f["features"][3].__setitem__("implemented_versions", "0"),
            lambda f: f.__setitem__("implementation_registry", "elsewhere"),
        ):
            with self.assertRaises(ValueError):
                self.verify_mutation(mutate_features=mutate)


    def test_optional_profiles_positive_baseline_is_scoped(self):
        registry, report = self.implementation_mutation()
        self.assertEqual(registry["implemented_api_versions"], MATRIX.IMPLEMENTED)
        self.assertEqual(registry["data_api_versions"], MATRIX.DATA_APIS)
        self.assertEqual(registry["controller_api_versions"], MATRIX.CONTROLLER_APIS)
        self.assertEqual(report["produce_golden_cases"], 708)
        self.assertEqual(report["controller_golden_cases"], 201)
        self.assertTrue(report["compiled_produce_report_checked"])
        self.assertTrue(report["compiled_controller_report_checked"])
        self.assertFalse(report["controller_core_state_reported"])
        self.assertIn("reflect registry", report["source_hash_scope"])
        self.assertEqual(report["qualification"], "not_run")

    def test_profile_registry_ranges_sources_and_gates_fail_closed(self):
        for field, value in (("data_api_versions", MATRIX.IMPLEMENTED),
                             ("controller_api_versions", MATRIX.CONTROLLER_APIS * 2),
                             ("data_implementation_gate", "none"),
                             ("controller_implementation_gate", "none"),
                             ("produce_source", "../outside"),
                             ("controller_source_sha256", "0" * 64),
                             ("election_source_sha256", "0" * 64),
                             ("raft_module_source_sha256", "0" * 64),
                             ("produce_fixture_root", "../outside")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.implementation_mutation(lambda registry: registry.__setitem__(field, value))
        for profile in ("data_api_versions", "controller_api_versions"):
            for invalid in (False, 0.0, -1, 99):
                with self.subTest(profile=profile, invalid=invalid), self.assertRaises(ValueError):
                    self.implementation_mutation(lambda registry: registry[profile][0].__setitem__("min_version", invalid))
        with self.assertRaisesRegex(ValueError, "profile source checksum"):
            self.implementation_mutation(mutate_files=lambda root, registry:
                (root / registry["produce_source"]).write_text("forged"))

    def test_produce_report_missing_forged_bytes_outcomes_and_profiles_fail(self):
        for mutate in (
            lambda report: report.__setitem__("schema_version", True),
            lambda report: report["case_results"].pop(),
            lambda report: report["case_results"].__setitem__(0, report["case_results"][1]),
            lambda report: report["case_results"][0].__setitem__("response_hex", "00"),
            lambda report: report["case_results"][0].__setitem__("outcome", "close"),
            lambda report: report["case_results"][0].__setitem__("release", "unreviewed"),
            lambda report: next(row for row in report["case_results"] if row["outcome"] == "no_response_keep_open").__setitem__("response_hex", ""),
            lambda report: next(row for row in report["case_results"] if row["outcome"] == "close").__setitem__("outcome", "no_response_keep_open"),
            lambda report: report["data_api_versions"][0].__setitem__("min_version", 0),
            lambda report: report["data_api_versions"].append({"api_key": 1, "min_version": 4, "max_version": 6}),
        ):
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_produce_report=mutate)

    def test_controller_report_missing_forged_bytes_labels_and_profiles_fail(self):
        for mutate in (
            lambda report: report.__setitem__("schema_version", True),
            lambda report: report["case_results"].pop(),
            lambda report: report["case_results"].__setitem__(0, report["case_results"][1]),
            lambda report: report["case_results"][0].__setitem__("response_hex", "00"),
            lambda report: report["case_results"][0].__setitem__("case", "forged"),
            lambda report: next(row for row in report["case_results"] if row["response_hex"] is None).__setitem__("response_hex", "00"),
            lambda report: report.__setitem__("election_source_sha256", "0" * 64),
            lambda report: report.__setitem__("raft_module_source_sha256", "0" * 64),
            lambda report: report.__setitem__("controller_source_sha256", "0" * 64),
            lambda report: report["implemented_api_versions"][1].__setitem__("max_version", 1),
            lambda report: report.__setitem__("qualification", "passed"),
            lambda report: report.__setitem__("profile", "full-kraft"),
        ):
            with self.assertRaises(ValueError):
                self.implementation_mutation(mutate_controller_report=mutate)

    def test_produce_and_controller_rehashed_manifest_forgery_fails(self):
        for label, filename, mutate in (
            ("fetch", "4.3.1/goldens.json", lambda manifest: manifest["cases"][0].__setitem__("response_hex", "00")),
            ("produce", "4.3.1/goldens.json", lambda manifest: manifest["cases"][0].__setitem__("response_hex", "00")),
            ("controller", "manifest.json", lambda manifest: manifest["releases"][0]["cases"][0].__setitem__("core_term", "999")),
        ):
            def corrupt(root, registry):
                path = root / registry[label + "_fixture_root"] / filename
                manifest = MATRIX.load_json(path)
                mutate(manifest)
                path.write_text(json.dumps(manifest), encoding="utf-8")
                registry[label + "_fixtures_sha256"][filename] = MATRIX.digest(path.read_bytes())
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "independent .*manifest checksum"):
                self.implementation_mutation(mutate_files=corrupt)

    def test_profile_fixture_corruption_missing_extra_and_index_fail(self):
        for label, filename in (("produce", "4.3.1/produce-v3-acks1.response.bin"),
                                ("fetch", "4.3.1/fetch-v4-offset0.response.bin"),
                                ("controller", "4.3.1/vote-positive.response.frame.bin")):
            for action in ("delete", "corrupt", "index", "extra"):
                def mutate(root, registry):
                    base = root / registry[label + "_fixture_root"]
                    path = base / filename
                    if action == "delete":
                        path.unlink()
                    elif action == "corrupt":
                        path.write_bytes(b"forged")
                        registry[label + "_fixtures_sha256"][filename] = MATRIX.digest(path.read_bytes())
                    elif action == "index":
                        path = base / "4.3.1/cases.tsv"
                        path.write_text("forged", encoding="utf-8")
                        registry[label + "_fixtures_sha256"]["4.3.1/cases.tsv"] = MATRIX.digest(path.read_bytes())
                    else:
                        path = base / "extra.bin"
                        path.write_bytes(b"extra")
                        registry[label + "_fixtures_sha256"]["extra.bin"] = MATRIX.digest(path.read_bytes())
                with self.subTest(label=label, action=action), self.assertRaises(ValueError):
                    self.implementation_mutation(mutate_files=mutate)

    def test_data_wire_profile_actual_baseline_and_forged_fields_fail(self):
        report = MATRIX.load_json(ROOT / "docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "wire.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            self.assertEqual(MATRIX.verify_data_wire_profile(path)["exchanges_checked"], 30)
            for mutate in (
                lambda value: value["cases"].pop(),
                lambda value: value["cases"].__setitem__(0, value["cases"][1]),
                lambda value: value["cases"][0].__setitem__("response_hex", value["cases"][0]["response_hex"] + "00"),
                lambda value: value["cases"][0].__setitem__("response_hex", "00"),
                lambda value: value["cases"][0].__setitem__("response_hex", value["cases"][0]["response_hex"][:28] + "0000" + value["cases"][0]["response_hex"][32:]),
                lambda value: value["cases"][0].__setitem__("correlation_id", 99),
                lambda value: value["cases"][0].__setitem__("api_version", False),
                lambda value: value.__setitem__("source_sha", "0" * 40),
            ):
                value = copy.deepcopy(report)
                mutate(value)
                path.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    MATRIX.verify_data_wire_profile(path)

    def test_controller_tcp_bytes_and_frames_positive_and_corrupt_controls(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            expected = {}
            names = [f"api-versions-{version}" for version in range(5)] + ["vote-positive"]
            for release in MATRIX.TARGETS:
                target = directory / release
                target.mkdir()
                for name in names:
                    base = ROOT / "partitionline-broker/tests/fixtures/raft-protocol" / release
                    payload = (base / (name + ".response.bin")).read_bytes()
                    expected[(release, name)] = payload.hex()
                    (target / (name + ".actual-response.bin")).write_bytes(payload)
                    shutil.copyfile(base / (name + ".response.frame.bin"), target / (name + ".actual-frame.bin"))
            self.assertEqual(len(MATRIX.verify_controller_tcp(directory, expected)), 36)
            frame = directory / "4.3.1/api-versions-0.actual-frame.bin"
            original = frame.read_bytes()
            for forged in (b"forged", original[:3] + b"\x00" + original[4:], original + b"\x00"):
                frame.write_bytes(forged)
                with self.assertRaises(ValueError):
                    MATRIX.verify_controller_tcp(directory, expected)
            frame.write_bytes(original)
            payload = directory / "4.3.1/api-versions-0.actual-response.bin"
            payload.write_bytes(b"forged")
            with self.assertRaisesRegex(ValueError, "response/golden"):
                MATRIX.verify_controller_tcp(directory, expected)
            payload.unlink()
            with self.assertRaisesRegex(ValueError, "missing/extra"):
                MATRIX.verify_controller_tcp(directory, expected)

    def test_unreviewed_global_profile_claims_fail(self):
        with self.assertRaisesRegex(ValueError, "unreviewed profile"):
            self.implementation_mutation(lambda registry:
                registry.__setitem__("full_broker_api_versions", [{"api_key": 0, "min_version": 0, "max_version": 99}]))
        with self.assertRaisesRegex(ValueError, "unreviewed profile"):
            self.verify_mutation(mutate_features=lambda features:
                features.__setitem__("full_controller_api_versions", MATRIX.CONTROLLER_APIS))

    def test_optional_feature_dispositions_cannot_claim_full_or_modern_support(self):
        for key in (0, 1, 2, 21, 52, 53, 54):
            for field, value in (("implementation", "implemented"), ("implemented_versions", "0-99"),
                                 ("implementation_profile", "default"), ("qualification", "passed")):
                with self.subTest(key=key, field=field), self.assertRaises(ValueError):
                    self.verify_mutation(mutate_features=lambda features:
                        features["features"][key].__setitem__(field, value))
        for field in ("data_api_versions", "read_write_api_versions", "controller_api_versions", "retention_api_versions"):
            with self.assertRaises(ValueError):
                self.verify_mutation(mutate_features=lambda features: features[field].pop())

    def test_read_write_seven_profile_and_compiled_layout_baseline(self):
        registry, report = self.implementation_mutation()
        self.assertEqual(registry["read_write_api_versions"], MATRIX.READ_WRITE_APIS)
        self.assertEqual(registry["data_api_versions"], MATRIX.DATA_APIS)
        self.assertEqual(registry["implemented_api_versions"], MATRIX.IMPLEMENTED)
        self.assertEqual(report["fetch_golden_cases"], 366)
        self.assertTrue(report["compiled_fetch_report_checked"])
        self.assertEqual(report["compiled_read_write_api_versions_layouts_checked"], 15)

    def test_fetch_report_missing_forged_outcomes_bytes_layouts_and_profiles_fail(self):
        for mutate in (
            lambda value: value.__setitem__("schema_version", True),
            lambda value: value["case_results"].pop(),
            lambda value: value["case_results"].__setitem__(0, value["case_results"][1]),
            lambda value: value["case_results"][0].__setitem__("response_hex", "00"),
            lambda value: value["case_results"][0].__setitem__("outcome", "structural_reject"),
            lambda value: next(r for r in value["case_results"] if r["response_hex"] is None).__setitem__("response_hex", "00"),
            lambda value: value["read_write_api_versions"][1].__setitem__("max_version", 16),
            lambda value: value["api_versions_cases"].pop(),
            lambda value: value["api_versions_cases"].__setitem__(0, value["api_versions_cases"][1]),
            lambda value: value["api_versions_cases"][0].__setitem__("header_version", False),
            lambda value: value["api_versions_cases"][0].__setitem__("version", True),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", value["api_versions_cases"][0]["response_hex"][:24] + "0000" + value["api_versions_cases"][0]["response_hex"][28:]),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", value["api_versions_cases"][0]["response_hex"] + "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("correlation_id", 99),
            lambda value: value["api_versions_cases"][0].update(correlation_id=99,
                response_hex=self.synthetic_api_versions(0, 99, MATRIX.READ_WRITE_APIS)),
            lambda value: value.__setitem__("qualification", "passed"),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.implementation_mutation(mutate_fetch_report=mutate)

    def test_read_write_registry_source_and_profile_forgery_fail(self):
        for field, value in (("read_write_api_versions", MATRIX.DATA_APIS),
                             ("read_write_implementation_gate", "none"),
                             ("fetch_source_sha256", "0" * 64),
                             ("fetch_fixture_root", "../outside")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.implementation_mutation(lambda registry: registry.__setitem__(field, value))
        with self.assertRaises(ValueError):
            self.implementation_mutation(mutate_files=lambda root, registry:
                (root / registry["fetch_source"]).write_text("forged"))

    def test_read_write_wire_synthetic_controls_reject_missing_pairs_and_forged_fields(self):
        original = MATRIX.load_json(ROOT / "docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json")
        report = {"schema_version": 1, "source_sha": "58810f2ed90ac9643d59a60098a29f5a2f87a8d0",
                  "passed": True, "actual_exchanges": 60, "cases": []}
        # Copy real header layouts only; seven-profile responses/extra phases
        # are synthetic controls. Independent live68 receipts prove execution.
        for row in original["cases"]:
            for phase in ("seed", "restart"):
                case = copy.deepcopy(row)
                case["phase"] = phase
                case["response_hex"] = self.synthetic_api_versions(
                    case["api_version"], case["correlation_id"], MATRIX.READ_WRITE_APIS)
                report["cases"].append(case)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "synthetic-wire-control.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            self.assertEqual(MATRIX.verify_read_write_wire_profile(path)["exchanges_checked"], 60)
            for mutate in (
                lambda value: value["cases"].pop(),
                lambda value: value["cases"].__setitem__(0, value["cases"][1]),
                lambda value: value["cases"][0].__setitem__("phase", "unknown"),
                lambda value: value["cases"][0].__setitem__("request_hex", "00"),
                lambda value: value["cases"][0].__setitem__("response_hex", "00"),
                lambda value: value["cases"][0].__setitem__("response_hex", value["cases"][0]["response_hex"][:24] + "0000" + value["cases"][0]["response_hex"][28:]),
                lambda value: value["cases"][0].__setitem__("response_hex", value["cases"][0]["response_hex"] + "00"),
                lambda value: value["cases"][0].__setitem__("correlation_id", 999),
                lambda value: value["cases"][0].__setitem__("api_version", True),
                lambda value: value["cases"][0].__setitem__("response_header_version", False),
                lambda value: value.__setitem__("source_sha", "0" * 40),
                lambda value: value.__setitem__("schema_version", True),
            ):
                changed = copy.deepcopy(report)
                mutate(changed)
                path.write_text(json.dumps(changed), encoding="utf-8")
                with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                    MATRIX.verify_read_write_wire_profile(path)

    def test_fetch_seed_cannot_be_rehashed_into_unreviewed_history(self):
        def corrupt(root, registry):
            for release in MATRIX.TARGETS:
                name = f"{release}/log-batch-0.bin"
                path = root / registry["fetch_fixture_root"] / name
                path.write_bytes(b"forged-seed")
                registry["fetch_fixtures_sha256"][name] = MATRIX.digest(path.read_bytes())
        with self.assertRaisesRegex(ValueError, "independently anchored response records"):
            self.implementation_mutation(mutate_files=corrupt)

    def test_retention_eight_profile_preserves_other_profiles_and_checks_all_layouts(self):
        registry, report = self.implementation_mutation()
        self.assertEqual(registry["retention_api_versions"], MATRIX.RETENTION_APIS)
        self.assertEqual(registry["implemented_api_versions"], MATRIX.IMPLEMENTED)
        self.assertEqual(registry["data_api_versions"], MATRIX.DATA_APIS)
        self.assertEqual(registry["read_write_api_versions"], MATRIX.READ_WRITE_APIS)
        self.assertEqual(registry["controller_api_versions"], MATRIX.CONTROLLER_APIS)
        self.assertEqual(report["retention_golden_cases"], 363)
        self.assertEqual(report["retention_response_cases"], 345)
        self.assertEqual(report["retention_local_rejections"], 18)
        self.assertTrue(report["compiled_retention_report_checked"])
        self.assertEqual(report["compiled_retention_api_versions_layouts_checked"], 15)
        self.assertEqual(report["qualification"], "not_run")

    def test_retention_registry_range_gate_source_and_fixture_path_forgery_fail(self):
        for field, value in (
            ("retention_api_versions", MATRIX.READ_WRITE_APIS),
            ("retention_api_versions", MATRIX.RETENTION_APIS * 2),
            ("retention_api_versions", MATRIX.RETENTION_APIS[:-1] + [{"api_key": 21, "min_version": 0, "max_version": 3}]),
            ("retention_implementation_gate", "KL11-07"),
            ("retention_source_sha256", "0" * 64),
            ("retention_test_source_sha256", "0" * 64),
            ("segments_source_sha256", "0" * 64),
            ("partition_source_sha256", "0" * 64),
            ("retention_source", "../outside"),
            ("retention_fixture_root", "../outside"),
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.implementation_mutation(lambda registry: registry.__setitem__(field, value))

    def test_retention_actual_source_changes_fail_without_registry_rebinding(self):
        for label in ("retention_source", "retention_test_source", "segments_source",
                      "segments_test_source", "partition_source", "partition_test_source"):
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "profile source checksum mismatch"):
                self.implementation_mutation(mutate_files=lambda root, registry:
                    (root / registry[label]).write_text("forged source", encoding="utf-8"))

    def test_retention_report_missing_duplicate_outcome_and_response_forgery_fail(self):
        for mutate in (
            lambda value: value.__setitem__("schema_version", True),
            lambda value: value["case_results"].pop(),
            lambda value: value["case_results"].__setitem__(0, value["case_results"][1]),
            lambda value: value["case_results"][0].__setitem__("response_hex", "00"),
            lambda value: value["case_results"][0].__setitem__("response_hex", value["case_results"][0]["response_hex"] + "00"),
            lambda value: value["case_results"][0].__setitem__("outcome", "reject"),
            lambda value: value["case_results"][0].__setitem__("release", "4.0.0"),
            lambda value: next(row for row in value["case_results"] if row["outcome"] == "reject").__setitem__("response_hex", "00000007"),
            lambda value: value["retention_api_versions"][-1].__setitem__("min_version", False),
            lambda value: value["retention_api_versions"][-1].__setitem__("max_version", 3),
            lambda value: value.__setitem__("qualification", "passed"),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.implementation_mutation(mutate_retention_report=mutate)

    def test_retention_api_versions_missing_pairs_and_forged_input_output_fail(self):
        for mutate in (
            lambda value: value["api_versions_cases"].pop(),
            lambda value: value["api_versions_cases"].__setitem__(0, value["api_versions_cases"][1]),
            lambda value: value["api_versions_cases"][0].__setitem__("api_version", False),
            lambda value: value["api_versions_cases"][0].__setitem__("correlation_id", 99),
            lambda value: value["api_versions_cases"][0].__setitem__("request_hex", "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("request_hex", value["api_versions_cases"][0]["request_hex"] + "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", value["api_versions_cases"][0]["response_hex"] + "00"),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", self.synthetic_api_versions(0, 7, MATRIX.READ_WRITE_APIS)),
            lambda value: value["api_versions_cases"][0].__setitem__("response_hex", self.synthetic_api_versions(0, 99, MATRIX.RETENTION_APIS)),
        ):
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.implementation_mutation(mutate_retention_report=mutate)

    def test_retention_rehashed_manifest_cannot_bless_changed_policy_or_observations(self):
        for mutate in (
            lambda manifest: manifest["cases"].pop(),
            lambda manifest: manifest["cases"][0].__setitem__("name", "../escaped"),
            lambda manifest: manifest["cases"][0].__setitem__("response_hex", "00"),
            lambda manifest: manifest["cases"][0]["apache_request_parse"].__setitem__("accepted", False),
            lambda manifest: manifest["cases"][-1]["apache_request_parse"].__setitem__("remaining_bytes", 0),
            lambda manifest: manifest.__setitem__("actual_serializer_parser_checks", 999),
        ):
            def corrupt(root, registry):
                filename = "4.3.1/goldens.json"
                path = root / registry["retention_fixture_root"] / filename
                manifest = MATRIX.load_json(path)
                mutate(manifest)
                path.write_text(json.dumps(manifest), encoding="utf-8")
                registry["retention_fixtures_sha256"][filename] = MATRIX.digest(path.read_bytes())
            with self.subTest(mutation=mutate), self.assertRaisesRegex(ValueError, "independent retention manifest checksum"):
                self.implementation_mutation(mutate_files=corrupt)

    def test_retention_fixture_missing_extra_corrupt_and_index_controls_fail(self):
        for action in ("missing", "extra", "corrupt", "index", "symlink"):
            def corrupt(root, registry):
                base = root / registry["retention_fixture_root"]
                name = "4.3.1/delete-records-v0-offset--1.response.bin"
                path = base / name
                if action == "missing":
                    path.unlink()
                    del registry["retention_fixtures_sha256"][name]
                elif action == "extra":
                    (base / "extra.bin").write_bytes(b"extra")
                    registry["retention_fixtures_sha256"]["extra.bin"] = MATRIX.digest(b"extra")
                elif action == "symlink":
                    path.unlink()
                    path.symlink_to(base / "4.1.2/delete-records-v0-offset--1.response.bin")
                else:
                    if action == "index":
                        name = "4.3.1/cases.tsv"
                        path = base / name
                    path.write_bytes(b"forged")
                    registry["retention_fixtures_sha256"][name] = MATRIX.digest(path.read_bytes())
            with self.subTest(action=action), self.assertRaises(ValueError):
                self.implementation_mutation(mutate_files=corrupt)


if __name__ == "__main__":
    unittest.main()

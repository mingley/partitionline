#!/usr/bin/env python3
"""Verify the broker inventory against retained Apache sources, without extraction.

The upstream inventory is separate from the checked local handler registry.
Registry source/fixture checks do not establish production qualification. An
optional report from the compiled Rust golden test verifies actual response bytes.
"""
import argparse
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
TARGETS = {
    "4.1.2": (
        "c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c",
        "22e7e13834390b3a7e38670a1115618ac1e82bf4c5a1bb9cc6dbf6390d0f235a",
    ),
    "4.2.1": (
        "18d5ecd939c8d510fdd72d0abb1f7099659dcd58",
        "faa55f0602830e89dcaaa5923b7d9cd76fd03751ec40767c6b065453b475b955",
    ),
    "4.3.1": (
        "26b251a451ce941d3d7a55e6487bcb7f16b5ad48",
        "4b5a65a52cbfdabe217856b6b4eb7cad7067f250524a38592ff27ee315824ea2",
    ),
}
# Independently anchored reviewed subset digests. Rewriting the matrix's own
# hashes cannot bless a different source while retaining an Apache commit pin.
RETAINED_SHA256 = {
    "4.1.2": "b966a4628d6c022063fa0345b9677c6acbb148c33b520734e0adf8f9fb2061b8",
    "4.2.1": "2d0882c3a7ca4b1a7aadb3ca38b469105b346d17c8d49b35fdbd63faebdb25ff",
    "4.3.1": "71dc8ab97c292015e1adaa685e76edaabfb88fa6e3fb3a5da075ebff4aa10332",
}
MESSAGE_DIR = "clients/src/main/resources/common/message/"
API_KEYS = "clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java"
GENERATOR_DIR = "generator/src/main/java/org/apache/kafka/message/"
GENERATOR_FILES = (
    "ApiMessageTypeGenerator.java", "MessageSpec.java", "Versions.java",
    "VersionConditional.java", "MessageGenerator.java", "RequestListenerType.java",
    "StructSpec.java",
)
REQUIRED_FILES = {"LICENSE", "NOTICE", API_KEYS} | {
    GENERATOR_DIR + name for name in GENERATOR_FILES
}
EXPECTED_KEYS = set(range(93))
MAX_ARCHIVE_BYTES = 2 * 1024 * 1024
MAX_TAR_BYTES = 8 * 1024 * 1024
MAX_MEMBER_BYTES = 512 * 1024
MAX_MEMBERS = 256
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SCOPE = "Pinned upstream broker protocol inventory; no implementation or advertisement claim"
IMPLEMENTED = [
    {"api_key": 3, "min_version": 0, "max_version": 13},
    {"api_key": 18, "min_version": 0, "max_version": 4},
    {"api_key": 19, "min_version": 2, "max_version": 4},
    {"api_key": 20, "min_version": 1, "max_version": 6},
]
IMPLEMENTED_BY_KEY = {row["api_key"]: row for row in IMPLEMENTED}
DATA_APIS = [{"api_key": 0, "min_version": 3, "max_version": 13}] + IMPLEMENTED
CONTROLLER_APIS = [IMPLEMENTED_BY_KEY[18]] + [
    {"api_key": key, "min_version": 0, "max_version": 0} for key in (52, 53, 54)
]
PROFILE_SOURCE_PATHS = {
    "produce_source": "partitionline-broker/src/produce.rs",
    "produce_test_source": "partitionline-broker/tests/produce.rs",
    "controller_source": "partitionline-broker/src/raft/protocol.rs",
    "controller_test_source": "partitionline-broker/tests/raft_protocol.rs",
    "election_source": "partitionline-broker/src/raft/election.rs",
    "raft_module_source": "partitionline-broker/src/raft/mod.rs",
}
PRODUCE_MANIFEST_SHA256 = {
    "4.1.2": "f0d9e7cceb398633978a7a9bffc8986b85f95d8d509c7df4a69a6e3dfd25a0a1",
    "4.2.1": "828d66746cfa4ae4e0b8e92818d9c782ded209a9194220dc41212decf9dda3de",
    "4.3.1": "e03751cdd9541dad4e133e86a5d0385c917d961a2778e5bfadf2e22cac5b36d7",
}
CONTROLLER_MANIFEST_SHA256 = "c61bdd119c59a0786eb8fb1cdb44a10610cc6e5621ee32dee2b2017ace3747b5"
PROFILE_NOTES = {
    0: "Optional ordinary data router Produce3-13; one magic2 batch/partition, local fsync/RF1 only; transactions/idempotence/control unsupported; qualification not_run.",
    52: "Optional fixed trusted controller Vote0 with pinned handler/policy distinctions; modern Vote/pre-vote versions missing; qualification not_run.",
    53: "Optional fixed trusted controller BeginQuorumEpoch0; modern membership/endpoint versions missing; qualification not_run.",
    54: "Optional fixed trusted controller EndQuorumEpoch0 with bounded configured successors; modern membership versions missing; qualification not_run.",
}
SOURCE_PATHS = {
    "protocol_source": "partitionline-broker/src/protocol.rs",
    "test_source": "partitionline-broker/tests/protocol.rs",
    "metadata_source": "partitionline-broker/src/metadata.rs",
    "metadata_test_source": "partitionline-broker/tests/metadata.rs",
}
METADATA_MANIFEST_SHA256 = {'4.1.2': '721dfc5e54a5f45cbb1dff288a1b64e49db28e864ba3d20691a5ee140d9e2d06', '4.2.1': '445ae5bfeebc59f8fbabb932cdf122bc7326139ffcad6be945db99db6f01f1ab', '4.3.1': 'e5d76d70e20d32fc7b132b08dec86b5d57a439cb0b35d2ed485125b02e25bd9b'}
SCHEMA_ONLY_MANIFEST_SHA256 = {'4.1.2': '2dab8a229f45dd0e627004f2cbfb3bd368b360a252d25a732f81135baeeddfe8', '4.2.1': '9cf6ba0c67ab68a7e13cda4974c4d7ffb183af3f65d414a73f1385c47b64cc39', '4.3.1': '3174c0e93ce395178cdfe1c8740c346a6a5c546cd7250b286ed206a9adb13390'}

REVIEWED_TRAILING_REJECTIONS = {
    "metadata-v13-all-tail1": (3, 13), "metadata-v13-all-tail2": (3, 13),
    "metadata-v13-all-tail4": (3, 13), "metadata-v13-all-tail-nonzero": (3, 13),
    "metadata-v13-named-tail3": (3, 13), "metadata-v13-empty-tail3": (3, 13),
    "metadata-v8-all-tail3": (3, 8), "create-v4-tail3": (19, 4), "delete-v6-tail3": (20, 6),
}
NATIVE_NULL_REQUEST_SHA256 = "617352cc7cfe21b49abca06778a07b969a4b73bf6a34d9b127c7a58e40629ef4"

IMPLEMENTED_NOTES = {
    3: "Metadata0-13 persistent single-node catalog and Apache wire goldens; automatic creation disabled; qualification not_run.",
    18: "ApiVersions0-4 standalone18-only or composed tested registry; qualification not_run.",
    19: "CreateTopics2-4 journal-backed single-node administration; nonempty overrides INVALID_CONFIG; versions5-7 missing; qualification not_run.",
    20: "DeleteTopics1-6 journal-backed tombstones with name/UUID validation and Apache wire goldens; qualification not_run.",
}
REGISTRY_PATH = "tests/conformance/broker/implemented-api-versions.json"
# Independent Apache generator outputs. Changing a registry's hashes alone
# cannot bless changed wire bytes or fabricated upstream observations.
GOLDEN_MANIFEST_SHA256 = {'4.1.2': 'ca8d4a2e1badf53a829b8286e19775b57f5361b9ba608819321102444ba1a040', '4.2.1': 'ca8d4a2e1badf53a829b8286e19775b57f5361b9ba608819321102444ba1a040', '4.3.1': 'ca8d4a2e1badf53a829b8286e19775b57f5361b9ba608819321102444ba1a040'}


def checked_implemented(value):
    require(isinstance(value, list) and value == IMPLEMENTED and
            all(isinstance(row, dict) and row.keys() == {"api_key", "min_version", "max_version"} and
                all(type(number) is int for number in row.values()) for row in value),
            "forged/extra/out-of-range implementation claim")
    return value


def verify_implementation(registry_path, repository_root, inventories, handler_report=None, metadata_handler_report=None,
                          produce_handler_report=None, controller_handler_report=None, controller_tcp_responses=None, data_api_versions_report=None):
    registry = load_json(registry_path)
    fields = {"schema_version", "scope", "implementation_gate", "implemented_api_versions", "qualification",
              "behavior_check", "standalone_api_versions", "fixture_root", "fixtures_sha256",
              "metadata_fixture_root", "metadata_fixtures_sha256", "produce_fixture_root", "produce_fixtures_sha256",
              "controller_fixture_root", "controller_fixtures_sha256", "data_api_versions", "controller_api_versions",
              "data_implementation_gate", "controller_implementation_gate"}
    for label in {**SOURCE_PATHS, **PROFILE_SOURCE_PATHS}:
        fields.update({label, label + "_sha256"})
    require(registry.keys() == fields, "missing/extra registry fields or unreviewed profile claim")
    require(type(registry.get("schema_version")) is int and registry["schema_version"] == 1 and
            registry.get("scope") == "Checked local handler registry; production qualification remains not_run" and
            registry.get("implementation_gate") == "KL11-05" and
            registry.get("qualification") == "not_run", "invalid implementation registry scope/gate")
    checked_implemented(registry.get("implemented_api_versions"))
    require(registry.get("standalone_api_versions") == [IMPLEMENTED_BY_KEY[18]],
            "standalone handler advertisement mismatch")
    for version, inventory in inventories.items():
        for implemented in IMPLEMENTED:
            row = inventory[implemented["api_key"]]
            require(row["disposition"] == "active" and "broker" in row["listeners"],
                    f"{version}: implementation outside upstream contract")
            for kind in ("request", "response"):
                require(set(range(implemented["min_version"], implemented["max_version"] + 1)) <=
                        set(expand_versions(row[kind]["valid_versions"])),
                        f"{version}: implementation outside upstream version range")
    for label, expected in SOURCE_PATHS.items():
        require(registry.get(label) == expected, "unexpected implementation source path")
        require(registry.get(label + "_sha256") == digest((repository_root / expected).read_bytes()),
                f"{label}: implementation source checksum mismatch")
    require(registry.get("fixture_root") == "partitionline-broker/tests/fixtures/protocol",
            "unexpected implementation fixture path")
    root = repository_root / registry["fixture_root"]
    files = [path for path in sorted(root.rglob("*")) if path.is_file()]
    require(len(files) == 135 and all(not path.is_symlink() and path.stat().st_size <= 128 * 1024
                                    for path in files), "unbounded/symlink implementation fixtures")
    actual = {str(path.relative_to(root)): digest(path.read_bytes()) for path in files}
    require(registry.get("fixtures_sha256") == actual, "implementation fixture checksum map mismatch")
    expected_paths, expected_results = set(), {}
    for version in TARGETS:
        manifest = root / version / "goldens.json"
        require(digest(manifest.read_bytes()) == GOLDEN_MANIFEST_SHA256[version],
                f"{version}: independent golden manifest checksum mismatch")
        golden = load_json(manifest)
        require(golden.get("api18_min") == 0 and golden.get("api18_max") == 4,
                "golden advertisement range mismatch")
        cases = golden.get("cases")
        require(isinstance(cases, list) and len(cases) == 33, "missing golden cases")
        expected_paths.add(f"{version}/goldens.json")
        for case in cases:
            name = case["name"]
            require(re.fullmatch(r"[a-z0-9-]+", name) and (version, name) not in expected_results,
                    "unsafe/duplicate golden case")
            expected_results[(version, name)] = case["response_hex"]
            for direction in ("request", "response"):
                value = case[direction + "_hex"]
                if value is not None:
                    path = f"{version}/{name}.{direction}.bin"
                    expected_paths.add(path)
                    require((root / path).read_bytes() == bytes.fromhex(value), "golden byte mismatch")
    require(actual.keys() == expected_paths, "missing/extra implementation fixtures")
    metadata_expected = verify_metadata_fixtures(registry, repository_root)
    profiles = verify_profiles(registry, repository_root, inventories,
                               produce_handler_report, controller_handler_report, controller_tcp_responses, data_api_versions_report)
    metadata_checked = False
    if metadata_handler_report is not None:
        verify_compiled_report(metadata_handler_report, registry, metadata_expected, ("metadata_source", "metadata_test_source"))
        metadata_checked = True
    report_checked = False
    if handler_report is not None:
        report = load_json(handler_report)
        checked_implemented(report.get("implemented_api_versions"))
        require(type(report.get("schema_version")) is int and report["schema_version"] == 1 and
                all(report.get(label + "_sha256") == registry[label + "_sha256"]
                    for label in ("protocol_source", "test_source")),
                "compiled handler report/source/registry mismatch")
        results = report.get("case_results")
        require(isinstance(results, list) and len(results) == 99, "incomplete compiled handler report")
        observed = {}
        for result in results:
            require(isinstance(result, dict) and result.keys() == {"release", "case", "response_hex"},
                    "invalid compiled handler case shape")
            pair = (result.get("release"), result.get("case"))
            require(pair not in observed, "duplicate compiled handler case")
            observed[pair] = result.get("response_hex")
        require(observed == expected_results, "compiled handler/golden response mismatch")
        report_checked = True
    return registry, {"implemented_api_versions": IMPLEMENTED,
                      "source_and_fixture_hashes_checked": True,
                      "golden_cases": len(expected_results),
                      "metadata_golden_cases": len(metadata_expected),
                      "metadata_response_cases": sum(value is not None for value in metadata_expected.values()),
                      "metadata_local_rejections": sum(value is None for value in metadata_expected.values()),
                      "compiled_metadata_report_checked": metadata_checked,
                      "compiled_handler_report_checked": report_checked,
                      **profiles,
                      "qualification": "not_run"}


def expand_versions(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9]+(?:-[0-9]+)?", value), "invalid implemented upstream range")
    pieces = value.split("-")
    lower, upper = int(pieces[0]), int(pieces[-1])
    require(lower <= upper <= 32767, "invalid upstream range endpoints")
    return range(lower, upper + 1)


def verify_metadata_fixtures(registry, repository_root):
    require(registry.get("metadata_fixture_root") == "partitionline-broker/tests/fixtures/metadata",
            "unexpected metadata fixture path")
    root = repository_root / registry["metadata_fixture_root"]
    files = [path for path in sorted(root.rglob("*")) if path.is_file()]
    require(1 <= len(files) <= 4096 and all(not path.is_symlink() and path.stat().st_size <= 512 * 1024
                                           for path in files), "unbounded/symlink metadata fixtures")
    actual = {str(path.relative_to(root)): digest(path.read_bytes()) for path in files}
    require(registry.get("metadata_fixtures_sha256") == actual, "metadata fixture checksum map mismatch")
    paths, results = set(), {}
    required_pairs = {(row["api_key"], version) for row in IMPLEMENTED
                      for version in range(row["min_version"], row["max_version"] + 1)}
    for release in TARGETS:
        manifest = root / release / "goldens.json"
        require(digest(manifest.read_bytes()) == METADATA_MANIFEST_SHA256[release],
                f"{release}: independent metadata golden manifest checksum mismatch")
        golden = load_json(manifest)
        cases = golden.get("cases")
        require(isinstance(cases, list) and 28 <= len(cases) <= 512, "missing metadata golden cases")
        paths.update({f"{release}/goldens.json", f"{release}/cases.tsv"})
        coverage, rows = set(), []
        for case in cases:
            name, key, version, seed = (case.get(field) for field in ("name", "api_key", "api_version", "seed"))
            require(isinstance(name, str) and re.fullmatch(r"[a-z0-9-]+", name) and
                    (release, name) not in results and type(key) is int and type(version) is int and
                    (key, version) in required_pairs and seed in ("empty", "fixture"), "invalid/duplicate metadata golden case")
            coverage.add((key, version))
            rows.append(f"{name}\t{key}\t{version}\t{seed}\n")
            results[(release, name)] = case.get("response_hex")
            if name == "native-v13-all-padding":
                require((key, version) == (3, 13) and seed == "fixture" and
                        digest(bytes.fromhex(case["request_hex"])) == NATIVE_NULL_REQUEST_SHA256,
                        "captured native Metadata request pin mismatch")
            for direction in ("request", "response"):
                value = case.get(direction + "_hex")
                if direction == "response" and value is None:
                    policy = case.get("handler_policy")
                    require((key == 3 and version in (12, 13) and name == f"metadata-v{version}-null-zero" and
                             policy == "reject_neither_identity") or
                            (REVIEWED_TRAILING_REJECTIONS.get(name) == (key, version) and
                             policy == "reject_unreviewed_trailing"), "unreviewed metadata rejection policy")
                    continue
                require(isinstance(value, str) and re.fullmatch(r"(?:[0-9a-f]{2})*", value), "invalid metadata golden bytes")
                path = f"{release}/{name}.{direction}.bin"
                paths.add(path)
                require((root / path).read_bytes() == bytes.fromhex(value), "metadata golden byte mismatch")
        require(coverage == required_pairs, f"{release}: missing advertised API/version golden")
        require((release, "native-v13-all-padding") in results and
                all((release, name) in results and results[(release, name)] is None
                    for name in REVIEWED_TRAILING_REJECTIONS), "missing native compatibility controls")
        schema_root = root / release / "schema-only"
        historical_path = schema_root / "goldens.json"
        require(digest(historical_path.read_bytes()) == SCHEMA_ONLY_MANIFEST_SHA256[release],
                f"{release}: independent schema-only manifest checksum mismatch")
        historical = load_json(historical_path)
        require(historical.get("release") == release and historical.get("qualification") ==
                "Excluded from positive router comparisons; historical malformed field combinations only",
                "schema-only diagnostics cannot become positive qualification")
        old_cases = historical.get("cases")
        require(isinstance(old_cases, list) and len(old_cases) == 2 and
                {case.get("name") for case in old_cases} == {"metadata-v12-null-zero", "metadata-v13-null-zero"},
                "unexpected schema-only diagnostic cases")
        paths.add(f"{release}/schema-only/goldens.json")
        for case in old_cases:
            name = case["name"]
            require(case.get("api_key") == 3 and case.get("api_version") in (12, 13) and
                    results[(release, name)] is None, "historical malformed response is not a handler golden")
            current = next(value for value in cases if value["name"] == name)
            require(case.get("request_hex") == current["request_hex"], "schema-only request history mismatch")
            for direction in ("request", "response"):
                value = case.get(direction + "_hex")
                require(isinstance(value, str) and re.fullmatch(r"(?:[0-9a-f]{2})*", value), "invalid schema-only bytes")
                path = f"{release}/schema-only/{name}.{direction}.bin"
                paths.add(path)
                content = (root / path).read_bytes()
                require(content == bytes.fromhex(value) and digest(content) == case.get(direction + "_sha256"),
                        "schema-only historical byte checksum mismatch")
        require((root / release / "cases.tsv").read_text(encoding="utf-8") == "".join(rows), "metadata golden index mismatch")
    require(actual.keys() == paths, "missing/extra metadata implementation fixtures")
    return results


def verify_compiled_report(path, registry, expected, labels):
    report = load_json(path)
    checked_implemented(report.get("implemented_api_versions"))
    require(type(report.get("schema_version")) is int and report["schema_version"] == 1 and
            all(report.get(label + "_sha256") == registry[label + "_sha256"] for label in labels),
            "compiled metadata report/source/registry mismatch")
    rows = report.get("case_results")
    require(isinstance(rows, list) and len(rows) == len(expected), "incomplete compiled metadata report")
    observed = {}
    for row in rows:
        require(isinstance(row, dict) and row.keys() == {"release", "case", "response_hex"}, "invalid compiled metadata case shape")
        pair = (row.get("release"), row.get("case"))
        require(pair not in observed, "duplicate compiled metadata case")
        observed[pair] = row.get("response_hex")
    require(observed == expected, "compiled metadata/golden response mismatch")


def checked_profile(value, expected, label):
    require(isinstance(value, list) and value == expected and
            all(isinstance(row, dict) and row.keys() == {"api_key", "min_version", "max_version"} and
                all(type(number) is int for number in row.values()) for row in value),
            f"{label}: forged/extra/out-of-range profile claim")


def fixture_map(registry, repository_root, label, expected_root, expected_count):
    require(registry.get(label + "_fixture_root") == expected_root,
            f"{label}: unexpected fixture path")
    root = repository_root / expected_root
    require(root.is_dir() and not root.is_symlink(), f"{label}: unsafe fixture directory")
    files = [path for path in sorted(root.rglob("*")) if path.is_file() or path.is_symlink()]
    require(len(files) == expected_count and
            all(not path.is_symlink() and path.stat().st_size <= 512 * 1024 for path in files),
            f"{label}: unbounded/symlink/missing/extra fixtures")
    actual = {str(path.relative_to(root)): digest(path.read_bytes()) for path in files}
    require(registry.get(label + "_fixtures_sha256") == actual,
            f"{label}: fixture checksum map mismatch")
    return root, actual


def verify_produce_fixtures(registry, repository_root):
    root, actual = fixture_map(registry, repository_root, "produce",
                               "partitionline-broker/tests/fixtures/produce", 1290)
    paths, expected = set(), {}
    for release in TARGETS:
        manifest = root / release / "goldens.json"
        require(digest(manifest.read_bytes()) == PRODUCE_MANIFEST_SHA256[release],
                f"{release}: independent Produce manifest checksum mismatch")
        golden = load_json(manifest)
        require(golden.get("release") == release and len(golden.get("cases", [])) == 236,
                "missing/incorrect Produce golden cases")
        paths.update({f"{release}/goldens.json", f"{release}/cases.tsv"})
        rows, versions, counts = [], set(), {}
        for case in golden["cases"]:
            name, key, version, seed, outcome = (case.get(field) for field in
                ("name", "api_key", "api_version", "seed", "expected_outcome"))
            require(isinstance(name, str) and re.fullmatch(r"[a-z0-9-]+", name) and
                    (release, name) not in expected and type(key) is int and key == 0 and
                    type(version) is int and 3 <= version <= 13 and
                    seed in ("fixture", "fixture_small_entry") and
                    outcome in ("response", "no_response_keep_open", "close", "structural_reject"),
                    "invalid/duplicate Produce case")
            require(isinstance(case.get("basis"), str) and bool(case["basis"]),
                    "Produce component/local-policy basis missing")
            versions.add(version)
            counts[outcome] = counts.get(outcome, 0) + 1
            response = case.get("response_hex")
            require((outcome == "response") == isinstance(response, str) and
                    (response is not None or outcome != "response"), "Produce response/outcome mismatch")
            if outcome != "response":
                require(response is None, "Produce forbidden response frame")
            expected[(release, name)] = (outcome, response)
            for direction in ("request", "response"):
                value = case.get(direction + "_hex")
                if value is None:
                    require(direction == "response" and case.get(direction + "_sha256") is None,
                            "missing Produce request/incorrect null response hash")
                    continue
                require(isinstance(value, str) and re.fullmatch(r"(?:[0-9a-f]{2})*", value),
                        "invalid Produce hex")
                raw = bytes.fromhex(value)
                path = f"{release}/{name}.{direction}.bin"
                paths.add(path)
                require(digest(raw) == case.get(direction + "_sha256") and (root / path).read_bytes() == raw,
                        "Produce golden byte/hash mismatch")
            rows.append(f"{name}\t{key}\t{version}\t{seed}\t{outcome}\n")
        require(versions == set(range(3, 14)) and counts ==
                {"response": 192, "no_response_keep_open": 11, "close": 11, "structural_reject": 22},
                "incomplete Produce version/outcome coverage")
        require((root / release / "cases.tsv").read_text(encoding="utf-8") == "".join(rows),
                "Produce golden index mismatch")
    require(actual.keys() == paths, "missing/extra Produce fixture paths")
    return expected


def verify_controller_fixtures(registry, repository_root):
    root, actual = fixture_map(registry, repository_root, "controller",
                               "partitionline-broker/tests/fixtures/raft-protocol", 953)
    manifest = root / "manifest.json"
    require(digest(manifest.read_bytes()) == CONTROLLER_MANIFEST_SHA256,
            "independent controller manifest checksum mismatch")
    golden = load_json(manifest)
    releases = golden.get("releases")
    require(type(golden.get("schema_version")) is int and golden["schema_version"] == 1 and isinstance(releases, list) and
            [row.get("release") for row in releases] == list(TARGETS), "controller manifest release mismatch")
    paths, expected = {"manifest.json", "README.md"}, {}
    columns = ("name", "key", "version", "correlation", "request_file", "response_file", "setup",
               "expected_disposition", "core_term", "voted_for", "leader", "deadline_ms")
    for release in releases:
        version = release["release"]
        directory = root / version
        rows = list(csv.DictReader(io.StringIO((directory / "cases.tsv").read_text()), delimiter="\t"))
        cases = release.get("cases")
        require(release.get("case_count") == len(rows) == len(cases) == 67 and
                all(tuple(row) == columns for row in rows), "missing/invalid controller case index")
        require(digest((directory / "cases.tsv").read_bytes()) == release.get("case_table_sha256") and
                digest((directory / "observations.json").read_bytes()) == release.get("observations_sha256"),
                "controller table/actual observation checksum mismatch")
        observation = load_json(directory / "observations.json")
        observed_components = {case["name"]: case for case in observation["cases"]}
        require(len(observed_components) == 67 and observation.get("case_count") == 67,
                "missing/duplicate actual Apache controller observations")
        file_map = release.get("file_sha256")
        require(isinstance(file_map, dict) and len(file_map) == 316 and
                all(isinstance(name, str) and re.fullmatch(r"[a-z0-9.-]+", name) for name in file_map),
                "unsafe/incomplete controller file map")
        for filename, checksum in file_map.items():
            paths.add(f"{version}/{filename}")
            require(digest((directory / filename).read_bytes()) == checksum,
                    "controller retained file checksum mismatch")
        paths.add(f"{version}/observations.json")
        pairs = set()
        for index, case in enumerate(cases):
            require(all(rows[index].get(field) == case.get(field) for field in columns),
                    "controller indexed input/fault history mismatch")
            name = case.get("name")
            require(isinstance(name, str) and re.fullmatch(r"[a-z0-9-]+", name) and
                    (version, name) not in expected, "unsafe/duplicate controller case")
            key, api_version = int(case["key"]), int(case["version"])
            pairs.add((key, api_version))
            outcome = case.get("expected_disposition")
            require(outcome in ("response", "close") and
                    case.get("policy") in golden["policy_notes"], "controller missing actual/local policy")
            component = observed_components[name]
            require(component.get("policy") == case["policy"] and
                    component.get("key") == key and component.get("version") == api_version,
                    "controller actual Apache observation mismatch")
            response_hex = None
            for direction in ("request", "response"):
                filename = case[direction + "_file"]
                if filename == "-":
                    require(direction == "response" and outcome == "close" and
                            case.get(direction + "_sha256") is None, "controller missing required frame")
                    continue
                require(filename == f"{name}.{direction}.bin", "controller unsafe frame path")
                raw = (directory / filename).read_bytes()
                require(digest(raw) == case[direction + "_sha256"], "controller golden frame hash mismatch")
                frame = (directory / filename.replace(".bin", ".frame.bin")).read_bytes()
                require(len(frame) >= 4 and int.from_bytes(frame[:4], "big", signed=True) == len(raw) and
                        frame[4:] == raw, "controller TCP frame length/payload mismatch")
                if direction == "response":
                    require(outcome == "response", "controller forbidden close response")
                    response_hex = raw.hex()
            require((outcome == "response") == (response_hex is not None), "controller disposition/response mismatch")
            expected[(version, name)] = response_hex
        require({(18, v) for v in range(5)} | {(key, 0) for key in (52, 53, 54)} <= pairs,
                "controller advertised pair missing from goldens")
    require(actual.keys() == paths, "missing/extra controller fixture paths")
    return expected


def verify_data_wire_profile(path):
    report = load_json(path)
    require(report.get("source_sha") == "c34bdfd3fbba65492ea49b3804dd0ae5311364b0" and
            report.get("passed") is True and report.get("actual_exchanges") == 30,
            "data wire profile source/count/verdict mismatch")
    cases = report.get("cases")
    require(isinstance(cases, list) and len(cases) == 30, "incomplete data wire profile exchanges")
    seen = set()
    for case in cases:
        tool, release, version, correlation = (case.get(field) for field in
            ("toolchain", "release", "api_version", "correlation_id"))
        require(tool in ("stable", "1-85-0") and release in TARGETS and
                type(version) is int and 0 <= version <= 4 and type(correlation) is int and
                0 <= correlation <= 2147483647 and case.get("api_key") == 18,
                "invalid data wire case")
        require((tool, release, version) not in seen, "duplicate data wire profile exchange")
        seen.add((tool, release, version))
        for direction in ("request", "response"):
            value = case.get(direction + "_hex")
            require(isinstance(value, str) and len(value) <= 8192 and re.fullmatch(r"(?:[0-9a-f]{2})+", value),
                    "invalid data wire profile hex")
        request, response = bytes.fromhex(case["request_hex"]), bytes.fromhex(case["response_hex"])
        require(len(request) >= 10 and int.from_bytes(request[:2], "big", signed=True) == 18 and
                int.from_bytes(request[2:4], "big", signed=True) == version and
                int.from_bytes(request[4:8], "big", signed=True) == correlation and
                case.get("request_header_version") == (2 if version >= 3 else 1) and
                case.get("response_header_version") == 0, "data wire request header mismatch")
        cursor = 0
        def take(count):
            nonlocal cursor
            require(count >= 0 and cursor + count <= len(response), "truncated data wire response")
            result = response[cursor:cursor + count]
            cursor += count
            return result
        require(int.from_bytes(take(4), "big", signed=True) == correlation and take(2) == b"\x00\x00",
                "data wire correlation/error mismatch")
        count = take(1)[0] - 1 if version >= 3 else int.from_bytes(take(4), "big", signed=True)
        require(count == 5, "data wire advertised API count mismatch")
        apis = []
        for _ in range(count):
            apis.append(dict(zip(("api_key", "min_version", "max_version"),
                (int.from_bytes(take(2), "big", signed=True) for _ in range(3)))))
            if version >= 3:
                require(take(1) == b"\x00", "data wire unexpected API tags")
        checked_profile(apis, DATA_APIS, "actual data wire")
        if version >= 1:
            require(take(4) == b"\x00" * 4, "data wire unexpected throttle")
        if version >= 3:
            require(take(1) == b"\x00", "data wire unexpected response tags")
        require(cursor == len(response), "data wire response trailing bytes")
    require(seen == {(tool, release, version) for tool in ("stable", "1-85-0")
                    for release in TARGETS for version in range(5)}, "missing data wire profile version")
    return {"exchanges_checked": len(seen), "api_versions": list(range(5)),
            "source_sha": report["source_sha"], "report_sha256": digest(path.read_bytes()),
            "scope": "Independently decoded captured response fields and complete pair coverage; actual execution provenance is the retained KL11-68 peer/source/command evidence."}


def verify_controller_tcp(directory, expected):
    require(directory.is_dir() and not directory.is_symlink(), "unsafe/missing controller TCP captures")
    names = [f"api-versions-{version}" for version in range(5)] + ["vote-positive"]
    expected_paths = {f"{release}/{name}.actual-{kind}.bin" for release in TARGETS
                      for name in names for kind in ("response", "frame")}
    files = [path for path in directory.rglob("*") if path.is_file() or path.is_symlink()]
    require(len(files) == 36 and all(not path.is_symlink() and path.stat().st_size <= 512 * 1024 for path in files),
            "unbounded/symlink/missing/extra controller TCP captures")
    actual = {str(path.relative_to(directory)): digest(path.read_bytes()) for path in files}
    require(actual.keys() == expected_paths, "missing/extra controller TCP capture paths")
    for release in TARGETS:
        for name in names:
            payload = (directory / release / (name + ".actual-response.bin")).read_bytes()
            frame = (directory / release / (name + ".actual-frame.bin")).read_bytes()
            require(payload.hex() == expected[(release, name)], "controller TCP response/golden mismatch")
            require(len(frame) >= 4 and int.from_bytes(frame[:4], "big", signed=True) == len(payload) and
                    frame[4:] == payload, "controller TCP length/payload mismatch")
    return dict(sorted(actual.items()))


def verify_profiles(registry, repository_root, inventories, produce_report, controller_report, controller_tcp_responses=None, data_api_versions_report=None):
    for label, expected in PROFILE_SOURCE_PATHS.items():
        require(registry.get(label) == expected, f"{label}: unexpected profile source path")
        require(registry.get(label + "_sha256") == digest((repository_root / expected).read_bytes()),
                f"{label}: profile source checksum mismatch")
    for label, apis, gate, listener in (("data", DATA_APIS, "KL11-06", "broker"),
                                       ("controller", CONTROLLER_APIS, "KL11-13", "controller")):
        checked_profile(registry.get(label + "_api_versions"), apis, label)
        require(registry.get(label + "_implementation_gate") == gate, f"{label}: profile gate mismatch")
        for release, inventory in inventories.items():
            for implemented in apis:
                source = inventory[implemented["api_key"]]
                require(source["disposition"] == "active" and listener in source["listeners"],
                        f"{release}: profile outside upstream listener contract")
                for direction in ("request", "response"):
                    require(set(range(implemented["min_version"], implemented["max_version"] + 1)) <=
                            set(expand_versions(source[direction]["valid_versions"])),
                            f"{release}: profile outside upstream version contract")
    produce_expected = verify_produce_fixtures(registry, repository_root)
    controller_expected = verify_controller_fixtures(registry, repository_root)
    if produce_report is not None:
        report = load_json(produce_report)
        require(type(report.get("schema_version")) is int and report["schema_version"] == 1,
                "invalid compiled Produce schema")
        checked_profile(report.get("data_api_versions"), DATA_APIS, "compiled data")
        rows = report.get("case_results")
        require(isinstance(rows, list) and len(rows) == 708, "incomplete compiled Produce report")
        observed = {}
        for row in rows:
            require(isinstance(row, dict) and row.keys() == {"release", "case", "outcome", "response_hex"},
                    "invalid compiled Produce case shape")
            pair = (row.get("release"), row.get("case"))
            require(pair not in observed, "duplicate compiled Produce case")
            observed[pair] = (row.get("outcome"), row.get("response_hex"))
        require(observed == produce_expected, "compiled Produce/golden outcome or bytes mismatch")
    if controller_report is not None:
        report = load_json(controller_report)
        require(type(report.get("schema_version")) is int and report["schema_version"] == 1 and
                report.get("profile") == "fixed-controller-v0" and
                report.get("qualification") == "not_run" and report.get("independent_fixture_cases") == 201,
                "invalid compiled controller scope/count")
        checked_profile(report.get("implemented_api_versions"), CONTROLLER_APIS, "compiled controller")
        for label in ("controller_source", "controller_test_source", "election_source", "raft_module_source"):
            require(report.get(label + "_sha256") == registry[label + "_sha256"],
                    "compiled controller reflected source label mismatch")
        rows = report.get("case_results")
        require(isinstance(rows, list) and len(rows) == 201, "incomplete compiled controller report")
        observed = {}
        for row in rows:
            require(isinstance(row, dict) and row.keys() == {"release", "case", "response_hex"},
                    "invalid compiled controller case shape")
            pair = (row.get("release"), row.get("case"))
            require(pair not in observed, "duplicate compiled controller case")
            observed[pair] = row.get("response_hex")
        require(observed == controller_expected, "compiled controller/golden response mismatch")
    tcp = verify_controller_tcp(controller_tcp_responses, controller_expected) if controller_tcp_responses is not None else {}
    data_wire = verify_data_wire_profile(data_api_versions_report) if data_api_versions_report is not None else {}
    return {"data_api_versions": DATA_APIS, "controller_api_versions": CONTROLLER_APIS,
            "produce_golden_cases": len(produce_expected), "controller_golden_cases": len(controller_expected),
            "compiled_produce_report_checked": produce_report is not None,
            "compiled_controller_report_checked": controller_report is not None,
            "controller_tcp_capture_bytes_checked": bool(tcp), "controller_tcp_capture_pairs": len(tcp) // 2,
            "controller_tcp_capture_sha256": tcp,
            "data_api_versions_wire": data_wire,
            "source_hash_scope": "Verifier hashes actual source files; compiled controller/protocol/metadata hash fields reflect registry labels. Actual execution provenance requires the retained immutable-source command reports.",
            "controller_core_state_reported": False,
            "wire_advertisement_scope": "Compiled static ranges and golden ApiVersions bytes; actual live TCP profile evidence is retained separately in KL11-68/KL11-69."}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def strip_comments(text):
    """Remove // and /* */ comments while preserving strings and line numbers."""
    result = []
    index = 0
    in_string = False
    while index < len(text):
        char = text[index]
        if in_string:
            result.append(char)
            if char == "\\":
                index += 1
                require(index < len(text), "unterminated string escape")
                result.append(text[index])
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            result.append(char)
            index += 1
        elif text.startswith("//", index):
            while index < len(text) and text[index] != "\n":
                result.append(" ")
                index += 1
        elif text.startswith("/*", index):
            end = text.find("*/", index + 2)
            require(end >= 0, "unterminated block comment")
            result.extend("\n" if c == "\n" else " " for c in text[index:end + 2])
            index = end + 2
        else:
            result.append(char)
            index += 1
    require(not in_string, "unterminated string")
    return "".join(result)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON property {key!r}")
        result[key] = value
    return result


def parse_json(data, commented=False):
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    return json.loads(strip_comments(text) if commented else text,
                      object_pairs_hook=unique_object,
                      parse_constant=lambda value: require(False, f"invalid JSON constant {value}"))


def load_json(path):
    require(path.stat().st_size <= MAX_TAR_BYTES, f"JSON input too large: {path.name}")
    return parse_json(path.read_bytes())


def read_archive(path, expected_hash=None):
    """Read bounded regular members in memory; never write a tar member to disk."""
    require(path.stat().st_size <= MAX_ARCHIVE_BYTES, "retained archive too large")
    compressed = path.read_bytes()
    if expected_hash is not None:
        require(isinstance(expected_hash, str) and SHA256.fullmatch(expected_hash),
                "invalid retained archive checksum")
        require(digest(compressed) == expected_hash, "retained archive checksum mismatch")
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(MAX_TAR_BYTES + 1)
    require(len(raw) <= MAX_TAR_BYTES, "retained tar exceeds decompression bound")
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for member in archive:
            require(len(result) < MAX_MEMBERS, "too many retained members")
            parts = PurePosixPath(member.name).parts
            require(parts and not member.name.startswith("/") and
                    ".." not in parts and "." not in parts and
                    "\\" not in member.name and
                    str(PurePosixPath(member.name)) == member.name,
                    f"unsafe retained member path {member.name!r}")
            require(member.type == tarfile.REGTYPE and not member.pax_headers,
                    f"non-regular retained member {member.name!r}")
            require(member.name not in result, f"duplicate retained member {member.name!r}")
            require(0 <= member.size <= MAX_MEMBER_BYTES,
                    f"retained member exceeds bound: {member.name}")
            require(member.name in REQUIRED_FILES or
                    (member.name.startswith(MESSAGE_DIR) and member.name.endswith(".json") and
                     "/" not in member.name[len(MESSAGE_DIR):]),
                    f"unexpected retained source {member.name}")
            stream = archive.extractfile(member)
            require(stream is not None, f"unreadable retained member {member.name}")
            result[member.name] = stream.read(MAX_MEMBER_BYTES + 1)
            require(len(result[member.name]) == member.size, "retained member size mismatch")
    require(REQUIRED_FILES <= result.keys(), "required license/generator source missing")
    return result


def version_range(value, label, allow_open=True):
    require(isinstance(value, str), f"{label}: version range must be a string")
    if value == "none":
        return (0, -1)
    match = re.fullmatch(r"(\d+)(?:(\+)|-(\d+))?", value)
    require(match is not None, f"{label}: invalid version range {value!r}")
    low = int(match[1])
    high = 32767 if match[2] else int(match[3] or match[1])
    require(0 <= low <= high <= 32767, f"{label}: invalid version bounds")
    require(allow_open or not match[2], f"{label}: valid range must be bounded")
    return low, high


def format_range(low, high):
    if low > high:
        return "none"
    return str(low) if low == high else f"{low}-{high}"


def enum_name(name):
    # MessageGenerator.toSnakeCase preserves consecutive uppercase characters.
    result = []
    previous_capitalized = True
    for char in name:
        if char.isupper():
            if not previous_capitalized:
                result.append("_")
            result.append(char.lower())
            previous_capitalized = True
        else:
            result.append(char)
            previous_capitalized = False
    return "".join(result).upper()


def parse_api_keys(data):
    text = strip_comments(data.decode("utf-8"))
    start = text.index("public enum ApiKeys {") + len("public enum ApiKeys {")
    end = text.index(";", start)
    block = text[start:end + 1]
    pattern = re.compile(r"\s*([A-Z_]+)\(ApiMessageType\.([A-Z_]+)"
                         r"(?:,\s*(true|false))?(?:,\s*(true|false))?\)\s*([,;])")
    result = {}
    index = 0
    while index < len(block):
        match = pattern.match(block, index)
        require(match is not None, "unrecognized ApiKeys declaration")
        name, message_type, cluster, forward, delimiter = match.groups()
        require(name == message_type, f"ApiKeys enum/type mismatch: {name}")
        require(name not in result, f"duplicate ApiKeys name: {name}")
        result[name] = {"cluster_action": cluster == "true", "forwardable": forward == "true"}
        index = match.end()
        require((delimiter == ";") == (index == len(block)), "ApiKeys terminator mismatch")
    return result


def verify_generator_rules(sources):
    """Fail on changed generator branches rather than guessing header semantics."""
    generator = sources[GENERATOR_DIR + "ApiMessageTypeGenerator.java"].decode("utf-8")
    required_fragments = (
        'if (!spec.hasValidVersion())',
        'if (type.equals("response") && apiKey == 18)',
        'ApiVersionsResponse always includes a v0 header.',
        'VersionConditional.forVersions(spec.flexibleVersions(),',
        'spec.validVersions())',
        'if (type.equals("request")) {\n                        buffer.printf("return (short) 2;',
        '} else {\n                        buffer.printf("return (short) 1;',
        'if (type.equals("request")) {\n                        buffer.printf("return (short) 1;',
        '} else {\n                        buffer.printf("return (short) 0;',
        'if (!this.latestVersionUnstable || enableUnstableLastVersion)',
        'return (short) (this.highestSupportedVersion - 1)',
    )
    require(all(fragment in generator for fragment in required_fragments),
            "pinned generator header/unstable rule is unrecognized")
    spec = sources[GENERATOR_DIR + "MessageSpec.java"].decode("utf-8")
    require(all(fragment in spec for fragment in (
        'if (struct.versions().empty())', 'this.flexibleVersions = Versions.NONE;',
        'this.latestVersionUnstable = false;',
    )) and re.search(r'this\.listeners = (?:List\.of\(\)|Collections\.emptyList\(\));', spec),
            "pinned MessageSpec removal rule is unrecognized")
    keys = sources[API_KEYS].decode("utf-8")
    require(all(fragment in keys for fragment in (
        'PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0;',
        'this == PRODUCE && listenerType.map(l -> l == ApiMessageType.ListenerType.BROKER)',
        'PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION : oldestVersion()',
        'if (this == ApiKeys.API_VERSIONS) return true;',
    )), "pinned ApiKeys negotiation exception is unrecognized")
    listener_source = sources[GENERATOR_DIR + "RequestListenerType.java"].decode("utf-8")
    listener_pairs = re.findall(r'@JsonProperty\("([a-z]+)"\)\s+([A-Z]+)', listener_source)
    require(listener_pairs == [("broker", "BROKER"), ("controller", "CONTROLLER")],
            "pinned listener types are unrecognized")


def derive_inventory(sources):
    verify_generator_rules(sources)
    enums = parse_api_keys(sources[API_KEYS])
    pairs = {}
    headers = {}
    for path, data in sorted(sources.items()):
        if not path.startswith(MESSAGE_DIR):
            continue
        spec = parse_json(data, commented=True)
        require(isinstance(spec, dict), f"{path}: message must be an object")
        kind = spec.get("type")
        if kind == "header":
            name = spec.get("name")
            require(name not in headers, f"duplicate header {name}")
            headers[name] = spec
        if kind not in ("request", "response"):
            continue
        key = spec.get("apiKey")
        require(type(key) is int and key in EXPECTED_KEYS, f"{path}: unknown API key {key!r}")
        require(kind not in pairs.setdefault(key, {}), f"duplicate {kind} for API key {key}")
        pairs[key][kind] = (path, spec)
    require(set(pairs) == EXPECTED_KEYS, f"missing API keys: {sorted(EXPECTED_KEYS - pairs.keys())}")
    require(set(headers) == {"RequestHeader", "ResponseHeader"}, "missing/extra header schemas")
    request_header, response_header = headers["RequestHeader"], headers["ResponseHeader"]
    require(request_header.get("validVersions") == "1-2" and
            request_header.get("flexibleVersions") == "2+" and
            response_header.get("validVersions") == "0-1" and
            response_header.get("flexibleVersions") == "1+", "pinned header schema ranges changed")
    client_ids = [field for field in request_header.get("fields", []) if field.get("name") == "ClientId"]
    require(len(client_ids) == 1 and all(client_ids[0].get(key) == value for key, value in {
        "type": "string", "versions": "1+", "nullableVersions": "1+", "flexibleVersions": "none",
    }.items()), "RequestHeader ClientId must remain classic nullable string")
    inventory = []
    for key, pair in sorted(pairs.items()):
        require(set(pair) == {"request", "response"}, f"missing request/response pair for API key {key}")
        request_path, request = pair["request"]
        response_path, response = pair["response"]
        name = request.get("name", "")
        require(isinstance(name, str) and name.endswith("Request"), f"API {key}: bad request name")
        message_name = name[:-len("Request")]
        require(response.get("name") == message_name + "Response", f"API {key}: request/response name mismatch")
        require(request_path == MESSAGE_DIR + name + ".json" and
                response_path == MESSAGE_DIR + response["name"] + ".json", f"API {key}: filename/name mismatch")
        enum = enum_name(message_name)
        require(enum in enums, f"API {key}: missing ApiKeys enum {enum}")
        require(request.get("validVersions") == response.get("validVersions"), f"API {key}: request/response valid range mismatch")
        low, high = version_range(request.get("validVersions"), f"API {key}", allow_open=False)
        removed = low > high
        listeners = request.get("listeners", [])
        require(isinstance(listeners, list) and len(listeners) == len(set(listeners)) and
                all(listener in ("broker", "controller") for listener in listeners), f"API {key}: invalid listeners")
        require(removed or bool(listeners), f"API {key}: active API has no listener")
        unstable = request.get("latestVersionUnstable", False)
        require(type(unstable) is bool and not response.get("latestVersionUnstable", False), f"API {key}: invalid unstable flag")
        require(not response.get("listeners"), f"API {key}: response has listeners")
        source_specs = {}
        flexible = {}
        for kind, (path, spec) in pair.items():
            raw_flexible = spec.get("flexibleVersions")
            effective = "none" if removed else raw_flexible
            flex_low, flex_high = version_range(effective, f"API {key} {kind} flexible")
            require(flex_low > flex_high or flex_high == 32767, f"API {key}: flexible range must be open-ended")
            deprecated = spec.get("deprecatedVersions", "none")
            version_range(deprecated, f"API {key} {kind} deprecated")
            source_specs[kind] = {
                "path": path, "sha256": digest(sources[path]), "name": spec["name"],
                "valid_versions": spec["validVersions"], "flexible_versions": raw_flexible,
                "effective_flexible_versions": effective, "deprecated_versions": deprecated,
            }
            flexible[kind] = (flex_low, flex_high)
        require(source_specs["request"]["effective_flexible_versions"] ==
                source_specs["response"]["effective_flexible_versions"], f"API {key}: request/response flexible range mismatch")
        listeners = [] if removed else sorted(listeners)
        unstable = False if removed else unstable
        stable_high = high - int(unstable)
        inventory.append({
            "api_key": key, "name": enum, "message_name": message_name,
            "disposition": "removed_api_key_reserved" if removed else "active",
            "listener_classification": "removed" if removed else "_and_".join(listeners),
            "listeners": listeners, **enums[enum], "latest_version_unstable": unstable,
            "stable_valid_versions": format_range(low, stable_high),
            "request": source_specs["request"], "response": source_specs["response"],
            "headers": [{
                "api_version": version,
                "request_header_version": 2 if flexible["request"][0] <= version <= flexible["request"][1] else 1,
                "response_header_version": 0 if key == 18 else (1 if flexible["response"][0] <= version <= flexible["response"][1] else 0),
            } for version in range(low, high + 1)],
            "upstream_negotiation": {
                "accepts_any_version_for_api_versions": key == 18,
                "broker_listener_advertised_minimum": (0 if key == 0 else low) if "broker" in listeners and low <= stable_high else None,
                "schema_minimum": low if not removed else None,
                "note": "Produce broker-listener minimum 0 is an upstream librdkafka workaround; actual schema starts at 3. No partitionline implementation or advertisement is inferred." if key == 0 else None,
            },
        })
    require({row["name"] for row in inventory} == enums.keys(), "ApiKeys/schema enum inventory mismatch")
    return inventory, {
        "request": {"valid_versions": "1-2", "flexible_versions": "2+", "classic_header_version": 1, "flexible_header_version": 2},
        "response": {"valid_versions": "0-1", "flexible_versions": "1+", "classic_header_version": 0, "flexible_header_version": 1},
        "api_versions_response_header_version": 0,
        "request_client_id_encoding": "classic_nullable_string_int16_length_including_flexible_headers",
        "unsupported_or_removed_api_key": "generator_throws_UnsupportedVersionException",
        "scope": "header mappings enumerate valid schema versions only",
    }


def projected_feature(row_by_version):
    first = row_by_version[next(iter(TARGETS))]
    removed = all(row["disposition"] == "removed_api_key_reserved" for row in row_by_version.values())
    return {
        "name": first["name"], "upstream_disposition": first["disposition"],
        "upstream_applicability": "not_applicable_removed_upstream" if removed else "required",
        "version_ranges": {version: {
            "request": row["request"]["valid_versions"], "response": row["response"]["valid_versions"],
            "request_flexible": row["request"]["effective_flexible_versions"],
            "response_flexible": row["response"]["effective_flexible_versions"],
            "stable": row["stable_valid_versions"],
        } for version, row in row_by_version.items()},
        "upstream_listeners": {version: row["listeners"] for version, row in row_by_version.items()},
        "inventory_gate": "KL11-57", "upstream_matrix": "tests/conformance/broker/api-matrix.json",
        "note": "Removed in Apache Kafka 4.0; assigned key retained to prevent reuse. No broker handler is required for a removed upstream API." if removed else "Pinned upstream contract only; handler implementation and qualification remain missing. Advertise only versions actually implemented and tested.",
    }


def verify(matrix_path, features_path, upstream_dir=None, registry_path=None,
           repository_root=None, handler_report=None, metadata_handler_report=None,
           produce_handler_report=None, controller_handler_report=None, controller_tcp_responses=None, data_api_versions_report=None):
    matrix, features = load_json(matrix_path), load_json(features_path)
    require(type(matrix.get("schema_version")) is int and matrix["schema_version"] == 1 and matrix.get("scope") == SCOPE and
            matrix.get("implementation_claim") is False, "invalid broker inventory schema/scope")
    releases = matrix.get("releases")
    require(isinstance(releases, list) and [r.get("version") for r in releases] == list(TARGETS), "missing/duplicate/unexpected release pins")
    upstream_dir = upstream_dir or matrix_path.parent / "upstream"
    inventories = {}
    reports = []
    for release in releases:
        version = release["version"]
        commit, original_sha = TARGETS[version]
        require(release.get("tag") == version and release.get("commit") == commit and
                release.get("source_archive_sha256") == original_sha and
                release.get("source_archive_url") == f"https://codeload.github.com/apache/kafka/tar.gz/{commit}", f"{version}: immutable source pin mismatch")
        filename = f"apache-kafka-{version}-protocol.tar.gz"
        require(release.get("retained_archive") == filename, f"{version}: unsafe/unexpected archive filename")
        require(release.get("retained_archive_sha256") == RETAINED_SHA256[version],
                f"{version}: reviewed retained-source pin mismatch")
        sources = read_archive(upstream_dir / filename, release.get("retained_archive_sha256"))
        actual_hashes = {path: digest(data) for path, data in sorted(sources.items())}
        require(release.get("files_sha256") == actual_hashes, f"{version}: retained per-file checksum map mismatch")
        inventory, rules = derive_inventory(sources)
        require(release.get("inventory") == inventory, f"{version}: matrix/source inventory mismatch")
        require(release.get("header_rules") == rules, f"{version}: matrix/source header rules mismatch")
        inventories[version] = inventory
        reports.append({"version": version, "commit": commit,
                        "retained_archive_sha256": release["retained_archive_sha256"],
                        "retained_files": len(sources), "api_keys": len(inventory),
                        "active_api_keys": sum(row["disposition"] == "active" for row in inventory),
                        "removed_api_keys": [row["api_key"] for row in inventory if row["disposition"] != "active"],
                        "unstable_latest_api_keys": [row["api_key"] for row in inventory if row["latest_version_unstable"]],
                        "header_version_pairs": sum(len(row["headers"]) for row in inventory)})
    require(features.keys() == {"schema_version", "scope", "target_releases", "upstream_pin_gate",
                                "implemented_api_versions", "features", "implementation_registry",
                                "data_api_versions", "controller_api_versions"},
            "missing/extra feature fields or unreviewed profile claim")
    require(type(features.get("schema_version")) is int and features["schema_version"] == 1 and features.get("target_releases") == list(TARGETS) and
            features.get("upstream_pin_gate") == "KL11-57", "features release/inventory gate mismatch")
    registry, implementation = verify_implementation(
        registry_path or ROOT / REGISTRY_PATH, repository_root or ROOT,
        inventories, handler_report, metadata_handler_report, produce_handler_report, controller_handler_report, controller_tcp_responses, data_api_versions_report)
    checked_implemented(features.get("implemented_api_versions"))
    require(features.get("implemented_api_versions") == registry["implemented_api_versions"] and
            features.get("implementation_registry") == REGISTRY_PATH,
            "features/implementation registry mismatch")
    for label, expected in (("data", DATA_APIS), ("controller", CONTROLLER_APIS)):
        checked_profile(features.get(label + "_api_versions"), expected, label)
        require(features[label + "_api_versions"] == registry[label + "_api_versions"],
                f"{label}: features/profile registry mismatch")
    feature_rows = features.get("features")
    require(isinstance(feature_rows, list), "missing feature rows")
    ids = [row.get("id") for row in feature_rows]
    require(len(ids) == len(set(ids)), "duplicate feature ids")
    api_features = {}
    for row in feature_rows:
        if "api_key" in row:
            key = row["api_key"]
            require(type(key) is int and key in EXPECTED_KEYS and key not in api_features,
                    "duplicate/unknown feature API key")
            api_features[key] = row
    require(api_features.keys() == EXPECTED_KEYS, "missing API features")
    for key, row in api_features.items():
        require(row.get("id") == f"api.{key}" and row.get("implementation") == ("partial" if key in PROFILE_NOTES or key == 19 else "implemented" if key in IMPLEMENTED_BY_KEY else "missing") and
                row.get("qualification") == "not_run", f"API {key}: unsupported implementation/qualification claim")
        if key in IMPLEMENTED_BY_KEY:
            implemented = IMPLEMENTED_BY_KEY[key]
            expected_range = f"{implemented['min_version']}-{implemented['max_version']}"
            require(row.get("implemented_versions") == expected_range and row.get("implementation_gate") == "KL11-05" and
                    "implementation_profile" not in row,
                    f"API{key} implementation version/gate mismatch")
        elif key in PROFILE_NOTES:
            gate, versions, profile = ("KL11-06", "3-13", "ordinary-data") if key == 0 else ("KL11-13", "0", "fixed-controller-v0")
            require(row.get("implementation_gate") == gate and row.get("implemented_versions") == versions and
                    row.get("implementation_profile") == profile, f"API {key}: optional profile version/gate mismatch")
        else:
            require("implemented_versions" not in row and "implementation_gate" not in row and "implementation_profile" not in row,
                    f"API {key}: forged implementation metadata")
        projection = projected_feature({version: inventory[key] for version, inventory in inventories.items()})
        if key in IMPLEMENTED_BY_KEY:
            projection["note"] = IMPLEMENTED_NOTES[key]
        elif key in PROFILE_NOTES:
            projection["note"] = PROFILE_NOTES[key]
        require(all(row.get(field) == value for field, value in projection.items()), f"API {key}: feature/source classification mismatch")
    return {"schema_version": 1, "verdict": "passed", "implementation_claim": False,
            "releases": reports, "local_implementation": implementation, "checks": ["immutable source pins", "bounded archives without extraction",
                "archive and per-file hashes", "93 unique complete request/response pairs per release",
                "ApiKeys flags and listener classifications", "exact valid/flexible/deprecated/stable ranges",
                "generator-derived header versions and exceptions", "truthful feature dispositions",
                "checked local implementation registry and source/fixture hashes"],
            "limitations": ["Static upstream inventory, not handler behavior or broker qualification.",
                "Future advertisement must use actual implemented and tested versions; upstream Produce minimum-0 workaround is not a partitionline support claim."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=ROOT / "tests/conformance/broker/api-matrix.json")
    parser.add_argument("--features", type=Path, default=ROOT / "tests/conformance/broker/features.json")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--registry", type=Path, default=ROOT / REGISTRY_PATH)
    parser.add_argument("--handler-report", type=Path,
                        help="JSON emitted by the compiled Rust protocol golden test")
    parser.add_argument("--metadata-handler-report", type=Path,
                        help="JSON emitted by the compiled Rust metadata/admin golden test")
    parser.add_argument("--produce-handler-report", type=Path,
                        help="Actual outcomes/response bytes from the compiled Produce golden test")
    parser.add_argument("--controller-handler-report", type=Path,
                        help="Actual response bytes from the compiled fixed-controller golden test")
    parser.add_argument("--controller-tcp-responses", type=Path,
                        help="Retained 18 actual TCP payload/frame pairs; execution provenance is separate")
    parser.add_argument("--data-api-versions-report", type=Path,
                        help="Independent KL11-68 captured data-profile API18 v0-4 exchanges")
    args = parser.parse_args()
    try:
        report = verify(args.matrix, args.features, registry_path=args.registry,
                        handler_report=args.handler_report, metadata_handler_report=args.metadata_handler_report,
                        produce_handler_report=args.produce_handler_report, controller_handler_report=args.controller_handler_report, controller_tcp_responses=args.controller_tcp_responses, data_api_versions_report=args.data_api_versions_report)
    except (ValueError, KeyError, TypeError, OSError, EOFError, tarfile.TarError) as error:
        report = {"schema_version": 1, "verdict": "failed", "implementation_claim": False, "error": str(error)}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["verdict"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())

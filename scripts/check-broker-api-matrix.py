#!/usr/bin/env python3
"""Verify the broker inventory against retained Apache sources, without extraction.

This inventories upstream protocol definitions. It neither implements an API nor
authorizes advertising any version from the partitionline broker.
"""
import argparse
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


def verify(matrix_path, features_path, upstream_dir=None):
    matrix, features = load_json(matrix_path), load_json(features_path)
    require(matrix.get("schema_version") == 1 and matrix.get("scope") == SCOPE and
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
    require(features.get("schema_version") == 1 and features.get("target_releases") == list(TARGETS) and
            features.get("upstream_pin_gate") == "KL11-57", "features release/inventory gate mismatch")
    require(features.get("implemented_api_versions") == [], "inventory cannot claim implemented API versions")
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
        require(row.get("id") == f"api.{key}" and row.get("implementation") == "missing" and
                row.get("qualification") == "not_run", f"API {key}: unsupported implementation/qualification claim")
        projection = projected_feature({version: inventory[key] for version, inventory in inventories.items()})
        require(all(row.get(field) == value for field, value in projection.items()), f"API {key}: feature/source classification mismatch")
    return {"schema_version": 1, "verdict": "passed", "implementation_claim": False,
            "releases": reports, "checks": ["immutable source pins", "bounded archives without extraction",
                "archive and per-file hashes", "93 unique complete request/response pairs per release",
                "ApiKeys flags and listener classifications", "exact valid/flexible/deprecated/stable ranges",
                "generator-derived header versions and exceptions", "truthful feature dispositions"],
            "limitations": ["Static upstream inventory, not handler behavior or broker qualification.",
                "Future advertisement must use actual implemented and tested versions; upstream Produce minimum-0 workaround is not a partitionline support claim."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=ROOT / "tests/conformance/broker/api-matrix.json")
    parser.add_argument("--features", type=Path, default=ROOT / "tests/conformance/broker/features.json")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        report = verify(args.matrix, args.features)
    except (ValueError, KeyError, TypeError, OSError, EOFError, tarfile.TarError) as error:
        report = {"schema_version": 1, "verdict": "failed", "implementation_claim": False, "error": str(error)}
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["verdict"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Compare selected ListOffsets message bodies with five pinned Apache SDKs."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import time
import urllib.request

sys.dont_write_bytecode = True
import generate_metadata_wire as common

JAVA = Path(__file__).with_name("ConformanceListOffsetsWire.java")
TEST = common.ROOT / "tests/list_offsets_wire_conformance.rs"
PINS_FILE = Path(__file__).with_name("pins.json")
PINS = json.loads(PINS_FILE.read_text())
HISTORICAL = (PINS["primary_pin"], PINS["secondary_pin"])
# The historical pins name annotated tag objects. These are their target commits.
COMMITS = {
    "3.9.1": "f745dfdcee2b9851204ddbbcd423626ab87294bc",
    "4.1.0": "13f70256db3c994c590e5d262a7cc50b9e973204",
} | common.COMMITS
RANGES = {"3.9.1": (0, 9), "4.1.0": (1, 10), "4.1.2": (1, 10),
          "4.2.1": (1, 11), "4.3.1": (1, 11)}
SLF4J_URL = "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
SLF4J_SHA256 = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
MAX_CORPUS_BYTES = 32_768


def logger_jar(output):
    with urllib.request.urlopen(SLF4J_URL, timeout=25) as response:
        data = response.read(65_537)
    if len(data) > 65_536 or hashlib.sha256(data).hexdigest() != SLF4J_SHA256:
        raise ValueError("wrong slf4j API download")
    path = output / "slf4j-api-1.7.36.jar"
    path.write_bytes(data)
    return path


def cases(max_version):
    cells = []
    for version in range(1, max_version + 1):
        cells.extend((f"{version}:empty", f"{version}:queries"))
        if version >= 6:
            cells.append(f"{version}:tagged")
        if version in (5, 6, 11):
            cells.append(f"{version}:error")
    return cells


def corpus(path, expected):
    if path.stat().st_size > MAX_CORPUS_BYTES:
        raise ValueError("ListOffsets corpus exceeds fixture budget")
    body = path.read_bytes()
    rows = body.decode("ascii").splitlines()
    if not body.endswith(b"\n") or len(rows) != len(expected):
        raise ValueError("wrong ListOffsets corpus size")
    for row, identity in zip(rows, expected, strict=True):
        fields = row.split(" ")
        if len(fields) != 3 or fields[0] != identity or not all(fields[1:]):
            raise ValueError("wrong ListOffsets row identity or fields")
    return body


def change_timeout(body, destination):
    rows = body.decode("ascii").splitlines()
    suffix = (1260).to_bytes(4, "big").hex() + "00"
    for index, row in enumerate(rows):
        identity, request, response = row.split(" ")
        if identity == "10:queries":
            if not request.endswith(suffix):
                raise ValueError("wrong ListOffsets timeout control suffix")
            tail = (1261).to_bytes(4, "big").hex() + "00"
            rows[index] = f"{identity} {request[:-len(suffix)]}{tail} {response}"
            destination.write_text("\n".join(rows) + "\n", encoding="ascii")
            return
    raise ValueError("missing ListOffsets timeout control row")


def compare_peer(output, release, logger, reference):
    directory = output / release
    directory.mkdir()
    jar = common.peer(output / "jars", release)
    low, high = RANGES[release]
    expected = cases(high)
    java = ["java", "-Xmx128m", "--class-path", f"{jar}{os.pathsep}{logger}", str(JAVA),
            str(jar), str(low), str(high)]
    apache = directory / "apache.txt"
    result = {"release": release, "upstream_commit": COMMITS[release],
              "declared_range": [low, high], "jar_sha256": common.digest(jar),
              "case_ids": expected}
    result["generate"] = common.run(java + ["generate", str(apache)], directory / "generate.log",
                                    marker=f"generated_pairs={len(expected)}")
    body = corpus(apache, expected)
    result["apache_fixture_sha256"] = common.digest(apache)
    if reference is not None:
        comparable = b"".join(reference.splitlines(keepends=True)[:len(expected)])
        if comparable != body:
            raise ValueError(f"Apache ListOffsets bodies differ for {release}")

    for profile in ("default", "all-features"):
        reverse = directory / f"{profile}-rust.txt"
        env = os.environ | {"LIST_OFFSETS_WIRE_INPUT": str(apache),
                            "LIST_OFFSETS_WIRE_OUTPUT": str(reverse),
                            "LIST_OFFSETS_WIRE_APACHE_MAX": str(high)}
        command = ["cargo", "test", "--locked"]
        if profile == "all-features":
            command.append("--all-features")
        command += ["--test", "list_offsets_wire_conformance", "--",
                    "list_offsets_wire_apache", "--ignored", "--exact"]
        result[profile] = common.run(command, directory / f"{profile}-rust.log", env=env,
                                     timeout=270, marker="1 passed")
        corpus(reverse, expected)
        result[f"{profile}-java"] = common.run(
            java + ["verify", str(reverse)], directory / f"{profile}-java.log",
            marker=f"independently_parsed_pairs={len(expected)}"
        )
        print(json.dumps({"peer": release, "profile": profile, "declared_range": [low, high],
                          "upstream_commit": COMMITS[release], "jar_sha256": result["jar_sha256"],
                          "apache_sha256": result["apache_fixture_sha256"],
                          "rust_sha256": common.digest(reverse), "body_pairs": len(expected)}),
              flush=True)
        if release == "4.3.1" and profile == "default":
            changed_apache = directory / "changed-apache.txt"
            changed_rust = directory / "changed-rust.txt"
            change_timeout(body, changed_apache)
            change_timeout(reverse.read_bytes(), changed_rust)
            changed = env | {"LIST_OFFSETS_WIRE_INPUT": str(changed_apache),
                             "LIST_OFFSETS_WIRE_OUTPUT": str(directory / "unused.txt")}
            result["rust_negative"] = common.run(
                command, directory / "rust-negative.log", env=changed, timeout=30,
                expect_failure=True, marker="ListOffsets request fields differ 10:queries")
            result["java_negative"] = common.run(
                java + ["verify", str(changed_rust)], directory / "java-negative.log",
                expect_failure=True, marker="ListOffsets request fields differ 10:queries")
            print("v10 changed timeout rejected by both checks", flush=True)
    return body, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    output = parser.parse_args().output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    common.PINS = {pin["version"]: pin["jar_sha256_maven_central"] for pin in HISTORICAL} | common.PINS
    if common.PINS.keys() != RANGES.keys() or COMMITS.keys() != RANGES.keys():
        raise ValueError("incomplete Apache SDK pins")
    common.END = time.monotonic() + 840
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, common.interrupt)
    files = (Path(__file__), Path(common.__file__), JAVA, TEST, PINS_FILE)

    def source_hashes():
        return {str(path.relative_to(common.ROOT)): common.digest(path) for path in files}

    inputs = source_hashes()
    results = []
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "could not enable child subreaper")
        timeout_control = common.verify_timeout_cleanup(output)
        logger = logger_jar(output)
        reference, result = compare_peer(output, "4.3.1", logger, None)
        results.append(result)
        for release in RANGES:
            if release != "4.3.1":
                _, result = compare_peer(output, release, logger, reference)
                results.append(result)
        if inputs != source_hashes():
            raise ValueError("source changed during comparison")
        print("APACHE_SHA256 " + hashlib.sha256(reference).hexdigest())
        print("BEGIN_LIST_OFFSETS_CORPUS")
        print(reference.decode("ascii"), end="")
        print("END_LIST_OFFSETS_CORPUS", flush=True)
        (output / "summary.json").write_text(json.dumps({
            "status": "pass", "api": "ListOffsets", "inputs": inputs, "results": results,
            "slf4j_sha256": common.digest(logger), "timeout_control": timeout_control,
            "scope": "Selected ListOffsets v1–v11 message bodies",
        }, indent=2) + "\n")
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps({
            "status": "fail", "error": type(error).__name__, "message": str(error),
            "completed": results,
        }, indent=2) + "\n")
        raise
    finally:
        if common.CHILD is not None:
            common.stop_group(common.CHILD)


if __name__ == "__main__":
    main()

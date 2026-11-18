#!/usr/bin/env python3
"""Compare selected Produce request and response bodies with pinned Apache SDKs."""
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

# This helper already provides bounded owned process groups and pinned downloads.
# Disable bytecode so it cannot enter the packaged test inventory.
sys.dont_write_bytecode = True
import generate_metadata_wire as common

JAVA = Path(__file__).with_name("ConformanceProduceWire.java")
TEST = common.ROOT / "tests/produce_wire_conformance.rs"
PINS_FILE = Path(__file__).with_name("pins.json")
PINS = json.loads(PINS_FILE.read_text())
HISTORICAL = (PINS["primary_pin"], PINS["secondary_pin"])
COMMITS = {pin["version"]: pin["upstream_sha"] for pin in HISTORICAL} | common.COMMITS
common.PINS = {pin["version"]: pin["jar_sha256_maven_central"] for pin in HISTORICAL} | common.PINS
VERSIONS = (4, 6, 7, 12)
SLF4J_SHA256 = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
SLF4J_URL = "https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
MAX_CORPUS_BYTES = 16_384


def slf4j(output):
    with urllib.request.urlopen(SLF4J_URL, timeout=25) as response:
        data = response.read(65_537)
    if len(data) > 65_536 or hashlib.sha256(data).hexdigest() != SLF4J_SHA256:
        raise ValueError("wrong slf4j API download")
    path = output / "slf4j-api-1.7.36.jar"
    path.write_bytes(data)
    return path


def corpus(path, versions):
    if path.stat().st_size > MAX_CORPUS_BYTES:
        raise ValueError("Produce corpus exceeds fixture budget")
    body = path.read_bytes()
    rows = body.decode("ascii").splitlines()
    if not body.endswith(b"\n") or len(rows) != len(versions):
        raise ValueError("wrong Produce corpus size")
    for row, version in zip(rows, versions):
        fields = row.split(" ")
        if len(fields) != 3 or fields[0] != str(version) or not fields[1] or not fields[2]:
            raise ValueError("wrong Produce row identity or fields")
    return body


def change_throttle(body, destination):
    rows = body.decode("ascii").splitlines()
    version, request, response = rows[0].split(" ")
    if version != "4" or not response.endswith("00000025"):
        raise ValueError("missing v4 throttle control")
    rows[0] = f"{version} {request} {response[:-8]}00000026"
    destination.write_text("\n".join(rows) + "\n")


def compare_peer(output, release, logger, reference):
    directory = output / release
    directory.mkdir()
    jar = common.peer(output / "jars", release)
    source = JAVA
    if release == "4.3.1":
        # Kafka moved these record classes. The retained compiled source records the change.
        source = directory / JAVA.name
        source.write_text(JAVA.read_text().replace(
            "org.apache.kafka.common.record.", "org.apache.kafka.common.record.internal."
        ))
    limit = 11 if release == "3.9.1" else 13
    versions = tuple(version for version in VERSIONS if version <= limit)
    java = ["java", "-Xmx128m", "--class-path", f"{jar}{os.pathsep}{logger}", str(source), str(jar), str(limit)]
    generated = directory / "apache.txt"
    result = {
        "release": release,
        "upstream_commit": COMMITS[release],
        "jar_sha256": common.digest(jar),
        "java_source_sha256": common.digest(source),
        "slf4j_sha256": common.digest(logger),
        "highest_declared_produce_version": limit,
        "versions": versions,
    }
    result["generate"] = common.run(java + ["generate", str(generated)], directory / "generate.log",
                                    marker=f"generated_pairs={len(versions)}")
    body = corpus(generated, versions)
    result["apache_fixture_sha256"] = common.digest(generated)
    if reference is not None:
        comparable = b"".join(reference.splitlines(keepends=True)[:len(versions)])
        if comparable != body:
            raise ValueError(f"Apache peers differ for Produce versions {versions}")
    for profile in ("default", "all-features"):
        reverse = directory / f"{profile}-rust.txt"
        env = os.environ | {
            "PRODUCE_WIRE_INPUT": str(generated),
            "PRODUCE_WIRE_OUTPUT": str(reverse),
            "PRODUCE_WIRE_APACHE_MAX": str(limit),
        }
        command = ["cargo", "test", "--locked"]
        if profile == "all-features":
            command.append("--all-features")
        command += ["--test", "produce_wire_conformance", "--", "produce_wire_apache", "--ignored", "--exact"]
        result[profile] = common.run(command, directory / f"{profile}-rust.log", env=env,
                                     timeout=270, marker="1 passed")
        result[f"{profile}-java"] = common.run(
            java + ["verify", str(reverse)], directory / f"{profile}-java.log",
            marker=f"independently_parsed_pairs={len(versions)}"
        )
        rebuilt = corpus(reverse, versions)
        if rebuilt != body:
            raise ValueError(f"Produce body differs for Apache {release}, Rust {profile}")
        print(json.dumps({
            "peer": release, "profile": profile, "declared_max": limit, "versions": versions,
            "fixture_sha256": common.digest(reverse), "body_pairs": len(versions),
        }), flush=True)
        if release == "4.1.0" and profile == "default":
            negative = directory / "changed-throttle.txt"
            change_throttle(body, negative)
            changed = env | {"PRODUCE_WIRE_INPUT": str(negative),
                             "PRODUCE_WIRE_OUTPUT": str(directory / "unused.txt")}
            result["rust_negative"] = common.run(
                command, directory / "rust-negative.log", env=changed, timeout=30,
                expect_failure=True, marker="v4 throttle differs"
            )
            result["java_negative"] = common.run(
                java + ["verify", str(negative)], directory / "java-negative.log",
                expect_failure=True, marker="Produce response fields differ"
            )
            print("v4 changed throttle rejected by both checks", flush=True)
    return body, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    common.END = time.monotonic() + 840
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, common.interrupt)
    files = (Path(__file__), Path(common.__file__), JAVA, TEST, PINS_FILE)
    inputs = {str(p.relative_to(common.ROOT)): common.digest(p) for p in files}
    results = []
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "could not enable child subreaper")
        timeout_control = common.verify_timeout_cleanup(output)
        logger = slf4j(output)
        reference, result = compare_peer(output, "4.1.0", logger, None)
        results.append(result)
        for release in common.PINS:
            if release != "4.1.0":
                _, result = compare_peer(output, release, logger, reference)
                results.append(result)
        if inputs != {str(p.relative_to(common.ROOT)): common.digest(p) for p in files}:
            raise ValueError("source changed during comparison")
        print("APACHE_SHA256 " + hashlib.sha256(reference).hexdigest())
        print("BEGIN_PRODUCE_CORPUS")
        print(reference.decode("ascii"), end="")
        print("END_PRODUCE_CORPUS", flush=True)
        (output / "summary.json").write_text(json.dumps({
            "status": "pass", "results": results, "inputs": inputs, "timeout_control": timeout_control,
            "scope": "Selected uncompressed Produce message bodies, without broker behavior.",
        }, indent=2) + "\n")
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps({
            "status": "fail", "error": type(error).__name__, "message": str(error), "completed": results,
        }, indent=2) + "\n")
        raise
    finally:
        if common.CHILD is not None:
            common.stop_group(common.CHILD)


if __name__ == "__main__":
    main()

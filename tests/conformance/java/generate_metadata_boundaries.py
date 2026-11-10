#!/usr/bin/env python3
"""Compare selected Metadata v0–v12 bodies with pinned Apache SDKs."""
import ctypes
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "tests/conformance/java/generate_metadata_wire.py"
JAVA = ROOT / "tests/conformance/java/ConformanceMetadataBoundaries.java"
RUST = ROOT / "tests/metadata_boundaries_conformance.rs"
VERSIONS = tuple(range(13))
PAIRS = 43

# The helper is shipped under tests/. Avoid creating ignored bytecode that
# Cargo's package include rules would otherwise consider an uncommitted file.
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("metadata_v13_runner", BASE)
if spec is None or spec.loader is None:
    raise RuntimeError("could not load existing bounded runner")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
common.PINS = {
    "3.9.1": "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9",
    "4.1.0": "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627",
    "4.1.2": "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
    "4.2.1": "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
    "4.3.1": "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
}
common.END = time.monotonic() + 840


def validate_case_rows(body):
    expected = []
    for version in VERSIONS:
        cells = ["empty", "name"]
        if version >= 1:
            cells.append("all")
        if version >= 9:
            cells.append("tagged")
        if version >= 12:
            cells.append("id")
        expected.extend(f"{version}:{cell}".encode("ascii") for cell in cells)
    rows = body.splitlines()
    if len(expected) != PAIRS or len(rows) != len(expected):
        raise ValueError("Metadata case count does not match declared versions")
    for row, identity in zip(rows, expected):
        fields = row.split(b" ")
        if len(fields) != 3 or fields[0] != identity or not fields[1] or not fields[2]:
            raise ValueError("Metadata case order does not match declared versions")


def corrupt_throttle(source, destination):
    lines = source.read_text().splitlines()
    if len(lines) != PAIRS:
        raise ValueError("wrong case count")
    for index, line in enumerate(lines):
        fields = line.split(" ")
        if len(fields) != 3:
            raise ValueError("wrong field count")
        if fields[0] == "3:empty":
            if not fields[2].startswith("00000025"):
                raise ValueError("unexpected v3 throttle")
            fields[2] = "00000026" + fields[2][8:]
            lines[index] = " ".join(fields)
            destination.write_text("\n".join(lines) + "\n")
            return
    raise ValueError("missing v3 corruption case")


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, common.interrupt)
    inputs = {str(p.relative_to(ROOT)): common.digest(p) for p in (BASE, JAVA, RUST, Path(__file__).resolve())}
    results = []
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "could not enable child subreaper")
        cleanup_control = common.verify_timeout_cleanup(output)
        apache_ref = None
        rust_ref = None
        for release in common.PINS:
            directory = output / release
            directory.mkdir()
            jar = common.peer(output / "jars", release)
            java = ["java", "-Xmx128m", "--class-path", str(jar), str(JAVA), str(jar)]
            fixture = directory / "apache.txt"
            result = {"release": release, "jar_sha256": common.digest(jar)}
            result["generate"] = common.run(java + ["generate", str(fixture)], directory / "generate.log",
                                             marker="generated_pairs=43")
            if fixture.stat().st_size > 32768:
                raise ValueError("fixture budget exceeded")
            body = fixture.read_bytes()
            validate_case_rows(body)
            if apache_ref is None:
                apache_ref = body
            elif body != apache_ref:
                raise ValueError("Apache peer fixture differs: " + release)
            print(json.dumps({"peer": release, "apache_fixture_sha256": common.digest(fixture)}), flush=True)
            if release == "3.9.1":
                result["unsupported13"] = common.run(
                    java + ["unsupported13", str(directory / "unused")], directory / "unsupported13.log",
                    marker="_response_same_as_v12=true")
                print((directory / "unsupported13.log").read_text().strip(), flush=True)
            for profile in ("default", "all-features"):
                import os
                reverse = directory / (profile + "-rust.txt")
                env = os.environ.copy()
                env.update(METADATA_BOUNDARY_INPUT=str(fixture), METADATA_BOUNDARY_OUTPUT=str(reverse))
                command = ["cargo", "test", "--locked"]
                if profile == "all-features":
                    command.append("--all-features")
                command += ["--test", "metadata_boundaries_conformance", "--",
                            "metadata_boundaries_apache_wire", "--ignored", "--exact"]
                result[profile] = common.run(command, directory / (profile + "-rust.log"),
                                             env=env, timeout=270, marker="1 passed")
                result[profile + "-java"] = common.run(
                    java + ["verify", str(reverse)], directory / (profile + "-java.log"),
                    marker="independently_parsed_pairs=43")
                body = reverse.read_bytes()
                validate_case_rows(body)
                if rust_ref is None:
                    rust_ref = body
                elif body != rust_ref:
                    raise ValueError("Rust reverse fixture differs: " + release + " " + profile)
                print(json.dumps({"peer": release, "profile": profile, "rust_reverse_sha256": common.digest(reverse),
                                  "rust_decoded_pairs": PAIRS, "java_parsed_pairs": PAIRS}), flush=True)
                if release == "3.9.1" and profile == "default":
                    bad_fixture = directory / "corrupt-apache.txt"
                    corrupt_throttle(fixture, bad_fixture)
                    bad_env = env | {"METADATA_BOUNDARY_INPUT": str(bad_fixture),
                                     "METADATA_BOUNDARY_OUTPUT": str(directory / "bad-rust.txt")}
                    result["rust-negative"] = common.run(
                        command, directory / "rust-negative.log", env=bad_env, timeout=30,
                        expect_failure=True, marker="assertion")
                    bad_reverse = directory / "corrupt-rust.txt"
                    corrupt_throttle(reverse, bad_reverse)
                    result["java-negative"] = common.run(
                        java + ["verify", str(bad_reverse)], directory / "java-negative.log",
                        expect_failure=True, marker="Rust response fields differ")
                    print(json.dumps({"corruption_controls": "both rejected"}), flush=True)
            results.append(result)
        if inputs != {str(p.relative_to(ROOT)): common.digest(p) for p in (BASE, JAVA, RUST, Path(__file__).resolve())}:
            raise ValueError("source changed during run")
        if apache_ref is None:
            raise ValueError("missing Apache corpus")
        bad_label = apache_ref.replace(b"0:empty ", b"99:empty ", 1)
        if bad_label == apache_ref:
            raise ValueError("missing case-order corruption target")
        try:
            validate_case_rows(bad_label)
        except ValueError:
            print("case_order_negative=rejected", flush=True)
        else:
            raise ValueError("case-order corruption was accepted")
        for label, body in (("APACHE", apache_ref), ("RUST", rust_ref)):
            if body is None or len(body) > 32768 or len(body.splitlines()) != PAIRS or not body.endswith(b"\n"):
                raise ValueError("corpus shape")
            print(label + "_SHA256 " + hashlib.sha256(body).hexdigest())
            print("BEGIN_" + label + "_CORPUS")
            print(body.decode("ascii"), end="")
            print("END_" + label + "_CORPUS", flush=True)
        summary = {"status": "pass", "versions": VERSIONS, "pairs_per_lane": PAIRS, "results": results,
                   "inputs": inputs, "timeout_control": cleanup_control,
                   "limit": "Finite body fixtures, not a live broker or complete Metadata conformance."}
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    except BaseException as error:
        (output / "failure.json").write_text(json.dumps(
            {"error": type(error).__name__, "message": str(error), "completed": results}, indent=2) + "\n")
        raise
    finally:
        if common.CHILD is not None:
            common.stop_group(common.CHILD)


if __name__ == "__main__":
    main()

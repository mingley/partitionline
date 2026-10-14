#!/usr/bin/env python3
"""Build pinned Apache Produce/Fetch quota-throttle bodies for KL01-25."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SOURCE = Path(__file__).with_name("ConformanceQuotaThrottleRuntime.java")
PINS = {
    "3.9.1": "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9",
    "4.1.0": "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627",
    "4.1.2": "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
    "4.2.1": "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
    "4.3.1": "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
}
SOURCE_COMMITS = {
    "3.9.1": "ce24d9b6aedca74f53c26f1ad2b2cc8ad950a03c",
    "4.1.0": "13f70256db3c994c590e5d262a7cc50b9e973204",
    "4.1.2": "c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c",
    "4.2.1": "18d5ecd939c8d510fdd72d0abb1f7099659dcd58",
    "4.3.1": "26b251a451ce941d3d7a55e6487bcb7f16b5ad48",
}
SLF4J = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
MAVEN = "https://repo1.maven.org/maven2"
REGISTERED_CASE_IDS = [
    "feature-produce-fetch-quota-throttle-runtime",
]
NONAPPLICABLE_CELLS = (
    "produce-v0-v2-removed",
    "produce-v3-v5-server-side-throttle",
    "fetch-v1-v7-pre-client-quota",
    "share-group-share-fetch",
    "kip-848-consumer-group-heartbeat",
)
NAMED_FILES = (
    "p01-produce-request.bin",
    "p01-produce-zero-response.bin",
    "p02-produce-quota-response.bin",
    "p03-produce-negative-response.bin",
    "f01-fetch-request.bin",
    "f01-fetch-zero-response.bin",
    "f02-fetch-quota-response.bin",
    "f03-empty-throttled-response.bin",
)
PRODUCE_VERSION = 9
FETCH_VERSION = 12
EXPECTED_BODIES = 8
EXPECTED_UNSUPPORTED = 0
TOPIC = "pl-quota"
SESSION_ID = 91
PRODUCE_QUOTA_MS = 250
FETCH_QUOTA_MS = 80


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def expected_cells(_release: str) -> list[dict]:
    rows = [
        {"cell": "p01-produce-zero", "disposition": "generated"},
        {"cell": "p02-produce-quota", "disposition": "generated"},
        {"cell": "p03-produce-negative", "disposition": "generated"},
        {"cell": "f01-fetch-zero", "disposition": "generated"},
        {"cell": "f02-fetch-quota", "disposition": "generated"},
        {"cell": "f03-empty-throttled-full", "disposition": "generated"},
    ]
    rows.extend({"cell": cell, "disposition": "nonapplicable"} for cell in NONAPPLICABLE_CELLS)
    return rows


def expected_bodies(_release: str) -> int:
    return EXPECTED_BODIES


def expected_unsupported(_release: str) -> int:
    return EXPECTED_UNSUPPORTED


def tree(directory: Path) -> dict[str, str]:
    return {
        str(path.relative_to(directory)): sha(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def fetch_jars(jars: Path, slf4j: Path) -> None:
    jars.mkdir(parents=True, exist_ok=True)
    slf4j.parent.mkdir(parents=True, exist_ok=True)
    for release, digest in PINS.items():
        dest = jars / f"kafka-clients-{release}.jar"
        if dest.exists() and sha(dest) == digest:
            continue
        url = f"{MAVEN}/org/apache/kafka/kafka-clients/{release}/kafka-clients-{release}.jar"
        subprocess.run(["curl", "-fsSL", url, "-o", str(dest)], check=True)
        if sha(dest) != digest:
            raise ValueError(f"downloaded jar pin differs: {release}")
    if not slf4j.exists() or sha(slf4j) != SLF4J:
        url = f"{MAVEN}/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
        subprocess.run(["curl", "-fsSL", url, "-o", str(slf4j)], check=True)
    if sha(slf4j) != SLF4J:
        raise ValueError("logging SDK differs")


def compile_and_run(jar: Path, slf4j: Path, classes: Path, command: list[str], timeout: float) -> str:
    classes.mkdir(parents=True, exist_ok=True)
    cp = f"{jar}{os.pathsep}{slf4j}"
    compile_cmd = [
        "java",
        "--add-modules",
        "jdk.compiler",
        "com.sun.tools.javac.Main",
        "-source",
        "21",
        "-target",
        "21",
        "-Xlint:all",
        "-Werror",
        "-cp",
        cp,
        "-d",
        str(classes),
        str(SOURCE),
    ]
    compiled = subprocess.run(compile_cmd, capture_output=True, text=True, timeout=30, check=False)
    if compiled.returncode:
        raise ValueError(f"compile failed: {compiled.stderr or compiled.stdout}")
    run = subprocess.run(
        ["java", "-Xmx128m", "-cp", f"{classes}{os.pathsep}{cp}", "ConformanceQuotaThrottleRuntime", *command],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if run.returncode:
        raise ValueError(f"java exited {run.returncode}: {run.stderr or run.stdout}")
    return run.stdout


def generate_release(release: str, jar: Path, slf4j: Path, workspace: Path, deadline: float) -> dict:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("overall quota throttle runtime deadline")
    classes = workspace / f"{release}-classes"
    first = workspace / f"{release}-fixtures-1"
    second = workspace / f"{release}-fixtures-2"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    out1 = compile_and_run(jar, slf4j, classes, ["generate", str(jar), str(first)], min(20.0, remaining))
    remaining = deadline - time.monotonic()
    out2 = compile_and_run(jar, slf4j, classes, ["generate", str(jar), str(second)], min(20.0, remaining))
    summary1 = json.loads(out1.splitlines()[-1])
    summary2 = json.loads(out2.splitlines()[-1])
    if summary1 != summary2:
        raise ValueError(f"{release} independent summaries differ")
    if tree(first) != tree(second):
        raise ValueError(f"{release} independent generations differ")
    if summary1["generated_bodies"] != EXPECTED_BODIES:
        raise ValueError(f"{release} body count {summary1['generated_bodies']} != {EXPECTED_BODIES}")
    if summary1["unsupported_receipts"] != EXPECTED_UNSUPPORTED:
        raise ValueError(f"{release} unsupported count differs")
    identity = json.loads((first / "identity.json").read_text())
    if identity["jar_sha256"] != PINS[release]:
        raise ValueError(f"{release} identity pin differs")
    if identity["generated_bodies"] != EXPECTED_BODIES:
        raise ValueError(f"{release} identity body count differs")
    if identity["topic"] != TOPIC or identity["session_id"] != SESSION_ID:
        raise ValueError(f"{release} topic/session differs")
    if identity["produce_quota_ms"] != PRODUCE_QUOTA_MS or identity["fetch_quota_ms"] != FETCH_QUOTA_MS:
        raise ValueError(f"{release} quota millis differ")
    for name in NAMED_FILES:
        if not (first / name).is_file():
            raise ValueError(f"{release} missing {name}")
    expected = json.loads((first / "expected.json").read_text())
    if expected["produce_quota_throttle"] != PRODUCE_QUOTA_MS:
        raise ValueError(f"{release} produce quota differs")
    if expected["fetch_quota_throttle"] != FETCH_QUOTA_MS:
        raise ValueError(f"{release} fetch quota differs")
    if expected["produce_negative_throttle"] != -1:
        raise ValueError(f"{release} negative produce throttle differs")
    if not expected["produce_should_client_throttle_v9"] or expected["produce_should_client_throttle_v5"]:
        raise ValueError(f"{release} Produce shouldClientThrottle gates differ")
    if not expected["fetch_should_client_throttle_v12"] or expected["fetch_should_client_throttle_v7"]:
        raise ValueError(f"{release} Fetch shouldClientThrottle gates differ")
    return {
        "release": release,
        "source_commit_sha": SOURCE_COMMITS[release],
        "jar_sha256": PINS[release],
        "generator_sha256": sha(SOURCE),
        "produce_version": PRODUCE_VERSION,
        "fetch_version": FETCH_VERSION,
        "generated_bodies": summary1["generated_bodies"],
        "unsupported_receipts": summary1["unsupported_receipts"],
        "independent_generations_match": True,
        "fixtures": str(first),
        "identity": identity,
    }


def install_fixtures(results: list[dict], dest: Path) -> dict:
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    installed = {}
    for row in results:
        target = dest / row["release"]
        shutil.copytree(row["fixtures"], target)
        installed[row["release"]] = {
            "bodies": row["generated_bodies"],
            "unsupported_receipts": row["unsupported_receipts"],
            "files": tree(target),
        }
    manifest = {
        "task": "KL01-25",
        "produce_version": PRODUCE_VERSION,
        "fetch_version": FETCH_VERSION,
        "topic": TOPIC,
        "session_id": SESSION_ID,
        "produce_quota_ms": PRODUCE_QUOTA_MS,
        "fetch_quota_ms": FETCH_QUOTA_MS,
        "registered_case_ids": REGISTERED_CASE_IDS,
        "nonapplicable_cells": list(NONAPPLICABLE_CELLS),
        "peers": {row["release"]: {
            "jar_sha256": row["jar_sha256"],
            "source_commit_sha": row["source_commit_sha"],
            "generated_bodies": row["generated_bodies"],
            "unsupported_receipts": row["unsupported_receipts"],
        } for row in results},
        "total_bodies": sum(row["generated_bodies"] for row in results),
        "total_unsupported_receipts": sum(row["unsupported_receipts"] for row in results),
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return installed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars", type=Path, default=Path("/tmp/partitionline-conformance"))
    parser.add_argument("--slf4j", type=Path, default=Path("/tmp/partitionline-conformance/slf4j-api-1.7.36.jar"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--install", type=Path)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--deadline-seconds", type=float, default=180.0)
    parser.add_argument("--list-cells", action="store_true")
    args = parser.parse_args()
    if args.list_cells:
        print(json.dumps({
            "registered_case_ids": REGISTERED_CASE_IDS,
            "peers": {release: expected_cells(release) for release in PINS},
            "expected_bodies": {release: expected_bodies(release) for release in PINS},
            "nonapplicable_cells": list(NONAPPLICABLE_CELLS),
        }, indent=2))
        return 0
    if args.output is None:
        raise SystemExit("--output is required unless --list-cells")
    deadline = time.monotonic() + args.deadline_seconds
    args.output.mkdir(parents=True, exist_ok=False)
    if args.fetch:
        fetch_jars(args.jars, args.slf4j)
    if sha(args.slf4j) != SLF4J:
        raise ValueError("logging SDK differs")
    results = []
    try:
        for release, digest in PINS.items():
            if time.monotonic() >= deadline:
                raise TimeoutError("overall quota throttle runtime deadline")
            jar = args.jars / f"kafka-clients-{release}.jar"
            if sha(jar) != digest:
                raise ValueError(f"SDK pin differs: {release}")
            results.append(generate_release(release, jar, args.slf4j, args.output, deadline))
        (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        if args.install:
            install_fixtures(results, args.install)
        print(json.dumps({
            "releases": [row["release"] for row in results],
            "total_bodies": sum(row["generated_bodies"] for row in results),
            "total_unsupported_receipts": sum(row["unsupported_receipts"] for row in results),
            "deadline_remaining_seconds": round(deadline - time.monotonic(), 3),
        }))
    except BaseException as error:
        (args.output / "failure.json").write_text(
            json.dumps({"error": type(error).__name__, "message": str(error)}, indent=2) + "\n"
        )
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())

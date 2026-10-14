#!/usr/bin/env python3
"""Build pinned Apache share-offset alter/delete bodies for KL01-29."""
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
SOURCE = Path(__file__).with_name("ConformanceShareOffsetMutations.java")
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
SPEAKING = ("4.1.0", "4.1.2", "4.2.1", "4.3.1")
SLF4J = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
MAVEN = "https://repo1.maven.org/maven2"
REGISTERED_CASE_IDS = [
    "current-public-api-091-v0-4-1-2",
    "current-public-api-091-v0-4-2-1",
    "current-public-api-091-v0-4-3-1",
    "current-public-api-092-v0-4-1-2",
    "current-public-api-092-v0-4-2-1",
    "current-public-api-092-v0-4-3-1",
]
UNSUPPORTED_REASON = (
    "3.9.1 kafka-clients has no AlterShareGroupOffsetsRequestData / "
    "DeleteShareGroupOffsetsRequestData (APIs 91/92 are 4.1+ KIP-932)."
)
NONAPPLICABLE_CELLS = (
    "describe-share-group-offsets",
    "share-fetch-acknowledge",
    "share-group-heartbeat",
    "ducktape-live-broker",
    "not-coordinator-public-retry-loop",
)
NAMED_PAIRS = (
    "alter-ok",
    "alter-invalid",
    "delete-ok",
    "delete-unknown",
)
ALTER_VERSION = 0
DELETE_VERSION = 0
SPEAKING_BODIES = 8
SPEAKING_UNSUPPORTED = 0
HISTORICAL_BODIES = 0
HISTORICAL_UNSUPPORTED = 8
TOPIC = "pl-share"
GROUP = "pl-share"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def speaks(release: str) -> bool:
    return release in SPEAKING


def expected_cells(release: str) -> list[dict]:
    if speaks(release):
        rows = [{"cell": stem, "disposition": "generated"} for stem in NAMED_PAIRS]
    else:
        rows = [{"cell": stem, "disposition": "unsupported", "reason": UNSUPPORTED_REASON} for stem in NAMED_PAIRS]
    rows.extend({"cell": cell, "disposition": "nonapplicable"} for cell in NONAPPLICABLE_CELLS)
    return rows


def expected_bodies(release: str) -> int:
    return SPEAKING_BODIES if speaks(release) else HISTORICAL_BODIES


def expected_unsupported(release: str) -> int:
    return SPEAKING_UNSUPPORTED if speaks(release) else HISTORICAL_UNSUPPORTED


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
        ["java", "-Xmx128m", "-cp", f"{classes}{os.pathsep}{cp}", "ConformanceShareOffsetMutations", *command],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if run.returncode:
        raise ValueError(f"java exited {run.returncode}: {run.stderr or run.stdout}")
    return run.stdout


def write_unsupported(release: str, dest: Path, digest: str) -> None:
    dest.mkdir(parents=True)
    receipts = [
        {
            "cell": stem,
            "disposition": "unsupported",
            "reason": UNSUPPORTED_REASON,
        }
        for stem in NAMED_PAIRS
    ]
    identity = {
        "apache_client_version": release,
        "jar_sha256": digest,
        "alter_version": ALTER_VERSION,
        "delete_version": DELETE_VERSION,
        "group_id": GROUP,
        "topic": TOPIC,
        "generated_bodies": 0,
        "unsupported_receipts": HISTORICAL_UNSUPPORTED,
        "unsupported": receipts,
    }
    (dest / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    (dest / "unsupported.json").write_text(json.dumps({"reason": UNSUPPORTED_REASON, "receipts": receipts}, indent=2) + "\n")


def generate_release(release: str, jar: Path, slf4j: Path, workspace: Path, deadline: float) -> dict:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("overall share-offset mutations deadline")
    if not speaks(release):
        first = workspace / f"{release}-fixtures-1"
        second = workspace / f"{release}-fixtures-2"
        write_unsupported(release, first, PINS[release])
        write_unsupported(release, second, PINS[release])
        if tree(first) != tree(second):
            raise ValueError(f"{release} independent unsupported receipts differ")
        identity = json.loads((first / "identity.json").read_text())
        return {
            "release": release,
            "source_commit_sha": SOURCE_COMMITS[release],
            "jar_sha256": PINS[release],
            "generator_sha256": sha(SOURCE),
            "alter_version": ALTER_VERSION,
            "delete_version": DELETE_VERSION,
            "generated_bodies": 0,
            "unsupported_receipts": HISTORICAL_UNSUPPORTED,
            "independent_generations_match": True,
            "fixtures": str(first),
            "identity": identity,
        }
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
    if summary1["generated_bodies"] != SPEAKING_BODIES:
        raise ValueError(f"{release} body count {summary1['generated_bodies']} != {SPEAKING_BODIES}")
    if summary1["unsupported_receipts"] != SPEAKING_UNSUPPORTED:
        raise ValueError(f"{release} unsupported count differs")
    identity = json.loads((first / "identity.json").read_text())
    if identity["jar_sha256"] != PINS[release]:
        raise ValueError(f"{release} identity pin differs")
    if identity["generated_bodies"] != SPEAKING_BODIES:
        raise ValueError(f"{release} identity body count differs")
    if identity["group_id"] != GROUP or identity["topic"] != TOPIC:
        raise ValueError(f"{release} group/topic differs")
    for stem in NAMED_PAIRS:
        if not (first / f"{stem}-request.bin").is_file() or not (first / f"{stem}-response.bin").is_file():
            raise ValueError(f"{release} missing {stem}")
    expected = json.loads((first / "expected.json").read_text())
    if expected["alter_start_offset"] != 7 or expected["alter_invalid_error"] != 42:
        raise ValueError(f"{release} alter profile differs")
    if expected["delete_unknown_error"] != 69 or not expected["delete_topics_are_names_only"]:
        raise ValueError(f"{release} delete profile differs")
    if expected["coordinator_key_type"] != 0:
        raise ValueError(f"{release} coordinator key type differs")
    return {
        "release": release,
        "source_commit_sha": SOURCE_COMMITS[release],
        "jar_sha256": PINS[release],
        "generator_sha256": sha(SOURCE),
        "alter_version": ALTER_VERSION,
        "delete_version": DELETE_VERSION,
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
        "task": "KL01-29",
        "alter_version": ALTER_VERSION,
        "delete_version": DELETE_VERSION,
        "topic": TOPIC,
        "group_id": GROUP,
        "registered_case_ids": REGISTERED_CASE_IDS,
        "speaking_peers": list(SPEAKING),
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
            "expected_unsupported": {release: expected_unsupported(release) for release in PINS},
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
                raise TimeoutError("overall share-offset mutations deadline")
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

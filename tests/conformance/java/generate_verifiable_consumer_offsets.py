#!/usr/bin/env python3
"""Build pinned Apache verifiable Consumer reset/autocommit bodies for KL01-27."""
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
SOURCE = Path(__file__).with_name("ConformanceVerifiableConsumerOffsets.java")
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
    "verifiable-consumer-reset-policies-3-9-1",
    "verifiable-consumer-autocommit-3-9-1",
]
NONAPPLICABLE_CELLS = (
    "earliest-reset-default-only",
    "message-create-time-elapsed-runtime",
    "command-config-4-2-plus",
    "broker-list-removed-4-2",
    "kip-848-consumer-protocol",
    "share-group",
    "verifiable-producer-options",
    "ducktape-live-broker",
)
NAMED_PAIRS = (
    "list-offsets-latest",
    "offset-fetch-empty",
    "offset-fetch-stored",
    "fetch-new",
    "fetch-resume",
    "fetch-first",
    "offset-commit-next-2",
    "offset-commit-interval-1",
)
FETCH_VERSION = 12
LIST_OFFSETS_VERSION = 5
OFFSET_COMMIT_VERSION = 8
OFFSET_FETCH_VERSION = 8
EXPECTED_BODIES = 16
EXPECTED_UNSUPPORTED = 0
TOPIC = "pl-offsets"
GROUP = "pl-offsets"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def expected_cells(_release: str) -> list[dict]:
    rows = [
        {"cell": "r01-latest-list-offsets", "disposition": "generated"},
        {"cell": "r02-offset-fetch-empty", "disposition": "generated"},
        {"cell": "r03-offset-fetch-stored", "disposition": "generated"},
        {"cell": "r04-latest-fetch-new", "disposition": "generated"},
        {"cell": "r05-none-resume-fetch", "disposition": "generated"},
        {"cell": "a01-autocommit-fetch-first", "disposition": "generated"},
        {"cell": "r06-latest-commit-next-2", "disposition": "generated"},
        {"cell": "a02-autocommit-interval-commit-1", "disposition": "generated"},
        {"cell": "e01-events", "disposition": "generated"},
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
        ["java", "-Xmx128m", "-cp", f"{classes}{os.pathsep}{cp}", "ConformanceVerifiableConsumerOffsets", *command],
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
        raise TimeoutError("overall verifiable Consumer offsets deadline")
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
    if identity["topic"] != TOPIC or identity["group_id"] != GROUP:
        raise ValueError(f"{release} topic/group differs")
    if identity["fetch_version"] != FETCH_VERSION or identity["list_offsets_version"] != LIST_OFFSETS_VERSION:
        raise ValueError(f"{release} Fetch/ListOffsets version differs")
    if identity["offset_commit_version"] != OFFSET_COMMIT_VERSION:
        raise ValueError(f"{release} OffsetCommit version differs")
    if identity["offset_fetch_version"] != OFFSET_FETCH_VERSION:
        raise ValueError(f"{release} OffsetFetch version differs")
    for stem in NAMED_PAIRS:
        if not (first / f"{stem}-request.bin").is_file() or not (first / f"{stem}-response.bin").is_file():
            raise ValueError(f"{release} missing {stem}")
    expected = json.loads((first / "expected.json").read_text())
    if expected["latest"]["delivered_value"] != "new" or expected["latest"]["commit_offset"] != 2:
        raise ValueError(f"{release} latest profile differs")
    if expected["none_missing"]["offset_fetch"] != -1 or expected["none_resume"]["offset_fetch"] != 2:
        raise ValueError(f"{release} none OffsetFetch differs")
    if expected["none_resume"]["delivered_value"] != "after-commit":
        raise ValueError(f"{release} none resume value differs")
    if expected["autocommit"]["interval_commit_offset"] != 1 or expected["autocommit"]["close_commit"]:
        raise ValueError(f"{release} autocommit profile differs")
    if expected["autocommit"]["offsets_committed_events"] != 0:
        raise ValueError(f"{release} autocommit events differ")
    profiles = json.loads((first / "profiles.json").read_text())
    if profiles["default_reset_policy"] != "earliest" or profiles["enable_autocommit_default"]:
        raise ValueError(f"{release} hashed defaults differ")
    if not profiles["autocommit_omits_offsets_committed"]:
        raise ValueError(f"{release} autocommit event suppression differs")
    events = json.loads((first / "events.json").read_text())
    if events["json_property_order"] != ["timestamp", "name"]:
        raise ValueError(f"{release} event field order differs")
    if events["record_data_property_order"] != [
        "timestamp", "name", "key", "value", "topic", "partition", "offset"
    ]:
        raise ValueError(f"{release} record_data field order differs")
    latest_names = [row["name"] for row in events["latest"]]
    if latest_names != [
        "startup_complete",
        "partitions_assigned",
        "record_data",
        "records_consumed",
        "offsets_committed",
        "shutdown_complete",
    ]:
        raise ValueError(f"{release} latest event names differ")
    if events["latest"][2]["offset"] != 1 or events["latest"][2]["value"] != "new":
        raise ValueError(f"{release} latest record_data differs")
    if events["latest"][4]["offsets"][0]["offset"] != 2:
        raise ValueError(f"{release} latest commit event differs")
    none_names = [row["name"] for row in events["none_missing"]]
    if none_names != ["startup_complete", "partitions_assigned", "shutdown_complete"]:
        raise ValueError(f"{release} none-missing events differ")
    if any(row["name"] == "offsets_committed" for row in events["autocommit"]):
        raise ValueError(f"{release} autocommit emitted offsets_committed")
    auto_records = [row for row in events["autocommit"] if row["name"] == "record_data"]
    if [row["value"] for row in auto_records] != ["first", "second"]:
        raise ValueError(f"{release} autocommit values differ")
    return {
        "release": release,
        "source_commit_sha": SOURCE_COMMITS[release],
        "jar_sha256": PINS[release],
        "generator_sha256": sha(SOURCE),
        "fetch_version": FETCH_VERSION,
        "list_offsets_version": LIST_OFFSETS_VERSION,
        "offset_commit_version": OFFSET_COMMIT_VERSION,
        "offset_fetch_version": OFFSET_FETCH_VERSION,
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
        "task": "KL01-27",
        "fetch_version": FETCH_VERSION,
        "list_offsets_version": LIST_OFFSETS_VERSION,
        "offset_commit_version": OFFSET_COMMIT_VERSION,
        "offset_fetch_version": OFFSET_FETCH_VERSION,
        "topic": TOPIC,
        "group_id": GROUP,
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
                raise TimeoutError("overall verifiable Consumer offsets deadline")
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

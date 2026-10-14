#!/usr/bin/env python3
"""Build pinned Apache JoinGroup v0/v1 fixtures for KL05-42."""
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
SOURCE = Path(__file__).with_name("LegacyJoinGroupFixtures.java")
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


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
        "-cp",
        cp,
        "-d",
        str(classes),
        str(SOURCE),
    ]
    compiled = subprocess.run(compile_cmd, check=False, capture_output=True, text=True, timeout=timeout)
    if compiled.returncode:
        raise ValueError(f"javac exited {compiled.returncode}: {compiled.stderr or compiled.stdout}")
    run = subprocess.run(
        ["java", "-cp", f"{classes}{os.pathsep}{cp}", "LegacyJoinGroupFixtures", *command],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if run.returncode:
        raise ValueError(f"java exited {run.returncode}: {run.stderr or run.stdout}")
    return run.stdout


def generate_release(release: str, jar: Path, slf4j: Path, workspace: Path, deadline: float) -> dict:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("overall JoinGroup v0/v1 deadline")
    classes = workspace / f"{release}-classes"
    first = workspace / f"{release}-fixtures-1"
    second = workspace / f"{release}-fixtures-2"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    out1 = compile_and_run(jar, slf4j, classes, ["generate", str(jar), str(first)], min(20.0, remaining))
    remaining = deadline - time.monotonic()
    out2 = compile_and_run(jar, slf4j, classes, ["generate", str(jar), str(second)], min(20.0, remaining))
    if tree(first) != tree(second):
        raise ValueError(f"{release} independent generations differ")
    identity = json.loads((first / "identity.json").read_text())
    if identity["jar_sha256"] != PINS[release]:
        raise ValueError(f"{release} identity pin differs")
    if identity["release"] != release:
        raise ValueError(f"{release} identity release differs")
    return {
        "release": release,
        "source_commit_sha": SOURCE_COMMITS[release],
        "jar_sha256": PINS[release],
        "generator_sha256": sha(SOURCE),
        "lowest_supported": identity["lowest_supported"],
        "highest_supported": identity["highest_supported"],
        "generated_bodies": identity["generated_bodies"],
        "unsupported_receipts": identity["unsupported_receipts"],
        "independent_generations_match": True,
        "stdout": out1.strip(),
        "fixtures": str(first),
    }


def install_fixtures(results: list[dict], dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for row in results:
        target = dest / row["release"]
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(row["fixtures"], target)
    manifest = {
        "task": "KL05-42",
        "crate_spoken_versions": [0, 1],
        "peers": {
            row["release"]: {
                "jar_sha256": row["jar_sha256"],
                "source_commit_sha": row["source_commit_sha"],
                "lowest_supported": row["lowest_supported"],
                "highest_supported": row["highest_supported"],
                "generated_bodies": row["generated_bodies"],
                "unsupported_receipts": row["unsupported_receipts"],
            }
            for row in results
        },
        "total_bodies": sum(row["generated_bodies"] for row in results),
        "total_unsupported_receipts": sum(row["unsupported_receipts"] for row in results),
        "independent_generations_match": all(row["independent_generations_match"] for row in results),
        "local_mock_labeled": True,
    }
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (dest / "README.md").write_text(
        "# Legacy JoinGroup v0/v1 fixtures\n\n"
        "Pinned Apache kafka-clients builders and parsers for JoinGroup v0 and v1.\n"
        "All five pins (3.9.1, 4.1.0, 4.1.2, 4.2.1, 4.3.1) report\n"
        "`ApiKeys.JOIN_GROUP` 0-9 and emit identical leftover-empty bodies.\n"
        "A present `group.instance.id` is `UnsupportedVersionException` on both\n"
        "versions (ten retained receipts).\n\n"
        "`tests/common` Mock public join runs are local and are not this oracle.\n"
        "Do not infer a successful old-group lifecycle from a field roundtrip.\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars", type=Path, default=Path("/tmp/partitionline-conformance"))
    parser.add_argument("--slf4j", type=Path, default=Path("/tmp/partitionline-conformance/slf4j-api-1.7.36.jar"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--install", type=Path)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--deadline-seconds", type=float, default=180.0)
    args = parser.parse_args()
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
                raise TimeoutError("overall JoinGroup v0/v1 deadline")
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

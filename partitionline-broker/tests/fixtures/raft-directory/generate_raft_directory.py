#!/usr/bin/env python3
"""Build pinned Apache Vote/Begin/End directory fixtures for KL11-70."""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SOURCE = Path(__file__).with_name("DirectoryFixtures.java")
PINS = {
    "4.1.2": "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431",
    "4.2.1": "6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8",
    "4.3.1": "52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36",
}
SLF4J = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
MAVEN = "https://repo1.maven.org/maven2"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def compile_and_run(jar: Path, slf4j: Path, classes: Path, dest: Path) -> None:
    classes.mkdir(parents=True, exist_ok=True)
    dest.mkdir(parents=True, exist_ok=True)
    cp = f"{jar}{os.pathsep}{slf4j}"
    compiled = subprocess.run(
        [
            "java",
            "--add-modules",
            "jdk.compiler",
            "com.sun.tools.javac.Main",
            "-Xlint:all",
            "-Werror",
            "-cp",
            cp,
            "-d",
            str(classes),
            str(SOURCE),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if compiled.returncode:
        sys.stderr.write(compiled.stderr)
        raise SystemExit(compiled.returncode)
    ran = subprocess.run(
        ["java", "-Xmx128m", "-cp", f"{classes}{os.pathsep}{cp}", "DirectoryFixtures", str(jar), str(dest)],
        check=False,
        capture_output=True,
        text=True,
    )
    sys.stdout.write(ran.stdout)
    if ran.returncode:
        sys.stderr.write(ran.stderr)
        raise SystemExit(ran.returncode)


def install_fixtures(generated: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for release in PINS:
        source = generated / release
        target = dest / release
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)
    for name in ("DirectoryFixtures.java", "generate_raft_directory.py"):
        source = Path(__file__).with_name(name)
        target = dest / name
        if source.resolve() != target.resolve():
            shutil.copy2(source, target)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jars", type=Path, default=Path("/tmp/raft-directory-jars"))
    parser.add_argument("--install", type=Path)
    args = parser.parse_args()
    slf4j = args.jars / "slf4j-api-1.7.36.jar"
    fetch_jars(args.jars, slf4j)
    for release in PINS:
        dest = args.output / release
        classes = args.output / "classes" / release
        compile_and_run(args.jars / f"kafka-clients-{release}.jar", slf4j, classes, dest)
    if args.install:
        install_fixtures(args.output, args.install)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

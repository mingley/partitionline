#!/usr/bin/env python3
"""Strict-compile the public native peer against the pinned, unchanged C SDK."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--sdk", type=Path, default=Path("/workspace/work/c-peer"))
    args = parser.parse_args()
    source = Path(__file__).with_name("compaction-native.c")
    header = args.sdk / "source/src/rdkafka.h"
    library = args.sdk / "lib/librdkafka.so.1"
    assert sha(header) == "33cca14d9fc87117100ce90bd84f2006b8ba521c788055db6af614d0f2f7ca2f"
    assert sha(library) == "8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc"
    args.evidence.mkdir(parents=True, exist_ok=False)
    args.binary.parent.mkdir(parents=True, exist_ok=True)
    (args.evidence / source.name).write_bytes(source.read_bytes())
    command = ["taskset", "-c", "2,4", "gcc", "-std=c11", "-O2", "-Wall", "-Wextra",
               "-Werror", "-pedantic", "-isystem", str(header.parent), str(source),
               "-L" + str(library.parent), "-Wl,-rpath," + str(library.parent),
               "-lrdkafka", "-o", str(args.binary)]
    log = args.evidence / "compile.log"
    with log.open("wb") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    compiler = subprocess.run(["gcc", "--version"], capture_output=True, text=True, check=True)
    pins = {str(p): sha(p) for p in (source, header, library)}
    if result.returncode == 0:
        pins[str(args.binary)] = sha(args.binary)
    report = {"schema_version": 1, "passed": result.returncode == 0,
              "scope": "Strict own-source compilation; unmodified pinned upstream header is a system include. No broker runtime.",
              "native_source_sha": "9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab",
              "command": command, "exit_code": result.returncode,
              "compiler": compiler.stdout, "pins": pins, "log_sha256": sha(log)}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "binary": str(args.binary)}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Strict-compile the same retention behavior against three pinned official SDKs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--jars", default=Path("/workspace/work/broker-wire/jars"), type=Path)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).with_name("RetentionPeer.java")
    common = source.read_text()
    results = []
    for release in ("4.1.2", "4.2.1", "4.3.1"):
        directory = args.work / release
        directory.mkdir(parents=True, exist_ok=True)
        classes = directory / "classes"
        classes.mkdir(exist_ok=True)
        adapted = directory / "RetentionPeer.java"
        text = common
        adaptation = "none"
        if release != "4.3.1":
            assert common.count("org.apache.kafka.common.record.internal.") == 2
            text = common.replace("org.apache.kafka.common.record.internal.", "org.apache.kafka.common.record.")
            adaptation = "Only the two record-class imports use the pre-4.3 official package; executable behavior unchanged."
        adapted.write_text(text)
        retained = args.evidence / release
        retained.mkdir()
        (retained / "RetentionPeer.java").write_text(text)
        jars = [args.jars / f"kafka-clients-{release}.jar", args.jars / "slf4j-api-1.7.36.jar"]
        command = ["taskset", "-c", "0-2,4", "java", "-Xmx128m", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
                   "-Xlint:all", "-Werror", "-cp", ":".join(map(str, jars)), "-d", str(classes), str(adapted)]
        log = retained / "compile.log"
        with log.open("w") as stream:
            completed = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, check=False)
        results.append({"release": release, "command": command, "exit_code": completed.returncode,
                        "common_source_sha256": digest(source), "adapted_source_sha256": digest(adapted),
                        "adaptation": adaptation, "jars": [{"path": str(j), "sha256": digest(j)} for j in jars],
                        "log": str(log), "classes": [{"path": str(p), "sha256": digest(p)} for p in sorted(classes.glob("*.class"))]})
    version = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
    report = {"schema_version": 1, "scope": "Strict peer-source compilation; no broker runtime outcome.",
              "passed": all(r["exit_code"] == 0 for r in results), "java_version": version.stderr, "results": results}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "results": [{"release": r["release"], "exit_code": r["exit_code"]} for r in results]}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

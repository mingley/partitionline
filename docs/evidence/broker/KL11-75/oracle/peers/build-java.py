#!/usr/bin/env python3
"""Strict-compile the same public compaction behavior against three pinned official SDKs."""
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
    source = Path(__file__).with_name("CompactionPeer.java")
    common = source.read_text()
    results = []
    for release in ("4.1.2", "4.2.1", "4.3.1"):
        directory = args.work / release
        directory.mkdir(parents=True, exist_ok=True)
        classes = directory / "classes"
        classes.mkdir(exist_ok=True)
        adapted = directory / "CompactionPeer.java"
        text = common
        adaptation = "none"
        adapted.write_text(text)
        retained = args.evidence / release
        retained.mkdir()
        (retained / "CompactionPeer.java").write_text(text)
        jars = [args.jars / f"kafka-clients-{release}.jar", args.jars / "slf4j-api-1.7.36.jar"]
        pins = {
            "kafka-clients-4.1.2.jar": "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed",
            "kafka-clients-4.2.1.jar": "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159",
            "kafka-clients-4.3.1.jar": "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e",
            "slf4j-api-1.7.36.jar": "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0",
        }
        if any(digest(jar) != pins[jar.name] for jar in jars):
            raise RuntimeError("Pinned Apache public SDK changed")
        command = ["taskset", "-c", "2,4", "java", "-Xmx128m", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
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

#!/usr/bin/env python3
"""Strict public OAuth peer compilation using already verified official SDK jars."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    base = Path(__file__).parent
    source = base / "OAuthMetadataPeer.java"
    pins = json.loads((base / "pins.json").read_text())
    rows = []
    for release in pins["releases"]:
        name = release["release"]
        evidence = args.evidence / name; evidence.mkdir()
        classes = args.work / name / "classes"; classes.mkdir(parents=True, exist_ok=False)
        jars = []
        for pin in release["runtime_jars"]:
            path = Path(pin["path"])
            if not path.is_file() or path.is_symlink() or sha(path) != pin["sha256"]:
                raise ValueError("Pinned official jar changed")
            jars.append(path)
        retained_source = evidence / source.name; retained_source.write_bytes(source.read_bytes())
        argv = ["taskset", "-c", "2,4", "/usr/bin/java", "-Xmx128m", "--add-modules", "jdk.compiler",
                "com.sun.tools.javac.Main", "-Xlint:all", "-Werror", "-cp", ":".join(map(str, jars)),
                "-d", str(classes), str(retained_source)]
        log = evidence / "compile.log"
        with log.open("w") as output:
            completed = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT, timeout=60, check=False)
        rows.append({"release": name, "source_sha256": sha(source), "retained_source_sha256": sha(retained_source),
                     "argv": argv, "exit_code": completed.returncode, "runtime_jars": release["runtime_jars"],
                     "log": str(log), "log_sha256": sha(log), "classes_dir": str(classes),
                     "classes": [{"file":str(path.relative_to(classes)), "sha256":sha(path), "bytes":path.stat().st_size}
                                 for path in sorted(classes.rglob("*.class"))]})
    java = Path("/usr/bin/java").resolve()
    version = subprocess.run([str(java), "-version"], capture_output=True, text=True, timeout=10, check=False)
    report = {"schema_version":1, "passed":all(row["exit_code"] == 0 for row in rows), "rows":rows,
              "java":{"path":str(java),"sha256":sha(java),"version":version.stderr},
              "source_pins_sha256":sha(base / "pins.json"), "build_script_sha256":sha(Path(__file__)),
              "scope":"Strict source compilation only. No HTTPS token retrieval, broker auth, metadata, read/write, or lifecycle runtime result.",
              "actual_live_peers":0}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed":report["passed"],"three_sdk_strict_compiles":len(rows),"actual_live_peers":0}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

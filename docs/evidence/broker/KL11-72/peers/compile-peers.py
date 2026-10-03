#!/usr/bin/env python3
"""Strict compile independently pinned Java and native SASL socket peers."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; pins are asserted.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars", type=Path, required=True)
    parser.add_argument("--native-source", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--source-sha")
    parser.add_argument("--git-repository", type=Path, default=ROOT)
    args = parser.parse_args()
    args.output.mkdir()
    args.build.mkdir(parents=True)
    native = json.loads((HERE / "native-references/pins.json").read_text())
    assert sha(args.native_library) == native["library_sha256"]
    header = args.native_source / "src/rdkafka.h"
    assert sha(header) == native["source_sha256"]["src/rdkafka.h"]
    sources = [HERE / "SaslSocketPeer.java", HERE / "sasl-native-peer.c",
               Path(__file__), HERE.parent / "oracle/SaslWireOracle.java"]
    report = {"scope": "Strict independent peer compilation only; no live connection claim.",
              "source_sha": args.source_sha, "production_qualification": False,
              "source_sha256": {str(p.relative_to(ROOT)): sha(p) for p in sources},
              "native_library_sha256": sha(args.native_library), "commands": []}
    if args.source_sha:
        for path in sources:
            relative = path.relative_to(ROOT).as_posix()
            raw = subprocess.run(["git", "show", args.source_sha + ":" + relative],
                                 cwd=args.git_repository, capture_output=True, check=True).stdout
            assert raw == path.read_bytes(), "Source object differs: " + relative

    def execute(argv, name):
        out, err = args.output / (name + ".stdout.log"), args.output / (name + ".stderr.log")
        with out.open("xb") as stdout, err.open("xb") as stderr:
            result = subprocess.run(argv, stdout=stdout, stderr=stderr, cwd=ROOT)
        report["commands"].append({"argv": argv, "exit_code": result.returncode,
                                  "stdout": out.name, "stderr": err.name,
                                  "stdout_sha256": sha(out), "stderr_sha256": sha(err)})
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        assert result.returncode == 0, name + " failed; retained logs"

    report["java_version"] = subprocess.run(["java", "-version"], capture_output=True, text=True,
                                             check=True).stderr
    report["compiler_version"] = subprocess.run(["cc", "--version"], capture_output=True, text=True,
                                                 check=True).stdout.splitlines()[0]
    report["native_peer_build"] = {}
    for version in ["4.1.2", "4.2.1", "4.3.1"]:
        jar = args.jars / ("kafka-clients-" + version + ".jar")
        pin = json.loads((ROOT / "docs/evidence/broker/KL11-04" / ("apache-" + version + ".provenance.json")).read_text())
        assert sha(jar) == pin["jar_sha256"]
        classes = args.build / version
        classes.mkdir()
        execute(["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "--add-modules", "jdk.compiler",
                 "com.sun.tools.javac.Main", "-Xlint:all", "-Werror", "-cp", str(jar), "-d", str(classes),
                 str(HERE.parent / "oracle/SaslWireOracle.java"), str(HERE / "SaslSocketPeer.java")], version + "-compile")
        report["native_peer_build"][version] = {"jar_sha256": sha(jar), "classes_sha256": {
            p.name: sha(p) for p in sorted(classes.glob("*.class"))}}
    binary = args.build / "sasl-native-peer"
    execute(["taskset", "-c", "0-2,4", "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-isystem",
             str(args.native_source / "src"), str(HERE / "sasl-native-peer.c"), "-L", str(args.native_library.parent),
             "-Wl,-rpath," + str(args.native_library.parent), "-lrdkafka", "-o", str(binary)], "native-compile")
    report.update(verdict="passed", native_binary_sha256=sha(binary),
                  warning_policy="Strict own-source warnings; exact external vendor header is a system header.")
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verdict": "passed", "strict_java_compiles": 3, "strict_native_compiles": 1}))


if __name__ == "__main__":
    main()

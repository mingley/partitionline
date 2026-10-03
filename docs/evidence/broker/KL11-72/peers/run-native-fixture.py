#!/usr/bin/env python3
"""Reproduce exact public native Alter bytes with three genuine Apache SDKs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python; pin checks are assertions.")
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["jars", "build", "output", "git-repository"]:
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    args.output.mkdir()
    args.build.mkdir(parents=True)
    fixtures = ROOT / "partitionline-broker/tests/fixtures/sasl-wire"
    names = ["native-alter-canonical.frame.hex", "native-alter-redundant-empty-tag.frame.hex"]
    sources = [Path(__file__), HERE / "NativeAlterFixture.java", HERE.parent / "oracle/SaslWireOracle.java"]
    sources += [fixtures / name for name in names]
    objects = {}
    for path in sources:
        relative = path.relative_to(ROOT).as_posix()
        raw = subprocess.run(["git", "show", args.source_sha + ":" + relative], cwd=args.git_repository,
                             check=True, capture_output=True).stdout
        assert raw == path.read_bytes(), "Source differs: " + relative
        objects[relative] = sha(path)
    slf = args.jars / "slf4j-api-1.7.36.jar"
    assert sha(slf) == "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
    report = {"source_sha": args.source_sha, "verified_source_objects_sha256": objects,
              "scope": "Actual Apache serializer/crypto reproduction of public fixture; authentic native TLS-write digest binding is separately retained in native-frame-byte-proof.json. No socket/server run here.",
              "production_qualification": False, "commands": [], "versions": {}}

    def retain():
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")

    for version in ["4.1.2", "4.2.1", "4.3.1"]:
        jar = args.jars / ("kafka-clients-" + version + ".jar")
        pin = json.loads((ROOT / "docs/evidence/broker/KL11-04" / ("apache-" + version + ".provenance.json")).read_text())
        assert sha(jar) == pin["jar_sha256"]
        classes = args.build / version
        classes.mkdir()
        commands = [("compile", ["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "--add-modules", "jdk.compiler",
                                "com.sun.tools.javac.Main", "-Xlint:all", "-Werror", "-cp", str(jar), "-d", str(classes),
                                str(HERE.parent / "oracle/SaslWireOracle.java"), str(HERE / "NativeAlterFixture.java")]),
                    ("generate", ["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "-cp",
                                 ":".join(map(str, [classes, jar, slf])), "NativeAlterFixture", str(classes / "output")])]
        for name, argv in commands:
            out, err = args.output / (version + "-" + name + ".stdout.log"), args.output / (version + "-" + name + ".stderr.log")
            with out.open("xb") as stdout, err.open("xb") as stderr:
                result = subprocess.run(argv, stdout=stdout, stderr=stderr, cwd=ROOT, timeout=30)
            report["commands"].append({"argv": argv, "exit_code": result.returncode,
                                       "stdout": out.name, "stderr": err.name,
                                       "stdout_sha256": sha(out), "stderr_sha256": sha(err)})
            retain()
            assert result.returncode == 0, "Apache fixture command failed; logs retained"
        for name in names:
            assert (classes / "output" / name).read_bytes() == (fixtures / name).read_bytes(), "Fixture bytes differ"
        report["versions"][version] = {"jar_sha256": sha(jar),
                                      "frame_digests": json.loads((args.output / (version + "-generate.stdout.log")).read_text())}
    assert len({json.dumps(x["frame_digests"], sort_keys=True) for x in report["versions"].values()}) == 1
    report.update(verdict="passed", strict_java_compiles=3, actual_fixture_generations=3, byte_identical=True)
    retain()
    print(json.dumps({"verdict": "passed", "strict_java_compiles": 3, "actual_fixture_generations": 3}))


if __name__ == "__main__":
    main()

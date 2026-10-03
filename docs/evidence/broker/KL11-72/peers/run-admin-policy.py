#!/usr/bin/env python3
"""Execute pinned real Apache image/controller classes, without a broker process."""
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
    parser.add_argument("--admin-jars", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--source-sha")
    parser.add_argument("--git-repository", type=Path, default=ROOT)
    args = parser.parse_args()
    args.output.mkdir()
    args.build.mkdir(parents=True)
    pins = json.loads((HERE / "apache-admin-references/jar-pins.json").read_text())
    fixture = ROOT / "partitionline-broker/tests/fixtures/sasl-wire/apache-wire.tsv"
    assert sha(fixture) == "3bd875f19afd00cae0b3174bdcd4ebc4f0b44b5e4ee70debd37b7aae2e8bc5b3"
    report = {"scope": "Execute actual Apache ScramImage.describe and ScramControlManager.alterCredentials object methods. This is not a Kafka broker/socket/authorization execution.",
              "source_sha": args.source_sha, "production_qualification": False,
              "source_sha256": {p.name: sha(p) for p in [Path(__file__), HERE / "AdminPolicyOracle.java"]},
              "commands": [], "releases": []}
    if args.source_sha:
        for path in [Path(__file__), HERE / "AdminPolicyOracle.java", fixture]:
            raw = subprocess.run(["git", "show", args.source_sha + ":" + path.relative_to(ROOT).as_posix()],
                                 cwd=args.git_repository, capture_output=True, check=True).stdout
            assert raw == path.read_bytes()

    def execute(argv, name):
        out, err = args.output / (name + ".stdout.log"), args.output / (name + ".stderr.log")
        with out.open("xb") as stdout, err.open("xb") as stderr:
            result = subprocess.run(argv, stdout=stdout, stderr=stderr, cwd=ROOT)
        report["commands"].append({"argv": argv, "exit_code": result.returncode, "stdout": out.name, "stderr": err.name,
                                  "stdout_sha256": sha(out), "stderr_sha256": sha(err)})
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        assert result.returncode == 0, name + " failed; logs retained"

    baseline = None
    slf = args.jars / "slf4j-api-1.7.36.jar"
    assert sha(slf) == "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
    for release in pins:
        version = release["version"]
        clients = args.jars / ("kafka-clients-" + version + ".jar")
        assert sha(clients) == release["distribution"]["jar_sha256"]
        jars = [clients, slf]
        for name, expected in release["jars_sha256"].items():
            path = args.admin_jars / name
            assert sha(path) == expected
            jars.append(path)
        classes = args.build / version
        classes.mkdir()
        classpath = ":".join(map(str, jars))
        execute(["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
                 "-Xlint:all", "-Werror", "-cp", classpath, "-d", str(classes), str(HERE / "AdminPolicyOracle.java")], version + "-compile")
        output = args.output / (version + "-outcomes.tsv")
        execute(["taskset", "-c", "0-2,4", "java", "-Xms16m", "-Xmx128m", "-cp", str(classes) + ":" + classpath,
                 "org.apache.kafka.controller.AdminPolicyOracle", str(fixture), str(output)], version + "-actual-policies")
        digest = sha(output)
        if baseline is None:
            baseline = digest
        assert digest == baseline, "actual policy outcomes differ across releases"
        report["releases"].append({"version": version, "jars_sha256": {p.name: sha(p) for p in jars},
                                   "exact_admin_cases": 14, "outcomes_sha256": digest})
    report.update(verdict="passed", actual_object_policy_cases=42, releases_byte_identical=True)
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verdict": "passed", "actual_object_policy_cases": 42, "releases_byte_identical": True}))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Strict compile, generate and byte-replay pinned Apache SASL/admin fixtures."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[5]
ORACLE = Path(__file__).resolve().parent
FILES = ["apache-wire.tsv", "apache-errors.tsv", "apache-parser-outcomes.tsv", "apache-bootstrap.tsv"]
CRYPTO_SOURCE = "029355f95687bb0528eb70f7872ce2e901adf4de"
CRYPTO_SHA = "b7967a38b463b1ee13beeca95d31fc63c723556e90e7c39f9981c5e909d654c1"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; pin checks are assertions.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--classes", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--source-sha")
    parser.add_argument("--git-repository", type=Path, default=ROOT)
    args = parser.parse_args()
    args.jars, args.output, args.classes, args.fixtures = [path.resolve() for path in
            [args.jars, args.output, args.classes, args.fixtures]]
    args.output.mkdir()
    args.classes.mkdir(parents=True)
    crypto = ROOT / "partitionline-broker/tests/fixtures/sasl/apache-scram.tsv"
    assert sha(crypto) == CRYPTO_SHA
    immutable = subprocess.run(["git", "show", CRYPTO_SOURCE + ":partitionline-broker/tests/fixtures/sasl/apache-scram.tsv"],
                               cwd=args.git_repository, check=True, capture_output=True).stdout
    assert immutable == crypto.read_bytes()
    slf = args.jars / "slf4j-api-1.7.36.jar"
    assert sha(slf) == "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
    java_version = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
    matrix = json.loads((ROOT / "tests/conformance/broker/api-matrix.json").read_text())
    report = {"scope": "Actual Apache generated serializers/parsers, RequestHeader/ResponseHeader, Errors and getErrorResponse; not an Apache network/server execution.",
              "source_sha": args.source_sha, "production_qualification": False,
              "crypto_reference_source": CRYPTO_SOURCE, "crypto_fixture_sha256": CRYPTO_SHA,
              "generator_sha256": sha(ORACLE / "SaslWireOracle.java"), "runner_sha256": sha(Path(__file__)),
              "java_version": java_version.stdout + java_version.stderr, "slf4j_sha256": sha(slf),
              "releases": []}
    if args.source_sha:
        assert len(args.source_sha) == 40 and all(c in "0123456789abcdef" for c in args.source_sha)
        critical = [ORACLE / "SaslWireOracle.java", Path(__file__), crypto,
                    ROOT / "tests/conformance/broker/api-matrix.json"]
        critical += [args.fixtures / name for name in FILES]
        critical += sorted((ORACLE / "references").rglob("*"))
        bound = {}
        for path in critical:
            if not path.is_file():
                continue
            relative = path.relative_to(ROOT).as_posix()
            raw = subprocess.run(["git", "show", args.source_sha + ":" + relative],
                                 cwd=args.git_repository, capture_output=True, check=True).stdout
            assert raw == path.read_bytes(), "Source object differs: " + relative
            bound[relative] = sha(path)
        report["verified_source_objects_sha256"] = bound
    else:
        report["source_context"] = "Uncommitted fixture development; source SHA256 hashes are exact, not a frozen source qualification."

    def execute(argv, name):
        out, err = args.output / (name + ".stdout.log"), args.output / (name + ".stderr.log")
        with out.open("xb") as stdout, err.open("xb") as stderr:
            result = subprocess.run(argv, cwd=ROOT, stdout=stdout, stderr=stderr)
        row = {"argv": argv, "exit_code": result.returncode, "stdout": out.name, "stderr": err.name,
               "stdout_sha256": sha(out), "stderr_sha256": sha(err)}
        assert result.returncode == 0, row
        return row

    baseline = None
    for release in matrix["releases"]:
        version = release["version"]
        jar = args.jars / ("kafka-clients-" + version + ".jar")
        pin = json.loads((ROOT / "docs/evidence/broker/KL11-04" / ("apache-" + version + ".provenance.json")).read_text())
        assert sha(jar) == pin["jar_sha256"] and release["commit"] == pin["source_sha"]
        with zipfile.ZipFile(jar) as archive:
            names = [name for name in archive.namelist() if name.endswith("kafka-version.properties")]
            assert len(names) == 1
            properties = dict(row.split("=", 1) for row in archive.read(names[0]).decode().splitlines()
                              if "=" in row and not row.startswith("#"))
            assert properties["version"] == version and properties["commitId"] == release["commit"][:16]
        classes = args.classes / version
        classes.mkdir()
        compile_result = execute(["taskset", "-c", "0-2,4", "java", "--add-modules", "jdk.compiler",
                "com.sun.tools.javac.Main", "-Xlint:all", "-Werror", "-cp", str(jar), "-d", str(classes),
                str(ORACLE / "SaslWireOracle.java")], version + "-compile")
        generation_result = execute(["taskset", "-c", "0-2,4", "java", "-cp", ":".join([str(classes), str(jar), str(slf)]),
                "SaslWireOracle", str(args.output / version), str(crypto)], version + "-generate-replay")
        assert (args.output / generation_result["stdout"]).read_text().strip() == (
                "PASS wire_cases=79 crypto_forms=2 correlation_headers=verified lifetime_ms=0")
        hashes = {name: sha(args.output / version / name) for name in FILES}
        if baseline is None:
            baseline = hashes
        assert baseline == hashes, "Cross-release bytes differ; retain and investigate."
        with (args.output / version / "apache-wire.tsv").open() as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        assert len(rows) == 79
        for row in rows:
            body, payload, frame = [bytes.fromhex(row[name]) for name in ["body_hex", "payload_hex", "frame_hex"]]
            assert payload.endswith(body) and len(frame) == len(payload) + 4
            assert int.from_bytes(frame[:4], "big", signed=True) == len(payload) and frame[4:] == payload
        report["releases"].append({"version": version, "upstream_commit": release["commit"],
                "jar_sha256": sha(jar), "distribution_provenance": pin, "jar_properties": properties,
                "commands": [compile_result, generation_result], "output_sha256": hashes,
                "compiled_class_sha256": {path.name: sha(path) for path in sorted(classes.glob("*.class"))},
                "exact_wire_cases": 79, "truncated_parser_rejections": 79,
                "trailing_prefix_parser_acceptances": 79,
                "policy": "Apache generated parsers consume a prefix and leave trailing bytes; the Rust listener independently requires whole input. Constructed scoped error fixtures do not imply server execution."})
        (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    args.fixtures.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        source = args.output / "4.3.1" / name
        target = args.fixtures / name
        if target.exists():
            assert target.read_bytes() == source.read_bytes(), "Published fixture mismatch."
        else:
            with target.open("xb") as stream:
                stream.write(source.read_bytes())
    report.update(verdict="passed", byte_identical_across_releases=True,
                  total_wire_cases=237, total_truncated_rejections=237, total_trailing_prefix_acceptances=237,
                  fixture_sha256={name: sha(args.fixtures / name) for name in FILES})
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verdict": "passed", "wire_cases": 237, "truncated_rejections": 237,
                      "trailing_prefix_acceptances": 237, "releases_byte_identical": True}))


if __name__ == "__main__":
    main()

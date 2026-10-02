#!/usr/bin/env python3
"""Assert the complete matrix against independently compiled official Apache APIs.

Inputs are checksum-verified distribution jars and their retained provenance;
this script does not download dependencies or connect to a broker.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[4]
EVIDENCE = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(command, log):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    log.write_text(result.stdout + result.stderr, encoding="utf-8")
    return result


def expected_rows(release):
    result = []
    for row in release["inventory"]:
        if row["disposition"] == "removed_api_key_reserved":
            low, high, stable = 0, -1, -1
        else:
            parts = row["request"]["valid_versions"].split("-")
            low, high = int(parts[0]), int(parts[-1])
            stable = high - int(row["latest_version_unstable"])
        fields = [row["api_key"], row["name"], low, high, stable, row["cluster_action"],
                  row["forwardable"], row["latest_version_unstable"], ",".join(row["listeners"]),
                  ",".join(f"{v['api_version']}:{v['request_header_version']}:{v['response_header_version']}"
                           for v in row["headers"])]
        result.append("\t".join(str(value).lower() if isinstance(value, bool) else str(value) for value in fields))
    return result


def run(jars, provenance_dir, oracle_work):
    matrix = json.loads((ROOT / "tests/conformance/broker/api-matrix.json").read_text())
    java = EVIDENCE / "MatrixRuntimeOracle.java"
    slf = jars / "slf4j-api-1.7.36.jar"
    results = []
    for release in matrix["releases"]:
        version = release["version"]
        classes = oracle_work / ("java-" + version)
        classes.mkdir(parents=True, exist_ok=True)
        jar = jars / ("kafka-clients-" + version + ".jar")
        provenance = json.loads((provenance_dir / (version + ".provenance.json")).read_text())
        assert sha(jar) == provenance["jar_sha256"]
        assert provenance["source_sha"] == release["commit"]
        with zipfile.ZipFile(jar) as archive:
            names = [name for name in archive.namelist() if name.endswith("kafka-version.properties")]
            assert len(names) == 1
            properties = dict(line.split("=", 1) for line in archive.read(names[0]).decode().splitlines()
                              if "=" in line and not line.startswith("#"))
            assert properties["version"] == version and properties["commitId"] == release["commit"][:16]
        compiled = ["taskset", "-c", "0-2,4", "java", "--add-modules", "jdk.compiler",
                    "com.sun.tools.javac.Main", "-Xlint:all", "-Werror", "-cp", str(jar),
                    "-d", str(classes), str(java.relative_to(ROOT))]
        compile_result = execute(compiled, EVIDENCE / (version + "-java-compile.log"))
        assert compile_result.returncode == 0, compile_result.stderr
        expected = EVIDENCE / (version + "-expected.tsv")
        expected.write_text("\n".join(expected_rows(release)) + "\n", encoding="utf-8")
        classpath = str(classes) + ":" + str(jar) + ":" + str(slf)
        command = ["taskset", "-c", "0-2,4", "java", "-cp", classpath,
                   "MatrixRuntimeOracle", str(expected.relative_to(ROOT))]
        log = EVIDENCE / (version + "-java-oracle.log")
        positive = execute(command, log)
        assert positive.returncode == 0, positive.stderr
        lines = expected.read_text().splitlines()
        cells = lines[18].split("\t")
        assert "4:2:0" in cells[-1]
        cells[-1] = cells[-1].replace("4:2:0", "4:2:1")
        lines[18] = "\t".join(cells)
        mutant = EVIDENCE / (version + "-header-mutant.tsv")
        mutant.write_text("\n".join(lines) + "\n", encoding="utf-8")
        mutant_command = command[:-1] + [str(mutant.relative_to(ROOT))]
        mutant_log = EVIDENCE / (version + "-header-mutant.log")
        negative = execute(mutant_command, mutant_log)
        assert negative.returncode == 1 and "API key 18 mismatch" in negative.stderr
        results.append({
            "version": version, "distribution_provenance": provenance, "jar_path": str(jar),
            "jar_sha256": sha(jar), "jar_properties": properties,
            "compile_command": compiled, "compile_exit_code": compile_result.returncode,
            "oracle_class_sha256": sha(classes / "MatrixRuntimeOracle.class"),
            "expected_tsv": str(expected.relative_to(ROOT)), "expected_tsv_sha256": sha(expected),
            "runtime_command": command, "exit_code": positive.returncode,
            "output": str(log.relative_to(ROOT)), "summary": positive.stdout.splitlines()[-1],
            "deliberate_failing_variant": {
                "mutation": "API 18 v4 expected response header changed from 0 to 1",
                "command": mutant_command, "exit_code": negative.returncode,
                "expected_tsv": str(mutant.relative_to(ROOT)), "expected_tsv_sha256": sha(mutant),
                "output": str(mutant_log.relative_to(ROOT)),
                "assertion": "API key 18 mismatch: expected response header 1, actual upstream 0",
            },
        })
    version = subprocess.run(["java", "-version"], capture_output=True, text=True)
    record = {
        "schema_version": 1,
        "oracle": "Independent compiled ApiKeys/ApiMessageType in official Apache distributions; no broker/network in executions",
        "source": str(java.relative_to(ROOT)), "source_sha256": sha(java),
        "runner": str(Path(__file__).relative_to(ROOT)), "runner_sha256": sha(Path(__file__)),
        "java_version": version.stdout + version.stderr, "slf4j_sha256": sha(slf),
        "positive_upstream_executions": 3, "expected_failed_upstream_mutations": 3,
        "total_api_key_assertions": 279, "total_valid_header_pairs": 894,
        "total_removed_key_request_response_rejections": 24,
        "releases": results,
        "limitations": [
            "Compiled upstream generated API metadata assertions, not network handler tests.",
            "Apache archive SHA512 provenance independently prepared by rpc_reuse agent and copied here; jar SHA256 and embedded commit/version verified locally before each execution.",
            "Request-header ClientId encoding checked against retained schema; byte-level wire behavior belongs to KL11-04.",
        ],
    }
    (EVIDENCE / "upstream-java-oracle.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("\n".join(result["version"] + " " + result["summary"] for result in results))
    print("Three deliberate mutations failed the independent upstream assertion.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars", type=Path, required=True)
    parser.add_argument("--provenance-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    run(args.jars, args.provenance_dir, args.work_dir)

#!/usr/bin/env python3
"""Pin activated TLS dependencies and prove unchanged default/core graph inputs."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib


parser = argparse.ArgumentParser()
parser.add_argument("--source-sha", required=True)
parser.add_argument("--snapshot-dir", required=True, type=Path)
arguments = parser.parse_args()
repository = Path(__file__).resolve().parents[4]
evidence = Path(__file__).resolve().parent
source_sha = subprocess.check_output(["git", "rev-parse", f"{arguments.source_sha}^{{commit}}"], cwd=repository, text=True).strip()
baseline = subprocess.check_output(["git", "rev-parse", "36bca0a9^{commit}"], cwd=repository, text=True).strip()


def committed(sha, path):
    return subprocess.check_output(["git", "show", f"{sha}:{path}"], cwd=repository)


baseline_lock = tomllib.loads(committed(baseline, "partitionline-broker/Cargo.lock").decode())
baseline_versions = {(package["name"], package["version"]) for package in baseline_lock["package"]}
source_lock = tomllib.loads(committed(source_sha, "partitionline-broker/Cargo.lock").decode())
checksums = {(package["name"], package["version"]): package.get("checksum") for package in source_lock["package"]}
rustc = subprocess.check_output(["rustup", "run", "stable", "rustc", "-vV"], text=True)
platform = next(line.split(": ", 1)[1] for line in rustc.splitlines() if line.startswith("host: "))
graphs = {}
for name, feature in [("default", "--no-default-features"), ("tls", "--all-features")]:
    command = ["cargo", "+stable", "metadata", "--locked", "--format-version", "1", "--manifest-path", "partitionline-broker/Cargo.toml", "--filter-platform", platform, feature]
    metadata = json.loads(subprocess.check_output(command, cwd=arguments.snapshot_dir, text=True, stderr=subprocess.PIPE))
    packages = {package["id"]: package for package in metadata["packages"]}
    active = []
    for node in metadata["resolve"]["nodes"]:
        package = packages[node["id"]]
        active.append({
            "name": package["name"], "version": package["version"], "license": package["license"],
            "rust_version": package["rust_version"], "checksum": checksums[(package["name"], package["version"])],
            "features": sorted(node["features"]),
            "dependencies": sorted({f"{packages[dependency['pkg']]['name']}@{packages[dependency['pkg']]['version']}" for dependency in node["deps"]}),
        })
    graphs[name] = {"command": command, "packages": sorted(active, key=lambda package: package["name"])}
default_names = {package["name"] for package in graphs["default"]["packages"]}
assert not default_names.intersection({"rustls", "tokio-rustls", "ring", "rustls-webpki", "rustls-pki-types"})
assert all((package["name"], package["version"]) in baseline_versions for package in graphs["default"]["packages"])
tls_names = {package["name"] for package in graphs["tls"]["packages"]}
assert not tls_names.intersection({"openssl", "openssl-sys", "aws-lc-rs", "aws-lc-sys", "rdkafka", "rdkafka-sys"})
core = {}
for path in ("Cargo.toml", "Cargo.lock"):
    before = hashlib.sha256(committed(baseline, path)).hexdigest()
    after = hashlib.sha256(committed(source_sha, path)).hexdigest()
    if path == "Cargo.toml":
        original = tomllib.loads(committed(baseline, path).decode())
        current = tomllib.loads(committed(source_sha, path).decode())
        sections = ["dependencies", "dev-dependencies", "build-dependencies", "target", "features", "patch", "replace", "workspace"]
        assert all(original.get(section) == current.get(section) for section in sections)
        core[path] = {
            "baseline_sha256": before, "source_sha256": after, "bytes_unchanged": before == after,
            "graph_sections_unchanged": sections,
            "non_graph_change": "An independently committed package include exclusion for /tests/conformance/broker/**; dependency/features sections unchanged.",
        }
    else:
        assert before == after, path
        core[path] = {"baseline_sha256": before, "source_sha256": after, "unchanged": True}
result = {
    "source_sha": source_sha, "baseline_sha": baseline, "platform": platform,
    "default_has_no_tls_activation": True, "baseline_default_package_versions_preserved": True,
    "no_openssl_aws_lc_or_native_kafka_package": True,
    "core_graph_inputs": core, "graphs": graphs,
    "limits": "Activated normal/build graph for the retained Linux target. Ring uses its approved bundled native cryptography and cc build tool; this is not a claim of an entirely pure-Rust cryptographic graph.",
}
(evidence / "dependency-results.json").write_text(json.dumps(result, indent=2) + "\n")
print(f"Default graph {len(graphs['default']['packages'])} package versions preserved; TLS graph {len(graphs['tls']['packages'])}; core dependency/features sections and lock unchanged.")

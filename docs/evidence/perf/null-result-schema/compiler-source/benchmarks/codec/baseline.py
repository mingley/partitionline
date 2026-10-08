#!/usr/bin/env python3
"""Assemble benchmarks/codec/baseline.json (KL04-09).

Stdlib only. Merges three verified inputs into one checked-in record:

1. `codec-preflight baseline` stdout (fixtures, census rows, slowcheck;
   fails closed before any JSON is emitted).
2. criterion `new/estimates.json` per bench (26 expected ids).
3. `fixtures/manifest.json` pins, re-hashed from disk (fail closed).

Writes baseline.json plus a `baseline.sha256` sidecar. All paths
default relative to this script (the codec crate root).

Usage:
  cargo bench --offline            # refresh criterion new/ estimates
  cargo build --offline --bin codec-preflight
  python3 benchmarks/codec/baseline.py
"""

import hashlib
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

SHAPES = ("f01", "f02", "f03", "f04")
CODECS = ("gzip", "snappy", "lz4")
ENTROPIES = ("random", "text")

EXPECTED = (
    [("encode", s) for s in SHAPES]
    + [("decode", s) for s in SHAPES]
    + [("crc32c", s) for s in SHAPES]
    + [("compress", f"{c}_{e}") for c in CODECS for e in ENTROPIES]
    + [("decompress", f"{c}_{e}") for c in CODECS for e in ENTROPIES]
    + [("transform", "fast"), ("transform", "slow")]
)


def fail(msg):
    print(f"baseline.py: {msg}", file=sys.stderr)
    sys.exit(1)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def rustc_info():
    try:
        out = subprocess.run(
            ["rustc", "-vV"], capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        fail(f"rustc -vV failed: {e}")
    info = {}
    for line in out.splitlines():
        if " " in line:
            k, v = line.split(" ", 1)
            info[k.rstrip(":")] = v
    return info


def preflight_doc(binary, fixtures_dir, rustc_version):
    if not os.path.isfile(binary):
        fail(f"preflight binary missing: {binary} (cargo build --bin codec-preflight)")
    env = dict(os.environ)
    env["RUSTC_VERSION"] = rustc_version
    try:
        proc = subprocess.run(
            [binary, "baseline", "--fixtures", fixtures_dir],
            capture_output=True,
            text=True,
            env=env,
        )
    except OSError as e:
        fail(f"preflight exec failed: {e}")
    if proc.returncode != 0:
        fail(f"preflight baseline failed:\n{proc.stderr.strip()}")
    start = proc.stdout.find("{")
    if start < 0:
        fail("preflight baseline emitted no JSON doc")
    try:
        return json.loads(proc.stdout[start:])
    except json.JSONDecodeError as e:
        fail(f"preflight JSON unparseable: {e}")


def bench_table(criterion_dir):
    rows = []
    for group, bench in EXPECTED:
        path = os.path.join(criterion_dir, group, bench, "new", "estimates.json")
        if not os.path.isfile(path):
            fail(f"missing estimates: {group}/{bench} (run cargo bench first)")
        with open(path) as f:
            est = json.load(f)
        try:
            row = {
                "id": f"{group}/{bench}",
                "ns_mean": est["mean"]["point_estimate"],
                "ns_mean_lo": est["mean"]["confidence_interval"]["lower_bound"],
                "ns_mean_hi": est["mean"]["confidence_interval"]["upper_bound"],
                "ns_median": est["median"]["point_estimate"],
                "ns_stddev": est["std_dev"]["point_estimate"],
            }
        except KeyError as e:
            fail(f"estimates schema drift at {group}/{bench}: missing {e}")
        rows.append(row)
    return rows


def fixture_pins(fixtures_dir):
    manifest_path = os.path.join(fixtures_dir, "manifest.json")
    if not os.path.isfile(manifest_path):
        fail(f"missing {manifest_path}")
    with open(manifest_path) as f:
        manifest = json.load(f)
    pins = []
    for name in sorted(manifest.get("fixtures", {})):
        entry = manifest["fixtures"][name]
        disk = os.path.join(fixtures_dir, entry["file"])
        actual = sha256_file(disk) if os.path.isfile(disk) else "MISSING"
        if actual != entry["sha256"]:
            fail(f"fixture {name} sha mismatch: disk {actual} != manifest {entry['sha256']}")
        pins.append(
            {
                "name": name,
                "file": entry["file"],
                "sha256": actual,
                "bytes": entry["bytes"],
                "records": entry["records"],
            }
        )
    if not pins:
        fail("manifest lists no fixtures")
    return pins, manifest.get("seed")


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Assemble codec baseline.json + .sha256.")
    ap.add_argument("--criterion-dir", default=os.path.join(HERE, "target", "criterion"))
    ap.add_argument("--fixtures", default=os.path.join(HERE, "fixtures"))
    ap.add_argument(
        "--preflight", default=os.path.join(HERE, "target", "debug", "codec-preflight")
    )
    ap.add_argument("--out", default=os.path.join(HERE, "baseline.json"))
    args = ap.parse_args()

    info = rustc_info()
    rustc_version = info.get("release", info.get("rustc", "unknown"))
    pre = preflight_doc(args.preflight, args.fixtures, rustc_version)
    pins, seed = fixture_pins(args.fixtures)
    benches = bench_table(args.criterion_dir)

    doc = {
        "tool": "baseline.py",
        "git_sha": pre.get("git_sha", "unknown"),
        "rustc": rustc_version,
        "rustc_host": info.get("host", "unknown"),
        "os": pre.get("os", "unknown"),
        "arch": pre.get("arch", "unknown"),
        "profiles": {
            "criterion": "bench",
            "census": "release" if "release" in args.preflight else "dev",
        },
        "seed": seed,
        "inputs": pins,
        "benches": benches,
        "census": pre.get("census", []),
        "slowcheck_ratio": pre.get("slowcheck_ratio"),
    }
    text = json.dumps(doc, indent=1, sort_keys=True) + "\n"
    with open(args.out, "w") as f:
        f.write(text)
    digest = hashlib.sha256(text.encode()).hexdigest()
    with open(args.out + ".sha256", "w") as f:
        f.write(f"{digest}  {os.path.basename(args.out)}\n")
    print(
        f"baseline: {len(benches)} benches, {len(pins)} fixtures, "
        f"slowcheck={doc['slowcheck_ratio']:.2f}x -> {args.out}"
    )


if __name__ == "__main__":
    main()

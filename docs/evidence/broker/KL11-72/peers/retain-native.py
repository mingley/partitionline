#!/usr/bin/env python3
"""Retain exact external native peer source references without rebuilding it."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

COMMIT = "9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab"
TREE = "429a83d1ba5350c0846aeaee4efe821b463f6b85"
LIBRARY = "8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc"
WANTED = ["LICENSE", "src/rdkafka.h", "src/rdkafka_sasl_scram.c", "src/rdkafka_sasl_plain.c",
          "src/rdkafka_admin.c", "src/rdkafka_request.c"]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; pins are asserted.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()

    def git(*arguments):
        return subprocess.run(["git", "-C", str(args.source), *arguments],
                              capture_output=True, check=True).stdout

    assert git("rev-parse", "HEAD").decode().strip() == COMMIT
    assert git("rev-parse", "HEAD^{tree}").decode().strip() == TREE
    assert not git("status", "--porcelain", "--untracked-files=no")
    assert sha(args.library.read_bytes()) == LIBRARY
    pins = {}
    for name in WANTED:
        raw = git("show", COMMIT + ":" + name)
        assert 0 < len(raw) <= 512 * 1024
        assert (args.source / name).read_bytes() == raw
        target = args.output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        pins[name] = sha(raw)
    report = {"repository": "https://github.com/confluentinc/librdkafka.git", "tag": "v2.15.0",
              "commit": COMMIT, "tree": TREE, "library_sha256": LIBRARY, "source_sha256": pins,
              "scope": "Reuse the independently pinned OpenSSL-enabled native library; retain SASL/admin/serializer references, no rebuild and no broker production dependency."}
    (args.output / "pins.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verdict": "passed", "files": len(WANTED), "library_pin_verified": True}))


if __name__ == "__main__":
    main()

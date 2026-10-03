#!/usr/bin/env python3
"""Verify every frozen runtime source, fixture and peer input by exact bytes."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[5])
    parser.add_argument("--manifest", type=Path,
                        default=Path("docs/evidence/broker/KL11-10/runtime/source-freeze.json"))
    args = parser.parse_args()
    manifest = args.manifest if args.manifest.is_absolute() else args.root / args.manifest
    data = json.loads(manifest.read_text())
    assert data["schema_version"] == 1
    inputs = data["inputs"]
    assert 1 <= len(inputs) <= 4096
    declared = set()
    for item in inputs:
        relative = Path(item["path"])
        assert not relative.is_absolute() and ".." not in relative.parts
        assert item["path"] not in declared
        declared.add(item["path"])
        path = args.root / relative
        assert path.is_file() and not path.is_symlink(), item["path"]
        contents = path.read_bytes()
        assert len(contents) == item["bytes"], item["path"]
        assert hashlib.sha256(contents).hexdigest() == item["sha256"], item["path"]
    fixture_root = args.root / "partitionline-broker/tests/fixtures/retention"
    actual = {str(p.relative_to(args.root)) for p in fixture_root.rglob("*") if p.is_file()}
    expected = {name for name in declared if name.startswith("partitionline-broker/tests/fixtures/retention/")}
    assert actual == expected
    print(json.dumps({"passed": True, "checked_inputs": len(inputs),
                      "fixture_files": len(expected),
                      "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()

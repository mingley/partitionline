#!/usr/bin/env python3
"""Verify the committed public client and standalone inputs before/after builds."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    rows, total = [], 0
    for row in manifest["files"]:
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Scoped committed source path required")
        path = args.source_root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError("Regular committed source input required: " + row["path"])
        data = path.read_bytes()
        blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        digest = hashlib.sha256(data).hexdigest()
        if len(data) != row["bytes"] or blob != row["git_blob"] or digest != row["sha256"] or path.stat().st_mode & 0o777 != row["mode"]:
            raise ValueError("Committed public client/peer input changed: " + row["path"])
        rows.append({"path": row["path"], "sha256": digest, "git_blob": blob})
        total += len(data)
    if total != manifest["input_bytes"] or len(rows) != manifest["input_files"]:
        raise ValueError("Committed input count/byte mismatch")
    result = {"schema_version": 1, "source_sha": manifest["source_sha"], "passed": True,
              "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
              "input_files": len(rows), "input_bytes": total,
              "scope": "Exact committed Rust-client/peer compile inputs and modes; separate full broker Git archive proof remains required.",
              "source_map_sha256": hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(",",":")).encode()).hexdigest()}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result),flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Rebuild the byte-preserving protocol subset from checksum-pinned source tarballs.

Writes to a caller-selected output directory, never downloads or changes features.
"""
import argparse
import gzip
import importlib.util
import io
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[4]
MODULE_SPEC = importlib.util.spec_from_file_location("broker_matrix", ROOT / "scripts/check-broker-api-matrix.py")
MATRIX = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(MATRIX)


def canonical_archive(sources):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, content in sorted(sources.items()):
            member = tarfile.TarInfo(name)
            member.size = len(content)
            member.mode = 0o644
            member.mtime = member.uid = member.gid = 0
            member.uname = member.gname = ""
            archive.addfile(member, io.BytesIO(content))
    compressed = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed, filename="", mode="wb", compresslevel=9, mtime=0) as stream:
        stream.write(raw.getvalue())
    return compressed.getvalue()


def retain(original_dir, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    releases = []
    for version, (commit, original_sha) in MATRIX.TARGETS.items():
        original = original_dir / (version + ".tar.gz")
        MATRIX.require(original.stat().st_size <= 32 * 1024 * 1024, "original source archive too large")
        content = original.read_bytes()
        MATRIX.require(MATRIX.digest(content) == original_sha, f"{version}: original archive checksum mismatch")
        prefix = "kafka-" + commit + "/"
        sources = {}
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            for member in archive:
                MATRIX.require(member.name.startswith(prefix) or member.name == prefix[:-1], "source root/commit mismatch")
                relative = member.name[len(prefix):]
                selected = relative in MATRIX.REQUIRED_FILES or (
                    relative.startswith(MATRIX.MESSAGE_DIR) and relative.endswith(".json") and
                    "/" not in relative[len(MATRIX.MESSAGE_DIR):])
                if not selected:
                    continue
                MATRIX.require(member.type == tarfile.REGTYPE,
                               "selected source is not a regular file")
                MATRIX.require(relative not in sources, "duplicate selected source")
                MATRIX.require(0 <= member.size <= MATRIX.MAX_MEMBER_BYTES, "selected source too large")
                sources[relative] = archive.extractfile(member).read()
        MATRIX.require(MATRIX.REQUIRED_FILES <= sources.keys(), "required source missing")
        inventory, header_rules = MATRIX.derive_inventory(sources)
        filename = f"apache-kafka-{version}-protocol.tar.gz"
        retained = canonical_archive(sources)
        (output_dir / filename).write_bytes(retained)
        releases.append({
            "version": version, "tag": version, "commit": commit,
            "source_archive_url": f"https://codeload.github.com/apache/kafka/tar.gz/{commit}",
            "source_archive_sha256": original_sha,
            "retained_archive": filename, "retained_archive_sha256": MATRIX.digest(retained),
            "files_sha256": {path: MATRIX.digest(data) for path, data in sorted(sources.items())},
            "header_rules": header_rules, "inventory": inventory,
        })
    matrix = {"schema_version": 1, "scope": MATRIX.SCOPE, "implementation_claim": False, "releases": releases}
    (output_dir / "api-matrix.json").write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"original_archives_verified": len(releases), "releases": [{
        "version": release["version"], "commit": release["commit"],
        "retained_archive_sha256": release["retained_archive_sha256"],
        "retained_files": len(release["files_sha256"]), "api_keys": len(release["inventory"]),
    } for release in releases]}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    retain(arguments.original_dir, arguments.output_dir)

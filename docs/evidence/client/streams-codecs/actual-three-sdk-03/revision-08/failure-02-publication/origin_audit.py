"""Complete observed source audits anchored to the frozen coordinator origin receipt."""
from __future__ import annotations
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat

PIN = "04f6bc2968c1d721c6815a6389897a62e4ca76f1"
ORIGIN_SHA = "5abaab9053b7b81d03a3c6ac161c4188f1f7a6ee909c71ecc69914224ed4ce29"
MANIFEST_SHA = "f5179aac9e296b356b80b3ae3e053ff836bc5d084593d21fd01678513dde2b5c"
GZIP_SHA = "e3e549aae650a679e50b4989f99022f4706abd7ef0c3ef18c237c1c101f63129"
FILES = 73938
BYTES = 713363027
MAX_FILES = 100000
MAX_AUDIT_BYTES = 23380322 + 1024 * 1024
MAX_DIRECTORY_BYTES = 1628041 + 512 * 1024
MAX_SOURCE_BYTES = BYTES + 32 * 1024 * 1024


def sha(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            value.update(block)
    return value.hexdigest()


def compact(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def expected_directories(names) -> set[str]:
    result = {"."}
    for name in names:
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or str(path) != name:
            raise ValueError("invalid origin source path")
        result.update(str(parent) for parent in path.parents)
    return result


def tree(source: Path) -> tuple[set[str], set[str]]:
    files = set()
    directories = {"."}
    for root, dirs, names in os.walk(source, followlinks=False):
        for name in dirs:
            path = Path(root) / name
            relative = str(path.relative_to(source))
            if path.is_symlink():
                files.add(relative)
            else:
                directories.add(relative)
        files.update(str((Path(root) / name).relative_to(source)) for name in names)
        if len(files) > MAX_FILES or len(directories) > MAX_FILES:
            raise ValueError("observed source path count exceeds explicit100000bound")
    return files, directories


class Origin:
    def __init__(self, path: Path, source: Path, git_entries: dict):
        if sha(path) != ORIGIN_SHA:
            raise ValueError("coordinator origin receipt SHA differs")
        self.path = path
        self.receipt = json.loads(path.read_bytes())
        if self.receipt["source_commit"] != PIN or self.receipt["verified_files"] != FILES or self.receipt["verified_bytes"] != BYTES:
            raise ValueError("actual full origin pin/count/bytes differs")
        if source.resolve() != Path(self.receipt["source_directory"]).resolve():
            raise ValueError("source directory differs from exact origin")
        manifest = self.receipt["source_manifest"]
        self.raw_path = Path(manifest["uncompressed_retained_path"])
        self.gzip_path = Path(manifest["path"])
        if sha(self.raw_path) != MANIFEST_SHA or sha(self.gzip_path) != GZIP_SHA:
            raise ValueError("coordinator complete source map identity differs")
        self.raw = self.raw_path.read_bytes()
        self.rows = json.loads(self.raw)
        if len(self.raw) != 23380322 or len(self.rows) != FILES or compact(self.rows) != self.raw:
            raise ValueError("actual compact full map shape/serialization differs")
        with gzip.open(self.gzip_path, "rb") as handle:
            restored = handle.read(MAX_AUDIT_BYTES + 1)
        if restored != self.raw:
            raise ValueError("origin gzip does not restore exact full raw bytes")
        self.input_modes = {str(p): stat.S_IMODE(p.stat().st_mode) for p in (path, self.raw_path, self.gzip_path)}
        if any(mode != 0o600 for mode in self.input_modes.values()):
            raise ValueError("origin complete receipt/maps full07777mode differs from0600")
        if set(git_entries) != set(self.rows):
            raise ValueError("complete origin and exact Git path sets differ")
        for name, row in self.rows.items():
            git_mode, git_blob = git_entries[name]
            if git_mode != row["mode"] or git_blob != row["git_blob_sha1"]:
                raise ValueError("complete origin and exact Git objects/modes differ")
            expected_mode = {"100644": 0o600, "100755": 0o700}.get(git_mode)
            if expected_mode is None or row["full_permission_mode"] != expected_mode:
                raise ValueError("origin full owner0600/0700mode differs")
        self.source = source
        self.git_entries = git_entries
        self.root_uid = source.stat().st_uid
        self.root_gid = source.stat().st_gid

    def unchanged_origin(self) -> None:
        for path, mode in self.input_modes.items():
            p = Path(path)
            if stat.S_IMODE(p.stat().st_mode) != mode:
                raise ValueError("origin full permission mode changed")
        if sha(self.path) != ORIGIN_SHA or sha(self.raw_path) != MANIFEST_SHA or sha(self.gzip_path) != GZIP_SHA:
            raise ValueError("origin receipt/raw/gzip bytes changed")


def observe(source: Path, rows: dict, uid: int, gid: int) -> tuple[dict, dict, dict]:
    """Observe every bounded file and directory even when an individual identity is wrong."""
    actual_files, actual_dirs = tree(source)
    required_dirs = expected_directories(rows)
    errors = []
    observed = {}
    directories = {}
    total_bytes = 0
    identity_checks = 0
    for name in sorted(actual_files | set(rows)):
        expected = rows.get(name)
        path = source / name
        if name not in actual_files:
            observed[name] = {"bytes": None, "full_permission_mode": None, "git_blob_sha1": None,
                              "mode": expected["mode"], "sha256": None}
            errors.append((name, "missing"))
            continue
        info = path.lstat()
        full_mode = stat.S_IMODE(info.st_mode)
        if not stat.S_ISREG(info.st_mode):
            observed[name] = {"bytes": info.st_size, "full_permission_mode": full_mode,
                              "git_blob_sha1": None, "mode": expected["mode"] if expected else None,
                              "sha256": None}
            errors.append((name, "nonregular"))
            continue
        total_bytes += info.st_size
        if total_bytes > MAX_SOURCE_BYTES:
            raise ValueError("source byte sum exceeds bounded audit admission")
        blob = hashlib.sha1(f"blob {info.st_size}\0".encode())
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(65536), b""):
                blob.update(block)
                digest.update(block)
        row = {"bytes": info.st_size, "full_permission_mode": full_mode,
               "git_blob_sha1": blob.hexdigest(), "mode": expected["mode"] if expected else None,
               "sha256": digest.hexdigest()}
        observed[name] = row
        if expected != row or info.st_uid != uid or info.st_gid != gid:
            errors.append((name, "bytes/Git/SHA/full07777mode/owner differ"))
        identity_checks += 1
    for name in sorted(actual_dirs | required_dirs):
        path = source if name == "." else source / name
        if name not in actual_dirs:
            row = {"full_permission_mode": None, "uid": None, "gid": None}
        else:
            info = path.lstat()
            row = {"full_permission_mode": stat.S_IMODE(info.st_mode), "uid": info.st_uid, "gid": info.st_gid}
        directories[name] = row
        if name not in required_dirs or row != {"full_permission_mode": 0o700, "uid": uid, "gid": gid}:
            errors.append((name, "directory path/full07777mode/owner differ"))
    if actual_files != set(rows):
        errors.append((".", "complete file path set differs"))
    if actual_dirs != required_dirs:
        errors.append((".", "complete directory path set differs"))
    return observed, directories, {"complete": True, "file_identity_checks": identity_checks,
             "observed_files": len(actual_files), "expected_files": len(rows),
             "directory_identity_checks": len(directories), "observed_file_bytes": total_bytes,
             "passed": not errors, "error_count": len(errors), "first16errors": [(name[:512], error) for name, error in errors[:16]],
             "expected_owner_uid": uid, "expected_owner_gid": gid, "file_owner_checks": identity_checks}


def write_owned(path: Path, data: bytes, guard, maximum: int) -> None:
    if path.exists():
        raise ValueError("fresh immutable audit artifact path required")
    if len(data) > maximum:
        raise ValueError("compact raw audit exceeds reserved byte bound")
    guard(len(data))
    with path.open("xb") as handle:
        handle.write(data)
    path.chmod(0o600)


def audit(origin: Origin, output: Path, phase: str, guard) -> dict:
    origin.unchanged_origin()
    observed, directories, receipt = observe(origin.source, origin.rows, origin.root_uid, origin.root_gid)
    raw = compact(observed)
    directory_raw = compact(directories)
    raw_path = output / f"source-{phase}.json"
    dirs_path = output / f"source-directories-{phase}.json"
    linked = []
    for path, data, maximum, before_name in [
            (raw_path, raw, MAX_AUDIT_BYTES, "source-before.json"),
            (dirs_path, directory_raw, MAX_DIRECTORY_BYTES, "source-directories-before.json")]:
        before = output / before_name
        if phase == "after" and before.exists() and before.read_bytes() == data:
            guard(4096)
            os.link(before, path)
            linked.append(path.name)
        else:
            write_owned(path, data, guard, maximum)
    # Only the independently byte-verified origin gzip is copied; never recompress
    # altered maps, so every failure retains both complete raw observed maps.
    gzip_path = output / f"source-{phase}.json.gz"
    if raw == origin.raw:
        if phase == "after" and (output / "source-before.json.gz").exists():
            guard(4096)
            os.link(output / "source-before.json.gz", gzip_path)
            linked.append(gzip_path.name)
        else:
            write_owned(gzip_path, origin.gzip_path.read_bytes(), guard, 2630489)
    origin.unchanged_origin()
    receipt.update(source_sha=PIN, origin_receipt_sha256=ORIGIN_SHA,
                   full_raw={"path": raw_path.name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "mode": "0600"},
                   full_directory_raw={"path": dirs_path.name, "bytes": len(directory_raw), "sha256": hashlib.sha256(directory_raw).hexdigest(), "mode": "0600"},
                   exact_byte_verified_hardlinks=linked,
                   file_map_equals_verified_origin=raw == origin.raw)
    write_owned(output / f"source-{phase}-receipt.json", compact(receipt), guard, 64 * 1024)
    return receipt

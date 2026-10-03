#!/usr/bin/env python3
"""Source-prepared bounded actual-compiler/SDK fixture runner; never fabricates vectors."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import time
import sys

sys.dont_write_bytecode = True
from origin_audit import Origin, audit, compact, MAX_AUDIT_BYTES, MAX_DIRECTORY_BYTES
import tarfile
import zipfile

PIN = "04f6bc2968c1d721c6815a6389897a62e4ca76f1"
ORACLE = "tests/fixtures/streams/oracle/StreamsWireOracle.java"
ORACLE_SHA = "7f81f6eeb83938ed6b5411e17b2d308ae9153feabf4bbcc50d13cc16c4557f2d"
OVERLAY_ORACLE = Path("/workspace/work/streams-codecs/revision-06/StreamsWireOracle.java")
OVERLAY_ORACLE_SHA = "848047c5fb87ca57e3d19dd76c575f3b2edbe86ff3ac1d378daca13f1206110a"
OVERLAY_ORACLE_BYTES = 56478
JARS = {
    "4.1.2": "33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed",
    "4.2.1": "9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159",
    "4.3.1": "dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e",
}
SLF4J_SHA = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
FLOOR = 350 * 1024 * 1024
MAX_OUTPUT = 96 * 1024 * 1024
MAX_STREAM = 64 * 1024
MAX_PROCESS_SECONDS = 60
MAX_FIXTURE_BYTES = 1024 * 1024
MAX_FILES = 2048
MAX_CLASS_BYTES = 2 * 1024 * 1024
MAX_FIXTURES_BYTES = 8 * 1024 * 1024
MAX_METADATA_BYTES = 2 * 1024 * 1024
ORIGIN_RECEIPT = Path("/workspace/work/integration/client-capabilities-source-04f6bc29/receipt.json")


def digest(path: Path, algorithm="sha256") -> str:
    result = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for data in iter(lambda: handle.read(65536), b""):
            result.update(data)
    return result.hexdigest()


def identity(path: Path) -> dict:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("input must be a regular file")
    return {"path": str(path), "size": info.st_size, "sha256": digest(path),
            "mode": f"{stat.S_IMODE(info.st_mode):04o}"}


def compiler_oracle(path: Path, snapshot: Path) -> tuple[Path, dict]:
    # One reviewed standalone compiler input only. Product source remains the
    # complete unchanged04f snapshot, including its original7f81 oracle.
    if not path.is_absolute() or path != OVERLAY_ORACLE:
        raise ValueError("explicit oracle must match pinned absolute WORK path")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o644:
        raise ValueError("standalone oracle must be regular full0644 file")
    if info.st_size != OVERLAY_ORACLE_BYTES or digest(path) != OVERLAY_ORACLE_SHA:
        raise ValueError("standalone oracle byte/hash pin differs")
    original = snapshot / ORACLE
    if digest(original) != ORACLE_SHA:
        raise ValueError("complete snapshot original oracle differs from7f81 pin")
    return path, {
        "scope": "Sole standalone compiler-input overlay; no product-source overlay or wider main-source claim",
        "complete_product_source_sha": PIN,
        "complete_source_original_oracle": identity(original),
        "standalone_compiler_oracle": identity(path),
        "changed_caller_lines": [38, 527],
        "actual_serialization_implementation": "Pinned genuine SDK MessageUtil.toByteBufferAccessor(...).buffer()",
    }


def git(repo: Path, *arguments: str) -> bytes:
    result = subprocess.run(["git", "-C", str(repo), *arguments], check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if len(result.stdout) > 16 * 1024 * 1024:
        raise ValueError("Git source inventory exceeds bound")
    return result.stdout


def git_entries(repo: Path) -> dict:
    inventory = git(repo, "ls-tree", "-r", "-z", PIN)
    entries = {}
    for entry in inventory.split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, kind, object_id = metadata.decode().split()
        name = raw_path.decode("utf-8")
        if kind != "blob" or name in entries:
            raise ValueError("invalid/duplicate pinned Git entry")
        entries[name] = mode, object_id
        if len(entries) > 100000:
            raise ValueError("Git path count exceeds explicit100000bound")
    return entries


def reference_objects(snapshot: Path, jars: Path) -> tuple[list[dict], dict]:
    archives = []
    schemas = []
    names = ["StreamsGroupHeartbeatRequest", "StreamsGroupHeartbeatResponse",
             "StreamsGroupDescribeRequest", "StreamsGroupDescribeResponse", "RequestHeader", "ResponseHeader"]
    for release in JARS:
        archive = snapshot / "tests/conformance/broker/upstream" / f"apache-kafka-{release}-protocol.tar.gz"
        archives.append(identity(archive))
        with tarfile.open(archive) as bundle:
            for name in names:
                object_name = f"clients/src/main/resources/common/message/{name}.json"
                info = bundle.getmember(object_name)
                if not info.isfile() or info.size > 128 * 1024:
                    raise ValueError("official schema object type/size exceeds bound")
                stream = bundle.extractfile(info)
                if stream is None:
                    raise ValueError("missing official schema object")
                with stream:
                    data = stream.read(128 * 1024 + 1)
                if len(data) != info.size:
                    raise ValueError("official schema object length differs")
                schemas.append({"release": release, "object": object_name, "size": len(data),
                                "sha256": hashlib.sha256(data).hexdigest(), "tar_mode": f"{info.mode:04o}"})
    schema_review = json.loads((snapshot / "docs/evidence/client/streams-codecs/preparation/schema-review-02.json").read_text())
    source_rows = {(row["release"], row["object"]): row for row in schemas}
    for row in schema_review["source_schemas"]:
        object_name = f"clients/src/main/resources/common/message/{row['name']}.json"
        if source_rows[row["release"], object_name]["sha256"] != row["sha256"]:
            raise ValueError("official schema archive/source review differs")
    provenance = json.loads((snapshot / "docs/evidence/client/streams-codecs/preparation/class-source-review-02.json").read_text())
    classes = []
    for row in provenance["classes"]:
        release = row["release"]
        if row["jar_sha256"] != JARS[release]:
            raise ValueError("official class source jar pin differs")
        jar = jars / f"kafka-clients-{release}.jar"
        with zipfile.ZipFile(jar) as bundle:
            info = bundle.getinfo(row["object"])
            if info.file_size > 1024 * 1024:
                raise ValueError("official class object size exceeds bound")
            data = bundle.read(info)
        if hashlib.sha256(data).hexdigest() != row["class_sha256"]:
            raise ValueError("official class source hash differs")
        classes.append({"release": release, "object": row["object"], "size": len(data),
                        "sha256": row["class_sha256"], "zip_external_attr": info.external_attr,
                        "zip_create_system": info.create_system,
                        "zip_unix_mode": f"{(info.external_attr >> 16) & 0o777:04o}"})
    if len(schemas) != 18 or len(classes) != 88:
        raise ValueError("reference object count differs")
    return archives, {"official_schema_objects": schemas, "actual_jar_class_objects": classes}


def usage(output: Path) -> dict:
    unique = {}
    allocated_unique = {}
    paths = 0
    class_bytes = 0
    fixtures_bytes = 0
    metadata_bytes = 0
    for path in output.rglob("*"):
        if path.is_symlink():
            raise RuntimeError("unexpected output symlink")
        info = path.stat()
        key = info.st_dev, info.st_ino
        allocated_unique[key] = max(allocated_unique.get(key, 0), info.st_blocks * 512)
        if not path.is_file():
            continue
        paths += 1
        # Byte-identical before/after audit hardlinks preserve both paths, while
        # counting their shared physical allocation once.
        unique[key] = max(unique.get(key, 0), info.st_size)
        if path.suffix == ".class":
            class_bytes += info.st_size
        elif "generation-1" in path.parts or "generation-2" in path.parts:
            fixtures_bytes += info.st_size
        elif not path.name.startswith("source-") and path.suffix == ".json":
            metadata_bytes += info.st_size
    root_info = output.stat()
    allocated_unique[(root_info.st_dev, root_info.st_ino)] = root_info.st_blocks * 512
    return {"unique_bytes": sum(unique.values()), "allocated_unique_bytes": sum(allocated_unique.values()), "paths": paths,
            "class_bytes": class_bytes, "fixtures_bytes": fixtures_bytes,
            "metadata_bytes": metadata_bytes}


def guard(output: Path, reserve: int = 0) -> None:
    if reserve < 0:
        raise RuntimeError("negative output reserve")
    current = usage(output)
    free = shutil.disk_usage(output).free
    if free < FLOOR + reserve:
        raise RuntimeError("free disk insufficient for reserved write plus350MiB floor")
    if max(current["unique_bytes"], current["allocated_unique_bytes"]) + reserve > MAX_OUTPUT or current["paths"] > MAX_FILES:
        raise RuntimeError("owned unique output bytes/files exceed96MiB bound")
    if current["class_bytes"] > MAX_CLASS_BYTES or current["fixtures_bytes"] > MAX_FIXTURES_BYTES or current["metadata_bytes"] > MAX_METADATA_BYTES:
        raise RuntimeError("bounded output cohort exceeded")


def forecast(origin: Origin, output: Path) -> dict:
    from origin_audit import expected_directories
    directory_map = {name: {"full_permission_mode": 0o700, "uid": origin.root_uid, "gid": origin.root_gid}
                     for name in expected_directories(origin.rows)}
    directory_bytes = len(compact(directory_map))
    raw_before = len(origin.raw)
    gzip_before = origin.gzip_path.stat().st_size
    # After a genuine command failure, changed source maps remain separate raw
    # artifacts. We reserve their configured maximum; unchanged maps can hardlink.
    categories = {
        "full_raw_before_exact_bytes": raw_before,
        "full_raw_after_configured_upper_bound_bytes": MAX_AUDIT_BYTES,
        "verified_origin_gzip_before_exact_bytes": gzip_before,
        "verified_origin_gzip_after_upper_bound_bytes": gzip_before,
        "full_directory_before_exact_compact_bytes": directory_bytes,
        "full_directory_after_configured_upper_bound_bytes": MAX_DIRECTORY_BYTES,
        "all_fresh_classes_enforced_upper_bound_bytes": MAX_CLASS_BYTES,
        "all_two_process_three_sdk_fixtures_enforced_upper_bound_bytes": MAX_FIXTURES_BYTES,
        "ten_command_two_streams_enforced_upper_bound_bytes": 10 * 2 * MAX_STREAM,
        "minimal_json_metadata_enforced_upper_bound_bytes": MAX_METADATA_BYTES,
        "file_allocation_and_directory_entry_reserve_bytes": MAX_FILES * 4096,
    }
    required = sum(categories.values())
    # The file-allocation reserve covers block rounding, all physical hardlink
    # entry blocks and result directories; report actual free space at prelaunch.
    free = shutil.disk_usage(output).free
    if required > MAX_OUTPUT or free < FLOOR + required:
        raise RuntimeError("measured full-audit/output forecast does not fit96MiB/floor")
    return {"source_file_count": len(origin.rows), "source_bytes": 713363027,
            "source_directory_count": len(directory_map), "categories": categories,
            "worst_case_new_output_allocated_bytes": required,
            "successful_unchanged_raw_maps_exact_physical_bytes": raw_before + gzip_before + directory_bytes,
            "required_free_bytes": FLOOR + required, "sampled_free_bytes": free,
            "minimum_remaining_free_bytes": FLOOR, "owned_output_limit_bytes": MAX_OUTPUT}



def write_json(output: Path, name: str, value, maximum: int = 1024 * 1024) -> None:
    data = (json.dumps(value, indent=2) + "\n").encode()
    if len(data) > maximum:
        raise RuntimeError("owned metadata serialization exceeds bound")
    path = output / name
    if path.exists():
        raise RuntimeError("fresh command/result receipt required")
    guard(output, ((len(data) + 4095) // 4096) * 4096 + 4096)
    with path.open("xb") as handle:
        handle.write(data)
    path.chmod(0o600)


def command(output: Path, label: str, arguments: list[str], environment: dict) -> dict:
    guard(output, MAX_AUDIT_BYTES + MAX_DIRECTORY_BYTES + MAX_CLASS_BYTES + MAX_FIXTURES_BYTES + 2 * MAX_STREAM)
    started = time.monotonic()
    receipt = {"label": label, "command": arguments, "timeout_seconds": MAX_PROCESS_SECONDS,
               "maximum_stdout_or_stderr_bytes": MAX_STREAM, "cpu_affinity": "0,1"}
    process = subprocess.Popen(["taskset", "-c", "0,1", *arguments], env=environment,
                               cwd=output, start_new_session=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    selector = selectors.DefaultSelector()
    streams = {}
    for name, stream in [("stdout", process.stdout), ("stderr", process.stderr)]:
        assert stream is not None
        selector.register(stream, selectors.EVENT_READ, name)
        streams[name] = (output / f"{label}.{name}").open("wb")
    lengths = {name: 0 for name in streams}
    failure = None
    try:
        while selector.get_map():
            if time.monotonic() - started > MAX_PROCESS_SECONDS:
                raise RuntimeError("command exceeded absolute60s deadline")
            guard(output)
            for key, _ in selector.select(timeout=0.2):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                name = key.data
                lengths[name] += len(data)
                if lengths[name] > MAX_STREAM:
                    raise RuntimeError("command stream exceeds bound")
                streams[name].write(data)
        process.wait(timeout=5)
    except BaseException as error:
        failure = f"{type(error).__name__}: {error}"
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
    finally:
        selector.close()
        for stream in streams.values():
            stream.close()
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
    receipt.update(exit_status=process.returncode, duration_seconds=time.monotonic() - started,
                   streams={name: identity(output / f"{label}.{name}") for name in streams},
                   actual_read_stream_bytes=lengths,
                   joined=True, bound_failure=failure)
    write_json(output, f"{label}.json", receipt, maximum=64 * 1024)
    return receipt


def fixture_tree(root: Path) -> list[dict]:
    rows = []
    for path in sorted(root.iterdir()):
        if not path.is_file() or path.stat().st_size > MAX_FIXTURE_BYTES:
            raise ValueError("fixture file type/size exceeds bound")
        row = identity(path)
        row["path"] = path.name
        rows.append(row)
    if len(rows) != 55:
        raise ValueError("expected53actual body/header vectors and2indices")
    referenced = set()
    for name, expected, columns in [("cases.tsv", 41, 8), ("headers.tsv", 12, 10)]:
        table = (root / name).read_text().splitlines()
        if len(table) != expected + 1:
            raise ValueError("actual fixture table row count differs")
        for row in table[1:]:
            cells = row.split("\t")
            if len(cells) != columns or not cells[0] or any(ch not in "abcdefghijklmnopqrstuvwxyz-8" for ch in cells[0]):
                raise ValueError("actual fixture row shape/name differs")
            filename = cells[0] + ".bin"
            if filename in referenced or digest(root / filename) != cells[-1]:
                raise ValueError("actual fixture identity/hash differs")
            referenced.add(filename)
    if referenced != {row["path"] for row in rows if row["path"].endswith(".bin")}:
        raise ValueError("unreferenced/missing actual generated vector")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--jars", type=Path, required=True)
    parser.add_argument("--java", type=Path, default=Path("/usr/bin/java"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--origin", type=Path, default=ORIGIN_RECEIPT)
    args = parser.parse_args()
    for name in ["repo", "snapshot", "jars", "java", "output", "origin"]:
        setattr(args, name, getattr(args, name).resolve())
    if args.output.exists():
        raise ValueError("fresh output directory required; no evidence overwritten")
    os.umask(0o077)
    args.output.mkdir(parents=True, mode=0o700)
    guard(args.output)
    result = {"source_sha": PIN, "scope": "Complete04f product snapshot plus sole pinned848 standalone oracle compiler input; actual generated data/header codecs only; no broker/runtime/framework claim",
              "runtime_broker_claim": False, "driver_completed": False, "commands": []}
    def execute(label, arguments, environment):
        record = command(args.output, label, arguments, environment)
        result["commands"].append(record)
        guard(args.output)
        if record["bound_failure"] or record["exit_status"] != 0:
            raise RuntimeError(f"actual command failed: {label}")
        return record
    try:
        origin = Origin(args.origin, args.snapshot, git_entries(args.repo))
        result["disk_forecast"] = forecast(origin, args.output)
        before = audit(origin, args.output, "before", lambda reserve: guard(args.output, reserve))
        result["source_before"] = before
        if not before["passed"]:
            raise ValueError("complete origin/Git/SHA/full07777/owner audit failed before dispatch")
        source, overlay = compiler_oracle(args.oracle, args.snapshot)
        result["compiler_input_overlay"] = overlay
        slf4j = args.jars / "slf4j-api-1.7.36.jar"
        if digest(slf4j) != SLF4J_SHA:
            raise ValueError("SLF4J reference differs")
        inputs = [identity(source), identity(args.snapshot / ORACLE), identity(slf4j), identity(args.java.resolve())]
        for release, expected in JARS.items():
            jar = args.jars / f"kafka-clients-{release}.jar"
            if digest(jar) != expected:
                raise ValueError("actual Kafka classpath pin differs")
            inputs.append(identity(jar))
        archives, reference_audit = reference_objects(args.snapshot, args.jars)
        inputs.extend(archives)
        result["read_only_reference_objects"] = reference_audit
        result["read_only_input_objects_before"] = inputs
        temporary = args.output / "tmp"
        temporary.mkdir()
        environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                       "TZ": "UTC", "TMPDIR": str(temporary)}
        execute("java-version", [str(args.java), "-version"], environment)
        result["sdk_results"] = {}
        for release in JARS:
            directory = args.output / release
            classes = directory / "classes"
            classes.mkdir(parents=True)
            jar = args.jars / f"kafka-clients-{release}.jar"
            classpath = f"{jar}:{slf4j}"
            args_compile = [str(args.java), "-Xms16m", "-Xmx128m", f"-Djava.io.tmpdir={temporary}",
                            "--add-modules", "jdk.compiler",
                            "com.sun.tools.javac.Main", "-encoding", "UTF-8", "-Xlint:all", "-Werror",
                            "-proc:none", "-implicit:none", "-sourcepath", "",
                            "-d", str(classes), "-classpath", classpath, str(source)]
            execute(f"{release}-compile", args_compile, environment)
            class_files = list(classes.rglob("*.class"))
            if not 1 <= len(class_files) <= 16 or any(p.stat().st_size > 2 * 1024 * 1024 for p in class_files):
                raise ValueError("fresh class output exceeds bounds")
            trees = []
            for attempt in [1, 2]:
                fixtures = directory / f"generation-{attempt}"
                args_run = [str(args.java), "-Xms16m", "-Xmx128m", f"-Djava.io.tmpdir={temporary}",
                            "-classpath", f"{classes}:{classpath}",
                            "StreamsWireOracle", str(fixtures)]
                label = f"{release}-generation-{attempt}"
                execute(label, args_run, environment)
                stdout = (args.output / f"{label}.stdout").read_text().splitlines()
                if len(stdout) != 1 or json.loads(stdout[0]) != {"actual_generated_cases": 41, "runtime_broker_claim": False}:
                    raise ValueError("actual generation stdout count/statement differs")
                trees.append(fixture_tree(fixtures))
                guard(args.output)
            if trees[0] != trees[1]:
                raise ValueError("two actual independent SDK generation processes differ")
            result["sdk_results"][release] = {"deterministic_two_processes": True, "actual_body_cases": 41,
                                              "actual_header_body_cases": 12, "artifacts": trees[0],
                                              "fresh_classes": [identity(p) for p in sorted(classes.rglob("*.class"))]}
        after = audit(origin, args.output, "after", lambda reserve: guard(args.output, reserve))
        result["source_after"] = after
        if not after["passed"] or before["full_raw"]["sha256"] != after["full_raw"]["sha256"] or before["full_directory_raw"]["sha256"] != after["full_directory_raw"]["sha256"]:
            raise ValueError("complete pinned snapshot changed")
        if inputs != [identity(Path(row["path"])) for row in inputs]:
            raise ValueError("read-only reference/classpath objects changed")
        result["read_only_input_objects_after"] = inputs
        result["driver_completed"] = True
        guard(args.output)
    except BaseException as error:
        result["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        # A genuine failed compiler/generator still gets the complete observed
        # after-map; no partial outputs or old attempts are deleted.
        if "origin" in locals() and "after" not in locals():
            try:
                after = audit(origin, args.output, "after", lambda reserve: guard(args.output, reserve))
                result["source_after"] = after
                result["complete_source_unchanged_after_failure_or_success"] = after["passed"]
            except BaseException as audit_error:
                result["driver_completed"] = False
                result["source_after_failure"] = f"{type(audit_error).__name__}: {audit_error}"
        if "inputs" in locals():
            try:
                final_inputs = [identity(Path(row["path"])) for row in inputs]
                result["read_only_input_objects_after"] = final_inputs
                if inputs != final_inputs:
                    result["driver_completed"] = False
                    result["input_after_failure"] = "reference/classpath objects differ"
            except BaseException as input_error:
                result["driver_completed"] = False
                result["input_after_failure"] = f"{type(input_error).__name__}: {input_error}"
        write_json(args.output, "results.json", result)
        print(json.dumps({"driver_completed": result["driver_completed"], "command_count": len(result["commands"]),
                          "failure": result.get("failure"), "source_sha": PIN}))
        if not result["driver_completed"] and "failure" not in result:
            raise RuntimeError("post-run immutable input check failed")

if __name__ == "__main__":
    main()

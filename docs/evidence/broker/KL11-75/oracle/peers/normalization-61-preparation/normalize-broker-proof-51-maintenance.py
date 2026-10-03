#!/usr/bin/env python3
"""WORK derivative: preserve51 qualification gates and audit ten guarded cache cleans."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import math
from pathlib import Path

BASE_SHA = "14f78e770b0be02e8486b7c8f48884e77877e71652bf7c854e289c78a14c178a"
CACHE = "/workspace/work/target-broker-segments"
PROCESS_FIELDS = ["cwd", "exe", "fd", "CARGO_TARGET_DIR_environment", "maps"]
RESERVE = 350 * 1024 * 1024


def require(value, label):
    if not value:
        raise ValueError(label)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def scoped(root, relative):
    path = Path(relative)
    require(not path.is_absolute() and ".." not in path.parts, "scoped retained object path required")
    path = root / path
    require(path.is_file() and not path.is_symlink(), "regular retained object required")
    return path


def audit_elf_objects(raw, root):
    objects = {}
    for item in raw.get("retained_elf_objects", []):
        key = item["uncompressed_sha256"]
        require(key not in objects, "unique content-addressed ELF object required")
        path = scoped(root, item["path"])
        require(item.get("compression") == "gzip" and item.get("decompression_verified") is True
                and digest(path) == item["sha256"], "actual retained compressed ELF binding required")
        value, length, header = hashlib.sha256(), 0, b""
        with gzip.open(path, "rb") as stream:
            while chunk := stream.read(1024 * 1024):
                if not header:
                    header = chunk[:4]
                length += len(chunk)
                require(length <= 512 * 1024 * 1024, "bounded retained ELF object required")
                value.update(chunk)
        require(header == b"\x7fELF" and length == item["uncompressed_bytes"] and value.hexdigest() == key,
                "actual lossless retained ELF bytes required")
        objects[key] = item
    return objects


def clean_guard(row, operation, objects, final):
    guard = row["cache_clean_guard"]
    require(guard.get("owners_before") == [] and guard.get("owners_after") == []
            and guard.get("clean_authorized") is True and guard.get("cache_path") == CACHE
            and guard.get("operation") == operation and guard.get("process_guard_fields") == PROCESS_FIELDS,
            "two actual process-owner guards and explicit clean authorization required")
    forecast = guard["retention_forecast"]
    require(forecast.get("sufficient") is True and forecast.get("disk_reserve_bytes") == RESERVE
            and forecast.get("metadata_reserve_bytes") == 1024 * 1024
            and forecast.get("required_free_bytes") == RESERVE + 1024 * 1024 + forecast["new_gzip_upper_bound_bytes"]
            and forecast.get("sampled_free_bytes") >= forecast["required_free_bytes"],
            "actual sufficient pre-clean lossless-retention reserve required")
    unknown = forecast["unretained_elfs"]
    require(sum(item["gzip_upper_bound_bytes"] for item in unknown) == forecast["new_gzip_upper_bound_bytes"],
            "full unknown ELF retention forecast must reconcile")
    for item in unknown:
        size = item["raw_bytes"]
        require(isinstance(size, int) and size >= 4
                and item["gzip_upper_bound_bytes"] == size + ((size + 16382) // 16383) * 5 + 64,
                "conservative gzip forecast cannot assume compression ratio")
    for retained in guard["retained_cache_elfs"]:
        obj = objects.get(retained["sha256"])
        require(obj is not None and obj["path"] == retained["retained_object"]
                and obj["uncompressed_bytes"] == retained["bytes"]
                and retained.get("pre_clean_bytes_and_gzip_verified") is True
                and retained.get("command") == row["name"]
                and isinstance(retained.get("original_mode"), int) and 0 <= retained["original_mode"] <= 0o7777,
                "pre-clean original ELF byte and full permission-mode restore receipt required")
    retained_paths = {item["original_path"]: item for item in guard["retained_cache_elfs"]}
    require(len(retained_paths) == len(guard["retained_cache_elfs"]), "one pre-clean record per original ELF path")
    for item in unknown:
        retained = retained_paths.get(item["path"])
        require(retained is not None and retained["bytes"] == item["raw_bytes"]
                and retained["original_mode"] == item["full_permission_mode"],
                "every forecast ELF must be retained with exact original bytes/mode before clean")
    for phase in ("source_before", "source_after"):
        require(row[phase] == final, "complete identical Git bytes and baseline modes required around clean")


def command_logs(row, root):
    log = scoped(root, row["name"] + "/command.log")
    require(digest(log) == row["log_sha256"], "actual command log binding required")
    path = scoped(root, row["name"] + "/disk-monitor.jsonl")
    monitor = row["disk_monitor"]
    require(path.stat().st_size <= 8 * 1024 * 1024 and digest(path) == monitor["sample_log_sha256"],
            "bounded actual disk-observation log binding required")
    samples, minimum, group, previous_time, final = 0, None, None, None, None
    with path.open() as stream:
        for line in stream:
            require(len(line) <= 8192 and samples < 100000, "bounded actual disk observation required")
            item = json.loads(line)
            free, stamp, process = item["free_bytes"], item["utc_unix_seconds"], item["process_group"]
            require(isinstance(free, int) and free >= RESERVE and item.get("below_reserve") is False
                    and isinstance(stamp, (float, int)) and math.isfinite(stamp)
                    and isinstance(process, int) and process > 0,
                    "actual disk samples must preserve reserve and bind a finite isolated process group")
            if samples == 0:
                require(item.get("pre_launch") is True, "raw pre-launch disk sample required")
                group = process
            else:
                require(item.get("pre_launch") is not True and process == group,
                        "one actual pre-launch sample and consistent command process group required")
            require(previous_time is None or stamp >= previous_time, "chronological actual disk samples required")
            require(final is None or final.get("process_completed") is not True, "completion must be final disk observation")
            samples += 1; minimum = free if minimum is None else min(minimum, free)
            previous_time, final = stamp, item
    require(samples >= 3 and final.get("process_completed") is True and monitor.get("samples") == samples
            and monitor.get("minimum_free_bytes") == minimum and monitor.get("actual_process_exit_code") == 0
            and monitor.get("triggered") is False and monitor.get("trigger") is None,
            "exact raw disk sample/minimum/final-completion/exit/trigger reconciliation required")


def normalize(raw, source_sha, binary_root, validation_path):
    base_path = Path(__file__).parent / "frozen-14f78e77" / "normalize-broker-proof-51.py"
    require(digest(base_path) == BASE_SHA, "frozen51 baseline helper changed")
    spec = importlib.util.spec_from_file_location("frozen_51_normalizer", base_path)
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)
    result = base.normalize(raw, source_sha, binary_root, validation_path)
    matrix = raw["qualification_matrix"]
    require(matrix.get("maintenance_command_count") == 10 and matrix.get("expected_actual_command_count") == 61,
            "explicit ten-maintenance/61-actual ledger counts required")
    require(raw.get("execution_counts") == {"qualification_commands": 51, "maintenance_commands": 10, "actual_commands": 61},
            "actual separate-ledger execution counts must reconcile51+10=61")
    cpu = raw["cpu_affinity"]
    ids = cpu["actual_cpu_ids"]
    require(isinstance(ids, list) and ids and ids == sorted(set(ids))
            and all(isinstance(item, int) for item in ids) and set(ids) <= {0,1,2,4}
            and cpu["taskset_argument"] == ",".join(map(str, ids)), "actual canonical CPU affinity excluding reservedCPU3 required")
    require(raw["environment"]["CARGO_TARGET_DIR"] == CACHE, "exact isolated generated-cache target required")
    expected_order, expected_specs = [], []
    expected_order.extend(("commands", name) for name in ("clean-before-source-switch", "format"))
    for compiler in base.TOOLCHAINS:
        if compiler == "1.85.0":
            expected_order.append(("commands", "clean-between-toolchains"))
        for profile in base.PROFILES:
            expected_order.extend(("commands", f"{compiler}-{profile}-{gate}") for gate in base.GATES)
            if profile != "all-features":
                name = f"{compiler}-maintenance-after-{profile}"
                argv = ["taskset", "-c", cpu["taskset_argument"], "cargo", "+" + compiler, "clean",
                        "--offline", "--locked", "--manifest-path", "partitionline-broker/Cargo.toml", "--target-dir", CACHE]
                expected_specs.append({"name": name, "after_profile": profile, "toolchain": compiler,
                                       "argv": argv, "operation": "full-generated-cache-clean"})
                expected_order.append(("maintenance_commands", name))
    require(matrix.get("maintenance_operations") == expected_specs, "exact ordered bounded full-cache maintenance plan required")
    order = raw["execution_order"]
    require(order == [{"sequence": index + 1, "ledger": ledger, "name": name}
                      for index, (ledger, name) in enumerate(expected_order)], "actual61 interleaved execution order required")
    maintenance = raw["maintenance_commands"]
    require(len(maintenance) == 10 and [row["name"] for row in maintenance] == [item["name"] for item in expected_specs],
            "ten actual maintenance rows remain separate from51 qualification gates")
    objects = audit_elf_objects(raw, binary_root)
    all_rows = raw["commands"] + maintenance
    for row in all_rows:
        require(row.get("cpu_affinity") == cpu and row["argv"][:3] == ["taskset", "-c", cpu["taskset_argument"]]
                and row.get("ledger") in ("commands", "maintenance_commands")
                and row.get("exit_code") == 0, "every actual command must bind successful execution and canonical affinity")
        monitor = row["disk_monitor"]
        require(monitor.get("threshold_bytes") == RESERVE and monitor.get("triggered") is False
                and monitor.get("samples", 0) >= 1 and monitor.get("minimum_free_bytes", 0) >= RESERVE,
                "actual monitored commands must preserve350MiB reserve")
        command_logs(row, validation_path.parent)
    for row, expected in zip(maintenance, expected_specs):
        require(row["argv"] == expected["argv"] and row["ledger"] == "maintenance_commands",
                "actual maintenance clean argv and separate ledger must match plan")
        clean_guard(row, "full-generated-cache-clean", objects, raw["final_source"])
    for name in ("clean-before-source-switch", "clean-between-toolchains"):
        row = next(item for item in raw["commands"] if item["name"] == name)
        require(row["argv"][3:] == ["cargo", "+stable", "clean", "--offline", "--locked", "--manifest-path",
                                    "partitionline-broker/Cargo.toml", "--target-dir", CACHE, "--package", "partitionline-broker"],
                "reviewed qualification package-clean argv required")
        clean_guard(row, "package-clean", objects, raw["final_source"])
    result.update({"base_normalizer_sha256": BASE_SHA, "normalizer_sha256": digest(Path(__file__)),
                   "actual_cache_maintenance_commands": 10, "actual_total_executed_commands": 61,
                   "maintenance_commands": maintenance, "execution_order": order, "cpu_affinity": cpu,
                   "lossless_retained_cache_elf_objects": len(objects),
                   "scope": "51 actual qualification gates remain separate from10 guarded generated-cache maintenance commands; twelve broker cells qualify, selecting only four default/all-feature genuine Fetch ELFs. No public client runtime claim."})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation", required=True, type=Path)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--binary-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    require(args.validation.is_file() and not args.validation.is_symlink()
            and args.validation.stat().st_size <= 64 * 1024 * 1024, "bounded regular61-command receipt required")
    result = normalize(json.loads(args.validation.read_text()), args.source_sha, args.binary_root, args.validation)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"passed": True, "actual_qualification_commands": 51,
                      "actual_cache_maintenance_commands": 10, "actual_total_executed_commands": 61,
                      "selected_public_compaction_lanes": 4}))


if __name__ == "__main__":
    main()

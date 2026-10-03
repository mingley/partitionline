#!/usr/bin/env python3
"""Synthetic helper controls only; no Cargo, broker, or public peer runs."""
import copy
import gzip
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / (sys.argv[1] if len(sys.argv) == 2 else "contract-controls-attempt-1")
OUT.mkdir(exist_ok=False)
spec = importlib.util.spec_from_file_location("maintenance_helper", ROOT / "normalize-broker-proof-51-maintenance.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
source_sha = "1" * 40
source = {"file_count": 1, "all_git_blobs_match": True, "all_baseline_permission_modes_match": True, "set_and_blob_sha256": "2" * 64}
audit = OUT / "source-integrity.json"; audit.write_text("{\"synthetic\":true}\n")
raw = {"schema": 1, "source_commit": source_sha, "archive_sha256": "3" * 64,
       "final_source": source, "source_manifest_sha256": helper.digest(audit),
       "source_manifest": {"path": audit.name, "sha256": helper.digest(audit), "uncompressed_sha256": helper.digest(audit),
                           "uncompressed_bytes": audit.stat().st_size, "original_mode": audit.stat().st_mode & 0o7777, "compression": None},
       "toolchains": {"stable": "release: 1.99.0", "1.85.0": "release: 1.85.0"},
       "environment": {"CARGO_TARGET_DIR": helper.CACHE},
       "cpu_affinity": {"requested": "2,4", "taskset_argument": "2,4", "actual_cpu_ids": [2,4]},
       "commands": [], "maintenance_commands": [], "execution_order": [], "retained_binaries": []}
profiles = [{"name":"default","cargo_flags":[]}, {"name":"tls","cargo_flags":["--no-default-features","--features","tls"]},
            {"name":"sasl","cargo_flags":["--no-default-features","--features","sasl"]},
            {"name":"sasl+tls","cargo_flags":["--no-default-features","--features","sasl,tls"]},
            {"name":"oidc","cargo_flags":["--no-default-features","--features","oidc"]},
            {"name":"all-features","cargo_flags":["--all-features"]}]
matrix = {"toolchains":["stable","1.85.0"],"profiles":profiles,"test_cells":12,"behavior_lint_doc_gates":48,
          "expected_command_count":51,"gates_per_cell":["all-targets","strict-clippy","strict-doc","strict-doctest"],
          "format_gates":1,"package_clean_commands":2,"dependency_flags":["--offline","--locked"],
          "peer_profiles":["default","all-features"],"maintenance_command_count":10,"expected_actual_command_count":61,"maintenance_operations":[]}
raw["qualification_matrix"] = matrix
raw["execution_counts"] = {"qualification_commands":51,"maintenance_commands":10,"actual_commands":61}
payload = b"\x7fELFsynthetic-helper-only-not-a-built-executable"
gz = OUT / "synthetic-elf.gz"; gz.write_bytes(gzip.compress(payload, mtime=0)); elf_sha = hashlib.sha256(payload).hexdigest()
raw["retained_elf_objects"] = [{"path":gz.name,"sha256":helper.digest(gz),"uncompressed_sha256":elf_sha,
                              "uncompressed_bytes":len(payload),"compression":"gzip","decompression_verified":True}]
prefix = ["taskset","-c","2,4"]
manifest = ["--offline","--locked","--manifest-path","partitionline-broker/Cargo.toml"]
clean_argv = lambda tc, package: [*prefix,"cargo","+"+tc,"clean",*manifest,"--target-dir",helper.CACHE,*(["--package","partitionline-broker"] if package else [])]

def add(ledger, name, argv, clean=False, operation=None):
    row = {"name":name,"ledger":ledger,"argv":argv,"exit_code":0,"source_before":source,"source_after":source,
           "cpu_affinity":raw["cpu_affinity"],"environment_additions":{"RUSTDOCFLAGS":"-D warnings"},
           "disk_monitor":{"threshold_bytes":helper.RESERVE,"minimum_free_bytes":helper.RESERVE+1,"samples":1,"triggered":False}}
    if clean:
        row["cache_clean_guard"] = {"owners_before":[],"owners_after":[],"clean_authorized":True,
            "cache_path":helper.CACHE,"operation":operation,"process_guard_fields":helper.PROCESS_FIELDS,
            "retention_forecast":{"sufficient":True,"disk_reserve_bytes":helper.RESERVE,"metadata_reserve_bytes":1048576,
                "new_gzip_upper_bound_bytes":len(payload)+69,"required_free_bytes":helper.RESERVE+1048576+len(payload)+69,
                "sampled_free_bytes":helper.RESERVE+2097152,"unretained_elfs":[{"path":helper.CACHE+"/debug/synthetic",
                    "raw_bytes":len(payload),"gzip_upper_bound_bytes":len(payload)+69,"full_permission_mode":0o751}]},
            "retained_cache_elfs":[{"original_path":helper.CACHE+"/debug/synthetic","sha256":elf_sha,"original_mode":0o751,
                "bytes":len(payload),"retained_object":gz.name,"command":name,"pre_clean_bytes_and_gzip_verified":True}]}
    raw[ledger].append(row);raw["execution_order"].append({"sequence":len(raw["execution_order"])+1,"ledger":ledger,"name":name})
    return row

add("commands","clean-before-source-switch",clean_argv("stable",True),True,"package-clean")
add("commands","format",[*prefix,"cargo","+stable","fmt","--manifest-path","partitionline-broker/Cargo.toml","--check"])
for tc in ("stable","1.85.0"):
    if tc == "1.85.0":add("commands","clean-between-toolchains",clean_argv("stable",True),True,"package-clean")
    for profile in profiles:
        name, flags = profile["name"],profile["cargo_flags"]
        for gate, args in [("all-targets",["test",*manifest,*flags,"--all-targets","--","--test-threads=1","--nocapture"]),
                           ("strict-clippy",["clippy",*manifest,*flags,"--all-targets","--","-D","warnings"]),
                           ("strict-doc",["doc",*manifest,*flags,"--no-deps"]),
                           ("strict-doctest",["test",*manifest,*flags,"--doc"])]:
            row=add("commands",f"{tc}-{name}-{gate}",[*prefix,"cargo","+"+tc,*args])
            if gate == "all-targets" and name in ("default","all-features"):
                path=OUT/"bin"/tc/("fetch" if name=="default" else "fetch-all-features");path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(payload);path.chmod(0o751)
                raw["retained_binaries"].append({"lane":row["name"],"path":str(path.relative_to(OUT)),"sha256":helper.digest(path),
                    "source_commit":source_sha,"build_command":row["argv"],"toolchain":raw["toolchains"][tc],
                    "original_mode":0o751,"copied_bytes_and_mode_verified":True})
        if name != "all-features":
            label=f"{tc}-maintenance-after-{name}";argv=clean_argv(tc,False)
            matrix["maintenance_operations"].append({"name":label,"after_profile":name,"toolchain":tc,"argv":argv,"operation":"full-generated-cache-clean"})
            add("maintenance_commands",label,argv,True,"full-generated-cache-clean")
receipt=OUT/"validation.json"
receipt.write_text(json.dumps(raw,indent=2)+"\n")
positive=helper.normalize(raw,source_sha,OUT,receipt)
mutations = {
    "missing_maintenance":lambda r:r["maintenance_commands"].pop(),
    "duplicate_maintenance":lambda r:r["maintenance_commands"].append(copy.deepcopy(r["maintenance_commands"][0])),
    "changed_execution_order":lambda r:r["execution_order"].reverse(),
    "wrong_total_count":lambda r:r["qualification_matrix"].update(expected_actual_command_count=51),
    "wrong_actual_counts":lambda r:r["execution_counts"].update(actual_commands=51),
    "active_owner_before":lambda r:r["maintenance_commands"][0]["cache_clean_guard"].update(owners_before=[{"pid":1}]),
    "active_owner_after":lambda r:r["maintenance_commands"][0]["cache_clean_guard"].update(owners_after=[{"pid":1}]),
    "missing_maps_guard":lambda r:r["maintenance_commands"][0]["cache_clean_guard"].update(process_guard_fields=helper.PROCESS_FIELDS[:-1]),
    "unverified_elf":lambda r:r["maintenance_commands"][0]["cache_clean_guard"]["retained_cache_elfs"][0].update(pre_clean_bytes_and_gzip_verified=False),
    "lost_original_mode":lambda r:r["maintenance_commands"][0]["cache_clean_guard"]["retained_cache_elfs"][0].update(original_mode=0o700),
    "optimistic_gzip_forecast":lambda r:r["maintenance_commands"][0]["cache_clean_guard"]["retention_forecast"]["unretained_elfs"][0].update(gzip_upper_bound_bytes=1),
    "insufficient_reserve":lambda r:r["maintenance_commands"][0]["cache_clean_guard"]["retention_forecast"].update(sampled_free_bytes=0),
    "maintenance_package_only":lambda r:r["maintenance_commands"][0]["argv"].extend(["--package","partitionline-broker"]),
    "changed_target":lambda r:r["environment"].update(CARGO_TARGET_DIR="/workspace/work/other"),
    "changed_source":lambda r:r["maintenance_commands"][0].update(source_after={**source,"set_and_blob_sha256":"4"*64}),
    "failed_maintenance":lambda r:r["maintenance_commands"][0].update(exit_code=1),
    "disk_cutoff":lambda r:r["maintenance_commands"][0]["disk_monitor"].update(triggered=True),
    "reserved_cpu3":lambda r:r["cpu_affinity"].update(actual_cpu_ids=[2,3,4]),
    "unsupported_peer_profile":lambda r:r["qualification_matrix"].update(peer_profiles=["tls","all-features"]),
    "changed_elf_sha":lambda r:r["retained_elf_objects"][0].update(uncompressed_sha256="5"*64),
}
results=[]
for name, change in mutations.items():
    candidate=copy.deepcopy(raw);change(candidate)
    try:helper.normalize(candidate,source_sha,OUT,receipt)
    except (ValueError,KeyError) as error:results.append({"name":name,"rejected":True,"reason":str(error)})
    else:results.append({"name":name,"rejected":False})
report={"schema_version":1,"scope":"Synthetic source-only normalizer controls; fake ELF magic fixture, zero Cargo/broker/client executions",
        "positive_baseline_passed":positive["passed"],"negative_controls":results,"passed":all(r["rejected"] for r in results),
        "helper_sha256":helper.digest(ROOT/"normalize-broker-proof-51-maintenance.py"),"actual_qualification_commands":0,"actual_public_peers":0}
(OUT/"helper-controls.json").write_text(json.dumps(report,indent=2)+"\n")
print(json.dumps({"passed":report["passed"],"synthetic_positive":1,"negative_controls":len(results),"actual_broker_commands":0}))

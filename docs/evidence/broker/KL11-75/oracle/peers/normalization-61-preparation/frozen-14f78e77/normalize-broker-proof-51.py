#!/usr/bin/env python3
"""WORK draft: qualify51 broker gates and select four genuine compaction ELFs."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

PROFILES = {
    "default": [], "tls": ["tls"], "sasl": ["sasl"],
    "sasl+tls": ["sasl", "tls"], "oidc": ["oidc"], "all-features": None,
}
GATES = ("all-targets", "strict-clippy", "strict-doc", "strict-doctest")
TOOLCHAINS = ("stable", "1.85.0")


def require(condition, label):
    if not condition:
        raise ValueError(label)


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024*1024):
            value.update(chunk)
    return value.hexdigest()


def source_manifest_binding(raw, validation_path):
    manifest = raw["source_manifest"]
    relative = Path(manifest["path"])
    require(not relative.is_absolute() and ".." not in relative.parts, "source audit path must be scoped")
    path = validation_path.parent / relative
    require(path.is_file() and not path.is_symlink() and sha(path) == manifest["sha256"],
            "retained source manifest byte binding required")
    mode = path.stat().st_mode & 0o7777
    compression = manifest.get("compression")
    require(compression in (None,"gzip"), "reviewed source audit encoding required")
    if compression == "gzip":
        require(manifest.get("decompression_verified") is True and mode == manifest.get("compressed_mode")
                and mode == manifest.get("original_mode"), "verified source audit gzip/mode provenance required")
        stream = gzip.open(path,"rb")
    else:
        require(mode == manifest.get("original_mode"), "retained source audit permission mode required")
        stream = path.open("rb")
    value, length = hashlib.sha256(), 0
    with stream:
        while chunk := stream.read(1024*1024):
            length += len(chunk)
            require(length <= 64*1024*1024, "bounded logical source audit bytes")
            value.update(chunk)
    require(length == manifest["uncompressed_bytes"]
            and value.hexdigest() == manifest["uncompressed_sha256"] == raw["source_manifest_sha256"],
            "actual logical source audit hash/size binding required")
    return manifest


def feature_selection(argv):
    features = []
    for index, argument in enumerate(argv):
        if argument == "--features":
            require(index+1 < len(argv), "feature value missing")
            features.extend(argv[index+1].replace(",", " ").split())
        elif argument.startswith("--features="):
            features.extend(argument.split("=",1)[1].replace(",", " ").split())
    return sorted(set(features)), "--all-features" in argv


def normalize(raw, source_sha, binary_root, validation_path):
    require(raw.get("source_commit") == source_sha and raw.get("schema") == 1,
            "exact merged immutable broker source/schema required")
    require(len(source_sha) == 40 and all(c in "0123456789abcdef" for c in source_sha),
            "full committed source pin required")
    final = raw["final_source"]
    require(final.get("all_git_blobs_match") is True and final.get("all_baseline_permission_modes_match") is True
            and final.get("file_count")
            and final.get("set_and_blob_sha256"), "complete final Git source proof required")
    source_manifest = source_manifest_binding(raw,validation_path)
    matrix = raw["qualification_matrix"]
    required_profiles = [
        {"name":"default","cargo_flags":[]}, {"name":"tls","cargo_flags":["--no-default-features","--features","tls"]},
        {"name":"sasl","cargo_flags":["--no-default-features","--features","sasl"]},
        {"name":"sasl+tls","cargo_flags":["--no-default-features","--features","sasl,tls"]},
        {"name":"oidc","cargo_flags":["--no-default-features","--features","oidc"]},
        {"name":"all-features","cargo_flags":["--all-features"]},
    ]
    require(matrix.get("toolchains") == list(TOOLCHAINS) and matrix.get("profiles") == required_profiles
            and matrix.get("test_cells") == 12 and matrix.get("behavior_lint_doc_gates") == 48
            and matrix.get("expected_command_count") == 51
            and matrix.get("gates_per_cell") == list(GATES)
            and matrix.get("format_gates") == 1 and matrix.get("package_clean_commands") == 2
            and matrix.get("dependency_flags") == ["--offline","--locked"]
            and matrix.get("peer_profiles") == ["default","all-features"],
            "explicit twelve-cell/six-profile qualification metadata required")
    materialization = raw.get("source_materialization")
    origin = raw.get("source_origin_receipt")
    if raw.get("archive_sha256") is None:
        require(isinstance(materialization,str) and materialization.startswith("reused existing complete tree")
                and raw.get("source_tree") and isinstance(origin,dict)
                and origin.get("path") and origin.get("sha256"),
                "null own archive digest needs explicit reused-source/external-origin provenance")
        require(sha(Path(origin["path"])) == origin["sha256"], "external reused-source origin receipt changed")
    expected = {"clean-before-source-switch", "format", "clean-between-toolchains"}
    for compiler in TOOLCHAINS:
        for profile in PROFILES:
            for gate in GATES:
                expected.add(f"{compiler}-{profile}-{gate}")
    commands = raw["commands"]
    require(len(commands) == 51 and {row["name"] for row in commands} == expected,
            "exact51-command/six-profile/two-toolchain matrix required")
    cells = []
    for row in commands:
        require(row.get("exit_code") == 0, "all51 actual broker gates must pass")
        for phase in ("source_before", "source_after"):
            proof = row[phase]
            require(proof.get("all_git_blobs_match") is True and proof.get("all_baseline_permission_modes_match") is True
                    and proof.get("file_count") == final["file_count"]
                    and proof.get("set_and_blob_sha256") == final["set_and_blob_sha256"],
                    "complete exact source bytes/modes must survive every command")
    for compiler in TOOLCHAINS:
        for profile, feature_set in PROFILES.items():
            selected = []
            for gate in GATES:
                command = next(row for row in commands if row["name"] == f"{compiler}-{profile}-{gate}")
                argv = command["argv"]
                require(isinstance(argv,list) and len(argv) <= 512 and all(isinstance(arg,str) for arg in argv),
                        "bounded actual command argv required")
                actual_features, all_features = feature_selection(argv)
                require((feature_set is None and all_features and not actual_features)
                        or (feature_set is not None and not all_features and actual_features == sorted(feature_set)),
                        "actual feature argv must match the declared broker cell")
                require(("--no-default-features" in argv) == (profile in ("tls","sasl","sasl+tls","oidc")),
                        "named broker profiles explicitly disable defaults; peer default/all-feature profiles preserve selection")
                require("partitionline-broker/Cargo.toml" in argv and "--locked" in argv and "--offline" in argv
                        and "+" + compiler in argv, "offline locked actual broker-manifest/compiler gate required")
                operation = argv[argv.index("+" + compiler)+1]
                if gate == "all-targets":
                    require(operation == "test" and "--all-targets" in argv and "--test-threads=1" in argv
                            and "--nocapture" in argv, "actual full behavioral target execution required")
                elif gate == "strict-clippy":
                    require(operation == "clippy" and "--all-targets" in argv and "-D" in argv and "warnings" in argv,
                            "actual strict full-target Clippy gate required")
                elif gate == "strict-doc":
                    require(operation == "doc" and "--no-deps" in argv
                            and command["environment_additions"].get("RUSTDOCFLAGS") == "-D warnings",
                            "actual strict documentation gate required")
                else:
                    require(operation == "test" and "--doc" in argv
                            and command["environment_additions"].get("RUSTDOCFLAGS") == "-D warnings",
                            "actual strict doctest gate required")
                selected.append(command["name"])
            cells.append({"toolchain":compiler,"profile":profile,"explicit_features":feature_set,
                          "all_features":feature_set is None,"actual_gate_names":selected})
    binaries = []
    for compiler in TOOLCHAINS:
        for profile in ("default", "all-features"):
            lane = f"{compiler}-{profile}-all-targets"
            wanted_name = "fetch" if profile == "default" else "fetch-all-features"
            rows = [row for row in raw["retained_binaries"] if row.get("lane") == lane
                    and Path(row["path"]).name == wanted_name and not row.get("compression")]
            require(len(rows) == 1, "one genuine copied Fetch ELF for each of four selected public-peer lanes")
            row = rows[0]
            relative = Path(row["path"])
            require(not relative.is_absolute() and ".." not in relative.parts, "copied broker ELF path must be scoped")
            path = binary_root / relative
            require(path.is_file() and not path.is_symlink(), "regular actual copied Fetch executable required")
            data = path.read_bytes()
            gate = next(command for command in commands if command["name"] == lane)
            require(data.startswith(b"\x7fELF") and hashlib.sha256(data).hexdigest() == row["sha256"]
                    and row.get("source_commit") == source_sha and row.get("build_command") == gate["argv"],
                    "actual copied ELF source/lane/alltargets-command/byte binding required")
            require(row.get("original_mode") == path.stat().st_mode & 0o7777
                    and row.get("copied_bytes_and_mode_verified") is True
                    and row.get("toolchain") == raw["toolchains"][compiler],
                    "actual copied Fetch compiler/permission-mode binding required")
            binaries.append(dict(row, actual_path=str(path), bytes=len(data)))
    compiler_versions = raw["toolchains"]
    require(set(compiler_versions) == set(TOOLCHAINS)
            and "release: 1.85.0" in compiler_versions["1.85.0"], "actual stable/MSRV compiler receipts required")
    return {"schema_version":1,"schema":1,"source_commit":source_sha,"passed":True,
            "actual_full_broker_commands":51,"actual_broker_lanes":12,"broker_profiles":cells,
            "qualification_matrix":matrix,
            "selected_public_compaction_lanes":4,"selected_public_compaction_profiles":["default","all-features"],
            "raw_validation":{"path":str(validation_path),"sha256":sha(validation_path)},
            "normalizer_sha256":sha(Path(__file__)),"retained_binaries":binaries,"commands":commands,
            "toolchains":compiler_versions,"final_source":final,"archive_sha256":raw.get("archive_sha256"),
            "source_manifest_sha256":raw["source_manifest_sha256"],"source_manifest":source_manifest,
            "source_materialization":materialization,"source_tree":raw.get("source_tree"),"source_origin_receipt":origin,
            "scope":"All51 actual merged broker gates/12 feature cells qualify; only four default/all-feature Fetch ELFs admit the independent public compaction peers. No client runtime is established by normalization."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation",required=True,type=Path)
    parser.add_argument("--source-sha",required=True)
    parser.add_argument("--binary-root",required=True,type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args = parser.parse_args()
    require(not args.validation.is_symlink() and args.validation.is_file()
            and args.validation.stat().st_size <= 16*1024*1024, "bounded regular broker receipt required")
    result = normalize(json.loads(args.validation.read_text()),args.source_sha,args.binary_root,args.validation)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({"passed":True,"source_sha":args.source_sha,"actual_full_broker_commands":51,
                      "actual_broker_lanes":12,"selected_public_compaction_lanes":4}),flush=True)


if __name__ == "__main__":
    main()

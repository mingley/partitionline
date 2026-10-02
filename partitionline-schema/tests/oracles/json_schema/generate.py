#!/usr/bin/env python3
"""Pinned offline Draft 2020-12 fixtures and validation of actual Rust frames."""

import argparse
import decimal
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import struct
import zipfile

DIALECT = "https://json-schema.org/draft/2020-12/schema"
RESOURCE = "urn:partitionline:json-schema:int64"


def serialized(value):
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def exact_number(text):
    # Preserve JSON Schema's mathematical integer semantics for 1.0/1e3 and
    # exact boundaries beyond f64. This changes JSON parsing, not the validator.
    value = decimal.Decimal(text)
    return int(value) if value == value.to_integral_value() else value


def forbidden_constant(_):
    raise ValueError("NaN and infinity are not JSON")


def main():
    if not __debug__:
        raise SystemExit("run without -O so peer assertions remain enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--rust-output", type=Path)
    args = parser.parse_args()
    lock = json.loads((Path(__file__).parent / "peer-lock.json").read_text())
    assert platform.python_version() == lock["python"]
    source_hashes = {}
    for package in lock["packages"]:
        distribution = importlib.metadata.distribution(package["name"])
        assert distribution.version == package["version"]
        wheel_path = args.wheel_dir / package["filename"]
        assert digest(wheel_path.read_bytes()) == package["sha256"]
        with zipfile.ZipFile(wheel_path) as wheel:
            for name in sorted(wheel.namelist()):
                if name.endswith((".py", ".so", ".json")) and ".dist-info/" not in name:
                    data = wheel.read(name)
                    assert Path(distribution.locate_file(name)).read_bytes() == data, name
                    source_hashes[name] = digest(data)

    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
    from referencing.exceptions import NoSuchResource, Unresolvable

    bounded = {"$schema": DIALECT, "$id": RESOURCE, "type": "integer",
               "minimum": -(2**63), "maximum": 2**63 - 1}
    writer = {"$schema": DIALECT, "$id": "urn:partitionline:json-schema:writer",
              "anyOf": [{"type": "null"}, {"$ref": RESOURCE}]}
    reader = {**writer, "$id": "urn:partitionline:json-schema:reader", "default": 7}
    incompatible = {"$schema": DIALECT, "type": "integer", "minimum": 0}

    def no_fetch(uri):
        raise NoSuchResource(ref=uri)

    offline = Registry(retrieve=no_fetch).with_resource(RESOURCE, Resource.from_contents(bounded))
    for schema in [writer, reader, bounded, incompatible, True, False]:
        Draft202012Validator.check_schema(schema)
    w = Draft202012Validator(writer, registry=offline)
    r = Draft202012Validator(reader, registry=offline)
    bad_reader = Draft202012Validator(incompatible, registry=offline)
    try:
        Draft202012Validator(writer, registry=Registry(retrieve=no_fetch)).validate(0)
    except Exception as error:
        assert isinstance(error.__cause__, Unresolvable), type(error).__name__
    else:
        raise AssertionError("unresolved resource was accepted")

    cases = [
        ("null", "null"), ("zero", "0"), ("negative-zero", "-0"),
        ("fractional-spelling-integer", "1.0"), ("exponent-integer", "1e3"),
        ("int64-min", "-9223372036854775808"), ("int64-max", "9223372036854775807"),
        ("above-f64-exact", "9007199254740993"),
        ("decimal-int64-max", "9223372036854775807.0"),
        ("decimal-above-max", "9223372036854775808.0"),
        ("above-max", "9223372036854775808"), ("below-min", "-9223372036854775809"),
        ("fraction", "1.5"), ("negative-exponent-fraction", "1e-1"),
        ("huge-exponent", "1e309"), ("tiny-fraction", "1e-400"),
        ("string-number", '"3"'), ("boolean", "true"), ("array", "[]"), ("object", "{}"),
        ("nan", "NaN"), ("infinity", "Infinity"), ("leading-zero", "01"),
        ("trailing-document", "0 1"), ("empty", ""), ("whitespace", " \t\n"),
        ("surrounding-json-space", " \t1\r\n"),
    ]
    artifacts = {".gitattributes": b"*.payload.json -diff -merge -text\n*.frame.bin -diff -merge -text\n",
                 "writer.schema.json": serialized(writer), "reader.schema.json": serialized(reader),
                 "int64.schema.json": serialized(bounded), "incompatible.schema.json": serialized(incompatible),
                 "true.schema.json": b"true\n", "false.schema.json": b"false\n"}
    header = struct.pack(">BI", 0, 42)
    records = []
    rust_lines = []
    reverse = 0
    for name, raw in cases:
        try:
            value = json.loads(raw, parse_float=exact_number, parse_constant=forbidden_constant)
            valid = w.is_valid(value)
            reader_valid = valid and r.is_valid(value)
            incompatible_valid = valid and bad_reader.is_valid(value)
        except (ValueError, decimal.InvalidOperation):
            valid = reader_valid = incompatible_valid = False
        artifacts[f"{name}.payload.json"] = raw.encode()
        artifacts[f"{name}.frame.bin"] = header + raw.encode()
        records.append({"name": name, "writer_valid": valid, "reader_valid": reader_valid,
                        "incompatible_reader_valid": incompatible_valid})
        rust_lines.append('    Case { name: "' + name + '", payload: include_str!("' + name +
                          '.payload.json"), frame: include_bytes!("' + name + '.frame.bin"), valid: ' +
                          str(valid).lower() + ', incompatible_valid: ' + str(incompatible_valid).lower() + ' },')
        if args.rust_output and valid:
            actual = (args.rust_output / f"{name}.frame.bin").read_bytes()
            assert actual == header + raw.encode(), name
            actual_value = json.loads(actual[5:].decode(), parse_float=exact_number,
                                      parse_constant=forbidden_constant)
            assert w.is_valid(actual_value) and r.is_valid(actual_value), name
            reverse += 1

    artifacts["cases.rs"] = ('const CASES: &[Case] = &[\n' + '\n'.join(rust_lines) + '\n];\n').encode()
    manifest = {"peer": "python-jsonschema", "version": "4.26.0", "python": lock["python"],
                "profile": "partitionline.json-schema.draft2020-12.v1", "dialect": DIALECT,
                "references": "explicit offline Registry; missing resources fail; no retrieval",
                "numeric_parser": "Python arbitrary-precision int; Decimal fractional values; mathematically integral decimal tokens become int; validator unchanged",
                "defaults": "annotations only; null remains null; no data transformation",
                "cases": records, "unresolved_reference_rejected": True,
                "installed_source_sha256": source_hashes,
                "artifacts_sha256": {name: digest(data) for name, data in sorted(artifacts.items())},
                "limits": "offline fixtures only; Rust codec recognizes these schemas only; no built-in production validator or registry qualification"}
    artifacts["manifest.json"] = serialized(manifest)
    if args.verify:
        for name, data in artifacts.items():
            assert (args.output / name).read_bytes() == data, f"fixture drift: {name}"
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        for name, data in artifacts.items():
            (args.output / name).write_bytes(data)
    print(json.dumps({"fixtures_verified": len(artifacts), "cases": len(cases),
                      "valid": sum(x["writer_valid"] for x in records),
                      "invalid": sum(not x["writer_valid"] for x in records),
                      "reverse_rust_cases": reverse, "peer": "jsonschema 4.26.0"}))


if __name__ == "__main__":
    main()

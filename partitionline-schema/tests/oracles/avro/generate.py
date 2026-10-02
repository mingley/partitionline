#!/usr/bin/env python3
"""Pinned independent Apache Avro datum fixtures and actual Rust-output checks."""

import argparse
import hashlib
import importlib.metadata
import io
import json
import platform
from pathlib import Path
import zipfile

VERSION = "1.12.1"
PYTHON = "3.12.14"
WHEEL_SHA256 = "970475dd6457924533966fe761be607c759d5a48390cc8fbed472f7c9a8868f2"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def serialized(value):
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def main():
    if not __debug__:
        raise SystemExit("run without -O so peer/fixture assertions remain enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--rust-output", type=Path)
    args = parser.parse_args()
    assert platform.python_version() == PYTHON, "unexpected Python version"
    assert digest(args.wheel.read_bytes()) == WHEEL_SHA256, "unexpected peer wheel"
    assert importlib.metadata.version("avro") == VERSION, "unexpected peer version"
    import avro
    import avro.errors
    import avro.io
    import avro.name
    import avro.schema

    package = Path(avro.__file__).parent
    sources = {}
    with zipfile.ZipFile(args.wheel) as wheel:
        for name in sorted(wheel.namelist()):
            if name.startswith("avro/") and name.endswith(".py"):
                source = wheel.read(name)
                assert (package.parent / name).read_bytes() == source, name
                sources[name] = digest(source)

    metadata_v1 = {
        "type": "record", "name": "Metadata", "namespace": "common",
        "fields": [{"name": "source", "type": "string"}],
    }
    metadata_v2 = {**metadata_v1, "fields": metadata_v1["fields"] + [
        {"name": "active", "type": "boolean", "default": True},
    ]}
    writer_json = {
        "type": "record", "name": "Event", "namespace": "example",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "metadata", "type": "common.Metadata"},
            {"name": "note", "type": ["null", "string"], "default": None},
        ],
    }
    reader_json = {**writer_json, "fields": [
        writer_json["fields"][1],
        {"name": "id", "type": "long"},
        writer_json["fields"][2],
        {"name": "status", "type": "string", "default": "new"},
        {"name": "tag", "type": ["null", "string"], "default": None},
    ]}
    incompatible_json = {**writer_json, "fields": [
        {"name": "id", "type": "string"}, *writer_json["fields"][1:],
    ]}
    required_json = {**writer_json, "fields": writer_json["fields"] + [
        {"name": "required", "type": "string"},
    ]}

    def schema(root, reference):
        names = avro.name.Names()
        avro.schema.make_avsc_object(reference, names)
        return avro.schema.make_avsc_object(root, names)

    writer = schema(writer_json, metadata_v1)
    reader = schema(reader_json, metadata_v2)
    incompatible = schema(incompatible_json, metadata_v1)
    required = schema(required_json, metadata_v1)
    try:
        avro.schema.make_avsc_object(writer_json, avro.name.Names())
    except avro.errors.SchemaParseException:
        pass
    else:
        raise AssertionError("unresolved reference accepted")

    inputs = {
        "null": {"id": 1, "metadata": {"source": "reference.avsc"}, "note": None},
        "present": {"id": 300, "metadata": {"source": "producer"}, "note": "hello 雪"},
        "negative": {"id": -(2**31), "metadata": {"source": "雪"}, "note": ""},
        "intmax": {"id": 2**31 - 1, "metadata": {"source": "boundary"}, "note": "large"},
    }
    artifacts = {
        "writer.avsc": serialized(writer_json),
        "reader.avsc": serialized(reader_json),
        "metadata-v1.avsc": serialized(metadata_v1),
        "metadata-v2.avsc": serialized(metadata_v2),
        "incompatible.avsc": serialized(incompatible_json),
        "required.avsc": serialized(required_json),
    }
    header = b"\0" + (42).to_bytes(4, "big")
    reverse_count = 0

    def decode(payload, selected_reader):
        stream = io.BytesIO(payload)
        value = avro.io.DatumReader(writer, selected_reader).read(avro.io.BinaryDecoder(stream))
        assert stream.tell() == len(payload), "trailing datum"
        return value

    for name, value in inputs.items():
        stream = io.BytesIO()
        avro.io.DatumWriter(writer).write(value, avro.io.BinaryEncoder(stream))
        payload = stream.getvalue()
        evolved = {
            **value, "metadata": {**value["metadata"], "active": True},
            "status": "new", "tag": None,
        }
        assert decode(payload, writer) == value
        assert decode(payload, reader) == evolved
        for bad_reader in [incompatible, required]:
            try:
                decode(payload, bad_reader)
            except avro.errors.SchemaResolutionException:
                pass
            else:
                raise AssertionError("incompatible reader accepted")
        artifacts[f"{name}.payload.bin"] = payload
        artifacts[f"{name}.frame.bin"] = header + payload
        artifacts[f"{name}.writer.json"] = serialized(value)
        artifacts[f"{name}.reader.json"] = serialized(evolved)
        if args.rust_output:
            frame = (args.rust_output / f"{name}.frame.bin").read_bytes()
            actual = (args.rust_output / f"{name}.payload.bin").read_bytes()
            assert frame[:5] == header and frame[5:] == actual, name
            assert actual == payload, name
            assert decode(actual, writer) == value, name
            assert decode(actual, reader) == evolved, name
            reverse_count += 1

    # A primitive null is a valid empty Avro datum, hence a five-byte frame.
    null_schema = avro.schema.parse('"null"')
    stream = io.BytesIO()
    avro.io.DatumWriter(null_schema).write(None, avro.io.BinaryEncoder(stream))
    assert stream.getvalue() == b""
    artifacts["root-null.frame.bin"] = header
    artifacts["root-null.payload.bin"] = b""
    if args.rust_output:
        frame = (args.rust_output / "root-null.frame.bin").read_bytes()
        actual = (args.rust_output / "root-null.payload.bin").read_bytes()
        assert frame == header and actual == b""
        assert avro.io.DatumReader(null_schema).read(avro.io.BinaryDecoder(io.BytesIO(actual))) is None
        reverse_count += 1

    manifest = {
        "peer": "Apache Avro Python", "version": VERSION,
        "license": "Apache-2.0", "python": PYTHON,
        "distribution": f"avro-{VERSION}-py2.py3-none-any.whl",
        "distribution_sha256": WHEEL_SHA256, "source_sha256": sources,
        "wire": "Confluent magic 0 + u32 big-endian writer ID 42 + raw binary datum",
        "schema_resolution": [
            "named common.Metadata resolved independently in writer/reader namespaces",
            "field order changes and int-to-long promotion",
            "reader-added string, null-union and referenced boolean defaults",
            "null and present string union branches, UTF-8, int32 min/max",
            "int-to-string and required-field-without-default reject at datum decode",
            "unresolved named reference rejects at schema parse",
        ],
        "inputs": inputs,
        "artifacts_sha256": {name: digest(data) for name, data in sorted(artifacts.items())},
        "scope": "offline datum interoperability; fixture-only Rust codec; no registry/broker",
    }
    artifacts["manifest.json"] = serialized(manifest)
    if args.verify:
        for name, data in artifacts.items():
            assert (args.output / name).read_bytes() == data, f"fixture drift: {name}"
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        for name, data in artifacts.items():
            (args.output / name).write_bytes(data)
    print(json.dumps({"fixtures_verified": len(artifacts), "datum_cases": 5,
                      "reverse_rust_cases": reverse_count, "peer": VERSION}))


if __name__ == "__main__":
    main()

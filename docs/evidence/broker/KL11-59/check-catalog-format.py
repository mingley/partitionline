#!/usr/bin/env python3
"""Independent bounded Python reader for the custom journal/catalog format.

This checks concrete bytes emitted by Rust on stable/MSRV. It is not an Apache
metadata-log reader and does not certify filesystem or hardware durability.
"""
import argparse
import hashlib
import json
from pathlib import Path


def require(value, reason):
    if not value:
        raise ValueError(reason)


def crc32c(data):
    value = 0xffffffff
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def integer(data, start, length):
    return int.from_bytes(data[start:start + length], "big")


def inspect(path):
    require(path.stat().st_size <= 16 * 1024 * 1024, "file byte budget")
    data = path.read_bytes()
    require(len(data) >= 24 and data[:8] == b"PLJRNL01", "journal magic/header")
    require(data[8:20] == bytes(12), "journal base offset/reserved fields")
    require(crc32c(data[:20]) == integer(data, 20, 4), "journal header CRC")
    position = 24
    records, ids, live = [], set(), {}
    operations, replay_bytes = 0, 0
    while position < len(data):
        require(operations < 8192, "operation budget")
        header = data[position:position + 32]
        require(len(header) == 32 and header[:8] == b"PLENTRY1", "entry magic/header")
        require(crc32c(header[:28]) == integer(header, 28, 4), "entry header CRC")
        length, offset, count = integer(header, 8, 4), integer(header, 12, 8), integer(header, 20, 4)
        require(34 <= length <= 283 and offset == operations and count == 1, "catalog entry length/offset/count")
        payload = data[position + 32:position + 32 + length]
        require(len(payload) == length, "incomplete entry")
        require(crc32c(payload) == integer(header, 24, 4), "payload CRC")
        require(payload[:8] == b"PLTCAT01" and payload[9:12] == bytes(3), "catalog magic/reserved flags")
        opcode, identity = payload[8], integer(payload, 12, 16)
        partitions, name_length = integer(payload, 28, 4), integer(payload, 32, 2)
        require(identity > 1 and name_length <= 249 and length == 34 + name_length, "catalog identity/length")
        if opcode == 1:
            name = payload[34:].decode("ascii")
            require(name and name not in (".", "..", "__cluster_metadata") and
                    all(char.isascii() and (char.isalnum() or char in "._-") for char in name), "illegal name")
            require(0 < partitions <= 10000, "partition count/budget")
            require(identity not in ids, "reused identity")
            require(name not in live, "duplicate live name")
            require(all(name.replace(".", "_") != existing.replace(".", "_") for existing in live), "name collision")
            ids.add(identity)
            live[name] = {"name": name, "id": identity, "partitions": partitions}
            records.append({"operation": "create", "id": identity, "name": name, "partitions": partitions})
        elif opcode == 2:
            require(name_length == 0 and partitions == 0, "invalid tombstone fields")
            names = [name for name, topic in live.items() if topic["id"] == identity]
            require(len(names) == 1, "unknown tombstone identity")
            del live[names[0]]
            records.append({"operation": "delete", "id": identity})
        else:
            raise ValueError("unknown opcode")
        operations += 1
        replay_bytes += length
        position += 32 + length
        require(len(live) <= 1024 and len(ids) <= 4096 and
                sum(topic["partitions"] for topic in live.values()) <= 100000 and
                replay_bytes <= 4 * 1024 * 1024, "state/replay budget")
    live_ids = {topic["id"] for topic in live.values()}
    return {"operations": records, "topics": sorted(live.values(), key=lambda topic: topic["name"]),
            "tombstones": sorted(ids - live_ids), "identities": len(ids),
            "total_partitions": sum(topic["partitions"] for topic in live.values()),
            "journal_bytes": len(data), "replay_bytes": replay_bytes}


EXPECTED = {
    "operations": [{"operation": "create", "id": 2, "name": "alpha", "partitions": 3},
                   {"operation": "create", "id": 3, "name": "beta", "partitions": 2},
                   {"operation": "delete", "id": 2},
                   {"operation": "create", "id": 4, "name": "alpha", "partitions": 6}],
    "topics": [{"name": "alpha", "id": 4, "partitions": 6}, {"name": "beta", "id": 3, "partitions": 2}],
    "tombstones": [2], "identities": 3, "total_partitions": 8, "journal_bytes": 302, "replay_bytes": 150,
}


def refresh(data, start):
    length = integer(data, start + 8, 4)
    payload = data[start + 32:start + 32 + length]
    data[start + 24:start + 28] = crc32c(payload).to_bytes(4, "big")
    data[start + 28:start + 32] = crc32c(data[start:start + 28]).to_bytes(4, "big")


def run(folder):
    # Independent known test history, not a state reconstructed by Rust itself.
    positives = []
    for toolchain in ("stable", "1.85.0"):
        path = folder / ("catalog-history-" + toolchain + ".bin")
        observed = inspect(path)
        require(observed == EXPECTED, "golden history mismatch")
        positives.append({"toolchain": toolchain, "file": path.name,
                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": observed})
    require(positives[0]["sha256"] == positives[1]["sha256"], "stable/MSRV byte mismatch")
    original = (folder / "catalog-history-stable.bin").read_bytes()
    starts, position = [], 24
    while position < len(original):
        starts.append(position)
        position += 32 + integer(original, position + 8, 4)
    rejected = []
    for case in ("payload-crc", "reused-id", "illegal-name", "record-count", "unknown-delete"):
        changed = bytearray(original)
        if case == "payload-crc":
            changed[starts[0] + 32 + 34] ^= 1
        elif case == "reused-id":
            changed[starts[3] + 32 + 12:starts[3] + 32 + 28] = (2).to_bytes(16, "big")
            refresh(changed, starts[3])
        elif case == "illegal-name":
            changed[starts[0] + 32 + 34:starts[0] + 32 + 39] = b"../.."
            refresh(changed, starts[0])
        elif case == "record-count":
            changed[starts[0] + 20:starts[0] + 24] = (2).to_bytes(4, "big")
            refresh(changed, starts[0])
        else:
            changed[starts[2] + 32 + 12:starts[2] + 32 + 28] = (9).to_bytes(16, "big")
            refresh(changed, starts[2])
        path = folder / ("format-mutant-" + case + ".bin")
        path.write_bytes(changed)
        try:
            inspect(path)
        except ValueError as error:
            rejected.append({"case": case, "file": path.name, "verdict": "failed",
                             "expected_rejection": True, "error": str(error),
                             "sha256": hashlib.sha256(changed).hexdigest()})
        else:
            raise AssertionError("corrupted history accepted: " + case)
    report = {"schema_version": 1, "verdict": "passed", "format": "custom PLJRNL01/PLTCAT01; not Kafka metadata log",
              "reader_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "positive_histories": positives, "expected_rejections": rejected,
              "limitations": ["Independent byte/history interpretation, not a synchronization, power-loss, replica or wire compatibility proof."]}
    (folder / "independent-format.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Two independently decoded stable/MSRV histories match; all five deliberate format/history mutants rejected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--folder", type=Path, default=Path(__file__).resolve().parent)
    run(parser.parse_args().folder)

#!/usr/bin/env python3
"""Independent KRaft body layout from the pinned 4.3.1 field order.

This writer does not call the broker crate. It packs unsigned varints,
compact strings, and tagged fields in schema order, including the empty
tagged-field suffix of a nested struct.
"""

from pathlib import Path


def uvarint(value: int) -> bytes:
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def compact_string(value: str | None) -> bytes:
    if value is None:
        return bytes([0])
    raw = value.encode()
    return uvarint(len(raw) + 1) + raw


def i32(value: int) -> bytes:
    return value.to_bytes(4, "big", signed=True)


def i64(value: int) -> bytes:
    return value.to_bytes(8, "big", signed=True)


def tagged(tag: int, payload: bytes) -> bytes:
    return uvarint(1) + uvarint(tag) + uvarint(len(payload)) + payload


def vote(version: int) -> bytes:
    out = bytearray()
    out += compact_string("cluster-1")
    if version >= 1:
        out += i32(1)
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += i32(4 if version == 2 else 1)
    out += i32(2)
    if version >= 1:
        out += bytes([7]) * 16
        out += bytes([8]) * 16
    out += i32(1 if version == 2 else 0)
    out += i64(3 if version == 2 else 0)
    if version >= 2:
        out += bytes([1])
    out += bytes([0, 0, 0])
    return bytes(out)


def fetch_snapshot_v1() -> bytes:
    directory = bytes([7]) * 16
    cluster = compact_string("cluster-1")
    out = bytearray()
    out += i32(3)
    out += i32(1024)
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += i32(2)
    out += i64(10)
    out += i32(2)
    out += bytes([0])  # SnapshotId tagged fields
    out += i64(0)
    out += tagged(0, directory)
    out += bytes([0])  # topic tagged fields
    out += tagged(0, cluster)
    return bytes(out)


def main() -> None:
    root = Path(__file__).resolve().parent
    (root / "vote-v0.bin").write_bytes(vote(0))
    (root / "vote-v2.bin").write_bytes(vote(2))
    (root / "fetch-snapshot-v1.bin").write_bytes(fetch_snapshot_v1())


if __name__ == "__main__":
    main()

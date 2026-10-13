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


def endpoint(name: str, host: str, port: int) -> bytes:
    return compact_string(name) + compact_string(host) + port.to_bytes(2, "big") + bytes([0])


def begin_v1() -> bytes:
    out = bytearray()
    out += compact_string("cluster-1")
    out += i32(1)
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += bytes([6]) * 16
    out += i32(2)
    out += i32(3)
    out += bytes([0, 0])
    out += bytes([2])
    out += endpoint("CONTROLLER", "localhost", 9093)
    out += bytes([0])
    return bytes(out)


def end_v1() -> bytes:
    out = bytearray()
    out += compact_string("cluster-1")
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += i32(2)
    out += i32(3)
    out += bytes([2])
    out += i32(1)
    out += bytes([6]) * 16
    out += bytes([0])
    out += bytes([0, 0])
    out += bytes([2])
    out += endpoint("CONTROLLER", "localhost", 9093)
    out += bytes([0])
    return bytes(out)


def update_voter() -> bytes:
    out = bytearray()
    out += compact_string("cluster-1")
    out += i32(3)
    out += i32(2)
    out += bytes([7]) * 16
    out += bytes([2])
    out += endpoint("CONTROLLER", "localhost", 9093)
    out += (0).to_bytes(2, "big", signed=True)
    out += (1).to_bytes(2, "big", signed=True)
    out += bytes([0, 0])
    return bytes(out)


def voters_record() -> bytes:
    out = bytearray()
    out += (0).to_bytes(2, "big", signed=True)
    out += bytes([2])
    out += i32(2)
    out += bytes([7]) * 16
    out += bytes([2])
    out += endpoint("CONTROLLER", "localhost", 9093)
    out += (0).to_bytes(2, "big", signed=True)
    out += (1).to_bytes(2, "big", signed=True)
    out += bytes([0, 0])
    out += bytes([0])
    return bytes(out)


def fetch_snapshot_response_v1() -> bytes:
    records = b"snap"
    leader = i32(2) + i32(1) + bytes([0])
    endpoints = bytes([2]) + i32(2) + compact_string("localhost") + (9093).to_bytes(2, "big") + bytes([0])
    out = bytearray()
    out += i32(0)
    out += (0).to_bytes(2, "big", signed=True)
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += (0).to_bytes(2, "big", signed=True)
    out += i64(4)
    out += i32(1)
    out += bytes([0])
    out += i64(4)
    out += i64(0)
    out += uvarint(len(records) + 1) + records
    out += tagged(0, leader)
    out += bytes([0])
    out += tagged(0, endpoints)
    return bytes(out)


def describe_quorum_v2() -> bytes:
    out = bytearray()
    out += (0).to_bytes(2, "big", signed=True)
    out += bytes([0])
    out += bytes([2])
    out += compact_string("__cluster_metadata")
    out += bytes([2])
    out += i32(0)
    out += (0).to_bytes(2, "big", signed=True)
    out += bytes([0])
    out += i32(2)
    out += i32(3)
    out += i64(9)
    out += bytes([2])
    out += i32(2)
    out += bytes([8]) * 16
    out += i64(10)
    out += i64(1)
    out += i64(2)
    out += bytes([0])
    out += bytes([1])
    out += bytes([0, 0])
    out += bytes([2])
    out += i32(2)
    out += bytes([2])
    out += endpoint("plaintext", "127.0.0.1", 9093)
    out += bytes([0])
    out += bytes([0])
    return bytes(out)


def main() -> None:
    root = Path(__file__).resolve().parent
    (root / "vote-v0.bin").write_bytes(vote(0))
    (root / "vote-v2.bin").write_bytes(vote(2))
    (root / "fetch-snapshot-v1.bin").write_bytes(fetch_snapshot_v1())
    (root / "begin-v1.bin").write_bytes(begin_v1())
    (root / "end-v1.bin").write_bytes(end_v1())
    (root / "update-voter.bin").write_bytes(update_voter())
    (root / "voters-record.bin").write_bytes(voters_record())
    (root / "fetch-snapshot-response-v1.bin").write_bytes(fetch_snapshot_response_v1())
    (root / "describe-quorum-v2.bin").write_bytes(describe_quorum_v2())


if __name__ == "__main__":
    main()

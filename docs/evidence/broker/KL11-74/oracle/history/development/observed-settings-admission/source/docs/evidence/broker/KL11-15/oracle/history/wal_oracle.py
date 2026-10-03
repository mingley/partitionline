#!/usr/bin/env python3
"""Independent, bounded replay of metadata operation journals.

This tool reads the actual Journal bytes. It imports no broker implementation
and does not treat a reported source SHA or an emitted verdict as validation.
The format follows runtime/trace-schema-proposal.json; causal network checks
are separate from this durable content decoder.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import struct
from dataclasses import dataclass
from pathlib import Path

MAX_FILE = 256 * 1024 * 1024
MAX_OPERATION = 4 * 1024 * 1024
MAX_OPERATIONS = 65_536
MAX_ENTRIES = 4096
MAX_LIVE_BYTES = 64 * 1024 * 1024
MAX_RECORD = 1024 * 1024
MAX_TERM = 1 << 31


class Rejected(ValueError):
    """The input violates a structural or durable-content invariant."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Rejected(message)


def crc_table() -> tuple[int, ...]:
    # Castagnoli reflected polynomial; no dependency on the Rust CRC library.
    rows = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
        rows.append(value)
    return tuple(rows)


CRC_TABLE = crc_table()


def crc32c(data: bytes) -> int:
    value = 0xffffffff
    for byte in data:
        value = (value >> 8) ^ CRC_TABLE[(value ^ byte) & 255]
    return value ^ 0xffffffff


class Cursor:
    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        require(0 <= count <= len(self.data) - self.offset, "truncated operation")
        result = self.data[self.offset:self.offset + count]
        self.offset += count
        return result

    def number(self, format_: str) -> int:
        return struct.unpack(format_, self.take(struct.calcsize(format_)))[0]

    def zero(self, count: int) -> None:
        require(self.take(count) == bytes(count), "nonzero reserved field")

    def finish(self) -> None:
        require(self.offset == len(self.data), "operation trailing bytes")


@dataclass(frozen=True)
class Record:
    index: int
    term: int
    kind: int
    payload: bytes

    def receipt(self) -> dict:
        return {"index": self.index, "term": self.term, "kind": self.kind,
                "payload_hex": self.payload.hex(),
                "sha256": hashlib.sha256(self.payload).hexdigest()}


class Replay:
    def __init__(self, expected_node: int | None = None, expected_group: dict | None = None):
        self.expected_node = expected_node
        self.expected_group = expected_group
        self.group: dict | None = None
        self.local: int | None = None
        self.records: list[Record] = []
        self.committed = 0
        self.max_term = 0
        self.operations: list[dict] = []
        self.live_bytes = 0
        self.journal_sha256: str | None = None
        self.journal_bytes = 0

    def apply(self, payload: bytes) -> None:
        require(len(self.operations) < MAX_OPERATIONS, "operation count ceiling")
        reader = Cursor(payload)
        require(reader.take(8) == b"PLREPL01", "operation magic")
        opcode = reader.number(">B")
        reader.zero(7)
        require(opcode in (1, 2, 3, 4), "unknown operation")
        if opcode == 1:
            require(not self.operations, "repeated identity operation")
            local, partition = reader.number(">I"), reader.number(">i")
            count, cluster_length, topic_length = (reader.number(">H") for _ in range(3))
            reader.zero(2)
            require(1 <= count <= 64 and 1 <= cluster_length <= 249 and
                    1 <= topic_length <= 249 and partition >= 0, "identity limits")
            voters = [reader.number(">I") for _ in range(count)]
            require(voters == sorted(set(voters)) and local in voters and
                    all(v <= 0x7fffffff for v in voters), "voter identity")
            try:
                cluster = reader.take(cluster_length).decode("utf-8")
                topic = reader.take(topic_length).decode("utf-8")
            except UnicodeDecodeError as error:
                raise Rejected("identity UTF-8") from error
            group = {"cluster_id": cluster, "topic": topic,
                     "partition": partition, "voters": voters}
            require(self.expected_node is None or self.expected_node == local, "foreign local ID")
            require(self.expected_group is None or self.expected_group == group, "foreign group")
            self.local, self.group = local, group
            receipt = {"opcode": "init", "node_id": local, "group": group}
        else:
            require(self.group is not None, "operation before identity")
            if opcode == 2:
                start, count = reader.number(">Q"), reader.number(">I")
                reader.zero(4)
                require(start == len(self.records) + 1 and 1 <= count <= MAX_ENTRIES and
                        len(self.records) + count <= MAX_ENTRIES, "append positions/count")
                previous = self.records[-1].term if self.records else 0
                rows = []
                for number in range(count):
                    term, kind = reader.number(">Q"), reader.number(">B")
                    reader.zero(7)
                    length = reader.number(">I")
                    reader.zero(4)
                    require(previous <= term <= MAX_TERM and term > 0 and kind in (0, 1),
                            "record term/kind")
                    require((kind == 0 and 1 <= length <= MAX_RECORD) or
                            (kind == 1 and length == 0), "record payload length")
                    require(self.live_bytes + length <= MAX_LIVE_BYTES, "live payload ceiling")
                    row = Record(start + number, term, kind, reader.take(length))
                    self.records.append(row)
                    self.live_bytes += length
                    self.max_term = max(self.max_term, term)
                    rows.append({"index": row.index, "term": row.term, "kind": row.kind,
                                 "payload_bytes": length,
                                 "sha256": hashlib.sha256(row.payload).hexdigest()})
                    previous = term
                receipt = {"opcode": "append", "start_index": start, "records": rows}
            elif opcode == 3:
                target, old_term, old_index, term, floor = (reader.number(">Q") for _ in range(5))
                require(self.records and (old_term, old_index) ==
                        (self.records[-1].term, len(self.records)), "truncate expected tail")
                require(floor == self.committed and floor <= target < old_index and
                        old_term < term <= MAX_TERM and term >= self.max_term,
                        "committed or stale truncation")
                self.records = self.records[:target]
                self.live_bytes = sum(len(row.payload) for row in self.records)
                self.max_term = max(self.max_term, term)
                receipt = {"opcode": "truncate", "target_index": target,
                           "leader_term": term, "committed_floor": floor}
            else:
                commit, term, authorizer = reader.number(">Q"), reader.number(">Q"), reader.number(">I")
                authority = reader.number(">B")
                reader.zero(3)
                sequence = reader.number(">Q")
                require(self.committed < commit <= len(self.records) and
                        1 <= term <= MAX_TERM and authorizer in self.group["voters"] and
                        authority in (0, 1) and self.records[commit - 1].term <= term and
                        term >= self.max_term,
                        "commit position/authority")
                if authority == 0:
                    require(authorizer == self.local and sequence == 0 and
                            self.records[commit - 1].term == term, "old-term local commit")
                else:
                    require(authorizer != self.local and sequence > 0, "follower authority")
                self.committed = commit
                self.max_term = max(self.max_term, term)
                receipt = {"opcode": "commit", "committed_end": commit,
                           "leader_term": term, "authorizer_id": authorizer,
                           "authority": authority, "request_sequence": sequence}
        reader.finish()
        receipt["operation_number"] = len(self.operations)
        self.operations.append(receipt)

    def receipt(self) -> dict:
        return {"node_id": self.local, "group": self.group,
                "operation_count": len(self.operations), "last_index": len(self.records),
                "last_term": self.records[-1].term if self.records else 0,
                "max_observed_term": self.max_term, "committed_end": self.committed,
                "live_payload_bytes": self.live_bytes,
                "committed_records": [row.receipt() for row in self.records[:self.committed]],
                "operations": self.operations}


def read_journal(path: Path, node: int | None = None, group: dict | None = None) -> Replay:
    require(24 <= path.stat().st_size <= MAX_FILE, "journal size ceiling/header")
    replay = Replay(node, group)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        header = stream.read(24)
        digest.update(header)
        replay.journal_bytes = 24
        require(header[:8] == b"PLJRNL01" and header[8:16] == bytes(8) and
                header[16:20] == bytes(4) and crc32c(header[:20]) ==
                int.from_bytes(header[20:24], "big"), "journal header identity/CRC")
        offset = 0
        while True:
            header = stream.read(32)
            if not header:
                break
            require(len(header) == 32, "partial journal entry header")
            digest.update(header)
            require(header[:8] == b"PLENTRY1" and crc32c(header[:28]) ==
                    int.from_bytes(header[28:32], "big"), "journal entry header CRC")
            length, first, count, checksum = struct.unpack(">IQII", header[8:28])
            require(1 <= length <= MAX_OPERATION and first == offset and count == 1,
                    "operation journal length/offset/count")
            require(replay.journal_bytes + 32 + length <= MAX_FILE, "growing journal size ceiling")
            payload = stream.read(length)
            digest.update(payload)
            replay.journal_bytes += 32 + len(payload)
            require(len(payload) == length and crc32c(payload) == checksum,
                    "partial or corrupt journal payload")
            replay.apply(payload)
            offset += 1
    require(replay.group is not None, "missing group identity")
    replay.journal_sha256 = digest.hexdigest()
    return replay


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--node", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(crc32c(b"123456789") == 0xe3069283, "published Castagnoli check vector")
    replay = read_journal(args.journal, args.node)
    result = replay.receipt()
    result.update(journal_path=str(args.journal),
                  journal_sha256=replay.journal_sha256, journal_bytes=replay.journal_bytes)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()

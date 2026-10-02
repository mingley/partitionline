#!/usr/bin/env python3
"""Independently check actual partition output, CRCs and Apache record histories."""
import argparse
import hashlib
import json
from pathlib import Path
import struct


def crc32c(data):
    value = 0xffffffff
    for byte in data:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def histories(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def check(proof, fixtures, oracle):
    assert crc32c(b"123456789") == 0xe3069283
    whole = (proof / "partition.journal").read_bytes()
    assert whole[:8] == b"PLJRNL01"
    base, flags, header_crc = struct.unpack(">QII", whole[8:24])
    assert (base, flags, header_crc) == (7, 0, crc32c(whole[:20]))
    cursor, next_offset, entries, batches = 24, base, [], []
    for name, count in [("multiple", 4), ("basic", 1)]:
        header = whole[cursor:cursor + 32]
        assert len(header) == 32 and header[:8] == b"PLENTRY1"
        size, first, actual_count, payload_crc, protected_crc = struct.unpack(">IQIII", header[8:])
        assert first == next_offset and actual_count == count
        assert protected_crc == crc32c(header[:28])
        payload = whole[cursor + 32:cursor + 32 + size]
        assert len(payload) == size and payload_crc == crc32c(payload)
        assert payload == (proof / f"assigned-{name}.bin").read_bytes()
        original = (fixtures / f"valid-{'multiple-batches' if name == 'multiple' else name}.bin").read_bytes()
        assert len(payload) == len(original)
        batch_cursor, batch_next = 0, first
        while batch_cursor < size:
            assigned_base, length = struct.unpack(">qi", payload[batch_cursor:batch_cursor + 12])
            assert 49 <= length <= size - batch_cursor - 12
            end = batch_cursor + 12 + length
            batch = payload[batch_cursor:end]
            assert assigned_base == batch_next and batch[16] == 2
            assert batch[8:] == original[batch_cursor + 8:end]
            assert struct.unpack(">I", batch[17:21])[0] == crc32c(batch[21:])
            records = struct.unpack(">i", batch[57:61])[0]
            last_delta = struct.unpack(">i", batch[23:27])[0]
            assert records > 0 and last_delta == records - 1
            batches.append({"base": assigned_base, "records": records, "bytes": len(batch)})
            batch_next += records
            batch_cursor = end
        assert batch_cursor == size and batch_next == first + count
        rows = histories(oracle / f"assigned-{name}.jsonl")
        source_rows = histories(oracle / f"original-{name}.jsonl")
        actual_records = [row for row in rows if row["kind"] == "record"]
        expected_records = [row for row in source_rows if row["kind"] == "record"]
        assert len(actual_records) == len(expected_records) == count
        for index, (actual, expected) in enumerate(zip(actual_records, expected_records)):
            assert actual["offset"] == first + index
            assert {k: v for k, v in actual.items() if k != "offset"} == {
                k: v for k, v in expected.items() if k != "offset"}
        batch_rows = [row for row in rows if row["kind"] == "batch"]
        actual_batches = batches[-len(batch_rows):]
        assert [{"kind": "batch", "base": b["base"], "last": b["base"] + b["records"] - 1}
                for b in actual_batches] == batch_rows
        entries.append({"first": first, "records": count, "payload_bytes": size})
        cursor += 32 + size
        next_offset = first + count
    assert cursor == len(whole) == 350 and next_offset == 12
    return {"journal_bytes": len(whole), "next_offset": next_offset, "entries": entries,
            "batches": batches, "verified_record_offsets": list(range(7, 12)),
            "sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in sorted(proof.iterdir()) if p.is_file()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(check(args.proof, args.fixtures, args.oracle), indent=2))


if __name__ == "__main__":
    main()

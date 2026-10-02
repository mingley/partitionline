#!/usr/bin/env python3
"""Independently verify the small actual Rust journal artifact; no dependencies."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import struct


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def main():
    if not __debug__:
        raise SystemExit("run without -O so validation assertions remain enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--source-sha")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.source_sha is not None:
        assert len(args.source_sha) == 40 and all(c in "0123456789abcdef" for c in args.source_sha)
    assert crc32c(b"123456789") == 0xE3069283
    with args.journal.open("rb") as journal:
        data = journal.read(1025)
    assert 24 <= len(data) <= 1024, "artifact exceeds bounded checker input"
    assert data[:8] == b"PLJRNL01"
    base, flags, header_crc = struct.unpack_from(">QII", data, 8)
    assert flags == 0 and header_crc == crc32c(data[:20])
    assert base == 7
    offset = base
    position = 24
    expected = [(3, b"first-payload"), (2, b"second-payload")]
    decoded = []
    for count, payload in expected:
        header = data[position:position + 32]
        assert len(header) == 32 and header[:8] == b"PLENTRY1"
        length, first, records, payload_crc, entry_crc = struct.unpack_from(">IQIII", header, 8)
        assert 1 <= length <= 1024 and records > 0
        assert first == offset and records == count
        assert entry_crc == crc32c(header[:28])
        actual = data[position + 32:position + 32 + length]
        assert len(actual) == length and actual == payload
        assert payload_crc == crc32c(actual)
        offset += records
        assert offset <= 2**64 - 1
        decoded.append({"first_offset": first, "record_count": records,
                        "payload": actual.decode("ascii"), "payload_crc32c": payload_crc})
        position += 32 + length
    assert position == len(data) and offset == 12
    result = {"source_sha": args.source_sha, "python": platform.python_version(),
              "artifact_bytes": len(data), "artifact_sha256": hashlib.sha256(data).hexdigest(),
              "entries": decoded, "base_offset": base, "next_offset": offset,
              "checker": "independent bitwise Castagnoli CRC32C, standard-library only",
              "scope": "custom journal format; no Kafka semantics/replication qualification"}
    output = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()

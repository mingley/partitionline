#!/usr/bin/env python3
"""Independent Kafka v2 record-batch fixture generator (KL04-09).

Stdlib only. Encodes fixtures per the protocol spec (NOT via
partitionline) so the Rust preflight decodes bytes our encoder never
produced. Deterministic: fixed seed, fixed timestamps.

Usage: python3 fixtures/gen.py  (writes fixtures/*.bin + manifest.json)
"""

import hashlib
import json
import os
import random
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = 0xC0DEC


def _crc32c_table():
    poly = 0x82F63B78  # reflected Castagnoli
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC_TABLE = _crc32c_table()


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def zigzag(n: int) -> int:
    return (n << 1) ^ (n >> 63)


def varint(n: int) -> bytes:
    n = zigzag(n)
    out = bytearray()
    while (n & ~0x7F) != 0:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n & 0x7F)
    return bytes(out)


def varbytes(buf: bytes | None) -> bytes:
    if buf is None:
        return varint(-1)
    return varint(len(buf)) + buf


WORDS = (
    "the quick brown fox jumps over lazy dogs while kafka streams flow "
    "through partitions keyed by order id with exactly once semantics and "
    "compacted topics retaining the latest value per key"
).split()


def text_like(rng: random.Random, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        out += WORDS[rng.randrange(len(WORDS))].encode()
        out += b" "
    return bytes(out[:n])


def encode_record(rec, base_ts: int) -> bytes:
    body = bytearray()
    body.append(0)  # attributes
    body += varint(rec["ts"] - base_ts)
    body += varint(rec["offset_delta"])
    body += varbytes(rec["key"])
    body += varbytes(rec["value"])
    headers = rec["headers"]
    body += varint(len(headers))
    for key, value in headers:
        body += varbytes(key.encode())
        body += varbytes(value)
    return varint(len(body)) + bytes(body)


def encode_batch(records, base_offset: int = 0) -> bytes:
    base_ts = records[0]["ts"]
    max_ts = max(r["ts"] for r in records)
    frames = b"".join(encode_record(r, base_ts) for r in records)
    body = struct.pack(
        ">h i q q q h i i",
        0,  # attributes: none
        len(records) - 1,  # lastOffsetDelta
        base_ts,
        max_ts,
        -1,  # producerId
        -1,  # producerEpoch
        -1,  # baseSequence
        len(records),
    ) + frames
    crc = crc32c(body)
    header = struct.pack(">q i i b I", base_offset, 4 + 1 + 4 + len(body), -1, 2, crc)
    return header + body


def make_records(rng, count, key_bytes, payload_bytes, entropy, header_count, null_header):
    records = []
    for i in range(count):
        if entropy == "random":
            key = bytes(rng.randrange(256) for _ in range(key_bytes)) if key_bytes else None
            value = bytes(rng.randrange(256) for _ in range(payload_bytes))
        else:
            key = text_like(rng, key_bytes) if key_bytes else None
            value = text_like(rng, payload_bytes)
        headers = []
        for h in range(header_count):
            hval = None if (null_header and h == 0) else text_like(rng, 8)
            headers.append((f"h{h}", hval))
        records.append(
            {
                "offset_delta": i,
                "ts": 1_700_000_000_000 + i,
                "key": key,
                "value": value,
                "headers": headers,
            }
        )
    return records


SPECS = {
    "f01": dict(count=8, key_bytes=16, payload_bytes=100, entropy="random", header_count=0, null_header=False),
    "f02": dict(count=8, key_bytes=16, payload_bytes=100, entropy="text", header_count=3, null_header=False),
    "f03": dict(count=500, key_bytes=16, payload_bytes=100, entropy="random", header_count=0, null_header=False),
    "f04": dict(count=32, key_bytes=16, payload_bytes=1024, entropy="text", header_count=2, null_header=True),
}


def main() -> int:
    manifest = {"seed": SEED, "fixtures": {}}
    for name, spec in SPECS.items():
        rng = random.Random(SEED + int(name[1:]))
        records = make_records(rng, **spec)
        blob = encode_batch(records)
        path = os.path.join(HERE, f"{name}.bin")
        with open(path, "wb") as f:
            f.write(blob)
        first, last = records[0], records[-1]
        manifest["fixtures"][name] = {
            "file": f"{name}.bin",
            "bytes": len(blob),
            "sha256": hashlib.sha256(blob).hexdigest(),
            "records": len(records),
            "key_bytes": spec["key_bytes"],
            "payload_bytes": spec["payload_bytes"],
            "entropy": spec["entropy"],
            "header_count": spec["header_count"],
            "first_key_sha256": hashlib.sha256(first["key"]).hexdigest(),
            "first_value_sha256": hashlib.sha256(first["value"]).hexdigest(),
            "last_key_sha256": hashlib.sha256(last["key"]).hexdigest(),
            "last_value_sha256": hashlib.sha256(last["value"]).hexdigest(),
            "first_ts": first["ts"],
            "last_ts": last["ts"],
        }
        print(f"{name}: {len(blob)} bytes, {len(records)} records")
    with open(os.path.join(HERE, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
        f.write("\n")
    print("manifest.json written")
    return 0


if __name__ == "__main__":
    sys.exit(main())

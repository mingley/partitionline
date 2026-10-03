"""Independent schema/literal preparation; never executes Rust or any SDK."""
import hashlib
import json
import re
import struct
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "candidate/partitionline-broker/src/security/session.rs"
MAXIMUM = 86_400_000


def compact(value):
    assert 0 <= value <= 0x7fffffff
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def encode(version, error, message, lifetime):
    # Header0 for versions0/1, Header1 for flexible version2, followed by the
    # independently specified error, nullable error text, auth bytes and INT64.
    out = struct.pack(">i", 9)
    if version == 2:
        out += b"\0"
    out += struct.pack(">h", error)
    if version == 2:
        out += b"\x01" if error == 0 else b"\0"
        out += compact(len(message) + 1)
    else:
        out += struct.pack(">h", 0 if error == 0 else -1)
        out += struct.pack(">i", len(message))
    out += message
    if version >= 1:
        out += struct.pack(">q", lifetime if error == 0 else 0)
    if version == 2:
        out += b"\0"
    return out


def decode(version, data):
    position = 0

    def take(length):
        nonlocal position
        if length < 0 or position + length > len(data):
            raise ValueError("truncated/negative length")
        result = data[position:position + length]
        position += length
        return result

    def integer(fmt):
        return struct.unpack(fmt, take(struct.calcsize(fmt)))[0]

    def varint():
        result = 0
        for index in range(5):
            value = integer(">B")
            if index == 4 and value & 0xf8:
                raise ValueError("overflowing compact length")
            result |= (value & 127) << (7 * index)
            if not value & 128:
                return result
        raise ValueError("unterminated compact length")

    if version not in (0, 1, 2) or integer(">i") != 9:
        raise ValueError("unsupported/correlation")
    if version == 2 and take(1) != b"\0":
        raise ValueError("unexpected response header tags")
    error = integer(">h")
    text_length = varint() - 1 if version == 2 else integer(">h")
    if text_length < -1 or text_length > 256:
        raise ValueError("error text cap")
    if text_length >= 0:
        take(text_length).decode("utf8")
    length = varint() - 1 if version == 2 else integer(">i")
    if not 0 <= length <= 65536:
        raise ValueError("auth bytes cap/null")
    message = take(length)
    lifetime = integer(">q") if version >= 1 else 0
    if not 0 <= lifetime <= MAXIMUM or (error != 0 and lifetime != 0):
        raise ValueError("invalid lifetime")
    if version == 2 and take(1) != b"\0":
        raise ValueError("unexpected body tags")
    if position != len(data):
        raise ValueError("trailing data")
    return error, message, lifetime


def run():
    before = SOURCE.read_bytes()
    literals = re.findall(r'\((0|1|2), "([0-9a-f]+)"\)', before.decode())
    assert len(literals) == 3
    rows = []

    def positive(name, version, wire, expected):
        assert decode(version, wire) == expected
        rows.append({"name": name, "expected": "accepted", "wire_hex": wire.hex()})

    def negative(name, version, wire):
        try:
            decode(version, wire)
        except (ValueError, UnicodeDecodeError, struct.error):
            rows.append({"name": name, "expected": "rejected", "wire_hex": wire.hex()})
        else:
            raise AssertionError(name)

    for version_text, literal in literals:
        version = int(version_text)
        wire = bytes.fromhex(literal)
        assert wire == encode(version, 0, b"\xaa\xbb", 1234)
        positive(f"literal-v{version}", version, wire, (0, b"\xaa\xbb", 1234 if version else 0))
        for length in range(len(wire)):
            negative(f"truncated-v{version}-{length}", version, wire[:length])
        negative(f"trailing-v{version}", version, wire + b"\0")
        positive(f"error-forces-zero-v{version}", version, encode(version, 58, b"", 1234), (58, b"", 0))
    for version in (1, 2):
        for lifetime in (0, 1, MAXIMUM):
            positive(f"boundary-v{version}-{lifetime}", version, encode(version, 0, b"", lifetime), (0, b"", lifetime))
        for lifetime in (-1, MAXIMUM + 1):
            negative(f"lifetime-v{version}-{lifetime}", version, encode(version, 0, b"", lifetime))
        wire = bytearray(encode(version, 58, b"", 0))
        start = len(wire) - (9 if version == 2 else 8)
        wire[start:start + 8] = struct.pack(">q", 1)
        negative(f"error-positive-lifetime-v{version}", version, bytes(wire))
    negative("null-auth-bytes-v2", 2, bytes.fromhex("000000090000000100000000000000000000"))
    negative("header-tags-v2", 2, bytes.fromhex(literals[2][1])[:4] + b"\x01" + bytes.fromhex(literals[2][1])[5:])
    negative("body-tags-v2", 2, bytes.fromhex(literals[2][1])[:-1] + b"\x01")
    assert SOURCE.read_bytes() == before
    result = {
        "schema_version": 1,
        "scope": "independent Python codec/literal preparation only; no Rust/SDK/runtime",
        "actual_rust_tests": 0,
        "actual_sdk_executions": 0,
        "source_sha256": hashlib.sha256(before).hexdigest(),
        "controls": len(rows),
        "positive_controls": sum(row["expected"] == "accepted" for row in rows),
        "negative_controls": sum(row["expected"] == "rejected" for row in rows),
        "source_bytes_unchanged": True,
        "rows": rows,
    }
    path = ROOT / (sys.argv[1] if len(sys.argv) == 2 else "reference-codec-controls.json")
    assert path.parent == ROOT and len(sys.argv) <= 2
    assert not path.exists()
    path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    path.chmod(0o600)
    print(json.dumps({key: value for key, value in result.items() if key != "rows"}, sort_keys=True))


if __name__ == "__main__":
    run()

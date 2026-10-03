#!/usr/bin/env python3
"""Independently parse bounded verifier-only test journals; never print keys."""
import argparse
import base64
import hashlib
import hmac
import json
from pathlib import Path
import struct

PASSWORDS = {"user": ["pencil"], "admin": ["pencil"], "unicode": ["péncil-🔑"],
             "created-user": ["pencil", "rotated-public-fixture"],
             "native-created": ["pencil", "native-rotated-public-fixture"]}


def crc32c(raw):
    value = 0xffffffff
    for byte in raw:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (0x82f63b78 if value & 1 else 0)
    return value ^ 0xffffffff


def audit(path):
    raw = path.read_bytes()
    assert 24 <= len(raw) <= 1024 * 1024, "bounded test journal"
    assert raw[:8] == b"PLJRNL01" and raw[16:20] == bytes(4)
    assert int.from_bytes(raw[20:24], "big") == crc32c(raw[:20])
    offset = int.from_bytes(raw[8:16], "big")
    assert offset == 0
    position, history, records, salted_inputs = 24, [], {}, []
    occurrences = {}
    while position < len(raw):
        header = raw[position:position + 32]
        assert len(header) == 32 and header[:8] == b"PLENTRY1"
        length, first, count, checksum, header_crc = struct.unpack(">IQIII", header[8:])
        assert 1 <= length <= 4096 and first == offset and count == 1
        assert header_crc == crc32c(header[:28])
        payload = raw[position + 32:position + 32 + length]
        assert len(payload) == length and checksum == crc32c(payload)
        cursor = 0

        def take(length):
            nonlocal cursor
            assert 0 <= length <= len(payload) - cursor
            result = payload[cursor:cursor + length]
            cursor += length
            return result

        def number(length):
            return int.from_bytes(take(length), "big")

        assert take(8) == b"PLSASL01"
        size = number(2)
        assert 1 <= size <= 256
        user = take(size).decode("utf-8")
        assert user in PASSWORDS
        changes = number(1)
        assert 1 <= changes <= 2
        seen = set()
        for _ in range(changes):
            algorithm, present = number(1), number(1)
            assert algorithm in [1, 2] and algorithm not in seen and present in [0, 1]
            seen.add(algorithm)
            key = (user, algorithm)
            if present:
                salt_size = number(2)
                assert 16 <= salt_size <= 64
                salt = take(salt_size)
                iterations = number(4)
                assert 4096 <= iterations <= 16384
                hash_name = "sha256" if algorithm == 1 else "sha512"
                size = 32 if algorithm == 1 else 64
                stored, server = take(size), take(size)
                occurrence = occurrences.get(key, 0)
                assert occurrence < len(PASSWORDS[user]), "unexpected historical mutation"
                password = PASSWORDS[user][occurrence].encode()
                occurrences[key] = occurrence + 1
                salted = hashlib.pbkdf2_hmac(hash_name, password, salt, iterations)
                client = hmac.digest(salted, b"Client Key", hash_name)
                assert stored == hashlib.new(hash_name, client).digest(), "actual StoredKey derivation"
                assert server == hmac.digest(salted, b"Server Key", hash_name), "actual ServerKey derivation"
                assert stored != salted and server != salted
                salted_inputs.append(salted)
                records[key] = True
                history.append({"entry": offset, "user": user, "algorithm": algorithm,
                                "operation": "upsert", "iterations": iterations,
                                "salt_bytes": len(salt), "stored_key_bytes": size,
                                "server_key_bytes": size, "independent_derivation_matches": True})
            else:
                assert key in records, "deletion must have an existing verifier"
                del records[key]
                history.append({"entry": offset, "user": user, "algorithm": algorithm, "operation": "delete"})
        assert cursor == len(payload), "canonical typed payload has no extra password/proof field"
        position += 32 + length
        offset += count
    assert position == len(raw)
    forbidden = [password.encode() for values in PASSWORDS.values() for password in values] + salted_inputs
    # Public test password and computed transient import representations must
    # not appear raw, hexadecimal or base64 inside any persisted record.
    for value in forbidden:
        for representation in [value, value.hex().encode(), base64.b64encode(value)]:
            assert representation not in raw, "forbidden password/transient import in journal"
    return {"journal_sha256": hashlib.sha256(raw).hexdigest(), "journal_bytes": len(raw),
            "entries": offset, "history": history,
            "retained_identity_algorithms": [{"user": user, "algorithm": algorithm} for user, algorithm in sorted(records)],
            "crc32c_headers_and_payloads_verified": True,
            "canonical_verifier_schema_only": True, "stored_server_keys_independently_derived": True,
            "password_and_salted_password_raw_hex_base64_absent": True,
            "proof_field_absent_from_canonical_schema": True,
            "limits": "Public synthetic test users only; structural replay plus independently computed verifiers. No live auth tokens are logged or collected by this audit."}


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; all checks are asserted.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.journal)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verdict": "passed", "entries": report["entries"],
                      "canonical_verifier_schema_only": True}))


if __name__ == "__main__":
    main()

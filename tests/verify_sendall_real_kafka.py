#!/usr/bin/env python3
"""Independently compare the complete Kafka CLI output against the intended values."""
from pathlib import Path
import hashlib
import sys

counts = (32, 512, 513, 1024, 4096, 8191, 8192)
expected = [b"warm"]
for count in counts:
    expected.extend(f"{count}:{index}".encode("ascii") for index in range(count))
assert len(expected) == 22561

def verify(values):
    assert len(values) == len(expected), (len(values), len(expected))
    for index, (actual, wanted) in enumerate(zip(values, expected)):
        assert actual == wanted, (index, actual, wanted)

data = Path(sys.argv[1]).read_bytes()
assert data.endswith(b"\n")
actual = data.splitlines()
verify(actual)
mutants = {
    "missing": actual[:-1],
    "duplicate": actual + [actual[-1]],
    "wrong_value": actual[:10] + [b"wrong"] + actual[11:],
    "reordered": actual[:10] + [actual[11], actual[10]] + actual[12:],
}
for name, mutant in mutants.items():
    try:
        verify(mutant)
    except AssertionError:
        print(f"VERIFIER_REJECTED {name}")
    else:
        raise AssertionError(f"verifier accepted {name}")
print(f"INDEPENDENT_READBACK records={len(actual)} sha256={hashlib.sha256(data).hexdigest()}")

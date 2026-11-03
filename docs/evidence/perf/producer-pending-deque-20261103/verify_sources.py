#!/usr/bin/env python3
"""Verify the exact v14 source files used for the pending-queue design."""
import hashlib
import json
import sys
from pathlib import Path

expected = {
    'src/producer.rs': 'ecf14e7e89fff1d1c29faa6778061d58545f9df565eb7858561d4de58920b279',
    'src/producer/split.rs': '03e893bc8b41182cae2d35f8702cb7821a41fafab6306205d017df4bd5593ad0',
    'src/net.rs': 'f73aa7a04d3e2571f347f6a9245cf6f4df4ab218f653e71ca824e86e7893907c',
    'tests/producer_retained_backing.rs': 'ed6b5fd8646b580e238f7521c68ae82d6d51bb739ddcb87cf7d89231954fa882',
    'tests/common/mod.rs': 'c129003ccb42305c1871a685210884f5db9a5779eb2de5872224d5c8d3001c8f',
}
catalog_hash = '96741778c14d8ea03358b8ece94850cbf7f89f156c4b50bacf818fe4a2530e9a'
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

if len(sys.argv) != 7:
    raise SystemExit('usage: verify_sources.py <producer.rs> <split.rs> <net.rs> <retained-test.rs> <common/mod.rs> <fixed-inputs.json>')
catalog = Path(sys.argv[6])
if sha(catalog) != catalog_hash:
    raise SystemExit('fixed-inputs catalog hash mismatch')
catalogue = json.loads(catalog.read_text())
for (name, digest), argument in zip(expected.items(), sys.argv[1:6]):
    if catalogue.get(name) != digest or sha(Path(argument)) != digest:
        raise SystemExit(f'v14 source mismatch: {name}')
    print(f'PASS {name} {digest}')
print('Source pins only; no benchmark, Rust build or runtime behavior was verified.')

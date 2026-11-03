#!/usr/bin/env python3
"""Recheck all repaired source bytes and modes after the Python-only controls."""
import hashlib,importlib.util,json
from pathlib import Path
w=Path('/workspace/work/broker-join-inventory-investigation-20261103')
r=Path('/workspace/work/broker-join-inventory-r2-20261103')
repo=Path('/workspace/partitionline');spec=json.loads((r/'spec.json').read_text())
driver=repo/spec['legacy_driver']['path'];helper=repo/spec['ownership_helper']['path']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(driver)==spec['legacy_driver']['sha256'] and sha(helper)==spec['ownership_helper']['sha256']
s=importlib.util.spec_from_file_location('pinned_broker_driver',driver);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
expected=m.load(r/spec['base_files']['path'])
expected[spec['runtime_path']]=spec['fixed_runtime']
expected[spec['join_source_path']]=spec['join_fixed_source']
expected[spec['join_registry_path']]=spec['join_registry_fixed_source']
m.verify_source(w/'source',expected)
result={'candidate_spec_sha256':sha(r/'spec.json'),'source_file_count':len(expected),
        'expected_manifest_canonical_sha256':hashlib.sha256(json.dumps(expected,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'all_source_hashes_lengths_and_modes_verified':True,'source_directory':str(w/'source')}
(w/'after-controls-source-verified.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))

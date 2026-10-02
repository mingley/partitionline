#!/usr/bin/env python3
"""Verify independent workspace, pinned graph, registry checksums, and frozen knobs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--sha',required=True)
p.add_argument('--core-metadata',type=Path,required=True);p.add_argument('--peer-metadata',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
a=p.parse_args();root=a.source.resolve();peer=root/'benchmarks/peers/rust'
core=json.loads(a.core_metadata.read_text());metadata=json.loads(a.peer_metadata.read_text())
assert not any(pkg['name'] in ('rdkafka','rdkafka-sys','partitionline-rust-rdkafka-peer') for pkg in core['packages'])
assert len(core['workspace_members'])==1 and len(metadata['workspace_members'])==1
assert 'partitionline-rust-rdkafka-peer' not in core['workspace_members'][0]
assert 'partitionline-rust-rdkafka-peer' in metadata['workspace_members'][0]
manifest=tomllib.loads((peer/'Cargo.toml').read_text());assert manifest['workspace']=={}
for name in ['rdkafka','rdkafka-sys']:
    dependency=manifest['dependencies'][name]
    assert dependency['default-features'] is False and dependency['features']==['dynamic-linking']
lock=tomllib.loads((peer/'Cargo.lock').read_text());checksums={(pkg['name'],pkg['version']):pkg.get('checksum') for pkg in lock['package']}
nodes={node['id']:node for node in metadata['resolve']['nodes']}
packages=[]
for pkg in metadata['packages']:
    features=nodes[pkg['id']]['features']
    if pkg['name']=='rdkafka':assert pkg['version']=='0.39.0' and 'tokio' not in features and 'libz' not in features
    if pkg['name']=='rdkafka-sys':assert pkg['version']=='4.10.0+2.12.1' and features==['dynamic-linking']
    packages.append(dict(name=pkg['name'],version=pkg['version'],license=pkg['license'],declared_msrv=pkg['rust_version'],features=features,registry_checksum=checksums[(pkg['name'],pkg['version'])]))
assert not any(pkg['name'] in ('openssl-sys','libz-sys','zstd-sys','tokio','partitionline') for pkg in packages)
baseline='e054ce0cdcc68a46e9688ca56f2698d45de6595e'
old_manifest=tomllib.loads(subprocess.check_output(['git','-C',str(root),'show',baseline+':Cargo.toml'],text=True))
current_manifest=tomllib.loads((root/'Cargo.toml').read_text())
graph_sections=['dependencies','dev-dependencies','build-dependencies','features','target','workspace']
assert all(old_manifest.get(section)==current_manifest.get(section) for section in graph_sections)
old_lock=subprocess.check_output(['git','-C',str(root),'show',baseline+':Cargo.lock'])
assert old_lock==(root/'Cargo.lock').read_bytes()
old_scenarios=json.loads(subprocess.check_output(['git','-C',str(root),'show',baseline+':benchmarks/scenarios.json'],text=True))
current_scenarios=json.loads((root/'benchmarks/scenarios.json').read_text())
for section in ['profiles','client_ceiling','equal_semantics','measurement_protocol','suite_hold']:
    assert old_scenarios[section]==current_scenarios[section]
for old,new in zip(old_scenarios['matched_scenarios'],current_scenarios['matched_scenarios']):
    assert old['frozen_knobs']==new['frozen_knobs']
    for name,value in old['peer_configurations'].items():assert value==new['peer_configurations'][name]
sources={str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(peer.rglob('*'))
         if path.is_file() and '__pycache__' not in path.parts}
sources.update({name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ['Cargo.toml','Cargo.lock','benchmarks/scenarios.json','benchmarks/result-schema.json','benchmarks/peers/librdkafka/run.py','scripts/check-record-history.py']})
result=dict(source_sha=a.sha,tested_base_sha=baseline,core_lock_byte_identical=True,core_graph_sections_identical=True,
            core_workspace_members=core['workspace_members'],peer_workspace_members=metadata['workspace_members'],
            core_native_kafka_dependencies=[],peer_resolved_package_count=len(packages),peer_packages=packages,
            native_library_note='C 2.15.0 / OpenSSL / zlib / zstd are external benchmark-only native installation dependencies, not Cargo core dependencies; binding header baseline remains 2.12.1',
            frozen_profiles_unchanged=True,frozen_named_knobs_unchanged=True,existing_peer_configurations_unchanged=True,source_sha256=sources)
a.out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(dict(core_unchanged=True,standalone_packages=len(packages),frozen_knobs_unchanged=True)))

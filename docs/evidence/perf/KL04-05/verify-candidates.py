#!/usr/bin/env python3
"""Retain bounded exact registry archives and verify the read-only source audit pins."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tomllib
import urllib.request

p=argparse.ArgumentParser();p.add_argument('--peer',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
a=p.parse_args();pins=json.loads((a.peer/'pure-rust-candidates.json').read_text());a.out.mkdir(parents=True,exist_ok=True)
results=[]
for candidate in pins['candidates']:
    name=candidate['crate'];version=candidate['version'];prefix=f'{name}-{version}'
    url=f'https://static.crates.io/crates/{name}/{prefix}.crate'
    destination=a.out/(prefix+'.crate')
    if destination.exists(): data=destination.read_bytes()
    else:
        with urllib.request.urlopen(url,timeout=30) as response:data=response.read(2*1024*1024+1)
        assert len(data)<=2*1024*1024,'archive limit exceeded'
        with destination.open('xb') as file:file.write(data)
    digest=hashlib.sha256(data).hexdigest();assert digest==candidate['crate_archive_sha256']
    with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
        def member(relative):
            item=archive.getmember(prefix+'/'+relative);assert item.isfile() and item.size<=1024*1024
            return archive.extractfile(item).read()
        vcs=json.loads(member('.cargo_vcs_info.json'));assert vcs['git']['sha1']==candidate['git_commit']
        package=tomllib.loads(member('Cargo.toml.orig').decode())['package']
        assert package['license']==candidate['license']
        assert package.get('rust-version')==candidate['declared_msrv']
        checked={}
        for path,expected in candidate['source_hashes'].items():
            actual=hashlib.sha256(member(path)).hexdigest();assert actual==expected,path;checked[path]=actual
        # Source-backed decisive constraints; the full pinned archives retain context.
        if name=='rskafka':
            source=member('src/client/partition.rs').decode()
            for token in ['producer_id: -1','producer_epoch: -1','base_sequence: -1',
                          'transactional_id: crate::protocol::primitives::NullableString(None)',
                          'acks: Int16(-1)','IsolationLevel::ReadCommitted']:assert token in source,token
            transport=member('src/connection/transport.rs').decode();assert 'set_nodelay' not in transport
            producer=member('src/client/producer.rs').decode();assert 'with_linger' in producer
        else:
            fetch=member('src/protocol/fetch.rs').decode();assert 'struct PartitionFetchRequest' in fetch and 'pub max_bytes: i32' in fetch
            protocol=member('src/protocol/mod.rs').decode();assert 'API_VERSION: i16 = 0' in protocol
            sources=[item for item in archive.getmembers() if item.isfile() and '/src/' in item.name and item.name.endswith('.rs')]
            assert not any(b'set_nodelay' in archive.extractfile(item).read() for item in sources)
    results.append(dict(crate=name,version=version,url=url,archive_sha256=digest,git_commit=candidate['git_commit'],
                        license=package['license'],declared_msrv=package.get('rust-version'),source_sha256=checked,
                        eligible_cell_ids=[],driver_status='not_implemented',comparison_standing='excluded'))
(a.out/'verified-candidates.json').write_text(json.dumps(dict(selection_status=pins['selection_status'],results=results),indent=2)+'\n')
print(json.dumps(dict(verified_archives=len(results),selection_status=pins['selection_status'])))

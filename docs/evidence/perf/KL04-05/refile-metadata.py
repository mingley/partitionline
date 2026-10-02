#!/usr/bin/env python3
"""Reuse proven identical compilation, explicitly rebinding only changed documentation metadata."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import jsonschema

p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--sha',required=True)
p.add_argument('--compiled-source',type=Path,required=True);p.add_argument('--compiled-sha',required=True)
p.add_argument('--out',type=Path,required=True);a=p.parse_args();sys.dont_write_bytecode=True
source=a.source.resolve();peer=source/'benchmarks/peers/rust';old=a.compiled_source.resolve()/'benchmarks/peers/rust'
out=a.out.resolve();out.mkdir(parents=True,exist_ok=True)
assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()==a.sha
assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True).strip()
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
files={str(path.relative_to(peer)):sha(path) for path in sorted(peer.rglob('*')) if path.is_file() and '__pycache__' not in path.parts}
old_files={str(path.relative_to(old)):sha(path) for path in sorted(old.rglob('*')) if path.is_file() and '__pycache__' not in path.parts}
assert files.keys()==old_files.keys()
differences=[name for name in files if files[name]!=old_files[name]]
assert differences==['README.md'],differences
invariants={name:value for name,value in files.items() if name!='README.md'}
env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
spec=importlib.util.spec_from_file_location('refiling_report',source/'scripts/benchmark-report.py')
report=importlib.util.module_from_spec(spec);spec.loader.exec_module(report)
validator=report.BenchmarkValidator(source/'benchmarks/result-schema.json');schema=json.loads((source/'benchmarks/result-schema.json').read_text())
registry=json.loads((source/'benchmarks/scenarios.json').read_text());named=registry['matched_scenarios'][0]
commands=[];results=[];reused=[]
def command(name,args,changes=None):
    path=out/(name+'.log')
    with path.open('x') as file:
        run=subprocess.run(['taskset','-c','0-2,4']+[str(arg) for arg in args],cwd=source,env=dict(env,**(changes or {})),stdout=file,stderr=subprocess.STDOUT)
    commands.append(dict(name=name,command=[str(arg) for arg in args],environment=changes or {},exit_code=run.returncode,log=path.name,log_sha256=sha(path),affinity='0-2,4'))
    assert run.returncode==0,(name,path)
def validate(path):
    result=json.loads(path.read_text());jsonschema.Draft7Validator(schema).validate(result)
    valid,errors,_=validator.validate(result,require_clean=True);assert valid,errors
    assert result['provenance']['source']['git_commit']==a.sha and result['provenance']['source']['clean']
    results.append(dict(path=str(path),sha256=sha(path),source_sha=a.sha,schema_valid=True,report_valid=True,
                        tier=result['scenario']['tier'],disposition=result['scenario']['cell_disposition'],acknowledged=result['outcomes']['acknowledged']))
    return result
for lane in ['stable','msrv']:
    original=Path('/workspace/work')/f'rust-peer-final-{a.compiled_sha[:8]}-{lane}'
    manifest=json.loads((original/'build-manifest.json').read_text())
    assert all(manifest['source_sha256'][name]==value for name,value in invariants.items())
    assert manifest['binary_sha256']==sha(original/'rust-peer') and manifest['library_sha256']==sha(original/'lib/librdkafka.so.1')
    for relative,digest in manifest['shared_helpers_sha256'].items():assert sha(source/relative)==digest
    execution=Path('/workspace/work')/f'rust-peer-refiling-{a.sha[:8]}-{lane}'
    (execution/'lib').mkdir(parents=True,exist_ok=False)
    shutil.copyfile(original/'rust-peer',execution/'rust-peer');shutil.copymode(original/'rust-peer',execution/'rust-peer')
    shutil.copyfile(original/'lib/librdkafka.so.1',execution/'lib/librdkafka.so.1')
    reuse=dict(compiled_source_sha=a.compiled_sha,metadata_source_sha=a.sha,original_build_manifest_sha256=sha(original/'build-manifest.json'),
               binary_sha256=manifest['binary_sha256'],library_sha256=manifest['library_sha256'],changed_peer_files=differences,
               unchanged_inputs_sha256=invariants,note='No compilation rerun; all Rust/Python/Cargo/lock/native pin/helper inputs are identical. Only README and scoped registry facts changed. Prior 9c6a9b35 full stable/MSRV gates and live controls remain authoritative for byte-identical executable behavior.')
    manifest['source_sha256']=files;manifest['compilation_reuse']=reuse
    (execution/'build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');reused.append(dict(lane=lane,**reuse))
    unsupported=out/(lane+'-unsupported-share.json')
    reason='Native C 2.15.0 supplies preview KIP-932; pinned wrapper/sys API and this adapter have no share lifecycle, and Kafka 3.9.1 fixture is ineligible. Unsupported driver/fixture cell is excluded, never a win.'
    command(lane+'-unsupported-share',['python3',peer/'run.py','unsupported','--binary',execution/'rust-peer','--result',unsupported,'--reason',reason],dict(PROFILE='group/share',SCENARIO_ID='share-exp-kip932-concurrency'))
    item=validate(unsupported);assert item['scenario']['cell_disposition']=='unsupported' and item['outcomes']['acknowledged']==0
    frozen=out/(lane+'-frozen-not-run.json')
    command(lane+'-frozen-not-run',['python3',peer/'run.py','not-run','--binary',execution/'rust-peer','--result',frozen,'--reason','Frozen 8M/10k/5 required campaign is unchanged and unexecuted; shared ID-key instrumentation does not prove frozen UUID distribution. No qualification or claim.'],dict(TIER='required',RTT_MS='0.1',COUNT='8000000',WARMUP='10000',TOTAL_REPETITIONS='5',ACKS='-1',IDEMPOTENT='1',ISOLATION='read_committed',BATCH_BYTES='1048576',KAFKA_TOPIC='plbench-matched',SCENARIO_ID=named['id']))
    item=validate(frozen);cfg=item['provenance']['config']['effective_settings'];knobs=named['frozen_knobs']
    assert item['scenario']['tier']==named['tier']=='required' and item['provenance']['topology']['rtt_ms']==knobs['network_topology']['rtt_ms']==0.1
    assert cfg['count']==knobs['workload']['record_count']==8000000 and cfg['warmup']==knobs['workload']['warmup_records']==10000
    assert item['execution']['total_repetitions']==knobs['workload']['repetitions']==5
    assert cfg['acks']==knobs['acks'] and cfg['idempotence']==knobs['idempotence'] and cfg['isolation_level']==knobs['isolation_level']
    assert cfg['batch_size_bytes']==knobs['batching']['batch_size_bytes'] and cfg['batch_num_messages']==knobs['batching']['batch_num_messages'] and cfg['linger_ms']==knobs['batching']['linger_ms']
    assert cfg['partitions']==knobs['partitions'] and cfg['max_in_flight']==knobs['connection_in_flight']['max_in_flight_requests_per_connection']
    command(lane+'-runtime-effective-config',['python3',peer/'run.py','emit-config','--binary',execution/'rust-peer'],dict(ACKS='-1',IDEMPOTENT='1',ISOLATION='read_committed',BATCH_BYTES='1048576'))
    runtime=json.loads((out/(lane+'-runtime-effective-config.log')).read_text())['runtime'];assert runtime['native_version']=='2.15.0' and runtime['loaded_library_sha256']==manifest['library_sha256']
# The two updated unsupported entries must describe driver/fixture exclusions,
# while frozen workload profiles remain unmodified by this metadata correction.
for entry in registry['unsupported_peer_cells']:
    if entry['peer'] in ('librdkafka','rust-rdkafka'):
        assert entry['disposition']=='unsupported' and ('preview' in entry['missing_capability'])
assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True).strip()
(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
(out/'result-validation.json').write_text(json.dumps(dict(source_sha=a.sha,compiled_source_sha=a.compiled_sha,unchanged_inputs=invariants,changed_peer_files=differences,reused_compilation=reused,results=results),indent=2)+'\n')
print(json.dumps(dict(metadata_source_sha=a.sha,compiled_source_sha=a.compiled_sha,commands=len(commands),validated_filings=len(results),compilation_reused=True)))

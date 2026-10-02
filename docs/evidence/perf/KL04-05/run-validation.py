#!/usr/bin/env python3
"""Exact-source isolated-worktree validation; all broker topics and outputs are unique."""
from __future__ import annotations
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
import jsonschema

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--source',type=Path,required=True)
p.add_argument('--source-sha',required=True)
p.add_argument('--native',type=Path,required=True)
p.add_argument('--kafka-home',type=Path,required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args();source=a.source.resolve();out=a.out.resolve();peer=source/'benchmarks/peers/rust'
sys.dont_write_bytecode=True
assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()==a.source_sha
assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True).strip()
out.mkdir(parents=True,exist_ok=True)
env=os.environ.copy();env.update(PYTHONDONTWRITEBYTECODE='1',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',
                               CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',
                               KAFKA_HEAP_OPTS='-Xms128m -Xmx256m')
commands=[]

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def command(name,args,changes=None,expected=0):
    log=out/(name+'.log');assert not log.exists(),name
    with log.open('x') as stream:
        run=subprocess.run(['taskset','-c','0-2,4']+[str(v) for v in args],cwd=source,
                           env=dict(env,**(changes or {})),stdout=stream,stderr=subprocess.STDOUT)
    commands.append(dict(name=name,command=[str(v) for v in args],environment=changes or {},
                         affinity='0-2,4',exit_code=run.returncode,expected_exit_code=expected,
                         log=log.name,log_sha256=sha(log)))
    (out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
    assert run.returncode==expected,(name,run.returncode,log)

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module;spec.loader.exec_module(module);return module

validator=load_module('final_benchmark_report',source/'scripts/benchmark-report.py').BenchmarkValidator(source/'benchmarks/result-schema.json')
schema=json.loads((source/'benchmarks/result-schema.json').read_text())
auditor=load_module('final_receipt_audit',peer/'audit.py')
results=[]

def validate(path,clean=True):
    data=json.loads(path.read_text());errors=[dict(path=list(e.path),message=e.message) for e in jsonschema.Draft7Validator(schema).iter_errors(data)]
    valid,problems,_=validator.validate(data,require_clean=clean)
    assert not errors,(path,errors)
    assert valid==clean,(path,problems)
    results.append(dict(path=str(path),sha256=sha(path),schema_valid=True,report_valid=valid,
                        report_errors=problems,expected_clean=clean,disposition=data['scenario']['cell_disposition'],
                        source_commit=data['provenance']['source']['git_commit'],source_clean=data['provenance']['source']['clean'],
                        acknowledged=data['outcomes']['acknowledged'],consumed=data['outcomes']['consumed'],verified=data['integrity']['verified']))
    assert data['provenance']['source']['git_commit']==a.source_sha
    assert data['provenance']['source']['clean'] is True
    return data

for toolchain,label in [('stable','stable'),('1.85.0','msrv')]:
    build=Path('/workspace/work')/f'rust-peer-final-{a.source_sha[:8]}-{label}'
    target=Path('/workspace/work')/('target-rust-peer' if label=='stable' else 'target-rust-peer-msrv')
    command(label+'-package-clean',['cargo','+'+toolchain,'clean','--manifest-path',peer/'Cargo.toml','--target-dir',target,'-p','partitionline-rust-rdkafka-peer'])
    command(label+'-build-checks',['python3',peer/'build.py','--native-build',a.native,'--out',build,'--target-dir',target,'--toolchain',toolchain,'--check'])
    retained=out/(label+'-build');retained.mkdir()
    for file in build.glob('*.log'):shutil.copyfile(file,retained/file.name)
    for name in ['build-manifest.json','commands.json']:shutil.copyfile(build/name,retained/name)
    command(label+'-python-integrity',['python3','-m','unittest','discover','-s',peer,'-p','test_adapter.py','-v'])
    topic=f'kl0405-final-{a.source_sha[:8]}-{label}'
    command(label+'-create-topic',[a.kafka_home/'bin/kafka-topics.sh','--bootstrap-server','127.0.0.1:19094','--create','--topic',topic,'--partitions','6','--replication-factor','1','--config','min.insync.replicas=1'])
    settings=dict(COUNT='256',WARMUP='32',ACKS='-1',IDEMPOTENT='1',ISOLATION='read_committed',BATCH_BYTES='1048576',KAFKA_BOOTSTRAP='127.0.0.1:19094',KAFKA_TOPIC=topic,BROKER_IMAGE='apache-kafka-3.9.1-distribution',BROKER_VERSION='3.9.1',SCENARIO_ID='rust-rdkafka-matched-settings-integrity-smoke-'+label)
    artifact=out/(label+'-smoke.json')
    command(label+'-receipt-smoke',['python3',peer/'run.py','roundtrip','--binary',build/'rust-peer','--kafka-home',a.kafka_home,'--result',artifact],settings)
    data=validate(artifact);assert data['outcomes']['acknowledged']==256 and data['integrity']['verified']
    command(label+'-effective-config',['python3',peer/'run.py','emit-config','--binary',build/'rust-peer'],settings)
    command(label+'-independent-history-cli',['python3',source/'scripts/check-record-history.py','--json',str(artifact)+'.history.json'])
    # Actual independent receipts, then a deliberate corrupt byte: both the
    # independent expectation and generic shared gate must reject it.
    config=json.loads(Path(str(artifact)+'.input-config.json').read_text());raw=json.loads(Path(str(artifact)+'.raw.json').read_text())
    attempts=auditor.read_rows(str(artifact)+'.attempts.jsonl');receipts=auditor.read_rows(str(artifact)+'.receipts.jsonl')
    mutated=copy.deepcopy(receipts);value=bytearray.fromhex(mutated[0]['value']);value[0]^=1;mutated[0]['value']=value.hex()
    history,verdict=auditor.verify(config,raw,attempts,mutated)
    assert not verdict['valid'] and not verdict['history_gate']['valid']
    (out/(label+'-corrupt-history.json')).write_text(json.dumps(history,indent=2)+'\n')
    (out/(label+'-corrupt-verdict.json')).write_text(json.dumps(verdict,indent=2)+'\n')
    command(label+'-corrupt-history-cli',['python3',source/'scripts/check-record-history.py','--json',out/(label+'-corrupt-history.json')],expected=1)
    if label=='stable':
        unsupported=out/'unsupported-share.json'
        command('unsupported-share',['python3',peer/'run.py','unsupported','--binary',build/'rust-peer','--result',unsupported,'--reason','Native librdkafka 2.15.0 lacks KIP-932 RPCs; no adapter share driver. Excluded, never a win.'],dict(PROFILE='group/share',SCENARIO_ID='share-exp-kip932-concurrency'))
        validate(unsupported)
        frozen=out/'frozen-matched-not-run.json'
        command('frozen-not-run',['python3',peer/'run.py','not-run','--binary',build/'rust-peer','--result',frozen,'--reason','Frozen 8M/10k/5 campaign remains unchanged and unexecuted; separate smoke cannot qualify it. Shared ID keys do not prove UUID distribution.'],dict(TIER='required',RTT_MS='0.1',COUNT='8000000',WARMUP='10000',TOTAL_REPETITIONS='5',ACKS='-1',IDEMPOTENT='1',ISOLATION='read_committed',BATCH_BYTES='1048576',KAFKA_TOPIC='plbench-matched',SCENARIO_ID='matched-bulk-acks-all-6p-uncompressed'))
        filing=validate(frozen);registry=json.loads((source/'benchmarks/scenarios.json').read_text())['matched_scenarios'][0]
        knobs=registry['frozen_knobs'];declared=filing['provenance']['config']['effective_settings']
        assert filing['scenario']['tier']==registry['tier']=='required'
        assert filing['provenance']['topology']['rtt_ms']==knobs['network_topology']['rtt_ms']==0.1
        assert filing['scenario']['scenario_id']==registry['id'] and filing['scenario']['cell_disposition']=='not_run'
        assert declared['count']==knobs['workload']['record_count']==8000000 and declared['warmup']==knobs['workload']['warmup_records']==10000
        assert filing['execution']['total_repetitions']==knobs['workload']['repetitions']==5
        assert declared['acks']==knobs['acks'] and declared['idempotence']==knobs['idempotence'] and declared['isolation_level']==knobs['isolation_level']
        assert declared['batch_size_bytes']==knobs['batching']['batch_size_bytes'] and declared['linger_ms']==knobs['batching']['linger_ms']
        queue=out/'queue-full-smoke.json';small=dict(settings,COUNT='32',WARMUP='0',QUEUE_MESSAGES='1',SCENARIO_ID='rust-rdkafka-queue-full-integrity-control')
        command('queue-full-control',['python3',peer/'run.py','roundtrip','--binary',build/'rust-peer','--kafka-home',a.kafka_home,'--result',queue],small)
        q=validate(queue);assert q['measurements']['queue_full_retries']>0 and q['outcomes']['acknowledged']==32
        reject_topic=topic+'-rejection'
        command('create-rejection-topic',[a.kafka_home/'bin/kafka-topics.sh','--bootstrap-server','127.0.0.1:19094','--create','--topic',reject_topic,'--partitions','6','--replication-factor','1','--config','min.insync.replicas=1','--config','max.message.bytes=200'])
        rejected=out/'broker-rejection.json'
        command('broker-rejection-control',['python3',peer/'run.py','roundtrip','--binary',build/'rust-peer','--kafka-home',a.kafka_home,'--result',rejected],dict(settings,COUNT='32',WARMUP='0',PAYLOAD_BYTES='1000',KAFKA_TOPIC=reject_topic,SCENARIO_ID='rust-rdkafka-oversize-rejection-control'),expected=1)
        r=validate(rejected,False);assert r['measurements']['callback_failures']==32 and r['outcomes']['acknowledged']==0 and r['integrity']['high_watermark_audit']['total_offset_delta']==0
        ack0=out/'acks-zero.json'
        command('acks-zero-control',['python3',peer/'run.py','produce','--binary',build/'rust-peer','--result',ack0],dict(settings,COUNT='8',WARMUP='0',ACKS='0',IDEMPOTENT='0',SCENARIO_ID='rust-rdkafka-acks-zero-control'),expected=1)
        z=validate(ack0,False);assert z['outcomes']['acknowledged']==0 and z['outcomes']['unknown']==8 and z['measurements']['throughput']['records_per_second']==0
        command('invalid-idempotence-config',['python3',peer/'run.py','emit-config','--binary',build/'rust-peer'],dict(ACKS='1',IDEMPOTENT='1'),expected=2)
        command('invalid-tier-config',['python3',peer/'run.py','emit-config','--binary',build/'rust-peer'],dict(TIER='made-up'),expected=2)
        # Correct runtime version with wrong loaded bytes must still fail before traffic.
        shadow=Path('/workspace/work')/f'rust-peer-shadow-{a.source_sha[:8]}'
        (shadow/'lib').mkdir(parents=True);library=shadow/'lib/librdkafka.so.1'
        with library.open('xb') as file:file.write((build/'lib/librdkafka.so.1').read_bytes()+b'\0')
        manifest=json.loads((build/'build-manifest.json').read_text())
        command('runtime-library-shadow-control',[build/'rust-peer','produce'],dict(LD_LIBRARY_PATH=str(shadow/'lib'),RUST_PEER_NATIVE_SHA256=manifest['library_sha256']),expected=2)
        assert 'loaded native library SHA256 differs' in (out/'runtime-library-shadow-control.log').read_text()
        (out/'runtime-shadow.json').write_text(json.dumps(dict(base_sha256=manifest['library_sha256'],mutant_sha256=sha(library),mutation='append one NUL byte; ELF/version unchanged',expected_exit=2,broker_traffic=False),indent=2)+'\n')

assert not subprocess.check_output(['git','-C',str(source),'status','--porcelain'],text=True).strip()
(out/'result-validation.json').write_text(json.dumps(dict(source_sha=a.source_sha,jsonschema_version=__import__('importlib.metadata',fromlist=['version']).version('jsonschema'),results=results),indent=2)+'\n')
print(json.dumps(dict(source_sha=a.source_sha,command_count=len(commands),validated_artifacts=len(results),test_lanes=['stable','1.85.0'],performance_claim=None)))

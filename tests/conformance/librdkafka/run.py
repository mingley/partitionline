#!/usr/bin/env python3
"""Execute pinned 0125 C assertion and Rust adaptation with independent C audits."""
from __future__ import annotations
import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import uuid

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
CASE_ID='librdkafka-0125-flush-overrides-linger'

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def write(path,value):
    with Path(path).open('x') as out:json.dump(value,out,indent=2);out.write('\n')
def events(path):return [json.loads(line) for line in Path(path).read_text().splitlines()]
def artifact(path,kind):return dict(path=str(path),type=kind,sha256=sha(path),size_bytes=Path(path).stat().st_size)
def execute(cmd,env,log,timeout=45):
    with Path(log).open('x') as out:
        return subprocess.run(cmd,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=timeout).returncode

def build_history(rows,audit_rows,topic,peer,variant):
    deliveries={r['id']:r for r in rows if r['event']=='delivery'}
    attempted=[]
    for i in range(100):
        ident=f'm-{i:03}';payload=f'{i:03}:'+('x'*46);result=deliveries.get(ident,{})
        attempted.append(dict(id=ident,topic=topic,partition=0,key=ident,payload=payload,
                              payload_hash=hashlib.sha256(payload.encode()).hexdigest(),
                              offset=result.get('offset'),status='acked' if result.get('error_code')==0 else 'ambiguous'))
    consumed=[dict(id=r['id'],topic=topic,partition=r['partition'],offset=r['offset'],key=r['id'],payload=r['payload'],
                   payload_hash=hashlib.sha256(r['payload'].encode()).hexdigest()) for r in audit_rows if r['event']=='consumed']
    return dict(id=f'{CASE_ID}-{peer}-{variant}',config=dict(acks=1,idempotent=False,isolation_level='read_uncommitted',transactional=False),
                attempted=attempted,consumed=consumed,
                input_history=dict(phases=[dict(records=list(range(50)),operation='enqueue_then_poll_natural_delivery'),
                                           dict(records=list(range(50,100)),operation='enqueue_then_flush_timeout2000')],
                                   linger_ms=10000,payload_bytes=50,partition=0),
                fault_history=[] if variant=='normal' else [dict(phase=1,mutation='omit_explicit_flush_then_wait_for_natural_delivery')])

def check_history(path,out):
    run=subprocess.run([sys.executable,str(ROOT/'scripts/check-record-history.py'),'--json',str(path)],capture_output=True,text=True)
    with Path(out).open('x') as f:f.write(run.stdout)
    return run.returncode,json.loads(run.stdout)

def validate_result(rows,audit_rows,variant,code):
    timing=[r for r in rows if r['event']=='timing_assertion']
    receipt=[r for r in audit_rows if r['event']=='consumed']
    expected={'NO_FLUSH':(10000,15000),'FLUSH':(0,2500)}
    valid_timing=(len(timing)==2 and {r['name'] for r in timing}==set(expected))
    for row in timing:
        bounds=expected.get(row['name'])
        valid_timing=valid_timing and bounds==(row.get('lower_ms'),row.get('upper_ms')) and math.isfinite(row['elapsed_ms'])
        valid_timing=valid_timing and bounds is not None and bounds[0]<=row['elapsed_ms']<=bounds[1] and row.get('passed') is True
    assertions_pass=all(r.get('passed',True) for r in rows+audit_rows)
    return dict(native_exit_code=code,timing_assertions=timing,
                assertions_pass=assertions_pass,
                expected_timing_bounds=dict(NO_FLUSH=[10000,15000],FLUSH=[0,2500]),
                delivered_count=len([r for r in rows if r['event']=='delivery' and r.get('error_code')==0]),
                independently_consumed_count=len(receipt),
                high_watermark=[r for r in audit_rows if r['event']=='high_watermark'],
                variant=variant,behavior_pass=code==0 and valid_timing and assertions_pass and len(receipt)==100)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--c-binary',type=Path,required=True)
    parser.add_argument('--rust-binary',type=Path,required=True)
    parser.add_argument('--kafka-home',type=Path,required=True)
    parser.add_argument('--build-manifest',type=Path,required=True)
    parser.add_argument('--bootstrap',default='127.0.0.1:19104')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():raise ValueError('output exists; every attempt needs a new directory')
    args.output.mkdir(parents=True)
    pin=json.load(open(HERE/'pin.json'))
    assert sha(HERE/'upstream/0125-immediate_flush.c')==pin['source_sha256']
    native_manifest=json.load(open(args.c_binary.parent/'build-manifest.json'))
    assert native_manifest['commit']==pin['commit']
    assert sha(args.c_binary.parent/'lib/librdkafka.so.1')==native_manifest['library_sha256']
    frozen=json.loads(args.build_manifest.read_text())
    assert frozen['source_unchanged_during_build']
    assert sha(args.c_binary)==frozen['native_binary_sha256'] and sha(args.rust_binary)==frozen['rust_binary_sha256']
    assert native_manifest['library_sha256']==frozen['native_library_build']['library_sha256']
    build_path=args.output/'behavior-build-manifest.json';write(build_path,frozen)
    e=os.environ.copy();e.pop('LD_LIBRARY_PATH',None);e.pop('LD_PRELOAD',None)
    e['KAFKA_BOOTSTRAP']=args.bootstrap
    profiles=[];normalized={}
    for variant in ('normal','omit-flush'):
        for peer,binary in [('librdkafka',args.c_binary),('partitionline',args.rust_binary)]:
            stem=args.output/f'{peer}-{variant}';topic=f'kl01-13-{peer}-{variant}-{uuid.uuid4().hex[:8]}'
            e.update(KAFKA_TOPIC=topic,VARIANT=variant,EVENTS_PATH=str(stem)+'.events.jsonl')
            creation=execute([str(args.kafka_home/'bin/kafka-topics.sh'),'--bootstrap-server',args.bootstrap,'--create','--topic',topic,
                              '--partitions','1','--replication-factor','1','--config','min.insync.replicas=1'],e,str(stem)+'.create.log')
            if creation:raise RuntimeError('fresh topic creation failed; logs retained')
            code=execute([str(binary)],e,str(stem)+'.run.log')
            rows=events(str(stem)+'.events.jsonl')
            if peer=='partitionline':
                ae=e.copy();ae['EVENTS_PATH']=str(stem)+'.audit.jsonl'
                audit_code=execute([str(args.c_binary),'audit'],ae,str(stem)+'.audit.log')
                audit_rows=events(ae['EVENTS_PATH'])
            else:audit_code=0;audit_rows=rows
            history=build_history(rows,audit_rows,topic,peer,variant)
            history_path=str(stem)+'.history.json';write(history_path,history)
            check_code,check=check_history(history_path,str(stem)+'.history-check.json')
            result=validate_result(rows,audit_rows,variant,code)
            result.update(peer=peer,topic=topic,history_checker_exit_code=check_code,audit_exit_code=audit_code,
                          source_pin=pin,source_git_commit=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip(),
                          build_provenance=dict(path=str(build_path),sha256=sha(build_path),compiled_source_commit=frozen['source_snapshot']['git_commit'],compiled_source_digest=frozen['source_snapshot']['source_digest']),
                          source_clean=not subprocess.check_output(['git','-C',str(ROOT),'status','--porcelain'],text=True).strip(),
                          binary_sha256=sha(binary),library_sha256=native_manifest['library_sha256'],
                          oracle='independent native C direct-assignment consumer + broker ListOffsets',
                          adapters=dict(upstream_source_unchanged=True,shim_source_sha256=sha(HERE/'shim/helpers.c'),rust_adapter_sha256=sha(HERE/'rust/src/main.rs')),
                          artifacts=[artifact(str(stem)+suffix,kind) for suffix,kind in [('.events.jsonl','raw_events'),('.history.json','history'),('.history-check.json','history_verdict'),('.run.log','stdout_stderr')]])
            result_path=str(stem)+'.result.json';write(result_path,result)
            expected=(variant=='normal')
            if result['behavior_pass']!=expected or check_code!=0 or audit_code!=0:
                raise RuntimeError(f'unexpected {peer}/{variant} verdict; every artifact retained')
            if not expected:
                assert code==1 and [r for r in result['timing_assertions'] if not r['passed']][0]['name']=='FLUSH'
            normalized[(peer,variant)]=[(r['id'],r['payload_hash'],r['partition'],r['offset']) for r in history['consumed']]
            profiles.append(dict(peer=peer,variant=variant,result=result_path,behavior_pass=result['behavior_pass'],history_pass=check_code==0))
            print(f'{peer}/{variant}: behavior={result["behavior_pass"]} history={check_code==0} exit={code}',flush=True)
    for variant in ('normal','omit-flush'):
        assert normalized[('librdkafka',variant)]==normalized[('partitionline',variant)],'peer record histories differ'
    # Same count, altered actual receipt evidence: checker must expose duplicate/loss.
    mutant=copy.deepcopy(json.load(open(args.output/'partitionline-normal.history.json')))
    mutant['id']+='-synthetic-duplicate-loss';mutant['consumed'][-1]=copy.deepcopy(mutant['consumed'][-2])
    path=args.output/'synthetic-duplicate-loss.history.json';write(path,mutant)
    code,check=check_history(path,args.output/'synthetic-duplicate-loss.history-check.json')
    assert code==1 and check['results'][0].get('minimal_counterexample'),'history mutation must fail with a counterexample'
    selected={'schema_version':1,'audited_source':pin['commit'],'cases':[dict(id=CASE_ID,source_pin=pin['commit'],peer_version_pin='2.15.0',denominator=True,disposition='independent_pass',reason='Only selected 0125 flush-overrides-linger assertion')]}
    write(args.output/'selected-registry.json',selected)
    case_attempts=[]
    for i,row in enumerate(profiles,1):
        case_attempts.append(dict(attempt=i,status='independent_pass' if row['behavior_pass'] else 'failed',source_pin=pin['commit'],peer_pin='2.15.0',
                                 peer_identity=row['peer'],reason=f'{row["peer"]} {row["variant"]}; selected assertion only',artifact=row['result']))
    report=dict(cases=[dict(id=CASE_ID,attempts=case_attempts)]);write(args.output/'conformance-attempts.json',report)
    run=subprocess.run([sys.executable,str(ROOT/'scripts/conformance-report.py'),str(args.output/'conformance-attempts.json'),'--registry',str(args.output/'selected-registry.json'),'--json'],capture_output=True,text=True)
    with (args.output/'conformance-aggregate.json').open('x') as out:out.write(run.stdout)
    with (args.output/'conformance-aggregate.stderr.log').open('x') as out:out.write(run.stderr)
    assert run.returncode==1,'retained deliberately failed attempts must prevent suite-green verdict'
    write(args.output/'summary.json',dict(case_id=CASE_ID,pin=pin,profiles=profiles,normalized_peer_histories_equal=True,
      synthetic_history_mutant_exit_code=code,retained_failed_attempts=2,conformance_aggregate_exit_code=run.returncode,
      full_c_cpp_suite_pass=False,scope='Only pinned main_0125_immediate_flush; brokerless sibling and remainder excluded'))
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (ValueError,RuntimeError,AssertionError,subprocess.TimeoutExpired) as error:
        print(f'0125 adapter: {error}',file=sys.stderr);sys.exit(1)

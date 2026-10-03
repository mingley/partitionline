#!/usr/bin/env python3
"""Run only coordinator-completed immutable membership capture cells.

This wrapper does not create runtime traces, add settings, invoke Cargo, retry
failed checkers, or turn synthetic controls into captured runtime evidence.
The completion receipt is a coordinator assertion whose exact bytes are bound.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

PIN = '403be1e3db073df86921d6fb21189f695c4f1eaf'
SOURCE = Path('/workspace/work/broker-merged-source-403be1e3')
OUT = Path('/workspace/work/membership-strict-403be1e3')
PREFIX = 'docs/evidence/broker/KL11-74/oracle/history/'
EXPECTED = {
    PREFIX+'membership_raw.py': '4bd82d8f7ecd9c7fff6ea3323e5cfd7c5fd4649c30462e2e64364d930e19d7b0',
    PREFIX+'check-membership-history.py': '2548c08901ed46a9c01501f9f66e087a41917679d475ebc520b4e001234176eb',
    PREFIX+'test-membership-history.py': 'a7d0ea99081c56064ec4ffb7e15c457043c3448116fad1ee553f18281a12d380',
    'docs/evidence/broker/KL11-15/oracle/history/wal_oracle.py': 'a2df594ffb1ba9f413fff5ae4606f510c32118a2ae192864acfc2351fa4fd8ae',
}
SETTINGS = {'quorum_timeout_ms':1000, 'election_min_ms':10, 'election_max_ms':10}

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1024*1024),b''):
            h.update(data)
    return h.hexdigest()

def dump(path, value):
    path.write_text(json.dumps(value,indent=2)+'\n')

def sources():
    result={}
    for name, expected in EXPECTED.items():
        path=SOURCE/name
        assert not path.is_symlink() and path.is_file(), 'source availability/type'
        value={'sha256':digest(path),'full_mode':oct(stat.S_IMODE(path.stat().st_mode))}
        assert value['sha256']==expected and value['full_mode']=='0o600', 'frozen source identity/mode'
        result[name]=value
    return result

def inventory(root):
    """Fingerprint every captured artifact, including nonreferenced raw outputs."""
    assert root.is_dir() and not root.is_symlink(), 'capture directory type'
    result={}
    for path in [root]+sorted(root.rglob('*')):
        s=path.lstat()
        assert not stat.S_ISLNK(s.st_mode), 'capture symlink rejected'
        value={'full_mode':oct(stat.S_IMODE(s.st_mode))}
        if stat.S_ISREG(s.st_mode):
            value.update(type='file',bytes=s.st_size,sha256=digest(path))
        else:
            assert stat.S_ISDIR(s.st_mode), 'nonregular capture artifact'
            value.update(type='directory')
        result[str(path.relative_to(root))]=value
    return result

def review_completion(path, capture, label, captured_inventory):
    receipt=json.loads(path.read_bytes())
    command=receipt['actual_command']
    encoded=json.dumps(command,sort_keys=True,separators=(',',':')).encode()
    assert hashlib.sha256(encoded).hexdigest()==receipt['actual_command_json_sha256'], 'exact actual command receipt'
    assert command['name']==label+'-all-targets' and command['exit_code']==0, 'actual all-targets command success'
    assert '--all-targets' in command['argv'], 'actual complete test invocation'
    before,after=command['source_before'],command['source_after']
    assert before==after and after['file_count']==71885, 'whole immutable source before/after'
    assert after['all_git_blobs_match'] is True and after['all_baseline_permission_modes_match'] is True, 'exact source blobs/full modes'
    env=command['environment_additions']
    assert env['PL_MEMBERSHIP_SOURCE_SHA']==PIN and Path(env['PL_MEMBERSHIP_CAPTURE_DIR']).resolve()==capture, 'actual runtime source/capture environment'
    assert command['disk_monitor']['actual_process_exit_code']==0 and command['disk_monitor']['triggered'] is False, 'actual runtime exit and disk supervisor'
    declared=receipt['captured_files']
    expected={r['path']:{'type':'file','sha256':r['sha256'],'bytes':r['bytes'],'full_mode':oct(r['full_mode'])} for r in declared}
    actual={name:r for name,r in captured_inventory.items() if r['type']=='file'}
    assert len(expected)==len(declared)==receipt['closed_file_count'] and expected==actual, 'closed capture bytes/full modes match actual command receipt'
    assert receipt['capture_file_hashes_before_after_match'] is True, 'runtime retention did not transform captures'
    origin=receipt['source_origin_receipt']
    assert digest(Path(origin['path']))==origin['sha256'], 'materialized whole Git origin receipt'
    assert json.loads(Path(origin['path']).read_bytes())['source_commit']==PIN, 'whole Git source pin'
    harness=receipt['retained_membership_harness']
    retained=capture.parent.parent/harness['path']
    assert harness['source_commit']==PIN and digest(retained)==harness['sha256'], 'retained actual membership harness'
    h=hashlib.sha256();count=0
    with gzip.open(retained,'rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):
            h.update(chunk);count+=len(chunk)
    assert h.hexdigest()==harness['uncompressed_sha256'] and count==harness['uncompressed_bytes'], 'retained harness decompression identity'
    return {'completion_sha256':digest(path),'actual_command_json_sha256':receipt['actual_command_json_sha256'],
            'whole_source_guard':after,'captured_files':len(actual),'retained_harness_sha256':harness['uncompressed_sha256'],
            'exact_command_source_environment_capture_modes_and_harness_verified':True}

def run_cell(args):
    label=args.label
    assert re.fullmatch(r'[A-Za-z0-9_.+-]+',label), 'cell label syntax'
    capture=args.capture.resolve(strict=True)
    completion=json.loads(args.completion.read_bytes())
    assert completion['schema_version']==1 and completion['source_sha']==PIN, 'completion source'
    assert completion['cell']==label and completion['exit_code']==0 and completion['captures_closed'] is True, 'completed runtime cell required'
    assert Path(completion['capture_root']).resolve(strict=True)==capture, 'completion capture binding'
    output=OUT/'cells'/label/args.attempt
    assert not output.exists(), 'refuse receipt overwrite; name an explicit new attempt'
    assert not output.is_relative_to(capture), 'checker output must be outside capture'
    source_before=sources()
    for count in (3,5):
        path=capture/f'history-{count}/trace.json'
        trace=json.loads(path.read_bytes())
        assert trace['source_sha']==PIN and len(trace['group']['genesis']['voters'])==count, 'actual runtime source/voter binding'
    output.mkdir(parents=True)
    before=inventory(capture)
    completion_before=review_completion(args.completion,capture,label,before)
    dump(output/'capture-before.json',before)
    dump(output/'source-before.json',source_before)
    (output/'completion.json').write_bytes(args.completion.read_bytes())
    command_results=[]
    for count in (3,5):
        result_path=output/f'history-{count}-result.json'
        command=['taskset','-c','2,4',sys.executable,'-B',str(SOURCE/PREFIX/'check-membership-history.py'),
                 str(capture/f'history-{count}/trace.json'),'--require-admission-settings','--output',str(result_path)]
        started=datetime.now(timezone.utc).isoformat()
        env=os.environ.copy()
        env['PYTHONDONTWRITEBYTECODE']='1'
        completed=subprocess.run(command,cwd=SOURCE,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
        stdout=output/f'history-{count}.stdout.bin'
        stderr=output/f'history-{count}.stderr.bin'
        stdout.write_bytes(completed.stdout)
        stderr.write_bytes(completed.stderr)
        row={'voters':count,'command':command,'cwd':str(SOURCE),'started_utc':started,
             'ended_utc':datetime.now(timezone.utc).isoformat(),'exit_code':completed.returncode,
             'stdout':{'path':stdout.name,'sha256':digest(stdout),'bytes':len(completed.stdout)},
             'stderr':{'path':stderr.name,'sha256':digest(stderr),'bytes':len(completed.stderr)}}
        if result_path.is_file():
            result=json.loads(result_path.read_bytes())
            row['result']={'path':result_path.name,'sha256':digest(result_path)}
            row['counts']={name:result[name] for name in ('events','paired_raw_checkpoints','remote_wal_authorities_bound')}
            row['admission_mode']=result['admission_mode']
            row['admission_changes']=len(result['admission_change_proofs'])
            row['result_scope']=result['scope']
            row['strict_result_valid']=(result['admission_mode']=='observed-settings' and result['admission_settings']==SETTINGS
                                        and row['admission_changes']==2 and result['events']>0)
        else:
            row['strict_result_valid']=False
        command_results.append(row)
        dump(output/f'history-{count}-command.json',row)
    after=inventory(capture)
    source_after=sources()
    completion_after=review_completion(args.completion,capture,label,after)
    dump(output/'capture-after.json',after)
    dump(output/'source-after.json',source_after)
    receipt={'schema_version':1,'source_sha':PIN,'cell':label,'attempt':args.attempt,
             'capture_root':str(capture),'completion_receipt_sha256':digest(output/'completion.json'),
             'captures_bytes_and_full_modes_unchanged':before==after,'frozen_sources_unchanged':source_before==source_after,
             'completion_before':completion_before,'completion_after':completion_after,
             'commands':command_results,'synthetic_control_executions':0,
             'scope':'Two fresh actual typed 3/5-node captured histories; strict observed-settings admission, no native wire or exhaustive proof.'}
    receipt['passed']=(before==after and source_before==source_after and completion_before==completion_after and
                       all(r['exit_code']==0 and r['strict_result_valid'] for r in command_results))
    dump(output/'cell-result.json',receipt)
    print(json.dumps({'cell':label,'passed':receipt['passed'],'receipt':str(output/'cell-result.json'),
                      'sha256':digest(output/'cell-result.json')}))
    return 0 if receipt['passed'] else 1

def seal_matrix(args):
    expected=json.loads(args.expected_cells.read_bytes())
    assert expected['source_sha']==PIN and type(expected['cells']) is list and len(expected['cells'])==12, 'coordinator twelve-cell specification'
    assert len(set(expected['cells']))==12, 'distinct twelve cells'
    cells=[]
    for label in expected['cells']:
        assert re.fullmatch(r'[A-Za-z0-9_.+-]+',label), 'matrix cell syntax'
        attempts=sorted((OUT/'cells'/label).glob('*/cell-result.json'),
                        key=lambda p:int(p.parent.name.removeprefix('attempt-')))
        rows=[{'path':str(p),'sha256':digest(p),'result':json.loads(p.read_bytes())} for p in attempts]
        cells.append({'cell':label,'status':'pending' if not rows else ('passed' if rows[-1]['result']['passed'] else 'failed'), 'attempts':rows})
    selected=[c['attempts'][-1]['result'] for c in cells if c['attempts']]
    commands=[r for c in selected for r in c['commands']]
    all_attempts=[a['result'] for c in cells for a in c['attempts']]
    receipt={'schema_version':1,'source_sha':PIN,'expected_cell_spec_sha256':digest(args.expected_cells),
             'passed':len(selected)==12 and all(c['status']=='passed' for c in cells),
             'cells':cells,'completed_cells':len(selected),'failed_cells':sum(c['status']=='failed' for c in cells),
             'pending_cells':sum(c['status']=='pending' for c in cells),
             'actual_history_commands':len(commands),'checker_exit_failures':sum(c['exit_code']!=0 for c in commands),
             'all_attempt_cell_failures':sum(not a['passed'] for a in all_attempts),
             'all_attempt_checker_exit_failures':sum(r['exit_code']!=0 for a in all_attempts for r in a['commands']),
             'events':sum(c.get('counts',{}).get('events',0) for c in commands),
             'raw_checkpoint_pairs':sum(c.get('counts',{}).get('paired_raw_checkpoints',0) for c in commands),
             'remote_authorities_bound':sum(c.get('counts',{}).get('remote_wal_authorities_bound',0) for c in commands),
             'actual_admission_change_proofs':sum(c.get('admission_changes',0) for c in commands),
             'synthetic_control_executions':0,
             'scope':'Finite coordinator-completed captured runtime cells; settings are read from immutable traces, never synthesized.'}
    assert not args.output.exists(), 'matrix receipt overwrite refused'
    dump(args.output,receipt)
    print(json.dumps({k:receipt[k] for k in ('passed','completed_cells','failed_cells','pending_cells','actual_history_commands','checker_exit_failures')}))
    return 0 if receipt['passed'] else 1

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run-cell')
    run.add_argument('--label',required=True)
    run.add_argument('--capture',type=Path,required=True)
    run.add_argument('--completion',type=Path,required=True)
    run.add_argument('--attempt',default='attempt-1')
    seal=sub.add_parser('seal-matrix')
    seal.add_argument('--expected-cells',type=Path,required=True)
    seal.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='run-cell':
        assert re.fullmatch(r'attempt-[0-9]+',args.attempt), 'attempt syntax'
        return run_cell(args)
    return seal_matrix(args)

if __name__=='__main__':
    raise SystemExit(main())

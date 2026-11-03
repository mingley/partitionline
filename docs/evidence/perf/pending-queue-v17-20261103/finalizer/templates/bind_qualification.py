#!/usr/bin/env python3
"""Record same-job workflow context and hashes after v17 qualification succeeds.

This is an ordering witness from the illustrative workflow, not a signed
attestation that independently proves the origin of an arbitrary file.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

SPEC_SHA='__V17_SPEC_SHA__'
MANIFEST_SHA='__V17_MANIFEST_SHA__'
GUARD_SHA='__V17_GUARD_SHA__'
GUARD_SIZE=__V17_GUARD_SIZE__
MANIFEST_PATH='docs/evidence/client/candidate-20261030/v17/import-manifest.json'

def verify_v17_helpers(checkout, spec):
    # Execute the independently pinned guard bytes, never a second file read.
    # Directory-relative opens reject a symlink in the guard's parent or file.
    flags=os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    rootfd=os.open(checkout,flags)
    try:
        scriptsfd=os.open('scripts',flags,dir_fd=rootfd)
        try:
            fd=os.open('ci-verify-client-candidate-helpers.py',os.O_RDONLY | os.O_NOFOLLOW,dir_fd=scriptsfd)
            try:
                with os.fdopen(fd,'rb') as stream:data=stream.read(GUARD_SIZE+1)
            except BaseException:
                raise
        finally:os.close(scriptsfd)
    finally:os.close(rootfd)
    if len(data)!=GUARD_SIZE or hashlib.sha256(data).hexdigest()!=GUARD_SHA:
        raise RuntimeError('unexpected v17 helper guard')
    namespace={'__name__':'pending_bench_guard','__file__':str(checkout/'scripts/ci-verify-client-candidate-helpers.py')}
    exec(compile(data,namespace['__file__'],'exec'),namespace)
    manifest=checkout/MANIFEST_PATH
    if digest(manifest)!=MANIFEST_SHA:raise RuntimeError('unexpected v17 import manifest')
    if namespace['verify'](checkout,manifest)!=6:raise RuntimeError('incomplete helper verification')
    document=json.loads(manifest.read_text(),object_pairs_hook=namespace['unique_pairs'],parse_constant=namespace['reject_constant'])
    if document['required_existing_root_files']!=spec['qualification_helper_overlay']['helper_pins']:
        raise RuntimeError('qualification helper pins differ from pinned specification')
    return {'helper_manifest_sha256':MANIFEST_SHA,'helper_guard_sha256':GUARD_SHA}


def digest(path):
    with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def load_pinned_controls(spec_path, spec):
    asset=spec['controls']
    name=asset['path'];path=Path(name)
    if path.is_absolute() or '..' in path.parts or str(path)!=name or len(path.parts)!=1:
        raise RuntimeError('unsafe controls asset path')
    source=spec_path.parent/path
    if source.is_symlink() or not source.is_file():raise RuntimeError('unsafe controls asset')
    raw=source.read_bytes()
    if len(raw)!=asset['bytes'] or hashlib.sha256(raw).hexdigest()!=asset['sha256']:
        raise RuntimeError('qualification controls drift')
    return json.loads(raw)

def require_complete_gates(summary, spec, controls):
    # Mirrors the pinned v17 driver's target construction, in addition to the
    # original count/passed checks. A same-job report is still trusted input.
    targets=set(spec['fixed_full_targets']) | {'partitionline'}
    targets.update(row['target'] for row in controls['integrations'])
    if not all(isinstance(t,str) and re.fullmatch('[A-Za-z0-9_]+',t) for t in targets):
        raise RuntimeError('invalid qualification target')
    expected={'format','clippy'} | {lane+'-'+target for lane in ('default','all-features') for target in targets}
    gates=summary.get('gates')
    if not isinstance(gates,list) or not all(isinstance(row,dict) and isinstance(row.get('name'),str) and row.get('passed') is True for row in gates):
        raise RuntimeError('qualification gates are malformed or failed')
    names=[row['name'] for row in gates]
    if len(names)!=len(expected) or len(names)!=len(set(names)) or set(names)!=expected:
        raise RuntimeError('qualification gates are missing, duplicated or unexpected')


def same_job_context(env,qualification,checkout):
    run=env.get('GITHUB_RUN_ID','');attempt=env.get('GITHUB_RUN_ATTEMPT','');sha=env.get('GITHUB_SHA','')
    if env.get('GITHUB_ACTIONS')!='true' or env.get('GITHUB_WORKFLOW')!='candidate qualification' or env.get('GITHUB_JOB')!='qualify':
        raise RuntimeError('requires the candidate qualification workflow job')
    if not re.fullmatch('[1-9][0-9]*',run) or not re.fullmatch('[1-9][0-9]*',attempt) or not re.fullmatch('[0-9a-f]{40}',sha):
        raise RuntimeError('invalid GitHub run, attempt or checkout SHA')
    temp=env.get('RUNNER_TEMP');workspace=env.get('GITHUB_WORKSPACE')
    if not temp or not workspace:raise RuntimeError('missing GitHub workspace or runner temp')
    temp=Path(temp).resolve();workspace=Path(workspace).resolve()
    qual=Path(qualification).resolve();checkout=Path(checkout).resolve()
    if qual!=temp/'candidate-qualification' or checkout!=workspace:
        raise RuntimeError('requires the fixed same-job qualification and workspace paths')
    return {'github_run_id':run,'github_run_attempt':attempt,'github_sha':sha,
            'github_workflow':env['GITHUB_WORKFLOW'],'github_job':env['GITHUB_JOB'],
            'qualification_path':str(qual),'checkout_path':str(checkout),'runner_temp':str(temp)}

def expected_witness(context,contract_sha,summary_sha,driver_sha,binder_sha,helper_manifest_sha,helper_guard_sha):
    return {'schema':1,'scope':'same-job workflow ordering witness; not an independent signature',**context,
            'spec_sha256':SPEC_SHA,'contract_sha256':contract_sha,'summary_sha256':summary_sha,
            'driver_sha256':driver_sha,'binder_sha256':binder_sha,
            'helper_manifest_sha256':helper_manifest_sha,'helper_guard_sha256':helper_guard_sha}

def validate_witness(actual,expected):
    return isinstance(actual,dict) and actual==expected

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--qualification',type=Path,required=True)
    parser.add_argument('--checkout',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();context=same_job_context(os.environ,args.qualification,args.checkout)
    qual=Path(context['qualification_path']);checkout=Path(context['checkout_path']);output=args.output.resolve()
    if output!=Path(context['runner_temp'])/'pending-queue-qualification.json':
        raise RuntimeError('witness must use the fixed same-job path')
    actual=subprocess.run(['git','rev-parse','HEAD'],cwd=checkout,check=True,capture_output=True,text=True,timeout=60).stdout.strip()
    if actual!=context['github_sha']:raise RuntimeError('checkout does not match GitHub SHA')
    spec=checkout/'docs/evidence/client/candidate-20261030/v17/spec.json'
    if digest(spec)!=SPEC_SHA:raise RuntimeError('unexpected v17 specification')
    spec_document=json.loads(spec.read_text())
    helper_pins=verify_v17_helpers(checkout,spec_document)
    contract_path=qual/'contract.json';summary_path=qual/'summary.json'
    contract=json.loads(contract_path.read_text());summary=json.loads(summary_path.read_text())
    driver=checkout/'scripts/ci-qualify-client-candidate.py'
    if contract.get('checkout_commit')!=actual or contract.get('spec_sha256')!=SPEC_SHA or contract.get('driver_sha256')!=digest(driver):
        raise RuntimeError('qualification contract does not match checkout and driver')
    if summary.get('passed') is not True or summary.get('baseline_expectations_matched') is not True or \
       summary.get('required_library_controls')!=166 or summary.get('required_integration_controls')!=17 or \
       not summary.get('gates') or not all(g.get('passed') is True for g in summary['gates']):
        raise RuntimeError('the complete v17 qualification has not passed')
    require_complete_gates(summary,spec_document,load_pinned_controls(spec,spec_document))
    witness=expected_witness(context,digest(contract_path),digest(summary_path),digest(driver),digest(Path(__file__)),helper_pins['helper_manifest_sha256'],helper_pins['helper_guard_sha256'])
    with output.open('x') as stream:json.dump(witness,stream,indent=2);stream.write('\n')
    return 0

if __name__=='__main__':
    try:raise SystemExit(main())
    except Exception as error:print(str(error),file=sys.stderr);raise SystemExit(1)

#!/usr/bin/env python3
"""Add schema fields to retained native runtime results without changing measurements.

Original result files and raw observations stay unchanged. Each new view binds
its original result and this formatter. This corrects the early adapter's broker
mode, histogram bounds, and attempt metadata; it does not qualify missing data.
"""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def module(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def corrected_view(original, original_path, formatter_commit):
    doc=copy.deepcopy(original)
    broker=doc['provenance']['broker']
    if broker['version']!='4.3.1' or broker['mode']!='Owned single-node KRaft broker/controller; acks=1 RF=1 minISR=1':
        raise ValueError('unexpected original broker identity or mode')
    broker['deployment_scope']=broker['mode'];broker['mode']='kraft'
    histogram=doc['measurements']['latency']['raw_histogram']
    for bucket in histogram['buckets']:
        upper=bucket['upper_bound_us']
        if type(upper) is not int or upper<0 or (upper and upper&(upper-1)):
            raise ValueError('original histogram is not the declared power-of-two census')
        bucket['min_us']=0 if upper==0 else upper//2+1
        bucket['max_us']=upper
    history=doc['repetition_history']
    if history['total_attempts']!=1 or len(history['attempts'])!=1:
        raise ValueError('original attempt population differs')
    attempt=history['attempts'][0]
    attempt['attempt_number']=1
    attempt['timestamp_utc']=doc['provenance']['timestamps']['end_time_utc']
    attempt['timestamp_scope']='Observed original workload end, not the formatting time'
    if attempt['status']=='failed_capacity':
        if doc['outcomes']['rejected']<=0 or doc['scenario']['cell_disposition']!='failed':
            raise ValueError('capacity failure metadata disagrees with outcomes')
        attempt['original_status']='failed_capacity';attempt['status']='failed_abort'
        attempt['failure_kind']='capacity_rejection'
        attempt['error_message']='benchmark pending capacity exhausted'
    elif attempt['status']=='passed_measurement':
        attempt['error_message']=None
    else:raise ValueError('unknown original attempt disposition')
    artifact=dict(path=str(original_path),type='original-runtime-result',sha256=sha(original_path),
                  size_bytes=original_path.stat().st_size)
    doc['provenance']['artifacts'].append(artifact)
    doc['provenance']['source']['artifact_formatter']=dict(git_commit=formatter_commit,
        script_sha256=sha(Path(__file__).resolve()),original_result_sha256=artifact['sha256'],
        scope='Post-capture schema-field correction only; timing, outcomes, resources, percentiles and raw observations unchanged')
    # These objects contain every measured value and outcome. No measurement
    # changes are permitted beyond adding exact histogram range endpoints.
    expected=copy.deepcopy(doc['measurements'])
    for bucket in expected['latency']['raw_histogram']['buckets']:
        del bucket['min_us'];del bucket['max_us']
    if expected!=original['measurements'] or doc['outcomes']!=original['outcomes'] or doc['integrity']!=original['integrity']:
        raise ValueError('formatting changed observations')
    return doc


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--formatter-commit',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--partial',action='store_true',help='Inspect retained completed records without claiming matrix completion')
    args=parser.parse_args()
    source=args.source.resolve();original=args.original.resolve();output=args.output.resolve()
    if subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()!=args.formatter_commit:
        raise ValueError('formatter source revision differs')
    if subprocess.check_output(['git','-C',str(source),'status','--porcelain']):
        raise ValueError('formatter checkout is dirty')
    if not args.partial and not (original/'completion.json').is_file():
        raise ValueError('matrix has no completion receipt')
    output.mkdir(parents=True,exist_ok=False)
    import jsonschema
    schema=jsonschema.Draft7Validator(read(source/'benchmarks/result-schema.json'))
    report=module(source/'scripts/benchmark-report.py','canonical_native_report')
    plan=read(original/'plan.json');manifest=[]
    for path in sorted(original.glob('*/*.result.json')):
        old=read(path)
        if old['provenance']['source']['git_commit']!=plan['source_commit']:
            raise ValueError('original driver source differs from captured plan')
        for item in old['provenance']['artifacts']:
            p=Path(item['path'])
            if not p.is_file() or p.stat().st_size!=item['size_bytes'] or sha(p)!=item['sha256']:
                raise ValueError('original raw artifact changed: '+str(p))
        binary=Path(old['provenance']['binary']['path'])
        if sha(binary)!=old['provenance']['binary']['sha256']:
            raise ValueError('captured executable changed')
        doc=corrected_view(old,path,args.formatter_commit)
        errors=list(schema.iter_errors(doc))
        if errors:raise ValueError('; '.join(str(list(e.absolute_path))+': '+e.message for e in errors))
        valid,errors,_=report.BenchmarkValidator().validate(doc)
        if not valid:raise ValueError('semantic validator rejected corrected view: '+repr(errors))
        directory=output/path.parent.name;directory.mkdir()
        result=directory/path.name
        with result.open('x') as file:
            json.dump(doc,file,indent=2,allow_nan=False);file.write('\n');file.flush();os.fsync(file.fileno())
        argv=[sys.executable,'-B',str(source/'benchmarks/runtime/tools/parent-bound-exec.py'),
            str(os.getpid()),sys.executable,'-B',str(source/'scripts/benchmark-report.py'),str(result)]
        started=time.monotonic_ns()
        run=subprocess.run(argv,text=True,capture_output=True,timeout=15)
        receipt=dict(command=argv,parent_waited=True,parent_death_bound=True,exit_code=run.returncode,
            wall_seconds=(time.monotonic_ns()-started)/1e9,deadline_seconds=15)
        (directory/'validator.process.json').write_text(json.dumps(receipt,indent=2)+'\n')
        (directory/'validator.stdout').write_text(run.stdout);(directory/'validator.stderr').write_text(run.stderr)
        if run.returncode:raise ValueError('actual CLI rejected corrected view')
        controls=[]
        for name in ('missing_histogram_bound','invalid_mode','missing_attempt_timestamp','changed_ack_count'):
            bad=copy.deepcopy(doc)
            if name=='missing_histogram_bound':del bad['measurements']['latency']['raw_histogram']['buckets'][0]['min_us']
            elif name=='invalid_mode':bad['provenance']['broker']['mode']='invented'
            elif name=='missing_attempt_timestamp':del bad['repetition_history']['attempts'][0]['timestamp_utc']
            else:bad['integrity']['high_watermark_audit']['total_offset_delta']-=1
            schema_errors=list(schema.iter_errors(bad));valid,semantic_errors,_=report.BenchmarkValidator().validate(bad)
            if not schema_errors and valid:raise ValueError('changed actual-result control was accepted')
            controls.append(dict(control=name,schema_errors=[e.message for e in schema_errors],
                semantic_errors=semantic_errors,rejected=True))
        (directory/'controls.json').write_text(json.dumps(controls,indent=2)+'\n')
        manifest.append(dict(original=str(path),original_sha256=sha(path),view=str(result),view_sha256=sha(result),
            schema_validation=True,actual_cli_exit_code=run.returncode,controls=len(controls),
            disposition=doc['scenario']['cell_disposition']))
    if not manifest:raise ValueError('no original result records')
    (output/'manifest.json').write_text(json.dumps(dict(formatter_commit=args.formatter_commit,
        driver_source_commit=plan['source_commit'],partial=args.partial,results=manifest,
        raw_measurements_unchanged=True),indent=2)+'\n')
    print(len(manifest),'actual native results re-expressed and validated')


if __name__=='__main__':main()

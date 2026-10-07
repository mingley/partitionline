#!/usr/bin/env python3
"""Replay retained baseline measurements and reject altered clocks/accounting."""
import argparse
import copy
import hashlib
import importlib.util
import json
import math
from pathlib import Path


def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return json.loads(p.read_text())
def module(p,name):
    spec=importlib.util.spec_from_file_location(name,p)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();b=a.input; receipts=0; failed_receipts=0
    for cohort in ('codec-01','codec-02','nb-01','nb-reproduce-01','iai-01','iai-02','iai-03',
                   'native-01','native-02','request-01','request-02','request-03','latency-01','latency-02'):
        for path in (b/cohort).glob('**/*.process.json'):
            row=read(path)
            if not row['parent_waited'] or not isinstance(row['exit_code'],int): raise ValueError('unreaped process receipt')
            for name,digest in row['artifacts'].items():
                if sha(path.parent/name)!=digest: raise ValueError('command artifact changed')
            receipts+=1;failed_receipts+=int(row['exit_code']!=0)
    mapping=[('codec-01',1),('codec-01',2),('codec-01',3),('codec-02',1),('codec-02',2)]
    cases=0
    for cohort,rep in mapping:
        directory=b/cohort/f'r{rep:02d}'
        for family,expected in (('codec',50),('zstd',72)):
            row=read(directory/(family+'-validated.json'));assert len(row['cases'])==expected
            found={}
            for estimate in (directory/(family+'-criterion')).glob('**/new/estimates.json'):
                meta=read(estimate.parent/'benchmark.json'); key=meta['full_id']
                if key in found: raise ValueError('duplicate case')
                found[key]=estimate
            if set(found)!=set(row['cases']): raise ValueError('native inventory differs')
            for key,case in row['cases'].items():
                estimate=found[key];sample=estimate.parent/'sample.json';samples=read(sample)
                if sha(estimate)!=case['estimates_sha256'] or sha(sample)!=case['samples_sha256']: raise ValueError('native samples changed')
                if len(samples['times'])!=30 or len(samples['iters'])!=30: raise ValueError('sample count differs')
                if any(not math.isfinite(v) or v<=0 for v in samples['times']+samples['iters']): raise ValueError('invalid native clock')
                if read(estimate)['median']['point_estimate']!=case['median_ns_per_operation']: raise ValueError('median differs')
                cases+=1
    instruction_cases=0
    for rep in range(1,6):
        for family,expected in (('iai',10),('json1k_iai',24)):
            row=read(b/'iai-03'/f'r{rep:02d}'/(family+'-validated.json'));assert len(row['cases'])==expected
            for case in row['cases'].values():
                source=Path(case['source_summary'])
                if sha(source)!=case['sha256']: raise ValueError('instruction summary changed')
                actual=read(source)['callgrind_summary']['callgrind_run']['total']['summary']['Ir']['metrics']['Left']
                if actual!=case['instructions'] or actual<=0: raise ValueError('instruction count differs')
                instruction_cases+=1
    native=read(b/'native-02/rows.json');assert len(native)==10
    for row in native:
        p=row['producer'];f=row['fetch'];j=row['java'];resources=row['producer_resources']
        if not (p['acked']==p['accepted']==8_000_000 and p['acknowledged_total']==8_010_000): raise ValueError('native admission/ack differs')
        if not (f['records_verified']==8_000_000 and f['consumer_closed'] and j['verified']==8_010_000 and j['consumer_closed']): raise ValueError('native independent readback incomplete')
        expected={str(i):(10000+5-i)//6+(8000000+5-i)//6 for i in range(6)}
        if row['offsets']!=expected: raise ValueError('native partition fences differ')
        if resources['exit_code']!=0 or not resources['parent_waited']: raise ValueError('resource child did not complete')
        if abs(p['acked_rec_s']*p['elapsed_s']/8_000_000-1)>.00001: raise ValueError('throughput arithmetic differs')
    request=read(b/'request-03/rows.json');assert len(request)==5
    for row in request:
        assert len(row['sdk_peers'])==3 and all(peer['actual']['full_seeded_records_verified'] for peer in row['sdk_peers'])
    request_controls=read(b/'request-03/negative-controls.json');assert len(request_controls)==6
    latency=module(b/'latency-02/executed-latency-wrapper.py','measured_latency_validator')
    latency_rows=read(b/'latency-02/rows.json');raw_rows=[row for row in latency_rows if row['cell']=='lb-latency-openloop'];assert len(raw_rows)==15
    streams={}
    for row in raw_rows:
        file=b/'latency-02'/f'r{row["repetition"]:02d}-load-{row["load_percent"]}'/'latency.stdout'
        if sha(file)!=row['raw_sha256']: raise ValueError('raw latency stream changed')
        samples=[json.loads(line) for line in file.read_text().splitlines()];summary=samples.pop()
        if summary!=row['summary']: raise ValueError('latency summary differs')
        latency.validate_open_loop(samples,summary,row['rate_per_second'])
        if row['java']['verified']!=10000+summary['outcomes']['acknowledged'] or not row['java']['consumer_closed']:
            raise ValueError('latency append fence/full payload verification differs')
        streams.setdefault(row['load_percent'],(samples,summary,row['rate_per_second']))
    negatives=[]
    for percent in (10,80):
        samples,summary,rate=streams[percent]
        for name in ('arrival-id','absolute-schedule','ack-before-offer','fake-acceptance','raw-duration','percentile',
                     'sample-count','offered','unknown','disposition','rate','warmup','floor'):
            changed=list(samples);changed[0]=dict(samples[0]);bad=copy.deepcopy(summary)
            if name=='arrival-id':changed[0]['id']+=1
            if name=='absolute-schedule':changed[0]['intended_arrival_ns']+=1
            if name=='ack-before-offer':changed[0]['acknowledgment_observed_ns']=0
            if name=='fake-acceptance':changed[0]['accepted']=False
            if name=='raw-duration':changed[0]['end_to_end_ns']+=1
            if name=='percentile':bad['end_to_end']['p99_us']+=1
            if name=='sample-count':bad['end_to_end']['sample_count']-=1
            if name=='offered':bad['outcomes']['offered']+=1
            if name=='unknown':bad['outcomes']['unknown']+=1
            if name=='disposition':bad['run_disposition']='executed' if percent==80 else 'failed'
            if name=='rate':bad['rate_per_second']+=1
            if name=='warmup':bad['warmup_records']-=1
            if name=='floor':bad['end_to_end']['sample_floor_met']=False
            try:latency.validate_open_loop(changed,bad,rate)
            except (ValueError,TypeError):negatives.append(dict(load_percent=percent,mutation=name,rejected=True))
            else:raise ValueError('modified latency history accepted: '+name)
    result=dict(status='pass',scope='local/unsigned',suite_hold='active',process_receipts=receipts,
        retained_nonzero_receipts=failed_receipts,qualified_native_timing_cases=cases,qualified_instruction_cases=instruction_cases,
        native_bulk_fetch_repetitions=10,micro_request_repetitions=5,genuine_request_corruption_controls=6,
        open_loop_repetitions=15,open_loop_independent_replay=True,negative_replay_controls=negatives,
        overloaded_runs_retained=sum(row['summary']['run_disposition']=='failed' for row in raw_rows),
        excluded_unwaited_codec_partial='codec-01/r04; environment interruption retained separately; no completion inferred',
        canonical_nb_validators=sum(1 for name in ('nb-01','nb-reproduce-01') for _ in (b/name).glob('**/validator.process.json')),
        no_missing_metric_filled_with_zero=True,performance_claims_valid=False)
    with a.output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(result))


if __name__=='__main__':main()

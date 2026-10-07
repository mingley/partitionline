#!/usr/bin/env python3
"""Summarize completed local cohorts without changing raw artifacts."""
import argparse
import hashlib
import importlib.util
import json
import math
import re
from pathlib import Path
import statistics


def read(p): return json.loads(p.read_text())

def measured(v): return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)

def numeric(obj,prefix=''):
    result={}
    for key,value in obj.items():
        name=prefix+'.'+key if prefix else key
        if isinstance(value,dict): result.update(numeric(value,name))
        elif measured(value): result[name]=value
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--recorder',type=Path,required=True)
    a=p.parse_args(); b=a.input
    spec=importlib.util.spec_from_file_location('aggregate_baseline_recorder',a.recorder)
    baseline=importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
    a.output.mkdir(exist_ok=False)
    qualified={name:read(b/name/'completion.json') for name in
        ['nb-01','nb-reproduce-01','codec-02','iai-03','native-02','request-03','latency-02']}
    for name,value in qualified.items():
        if value['status']!='recorded' or not value['source_guards_passed'] or not value['owned_process_groups_empty']:
            raise ValueError('cohort incomplete: '+name)
    timings=[]
    mapping=[('codec-01',1),('codec-01',2),('codec-01',3),('codec-02',1),('codec-02',2)]
    census=[]
    for global_rep,(cohort,rep) in enumerate(mapping,1):
        directory=b/cohort/f'r{rep:02d}'
        census.append(dict(aggregate_repetition=global_rep,cohort=cohort,raw_repetition=rep,
                          census_sha256=baseline.sha(directory/'census.stdout'),
                          json1k=read(directory/'json1k-census.stdout'),
                          zstd=[json.loads(s) for s in (directory/'zstd-census.stdout').read_text().splitlines()]))
        for line in (directory/'census.stdout').read_text().splitlines():
            match=re.fullmatch(r'census (.+): allocs=(\d+) bytes=(\d+)',line)
            if not match: raise ValueError('allocation census line differs')
            timings.append(dict(family='codec-allocation-census',cell=match[1],repetition=global_rep,
                metrics={'allocation_calls_per_operation':int(match[2]),'allocated_bytes_per_operation':int(match[3])}))
        for row in census[-1]['json1k']['cells']:
            timings.append(dict(family='json1k-allocation-census',cell=row['cell'],repetition=global_rep,
                metrics={key:row[key] for key in ('allocations','allocated_bytes','compressed_to_section_ratio')}))
        for row in census[-1]['zstd']:
            timings.append(dict(family='zstd-allocation-census',cell=row['operation']+'/'+row['case'],repetition=global_rep,
                metrics={key:row[key] for key in ('allocations_per_operation','requested_allocation_bytes_per_operation','compressed_over_uncompressed')}))
        for family,expected in [('codec',50),('zstd',72)]:
            row=read(directory/(family+'-validated.json'))
            if len(row['cases'])!=expected: raise ValueError('native case population differs')
            receipt=read(directory/(family+'.process.json'))
            if not receipt['parent_waited'] or receipt['exit_code']!=0: raise ValueError('unqualified native repetition')
            for name,case in row['cases'].items():
                value=case['median_ns_per_operation']; throughput=case['throughput']
                metrics=dict(ns_per_operation=value)
                if 'Elements' in throughput: metrics['ns_per_record']=value/throughput['Elements']
                if 'Bytes' in throughput: metrics['bytes_per_second']=throughput['Bytes']*1e9/value
                timings.append(dict(family=family,cell=name,repetition=global_rep,
                                    original_cohort=cohort,original_repetition=rep,metrics=metrics))
    native=read(b/'native-02/rows.json')
    if len(native)!=10 or [row['repetition'] for row in native]!=list(range(1,11)):
        raise ValueError('ten native full readbacks required')
    for row in native:
        if row['producer']['acked']!=8_000_000 or row['fetch']['records_verified']!=8_000_000 or row['java']['verified']!=8_010_000:
            raise ValueError('record accounting differs')
        rep=(row['repetition']-1)%5+1
        timings.append(dict(family=row['cohort'],cell='lb-bulk',repetition=rep,metrics={
            'records_per_second':row['producer']['acked_rec_s'],'measured_elapsed_seconds':row['producer']['elapsed_s'],
            **{('whole_child_'+key):row['producer_resources'][key] for key in ('user_cpu_seconds','system_cpu_seconds','peak_rss_kbytes','wall_seconds','voluntary_context_switches','involuntary_context_switches')}}))
        timings.append(dict(family=row['cohort'],cell='lb-fetch',repetition=rep,
            metrics=numeric({key:row['fetch'][key] for key in ('records_per_second','cpu_ns_per_record','allocations',
                'allocated_bytes','baseline_rss_bytes','peak_rss_bytes','average_rss_bytes','elapsed_seconds','fetch_rounds')})))
    for cohort in ('nb-01','nb-reproduce-01'):
        for row in read(b/cohort/'rows.json'):
            timings.append(dict(family=cohort,cell=row['cell'],repetition=row['repetition'],metrics=numeric(row['measurements'])))
    for row in read(b/'request-03/rows.json'):
        timings.append(dict(family='request',cell='micro-request',repetition=row['repetition'],metrics={
            'ns_per_operation':row['median_ns_per_operation'],'ns_per_record':row['median_ns_per_operation']/100,
            'allocation_calls_per_operation':row['raw']['allocations_per_operation'],
            'allocated_bytes_per_operation':row['raw']['allocated_bytes_per_operation']}))
    for row in read(b/'iai-03/rows.json'):
        for name,case in row['cases'].items():
            timings.append(dict(family=row['family'],cell=name,repetition=row['repetition'],metrics={'simulated_user_instructions':case['instructions']}))
    for row in read(b/'latency-02/rows.json'):
        if row['cell']=='lb-capacity-sequential':
            timings.append(dict(family='latency',cell=row['cell'],repetition=row['repetition'],metrics={
                'records_per_second':row['records_per_second'],**numeric(row['raw'])}))
        else:
            timings.append(dict(family='latency',cell='lb-latency-openloop-'+str(row['load_percent']),
                repetition=row['repetition'],metrics=numeric({key:row['summary'][key] for key in
                    ('end_to_end','enqueue_to_ack_upper_bound','schedule_lag','enqueue_wait_upper_bound','max_pending_observed','outcomes','capacity_rejections')})))
    grouped={}
    for row in timings:
        for metric,value in row['metrics'].items():
            key=(row['family'],row['cell'],metric)
            grouped.setdefault(key,[]).append((row['repetition'],value))
    summary=[]
    for (family,cell,metric),values in sorted(grouped.items()):
        values.sort()
        if [rep for rep,_ in values]!=[1,2,3,4,5]: raise ValueError('five independent repetitions per metric required')
        summary.append(dict(family=family,cell=cell,metric=metric,**baseline.bootstrap([v for _,v in values])))
    policy=read(b/'reproducibility-policy-before-reruns.json')
    repeat=[]
    for cell,primary,reproduce,metric in [('nb-produce-bulk','nb-01','nb-reproduce-01','throughput.records_per_second'),
                                        ('lb-bulk','primary','reproduce','records_per_second')]:
        def select(family):
            return next(row for row in summary if row['family']==family and row['cell']==cell and row['metric']==metric)
        first,second=select(primary),select(reproduce)
        difference=abs(second['median']/first['median']-1)
        repeat.append(dict(cell=cell,metric=metric,primary=first,reproduce=second,
            relative_median_difference=difference,declared_limit=policy['max_absolute_relative_median_difference'],
            within_declared_noise=difference<=policy['max_absolute_relative_median_difference']))
    for cohort in ('native-02','latency-02'):
        closure=read(b/cohort/'closure.json')
        if not closure['parent_waited'] or not closure['group_empty'] or not closure['supervisor_joined'] or not all(row['reusable'] for row in closure['ports']):
            raise ValueError('owned broker did not close')
    baseline.save(a.output/'statistics.json',dict(schema_version=1,source_commit='c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25',
        scope='local/unsigned',suite_hold='active',statistics='Median of independent repetition medians; bootstrap 95% CI, 20000 resamples, seed912',
        qualified_codec_mapping=mapping,excluded_codec_repetitions=['codec-01/r04'],measurements=summary))
    baseline.save(a.output/'census.json',census)
    baseline.save(a.output/'reproducibility.json',dict(policy=policy,results=repeat,
        all_within_declared_noise=all(row['within_declared_noise'] for row in repeat)))
    baseline.save(a.output/'completion.json',dict(status='aggregated',scope='local/unsigned',suite_hold='active',
        metric_rows=len(summary),source_cohorts=qualified,reproducibility_qualified=all(row['within_declared_noise'] for row in repeat),
        no_public_performance_claim=True,production_ready=False))
    print(f'{len(summary)} actual metric rows summarized; repeatability='+str(all(row['within_declared_noise'] for row in repeat)))


if __name__=='__main__': main()

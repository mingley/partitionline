#!/usr/bin/env python3
"""Summarize matched benchmark build profiles and their fresh baseline cohort."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics

SEED = 962
RESAMPLES = 20_000


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def median_interval(values, seed):
    if len(values)!=5 or any(not math.isfinite(v) or v<0 for v in values):
        raise ValueError('five finite observed repetitions required')
    rng=random.Random(seed)
    medians=sorted(statistics.median(rng.choices(values,k=5)) for _ in range(RESAMPLES))
    return dict(values=values,median=statistics.median(values),bootstrap_95_ci=[medians[499],medians[19499]])


def metric_values(rows, cell):
    fields={'acknowledged_or_verified_records_per_second':lambda x:x['measurements']['throughput']['records_per_second'],
        'client_cpu_ns_per_record':lambda x:x['measurements']['client_resources']['cpu_ns_per_record'],
        'client_allocations_per_record':lambda x:x['measurements']['client_resources']['allocations']['allocations_per_record'],
        'client_sampled_peak_rss_bytes':lambda x:x['measurements']['client_resources']['rss']['peak_rss_bytes']}
    if cell=='lb-latency-openloop':fields['scheduled_ack_p99_us']=lambda x:x['measurements']['latency']['p99']
    return {name:[getter(row) for row in rows] for name,getter in fields.items()}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--nb',type=Path,required=True);p.add_argument('--native',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('new output required')
    inputs={};rows=[]
    for directory in (a.nb,a.native):
        for name in ('rows.json','completion.json','matrix-plan.json','host-before.json','host-after.json'):
            path=directory/name;inputs[str(path)]=sha(path)
        data=json.loads((directory/'rows.json').read_text());rows.extend(data)
    keys=sorted({(row['cell'],row['load']) for row in rows},key=str)
    plans=[json.loads((directory/'matrix-plan.json').read_text()) for directory in (a.nb,a.native)]
    configs=plans[0]['configs']
    if configs!=plans[1]['configs'] or any(plan['repetitions']!=5 for plan in plans):
        raise ValueError('identical five-repetition build matrices required')
    expected=['none_16_portable','thin_16_portable','fat_16_portable','thin_1_portable','thin_16_native','pgo_use']
    if [config['name'] for config in configs]!=expected:
        raise ValueError('six declared comparison builds required in baseline-first order')
    if any(config['flavor']!='current_thread' or config['workers']!=0 or config['instrumented_training'] for config in configs):
        raise ValueError('comparison must exclude instrumented training and keep the runtime fixed')
    if keys!=[('lb-bulk',None),('nb-fetch-bulk',None),('nb-produce-bulk',None)] or len(rows)!=105:
        raise ValueError('all 90 primary and 15 fresh-baseline rows required')
    seen=set()
    for row in rows:
        path=Path(row['artifact'])
        if path in seen or sha(path)!=row['sha256']:
            raise ValueError('unique hash-bound actual result required')
        seen.add(path);inputs[str(path)]=row['sha256']
        sidecar=path.parent/'benchmark-build.json'
        binding=json.loads(sidecar.read_text());config=row['config']
        if binding!=dict(name=config['name'],build=config['build'],binaries=config['binaries'],instrumented_training=False):
            raise ValueError('actual measured build binding differs')
        inputs[str(sidecar)]=sha(sidecar)
        for binary in config['binaries'].values():
            if sha(binary['path'])!=binary['sha256']:raise ValueError('retained executable differs')
            inputs[binary['path']]=binary['sha256']
    results=[]
    for index,(cell,load) in enumerate(keys):
        arms={}
        for config in configs:
            series=sorted([row for row in rows if row['cell']==cell and row['load']==load
                and row['cohort']=='primary' and row['config']==config],key=lambda row:row['rep'])
            if [r['rep'] for r in series]!=[1,2,3,4,5]:raise ValueError('matched primary population differs')
            values=metric_values(series,cell)
            failed=[row['rep'] for row in series if row.get('disposition','executed')!='executed']
            arms[config['name']]=dict(config=config,failed_repetitions=failed,
                metrics={name:median_interval(v,SEED+index) for name,v in values.items()},
                rejections=[row['outcomes']['rejected'] for row in series],
                source_artifacts=[dict(path=row['artifact'],sha256=row['sha256']) for row in series],
                pre_barrier_live_tasks=[row['runtime']['post_close_tasks_before_barrier'] for row in series])
        baseline=arms['none_16_portable'];comparisons=[]
        for config in configs[1:]:
            arm=arms[config['name']];deltas={}
            for name,metric in arm['metrics'].items():
                first=baseline['metrics'][name]['values'];second=metric['values']
                if any(v==0 for v in first):
                    deltas[name]=dict(status='not_computed',reason='observed baseline includes zero; no finite ratio')
                    continue
                paired=[100*(b/f-1) for f,b in zip(first,second)]
                deltas[name]=median_interval(paired,SEED+index) if all(v>=0 for v in paired) else signed_interval(paired,SEED+index)
            comparisons.append(dict(config=config['name'],comparison_eligible=not baseline['failed_repetitions'] and not arm['failed_repetitions'],
                scope='Matched build-profile observations; positive throughput delta is faster, positive cost/RSS/p99 delta is worse. Failed profiles are retained diagnostics and cannot qualify a winning configuration.',
                paired_percent_delta=deltas))
        reproduce=sorted([row for row in rows if row['cell']==cell and row['load']==load
            and row['cohort']=='reproduce' and row['config']==configs[0]],key=lambda row:row['rep'])
        if [row['rep'] for row in reproduce]!=[1,2,3,4,5]:raise ValueError('reproduction population differs')
        metrics=metric_values(reproduce,cell);rerun={}
        for name,values in metrics.items():
            metric=median_interval(values,SEED+index);old=baseline['metrics'][name]['median']
            metric['percent_delta_from_primary_median']=100*(metric['median']/old-1) if old else None
            rerun[name]=metric
        results.append(dict(cell=cell,load_percent=load,arms=arms,comparisons=comparisons,
            reproduction=dict(config='none_16_portable',metrics=rerun,
                rejections=[row['outcomes']['rejected'] for row in reproduce],
                failed_repetitions=[row['rep'] for row in reproduce if row.get('disposition','executed')!='executed'])))
        factors=[]
        for factor,first_name,second_name in (
                ('thin_lto','none_16_portable','thin_16_portable'),
                ('fat_vs_thin_lto','thin_16_portable','fat_16_portable'),
                ('single_codegen_unit','thin_16_portable','thin_1_portable'),
                ('native_cpu','thin_16_portable','thin_16_native'),
                ('pgo','thin_16_native','pgo_use')):
            first_arm=arms[first_name];second_arm=arms[second_name];deltas={}
            for name,metric in second_arm['metrics'].items():
                first=first_arm['metrics'][name]['values'];second=metric['values']
                if any(value==0 for value in first):
                    deltas[name]=dict(status='not_computed',reason='observed baseline includes zero; no finite ratio')
                else:
                    deltas[name]=signed_interval([100*(b/a-1) for a,b in zip(first,second)],SEED+index)
            factors.append(dict(factor=factor,baseline=first_name,candidate=second_name,
                comparison_eligible=not first_arm['failed_repetitions'] and not second_arm['failed_repetitions'],
                paired_percent_delta=deltas))
        results[-1]['factor_comparisons']=factors
    result=dict(scope='local/unsigned',suite_hold='active',inputs_sha256=inputs,analyzer_sha256=sha(Path(__file__).resolve()),
        method='Per-arm medians bootstrap five observed repetitions with replacement. Paired percent deltas bootstrap the five matched B_i/A_i-1 values. 20,000 resamples; percentile indices499/19499; seed962 plus cell index. Reproduction is a fresh cohort, not a paired intervention.',
        primary_repetitions=5,resamples=RESAMPLES,base_seed=SEED,rows=len(rows),
        limitations=['CPU frequency is not controlled on this shared host.',
            'Native whole-workload resources include setup, warmup, diagnostic output and close; null-broker resources cover timed work.',
            'Process-wide allocation instrumentation can affect measured costs; all profiles use these instrumented drivers.',
            'Bulk timing bounds are not individual acknowledgment latency.',
            'Null-broker fixtures have 20,000 records; native bulk has eight million timed records and 10,000 warmup records.',
            'The native target depends on this CPU. PGO is trained on these same workload families.',
            'Intervals describe each exploratory comparison; they are not adjusted for multiple comparisons.',
            'These exploratory cells do not establish production performance, every frozen timing floor, or a ranking.'],cells=results)
    with a.output.open('x') as file:json.dump(result,file,indent=2,allow_nan=False);file.write('\n')
    print(len(results),'cell/load groups;',len(rows),'retained result rows')


def signed_interval(values,seed):
    if len(values)!=5 or any(not math.isfinite(v) for v in values):raise ValueError('five finite signed deltas required')
    rng=random.Random(seed);medians=sorted(statistics.median(rng.choices(values,k=5)) for _ in range(RESAMPLES))
    return dict(values=values,median=statistics.median(values),bootstrap_95_ci=[medians[499],medians[19499]])


if __name__=='__main__':main()

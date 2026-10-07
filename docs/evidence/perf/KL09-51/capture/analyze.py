#!/usr/bin/env python3
"""Replay paired lookup results and bootstrap complete repetition pairs."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import statistics


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('capture','validator','output'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();b=a.capture
    spec=importlib.util.spec_from_file_location('paired_result_validator',a.validator)
    validator=importlib.util.module_from_spec(spec);spec.loader.exec_module(validator)
    completion=json.loads((b/'completion.json').read_text())
    if not completion['source_guards_passed'] or not completion['owned_process_groups_empty']:raise ValueError('incomplete capture')
    rows=json.loads((b/'rows.json').read_text());pairs={}
    for row in rows:
        index=row['repetition'];arm=row['arm'];path=Path(row['artifact'])
        if arm not in ('parent','candidate') or type(index)!=int or index<1:raise ValueError('invalid pair/arm')
        pair=pairs.setdefault(index,{})
        if arm in pair:raise ValueError('duplicate pair arm')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=row['sha256']:raise ValueError('raw result changed')
        measured=validator.validate(path)
        if measured!=row['measurements']:raise ValueError('aggregate row differs from actual result')
        pair[arm]=measured
    if len(pairs)<5 or sorted(pairs)!=list(range(1,len(pairs)+1)) or any(set(pair)!={'parent','candidate'} for pair in pairs.values()):
        raise ValueError('five complete contiguous repetition pairs required')
    def bootstrap(values):
        rng=random.Random(951)
        sampled=sorted(statistics.median(rng.choices(values,k=len(values))) for _ in range(20000))
        return dict(median=statistics.median(values),bootstrap_95_ci=[sampled[499],sampled[19499]],values=values)
    result={arm:bootstrap([pairs[i][arm]['ns_per_record'] for i in sorted(pairs)]) for arm in ('parent','candidate')}
    result['paired_time_ratio_candidate_over_parent']=bootstrap([pairs[i]['candidate']['ns_per_record']/pairs[i]['parent']['ns_per_record'] for i in sorted(pairs)])
    result['bootstrap']=dict(resamples=20000,seed=951,method='Resample complete same-repetition pairs; median of candidate/parent ratios')
    result['median_time_reduction_percent']=100*(1-result['candidate']['median']/result['parent']['median'])
    result['allocations_equal']=all(pair[arm]['lookup_allocation_count']==pair[arm]['lookup_allocated_bytes']==0 for pair in pairs.values() for arm in ('parent','candidate'))
    result['performance_acceptance']=result['paired_time_ratio_candidate_over_parent']['bootstrap_95_ci'][1]<.97 and result['allocations_equal']
    result['limits']=['Local unsigned isolated lookup; no full ShareFetch throughput improvement inferred.',
        'Current parent already used binary search; no historical linear-scan baseline is substituted.',
        'Whole-child CPU/RSS receipts include fixture/oracle/warmup; they are not measured-phase guardrails.',
        'No controlled hardware or independent reproduction; no performance leadership claim.']
    result['rows_sha256']=hashlib.sha256((b/'rows.json').read_bytes()).hexdigest()
    with a.output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps(dict(parent_median=result['parent']['median'],candidate_median=result['candidate']['median'],
        reduction_percent=result['median_time_reduction_percent'],ratio_95_ci=result['paired_time_ratio_candidate_over_parent']['bootstrap_95_ci'],
        passed=result['performance_acceptance'])))


if __name__=='__main__':main()

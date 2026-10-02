import json, pathlib, random, statistics
root=pathlib.Path('/workspace/work/consumer-aborts')
rows=json.loads((root/'revised-paired-summary-raw.json').read_text())
def percentile(xs,p):
    xs=sorted(xs);pos=(len(xs)-1)*p;lo=int(pos)
    return xs[lo]+(xs[min(lo+1,len(xs)-1)]-xs[lo])*(pos-lo)

def stats(rs,key):
    ids=sorted(set(r['pair'] for r in rs))
    aa=[next(r[key] for r in rs if r['pair']==i and r['variant']=='A') for i in ids]
    bb=[next(r[key] for r in rs if r['pair']==i and r['variant']=='B') for i in ids]
    rng=random.Random(20261002);boot_a=[];boot_b=[];deltas=[]
    for _ in range(20000):
        choices=[rng.randrange(len(ids)) for _ in ids]
        ma=statistics.median(aa[i] for i in choices);mb=statistics.median(bb[i] for i in choices)
        boot_a.append(ma);boot_b.append(mb);deltas.append(100*(mb/ma-1))
    ma=statistics.median(aa);mb=statistics.median(bb)
    ci=lambda xs:[percentile(xs,.025),percentile(xs,.975)]
    return {'baseline_median':ma,'candidate_median':mb,'delta_pct':100*(mb/ma-1),'baseline_ci95':ci(boot_a),'candidate_ci95':ci(boot_b),'ci95_pct':ci(deltas),'repetitions':len(ids),'baseline_values':aa,'candidate_values':bb}

keys=['cpu_ns_per_record','allocations_per_record','allocation_count','allocated_bytes_per_record','process_peak_rss_bytes','phase_sampled_peak_rss_bytes','phase_average_rss_bytes','rec_s','p99_us','external_client_vmhwm_bytes','external_sample_peak_rss_bytes','external_wait4_peak_rss_bytes']
summary={'source_sha':rows[0]['source_sha'] if rows[0]['variant']=='B' else next(r['source_sha'] for r in rows if r['variant']=='B'),'baseline_sha':'824662e39980604c799e3398a7e3bc46db8400b3','bootstrap':{'method':'20,000 paired bootstrap resamples of ratio of A/B medians; percentile95% CI','seed':20261002},'cohort':'Predeclared fresh revised candidate cohort:5 committed-aborts,15 bulk,5 sparse1000p pairs. No unchanged-code extensions. Both variants use low-RSS launcher+extra fork, frozen binaries, CPU3runtime/broker and CPU2observer. Other client builds paused for entire cohort.','cells':{},'vmhwm_verification':{}}
for cell in sorted(set(r['cell'] for r in rows)):
    rs=[r for r in rows if r['cell']==cell]
    count=15 if cell=='nb-fetch-bulk' else 5
    assert len(rs)==count*2 and sorted(set(r['pair'] for r in rs))==list(range(count))
    metrics={k:stats(rs,k) for k in keys}
    diffs=[r['vmhwm_vs_self_pct'] for r in rs]
    summary['cells'][cell]={'rows':rs,'metrics':metrics}
    summary['vmhwm_verification'][cell]={'runs':len(diffs),'exact_matches':diffs.count(0),'range_delta_pct':[min(diffs),max(diffs)],'median_delta_pct':statistics.median(diffs)}
(root/'revised-metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
brief=lambda s:{k:{x:v[x] for x in ['baseline_median','candidate_median','delta_pct','ci95_pct']} for k,v in s['metrics'].items() if k in ['cpu_ns_per_record','allocations_per_record','process_peak_rss_bytes','external_client_vmhwm_bytes','rec_s','phase_sampled_peak_rss_bytes']}
print(json.dumps({'cells':{c:brief(s) for c,s in summary['cells'].items()},'vmhwm_verification':summary['vmhwm_verification']},indent=2))

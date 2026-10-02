import json, pathlib, random, statistics
root = pathlib.Path('/workspace/work/consumer-aborts')
rows = json.loads((root/'paired-summary-raw.json').read_text())
extra = root/'bulk-corroboration-summary-raw.json'
additional = json.loads(extra.read_text()) if extra.exists() else []

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
summary={'bootstrap':{'method':'20,000 paired bootstrap resamples of ratio of A/B medians; percentile 95% CI','seed':20261002},'order':'A0 B0 B1 A1 A2 B2 B3 A3 A4 B4; reversed initial variant in odd-numbered pairs','initial5':{},'additional10_bulk':None,'all15_bulk':None}
for cell in sorted(set(r['cell'] for r in rows)):
    rs=[r for r in rows if r['cell']==cell]
    summary['initial5'][cell]={'rows':rs,'metrics':{k:stats(rs,k) for k in keys}}
if additional:
    summary['additional10_bulk']={'rows':additional,'metrics':{k:stats(additional,k) for k in keys}}
    combined=[r for r in rows if r['cell']=='nb-fetch-bulk']+additional
    summary['all15_bulk']={'rows':combined,'metrics':{k:stats(combined,k) for k in keys}}
allrows=rows+additional
verification={}
for cell in sorted(set(r['cell'] for r in allrows)):
    diffs=[r['vmhwm_vs_self_pct'] for r in allrows if r['cell']==cell]
    verification[cell]={'runs':len(diffs),'exact_matches':diffs.count(0),'range_delta_pct':[min(diffs),max(diffs)],'median_delta_pct':statistics.median(diffs)}
summary['vmhwm_verification']=verification
(root/'metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
brief=lambda s:{k:{x:v[x] for x in ['baseline_median','candidate_median','delta_pct','ci95_pct']} for k,v in s['metrics'].items() if k in ['cpu_ns_per_record','allocations_per_record','process_peak_rss_bytes','external_client_vmhwm_bytes','rec_s','phase_sampled_peak_rss_bytes']}
print(json.dumps({'initial5':{c:brief(s) for c,s in summary['initial5'].items()},'additional10_bulk':brief(summary['additional10_bulk']) if additional else None,'all15_bulk':brief(summary['all15_bulk']) if additional else None,'vmhwm_verification':verification},indent=2))

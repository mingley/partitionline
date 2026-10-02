import json,random,statistics
from pathlib import Path
root=Path('/workspace/work/consumer-pending');prior=json.loads((root/'guard-rss-metrics.json').read_text());new=json.loads((root/'guard-hwm-summary-raw.json').read_text());allrows=prior['all15']['rows']+new
for r in new:
 r['external_peak_coverage_self_pct']=100*r['external_rss_peak_bytes']/r['process_peak_rss_bytes']
def perc(xs,p):
 xs=sorted(xs);x=(len(xs)-1)*p;i=int(x);return xs[i]+(xs[min(i+1,len(xs)-1)]-xs[i])*(x-i)
def stats(rs,keys,ids):
 out={}
 for k in keys:
  aa=[next(r[k] for r in rs if r['pair']==i and r['variant']=='A') for i in ids];bb=[next(r[k] for r in rs if r['pair']==i and r['variant']=='B') for i in ids];rng=random.Random(20261002);boot=[]
  for _ in range(20000):
   choices=[rng.randrange(len(ids)) for _ in ids];boot.append(100*(statistics.median(bb[i] for i in choices)/statistics.median(aa[i] for i in choices)-1))
  ma=statistics.median(aa);mb=statistics.median(bb);out[k]={'baseline_median':ma,'candidate_median':mb,'delta_pct':100*(mb/ma-1),'ci95_pct':[perc(boot,.025),perc(boot,.975)],'repetitions':len(ids),'baseline_values':aa,'candidate_values':bb}
 return out
agg=stats(allrows,['cpu_ns_per_round','allocs_per_round','allocated_bytes_per_round','process_peak_rss_bytes','rec_s'],list(range(25)))
cohort=stats(new,['cpu_ns_per_round','rss_peak_bytes','rss_mean_bytes','process_peak_rss_bytes','rec_s','external_rss_peak_bytes','external_client_vmhwm_bytes','external_wait4_peak_rss_bytes'],list(range(15,25)))
diffs=[r['vmhwm_vs_self_pct'] for r in new];assert all(0<=x<.2 for x in diffs)
summary={'all25':{'rows':allrows,'metrics':agg},'additional10_hwm':{'rows':new,'metrics':cohort},'vmhwm_verification':{'runs':20,'exact_matches':diffs.count(0),'within_0_2pct':20,'range_delta_pct':[min(diffs),max(diffs)],'median_delta_pct':statistics.median(diffs),'single_positive_delta_explanation':'A22 lifetime VmHWM63,815,680 versus phase-end RUSAGE_SELF63,696,896 bytes (+0.18648%); lifetime observer includes minor later allocation. Allother19 match exactly.','verified':True}}
(root/'guard-hwm-metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'all25':{k:{x:v[x] for x in ['baseline_median','candidate_median','delta_pct','ci95_pct']} for k,v in agg.items()},'hwm10':{k:{x:v[x] for x in ['baseline_median','candidate_median','delta_pct','ci95_pct']} for k,v in cohort.items()},'verification':summary['vmhwm_verification']},indent=2))

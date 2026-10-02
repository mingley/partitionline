import json,pathlib,random,statistics
root=pathlib.Path('/workspace/work/pending-sparse-memory')
rows=json.loads((root/'paired-summary-raw.json').read_text())
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
keys += ['cpu_ns_per_round','allocs_per_round','allocated_bytes_per_round']
summary={'source_sha':'6f181217ac9555de66012b8609eacb890d7507f6','baseline_sha':'072ef8f90cec41bdf8c60243c47bdfb2c030584c','bootstrap':{'method':'20,000 paired bootstrap resamples of ratio of A/B medians; percentile 95% CI','seed':20261002},'cohort':'Predeclared exactly five interleaved pairs: A0 B0 B1 A1 A2 B2 B3 A3 A4 B4. No extensions. Original frozen binaries/settings, same verified low-RSS fork launcher and actual Rust child PID observer on both variants.','rows':rows,'metrics':{k:stats(rows,k) for k in keys}}
diffs=[r['vmhwm_vs_self_pct'] for r in rows]
summary['lifetime_vmhwm_vs_phase_self']={'runs':len(diffs),'exact_matches':diffs.count(0),'range_delta_pct':[min(diffs),max(diffs)],'median_delta_pct':statistics.median(diffs),'explanation':'Runtime SELF read at phase end, lifetime VmHWM also includes later binary provenance/hash and artifact work; launcher controls prove Python pre-exec floor removed. Distinct scope is intentional and not a claimed corroborating match.'}
coverage=[]
for r in rows:
    o=json.load(open(r['external_rss_result']));s=o['samples'];first=next(i for i,v in enumerate(s) if v['hwm_bytes']==o['client_vmhwm_bytes'])
    coverage.append({'pair':r['pair'],'variant':r['variant'],'actual_runtime_pid':o['observed_runtime_pid'],'peak_plateau_samples':len(s)-first,'peak_plateau_ms':(s[-1]['time_ns']-s[first]['time_ns'])/1e6,'last_sample_to_exit_ms':(o['observer_duration_ns']-s[-1]['time_ns'])/1e6,'observer_interval_ms':o['actual_interval_ms']})
summary['observer_coverage']={'minimum_peak_plateau_samples':min(r['peak_plateau_samples'] for r in coverage),'minimum_peak_plateau_ms':min(r['peak_plateau_ms'] for r in coverage),'maximum_last_sample_to_exit_ms':max(r['last_sample_to_exit_ms'] for r in coverage),'runs':coverage}
summary['memory_guard_qualified']=summary['metrics']['external_client_vmhwm_bytes']['delta_pct']<=2
(root/'metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'memory_guard_qualified':summary['memory_guard_qualified'],'metrics':{k:v for k,v in summary['metrics'].items() if k in ['external_client_vmhwm_bytes','process_peak_rss_bytes','cpu_ns_per_round','allocs_per_round','rec_s','p99_us']},'observer_coverage':{k:v for k,v in summary['observer_coverage'].items() if k!='runs'}},indent=2))

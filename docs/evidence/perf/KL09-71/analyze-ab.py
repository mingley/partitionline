import argparse,json,random,statistics
from pathlib import Path
parser=argparse.ArgumentParser(description='Paired bootstrap medians and percentile confidence intervals for frozen nb-connect artifacts.')
parser.add_argument('--artifacts',type=Path,default=Path(__file__).resolve().parent/'ab')
parser.add_argument('--output',type=Path,default=Path(__file__).resolve().parent/'ab-analysis.json')
parser.add_argument('--pairs',type=int,default=10)
parser.add_argument('--resamples',type=int,default=20000)
parser.add_argument('--seed',type=int,default=71002)
args=parser.parse_args()
if args.pairs < 1 or args.resamples < 1: parser.error('pair and resample counts must be positive')
root=args.artifacts
variants={v:[json.loads((root/f'pair{i}-{v}'/'nb-connect-rep0.result.json').read_text()) for i in range(args.pairs)] for v in ['A','B']}
def percentile(x,p):
 x=sorted(x);i=(len(x)-1)*p;lo=int(i);hi=min(lo+1,len(x)-1);return x[lo]*(1-(i-lo))+x[hi]*(i-lo)
def analyze(extract):
 a=[extract(x) for x in variants['A']];b=[extract(x) for x in variants['B']];rng=random.Random(args.seed);ratio=[];paired=[]
 pd=[(y/x-1)*100 for x,y in zip(a,b)]
 for _ in range(args.resamples):
  ix=[rng.randrange(len(a)) for _ in a];aa=[a[i] for i in ix];bb=[b[i] for i in ix]
  ratio.append((statistics.median(bb)/statistics.median(aa)-1)*100)
  paired.append(statistics.median([pd[i] for i in ix]))
 return {'baseline_samples':a,'candidate_samples':b,'baseline_median':statistics.median(a),'candidate_median':statistics.median(b),'ratio_of_medians_delta_pct':(statistics.median(b)/statistics.median(a)-1)*100,'ratio_of_medians_paired_bootstrap_ci95_pct':[percentile(ratio,.025),percentile(ratio,.975)],'paired_delta_pct_samples':pd,'median_paired_delta_pct':statistics.median(pd),'median_paired_delta_ci95_pct':[percentile(paired,.025),percentile(paired,.975)],'pairs':len(a),'resamples':args.resamples,'seed':args.seed}
results={}
for kind in ['live','refused','stalled_tcp']:
 results[kind]=analyze(lambda x,k=kind:statistics.median(r['first_ack_us']/1000 for r in x['execution']['connect_cases'] if r['bootstrap_kind']==k))
 for partitions in [1,6,64]:
  results[kind+'_'+str(partitions)]=analyze(lambda x,k=kind,p=partitions:next(r['first_ack_us']/1000 for r in x['execution']['connect_cases'] if r['bootstrap_kind']==k and r['partitions']==p))
results['cpu_ns_per_record']=analyze(lambda x:x['measurements']['client_resources']['cpu_ns_per_record'])
results['rss_peak_bytes']=analyze(lambda x:x['measurements']['client_resources']['rss']['peak_rss_bytes'])
results['rss_average_bytes']=analyze(lambda x:x['measurements']['client_resources']['rss']['average_rss_bytes'])
results['allocation_count']=analyze(lambda x:x['measurements']['client_resources']['allocations']['allocation_count'])
results['allocated_bytes']=analyze(lambda x:x['measurements']['client_resources']['allocations']['total_allocated_bytes'])
results['latency_p99_us']=analyze(lambda x:x['measurements']['latency']['p99'])
results['open_sockets']=analyze(lambda x:statistics.median(r['open_sockets'] for r in x['execution']['connect_cases']))
args.output.write_text(json.dumps(results,indent=2)+'\n')
for metric,result in results.items():
 print(metric, 'medians',round(result['baseline_median'],4),round(result['candidate_median'],4),'delta',round(result['ratio_of_medians_delta_pct'],4),'ci',[round(n,4) for n in result['ratio_of_medians_paired_bootstrap_ci95_pct']], 'paired median delta',round(result['median_paired_delta_pct'],4),'ci',[round(n,4) for n in result['median_paired_delta_ci95_pct']])

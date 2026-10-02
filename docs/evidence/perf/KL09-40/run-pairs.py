import pathlib,subprocess,json,time
root=pathlib.Path('/workspace/work/consumer-lookups')
rows=[]
for pair in range(10):
 for variant in ('AB' if pair%2==0 else 'BA'):
  name='baseline' if variant=='A' else 'candidate'
  out=root/'paired'/f'pair-{pair:02}-{variant}'
  cmd=['taskset','-c','3',str(root/f'{name}-target'/'release'/'runtime'),'--cell','nb-fetch-1000p','--out',str(out),'--repetitions','1']
  subprocess.run(cmd,cwd=root/name,check=True,stdout=subprocess.DEVNULL)
  result=next(out.glob('*.result.json'))
  j=json.loads(result.read_text())
  r=j['measurements']['client_resources'];e=j['execution']
  row={'pair':pair,'variant':variant,'cpu_ns_per_round':e['cpu_ns_per_round'],'allocs_per_round':e['allocs_per_round'],'allocated_bytes_per_round':r['allocations']['total_allocated_bytes']/e['fetch_rounds'],'process_peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'rss_sample_peak_bytes':r['rss']['peak_rss_bytes'],'rec_s':j['measurements']['throughput']['records_per_second'],'p99_us':j['measurements']['latency']['p99'],'verified_records':e['records_consumed'],'fetch_rounds':e['fetch_rounds'],'mismatched':e['mismatched'],'validation_failures':e['validation_failures'],'result':str(result)}
  rows.append(row)
  print(json.dumps({k:v for k,v in row.items() if k!='result'}),flush=True)
(root/'paired-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')

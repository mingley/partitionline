import pathlib,subprocess,json
root=pathlib.Path('/workspace/work/consumer-pending')
rows=[]
for pair in range(5):
 for variant in ('AB' if pair%2==0 else 'BA'):
  name='baseline' if variant=='A' else 'candidate'
  out=root/'paired'/f'pair-{pair:02}-{variant}'
  result=out/'nb-fetch-capped-paused-rep0.result.json'
  if not result.exists():
   cmd=['taskset','-c','3',str(root/f'{name}-bin'/'runtime'),'--cell','nb-fetch-capped-paused','--out',str(out),'--repetitions','1']
   with (root/f'pair-{pair:02}-{variant}.log').open('w') as log:
    ret=subprocess.run(cmd,cwd=root/name,stdout=log,stderr=subprocess.STDOUT)
   if ret.returncode!=0:raise RuntimeError(f'{variant}{pair} exit{ret.returncode}; all raw data preserved')
  j=json.loads(result.read_text());e=j['execution'];r=j['measurements']['client_resources']
  row={'pair':pair,'variant':variant,'cpu_ns_per_poll':e['cpu_ns_per_poll'],'allocs_per_poll':e['allocs_per_round'],'allocated_bytes_per_poll':r['allocations']['total_allocated_bytes']/e['fetch_rounds'],'rss_peak_bytes':r['rss']['peak_rss_bytes'],'rss_mean_bytes':r['rss']['average_rss_bytes'],'process_peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'rec_s':j['measurements']['throughput']['records_per_second'],'p99_us':j['measurements']['latency']['p99'],'verified_records':e['records_consumed'],'fetch_rounds':e['fetch_rounds'],'fetched_records':e['fetched_records'],'paused_backlog_records':e['paused_backlog_records'],'prefill_buffered_bytes':e['prefill_buffered_bytes'],'mismatched':e['mismatched'],'validation_failures':e['validation_failures'],'git_commit':j['provenance']['source']['git_commit'],'result':str(result)}
  assert row['paused_backlog_records']==100000 and row['fetched_records']==120000
  assert row['prefill_buffered_bytes']==119999*116
  assert row['verified_records']==2000 and row['fetch_rounds']==2000
  rows.append(row)
  (root/'paired-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')
  print(json.dumps({k:v for k,v in row.items() if k!='result'}),flush=True)

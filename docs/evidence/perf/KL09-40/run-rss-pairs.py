import pathlib,subprocess,json,time,os
os.sched_setaffinity(0, {2})
root=pathlib.Path('/workspace/work/consumer-lookups')
rows=[]
for pair in range(10):
 for variant in ('AB' if pair%2==0 else 'BA'):
  name='baseline' if variant=='A' else 'candidate'
  out=root/'rss-paired'/f'pair-{pair:02}-{variant}'
  cmd=['taskset','-c','3',str(root/f'{name}-target'/'release'/'runtime'),'--cell','nb-fetch-1000p','--out',str(out),'--repetitions','1']
  proc=subprocess.Popen(cmd,cwd=root/name,stdout=subprocess.DEVNULL)
  samples=[]
  while proc.poll() is None:
   try:
    if pathlib.Path(f'/proc/{proc.pid}/exe').resolve()==root/f'{name}-target'/'release'/'runtime':
     statm=pathlib.Path(f'/proc/{proc.pid}/statm').read_text().split()
     samples.append({'elapsed_ns':time.monotonic_ns(),'rss_bytes':int(statm[1])*os.sysconf('SC_PAGE_SIZE')})
   except (FileNotFoundError,ProcessLookupError,PermissionError):pass
   time.sleep(0.001)
  assert proc.returncode==0,proc.returncode
  assert samples
  (out/'external-rss.json').write_text(json.dumps(samples,indent=2)+'\n')
  result=next(out.glob('*.result.json'))
  j=json.loads(result.read_text())
  r=j['measurements']['client_resources'];e=j['execution']
  row={'pair':pair,'variant':variant,'cpu_ns_per_round':e['cpu_ns_per_round'],'allocs_per_round':e['allocs_per_round'],'allocated_bytes_per_round':r['allocations']['total_allocated_bytes']/e['fetch_rounds'],'process_peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'rss_sample_peak_bytes':r['rss']['peak_rss_bytes'],'rec_s':j['measurements']['throughput']['records_per_second'],'p99_us':j['measurements']['latency']['p99'],'verified_records':e['records_consumed'],'fetch_rounds':e['fetch_rounds'],'mismatched':e['mismatched'],'validation_failures':e['validation_failures'],'result':str(result)}
  row['external_process_peak_rss_bytes']=max(x['rss_bytes'] for x in samples)
  row['external_rss_samples']=len(samples)
  rows.append(row)
  print(json.dumps({k:v for k,v in row.items() if k!='result'}),flush=True)
(root/'rss-paired-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')

import pathlib,subprocess,json,os
root=pathlib.Path('/workspace/work/consumer-pending')
rows=[]
env=os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+env['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1')
for cell in ['nb-fetch-bulk','nb-fetch-1000p']:
 for pair in range(5):
  for variant in ('AB' if pair%2==0 else 'BA'):
   name='baseline' if variant=='A' else 'candidate'
   out=root/'guard-paired'/cell/f'pair-{pair:02}-{variant}'
   result=out/f'{cell}-rep0.result.json'
   reuse = result.exists()
   if reuse and not (cell=='nb-fetch-bulk' and pair==0 and variant=='A'): raise RuntimeError(f'Unexpected existing result: {result}; no replacement')
   cmd=['taskset','-c','3',str(root/f'{name}-bin'/'runtime'),'--cell',cell,'--out',str(out),'--repetitions','1']
   if not reuse:
    with (root/f'guard-{cell}-pair-{pair:02}-{variant}.log').open('w') as log:
     ret=subprocess.run(cmd,cwd=root/name,env=env,stdout=log,stderr=subprocess.STDOUT)
    if ret.returncode!=0:raise RuntimeError(f'{cell} {variant}{pair} exit {ret.returncode}; all raw data preserved')
   j=json.loads(result.read_text());e=j['execution'];r=j['measurements']['client_resources']
   row={'cell':cell,'pair':pair,'variant':variant,'cpu_ns_per_poll':e['cpu_ns_per_round'],'allocs_per_poll':e['allocs_per_round'],'allocated_bytes_per_poll':r['allocations']['total_allocated_bytes']/e['fetch_rounds'],'rss_peak_bytes':r['rss']['peak_rss_bytes'],'rss_mean_bytes':r['rss']['average_rss_bytes'],'process_peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'rec_s':j['measurements']['throughput']['records_per_second'],'p99_us':j['measurements']['latency']['p99'],'verified_records':e['records_consumed'],'fetch_rounds':e['fetch_rounds'],'fetched_records':e['fetched_records'],'mismatched':e['mismatched'],'validation_failures':e['validation_failures'],'git_commit':j['provenance']['source']['git_commit'],'result':str(result)}
   assert row['git_commit']==('072ef8f90cec41bdf8c60243c47bdfb2c030584c' if variant=='A' else '6f181217ac9555de66012b8609eacb890d7507f6')
   assert row['mismatched']==0 and row['validation_failures']==0
   rows.append(row)
   (root/'guard-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')
   print(json.dumps({k:v for k,v in row.items() if k!='result'}),flush=True)

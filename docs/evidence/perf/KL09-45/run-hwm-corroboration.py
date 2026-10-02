import pathlib,subprocess,json,os,time,statistics
root=pathlib.Path('/workspace/work/consumer-pending');cell='nb-fetch-bulk';rows=[]
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+env['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1')
def percentile(xs,p):
 if not xs:return None
 xs=sorted(xs);x=(len(xs)-1)*p;i=int(x);return xs[i]+(xs[min(i+1,len(xs)-1)]-xs[i])*(x-i)
for pair in range(15,25):
 for variant in ('AB' if pair%2==0 else 'BA'):
  name='baseline' if variant=='A' else 'candidate';binary=root/f'{name}-bin'/'runtime';out=root/'guard-hwm-corroboration'/f'pair-{pair:02}-{variant}'
  if out.exists():raise RuntimeError(f'Existing output {out}; no replacement')
  out.mkdir(parents=True)
  cmd=['taskset','-c','3',str(binary),'--cell',cell,'--out',str(out),'--repetitions','1']
  exe_confirmed=False;samples=[];start=time.monotonic_ns();confirm_time=None
  with (root/f'guard-hwm-pair-{pair:02}-{variant}.log').open('w') as log:
   p=subprocess.Popen(cmd,cwd=root/name,env=env,stdout=log,stderr=subprocess.STDOUT)
   proc=pathlib.Path('/proc')/str(p.pid)
   while True:
    try:
     if not exe_confirmed and os.readlink(proc/'exe')==str(binary):
      exe_confirmed=True;confirm_time=time.monotonic_ns()-start
     if exe_confirmed:
      txt=(proc/'smaps_rollup').read_text()
      rss=int(next(x.split()[1] for x in txt.splitlines() if x.startswith('Rss:')))*1024
      status_txt=(proc/'status').read_text()
      hwm=int(next(x.split()[1] for x in status_txt.splitlines() if x.startswith('VmHWM:')))*1024
      samples.append({'time_ns':time.monotonic_ns()-start,'rss_bytes':rss,'hwm_bytes':hwm})
    except (FileNotFoundError,ProcessLookupError,StopIteration):pass
    pid,status,usage=os.wait4(p.pid,os.WNOHANG)
    if pid:
     p.returncode=os.waitstatus_to_exitcode(status);break
    time.sleep(.001)
  end=time.monotonic_ns()-start
  observation={'method':'External observer on CPU2; /proc/PID/exe equality with frozen runtime confirmed before first read; /proc/PID/smaps_rollup Rss and /proc/PID/status VmHWM sampled after a1ms sleep, through remaining runtime lifetime. This includes setup, measured phase and teardown; not an exact phase boundary match.','requested_sleep_ms':1,'exe_confirmed':exe_confirmed,'exe_confirmed_at_ns':confirm_time,'observer_duration_ns':end,'sample_count':len(samples),'sample_peak_rss_bytes':max(x['rss_bytes'] for x in samples) if samples else 0,'client_vmhwm_bytes':max(x['hwm_bytes'] for x in samples) if samples else 0,'wait4_peak_rss_bytes':int(usage.ru_maxrss)*1024,'wait4_user_cpu_seconds':usage.ru_utime,'wait4_system_cpu_seconds':usage.ru_stime,'samples':samples,'exit_code':p.returncode}
  intervals=[(b['time_ns']-a['time_ns'])/1000000 for a,b in zip(samples,samples[1:])];observation['actual_interval_ms']={'median':statistics.median(intervals) if intervals else None,'p95':percentile(intervals,.95),'max':max(intervals) if intervals else None}
  observation['peak_coverage_pct']=100*observation['sample_peak_rss_bytes']/observation['wait4_peak_rss_bytes'] if observation['wait4_peak_rss_bytes'] else None
  (out/'external-rss.json').write_text(json.dumps(observation,indent=2)+'\n')
  if p.returncode!=0:raise RuntimeError(f'{variant}{pair} runtime exit{p.returncode}; all data preserved')
  assert exe_confirmed and samples
  result=out/f'{cell}-rep0.result.json';j=json.loads(result.read_text());e=j['execution'];r=j['measurements']['client_resources']
  row={'cell':cell,'pair':pair,'variant':variant,'cohort':'additional10 VmHWM corroboration','cpu_ns_per_round':e['cpu_ns_per_round'],'allocs_per_round':e['allocs_per_round'],'allocated_bytes_per_round':r['allocations']['total_allocated_bytes']/e['fetch_rounds'],'rss_peak_bytes':r['rss']['peak_rss_bytes'],'rss_mean_bytes':r['rss']['average_rss_bytes'],'process_peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'rec_s':j['measurements']['throughput']['records_per_second'],'p99_us':j['measurements']['latency']['p99'],'verified_records':e['records_consumed'],'fetch_rounds':e['fetch_rounds'],'fetched_records':e['fetched_records'],'mismatched':e['mismatched'],'validation_failures':e['validation_failures'],'git_commit':j['provenance']['source']['git_commit'],'result':str(result),'external_client_vmhwm_bytes':observation['client_vmhwm_bytes'],'external_rss_peak_bytes':observation['sample_peak_rss_bytes'],'external_wait4_peak_rss_bytes':observation['wait4_peak_rss_bytes'],'external_peak_coverage_pct':observation['peak_coverage_pct'],'external_sample_count':len(samples),'external_actual_interval_ms':observation['actual_interval_ms'],'external_rss_result':str(out/'external-rss.json')}
  assert e['records_consumed']==20000 and e['fetch_rounds']==1 and e['fetched_records']==135000 and not e['mismatched'] and not e['validation_failures']
  row['vmhwm_vs_self_pct']=100*(row['external_client_vmhwm_bytes']/row['process_peak_rss_bytes']-1)
  assert row['git_commit']==('072ef8f90cec41bdf8c60243c47bdfb2c030584c' if variant=='A' else '6f181217ac9555de66012b8609eacb890d7507f6')
  rows.append(row);(root/'guard-hwm-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')
  print(json.dumps({k:v for k,v in row.items() if k not in ['result','external_rss_result']}),flush=True)

import pathlib,subprocess,json,os
root=pathlib.Path('/workspace/work/consumer-aborts');rows=[]
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+env['PATH'],CARGO_PROFILE_RELEASE_DEBUG='true',CARGO_INCREMENTAL='0')
expected=[[p,g*2500,g*2500+2000] for p,n in [(0,20),(1,20),(2,13)] for g in range(n)]+[[2,32500,33000],[3,0,500],[4,0,500],[5,0,500]]
for i in range(5):
 out=root/'baseline-study'/f'run-{i:02}';cmd=['taskset','-c','3',str(root/'corrected-baseline-bin/runtime'),'--cell','nb-fetch-committed-aborts','--out',str(out),'--repetitions','1']
 with (root/f'baseline-study-{i:02}.log').open('w') as log:ret=subprocess.run(cmd,cwd=root/'harness-baseline',env=env,stdout=log,stderr=subprocess.STDOUT)
 if ret.returncode:raise RuntimeError(f'Run{i} failed{ret.returncode};data preserved')
 path=out/'nb-fetch-committed-aborts-rep0.result.json';j=json.loads(path.read_text());e=j['execution'];r=j['measurements']['client_resources']
 assert j['provenance']['source']['git_commit']=='824662e39980604c799e3398a7e3bc46db8400b3' and not j['provenance']['source']['dirty_tree']
 assert e['target_records']==20000 and e['returned_records']==108000 and e['records_consumed']==108000 and e['fetched_records']==134500
 assert e['committed_history']==expected and e['committed_aborted_deliveries']==0 and e['committed_abort_gap_records']==25500 and e['filtered_records']==26500
 assert e['committed_partition_cursors']==[[0,50000],[1,50000],[2,33000],[3,500],[4,500],[5,500]]
 row={'repetition':i,'cpu_ns_per_record':r['cpu_ns_per_record'],'allocations_per_record':r['allocations']['allocations_per_record'],'allocation_count':r['allocations']['allocation_count'],'peak_rss_bytes':r['rss']['process_peak_rss_bytes'],'verified':e['records_consumed'],'history_ranges':len(expected),'result':str(path)}
 rows.append(row);(root/'baseline-study.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)

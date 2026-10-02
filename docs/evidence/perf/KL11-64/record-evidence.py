import hashlib,json,pathlib,shutil,subprocess,tarfile
root=pathlib.Path('/workspace/work/pending-sparse-memory');frozen=pathlib.Path('/workspace/work/consumer-pending');repo=pathlib.Path('/workspace/partitionline')
out=repo/'docs/evidence/perf/KL11-64';raw=root/'sanitized/raw';out.mkdir(parents=True,exist_ok=True);raw.mkdir(parents=True,exist_ok=True)
A='072ef8f90cec41bdf8c60243c47bdfb2c030584c';B='6f181217ac9555de66012b8609eacb890d7507f6'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
expected_hashes={A:'973f3344c7569fb40b9911276c899de08c569dd360e62982d47d48ff231865f2',B:'9c9e4979ca07b63557bda788b2dbf76ec65218208f805a0eb418b4af14ed400f'}
original_hashes={'docs/plan/evidence/KL09-45.json':'299447b4be5548155037124c692570f9fc721845aa6f6c184fc2c9b022f210ae','docs/evidence/perf/KL09-45/raw-artifacts.tar.gz':'af1d09f6cd89c39be22f8f997359ea338ed00efabbe72d30ca0a8b52c38a4d00','docs/evidence/perf/KL09-45/checksums.sha256':'0404d8b2fe0e2b689dc12cfafadbeaa7822107f61f2de8b858895baa2c72881b'}
for name,h in original_hashes.items():assert sha(repo/name)==h
for name,s in [('baseline',A),('candidate',B)]:
 assert sha(frozen/f'{name}-bin/runtime')==expected_hashes[s]
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=frozen/name,text=True).strip()==s
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=frozen/name,text=True).strip()
checks=[];sidecars=0;observers=0
for phase,path in [('original',frozen/'guard-paired/nb-fetch-1000p'),('fresh',root/'paired/nb-fetch-1000p')]:
 for src in sorted(path.rglob('*.result.json')):
  run=pathlib.Path(phase)/src.parent.name;dest=raw/run;dest.mkdir(parents=True,exist_ok=True)
  j=json.loads(src.read_text());source=j['provenance']['source'];e=j['execution']
  assert source['git_commit'] in expected_hashes and not source['dirty_tree']
  assert j['provenance']['binary']['sha256']==expected_hashes[source['git_commit']]
  assert e['records_consumed']==e['fetched_records']==e['records_offered']==10000 and e['fetch_rounds']==e['fetch_requests']==1
  assert e['per_partition_delivered']==[{'partition':p,'records':10} for p in range(1000)]
  assert e['mismatched']==e['validation_failures']==0 and not e['timed_out']
  assert j['provenance']['config']['sha256']=='098e5aade2526858abc5d9c0295f162e7f93f4d8ef1fd89d20e78770307de9a1'
  j['provenance']['host']['hostname']='redacted';j['provenance']['binary']['path']=('baseline' if source['git_commit']==A else 'candidate')+'/runtime'
  for art in j['provenance']['artifacts']:
   original=pathlib.Path(art['path']);data=original.read_bytes()
   assert sha(original)==art['sha256'] and len(data)==art['size_bytes'];sidecars+=1
   (dest/original.name).write_bytes(data);art['path']=str(pathlib.Path('raw')/run/original.name)
  j['execution']['result_path']=str(pathlib.Path('raw')/run/src.name)
  dst=dest/src.name;dst.write_text(json.dumps(j,separators=(',',':'))+'\n')
  ret=subprocess.run(['python3',str(repo/'scripts/benchmark-report.py'),str(dst),'--quiet','--json'],capture_output=True,text=True,check=True);v=json.loads(ret.stdout)
  assert v['valid'] and v['error_count']==0
  obs=src.parent/'external-rss.json'
  if obs.exists():
   o=json.loads(obs.read_text());assert o['exit_code']==0 and o['exe_confirmed'] and o['sample_count']>0
   assert o['observed_runtime_pid']!=o['launcher_pid']
   (dest/obs.name).write_bytes(obs.read_bytes());observers+=1
  checks.append({'artifact':str(pathlib.Path('raw')/run/src.name),'valid':True,'errors':0,'cohort':phase,'source_sha':source['git_commit'],'original_result_sha256':sha(src),'verified_records':10000,'partition_count':1000,'records_each_partition':10})
assert len(checks)==20 and sidecars==40 and observers==10
validation={'valid':20,'failed':0,'original_results':10,'fresh_results':10,'sidecar_hashes_verified':40,'observer_records':10,'runs':checks}
(out/'artifact-validation.json').write_text(json.dumps(validation,indent=2)+'\n')
with tarfile.open(out/'raw-artifacts.tar.gz','w:gz') as tar:tar.add(raw,arcname='raw')
for p in root.glob('*.log'):shutil.copyfile(p,out/p.name)
for name in ['run-pairs.py','calculate-stats.py','record-evidence.py','verify-launcher.py','low-rss-launcher.c','rss-exec-control.c']:
 shutil.copyfile(root/name,out/name)
for name in ['launcher-build-final.log','launcher-control-final.log','rss-exec-control.log']:
 shutil.copyfile(repo/'docs/evidence/perf/KL09-47'/name,out/('prior-'+name))
metrics=json.loads((root/'metrics.json').read_text())
for row in metrics['rows']:
 for k in ['result','external_rss_result']:
  row[k]='raw/fresh/'+pathlib.Path(row[k]).parent.name+'/'+pathlib.Path(row[k]).name
(out/'measurements.json').write_text(json.dumps(metrics,indent=2)+'\n')
old=json.loads((repo/'docs/plan/evidence/KL09-45.json').read_text());old_sparse=old['results']['guard_cells']['nb-fetch-1000p']
(out/'original-sparse-metrics.json').write_text(json.dumps(old_sparse,indent=2)+'\n')
m=metrics['metrics'];qualified=metrics['memory_guard_qualified']
evidence={
 'id':'KL11-64','kind':'analysis','status':'done','source_sha':B,'baseline_sha':A,'disposition':'accepted' if qualified else 'correction-needed',
 'subject_card':'KL09-45','memory_guard_qualified':qualified,
 'hypothesis':'The original sparse RUSAGE_SELF guard was read at phase end and affected by an inherited Python pre-exec maximum. Requalify actual original Rust lifetime maximum with the same verified low-current-RSS C launcher, an extra fork before exec, and persistent VmHWM of the exact emitted Rust child PID.',
 'cells':['nb-fetch-1000p'],
 'host':'AMD EPYC 9V74 80-Core Processor; x86_64 Linux 6.18.44; affinity exposes CPUs0–4, cgroup cpu.max=400000 100000. Frozen original optimized-release binaries, no rebuild. Runtime/launcher/inherited broker on CPU3, observer CPU2. Five live compiler/cargo processes stopped by coordinator for the full ten-process window and resumed immediately afterward; no new core build launches during measurement. Host frequency unmanaged. rustc1.99.0(b940084d7 2026-09-28), cargo1.99.0; C launcher compiled with cc(Debian14.2.0-19)14.2.0.',
 'frozen_identities':{
  'baseline_runtime_sha256':expected_hashes[A],'candidate_runtime_sha256':expected_hashes[B],
  'baseline_runtime_size_bytes':(frozen/'baseline-bin/runtime').stat().st_size,'candidate_runtime_size_bytes':(frozen/'candidate-bin/runtime').stat().st_size,
  'baseline_broker_sha256':sha(frozen/'baseline-bin/nb-serve'),'candidate_broker_sha256':sha(frozen/'candidate-bin/nb-serve'),
  'launcher_sha256':sha(root/'low-rss-launcher'),'launcher_source_sha256':sha(root/'low-rss-launcher.c'),
  'launcher_identity':'Same source/binary verified in KL09-47: binary e4008078db50a97e6116cf45508779826a06d662624b68deba2069c0224ddaf3, source2d78cca916147f7b29043f03944ba967ea3fb251f94d07ddc57a44fa37b6ded0; copied without rebuilding or changing it.',
  'source_checkouts':'Exact original baseline072ef8f9 and candidate6f181217 are clean detached snapshots. Runtime hashes match prior KL09-45 evidence exactly before and after measurements. Current main changes are excluded.',
  'settings_sha256':'098e5aade2526858abc5d9c0295f162e7f93f4d8ef1fd89d20e78770307de9a1',
  'settings':{'partitions':1000,'records_per_partition':10,'target_records':10000,'payload_bytes':100,'records_per_batch':500,'abort_every':0,'codec':0,'isolation':'read_uncommitted','seed':4269539330},
  'original_evidence_commit':'e255fe8c355f3a4d33ea7a464a4189b268b19ebf','original_files_unchanged_sha256':original_hashes
 },
 'cohort':{'pairs':5,'processes':10,'order':'A0 B0 B1 A1 A2 B2 B3 A3 A4 B4','predeclared':'Exactly five interleaved sparse pairs; no extensions to seek a pass. All original and fresh outcomes retained; no pooling across changed launcher methods.','extensions':0},
 'commands':[
  'sha256sum /workspace/work/consumer-pending/baseline-bin/runtime /workspace/work/consumer-pending/candidate-bin/runtime /workspace/work/pending-sparse-memory/low-rss-launcher /workspace/work/pending-sparse-memory/low-rss-launcher.c (hashes match frozen evidence)',
  'taskset -c 2 python3 /workspace/work/pending-sparse-memory/verify-launcher.py (three fork/exec controls plus exact exit17 propagation pass; exit0)',
  'taskset -c 2 python3 /workspace/work/pending-sparse-memory/run-pairs.py (exactly ten CPU3 frozen original runtime processes; all exit0)',
  'taskset -c 3 /workspace/work/pending-sparse-memory/low-rss-launcher <fresh-out/runtime.pid> /workspace/work/consumer-pending/{baseline,candidate}-bin/runtime --cell nb-fetch-1000p --out <fresh-out> --repetitions 1 (each exact runtime child observed after exe match)',
  'taskset -c 2 python3 /workspace/work/pending-sparse-memory/calculate-stats.py (20000 paired bootstrap resamples, seed20261002; exit0)',
  'python3 scripts/benchmark-report.py <each sanitized old/fresh result.json> --quiet --json (20/20 valid,0errors)',
  'python3 /workspace/work/pending-sparse-memory/record-evidence.py (40 sidecar hashes/sizes, original evidence/archive hashes unchanged, exact source/binary/config/cardinalities verified; exit0)',
  'sha256sum -c checksums.sha256 (all companion artifacts verified)'
 ],
 'results':{
  'primary':dict(m['external_client_vmhwm_bytes'],metric='Actual Rust process lifetime maximum resident bytes; persistent VmHWM'),
  'phase_end_rusage_self':dict(m['process_peak_rss_bytes'],metric='Rust RUSAGE_SELF maximum read at measured phase end, Python floor removed'),
  'all_frozen_guard_metrics':{k:m[k] for k in ['cpu_ns_per_round','allocs_per_round','allocated_bytes_per_round','phase_sampled_peak_rss_bytes','phase_average_rss_bytes','process_peak_rss_bytes','external_client_vmhwm_bytes','rec_s','p99_us']},
  'other_metrics':{k:m[k] for k in ['cpu_ns_per_record','allocations_per_record','allocated_bytes_per_record','external_sample_peak_rss_bytes','external_wait4_peak_rss_bytes']},
  'original_sparse_guard_metrics':old_sparse,
  'threshold_regression_pct':2,'qualification':'Actual lifetime peak median regression is at most2%; fresh pairedCI[-0.5436,+0.2134]% also stays below2%. No correction to original pending-queue production is indicated by this memory requalification.',
  'integrity':{'fresh_verified_records':100000,'fresh_partitions_each_run':1000,'fresh_records_each_partition':10,'fresh_fetch_rounds_each_run':1,'fresh_fetch_requests_each_run':1,'mismatches':0,'validation_failures':0,'all_runtime_results_valid':20,'sidecar_hashes_verified':40,'actual_child_observers_valid':10},
  'net_production_lines':0
 },
 'memory_scope_and_controls':{
  'launcher_control':'Python parent9.175MB→C parent retains9.437MB pre-exec SELF despite~0.659MB currentRSS. Extra fork fresh child has0 reported pre-exec SELF/~0.254MBcurrentRSS; tiny execprobe peak0.393MB, eliminating Python floor. Three controls and exit17 propagation pass on identical launcher bytes.',
  'observed_pid':'Launcher writes the actual fresh Rust child PID. Observer stores observed_runtime_pid and launcher_pid, asserts they differ, then confirms exact /proc/childPID/exe before sampling. All10 exit codes, PID identities and observers are retained in raw archive.',
  'phase_vs_lifetime':metrics['lifetime_vmhwm_vs_phase_self'],
  'scope_explanation':'Actual lifetime VmHWM medians13.562MB/13.521MB include later binary hash/provenance/artifact work, versus truthful phase-end SELF9.638MB/9.482MB. The~40–44% difference is scope, not a claimed match or residual Python floor. Original SELF medians10.486MB/10.486MB had inherited-floor and phase-end limitations; never retroactively treated as lifetime maxima.',
  'observer_coverage':metrics['observer_coverage'],
  'wait4_excluded':'External parent wait4 covers launcher/runtime/compiler descendants; rustc provenance child can reach~85MB. It is retained as diagnostic and excluded from actual client maximum.',
  'large_prior_cells':'Original named/bulk SELF peaks exceed inherited Python floor and separate prior bulk VmHWM corroboration already exists. No repeat solely for this discovery; their original artifacts remain unchanged.'
 },
 'limits':[
  'This is a follow-up measurement analysis, not a new optimization or Kafka/public performance claim; Suite HOLD unchanged.',
  'All frozen original driver record ID/offset/hash/key checks pass and every partition delivers ten records; fetched/offered/verified all equal10000, so target truncation does not leave a returned tail. Original harness does not emit ordered offset lists or explicitly detect duplicate offsets with equal counts; this limitation is disclosed without changing frozen harness.',
  'No statistical superiority claim for actual lifetime memory: pairedCI crosseszero while comfortably inside2% guard. CPU/throughput/p99 timing CIs remain broad despite paused local builds and unmanaged host frequency.',
  'Observer requested1ms sleeps; actual intervals include scheduling/read overhead. Persistent VmHWM survives transient sample gaps; final peak plateaus persist at least22samples/24.54ms and lastsample-to-exit gap is at most2.35ms. This does not supply exact measured-phase boundaries.',
  'Old10 sparse artifacts and all original KL09-45 evidence remain unchanged and preserved; fresh5pair statistics are separate. No unchanged-code extensions, no raised budget, no production/test/harness edits.'
 ]
}
for key in ['host']:
 evidence[key]=evidence[key].replace('CPUs0','CPUs 0').replace('CPU3','CPU 3').replace('CPU2','CPU 2').replace('rustc1.','rustc 1.').replace('cargo1.','cargo 1.').replace('cc(Debian14.2.0-19)14.2.0','cc (Debian 14.2.0-19) 14.2.0')
evidence['artifacts']=[str(p.relative_to(repo)) for p in sorted(out.iterdir()) if p.is_file()]+list(original_hashes)
(repo/'docs/plan/evidence/KL11-64.json').write_text(json.dumps(evidence,indent=2)+'\n')
(out/'checksums.sha256').write_text(''.join(sha(p)+'  '+p.name+'\n' for p in sorted(out.iterdir()) if p.is_file() and p.name!='checksums.sha256'))
print(json.dumps({'disposition':evidence['disposition'],'memory_guard_qualified':qualified,'actual_lifetime_peak':m['external_client_vmhwm_bytes'],'valid_results':20,'verified_sidecars':40,'actual_child_observers':10,'production_edits':0},indent=2))

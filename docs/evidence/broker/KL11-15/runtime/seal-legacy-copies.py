from pathlib import Path
import hashlib,json,shutil,subprocess,stat
repo=Path('/workspace/partitionline');runtime=Path('/workspace/work/retention-final-d147bcf1');source=runtime/'source';out=repo/'docs/evidence/broker/KL11-15/runtime/legacy-replay-d147bcf1-fixed';out.mkdir(exist_ok=False);sha='d147bcf1c0164778bdbad625842363f3721bc10e';script=source/'docs/evidence/broker/KL11-73/runtime/seal-traces.py';commands=[];cells=[]
def files(root):return {str(p.relative_to(root)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mode':stat.S_IMODE(p.stat().st_mode),'bytes':p.stat().st_size} for p in sorted(root.rglob('*')) if p.is_file()}
def preserved(old,new):
 if isinstance(old,dict):
  assert isinstance(new,dict)
  for k,v in old.items():assert k in new;preserved(v,new[k])
 elif isinstance(old,list):
  assert isinstance(new,list) and len(old)==len(new)
  for a,b in zip(old,new):preserved(a,b)
 else:assert old==new
for name in ['stable-default','stable-all-features','1.85.0-default','1.85.0-all-features']:
 original=runtime/name/'replication';before=files(original);destination=out/name;shutil.copytree(original,destination,copy_function=shutil.copy2);assert files(destination)==before
 log=out/(name+'.log');argv=['python3','-B',str(script),str(destination),'--source-root',str(source),'--source-sha',sha]
 with log.open('wb') as f:result=subprocess.run(argv,cwd=source,stdout=f,stderr=subprocess.STDOUT)
 row={'argv':argv,'cwd':str(source),'exit_code':result.returncode,'log':str(log.relative_to(repo)),'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()};commands.append(row);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');assert result.returncode==0,name
 after=files(destination);assert files(original)==before
 for p,r in before.items():
  raw_path=str(Path(p).with_name('trace-raw.json')) if Path(p).name=='trace.json' else p
  assert after[raw_path]==r,(name,p)
  if raw_path!=p:preserved(json.loads((destination/raw_path).read_text()),json.loads((destination/p).read_text()))
 trace_counts=[]
 for voters in [3,5]:
  data=json.loads((destination/f'history-{voters}/trace.json').read_text());assert data['source_sha']==sha;trace_counts.append({'voters':voters,'events':len(data['events']),'journal_pairs':len(data['final_journals'])})
 cells.append({'name':name,'original_capture':str(original),'sealed_copy':str(destination.relative_to(repo)),'original_and_copy_raw_files_unchanged':True,'original_files_before_sha256_mode_bytes':before,'generated_artifacts':{p:r for p,r in after.items() if p not in before},'histories':trace_counts})
receipt={'schema_version':1,'task':'KL11-15','source_sha':sha,'commands':commands,'results':{'seal_cells_passed':4,'legacy_histories':8,'events':sum(h['events'] for c in cells for h in c['histories']),'journal_pairs':sum(h['journal_pairs'] for c in cells for h in c['histories']),'original_runtime_captures_unchanged':True,'copied_raw_trace_and_wal_bytes_and_modes_unchanged':True,'all_original_trace_fields_preserved_in_annotated_trace':True},'sealer_source':{'path':str(script.relative_to(source)),'sha256':hashlib.sha256(script.read_bytes()).hexdigest(),'git_blob':subprocess.check_output(['git','rev-parse',sha+':docs/evidence/broker/KL11-73/runtime/seal-traces.py'],cwd=repo,text=True).strip()},'cells':cells,'prior_wrapper_failure':{'path':'docs/evidence/broker/KL11-15/runtime/legacy-replay-d147bcf1','classification':'Wrapper incorrectly expected annotated trace.json unchanged. Actual exact sealer preserves bytes in trace-raw.json and adds provenance to trace.json. First sealer exited0; originals never modified. Fresh second attempt verifies raw copy and all original JSON fields explicitly.'},'scope':'Inherited exact-source73 hash/state trace sealer on isolated copies; full independent causal checks are separate receipts.'};(out/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt['results']));print(hashlib.sha256((out/'validation.json').read_bytes()).hexdigest())

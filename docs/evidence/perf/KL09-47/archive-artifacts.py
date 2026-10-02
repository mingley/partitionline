import hashlib, json, pathlib, shutil, subprocess, tarfile
root=pathlib.Path('/workspace/work/consumer-aborts')
repo=pathlib.Path('/workspace/partitionline')
out=repo/'docs/evidence/perf/KL09-47'
raw=root/'sanitized/raw'
raw.mkdir(parents=True,exist_ok=True);out.mkdir(parents=True,exist_ok=True)
phases=['initial-baseline','initial-baseline-correct-provenance','harness-first-failure','harness-complete-history-failure','baseline-study','callgrind/run','paired','bulk-corroboration','revised-paired']
checks=[];sidecars=0;observers=0
for phase in phases:
    for src in sorted((root/phase).rglob('*.result.json')):
        run=src.parent.relative_to(root);dest=raw/run;dest.mkdir(parents=True,exist_ok=True)
        j=json.loads(src.read_text());j['provenance']['host']['hostname']='redacted'
        j['provenance']['binary']['path']=('baseline' if j['provenance']['source']['git_commit']=='824662e39980604c799e3398a7e3bc46db8400b3' else 'candidate-or-historical')+'/runtime'
        for art in j['provenance']['artifacts']:
            path=pathlib.Path(art['path'])
            if not path.exists():path=src.parent/path.name
            data=path.read_bytes()
            assert hashlib.sha256(data).hexdigest()==art['sha256'] and len(data)==art['size_bytes']
            (dest/path.name).write_bytes(data);sidecars+=1
            art['path']=str(pathlib.Path('raw')/run/path.name)
        j['execution']['result_path']=str(pathlib.Path('raw')/run/src.name)
        dst=dest/src.name;dst.write_text(json.dumps(j,separators=(',',':'))+'\n')
        ret=subprocess.run(['python3',str(repo/'scripts/benchmark-report.py'),str(dst),'--quiet','--json'],capture_output=True,text=True)
        v=json.loads(ret.stdout)
        checks.append({'artifact':str(pathlib.Path('raw')/run/src.name),'valid':v['valid'],'errors':v['error_count'],'error_details':v.get('errors',[]),'source_sha':j['provenance']['source']['git_commit'],'dirty_tree':j['provenance']['source']['dirty_tree'],'included_in_final_acceptance':phase=='revised-paired','included_in_rejected_candidate':phase in ['paired','bulk-corroboration'],'baseline_attribution':phase=='baseline-study','diagnostic_trace':phase=='callgrind/run','excluded_prefix_or_harness_failure':phase.startswith(('initial-','harness-'))})
        if phase in ['baseline-study','callgrind/run','paired','bulk-corroboration','revised-paired']:
            assert ret.returncode==0 and v['valid'] and v['error_count']==0
        observer=src.parent/'external-rss.json'
        if observer.exists():shutil.copyfile(observer,dest/observer.name);observers+=1
validation={'runtime_artifacts':len(checks),'valid':sum(c['valid'] for c in checks),'failed':sum(not c['valid'] for c in checks),'sidecar_hashes_verified':sidecars,'external_observer_artifacts':observers,'runs':checks}
(out/'artifact-validation.json').write_text(json.dumps(validation,indent=2)+'\n')
with tarfile.open(out/'raw-artifacts.tar.gz','w:gz') as tar:tar.add(raw,arcname='raw')
for p in sorted(root.glob('*.log')):shutil.copyfile(p,out/p.name)
for name in ['run-baseline-study.py','run-pairs.py','run-pairs-initial.py','run-revised-pairs.py','calculate-stats.py','calculate-revised-stats.py','archive-artifacts.py','record-evidence.py','rss-exec-control.c','low-rss-launcher.c','verify-launcher.py','attribution.json','baseline-study.json','metrics.json','revised-metrics.json']:
    p=root/name
    if p.exists():
        if p.suffix=='.json':
            def clean_paths(value):
                if isinstance(value,dict):
                    return {k:('raw/'+str(pathlib.Path(v).relative_to(root)) if k in ['result','external_rss_result'] and isinstance(v,str) and v.startswith(str(root)+'/') else clean_paths(v)) for k,v in value.items()}
                if isinstance(value,list):return [clean_paths(v) for v in value]
                return value
            (out/name).write_text(json.dumps(clean_paths(json.loads(p.read_text())),indent=2)+'\n')
        else:shutil.copyfile(p,out/name)
callgrind=out/'callgrind';callgrind.mkdir(exist_ok=True)
for p in sorted((root/'callgrind').glob('*')):
    if p.is_file():shutil.copyfile(p,callgrind/p.name)
for name,a,b,files in [('harness-complete-history.patch','0c2338e4d6a61d8e4b273a17a55e618583632041','824662e39980604c799e3398a7e3bc46db8400b3',['benchmarks/runtime/src/fdrive.rs','benchmarks/runtime/src/main.rs','benchmarks/runtime/tests/e2e.rs']),('candidate-rejected.patch','824662e39980604c799e3398a7e3bc46db8400b3','c0cc3cde39a156350502dc65890fc66dd5dd8b60',['src/consumer.rs','tests/consumer_fetch_semantics.rs'])]:
    patch=subprocess.run(['git','diff',a,b,'--']+files,cwd=repo,capture_output=True,text=True,check=True).stdout
    (out/name).write_text(patch)
(out/'checksums.sha256').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(out))+'\n' for p in sorted(out.rglob('*')) if p.is_file() and p.name!='checksums.sha256'))
print(json.dumps({k:v for k,v in validation.items() if k!='runs'},indent=2))

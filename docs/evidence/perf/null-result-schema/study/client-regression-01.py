import ctypes,hashlib,importlib.util,json,os,re,shutil,subprocess,signal
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/null-result-source-39bb8f');out=s/'client-regression-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
pins=json.loads((s/'client-source-pins-01.json').read_text())
def guard():
 for n,d in pins.items():
  with (src/n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
def interrupted(signum,frame):raise InterruptedError('client regression owner interrupted')
for signum in [signal.SIGTERM,signal.SIGINT]:signal.signal(signum,interrupted)
env=o.base_env();env.update(CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0');helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())]
results=[]
for profile,flags in [('default',[]),('all-features',['--all-features'])]:
 guard()
 command=['cargo','+stable','test','--offline','--locked','--manifest-path',str(src/'Cargo.toml'),'--lib','--test','full_surface','--test','update_features_conformance']+flags+['update_features','--','--nocapture']
 o.execute(helper+command,env,out,profile,600);guard()
 text=(out/(profile+'.stderr')).read_text();binaries=re.findall(r'Running .*? \((/workspace/work/target-update-features/[^)]+)\)',text);assert len(binaries)==3,text
 kept=[]
 for index,binary in enumerate(binaries):
  path=out/(profile+'-'+str(index)+'.elf');shutil.copy2(binary,path);kept.append(dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
 o.execute(helper+[str(out/(profile+'-2.elf')),'--nocapture'],env,out,profile+'-safety',15);guard()
 assert '2 passed' in (out/(profile+'-safety.stdout')).read_text()
 counts=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',(out/(profile+'.stdout')).read_text())
 results.append(dict(profile=profile,selected_tests_passed=sum(int(r[0]) for r in counts),safety_tests_passed=2,binaries=kept))
(out/'summary.json').write_text(json.dumps(dict(source_commit='39bb8fa3c6531113c109acc682f24b00c83f104c',source_guards_passed=True,results=results),indent=2)+'\n')
print('Existing UpdateFeatures unit/public checks and both safety regressions pass on default/all-feature builds')

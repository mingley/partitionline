import ctypes,hashlib,importlib.util,json,os,signal,subprocess
from pathlib import Path
s=Path(__file__).parent;source=Path('/workspace/work/open-cards-20261006/update-features-source-e6fe76');out=s/'regression-after-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('update_feature_regression_owner',source/'scripts/run-benchmark-matrix.py');owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('subreaper unavailable')
def interrupt(signum,frame):raise InterruptedError('regression owner interrupted')
for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,interrupt)
pins=json.loads((s/'fix-source-pins-01.json').read_text())
def guard():
 for name,digest in pins.items():
  with (source/name).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==digest,name
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()=='e6fe76e13ffb356c4902014001fbc9a51093e159'
guard();env=owner.base_env();env.update(CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
argv=['python3','-B',str(source/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),'cargo','+stable','test','--offline','--locked','--manifest-path',str(source/'Cargo.toml'),'--package','partitionline','--test','update_features_conformance','--','--nocapture']
receipts=[]
for features,label in [( [],'default-regression'),(['--all-features'],'all-feature-regression')]:
 command=argv[:-2]+features+argv[-2:]
 owner.execute(command,env,out,label,600)
 receipts.append(json.loads((out/(label+'.process.json')).read_text()))
 assert '2 passed' in (out/(label+'.stdout')).read_text()
 guard()
with (out/'actual-pass.json').open('x') as f:json.dump(dict(source_commit='e6fe76e13ffb356c4902014001fbc9a51093e159',regression_tests_passed_per_build=2,source_guards_passed=True,process_receipts=receipts),f,indent=2);f.write('\n')
print('Both safety regressions pass on default/all-feature builds; source guards and owned process closure passed')

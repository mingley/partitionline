import ctypes,hashlib,importlib.util,json,os,signal,subprocess
from pathlib import Path
s=Path(__file__).parent;source=Path('/workspace/work/open-cards-20261006/update-features-source-82cf32');out=s/'regression-before-02';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('update_feature_regression_owner',source/'scripts/run-benchmark-matrix.py');owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('subreaper unavailable')
def interrupt(signum,frame):raise InterruptedError('regression owner interrupted')
for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,interrupt)
pins=json.loads((s/'regression-source-pins-02.json').read_text())
def guard():
 for name,digest in pins.items():
  with (source/name).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==digest,name
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()=='82cf32f535f5503e3aee7da7662b7fbcd1e7f35f'
guard();env=owner.base_env();env.update(CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
argv=['python3','-B',str(source/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),'cargo','+stable','test','--offline','--locked','--manifest-path',str(source/'Cargo.toml'),'--package','partitionline','--test','update_features_conformance','--','--nocapture']
try:owner.execute(argv,env,out,'default-regression',600)
except ValueError:pass
guard();receipt=json.loads((out/'default-regression.process.json').read_text());assert receipt['parent_waited'] and receipt['exit_code']==101 and not receipt.get('failure'),receipt
text=(out/'default-regression.stdout').read_text();assert '2 failed' in text and 'v0_public_validation_does_not_dispatch_or_mutate' in text and 'v0_validation_request_rejected_before_body_changes' in text,text
with (out/'actual-failure.json').open('x') as f:json.dump(dict(source_commit='82cf32f535f5503e3aee7da7662b7fbcd1e7f35f',expected_regression_tests_failed=2,source_guards_passed=True,process_receipt=receipt,qualification='Actual before-fix safety regression; not a passing implementation.'),f,indent=2);f.write('\n')
print('Both before-fix safety regressions failed as expected; source guards and owned process closure passed')

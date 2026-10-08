import ctypes,hashlib,importlib.util,json,os,subprocess,signal,shutil,re
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/update-features-retry-source-0cf6c1');out=s/'prove-malformed-before-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
pins=json.loads((s/'source-pins-04.json').read_text())
def guard():
 for n,d in pins.items():
  with (src/n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env();env.update(PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo',CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
binding=json.loads((s/'compile-preparation-05/binary-binding.json').read_text());binary=s/'compile-preparation-05/before-fix.elf';assert hashlib.sha256(binary.read_bytes()).hexdigest()==binding['binary_sha256']
for sdk in json.loads((s/'compile-preparation-06/sdks.json').read_text()):
 tag=sdk['tag'];directory=s/'compile-preparation-06'/(tag+'-malformed');assert len(list(directory.glob('*.bin')))==10
 guard()
 try:o.execute(helper+[str(binary),'actual_sdk_update_features_malformed_bodies','--ignored','--exact','--nocapture'],env|{'UPDATE_FEATURES_MALFORMED':str(directory)},out,tag+'-Rust-malformed',8)
 except ValueError:pass
 guard();receipt=json.loads((out/(tag+'-Rust-malformed.process.json')).read_text());assert receipt['exit_code']==101 and receipt['parent_waited'] and not receipt.get('failure'),receipt
(out/'actual-failure.json').write_text(json.dumps(dict(source_commit=binding['source_commit'],binary_sha256=binding['binary_sha256'],qualification='Three genuine SDKs reject ten nonnullable field mutations each; before-fix Rust accepts the bodies and each test exits 101.'),indent=2)+'\n')
print('Three actual before-fix malformed failures retained')

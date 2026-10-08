import ctypes,hashlib,importlib.util,json,os,signal,subprocess
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/null-result-source-39bb8f');out=s/'clippy-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
pins=json.loads((s/'compiler-source-pins-02.json').read_text())
def guard():
 for n,d in pins.items():
  with (src/n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
guard();env=o.base_env();env.update(CARGO_TARGET_DIR='/workspace/work/target-null-result-schema',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
o.execute(['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),'cargo','+stable','clippy','--offline','--locked','--manifest-path',str(src/'benchmarks/runtime/Cargo.toml'),'--all-targets','--','-D','warnings'],env,out,'clippy',600);guard();print('Strict Clippy passed with unchanged compiler inputs')

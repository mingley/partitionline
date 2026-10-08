import ctypes,hashlib,importlib.util,json,os,subprocess,signal,shutil,re
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/update-features-qualified-9eb1bb');out=s/'compile-final-all-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
pins=json.loads((s/'source-pins-final-01.json').read_text())
def guard():
 for n,d in pins.items():
  with (src/n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env();env.update(PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo',CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
guard();o.execute(helper+['cargo','+stable','test','--offline','--locked','--all-features','--manifest-path',str(src/'Cargo.toml'),'--test','update_features_conformance','--','--nocapture'],env,out,'Rust-all',600);guard()
paths=re.findall(r'Running tests/update_features_conformance.rs \(([^)]+)\)',(out/'Rust-all.stderr').read_text());assert len(paths)==1
binary=out/'all-features.elf';shutil.copy2(paths[0],binary);(out/'binary-binding.json').write_text(json.dumps(dict(source_commit='9eb1bbba82bc997d314deadfd4bd48fa76ea4a09',sources=pins,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()),indent=2)+'\n')
o.execute(helper+['cargo','+stable','test','--offline','--locked','--all-features','--manifest-path',str(src/'Cargo.toml'),'--lib','update_features','--','--nocapture'],env,out,'Rust-all-unit',600);guard()
o.execute(helper+['cargo','+stable','test','--offline','--locked','--all-features','--manifest-path',str(src/'Cargo.toml'),'--test','full_surface','admin_update_features','--','--nocapture'],env,out,'Rust-all-public',600);guard()
o.execute(helper+['cargo','+stable','clippy','--offline','--locked','--manifest-path',str(src/'Cargo.toml'),'--lib','--test','full_surface','--test','update_features_conformance','--','-D','warnings'],env,out,'clippy-default',600);guard()
o.execute(helper+['cargo','+stable','clippy','--offline','--locked','--all-features','--manifest-path',str(src/'Cargo.toml'),'--lib','--test','full_surface','--test','update_features_conformance','--','-D','warnings'],env,out,'clippy-all',600);guard()
print('All-features selected tests and default/all strict Clippy passed; actual executable retained')

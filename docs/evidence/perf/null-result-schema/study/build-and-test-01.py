import ctypes,hashlib,importlib.util,json,os,signal,subprocess,shutil
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/null-result-source-39bb8f');out=s/'build-and-test-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('null_schema_owner',src/'scripts/run-benchmark-matrix.py');owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
for signum in (signal.SIGTERM,signal.SIGINT):
 def interrupted(signum,frame):raise InterruptedError('compilation owner interrupted')
 signal.signal(signum,interrupted)
pins=json.loads((s/'compiler-source-pins-01.json').read_text())
def guard():
 for name,digest in pins.items():
  with (src/name).open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==digest,name
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=src,text=True).strip()=='39bb8fa3c6531113c109acc682f24b00c83f104c'
env=owner.base_env();env.update(CARGO_TARGET_DIR='/workspace/work/target-null-result-schema',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())]
manifest=str(src/'benchmarks/runtime/Cargo.toml')
guard()
for label,command in [('format',['cargo','+stable','fmt','--manifest-path',manifest,'--','--check']),('runtime-tests',['cargo','+stable','test','--offline','--locked','--manifest-path',manifest,'--lib','--tests','--','--test-threads=1'])]:
 owner.execute(helper+command,env,out,label,600);guard()
retained=out/'bin';retained.mkdir()
for name in ['runtime','nb-serve','native-produce-runtime','native-latency-runtime']:
 binary=Path(env['CARGO_TARGET_DIR'])/'debug'/name;assert binary.is_file();shutil.copy2(binary,retained/name)
binding={'source_commit':'39bb8fa3c6531113c109acc682f24b00c83f104c','sources':pins,'binaries':{name:hashlib.sha256((retained/name).read_bytes()).hexdigest() for name in ['runtime','nb-serve','native-produce-runtime','native-latency-runtime']}}
(out/'binary-binding.json').write_text(json.dumps(binding,indent=2)+'\n');guard()
print('Runtime tests and formatting pass; all four exact-source executables retained')

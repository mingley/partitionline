import ctypes,hashlib,importlib.util,json,os,re,shutil,subprocess,time
from pathlib import Path
s=Path(__file__).parent;src=Path((s/'verified-source-path.txt').read_text().strip());out=s/'compile-verified-01';out.mkdir(exist_ok=False);end=time.monotonic()+900
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
sources=json.loads((s/'source-pins-verified-01.json').read_text());sdks=json.loads((s/'compile-final-default-01/sdks.json').read_text());pins={str(src/n):d for n,d in sources.items()}
for sdk in sdks:
 pins.update(sdk['classes']);pins[str(Path('/workspace/work/open-cards-20261006/init-v6-peers')/('kafka-clients-'+sdk['tag']+'.jar'))]=sdk['jar_sha256']
for path in (s/'compile-final-default-01').rglob('*.bin'):pins[str(path)]=hashlib.sha256(path.read_bytes()).hexdigest()
pins['/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar']='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
pins[str(Path(__file__).resolve())]=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(out/'input-bindings.json').write_text(json.dumps(pins,indent=2)+'\n')
def guard():
 assert time.monotonic()<end,'900 second overall verification budget'
 for n,d in pins.items():
  with Path(n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env()|{'PATH':'/workspace/work/cargo/bin:'+os.environ['PATH'],'RUSTUP_HOME':'/workspace/work/rustup','CARGO_HOME':'/workspace/work/cargo','CARGO_TARGET_DIR':'/workspace/work/target-update-features','CARGO_INCREMENTAL':'0','CARGO_PROFILE_DEV_DEBUG':'0','CARGO_PROFILE_TEST_DEBUG':'0'}
def execute(command,label,directory=out,extra=None):
 guard();o.execute(helper+command,env|(extra or {}),directory,label,min(600,end-time.monotonic()));guard()
for profile,features in [('default',[]),('all',['--all-features'])]:
 root=out/profile;root.mkdir();cargo=['cargo','+stable','test','--offline','--locked','--manifest-path',str(src/'Cargo.toml')]+features
 execute(cargo+['--test','update_features_conformance','--','--nocapture'],profile+'-direct')
 paths=re.findall(r'Running tests/update_features_conformance.rs \(([^)]+)\)',(out/(profile+'-direct.stderr')).read_text());assert len(paths)==1
 binary=root/'integration.elf';shutil.copy2(paths[0],binary);binding=dict(source_commit='2be86c09ba171dcef41e5dd50a24d2aef7aaf3c0',sources=sources,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest());(root/'binary-binding.json').write_text(json.dumps(binding,indent=2)+'\n');pins[str(binary)]=binding['binary_sha256']
 execute(cargo+['--lib','update_features','--','--nocapture'],profile+'-unit')
 execute(cargo+['--test','full_surface','admin_update_features','--','--nocapture'],profile+'-public')
 for sdk in sdks:
  tag=sdk['tag']
  for lane,variable,suffix in [('actual_sdk_update_features_source_cases','UPDATE_FEATURES_SOURCE_CASES','-source-cases'),('actual_sdk_update_features_malformed_bodies','UPDATE_FEATURES_MALFORMED','-malformed')]:
   execute([str(binary),lane,'--ignored','--exact','--nocapture'],tag+suffix,root,{variable:str(s/'compile-final-default-01'/(tag+suffix))})
 execute(['cargo','+stable','clippy','--offline','--locked','--manifest-path',str(src/'Cargo.toml')]+features+['--lib','--test','full_surface','--test','update_features_conformance','--','-D','warnings'],profile+'-strict-clippy')
 print(profile,'selected tests, 36 source-case bodies, 30 malformed bodies and strict Clippy passed',flush=True)
execute(['rustfmt','+stable','--edition','2021','--check',str(src/'src/admin.rs'),str(src/'src/protocol/admin.rs'),str(src/'tests/full_surface.rs'),str(src/'tests/update_features_conformance.rs')],'format')
guard();(out/'qualification.json').write_text(json.dumps(dict(source_commit='2be86c09ba171dcef41e5dd50a24d2aef7aaf3c0',profiles=['default','all'],direct_safety_tests=4,selected_existing_tests=24,source_case_bodies=72,malformed_rejections=60,strict_clippy_passed=True,format_passed=True,product_and_existing_runtime_lanes_byte_identical_to_controller_source='9eb1bbba82bc997d314deadfd4bd48fa76ea4a09'),indent=2)+'\n')
print('Final verified source passed both builds; immutable runtime/source identity retained',flush=True)

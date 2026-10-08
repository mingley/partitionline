import ctypes,hashlib,importlib.util,json,os,subprocess,signal,shutil,re
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/update-features-qualified-9eb1bb');out=s/'compile-final-default-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
pins=json.loads((s/'source-pins-final-01.json').read_text())
def guard():
 for n,d in pins.items():
  with (src/n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=src)
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env();env.update(PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo',CARGO_TARGET_DIR='/workspace/work/target-update-features',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
guard();o.execute(helper+['cargo','+stable','test','--offline','--locked','--manifest-path',str(src/'Cargo.toml'),'--test','update_features_conformance','--','--nocapture'],env,out,'Rust-default',600);guard()
paths=re.findall(r'Running tests/update_features_conformance.rs \(([^)]+)\)',(out/'Rust-default.stderr').read_text());assert len(paths)==1
binary=out/'default.elf';shutil.copy2(paths[0],binary);(out/'binary-binding.json').write_text(json.dumps(dict(source_commit='9eb1bbba82bc997d314deadfd4bd48fa76ea4a09',sources=pins,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()),indent=2)+'\n')
o.execute(helper+['cargo','+stable','test','--offline','--locked','--manifest-path',str(src/'Cargo.toml'),'--lib','update_features','--','--nocapture'],env,out,'Rust-default-unit',600);guard()
o.execute(helper+['cargo','+stable','test','--offline','--locked','--manifest-path',str(src/'Cargo.toml'),'--test','full_surface','admin_update_features','--','--nocapture'],env,out,'Rust-default-public',600);guard()
results=[]
for tag,digest in [('4.1.2','afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431'),('4.2.1','6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8'),('4.3.1','52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36')]:
 jar=Path('/workspace/work/open-cards-20261006/init-v6-peers')/('kafka-clients-'+tag+'.jar');assert hashlib.sha256(jar.read_bytes()).hexdigest()==digest
 classes=out/(tag+'-classes');classes.mkdir();cp=str(jar)+':/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar'
 o.execute(helper+['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',str(classes),str(src/'tests/conformance/java/ConformanceUpdateFeaturesPeer.java')],env,out,tag+'-compile',30);guard()
 java=['java','-Xmx128m','-cp',str(classes)+':'+cp,'ConformanceUpdateFeaturesPeer'];o.execute(helper+java+['values'],env,out,tag+'-values',8);guard();o.execute(helper+java+['malformed',str(out/(tag+'-malformed'))],env,out,tag+'-malformed',8);guard();o.execute(helper+java+['source-cases',str(out/(tag+'-source-cases'))],env,out,tag+'-source-cases',8);guard();results.append(dict(tag=tag,java=java,jar_sha256=digest,classes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in classes.rglob('*.class')}))
(out/'sdks.json').write_text(json.dumps(results,indent=2)+'\n');print('Default safety tests and three actual peer compilers passed; before-fix executable retained')

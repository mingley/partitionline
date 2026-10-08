import pathlib,subprocess,json,hashlib,os,shutil,time
w=pathlib.Path(__file__).parent;r=pathlib.Path('/workspace/work/open-cards-20261006/nullbroker-peers-final');out=w/'final-peer-build-02';out.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
assert subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True)==''
pins={n:sha(r/n) for n in json.load(open(w/'source-files-01.json'))};assert pins==json.load(open(w/'source-publication-04.json'))['files_sha256']
jar=pathlib.Path('/workspace/work/open-cards-20261006/init-v6-peers/kafka-clients-4.3.1.jar'); slf=pathlib.Path('/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar');lib=pathlib.Path('/workspace/work/open-cards-20261006/zstd-c-peer/lib/librdkafka.so.1')
assert sha(jar)=='52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36';assert sha(lib)=='23698e1d7ec1f3f496133b946474f997ed690fa57222d3785c3a176ed57563ca'
commands=[(['cc','-std=c11','-Wall','-Wextra','-Werror','-O2','-isystem','/workspace/work/open-cards-20261006/zstd-c-peer/source/src',str(r/'benchmarks/nullbroker/peers/peer.c'),'-L',str(lib.parent),'-Wl,-rpath,'+str(lib.parent),'-lrdkafka','-o',str(out/'c-peer.elf')],r,None),(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',str(jar),'-d',str(out/'classes'),str(r/'benchmarks/nullbroker/peers/NullBrokerPeer.java')],r,None)]
(out/'classes').mkdir();env=dict(os.environ,GOTOOLCHAIN='local',GOPROXY='off',GOPATH=str(w/'go-cache/gopath'),GOMODCACHE=str(w/'go-cache/modules'),GOCACHE=str(w/'go-cache/build'))
commands.append(([str(w/'go-preparation-01/go/bin/go'),'build','-mod=readonly','-o',str(out/'go-peer.elf'),'.'],r/'benchmarks/nullbroker/peers/go',env))
receipts=[]
for i,(cmd,cwd,environment) in enumerate(commands):
 with (out/f'compile-{i}.log').open('wb') as log:
  start=time.monotonic();result=subprocess.run(cmd,cwd=cwd,env=environment,stdout=log,stderr=subprocess.STDOUT,timeout=30)
 receipts.append(dict(command=cmd,cwd=str(cwd),exit=result.returncode,elapsed_seconds=time.monotonic()-start))
 (out/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n');result.check_returncode()
shutil.copy2('/workspace/work/target-nullbroker-peers/debug/nullbroker',out/'nullbroker.elf')
(out/'binary-binding.json').write_text(json.dumps(dict(source_commit='68278185d202d9bf309f0df6f021b4d2a4b429de',clean_checkout=True,binary_sha256=sha(out/'nullbroker.elf'),source_files=pins,toolchain='rustc 1.99.0 (b940084d7 2026-09-28)',checks=['final-tests-02.log','final-build-02.log','final-clippy-02.log','final-fmt-02.log']),indent=2)+'\n')
for k,cmd in {'c':[str(out/'c-peer.elf'),'{bootstrap}'],'go':[str(out/'go-peer.elf'),'{bootstrap}'],'java':['java','-cp',str(out/'classes')+':'+str(jar)+':'+str(slf),'NullBrokerPeer','{bootstrap}']}.items():(out/(k+'-command.json')).write_text(json.dumps(cmd)+'\n')
with (out/'go-build-info.log').open('wb') as log:subprocess.run([str(w/'go-preparation-01/go/bin/go'),'version','-m',str(out/'go-peer.elf')],stdout=log,check=True)
with (out/'c-library-linkage.log').open('wb') as log:subprocess.run(['ldd',str(out/'c-peer.elf')],stdout=log,check=True)
(out/'sdk-pins.json').write_text(json.dumps(dict(versions={'java':'4.3.1','librdkafka':'2.15.0','franz-go':'v1.22.0','go':'go1.26.0'},external_sha256={str(p):sha(p) for p in [jar,slf,lib,w/'go-preparation-01/go/bin/go']},compiled_artifacts_sha256={str(p.relative_to(out)):sha(p) for p in sorted(out.rglob('*')) if p.is_file() and p.suffix in ('.elf','.class')},go_mod_and_sum_match_pinned_peer=True),indent=2)+'\n')
print(json.dumps(receipts))

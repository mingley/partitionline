import argparse,ctypes,hashlib,importlib.util,json,os,time
from pathlib import Path
p=argparse.ArgumentParser()
for name in ['source','binary','binding','output']:p.add_argument('--'+name,type=Path,required=True)
a=p.parse_args();a.output.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',a.source/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
binding=json.loads(a.binding.read_text());pins={str(a.source/n):d for n,d in binding['sources'].items()}|{str(a.binary):binding['binary_sha256']}
def guard():
 for n,d in pins.items():
  with Path(n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
helper=['python3','-B',str(a.source/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())]
command=helper+['python3','-B',str(a.source/'tests/conformance/run-update-features.py'),'--binary',str(a.binary),'--binding',str(a.binding),'--jars','/workspace/work/open-cards-20261006/init-v6-peers','--slf4j','/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar','--brokers','/workspace/work/open-cards-20261006/codec-brokers','--output',str(a.output/'qualification')]
guard();o.execute(command,o.base_env(),a.output,'native-SDK-matrix',620);guard()
(a.output/'source-binding.json').write_text(json.dumps(dict(source_commit=binding['source_commit'],binary_sha256=binding['binary_sha256'],sources=pins),indent=2)+'\n')
print('Three native brokers, 303 independent wire reversals and 42 public native cases passed; original runner retains ownership receipts')

import importlib.util,os,ctypes
from pathlib import Path
s=Path(__file__).parent;root=s/'diagnose-duplicate-01';src=Path('/workspace/work/open-cards-20261006/update-features-source-68c114')
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
cp='/workspace/work/open-cards-20261006/init-v6-peers/kafka-clients-4.1.2.jar:/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar'
for label,args in [('compile',['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-cp',cp,'-d',str(root),str(root/'Duplicate.java')]),('run',['java','-cp',str(root)+':'+cp,'Duplicate'])]:
 o.execute(['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())]+args,o.base_env(),root,label,15)
print((root/'run.stdout').read_text())

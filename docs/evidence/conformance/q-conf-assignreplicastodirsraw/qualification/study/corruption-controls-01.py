import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

s=Path(__file__).parent
repo=Path('/workspace/work/open-cards-20261006/assign-dirs-source-14f90f')
root=s/'sdk-default-02'
pins={'4.1.2':'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
      '4.2.1':'6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
      '4.3.1':'52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
dest=s/'corruption-controls-01';dest.mkdir(exist_ok=False)
results=[]
for release,digest in pins.items():
    jar=Path('/workspace/work/open-cards-20261006/init-v6-peers')/f'kafka-clients-{release}.jar'
    assert hashlib.sha256(jar.read_bytes()).hexdigest()==digest
    cp=str(root/(release+'-classes'))+os.pathsep+str(jar)+os.pathsep+'/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar'
    for control in ['changed_broker_identity','changed_top_error','truncated_nested_body']:
        candidate=dest/(release+'-'+control);candidate.mkdir()
        for path in (root/(release+'-reverse')).glob('*.bin'):shutil.copy2(path,candidate/path.name)
        name={'changed_broker_identity':'case-0-request.bin','changed_top_error':'case-9-response.bin','truncated_nested_body':'case-5-request.bin'}[control]
        path=candidate/name;data=bytearray(path.read_bytes())
        if control=='changed_broker_identity':data[3]^=1
        elif control=='changed_top_error':data[5]^=1
        else:data=data[:-1]
        path.write_bytes(data)
        receipt=dest/(release+'-'+control+'.process.json')
        command=['python3',str(repo/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),
          'python3','/workspace/partitionline/benchmarks/runtime/tools/measure-process.py',str(receipt),
          'java','-Xmx128m','-cp',cp,'ConformanceAssignReplicasToDirsRaw','verify',str(candidate),str(root/(release+'-fixtures-1'))]
        with (dest/(release+'-'+control+'.stdout')).open('x') as out,(dest/(release+'-'+control+'.stderr')).open('x') as err:
            result=subprocess.run(command,stdout=out,stderr=err,timeout=15)
        actual=json.loads(receipt.read_text())
        assert result.returncode==1 and actual['exit_code']==1 and actual['parent_waited']
        results.append(dict(release=release,control=control,changed_file=str(path),changed_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),actual_exit_code=1,parent_waited=True))
with (dest/'summary.json').open('x') as file:json.dump(dict(actual_rejected_mutations=len(results),results=results),file,indent=2)
print(len(results),'actual changed-body SDK subprocess controls rejected')

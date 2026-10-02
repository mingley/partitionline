#!/usr/bin/env python3
"""Run actual official Apache parsers on the retained actual native frame."""
import hashlib
import json
from pathlib import Path
import subprocess
ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
OUT = ROOT / 'native-all-topics'
frames=json.loads((OUT/'capture.json').read_text())['frames']
frame=next(f for f in frames if f['api_key']==3 and f['length_without_prefix']==33)
results=[]
for version in ['4.1.2','4.2.1','4.3.1']:
    jar=f'/workspace/work/broker-wire/jars/kafka-clients-{version}.jar';classes='/workspace/work/broker-metadata/classes'
    compile=['taskset','-c','0-2,4','java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-Xlint:all','-Werror','-cp',jar,'-d',classes,str(ROOT/'CapturedRequestProbe.java')]
    compiled=subprocess.run(compile,cwd=REPO,capture_output=True,text=True)
    assert compiled.returncode==0,compiled.stderr
    cmd=['taskset','-c','0-2,4','java','-cp',f'{classes}:{jar}:/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar','CapturedRequestProbe',str(OUT/frame['file'])]
    result=subprocess.run(cmd,cwd=REPO,capture_output=True,text=True)
    (OUT/f'apache-parser-{version}.txt').write_text('$ '+' '.join(compile)+'\n'+compiled.stdout+compiled.stderr+f'compile_exit={compiled.returncode}\n$ '+' '.join(cmd)+'\n'+result.stdout+result.stderr+f'exit={result.returncode}\n')
    assert result.returncode==0,result.stderr
    parsed=json.loads(result.stdout)
    assert parsed['remaining']==3 and parsed['remaining_hex']=='000000' and parsed['is_all_topics'] and not parsed['allow_auto_topic_creation'] and not parsed['include_topic_authorized_operations']
    results.append({'release':version,'command':cmd,'exit_code':result.returncode,'parsed':parsed})
assert results[0]['parsed']==results[1]['parsed']==results[2]['parsed']
(OUT/'canonical-all.request.bin').write_bytes(bytes.fromhex(results[0]['parsed']['canonical_hex']))
files=[]
for name in ['rdkafka_request.c','rdkafka_buf.h']:
    data=subprocess.check_output(['git','-C','/workspace/work/c-peer/source','show','9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab:src/'+name])
    assert data==Path('/workspace/work/c-peer/source/src',name).read_bytes()
    dest=ROOT/'upstream/librdkafka-2.15.0'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
    files.append({'path':str(dest.relative_to(ROOT)),'source_commit':'9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab','upstream_path':'src/'+name,'sha256':hashlib.sha256(data).hexdigest(),'url':'https://raw.githubusercontent.com/confluentinc/librdkafka/9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab/src/'+name})
(OUT/'apache-parser.json').write_text(json.dumps({'scope':'Actual Apache parser accepts fields and leaves3bytes; canonical serializer emits compactnull with no padding. No Apache broker/controller runtime claim.','native_frame':frame,'probe_sha256':hashlib.sha256((ROOT/'CapturedRequestProbe.java').read_bytes()).hexdigest(),'results':results,'native_source_files':files},indent=2)+'\n')
print(json.dumps(results[0]['parsed']))

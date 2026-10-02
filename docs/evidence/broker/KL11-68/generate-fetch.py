#!/usr/bin/env python3
"""Compile/replay independent pinned Apache Fetch oracles across three releases."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile
ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
PINS = {'4.1.2':'33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed','4.2.1':'9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159','4.3.1':'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'}
SLF4J='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def run(command,log):
    result=subprocess.run(command,cwd=REPO,capture_output=True,text=True,check=False)
    log.write_text('$ '+' '.join(map(str,command))+'\n'+result.stdout+result.stderr+f'\nexit_code={result.returncode}\n')
    if result.returncode:raise RuntimeError(f'failed: {log}')
    return {'command':list(map(str,command)),'exit_code':result.returncode,'log':str(log.relative_to(ROOT))}
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--jars',type=Path,required=True);parser.add_argument('--scratch',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    args.scratch.mkdir(parents=True,exist_ok=True);logs=ROOT/'logs';logs.mkdir(exist_ok=True)
    source=(ROOT/'FetchOracle.java').read_text()
    slf4j=args.jars/'slf4j-api-1.7.36.jar';assert sha(slf4j)==SLF4J
    summary={'source_sha256':sha(ROOT/'FetchOracle.java'),'scope':'Only official Apache clients loaded. Ordinary log policies are declared; parsers/error methods and separately retained storage components execute independently. No full broker runtime.','releases':[]}
    parsed=[]
    for version,expected in PINS.items():
        jar=args.jars/f'kafka-clients-{version}.jar';assert sha(jar)==expected
        with zipfile.ZipFile(jar) as archive:
            package='org.apache.kafka.common.record.internal' if 'org/apache/kafka/common/record/internal/MemoryRecords.class' in archive.namelist() else 'org.apache.kafka.common.record'
        adapted=source.replace('org.apache.kafka.common.record.internal.',package+'.')
        scratch=args.scratch/version;scratch.mkdir(exist_ok=True);src=scratch/'FetchOracle.java';src.write_text(adapted);classes=scratch/'classes';classes.mkdir(exist_ok=True)
        compile=run(['taskset','-c','0-2,4','java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-Xlint:all','-Werror','-cp',str(jar),'-d',str(classes),str(src)],logs/f'fetch-compile-{version}.txt')
        output=args.output/version
        command=['taskset','-c','0-2,4','java','-cp',f'{classes}:{jar}:{slf4j}','FetchOracle',version,str(output)]
        generated=run(command,logs/f'fetch-generate-{version}.txt')
        with tempfile.TemporaryDirectory(dir=scratch,prefix='replay-') as tmp:
            replay=run(command[:-1]+[tmp],logs/f'fetch-replay-{version}.txt')
            original={str(p.relative_to(output)):sha(p) for p in output.rglob('*') if p.is_file()}
            repeated={str(p.relative_to(Path(tmp))):sha(p) for p in Path(tmp).rglob('*') if p.is_file()}
            assert original==repeated
        manifest=json.loads((output/'goldens.json').read_text());cases=manifest['cases'];parsed.append(cases)
        assert {(c['api_key'],c['api_version']) for c in cases}=={(1,v) for v in range(4,7)} | {(2,v) for v in range(1,4)}
        assert len((output/'cases.tsv').read_text().splitlines())==len(cases)
        for line in (output/'cases.tsv').read_text().splitlines():assert len(line.split('\t'))==5
        for case in cases:
            for direction in ['request','response']:
                path=output/f"{case['name']}.{direction}.bin"
                if case[f'{direction}_hex'] is None:assert direction=='response' and not path.exists()
                else:
                    assert path.read_bytes().hex()==case[f'{direction}_hex'] and sha(path)==case[f'{direction}_sha256']
        storage=json.loads((ROOT/f'storage-component-{version}.json').read_text())
        assert (output/'log-batch-0.bin').read_bytes().hex()==storage['first_batch_hex']
        assert (output/'log-batch-3.bin').read_bytes().hex()==storage['second_batch_hex']
        summary['releases'].append({'release':version,'jar_sha256':expected,'record_package':package,'adaptation':'Only imports adapt to the actual upstream package exposed by this pinned jar; no semantic changes.','adapted_source_sha256':sha(src),'class_sha256':sha(classes/'FetchOracle.class'),'compile':compile,'generate':generated,'replay':replay,'cases':len(cases),'outcomes':{outcome:sum(c['expected_outcome']==outcome for c in cases) for outcome in ['response','no_response_keep_open','close','structural_reject']},'hashes':original})
    assert [{(c['name'],c['request_hex'],c['response_hex']) for c in cases} for cases in parsed][0]==[{(c['name'],c['request_hex'],c['response_hex']) for c in cases} for cases in parsed][1]==[{(c['name'],c['request_hex'],c['response_hex']) for c in cases} for cases in parsed][2]
    summary['cross_release_wire_bytes_identical']=True;summary['total_cases']=sum(r['cases'] for r in summary['releases'])
    (ROOT/'fetch-generation.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'total_cases':summary['total_cases'],'wire_bytes_identical':True,'outcomes':[r['outcomes'] for r in summary['releases']]}))
if __name__=='__main__':main()

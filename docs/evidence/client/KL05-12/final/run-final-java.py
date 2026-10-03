#!/usr/bin/env python3
"""Fresh Apache builds and independent parsing of source-pinned Rust bodies."""
import hashlib,json,subprocess,time
from pathlib import Path

BASE=Path('/workspace/work/fetch-v18')
SHA='7bae3e34b5acedb6cac8b8232c9878f83fd7be30'
SOURCE=BASE/('source-'+SHA)
OUT=BASE/'final-java'
OUT.mkdir(exist_ok=False)
JAVA=Path('/usr/lib/jvm/java-21-openjdk-amd64/bin')
SLF=Path('/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar')
VERSIONS=['4.1.0','4.1.2','4.2.1','4.3.1']
report={'source_sha':SHA,'status':'running','commands':[]}
def save(): (OUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
def run(name,command):
    with (OUT/(name+'.log')).open('w') as log:
        start=time.time();r=subprocess.run(['taskset','-c','0-2,4']+command,cwd=SOURCE,stdout=log,stderr=subprocess.STDOUT,timeout=120)
    report['commands'].append({'name':name,'command':['taskset','-c','0-2,4']+command,'exit_code':r.returncode,'seconds':round(time.time()-start,3),'log':name+'.log'})
    save();print(name,r.returncode,flush=True)
    if r.returncode: report['status']='failed';save();raise SystemExit(r.returncode)
save()
for version in VERSIONS:
    jar=BASE/'jars/kafka-clients-4.1.0.jar' if version=='4.1.0' else Path('/workspace/work/broker-wire/jars')/('kafka-clients-'+version+'.jar')
    artifacts=OUT/version
    run(version+'-fresh-compile-and-verify',['python3',str(SOURCE/'tests/conformance/java/generate_fetch_v18.py'),'--version',version,'--kafka-jar',str(jar),'--slf4j-jar',str(SLF),'--java-bin',str(JAVA),'--output',str(SOURCE/'tests/fixtures/protocol_oracles/fetch_v18'),'--artifacts',str(artifacts),'--verify'])
    for toolchain in ['stable','1.85.0']:
        bodies=BASE/'immutable-attempt1'/(toolchain+'-rust-bodies')
        assert len(list(bodies.glob('*.bin')))==8,'Rust emission prerequisite missing'
        run(version+'-'+toolchain+'-decode-rust',[str(JAVA/'java'),'-Xmx64m','-cp',str(artifacts)+':'+str(jar)+':'+str(SLF),'FetchV18Fixtures',str(jar),str(bodies),'--decode-rust'])
    classes=sorted(artifacts.glob('*.class'))
    assert classes
    (artifacts/'compiled-class-hashes.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in classes},indent=2)+'\n')
report.update(status='passed',apache_releases=4,regenerated_bodies_per_release=30,rust_pairs_per_release_and_toolchain=4,rust_body_parse_pairs=32)
save()

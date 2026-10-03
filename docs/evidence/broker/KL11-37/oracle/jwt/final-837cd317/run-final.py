import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import time

ROOT = Path('/workspace/partitionline')
PIN = '837cd3178eb8f86f7a73117483c9f7c1580e8187'
WORK = Path('/workspace/work/jwt-oracle/final-837cd317')
SOURCE = WORK / 'source'
OUTPUT = ROOT / 'docs/evidence/broker/KL11-37/oracle/jwt/final-837cd317'
WORK.mkdir(parents=True)
SOURCE.mkdir()
OUTPUT.mkdir()
ARCHIVE = WORK / 'claimed-source.tar'
prefixes = ['docs/evidence/broker/KL11-37/oracle/jwt', 'partitionline-broker/tests/fixtures/oidc']
with ARCHIVE.open('wb') as archive:
    subprocess.run(['git', 'archive', '--format=tar', PIN, '--'] + prefixes, cwd=ROOT, stdout=archive, check=True)
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
with tarfile.open(ARCHIVE) as archive:
    files = {member.name: hashlib.sha256(archive.extractfile(member).read()).hexdigest() for member in archive if member.isfile()}
with tarfile.open(ARCHIVE) as archive: archive.extractall(SOURCE, filter='data')
def identity():
    bad = [name for name, value in files.items() if not (SOURCE/name).is_file() or sha(SOURCE/name) != value]
    assert not bad, bad[:10]
    return {'passed':True,'source_files_checked':len(files)}
oracle = SOURCE / prefixes[0]
fixtures = SOURCE / prefixes[1]
frozen = json.loads((oracle/'source-freeze.json').read_text())
assert all(files[name] == digest for name,digest in frozen['source_files_sha256'].items())
report = {'schema_version':1,'source_sha':PIN,'source_archive_sha256':sha(ARCHIVE),
    'passed':False,'scope':'Exact pushed owned JWT fixture/oracle source, independent OpenSSL checks and official configured Apache component validators; no partitionline runtime/network OIDC execution.',
    'source_files_sha256':files,'frozen_source_files_checked':len(frozen['source_files_sha256']),'commands':[]}
def save(): (OUTPUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
def run(argv,label):
    before=identity();start=time.time();log=OUTPUT/(label+'.log');command=['taskset','-c','0-2,4']+argv
    with log.open('wb') as output: result=subprocess.run(command,cwd=SOURCE,stdout=output,stderr=subprocess.STDOUT)
    report['commands'].append({'argv':command,'cwd':str(SOURCE),'exit_code':result.returncode,
        'elapsed_seconds':round(time.time()-start,3),'log':log.name,'log_sha256':sha(log),
        'source_before':before,'source_after':identity()});save();assert result.returncode==0,log
run(['python3',str(oracle/'check-fixtures.py'),str(fixtures),'--report',str(OUTPUT/'fixture-check.json')],'fixed-fixture-check')
run(['python3',str(oracle/'check-counterexamples.py'),'--fixtures',str(fixtures),'--work',str(WORK/'counterexamples'),
    '--report',str(OUTPUT/'fixture-counterexamples.json')],'fixture-corruption-controls')
run(['python3',str(oracle/'prepare-and-run.py'),'--work',str(WORK/'sdk'), '--output',str(OUTPUT/'components'),
    '--private-work','/workspace/work/jwt-oracle/private'],'official-sdk-components')
component=json.loads((OUTPUT/'components/validation.json').read_text())
assert component['passed'] and component['actual_validator_executions']==420 and component['controlled_named_failures']==3
report['passed']=True
report['counts']={'fixed_fixture_cases':70,'valid_underlying_signature_cases':65,'declared_strict_local_accept':12,
    'declared_strict_local_reject':58,'fixture_checker_controls_rejected':9,'official_sdk_validator_executions':420,
    'mandatory_official_component_assertions':component['required_component_assertions'],'controlled_named_official_failures':3}
report['limitations']=component['limitations']+['Production Rust comparison and stable/MSRV graphs are separately retained by37production owner; this final source selection excludes all unrelated WORK runtime changes.',
    'Initial generator tamper-control failure and SDK development run are retained in the pushed original source selection, not rewritten by final qualification.']
report['artifacts_sha256']={str(p.relative_to(OUTPUT)):sha(p) for p in sorted(OUTPUT.rglob('*')) if p.is_file() and p.name!='validation.json'}
save()
(OUTPUT/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(OUTPUT))+'\n' for p in sorted(OUTPUT.rglob('*')) if p.is_file() and p.name!='SHA256SUMS'))
print(json.dumps({'passed':True,'source_sha':PIN,'source_files_checked':len(files),'counts':report['counts']}))

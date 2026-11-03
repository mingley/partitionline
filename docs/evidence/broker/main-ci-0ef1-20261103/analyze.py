#!/usr/bin/env python3
"""Derive directly reported counts from bounded CI log sections."""
import hashlib,json,re
from pathlib import Path
w=Path('/workspace/work/main-broker-ci-41603566088-evidence')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
meta=json.loads((w/'connector-metadata.json').read_text());source=json.loads((w/'source-pins.json').read_text())
assert meta['run_id']==41603566088 and meta['head_sha']==source['commit']=='0ef1cf8581605cedff1117c99bcbbccc6a9cb68e'
assert meta['run_conclusion']=='success' and len(meta['jobs'])==3 and all(j['conclusion']=='success' for j in meta['jobs'])
log=(w/'broker-job.log').read_text()
start='##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --all-targets\n'
allfeatures='##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --all-targets --all-features\n'
clippy='##[group]Run cargo clippy --locked --manifest-path partitionline-broker/Cargo.toml --all-targets --all-features -- -D warnings\n'
assert log.count(start)==log.count(allfeatures)==log.count(clippy)==1
pattern=r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;'
def parse(t):
 rows=re.findall(pattern,t)
 return [{'status':x[0],'passed':int(x[1]),'failed':int(x[2]),'ignored':int(x[3]),'measured':int(x[4]),'filtered':int(x[5])} for x in rows]
def profile(a,b,expected,lib):
 rows=parse(log.split(a,1)[1].split(b,1)[0])
 assert len(rows)==36 and all(x['status']=='ok' and x['failed']==x['ignored']==x['measured']==x['filtered']==0 for x in rows)
 assert sum(x['passed'] for x in rows)==expected and rows[0]['passed']==lib
 return {'result_sections':len(rows),'passed':expected,'library_passed':lib,'failed':0,'ignored':0,'measured':0,'filtered':0,'per_section_passed':[x['passed'] for x in rows]}
def doc_section(flag,next_marker):
 marker='##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --doc'+flag+'\n'
 t=log.split(marker,1)[1].split(next_marker,1)[0]
 rows=parse(t)
 assert len(rows)==1 and rows[0]=={'status':'ok','passed':0,'failed':0,'ignored':0,'measured':0,'filtered':0}
 return rows[0]
doc_all='##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --doc --all-features\n'
post='Post job cleanup.'
default=profile(start,allfeatures,734,447)
allfeat=profile(allfeatures,clippy,882,491)
doc1=doc_section('',doc_all);doc2=doc_section(' --all-features',post)
assert 'test stale_genesis_leader_observation_is_fenced_before_proposal_append ... ok' in log
fuzz=(w/'fuzz-job.log').read_text();inventory=(w/'inventory-job.log').read_text()
assert re.search(r'Ran 12 tests in [0-9.]+s',fuzz) and re.search(r'Ran 55 tests in [0-9.]+s',inventory)
assert '"verdict": "passed"' in inventory and 'test_real_short_run_has_coverage_and_never_qualifies_a_campaign' in fuzz
result={'run_id':41603566088,'head_sha':source['commit'],'conclusion':'success',
 'broker_default_all_targets':default,'broker_all_features_all_targets':allfeat,
 'separate_stale_leader_exact':'1 passed, 0 failed, 0 ignored; 32 filtered out',
 'strict_doctests_default':doc1,'strict_doctests_all_features':doc2,
 'fuzz_supervision_python_tests':12,'inventory_negative_controls':55,
 'log_sha256':{name:sha(w/name) for name in ('broker-job.log','fuzz-job.log','inventory-job.log')},
 'source_pins_sha256':sha(w/'source-pins.json'),'connector_metadata_sha256':sha(w/'connector-metadata.json'),
 'limits':['Per-profile totals sum the 36 reported target result sections; profiles are separate runs, not unique combined test counts.',
 'Doctest commands succeeded but each executed zero doctests.','The instrumented fuzz job ran short supervision checks and does not qualify a long fuzz campaign.',
 'Inventory is a static upstream/source/fixture check, not a complete broker behavior or compatibility qualification.',
 'Decoded connector text was re-encoded as UTF-8; it is not the original compressed GitHub log. Listed artifact bytes were not downloaded or independently hashed.',
 'This CI run alone does not establish full production readiness, complete Kafka compatibility or a global performance ranking.']}
(w/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
print('default',default['passed'],'all-features',allfeat['passed'],'doctests',doc1['passed'],doc2['passed'])

#!/usr/bin/env python3
import hashlib,json,shutil,subprocess
from pathlib import Path
REPO=Path('/workspace/partitionline');BASE=Path('/workspace/work/fetch-v18')
SHA='7bae3e34b5acedb6cac8b8232c9878f83fd7be30'
DEST=REPO/'docs/evidence/client/KL05-12/final'
DEST.mkdir(exist_ok=False)
git_entries=subprocess.check_output(['git','ls-tree','-r','-z',SHA],cwd=REPO).split(b'\0')[:-1]
expected={entry.split(b'\t',1)[1].decode():entry.split(b'\t',1)[0].decode().split()[2] for entry in git_entries}
integrity=[]
for folder in [BASE/('source-'+SHA),BASE/'compiled-mutant-source']:
    files={str(p.relative_to(folder)) for p in folder.rglob('*') if p.is_file()}
    assert files==expected.keys()
    for name,oid in expected.items():
        b=(folder/name).read_bytes();assert hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==oid,(str(folder),name)
    integrity.append({'directory':str(folder),'git_blob_files':len(expected),'all_files_unchanged':True})
for source,name in [(BASE/'immutable-attempt1','rust-matrix'),(BASE/'final-java','apache-java'),(BASE/'compiled-mutants','compiled-mutants')]:shutil.copytree(source,DEST/name)
for name in ['run-immutable.py','run-final-java.py','run-mutants.py']:shutil.copy2(BASE/name,DEST/name)
matrix=json.loads((DEST/'rust-matrix/commands.json').read_text());java=json.loads((DEST/'apache-java/validation.json').read_text());mutants=json.loads((DEST/'compiled-mutants/validation.json').read_text())
assert matrix['status']==java['status']==mutants['status']=='passed'
assert all(r['exit_code']==0 and r['source_unchanged'] for r in matrix['commands'])
assert all(r['exit_code']==0 for r in java['commands'])
assert all(r['intended_behavior_failure'] and r['source_restored'] for r in mutants['controls'])
summary={'schema_version':1,'source_sha':SHA,'status':'passed','immutable_source_integrity':integrity,'full_rust_commands':len(matrix['commands']),'fresh_apache_commands':len(java['commands']),'compiled_negative_controls':len(mutants['controls']),'full_test_counts':{c['name']:{'passed':sum(t['passed'] for t in c['test_counts']),'ignored':sum(t['ignored'] for t in c['test_counts'])} for c in matrix['commands'] if c['name'].endswith('-tests')},'apache_releases':['4.1.0','4.1.2','4.2.1','4.3.1'],'fresh_fixture_bodies':120,'independently_parsed_rust_pairs':32,'ignored_tests':'Four existing live/environment tests per full lane; no removed or newly ignored tests.','limits':['Request/response body oracle and local simulated-peer negotiation proof; no new real Apache cluster production or distributed fault qualification.','Ordinary consumer default omission and explicit replica sidecars only; no follower acknowledgement/quorum-commit runtime.','No performance comparison or global fastest claim.']}
(DEST/'validation.json').write_text(json.dumps(summary,indent=2)+'\n')
cases=REPO/'tests/conformance/cases.json';d=json.loads(cases.read_text());changed=[]
for row in d['cases']:
    if row['id'].startswith('proto-fetch-v18-independent-apache-'):
        assert row['disposition']=='not_run';row['disposition']='independent_pass';row['tested_source_sha']=SHA
        row['reason']='KL05-12: exact-source stable/MSRV default/all-feature and strict gates; fresh checksum-pinned Apache fixtures and independent parsing of Rust-emitted bodies; lower-version and actual simulated-peer Fetch18 regression coverage retained.'
        row['artifacts'].append('docs/evidence/client/KL05-12/final/validation.json');changed.append(row['id'])
assert len(changed)==3
cases.write_text(json.dumps(d,indent=2)+'\n')
proof={'schema_version':1,'task':'KL05-12','status':'done','acceptance_met':True,'source_sha':SHA,'source_freeze':'docs/evidence/client/KL05-12/source-freeze.json','commands':matrix['commands']+java['commands']+mutants['controls'],'results':summary,'maintained_regressions':'A01-A04 capped delivery/order/seek/pause run at12 and18; A05 delivered-only commit runs at17 and18 with actual observed request version. Full suites also cover UUID/name boundaries, sessions, both mixed bootstrap/leader orders, restart/recreation, cancellation/wakeup/quota/terminal close.','independent_review':'docs/evidence/client/KL05-12/independent-review.json','artifacts':['docs/evidence/client/KL05-12/final/validation.json','docs/evidence/client/KL05-12/final/rust-matrix/commands.json','docs/evidence/client/KL05-12/final/apache-java/validation.json','docs/evidence/client/KL05-12/final/compiled-mutants/validation.json','tests/conformance/fetch-v18-delta.json','tests/fixtures/protocol_oracles/fetch_v18/manifest.json'],'limits':summary['limits']}
(REPO/'docs/plan/evidence/KL05-12.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps({'source_files':len(expected),'promoted_independent_cells':changed,'retained_command_rows':len(proof['commands'])}))

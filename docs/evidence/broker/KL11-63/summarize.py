#!/usr/bin/env python3
"""Verify retained histories/source identities and produce final qualification summary."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
SOURCE = '4e70bbd1cfab39c59e975989b0d480b032dbe0de'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
generation=json.loads((ROOT/'oracle-generation.json').read_text())
assert generation['total_cases']==555 and generation['all_release_cases_byte_identical']
assert sum(r['response_golden_count'] for r in generation['releases'])==522
assert sum(r['deliberate_rejection_count'] for r in generation['releases'])==33
source_files=['MetadataOracle.java','generate.py','MetadataPeer.java','metadata-peer.c','run-live.py','CapturedRequestProbe.java','capture-native-all.py','analyze-native-all.py','TopicPolicyProbe.java']
source_hashes={}
for name in source_files:
    path=ROOT/name
    blob=subprocess.check_output(['git','show',SOURCE+':'+str(path.relative_to(REPO))],cwd=REPO)
    assert hashlib.sha256(blob).hexdigest()==sha(path),(name,'differs from exact tested source')
    source_hashes[name]=sha(path)
required={(3,v) for v in range(14)}|{(18,v) for v in range(5)}|{(19,v) for v in range(2,5)}|{(20,v) for v in range(1,7)}
cells=[]
for toolchain,attempt in [('stable',2),('1-85-0',1)]:
    base=ROOT/'live'/toolchain/f'attempt-{attempt}'
    report=json.loads((base/'validation.json').read_text())
    assert report['status']=='pass' and report['source_sha']==SOURCE and report['snapshot_unchanged']
    assert len(report['server_runs'])==2 and all(r['exit_code']==0 for r in report['server_runs'])
    for peer,expected in [('java_peer_sha256',source_hashes['MetadataPeer.java']),('c_peer_sha256',source_hashes['metadata-peer.c'])]:assert report['peers'][peer]==expected
    java=[]
    for release in ['4.1.2','4.2.1','4.3.1']:
        before=json.loads((base/'histories'/f'{release}-create-history.json').read_text())
        after=json.loads((base/'histories'/f'{release}-restart-history.json').read_text())
        pairs={(e['api_key'],e['version']) for e in before['history'] if 'response_hex' in e}
        assert pairs==required
        rejected=[e for e in before['history'] if e.get('outcome')=='clean EOF before response']
        assert len(rejected)==14
        old=[e for e in before['history'] if e['label']=='actual-AdminClient-describe'];new=after['history']
        assert len(old)==1 and len(new)==2
        saved=(base/'histories'/f'{release}.uuid').read_text()
        assert old[0]['uuid']==saved==new[0]['uuid'] and new[1]['uuid']!=saved
        assert before['assertions']==522 and after['assertions']==5
        java.append({'release':release,'create_assertions':before['assertions'],'restart_assertions':after['assertions'],'advertised_pairs_forced':len(pairs),'rejected_eof_count':len(rejected),'before_restart_uuid':saved,'after_restart_uuid':new[0]['uuid'],'recreated_uuid':new[1]['uuid'],'uuid_retained':True,'recreated_uuid_changed':True,'name':old[0]['name'],'history_files':[str(p.relative_to(ROOT)) for p in [base/'histories'/f'{release}-create-history.json',base/'histories'/f'{release}-restart-history.json']]})
    native=[]
    for phase,checks in [('create',42),('restart',29)]:
        text=(base/f'c-{phase}.txt').read_text()
        outcomes=[json.loads(line) for line in text.splitlines() if line.startswith('{')]
        assert outcomes[-1]['status']=='pass' and outcomes[-1]['assertions']==checks
        versions=sorted(set(re.findall(r'Sent (\w+)Request \(v(\d+)',text)))
        assert ('Metadata','13') in versions and '37 bytes' in text
        native.append({'phase':phase,'assertions':checks,'outcomes':outcomes,'wire_versions':versions,'actual_all_topics_api':True,'log':str((base/f'c-{phase}.txt').relative_to(ROOT))})
    cells.append({'toolchain':report['toolchain'],'attempt':attempt,'status':'pass','source_sha':SOURCE,'snapshot_unchanged':True,'snapshot_file_count':report['snapshot_file_count'],'rust_test_binary_sha256':report['rust_test_binary_sha256'],'server_processes':report['server_runs'],'java':java,'c':native,'validation':str((base/'validation.json').relative_to(ROOT))})
failed=json.loads((ROOT/'live/stable/attempt-1/validation.json').read_text())
assert failed['status']=='failed' and failed['source_sha']=='6b4d3306fd517decbc01839ae25835c695de7572'
captured=json.loads((ROOT/'native-all-topics/apache-parser.json').read_text())
assert all(r['parsed']['remaining_hex']=='000000' and r['parsed']['consumed']==30 for r in captured['results'])
summary={'id':'KL11-63','status':'accepted','source_sha':SOURCE,'initial_remote_fixture_source_sha':'9c6a9b3539bd0bf3eae8dc851d18186029a656b8','initial_local_fixture_context_sha':subprocess.check_output(['git','rev-parse','b2937766'],cwd=REPO,text=True).strip(),'failed_pre_compat_handler_sha':failed['source_sha'],'source_hashes':source_hashes,'oracle':{'releases':['4.1.2','4.2.1','4.3.1'],'declared_cases':555,'response_goldens':522,'deliberate_rejections':33,'every_advertised_pair_per_release':28,'repeated_generator_bytes_identical':True,'cross_release_cases_identical':True,'generation_report':'oracle-generation.json','upstream_pins':'upstream-pins.json'},'live':cells,'java_assertions_total':sum(j['create_assertions']+j['restart_assertions'] for c in cells for j in c['java']),'c_assertions_total':sum(n['assertions'] for c in cells for n in c['c']),'rejected_eof_requests_total':sum(j['rejected_eof_count'] for c in cells for j in c['java']),'retained_failure':{'validation':'live/stable/attempt-1/validation.json','reason':'Actual pinned librdkafka flexible all-topics Metadata request retains three zero bytes after canonical fields; original strict handler rejected it. Three official Apache parsers accept fields and leave exactly those bytes. Scoped compatibility correction is tested; original failed attempt is retained.','catalog':'live/stable/attempt-1/catalog-failed-attempt.journal'},'native_compatibility':{'exact_frame_sha256':captured['native_frame']['sha256'],'wire_length':37,'without_transport_prefix':33,'apache_consumed':30,'apache_remaining_hex':'000000','canonical_length':30,'scope':'Exactly three zero body-tail bytes for flexible Metadata all-topics/null selectors only; no general suffix/Create/Delete relaxation.','capture':'native-all-topics/capture.json','actual_apache_parser_results':'native-all-topics/apache-parser.json'},'limits':['Golden responses express the declared single-node policy and execute official serialization/parsing; no full Apache broker/controller runtime equivalence claim.','Live peers cover the advertised local metadata/admin subset, not Produce, replication, ACLs or arbitrary topic configurations.','Restart is an orderly process shutdown/reopen with the same durable catalog, not an injected machine/power loss.','Live Rust builds use default broker features on stable and MSRV; the metadata worker separately owns full default/all-feature unit and strict Rust lint/doc qualification.','Native compatibility allowance is the exact observed flexible null-selector padding; other suffixes/flags remain strict.']}
(ROOT/'validation.json').write_text(json.dumps(summary,indent=2)+'\n')
artifacts={str(p.relative_to(ROOT)):sha(p) for p in ROOT.rglob('*') if p.is_file() and p.name not in {'artifact-hashes.json','SHA256SUMS'} and '__pycache__' not in p.parts}
(ROOT/'artifact-hashes.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(ROOT/'SHA256SUMS').write_text(''.join(f'{digest}  {path}\n' for path,digest in sorted(artifacts.items())))
print(json.dumps({'status':'accepted','source_sha':SOURCE,'java_assertions':summary['java_assertions_total'],'c_assertions':summary['c_assertions_total'],'rejected_eof':summary['rejected_eof_requests_total'],'artifacts':len(artifacts)}))

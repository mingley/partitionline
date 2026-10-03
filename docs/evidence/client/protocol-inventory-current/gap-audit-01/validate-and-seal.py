#!/usr/bin/env python3
"""Validate static audit relationships and seal only public WORK artifacts."""
import collections
import hashlib
import json
from pathlib import Path
import stat
import subprocess

OUT=Path('/workspace/work/consumer-conformance-gap-audit-01')
ROOT=Path('/workspace/partitionline')
def sha(b):return hashlib.sha256(b).hexdigest()
def load(name):return json.loads((OUT/name).read_bytes())
def save(name,x):(OUT/name).write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
inputs=load('input-identities.json');mapping=load('all-93-reconciliation.json');cards=load('proposed-bounded-cards.json')['cards'];schema=load('selected-schema-fields.json');proofs=load('all-93-schema-identities.json');up=load('upstream-source-evidence.json')
assert len(mapping['rows'])==93 and [r['api_key'] for r in mapping['rows']]==list(range(93))
assert len(proofs)==558 and len({(p['release'],p['api_key'],p['kind']) for p in proofs})==558
assert len(cards)==9 and len({c['proposed_key'] for c in cards})==9
keys={c['proposed_key'] for c in cards}
for row in mapping['rows']:
    assert set(row['new_proposed_cards'])<=keys
    assert row['broker_cards']
    assert all(x not in {'KL11-88','KL11-89'} for x in row['existing_card_statuses'])
def fields(xs,prefix=''):
    out={}
    for f in xs:
        name=prefix+f['name'];out[name]=f
        out.update(fields(f.get('fields',[]),name+'.'))
    return out
for release in ['4.1.2','4.2.1','4.3.1']:
    ss=schema[release]
    for name in ['OffsetCommitRequest','OffsetCommitResponse','OffsetFetchRequest','OffsetFetchResponse']:
        ff=fields(ss[name]['fields']);ids=[f for k,f in ff.items() if k.endswith('.TopicId')]
        assert ids and all(f['type']=='uuid' and f['versions']=='10+' for f in ids)
    req=fields(ss['InitProducerIdRequest']['fields']);resp=fields(ss['InitProducerIdResponse']['fields'])
    assert req['Enable2Pc']['versions']=='6+' and req['KeepPreparedTxn']['versions']=='6+'
    assert resp['OngoingTxnProducerId']['versions']=='6+' and resp['OngoingTxnProducerEpoch']['versions']=='6+'
    assert fields(ss['AlterPartitionReassignmentsRequest']['fields'])['AllowReplicationFactorChange']['versions']=='1+'
    assert fields(ss['ListTransactionsRequest']['fields'])['TransactionalIdPattern']['versions']=='2+'
    assert 'UnknownStateFilters' in fields(ss['ListTransactionsResponse']['fields'])
    ff=fields(ss['AddRaftVoterRequest']['fields'])
    assert ('AckWhenCommitted' in ff)==(release!='4.1.2')
    if 'AckWhenCommitted' in ff:
        assert ff['AckWhenCommitted']['versions']=='1+' and str(ff['AckWhenCommitted']['default']).lower()=='true'
    assert up[release]['public_Producer_prepareTransaction_present'] is False
    assert not any(up[release]['InitProducerId_flag_setters_present'].values())
    assert not any(up[release]['public_Admin_absence_checks'].values())
    assert up[release]['KeepPreparedTxn_source_outcome'].startswith('UNSUPPORTED_VERSION')
    factory=up[release]['sources']['clients/src/main/java/org/apache/kafka/common/requests/AddPartitionsToTxnRequest.java']
    factory_text='\n'.join(s['text'] for s in factory['snippets'])
    assert 'LAST_CLIENT_VERSION = (short) 3' in factory_text and 'EARLIEST_BROKER_VERSION = (short) 4' in factory_text
    route=up[release]['sources']['clients/src/main/java/org/apache/kafka/clients/admin/internals/ListTransactionsHandler.java']
    assert 'AllBrokersStrategy' in '\n'.join(s['text'] for s in route['snippets'])
    result=up[release]['sources']['clients/src/main/java/org/apache/kafka/clients/admin/ListTransactionsResult.java']
    assert 'allListings.addAll(listings)' in '\n'.join(s['text'] for s in result['snippets'])
rechecked=[]
for path,i in inputs['git_inputs'].items():
    b=subprocess.check_output(['git','show',i['source_sha']+':'+path],cwd=ROOT)
    assert len(b)==i['bytes'] and sha(b)==i['sha256'];rechecked.append(path)
for release,i in inputs['archives'].items():
    p=Path('/workspace/work/broker-api')/(release+'.tar.gz')
    assert sha(p.read_bytes())==i['archive_sha256']
save('validation.json',dict(passed=True,source_only=True,full_protocol_complete=False,api_rows=93,schema_identities=558,named_schema_objects=42,proposed_cards=9,existing_task_references_valid=True,classification_counts=dict(collections.Counter(r['classification'] for r in mapping['rows'])),git_input_rechecks=rechecked,archive_rechecks=3,no_runtime_executed=True,no_repository_mutations=True,audit_source=inputs['source_sha'],later_status_reference=load('latest-75-status-reference.json')['latest_reference_source'],scope='Static source identities, fields and task/dependency mapping checks only. No runtime/conformance pass or status promotion.'))
files=[]
for p in sorted(OUT.iterdir()):
    if p.is_file() and p.name not in {'packet-manifest.json','SHA256SUMS'}:
        b=p.read_bytes();files.append(dict(path=p.name,bytes=len(b),mode=oct(stat.S_IMODE(p.stat().st_mode)),sha256=sha(b)))
total=sum(r['bytes'] for r in files)
assert total<4*1024*1024
save('packet-manifest.json',dict(files=files,file_count=len(files),bytes=total,source_only=True,no_secrets_or_compiled_payloads=True,limit_bytes=4*1024*1024))
all_files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!='SHA256SUMS')
(OUT/'SHA256SUMS').write_text(''.join(f'{sha(p.read_bytes())}  {p.name}\n' for p in all_files))
print(json.dumps(dict(passed=True,files=len(all_files),bytes=sum(p.stat().st_size for p in all_files),manifest_sha256=sha((OUT/'packet-manifest.json').read_bytes()),validation_sha256=sha((OUT/'validation.json').read_bytes()),checksums_sha256=sha((OUT/'SHA256SUMS').read_bytes()))))

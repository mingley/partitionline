#!/usr/bin/env python3
"""WORK-only case/task plan; immutable input reads and no product execution."""
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile

ROOT=Path('/workspace/partitionline');OUT=Path('/workspace/work/consumer-case-task-reconciliation-03')
PIN='497f0722100d2ec34cfa6e0775bb278190f53693'
def sha(b):return hashlib.sha256(b).hexdigest()
def save(n,j):(OUT/n).write_text(json.dumps(j,indent=2,sort_keys=True)+'\n')
inputs={}
def read(p):
 b=subprocess.check_output(['git','show',PIN+':'+p],cwd=ROOT)
 meta=subprocess.check_output(['git','ls-tree',PIN,'--',p],cwd=ROOT).decode().split('\t')[0].split()
 inputs[p]=dict(sha256=sha(b),bytes=len(b),git_blob=meta[2],git_mode=meta[0],source_sha=PIN)
 return b
cases=json.loads(read('tests/conformance/cases.json'))['cases'];features=json.loads(read('tests/conformance/features.json'))
tasks={t['id']:t for t in json.loads(read('docs/plan/tasks.json'))['tasks']}
inv=json.loads(read('docs/evidence/client/protocol-inventory-current/review-01/corrected-inventory-proposal.json'))
for p in ['scripts/check-protocol-coverage.py','tests/conformance/test_check_protocol_coverage.py','scripts/conformance-report.py','tests/conformance/librdkafka/pin.json','tests/conformance/java/pins.json']:
 read(p)
prior93=json.loads(read('docs/evidence/client/protocol-inventory-current/gap-audit-01/all-93-reconciliation.json'))
priorinstall=json.loads(read('docs/evidence/client/protocol-inventory-current/gap-audit-01/root-card-installation.json'))
open_tasks={i for i,t in tasks.items() if t['status'] in ['pending','in_progress']}
assert len(cases)==169 and len(tasks)==316 and len(inv)==93

proposals=[]
def propose(key,title,api,caseids,extra='',deps=None):
 if any(x['proposed_key']==key for x in proposals):return
 suffix=key.removeprefix('Q-CONF-');stem=re.sub(r'(?<!^)(?=[A-Z])','_',suffix).lower()
 proposals.append(dict(proposed_key=key,id=None,status='proposed_not_in_taskbook',kind='evidence',priority='P1',title=title,api_scope=api,case_ids=caseids,depends_on=deps or ['KL01-03'],write_set=['new: tests/conformance/java/Conformance'+suffix+'.java','new: tests/conformance/java/generate_'+stem+'.py','new: tests/'+stem+'_conformance.rs','new: tests/fixtures/'+stem+'/','new: docs/evidence/conformance/'+key.lower()+'/'],root_integration_only=['tests/conformance/cases.json','tests/conformance/features.json','docs/plan/tasks.json'],acceptance=['Enumerate exact applicable source cases and parameter invocations for this single API or small behavioral family; retain unsupported/nonapplicable cases with source-bound reasons.','Run authentic pinned Apache4.1.2/4.2.1/4.3.1 builders/parsers and public behavior where available, plus the stated historical peer cells. Actual fields/histories/errors must match independently.','Preserve every old source/peer/disposition and actual failed attempt. Any later supersession needs an explicit independently reviewed new-source result; never promote authored or local roundtrip coverage.','Keep current corpus and qualification denominators unchanged except additive applicable cells. No product implementation dependency or done tool alone closes this evidence card.',extra],boundedness=['Finite named versions/cases, nested counts/bytes, source/runtime hashes and one overall process/deadline budget.','No unbounded all-oracles job; all owned peers join and failures remain in the report.'],full_qualification_claim=False))

families={'Produce':'Q-CONF-ProduceWire','Fetch':'Q-CONF-FetchWire','Metadata':'Q-CONF-MetadataWire','ListOffsets':'Q-CONF-ListOffsetsWire'}
grouped=collections.defaultdict(list)
rows=[]
excluded=[]
for c in cases:
 cid=c['id'];status=c['disposition'];required=c.get('denominator',True)
 b=[];new=[];why='';closed=set(re.findall(r'KL\d\d-\d+',json.dumps(c)))
 if required and status!='independent_pass':
  if cid.startswith('audit-regression-'):
   new=['Q-CONF-FetchDeliveryRegressions'];why='The failed historical delivered-position/fetch filtering/failure invariants cite done fixes; require independently evidenced replay, not automatic promotion.'
  elif cid.startswith('share-v2-'):
   b=['KL03-21'];why='Exact share acquisition/lock/session/coordinator failure histories are part of the open share churn cell; controlled localTCP outcomes remain local.'
  elif cid.startswith('current-public-api-'):
   key=int(cid.split('-')[3])
   if key==27:b=['KL05-29']
   elif key==57:new=['Q-CONF-UpdateFeatures']
   elif key==88:b=['KL05-31','KL05-32']
   elif key==89:b=['KL05-31','KL05-33']
   elif key==90:b=['KL05-30']
   elif key in [91,92]:new=['Q-CONF-ShareOffsetMutations']
   else:raise AssertionError(cid)
   why='Exact required public API/version/peer cell needs independent execution/import; current callable feature or old internal-role exclusion does not qualify it.'
  elif cid=='feature-compression-zstd-decode-encode':b=['KL05-03','KL05-04','KL05-05'];why='Missing codec implementation and independent bidirectional qualification are all still open.'
  elif cid=='feature-auth-gssapi-kerberos':b=['KL06-08','KL06-09','KL06-10'];why='Concrete Kerberos decision/exchange/qualification remains open; Cyrus/native footprint does not exclude transferable auth semantics.'
  elif cid=='feature-fetch-incremental-session-runtime':new=['Q-CONF-FetchSessionRuntime'];why='Done implementation06/07 does not independently qualify the retained historical not_run cell.'
  elif cid=='feature-produce-fetch-quota-throttle-runtime':new=['Q-CONF-QuotaThrottleRuntime'];why='Done throttle implementations08/09 do not independently qualify actual quota delay/runtime histories.'
  elif cid.startswith('broker-cell-historical-') or cid=='verifiable-produce-consume-3-9-1':new=['Q-CONF-HistoricalOrdinary'];why='Retained historical smoke/mock histories are not complete independently validated ordinary behavior; current3SDK proof cannot promote those pins.'
  elif cid.startswith('verifiable-rebalance-') or cid.startswith('verifiable-commit-failure-'):b=['KL03-21'];why='Open classic/cooperative group churn explicitly includes delivered/committed offsets and recovery; mock event parser consistency remains unqualified.'
  elif cid.startswith('verifiable-producer-options-'):new=['Q-CONF-VerifiableProducerOptions'];why='Finite CLI/config option parity needs actual public reference execution, not synthetic events.'
  elif cid.startswith('verifiable-consumer-'):new=['Q-CONF-VerifiableConsumerOffsets'];why='Finite reset/autocommit CLI profiles need actual reference histories, not mock event transcripts.'
  elif c.get('api_family') in families:new=[families[c['api_family']]];why='Required historical local/blocked/unsupported wire cell remains required; newer max-version evidence and done generators do not qualify other versions or expected refusals.'
  else:raise AssertionError(('unmapped required gap',cid))
  assert b or new
  assert all(t in open_tasks for t in b)
  for n in new:grouped[n].append(cid)
 elif not required:
  key=int(cid.split('-')[3]);assert status=='not_applicable'
  scope='removed_current_role' if key in [4,5,6,7] else 'historical_internal_role' if key in [27,55,57,67] else 'current_broker_internal_role'
  why='Historical/internal source role only; neither all use of this key nor the broker implementation is excluded from the user goal.'
  excluded.append(dict(case_id=cid,api_key=key,original_reason=c['reason'],proposed_exclusion_scope=scope,denominator=False,requires_current_applicable_parallel_cells=key in [27,55,57,67],reason_needs_narrow_correction=key in [55,67]))
 else:why='Retain independently accepted original source/peer/artifact scope exactly; no new runtime result or blanket family qualification.'
 artifact_checks=[]
 for a in c.get('artifacts',[]):
  try:
   bb=read(a);artifact_checks.append(dict(path=a,pinned_git_object_present=True,sha256=sha(bb),bytes=len(bb)))
  except subprocess.CalledProcessError:artifact_checks.append(dict(path=a,pinned_git_object_present=False))
 rows.append(dict(case_id=cid,case_source_sha=c.get('immutable_source_pin',c.get('source_pin')),peer_pin=c.get('peer_pin'),profile=c['profile'],disposition=status,denominator=required,api_family=c.get('api_family'),api_version=c.get('api_version'),existing_case_fields_unchanged=True,existing_explicit_backlog_fields_present=bool(c.get('backlog_tasks')),open_task_bindings=b,proposed_task_bindings=new,reason_only_card_references=sorted(closed),done_reason_references=[t for t in sorted(closed) if t in tasks and tasks[t]['status']=='done'],binding_reason=why,artifact_checks=artifact_checks))

titles={
 'Q-CONF-ProduceWire':('Qualify complete registered Produce wire versions and unsupported historical peers',[0]),
 'Q-CONF-FetchWire':('Qualify complete registered Fetch wire versions and unsupported historical peers',[1]),
 'Q-CONF-MetadataWire':('Qualify complete registered Metadata wire versions and unsupported historical peers',[3]),
 'Q-CONF-ListOffsetsWire':('Qualify complete registered ListOffsets wire versions and unsupported historical peers',[2]),
 'Q-CONF-FetchDeliveryRegressions':('Independently replay delivered-position/fetch boundary regression histories',[1,8,9]),
 'Q-CONF-FetchSessionRuntime':('Qualify incremental Fetch session runtime and bounded reset histories',[1]),
 'Q-CONF-QuotaThrottleRuntime':('Qualify Producer/Consumer quota throttle runtime histories',[0,1]),
 'Q-CONF-HistoricalOrdinary':('Qualify the declared historical ordinary broker/record-event cells',[0,1,2,3]),
 'Q-CONF-VerifiableProducerOptions':('Qualify the finite verifiable Producer option profile',[0]),
 'Q-CONF-VerifiableConsumerOffsets':('Qualify finite verifiable reset and autocommit profiles',[1,2,8,9]),
 'Q-CONF-UpdateFeatures':('Qualify public UpdateFeatures version/option/error behavior',[57]),
 'Q-CONF-ShareOffsetMutations':('Qualify public share offset alter/delete behavior',[91,92]),
}
for key,cids in grouped.items():
 title,apis=titles[key];propose(key,title,apis,cids,'Exact registered case IDs are the declared starting cohort; parameter/source applicability expansion must be recorded before qualification.')
additive=[]
for key,name,versionlist,proposal,role in [
 (55,'DescribeQuorum',[0,1,2],'Q-CONF-DescribeQuorum','public_Java_Admin'),
 (67,'AllocateProducerIds',[0],'Q-CONF-AllocateProducerIdsRaw','Rust_raw_extension_no_Java_Admin_equivalent'),
 (73,'AssignReplicasToDirs',[0],'Q-CONF-AssignReplicasToDirsRaw','Rust_raw_extension_no_Java_Admin_equivalent')]:
 for peer in ['4.1.2','4.2.1','4.3.1']:
  for v in versionlist:
   additive.append(dict(id=f'current-claimed-api-{key:03d}-v{v}-{peer.replace(".","-")}',api_key=key,api_family=name,api_version=v,profile='admin',adapter='none',source_pin=PIN,immutable_source_pin=PIN,peer_pin=peer,peer_version_pin=peer,disposition='not_run',denominator=True,applicability_role=role,proposed_backlog_tasks=[proposal],reason='Additive current claimed operation cell, independent of historical internal-role exclusions; no execution imported by this WORK plan.'))
 ids=[c['id'] for c in additive if c['api_key']==key]
 extra='Use genuine public Java Admin.describeMetadataQuorum where available; quorum leader/internal-only role is not a blanket public exclusion.' if key==55 else 'Do not invent a standard Java Admin method: use authentic generated Apache request/parser and actual broker/reference implementation, plus bounded Rust raw-extension dispatch. Wire/body comparison is distinguished from runtime behavior.'
 propose(proposal,'Qualify current '+name+(' public Admin' if key==55 else ' raw extension')+' cells',[key],ids,extra,deps=['KL01-03','KL05-19'] if key==55 else ['KL01-03'])

save('case-binding-plan.json',dict(source_sha=PIN,case_count=169,required_cases=149,required_independent_cases=16,required_non_independent_cases=133,excluded_cases=20,rows=rows,additive_current_claimed_cases=additive,proposed_required_after_additive=164,full_protocol_complete=False,core_protocol_complete=False,changed_original_source_peer_disposition_artifact_fields=False))
save('proposed-case-conformance-cards.json',dict(source_sha=PIN,proposals=proposals,new_IDs_assigned=False,scope='Finite new conformance families. Actual existing open tasks are used where their acceptance covers the gap; done implementation/tool evidence never counts as remaining work.'))
save('excluded-denominator-review.json',dict(source_sha=PIN,current_excluded_cases=20,removed_current_role_count=4,historical_dual_or_raw_internal_role_count=4,current_internal_role_count=12,rows=excluded,framework_limit='Streams/Connect execution and C symbol ABI exclusions do not exclude transferable protocol/producer/consumer/group/transaction/auth behavior.',full_broker_goal_excluded=False))

# Recheck every known candidate source from its immutable archive/Git object.
candidate_path=Path('/workspace/work/conformance-reconciliation-14/candidate-upstream-corpus.json')
candidate_bytes=candidate_path.read_bytes();candidate=json.loads(candidate_bytes)
source_rows=[]
for release in candidate['apache']:
 archive=Path('/workspace/work/broker-api')/(release['release']+'.tar.gz')
 assert sha(archive.read_bytes())==release['source_archive_sha256']
 with tarfile.open(archive) as tf:
  idx={n.partition('/')[2]:n for n in tf.getnames()}
  for row in release['source_files']:
   b=tf.extractfile(idx[row['path']]).read();assert sha(b)==row['sha256']
   source_rows.append(dict(release=release['release'],commit=release['source_commit'],path=row['path'],sha256=row['sha256'],kind=row['kind'],candidate_methods=row.get('candidate_methods',[]),parameter_expansion_enumerated=row.get('parameter_expansion_enumerated',False),applicability='unresolved until exact source case/parameters are classified',qualified_by_source_enumeration=False))
native=candidate['librdkafka'];native_repo=Path('/workspace/work/c-peer/source')
for row in native['source_files']:
 b=subprocess.check_output(['git','show',native['source_commit']+':'+row['path']],cwd=native_repo)
 assert sha(b)==row['sha256']
 source_rows.append(dict(release='librdkafka2.15.0',commit=native['source_commit'],path=row['path'],sha256=row['sha256'],kind='native_numbered_regression',candidate_methods=row.get('candidate_entries',[]),parameter_expansion_enumerated=row.get('parameter_expansion_enumerated',False),applicability='only0125 selected assertion independently evidenced; siblings/other candidates unresolved',qualified_by_source_enumeration=False))
save('known-upstream-corpus-review.json',dict(original_inventory_path=str(candidate_path),original_inventory_sha256=sha(candidate_bytes),source_files=len(source_rows),candidate_method_or_entry_names=sum(len(r['candidate_methods']) for r in source_rows),rows=source_rows,all_known_source_bytes_rechecked=True,complete_upstream_corpus=False,unresolved_applicability_backlog_tasks=['KL01-14'],unresolved_task_status_at_review=tasks['KL01-14']['status'],remaining_limit='This known candidate inventory covers request/Admin tests, kafkatest system files and numbered native tests. Other Java producer/consumer/security/unit sources and actual parameter expansion remain unenumerated. No exhaustive applicability or passing claim. KL01-14 remains open.',root_prior_source_applicability_proposals='/workspace/work/conformance-reconciliation-14/source-applicability-proposals.json'))
save('current-93-audit-reference.json',dict(source_sha=PIN,original_audit_source_sha=prior93.get('source_sha',prior93.get('source_commit')),prior93_artifact='docs/evidence/client/protocol-inventory-current/gap-audit-01/all-93-reconciliation.json',prior93_identity=inputs['docs/evidence/client/protocol-inventory-current/gap-audit-01/all-93-reconciliation.json'],installed_cards_artifact='docs/evidence/client/protocol-inventory-current/gap-audit-01/root-card-installation.json',installed_cards_identity=inputs['docs/evidence/client/protocol-inventory-current/gap-audit-01/root-card-installation.json'],installed_card_record=priorinstall,reference_only=True,no_source_or_runtime_qualification_transfer=True))
save('input-identities.json',dict(source_sha=PIN,git_inputs=inputs,known_upstream_inventory_sha256=sha(candidate_bytes),no_runtime_executed=True,no_repo_edits=True))
print(json.dumps(dict(cases=169,gaps=133,new_conformance_families=len(proposals),additive_required=15,known_sources=len(source_rows),candidate_names=sum(len(r['candidate_methods']) for r in source_rows),bytes=sum(p.stat().st_size for p in OUT.iterdir() if p.is_file()))))

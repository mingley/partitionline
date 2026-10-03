#!/usr/bin/env python3
import json
from pathlib import Path
P=Path('/workspace/work/consumer-case-task-reconciliation-03')
def load(n):return json.loads((P/n).read_text())
def save(n,v):(P/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
j=load('feature-independent-review.json');base=load('proposed-case-conformance-cards.json')
alias={
 'P01-registry-scope-reconciliation':'KL01-14',
 'P02-producer-owned-backing':'Q-IMPL-ProducerBacking',
 'P03-kip848-interval-conformance':'KL03-21',
 'P04-producer-reauth':'Q-IMPL-ProducerReauth',
 'P05-consumer-reauth':'Q-IMPL-RpcReauth',
 'P06-group-reauth':'Q-IMPL-RpcReauth',
 'P07-share-reauth':'Q-IMPL-RpcReauth',
 'P08-admin-reauth':'Q-IMPL-RpcReauth',
 'P09-oidc-manager-retirement':'Q-IMPL-OidcManagerOwnership',
 'P10-avro-production-codec-conformance':'Q-CONF-AvroBackend',
 'P11-json-production-codec-conformance':'Q-CONF-JsonSchemaBackend',
 'P12-generic-header-scope':'Q-CONF-GenericSchemaHeader',
 'P13-quorum-current-cells':'Q-CONF-DescribeQuorum',
 'P14-raw-api67-cells':'Q-CONF-AllocateProducerIdsRaw',
 'P15-raw-api73-cells':'Q-CONF-AssignReplicasToDirsRaw',
}
# Use exact actual report keys; retain original independent proposal identifiers.
keys=[c['proposal_key'] for c in j['proposed_bounded_tasks']]
assert len(keys)==15
assert set(keys)==set(alias), (keys, list(alias))
mapping=dict(alias)
existing={c['proposed_key'] for c in base['proposals']}
files={
 'Q-IMPL-ProducerBacking':['src/producer.rs','new: tests/producer_retained_backing.rs','new: docs/evidence/client/producer-retained-backing/'],
 'Q-IMPL-ProducerReauth':['src/producer.rs','src/protocol/sasl.rs','src/net.rs','new: tests/producer_sasl_reauthentication.rs','new: docs/evidence/client/producer-sasl-reauthentication/'],
 'Q-IMPL-RpcReauth':['src/net.rs','src/protocol/sasl.rs','src/consumer.rs','src/group.rs','src/share.rs','src/admin.rs','new: tests/rpc_sasl_reauthentication.rs','new: docs/evidence/client/rpc-sasl-reauthentication/'],
 'Q-IMPL-OidcManagerOwnership':['src/protocol/oidc.rs','src/protocol/sasl.rs','new: tests/oidc_manager_ownership.rs','new: docs/evidence/client/oidc-manager-ownership/'],
 'Q-CONF-AvroBackend':['new: partitionline-schema/tests/avro_selected_codec_conformance.rs','new: tests/conformance/java/ConformanceAvroSelectedCodec.java','new: tests/fixtures/avro_selected_codec/','new: docs/evidence/conformance/avro-selected-codec/'],
 'Q-CONF-JsonSchemaBackend':['new: partitionline-schema/tests/json_schema_selected_codec_conformance.rs','new: tests/conformance/java/ConformanceJsonSchemaSelectedCodec.java','new: tests/fixtures/json_schema_selected_codec/','new: docs/evidence/conformance/json-schema-selected-codec/'],
 'Q-CONF-GenericSchemaHeader':['new: partitionline-schema/tests/generic_schema_header_conformance.rs','new: tests/conformance/java/ConformanceGenericSchemaHeader.java','new: tests/fixtures/generic_schema_header/','new: docs/evidence/conformance/generic-schema-header/'],
}
additional={}
for c in j['proposed_bounded_tasks']:
 key=mapping[c['proposal_key']]
 if key.startswith('KL') or key in existing:continue
 if key not in additional:
  additional[key]=dict(proposed_key=key,id=None,status='proposed_not_in_taskbook',title=c['title'],kind='implementation' if key.startswith('Q-IMPL') else 'evidence',priority='P1',depends_on=c['depends_on_actual_task_ids'],write_set=files[key],root_integration_only=['docs/plan/tasks.json','tests/conformance/cases.json','tests/conformance/features.json','Cargo manifests/lock if needed'],scope=c['scope'],acceptance=list(dict.fromkeys(c['acceptance'])),caller_scopes=[c['scope']],original_independent_proposal_keys=[c['proposal_key']],qualification_limit='Current source/accepted tool does not prove caller integration, physical backing ownership, or full schema semantics. New independent exact-source qualification required.')
 else:
  x=additional[key];x['original_independent_proposal_keys'].append(c['proposal_key']);x['acceptance']=list(dict.fromkeys(x['acceptance']+c['acceptance']));x['caller_scopes'].append(c['scope'])
additional['Q-IMPL-RpcReauth']['title']='Integrate owned session reauthentication into shared roundtrip RPC callers'
additional['Q-IMPL-RpcReauth']['scope']='One shared roundtrip transport implementation with separate Consumer/ConsumerGroup/ShareGroup/Admin caller qualification, not four duplicated transport engines.'
rows=[]
for f in j['feature_findings']:
 pkeys=sorted({mapping[k] for k in f.get('proposed_task_keys',[]) if not mapping[k].startswith('KL')})
 bound=sorted({t['id'] for t in f.get('actual_open_tasks',[])})
 if 'P03-kip848-interval-conformance' in f.get('proposed_task_keys',[]):bound=sorted(set(bound+['KL03-21']))
 rows.append(dict(feature_id=f['feature_id'],original_disposition=f['current_disposition'],profile=f['profile'],layer=f['layer'],open_task_bindings=bound,proposed_task_bindings=pkeys,source_scope_reconciliation_task='KL01-14' if any(mapping[k]=='KL01-14' for k in f.get('proposed_task_keys',[])) else None,scope_reconciliation_does_not_qualify_runtime=True,done_task_provenance=f['related_done_tasks'],finding=f['finding'],original_notes_unchanged=True,exclusion_scope='framework_or_internal_role_only' if f['current_disposition']=='out_of_scope' else None))
allproposals=base['proposals']+list(additional.values())
assert len(allproposals)==22
save('feature-binding-plan.json',dict(source_sha=j['source_commit'],reviewed_non_present_features=28,rows=rows,present_feature_count=147,present_means_implementation_not_independent_qualification=True,full_feature_qualification=False,prototype_api_aliases=mapping,original_report_retained=True))
save('unified-bounded-proposals.json',dict(source_sha=base['source_sha'],proposals=allproposals,count=22,source_only=True,proposal_IDs_assigned=False,scope='15 registered/current case conformance families and7 residual implementation/selected-backend families. Existing open scopes reused where applicable.',omitted_as_duplicate_or_existing=sorted(set(keys)-{k for c in additional.values() for k in c['original_independent_proposal_keys']})))
print(json.dumps(dict(new=22,features=28)))

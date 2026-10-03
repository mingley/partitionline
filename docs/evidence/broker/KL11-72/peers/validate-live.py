#!/usr/bin/env python3
"""Validate exact live follow-up sidecars; mixed native SDK completions stay failed."""
import argparse,hashlib,json
from pathlib import Path

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 r=json.loads((a.directory/'results.json').read_text());assert r['verdict']=='scoped_server_gates_passed_with_sdk_completion_failures';assert r['driver_completed'] and not r['unqualified_all_peer_pass']
 assert r['server_contract_gates']=='passed' and r['positive_native_admin_restart_completion']=='passed' and r['native_describe_error_completion']=='failed'
 names=set();passing=[];failed=[];assertions={}
 for c in r['commands']:
  for stream in ['stdout','stderr']:
   file=a.directory/c[stream];assert sha(file)==c[stream+'_sha256'];assert file.name not in names;names.add(file.name)
  assert c['expected_observation'] and not c['timed_out']
  if c['exit_code']==0:
   assert c['completion_verdict']=='passed';passing.append(c['stdout'])
   if c['stdout'].startswith(('4.','native-')):
    events=[json.loads(x) for x in (a.directory/c['stdout']).read_text().splitlines()];assert events[-1]['status']=='pass' and events[-1]['assertions']>0;assertions[c['stdout']]=events[-1]['assertions']
  else:
   assert c['exit_code']==-6 and c['completion_verdict']=='failed_external_sdk_completion';failed.append(c['stdout'])
 assert len(passing)==13 and len(failed)==2 and len(assertions)==10
 assert len(r['known_external_sdk_failures'])==2
 for c in r['known_external_sdk_failures']:
  assert c['verdict']=='failed_external_sdk_completion' and not c['clean_native_completion'] and c['pre_cleanup_marker_verified'];assert c['stdout'] in failed
  events=[json.loads(x) for x in (a.directory/c['stdout']).read_text().splitlines()];assert events[-1]=={'operation':'native-describe-cleanup','top_level_error':31,'action':'destroy-event'};assert not any(x.get('status')=='pass' for x in events)
 assert len(r['server_lifecycles'])==3;accepted=joined=0
 for s in r['server_lifecycles']:
  assert s['exit_code']==0 and s['pending_sockets_at_stop']==2
  for stream in ['stdout','stderr']:assert sha(a.directory/s[stream])==s[stream+'_sha256']
  events=[json.loads(x) for x in (a.directory/s['stdout']).read_text().splitlines()];assert events==s['events'];e=events[-1];assert e['event']=='shutdown' and e['worker_failures']==0 and e['credential_store_joined'];assert e['tls_accepted']==e['tls_joined'] and e['plain_accepted']==e['plain_joined'];accepted+=e['tls_accepted']+e['plain_accepted'];joined+=e['tls_joined']+e['plain_joined']
 assert r['final_journal_audit']['entries']==9
 assert r['final_journal_audit']['canonical_verifier_schema_only'] and r['final_journal_audit']['stored_server_keys_independently_derived'] and r['final_journal_audit']['password_and_salted_password_raw_hex_base64_absent'] and r['passwords_absent_from_logs']
 for s in r['describe31_official_schema_proofs']:assert s['whole_input_consumed'] and s['results_empty'] and s['error_message_null'] and s['error']==31
 assert r['captured_rotation_generation']<r['new_rotation_generation'] and r['preauth_application_dispatches']==0
 out={'source_sha':r['source_sha'],'scope':'Sidecar/source/actual count validation; two external SDK error-cleanup completions remain failed.','verdict':r['verdict'],'passing_commands':len(passing),'failed_sdk_completion_commands':len(failed),'successful_peer_processes':len(assertions),'successful_peer_assertions':assertions,'successful_peer_assertions_total':sum(assertions.values()),'owned_listener_lifecycles':3,'accepted_connections':accepted,'joined_connections':joined,'verifier_journal_entries':9,'official_describe31_schema_proofs':len(r['describe31_official_schema_proofs']),'checked_result_sha256':sha(a.directory/'results.json'),'validation_producer_sha256':sha(Path(__file__)),'sidecar_hashes_verified':True,'production_qualification':False}
 a.output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
if __name__=='__main__': main()

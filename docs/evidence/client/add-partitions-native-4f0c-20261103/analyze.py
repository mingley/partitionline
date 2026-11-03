#!/usr/bin/env python3
"""Offline analysis of the retained AddPartitions hosted-run evidence."""
import hashlib
import json
from pathlib import Path
import re
import zipfile

HERE=Path(__file__).resolve().parent
TRIGGER='4f0c13d8c6600b0c7a5048f7a0499b550cf5e000'
BASE='45cbff24de76490594970fc962cd04e7be9fafb1'
PATCH='5e356bd2d7616354de4a3e60ebd23bd12104bc4a9d8a29cc65cbd258f38d8cfe'
WITNESS='539c1df76bc8114792699c09d65ff0986b1afddfbcc2803ec3cf115e783f4a6c'
HELPER='34ddbf04a4bbead974fad4a451d30c7a56818248bd5b58c88c87bdfc41896da9'
POST='9154ec1b502bc4c639b6a1f6cb30568f72a91f442123a7f6d1378e46ea2e93ff'
WITNESSES={
 'missing_response_must_not_mark_partitions_added':'WITNESS_MISSING_EXPECTS_PROTOCOL',
 'mixed_load_and_fence_must_record_fence':'WITNESS_FENCE_EXPECTS_BROKER',
}
FOCUSED=(
 'coverage_and_fatal_precedence_for_all_spoken_versions',
 'rejects_incomplete_duplicate_unrequested_and_malformed_results',
 'flexible_tags_must_be_complete_and_strictly_ordered',
 'dropping_validation_before_later_fence_fails_closed',
 'worker_handles_mixed_fence_missing_and_success',
 'worker_retries_noncoordinator_response_then_sees_fence_or_success',
)
sha=lambda b:hashlib.sha256(b).hexdigest()
read=lambda name:json.loads((HERE/name).read_text())
run,jobs,artifacts,pins=map(read,['run.json','jobs.json','artifacts.json','source-pins.json'])
assert run['id']==41676355784 and run['head_sha']==TRIGGER and run['run_attempt']==1
assert pins['source_commit']==BASE and pins['trigger_commit']==TRIGGER
assert pins['independently_applied_candidate_source_sha256']==POST
assert sha((HERE/'candidate-v3.patch').read_bytes())==PATCH
assert sha((HERE/'witness.rs').read_bytes())==WITNESS
assert sha((HERE/'candidate-command-ownership.py').read_bytes())==HELPER
assert len(jobs['jobs'])==1 and jobs['jobs'][0]['id']==126057918582
art=next(a for a in artifacts['artifacts'] if a['name'].startswith('add-partitions-v3-') and a['workflow_run']['head_sha']==TRIGGER)
archive=(HERE/'artifact.zip').read_bytes(); assert art['digest']=='sha256:'+sha(archive)

def result_line(text,ok,count):
 return bool(re.search(r'(?m)^test result: '+('ok' if ok else 'FAILED')+r'\. '+str(count if ok else 0)+r' passed; '+str(0 if ok else 1)+r' failed; 0 ignored;',text))
with zipfile.ZipFile(HERE/'artifact.zip') as z:
 assert z.testzip() is None
 names=z.namelist()
 summary_names=[n for n in names if n.endswith('summary.json')]
 if len(summary_names)!=1:
  print(json.dumps({'run_conclusion':run['conclusion'],'artifact_id':art['id'],'archive_sha256':sha(archive),'problem':'summary.json missing or ambiguous','archive_entries':names},indent=2));raise SystemExit(0)
 prefix=summary_names[0][:-len('summary.json')]
 summary=json.loads(z.read(prefix+'summary.json')); steps={s['name']:s for s in summary.get('steps',[])}
 assert summary['base_commit']==BASE and summary['trigger_sha']==TRIGGER
 assert summary['run_id']=='41676355784' and summary['run_attempt']=='1'
 assert summary['patch_sha256']==PATCH and summary['witness_sha256']==WITNESS and summary['ownership_helper_sha256']==HELPER
 for key,value in pins['independently_reconstructed_run_inputs'].items():assert summary[key]==value, key
 def inspect(name,kind,test=None,marker=None):
  s=steps.get(name)
  if not s:return {'status':'missing'}
  entry=prefix+s.get('log','')
  if entry not in names:return {'status':'missing_log','exit_code':s.get('exit_code')}
  raw=z.read(entry);log=raw.decode('utf-8',errors='replace');flags=[]
  if s.get('timed_out') is not False:flags.append('timeout')
  if s.get('disk_floor_breached') is not False:flags.append('disk_floor')
  if s.get('spawn_or_wait_error'):flags.append('spawn_or_wait_error')
  if s.get('unexpected_descendants'):flags.append('unexpected_descendants')
  c=s.get('ownership_cleanup') or {}
  if c.get('leader_waited') is not True or c.get('remaining_descendants')!=[] or c.get('leader_exit')!=s.get('exit_code'):flags.append('cleanup_unconfirmed')
  if kind=='baseline':
   full='producer::add_partitions_regression_witness::'+test
   if s.get('exit_code')!=101 or marker not in log or not re.search(r'(?m)^running 1 test$',log) or not re.search(r'(?m)^test '+re.escape(full)+r' \.\.\. FAILED$',log) or not result_line(log,False,1):flags.append('expected_failure_not_verified')
  elif kind=='focused':
   expected=['producer::add_partitions_validation_tests::'+n for n in FOCUSED]
   if s.get('exit_code')!=0 or not re.search(r'(?m)^running 6 tests$',log) or not result_line(log,True,6) or any(not re.search(r'(?m)^test '+re.escape(n)+r' \.\.\. ok$',log) for n in expected):flags.append('focused_tests_not_verified')
  elif kind=='witness':
   full='producer::add_partitions_regression_witness::'+test
   if s.get('exit_code')!=0 or not re.search(r'(?m)^running 1 test$',log) or not result_line(log,True,1) or not re.search(r'(?m)^test '+re.escape(full)+r' \.\.\. ok$',log):flags.append('witness_success_not_verified')
  elif kind=='command':
   if s.get('exit_code')!=0:flags.append('command_failed')
  return {'status':'verified' if not flags else 'unverified','issues':flags,'exit_code':s.get('exit_code'),'duration_seconds':s.get('duration_seconds'),'log':entry,'log_sha256':sha(raw),'recorded_cleanup':c}
 checks={}
 for profile in ('default','all-features'):
  checks[profile]={'baseline':{n:inspect(f'baseline-{profile}-{n}','baseline',n,m) for n,m in WITNESSES.items()},
   'candidate_focused':inspect(f'candidate-{profile}-focused','focused'),
   'candidate_witnesses':{n:inspect(f'candidate-{profile}-{n}','witness',n) for n in WITNESSES}}
 for s in steps.values():
  if not s.get('name','').startswith(('baseline-','candidate-')) and s.get('log') and prefix+s['log'] in names:pass
 prereq={name:inspect(name,'command') for name in ('checkout-head','checkout-status','rustc-version','cargo-version','rustfmt-version','clippy-version','patch-check','patch-apply','candidate-rustfmt','candidate-clippy-all-features')}
 capacity=[]
 if prefix+'capacity.jsonl' in names:
  capacity=[json.loads(row) for row in z.read(prefix+'capacity.jsonl').splitlines()]
 mins=[v for s in capacity for v in s.get('free_bytes',{}).values()]
 payload={'run_id':run['id'],'job_id':jobs['jobs'][0]['id'],'trigger_commit':TRIGGER,'tested_base_commit':BASE,
  'run_conclusion':run['conclusion'],'job_conclusion':jobs['jobs'][0]['conclusion'],'artifact_id':art['id'],
  'archive_sha256':sha(archive),'summary_success':summary.get('success'),'summary_error':summary.get('error'),
  'expected_candidate_source_sha256':POST,'recorded_candidate_with_witness_sha256':summary.get('candidate_source_with_witness_sha256'),
  'checks':checks,'prerequisites':prereq,'recorded_step_count':len(steps),'capacity_sample_count':len(capacity),
  'minimum_sampled_free_bytes':min(mins) if mins else None,
  'scope':'Focused unit and local mock-socket tests; no real broker, full client suite, performance or production qualification.'}
 print(json.dumps(payload,sort_keys=True,indent=2))

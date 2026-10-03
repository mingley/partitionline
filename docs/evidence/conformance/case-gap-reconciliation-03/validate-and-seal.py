#!/usr/bin/env python3
"""Validate the static proposal packet, without running or changing the product."""
import collections
import hashlib
import json
from pathlib import Path
import stat
import subprocess

P=Path('/workspace/work/consumer-case-task-reconciliation-03')
ROOT=Path('/workspace/partitionline')
PIN='497f0722100d2ec34cfa6e0775bb278190f53693'
def sha(b):return hashlib.sha256(b).hexdigest()
def load(n):return json.loads((P/n).read_text())
def save(n,v):(P/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def git(p):return subprocess.check_output(['git','show',PIN+':'+p],cwd=ROOT)
original=json.loads(git('tests/conformance/cases.json'))['cases']
features=json.loads(git('tests/conformance/features.json'))
tasks={t['id']:t for t in json.loads(git('docs/plan/tasks.json'))['tasks']}
plan=load('case-binding-plan.json');fplan=load('feature-binding-plan.json')
props=load('unified-bounded-proposals.json')['proposals']
source=load('known-upstream-corpus-review.json')
inputs=load('input-identities.json')
checks=[]
def check(name,v):
 assert v,name
 checks.append(name)
check('case rows exactly preserve all169 IDs', [r['case_id'] for r in plan['rows']]==[c['id'] for c in original])
check('each proposed key unique', len({p['proposed_key'] for p in props})==len(props)==22)
proposed={p['proposed_key'] for p in props}
open_tasks={i for i,t in tasks.items() if t['status'] in ('pending','in_progress')}
check('audit task remains open', 'KL01-14' in open_tasks)
for c,r in zip(original,plan['rows']):
 check(c['id']+' original classification/source retained', r['profile']==c['profile'] and r['disposition']==c['disposition'] and r['denominator']==c.get('denominator',True) and r['case_source_sha']==c.get('immutable_source_pin',c.get('source_pin')) and r['peer_pin']==c.get('peer_pin') and r['existing_case_fields_unchanged'])
 check(c['id']+' references actual open/proposed tasks',set(r['open_task_bindings'])<=open_tasks and set(r['proposed_task_bindings'])<=proposed)
 if r['denominator'] and r['disposition']!='independent_pass':
  check(c['id']+' gap remains tracked',bool(r['open_task_bindings'] or r['proposed_task_bindings']))
 if r['disposition']=='independent_pass':
  check(c['id']+' original accepted artifact structurally present',bool(r['artifact_checks']) and all(a['pinned_git_object_present'] for a in r['artifact_checks']))
check('required149 independent16 unresolved133 excluded20',sum(r['denominator'] for r in plan['rows'])==149 and sum(r['denominator'] and r['disposition']=='independent_pass' for r in plan['rows'])==16 and sum(r['denominator'] and r['disposition']!='independent_pass' for r in plan['rows'])==133 and sum(not r['denominator'] for r in plan['rows'])==20)
check('133 remaining cases consist of39 existing scopes and94 proposed scopes',sum(bool(r['open_task_bindings']) for r in plan['rows'])==39 and sum(bool(r['proposed_task_bindings']) for r in plan['rows'])==94)
check('additive current55/67/73 cells unique and required',len(plan['additive_current_claimed_cases'])==15 and len({r['id'] for r in plan['additive_current_claimed_cases']})==15 and all(r['denominator'] and r['disposition']=='not_run' and set(r['proposed_backlog_tasks'])<=proposed for r in plan['additive_current_claimed_cases']))
check('no old case ID collides with additive cells',not ({r['id'] for r in plan['additive_current_claimed_cases']} & {r['case_id'] for r in plan['rows']}))
check('new cells use three actual pinned SDK releases',{r['peer_pin'] for r in plan['additive_current_claimed_cases']}=={'4.1.2','4.2.1','4.3.1'})
check('raw extensions explicitly lack fake Java Admin counterpart',all(r['applicability_role']=='Rust_raw_extension_no_Java_Admin_equivalent' for r in plan['additive_current_claimed_cases'] if r['api_key'] in (67,73)))
for p in props:
 check(p['proposed_key']+' has concrete scope and acceptance',bool(p['title']) and bool(p['write_set']) and bool(p['acceptance']) and p['id'] is None and p['status']=='proposed_not_in_taskbook')
 check(p['proposed_key']+' dependencies actual or proposed',set(p['depends_on']) <= (set(tasks)|proposed))
 for path in p['write_set']:
  if not path.startswith('new: '):
   check(p['proposed_key']+' existing write path '+path, bool(git(path)))
 check(p['proposed_key']+' acceptance paragraphs unique',len(p['acceptance'])==len(set(p['acceptance'])))
rpc=next(p for p in props if p['proposed_key']=='Q-IMPL-RpcReauth')
check('shared RPC reauth preserves all4 distinct actual caller scopes',len(rpc['caller_scopes'])==4 and len(set(rpc['caller_scopes']))==4 and len(rpc['original_independent_proposal_keys'])==4)
check('non-present feature rows exactly cover all28',{r['feature_id'] for r in fplan['rows']}=={r['id'] for r in features if r['disposition']!='present'})
for r in fplan['rows']:
 check(r['feature_id']+' bindings open/proposed',set(r['open_task_bindings'])<=open_tasks and set(r['proposed_task_bindings'])<=proposed)
 if r['original_disposition'] in ('missing','partial'):
  check(r['feature_id']+' missing/partial remains tracked',bool(r['open_task_bindings'] or r['proposed_task_bindings']))
check('all known567 upstream file identities rechecked',source['all_known_source_bytes_rechecked'] and source['source_files']==len(source['rows'])==567)
check('2477 candidate names are not runtime/parameter coverage',source['candidate_method_or_entry_names']==2477 and not source['complete_upstream_corpus'] and all(not r['qualified_by_source_enumeration'] for r in source['rows']))
check('unexpanded corpus is bound to open14',source['unresolved_applicability_backlog_tasks']==['KL01-14'])
for p,i in inputs['git_inputs'].items():
 b=git(p);tree=subprocess.check_output(['git','ls-tree',PIN,'--',p],cwd=ROOT).decode().split('\t')[0].split()
 check(p+' exact immutable byte/Git-mode identity',sha(b)==i['sha256'] and len(b)==i['bytes'] and tree[0]==i['git_mode'] and tree[2]==i['git_blob'])
check('source-only preserves false completeness',not plan['core_protocol_complete'] and not plan['full_protocol_complete'] and not load('checker-design-plan.json')['core_protocol_complete'] and not load('checker-design-plan.json')['full_protocol_complete'])
check('packet remains below4MiB',sum(p.stat().st_size for p in P.iterdir() if p.is_file())<4*1024*1024)
save('validation.json',dict(passed=True,source_sha=PIN,checks=len(checks),check_names=checks,counts=dict(registered_cases=169,required=149,independent=16,unresolved=133,excluded=20,existing_open_bound_cases=39,new_family_bound_cases=94,additive_required=15,after_additive_cases=184,after_additive_required=164,feature_rows=175,reviewed_missing_partial=13,reviewed_out_of_scope=15,present_implementation_rows=147,new_case_conformance_proposals=15,new_other_proposals=7,unified_proposals=22,known_source_files=567,candidate_names_without_parameter_expansion=2477),dispositions=dict(collections.Counter(c['disposition'] for c in original)),required_profile_denominators=dict(collections.Counter(c['profile'] for c in original if c.get('denominator',True))),no_sdk_jvm_cargo_product_runtime=True,no_canonical_edits=True,source_plan_only_not_independent_qualification=True))
manifest=[]
for p in sorted(P.iterdir()):
 if p.is_file() and p.name not in ('packet-manifest.json','SHA256SUMS'):
  b=p.read_bytes();manifest.append(dict(path=p.name,bytes=len(b),full_mode=oct(stat.S_IMODE(p.stat().st_mode)),sha256=sha(b)))
assert sum(r['bytes'] for r in manifest)<4*1024*1024
save('packet-manifest.json',dict(source_sha=PIN,files=manifest,file_count=len(manifest),total_bytes=sum(r['bytes'] for r in manifest),scope='WORK-only static reconciliation/proposals; no runtime qualification or repository edits'))
allfiles=sorted(p for p in P.iterdir() if p.is_file() and p.name!='SHA256SUMS')
(P/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+p.name+'\n' for p in allfiles))
print(json.dumps(dict(passed=True,checks=len(checks),files=len(manifest),total_bytes=sum(r['bytes'] for r in manifest),manifest_sha256=sha((P/'packet-manifest.json').read_bytes()),validation_sha256=sha((P/'validation.json').read_bytes()))))

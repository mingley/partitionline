#!/usr/bin/env python3
"""Verify the saved hosted flush artifact and emit a compact offline report."""
import hashlib
import json
from pathlib import Path
import re
import zipfile

HERE = Path(__file__).resolve().parent
ARCHIVE = 'e51a183af3728e19d39a54e3c3e669b2d684939afb176daf9e65854d52eeb32a'
BASE = '45cbff24de76490594970fc962cd04e7be9fafb1'
TRIGGER = '86f285c706645e68146737a243707953c524b89d'
PATCH = 'dcb7fa244056a3ccad7c81ba38611e5cb2157d4a7bdfbe8245cedad2cdfd0c03'
POST = '226ca81821663a3d67488dc6086607bde0722ae5bf72a163e8524457fb8ffa50'
TESTS = {
 'flush_reports_an_accepted_record_expiring_in_pending': 'flush lost the delivery timeout: Ok(())',
 'cancelled_flush_cannot_consume_an_unobserved_record_failure': 'cancelled flush discarded the only failure report',
 'flush_cannot_succeed_while_a_retired_worker_still_owns_an_accepted_record': 'flush reported success before the retired worker processed the record',
 'flush_reports_failure_when_retry_guard_drops_an_accepted_record': 'flush lost the retry failure: Ok(())',
}
read_json = lambda p: json.loads((HERE / p).read_text())
sha = lambda data: hashlib.sha256(data).hexdigest()
assert sha((HERE / 'artifact.zip').read_bytes()) == ARCHIVE
run, jobs, arts = read_json('run.json'), read_json('jobs.json'), read_json('artifacts.json')
assert run['id'] == 41674604362 and run['head_sha'] == TRIGGER
assert run['run_attempt'] == 1 and run['status'] == 'completed' and run['conclusion'] == 'success'
assert len(jobs['jobs']) == 1 and jobs['jobs'][0]['id'] == 126051745072 and jobs['jobs'][0]['conclusion'] == 'success'
artifact = next(a for a in arts['artifacts'] if a['id'] == 13249627527)
assert artifact['digest'] == 'sha256:' + ARCHIVE and artifact['workflow_run']['head_sha'] == TRIGGER
with zipfile.ZipFile(HERE / 'artifact.zip') as z:
 assert z.testzip() is None
 result = json.loads(z.read('result.json'))
 assert result['base_commit'] == result['checkout_commit'] == BASE
 assert result['expected_patch_sha256'] == sha(z.read('regression-controls.patch')) == PATCH
 assert result['expected_patched_source_sha256'] == result['patched_source_sha256'] == POST
 assert result['verdict'] == 'expected-assertions-reproduced'
 assert result['machine'] == 'x86_64'
 checks = {}
 for profile in ('default', 'all-features'):
  section = result['profiles'][profile]
  comp = section['compile']
  log = z.read(comp['log']).decode()
  assert comp['exit_code'] == 0 and comp['timed_out'] is False and comp['disk_stop'] is False
  assert 'Finished `test` profile' in log and 'Executable unittests src/lib.rs' in log
  c = comp['cleanup_receipt']; assert c['leader_exit'] == 0 and c['leader_waited'] is True and c['remaining_descendants'] == []
  tests = {}
  assert set(section['tests']) == set(TESTS)
  for name, message in TESTS.items():
   t = section['tests'][name]; log = z.read(t['log']).decode(); full = 'producer::tests::' + name
   assert t['classification'] == 'expected-assertion' and t['expected_assertion'] == message
   assert t['exit_code'] == 101 and t['timed_out'] is False and t['disk_stop'] is False
   assert '--exact' in t['command'] and full in t['command'] and ('--all-features' in t['command']) == (profile == 'all-features')
   assert re.search(r'(?m)^running 1 test$', log) and re.search(r'(?m)^test '+re.escape(full)+r' \.\.\. FAILED$', log)
   assert re.search(r'(?m)^test result: FAILED\. 0 passed; 1 failed; 0 ignored;', log)
   assert message in log and 'exit_code: 101\ntimed_out: false\ndisk_stop: false' in log
   c=t['cleanup_receipt']; assert c['leader_exit'] == 101 and c['leader_waited'] is True and c['remaining_descendants'] == []
   tests[name] = {'assertion': message, 'exit_code': 101, 'log': t['log'], 'log_sha256': sha(z.read(t['log'])), 'cleanup_waited_no_remaining_descendants': True}
  checks[profile]={'compile_exit_code':0, 'compile_log':comp['log'], 'compile_log_sha256':sha(z.read(comp['log'])), 'compile_cleanup_waited_no_remaining_descendants':True, 'tests':tests}
 for logname in ('apply-check.log', 'apply.log'):
  log=z.read(logname).decode(); assert 'exit_code: 0\ntimed_out: false\ndisk_stop: false' in log
 samples=[json.loads(s) for s in z.read('disk.jsonl').splitlines()]
 minimum=min(v for s in samples for v in s['available_bytes'].values())
 assert minimum >= result['disk_stop_bytes']
 assert 'run_id=41674604362' in z.read('runner.txt').decode() and ('trigger_sha='+TRIGGER) in z.read('runner.txt').decode()
 assert 'classifier self-test passed (7 cases)' in z.read('qualification.log').decode()
 summary={'run_id':41674604362,'job_id':126051745072,'run_attempt':1,'run_conclusion':'success',
  'trigger_sha':TRIGGER,'tested_source_sha':BASE,'github_artifact_id':13249627527,'archive_sha256':ARCHIVE,
  'patch_sha256':PATCH,'patched_source_sha256':POST,'rustc':result['rustc'],'cargo':result['cargo'],
  'verdict':result['verdict'],'profiles':checks,'sampled_disk_minimum_bytes':minimum,'disk_samples':len(samples),
  'cleanup_scope':'10 retained compile/test receipts only; other command cleanup receipts were not retained',
  'limitations':['expected failures on patched 45cb only; not a fix', 'local socket and injected worker fixtures; no broker interoperability or throughput measurement', 'no independent execution in this review']}
 print(json.dumps(summary,sort_keys=True,indent=2))

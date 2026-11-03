#!/usr/bin/env python3
"""Bind connector-observed job result to pinned runner source."""
import hashlib,json,re
from pathlib import Path
w=Path('/workspace/work/broker-join-r2-job-125798406078-evidence')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
log=(w/'connector-job.log').read_text();meta=json.loads((w/'connector-metadata.json').read_text());source=json.loads((w/'source.json').read_text());spec=json.loads((w/'source/spec.json').read_text())
assert meta['job']['id']==125798406078 and meta['job']['conclusion']=='success'
assert meta['run']['head_sha']==source['commit']=='7e91e2bee412857c728ecd5c7a368fdefacf7af7'
assert sha(w/'connector-job.log')=='a1171ea7f960072c70d9b5db163b74cb602df5684f77b93df0f42dee540363b5'
for name,item in source['files'].items(): assert sha(w/'source'/name)==item['sha256']
assert sha(w/'source/qualify.py')=='5921944995f2da7d40772562ea43e3c853703eba6e9c317f0e3c9611124ad50f'
assert sha(w/'source/legacy-driver.py')==spec['legacy_driver']['sha256']
assert sha(w/'source/spec.json')=='ab738fcaa40fc9f565c105513d512553105bfde778a1769bfcaf6c239a80eb0e'
assert source['v17_registry_pin']=='511b2e432d609381b7878704aa1036c2db506b2244f6ffc28bcd0be169541dc8'
assert source['candidate_fixed_registry_sha256']=='e127e3daeca095363f66ed45d7a2ffa1adbef17ff584d3f23dcfde63fb576e0a'
lines=log.splitlines();summaries=[]
for line in lines:
 if '{"passed": true, "error": null, "gate_errors": []' in line:
  summaries.append(json.loads(line[line.index('{'):]))
assert len(summaries)==1 and summaries[0]['passed'] and summaries[0]['error'] is None and summaries[0]['gate_errors']==[]
assert 'Ran 12 tests in ' in log
assert len([l for l in lines if 'verified_join_static_controls.Controls.test_' in l and l.endswith(' ... ok')])==12
assert not any('gate_failure' in l for l in lines)
art={a['id']:a for a in meta['artifacts']}
assert art[13214552345]['digest']=='sha256:27ed120e7f86dd4a2575a08371c95b6cc09565da894eaa9ee8758e2020606a45'
assert art[13214552345]['size_in_bytes']==464257
result={'status':'hosted broker controller-join r2 job passed',
 'job_id':125798406078,'run_id':41600679287,'trigger_sha':source['commit'],
 'decoded_job_log_sha256':sha(w/'connector-job.log'),'connector_metadata_sha256':sha(w/'connector-metadata.json'),
 'source_catalog_sha256':sha(w/'source.json'),'direct_console':{'static_tests_passed':12,'summary':summaries[0],
 'stable_rustc':'1.99.1 (58cf8f9c3 2026-10-13)','job_conclusion':'success'},
 'source_bound_control_flow_inference':[
 'The capacity baseline positive witnesses and required negative assertion matched.',
 'In both feature profiles the join baseline positive case passed and each of four negative cases matched its pinned panic sites.',
 'In both profiles the required fixed capacity and five join cases, inherited focused and async filesystem controls, all-target minimum and Fetch repeats passed; the stale-leader check passed.',
 'Formatting, strict Clippy and Rustdoc, both doctest profiles, the broker inventory, and its 55-control Python suite passed the pinned runner conditions.'
 ],
 'source_scope':{'base_commit':spec['base_commit'],'base_archive_sha256':spec['base_source']['sha256'],
 'fixed_runtime_sha256':spec['fixed_runtime']['sha256'],'fixed_join_sha256':spec['join_fixed_source']['sha256'],
 'fixed_registry_sha256':spec['join_registry_fixed_source']['sha256'],'fixed_registry_patch_sha256':spec['join_registry_patch']['sha256'],
 'v17_base_registry_sha256':source['v17_registry_pin']},
 'compact_report_artifact':art[13214552345],
 'artifact_bytes_downloaded':False,
 'limits':['The job log is connector-decoded text re-encoded as UTF-8, not original compressed GitHub log bytes.',
 'The compact report connector returned a File Service reference without a local path; the enforced network allowlist excludes that host, and the available workspace downloader is Sediment-only. No unsupported download or network bypass was attempted.',
 'Individual command logs, exact test counts, exit codes and per-command OOM/resource receipts were not read; the detailed gate descriptions are source-bound control-flow inferences from the summary, not separately observed command results.',
 'The candidate uses the pinned 6fa8 source archive with reviewed capacity, join and registry patches; this is not a whole-current-main qualification. Synthetic actor tests do not independently cover real controller wake, WAL or transport.',
 'Adopting the registry update on current main would invalidate v17 current-head base reconstruction unless its checkout strategy is separately reviewed.',
 'The job result does not establish complete production readiness, Kafka compatibility, or a global performance ranking.']}
(w/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
print(sha(w/'analysis.json'))

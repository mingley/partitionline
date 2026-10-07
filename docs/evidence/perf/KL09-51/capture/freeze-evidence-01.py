#!/usr/bin/env python3
"""Seal the completed acquisition lookup evidence once."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

r=Path('/workspace/partitionline')
b=Path('/workspace/work/open-cards-20261006/share-ranges-20261007')
q=r/'docs/evidence/perf/KL09-51'
if q.exists():raise ValueError('fresh evidence directory required')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
analysis=json.loads((b/'paired-analysis-02.json').read_text());assert analysis['performance_acceptance']
assert json.loads((b/'checks-final-01/completion.json').read_text())['passed']
for cohort in ('baseline-01','paired-01','paired-02'):
 d=b/cohort;completion=json.loads((d/'completion.json').read_text())
 assert completion['source_guards_passed'] and completion['owned_process_groups_empty']
 for path in d.rglob('*.process.json'):
  receipt=json.loads(path.read_text());assert receipt['parent_waited'] and receipt['exit_code']==0 and not receipt.get('failure')
  for name,digest in receipt['artifacts'].items():assert sha(path.parent/name)==digest
  resource=path.parent/'resources.json'
  if resource.exists():assert json.loads(resource.read_text())['parent_waited']
candidate=Path('/workspace/work/open-cards-20261006/share-ranges-source-candidate')
assert subprocess.check_output(['git','-C',str(candidate),'rev-parse','HEAD'],text=True).strip()=='b829dc627446ea6ad92042d319fc7ad04c3761bb'
assert not subprocess.check_output(['git','-C',str(candidate),'status','--porcelain'])
for relative in ('src/share.rs','src/share/acquired.rs','benchmarks/codec/src/share-ranges.rs','benchmarks/codec/Cargo.toml'):
 assert sha(candidate/relative)==sha(r/relative)
q.mkdir(parents=True);capture=q/'capture';capture.mkdir()
for path in sorted(b.iterdir()):
 if path.is_dir():shutil.copytree(path,capture/path.name)
 elif not path.name.endswith(('.index','.paths')):shutil.copy2(path,capture/path.name)
source=q/'source';source.mkdir()
pins=json.loads((b/'parent-source-pins.json').read_text())
for label,path in [('parent',Path('/workspace/work/open-cards-20261006/share-ranges-source-parent')),('candidate',candidate)]:
 for relative in pins:
  target=source/label/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path/relative,target)
fixtures=q/'existing-zstd-fixtures';fixtures.mkdir()
for path in (candidate/'tests/fixtures/zstd-decode').iterdir():
 if path.is_file():shutil.copy2(path,fixtures/path.name)
summary=dict(task='KL09-51',status='done',disposition='accepted',
 baseline_commit='de7c68d30c97373656eed0afa287ed39ef84d4f6',
 first_candidate_commit='56525ec1e70a527bf78e8471c669b2e09e620310',
 qualified_candidate_commit='b829dc627446ea6ad92042d319fc7ad04c3761bb',
 toolchain='rustc1.99.0 (b940084d7 2026-09-28), latest stable',
 cell='micro-share-ranges',fixture=dict(ranges=1000,offsets_per_iteration=5000,acquired_offsets=3000,gaps=2000,excluded_warmup_iterations=100,timed_iterations=10000),
 primary='ns per offset lookup',baseline_repetitions=5,paired_repetitions_per_cohort=7,paired_cohorts=2,
 final_comparison=analysis,binary_sha256={str(p.relative_to(q)):sha(p) for p in capture.rglob('*') if p.is_file() and p.parent.name=='bin'},
 tests=dict(share_passed=103,full_surface_passed=330,shared_benchmark_tests_passed=2,zstd_fixture_fuzz_passed=9,zstd_decode_passed=4,zstd_encode_passed=7,zstd_encode_existing_ignored_native_case=1),
 result_validator=dict(positive_passed=True,actual_changed_result_controls_rejected=16),
 strict_core_all_target_all_feature_clippy=True,strict_benchmark_all_target_all_feature_clippy=True,core_and_benchmark_fmt=True,
 source_guards_passed=True,owned_process_groups_empty=True,
 behavior='Range validation precedes lookup. Repeated, descending and shuffled offsets match a linear oracle. Gaps remain undelivered; delivery counts and integer limits are preserved. Cursor steps are capped at4 before binary fallback.',
 complexity=dict(net_added_production_lines=69,includes_baseline_helper_extraction=True),
 first_failure='Clean-checkout core Clippy exposed two narrowing casts in new tests and an existing untracked zstd compile-time fixture. Checked conversions and the29 manifest-matched existing fixture files fix that gate. The initial failed log and exact initial/final source are retained.',
 limits=analysis['limits']+['The existing native-only zstd test remains ignored in this finite local check; its previous qualification is not rerun or counted as passed.',
 'No Kafka wire format, range validation, delivery semantics, public API, runtime dependency, default or allocation budget changed.'])
(q/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(q/'README.md').write_text('''# Share acquisition lookup (KL09-51)

The ShareFetch lookup now reuses a range cursor for ordered offsets. It
advances at most four ranges before binary search and uses a full search
when offsets move backward. Range validation, gaps and delivery counts
are unchanged.

The benchmark was committed first at `de7c68d3`. Its production helper
already used binary search; this comparison does not substitute an older
linear-scan baseline. The cursor was committed separately at `56525ec1`.
The final qualified source is `b829dc62`.

Five baseline runs preceded the change. Two cohorts of seven interleaved
A/B pairs compare the same 1,000 short ranges and 5,000 offsets, including
2,000 gaps. Each run excludes 100 warmup iterations and times 10,000
iterations. The final median fell from 11.89 to 3.98 ns per lookup
(66.5%). The paired time-ratio 95% CI is [0.319, 0.347], from 20,000
resamples of complete pairs with seed951. Both lookup allocation censuses
are zero. These results describe the lookup microbenchmark.

The 103 selected share tests and330 full-surface tests passed. The same
helper's two tests also pass in the benchmark crate. Strict core and
benchmark Clippy and formatting passed. Repeated, reverse, shuffled,
empty and integer-limit cases check counts against a linear oracle.
Sixteen changed captured results were rejected by the result validator.

A clean-checkout Clippy run found narrowing casts in the new tests and
a missing preexisting zstd fixture. The corrected tests and29 existing
manifest-matched fixtures are checked in. The fixture suites passed
20 tests; their existing native-only ignored case is recorded separately.
The failed lint log remains in the archive.

`capture/capture.py` records guarded source/ELF inputs, parent-bound
processes, successful waits, raw results and whole-child resources.
`capture/analyze.py` replays the actual results and paired bootstrap.
`capture/validate-result.py` checks fixture facts, clocks, checksums and
allocation counts. `source/` retains both helper/benchmark source trees.
Commands and process receipts retain their original absolute workspace
paths. `SHA256SUMS` covers every other archive file.

All measurements are local and unsigned. Whole-child CPU/RSS includes
setup, oracle and warmup; it is not a measured-phase guardrail. No complete
ShareFetch throughput improvement, production qualification or leadership
result is inferred. No allocation budget was raised.
''')
files=sorted(p for p in q.rglob('*') if p.is_file())
(q/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(q))+'\n' for p in files))
print(json.dumps(dict(files=len(files)+1,bytes=sum(p.stat().st_size for p in q.rglob('*') if p.is_file()),summary=str(q/'summary.json'))))

#!/usr/bin/env python3
"""Retain the initial partial profiles without completing the ranking card."""
import hashlib
import json
from pathlib import Path
import shutil

r=Path('/workspace/partitionline');b=Path('/workspace/work/open-cards-20261006/baseline-profile-20261007')
q=r/'docs/evidence/perf/baseline-profile/initial-20261007'
if q.exists():raise ValueError('fresh initial archive required')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for cohort in ('nb-bulk-cpu-04','nb-bulk-cpu-05','nb-bulk-syscalls-01'):
 d=b/cohort;v=json.loads((d/'completion.json').read_text())
 assert v['source_guards_passed'] and v['owned_process_groups_empty']
 for path in d.glob('*-tree.json'):
  v=json.loads(path.read_text());assert v['parent_waited'] and v['tool_exit_code']==0 and v['children_empty'] and not v['failure']
q.mkdir(parents=True);shutil.copytree(b,q/'capture')
tools=q/'tools';tools.mkdir()
shutil.copytree(Path('/workspace/work/open-cards-20261006/local-baseline-20261007/perf-tools'),tools/'perf-tools',symlinks=False)
shutil.copy2(Path('/workspace/work/open-cards-20261006/local-baseline-20261007/parent-bound-exec.py'),tools/'parent-bound-exec.py')
shutil.copy2(Path('/usr/lib/x86_64-linux-gnu/libc.so.6'),tools/'libc-observed-after-captures.so')
cpu=[json.loads((b/name).read_text()) for name in ('nb-bulk-cpu-analysis-01.json','nb-bulk-cpu-analysis-02.json')]
calls=json.loads((b/'nb-bulk-syscall-analysis-01.json').read_text())
summary=dict(task='KL09-13',status='in_progress',scope='Initial partial local CPU/syscall diagnostics; no complete hypothesis ranking',
 source_commit='c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25',cpu_event='cpu-clock:u',frequency_hz=997,
 cpu_fresh_captures=2,repetitions_per_cpu_capture=25,
 completed_analysis_sources=['nb-bulk-cpu-04 (replay of the original01 raw capture)','nb-bulk-cpu-05 (fresh capture)'],
 measured_client_samples=[x['measured_runtime_samples'] for x in cpu],
 unresolved_leaf_share_percent=[x['unresolved_leaf_percent'] for x in cpu],
 excluded_runtime_setup_samples=[x['excluded_setup_fixture_shutdown_samples'] for x in cpu],
 syscall_repetitions=5,measured_client_syscall_entries=sum(p['entry_count'] for p in calls['phases']),
 syscall_phase_entry_counts=[p['entry_count'] for p in calls['phases']],
 completed_capture_guards_and_owned_process_groups=True,
 limitations=['Only nb-produce-bulk is profiled here; other required cells and every section3 hypothesis are not ranked.',
 'About43–45 percent of measured client leaf samples are unresolved in libc. The available debug package build ID differs; it was not used.',
 'Software userspace CPU samples do not measure hardware cycles, kernel CPU or off-CPU time. Hardware events are unavailable on this host.',
 'Inlining merges work into enclosing symbols. Missing attribution is not a measured zero or a below-one-percent rejection.',
 'Inclusive call-chain shares overlap and cannot be added. About500 measured samples per capture do not support precise fine-grained rankings.',
 'Syscall elapsed time includes blocking and ptrace overhead. Count share is not CPU cost. Raw arguments contain no decoded strings or payloads.',
 'The observed libc hash/build ID was recorded after capture, not independently attested for every loaded process.',
 'No complete ShareFetch/TLS/OIDC/native-broker profile or allocation call-site attribution is supplied.',
 'The standalone tree-control03 passed. A prior control timed out before a receipt; it is unqualified. Control02 exposed a missing proc children file and was cleaned up by the outer owner.',
 'Capture01 raw recording completed, but the first report left a resolver child; the owner terminated/reaped it. Replay04 uses the same raw capture and is not a third fresh sample.',
 'Capture02 failed its clean-source guard after a control created bytecode. Capture03 failed on an existing validator filename. Both failures and the bytecode repair are retained.'],
 new_performance_claims=False,production_qualification=False,suite_hold='active')
(q/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(q/'README.md').write_text('''# Initial baseline profiles

Two fresh `nb-produce-bulk` captures each run25 repetitions of the pinned
`c4b9157` baseline under997 Hz userspace CPU sampling. Actual result timestamps
exclude fixture construction, setup and shutdown from the client analysis.
The first raw capture was replayed after a resolver cleanup failure; that
replay is not another fresh capture.

The analyses retain511 and493 measured client samples. Unresolved libc leaf
samples account for42.86% and45.23%. The available debug package has a different
build ID and was not used. Both profiles identify clock reads, frees, producer
admission and task wakeups among the resolved costs. Inlining and the unresolved
samples prevent a complete ranking or a below-one-percent conclusion.

A separate five-repetition raw syscall trace isolates the actual runtime
main PID and measured phase intervals. It counts1378 entries, excluding
2760 client startup/shutdown entries and all descendant lines. Elapsed syscall
time includes blocking and observation overhead. Raw hexadecimal arguments
exclude strings, packet payloads, I/O-vector contents and TLS record sizes.

`capture/` retains commands, phase results, raw profiles, observers, analyses,
source/ELF pins, process receipts and failed attempts. Executed wrappers from
different attempts are preserved separately. `tools/` retains the perf
executable and libraries, parent-binding helper and the libc object inspected
afterward. Source111-file pins refer to the unchanged sealed baseline source.
Paths in receipts retain their original workspace locations.

This is partial evidence for KL09-13. Other cells and allocation call-site
shares remain open. No full ranking, production readiness or performance
leadership result is established. `SHA256SUMS` covers every other archive file.
''')
files=sorted(p for p in q.rglob('*') if p.is_file())
(q/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(q))+'\n' for p in files))
print(json.dumps(dict(files=len(files)+1,bytes=sum(p.stat().st_size for p in q.rglob('*') if p.is_file()))))

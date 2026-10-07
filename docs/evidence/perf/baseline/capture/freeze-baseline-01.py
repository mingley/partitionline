#!/usr/bin/env python3
"""Retain completed baseline cohorts and record their measured scope."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

r=Path('/workspace/partitionline')
b=Path('/workspace/work/open-cards-20261006/local-baseline-20261007')
q=r/'docs/evidence/perf/baseline'
assert not q.exists()
read=lambda p:json.loads(p.read_text())
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
replay=read(b/'replay-verdict-01.json');assert replay['status']=='pass' and replay['canonical_nb_validators']==95
aggregate=read(b/'aggregate-01/completion.json');assert aggregate['reproducibility_qualified'] and aggregate['metric_rows']==1385
source=Path('/workspace/work/open-cards-20261006/local-baseline-source-c4b915')
assert subprocess.check_output(['git','-C',str(source),'status','--porcelain'])==b''
assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()=='c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25'
for relative,digest in read(b/'source-bindings-extended-01.json').items():assert h(source/relative)==digest,relative
for cohort in ('nb-01','nb-reproduce-01','codec-02','iai-03','native-02','request-03','latency-02'):
    done=read(b/cohort/'completion.json');assert done['source_guards_passed'] and done['owned_process_groups_empty']
q.mkdir(parents=True)
manifest=[]
# Raw data and actual executed ELFs are retained unchanged; broker log directories
# are omitted after each topic was independently verified and deleted.
for cohort in ('codec-01','codec-02','nb-01','nb-reproduce-01','iai-01','iai-02','iai-03',
               'native-01','native-02','request-01','request-02','request-03','latency-01','latency-02','aggregate-01'):
    for p in sorted((b/cohort).rglob('*')):
        if not p.is_file() or 'data' in p.relative_to(b/cohort).parts:continue
        target=q/'capture'/p.relative_to(b);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
        assert h(p)==h(target)
        manifest.append(dict(original_path=str(p),retained_path=str(target.relative_to(q)),sha256=h(target),size=target.stat().st_size))
for p in sorted(b.iterdir()):
    if not p.is_file() or p.suffix=='.deb':continue
    # Signed apt acquisition logs, controls, source bindings and tool controllers.
    target=q/'capture'/p.name;shutil.copy2(p,target);assert h(p)==h(target)
for p in sorted((b/'fetch-verifier-src').rglob('*')):
    if p.is_file():
        target=q/'capture/fetch-verifier-src'/p.relative_to(b/'fetch-verifier-src');target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
for relative in read(b/'source-bindings-extended-01.json'):
    target=q/'source'/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source/relative,target)
(q/'retained-paths.json').write_text(json.dumps(manifest,indent=2)+'\n')
profile=read(b/'availability-before-final-latency.json')
receipt=dict(schema_version=1,task='KL09-12',status='done',acceptance_met=True,date='2026-10-07',
    source_commit='c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25',source_tree_clean=True,
    scope='Pinned local unsigned baseline capture, including unsuccessful load profiles. No comparative performance or production qualification.',
    toolchain='latest stable Rust 1.99.0; Java21',suite_hold='active',
    host='AMD EPYC9V74 KVM guest; five exposed CPUs; uncontrolled guest frequency',
    ordinary_and_json1k_timing_cases=50,zstd_timing_cases=72,valid_repetitions_each=5,
    native_samples_each_timing_repetition=30,instruction_cases=34,instruction_repetitions_each=5,
    instruction_scope='Callgrind simulated user instructions; hardware cycles/instructions unavailable',
    null_broker_cells=18,null_broker_repetitions_each=5,additional_null_broker_reproduce_repetitions=5,
    native_bulk_fetch_repetitions=10,native_records_per_measured_phase=8_000_000,
    native_warmup_records_each=10_000,rust_native_measured_full_byte_reads=80_000_000,
    java_native_full_byte_reads_including_warmup=80_100_000,
    micro_request_repetitions=5,genuine_sdk_request_readbacks=15,genuine_request_corruption_controls=6,
    latency_calibrations=5,latency_load_percentages=[10,50,80],frozen_latency_rates=[1000,5000,8000],
    latency_repetitions_each_rate=5,latency_offers_each=20_000,latency_warmup_each=10_000,
    failed_80_percent_profiles=5,failed_80_percent_rejections=[192,1975,1322,2940,3380],
    latency_integrity='Full fixed x payload and contiguous offsets verified by genuine Java; no unique-ID delivery-history qualification',
    qualified_metric_rows=1385,statistics='Median across five independent repetitions; bootstrap95%CI;20000 resamples;seed912',
    reproducibility=read(b/'aggregate-01/reproducibility.json'),
    process_receipts=replay['process_receipts'],retained_nonzero_receipts=replay['retained_nonzero_receipts'],
    canonical_benchmark_report_validations=95,harness_replay='capture/verify-baseline-01.py',
    replay_corruption_controls=26,unavailable_cells=profile['not_run'],metric_limits=profile['metric_limits'],
    preserved_failures=[
        'Default apt snapshot returned403; signed current Debian index acquisition and actual tool extraction then succeeded.',
        'Iai initial launcher used wrong cargo-metadata working directory; second launch lost VALGRIND_LIB through env_clear. Both failed attempts and exact helpers retained.',
        'Codec recorder was interrupted by an environment restart in repetition4. That process was not waited by its owner; all partial samples excluded. Three complete earlier repetitions plus two fresh repetitions form the qualified cohort.',
        'First native producer launcher required missing /usr/bin/time. Owned broker closed; actual Python wait4 and parent-death controls then passed.',
        'Request SDK4.3 moved MemoryRecords into record.internal; the exact failed source/compiler receipt is retained. Corrected per-SDK import rendering keeps genuine SDK types.',
        'First valid-CRC request corruption changed a structural byte; it was rejected for an unexpected reason. Failed source/control retained; corrected payload-byte mutation was independently rejected by all three SDKs.',
        'First10000-offer80%latency run acknowledged8837/rejected1163, below the sample floor. Original failure remains. New20000-offer windows keep the same rates,1024pending cap and10000sample floor; all five80%profiles still contain rejections.',
        'Optional miniz default allocation-budget mismatch remains unqualified under KL09-36; no backend budget raised or waived.'
    ],production_ready=False,performance_claims_valid=False,core_protocol_complete=False,full_protocol_complete=False,
    evidence_directory=str(q.relative_to(r)),large_raw_archives='Retained in this checkout; source/summary publication may omit large capture files with an explicit publication manifest.')
(q/'summary.json').write_text(json.dumps(receipt,indent=2)+'\n')
(r/'docs/plan/evidence/KL09-12.json').write_text(json.dumps(receipt,indent=2)+'\n')
(q/'README.md').write_text('''# Local baseline

This captures the client at `c4b915757b47fd2d6e2ab11c85a9db93c2e8ed25` on
an AMD EPYC KVM guest using stable Rust 1.99.0. Guest frequency was uncontrolled.
All results are local and unsigned. Suite HOLD remains active.

Five independent repetitions cover 122 codec/zstd timing cases, 34 simulated
instruction cases, 18 null-broker cells, request encoding, and latency at three
loads. Ten native broker runs each acknowledge eight million measured records;
Rust checks every measured record and Java checks all records including warmup.
Both throughput reruns meet the declared 20% median-difference limit: 0.6% for
the null broker and 1.8% for the real broker. These are repeatability checks.

The five 80% latency profiles all reject some offers at the unchanged 1,024-task
limit. Their raw timestamps, failed exit statuses, outcomes and Java readbacks
are retained. The longer 20,000-offer windows exceed the unchanged 10,000-sample
latency floor. The earlier shorter run remains a failed attempt. Arrival rates
are 1,000, 5,000 and 8,000 per second, fixed from the original five sequential
capacity measurements. They were not recalibrated after the failure.

`capture/aggregate-01/statistics.json` contains medians and bootstrap 95% confidence
intervals for 1,385 actual metric rows. `summary.json` lists missing cells and
measurement limits. Missing metrics are not zero-filled. The producer resource
wrapper measures its whole process; fetch timing includes full-byte verification.
Latency uses a fixed payload without unique IDs. Callgrind instructions are
simulated counts, not native cycles. Optional miniz's allocation failure remains
unqualified; the default budget is unchanged.

The canonical benchmark reporter validated all 95 complete null-broker result
artifacts. Codec, instruction, request, native readback and latency outputs are
harness data rather than complete benchmark-contract results; their replay
validator checked raw sample hashes, counts, fences, outcomes and percentiles.
It rejected 26 changed latency histories. Three genuine SDKs each rejected bad
CRC and valid-CRC/wrong-payload request bodies. These checks do not establish a
production profile or a fastest-client/server claim.

`capture/` retains original data, actual executed binaries, source-bound command
receipts, tool controllers, setup failures and the interrupted codec run. The
qualified codec mapping excludes that unwaited partial run. `source/` retains
111 bound source files; `retained-paths.json` maps original paths to exact copies.
Native topic segments were deleted only after full readback and fence checks.
Every completed owned broker was waited, its supervisor joined and its ports
rebound. The interrupted codec process is explicitly excluded from that claim.

Reproduce using `scripts/record-local-baseline.py --help` and the captured command
receipts. The external tools are evidence sources with paths to the original
checkout; adjust their path dependencies when building in a new checkout.
''')
paths=sorted(p for p in q.rglob('*') if p.is_file())
(q/'SHA256SUMS').write_text(''.join(h(p)+'  '+str(p.relative_to(q))+'\n' for p in paths))
print(json.dumps(dict(files=len(paths)+1,bytes=sum(p.stat().st_size for p in paths),task=receipt['task'],status='retained',scope='local/unsigned')))

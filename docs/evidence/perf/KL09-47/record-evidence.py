import hashlib, json, pathlib, shutil, subprocess

root=pathlib.Path('/workspace/work/consumer-aborts');repo=pathlib.Path('/workspace/partitionline');out=repo/'docs/evidence/perf/KL09-47'
A='824662e39980604c799e3398a7e3bc46db8400b3';B='96986ba02ba5c7c480295c855a9e0dfe9efab549';oldB='c0cc3cde39a156350502dc65890fc66dd5dd8b60'
metrics=json.loads((out/'revised-metrics.json').read_text());old=json.loads((out/'metrics.json').read_text());validation=json.loads((out/'artifact-validation.json').read_text())
cell_metrics={c:s['metrics'] for c,s in metrics['cells'].items()}
named=cell_metrics['nb-fetch-committed-aborts']
patch=subprocess.check_output(['git','diff',A,B,'--','src/consumer.rs','tests/consumer_fetch_semantics.rs'],cwd=repo,text=True)
(out/'consumer-and-tests.patch').write_text(patch)
git_value=lambda expression:subprocess.check_output(['git','rev-parse',expression],cwd=repo,text=True).strip()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
select=lambda m:{k:m[k] for k in ['cpu_ns_per_record','allocations_per_record','allocated_bytes_per_record','process_peak_rss_bytes','external_client_vmhwm_bytes','rec_s','phase_sampled_peak_rss_bytes']}
guardrails={c:select(m) for c,m in cell_metrics.items()}
for c,m in cell_metrics.items():
    assert m['cpu_ns_per_record']['delta_pct']<=2
    assert m['allocations_per_record']['delta_pct']<=2
    assert m['process_peak_rss_bytes']['delta_pct']<=2
    assert m['external_client_vmhwm_bytes']['delta_pct']<=2
    assert m['rec_s']['delta_pct']>=-2
assert named['allocations_per_record']['delta_pct']<=-5
assert validation['runtime_artifacts']==110 and validation['valid']==110 and validation['failed']==0
primary=dict(named['cpu_ns_per_record'],metric='CPU nanoseconds per actual verified record')
evidence={
 'id':'KL09-47','status':'done','source_sha':B,'baseline_sha':A,'disposition':'accepted',
 'hypothesis':'Per-record read_committed filtering clones topic keys and hashes the partition-state maps repeatedly. One partition-local aborted-interval heap and active/completed PID state, held by a borrowed topic lookup through each response, permits incremental activation and a cached per-data-batch aborted offset. This removes per-record partition-key allocation/hash work and repeated pending-interval sorting.',
 'cells':['nb-fetch-committed-aborts','nb-fetch-bulk','nb-fetch-1000p'],
 'host':'AMD EPYC 9V74 80-Core Processor; x86_64 Linux 6.18.44; affinity exposes CPUs0–4, cgroup cpu.max=400000 100000; runtime client, C launcher and inherited broker pinned CPU3, external observer CPU2, builds/tests CPUs0–1. Host frequency unmanaged, cpufreq governor/driver unavailable. rustc1.99.0(b940084d7 2026-09-28), cargo1.99.0. Optimized release with CARGO_PROFILE_RELEASE_DEBUG=true for attribution; only debug sections stripped from frozen A/B binaries with objcopy and separate GNU debuglink sidecars, code unchanged. CARGO_INCREMENTAL=0, CARGO_BUILD_JOBS=1. Other client builds paused for the fresh accepted cohort; shared host/frequency uncertainty remains.',
 'tested_source':{
   'initial_baseline_sha':'0c2338e4d6a61d8e4b273a17a55e618583632041','corrected_baseline_tree':git_value(A+'^{tree}'),'candidate_tree':git_value(B+'^{tree}'),
   'consumer_blob':git_value(B+':src/consumer.rs'),'regression_test_blob':git_value(B+':tests/consumer_fetch_semantics.rs'),'consumer_sha256':sha(root/'candidate/src/consumer.rs'),
   'relation':'Clean immutable exact pushed SHAs for measurements. Among src/, runtime, codec and nullbroker inputs, only src/consumer.rs differs between corrected baseline and accepted candidate; the owned regression is also committed. Independent broker-test/docs commits are outside runtime inputs. Later lint-only main changes are excluded. Focused tests ran on c0cc plus exactly the frozen four-line guard revision, whose consumer/regression/full_surface/common bytes match96986ba; final release build ran clean96986ba after source push.',
   'frozen_runtime_sha256':{'baseline':sha(root/'corrected-baseline-bin/runtime'),'rejected_candidate':sha(root/'candidate-bin/runtime'),'accepted_candidate':sha(root/'revised-candidate-bin/runtime')},
   'frozen_broker_sha256':{'baseline':sha(root/'corrected-baseline-bin/nb-serve'),'accepted_candidate':sha(root/'revised-candidate-bin/nb-serve')},
   'patch_sha256':hashlib.sha256(patch.encode()).hexdigest(),
   'build_note':'After importing first candidate source SHA, a sequential exact-source rebuild confirmed the frozen executable; earlier concurrent checkout/build output was not measured. Original wrong package-selector rebuild exited101 and is retained. All accepted cohort binaries were frozen before execution.'
 },
 'harness_correction':{
   'claim_commit':'fcd9ba0fcccd4f6dcfff9a950ec44457e65312ce','harness_only_baseline_commit':A,
   'problem':'Original driver verified only a20k target prefix although one Fetch returned108k records. A corrupted or exposed aborted record in the unverified tail could pass. Earlier prefix and two mistaken history-shape fixtures are preserved and excluded from qualifying comparisons.',
   'fix':'Keep frozen target20k and synthesis settings unchanged, but validate offsets/ID/hash/key/order for EVERY returned record and complete per-partition histories through all observed cursors. Fail if returned!=verified, target unmet, cursor-derived complete count differs, or any aborted/out-of-order/gap record appears. Report actual offered/consumed/verified108k, nominal target20k separately; CPU/allocation denominators use actual verified108k on both corrected SHAs. Proof vectors and latency capacity are reserved outside the allocation census.',
   'settings':{'partitions':6,'records_per_partition':50000,'records_per_batch':500,'payload_bytes':100,'abort_every':5,'target_records':20000,'isolation':'read_committed','seed':4269539331,'config_sha256':'fa4ef127133a1b4dfbaf190fccd16747da0d0df16b36d6cf137e9ef713b0469e'},
   'proof_each_named_run':{'target_records':20000,'fetched_records':134500,'returned_records':108000,'verified_records':108000,'filtered_records':26500,'committed_abort_gap_records':25500,'aborted_deliveries':0,'exact_history_ranges':57,'verified_per_partition':[40000,40000,26500,500,500,500],'partition_cursors':[[0,50000],[1,50000],[2,33000],[3,500],[4,500],[5,500]],'fetch_requests':1,'metadata_aborted_intervals':53,'decoded_batches':269},
   'history':'For p0,p1:20 ranges[2500*g,2500*g+2000),g0..19; p2:13 such ranges,g0..12, then[32500,33000); p3,p4,p5 each[0,500). Every delivered ID/hash/key and offset is checked; cursor-derived complete counts match134500 fetched offsets. The broker admits a first500-record batch on later partitions after the aggregate byte limit; this accounts for p3–5 without changing frozen knobs.',
   'harness_e2e':'Complete-history harness against original consumer:1passed0failed0ignored, separately pushed before attribution. A later invalid prefix-only proof is explicitly rejected despite its test passing; logs retained, helper removed its successful temporary raw output.'
 },
 'baseline_attribution':json.loads((out/'attribution.json').read_text()),
 'commands':[
   'export CARGO_HOME=/workspace/work/cargo RUSTUP_HOME=/workspace/work/rustup PATH=/workspace/work/cargo/bin:$PATH CARGO_TARGET_DIR=/workspace/work/consumer-aborts/target CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1',
   'taskset -c 0,1 cargo build --offline --locked --release --manifest-path benchmarks/runtime/Cargo.toml --bins (initial0c snapshot; exit0)',
   'taskset -c 3 /workspace/work/consumer-aborts/baseline-bin/runtime --cell nb-fetch-committed-aborts --out /workspace/work/consumer-aborts/initial-baseline-correct-provenance --repetitions 1 (exit0; excluded original prefix workload)',
   'taskset -c 0,1 cargo test --offline --locked --test consumer_fetch_semantics read_committed_repeated_metadata_and_spanning_markers_survive_seek -- --test-threads=1 (initial consumer plus new invariant; final1pass exit0, two development failures retained)',
   'taskset -c 0,1 cargo test --release --offline --locked --manifest-path benchmarks/runtime/Cargo.toml --test e2e committed_aborts_cell_verifies_exact_offset_history -- --test-threads=1 (harness-only original consumer; final1pass exit0, both earlier history-shape failures retained)',
   'CARGO_PROFILE_RELEASE_DEBUG=true taskset -c 0,1 cargo build --offline --locked --release --manifest-path benchmarks/runtime/Cargo.toml --bins (clean824 baseline, cleanc0 candidate, clean969 revised candidate; each exit0)',
   'objcopy --only-keep-debug runtime runtime.debug; objcopy --strip-debug runtime; objcopy --add-gnu-debuglink=runtime.debug runtime; objcopy --strip-debug nb-serve (each frozen A/B directory; exit0, separate debug symbols preserve attribution while avoiding112MB binary-provenance file reads)',
   'python3 /workspace/work/consumer-aborts/run-baseline-study.py (5 baseline-first runs on CPU3, each108000 fully verified,217088 allocations; exit0)',
   "VALGRIND_LIB=/workspace/work/request-header/valgrind/usr/libexec/valgrind taskset -c 3 /workspace/work/request-header/valgrind/usr/bin/valgrind --tool=callgrind --cache-sim=no --branch-sim=no --collect-atstart=no '--toggle-collect=*Consumer*apply_fetch_body' --callgrind-out-file=/workspace/work/consumer-aborts/callgrind/body.out /workspace/work/consumer-aborts/corrected-baseline-bin/runtime --cell nb-fetch-committed-aborts --out /workspace/work/consumer-aborts/callgrind/run --repetitions 1 (baseline-only scoped attribution; exit0, warning preserved)",
   'taskset -c 0,1 cargo test --offline --locked --manifest-path /workspace/work/consumer-aborts/candidate/Cargo.toml --test consumer_fetch_semantics --test full_surface -- --test-threads=2 (first and revised frozen source:386pass/1existingignore each, exit0)',
   'taskset -c 0,1 cargo clippy --offline --locked --manifest-path /workspace/work/consumer-aborts/candidate/Cargo.toml --lib --test consumer_fetch_semantics --test full_surface -- -D warnings (first and revised source:exit0)',
   'taskset -c 2 python3 /workspace/work/consumer-aborts/run-pairs.py (first candidate5pairs each named/bulk/1000p:30runs, all exit0)',
   'taskset -c 2 python3 /workspace/work/consumer-aborts/run-pairs.py --bulk-more (exactly10 added bulk pairs, all exit0; predeclared pooled15 guard, no further sampling; first candidate fails)',
   'taskset -c 2 cc -O2 /workspace/work/consumer-aborts/rss-exec-control.c -o /workspace/work/consumer-aborts/rss-exec-control (exit0)',
   'taskset -c 2 cc -O2 -Wall -Wextra -Werror /workspace/work/consumer-aborts/low-rss-launcher.c -o /workspace/work/consumer-aborts/low-rss-launcher (final exit0)',
   'taskset -c 2 python3 /workspace/work/consumer-aborts/verify-launcher.py (3 low-RSS fork/exec controls and exit17 propagation pass; exit0)',
   'taskset -c 2 python3 /workspace/work/consumer-aborts/run-revised-pairs.py 96986ba02ba5c7c480295c855a9e0dfe9efab549 (fresh predeclared5named+15bulk+5sparse pairs;50runs all exit0, no extensions)',
   'taskset -c 2 python3 /workspace/work/consumer-aborts/calculate-revised-stats.py (20k paired bootstrap resamples, seed20261002, exit0)',
   'python3 scripts/benchmark-report.py <each sanitized result.json> --quiet --json (110/110 valid,0errors;220 sidecar SHA256/size checks pass)',
   'git diff --check 824662e39980604c799e3398a7e3bc46db8400b3 96986ba02ba5c7c480295c855a9e0dfe9efab549 -- src/consumer.rs tests/consumer_fetch_semantics.rs (exit0)',
   'sha256sum -c checksums.sha256 (all companion checksums verified)'
 ],
 'results':{
   'primary':primary,
   'allocations':dict(named['allocations_per_record'],unit='per actual verified record',baseline_count=217088,candidate_count=108465,actual_verified_count=108000,identical_counts_in_every_variant_repetition=True),
   'instructions':{'qualifying_A_B_measured':False,'baseline_attribution_only':True,'tracked_benchmark_note':'No consumer instruction-count cell is tracked. Existing codec/protocol instruction-gate sources are unchanged, no budget was raised. Scoped baseline apply_fetch_body Ir is diagnostic, not a whole-cell instruction or CPU percentage claim.'},
   'guardrails':{'threshold_regression_pct':2,'fresh_cohort':guardrails,'actual_peak_authority':'Direct observed Rust process lifetime VmHWM, corroborated by phase-end RUSAGE_SELF where the measured phase dominates. Both actual-lifetime and phase-end maxima meet2% median guards. Coarse10ms instantaneous phase RSS is retained and not declared passing when it exceeds2%; it does not estimate the persistent process maximum reliably. Parent wait4 includes launcher/compiler descendants and is excluded from client peak.',
                 'coarse_losses_explicit':{'fresh_bulk_15_phase_sampled_peak_delta_pct':cell_metrics['nb-fetch-bulk']['phase_sampled_peak_rss_bytes']['delta_pct'],'old_sparse_5_phase_sampled_peak_delta_pct':old['initial5']['nb-fetch-1000p']['metrics']['phase_sampled_peak_rss_bytes']['delta_pct'],'old_bulk_extra10_phase_sampled_peak_delta_pct':old['additional10_bulk']['metrics']['phase_sampled_peak_rss_bytes']['delta_pct'],'old_bulk_all15_phase_sampled_peak_delta_pct':old['all15_bulk']['metrics']['phase_sampled_peak_rss_bytes']['delta_pct']},
                 'vmhwm_vs_phase_self':metrics['vmhwm_verification'],'observer_coverage':json.loads((out/'observer-coverage.json').read_text())},
   'net_production_lines':10,
   'correctness':{'passed':386,'failed':0,'ignored':1,'new_skips':0,'suites':[{'name':'consumer_fetch_semantics','passed':62,'failed':0,'ignored':1,'ignored_test':'live_fetch_session_recovery_required','reason':'Pre-existing: requires owned digest-pinnedKL05-07broker and explicit observer ports'},{'name':'full_surface','passed':324,'failed':0,'ignored':0}],'supplemental':'New regression passes original and candidate consumers:unordered/repeated PID interval metadata, ABORT/COMMIT control records spanning three responses, repeated PID transactions, exact offsets/cursors, then seek resets and replays same history. Existing suite covers LSO, nontransactional flags and seek/pause/assignment filtering. Complete-history harness e2e1pass; focused strict Clippy passes for both candidate attempts.'},
   'integrity':{'fresh_runs':50,'fresh_named_verified_records':1080000,'fresh_bulk_verified_records':600000,'fresh_sparse_verified_records':100000,'mismatches':0,'validation_failures':0,'runtime_artifacts_valid':110,'runtime_artifacts_failed':0,'sidecar_hashes_verified':220,'external_observer_artifacts':100}
 },
 'candidate_history':[{
   'source_sha':oldB,'disposition':'rejected-regression','reason':'Original5bulk throughput-2.271978% exceeded2%; predeclared exactly10 additional pairs yielded pooled15 CPU+2.021989% exceeding2%. No waiver or further unchanged-code sampling. Named allocation gain and every losing/coarse RSS observation remain valid and archived.',
   'named_initial5':select(old['initial5']['nb-fetch-committed-aborts']['metrics']),'bulk_initial5':select(old['initial5']['nb-fetch-bulk']['metrics']),'bulk_additional10':select(old['additional10_bulk']['metrics']),'bulk_pooled15':select(old['all15_bulk']['metrics']),'sparse_initial5':select(old['initial5']['nb-fetch-1000p']['metrics']),
   'revision':'Restore explicit read_committed isolation guard around cached abort predicate; RUC bypasses per-record transaction filtering as before. Cursor-update ordering is unchanged. Revised candidate pushed before a fresh fixed25-pair cohort; orchestration fork fix applies equally to both SHAs.',
   'method_distinction':'Old named/bulk process peaks exceeded inherited Python floor and remain valid. Old sparse RUSAGE_SELF has a constant13.197MB pre-exec floor, whereas observed runtime VmHWM12.94–13.16MB; those floored readings are retained and excluded from final acceptance.'
 }],
 'measurement_orchestration':{
   'launcher_sha256':sha(root/'low-rss-launcher'),'launcher_source_sha256':sha(root/'low-rss-launcher.c'),'compiler':'cc(Debian14.2.0-19)14.2.0',
   'floor_control':'Python→exec smallC retains9.44MB RUSAGE_SELF despite0.655MBcurrentRSS. ExtraC fork creates fresh child with0reported preexec peak/~0.25MBRSS; tiny execprobe selfpeak0.393MB removes Python floor; exit17 propagates. Both accepted A/B binaries use this same launcher with CPU3 inherited affinity; emitted actual Rust childPID is observed only after exactexe confirmation.',
   'sparse_phase_vs_lifetime':'With floor removed, sparse phase-end SELF medians9.359MB/9.343MB are truthful phase-end process maxima; lifetime VmHWM13.091MB/13.115MB includes later binary hash/provenance/artifact work. This ~40% difference is explicit, not falsely labelled a corroborating match. Actual lifetime peak plateau persists at least21samples/>25.6ms before exit, lastsample gap<=2.365ms.',
   'method_change':'First candidate cohorts use direct Python spawn; final fresh cohorts use low-current-RSS C fork/exec. Their statistics are never pooled across candidate/method changes. Original fixed5 and extra10 bulk cohorts remain separate, and their old pooled15 is also retained.',
   'wait4_control':'Compiler-version child reaches~85.18MB while true child inherits Python~8.78MB; external wait4 includes descendants. It is diagnostic only and never substituted for direct client peak.'
 },
 'preserved_failures':[
   {'artifact':'baseline-invariant-compile-failure.log','exit':101,'reason':'New fixture first called absent fetch_attempts helper; corrected to existing attempt_count.'},
   {'artifact':'baseline-invariant-runtime-failure.log','exit':101,'reason':'Attempt counter is zero-based; first subtract-one branch panicked/retried. Corrected fixture then passes against original consumer.'},
   {'artifact':'harness-proof-tests.log','exit':101,'reason':'Initial proof guessed135000fetched rather than actual134500. Raw artifact retained; source expectation corrected.'},
   {'artifact':'harness-proof-tests-final.log','exit':0,'reason':'Prefix proof passed but was rejected in review because tail108000returned vs20000verified could conceal aborted delivery. Final complete-history proof replaces it; original log retained.'},
   {'artifact':'harness-complete-history-tests.log','exit':101,'reason':'Initial complete-history expected wrong p2 tail range; raw exact actual history retained, corrected final57range proof passes.'},
   {'artifact':'candidate-exact-source-build.log','exit':101,'reason':'Root workspace package selector did not include excluded nullbroker harness. Correct explicit runtime manifest rebuild succeeds before measurement.'},
   {'artifact':'initial-baseline.log','exit':0,'excluded':True,'reason':'Initial binary launched from live sharedcwd emitted dirty/wrong provenance; rerun clean0c provenance preserved. Neither prefix artifact qualifies.'},
   {'artifact':'callgrind-body.log','exit':0,'reason':'Valgrind brk-segment warning preserved; full108k history still validates. Source-annotation warnings reproduced and retained; traced timing/RSS excluded.'}
 ],
 'acceptance_met':[
   'Corrected complete-history harness independently claimed/pushed before admission gate. Baseline-only exact108000 lookup String clone calls account49.7494%of217088 whole-cell allocations, exceeding1% admission threshold; diagnostic body-only Ir scope is explicit.',
   'Deterministic route: allocations per actual verified record fall50.0364%, identical counts in all5fresh A/B pairs. Wall-clock route is not claimed because fresh primary CPU pairedCI includeszero despite18.28%median improvement.',
   'Fresh predeclared cohorts meet CPU/allocation/throughput/actual-client-maximum median guards<=2% for all3cells. Coarse phaseRSS increases and old rejected-candidate guards remain explicit; no waiver or selection of further samples.',
   'One safe private partition-local state index, no runtime dependency/default/public API change, net10production lines. Active PID offset is looked up once per data batch; marker and seek lifecycle matches pinned histories.',
   'Both required correctness suites pass386tests with only1pre-existing live-broker ignore and no new skips; strict focused Clippy passes. No allocation/instruction baseline budget was raised.'
 ],
 'limits':[
   'Local unsigned null-broker optimization evidence, no Kafka comparison or public performance claim; SuiteHOLD unchanged.',
   'Synthetic uncompressed unique-PID aborted metadata cell is one Fetch response; repeatedPID, ABORT/COMMIT marker-spanning responses, LSO and seek behavior are verified by correctness fixtures rather than performance cells.',
   'Common bulk and sparse cells keep their existing target-prefix ID validation; only named committed-aborts was upgraded to complete returned-history verification. Common throughput is normalized to their existing20000/10000 verified target counts.',
   'Named fresh CPU pairedCI[-28.7124,+11.0220]% crosseszero. Common timing CIs also cannot exclude every2% regression; deterministic allocation gain is robust while CPU/wall timing remains affected by unmanaged host frequency/shared quota despite paused client builds.',
   'Coarse10ms phaseRSS freshbulk+3.8436%, old sparse+2.2544%, old additionalbulk+8.0733% and old pooledbulk+2.8919% remain reported as losses, not passing metrics. Direct persistent actual process maximum is the declared authoritative memory guard.',
   'Phase-end RUSAGE_SELF excludes later artifact/provenance work. Larger named/bulk maxima dominate lifetime and mostlymatchVmHWM; sparse lifetime~13MB exceeds phase-end~9.35MB. Both scopes and the removed preexec-floor controls are retained.',
   'External1msrequested observer intervals are approximately1.2–1.4ms with scheduling jitter. PersistentVmHWM retains high-water values across samples; sampled instantaneousRSS still misses brief peaks. Last peak plateaus provide coverage evidence but do not constitute an exact phase-boundary timestamp.',
   'Partition pending-header dedup still checks pending intervals; the heap avoids full re-sorts but this work does not qualify giant duplicate-header adversarial workloads or bound completed-transaction history beyond original lifecycle.'
 ]
}
# Keep human descriptions legible without changing source IDs or shell arguments.
replacements={
 'CPU3':'CPU 3','CPU2':'CPU 2','CPUs0':'CPUs 0','rustc1.':'rustc 1.','cargo1.':'cargo 1.',
 'match96986ba':'match 96986ba','a20k':'a 20k','returned108k':'returned 108k',
 'tail108000returned vs20000verified':'tail 108000 returned versus 20000 verified',
 'target20k':'target 20k','verified108k':'verified 108k','target unmet':'target unmet',
 'nominal target20k':'nominal target 20k','reported108k':'reported 108k',
 'one Fetch returned108k':'one Fetch returned 108k','every20k':'every 20k',
 'match134500':'match 134500','later500':'later 500','a first500':'a first 500',
 'each108000':'each 108000','then[32500':'then [32500','each[0':'each [0',
 '1passed0failed0ignored':'1 passed, 0 failed, 0 ignored','final1pass':'final 1 pass',
 'source:386pass/1existingignore':'source: 386 pass/1 existing ignore',
 'seed20261002':'seed 20261002','50runs':'50 runs','30runs':'30 runs',
 '25-pair':'25-pair','5named+15bulk+5sparse':'5 named + 15 bulk + 5 sparse',
 '5pairs each':'5 pairs each','exactly10':'exactly 10','pooled15':'pooled 15',
 'candidate5':'candidate 5','all5':'all 5','all3cells':'all 3 cells',
 'per data batch':'per data batch','meet2%':'meet 2%','exceeding1%':'exceeding 1%',
 'account49.7494%of217088':'account 49.7494% of 217088','exact108000':'exact 108000',
 'fall50.0364%':'fall 50.0364%','despite18.28%median':'despite 18.28% median',
 'pass386tests':'pass 386 tests','only1pre-existing':'only 1 pre-existing',
 'net10production':'net 10 production','original5bulk':'original 5 bulk','Original5bulk':'Original 5 bulk',
 'bulk+':'bulk +','bulk throughput-':'bulk throughput -','CPU+':'CPU +',
 'exceeded2%':'exceeded 2%','exceeding2%':'exceeding 2%','falls50':'falls 50',
 'constant13.197MB':'constant 13.197MB','runtime VmHWM12.94':'runtime VmHWM 12.94',
 'retains9.44MB':'retains 9.44MB','despite0.655MB':'despite 0.655MB',
 'with0reported':'with 0 reported','execprobe selfpeak0.393MB':'exec probe self peak 0.393MB',
 'extraC fork':'extra C fork','Python floor':'Python floor','exit17':'exit 17',
 'least21samples':'least 21 samples','true child inherits Python~8.78MB':'true child inherits Python ~8.78MB',
 'First candidate cohorts':'First candidate cohorts','original consumer':'original consumer',
 'attempt_count.':'attempt_count.','135000fetched':'135000 fetched','actual134500':'actual 134500',
 'final57range':'final 57-range','full108k':'full 108k','code_budget':'code_budget',
 'source SHA,':'source SHA,','named CPU pairedCI':'named CPU paired CI',
 'fresh primary CPU pairedCI':'fresh primary CPU paired CI','pairedCI[':'paired CI [',
 '20k paired':'20k paired','every2%':'every 2%','No consumer':'No consumer',
 'claimed because':'claimed because','fixed5':'fixed 5','extra10':'extra 10',
 'Old sparse':'Old sparse','first20k':'first 20k','data batch':'data batch',
 'codec(Debian':'codec (Debian','cc(Debian14.2.0-19)14.2.0':'cc (Debian 14.2.0-19) 14.2.0',
 'SuiteHOLD':'Suite HOLD','External1msrequested':'External 1ms requested',
 'persistentVmHWM':'persistent VmHWM','Fresh predeclared':'Fresh predeclared',
 'lastsample gap':'last sample gap','~85.18MB':'~85.18MB','original30':'original 30',
 'frozen-source':'frozen source','fixed25-pair':'fixed 25-pair','25.6ms':'25.6ms'
}
def legible(value):
    if isinstance(value,dict):return {k:legible(v) for k,v in value.items()}
    if isinstance(value,list):return [legible(v) for v in value]
    if isinstance(value,str):
        for a,b in replacements.items():value=value.replace(a,b)
    return value
evidence=legible(evidence)
evidence['artifacts']=[str(p.relative_to(repo)) for p in sorted(out.rglob('*')) if p.is_file()]
(repo/'docs/plan/evidence/KL09-47.json').write_text(json.dumps(evidence,indent=2)+'\n')
(out/'checksums.sha256').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n' for p in sorted(out.rglob('*')) if p.is_file() and p.name!='checksums.sha256'))
print(json.dumps({'disposition':evidence['disposition'],'source_sha':B,'primary':primary,'allocation_delta_pct':named['allocations_per_record']['delta_pct'],'valid_artifacts':110,'net_production_lines':10},indent=2))

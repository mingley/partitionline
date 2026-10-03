from pathlib import Path
import difflib,hashlib,json,os,stat,subprocess,time
root=Path('/workspace/work/client-sticky-performance-practical-497f')
bench=root/'benchmarks/sticky-partitioner'
original=Path('/workspace/work/client-sticky-performance-source-04f6bc29')
# Tiny source-only cleanup; no runner/SDK/driver is invoked.
p=bench/'run-cell.py';s=p.read_text().replace('import resource\n','')
s=s.replace("    write(out / 'cell-complete.json'", "    assert not cancel_signals, 'Host cancellation prevents completed-cell qualification'\n    write(out / 'cell-complete.json'")
p.write_text(s)
# Supplemental source review, explicitly separate prepared checks from executed format/syntax.
review={
 'classification':'WORK source-only practical adaptation; no Cargo resolver/compiler, javac/JVM, broker, driver, audit or measurement execution',
 'purpose':'Six serial small actual API/admission/public-ack/independent-delivery qualification cells, not ranking',
 'required_profile_names':['rust-rr-keyed','rust-uniform-keyed','java-uniform-keyed','rust-rr-null','rust-uniform-null','java-uniform-null'],
 'runtime_scope':{'exact_records':24576,'warmup':8192,'exercise':16384,'public_send_window':8192,'throughput_rates':'null','performance_qualified':False,'requires_already_ready_genuine_broker':True,'new_broker_image_VFS_topic_provisioning_cost_hidden_as_zero':False},
 'deadline_review':{'Rust':'One absolute90s qualification/300s ranking phase deadline across joins and flush; closes30s qualification/120s ranking. Send failures retain ambiguous history.',
   'Java':'Checks90s qualification/300s ranking between public calls; send can block5s/60s separately; late flush return fails phase. Host hard360s/900s producer and bounded close30s/120s prevent indefinite qualification.',
   'host':'1000s qualification or2400s ranking total. Runtime stops90s early for bounded source/input guards and owned process closure. Signals set state across process-creation ownership capture; own SIGKILL only after PGID/session checks.',
   'disk':'0.2s during each child at350MiB floor+16MiB stop margin. Source read guards check physical floor per file. Small failure receipts may spend margin but must stay above350MiB floor.'},
 'source_identity_review':{'immutable':'Entire actual full source file path set, SHA256/bytes/full07777 before and after each command; excludes .git metadata only. Origin compressed manifest is hash-bound.',
   'tools_and_inputs':'Actual retained ELF, real build receipt, Java classes/JAR/actual Java executable, Python/taskset executable, broker/origin receipts bind actual path/SHA256/bytes/fullmode before/after; no inode/nlink/mtime equality assumption.',
   'force_recompilation':'Future receipt must prove partitionline+adopter recompiled against actual source pin; dependency cache may be reused but archived mtime does not prove package identity.',
   'metadata':'Rust public metadata includes leader epoch; Java PartitionInfo binds partition/leader IDs only. No stronger Java leader-epoch claim.'},
 'bounds_and_limits':{'one_qualification_cell_launch_requirement_bytes':620298256,'one_cold_development_compile_launch_requirement_bytes':914358272,'all_six_runtime_cells_fit_guaranteed':False,'compile_and_runtime_overlap_allowed':False,
   'SQLite_qualification_cap_bytes':16777216,'stream_capture_each_bytes':1048576,'Java_qualification_Xmx_bytes':268435456,
   'unkeyed_successful_Rust_record_bound_sum_if_all24576_acknowledged':3072000,
   'unkeyed_input_volume':'Above two1MiB thresholds in total, across real drains; no public request/batch/cohort count is observed or inferred. This driver cannot prove exact switch history.',
   'driver_comparison':'Rust baseline and sticky use same public driver. Genuine Java driver has different allocation/JIT overhead; comparison must retain full settings and cannot claim identical accumulator/compression/RNG/record-byte history.',
   'ranking_input_history':'Warmup record counts may differ by client, so measured starting IDs may differ. Within-block generator/key/value sizes/seed distribution match; exact measured byte history equality is not claimed.',
   'CPU':'CPU0,1 qualification future lease. Ranking remains future exclusive CPU3 and no compile/broker/guard work on3.'},
 'prepared_independent_negative_controls':[
   {'mutation':'Actual accepted consumer journal duplicate/changed/missingID or hash/public offset','expected':'Offline audit fails; preserve actual copied input mutation+raw outcome'},
   {'mutation':'Actual accepted journal short body or appended extra row','expected':'Exact declared size rejection before SQLite allocation'},
   {'mutation':'Actual accepted phase map/count/phase ordering or wrong purpose','expected':'Mode/count/time/dense prefix failure'},
   {'mutation':'Real ranking duration<60s or measured independently delivered<1M','expected':'Ranking audit failure; never count small qualification as performance'},
   {'mutation':'Wrong actual ELF/class/JAR/source fullmode/hash identity or explicit partition config','expected':'Host prelaunch/source/config refusal'},
   {'mutation':'Own bounded control child exceeds capture cap/deadline, or cancellation signal','expected':'Only owned PGID stopped; exact failure receipts/source-after check; no false passed cell'}],
 'controls_execution_status':'Prepared only. Future controls mutate copied actual accepted outputs or controlled own child; no fabricated SDK captures. Separate small disk lease/forecast needed.',
 'lock':'Manual88-package candidate; all86 actual cached registry archive checksums verified. Real offline Cargo resolver and locked stable/MSRV default/all compile still pending.',
 'original_ranking_preserved':{'minimum_measure_seconds':60,'minimum_independently_delivered_measure_records':1000000,'minimum_paired_blocks':5,'original_forecast_bytes':8396561424},
 'original_script_summary_note':'finish-practical-review.py printed620298256 under a mislabeled cold_compile_forecast_bytes key during source adaptation. This is the small runtime guard. Authoritative cold compile proposal in offline-compile-plan.json is914358272; neither is an observed runtime/build.',
 'repository_or_product_changes_performed':False
}
(root/'source-review.json').write_text(json.dumps(review,indent=2)+'\n')
# Refresh actual final source diffs; earlier temporary diffs stay saved in pre-review history.
for path,label in [('src/main.rs','rust-qualification-and-deadline.patch'),('StickyBenchmark.java','java-qualification.patch'),('run-cell.py','host-qualification-and-failure-paths.patch'),('audit.py','audit-qualification-and-size-bounds.patch')]:
    before=(original/'benchmarks/sticky-partitioner'/path).read_text().splitlines(keepends=True)
    after=(bench/path).read_text().splitlines(keepends=True)
    p=root/label
    if p.exists():
        prior=root/'history/practical-review-01'/label
        if not prior.exists():
            prior.write_bytes(p.read_bytes());os.chmod(prior,stat.S_IMODE(p.stat().st_mode))
    p.write_text(''.join(difflib.unified_diff(before,after,fromfile='original/'+path,tofile='practical/'+path)))
# Source-only checks use actual binaries; no Cargo or JVM process starts.
checks=root/'source-only-checks';checks.mkdir(exist_ok=False)
env=dict(os.environ);env.update(RUSTUP_HOME='/workspace/work/rustup',CARGO_HOME='/workspace/work/cargo')
executed=[]
for label,toolchain in [('stable','stable-x86_64-unknown-linux-gnu'),('msrv','1.85.0-x86_64-unknown-linux-gnu')]:
    command=['taskset','-c','0,1',f'/workspace/work/rustup/toolchains/{toolchain}/bin/rustfmt','--check','--edition','2021',str(bench/'src/main.rs')]
    start=time.monotonic();result=subprocess.run(command,env=env,capture_output=True,timeout=30)
    (checks/(label+'.stdout')).write_bytes(result.stdout);(checks/(label+'.stderr')).write_bytes(result.stderr)
    receipt={'command':command,'exit':result.returncode,'elapsed_seconds':time.monotonic()-start,'classification':'Executed rustfmt source syntax/format only; not compile/behavior proof'}
    (checks/(label+'.json')).write_text(json.dumps(receipt,indent=2)+'\n');executed.append(receipt)
    assert result.returncode==0, receipt
python_checks=[]
for p in sorted(bench.glob('*.py')):
    compile(p.read_bytes(),str(p),'exec')
    python_checks.append({'path':str(p.relative_to(root)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'syntax_compile_only_no_exec':True})
(checks/'python-syntax.json').write_text(json.dumps({'classification':'Python built-in compile(source) only; no runner/audit main invoked, no pyc created','files':python_checks},indent=2)+'\n')
# All original phases remain exact; preserve original forecast byte/full mode in additive copy.
stage=json.loads((original/'stage-handoff.json').read_text());original_results=[]
for rel,expected in stage['files'].items():
    p=original/rel;raw=p.read_bytes();mode=stat.S_IMODE(p.stat().st_mode)
    assert hashlib.sha256(raw).hexdigest()==expected['sha256'] and len(raw)==expected['bytes'] and mode==expected['full_mode']
    original_results.append(rel)
assert len(original_results)==19
orig_forecast=original/'benchmarks/sticky-partitioner/resource-forecast.json'
assert orig_forecast.read_bytes()==(bench/'resource-forecast.json').read_bytes()
assert stat.S_IMODE(orig_forecast.stat().st_mode)==stat.S_IMODE((bench/'resource-forecast.json').stat().st_mode)
(checks/'preservation-after.json').write_text(json.dumps({'all_19_original_paths_SHA_bytes_fullmode_exact':True,'paths':original_results,'original_stage_sha256':hashlib.sha256((original/'stage-handoff.json').read_bytes()).hexdigest(),'original_forecast_SHA256':hashlib.sha256(orig_forecast.read_bytes()).hexdigest(),'original_forecast_bytes_proposal':8396561424},indent=2)+'\n')
print(json.dumps({'executed_format_checks':len(executed),'executed_Python_compile_source_checks':len(python_checks),'original_19_paths_exact':True,'Cargo_JVM_or_driver_execution':False},indent=2))

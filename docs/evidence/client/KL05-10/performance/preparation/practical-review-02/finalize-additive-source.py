from pathlib import Path
import difflib,hashlib,json,shutil,stat
root=Path('/workspace/work/client-sticky-performance-practical-review-02')
prior=Path('/workspace/work/client-sticky-performance-practical-497f');bench=root/'benchmarks/sticky-partitioner'
# Live prose/config refers to corrected envelope; all historical text is untouched.
p=bench/'README.md';s=p.read_text()
s=s.replace('620,298,256 free bytes (about 592 MiB)','687,407,120 free bytes (about 656 MiB)')
s=s.replace('reserve, 3.74 MB two journals, a 16 MiB SQLite cap, bounded logs and a 64 MiB ready','reserve, 3.74 MB two journals, a fixed-page 16 MiB SQLite DB cap, separate\n32 MiB normal rollback and 32 MiB statement/subjournal reserves, bounded logs\nand a 64 MiB ready')
s=s.replace('The original 8.4 GB ranking forecast remains byte-for-byte in resource-forecast.json.','The original 8.4 GB ranking forecast remains byte-for-byte in resource-forecast.json.\nThe runner now uses the supplemental ranking-resource-forecast.json, requiring\n12,725,083,152 bytes for its unchanged 10M record cap. This includes both SQLite\nsidecar reserves; it does not reuse the separate archive allowance. Ranking remains\nheld until this real budget and the independent CPU 3 lease are available.')
s += '''\nSQLite page_size=4096 and max_page_count are read back before any schema creation.\nThe checker also verifies DELETE journal mode, FULL synchronization, disabled\ncache spill and MEMORY temp store. Normal rollback and statement/subjournal\nallowances are conservative proposals, not observed allocations. The 16 MiB\ncache_size is a configured soft target: dirty pages may exceed it up to the DB\ncap, with additional statement-page and allocator overhead. No hard 16 MiB RAM\ncap is claimed. The connection closes even when configuration checks fail.\n\nProcess-group inspection now fails closed on unreadable or malformed PID state,\nwrong PID/session or changed owner starttime. A missing stat must also pass an\nexistence probe confirming the PID vanished. Unknown membership is recorded as\nnull/unverified, never an empty closed group, and refuses group signalling. The\nwrapper may kill its known unreaped direct Popen child while recording that\ndescendant closure remains unverified. No daemon exception silently hides a PID.\n'''
p.write_text(s)
p=bench/'qualification.json';d=json.loads(p.read_text());d['SQLite_geometry_and_sidecars']={'verified_page_bytes':4096,'max_database_pages':4096,'database_byte_cap':16777216,'DELETE_rollback_reserve_bytes':33554432,'statement_subjournal_reserve_bytes':33554432,'allocation_claim':'Prepared conservative forecast; no actual SQLite connection or allocation proof executed'};p.write_text(json.dumps(d,indent=2)+'\n')
plan=json.loads((prior/'offline-compile-plan.json').read_text());plan['qualification_runtime']['one_cell_free_requirement_bytes']=687407120
plan['ranking']='Original8,396,561,424B proposal preserved historically; actual-use supplemental forecast12,725,083,152B includes both SQLite sidecars. >=60s/>=1M independently delivered/5paired blocks and CPU3 lease unchanged.'
(root/'offline-compile-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
# Lossless independent peer finding/control receipts.
peer=Path('/workspace/work/integration/client-sticky-performance-review-01');dest=root/'independent-frozen-review';dest.mkdir()
expected={'controls.json':'f5967d4ea5fd6c6ed2a8584ab7a2a7dac2e869fd3046fcd1b83d16b745ff8c31','validation.json':'71020e3c3cfc105997155fcdca8489d9b0164a172c99aa30b23b9bdeb42e6a68'}
for p in peer.iterdir():
    assert p.is_file();raw=p.read_bytes()
    if p.name in expected:assert hashlib.sha256(raw).hexdigest()==expected[p.name]
    shutil.copy2(p,dest/p.name)
    assert raw==(dest/p.name).read_bytes() and stat.S_IMODE(p.stat().st_mode)==stat.S_IMODE((dest/p.name).stat().st_mode)
# Final authoritative narrow review and supplemental limits.
review={'classification':'Frozen-source findings addressed in additive WORK derivative; no repo/Cargo/JVM/SQLite/process/audit/main/benchmark execution',
 'original_stage_sha256':'5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1',
 'findings':[{'source':'OpenLoop independent63 pure source/semantic controls','problem':'4096 max pages did not bind4096-byte page geometry and DELETE rollback journal was unbudgeted','fix':'Explicit verified4096 page geometry/max_page_count before schema; verified DELETE/FULL/cache_spill OFF/temp MEMORY; separate conservative normal and statement sidecars; configuration failure closes connection','limits':'No real SQLite allocation/control executed. Cache target16MiB is soft; dirty/statement pages can exceed it. Future positive/raw mutation controls and actual allocation receipts still required.'},
             {'source':'OpenLoop independent PermissionError counterexample','problem':'members() dropped unreadable live PID state and could claim an empty closed group','fix':'Only independently confirmed vanished PIDs may be absent. Unknown/malformed/changed PID/session/starttime fails closed; group stop refused and closure_verified=false; known direct child only kill may settle parent without claiming descendant closure','limits':'No real proc inspection/group signal/process lifecycle executed. Future genuine own process/cancellation/output/deadline controls still required.'}],
 'executed_pure_semantic_controls':{'count':38,'receipt':'source-semantic-controls.json','classification':'AST extracted helper definitions with FakeConnection/FakePath/FakeOS only; no actual SQLite/proc/processes','first_attempt':'history/control-attempt-01-syntax contains actual parse failure before control execution; corrected run passes38'},
 'qualification_forecast_bytes':687407120,'supplemental_ranking_forecast_bytes':12725083152,
 'original_ranking_resource_forecast_unchanged_bytes':8396561424,
 'ranking_minima_unchanged':{'seconds':60,'independent_delivered_measure_records':1000000,'paired_blocks':5},
 'cold_compilation_forecast_unchanged_bytes':914358272,
 'bench_Rust_Java_and_Cargo_sources_unchanged_from_frozen5c94':True,
 'new_ranking_forecast_is_effective_launch_guard':True,
 'no_archive_reserve_reuse_for_journals':True,
 'future_required_gates':['Root independent source review/install/push','Actual offline Cargo resolver validation of manual lock','Strict actual immutable stable/MSRV default/all package recompilation','Actual genuine Java build and six small live delivery cells plus raw negative controls','Separate CPU3/ranking resource lease and complete>=60s/>=1M/5paired measurements'],
 'card_status_or_performance_claim_changed':False}
(root/'source-review.json').write_text(json.dumps(review,indent=2)+'\n')
for rel in ['audit.py','run-cell.py','qualification-resource-forecast.json','qualification.json','README.md']:
    old=(prior/'benchmarks/sticky-partitioner'/rel).read_text().splitlines(keepends=True)
    new=(bench/rel).read_text().splitlines(keepends=True)
    (root/(rel.replace('.','-')+'.patch')).write_text(''.join(difflib.unified_diff(old,new,fromfile='frozen5c94/'+rel,tofile='corrected/'+rel)))
for p in bench.glob('*.py'):compile(p.read_bytes(),str(p),'exec')
(root/'Python-syntax.json').write_text(json.dumps({'classification':'Executed compile(source) syntax only; no main/module import/audit/SQLite/proc/process','files':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in bench.glob('*.py')}},indent=2)+'\n')
# Current Rust/Java/Cargo bodies remain exactly original so prior source-only format receipt is scoped valid.
unchanged=['src/main.rs','StickyBenchmark.java','Cargo.toml','Cargo.lock','profiles.json','resource-forecast.json']
for rel in unchanged:
    old=prior/'benchmarks/sticky-partitioner'/rel;new=bench/rel
    assert old.read_bytes()==new.read_bytes() and stat.S_IMODE(old.stat().st_mode)==stat.S_IMODE(new.stat().st_mode)
(root/'unchanged-driver-source.json').write_text(json.dumps({'actual_read_only_byte_fullmode_verification':True,'paths':unchanged,'format_receipts':'Historical stable/MSRV rustfmt check applies only to unchanged Rust body; no compile/runtime qualification inferred'},indent=2)+'\n')
print(json.dumps({'narrow_work_sources_finalized':True,'Rust_Java_Cargo_bodies_unchanged':True,'actual_Cargo_JVM_SQLite_proc_runtime':False},indent=2))

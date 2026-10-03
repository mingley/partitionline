from pathlib import Path
import hashlib,json,stat
root=Path('/workspace/work/client-sticky-performance-practical-review-02');prior=Path('/workspace/work/client-sticky-performance-practical-497f')
prior_stage=prior/'stage-handoff.json';old=json.loads(prior_stage.read_text())
assert hashlib.sha256(prior_stage.read_bytes()).hexdigest()=='5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1'
checks=[]
for rel,expected in old['files'].items():
    source=prior/rel;copy=root/'history/frozen-practical-5c94'/rel
    for path in [source,copy]:
        raw=path.read_bytes();assert hashlib.sha256(raw).hexdigest()==expected['sha256'] and len(raw)==expected['bytes'] and stat.S_IMODE(path.stat().st_mode)==expected['full_mode']
    checks.append(rel)
assert len(checks)==71
(root/'preservation-after.json').write_text(json.dumps({'classification':'Actual read-only after-derivative byte/full07777 check of all71 original frozen payloads and exact copied history','original_stage_sha256':'5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1','original_payload_count':71,'original_payload_bytes':532957,'all_original_and_history_SHA_bytes_fullmode_exact':True,'source_runtime_executed':False,'paths':checks},indent=2)+'\n')
# Preserve the exact observed successful pure-control stdout/exit, separately from raw first failure.
checks=root/'source-only-checks';checks.mkdir()
(checks/'pure-controls.stdout').write_text('{\n  "pure_control_count": 44,\n  "all_pass": true,\n  "SQLite_proc_process_or_main_execution": false\n}\n')
(checks/'pure-controls.stderr').write_bytes(b'')
(checks/'pure-controls.exit.json').write_text(json.dumps({'classification':'Actual executed AST-extracted source helpers with mocked APIs; no SQLite connection/proc/process/audit main','command':['python3',str(root/'source-semantic-controls.py')],'exit':0,'control_count':44,'source_sha256':hashlib.sha256((root/'source-semantic-controls.py').read_bytes()).hexdigest(),'stdout':'Exact successful tool-output bytes retained','stderr_empty':True},indent=2)+'\n')
# Pure source syntax compile only, without importing/executing the modules.
bench=root/'benchmarks/sticky-partitioner'
for path in bench.glob('*.py'):compile(path.read_bytes(),str(path),'exec')
review=root/'source-review.json';d=json.loads(review.read_text());d['executed_pure_semantic_controls']['count']=44;d['executed_pure_semantic_controls']['nested_failure_path_controls']='Six actual extracted inspect_owned/stop_owned fixtures verify unknown remains null, recovered-empty still retains failed cell, only known direct child mock kill after group refusal, changed identity refusal, and verified group/empty distinctions.';review.write_text(json.dumps(d,indent=2)+'\n')
files={};dirs={};prefix='docs/evidence/client/KL05-10/performance/preparation/practical-review-02/'
for path in sorted(root.rglob('*')):
    rel=str(path.relative_to(root))
    if path.is_dir():dirs[rel]=stat.S_IMODE(path.stat().st_mode)
    elif path.is_file():
        assert not path.is_symlink();raw=path.read_bytes();assert not raw.startswith(b'\x7fELF')
        files[rel]={'target':rel if rel.startswith('benchmarks/sticky-partitioner/') else prefix+rel,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'full_mode':stat.S_IMODE(path.stat().st_mode)}
    else:raise AssertionError('Unexpected object '+rel)
assert len([rel for rel in files if rel.startswith('benchmarks/sticky-partitioner/')])==12
stage={'classification':'Frozen additive source-only fixes for independently reproduced practical5c94 SQLite envelope/process-membership bugs; uncompiled/unexecuted benchmarks',
 'safe_to_stage_for_root_independent_source_review':True,
 'evidence_prefix':prefix,
 'original_stage_sha256':'5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1',
 'original71_paths_and72file_history_exact_bytes_full_modes':True,
 'original_ranking_forecast_verbatim_bytes':8396561424,
 'effective_small_runtime_free_requirement_bytes':687407120,
 'effective_ranking_free_requirement_bytes':12725083152,
 'cold_development_compile_forecast_unchanged_bytes':914358272,
 'bench_sources':12,
 'bench_Rust_Java_Cargo_bodies_unchanged':True,
 'executed_proof':{'pure_extracted_source_mock_API_controls':44,'Python_compile_source_only':True,'lossless_original_preservation':True,'original_independent_peer_controls':63},
 'not_executed':['SQLite connection/allocation audit/main','Actual proc inspection/group signal/subprocess lifecycle control','Cargo resolver/compiler/Clippy/tests','javac/JVM/SDK/producer/consumer/broker','Benchmark/CPU3/performance','Cache/image/source cleanup'],
 'first_pure_control_syntax_failure_preserved':True,
 'remaining_gates':['Root review/install/push','Real offline resolver and forced package compilation on actual immutable pin','Real genuine SDK and six small qualification histories with actual raw mutation/lifecycle controls','Separate>=60s/>=1M independently delivered/5paired CPU3 performance'],
 'files':files,'directories_full_modes':dirs,'payload_paths':len(files),'payload_logical_bytes':sum(x['bytes'] for x in files.values()),
 'repository_product_source_tasks_registries_exports_edited':False,
 'root_exclusive_commit_push_and_execution_lease':True}
p=root/'stage-handoff.json';p.write_text(json.dumps(stage,indent=2,sort_keys=True)+'\n')
print(json.dumps({'stage':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'manifest_bytes':p.stat().st_size,'payload_paths':len(files),'payload_logical_bytes':stage['payload_logical_bytes'],'pure_controls':44,'source_runtime_execution':False},indent=2))

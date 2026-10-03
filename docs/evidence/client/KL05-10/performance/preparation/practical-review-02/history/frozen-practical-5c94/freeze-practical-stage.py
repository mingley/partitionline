from pathlib import Path
import hashlib,json,os,shutil,stat
root=Path('/workspace/work/client-sticky-performance-practical-497f')
original=Path('/workspace/work/client-sticky-performance-source-04f6bc29')
original_stage=json.loads((original/'stage-handoff.json').read_text())
history=root/'history/original-performance-ebb7b91b'
assert not history.exists()
for rel,expected in original_stage['files'].items():
    source=original/rel;target=history/rel
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(source,target)
    raw=target.read_bytes()
    assert hashlib.sha256(raw).hexdigest()==expected['sha256'] and len(raw)==expected['bytes'] and stat.S_IMODE(target.stat().st_mode)==expected['full_mode']
shutil.copy2(original/'stage-handoff.json',history/'stage-handoff.json')
identities={}
source=Path('/workspace/work/client-capabilities-source-04f6bc29')
expected_hashes={
'src/partitioner.rs':'40a756c2f12f65c1f53cc44a42f604619156ab760321745f4d0899fb30dbdc9a',
'src/producer.rs':'37098eee8da69a2e8c39b24cd88358447d55a8992d3ef6b21e9097fabcfebc10',
'src/protocol/records.rs':'2f415486f1df8251b30f502dfc66b085efcde80808ee85a61f9763b15b6049b8',
'src/protocol/buf.rs':'ceb274267855a783e985cab906929298ce35393c4001809626fce452f3adcebe',
}
for rel,expected in expected_hashes.items():
    raw=(source/rel).read_bytes();sha=hashlib.sha256(raw).hexdigest()
    assert sha==expected
    work=Path('/workspace/partitionline')/rel
    identities[rel]={'immutable_source_sha256':sha,'immutable_source_bytes':len(raw),'immutable_source_full_mode':stat.S_IMODE((source/rel).stat().st_mode),'current_repo_same_bytes':work.read_bytes()==raw,'current_repo_not_edited_by_this_preparation':True}
(root/'source-identity.json').write_text(json.dumps({'classification':'Read-only pinned product/held-file identity snapshot; WORK benchmark does not alter these files','source_commit':'04f6bc2968c1d721c6815a6389897a62e4ca76f1','origin_receipt_sha256':'5abaab9053b7b81d03a3c6ac161c4188f1f7a6ee909c71ecc69914224ed4ce29','full_immutable_source_files_in_origin':73938,'files':identities,'future_qualification_pin':'Root chooses actual pushed benchmark+product pin; no runtime/compile qualification on04f assumed'},indent=2)+'\n')
# Tight bounded proof copy: all payloads are source/config/diffs or actual syntax/format/preservation receipts.
files={};dirs={};evidence='docs/evidence/client/KL05-10/performance/preparation/practical-497f/'
for path in sorted(root.rglob('*')):
    rel=str(path.relative_to(root))
    if path.is_dir():
        dirs[rel]=stat.S_IMODE(path.stat().st_mode)
    elif path.is_file():
        raw=path.read_bytes()
        assert not path.is_symlink() and not raw.startswith(b'\x7fELF')
        target=rel if rel.startswith('benchmarks/sticky-partitioner/') else evidence+rel
        files[rel]={'target':target,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'full_mode':stat.S_IMODE(path.stat().st_mode)}
    else:
        raise AssertionError('Unexpected filesystem object '+rel)
assert len([p for p in files if p.startswith('benchmarks/sticky-partitioner/')])==11
manifest={
 'classification':'Frozen additive source-only practical performance-driver packet; prepared and uncompiled/unexecuted; not sticky/performance closure',
 'safe_to_stage_for_root_source_review':True,
 'sole_future_repository_install_prefix':'benchmarks/sticky-partitioner/',
 'published_claim_write_set_reference':'KL05-10 benchmark prefix formally added by root main497f',
 'evidence_prefix':evidence,
 'source_only_checks_executed':['Explicit stable rustfmt --check','Explicit1.85 rustfmt --check','Python built-in compile(source) only','Original19 file/hash/bytes/fullmode preservation','Read-only cached86 registry archive checksum validation'],
 'not_executed':['Cargo resolver/metadata/compiler/Clippy/test','javac/JVM/SDK','producer/consumer/broker','audit main/negative controls','CPU3 ranking or performance measurement','cache/image cleanup'],
 'manual_Cargo_lock_status':'88-package source candidate only; actual offline resolver validation pending. Preserve any resolver differences and push corrected lock before final immutable locked compilation.',
 'qualification_runtime_guard_bytes':620298256,
 'cold_development_compile_guard_bytes':914358272,
 'ranking_gate_preserved':{'seconds':60,'independent_measure_records':1000000,'paired_blocks':5,'original_forecast_bytes':8396561424},
 'old_original_stage_sha256':'ebb7b91be278179ac2d38ed00678177fa0c915be84ac24a0597a4ab57a0e9ac0',
 'original_history_copy_exact19_paths_plus_manifest':True,
 'pre_review_practical_history_exact12_paths_plus_prior_diff_copies':True,
 'files':files,'directories_full_modes':dirs,
 'path_count':len(files),'logical_payload_bytes':sum(v['bytes'] for v in files.values()),
 'repository_sources_tasks_registries_or_exports_edited':False,
 'root_exclusively_commits_pushes_and_grants_execution_lease':True,
}
path=root/'stage-handoff.json';path.write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
print(json.dumps({'stage':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'manifest_bytes':path.stat().st_size,'payload_paths':len(files),'payload_logical_bytes':manifest['logical_payload_bytes'],'bench_sources':11,'all_uncompiled_unexecuted':True},indent=2))

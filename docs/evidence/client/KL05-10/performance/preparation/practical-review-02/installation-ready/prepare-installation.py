"""Read-only bind the frozen benchmark payloads to their exact root install paths."""
import hashlib
import json
from pathlib import Path
import stat

OUT = Path(__file__).resolve().parent
CURRENT = Path('/workspace/work/client-sticky-performance-practical-review-02')
PRACTICAL = Path('/workspace/work/client-sticky-performance-practical-497f')
ORIGINAL = Path('/workspace/work/client-sticky-performance-source-04f6bc29')
PEER = Path('/workspace/work/integration/client-sticky-performance-review-02')

def info(path):
    assert path.is_file() and not path.is_symlink(), str(path)
    raw = path.read_bytes()
    return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'full_mode': stat.S_IMODE(path.stat().st_mode)}

def save(name, value):
    path = OUT / name
    assert not path.exists(), str(path)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)
    return path

def verify(root, expected_manifest):
    path = root / 'stage-handoff.json'
    observed = info(path)
    assert observed['sha256'] == expected_manifest
    data = json.loads(path.read_text())
    payloads = []
    for relative, expected in data['files'].items():
        source = root / relative
        actual = info(source)
        assert actual == {k: expected[k] for k in actual}, str(source)
        payloads.append({'source': str(source), 'original_relative_path': relative,
                         'target': expected.get('target'), **actual})
    for relative, full_mode in data.get('directories_full_modes', {}).items():
        source = root / relative
        assert source.is_dir() and not source.is_symlink()
        assert stat.S_IMODE(source.stat().st_mode) == full_mode
    return data, payloads, {'path': str(path), **observed}

original, original_files, original_manifest = verify(ORIGINAL, 'ebb7b91be278179ac2d38ed00678177fa0c915be84ac24a0597a4ab57a0e9ac0')
practical, practical_files, practical_manifest = verify(PRACTICAL, '5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1')
corrected, current_files, current_manifest = verify(CURRENT, '2724dd786ec25e9bb371fceaaac6d0deb5f384969f7fa9ba2826d9afb4820ea9')
assert len(original_files) == 19 and len(practical_files) == 71
assert len(current_files) == 114 and sum(row['bytes'] for row in current_files) == 911443
assert len({row['target'] for row in current_files}) == 114
source_files = [row for row in current_files if row['target'].startswith('benchmarks/sticky-partitioner/')]
assert len(source_files) == 12
for row in practical_files:
    original_path = Path(row['source'])
    history_path = CURRENT / 'history/frozen-practical-5c94' / row['original_relative_path']
    assert info(original_path) == info(history_path)
assert info(PRACTICAL / 'stage-handoff.json') == info(CURRENT / 'history/frozen-practical-5c94/stage-handoff.json')

review_info = info(PEER / 'validation.json')
assert review_info['sha256'] == '50d1f9e77c70e46283435f6753f8f83b9d7e7622395de5e2b122348c4f424f0c'
review = json.loads((PEER / 'validation.json').read_text())
assert review['all_114_bytes_full_modes_unchanged'] and review['no_additional_concrete_source_blocker']
assert info(PEER / 'controls.json')['sha256'] == '07ea486f343d9a61b0e6e17e8064c2a38c2aedae8e7ff1013ef06b0b9ef1396e'
prefix = corrected['evidence_prefix']
review_files = [{'source': str(PEER / name), 'target': prefix + 'independent-corrected-review/' + name,
                 **info(PEER / name)}
                for name in ['validation.json', 'controls.json', 'review.py', 'reviewer-first-assumption-failure.txt']]
frozen_manifest = {'source': str(CURRENT / 'stage-handoff.json'),
                   'target': prefix + 'stage-handoff.json', **info(CURRENT / 'stage-handoff.json')}
manifest = save('installation-manifest.json', {
    'classification': 'Ready for root source installation only; benchmark prototype remains uncompiled and unexecuted.',
    'safe_to_install_source': True, 'root_exclusive_commit_push': True,
    'production_or_taskbook_edits_authorized_here': False,
    'payload_packet': current_manifest,
    'base_payloads': {'paths': 114, 'bytes': 911443, 'benchmark_source_paths': 12,
                      'proof_paths': 102, 'all_bytes_full_07777_verified': True,
                      'mapping': current_files},
    'exact_benchmark_sources': source_files,
    'provenance': {'original': {**original_manifest, 'payload_paths': 19, 'payload_bytes': 148441},
                   'practical01': {**practical_manifest, 'payload_paths': 71, 'payload_bytes': 532957},
                   'corrected02': {**current_manifest, 'payload_paths': 114, 'payload_bytes': 911443},
                   'original_and_practical_sources_unmodified': True,
                   'all71_practical_payloads_plus_manifest_losslessly_preserved_under_corrected_history': True},
    'independent_review': {'receipt': {'path': str(PEER / 'validation.json'), **review_info},
                          '44_frozen_extracted_source_controls_replayed': True,
                          '24_independent_pure_memory_controls': True,
                          'source_blockers': [], 'actual_SQLite_proc_runtime_or_benchmark': False},
    'supplemental_proof_mapping': [frozen_manifest, *review_files],
    'resources': {'qualification_runtime_required_free_bytes': 687407120,
                  'cold_development_compile_forecast_bytes': 914358272,
                  'original_ranking_forecast_preserved_bytes': 8396561424,
                  'effective_ranking_required_free_bytes': 12725083152,
                  '350MiB_live_floor_still_required': True,
                  'classification': 'Unobserved conservative forecasts, not measured consumption.'},
    'execution_limits': {'Cargo_lock': 'Manually prepared unresolved candidate; actual offline resolution and forced package compile required.',
                         'six_small_profiles': 'Prepared8192 warmup+16384 measured records/profile; no ranking.',
                         'ranking': 'Separate future CPU3 lease,>=60s and>=1M independently delivered per cell,minimum5 paired blocks.',
                         'no_unchanged_giant_source_tree_copies': True,
                         'not_executed': corrected['not_executed'],
                         'remaining_gates': corrected['remaining_gates']}})

# Independently bound the already identified historical proof-only duplicate;
# this is a proposal and intentionally does not perform any link/unlink/delete.
duplicates = [Path('/workspace/work/client-share-assessment/final-255f2325/source-verification.json'),
              Path('/workspace/work/client-share-assessment/sealed-source-audit-255f2325/source-verification.json')]
duplicate_rows = []
for path in duplicates:
    before = path.stat()
    identity = info(path)
    after = path.stat()
    assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_mode) == (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_mode)
    duplicate_rows.append({'path': str(path), **identity, 'device': before.st_dev,
                           'inode': before.st_ino, 'nlink': before.st_nlink,
                           'allocated_bytes': before.st_blocks * 512,
                           'mtime_ns': before.st_mtime_ns, 'ctime_ns': before.st_ctime_ns,
                           'uid': before.st_uid, 'gid': before.st_gid,
                           'unchanged_during_read': True})
assert {row['sha256'] for row in duplicate_rows} == {'82ea54efa416c6b64f53c98870e2776d3981a642cc9063287b68eef77a4be65e'}
with duplicates[0].open('rb') as first, duplicates[1].open('rb') as second:
    while True:
        a, b = first.read(1024 * 1024), second.read(1024 * 1024)
        assert a == b
        if not a:
            break
assert duplicate_rows[0]['full_mode'] == duplicate_rows[1]['full_mode'] == 0o600
proposal = save('duplicate-proof-proposal.json', {
    'classification': 'Read-only exact duplicate proposal; not permission to mutate.',
    'performed_link_unlink_delete_or_cleanup': False,
    'files': duplicate_rows, 'chunkwise_byte_equality': True,
    'potential_redundant_allocation_bytes': duplicate_rows[1]['allocated_bytes'],
    'measured_recovered_bytes': 0,
    'scan_scope': 'Only11 completed own Share proof directories; no whole-work or immutable full-source scan.',
    'excluded': ['Immutable403/04f/full-source trees', 'Caches/ELF/SDK/JAR/images',
                 'Raw Rust/Java/Python/source files', 'All current mutable paths'],
    'already_coalesced_proofs': ['0057972d and12f43986 audit copies already share inodes and offer0 new recovery.'],
    'closed_no_future_writer_confirmed_by_owner': True,
    'needed_identity': 'Preserve both paths, exact bytes,SHA,07777 and ownership; no distinct inode/ctime/nlink requirement.',
    'binding_receipt': {'path': '/workspace/work/client-share-assessment/sealed-source-audit-255f2325/raw-source-audit-manifest.json',
                        **info(Path('/workspace/work/client-share-assessment/sealed-source-audit-255f2325/raw-source-audit-manifest.json'))},
    'required_before_action': 'Parent separate review/GO and fresh process/path/byte/fullmode/ownership guards; actual recovered space only after measured action.'})

ready = save('ready-stage.json', {
    'safe_to_stage_source_for_root': True, 'safe_to_claim_runtime_or_performance': False,
    'base_frozen_packet': current_manifest,
    'base_payloads': current_files,
    'base_payload_count': 114, 'base_payload_bytes': 911443,
    'additional_metadata_and_peer_proof': [frozen_manifest, *review_files,
        {'source': str(manifest), 'target': prefix + 'installation-ready/installation-manifest.json', **info(manifest)},
        {'source': str(OUT / 'prepare-installation.py'), 'target': prefix + 'installation-ready/prepare-installation.py',
         **info(OUT / 'prepare-installation.py')}],
    'duplicate_proposal_WORK_only_not_part_of_source_installation': {'path': str(proposal), **info(proposal)},
    'copied_payload_bytes': 0,
    'root_action': 'Install mapped12 benchmark sources and102 preparation proofs plus optional named supplemental metadata; commit/push only after independent review.',
    'no_Cargo_JVM_SQLite_proc_benchmark_cleanup_executed': True})
print(json.dumps({'ready_stage': {'path': str(ready), **info(ready)},
                  'installation_manifest': {'path': str(manifest), **info(manifest)},
                  'duplicate_proposal': {'path': str(proposal), **info(proposal)},
                  'base_payload_count': len(current_files), 'base_payload_bytes': sum(row['bytes'] for row in current_files)}))

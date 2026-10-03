#!/usr/bin/env python3
"""Freeze the accepted retention handoff without changing its raw receipts."""
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[6]
BASE = Path(__file__).resolve().parent
EVIDENCE = ROOT / 'docs/evidence/broker/KL11-10'
SOURCE = 'd147bcf1c0164778bdbad625842363f3721bc10e'


def require(value, reason):
    if not value:
        raise ValueError(reason)


def pin(path):
    require(path.is_file() and not path.is_symlink(), 'regular pinned artifact')
    return {'path': str(path.relative_to(ROOT)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def read(path):
    return json.loads(path.read_text())


def main():
    named = {
        'storage_handoff': EVIDENCE / 'storage/final-validation.json',
        'whole_broker_qa': EVIDENCE / 'storage/final-d147bcf1/validation.json',
        'whole_broker_summary': EVIDENCE / 'storage/final-d147bcf1/summary.json',
        'independent_storage_oracle': EVIDENCE / 'oracle/storage/final-d147bcf1/validation.json',
        'independent_apache_components': EVIDENCE / 'oracle/apache/final-4ffd7557/validation.json',
        'independent_apache_wire': EVIDENCE / 'oracle/apache/wire/final-d147bcf1/validation.json',
        'independent_compiled_registry': EVIDENCE / 'oracle/apache/registry-gates/final-e1917eed/validation.json',
        'independent_live_registry': EVIDENCE / 'oracle/apache/registry-gates/live-e1917eed/validation.json',
        'actual_live_records': BASE / 'records-validation.json',
        'actual_reverse_apache_records': BASE / 'reverse-validation.json',
        'actual_api_versions': BASE / 'api-versions-validation.json',
        'complete_source_after_peers': BASE / 'source-after-peers.json',
        'original_server_provenance_used_by_live_driver': BASE / 'server-provenance.json',
        'java_preparation': BASE / 'java-build-attempt-1/validation.json',
        'native_preparation': BASE / 'native-build-attempt-1/validation.json',
        'public_rust_preparation': BASE / 'rust-build-matrix-attempt-1/validation.json',
        'owned_source_freeze': EVIDENCE / 'runtime/source-freeze.json',
        'development_failures': EVIDENCE / 'runtime/development/failure-index.json',
        'final_sealer_preparation_failure': BASE / 'sealer-attempt-1.json',
    }
    artifacts = {name: pin(path) for name, path in named.items()}
    data = {name: read(path) for name, path in named.items()}
    qa, summary = data['whole_broker_qa'], data['whole_broker_summary']
    require(qa['source_commit'] == SOURCE and summary['source_commit'] == SOURCE and
            len(qa['commands']) == 19 and all(c['exit_code'] == 0 for c in qa['commands']) and
            summary['test_passes'] == 1216 and summary['full_source_files'] == 44019, 'exact whole broker qualification')
    for name in ['independent_storage_oracle', 'actual_live_records', 'actual_reverse_apache_records',
                 'actual_api_versions', 'complete_source_after_peers', 'public_rust_preparation']:
        require(data[name]['passed'] and data[name]['source_sha'] == SOURCE, 'exact accepted runtime source ' + name)
    require(data['independent_live_registry']['input_aggregate_sha256'] == artifacts['actual_api_versions']['sha256'] and
            data['independent_live_registry']['passed'] and data['independent_live_registry']['actual_exchanges'] == 120,
            'independent actual profile proof')
    require(data['actual_reverse_apache_records']['input_receipt'] == artifacts['actual_live_records'], 'actual reverse proof input')
    require(data['complete_source_after_peers']['checked_git_blobs'] == 44019, 'complete final source scope')
    full_manifest = data['storage_handoff']['full_source_manifest']
    compressed = (ROOT / full_manifest['path']).read_bytes()
    require(hashlib.sha256(compressed).hexdigest() == full_manifest['sha256'] and
            hashlib.sha256(gzip.decompress(compressed)).hexdigest() == full_manifest['uncompressed_sha256'],
            'durable complete source manifest')
    require(artifacts['whole_broker_qa']['sha256'] == data['original_server_provenance_used_by_live_driver']['matrix_receipt_sha256'],
            'scratch and durable original QA receipt bytes identical')
    frozen = data['owned_source_freeze']
    require(len(frozen['inputs']) == 744, 'owned input denominator')
    for item in frozen['inputs']:
        path = ROOT / item['path']
        require(path.stat().st_size == item['bytes'] and pin(path)['sha256'] == item['sha256'], 'unchanged frozen owned input')
    limits = [
        'Explicit retention-enabled rolling Router only; selectable old4/5/7 profiles and monolithic/default contract remain separate. API21 supports versions0–2.',
        'Ordinary nontransactional/nonidempotent RF1 stores supply actual fsynced HW as both confirmed HW and retained-safe floor. Replication must supply a real safe retained floor before reuse; no fabricated ISR/quorum/transaction safety.',
        'Synchronous successful floor publication and completed cleanup provide a local durable contract. Actual Kafka UnifiedLog floor mutation does not synchronously checkpoint its start; local durability is stronger and separately proven.',
        'Whole containing Kafka batches remain physically present; Fetch below floor returns OFFSET_OUT_OF_RANGE, Fetch at floor can return the containing batch, and client decoding filters individual records below floor.',
        'Age requires strict now-maxTimestamp>age and a verified nonnegative maximum. Unknown/negative-time segments are preserved by age, unlike Apache mutable-file-mtime fallback; guarded size/DeleteRecords remain available.',
        'ListOffsets scans later bounded segments after floor filtering. Actual three-release Apache component returns no match for floor2/time1004 in the nonmonotonic seed, while local complete scan returns offset3; this difference is intentional.',
        'Retention is an explicit bounded sweep with optional age/payload-byte thresholds; no background timer, compaction, physical power-loss, exhaustive crash scheduling, production load or multi-replica qualification.',
        'Live tests use uncompressed ordinary CreateTime batches; all-feature Rust broker lanes are distinct from additional codec retention peer qualification, which is not claimed.',
        'Standalone public Rust peer all-features selects its own partitionline/tracing feature. Whole client all-feature qualification remains the independently sealed KL05-28/KL11-08 evidence.',
    ]
    counts = {
        'whole_broker_commands_passed': 19, 'whole_broker_test_passes': 1216,
        'four_cells_tests': [256, 352, 256, 352], 'compiled_wire_cases_per_cell': 363,
        'compiled_response_goldens_per_cell': 345, 'compiled_structural_rejections_per_cell': 18,
        'compiled_api_versions_cases_per_cell': 15, 'retention_fault_histories': 184, 'retention_fault_states': 368,
        'independent_storage_negative_controls': 32, 'independent_storage_positive_controls': 4,
        'additional_getter_scenarios_per_cell': 23,
        'independent_apache_component_assertions': 402, 'independent_apache_component_controlled_failures': 12,
        'independent_sdk_serializer_parser_checks': 1488, 'independent_global_error_helper_executions': 72,
        'independent_wire_corruption_controls': 9, 'compiled_registry_ci_tests': 55,
        'actual_tcp_api18_exchanges': 120, 'independent_live_registry_negative_controls': 17,
        'fresh_tcp_peer_jobs': 72, 'fresh_peer_assertions': 8460, 'clean_server_lifecycles': 8,
        'exact_retained_read_records': 372, 'actual_producer_receipts': 36, 'selected_live_v2_states': 24,
        'actual_reverse_apache_executions': 72, 'reverse_physical_record_comparisons': 540,
        'reverse_retained_record_comparisons': 324, 'source_git_blobs_checked_before_after': 44019,
        'owned_frozen_inputs_checked': 744,
    }
    result = {'schema_version': 1, 'task': 'KL11-10', 'source_sha': SOURCE, 'passed': True,
              'acceptance_complete': True, 'card_close_eligible': True,
              'scope': 'Bounded ordinary RF1 retention/DeleteRecords and durable monotonic logical floor, exact-source stable/MSRV default/all-feature behavioral/fault/strict matrix plus independent Apache/wire/actual Java-native-public Rust restart proof.',
              'counts': counts, 'artifacts': artifacts, 'full_source_manifest': full_manifest,
              'accepted_cells': summary['cells'], 'limits': limits,
              'failure_history': {'accepted_final_runtime_failures': 0,
                                 'unexpected_development_failure_groups': len(data['development_failures']['failures']),
                                 'additional_evidence_sealer_preparation_failure': artifacts['final_sealer_preparation_failure'],
                                 'controlled_faults_and_negative_oracles': 'Expected named failures remain separate from unexpected development/preparation failures.',
                                 'storage_and_shared_decoder_development': 'Retained unchanged in storage and Apache evidence, including failing-first scalar getter case and first wrapper array/dict mismatch.'},
              'durability_bridge': 'The original scratch QA receipt used by every actual live lane is byte-identical to the durable storage receipt; original live source-provenance file remains unchanged.',
              'retained_outside_Git': {'broker_binaries': '/workspace/work/retention-final-d147bcf1/bin',
                                       'peer_binaries_and_classes': '/workspace/work/retention-final-peers',
                                       'complete_source': '/workspace/work/retention-final-d147bcf1/source',
                                       'public_rust_cargo_cache_cleaned_after_binary_verification': True,
                                       'no_required_runtime_dependency_on_cleaned_cargo_target': True},
              'commands': [{'name': c['name'], 'argv': c['argv'], 'exit_code': c['exit_code'], 'log_sha256': c['log_sha256']}
                           for c in qa['commands']],
              'peer_command_receipts': 'All exact Java/native/Rust preparation,72 TCP peer/server commands and72 reverse decoder commands are retained in the pinned constituent receipts; no hidden retry.',
              'checker_sha256': pin(Path(__file__))['sha256'],
              'checksum_verification_command': ['sha256sum', '-c', 'docs/evidence/broker/KL11-10/runtime/final/SHA256SUMS']}
    output = BASE / 'validation.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    card = {'schema_version': 1, 'task': 'KL11-10', 'source_sha': SOURCE,
            'acceptance_complete': True, 'disposition': 'accepted bounded ordinary RF1 retention/DeleteRecords',
            'evidence': pin(output), 'counts': counts, 'artifacts': artifacts,
            'commands': result['commands'], 'limits': limits,
            'acceptance_mapping': [
                {'requirement': 'Do not delete retained/unreplicated records; readers see correct offset-out-of-range behavior.',
                 'proof': ['DeletionGuard validates requested floor<=confirmed HW and retained-safe floor; ordinary RF1 Store uses actual fsynced durable end.',
                           'HW/protected-bounds and resource/error cases, whole containing batches, Fetch below floor/OOR, ListOffsets earliest/timestamp floor filtering; all four compiled lanes.',
                           'Actual Java/native/public Rust API21/delete/consumer/list-offset behaviors across seed/restart; exact retained bytes/hash equality with three Apache decoders.']},
                {'requirement': 'Restart/partial deletion histories preserve a monotonic log start.',
                 'proof': ['Atomic checksummed V2 floor/selected/victim publication before bounded unlink; uncertain I/O poisons and recovery completes recorded cleanup.',
                           '184 actual interruption/IO histories/368 states plus32 independent negative and4 positive controls.',
                           '23 poisoned scalar getter/publication/reopen scenarios per cell; fresh eight clean live seed/restart servers retain floor3 and UUID/catalog/containing batch bytes.']},
                {'requirement': 'Record exact tested source, commands, case counts, failures and limitations.',
                 'proof': ['Full44019 Git blobs, SHA256, sizes and modes checked before/after19 broker commands and after all peer work.',
                           'All raw attempt/source/command/build/binary/outcome/checksum evidence retained; independent component/wire/runtime sources and deliberate policy differences labeled.']},
            ]}
    card_path = ROOT / 'docs/plan/evidence/KL11-10.json'
    card_path.write_text(json.dumps(card, indent=2) + '\n')
    all_files = [p for p in BASE.rglob('*') if p.is_file() and '__pycache__' not in p.parts
                 and p.name not in ['artifacts.json', 'SHA256SUMS']]
    all_files.append(card_path)
    inventory = {str(p.relative_to(ROOT)): {'sha256': pin(p)['sha256'], 'bytes': p.stat().st_size,
                                          'mode': '100755' if p.stat().st_mode & 0o111 else '100644'}
                 for p in sorted(all_files)}
    inventory_path = BASE / 'artifacts.json'
    inventory_path.write_text(json.dumps({'schema_version': 1, 'source_sha': SOURCE, 'files': inventory,
                                          'safe_stage_files': sorted(inventory),
                                          'exclude': ['All __pycache__/.pyc files, scratch binaries/classes/targets/source archives.'],
                                          'checksum_scope': 'All owned final runtime raw/build/record evidence plus exact plan evidence card; delegated storage/oracle trees retain their own manifests.'}, indent=2) + '\n')
    all_files.append(inventory_path)
    sums = BASE / 'SHA256SUMS'
    sums.write_text(''.join(f"{pin(p)['sha256']}  {p.relative_to(ROOT)}\n" for p in sorted(all_files)))
    print(json.dumps({'passed': True, 'card_close_eligible': True, 'validation': pin(output),
                      'card': pin(card_path), 'checksummed_files': len(all_files)}))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Independent finite bytes/source review; does not execute broker artifacts."""
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import tarfile

sys.dont_write_bytecode = True
ROOT = Path('/workspace/work/raft-runtime-76')
ACTUAL = ROOT / 'development/incoming-lifetime-candidate47a9-01'
SUP = ACTUAL / 'retention-supplement-01'
PROPOSAL = ROOT / 'coverage-proposal-01'
OUTPUT = ROOT / 'incoming-image-candidate-review-01'
ORACLE = Path('/workspace/partitionline/docs/evidence/broker/KL11-74/oracle/history/membership_raw.py')
FIXED = ORACLE.parents[3] / 'KL11-15/oracle/history/wal_oracle.py'


def need(ok, reason):
    if not ok:
        raise ValueError(reason)


def metadata(path):
    st = path.lstat()
    need(stat.S_ISREG(st.st_mode) and not path.is_symlink(), 'regular input: ' + str(path))
    need(st.st_size <= 32 * 1024 * 1024, 'bounded single input')
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            h.update(chunk)
    return {'sha256': h.hexdigest(), 'bytes': st.st_size, 'full07777': stat.S_IMODE(st.st_mode)}


def inventory(root, cap=100):
    paths = sorted(p for p in root.rglob('*') if p.is_file())
    need(len(paths) <= cap and all(not p.is_symlink() for p in root.rglob('*')), 'bounded regular pathset')
    return {str(p.relative_to(root)): metadata(p) for p in paths}


def cite(path, begin, end):
    return {'path': str(path), 'sha256': metadata(path)['sha256'], 'start_line': begin,
            'end_line': end, 'text': '\n'.join(path.read_text().splitlines()[begin - 1:end])}


def main():
    need(tuple(sorted(os.sched_getaffinity(0))) == (2, 4), 'review affinity')
    core = PROPOSAL / 'candidate/partitionline-broker/src/raft/replication.rs'
    tests = PROPOSAL / 'candidate-selective66-review-03/partitionline-broker/tests/raft_membership.rs'
    old_tests = PROPOSAL / 'before/partitionline-broker/tests/raft_membership.rs'
    old_review = ROOT / 'incoming-image-review-01/validation.json'
    inputs = [ACTUAL / 'validation.json', SUP / 'validation.json',
        ACTUAL / 'selected-source-manifest.json',
        ACTUAL / 'candidate-four-regressions/command.log',
        ACTUAL / 'candidate-four-regressions/command.exit',
        ACTUAL / 'candidate-four-regressions/disk-monitor.jsonl',
        ACTUAL / 'package-clean/command.log', ACTUAL / 'package-clean/command.exit',
        ACTUAL / 'package-clean/disk-monitor.jsonl', core, tests, old_tests, old_review, ORACLE, FIXED,
        SUP / 'post-candidate-retention-only-cache-map.json',
        SUP / 'post-candidate-retention-only-all-cache.tar.gz']
    before = {str(p): metadata(p) for p in inputs}
    pins = {str(inputs[0]): '3778abf641eb43a6c0b461a8744758c9d8fc6be892bf293f9b2b6a1524aa7ae6',
            str(inputs[1]): 'bd4001cf31681f7e4f0f05eb59753f4756b10dbc66b9fbea83e0a565794077dc',
            str(core): '47a9b89cc7f0c24e667f0ba8433d1b6beca3fdf2c6692099747ac80c0a5020e0',
            str(tests): 'a4ffda2a35c905c9510278feac6a404c8caf12ff4c1d000fcad38da9b3e9a77d',
            str(old_review): '48d9b8a7e9b2b46c26279446d0e4ef69b6604932c716fc4813df8be2b03ac5c3'}
    for p, sha in pins.items():
        need(before[p]['sha256'] == sha, 'exact receipt/source pin: ' + p)
    original = json.loads(inputs[0].read_bytes())
    supplement = json.loads(inputs[1].read_bytes())
    need('executed ELF missing before retention' in original['runner_outcome'], 'post-test parser failure retained')
    need(supplement['original_failed_driver_receipt']['sha256'] == pins[str(inputs[0])] and
         supplement['original_failed_receipt_unchanged'], 'supplement exact original binding')
    need(supplement['commands'] == supplement['operations'] == [], 'retention supplement no Cargo rerun')
    need(len(original['commands']) == 2, 'two actual original commands')
    clean, test = original['commands']
    need(clean['name'] == 'package-clean' and test['name'] == 'candidate-four-regressions' and
         clean['exit_code'] == test['exit_code'] == 0, 'actual clean+test exit0')
    raw_logs = {}
    for command in [clean, test]:
        root = ACTUAL / command['name']
        log = (root / 'command.log').read_text()
        raw_logs[command['name']] = log
        need(metadata(root / 'command.log')['sha256'] == command['log_sha256'] and
             (root / 'command.exit').read_text().strip() == '0', 'actual log/exit binding')
        rows = [json.loads(line) for line in (root / 'disk-monitor.jsonl').read_text().splitlines()]
        monitor = command['disk_monitor']
        need(metadata(root / 'disk-monitor.jsonl')['sha256'] == monitor['sample_log_sha256'] and
             len(rows) == monitor['samples'] and rows[0]['pre_launch'] and rows[-1]['process_completed'] and
             min(r['free_bytes'] for r in rows) == monitor['minimum_free_bytes'] and
             monitor['actual_process_exit_code'] == 0 and not monitor['triggered'] and
             all(r['free_bytes'] >= 367001600 and not r['below_reserve'] for r in rows), 'raw disk guard binding')
        need(command['source_before'] == command['source_after'] == supplement['final_source_guards'],
             'original commands+supplement same full/selective guards')
    log = raw_logs[test['name']]
    test_names = [
        'incoming_dynamic_image_old_offer_cannot_survive_a_real_higher_term_known_leader',
        'incoming_dynamic_image_publication_io_error_poison_fences_late_chunks_and_finish',
        'incoming_dynamic_image_rejects_changed_offer_and_abort_or_expiry_cannot_revive_it',
        'incoming_dynamic_image_survives_follower_polls_and_preserves_local_vote_on_reopen']
    need(all('test ' + name + ' ... ok' in log for name in test_names) and
         'test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 27 filtered out' in log,
         'actual exact four results')
    fresh = [line for line in log.splitlines() if ' Running `' in line and
             ('rustc --crate-name partitionline_broker ' in line or 'rustc --crate-name raft_membership ' in line)]
    need(len(fresh) == 2 and any('--crate-type lib' in line for line in fresh) and
         any(' tests/raft_membership.rs ' in line and ' --test ' in line for line in fresh),
         'actual fresh library/test compiler lines')
    removed = clean['actual_removed_package_outputs']
    need('debug/deps/libpartitionline_broker-b1c41f23d75d8ad5.rlib' in removed and
         'debug/deps/raft_membership-df70681fff0cc180' in removed, 'previous lib/harness actually removed')
    need(tests.read_bytes().startswith(old_tests.read_bytes().rstrip()), 'original27 tests byte-preserved prefix')

    source = Path(original['source_tree'])
    source_before = inventory(source)
    manifest = json.loads(inputs[2].read_bytes())
    need(len(manifest) == len(source_before) == 66 and set(source_before) == set(manifest), 'selective66 pathset')
    for relative, row in manifest.items():
        expected = {'sha256': row['sha256'], 'bytes': row['bytes'], 'full07777': row['full_permission_mode']}
        need(source_before[relative] == expected, 'selected bytes/modes: ' + relative)
        data = (source / relative).read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        need(blob == row['git_blob_sha1'], 'selected exact manifest blob: ' + relative)
    need(source_before['partitionline-broker/src/raft/replication.rs'] == before[str(core)] and
         source_before['partitionline-broker/tests/raft_membership.rs'] == before[str(tests)], 'actual candidate compiler inputs')
    need(supplement['before'] == supplement['after_elf_retention'] == supplement['final_source_guards'], 'supplement guard closure')

    objects_before = {}
    uncompressed = {}
    for row in supplement['retained_elf_objects']:
        p = Path(row['path'])
        objects_before[str(p)] = metadata(p)
        need(objects_before[str(p)]['sha256'] == row['sha256'] and row['decompression_verified'], 'retained object compressed SHA')
        size, h = 0, hashlib.sha256()
        with gzip.open(p, 'rb') as stream:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                need(size <= row['uncompressed_bytes'] <= 16 * 1024 * 1024, 'retained ELF bounded decompression')
                h.update(chunk)
        need(size == row['uncompressed_bytes'] and h.hexdigest() == row['uncompressed_sha256'], 'retained ELF exact logical bytes')
        uncompressed[row['uncompressed_sha256']] = size
    cache_map = json.loads(inputs[15].read_bytes())
    cache_retention = supplement['whole_cache_retention'][0]
    need(before[str(inputs[15])]['sha256'] == cache_retention['map_sha256'] and
         before[str(inputs[16])]['sha256'] == cache_retention['archive_sha256'], 'actual cache delta hashes')
    need(len(supplement['all_current_elfs']) == 117, '117 actual retained paths')
    for row in supplement['all_current_elfs']:
        relative = str(Path(row['original_path']).relative_to('/workspace/work/target-broker-segments'))
        mapped = cache_map[relative]
        need(mapped == {'type': 'file', 'bytes': row['bytes'], 'sha256': row['sha256'], 'full_mode': row['original_mode']} and
             uncompressed[row['sha256']] == row['bytes'] and row['pre_clean_bytes_and_gzip_verified'],
             'all117 mode/byte/map/object closure')
    delta_seen = {}
    with tarfile.open(inputs[16], 'r:gz') as archive:
        for member in archive:
            need(member.isfile() and not member.issym() and not member.islnk() and
                 member.size <= 16 * 1024 * 1024, 'only bounded actual new cache regular files')
            stream = archive.extractfile(member)
            h, size = hashlib.sha256(), 0
            while chunk := stream.read(1024 * 1024):
                h.update(chunk)
                size += len(chunk)
            delta_seen[member.name] = {'type': 'file', 'bytes': size, 'sha256': h.hexdigest(), 'full_mode': member.mode & 0o7777}
    need(delta_seen == cache_retention['delta_rows'] and len(delta_seen) == 3, 'three exact new cache delta rows bytes/fullmode')
    harness = supplement['actual_executed_harness'][0]
    need(harness['sha256'] == '4f9e065b5480d6c3dc260fc62051710be645c561856ca0eebe62eff1d56d32a2' and
         harness['bytes'] == 3692320 and harness['original_mode'] == 0o700 and
         'Running `/workspace/work/target-broker-segments/debug/deps/raft_membership-df70681fff0cc180 incoming_dynamic_image_' in log,
         'actual new harness bytes/fullmode/execution')

    raw_before = inventory(ACTUAL / 'captures')
    need(len(raw_before) == 66 and sum(r['bytes'] for r in raw_before.values()) == 73679 and
         set(raw_before) == set(supplement['capture_files']), 'six actual phases66 files/73679B')
    for relative, row in raw_before.items():
        expected = supplement['capture_files'][relative]
        need(row == {'sha256': expected['sha256'], 'bytes': expected['bytes'], 'full07777': expected['full_mode']}, 'actual capture mode/SHA/bytes')
    spec = importlib.util.spec_from_file_location('candidate_review_membership_raw', ORACLE)
    oracle = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = oracle
    spec.loader.exec_module(oracle)
    phases = ['before-poll', 'after-poll', 'after-first-chunk', 'after-finish', 'after-reopen', 'voted-after-reopen']
    decoded, facts = {}, {}
    for phase in phases:
        root = ACTUAL / 'captures' / phase
        need(sum(p.startswith(phase + '/') for p in raw_before) == 11, '11files/phase')
        values = {}
        for line in (root / 'actual-facts.txt').read_text().splitlines()[1:]:
            k, value = line.split('=', 1)
            values[k] = value
        facts[phase] = values
        rows = {}
        for node in range(1, 5):
            stem = str(node) + '-' + bytes([node + 1] * 16).hex()
            content = oracle.read_content(root / (stem + '.wal'), root / (stem + '.images'))
            election = oracle.read_election(root / (stem + '.election'), content)
            rows[str(node)] = {'local': content.local, 'tail': content.tail(), 'committed': content.committed,
                'selected': content.selected, 'records': [r.receipt() for r in content.rows],
                'operations': content.operations, 'election': election}
        decoded[phase] = rows
    for phase in phases[:3]:
        peer = decoded[phase]['4']
        need(peer['tail'] == (0, 0) and peer['committed'] == 0 and peer['selected'] is None and
             len(peer['operations']) == 1 and peer['election']['final']['term'] == 2 and
             peer['election']['final']['vote'] is None, 'early observer no install/vote mutation')
    need(decoded['before-poll'] == decoded['after-poll'] == decoded['after-first-chunk'], 'early durable states identical')
    first = ACTUAL / ('captures/after-first-chunk/4-' + '05' * 16 + '.images/snapshot-' + '3d' * 16 + '.partial')
    donor = ACTUAL / ('captures/after-first-chunk/1-' + '02' * 16 + '.images/snapshot-' + '3d' * 16 + '.image')
    need(first.read_bytes() == donor.read_bytes()[:128] and first.stat().st_size == 128,
         'physical first128B partial equals exact donor image prefix')
    need(facts['after-first-chunk']['outcome'] == 'Ok(())', 'actual first Chunk success')
    image_metadata = {}
    for phase in phases[3:]:
        receiver = '3' if phase == 'voted-after-reopen' else '4'
        peer = decoded[phase][receiver]
        leader = decoded[phase]['1']
        need(peer['selected'] == leader['selected'] and peer['tail'] == (2, 2) and peer['committed'] == 2 and
             peer['records'] == leader['records'] and len(peer['records']) == 2 and
             peer['records'][1]['payload_hex'] == b'committed image payload'.hex(), 'real selected image+record extent')
        need([op['opcode'] for op in peer['operations']] == [1, 5], 'receiver actual initialization+Install WAL')
        auth = peer['operations'][-1]['authority']
        need(auth['kind'] == 1 and auth['term'] == 2 and auth['leader'] == {'id': 1, 'directory': '02' * 16} and
             auth['peer'] == peer['local'] and auth['leader_commit'] == 2 and auth['configuration_epoch'] == 0 and
             auth['sequence'] == (6 if receiver == '3' else 7), 'actual Install full directory/sequence authority')
        generation = '3e' if receiver == '3' else '3d'
        local_stem = receiver + '-' + bytes([int(receiver) + 1] * 16).hex()
        local_image = ACTUAL / ('captures/' + phase + '/' + local_stem + '.images/snapshot-' + generation * 16 + '.image')
        donor_image = ACTUAL / ('captures/' + phase + '/1-' + '02' * 16 + '.images/snapshot-' + generation * 16 + '.image')
        need(local_image.read_bytes() == donor_image.read_bytes() and local_image.stat().st_size == 431,
             'actual selected image exact donor bytes431')
        need(peer['election']['final']['term'] == 2 and peer['election']['final']['log'] == (2, 2), 'local election exact reconciled log')
        image_metadata[phase] = {'descriptor': peer['selected'], 'selected_file': metadata(local_image),
                                'authority': auth, 'receiver': receiver}
        need(not list((local_image.parent).glob('*.partial')), 'selected image no partial after completion')
    need(decoded['after-finish'] == decoded['after-reopen'], 'all durable node states preserved through observer reopen')
    voter = decoded['voted-after-reopen']['3']
    actual_vote = {'id': 1, 'directory': '02' * 16}
    states = voter['election']['states']
    need(states[-1]['vote'] == actual_vote and any(s['term'] == 2 and s['vote'] == actual_vote and
         s['log'] == (0, 0) for s in states[:-1]) and
         all(s['vote'] == actual_vote for s in states if s['term'] == 2), 'actual prior non-null same-term vote survives Install/reopen')
    candidate_text = tests.read_text()
    rejection = candidate_text.split('fn incoming_dynamic_image_rejects_changed_offer_and_abort_or_expiry_cannot_revive_it()')[1].split('\n#[test]')[0]
    need(rejection.count('forged.push(q)') == 15 and 'Err(replication::Error::InvalidPeer)' in rejection and
         'group.node(3)?.state().ready' in rejection and '!group.node(3)?.state().poisoned' in rejection,
         '15 exact typed rejection/source assertions')
    minimum_chunk_visits = (431 + 128 - 1) // 128
    need(minimum_chunk_visits == 4 and 'const POLLED_IMAGE_CHUNK_BYTES: usize = 128;' in candidate_text and
         'assert!(voting_chunks >= 3)' in candidate_text, 'successful source-bound loops need at least4 actual chunks')

    after = {str(p): metadata(p) for p in inputs}
    objects_after = {p: metadata(Path(p)) for p in objects_before}
    need(before == after and objects_before == objects_after and source_before == inventory(source) and
         raw_before == inventory(ACTUAL / 'captures'), 'all reviewed inputs/captures/source/object metadata exact unchanged')
    receipt = {'schema_version': 1, 'task': 'KL11-76 candidate incoming-image finite independent review',
        'scope': 'actual selected66 core experiment; independent raw state/object verification plus source-bound executed assertions',
        'affinity': [2, 4], 'new_Cargo_SDK_broker_runtime_Git_repo_mutations': 0,
        'baseline_source_commit': original['source_commit'], 'candidate_core_sha256': pins[str(core)],
        'candidate_test_sha256': pins[str(tests)], 'input_before': before, 'input_after': after,
        'actual_source_66_before_and_after': source_before, 'actual_raw_66_before_and_after': raw_before,
        'original_driver_failure_preserved': original['runner_outcome'],
        'original_actual_commands': [{k: c[k] for k in ['name', 'argv', 'exit_code', 'elapsed_seconds', 'source_before', 'source_after', 'log_sha256', 'disk_monitor']} for c in original['commands']],
        'retention_only_supplement': {'receipt': before[str(inputs[1])], 'commands': 0, 'operations': 0,
            'retained_ELF_paths': 117, 'unique_current_logical_ELFs': len({r['sha256'] for r in supplement['all_current_elfs']}),
            'retained_gzip_objects_independently_verified': len(objects_before), 'object_metadata_before': objects_before,
            'object_metadata_after': objects_after, 'whole_current_cache_map_rows': len(cache_map),
            'new_delta_rows_actual_archive_verified': delta_seen, 'actual_harness': harness,
            'source_guards': supplement['final_source_guards']},
        'fresh_compiler_lines': fresh, 'original27_tests_byte_preserved_prefix': True,
        'actual_four_tests': supplement['actual_four_tests'], 'facts': facts,
        'independent_decoded_node_phases': decoded, 'selected_image_checks': image_metadata,
        'physical_first_chunk': {'path': str(first), 'bytes': 128, 'exact_donor_prefix': True},
        'minimum_successful_chunk_visits_from_actual431B_extent_and_source128Bcap': 4,
        'all_reviewed_input_bytes_full07777_pathsets_unchanged': True,
        'source_assertion_evidence': [cite(tests, 1917, 2010), cite(tests, 2010, 2138),
                                    cite(tests, 2221, 2465)],
        'acceptance_support': [
            {'claim': 'Four genuine candidate tests pass; lib and harness freshly compiled after package clean',
             'support': 'actual verbose rustc lines, exact test log/results, actual exit0, removed baseline lib/harness rows, matched selected66 source and executed harness object',
             'limit': 'original wrapper fails later during ELF-log parsing; supplement is retention-only, not a passing rerun'},
            {'claim': 'Unchanged follower polls preserve incoming transfer; ordinary image completes and reopens',
             'support': 'first128B actual partial exact donor prefix; after-Finish receiver WAL has remote Install opcode5/full directory/sequence; selected431B image exact donor; election log reconciles to(2,2); after-reopen durable bytes/states preserved',
             'limit': 'direct synchronous localcore path; no completed TCP Begin/Chunk/Finish/Finished stream'},
            {'claim': 'Genuine non-null same-term vote survives successful image install/reopen',
             'support': 'receiver3 actual election history has earlier term2 Some(Key1,dir02)/empty log and same vote through reconciled(2,2) final state; selected gen3e431B image and Install exact source authority',
             'limit': 'finite one voter fixture, not exhaustive election safety proof'},
            {'claim': 'All15 changed offer Context/request/descriptor variants yield typed InvalidPeer without poisoning or durable counter changes, followed by valid completion/reopen',
             'support': 'executed source assertions in actual passed test:15 forged values, typed Chunk+Finish rejections, ready/unpoisoned per variant, persistent/WAL/election counters compared afterall; successful valid Install/reopen afterward',
             'limit': 'these negative variants are not separately raw captured; independent decoder does not claim15 raw histories'},
            {'claim': 'Receiver deadline equality accepted, +1 rejects and partial disappears; explicit abort/campaign/higher term/IO poison fence old traffic',
             'support': 'actual passed source assertions; equality performs Finish, +1 checks broad error/no selected/no partial; campaign checks Candidate/newterm and typed errors; IO publication blocker checks poison/notready',
             'limit': '+1/higher-term/poison late errors use is_err rather than all typed InvalidPeer; no independent raw captures for those subcases'}],
        'verdict': 'Finite byte/state review supports installing narrow core47a9 fix and test a4ff; no additional source-level blocker found.',
        'remaining_gaps': ['Full original wrapper success is not claimed; post-test parsing failure remains immutable.',
            'Original full73171 guard is retained provenance; this reviewer independently rechecks selected66, captures66 and retained objects, not whole675MBsource.',
            'Completed large actualTCP Finish/Finished, non-genesis runtime observer lifecycle, exact timeout targets/errors, resource peaks/join proof remain separate KL11-76 qualification.',
            'Configuration-only authority loss may leave bounded staged data until deadline/disconnect/abort; documented scope, no immediate cleanup claim.',
            'No SDK/KRaft compatibility, performance, crash/power-loss or full task closure claim.']}
    data = json.dumps(receipt, sort_keys=True, indent=2).encode() + b'\n'
    need(len(data) <= 2 * 1024 * 1024, 'small reviewer receipt')
    target = OUTPUT / 'validation.json'
    need(not target.exists(), 'fresh WORK reviewer receipt')
    with target.open('xb') as stream:
        stream.write(data)
    target.chmod(0o600)
    print(json.dumps({'path': str(target), 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
        'full07777': 0o600, 'raw_phases': 6, 'raw_files': 66, 'decoded_node_phases': 24,
        'retained_ELF_paths': 117, 'actual_new_Cargo_SDK_runtime_invocations': 0}))


if __name__ == '__main__':
    main()

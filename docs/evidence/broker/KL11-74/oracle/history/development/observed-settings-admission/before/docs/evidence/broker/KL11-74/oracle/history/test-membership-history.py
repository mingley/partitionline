#!/usr/bin/env python3
"""Semantic counterexamples with valid framing/checksums and untouched originals."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import tempfile

import membership_raw as raw

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('membership_history', HERE / 'check-membership-history.py')
history = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(history)


def checksums(entries):
    header = b'PLJRNL01' + bytes(12)
    out = bytearray(header + struct.pack('>I', raw.crc32c(header)))
    for index, payload in enumerate(entries):
        header = b'PLENTRY1' + struct.pack('>IQII', len(payload), index, 1, raw.crc32c(payload))
        out.extend(header + struct.pack('>I', raw.crc32c(header)) + payload)
    return bytes(out)


def fingerprint(base):
    return {str(p.relative_to(base)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mode & 0o7777)
            for p in base.rglob('*') if p.is_file()}


def event(trace, kind, predicate=lambda e: True):
    return next(e for e in trace['events'] if e['kind'] == kind and predicate(e))


def actor(event_):
    return next(s for s in event_['after'] if s['key'] == event_['key'])


def mutate_trace(name, t):
    if name == 'invented_ack_sequence':
        event(t, 'acknowledge')['args']['sequence'] += 10000
        return 'ACK actual once-only emitted correlation'
    if name == 'wrong_response_target':
        e = event(t, 'receive', lambda e: e['result']['success'] and e['result']['matched']['index'] > 0)
        e['result']['matched']['index'] += 1
        return 'response exact request target'
    if name == 'changed_sender_configuration_epoch':
        event(t, 'prepare')['result']['context']['configuration_epoch'] += 1
        return 'append emitted authority/configuration'
    if name == 'changed_sender_committed_floor':
        event(t, 'prepare')['result']['leader_commit'] += 1
        return 'append emitted authority/configuration'
    if name == 'invented_source_record':
        e = event(t, 'prepare', lambda e: any(r['kind'] == 0 for r in e['result']['entries']))
        next(r for r in e['result']['entries'] if r['kind'] == 0)['payload_hex'] = 'ab'
        return 'append emitted exact source records'
    if name == 'wrong_vote_directory':
        event(t, 'vote_request')['args']['context']['peer']['directory'] = '63' * 16
        return 'vote emitted request correlation'
    if name == 'wrong_vote_receiver':
        event(t, 'vote_request')['result']['voter'] += 100
        return 'vote emitted request correlation'
    if name == 'old_majority_addition_commit':
        e = event(t, 'acknowledge', lambda e: actor(e)['voters']['epoch'] == 1 and
                  e['args']['term'] == 2 and e['args']['matched']['index'] == 3)
        e['result']['committed_end'] = 3
        return 'local commit distinct NEW-set/current-term majority'
    if name == 'removed_self_authorizes_commit':
        e = event(t, 'acknowledge', lambda e: actor(e)['voters']['epoch'] == 2 and
                  e['args']['matched']['index'] == 5 and e['result']['committed_end'] == 4)
        e['result']['committed_end'] = 5
        return 'local commit distinct NEW-set/current-term majority'
    if name == 'caller_clock_regression':
        t['events'][10]['now_ms'] = 0
        return 'caller clock regression'
    if name == 'wrong_observed_exit':
        event(t, 'process_exit_observed')['result']['observed_exit_code'] = 0
        return 'actual process exit/clock handoff'
    if name == 'lost_clock_handoff':
        e = event(t, 'process_exit_observed')
        e['now_ms'] = t['events'][e['ordinal'] - 1]['now_ms']
        return 'actual process exit/clock handoff'
    if name == 'wrong_feature_advertisement':
        event(t, 'feature_receive')['result']['voter']['kraft_max'] = 0
        return 'feature receive emitted correlation'
    if name == 'wrong_change_receipt_epoch':
        event(t, 'add_voter')['result']['epoch'] += 1
        return 'change receipt epoch'
    if name == 'invented_change_position':
        event(t, 'add_voter')['result']['position']['index'] += 1
        return 'configuration appended receipt position'
    if name == 'uncommitted_change_reports_committed':
        event(t, 'add_voter')['result']['committed'] = True
        return 'configuration appended receipt position'
    if name == 'wrong_stream_bytes':
        e = event(t, 'image_chunk_receive')
        e['args']['bytes_hex'] = 'ff' + e['args']['bytes_hex'][2:]
        return 'image stream emitted contiguous bytes'
    if name == 'wrong_stream_offset':
        event(t, 'image_chunk_receive')['args']['offset'] += 1
        return 'image stream emitted contiguous bytes'
    if name == 'undeclared_changed_response':
        event(t, 'acknowledge_mutated')['args']['response']['sequence'] += 1
        return 'only declared response field changed/rejected'
    if name == 'wrong_checkpoint_count':
        t['checkpoints'][0]['wal_confirmed_ops'] += 1
        return 'checkpoint actual durable denominators'
    if name == 'removed_leader_authority':
        event(t, 'removed_leader_propose')['after'][0]['active_term'] = 3
        return 'committed removed leader fencing'
    raise ValueError(name)


CAUSAL = ['invented_ack_sequence', 'wrong_response_target', 'changed_sender_configuration_epoch',
          'changed_sender_committed_floor', 'invented_source_record', 'wrong_vote_directory',
          'wrong_vote_receiver', 'old_majority_addition_commit', 'removed_self_authorizes_commit',
          'caller_clock_regression', 'wrong_observed_exit', 'lost_clock_handoff',
          'wrong_feature_advertisement', 'wrong_change_receipt_epoch', 'invented_change_position',
          'uncommitted_change_reports_committed', 'wrong_stream_bytes', 'wrong_stream_offset',
          'undeclared_changed_response', 'wrong_checkpoint_count', 'removed_leader_authority']


def rejected(call, expected):
    try:
        call()
    except raw.Rejected as error:
        raw.require(str(error) == expected, f'wrong rejection guard: expected {expected!r}, got {str(error)!r}')
        return str(error)
    raise AssertionError('semantic control was accepted')


def raw_controls(base, trace, scratch):
    result = []
    c = next(c for c in trace['checkpoints'] if c['phase'] == 'final-reopen' and c['key']['id'] == 1)
    content_path, election_path = base / c['wal_path'], base / c['election_path']
    content = raw.read_content(content_path, base / c['images_dir'], c['key'], trace['group'])
    original_entries = raw.read_journal_entries(content_path)
    append_ordinal = next(i for i, p in enumerate(original_entries) if p[8] == 2 and
                          content.operations[i]['source_operation'] == i)
    for name in ('result_descriptor_changed', 'local_commit_envelope_changed', 'wrong_receiver_directory',
                 'voter_record_position_changed'):
        entries = original_entries.copy()
        p = bytearray(entries[append_ordinal])
        size = int.from_bytes(p[16:20], 'big')
        authority, body = 24 + size, 104 + size
        if name == 'result_descriptor_changed':
            # Legal positive endpoint port in the wrapper, with record bytes unchanged.
            offset = 24 + 40 + 28 + 4
            p[offset:offset + 2] = struct.pack('>H', int.from_bytes(p[offset:offset + 2], 'big') + 1)
            expected = 'forged resulting configuration'
            raw.decode_configuration(bytes(p[24:24 + size]))
        elif name == 'local_commit_envelope_changed':
            p[authority + 32:authority + 40] = struct.pack('>Q', int.from_bytes(p[authority + 32:authority + 40], 'big') + 1)
            expected = 'local authority/configuration/commit'
        elif name == 'wrong_receiver_directory':
            p[authority + 56:authority + 72] = bytes.fromhex('63' * 16)
            expected = 'authority kind/term/intended directory'
        else:
            # Typed payload starts after Append16 and record24; position index is24 in PLVOTR01.
            offset = body + 16 + 24 + 24
            p[offset:offset + 8] = struct.pack('>Q', int.from_bytes(p[offset:offset + 8], 'big') + 1)
            expected = 'append voter exact position'
        entries[append_ordinal] = bytes(p)
        path = scratch / (name + '.wal')
        path.write_bytes(checksums(entries))
        raw.read_journal_entries(path)  # All framing/counts/checksums accepted before semantic rejection.
        guard = rejected(lambda: raw.read_content(path, base / c['images_dir'], c['key'], trace['group']), expected)
        result.append({'name': name, 'guard': guard, 'all_outer_checksums_valid': True,
                       'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    # Rebind an entire historical equal-view interval, leaving the final genuine view unchanged.
    source = append_ordinal
    decoy = next(i for i in range(source + 1, len(content.operations)) if
                 content.operations[i]['view'] == content.operations[source]['view'] and
                 content.operations[i]['source_operation'] != i)
    entries = raw.read_journal_entries(election_path)
    changed = []
    for i, payload in enumerate(entries):
        if payload[:8] in (b'PLELECT2', b'PLRECON2', b'PLELCFG2') and int.from_bytes(payload[76:84], 'big') == source:
            p = bytearray(payload)
            p[76:84] = struct.pack('>Q', decoy)
            entries[i] = bytes(p)
            changed.append(i)
    raw.require(changed and int.from_bytes(entries[-1][76:84], 'big') != decoy,
                'historical interval control preserves genuine final context')
    path = scratch / 'historical_equal_view_origin.wal'
    path.write_bytes(checksums(entries))
    raw.read_journal_entries(path)
    guard = rejected(lambda: raw.read_election(path, content), 'election source must be actual configuration-changing operation')
    result.append({'name': 'historical_equal_view_origin', 'guard': guard, 'all_outer_checksums_valid': True,
                   'changed_election_frames': changed, 'original_source': source, 'same_view_decoy': decoy,
                   'final_original_context_preserved': True, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    return result


def supplementary_cases(base, trace, scratch):
    """Real byte mutants that the preserved pre-fix predicates accepted.

    Each yielded call is consumed before advancing this generator. Causal
    variants change every copied occurrence of one actual WAL operation, so
    unchanged-prefix checks cannot conceal a missing emitted-request binding.
    """
    removed = next(c for c in trace['checkpoints'] if c['phase'] == 'final-reopen' and c['key']['id'] == 1)
    content = raw.read_content(base / removed['wal_path'], base / removed['images_dir'], removed['key'], trace['group'])
    elected = raw.read_election(base / removed['election_path'], content)['final']
    raw.require(elected['vote'] is not None and not raw.contains(content.view, elected['vote']),
                'control requires actual carried removed vote')
    entries = raw.read_journal_entries(base / removed['election_path'])
    frame = bytearray(entries[-1])
    size = int.from_bytes(frame[84:88], 'big')
    frame = frame[:96 + size]
    frame[:8] = b'PLELECT2'
    frame[28:36] = struct.pack('>Q', elected['term'] + 1)
    vote_path = scratch / 'higher_term_carried_inactive_vote.wal'
    vote_path.write_bytes(checksums(entries + [bytes(frame)]))
    raw.read_journal_entries(vote_path)
    yield {'name': 'higher_term_carried_inactive_vote', 'guard': 'new vote outside current full-directory set',
           'call': lambda: raw.read_election(vote_path, content), 'all_outer_checksums_valid': True,
           'sha256': hashlib.sha256(vote_path.read_bytes()).hexdigest()}

    final = next(c for c in trace['checkpoints'] if c['phase'] == 'final-reopen' and
                 c['key'] == trace['locals'][-1]['key'])
    original = raw.read_content(base / final['wal_path'], base / final['images_dir'], final['key'], trace['group'])
    entries = raw.read_journal_entries(base / final['wal_path'])
    p = bytearray(entries[-1])
    length = int.from_bytes(p[16:20], 'big')
    authority, body = 24 + length, 104 + length
    raw.require(p[8] == 4 and p[authority] == 1, 'control requires actual last remote Commit')
    higher = original.max_term + 1
    p[authority + 8:authority + 16] = struct.pack('>Q', higher)
    p[body + 8:body + 16] = struct.pack('>Q', higher)
    entries[-1] = bytes(p)
    term_path = scratch / 'wal_authority_exceeds_election_term.wal'
    term_path.write_bytes(checksums(entries))
    changed_content = raw.read_content(term_path, base / final['images_dir'], final['key'], trace['group'])
    raw.require(changed_content.rows == original.rows and changed_content.committed == original.committed and
                changed_content.max_term == higher, 'authority control preserves actual old-term content')
    yield {'name': 'wal_authority_exceeds_election_term', 'guard': 'election below durable WAL authority term',
           'call': lambda: raw.read_election(base / final['election_path'], changed_content),
           'all_outer_checksums_valid': True, 'sha256': hashlib.sha256(term_path.read_bytes()).hexdigest()}

    for opcode, field in ((2, 'sequence'), (2, 'configuration_epoch'), (2, 'leader_commit'),
                          (2, 'leader_key'), (5, 'sequence'), (5, 'configuration_epoch'), (5, 'leader_commit')):
        name = f"unemitted_remote_{'append' if opcode == 2 else 'install'}_{field}"
        owner = next(c for c in trace['checkpoints'] if c['phase'] == 'final-reopen' and
                     any(o['opcode'] == opcode and o['authority'] and o['authority']['kind'] == 1
                         for o in raw.read_content(base / c['wal_path'], base / c['images_dir'],
                                                   c['key'], trace['group']).operations))
        replay = raw.read_content(base / owner['wal_path'], base / owner['images_dir'], owner['key'], trace['group'])
        target = next(i for i, o in enumerate(replay.operations) if o['opcode'] == opcode and
                      o['authority'] and o['authority']['kind'] == 1)
        source = raw.read_journal_entries(base / owner['wal_path'])[target]
        p = bytearray(source)
        length = int.from_bytes(p[16:20], 'big')
        authority, body = 24 + length, 104 + length
        if field == 'leader_key':
            alternative = next(v['key'] for v in trace['group']['genesis']['voters']
                               if v['key'] not in (replay.operations[target]['authority']['leader'], owner['key']))
            p[authority + 16:authority + 20] = struct.pack('>I', alternative['id'])
            p[authority + 40:authority + 56] = bytes.fromhex(alternative['directory'])
        else:
            offset = {'sequence': 24, 'leader_commit': 32, 'configuration_epoch': 72}[field]
            value = int.from_bytes(p[authority + offset:authority + offset + 8], 'big') + 10000
            p[authority + offset:authority + offset + 8] = struct.pack('>Q', value)
            if opcode == 5 and field in ('sequence', 'leader_commit'):
                p[body + offset:body + offset + 8] = struct.pack('>Q', value)
        with tempfile.TemporaryDirectory(prefix=name + '-', dir=scratch) as folder:
            clone = Path(folder) / 'capture'
            shutil.copytree(base, clone)
            modified = []
            for c in trace['checkpoints']:
                if c['key'] != owner['key']:
                    continue
                entries = raw.read_journal_entries(clone / c['wal_path'])
                if len(entries) > target:
                    raw.require(entries[target] == source, 'control source repeated byte identity')
                    entries[target] = bytes(p)
                    path = clone / c['wal_path']
                    path.write_bytes(checksums(entries))
                    raw.read_content(path, clone / c['images_dir'], c['key'], trace['group'])
                    modified.append({'path': c['wal_path'], 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            raw.require(modified, 'control changed actual persisted receipts')
            yield {'name': name, 'guard': 'remote WAL authority lacks actual received request',
                   'call': lambda: history.verify(clone / 'trace.json'), 'all_outer_checksums_valid': True,
                   'changed_wal_operation': target, 'modified_artifacts': modified,
                   'actual_trace_bytes_unchanged': (clone / 'trace.json').read_bytes() == (base / 'trace.json').read_bytes()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('captures', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    before = fingerprint(args.captures)
    controls, originals = [], []
    sources = sorted(args.captures.glob('history-*/trace.json'))
    raw.require({p.parent.name for p in sources} == {'history-3', 'history-5'} and len(sources) == 2,
                'exact three/five voter capture denominator')
    with tempfile.TemporaryDirectory(prefix='membership-controls-', dir=args.output.parent) as folder:
        scratch = Path(folder)
        for source in sources:
            t = json.loads(source.read_bytes())
            originals.append(history.verify(source))
            for name in CAUSAL:
                changed = copy.deepcopy(t)
                expected = mutate_trace(name, changed)
                path = scratch / (source.parent.name + '-' + name + '.json')
                path.write_text(json.dumps(changed) + '\n')
                guard = rejected(lambda: history.verify(path, source.parent), expected)
                controls.append({'history': source.parent.name, 'name': name, 'guard': guard,
                                 'source_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                 'original_raw_artifacts_unchanged': True})
            controls.extend({'history': source.parent.name, **c} for c in raw_controls(source.parent, t, scratch))
            for c in supplementary_cases(source.parent, t, scratch):
                call = c.pop('call')
                guard = rejected(call, c['guard'])
                controls.append({'history': source.parent.name, **c, 'guard': guard})
            originals.append(history.verify(source))
    raw.require(before == fingerprint(args.captures), 'original capture bytes/full modes changed')
    raw.require(len(controls) == 70 and len(originals) == 4, 'exact semantic-control/positive denominator')
    args.output.write_text(json.dumps({'negative_controls_rejected': len(controls), 'original_positive_runs': len(originals),
                                      'controls': controls, 'all_original_bytes_and_full_modes_unchanged': True,
                                      'scope': 'finite independent semantic guard controls; valid Journal checksums and untouched captured originals'}, indent=2) + '\n')
    print(json.dumps({'negative_controls_rejected': len(controls), 'positive_runs': len(originals)}))


if __name__ == '__main__':
    main()

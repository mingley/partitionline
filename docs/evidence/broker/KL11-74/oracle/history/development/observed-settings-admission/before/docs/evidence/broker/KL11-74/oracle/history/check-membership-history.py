#!/usr/bin/env python3
"""Correlate finite emitted membership exchanges with independently replayed bytes."""
from __future__ import annotations

import argparse
from collections import Counter
import copy
import hashlib
import json
from pathlib import Path
import struct

import membership_raw as raw
from membership_raw import Record, require

MAX_TRACE = 32 * 1024 * 1024
MAX_EVENTS = 4096
KINDS = {'open', 'campaign', 'vote_request', 'vote_response', 'activate', 'propose',
         'prepare', 'receive', 'acknowledge', 'checkpoint', 'checkpoint_image',
         'feature_prepare', 'feature_receive', 'feature_acknowledge', 'add_voter',
         'remove_voter', 'prior_configuration_guard', 'process_exit',
         'process_exit_observed', 'reopen', 'reopen_final', 'image_prepare',
         'image_begin', 'image_chunk_prepare', 'image_chunk_receive', 'image_finish',
         'image_acknowledge', 'acknowledge_mutated', 'duplicate_acknowledge',
         'removed_leader_propose', 'removed_campaign'}


def integer(value, low=0, high=(1 << 64) - 1):
    require(type(value) is int and low <= value <= high, 'integer type/range')
    return value


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def boolean(value):
    require(type(value) is bool, 'boolean type')
    return value


def key(value) -> tuple:
    node = integer(value['id'], 0, 0x7fffffff)
    directory = value['directory']
    require(type(directory) is str and len(directory) == 32 and
            bytes.fromhex(directory).hex() == directory and directory != '00' * 16,
            'full directory identity')
    return node, directory


def position(value) -> tuple:
    term, index = integer(value['term'], 0, raw.MAX_TERM), integer(value['index'], 0, raw.MAX_ENTRIES)
    require((term == 0) == (index == 0), 'position empty/term')
    return term, index


def record(value) -> Record:
    term, index = position(value)
    kind = integer(value['kind'], 0, 2)
    encoded = value['payload_hex']
    require(type(encoded) is str and len(encoded) <= 2 * raw.MAX_RECORD, 'record payload bound')
    payload = bytes.fromhex(encoded)
    require((kind == 1) == (not payload), 'record typed payload')
    return Record(index, term, kind, payload)


def tail(rows) -> tuple:
    return (rows[-1].term, len(rows)) if rows else (0, 0)


def safe_path(base, name, directory=False):
    require(type(name) is str, 'artifact path type')
    path = (base / name).resolve()
    require(path.is_relative_to(base.resolve()) and not (base / name).is_symlink() and
            (path.is_dir() if directory else path.is_file()), 'artifact path/availability')
    return path


def encode_view(view) -> bytes:
    p = view['position']
    out = bytearray(b'PLVOTR01' + struct.pack('>QQQhH4x', view['epoch'], p['term'], p['index'],
                                          view['feature'], len(view['voters'])))
    for voter in view['voters']:
        node, directory = key(voter['key'])
        endpoints = voter['endpoints']
        out.extend(struct.pack('>I', node) + bytes.fromhex(directory) +
                   struct.pack('>hhH2x', voter['kraft_min'], voter['kraft_max'], len(endpoints)))
        for endpoint in endpoints:
            listener, host = endpoint['listener'].encode(), endpoint['host'].encode()
            out.extend(struct.pack('>HHH', len(listener), len(host), endpoint['port']) + listener + host)
    raw.decode_configuration(bytes(out))
    return bytes(out)


def changed_view(view, peer, voter, new_position, remove=False):
    result = copy.deepcopy(view)
    result['epoch'] += 1
    result['position'] = {'term': new_position[0], 'index': new_position[1]}
    if remove:
        require(any(v['key'] == peer for v in result['voters']), 'remove exact existing key')
        result['voters'] = [v for v in result['voters'] if v['key'] != peer]
    else:
        require(not any(v['key']['id'] == peer['id'] for v in result['voters']), 'addition existing numeric ID')
        result['voters'].append(copy.deepcopy(voter))
        result['voters'].sort(key=lambda v: v['key']['id'])
    result['canonical_hex'] = encode_view(result).hex()
    raw.successor(view, result)
    return result


def message_context(message):
    context = message['context']
    leader, peer = key(context['leader']), key(context['peer'])
    integer(context['configuration_epoch'])
    term, sequence = integer(message['term'], 1, raw.MAX_TERM), integer(message['sequence'], 1)
    return leader, peer, term, sequence


def remote_authority(message):
    """The persisted authority must name this actual fully qualified input."""
    message_context(message)
    context = message['context']
    return {'kind': 1, 'term': message['term'], 'leader': context['leader'],
            'peer': context['peer'], 'sequence': message['sequence'],
            'leader_commit': integer(message['leader_commit']),
            'configuration_epoch': context['configuration_epoch']}


def verify(path: Path, artifact_root: Path | None = None) -> dict:
    base = artifact_root or path.parent
    require(path.stat().st_size <= MAX_TRACE, 'trace byte bound')
    trace_bytes = path.read_bytes()
    trace = json.loads(trace_bytes)
    require(trace['schema_version'] == 1 and trace['profile'] == 'caller-driven-durable-directory-membership',
            'trace schema/profile')
    group, events, checkpoints = trace['group'], trace['events'], trace['checkpoints']
    genesis = raw.decode_configuration(bytes.fromhex(group['genesis']['canonical_hex']))
    require(genesis == group['genesis'] and genesis['epoch'] == 0, 'trace immutable genesis')
    require(type(events) is list and 1 <= len(events) <= MAX_EVENTS and events[0]['kind'] == 'open', 'event bound/initial')
    require(type(checkpoints) is list and 1 <= len(checkpoints) <= MAX_EVENTS * 64, 'checkpoint bound')
    locals_ = {key(v['key']): v for v in trace['locals']}
    require(len(locals_) == len(trace['locals']) <= 64 and len(genesis['voters']) in (3, 5), 'local identities/profile')
    models = {k: {'rows': [], 'view': genesis, 'commit': 0, 'selected': None} for k in locals_}
    raw_points, image_bytes = {}, {}
    content_operations = election_frames = 0
    for c in checkpoints:
        owner, ordinal = key(c['key']), integer(c['event_ordinal'], 0, len(events) - 1)
        require(owner in locals_ and (owner, ordinal) not in raw_points, 'checkpoint identity/uniqueness')
        images_dir = safe_path(base, c['images_dir'], True)
        content = raw.read_content(safe_path(base, c['wal_path']), images_dir, c['key'], group)
        election = raw.read_election(safe_path(base, c['election_path']), content)
        require(len(content.operations) == c['wal_confirmed_ops'] and election['state_count'] == c['election_confirmed_states'],
                'checkpoint actual durable denominators')
        raw_points[owner, ordinal] = content, election, c
        content_operations += len(content.operations)
        election_frames += election['state_count']
        image_paths = sorted(images_dir.glob('snapshot-*.image'))
        require(len(image_paths) <= 64, 'image generation bound')
        for image_path in image_paths:
            require(not image_path.is_symlink() and image_path.stat().st_size <= raw.MAX_FILE, 'image artifact bound')
            encoded = image_path.read_bytes()
            image = raw.decode_image(encoded, group)
            identity = owner, image['descriptor']['generation']
            require(image_path.name == 'snapshot-' + identity[1] + '.image' and
                    (identity not in image_bytes or image_bytes[identity] == encoded), 'immutable generation identity')
            image_bytes[identity] = encoded
    prepared, replies, used_replies = {}, {}, set()
    votes, vote_replies, grants = {}, {}, {}
    probes, feature_replies, discovered = {}, {}, {}
    offers, chunks, streams, image_replies = {}, {}, {}, {}
    remote_receipts = {owner: [] for owner in locals_}
    bound_operations = {owner: [] for owner in locals_}
    bound_checkpoint = {owner: -1 for owner in locals_}
    last_remote_event = {owner: -1 for owner in locals_}
    authority_bindings = []
    matches = {}
    previous = None
    last_clock = -1
    counters = Counter()
    commit_proofs, election_proofs = [], []
    final_covered = set()
    exit_event = None
    for ordinal, event in enumerate(events):
        require(event['ordinal'] == ordinal and event['kind'] in KINDS, 'event ordinal/kind')
        now = integer(event['now_ms'])
        require(now >= last_clock, 'caller clock regression')
        last_clock = now
        kind, acting, args, result = event['kind'], key(event['key']), event['args'], event['result']
        require(acting in models, 'acting local identity')
        after = {key(s['key']): s for s in event['after']}
        require(len(after) == len(locals_) and set(after) == set(locals_), 'complete distinct after owners')
        model = models[acting]
        counters[kind] += 1
        if kind == 'campaign':
            require(raw.contains(model['view'], event['key']), 'campaign removed identity')
            requests = result['requests']
            require(len(requests) == len(model['view']['voters']) - 1, 'campaign voter denominator')
            peers = set()
            term = after[acting]['term']
            grants[acting, term] = {acting}
            matches[acting, term] = {}
            for request in requests:
                ident = message_context(request)
                require(ident[0] == acting and ident[1] != acting and ident[1] not in peers and
                        raw.contains(model['view'], request['context']['peer']) and ident[2] == term and
                        request['candidate'] == acting[0] and request['context']['configuration_epoch'] == model['view']['epoch'] and
                        position(request['log']) == tail(model['rows']), 'campaign exact source/configuration')
                peers.add(ident[1])
                votes[canonical(request)] = ordinal
        elif kind == 'vote_request':
            require(canonical(args) in votes and message_context(args)[1] == acting and result['request'] == args and
                    result['voter'] == acting[0] and result['candidate'] == args['candidate'] and
                    result['term'] == after[acting]['term'], 'vote emitted request correlation')
            if boolean(result['granted']):
                candidate = args['context']['leader']
                require(raw.contains(model['view'], candidate) and position(args['log']) >= tail(model['rows']) and
                        result['term'] == args['term'] and after[acting]['voted_for'] == candidate,
                        'granted vote full directory/freshness/persistence')
            vote_replies[canonical(result)] = ordinal
        elif kind == 'vote_response':
            require(canonical(args) in vote_replies and message_context(args['request'])[0] == acting,
                    'vote response actual emitted correlation')
            ident = message_context(args['request'])
            if boolean(args['granted']) and args['term'] == ident[2] == after[acting]['term']:
                grants.setdefault((acting, ident[2]), set()).add(ident[1])
            if result['tally'] == 'Elected':
                members = {key(v['key']) for v in model['view']['voters']}
                counted = grants[acting, ident[2]] & members
                require(len(counted) >= len(members) // 2 + 1, 'election distinct NEW-set majority')
                election_proofs.append({'event': ordinal, 'term': ident[2], 'configuration_epoch': model['view']['epoch'],
                                        'distinct_grants': sorted(counted), 'majority': len(members) // 2 + 1})
        elif kind in ('activate', 'propose'):
            require(previous is not None and previous[acting]['role'] == 'Leader' and
                    raw.contains(model['view'], event['key']), 'leader append eligibility')
            index = integer(result['barrier_index' if kind == 'activate' else 'index'], 1, raw.MAX_ENTRIES)
            require(index == len(model['rows']) + 1, 'local append continuity')
            payload = b'' if kind == 'activate' else bytes.fromhex(args['payload_hex'])
            model['rows'].append(Record(index, after[acting]['term'], 1 if kind == 'activate' else 0, payload))
            matches.setdefault((acting, after[acting]['term']), {})
        elif kind in ('add_voter', 'remove_voter'):
            peer = args['peer']
            require(model['view']['position']['index'] <= model['commit'], 'local prior configuration commit')
            p = position(result['position'])
            require(p == (after[acting]['term'], len(model['rows']) + 1) and result['committed'] is False,
                    'configuration appended receipt position')
            voter = locals_.get(key(peer))
            if kind == 'add_voter':
                require((acting, key(peer), after[acting]['term'], model['view']['epoch']) in discovered,
                        'addition correlated feature discovery')
            new = changed_view(model['view'], peer, voter, p, kind == 'remove_voter')
            require(result['epoch'] == new['epoch'], 'change receipt epoch')
            model['rows'].append(Record(p[1], p[0], 2, bytes.fromhex(new['canonical_hex'])))
            model['view'] = new
        elif kind == 'prepare':
            ident = message_context(result)
            require(ident[0] == acting and result['leader'] == acting[0] and result['peer'] == ident[1][0] and
                    ident[2] == after[acting]['term'] == after[acting]['active_term'] and
                    after[acting]['role'] == 'Leader' and
                    result['context']['configuration_epoch'] == model['view']['epoch'] and
                    result['leader_commit'] == model['commit'], 'append emitted authority/configuration')
            p = position(result['previous'])
            require(p[1] <= len(model['rows']) and
                    (p == (model['rows'][p[1] - 1].term, p[1]) if p[1] else p == (0, 0)),
                    'append emitted previous exact source')
            rows = [record(x) for x in result['entries']]
            require(rows == model['rows'][p[1]:p[1] + len(rows)], 'append emitted exact source records')
            require(ident not in prepared, 'duplicate emitted sequence')
            prepared[ident] = copy.deepcopy(result)
        elif kind == 'receive':
            ident = message_context(args)
            require(prepared.get(ident) == args and ident[1] == acting and result['context'] == args['context'] and
                    (result['leader'], result['peer'], result['sequence']) == (args['leader'], args['peer'], args['sequence']) and
                    result['term'] == after[acting]['term'],
                    'append receive actual source/correlated response')
            if boolean(result['success']):
                p = position(args['previous'])
                require(p[1] <= len(model['rows']) and (p == (0, 0) or model['rows'][p[1] - 1].term == p[0]),
                        'successful append verified previous')
                added = [record(x) for x in args['entries']]
                for row in added:
                    require(row.index <= len(model['rows']) + 1, 'received append continuity')
                    if row.index <= len(model['rows']) and model['rows'][row.index - 1] != row:
                        require(row.index > model['commit'] and model['rows'][row.index - 1].term != row.term and
                                args['term'] > tail(model['rows'])[0], 'received conflict committed/same-term fence')
                        model['rows'] = model['rows'][:row.index - 1]
                    if row.index > len(model['rows']):
                        model['rows'].append(row)
                target = (added[-1].term, added[-1].index) if added else p
                require(position(result['matched']) == target and result['term'] == args['term'], 'response exact request target')
                model['commit'] = max(model['commit'], min(args['leader_commit'], target[1]))
                model['view'] = raw.configuration_in(model['rows'], genesis)
                remote_receipts[acting].append({'event': ordinal, 'kind': kind,
                    'authority': remote_authority(args), 'opcodes': (3, 2, 4), 'used': []})
            replies[canonical(result)] = ident
        elif kind in ('acknowledge', 'image_acknowledge'):
            response = args['response'] if kind == 'image_acknowledge' else args
            token = canonical(response)
            ident = message_context(response)
            table = image_replies if kind == 'image_acknowledge' else replies
            require(token in table and token not in used_replies and ident[0] == acting,
                    'ACK actual once-only emitted correlation')
            used_replies.add(token)
            if boolean(response['success']) and response['term'] == after[acting]['term']:
                matched = position(response['matched'])
                require(matched[1] <= len(model['rows']) and (matched == (0, 0) or
                        model['rows'][matched[1] - 1].term == matched[0]), 'ACK exact local position')
                progress = matches.setdefault((acting, response['term']), {})
                progress[ident[1]] = max(progress.get(ident[1], 0), matched[1])
            new_commit = integer(result['committed_end'], 0, len(model['rows']))
            require(new_commit >= model['commit'], 'local committed prefix regression')
            if new_commit > model['commit']:
                members = {key(v['key']) for v in model['view']['voters']}
                progress = matches.get((acting, after[acting]['term']), {})
                counted = {peer for peer in members if progress.get(peer, 0) >= new_commit}
                if acting in members and len(model['rows']) >= new_commit:
                    counted.add(acting)
                majority = len(members) // 2 + 1
                require(len(counted) >= majority and model['rows'][new_commit - 1].term == after[acting]['term'],
                        'local commit distinct NEW-set/current-term majority')
                commit_proofs.append({'event': ordinal, 'configuration_epoch': model['view']['epoch'],
                                      'term': after[acting]['term'], 'commit': new_commit,
                                      'distinct_matches': sorted(counted), 'majority': majority})
            model['commit'] = new_commit
        elif kind == 'feature_prepare':
            require(result['leader'] == event['key'] and result['configuration_epoch'] == model['view']['epoch'] and
                    result['term'] == after[acting]['term'] and locals_[key(result['peer'])] == args,
                    'feature emitted exact declared peer')
            probes[canonical(result)] = ordinal
        elif kind == 'feature_receive':
            require(canonical(args) in probes and key(args['peer']) == acting and result['request'] == args and
                    result['voter'] == locals_[acting], 'feature receive emitted correlation')
            feature_replies[canonical(result)] = ordinal
        elif kind == 'feature_acknowledge':
            require(canonical(args) in feature_replies and key(args['request']['leader']) == acting and
                    result['accepted'] is True and args['voter']['kraft_min'] <= 1 <= args['voter']['kraft_max'],
                    'feature ACK actual correlation/range')
            q = args['request']
            discovered[acting, key(q['peer']), q['term'], q['configuration_epoch']] = args['voter']
        elif kind == 'checkpoint_image':
            image = raw.decode_image(image_bytes[acting, result['generation']], group)
            require(result == image['descriptor'] and image['records'] == model['rows'][:model['commit']],
                    'local image exact committed prefix')
            model['selected'] = result
        elif kind == 'image_prepare':
            ident = message_context(result)
            require(ident[0] == acting and result['context']['configuration_epoch'] == model['view']['epoch'] and
                    ident[2] == after[acting]['term'] == after[acting]['active_term'] and
                    after[acting]['role'] == 'Leader' and
                    result['leader_commit'] == model['commit'] and result['descriptor'] == model['selected'],
                    'image offer exact source authority')
            offers[ident] = copy.deepcopy(result)
        elif kind == 'image_begin':
            ident = message_context(args)
            require(offers.get(ident) == args and ident[1] == acting and result['accepted'] is True,
                    'image begin actual offer')
            streams[ident] = bytearray()
        elif kind == 'image_chunk_prepare':
            ident = message_context(args)
            require(offers.get(ident) == args and ident[0] == acting, 'image chunk source offer')
            encoded = image_bytes[acting, args['descriptor']['generation']]
            chunk = bytes.fromhex(result['bytes_hex'])
            offset = integer(result['offset'], 0, len(encoded))
            require(chunk and encoded[offset:offset + len(chunk)] == chunk and
                    boolean(result['done']) == (offset + len(chunk) == len(encoded)), 'image chunk exact source bytes')
            chunks[ident, offset] = chunk
        elif kind == 'image_chunk_receive':
            ident = message_context(args['offer'])
            offset, chunk = integer(args['offset']), bytes.fromhex(args['bytes_hex'])
            require(ident[1] == acting and chunks.get((ident, offset)) == chunk and
                    offset == len(streams[ident]) and result['accepted'] is True, 'image stream emitted contiguous bytes')
            streams[ident].extend(chunk)
        elif kind == 'image_finish':
            ident = message_context(args)
            require(offers.get(ident) == args and ident[1] == acting, 'image finish offer')
            image = raw.decode_image(bytes(streams[ident]), group)
            require(image['descriptor'] == args['descriptor'] == result['descriptor'] and
                    result['context'] == args['context'] and result['response']['success'] is True,
                    'image finish complete descriptor/response')
            rows = image['records']
            require(rows[:model['commit']] == model['rows'][:model['commit']], 'image receive committed overlap')
            exact = all(a == b for a, b in zip(rows, model['rows']))
            if not exact:
                require(args['term'] > tail(model['rows'])[0] and all(a == b or a.term != b.term for a, b in zip(rows, model['rows'])),
                        'image conflict authority')
            model['rows'] = rows + model['rows'][len(rows):] if exact and len(model['rows']) > len(rows) else rows.copy()
            model['commit'], model['selected'] = len(rows), image['descriptor']
            model['view'] = raw.configuration_in(model['rows'], genesis)
            require(position(result['response']['matched']) == tail(rows),
                    'image response exact target')
            image_replies[canonical(result['response'])] = ident
            remote_receipts[acting].append({'event': ordinal, 'kind': kind,
                'authority': remote_authority(args), 'opcodes': (5,), 'used': []})
        elif kind == 'acknowledge_mutated':
            origin = args['input_origin']
            source = integer(origin['source_ordinal'], 0, ordinal - 1)
            require(events[source]['kind'] == 'receive' and origin['type'] == 'mutated_actual_response' and
                    origin['changed_fields'] == ['context.peer.directory'], 'declared changed input origin')
            original = copy.deepcopy(events[source]['result'])
            changed = args['response']
            original['context']['peer']['directory'] = changed['context']['peer']['directory']
            require(original == changed and changed != events[source]['result'] and result['rejected'] is True,
                    'only declared response field changed/rejected')
        elif kind == 'duplicate_acknowledge':
            require(canonical(args) in used_replies and result['rejected'] is True, 'duplicate real ACK rejection')
        elif kind == 'prior_configuration_guard':
            require(model['view']['position']['index'] > model['commit'] and result['rejected'] is True,
                    'pending configuration guard exercised')
        elif kind == 'process_exit':
            require(args['exit_code'] == 88 and model['view']['position']['index'] > model['commit'], 'pending-add exit boundary')
            exit_event = ordinal
        elif kind == 'process_exit_observed':
            require(exit_event is not None and ordinal == exit_event + 1 and
                    now > events[exit_event]['now_ms'] and result['observed_exit_code'] == args['requested_exit_code'] == 88 and
                    all(s['open'] is False for s in after.values()), 'actual process exit/clock handoff')
            matches.clear()
        elif kind == 'removed_leader_propose':
            require(not raw.contains(model['view'], event['key']) and model['view']['position']['index'] <= model['commit'] and
                    result['rejected'] is True and after[acting]['role'] != 'Leader' and
                    after[acting]['active_term'] is None, 'committed removed leader fencing')
        elif kind == 'removed_campaign':
            require(not raw.contains(model['view'], event['key']) and result['requests'] == [], 'removed voter cannot campaign')
        elif kind in ('reopen', 'reopen_final'):
            require(result['recovered'] is True and after[acting]['open'] is True, 'actual reopen result')
        elif kind == 'checkpoint':
            require((acting, ordinal) in raw_points and args == raw_points[acting, ordinal][2] and
                    result['copied_actual_files'] is True, 'checkpoint raw receipt correlation')
            content, election, _ = raw_points[acting, ordinal]
            prior = bound_operations[acting]
            require(content.operations[:len(prior)] == prior, 'checkpoint WAL prefix changed')
            for operation, receipt in enumerate(content.operations[len(prior):], start=len(prior)):
                authority = receipt['authority']
                if authority is None or authority['kind'] == 0:
                    continue
                # A successful typed receive can sync Truncate, Append, Commit
                # in that order; finishing one received image syncs one Install.
                # Reopening cannot invent new receipts from old message history.
                candidates = [r for r in remote_receipts[acting]
                              if bound_checkpoint[acting] < r['event'] < ordinal and
                              r['event'] >= last_remote_event[acting] and
                              r['authority'] == authority and receipt['opcode'] in r['opcodes'] and
                              receipt['opcode'] not in r['used'] and
                              (not r['used'] or r['opcodes'].index(receipt['opcode']) >
                               r['opcodes'].index(r['used'][-1]))]
                require(candidates, 'remote WAL authority lacks actual received request')
                actual = candidates[0]
                actual['used'].append(receipt['opcode'])
                last_remote_event[acting] = actual['event']
                authority_bindings.append({'owner': list(acting), 'wal_operation': operation,
                    'request_event': actual['event'], 'request_kind': actual['kind'],
                    'opcode': receipt['opcode'], 'authority': authority})
            bound_operations[acting] = content.operations
            bound_checkpoint[acting] = ordinal
            require(content.rows == model['rows'] and content.view == model['view'] and
                    content.committed == model['commit'] and content.selected == model['selected'], 'causal reconstruction/raw content match')
            require(election['final']['term'] == after[acting]['term'] and
                    election['final']['vote'] == after[acting]['voted_for'], 'raw election/observed exact match')
            if args['phase'] == 'final-reopen':
                final_covered.add(acting)
        for owner, observed in after.items():
            expected = models[owner]
            integer(observed['term'], 0, raw.MAX_TERM)
            boolean(observed['open'])
            boolean(observed['ready'])
            boolean(observed['poisoned'])
            require(raw.decode_configuration(bytes.fromhex(observed['voters']['canonical_hex'])) == observed['voters'] == expected['view'],
                    'observed exact reconstructed configuration')
            require(position(observed['last_position']) == tail(expected['rows']) and
                    observed['committed_end'] == expected['commit'] and observed['selected_snapshot'] == expected['selected'] and
                    [record(r) for r in observed['committed_records']] == expected['rows'][:expected['commit']],
                    'observed log/commit/image exact reconstruction')
            if (not raw.contains(expected['view'], observed['key']) and
                    expected['view']['position']['index'] <= expected['commit']):
                require(observed['role'] != 'Leader' and observed['active_term'] is None,
                        'committed removed owner has no leader authority')
            if previous is not None:
                require(observed['term'] >= previous[owner]['term'] and
                        (observed['term'] != previous[owner]['term'] or previous[owner]['voted_for'] is None or
                         observed['voted_for'] == previous[owner]['voted_for']), 'persistent term/full vote regression')
        prefixes = [m['rows'][:m['commit']] for m in models.values()]
        for a in prefixes:
            for b in prefixes:
                require(a[:min(len(a), len(b))] == b[:min(len(a), len(b))], 'cross-owner committed prefix disagreement')
        previous = after
    require(final_covered == set(locals_) and len(commit_proofs) >= 4 and len(election_proofs) >= 3 and
            counters['process_exit'] == counters['process_exit_observed'] == 1 and counters['image_finish'] >= 1 and
            counters['add_voter'] == counters['remove_voter'] == 1 and counters['acknowledge_mutated'] == 1,
            'required finite coverage denominator')
    return {'trace_sha256': hashlib.sha256(trace_bytes).hexdigest(), 'events': len(events),
            'paired_raw_checkpoints': len(checkpoints), 'decoded_content_operations': content_operations,
            'decoded_election_frames': election_frames, 'distinct_image_generations': len(image_bytes),
            'event_counts': dict(counters), 'local_commit_proofs': commit_proofs, 'election_proofs': election_proofs,
            'remote_wal_authorities_bound': len(authority_bindings), 'remote_wal_authority_bindings': authority_bindings,
            'admission_proof_limits': [
                'Current/prior leader-end catch-up timestamps and recent-contact addition eligibility are not independently reconstructed.',
                'Feature negotiation deadline and leader lease timers are absent from this trace schema and are not inferred.',
                'Prior configuration commit and NEW-set/current-term commit majority are checked; these alone do not qualify every addition admission criterion.'
            ],
            'scope': 'finite typed caller-driven 3/5 directory membership; raw durable images/journals and actual process exits; no autonomous/native wire or exhaustive safety claim'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('trace', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(verify(args.trace), indent=2) + '\n')

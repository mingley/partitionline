#!/usr/bin/env python3
"""Fail closed on the complete KL05-07 wire and independent Java record history."""
import argparse
import hashlib
import json
from pathlib import Path
import re

REFERENCE = 'apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e'


def require(value, message):
    if not value:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON field: ' + key)
        result[key] = value
    return result


def mapping(text, offsets=False):
    rows = []
    if text:
        for part in text.split(','):
            if offsets:
                require(re.fullmatch(r'\d+:\d+', part), 'invalid wire offset pair')
                rows.append(tuple(map(int, part.split(':'))))
            else:
                require(re.fullmatch(r'\d+', part), 'invalid forgotten partition')
                rows.append(int(part))
    require(rows == sorted(set(rows)), 'duplicate/unsorted wire partitions')
    return rows


def history(text, source, topic):
    entries = {name: [] for name in ['ACK', 'RECORD', 'POSITION', 'WIRE', 'SOURCE', 'TOPIC', 'COMPLETE']}
    for line in text.splitlines():
        if not line.startswith('PL_FETCH_'):
            continue
        fields = line.split('\t')
        tag = fields.pop(0).removeprefix('PL_FETCH_')
        require(tag in entries, 'unknown observation tag')
        entries[tag].append(fields)
    require(entries['SOURCE'] == [[source]] and entries['TOPIC'] == [[topic]]
            and entries['COMPLETE'] == [[]], 'missing/duplicate/wrong completion identity')
    require(re.search(r'test result: ok\. 1 passed; 0 failed; 0 ignored;', text), 'required test did not execute')
    expected = {(p, i, str(p), f'KL05-07/{p}/{i}') for p in range(32) for i in range(2)}
    expected.add((0, 2, '0', 'KL05-07/0/2'))
    acks = []
    for row in entries['ACK']:
        require(len(row) == 4, 'wrong ack fields')
        acks.append((int(row[0]), int(row[1]), row[2], row[3]))
    require(len(acks) == 65 and set(acks) == expected, 'missing/duplicate/wrong producer acknowledgement')
    records = []
    phases = {0: {(p, 0) for p in range(32)}, 3: {(p, 1) for p in range(16)},
              4: {(p, 1) for p in range(16, 32)}, 5: {(0, 2)}}
    for row in entries['RECORD']:
        require(len(row) == 5, 'wrong record fields')
        phase, p, offset = map(int, row[:3])
        require(phase in phases and (p, offset) in phases[phase], 'delivery phase/paused partition mismatch')
        records.append((p, offset, row[3], row[4]))
    require(len(records) == 65 and set(records) == expected, 'missing/duplicate/corrupt consumer history')
    positions = []
    for row in entries['POSITION']:
        require(len(row) == 2, 'wrong position fields')
        positions.append(tuple(map(int, row)))
    require(len(positions) == 32 and dict(positions) == {p: 3 if p == 0 else 2 for p in range(32)},
            'delivery position mismatch')
    wire = []
    sid = 0
    next_epoch = 0
    previous_phase = 0
    for fields in entries['WIRE']:
        require(len(fields) == 9, 'wrong wire fields')
        phase, version, requested, epoch, responded, code, size = map(int, fields[:7])
        changed, forgotten = mapping(fields[7], True), mapping(fields[8])
        require(0 <= previous_phase <= phase <= 6 and version == 17 and code == 0 and size > 0,
                'wire phase/version/error/size mismatch')
        previous_phase = phase
        if epoch == 0:
            require(requested == sid and phase in (0, 5) and responded > 0 and responded != sid,
                    'missing/incorrect observed full session creation/reset')
            require(changed == [(p, 0 if phase == 0 else 2) for p in range(32)] and not forgotten,
                    'full reset lost partition/offset state')
            sid, next_epoch = responded, 1
        elif epoch == -1:
            require(phase == 6 and requested == sid and responded == 0 and not changed and not forgotten,
                    'terminal close did not retire the known session')
        else:
            require(requested == responded == sid and sid > 0 and epoch == next_epoch,
                    'incremental session identity/epoch discontinuity')
            next_epoch += 1
        wire.append({'phase': phase, 'version': version, 'requested_session_id': requested, 'epoch': epoch,
                     'response_session_id': responded, 'error': code, 'request_bytes': size,
                     'changed': changed, 'forgotten': forgotten})
    require(len(wire) >= 7 and wire[0]['phase'] == 0 and wire[0]['epoch'] == 0
            and wire[-1]['phase'] == 6 and wire[-1]['epoch'] == -1, 'incomplete wire lifecycle')
    require(sum(r['epoch'] == 0 for r in wire) == 2, 'expected one initial session and one deliberate reconnect reset')
    for phase in range(7):
        require(any(r['phase'] == phase for r in wire), 'missing wire phase')
    require(any(r['phase'] == 2 and r['epoch'] > 0 and not r['changed'] and not r['forgotten'] for r in wire),
            'unchanged incremental request was not observed')
    paused = next(r for r in wire if r['phase'] == 3)
    require(paused['forgotten'] == list(range(16, 32)), 'pause did not forget every removed partition')
    resumed = next(r for r in wire if r['phase'] == 4)
    require(resumed['changed'] == [(p, 2 if p < 16 else 1) for p in range(32)] and not resumed['forgotten'],
            'resume did not preserve active offsets and restore paused partitions')
    return {'acks': acks, 'records': records, 'positions': positions, 'wire': wire}


def finish(directory, source):
    require(re.fullmatch(r'[0-9a-f]{40}', source), 'full candidate SHA required')
    identity = json.loads((directory / 'identity.json').read_text(), object_pairs_hook=unique_object)
    require(identity['source_sha'] == source and identity['broker_reference'] == identity['actual_reference'] == REFERENCE, 'source/broker pin mismatch')
    require(re.fullmatch(r'pl-fetch-recovery-[0-9a-f]{32}', identity['container']), 'unowned container identity')
    require(identity['topic'] == 'plfetch-recovery-' + identity['container'].removeprefix('pl-fetch-recovery-'),
            'unowned fixture topic')
    require(re.fullmatch(r'127\.0\.0\.1:\d+', identity['backend'])
            and re.fullmatch(r'127\.0\.0\.1:\d+', identity['proxy']) and identity['backend'] != identity['proxy'],
            'invalid observer ports')
    require(identity['advertised_listeners'] == 'PLAINTEXT://' + identity['proxy'] + ',INTERNAL://localhost:9094',
            'client can bypass wire observer')
    require(identity['mapped_backend'] == identity['backend'], 'actual broker mapping mismatch')
    require(re.fullmatch(r'sha256:[0-9a-f]{64}', identity['container_image_id'])
            and identity['container_image_id'] == identity['inspected_image_id']
            and REFERENCE in identity['repo_digests'], 'container image/digest identity mismatch')
    require(re.match(r'4\.1\.2(?:\s|$)', identity['java_cli_version']), 'wrong actual Java peer version')
    require(identity['host_os'] == 'Linux' and identity['host_arch'] == 'x86_64', 'wrong broker qualification host')
    required = {'create', 'start', 'readiness', 'create-topic', 'build', 'runtime', 'java-records', 'broker-logs', 'cleanup'}
    codes = identity['exit_codes']
    require(required <= codes.keys() and all(type(codes[k]) is int and codes[k] == 0 for k in required),
            'missing/failed prerequisite, runtime, Java peer or cleanup')
    require('failure' not in identity, 'failed attempt cannot be promoted')
    runtime = history((directory / 'runtime.stdout.log').read_text(), source, identity['topic'])
    java = []
    for line in (directory / 'java-records.stdout.log').read_text().splitlines():
        match = re.fullmatch(r'Partition:(\d+)\tOffset:(\d+)\t([^\t]+)\t([^\t]+)', line)
        require(match, 'unparsed independent Java record row')
        java.append((int(match[1]), int(match[2]), match[3], match[4]))
    require(len(java) == 65 and set(java) == set(runtime['acks']), 'Java peer missing/duplicate/corrupt record history')
    artifacts = {}
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.name not in ('report.json', 'report-validation.stdout.log', 'report-validation.stderr.log', 'validation-exit.json'):
            data = path.read_bytes()
            artifacts[path.name] = {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
    report = {'status': 'passed', 'source_sha': source, 'identity': identity, 'runtime': runtime,
              'java_records': java, 'artifacts': artifacts,
              'scope': '32 assigned partitions, pause/resume, one observed idle reconnect reset, terminal session close, 65 exact records; no crash campaign or throughput claim.'}
    (directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('source_sha')
    args = parser.parse_args()
    result = finish(args.directory, args.source_sha)
    print('Observed Fetch recovery: 32 partitions, 65 Rust/Java records, one reset and terminal close passed')

#!/usr/bin/env python3
"""Independently bind finite TCP observations to retained rolling file bytes."""
import base64
import hashlib
import json
from pathlib import Path
import struct
import sys

ROOT = Path(__file__).resolve().parents[6]
BASE = Path(__file__).resolve().parent
SOURCE = 'd147bcf1c0164778bdbad625842363f3721bc10e'
sys.path.insert(0, str(ROOT / 'docs/evidence/broker/KL11-10/oracle/storage'))
from legacy_oracle import bounded, crc, require, seek, sha  # noqa: E402
import importlib.util  # noqa: E402
spec = importlib.util.spec_from_file_location('retention_oracle', ROOT / 'docs/evidence/broker/KL11-10/oracle/storage/check-retention-history.py')
oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oracle)
manifest = oracle.manifest


def pin(path):
    return {'path': str(path.relative_to(ROOT)), 'sha256': sha(bounded(path))}


def expected(topic, offset):
    times = [1000, 1007, 1003, 1007, 1010, 1011, 1012, 1013]
    require(0 <= offset < len(times), 'declared finite record index')
    return {'topic': topic, 'partition': 0, 'offset': offset, 'timestamp': times[offset],
            'key_hex': None if offset % 3 == 0 else f'key:{topic}:{offset}'.encode().hex(),
            'value_hex': None if offset % 3 == 0 else '' if offset % 3 == 1 else f'value:{topic}:{offset}'.encode().hex(),
            'headers': [{'key': 'receipt', 'value_hex': f'{topic}:{offset}'.encode().hex()},
                        {'key': 'dup', 'value_hex': '61'}, {'key': 'dup', 'value_hex': None}]}


def record_receipt(record):
    require(record == expected(record['topic'], record['offset']), 'exact declared record bytes/null/empty/headers/time')
    canonical = json.dumps(record, separators=(',', ':'), ensure_ascii=True).encode()
    return {'sha256': sha(canonical), 'record': record}


def journal(path, base, output):
    data = bounded(path)
    require(len(data) >= 24 and data[:8] == b'PLJRNL01' and
            int.from_bytes(data[8:16], 'big') == base and data[16:20] == bytes(4) and
            crc(data[:20]) == int.from_bytes(data[20:24], 'big'), 'journal header identity/CRC')
    position, next_offset, maximum, entries, payloads = 24, base, -(1 << 63), [], []
    while position < len(data):
        require(len(entries) < 128 and len(data) - position >= 32, 'bounded complete entry')
        header = data[position:position + 32]
        require(header[:8] == b'PLENTRY1' and crc(header[:28]) == int.from_bytes(header[28:], 'big'), 'entry header CRC')
        length, first, count, checksum = struct.unpack('>IQII', header[8:28])
        require(first == next_offset and 1 <= count <= 8 and 61 <= length <= 128 * 1024,
                'bounded contiguous entry identity')
        payload = data[position + 32:position + 32 + length]
        require(len(payload) == length and crc(payload) == checksum, 'entry payload CRC')
        require(payload[16] == 2 and len(payload) == 12 + int.from_bytes(payload[8:12], 'big', signed=True)
                and crc(payload[21:]) == int.from_bytes(payload[17:21], 'big'), 'whole ordinary Kafka batch/CRC')
        require(int.from_bytes(payload[:8], 'big', signed=True) == first and
                int.from_bytes(payload[57:61], 'big', signed=True) == count and
                int.from_bytes(payload[23:27], 'big', signed=True) == count - 1,
                'journal/Kafka first/count/last identity')
        require(payload[21:23] == bytes(2) and int.from_bytes(payload[43:51], 'big', signed=True) == -1,
                'ordinary uncompressed CreateTime/nontransactional producer')
        timestamp = int.from_bytes(payload[35:43], 'big', signed=True)
        entries.append({'offset': first, 'position': position, 'prefix_max': maximum,
                        'record_count': count, 'payload_sha256': sha(payload)})
        maximum = max(maximum, timestamp)
        next_offset += count
        require(next_offset <= 8, 'finite declared durable end')
        output.mkdir(parents=True, exist_ok=True)
        target = output / f'{first:016x}.payload.bin'
        target.write_bytes(payload)
        payloads.append(pin(target))
        position += 32 + length
    return {'base': base, 'end': next_offset, 'bytes': len(data), 'entries': entries,
            'max_time': maximum, 'fingerprint': crc(data), 'sha256': sha(data), 'payloads': payloads}


def store(directory, expected_end, output):
    files = list(directory.iterdir())
    require(len(files) <= 32 and all(p.is_file() and not p.is_symlink() for p in files), 'bounded flat selected state')
    data = bounded(directory / 'manifest')
    m = manifest(data)
    require(data[:8] == b'PLSEGM02' and m['origin'] == m['physical'] == 0 and
            m['floor'] == 3 and not m['victims'], 'durable mid-batch floor and completed cleanup')
    names, journals = {'manifest'}, []
    for raw, descriptor in m['selected']:
        stem = f"{descriptor['base']:016x}-{descriptor['generation']:016x}"
        parsed = journal(directory / (stem + '.journal'), descriptor['base'], output)
        for key in ['end', 'bytes', 'max_time', 'fingerprint']:
            require(parsed[key] == descriptor[key], 'independent selected descriptor ' + key)
        require(len(parsed['entries']) == descriptor['entries'], 'selected entry count')
        parsed['index'] = seek(directory / (stem + '.seek'), raw, descriptor, parsed)
        names.update({stem + '.journal', stem + '.seek'})
        journals.append(parsed)
    name = f"{m['active']:016x}-{m['generation']:016x}.journal"
    active = journal(directory / name, m['active'], output)
    require(active['end'] == expected_end and m['floor'] <= active['end'], 'floor/confirmed actual end')
    journals.append(active)
    names.add(name)
    require(names == {p.name for p in files}, 'exact selected authority/no hidden victim files')
    offsets = [offset for parsed in journals for e in parsed['entries']
               for offset in range(e['offset'], e['offset'] + e['record_count'])]
    require(offsets == list(range(expected_end)), 'whole containing batch and all selected records')
    return {'logical_floor': 3, 'physical_base': 0, 'actual_end': expected_end,
            'manifest': pin(directory / 'manifest'), 'journals': journals,
            'scope': 'Exact finite V2 selected descriptors/journal CRC/seek positions/prefix maxima; Apache independently decodes record values separately.'}


def main():
    lanes, observations, producers, assertions, inputs, selected = [], [], [], 0, [], []
    for toolchain in ['stable', '1.85.0']:
        for features in ['default', 'all-features']:
            label = f'{toolchain}-{features}'
            directory = BASE / f'live-{label}-attempt-1'
            path = directory / 'validation.json'
            lane = json.loads(bounded(path))
            require(lane['passed'] and lane['source_sha'] == SOURCE and lane['actual_peer_jobs'] == 18
                    and lane['failure'] is None, 'accepted immutable lane')
            require(len(lane['results']) == 18 and len(lane['servers']) == 2, 'actual job/server denominator')
            for receipt in lane['receipts'].values():
                require(pin(ROOT / receipt['path'])['sha256'] == receipt['sha256'], 'actual used preparation/source receipt')
            for binary in lane['binaries'].values():
                require(sha(Path(binary['path']).read_bytes()) == binary['sha256'], 'actual preserved executable')
            for server in lane['servers']:
                require(server['ready'] and server['exit_code'] == 0 and
                        sha(bounded(directory / server['log'])) == server['log_sha256'], 'actual clean server lifecycle')
            for state in lane['states']:
                state_directory = directory / (state['phase'] + '-state')
                require(len(state['files']) <= 128 and state['bytes'] <= 16 * 1024 * 1024, 'bounded actual state snapshot')
                actual = {str(p.relative_to(state_directory)) for p in state_directory.rglob('*') if p.is_file()}
                require(actual == {p['path'] for p in state['files']}, 'snapshot inventory')
                for file in state['files']:
                    data = bounded(state_directory / file['path'])
                    require(len(data) == file['bytes'] and sha(data) == file['sha256'], 'actual state pin')
            require(bounded(directory / 'seed-state/catalog.journal') == bounded(directory / 'restart-state/catalog.journal'),
                    'unchanged durable topic identity catalog across restart')
            for result in lane['results']:
                require(result['exit_code'] == 0 and result['passed'] and
                        sha(bounded(directory / result['log'])) == result['log_sha256'], 'actual command outcome/log')
                report_path = directory / result['report']
                require(sha(bounded(report_path)) == result['report_sha256'], 'actual peer report pin')
                report = json.loads(bounded(report_path))
                require(report['passed'], 'actual peer pass')
                assertions += report['assertions']
                inputs.append(pin(report_path))
                phase, kind, release = result['phase'], result['kind'], result['release']
                topic = 'retention-java-' + release.replace('.', '-')
                history = report['history']
                label_record = {'java': 'retained-consumer-record', 'native': 'retained-native-consumer', 'rust': 'retained-public-consumer'}[kind]
                records = [h for h in history if h['label'] == label_record]
                expected_offsets = list(range(3, 7)) if phase == 'seed' else (list(range(3, 7)) + list(range(3, 8)) if kind == 'java' else list(range(3, 8)))
                actual_offsets = []
                for h in records:
                    r = h['record'] if kind == 'native' else h['receipt']['record']
                    require(r['topic'] == topic, 'per-job topic identity')
                    receipt = record_receipt(r)
                    if kind != 'native':
                        require(receipt == h['receipt'], 'actual canonical receipt digest')
                    actual_offsets.append(r['offset'])
                    observations.append(dict(receipt, lane=label, phase=phase, kind=kind, release=release))
                require(actual_offsets == expected_offsets, 'exact per-peer retained read sequence')
                if kind == 'java':
                    produced = [h['receipt'] for h in history if h['label'] == 'actual-producer']
                    require([p['record']['offset'] for p in produced] == ([5, 6] if phase == 'seed' else [7]), 'actual append receipt sequence')
                    for receipt in produced:
                        require(record_receipt(receipt['record']) == receipt, 'actual append bytes/hash')
                        producers.append(dict(receipt, lane=label, phase=phase, release=release))
            for release in ['4.1.2', '4.2.1', '4.3.1']:
                topic = 'retention-java-' + release.replace('.', '-')
                encoded = bounded(directory / (topic + '.uuid')).decode().strip()
                topic_id = base64.urlsafe_b64decode(encoded + '==').hex()
                require(len(topic_id) == 32 and topic_id != '0' * 32, 'actual nonzero topic UUID')
                for phase, end in [('seed', 7), ('restart', 8)]:
                    state_directory = directory / f'{phase}-state/partitions/{topic_id}-0.segments'
                    output = BASE / f'reverse-records/inputs/{label}/{phase}/{release}'
                    verified = store(state_directory, end, output)
                    selected.append(dict(verified, lane=label, phase=phase, release=release, topic=topic,
                                         topic_id=topic_id, payload_directory=str(output.relative_to(ROOT))))
                for suffix in ['journal', 'seek']:
                    name = f'partitions/{topic_id}-0.segments/0000000000000000-0000000000000000.{suffix}'
                    require(bounded(directory / ('seed-state/' + name)) == bounded(directory / ('restart-state/' + name)),
                            'whole containing first batch/index byte identity across restart')
            lanes.append(pin(path))
    require(len(observations) == 372 and len(producers) == 36 and len(selected) == 24, 'actual finite denominators')
    result = {'schema_version': 1, 'source_sha': SOURCE, 'passed': True, 'actual_peer_jobs': 72,
              'actual_peer_assertions': assertions, 'server_lifecycles': 8,
              'read_record_comparisons': len(observations), 'actual_producer_receipts': len(producers),
              'observations': observations, 'producer_receipts': producers,
              'selected_states': selected, 'lanes': lanes, 'raw_reports': inputs,
              'checker_sha256': sha(Path(__file__).read_bytes()),
              'independent_file_checker_sources': [pin(ROOT / 'docs/evidence/broker/KL11-10/oracle/storage' / p)
                                                  for p in ['legacy_oracle.py', 'check-retention-history.py']],
              'command': ['python3', str(Path(__file__).relative_to(ROOT))],
              'limits': ['Finite ordinary RF1 histories; no replication/transaction/power-loss claim.',
                         'Live floor3 lies inside the retained five-record first batch; sealed-prefix/active retirement and interruption cleanup are separate storage histories.',
                         'Java restart intentionally reads3–6 before append and3–7 after append; counts preserve both observations.',
                         'Unknown-time age preservation and complete later-segment timestamp scanning are deliberate Apache policy differences.']}
    output = BASE / 'records-validation.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['passed', 'actual_peer_jobs', 'actual_peer_assertions', 'read_record_comparisons', 'actual_producer_receipts']}))


if __name__ == '__main__':
    main()

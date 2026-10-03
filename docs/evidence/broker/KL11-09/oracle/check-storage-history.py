#!/usr/bin/env python3
"""Independently decode bounded retained rolling-store publication checkpoints.

No Rust module, encoder or expected runtime result is imported. Test seed bytes
come from the committed ordinary-record fixture; authoritative file fields and
all checksums/index positions/prefix maxima are reconstructed here.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct

MAX_FILE = 1024 * 1024
MAX_ENTRIES = 4096
MAX_FILES = 64
ACKNOWLEDGED = {'roll-error': 1, 'replace-error': 2, 'roll-exit': 1, 'replace-exit': 4}
TABLE = []
for value in range(256):
    for _ in range(8):
        value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    TABLE.append(value)


def require(value, reason):
    if not value:
        raise ValueError(reason)


def crc(data):
    value = 0xffffffff
    for byte in data:
        value = TABLE[(value ^ byte) & 255] ^ (value >> 8)
    return value ^ 0xffffffff


def sha(data):
    return hashlib.sha256(data).hexdigest()


def bounded(path):
    require(path.is_file() and not path.is_symlink(), 'regular bounded file required')
    require(path.stat().st_size <= MAX_FILE, 'file ceiling')
    return path.read_bytes()


def checked(data, magic, minimum):
    require(len(data) >= minimum and data[:8] == magic, 'protected file magic/length')
    require(crc(data[:-4]) == int.from_bytes(data[-4:], 'big'), 'protected file CRC')


def descriptor(data):
    require(len(data) == 56, 'descriptor length')
    base, end, generation, size, entries, maximum, fingerprint, reserved = struct.unpack('>QQQQQqII', data)
    require(reserved == 0 and base < end and 1 <= entries <= MAX_ENTRIES, 'descriptor fields')
    return {'base': base, 'end': end, 'generation': generation, 'bytes': size,
            'entries': entries, 'max_time': maximum, 'fingerprint': fingerprint}


def journal(path, base, fixture):
    data = bounded(path)
    require(len(data) >= 24 and data[:8] == b'PLJRNL01', 'journal magic/length')
    require(int.from_bytes(data[8:16], 'big') == base and data[16:20] == bytes(4)
            and crc(data[:20]) == int.from_bytes(data[20:24], 'big'), 'journal header identity/CRC')
    position = 24
    next_offset = base
    maximum = -(1 << 63)
    entries = []
    while position < len(data):
        require(len(entries) < MAX_ENTRIES and len(data) - position >= 32, 'clean bounded entry header')
        header = data[position:position + 32]
        require(header[:8] == b'PLENTRY1' and crc(header[:28]) == int.from_bytes(header[28:], 'big'), 'entry header CRC')
        length, first, count, checksum = struct.unpack('>IQII', header[8:28])
        require(first == next_offset and count == 1 and length == len(fixture), 'static seeded entry identity/length/count')
        payload = data[position + 32:position + 32 + length]
        require(len(payload) == length and crc(payload) == checksum, 'entry payload CRC')
        expected = first.to_bytes(8, 'big') + fixture[8:]
        require(payload == expected, 'acknowledged ordinary seed payload bytes')
        require(payload[16] == 2 and crc(payload[21:]) == int.from_bytes(payload[17:21], 'big'), 'ordinary Kafka batch CRC')
        timestamp = int.from_bytes(payload[35:43], 'big', signed=True)
        entries.append({'offset': first, 'position': position, 'prefix_max': maximum,
                        'payload_sha256': sha(payload)})
        maximum = max(maximum, timestamp)
        next_offset += 1
        position += 32 + length
    return {'base': base, 'end': next_offset, 'bytes': len(data), 'entries': entries,
            'max_time': maximum, 'fingerprint': crc(data), 'sha256': sha(data)}


def seek(path, raw_descriptor, decoded, data):
    bundle = bounded(path)
    checked(bundle, b'PLSEEK01', 72)
    require(bundle[8:64] == raw_descriptor, 'seek generation/descriptor binding')
    count = int.from_bytes(bundle[64:68], 'big')
    require(1 <= count <= len(data['entries']) and len(bundle) == 72 + 24 * count, 'seek count/length')
    by_offset = {entry['offset']: entry for entry in data['entries']}
    previous = -1
    for ordinal in range(count):
        offset, position, prefix = struct.unpack('>QQq', bundle[68 + 24 * ordinal:92 + 24 * ordinal])
        require(offset in by_offset and offset > previous, 'seek ordered entry boundary')
        entry = by_offset[offset]
        require(position == entry['position'] and prefix == entry['prefix_max'], 'seek physical position/prefix maximum')
        if ordinal == 0:
            require(offset == decoded['base'], 'seek first segment boundary')
        previous = offset
    return {'sha256': sha(bundle), 'checkpoints': count}


def state(path, fixture, kind, stage):
    case = json.loads(bounded(path / 'case.json'))
    require(case['kind'] == kind and case['stage'] == stage, 'static case identity')
    require(case['acknowledged_end'] == ACKNOWLEDGED[kind], 'static acknowledged floor')
    files = [p for p in path.iterdir() if p.name != 'case.json']
    require(len(files) <= MAX_FILES and all(p.is_file() and not p.is_symlink() for p in files), 'bounded flat file layout')
    manifest = bounded(path / 'manifest')
    checked(manifest, b'PLSEGM01', 48)
    revision, base, active_base, active_generation, count = struct.unpack('>QQQQI', manifest[8:44])
    require(base == 0 and 0 <= count < MAX_FILES and len(manifest) == 48 + 56 * count, 'manifest exact bounded fields')
    require(active_generation <= revision, 'active generation authority')
    expected_base = base
    selected = []
    seed_entries = []
    for ordinal in range(count):
        raw = manifest[44 + 56 * ordinal:100 + 56 * ordinal]
        desc = descriptor(raw)
        require(desc['base'] == expected_base and desc['generation'] <= revision, 'selected contiguous generations')
        filename = f"{desc['base']:016x}-{desc['generation']:016x}"
        data = journal(path / (filename + '.journal'), desc['base'], fixture)
        for key in ('end', 'bytes', 'max_time', 'fingerprint'):
            require(data[key] == desc[key], 'selected descriptor ' + key)
        require(len(data['entries']) == desc['entries'], 'selected descriptor entry count')
        index = seek(path / (filename + '.seek'), raw, desc, data)
        seed_entries.extend(data['entries'])
        selected.append({'descriptor': desc, 'journal_sha256': data['sha256'], 'index': index})
        expected_base = desc['end']
    require(expected_base == active_base, 'manifest active boundary')
    filename = f'{active_base:016x}-{active_generation:016x}.journal'
    active = journal(path / filename, active_base, fixture)
    seed_entries.extend(active['entries'])
    require(active['end'] >= ACKNOWLEDGED[kind], 'selected acknowledged prefix preserved')
    require([x['offset'] for x in seed_entries] == list(range(active['end'])), 'whole selected content contiguous')
    return {'kind': kind, 'stage': stage, 'phase': case['phase'], 'revision': revision,
            'acknowledged_end': ACKNOWLEDGED[kind], 'selected_end': active['end'],
            'manifest_sha256': sha(manifest), 'sealed': selected, 'active_sha256': active['sha256'],
            'seed_payload_receipts': seed_entries,
            'all_file_hashes': {p.name: sha(bounded(p)) for p in sorted(files)}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--histories', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(crc(b'123456789') == 0xe3069283, 'independent Castagnoli known vector')
    fixture = bounded(args.fixture)
    results = []
    for kind in ACKNOWLEDGED:
        phases = sorted((args.histories / kind).iterdir())
        require(1 <= len(phases) <= 16, 'bounded declared fault phases')
        for phase in phases:
            interrupted = state(phase / 'interrupted', fixture, kind, 'interrupted')
            recovered = state(phase / 'recovered', fixture, kind, 'recovered')
            require(interrupted['phase'] == recovered['phase'] == phase.name, 'paired phase identity')
            floor = ACKNOWLEDGED[kind]
            require(interrupted['seed_payload_receipts'][:floor] == recovered['seed_payload_receipts'][:floor], 'paired acknowledged bytes preserved')
            results.append({'kind': kind, 'phase': phase.name, 'interrupted': interrupted, 'recovered': recovered})
    require(len(results) == 33, 'actual fault history denominator')
    report = {'scope': 'Independent selected file/seed/checkpoint verification on actual finite process-publication captures; no Rust imports, power-loss, retention, global bounds or performance claim.',
              'histories': 33, 'states': 66, 'fixture_sha256': sha(fixture),
              'checker_sha256': sha(Path(__file__).read_bytes()), 'passed': True, 'results': results,
              'limitations': ['Clean selected journal boundaries only; active torn-tail repair is separately tested by Rust and not inferred here.',
                              'Derived checkpoint positions/prefix maxima are verified; configured interval and resource-constructor envelopes are separate contracts.',
                              'Known unreferenced staging generations are hash-retained, not promoted to committed authority.']}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('histories', 'states', 'passed')}))


if __name__ == '__main__':
    main()

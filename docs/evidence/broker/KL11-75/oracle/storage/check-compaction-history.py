#!/usr/bin/env python3
"""Independently certify captured changed-generation authority and seeded values.

This checker imports no Rust encoder or emitted runtime verdict. The declared
ordinary BASIC input has the same non-null key in four one-record appends; only
offset2 in the eligible prefix and protected active offset3 survive cleaning.
This finite capture contract does not qualify arbitrary histories or power loss.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct

MAX_FILE = 32768
PHASES = {
    'CompactionHeaderSynced', 'CompactionEntrySynced', 'IndexWritten',
    'IndexSynced', 'IndexRenamed', 'ManifestWritten', 'ManifestSynced',
    'ManifestRenamed', 'DirectorySynced', 'CompactionDataRemoved',
    'CompactionIndexRemoved', 'CompactionCleanupSynced', 'RetentionClearWritten',
    'RetentionClearSynced', 'RetentionClearRenamed', 'RetentionClearDirectorySynced',
}
OLD = {'CompactionHeaderSynced', 'CompactionEntrySynced', 'IndexWritten',
       'IndexSynced', 'IndexRenamed', 'ManifestWritten', 'ManifestSynced'}
TABLE = []
for number in range(256):
    for _ in range(8):
        number = (number >> 1) ^ (0x82f63b78 if number & 1 else 0)
    TABLE.append(number)


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def crc(data):
    value = 0xffffffff
    for byte in data:
        value = TABLE[(value ^ byte) & 255] ^ (value >> 8)
    return value ^ 0xffffffff


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_FILE,
            'bounded regular capture file')
    return path.read_bytes()


def protected(data, magic, minimum):
    require(len(data) >= minimum and data[:8] == magic, 'protected magic/length')
    require(crc(data[:-4]) == int.from_bytes(data[-4:], 'big'), 'protected checksum')


def descriptor(raw):
    require(len(raw) == 56, 'descriptor length')
    base, end, generation, size, count, maximum, fingerprint, kind = struct.unpack('>QQQQQqII', raw)
    require(0 <= base < end <= (1 << 63) - 1 and 1 <= count <= 16 and kind in (0, 1)
            and end - base >= count and size >= 24 + count * (33 if kind == 0 else 32),
            'descriptor extent/count/kind/length')
    return dict(base=base, end=end, generation=generation, bytes=size, entries=count,
                max_time=maximum, fingerprint=fingerprint, kind=kind)


def manifest(data):
    magic = data[:8]
    require(magic in (b'PLSEGM01', b'PLSEGM02', b'PLSEGM03'), 'manifest format')
    protected(data, magic, 48)
    if magic == b'PLSEGM01':
        revision, origin, active, generation, count = struct.unpack('>QQQQI', data[8:44])
        physical, floor, victims, obsolete, header = origin, origin, 0, 0, 44
    else:
        header = 72 if magic == b'PLSEGM03' else 64
        require(len(data) >= header + 4, 'manifest complete header')
        revision, origin, physical, floor, active, generation, count, victims = struct.unpack('>QQQQQQII', data[8:64])
        obsolete, reserved = struct.unpack('>II', data[64:72]) if header == 72 else (0, 0)
        require(reserved == 0, 'V3 reserved field')
    require(origin == physical == floor == 0 and active == 3 and generation == 3,
            'seeded origin/floor/active authority')
    require(count == 3 and victims == 0 and 0 <= obsolete <= count
            and len(data) == header + 4 + 56 * (count + victims + obsolete),
            'exact selected/obsolete denominator')
    selected, old, next_offset = [], [], physical
    for ordinal in range(count + obsolete):
        raw = data[header + ordinal * 56:header + (ordinal + 1) * 56]
        d = descriptor(raw)
        require(d['generation'] <= revision, 'descriptor generation authority')
        if ordinal < count:
            require(d['base'] == next_offset and (header == 72 or d['kind'] == 0),
                    'selected contiguous logical extent/legacy kind')
            next_offset = d['end']
            selected.append((raw, d))
        else:
            matches = [s for _, s in selected if s['base'] == d['base']]
            require(len(matches) == 1, 'obsolete selected base binding')
            new = matches[0]
            require(d['end'] == new['end'] and d['entries'] == new['entries']
                    and d['generation'] < new['generation'] == revision
                    and (not old or old[-1][1]['end'] <= d['base']),
                    'obsolete range/count/older-generation/uniqueness binding')
            old.append((raw, d))
    require(next_offset == active and generation <= revision, 'active boundary authority')
    return dict(magic=magic.decode(), revision=revision, selected=selected, obsolete=old,
                active=active, active_generation=generation, floor=floor)


def expected_file(base, end, fixture, sparse):
    prefix = (b'PLSPRS01' if sparse else b'PLJRNL01') + struct.pack('>Q', base) + bytes(4)
    result = bytearray(prefix + struct.pack('>I', crc(prefix)))
    maximum = -(1 << 63)
    entries = []
    for offset in range(base, end):
        payload = b'' if sparse and offset < 2 else struct.pack('>Q', offset) + fixture[8:]
        entry = b'PLENTRY1' + struct.pack('>IQII', len(payload), offset, 1, crc(payload))
        entries.append(dict(offset=offset, position=len(result), prefix_max=maximum,
                            payload_sha256=sha(payload), retained=bool(payload)))
        result.extend(entry + struct.pack('>I', crc(entry)) + payload)
        if payload:
            require(payload[16] == 2 and crc(payload[21:]) == int.from_bytes(payload[17:21], 'big'),
                    'canonical Kafka protected bytes')
            maximum = max(maximum, int.from_bytes(payload[35:43], 'big', signed=True))
    return bytes(result), entries, maximum


def certify(path, raw, d, fixture, sparse):
    stem = f"{d['base']:016x}-{d['generation']:016x}"
    expected, entries, maximum = expected_file(d['base'], d['end'], fixture, sparse)
    actual = read(path / (stem + '.journal'))
    require(actual == expected, 'selected canonical sparse/dense payload and logical extent')
    require(d['kind'] == int(sparse) and d['entries'] == len(entries)
            and d['bytes'] == len(actual) and d['fingerprint'] == crc(actual)
            and d['max_time'] == maximum, 'descriptor exact selected bytes/maximum/count')
    bundle = read(path / (stem + '.seek'))
    protected(bundle, b'PLSEEK02' if sparse else b'PLSEEK01', 72)
    require(bundle[8:64] == raw, 'sidecar exact descriptor/kind binding')
    count = int.from_bytes(bundle[64:68], 'big')
    require(1 <= count <= len(entries) and len(bundle) == 72 + 24 * count, 'sidecar count/length')
    previous = -1
    by_offset = {e['offset']: e for e in entries}
    for ordinal in range(count):
        offset, position, prefix = struct.unpack('>QQq', bundle[68 + ordinal * 24:92 + ordinal * 24])
        require(offset in by_offset and offset > previous, 'sidecar unique entry offsets')
        entry = by_offset[offset]
        require(position == entry['position'] and prefix == entry['prefix_max'], 'sidecar physical/prefix maximum')
        require(ordinal != 0 or offset == d['base'], 'sidecar first logical entry')
        previous = offset
    return entries


def state(path, fixture, kind, phase, stage):
    sparse = phase not in OLD
    case = json.loads(read(path / 'case.json'))
    expected_offsets = [2, 3] if sparse else [0, 1, 2, 3]
    require(case == dict(schema=1, kind=kind, phase=phase, stage=stage, source_end=4,
                         confirmed_floor=0, cleaned_end=3, expected_retained_offsets=expected_offsets,
                         active_offset=3, physical_power_loss_claim=False), 'declared capture identity/seed/phase')
    files = list(path.iterdir())
    require(len(files) <= 72 and all(f.is_file() and not f.is_symlink() for f in files), 'flat bounded capture')
    m = manifest(read(path / 'manifest'))
    require(m['magic'] == ('PLSEGM03' if sparse else 'PLSEGM01')
            and m['revision'] == (4 if sparse else 3), 'one atomic changed-generation authority')
    names, entries = {'manifest', 'case.json'}, []
    for raw, d in m['selected']:
        require(d['base'] in range(3) and d['end'] == d['base'] + 1
                and d['generation'] == (4 if sparse else d['base']), 'static selected epoch/range')
        entries.extend(certify(path, raw, d, fixture, sparse))
        stem = f"{d['base']:016x}-{d['generation']:016x}"
        names.update({stem + '.journal', stem + '.seek'})
    active_name = f"{m['active']:016x}-{m['active_generation']:016x}.journal"
    active, active_entries, _ = expected_file(3, 4, fixture, False)
    require(read(path / active_name) == active, 'protected active record unchanged')
    names.add(active_name)
    entries.extend(active_entries)
    require([e['offset'] for e in entries] == [0, 1, 2, 3]
            and [e['offset'] for e in entries if e['retained']] == expected_offsets,
            'preserved logical coverage and exact last-key/protected retained values')
    for raw, d in m['obsolete']:
        require(d['generation'] == d['base'] and d['kind'] == 0, 'obsolete actual seeded generation')
        expected, _, maximum = expected_file(d['base'], d['end'], fixture, False)
        require(d['bytes'] == len(expected) and d['fingerprint'] == crc(expected)
                and d['max_time'] == maximum, 'obsolete original byte fingerprint')
        stem = f"{d['base']:016x}-{d['generation']:016x}"
        if (path / (stem + '.journal')).exists():
            require(read(path / (stem + '.journal')) == expected, 'remaining obsolete bytes')
        # A partial unlink may leave only a sidecar. Its original binding can
        # be checked without promoting the obsolete data to selected authority.
        if (path / (stem + '.seek')).exists():
            bundle = read(path / (stem + '.seek'))
            protected(bundle, b'PLSEEK01', 72)
            require(bundle[8:64] == raw, 'remaining obsolete sidecar binding')
    if stage == 'recovered':
        require(not m['obsolete'] and {f.name for f in files} == names, 'joined durable obsolete/staging cleanup')
    return dict(kind=kind, phase=phase, stage=stage, selected_revision=m['revision'],
                logical_offsets=[e['offset'] for e in entries], retained_offsets=expected_offsets,
                floor=0, end=4, all_file_sha256={f.name: sha(read(f)) for f in sorted(files)})


def verify(histories, fixture):
    require(crc(b'123456789') == 0xe3069283, 'Castagnoli published vector')
    results = []
    for kind in ('io-error', 'process-exit'):
        directory = histories / kind
        require({p.name for p in directory.iterdir()} == PHASES, 'complete fault phase denominator')
        for phase in sorted(PHASES):
            paired = [state(directory / phase / stage, fixture, kind, phase, stage)
                      for stage in ('interrupted', 'recovered')]
            require(paired[0]['retained_offsets'] == paired[1]['retained_offsets'], 'recovery changed authoritative values')
            results.append(dict(kind=kind, phase=phase, states=paired))
    return dict(passed=True, histories=len(results), states=2 * len(results), results=results,
                fixture_sha256=sha(fixture), scope='Finite static ordinary-key local IO/process captures; no physical powerloss/global qualification')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--histories', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    files = sorted(p for p in args.histories.rglob('*') if p.is_file())
    before = {str(p): sha(read(p)) for p in files}
    result = verify(args.histories, read(args.fixture))
    require(before == {str(p): sha(read(p)) for p in files}, 'raw input changed')
    result.update(inputs_unchanged=True, raw_input_files=len(files), checker_sha256=sha(Path(__file__).read_bytes()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ('passed', 'histories', 'states', 'raw_input_files')}))


if __name__ == '__main__':
    main()

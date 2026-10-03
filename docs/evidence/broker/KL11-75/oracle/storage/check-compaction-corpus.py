#!/usr/bin/env python3
"""Independently decode selected disk files and compare actual Apache cleaner output.

The finite declared corpus covers three releases, keyed values, nullable keys,
empty keys, tombstones, the exact expiry boundary and protected active suffixes.
No Rust parser, encoder, test verdict or emitted expected-value list is imported.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('history', HERE / 'check-compaction-history.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
MIN_TIME = -(1 << 63)
RELEASES = ('4.1.2', '4.2.1', '4.3.1')
SCENARIOS = {
    'mixed': (8, 10, [
        (2000, 'cleaner-first-horizon-sparse'),
        (2999, 'cleaner-before-horizon-sparse'),
        (3000, 'cleaner-equal-horizon-sparse'),
        (3001, 'cleaner-after-horizon-sparse')]),
    'allremoved': (3, 4, [
        (2000, 'cleaner-intermediate-empty-dropped'), (3000, 'cleaner-last-empty-61')]),
    'nullonly': (2, 3, [
        (2000, 'cleaner-nullonly-empty-61'), (3000, 'cleaner-nullonly-empty-61')]),
}


class Reader:
    def __init__(self, raw):
        self.raw = raw
        self.at = 0

    def take(self, size):
        CHECK.require(0 <= size <= len(self.raw) - self.at, 'bounded decoded field')
        result = self.raw[self.at:self.at + size]
        self.at += size
        return result

    def signed(self, bits=32):
        value = 0
        for ordinal in range((bits + 6) // 7):
            byte = self.take(1)[0]
            value |= (byte & 127) << (7 * ordinal)
            if byte < 128:
                CHECK.require(value < 1 << bits, 'varint range')
                return (value >> 1) ^ -(value & 1)
        raise ValueError('varint termination')

    def nullable(self):
        size = self.signed()
        CHECK.require(size >= -1, 'nullable field length')
        return None if size == -1 else self.take(size).hex()


def batches(data):
    reader = Reader(data)
    result = []
    while reader.at < len(data):
        CHECK.require(len(result) < 16, 'batch denominator')
        head = reader.take(12)
        base, size = struct.unpack('>qi', head)
        CHECK.require(base >= 0 and 49 <= size <= 32768, 'batch logical base/length')
        raw = head + reader.take(size)
        CHECK.require(raw[16] == 2 and CHECK.crc(raw[21:]) == int.from_bytes(raw[17:21], 'big'),
                      'Kafka magic and protected bytes')
        attrs, delta, timestamp, maximum, producer, epoch, sequence, count = struct.unpack('>hiqqqhii', raw[21:61])
        CHECK.require(attrs in (0, 64) and delta >= 0 and producer == epoch == sequence == -1
                      and 0 <= count <= 16, 'ordinary batch feature/count denominator')
        records, body = [], Reader(raw[61:])
        previous = -1
        for _ in range(count):
            record = Reader(body.take(body.signed()))
            CHECK.require(record.take(1) == b'\0', 'ordinary record attributes')
            time, offset = timestamp + record.signed(64), record.signed()
            CHECK.require(previous < offset <= delta and -(1 << 63) <= time < 1 << 63,
                          'record offset/timestamp arithmetic')
            previous = offset
            key, value, headers = record.nullable(), record.nullable(), []
            header_count = record.signed()
            CHECK.require(0 <= header_count <= 16, 'header denominator')
            for _ in range(header_count):
                header = record.take(record.signed()).decode('utf-8')
                headers.append(dict(key=header, value_hex=record.nullable()))
            CHECK.require(record.at == len(record.raw), 'record exact encoded length')
            records.append(dict(offset=base + offset, timestamp=time, key_hex=key,
                                value_hex=value, headers=headers))
        CHECK.require(body.at == len(body.raw), 'batch exact record count/length')
        result.append(dict(base_offset=base, last_offset=base + delta, logical_span=delta + 1,
                           record_count=count, bytes=len(raw), attributes=attrs,
                           base_timestamp=timestamp, max_timestamp=maximum,
                           delete_horizon_ms=timestamp if attrs == 64 else None,
                           producer_id=producer, records=records))
    return result


def ordinary_file(data, magic, first, span, payload):
    CHECK.require(data[:8] == magic and len(data) == 56 + len(payload), 'exact selected file envelope')
    CHECK.require(data[8:20] == struct.pack('>Q', first) + bytes(4)
                  and CHECK.crc(data[:20]) == int.from_bytes(data[20:24], 'big'),
                  'file base/header protection')
    header = data[24:56]
    CHECK.require(header[:8] == b'PLENTRY1'
                  and struct.unpack('>IQII', header[8:28]) == (len(payload), first, span, CHECK.crc(payload))
                  and CHECK.crc(header[:28]) == int.from_bytes(header[28:], 'big')
                  and data[56:] == payload, 'selected logical entry/CRC/exact Apache bytes')


def golden(directory, manifest, name):
    rows = [case for case in manifest['cases'] if case['file'] == name + '.bin']
    CHECK.require(len(rows) == 1, 'unique independently executed Apache fixture')
    case = rows[0]
    data = CHECK.read(directory / case['file'])
    CHECK.require(len(data) == case['bytes'] and CHECK.sha(data) == case['sha256'], 'Apache fixture provenance')
    decoded = batches(data)
    CHECK.require(decoded == case['batches'], 'independent native decoder agrees with actual Apache records')
    return data, decoded


def state(path, release, scenario, clock, phase, ordinal, fixture_directory, goldens):
    sealed_end, end, sequence = SCENARIOS[scenario]
    expected_name = sequence[ordinal][1]
    input_name, active_name = f'cleaner-{scenario}-input', f'cleaner-{scenario}-active-protected'
    payload, decoded = golden(fixture_directory, goldens, expected_name)
    initial, initial_batches = golden(fixture_directory, goldens, input_name)
    active, active_batches = golden(fixture_directory, goldens, active_name)
    previous = initial if ordinal == 0 else golden(fixture_directory, goldens, sequence[ordinal - 1][1])[0]
    source_count = sum(b['record_count'] for b in batches(previous))
    retained = [r for batch in decoded for r in batch['records']]
    latest = {}
    for batch in initial_batches:
        for record in batch['records']:
            if record['key_hex'] is not None:
                latest[record['key_hex']] = record
    expected = sorted((record for record in latest.values()
                       if clock < 3000 or record['value_hex'] is not None), key=lambda r: r['offset'])
    CHECK.require(retained == expected, 'independent last-key/null-key/tombstone expiry semantics')
    case = json.loads(CHECK.read(path / 'case.json'))
    expected_case = dict(schema=1, release=release, scenario=scenario, clock_ms=clock, phase=phase,
                         expected_fixture=expected_name, active_fixture=active_name,
                         logical_floor=0, logical_end=end,
                         outcome=dict(start_offset=0, end_offset=sealed_end, scanned_records=source_count,
                                      retained_records=len(retained), rewritten_segments=1,
                                      bytes_before=56 + len(previous), bytes_after=56 + len(payload)))
    CHECK.require(case == expected_case, 'actual capture identity and independent outcome denominator')
    manifest = CHECK.read(path / 'manifest')
    CHECK.protected(manifest, b'PLSEGM03', 76)
    revision = ordinal + 2
    CHECK.require(len(manifest) == 132
                  and struct.unpack('>QQQQQQIIII', manifest[8:72]) ==
                  (revision, 0, 0, 0, sealed_end, 1, 1, 0, 0, 0),
                  'durable joined generation/floor/protected active authority')
    descriptor = manifest[72:128]
    d = CHECK.descriptor(descriptor)
    name = f'{0:016x}-{revision:016x}'
    selected = CHECK.read(path / (name + '.journal'))
    ordinary_file(selected, b'PLSPRS01', 0, sealed_end, payload)
    maximum = max((b['max_timestamp'] for b in decoded), default=MIN_TIME)
    CHECK.require(d == dict(base=0, end=sealed_end, generation=revision, bytes=len(selected),
                           entries=1, max_time=maximum, fingerprint=CHECK.crc(selected), kind=1),
                  'sparse descriptor byte/extent/time/kind binding')
    seek = CHECK.read(path / (name + '.seek'))
    CHECK.protected(seek, b'PLSEEK02', 72)
    CHECK.require(len(seek) == 96 and seek[8:64] == descriptor
                  and seek[64:92] == struct.pack('>IQQq', 1, 0, 24, MIN_TIME),
                  'seek descriptor/physical position/prefix maximum binding')
    active_file = f'{sealed_end:016x}-{1:016x}.journal'
    ordinary_file(CHECK.read(path / active_file), b'PLJRNL01', sealed_end, end - sealed_end, active)
    CHECK.require({p.name for p in path.iterdir()} ==
                  {'manifest', 'case.json', name + '.journal', name + '.seek', active_file},
                  'joined exact inventory with no stale authority or staging')
    return dict(release=release, scenario=scenario, clock_ms=clock, phase=phase,
                retained_records=retained, protected_records=[r for b in active_batches for r in b['records']],
                selected_header_batches=decoded, floor=0, end=end, revision=revision,
                raw_sha256={p.name: CHECK.sha(CHECK.read(p)) for p in sorted(path.iterdir())})


def verify(corpus, fixtures):
    results = []
    CHECK.require({p.name for p in corpus.iterdir()} == set(RELEASES), 'complete release denominator')
    for release in RELEASES:
        golden_directory = fixtures / release
        golden_manifest = golden_directory / 'goldens.json'
        CHECK.require(golden_manifest.is_file() and not golden_manifest.is_symlink()
                      and golden_manifest.stat().st_size <= 1024 * 1024,
                      'bounded independent Apache JSON manifest')
        goldens = json.loads(golden_manifest.read_bytes())
        CHECK.require({p.name for p in (corpus / release).iterdir()} == set(SCENARIOS),
                      'complete scenario denominator')
        for scenario, (_, _, sequence) in SCENARIOS.items():
            CHECK.require({p.name for p in (corpus / release / scenario).iterdir()} ==
                          {str(clock) for clock, _ in sequence}, 'complete clock denominator')
            for ordinal, (clock, _) in enumerate(sequence):
                CHECK.require({p.name for p in (corpus / release / scenario / str(clock)).iterdir()} ==
                              {'selected', 'reopened'}, 'complete reopen denominator')
                pair = [state(corpus / release / scenario / str(clock) / phase,
                              release, scenario, clock, phase, ordinal, golden_directory, goldens)
                        for phase in ('selected', 'reopened')]
                CHECK.require(pair[0]['raw_sha256']['manifest'] == pair[1]['raw_sha256']['manifest']
                              and pair[0]['retained_records'] == pair[1]['retained_records'],
                              'reopen preserves authority and exact retained records')
                results.extend(pair)
    return dict(passed=True, cases=len(results) // 2, states=len(results), results=results,
                scope='Finite three-release ordinary cleaner raw files and public record semantics; no replication/transaction/powerloss claim')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    files = sorted(p for p in args.corpus.rglob('*') if p.is_file())
    before = {str(p): CHECK.sha(CHECK.read(p)) for p in files}
    result = verify(args.corpus, args.fixtures)
    CHECK.require(before == {str(p): CHECK.sha(CHECK.read(p)) for p in files}, 'captured inputs changed')
    result.update(inputs_unchanged=True, raw_input_files=len(files),
                  checker_sha256=CHECK.sha(Path(__file__).read_bytes()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ('passed', 'cases', 'states', 'raw_input_files')}))


if __name__ == '__main__':
    main()

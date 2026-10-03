#!/usr/bin/env python3
"""Independent V2 logical-floor/victim/selected-byte checks on actual captures.

The immutable ordinary BASIC fixture is the declared input. No Rust encoder,
implementation module or emitted verdict is imported. Retired files are never
promoted to selected content, and partial cleanup can leave victims absent.
"""
import argparse
import json
from pathlib import Path
import struct

from legacy_oracle import bounded, checked, crc, descriptor, journal, require, seek, sha

PHASES = {'ManifestWritten', 'ManifestSynced', 'ManifestRenamed', 'DirectorySynced',
          'RetentionDataRemoved', 'RetentionIndexRemoved', 'RetentionCleanupSynced',
          'RetentionClearWritten', 'RetentionClearSynced', 'RetentionClearRenamed',
          'RetentionClearDirectorySynced'}
PRE_PUBLICATION = {'ManifestWritten', 'ManifestSynced', 'NewActiveSynced'}


def expected_journal(base, end, fixture):
    header = b'PLJRNL01' + struct.pack('>Q', base) + bytes(4)
    result = bytearray(header + struct.pack('>I', crc(header)))
    for offset in range(base, end):
        payload = struct.pack('>Q', offset) + fixture[8:]
        entry = b'PLENTRY1' + struct.pack('>IQII', len(payload), offset, 1, crc(payload))
        result.extend(entry + struct.pack('>I', crc(entry)) + payload)
    return bytes(result)


def manifest(data):
    require(data[:8] in (b'PLSEGM01', b'PLSEGM02'), 'manifest revision magic')
    checked(data, data[:8], 48)
    if data[:8] == b'PLSEGM02':
        require(len(data) >= 68, 'V2 manifest header')
        revision, origin, physical, floor, active, generation, count, victim_count = struct.unpack('>QQQQQQII', data[8:64])
        header = 64
    else:
        revision, origin, active, generation, count = struct.unpack('>QQQQI', data[8:44])
        physical, floor, victim_count, header = origin, origin, 0, 44
    require(origin == 0 and origin <= physical <= floor <= (1 << 63) - 1 and
            physical <= active and generation <= revision, 'floor/physical/origin/generation bounds')
    require(count < 16 and count + victim_count <= 16 and len(data) == header + 4 + 56 * (count + victim_count),
            'bounded exact selected/victim manifest size')
    selected, victims, next_offset = [], [], physical
    for ordinal in range(count + victim_count):
        raw = data[header + ordinal * 56:header + (ordinal + 1) * 56]
        d = descriptor(raw)
        require(d['generation'] <= revision and d['end'] <= (1 << 63) - 1 and
                d['end'] - d['base'] >= d['entries'] and d['bytes'] >= 24 + d['entries'] * 33,
                'protected descriptor structural bounds')
        if ordinal < count:
            require(d['base'] == next_offset, 'selected descriptor continuity')
            next_offset = d['end']
            selected.append((raw, d))
        else:
            require(d['base'] >= origin and d['end'] <= physical and
                    (not victims or victims[-1][1]['end'] == d['base']), 'victim prefix/physical binding')
            victims.append((raw, d))
    require(next_offset == active, 'selected active boundary')
    require(not victims or victims[-1][1]['end'] == physical, 'last victim physical boundary')
    return {'revision': revision, 'origin': origin, 'physical': physical, 'floor': floor,
            'active': active, 'generation': generation, 'selected': selected, 'victims': victims}


def state(path, fixture, kind, target, phase, stage):
    expected_floor = 1 if phase in PRE_PUBLICATION else target
    case = json.loads(bounded(path / 'case.json'))
    require(case['kind'] == kind and case['requested_floor'] == target and case['phase'] == phase and
            case['stage'] == stage and case['previous_acknowledged_floor'] == 1 and
            case['durable_end'] == case['confirmed_high_watermark'] == case['retain_from'] == 5 and
            case['expected_recovered_floor'] == expected_floor and not case['physical_power_loss_claim'],
            'declared static fixture/phase/guard bounds')
    files = list(path.iterdir())
    require(len(files) <= 72 and all(p.is_file() and not p.is_symlink() for p in files),
            'bounded flat captured file layout')
    m = manifest(bounded(path / 'manifest'))
    require(m['floor'] == expected_floor and 1 <= m['floor'] <= 5,
            'confirmed acknowledged floor/phase mismatch')
    entries, selected, names = [], [], {'manifest', 'case.json'}
    for raw, d in m['selected']:
        stem = f"{d['base']:016x}-{d['generation']:016x}"
        decoded = journal(path / (stem + '.journal'), d['base'], fixture)
        for key in ['end', 'bytes', 'max_time', 'fingerprint']:
            require(decoded[key] == d[key], 'selected descriptor ' + key)
        require(len(decoded['entries']) == d['entries'], 'selected entry count')
        index = seek(path / (stem + '.seek'), raw, d, decoded)
        entries.extend(decoded['entries'])
        names.update({stem + '.journal', stem + '.seek'})
        selected.append({'descriptor': d, 'journal_sha256': decoded['sha256'], 'index': index})
    active_name = f"{m['active']:016x}-{m['generation']:016x}.journal"
    active = journal(path / active_name, m['active'], fixture)
    entries.extend(active['entries'])
    names.add(active_name)
    require(active['end'] == 5 and m['floor'] <= active['end'], 'floor exceeds actual confirmed end')
    require([e['offset'] for e in entries] == list(range(m['physical'], 5)), 'survivor physical prefix continuity')
    require([e['offset'] for e in entries if e['offset'] >= m['floor']] == list(range(m['floor'], 5)),
            'logically retained canonical records missing')
    victims = []
    for raw, d in m['victims']:
        require(1 <= d['base'] < d['end'] <= m['physical'] <= m['floor'] <= 5,
                'victim overlaps logical/confirmed protected records')
        require(d['entries'] == d['end'] - d['base'], 'seeded victim entry/range binding')
        projected = expected_journal(d['base'], d['end'], fixture)
        require(d['bytes'] == len(projected) and d['fingerprint'] == crc(projected) and
                d['max_time'] == int.from_bytes(fixture[35:43], 'big', signed=True),
                'victim seeded bytes/timestamp/fingerprint')
        stem = f"{d['base']:016x}-{d['generation']:016x}"
        victim_journal, victim_seek = path / (stem + '.journal'), path / (stem + '.seek')
        if victim_journal.exists():
            require(bounded(victim_journal) == projected, 'remaining victim seeded bytes')
        if victim_seek.exists():
            # Recreate only the declared static entry positions/prefix maxima.
            maximum = -(1 << 63)
            rows = []
            timestamp = int.from_bytes(fixture[35:43], 'big', signed=True)
            for offset in range(d['base'], d['end']):
                rows.append({'offset': offset, 'position': 24 + (offset - d['base']) * (32 + len(fixture)),
                             'prefix_max': maximum})
                maximum = max(maximum, timestamp)
            seek(victim_seek, raw, d, {'entries': rows})
        victims.append({'descriptor': d, 'remaining_data': victim_journal.exists(),
                        'remaining_seek': victim_seek.exists()})
    if stage == 'recovered':
        require(not victims and {p.name for p in files} == names, 'recovered cleanup not complete')
    return {'kind': kind, 'target': target, 'phase': phase, 'stage': stage, 'logical_floor': m['floor'],
            'physical_base': m['physical'], 'actual_end': active['end'], 'selected': selected,
            'victims': victims, 'manifest_sha256': sha(bounded(path / 'manifest')),
            'retained_records': [e for e in entries if e['offset'] >= m['floor']],
            'all_file_hashes': {p.name: sha(bounded(p)) for p in sorted(files)}}


def verify(histories, fixture):
    results = []
    for kind in ['retention-error', 'retention-exit']:
        for target in [3, 5]:
            phases = PHASES | ({'NewActiveSynced'} if target == 5 else set())
            directory = histories / kind / str(target)
            require({p.name for p in directory.iterdir()} == phases, 'complete declared phase denominator')
            for phase in sorted(phases):
                interrupted = state(directory / phase / 'interrupted', fixture, kind, target, phase, 'interrupted')
                recovered = state(directory / phase / 'recovered', fixture, kind, target, phase, 'recovered')
                require(interrupted['logical_floor'] == recovered['logical_floor'] and
                        interrupted['retained_records'] == recovered['retained_records'],
                        'paired selected floor/retained bytes changed')
                results.append({'kind': kind, 'target': target, 'phase': phase,
                                'interrupted': interrupted, 'recovered': recovered})
    require(len(results) == 46, 'actual retention history denominator')
    return {'passed': True, 'histories': 46, 'states': 92, 'fixture_sha256': sha(fixture), 'results': results,
            'scope': 'Finite actual local retention publication/partial cleanup histories; independent static seed/selected/victim parsing',
            'limitations': ['Process-visible file states only; no physical powerloss or replicated quorum qualification.',
                            'Whole input boundaries, static BASIC fixture only; API21/Apache/public peers are separate.',
                            'Configured resource envelopes and scalar getter behavior are separate compiled tests.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--histories', type=Path, required=True)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(crc(b'123456789') == 0xe3069283, 'independent Castagnoli published vector')
    files = [p for p in args.histories.rglob('*') if p.is_file()]
    before = {str(p): sha(bounded(p)) for p in files}
    result = verify(args.histories, bounded(args.fixture))
    after = {str(p): sha(bounded(p)) for p in files}
    require(before == after, 'retention input changed')
    result.update(inputs_unchanged=True, raw_input_files=len(before))
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['passed', 'histories', 'states', 'raw_input_files']}))


if __name__ == '__main__':
    main()

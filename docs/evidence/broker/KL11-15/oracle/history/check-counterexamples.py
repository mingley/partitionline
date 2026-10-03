#!/usr/bin/env python3
"""Positive controls and explicitly changed, outer-checksummed negative inputs.

Every mutation is written to a new directory; original captures are read only.
These finite deliberate corruptions test checker guards, not runtime fault runs.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import struct

from history_oracle import verify
from snapshot_oracle import read_journal
from wal_oracle import Rejected, crc32c, require


def operations(data: bytes) -> list[bytes]:
    require(len(data) >= 24, "mutation source journal header")
    rows, offset = [], 24
    while offset < len(data):
        require(offset + 32 <= len(data), "mutation source entry header")
        size = struct.unpack_from(">I", data, offset + 8)[0]
        require(offset + 32 + size <= len(data), "mutation source entry payload")
        rows.append(data[offset + 32:offset + 32 + size])
        offset += 32 + size
    return rows


def journal(rows: list[bytes]) -> bytes:
    header = b"PLJRNL01" + bytes(12)
    result = bytearray(header + struct.pack(">I", crc32c(header)))
    for index, payload in enumerate(rows):
        entry = b"PLENTRY1" + struct.pack(">IQII", len(payload), index, 1, crc32c(payload))
        result.extend(entry + struct.pack(">I", crc32c(entry)) + payload)
    return bytes(result)


def seal_image(data: bytearray) -> None:
    header = struct.unpack_from(">I", data, 12)[0]
    struct.pack_into(">I", data, header - 4, crc32c(data[:header - 4]))
    struct.pack_into(">I", data, len(data) - 8, crc32c(data[:-24]))
    struct.pack_into(">I", data, len(data) - 4, crc32c(data[-24:-4]))


def hashes(path: Path) -> dict:
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(path.rglob('*')) if p.is_file()}


def binary_checks(seed: Path, output: Path) -> list[dict]:
    original = read_journal(seed)
    rows = operations(seed.read_bytes())
    install_index = next(i for i, row in enumerate(rows) if row[8] == 5)
    install = rows[install_index]
    generation = install[56:72].hex()
    name = 'snapshot-' + generation + '.image'
    source_images = seed.parent / 'images'
    require(source_images.is_dir(), "counterexample source images")
    definitions = [
        ('authority', 16, '>B', 2, 'Install authority/term/peer'),
        ('stale-term', 24, '>Q', 1, 'Install authority/term/peer'),
        ('foreign-leader', 32, '>I', 99, 'Install authority/term/peer'),
        ('foreign-peer', 36, '>I', 99, 'Install authority/term/peer'),
        ('local-sequence', 40, '>Q', 1, 'local Install authority'),
        ('leader-commit', 48, '>Q', 0, 'local Install authority'),
        ('prior-tail', 128, '>Q', 0, 'Install prior tail/commit mismatch'),
        ('prior-commit', 136, '>Q', 0, 'Install prior tail/commit mismatch'),
        ('retained-tail', 152, '>Q', 1, 'Install retained suffix/tail mismatch'),
        ('result-commit', 160, '>Q', 0, 'local Install authority'),
        ('prior-present', 168, '>B', 1, 'Install prior selection flag'),
        ('record-count', 88, '>I', 3, 'Install descriptor/image binding'),
        ('payload-size', 96, '>Q', 0, 'Install descriptor/image binding'),
        ('image-size', 104, '>Q', 1, 'Install descriptor/image binding'),
        ('image-crc', 112, '>I', 0, 'Install descriptor/image binding'),
        ('reserved', 116, '>I', 1, 'nonzero reserved field'),
        ('foreign-group', 216, '>i', 7, 'Install foreign group'),
    ]
    special = [
        ('missing-install', 'none', 'prefix'),
        ('changed-committed-image', 'committed', 'snapshot changed committed overlap'),
        ('image-record-index', 'index', 'image record index/term continuity'),
        ('image-base-term', 'term', 'image aggregate/base term'),
        ('image-foreign-group', 'group', 'foreign image group'),
        ('image-truncated-seal', 'truncate', 'image exact encoded length'),
    ]
    results = []
    for title, offset, fmt, value, expected in definitions:
        case = output / title
        case.mkdir(parents=True)
        shutil.copytree(source_images, case / 'images')
        changed = bytearray(install)
        struct.pack_into(fmt, changed, offset, value)
        altered = rows.copy()
        altered[install_index] = bytes(changed)
        target = case / 'metadata.wal'
        target.write_bytes(journal(altered))
        try:
            read_journal(target)
            raise AssertionError('counterexample accepted: ' + title)
        except Rejected as error:
            require(expected in str(error), 'wrong rejection for ' + title + ': ' + str(error))
            result = {'name': title, 'changed_offset': offset, 'expected_guard': expected,
                      'actual_rejection': str(error), 'all_outer_journal_crcs_repaired': True}
        result['artifacts_sha256'] = hashes(case)
        results.append(result)
    for title, change, expected in special:
        case = output / title
        case.mkdir(parents=True)
        shutil.copytree(source_images, case / 'images')
        altered = rows.copy()
        image_path = case / 'images' / name
        image = bytearray(image_path.read_bytes())
        changed = bytearray(install)
        if change == 'none':
            del altered[install_index]
        elif change == 'truncate':
            image_path.write_bytes(image[:-1])
        else:
            header = struct.unpack_from('>I', image, 12)[0]
            if change == 'committed':
                cursor = header
                while image[cursor + 16] == 1:
                    cursor += 32
                require(struct.unpack_from('>I', image, cursor + 24)[0] > 0, 'mutation data record')
                image[cursor + 32] ^= 1
            elif change == 'index':
                struct.pack_into('>Q', image, header + 8, 9)
            elif change == 'term':
                struct.pack_into('>Q', image, 40, 1)
            elif change == 'group':
                image[68] ^= 1
            seal_image(image)
            image_path.write_bytes(image)
            struct.pack_into('>I', changed, 112, crc32c(image))
            altered[install_index] = bytes(changed)
        target = case / 'metadata.wal'
        target.write_bytes(journal(altered))
        if change == 'none':
            # A valid WAL without Install must leave the complete image inert.
            recovered = read_journal(target)
            require(recovered.selected is None and recovered.records == original.records and
                    recovered.committed == original.committed, 'unreceipted image selected')
            result = {'name': title, 'disposition': 'accepted WAL; image remains inert',
                      'selected_snapshot': recovered.selected}
        else:
            try:
                read_journal(target)
                raise AssertionError('counterexample accepted: ' + title)
            except Rejected as error:
                require(expected in str(error), 'wrong rejection for ' + title + ': ' + str(error))
                result = {'name': title, 'expected_guard': expected, 'actual_rejection': str(error),
                          'outer_journal_crcs_repaired': True, 'image_seals_repaired': change != 'truncate'}
        result['artifacts_sha256'] = hashes(case)
        results.append(result)
    return results


def trace_checks(seed: Path, output: Path) -> list[dict]:
    output.mkdir(parents=True)
    original = json.loads(seed.read_text())
    verify(seed)
    changes = [
        ('forged-ack-accepted', 'ack_snapshot', lambda e: 'input_origin' in e['args'],
         lambda e: e['result'].update(disposition='accepted'), 'snapshot ack fabricated/stale/descriptor mismatch'),
        ('expired-ack-accepted', 'ack_snapshot', lambda e: e['result']['disposition'] == 'rejected' and 'input_origin' not in e['args'],
         lambda e: e['result'].update(disposition='accepted'), 'snapshot ack fabricated/stale/descriptor mismatch'),
        ('wrong-emitted-done', 'snapshot_chunk', lambda e: True,
         lambda e: e['result'].update(done=not e['result']['done']), 'emitted chunk differs'),
        ('wrong-emitted-image-byte', 'snapshot_chunk', lambda e: True,
         lambda e: e['result'].update(bytes_hex=('00' if e['result']['bytes_hex'][:2] != '00' else '01') + e['result']['bytes_hex'][2:]),
         'emitted chunk differs'),
        ('invented-partial-origin', 'partial_snapshot_chunk', lambda e: True,
         lambda e: e['args']['input_origin'].update(source_ordinal=0), 'partial chunk emitted origin missing'),
        ('incomplete-finish-accepted', 'finish_incomplete_snapshot', lambda e: True,
         lambda e: e['result'].update(disposition='accepted'), 'incomplete image installation accepted'),
        ('changed-offer-descriptor', 'prepare_snapshot', lambda e: True,
         lambda e: e['result']['descriptor'].update(checksum=e['result']['descriptor']['checksum'] ^ 1),
         'snapshot offer selected commit binding'),
        ('unreceipted-selection', 'checkpoint', lambda e: True,
         lambda e: next(s for s in e['after'] if s['node_id'] == e['node_id']).update(wal_durable_ops=0),
         'checkpoint selecting operation count'),
    ]
    results = []
    for name, kind, predicate, mutate, expected in changes:
        changed = copy.deepcopy(original)
        event = next(e for e in changed['events'] if e['kind'] == kind and predicate(e))
        mutate(event)
        target = output / (name + '.json')
        target.write_text(json.dumps(changed, indent=2) + '\n')
        try:
            verify(target, seed.parent)
            raise AssertionError('trace counterexample accepted: ' + name)
        except (ValueError, KeyError) as error:
            require(expected in str(error), 'wrong trace guard for ' + name + ': ' + str(error))
            results.append({'name': name, 'event_ordinal': event['ordinal'], 'actual_rejection': str(error)})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wal', type=Path, required=True)
    parser.add_argument('--trace', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'counterexample output must be fresh')
    args.output.mkdir(parents=True)
    results = binary_checks(args.wal, args.output / 'binary')
    if args.trace:
        results.extend(trace_checks(args.trace, args.output / 'trace'))
    (args.output / 'results.json').write_text(json.dumps({'scope': 'Finite checker controls, separately identified deliberate mutations',
        'positive_control_wal_sha256': hashlib.sha256(args.wal.read_bytes()).hexdigest(),
        'results': results, 'passed': True}, indent=2) + '\n')
    print(json.dumps({'passed': True, 'cases': len(results)}))


if __name__ == '__main__':
    main()

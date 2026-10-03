#!/usr/bin/env python3
"""Bounded independent CRC/control-schema and ordered native voter-history fixture checker."""
import argparse
import hashlib
import json
from pathlib import Path
import struct

MAX_FILE = 512 * 1024
MAX_FILES = 512
MAX_TOTAL = 16 * 1024 * 1024


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


class Cursor:
    def __init__(self, raw):
        self.raw = raw
        self.pos = 0

    def take(self, count):
        require(0 <= count <= len(self.raw) - self.pos, 'Truncated bounded schema')
        result = self.raw[self.pos:self.pos + count]
        self.pos += count
        return result

    def integer(self, fmt):
        return struct.unpack('>' + fmt, self.take(struct.calcsize(fmt)))[0]

    def uint(self, bits=32):
        value = 0
        for index in range((bits + 6) // 7):
            byte = self.integer('B')
            require(index * 7 < bits and (byte & 127) < 1 << min(7, bits - index * 7), 'Overflowed varint')
            value |= (byte & 127) << (index * 7)
            if byte < 128:
                require(index == 0 or byte != 0, 'Noncanonical varint')
                return value
        raise ValueError('Overlong varint')

    def signed(self, bits=32):
        value = self.uint(bits)
        return (value >> 1) ^ -(value & 1)

    def array(self, maximum=64):
        count = self.uint() - 1
        require(0 <= count <= maximum, 'Invalid compact array bound/null')
        return count

    def text(self, maximum):
        size = self.uint() - 1
        require(0 <= size <= maximum, 'Invalid compact string bound/null')
        return self.take(size).decode('utf-8', errors='strict')

    def tags(self):
        require(self.uint() == 0, 'Unexpected nonzero fixture schema tags')

    def finish(self):
        require(self.pos == len(self.raw), 'Unreviewed trailing schema bytes')


def crc32c(raw):
    crc = 0xffffffff
    for byte in raw:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def decode_control(key, value):
    require(len(key) == 4, 'Wrong native control-key length')
    version, kind = struct.unpack('>hh', key)
    require(version == 0 and kind in (5, 6), 'Unexpected native control-key version/type')
    cursor = Cursor(value)
    require(cursor.integer('h') == 0, 'Unsupported native value schema version')
    if kind == 5:
        feature = cursor.integer('h')
        require(feature in (0, 1), 'Unsupported pinned KRaft feature')
        cursor.tags(); cursor.finish()
        return {'type': 'kraft_version', 'version': 0, 'kraft_version': feature}
    rows = []
    for _ in range(cursor.array()):
        voter_id = cursor.integer('i')
        directory = cursor.take(16).hex()
        require(voter_id >= 0 and directory != '0' * 32, 'Invalid native voter identity')
        endpoints = []
        for _ in range(cursor.array(maximum=8)):
            endpoint = {'listener': cursor.text(64), 'host': cursor.text(256), 'port': cursor.integer('H')}
            require(endpoint == {'listener': 'CONTROLLER', 'host': '127.0.0.1', 'port': 19200 + voter_id}, 'Unexpected fixture endpoint')
            cursor.tags(); endpoints.append(endpoint)
        require(len(endpoints) == 1, 'Wrong fixture endpoint count')
        minimum, maximum = cursor.integer('h'), cursor.integer('h')
        require((minimum, maximum) == (0, 1), 'Unexpected pinned voter feature range')
        cursor.tags(); cursor.tags()
        rows.append({'id': voter_id, 'directory_id': directory, 'endpoints': endpoints,
                     'kraft_min': minimum, 'kraft_max': maximum})
    cursor.tags(); cursor.finish()
    require(rows and len({row['id'] for row in rows}) == len(rows), 'Empty/duplicate native voter set')
    require(len({row['directory_id'] for row in rows}) == len(rows), 'Duplicate native directory IDs')
    return {'type': 'voters', 'version': 0, 'voters': sorted(rows, key=lambda row: row['id'])}


def decode_batch(raw):
    require(61 <= len(raw) <= MAX_FILE, 'Native batch outside bounds')
    require(struct.unpack_from('>i', raw, 8)[0] == len(raw) - 12, 'Native batch size mismatch')
    base = struct.unpack_from('>q', raw, 0)[0]
    epoch = struct.unpack_from('>i', raw, 12)[0]
    require(base >= 0 and epoch == 5 and raw[16] == 2, 'Native batch offset/epoch/magic mismatch')
    require(struct.unpack_from('>I', raw, 17)[0] == crc32c(raw[21:]), 'Native CRC32C mismatch')
    require(struct.unpack_from('>h', raw, 21)[0] == 0x20, 'Native batch must be plain control/CreateTime')
    require(struct.unpack_from('>q', raw, 43)[0] == -1, 'Native control producer ID mismatch')
    count = struct.unpack_from('>i', raw, 57)[0]
    delta = struct.unpack_from('>i', raw, 23)[0]
    require(1 <= count <= 2 and delta == count - 1, 'Native count/last delta mismatch')
    cursor = Cursor(raw[61:]); rows = []
    for index in range(count):
        size = cursor.signed(); require(size >= 0, 'Negative native record size')
        record = Cursor(cursor.take(size))
        require(record.integer('b') == 0, 'Unexpected record attributes')
        timestamp_delta = record.signed(64)
        require(timestamp_delta == 0, 'Unexpected fixture timestamp delta')
        require(record.signed() == index, 'Nonconsecutive native record delta')
        key_size = record.signed(); require(key_size == 4, 'Unexpected native key size')
        key = record.take(key_size)
        value_size = record.signed(); require(0 <= value_size <= 8192, 'Native value outside bounds')
        value = record.take(value_size)
        require(record.signed() == 0, 'Unexpected native record headers'); record.finish()
        rows.append({'native_offset': base + index, 'native_epoch': epoch,
            'key': key, 'value': value, 'decoded': decode_control(key, value)})
    cursor.finish()
    return rows


def verify_release(directory):
    raw = (directory / 'manifest.json').read_bytes()
    require(len(raw) <= MAX_FILE, 'Oversized manifest')
    manifest = json.loads(raw)
    pins = json.loads((Path(__file__).resolve().parent / 'pins.json').read_bytes())
    expected = next((row for row in pins['releases'] if row['release'] == manifest['release']), None)
    require(expected is not None and manifest['source_sha'] == expected['source_sha']
        and manifest['client_jar_sha256'] == expected['client_jar_sha256'], 'Official fixture source/client pin mismatch')
    require(manifest['probe_sha256'] == sha((Path(__file__).resolve().parent / 'ApacheMembershipProbe.java').read_bytes()), 'Executable fixture probe pin mismatch')
    require(manifest['schema_version'] == 1 and manifest['native_epoch'] == 5 and manifest['local_normalized_term'] == 6 and manifest['native_voters_record_version'] == 0, 'Wrong fixture mapping/profile')
    files = manifest['files_sha256']
    require(0 < len(files) <= MAX_FILES, 'Fixture file count outside bound')
    actual = {str(p.relative_to(directory)) for p in directory.rglob('*') if p.is_file()}
    require(actual == set(files) | {'manifest.json'}, 'Missing/unpinned/unexpected fixture file')
    total = 0
    for name, expected in files.items():
        require(Path(name).as_posix() == name and not Path(name).is_absolute() and '..' not in Path(name).parts, 'Unsafe fixture path')
        path = directory / name
        require(not path.is_symlink() and path.stat().st_size <= MAX_FILE, 'Unsafe/oversized fixture file')
        contents = path.read_bytes(); total += len(contents)
        require(total <= MAX_TOTAL and sha(contents) == expected, 'Fixture byte pin mismatch/budget exceeded')
    rows = (directory / 'cases.tsv').read_text().splitlines()
    require(rows[0] == 'id\tcomponent\ttrace_file\texpected_disposition', 'Wrong cases.tsv schema')
    cases = [row.split('\t') for row in rows[1:]]
    require(len(cases) == manifest['cases'] and len({row[0] for row in cases}) == len(cases), 'Duplicate/missing named fixture cases')
    assertions = 0; batches = 0; records = 0; used_native = set(); decoded_configs = []
    for name, component, filename, disposition in cases:
        require(component == 'ApacheMembershipProbe' and filename in files, 'Wrong component/trace pin')
        trace = json.loads((directory / filename).read_bytes())
        require(trace['schema_version'] == 1 and trace['id'] == name and trace['release'] == manifest['release'] and trace['expected_disposition'] == disposition, 'Trace identity/disposition mismatch')
        require(disposition in ['one_change_component_behavior', 'unsupported_local_early_ack', 'permissive_component_requires_outer_fencing', 'component_timeout_requires_committed_removal_fence'], 'Unreviewed fixture disposition')
        configs = {}; fallback = None
        for step in trace['steps']:
            require(step.get('scenario', name) == name, 'Cross-scenario trace step')
            if step['event'] == 'control-batch':
                native = 'native/' + step['file']; require(native in files and native not in used_native, 'Missing/duplicate control batch')
                used_native.add(native)
                contents = (directory / native).read_bytes(); require(len(contents) == step['bytes'], 'Capture length mismatch')
                decoded = decode_batch(contents); require(len(decoded) == len(step['records']), 'Capture record manifest mismatch')
                batches += 1; records += len(decoded)
                for record, sidecar in zip(decoded, step['records']):
                    require(sidecar['native_offset'] == record['native_offset'] and sidecar['native_epoch'] == record['native_epoch'], 'Capture record position mismatch')
                    for field in ['key', 'value']:
                        path = 'native/' + sidecar[field + '_file']
                        require(path in files and path not in used_native, 'Missing/duplicate native sidecar')
                        used_native.add(path)
                        require((directory / path).read_bytes() == record[field], 'Native sidecar disagrees with actual CRC-framed record')
                    if record['decoded']['type'] == 'voters':
                        voters = [{field: voter[field] for field in ['id', 'directory_id']} for voter in record['decoded']['voters']]
                        configs[record['native_offset']] = voters
                        decoded_configs.append({'case': name, 'native_offset': record['native_offset'], 'local_index': record['native_offset'] + 1, 'decoded': record['decoded']})
            elif step['event'] == 'input' and step['operation'] == 'truncate-uncommitted':
                end = step['inputs']['end']; configs = {offset: voters for offset, voters in configs.items() if offset < end}
            elif step['event'] == 'state':
                if fallback is None:
                    fallback = step['voters']
                offset = max(configs) if configs else -2
                require(step['latest_voters_offset'] == offset and step['voters'] == configs.get(offset, fallback), 'Active native voter history differs from ordered trace')
                require(-1 <= step['hw'] <= step['leo'], 'Impossible component high watermark')
            elif step['event'] == 'assertion':
                require(step['passed'] is True and step['actual'] == step['expected'], 'Failed or forged fixture assertion')
                assertions += 1
        require(configs or name.endswith('no-config'), 'No retained config outside reviewed no-config scenarios')
    require(assertions == manifest['assertions'], 'Wrong fixture assertion count')
    require(used_native == {name for name in files if name.startswith('native/')}, 'Unused/unreviewed native capture')
    return {'release': manifest['release'], 'manifest_sha256': sha(raw), 'cases': len(cases), 'assertions': assertions,
            'native_batches': batches, 'native_records': records, 'decoded_configurations': decoded_configs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = {'schema_version': 1, 'scope': 'Independent bounded CRC32C and VotersRecord schema/history verification; no runtime quorum proof',
        'checker_sha256': sha(Path(__file__).read_bytes()), 'passed': False, 'releases': []}
    try:
        for version in ['4.1.2', '4.2.1', '4.3.1']:
            result['releases'].append(verify_release(args.fixtures / version))
        result['passed'] = True
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: sum(row[key] for row in result['releases']) for key in ['cases', 'assertions', 'native_batches', 'native_records']}))


if __name__ == '__main__':
    main()

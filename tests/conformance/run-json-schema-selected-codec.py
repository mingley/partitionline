#!/usr/bin/env python3
"""Independent, offline Python Draft2020-12 fixtures and Rust frame checks."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import struct

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[2]
DIALECT = 'https://json-schema.org/draft/2020-12/schema'
URI = 'urn:partitionline:selected-json:integer'
PEER_VERSIONS = {'jsonschema': '4.26.0', 'referencing': '0.37.0',
                 'rpds-py': '2026.6.3', 'attrs': '26.1.0',
                 'jsonschema-specifications': '2025.9.1'}
REFERENCE = {'$schema': DIALECT, '$id': URI, 'type': 'integer',
             'minimum': -2147483648, 'maximum': 2147483647}
WRITER = {'$schema': DIALECT, '$id': 'urn:partitionline:selected-json:writer',
          'anyOf': [{'type': 'null'}, {'$ref': URI}]}
READER = dict(WRITER, **{'$id': 'urn:partitionline:selected-json:reader', 'default': 7})
WRITER_RECORD = {'$schema': DIALECT, '$id': 'urn:partitionline:selected-json:record-writer',
                 'type': 'object', 'properties': {'id': {'$ref': URI}, 'note': {'type': ['null', 'string']}},
                 'required': ['id'], 'additionalProperties': False}
READER_RECORD = json.loads(json.dumps(WRITER_RECORD))
READER_RECORD['$id'] = 'urn:partitionline:selected-json:record-reader'
READER_RECORD['properties']['note']['default'] = 'default-must-not-be-inserted'
CASES = {
    'null': 'null', 'zero': '0', 'negative': '-17', 'min': '-2147483648',
    'max': '2147483647', 'decimal-integer': '1.0', 'exponent-integer': '1e2',
    'fraction': '0.5', 'string-number': '"123"', 'boolean': 'true',
    'above-max': '2147483648', 'below-min': '-2147483649', 'object': '{}',
    'array': '[]', 'invalid-json': '{', 'trailing-document': '1 2',
    'record-missing-note': '{"id":7}', 'record-null-note': '{"id":-17,"note":null}',
}

def canonical(value):
    return json.dumps(value, separators=(',', ':'), sort_keys=True, allow_nan=False)

def versions():
    actual = {p: importlib.metadata.version(p) for p in PEER_VERSIONS}
    assert actual == PEER_VERSIONS, actual
    return actual

def deny_network(uri):
    raise ValueError('offline-only resource set: ' + uri)

def validators(profile='scalar'):
    registry = Registry(retrieve=deny_network).with_resource(URI, Resource.from_contents(REFERENCE))
    schemas = [WRITER_RECORD, READER_RECORD] if profile == 'record' else [WRITER, READER]
    for schema in schemas:
        Draft202012Validator.check_schema(schema)
    return [Draft202012Validator(schema, registry=registry) for schema in schemas]

def disposition(text, peers):
    try:
        value = json.loads(text, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except ValueError:
        return False
    return all(peer.is_valid(value) for peer in peers)

def generate(directory):
    peer_versions = versions()
    directory.mkdir()
    for name, value in [('writer.schema.json', WRITER), ('reader.schema.json', READER), ('integer.schema.json', REFERENCE),
                        ('writer-record.schema.json', WRITER_RECORD), ('reader-record.schema.json', READER_RECORD)]:
        (directory / name).write_text(canonical(value) + '\n')
    cases = []
    for name, text in CASES.items():
        profile = 'record' if name.startswith('record-') else 'scalar'
        valid = disposition(text, validators(profile))
        (directory / (name + '.payload.json')).write_text(text)
        (directory / (name + '.frame.bin')).write_bytes(struct.pack('>BI', 0, 42) + text.encode())
        cases.append({'name': name, 'valid': valid, 'profile': profile})
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir())}
    manifest = {'profile': 'selected-json-schema-draft202012-offline-v1',
                'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'peer_versions': peer_versions,
                'cases': cases, 'sha256': hashes, 'network_retrieval': 'explicitly denied'}
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'status': 'pass', 'generated': len(cases), 'valid': sum(c['valid'] for c in cases)}))

def verify(directory, report):
    peer_versions = versions()
    fixture = ROOT / 'tests/fixtures/json_schema_selected_codec'
    manifest = json.loads((fixture / 'manifest.json').read_text())
    assert manifest['peer_versions'] == peer_versions
    assert manifest['source_sha256'] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    for name, expected in manifest['sha256'].items():
        assert hashlib.sha256((fixture / name).read_bytes()).hexdigest() == expected, name
    results = []
    for case in manifest['cases']:
        name = case['name']
        peers = validators(case['profile'])
        if case['valid']:
            frame = (directory / (name + '.frame.bin')).read_bytes()
            assert len(frame) <= 4096 and frame[:5] == struct.pack('>BI', 0, 42)
            text = frame[5:].decode()
            assert disposition(text, peers), name
            expected = json.loads((fixture / (name + '.payload.json')).read_text())
            assert json.loads(text) == expected, name
            # Validate the actual Rust decoded value independently as well.
            decoded = json.loads((directory / (name + '.decoded.json')).read_text())
            assert decoded == expected and all(peer.is_valid(decoded) for peer in peers), name
        else:
            outcome = json.loads((directory / (name + '.disposition.json')).read_text())
            assert outcome['rejected'] and not disposition((fixture / (name + '.payload.json')).read_text(), peers), name
        results.append({'name': name, 'valid': case['valid'], 'status': 'pass'})
    value = {'status': 'pass', 'cases': results,
             'peer_versions': peer_versions,
             'network_retrieval': 'explicitly denied',
             'rust_artifact_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir()) if p.is_file()},
             'qualification': 'Finite offline schema/instance family only; no live Registry or arbitrary decimal semantics.'}
    with report.open('x') as output:
        json.dump(value, output, indent=2); output.write('\n')
    print(json.dumps({'status': 'pass', 'checked': len(results)}))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--generate', type=Path)
    parser.add_argument('--rust-output', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.generate:
        generate(args.generate)
    else:
        if args.rust_output is None or args.report is None:
            parser.error('--rust-output and --report are required')
        verify(args.rust_output, args.report)

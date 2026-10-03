#!/usr/bin/env python3
"""Independently check the 120 retained, actual API18 TCP exchanges."""
import hashlib
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[6]
BASE = Path(__file__).resolve().parent
SOURCE = 'd147bcf1c0164778bdbad625842363f3721bc10e'
EXPECTED = [(0, 3, 13), (1, 4, 6), (2, 1, 3), (3, 0, 13),
            (18, 0, 4), (19, 2, 4), (20, 1, 6), (21, 0, 2)]


def require(value, reason):
    if not value:
        raise ValueError(reason)


def sha(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 1024 * 1024,
            'bounded regular report')
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Reader:
    def __init__(self, data):
        require(len(data) <= 65536, 'frame ceiling')
        self.data, self.position = data, 0

    def take(self, count):
        require(0 <= count <= len(self.data) - self.position, 'complete field')
        result = self.data[self.position:self.position + count]
        self.position += count
        return result

    def number(self, fmt):
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))[0]

    def uint(self):
        result = 0
        for ordinal in range(5):
            byte = self.number('>B')
            require(ordinal < 4 or byte <= 15, 'u32 varint bound')
            result |= (byte & 127) << (7 * ordinal)
            if byte < 128:
                require(ordinal == 0 or byte != 0, 'minimal uvarint')
                return result
        raise ValueError('unterminated uvarint')

    def tags(self):
        count = self.uint()
        require(count <= 32, 'tag budget')
        previous = -1
        for _ in range(count):
            tag, size = self.uint(), self.uint()
            require(tag > previous and size <= 1024, 'tag order/size')
            self.take(size)
            previous = tag

    def done(self):
        require(self.position == len(self.data), 'full frame consumption')


def check(case):
    version, correlation = case['api_version'], case['correlation_id']
    flexible = version >= 3
    require(case['api_key'] == 18 and case['request_header_version'] == (2 if flexible else 1)
            and case['response_header_version'] == 0, 'API/header versions')
    request = Reader(bytes.fromhex(case['request_hex']))
    require(request.number('>h') == 18 and request.number('>h') == version
            and request.number('>i') == correlation, 'request identity')
    count = request.number('>h')
    require(count == 16 and request.take(count) == b'retention-oracle', 'request client ID')
    if flexible:
        request.tags()
        for _ in range(2):
            count = request.uint()
            require(1 <= count <= 129, 'software identity budget')
            request.take(count - 1).decode('utf-8')
        request.tags()
    request.done()
    response = Reader(bytes.fromhex(case['response_hex']))
    require(response.number('>i') == correlation and response.number('>h') == 0,
            'response correlation/error')
    count = response.uint() - 1 if flexible else response.number('>i')
    require(count == 8, 'actual eight-entry profile')
    actual = []
    for _ in range(count):
        actual.append(tuple(response.number('>h') for _ in range(3)))
        if flexible:
            response.tags()
    require(actual == EXPECTED, 'exact advertised API ranges')
    if version >= 1:
        require(response.number('>i') == 0, 'throttle')
    if flexible:
        response.tags()
    response.done()


def main():
    cases, inputs = [], []
    for toolchain in ['stable', '1.85.0']:
        for features in ['default', 'all-features']:
            directory = BASE / f'live-{toolchain}-{features}-attempt-1'
            lane = json.loads((directory / 'validation.json').read_text())
            require(lane['source_sha'] == SOURCE and lane['passed'] and lane['actual_peer_jobs'] == 18,
                    'accepted exact-source live lane')
            for release in ['4.1.2', '4.2.1', '4.3.1']:
                for phase in ['seed', 'restart']:
                    path = directory / f'{release}-{phase}.json'
                    report = json.loads(path.read_text())
                    require(report['passed'] and report['release'] == release and report['phase'] == phase,
                            'actual Java outcome')
                    observations = [h for h in report['history'] if h['label'] == 'retention-profile']
                    require([h['api_version'] for h in observations] == list(range(5)), 'five API18 versions')
                    pin = {'path': str(path.relative_to(ROOT)), 'sha256': sha(path)}
                    inputs.append(pin)
                    for observation in observations:
                        check(observation)
                        cases.append(dict(observation, toolchain=toolchain.replace('.', '-'),
                                          features=features, release=release, phase=phase,
                                          raw_report=pin))
    require(len(cases) == 120, 'actual exchange denominator')
    result = {'schema_version': 1, 'source_sha': SOURCE, 'passed': True,
              'actual_exchanges': len(cases), 'cases': cases, 'raw_reports': inputs,
              'expected_api_versions': [list(row) for row in EXPECTED],
              'checker_sha256': sha(Path(__file__)),
              'command': ['python3', str(Path(__file__).relative_to(ROOT))],
              'scope': 'Actual eight-profile TCP API18 versions0–4, four Rust lanes, three official SDKs, seed/restart; independent full payload parsing.'}
    output = BASE / 'api-versions-validation.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'actual_exchanges': 120,
                      'path': str(output.relative_to(ROOT)), 'sha256': sha(output)}))


if __name__ == '__main__':
    main()

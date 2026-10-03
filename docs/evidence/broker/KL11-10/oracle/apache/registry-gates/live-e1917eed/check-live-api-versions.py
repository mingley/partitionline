#!/usr/bin/env python3
"""Check real retention-profile frames with the exact pushed registry decoder."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
REPO = Path('/workspace/partitionline')
GATE_SHA = 'e1917eed4075ea3fcec80546a569c0febb319b90'
RUNTIME_SHA = 'd147bcf1c0164778bdbad625842363f3721bc10e'
INPUT_SHA = 'e15e96653c7aa757f0dce8d88a483668e50091dba1b448edc40c5abe81955970'
DECODER_PATH = 'scripts/check-broker-api-matrix.py'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def request_fields(case):
    value = case.get('request_hex')
    require(isinstance(value, str) and len(value) <= 8192 and re.fullmatch(r'(?:[0-9a-f]{2})+', value),
            'invalid bounded request hex')
    raw, cursor = bytes.fromhex(value), 0

    def take(count):
        nonlocal cursor
        require(count >= 0 and cursor + count <= len(raw), 'truncated request')
        result = raw[cursor:cursor + count]
        cursor += count
        return result

    def uint():
        value = 0
        for index in range(5):
            byte = take(1)[0]
            require(index != 4 or byte < 16, 'overflowing compact length')
            value |= (byte & 127) << (7 * index)
            if byte < 128:
                return value
        raise ValueError('unterminated compact length')

    def text(count):
        require(0 <= count <= 1024, 'unbounded request string')
        try:
            return take(count).decode('utf-8')
        except UnicodeDecodeError as error:
            raise ValueError('invalid request UTF8') from error

    key = int.from_bytes(take(2), 'big', signed=True)
    version = int.from_bytes(take(2), 'big', signed=True)
    correlation = int.from_bytes(take(4), 'big', signed=True)
    require(key == 18 and version == case['api_version'] and correlation == case['correlation_id'],
            'request header API/version/correlation mismatch')
    length = int.from_bytes(take(2), 'big', signed=True)
    require(length >= -1, 'invalid classic nullable client ID')
    client = None if length == -1 else text(length)
    software, software_version = None, None
    if version >= 3:
        require(uint() == 0, 'unreviewed request header tags')
        software_length = uint()
        require(software_length > 1, 'empty/null software name')
        software = text(software_length - 1)
        version_length = uint()
        require(version_length > 1, 'empty/null software version')
        software_version = text(version_length - 1)
        require(uint() == 0, 'unreviewed request body tags')
    require(cursor == len(raw), 'request trailing bytes')
    return {'request_bytes': len(raw), 'client_id': client, 'software': software,
            'software_version': software_version, 'consumed_request_bytes': cursor}


def check(report, decoder, raw_reports):
    require(type(report.get('schema_version')) is int and report['schema_version'] == 1 and
            report.get('source_sha') == RUNTIME_SHA and report.get('passed') is True and
            type(report.get('actual_exchanges')) is int and report['actual_exchanges'] == 120,
            'live source/schema/count/verdict mismatch')
    apis = report.get('expected_api_versions')
    require(isinstance(apis, list) and all(isinstance(row, list) and len(row) == 3 and
            all(type(number) is int for number in row) for row in apis) and
            apis == [[row[field] for field in ('api_key', 'min_version', 'max_version')]
                     for row in decoder.RETENTION_APIS], 'live profile mismatch')
    cases = report.get('cases')
    require(isinstance(cases, list) and len(cases) == 120, 'incomplete live cases')
    seen, observations = set(), []
    for case in cases:
        require(isinstance(case, dict), 'invalid live case')
        identity = tuple(case.get(field) for field in ('toolchain', 'features', 'release', 'phase', 'api_version'))
        tool, feature, release, phase, version = identity
        correlation = case.get('correlation_id')
        require(tool in ('stable', '1-85-0') and feature in ('default', 'all-features') and
                release in decoder.TARGETS and phase in ('seed', 'restart') and
                type(version) is int and 0 <= version <= 4 and type(correlation) is int and
                -2147483648 <= correlation <= 2147483647 and type(case.get('api_key')) is int and
                case['api_key'] == 18 and type(case.get('request_header_version')) is int and
                case['request_header_version'] == (2 if version >= 3 else 1) and
                type(case.get('response_header_version')) is int and case['response_header_version'] == 0,
                'live identity/header mismatch')
        require(identity not in seen, 'duplicate live identity')
        seen.add(identity)
        fields = request_fields(case)
        decoder.verify_api_versions_response(case.get('response_hex'), version, correlation, decoder.RETENTION_APIS)
        reference = case.get('raw_report')
        require(isinstance(reference, dict) and reference.keys() == {'path', 'sha256'} and
                reference.get('path') in raw_reports and
                reference.get('sha256') == raw_reports[reference['path']]['sha256'], 'raw report reference mismatch')
        raw = raw_reports[reference['path']]['json']
        require(raw.get('passed') is True and raw.get('release') == release and raw.get('phase') == phase,
                'raw execution receipt scope/verdict mismatch')
        wire = {key: value for key, value in case.items()
                if key not in ('toolchain', 'features', 'release', 'phase', 'raw_report')}
        require(sum(row == wire for row in raw.get('history', [])) == 1, 'live frame missing/duplicated in raw history')
        observations.append({'identity': list(identity), 'correlation_id': correlation, **fields,
                             'response_sha256': hashlib.sha256(bytes.fromhex(case['response_hex'])).hexdigest()})
    require(seen == {(tool, feature, release, phase, version)
                    for tool in ('stable', '1-85-0') for feature in ('default', 'all-features')
                    for release in decoder.TARGETS for phase in ('seed', 'restart') for version in range(5)},
            'missing live Cartesian coverage')
    return observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gate-source', type=Path, required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()
    decoder_file = args.gate_source / DECODER_PATH
    committed = subprocess.check_output(['git', 'show', GATE_SHA + ':' + DECODER_PATH], cwd=REPO)
    require(decoder_file.read_bytes() == committed, 'pushed decoder bytes mismatch')
    source_before = sha(decoder_file)
    wrapper_before = sha(Path(__file__))
    require(sha(args.input) == INPUT_SHA, 'frozen live aggregate changed')
    report = json.loads(args.input.read_text())
    spec = importlib.util.spec_from_file_location('retention_pushed_decoder', decoder_file)
    decoder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(decoder)
    shutil.copyfile(Path(__file__), args.output / 'check-live-api-versions.py')
    shutil.copyfile(args.input, args.output / 'actual-api-versions.json')
    raw_reports = {}
    for reference in report['raw_reports']:
        name = reference['path']
        require(isinstance(name, str) and name.startswith('docs/evidence/broker/KL11-10/runtime/final/') and
                not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts and name not in raw_reports,
                'unsafe/duplicate raw report path')
        path = REPO / name
        require(not path.is_symlink() and path.is_file() and path.stat().st_size <= 2 * 1024 * 1024 and
                sha(path) == reference['sha256'], 'raw execution receipt changed')
        destination = args.output / 'raw-reports' / Path(name).parent.name / Path(name).name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
        raw_reports[name] = {'sha256': reference['sha256'], 'json': json.loads(path.read_text())}
    require(len(raw_reports) == 24, 'incomplete raw execution histories')
    observations = check(report, decoder, raw_reports)
    controls = [
        ('missing-case', lambda value: value['cases'].pop()),
        ('duplicate-case', lambda value: value['cases'].__setitem__(0, value['cases'][1])),
        ('wrong-source', lambda value: value.__setitem__('source_sha', '0' * 40)),
        ('boolean-count', lambda value: value.__setitem__('actual_exchanges', True)),
        ('wrong-feature', lambda value: value['cases'][0].__setitem__('features', 'production')),
        ('wrong-phase', lambda value: value['cases'][0].__setitem__('phase', 'unknown')),
        ('boolean-version', lambda value: value['cases'][0].__setitem__('api_version', False)),
        ('wrong-request-header', lambda value: value['cases'][0].__setitem__('request_header_version', 2)),
        ('boolean-response-header', lambda value: value['cases'][0].__setitem__('response_header_version', False)),
        ('request-truncated', lambda value: value['cases'][0].__setitem__('request_hex', '00')),
        ('request-trailing', lambda value: value['cases'][0].__setitem__('request_hex', value['cases'][0]['request_hex'] + '00')),
        ('wrong-correlation', lambda value: value['cases'][0].__setitem__('correlation_id', 301)),
        ('response-truncated', lambda value: value['cases'][0].__setitem__('response_hex', '00')),
        ('response-trailing', lambda value: value['cases'][0].__setitem__('response_hex', value['cases'][0]['response_hex'] + '00')),
        ('wrong-profile', lambda value: value['expected_api_versions'].pop()),
        ('wrong-advertised-version', lambda value: value['cases'][0].__setitem__('response_hex', value['cases'][0]['response_hex'][:-4] + '0003')),
        ('forged-raw-receipt', lambda value: value['cases'][0]['raw_report'].__setitem__('sha256', '0' * 64)),
    ]
    failed_controls = []
    for name, mutate in controls:
        value = copy.deepcopy(report)
        mutate(value)
        try:
            check(value, decoder, raw_reports)
        except ValueError as error:
            failed_controls.append({'name': name, 'rejected': True, 'reason': str(error)})
        else:
            raise ValueError('Corruption control admitted: ' + name)
    require(sha(decoder_file) == source_before and sha(Path(__file__)) == wrapper_before and sha(args.input) == INPUT_SHA,
            'decoder/wrapper/input changed during execution')
    result = {'schema_version': 1, 'passed': True, 'gate_source_sha': GATE_SHA, 'runtime_source_sha': RUNTIME_SHA,
              'input_aggregate_sha256': INPUT_SHA, 'decoder_source_sha256': source_before,
              'wrapper_sha256': wrapper_before, 'actual_exchanges': len(observations),
              'raw_execution_histories_checked': len(raw_reports), 'controls': failed_controls,
              'command': [sys.executable, '-B', str(Path(__file__)), '--gate-source', str(args.gate_source),
                          '--input', str(args.input), '--output', str(args.output)],
              'observations': observations,
              'scope': 'Complete actual request/header/body parse plus the exact pushed independent API18 response decoder; raw history hashes/frames matched. Source/peer/build/TCP execution provenance remains the separate d147 live receipts. Wrapper itself is retained by exact bytes, not claimed present in e191 Git.',
              'artifacts_sha256': {str(path.relative_to(args.output)): sha(path)
                                  for path in sorted(args.output.rglob('*')) if path.is_file()}}
    (args.output / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
    (args.output / 'SHA256SUMS').write_text(''.join(sha(path) + '  ' + str(path.relative_to(args.output)) + '\n'
        for path in sorted(args.output.rglob('*')) if path.is_file() and path.name != 'SHA256SUMS'))
    print(json.dumps({'passed': True, 'actual_exchanges': len(observations), 'corrupt_controls_rejected': len(failed_controls),
                      'validation_sha256': sha(args.output / 'validation.json')}))


if __name__ == '__main__':
    main()

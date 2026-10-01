#!/usr/bin/env python3
"""Fail closed on missing histories, peer identity or required broker features."""
import json
import base64
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]


def validate(identity, result, cell, source):
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("source SHA required")
    if (identity['host_os'], identity['host_arch'], identity['rustc_host']) != ('Linux', 'x86_64', 'x86_64-unknown-linux-gnu'):
        raise ValueError('native Linux x86_64 runtime/compiler required')
    if identity['requested'] != cell['reference'] or identity['actual_reference'] != cell['reference']:
        raise ValueError("requested/actual image reference mismatch; no fallback")
    if 'apache/kafka@' + cell['digest'] not in identity['repo_digests']:
        raise ValueError("actual image digest mismatch")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity['container_image_id']):
        raise ValueError("container image identity missing")
    if identity['container_image_id'] != identity['inspected_image_id']:
        raise ValueError("container did not use inspected image")
    if not re.search(r"^" + re.escape(cell['version']) + r"(?:\s|$)", identity['kafka_cli_version'], re.M):
        raise ValueError("actual broker distribution version mismatch")
    if result['source_sha'] != source or result['requested'] != cell['reference']:
        raise ValueError("runtime source/image mismatch")
    if set(result['scenarios']) != set(cell['required_scenarios']):
        raise ValueError("required scenario denominator changed")
    total = cell['partitions'] * cell['records_per_partition']
    for name, scenario in result['scenarios'].items():
        if scenario['status'] != 'passed':
            raise ValueError(f"required {name} is {scenario['status']}")
        if type(scenario['records']) is not int or scenario['records'] != (0 if name == 'admin' else total):
            raise ValueError(f"{name}: exact history count missing")
        if any(type(scenario[key]) is not int or scenario[key] != 0 for key in ('duplicates', 'missing', 'corrupt')):
            raise ValueError(f"{name}: invalid record history")
    for key, minimum in [('0', 3), ('1', 4), ('2', 1), ('11', 2), ('68', 0), ('76', 0), ('78', 0), ('79', 0)]:
        versions = result['api_ranges'].get(key)
        if not versions or not all(type(v) is int for v in versions) or len(versions) != 2:
            raise ValueError(f"API {key} absent/malformed")
        if versions[0] > versions[1] or versions[1] < minimum:
            raise ValueError(f"API {key} unsupported")
    features = result['finalized_features']
    for name in ('share.version', 'group.version', 'transaction.version', 'metadata.version'):
        levels = features.get(name)
        if not levels or len(levels) != 2 or any(type(level) is not int for level in levels) or not 1 <= levels[0] <= levels[1]:
            raise ValueError(f"required finalized {name} absent/disabled")
    for group in ('classic', 'cooperative', 'kip848', 'transaction'):
        if result['committed_offsets'][group] != {str(p): cell['records_per_partition'] for p in range(cell['partitions'])}:
            raise ValueError(f"{group}: committed offsets differ")
        if result['group_ids'].get(group) != 'plcompat-' + cell['version'].replace('.', '-') + '-' + group:
            raise ValueError(f"{group}: group identity differs")
    if type(result['transaction_aborted_visible']) is not int or result['transaction_aborted_visible'] != 0 or type(result['share_accepted']) is not int or result['share_accepted'] != total:
        raise ValueError("transaction isolation/share acknowledgments incomplete")
    observations = result['transaction_visibility']
    if not observations or observations[-1].get('offsets') != {'0': 8, '1': 8}:
        raise ValueError('transaction stable-offset observation missing')
    elapsed = 0
    for attempt, observation in enumerate(observations):
        if type(observation['attempt']) is not int or observation['attempt'] != attempt or type(observation['elapsed_ms']) is not int or not elapsed <= observation['elapsed_ms'] <= 5000:
            raise ValueError('transaction visibility sequence/deadline differs')
        elapsed = observation['elapsed_ms']
        if 'error' in observation:
            if observation['error'] != 88 or 'offsets' in observation:
                raise ValueError('unexpected transaction visibility error')
        elif set(observation['offsets']) != {'0', '1'} or any(type(v) is not int or not -1 <= v <= 8 for v in observation['offsets'].values()):
            raise ValueError('transaction visibility offsets invalid')


def parse_runtime(output):
    result = {'scenarios': {}, 'api_ranges': {}, 'finalized_features': {},
              'committed_offsets': {}, 'group_ids': {}, 'startup_attempts': [], 'transaction_visibility': []}
    seen = set()
    for line in output.splitlines():
        at = line.find('PL_COMPAT_')
        if at < 0:
            continue
        fields = line[at:].split('\t')
        tag, values = fields[0], fields[1:]
        if tag in ('PL_COMPAT_SCENARIO', 'PL_COMPAT_API', 'PL_COMPAT_FEATURE', 'PL_COMPAT_GROUP') and not values:
            raise ValueError(f'malformed runtime field {tag}')
        key = (tag, values[0]) if tag in ('PL_COMPAT_SCENARIO', 'PL_COMPAT_API', 'PL_COMPAT_FEATURE', 'PL_COMPAT_GROUP') else (tag,)
        if tag == 'PL_COMPAT_COMMITTED':
            key = (tag, *values[:2])
        if tag in ('PL_COMPAT_VISIBILITY', 'PL_COMPAT_VISIBILITY_ERROR') and values:
            key = ('PL_COMPAT_VISIBILITY', values[0])
        if tag != 'PL_COMPAT_STARTUP':
            if key in seen:
                raise ValueError(f'duplicate runtime field {key}')
            seen.add(key)
        if tag in ('PL_COMPAT_SOURCE', 'PL_COMPAT_REFERENCE', 'PL_COMPAT_INPUT_TOPIC', 'PL_COMPAT_OUTPUT_TOPIC') and len(values) == 1:
            field = {'PL_COMPAT_SOURCE':'source_sha', 'PL_COMPAT_REFERENCE':'requested',
                     'PL_COMPAT_INPUT_TOPIC':'input_topic', 'PL_COMPAT_OUTPUT_TOPIC':'output_topic'}[tag]
            result[field] = values[0]
        elif tag == 'PL_COMPAT_SEED_TIMESTAMP' and len(values) == 1:
            result['seed_timestamp'] = int(values[0])
        elif tag == 'PL_COMPAT_SCENARIO' and len(values) == 6:
            name, status, *counts = values
            result['scenarios'][name] = dict(zip(('records', 'duplicates', 'missing', 'corrupt'), map(int, counts)), status=status)
        elif tag in ('PL_COMPAT_API', 'PL_COMPAT_FEATURE') and len(values) == 3:
            name, minimum, maximum = values
            result['api_ranges' if tag == 'PL_COMPAT_API' else 'finalized_features'][name] = [int(minimum), int(maximum)]
        elif tag == 'PL_COMPAT_COMMITTED' and len(values) == 3:
            group, partition, offset = values
            result['committed_offsets'].setdefault(group, {})[partition] = int(offset)
        elif tag == 'PL_COMPAT_GROUP' and len(values) == 2:
            result['group_ids'][values[0]] = values[1]
        elif tag == 'PL_COMPAT_STARTUP' and len(values) == 2:
            result['startup_attempts'].append({'phase': 'setup', 'scenario': values[0],
                'error': base64.b64decode(values[1], validate=True).decode('utf-8')})
        elif tag == 'PL_COMPAT_VISIBILITY' and len(values) == 4:
            attempt, elapsed, first, second = map(int, values)
            result['transaction_visibility'].append({'attempt': attempt, 'elapsed_ms': elapsed, 'offsets': {'0': first, '1': second}})
        elif tag == 'PL_COMPAT_VISIBILITY_ERROR' and len(values) == 3:
            attempt, elapsed, error = map(int, values)
            result['transaction_visibility'].append({'attempt': attempt, 'elapsed_ms': elapsed, 'error': error})
        elif tag in ('PL_COMPAT_ABORTED_VISIBLE', 'PL_COMPAT_SHARE_ACCEPTED') and len(values) == 1:
            result['transaction_aborted_visible' if tag == 'PL_COMPAT_ABORTED_VISIBLE' else 'share_accepted'] = int(values[0])
        elif tag != 'PL_COMPAT_COMPLETE' or values:
            raise ValueError(f'unknown/malformed runtime field {tag}')
    if ('PL_COMPAT_COMPLETE',) not in seen:
        raise ValueError('runtime profile incomplete')
    return result


def validate_java(directory, result, cell):
    row = re.compile(r'^CreateTime:(\d+)\tPartition:(\d+)\tOffset:(\d+)\t([^\t]+)\t([^\r\n]+)$')
    expected = {(str(p) + ':' + str(i)): (p, i, result['seed_timestamp'] + i,
        f'KL01-10/{p}/{i}/' + 'x' * (i + 1))
        for p in range(cell['partitions']) for i in range(cell['records_per_partition'])}
    for name in ('input', 'output'):
        observed = {}; offsets = set()
        for line in (directory / ('java-' + name + '.log')).read_text(encoding='utf-8').splitlines():
            match = row.fullmatch(line)
            if not match:
                raise ValueError(f'unexpected Java {name} record line')
            timestamp, partition, offset = map(int, match.group(1, 2, 3))
            key, value = match.group(4, 5)
            if key in observed or (partition, offset) in offsets:
                raise ValueError(f'duplicate Java {name} record')
            expected_row = expected.get(key)
            if not expected_row or (timestamp, value) != (expected_row[2], expected_row[3]):
                raise ValueError(f'corrupt/unexpected Java {name} record')
            if name == 'input' and (partition, offset) != expected_row[:2]:
                raise ValueError('Java input offset/partition mismatch')
            if name == 'output' and partition != 0:
                raise ValueError('Java output partition mismatch')
            observed[key] = True
            offsets.add((partition, offset))
        if set(observed) != set(expected):
            raise ValueError(f'missing Java {name} records')
    for group in ('classic', 'cooperative', 'kip848', 'transaction'):
        found = {}
        for line in (directory / f'java-offsets-{group}.log').read_text(encoding='utf-8').splitlines():
            fields = line.split()
            if len(fields) >= 6 and fields[:2] == [result['group_ids'][group], result['input_topic']]:
                partition, committed, end, lag = map(int, fields[2:6])
                if partition in found or (committed, end, lag) != (cell['records_per_partition'], cell['records_per_partition'], 0):
                    raise ValueError(f'Java {group} stored offsets/lag differ')
                found[partition] = committed
        if found != {p: cell['records_per_partition'] for p in range(cell['partitions'])}:
            raise ValueError(f'Java {group} group offsets incomplete')
    features = (directory / 'features.log').read_text(encoding='utf-8')
    for name in ('share.version', 'group.version', 'transaction.version'):
        match = re.search(r'Feature: ' + re.escape(name) + r'\s+.*?FinalizedVersionLevel: (\d+)', features)
        if not match or int(match[1]) != result['finalized_features'][name][1]:
            raise ValueError(f'Java {name} finalized level disagrees')


def finish(directory, version, source):
    directory = Path(directory)
    cell = next(c for c in json.loads((ROOT / 'tests/conformance/current-broker-cells.json').read_text())['cells'] if c['version'] == version)
    identity = json.loads((directory / 'identity.json').read_text())
    output = (directory / 'runtime.log').read_text(encoding='utf-8')
    summaries = re.findall(r'^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;', output, re.M)
    if len(summaries) != 1:
        raise ValueError("exact live runtime test did not complete")
    result = parse_runtime(output)
    validate(identity, result, cell, source)
    validate_java(directory, result, cell)
    report = {'schema_version': 1, 'source_sha': source, 'cell': cell, 'identity': identity, 'runtime': result, 'status': 'passed'}
    (directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f"broker-compatibility: qualified {version} digest={cell['digest']} scenarios={len(cell['required_scenarios'])} records={cell['partitions'] * cell['records_per_partition']}")


if __name__ == '__main__':
    try:
        finish(*sys.argv[1:])
    except (ValueError, KeyError, TypeError, OSError, StopIteration) as error:
        print(f"broker-compatibility: FAIL: {error}", file=sys.stderr)
        sys.exit(1)

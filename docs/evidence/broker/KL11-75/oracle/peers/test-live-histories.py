#!/usr/bin/env python3
"""Offline, explicitly synthetic native EOF controls; no broker/runtime claim."""
import copy
import argparse
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location('proposed_driver', ROOT / 'run-live.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def positions(value):
    return [row for row in value['history'] if row['label'] == 'public-consumer-position']


def eofs(value):
    return [row for row in value['history'] if row['label'] == 'public-consumer-eof']


def end_position(value):
    return next(row for row in positions(value) if row['topic'] == 'cp-j412-mixed' and row['seek'] == 9)


def end_eof(value):
    return next(row for row in eofs(value) if row['topic'] == 'cp-j412-mixed' and row['seek'] == 9)


def old_record_comparison(value):
    # This is the exact old driver's records-only semantic projection, not an
    # assertion that the original native executable would emit a forged receipt.
    return [(row['seek'], row['record']) for row in value['history'] if row['label'] == 'public-consumer-record']


def synthetic_native(java):
    value = copy.deepcopy(java)
    value['peer'] = 'native-c'
    value['release'] = '2.15.0'
    rows = []
    for row in value['history']:
        if row['label'] != 'public-consumer-position':
            rows.append(row)
            continue
        offsets = [event['record']['offset'] for event in value['history']
                   if event['label'] == 'public-consumer-record' and event['seek'] == row['seek']
                   and event['record']['topic'] == row['topic']]
        count = len(offsets)
        row.update(position=offsets[-1] + 1 if offsets else -1001,
                   position_semantics='last-consumed-plus-one-or-invalid',
                   eof_offset=row['end_offset'], records_since_seek=count)
        eof = copy.deepcopy(row)
        eof.update(label='public-consumer-eof', partition=0)
        eof.pop('position_semantics')
        rows.append(eof)
        rows.append(row)
    value['history'] = rows
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--actual-receipts', required=True, type=Path,
                        help='Preserved first failed lane with the three completed Java reader receipts')
    parser.add_argument('--output', required=True, type=Path, help='New WORK-only result path; source files remain unchanged')
    args = parser.parse_args()
    assert not args.output.exists(), 'no overwrite of a prior actual offline check'
    results = []
    java = None
    for reader in ('j412', 'j421', 'j431'):
        observed = json.loads((args.actual_receipts / f'initial-read-{reader}-j412.json').read_text())
        driver.validate_read_history(reader, 'j412', 'initial', observed)
        results.append({'name': reader + '-retained-actual-receipt', 'scope': 'Previously completed actual Java receipt; offline recheck only.', 'passed': True})
        if java is None:
            java = observed
    native = synthetic_native(java)
    driver.validate_read_history('native', 'j412', 'initial', native)
    results.append({'name': 'native-eof-contract-positive', 'scope': 'Synthetic native receipt from actual Java record fields; not a native execution.', 'passed': True})
    rust = copy.deepcopy(java)
    rust.update(peer='public-rust', release='partitionline')
    driver.validate_read_history('rust', 'j412', 'initial', rust)
    results.append({'name': 'rust-position-contract-positive', 'scope': 'Synthetic Rust receipt; not a Rust execution.', 'passed': True})
    controls = [
        ('missing-eof', lambda x: x['history'].remove(end_eof(x))),
        ('wrong-eof-offset', lambda x: end_eof(x).update(eof_offset=8)),
        ('wrong-eof-partition', lambda x: end_eof(x).update(partition=1)),
        ('wrong-eof-watermark', lambda x: end_eof(x).update(end_offset=10)),
        ('wrong-eof-floor', lambda x: end_eof(x).update(beginning_offset=1)),
        ('wrong-eof-position', lambda x: end_eof(x).update(position=9)),
        ('eof-false-record-count', lambda x: end_eof(x).update(records_since_seek=1)),
        ('fabricated-leo-app-position', lambda x: end_position(x).update(position=9)),
        ('invalid-on-data-round', lambda x: positions(x)[0].update(position=-1001)),
        ('duplicate-eof', lambda x: x['history'].append(copy.deepcopy(end_eof(x)))),
        ('missing-position', lambda x: x['history'].remove(end_position(x))),
        ('wrong-position-watermark', lambda x: end_position(x).update(end_offset=10)),
        ('position-missing-eof-link', lambda x: end_position(x).pop('eof_offset')),
        ('position-wrong-eof-link', lambda x: end_position(x).update(eof_offset=8)),
        ('position-wrong-record-count', lambda x: end_position(x).update(records_since_seek=1)),
        ('position-wrong-semantics', lambda x: end_position(x).update(position_semantics='logical-fetch-position')),
        ('typed-eof-bool', lambda x: end_eof(x).update(partition=False)),
        ('typed-position-bool', lambda x: end_position(x).update(records_since_seek=False)),
        ('reported-record-cardinality', lambda x: x.update(records=x['records'] + 1)),
        ('unreviewed-event', lambda x: x['history'].append({'label': 'invented-eof'})),
    ]
    for name, mutate in controls:
        value = copy.deepcopy(native)
        mutate(value)
        assert old_record_comparison(value) == old_record_comparison(native)
        try:
            driver.validate_read_history('native', 'j412', 'initial', value)
        except RuntimeError as error:
            results.append({'name': name, 'scope': 'Synthetic receipt mutation only; original records-only driver projection unchanged.',
                            'old_records_projection_accepted': True, 'new_typed_validator_rejected': True,
                            'rejection': str(error), 'passed': True})
        else:
            raise AssertionError('New verifier accepted ' + name)
    # Every declared phase, independently expected cardinality and the native
    # exact-end no-record/INVALID/EOF contract, remain covered offline.
    phases = []
    for stage in ('first', 'before-expiry', 'expired', 'restart', 'appended'):
        value = copy.deepcopy(java)
        value['stage'] = stage
        history = []
        for scenario, initial_end in driver.SCENARIOS.items():
            end = initial_end + (stage == 'appended')
            topic = 'cp-j412-' + scenario
            kept = {'mixed': [2, 5, 8], 'removed': [3], 'nulls': [2]}[scenario][:]
            if stage in ('first', 'before-expiry'):
                kept += {'mixed': [4, 7], 'removed': [2], 'nulls': []}[scenario]
            if stage == 'appended':
                kept.append(initial_end)
            for start in (0, 3 if scenario == 'mixed' else 1, end):
                for offset in sorted(n for n in kept if n >= start):
                    # Fields are placeholders for this cardinality-only
                    # synthetic unit; field equality is preserved in live peers.
                    history.append({'label': 'public-consumer-record', 'seek': start, 'stage': stage,
                                    'record': {'topic': topic, 'partition': 0, 'offset': offset}})
                history.append({'label': 'public-consumer-position', 'topic': topic, 'seek': start,
                                'position': end, 'beginning_offset': 0, 'end_offset': end})
        value['history'] = history
        value['records'] = sum(row['label'] == 'public-consumer-record' for row in history)
        value = synthetic_native(value)
        driver.validate_read_history('native', 'j412', stage, value)
        phases.append({'stage': stage, 'records': value['records'], 'scope': 'Synthetic phase cardinality/EOF logic only.', 'passed': True})
    frontier_cases = []
    frontier_controls = []
    for name, start, end, offsets in (
            ('empty-below-leo', 1, 9, []),
            ('trailing-holes', 0, 9, [2, 5]),
            ('single-record-before-trailing-holes', 3, 9, [5]),
            ('empty-exact-leo', 9, 9, [])):
        raw_position = offsets[-1] + 1 if offsets else -1001
        position = {'label': 'public-consumer-position', 'topic': 'synthetic-frontier', 'seek': start,
                    'position': raw_position, 'position_semantics': 'last-consumed-plus-one-or-invalid',
                    'eof_offset': end, 'records_since_seek': len(offsets), 'beginning_offset': 0, 'end_offset': end}
        eof = copy.deepcopy(position)
        eof.update(label='public-consumer-eof', partition=0)
        eof.pop('position_semantics')
        assert driver.validate_native_frontier(position, eof, offsets, start, end) == raw_position
        frontier_cases.append({'name': name, 'seek': start, 'preserved_leo': end,
                               'expected_retained_offsets': offsets, 'raw_position': raw_position,
                               'scope': 'Synthetic native API frontier unit only; not an actual compaction corpus execution.', 'passed': True})
        mutations = (
            ('invalid-alone-no-eof', lambda p, e: e.clear()),
            ('wrong-eof-offset', lambda p, e: e.update(eof_offset=end - 1)),
            ('wrong-public-hw', lambda p, e: e.update(end_offset=end + 1)),
            ('reported-position-as-leo', lambda p, e: (p.update(position=end), e.update(position=end))),
            ('wrong-record-count', lambda p, e: (p.update(records_since_seek=len(offsets) + 1),
                                               e.update(records_since_seek=len(offsets) + 1))),
        )
        for suffix, mutate in mutations:
            pos, observed_eof = copy.deepcopy(position), copy.deepcopy(eof)
            mutate(pos, observed_eof)
            try:
                driver.validate_native_frontier(pos, observed_eof, offsets, start, end)
            except RuntimeError as error:
                frontier_controls.append({'name': name + '-' + suffix, 'scope': 'Synthetic native frontier mutation only.',
                                          'rejection': str(error), 'new_typed_validator_rejected': True, 'passed': True})
            else:
                raise AssertionError('Frontier verifier accepted ' + name + '-' + suffix)
    report = {'schema_version': 1, 'passed': True, 'scope': 'Offline policy-unit controls only; no new compiler/native/server/Rust executions.',
              'actual_java_receipts_rechecked': 3, 'synthetic_contract_positives': 2,
              'synthetic_phase_positives': len(phases), 'synthetic_negative_controls': len(controls),
              'synthetic_frontier_positives': len(frontier_cases), 'synthetic_frontier_negative_controls': len(frontier_controls),
              'results': results, 'phases': phases, 'frontier_cases': frontier_cases, 'frontier_controls': frontier_controls}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k not in ('results', 'phases', 'frontier_cases', 'frontier_controls')}))


if __name__ == '__main__':
    main()

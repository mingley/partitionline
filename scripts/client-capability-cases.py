"""Exact focused Rust cases, with external peer server tests kept separate."""
import json
import re


class CaseInventoryError(ValueError):
    """The reviewed case inventory or compiled discovery has changed."""


class CaseExecutionError(ValueError):
    """The test process did not report the exact selected case results."""


SELECTORS = {
    'write_txn_markers_v2': ['--test', 'write_txn_markers_v2'],
    'share_offsets_v1': ['--test', 'share_offsets_v1'],
    'partitioner': ['--lib', 'partitioner::'],
    'sticky_partitioner': ['--test', 'sticky_partitioner'],
    'streams_protocol': ['--test', 'streams_protocol'],
}
EXTERNAL_PROBES = {
    'write_txn_markers_v2': ['serve_public_admin_probe'],
    'share_offsets_v1': ['serve_share_admin_probe'],
}


def load_suites(path):
    value = json.loads(path.read_text())
    if value.get('schema_version') != 1 or not isinstance(value.get('suites'), list):
        raise CaseInventoryError('unsupported case inventory')
    suites = value['suites']
    labels = [suite['label'] for suite in suites]
    if len(labels) != len(set(labels)) or set(labels) != set(SELECTORS):
        raise CaseInventoryError('missing, duplicate or unexpected focused suite')
    for suite in suites:
        label = suite['label']
        required = suite['required_cases']
        external = suite['external_probes']
        names = required + external
        if (suite['selector'] != SELECTORS[label] or not required
                or external != EXTERNAL_PROBES.get(label, [])
                or len(names) != len(set(names))
                or any(not isinstance(name, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9:]*', name)
                       for name in names)):
            raise CaseInventoryError('invalid focused case inventory: ' + label)
    return suites


def selected_names(suite, discovered):
    expected = set(suite['required_cases'] + suite['external_probes'])
    actual = set(discovered)
    if len(discovered) != len(actual) or actual != expected:
        raise CaseInventoryError(
            f"{suite['label']}: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}, duplicates={len(discovered) != len(actual)}")
    # Match Cargo's discovery order while excluding only named external servers.
    return [name for name in discovered if name not in suite['external_probes']]


def verify_execution(row, data, errors, names):
    text = data.decode()
    summaries = re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text, re.M)
    executed = re.findall(r'^test ([A-Za-z_][A-Za-z_0-9:]*) \.\.\. (ok|FAILED|ignored)$', text, re.M)
    if len(names) != len(set(names)) or len(executed) != len(names) or {n for n, _ in executed} != set(names):
        raise CaseExecutionError('executed case identities differ from the selection')
    passed = sum(s == 'ok' for _, s in executed)
    failed = sum(s == 'FAILED' for _, s in executed)
    ignored = sum(s == 'ignored' for _, s in executed)
    if summaries != [('ok' if failed == 0 else 'FAILED', str(passed), str(failed), str(ignored))]:
        raise CaseExecutionError('missing, duplicate or inconsistent test summary')
    if 'could not compile' in errors.decode() or 'error[E' in errors.decode():
        raise CaseExecutionError('compiler errors cannot qualify executed tests')
    return {'command_name': row['name'], 'actual_names': [n for n, _ in executed],
            'actual_results': executed, 'actual_passed': passed, 'actual_failed': failed,
            'actual_ignored': ignored, 'passed': row['exit_code'] == 0 and failed == ignored == 0}

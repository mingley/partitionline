"""Pinned Apache serializer fixtures and reverse parsing for two capability suites.

The caller owns command supervision, the source guard, and the evidence archive.
This module never starts socket peers or changes the required Rust case list.
"""
from pathlib import Path
import hashlib
import json
import os
import re
import stat

RELEASES = ('4.1.2', '4.2.1', '4.3.1')
PINS = {
    '4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
    '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
    '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36',
}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
SOURCE_PINS = {
    'docs/evidence/client/write-txn-markers-v2/oracle/CapabilityOracle.java': 'bba1a3c3c005ff40149096c628edc81937a683373b87a3d757bb7eb9c7f795ca',
    'docs/evidence/client/write-txn-markers-v2/oracle/MarkerNullOracle.java': 'fdc72702ce67659a062377181ba895aab2a0e1c62eef5498323672c3bcf17fbc',
    'docs/evidence/client/share-offsets-v1/oracle/ShareCapabilityOracle.java': '2f75916c9a77231735fd2dffbed013ec689d1eefcfd5a6fb6d7acefda6af5029',
    'docs/evidence/client/share-offsets-v1/oracle/ShareNullOracle.java': 'a4a51e4441f25e6680f1226e4a7a09931724df8594646f1a3cd236dd04aee6cf',
}
JAVA_ENV_KEYS = ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
                 'CLASSPATH', 'LD_PRELOAD', 'LD_LIBRARY_PATH')
SUITES = {
    'write_txn_markers_v2': dict(directory='markers', api=27, oracle='CapabilityOracle.java',
                                 null='MarkerNullOracle.java', environment='PARTITIONLINE_MARKER_ORACLE_DIR'),
    'share_offsets_v1': dict(directory='share', api=90, oracle='ShareCapabilityOracle.java',
                             null='ShareNullOracle.java', environment='PARTITIONLINE_SHARE_ORACLE_DIR'),
}


class FixtureError(RuntimeError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def expected_cases(suite, release):
    """Exact source-defined names, API versions and official support decisions."""
    if suite not in SUITES or release not in RELEASES:
        raise FixtureError('unknown fixture suite/release')
    result = {}
    marker_labels = ['tv-' + str(n) for n in (-128, -1, 0, 1, 2, 127)] + ['unknown-tags']
    marker_labels += ['empty', 'wrong-producer', 'wrong-topic', 'wrong-partition', 'no-topic',
                      'no-partition', 'duplicate-producer', 'duplicate-topic', 'duplicate-partition',
                      'unrelated-zero', 'unrelated-error']
    marker_labels += ['error-' + str(n) for n in (3, 6, 8, 9, 31, 47, 52)]
    share_labels = ['lag-' + str(n) for n in (-(1 << 63), -2, -1, 0, 1, (1 << 63) - 1)]
    share_labels += ['topics-empty', 'topics-named', 'partition-error-3', 'partition-error-29', 'partition-error-87']
    if suite == 'write_txn_markers_v2':
        for version in (0, 1, 2):
            supported = version == 1 or version == 2 and release != '4.1.2'
            for label in marker_labels:
                result[f'api-27-v{version}-{label}'] = (27, version, 'supported' if supported else 'unsupported')
    else:
        share_labels += ['unknown-tags', 'group-error-14', 'group-error-15', 'group-error-16',
                         'group-error-30', 'group-error-69', 'missing-group', 'duplicate-group', 'empty-groups']
    # CapabilityOracle also generates its original API90 reference cells.
    for version in (0, 1):
        supported = version == 0 or release != '4.1.2'
        for label in share_labels:
            result[f'api-90-v{version}-{label}'] = (90, version, 'supported' if supported else 'unsupported')
    return result


def expected_nulls(suite, release):
    if suite == 'write_txn_markers_v2':
        versions = (1,) if release == '4.1.2' else (1, 2)
        fields = {'request': ('markers', 'topics', 'name', 'partitions'),
                  'response': ('markers', 'topics', 'name', 'partitions')}
    elif suite == 'share_offsets_v1':
        versions = (0,) if release == '4.1.2' else (0, 1)
        fields = {'request': ('groups', 'group-name', 'topic-name', 'partitions'),
                  'response': ('groups', 'group-name', 'topics', 'topic-name', 'partitions')}
    else:
        raise FixtureError('unknown null fixture suite')
    return {f'null-v{version}-{side}-{field}': (side, str(version), field)
            for version in versions for side, names in fields.items() for field in names}


def file_inventory(directory):
    """Regular private files only; reject symlinks and bound each fixture/table."""
    result = {}
    for path in sorted(directory.iterdir()):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise FixtureError('non-private/non-regular fixture: ' + str(path))
        if info.st_size > 2 * 1024 * 1024:
            raise FixtureError('fixture/table bound exceeded: ' + str(path))
        data = path.read_bytes()
        result[path.name] = dict(sha256=digest(data), bytes=len(data), full_mode=0o600)
    return result


def table(path, columns):
    data = path.read_bytes()
    if len(data) > 65536:
        raise FixtureError('table bound exceeded: ' + str(path))
    rows = [line.split('\t') for line in data.decode().splitlines()]
    if any(len(row) != columns or not re.fullmatch('[A-Za-z0-9-]+', row[0]) for row in rows):
        raise FixtureError('invalid table row: ' + str(path))
    if len({row[0] for row in rows}) != len(rows):
        raise FixtureError('duplicate table case: ' + str(path))
    return rows


def validate_fixtures(directory, suite, release, with_rust=False):
    expected = expected_cases(suite, release)
    rows = table(directory / 'cases.tsv', 4)
    observed = {name: (int(api), int(version), status) for name, api, version, status in rows}
    if observed != expected:
        raise FixtureError('exact official case inventory differs: ' + suite + '/' + release)
    golden = json.loads((directory / 'goldens.json').read_bytes())
    cases = golden['cases']
    if golden.get('schema_version') != 1 or len(cases) != len(expected):
        raise FixtureError('golden count/version differs')
    names = set()
    members = {'cases.tsv', 'goldens.json', 'null-controls.tsv'}
    for case in cases:
        name = case['name']
        if name in names or name not in expected:
            raise FixtureError('duplicate/unexpected golden')
        names.add(name)
        api, version, status = expected[name]
        if (case['api_key'], case['api_version']) != (api, version):
            raise FixtureError('golden API identity differs')
        if status == 'unsupported':
            if case['status'] != 'actual-official-unsupported' or case.get('exception_type') != 'org.apache.kafka.common.errors.UnsupportedVersionException':
                raise FixtureError('missing actual official unsupported result')
            continue
        if case['status'] != 'actual-official-serialized-and-parsed':
            raise FixtureError('missing actual official serialization result')
        for side in ('request', 'response'):
            filename = name + '.' + side + '.bin'
            members.add(filename)
            data = (directory / filename).read_bytes()
            if not data or len(data) > 65536 or case[side + '_hex'] != data.hex() or case[side + '_sha256'] != digest(data):
                raise FixtureError('official binary/JSON pin differs')
    nulls = expected_nulls(suite, release)
    rows = table(directory / 'null-controls.tsv', 5)
    observed = {name: (side, version, field) for name, side, version, field, failure in rows}
    if observed != nulls or any(not re.fullmatch('[A-Za-z_][A-Za-z_0-9$.]*', row[4]) or row[4].endswith('UnsupportedVersionException') for row in rows):
        raise FixtureError('actual null rejection inventory differs')
    for name in nulls:
        members.add(name + '.bin')
        data = (directory / (name + '.bin')).read_bytes()
        if not data or len(data) > 65536:
            raise FixtureError('null frame bound differs')
    if with_rust:
        members.add('rust-emitted.tsv')
    actual = file_inventory(directory)
    if set(actual) != members:
        raise FixtureError('generated fixture member inventory differs')
    target = SUITES[suite]['api']
    return dict(files=actual, total_cases=len(expected), supported=sum(v[2] == 'supported' for v in expected.values()),
                unsupported=sum(v[2] == 'unsupported' for v in expected.values()), null_controls=len(nulls),
                rust_pairs=sum(v[0] == target and v[2] == 'supported' for v in expected.values()))


def validate_rust_export(directory, suite, release):
    target = SUITES[suite]['api']
    expected = {name: (api, version) for name, (api, version, status) in expected_cases(suite, release).items()
                if api == target and status == 'supported'}
    rows = table(directory / 'rust-emitted.tsv', 5)
    observed = {}
    for name, api, version, request, response in rows:
        if name in observed:
            raise FixtureError('duplicate Rust reverse case')
        observed[name] = (int(api), int(version))
        for frame in (request, response):
            data = bytes.fromhex(frame)
            if not data or len(data) > 65536 or data.hex() != frame:
                raise FixtureError('Rust reverse frame bound/encoding differs')
    if observed != expected:
        raise FixtureError('exact Rust reverse name/version inventory differs')
    return dict(pair_count=len(rows), expected_java_parse_checks=2 * len(rows),
                tsv_sha256=digest((directory / 'rust-emitted.tsv').read_bytes()))


class JavaFixtureProof:
    """Small adapter for ci-client-capabilities.command and .save callbacks.

    command accepts remove_env=() and returns (ledger_row, stdout_bytes, stderr_bytes).
    Call prepare once, generate before each fixture-bearing Rust suite, reverse
    after that Rust execution, and include report() even if a phase raises.
    """
    def __init__(self, root, evidence, command, save):
        self.root = Path(root).resolve()
        self.evidence = Path(evidence).resolve()
        if not self.evidence.is_relative_to(self.root / 'target'):
            raise FixtureError('Java outputs must stay under target evidence')
        self.directory = self.evidence / 'capability-java'
        self.directory.mkdir(mode=0o700)
        self.jars = self.directory / 'jars'
        self.jars.mkdir(mode=0o700)
        self.command, self.save = command, save
        self.state = {'prepared': False, 'suites': {}, 'scope': 'actual pinned Apache fixture generation and reverse parse; SDK socket peers not run'}
        self.inputs = {name: (self.root / name, pin) for name, pin in SOURCE_PINS.items()}
        self.artifacts = []
        for release, pin in PINS.items():
            name = 'kafka-clients-' + release + '.jar'
            self.artifacts.append((name, pin, f'https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/{release}/{name}'))
        self.artifacts.append(('slf4j-api-1.7.36.jar', SLF4J, 'https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar'))
        self.inputs.update({'capability-java/jars/' + name: (self.jars / name, pin) for name, pin, _ in self.artifacts})
        self.initial = self.input_inventory()
        self.save('capability-java/inputs-initial.json', self.initial)

    def input_inventory(self):
        result = {}
        for name, (path, expected) in self.inputs.items():
            row = {'expected_sha256': expected, 'exists': path.exists()}
            if path.exists():
                info = path.lstat()
                row.update(sha256=digest(path.read_bytes()), bytes=info.st_size, regular_file=stat.S_ISREG(info.st_mode))
            result[name] = row
        return result

    def verify_inputs(self, sources_only=False):
        for name, row in self.input_inventory().items():
            if sources_only and name not in SOURCE_PINS:
                continue
            if not row['exists'] or not row['regular_file'] or row['sha256'] != row['expected_sha256']:
                raise FixtureError('pinned Java input differs: ' + name)

    def checked(self, name, argv, java=False):
        row, out, error = self.command(name, argv, timeout=120,
                                       remove_env=JAVA_ENV_KEYS if java else ())
        if row['exit_code'] != 0 or row.get('trigger') is not None:
            raise FixtureError('Java fixture command failed: ' + name)
        return out

    def java(self, suite, release, null=False, reverse=False):
        spec = SUITES[suite]
        filename = spec['null'] if null else spec['oracle']
        source = next(name for name in SOURCE_PINS if name.endswith('/' + filename))
        fixtures = self.directory / 'fixtures' / spec['directory'] / release
        output = self.directory / 'reverse' / spec['directory'] / release if reverse else fixtures
        argv = ['java', '--class-path', os.pathsep.join(str(self.jars / name) for name in
                ('kafka-clients-' + release + '.jar', 'slf4j-api-1.7.36.jar')),
                str(self.root / source), str(output)]
        if reverse:
            argv.append(str(fixtures))
        return argv

    def prepare(self):
        try:
            self.verify_inputs(sources_only=True)
            self.checked('capability-java-version', ['java', '-version'], java=True)
            for name, pin, url in self.artifacts:
                self.checked('capability-java-download-' + name,
                             ['curl', '--fail', '--location', '--silent', '--show-error', '--max-time', '90',
                              '--max-filesize', '16777216', '--output', str(self.jars / name), url])
                if digest((self.jars / name).read_bytes()) != pin:
                    raise FixtureError('downloaded Java jar pin differs: ' + name)
            self.verify_inputs()
            self.state['prepared'] = True
        except BaseException as error:
            self.state['prepare_failure'] = {'type': type(error).__name__, 'detail': str(error)}
            raise
        finally:
            self.save('capability-java/inputs-preparation-after.json', self.input_inventory())

    def generate(self, suite):
        row = self.state['suites'].setdefault(suite, {})
        try:
            if not self.state['prepared']:
                raise FixtureError('pinned Java inputs were not prepared')
            self.verify_inputs()
            spec = SUITES[suite]
            result = {}
            for release in RELEASES:
                directory = self.directory / 'fixtures' / spec['directory'] / release
                directory.mkdir(parents=True, mode=0o700)
                self.checked('capability-java-' + spec['directory'] + '-' + release + '-generate', self.java(suite, release), java=True)
                self.checked('capability-java-' + spec['directory'] + '-' + release + '-null', self.java(suite, release, null=True), java=True)
                result[release] = validate_fixtures(directory, suite, release)
            row['generated'] = result
            self.save('capability-java/' + spec['directory'] + '-generated.json', result)
            self.verify_inputs()
            return {spec['environment']: str(self.directory / 'fixtures' / spec['directory'])}
        except BaseException as error:
            row['generation_failure'] = {'type': type(error).__name__, 'detail': str(error)}
            raise
        finally:
            row['inputs_after_generation'] = self.input_inventory()

    def reverse(self, suite):
        row = self.state['suites'].setdefault(suite, {})
        try:
            if 'generated' not in row:
                raise FixtureError('actual fixture generation did not pass')
            self.verify_inputs()
            spec = SUITES[suite]
            result = {}
            for release in RELEASES:
                directory = self.directory / 'fixtures' / spec['directory'] / release
                current = validate_fixtures(directory, suite, release, with_rust=True)
                original = row['generated'][release]['files']
                if {name: value for name, value in current['files'].items() if name != 'rust-emitted.tsv'} != original:
                    raise FixtureError('Java fixtures changed during Rust execution')
                exported = validate_rust_export(directory, suite, release)
                before = digest((directory / 'rust-emitted.tsv').read_bytes())
                output = self.directory / 'reverse' / spec['directory'] / release
                output.mkdir(parents=True, mode=0o700)
                self.checked('capability-java-' + spec['directory'] + '-' + release + '-reverse', self.java(suite, release, reverse=True), java=True)
                actual = json.loads((output / 'reverse-validation.json').read_bytes())
                if (actual.get('passed') is not True or type(actual.get('actual_parse_checks')) is not int
                        or actual != {'passed': True, 'actual_parse_checks': exported['expected_java_parse_checks']}):
                    raise FixtureError('Java reverse parse count differs')
                if before != digest((directory / 'rust-emitted.tsv').read_bytes()):
                    raise FixtureError('Rust reverse table changed during Java parse')
                after = validate_fixtures(directory, suite, release, with_rust=True)
                if after['files'] != current['files']:
                    raise FixtureError('fixture members changed during Java reverse parse')
                output_files = file_inventory(output)
                if set(output_files) != {'reverse-validation.json'}:
                    raise FixtureError('unexpected Java reverse output')
                result[release] = dict(exported, actual=actual, files=output_files)
            self.verify_inputs()
            row['reverse'] = result
            row['passed'] = True
            self.save('capability-java/' + spec['directory'] + '-reverse.json', result)
            return result
        except BaseException as error:
            row['reverse_failure'] = {'type': type(error).__name__, 'detail': str(error)}
            row['passed'] = False
            raise
        finally:
            row['inputs_after_reverse'] = self.input_inventory()

    def report(self):
        final = {}
        final_failure = None
        try:
            if self.state['prepared']:
                self.verify_inputs()
            for suite, row in self.state['suites'].items():
                spec = SUITES[suite]
                for release in RELEASES:
                    directory = self.directory / 'fixtures' / spec['directory'] / release
                    if not directory.exists():
                        continue
                    final[suite + '/' + release] = file_inventory(directory)
                    if row.get('passed'):
                        current = validate_fixtures(directory, suite, release, with_rust=True)
                        original = row['generated'][release]['files']
                        if {name: value for name, value in current['files'].items() if name != 'rust-emitted.tsv'} != original:
                            raise FixtureError('generated fixtures changed after reverse validation')
                        exported = validate_rust_export(directory, suite, release)
                        if exported != {key: row['reverse'][release][key] for key in exported}:
                            raise FixtureError('Rust exports changed after reverse validation')
        except BaseException as error:
            final_failure = {'type': type(error).__name__, 'detail': str(error)}
        return dict(self.state, inputs_initial=self.initial, inputs_final=self.input_inventory(),
                    generated_members_final=final, final_failure=final_failure,
                    passed=self.state['prepared'] and final_failure is None
                    and all(self.state['suites'].get(label, {}).get('passed') for label in SUITES))

#!/usr/bin/env python3
"""Run pinned Apache membership components twice and retain independent native fixtures."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import zipfile

HERE = Path(__file__).resolve().parent
SELECTED = ['LICENSE', 'NOTICE'] + ['raft/src/main/java/org/apache/kafka/raft/' + name + '.java' for name in
    ['LeaderState', 'VoterSet', 'ReplicaKey', 'KafkaRaftClient', 'Endpoints', 'MetadataLogConfig']] + [
    'raft/src/main/java/org/apache/kafka/raft/internals/' + name + '.java' for name in
    ['AddVoterHandler', 'AddVoterHandlerState', 'RemoveVoterHandler', 'RemoveVoterHandlerState',
     'KRaftControlRecordStateMachine', 'VoterSetHistory', 'TreeMapLogHistory', 'BatchAccumulator',
     'IdentitySerde', 'KafkaRaftMetrics']] + ['raft/src/test/java/org/apache/kafka/raft/' + name + '.java' for name in
    ['KafkaRaftClientReconfigTest', 'VoterSetTest', 'LeaderStateTest']] + [
    'clients/src/main/resources/common/message/VotersRecord.json',
    'clients/src/main/resources/common/message/KRaftVersionRecord.json',
    'clients/src/main/java/org/apache/kafka/common/utils/Time.java']
VARIANTS = {'wrong-old-majority': 'add.two-of-four-insufficient',
            'wrong-early-resign': 'remove.no-early-resign',
            'wrong-history-rejects': 'history.bare-helper-accepts-disjoint'}


def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


def retained_source(original, destination, version):
    selected = SELECTED + (['raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java']
        if version == '4.3.1' else ['core/src/main/scala/kafka/raft/KafkaMetadataLog.scala'])
    contents = {}
    with tarfile.open(original, 'r:gz') as archive:
        for member in archive:
            name = member.name.split('/', 1)[-1]
            if name in selected:
                if name in contents or not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise ValueError('Unsafe selected member: ' + name)
                contents[name] = archive.extractfile(member).read()
    if set(contents) != set(selected):
        raise ValueError('Missing selected sources: ' + str(set(selected) - set(contents)))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(contents.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode, member.mtime = len(data), 0o644, 0
            archive.addfile(member, io.BytesIO(data))
    destination.write_bytes(gzip.compress(raw.getvalue(), mtime=0))
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(contents.items())}


def adapt(source, version):
    changes = []
    if version != '4.3.1':
        for before, after in [('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.'),
                             ('org.apache.kafka.raft.internals.KafkaRaftLog', 'kafka.raft.KafkaMetadataLog'),
                             ('KafkaRaftLog', 'KafkaMetadataLog'),
                             ('KafkaMetadataLog.createLog(', 'KafkaMetadataLog.apply(')]:
            count = source.count(before)
            if not count:
                raise ValueError('Missing reviewed adaptation: ' + before)
            source = source.replace(before, after)
            changes.append({'from': before, 'to': after, 'occurrences': count})
    if version == '4.1.2':
        start = source.index('        // BEGIN_NEWER_EARLY_ACK')
        end = source.index('        // END_NEWER_EARLY_ACK') + len('        // END_NEWER_EARLY_ACK')
        source = source[:start] + source[end:]
        changes.append({'from': 'newer ackWhenCommitted=false scenario', 'to': 'not available in 4.1.2'})
        for before, after in [('voter(id).listeners(), true, clock.now', 'voter(id).listeners(), clock.now'),
                             ('Endpoints.empty(), true, h.clock.now', 'Endpoints.empty(), h.clock.now')]:
            count = source.count(before)
            if count != 1:
                raise ValueError('Unexpected 4.1.2 commit-ACK adaptation count')
            source = source.replace(before, after)
            changes.append({'from': before, 'to': after, 'occurrences': count})
    return source, changes


def validate_jars(distribution, directory, pin):
    actual = {path.name: digest(path) for path in sorted(directory.glob('*.jar'))}
    if not actual:
        raise ValueError('No SDK jars')
    found = {}
    with tarfile.open(distribution, 'r:gz') as archive:
        for member in archive:
            filename = Path(member.name).name
            if member.name == 'kafka_2.13-' + pin['release'] + '/libs/' + filename and filename in actual:
                if filename in found or not member.isfile() or member.size > 32 * 1024 * 1024:
                    raise ValueError('Unsafe or duplicate official jar')
                found[filename] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
    if found != actual:
        raise ValueError('Existing jars differ from exact verified Apache distribution')
    client = directory / ('kafka-clients-' + pin['release'] + '.jar')
    if digest(client) != pin['client_jar_sha256']:
        raise ValueError('Client pin mismatch')
    with zipfile.ZipFile(client) as archive:
        properties = archive.read('kafka/kafka-version.properties').decode()
    if 'version=' + pin['release'] not in properties or 'commitId=' + pin['source_sha'][:16] not in properties:
        raise ValueError('Embedded client version mismatch')
    return actual, properties


def events(path):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]
    if not rows or rows[-1].get('event') != 'summary' or not rows[-1]['passed']:
        raise ValueError('No successful complete component history')
    assertions = [row for row in rows if row['event'] == 'assertion']
    if not all(row['passed'] for row in assertions) or rows[-1]['assertions'] != len(assertions):
        raise ValueError('Assertion count/outcome mismatch')
    return rows


def fixtures(destination, rows, capture, pin, source_sha):
    destination.mkdir(parents=True, exist_ok=False)
    traces = {}
    current = None
    files = {}
    for row in rows:
        if row['event'] == 'summary':
            continue
        current = row.get('scenario', current)
        if current is None:
            raise ValueError('Unscoped event')
        traces.setdefault(current, []).append(row)
        if row['event'] == 'control-batch':
            names = [row['file']] + [record[field] for record in row['records'] for field in ['key_file', 'value_file']]
            for name in names:
                if Path(name).name != name or name in files:
                    raise ValueError('Unsafe/duplicate native capture filename')
                target = destination / 'native' / name
                target.parent.mkdir(exist_ok=True)
                shutil.copyfile(capture / name, target)
                files[str(target.relative_to(destination))] = digest(target)
    table = ['id\tcomponent\ttrace_file\texpected_disposition']
    for name, steps in traces.items():
        disposition = ('component_timeout_requires_committed_removal_fence' if name == 'remove-leader-timeout-late-commit' else
                       'unsupported_local_early_ack' if name == 'add-newer-early-ack' else
                       'permissive_component_requires_outer_fencing' if name.startswith('history-permissive') or name == 'catch-up-component-boundary' else
                       'one_change_component_behavior')
        path = destination / 'traces' / (name + '.json')
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps({'schema_version': 1, 'id': name, 'release': pin['release'],
            'component': 'official Apache membership components', 'native_epoch': 5,
            'local_normalized_term': 6, 'index_mapping': 'inclusive local index equals exclusive native end',
            'expected_disposition': disposition, 'steps': steps}, indent=2) + '\n')
        files[str(path.relative_to(destination))] = digest(path)
        table.append('\t'.join([name, 'ApacheMembershipProbe', str(path.relative_to(destination)), disposition]))
    path = destination / 'cases.tsv'; path.write_text('\n'.join(table) + '\n'); files[path.name] = digest(path)
    manifest = {'schema_version': 1, 'release': pin['release'], 'source_sha': pin['source_sha'],
        'client_jar_sha256': pin['client_jar_sha256'], 'probe_sha256': source_sha, 'native_epoch': 5,
        'local_normalized_term': 6, 'native_voters_record_version': 0,
        'index_mapping': 'native VotersRecord offset N-1 -> inclusive local configuration index N',
        'cases': len(traces), 'assertions': rows[-1]['assertions'],
        'transport': 'Local RequestSender capture adapter; no actual TCP request',
        'files_sha256': dict(sorted(files.items()))}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--source-archives', type=Path, default=Path('/workspace/work/broker-api'))
    parser.add_argument('--distributions', type=Path, default=Path('/workspace/work/broker-wire/releases'))
    parser.add_argument('--jar-root', type=Path, default=Path('/workspace/work/raft-quorum-final-f0d4e5d/runtime/jars'))
    args = parser.parse_args()
    for field in ['work', 'output', 'fixtures', 'source_archives', 'distributions', 'jar_root']:
        setattr(args, field, getattr(args, field).resolve())
    args.work.mkdir(parents=True, exist_ok=False)
    args.output.mkdir(parents=True, exist_ok=False)
    pins = json.loads((HERE / 'pins.json').read_text())
    report = {'schema_version': 1, 'scope': 'Official Apache membership components and actual control-record local log; no partitionline74 execution or replicated durable/network claim',
        'passed': False, 'probe_sha256': digest(HERE / 'ApacheMembershipProbe.java'), 'runner_sha256': digest(__file__),
        'pins_sha256': digest(HERE / 'pins.json'), 'releases': [], 'commands': []}
    def checkpoint():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    def execute(argv, label, expected_assertion=None):
        cmd = ['taskset', '-c', '0-2,4'] + argv
        stdout, stderr = args.output / (label + '.stdout'), args.output / (label + '.stderr')
        started = time.time()
        with stdout.open('wb') as out, stderr.open('wb') as err:
            result = subprocess.run(cmd, cwd=args.work, stdout=out, stderr=err, timeout=60)
        accepted = result.returncode == 0 if expected_assertion is None else result.returncode == 1
        row = {'argv': cmd, 'cwd': str(args.work), 'exit_code': result.returncode,
            'expected': 'zero' if expected_assertion is None else 'AssertionError: ' + expected_assertion,
            'accepted': accepted, 'elapsed_seconds': round(time.time() - started, 3),
            'stdout': stdout.name, 'stderr': stderr.name, 'stdout_sha256': digest(stdout), 'stderr_sha256': digest(stderr)}
        report['commands'].append(row); checkpoint()
        if not accepted:
            raise ValueError('Unexpected command result: ' + label)
        if expected_assertion:
            rows = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith('{')]
            if rows[-1].get('case') != expected_assertion or rows[-1].get('passed') is not False or 'AssertionError: ' + expected_assertion not in stderr.read_text():
                raise ValueError('Negative command failed at wrong assertion')
        return stdout
    try:
        execute(['java', '-version'], 'java-version')
        common = None
        for pin in pins['releases']:
            version = pin['release']; original = args.source_archives / (version + '.tar.gz')
            distribution = args.distributions / ('kafka_2.13-' + version + '.tgz')
            if digest(original) != pin['source_archive_sha256'] or digest(distribution) != pin['distribution_sha256'] or digest(distribution, 'sha512') != pin['distribution_sha512']:
                raise ValueError('Official source/distribution digest mismatch')
            checksum = ''.join((args.distributions / (distribution.name + '.sha512')).read_text().split(':', 1)[-1].split()).lower()
            if checksum != pin['distribution_sha512']:
                raise ValueError('Official checksum-file mismatch')
            retained = args.output / ('apache-membership-' + version + '.tar.gz')
            selected = retained_source(original, retained, version)
            jars, properties = validate_jars(distribution, args.jar_root / version, pin)
            adapted_source, adaptations = adapt((HERE / 'ApacheMembershipProbe.java').read_text(), version)
            adapted = args.work / 'adapted' / version / 'ApacheMembershipProbe.java'
            adapted.parent.mkdir(parents=True); adapted.write_text(adapted_source)
            copy = args.output / ('ApacheMembershipProbe-' + version + '.adapted.java'); copy.write_text(adapted_source)
            classes = args.work / 'classes' / version; classes.mkdir(parents=True)
            cp = ':'.join(str(args.jar_root / version / name) for name in sorted(jars))
            row = {'release': version, 'source_sha': pin['source_sha'], 'source_archive_sha256': pin['source_archive_sha256'],
                'distribution_sha256': pin['distribution_sha256'], 'distribution_sha512': pin['distribution_sha512'],
                'jars_sha256': jars, 'embedded_client_version_properties': properties, 'retained_archive': retained.name,
                'retained_archive_sha256': digest(retained), 'retained_sources_sha256': selected,
                'adapted_source': copy.name, 'adapted_source_sha256': digest(copy), 'api_adaptations': adaptations}
            report['releases'].append(row)
            execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(adapted)], version + '-compile')
            row['class_files_sha256'] = {str(p.relative_to(classes)): digest(p) for p in sorted(classes.rglob('*.class'))}
            observed = []
            for replay in [1, 2]:
                capture = args.output / (version + '-replay-' + str(replay) + '-native')
                stdout = execute(['java', '-Xmx128m', '-cp', str(classes) + ':' + cp, 'org.apache.kafka.raft.ApacheMembershipProbe',
                    str(args.work / (version + '-replay-' + str(replay))), str(capture)], version + '-replay-' + str(replay))
                observed.append(events(stdout))
                if replay == 1:
                    generated = fixtures(args.fixtures / version, observed[-1], capture, pin, report['probe_sha256'])
                    row['fixture_manifest_sha256'] = digest(args.fixtures / version / 'manifest.json')
                    row['fixture_cases'] = generated['cases']
                else:
                    before = {p.name: digest(p) for p in (args.output / (version + '-replay-1-native')).glob('*')}
                    after = {p.name: digest(p) for p in capture.glob('*')}
                    if before != after:
                        raise ValueError('Native capture replay mismatch')
            if observed[0] != observed[1]:
                raise ValueError('Component trace replay mismatch')
            # Assertion events do not carry a scenario: remove the complete contiguous newer scenario.
            filtered = []; scenario = None
            for event in observed[0]:
                scenario = event.get('scenario', scenario)
                if scenario != 'add-newer-early-ack' and event['event'] != 'summary':
                    copy_event = json.loads(json.dumps(event))
                    if copy_event['event'] == 'control-batch':
                        copy_event.pop('file')
                        for entry in copy_event['records']:
                            entry.pop('key_file'); entry.pop('value_file')
                    filtered.append(copy_event)
            if common is None:
                common = filtered
            if filtered != common:
                raise ValueError('Common three-release component behavior differs')
            row['positive_assertions_per_replay'] = observed[0][-1]['assertions']; row['positive_replays'] = 2
            row['negative_variants'] = []
            for variant, assertion in VARIANTS.items():
                execute(['java', '-Xmx128m', '-cp', str(classes) + ':' + cp, 'org.apache.kafka.raft.ApacheMembershipProbe',
                    str(args.work / (version + '-' + variant)), str(args.output / (version + '-' + variant + '-native')), variant],
                    version + '-' + variant, assertion)
                row['negative_variants'].append({'variant': variant, 'failed_assertion': assertion})
            checkpoint()
        report['passed'] = True
        report['positive_component_assertions'] = sum(r['positive_assertions_per_replay'] * r['positive_replays'] for r in report['releases'])
        report['deliberate_failing_executions'] = len(pins['releases']) * len(VARIANTS)
        report['limitations'] = [
            'No actual Apache networked quorum or partitionline membership runtime is executed.',
            'RequestSender captures actual discovery request creation but performs no TCP exchange.',
            'LeaderState catch-up history permits regressed reported offsets and uses a one-hour fetch horizon; correlated durable-prefix ACK safety needs independent runtime evidence.',
            'Control record history helper logs but accepts disjoint voter sets. One-change protocol handlers and durable runtime admission supply separate prerequisites.',
            'Apache local-log reopen preserves control records but resets the component high watermark; this is not durable replicated-commit proof.',
            'Newer ackWhenCommitted=false is tested only4.2.1/4.3.1 and is explicitly unsupported by the local common commit-ACK profile.',
            'Removed-leader request timeout clears the bare handler; later commit does not request resignation through that component. Local durable committed-removal fencing is explicit additional policy, not inferred from this helper.',
            'Negative variants alter only expected assertions, never the pinned Apache jars.']
        checkpoint(); print(json.dumps({'passed': True, 'component_assertions': report['positive_component_assertions'], 'negative_executions': report['deliberate_failing_executions']}))
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error); checkpoint(); raise


if __name__ == '__main__':
    main()

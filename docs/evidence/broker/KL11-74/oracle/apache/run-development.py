#!/usr/bin/env python3
"""Explicit SDK adaptations and retained compile/runtime results; no Rust oracle input."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--attempt', required=True)
    parser.add_argument('--version', default='4.3.1', choices=['4.1.2', '4.2.1', '4.3.1'])
    parser.add_argument('--variant', default='baseline')
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    dest = base / 'development' / args.attempt / args.version / args.variant
    dest.mkdir(parents=True, exist_ok=False)
    work = Path('/workspace/work/membership-apache') / args.attempt / args.version / args.variant
    classes = work / 'classes'
    classes.mkdir(parents=True, exist_ok=False)
    source = (base / 'ApacheMembershipProbe.java').read_text()
    adaptations = []
    if args.version != '4.3.1':
        for old, new in [('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.'),
                         ('org.apache.kafka.raft.internals.KafkaRaftLog', 'kafka.raft.KafkaMetadataLog'),
                         ('KafkaRaftLog', 'KafkaMetadataLog'),
                         ('KafkaMetadataLog.createLog(', 'KafkaMetadataLog.apply(')]:
            source = source.replace(old, new)
            adaptations.append({'from': old, 'to': new})
    if args.version == '4.1.2':
        start = source.index('        // BEGIN_NEWER_EARLY_ACK')
        end = source.index('        // END_NEWER_EARLY_ACK') + len('        // END_NEWER_EARLY_ACK')
        source = source[:start] + source[end:]
        adaptations.append({'from': 'newer ackWhenCommitted=false scenario', 'to': 'not available in 4.1.2'})
        source = source.replace('voter(id).listeners(), true, clock.now', 'voter(id).listeners(), clock.now')
        source = source.replace('Endpoints.empty(), true, h.clock.now', 'Endpoints.empty(), h.clock.now')
        adaptations.append({'from': 'ackWhenCommitted=true parameter', 'to': '4.1.2 handler always acknowledges after commit'})
    src = dest / 'ApacheMembershipProbe.java'
    src.write_text(source)
    jars = sorted((Path('/workspace/work/raft-quorum-final-f0d4e5d/runtime/jars') / args.version).glob('*.jar'))
    assert jars
    cp = ':'.join(map(str, jars))
    commands = []
    compile_cmd = ['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler',
                   'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(src)]
    run_cmd = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '-cp', str(classes) + ':' + cp,
               'org.apache.kafka.raft.ApacheMembershipProbe', str(work / 'logs'), str(dest / 'captures')]
    if args.variant != 'baseline':
        run_cmd.append(args.variant)
    for kind, cmd in [('compile', compile_cmd), ('run', run_cmd)]:
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=60)
            (dest / (kind + '.stdout')).write_bytes(result.stdout)
            (dest / (kind + '.stderr')).write_bytes(result.stderr)
            commands.append({'kind': kind, 'argv': cmd, 'exit_code': result.returncode})
            print(kind, result.returncode)
            if result.returncode:
                print(result.stdout.decode(errors='replace')[-7000:])
                print(result.stderr.decode(errors='replace')[-4000:])
                break
        except subprocess.TimeoutExpired as error:
            (dest / (kind + '.stdout')).write_bytes(error.stdout or b'')
            (dest / (kind + '.stderr')).write_bytes(error.stderr or b'')
            commands.append({'kind': kind, 'argv': cmd, 'exit_code': None, 'timeout_seconds': 60})
            print('timeout', kind)
            break
    receipt = {'schema_version': 1, 'version': args.version, 'variant': args.variant,
               'source_sha256': hashlib.sha256(src.read_bytes()).hexdigest(), 'adaptations': adaptations,
               'jars': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in jars}, 'commands': commands}
    (dest / 'commands.json').write_text(json.dumps(receipt, indent=2) + '\n')
    if len(commands) == 2 and all(c['exit_code'] == 0 for c in commands):
        print((dest / 'run.stdout').read_text()[-1000:])
    else:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

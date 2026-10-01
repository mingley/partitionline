#!/usr/bin/env python3
"""Offline pinned Confluent/Google oracle; never reads Rust output to generate."""
import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

SOURCE_SHA = 'd1e82aa164930437627f8d94723963c43f42ffa427af559ba70f814598c0103c'
JAR_SHA = '180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb'
COMMIT = '039a339d824debade488573769ad17b0d9975643'
CELLS = [
    ('first', 'fixture.First', [0], 'text: "hello"'),
    ('outer', 'fixture.Outer', [1], 'label: "top-level"'),
    ('nested', 'fixture.Outer.Envelope', [1, 1],
     'metadata { id: 300 source: "reference.proto" } body: "hello"'),
    ('empty', 'fixture.First', [0], ''),
    # Framing-only numeric boundary, not a path that exists in messages.proto.
    ('varints', None, [0, 64, 8192, 2147483647], ''),
]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def run(args, data=None):
    return subprocess.check_output(list(map(str, args)), input=data, stderr=subprocess.STDOUT)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True, help='pinned MessageIndexes.java')
    parser.add_argument('--kafka-jar', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, required=True)
    parser.add_argument('--protoc', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verify', action='store_true')
    parser.add_argument('--rust-output', type=Path, help='also independently decode Rust adapter output')
    args = parser.parse_args()
    if sha(args.source.read_bytes()) != SOURCE_SHA or sha(args.kafka_jar.read_bytes()) != JAR_SHA:
        raise ValueError('pinned source/jar checksum mismatch')
    java, javac = [args.java_home / 'bin' / name for name in ('java', 'javac')]
    java_version = run([java, '-version']).decode().splitlines()[0]
    protoc_version = run([args.protoc, '--version']).decode().strip()
    if '"21.0.12.1"' not in java_version or run([javac, '-version']).strip() != b'javac 21.0.12.1':
        raise ValueError('use pinned JDK 21.0.12.1')
    if protoc_version != 'libprotoc 3.21.12':
        raise ValueError('use pinned Google protoc 3.21.12')
    here = Path(__file__).resolve().parent
    args.output.mkdir(parents=True, exist_ok=True)
    def output(name, data):
        target = args.output / name
        if args.verify:
            if target.read_bytes() != data: raise ValueError('fixture differs: '+name)
        else:
            target.write_bytes(data)
    with tempfile.TemporaryDirectory(prefix='partitionline-protobuf-oracle-') as temporary:
        temp = Path(temporary)
        source = temp / 'io/confluent/kafka/schemaregistry/protobuf/MessageIndexes.java'
        source.parent.mkdir(parents=True)
        shutil.copyfile(args.source, source)
        classes = temp / 'classes'; classes.mkdir()
        run([javac, '-cp', args.kafka_jar.resolve(), '-d', classes,
             source, here / 'ProtoFrameOracle.java'])
        oracle = [java, '-cp', str(args.kafka_jar.resolve())+':'+str(classes),
                  'ProtoFrameOracle']
        manifest = {
            'confluent_version': '8.1.0', 'confluent_commit': COMMIT,
            'message_indexes_sha256': SOURCE_SHA,
            'source_license': 'Confluent Community License; fetched externally, not vendored or a runtime dependency',
            'kafka_version': '4.1.0', 'kafka_jar_origin': 'apache/kafka:4.1.0 distribution',
            'kafka_jar_sha256': JAR_SHA, 'jdk': '21.0.12.1', 'protoc': protoc_version,
            'proto_sha256': {name: sha((here / name).read_bytes()) for name in ('common.proto', 'messages.proto')},
            'cells': [],
        }
        for name, message, indexes, text in CELLS:
            payload = run([args.protoc, '-I'+str(here), '--encode='+message,
                           here / 'messages.proto'], text.encode()) if message else b''
            payload_path = temp / (name+'.payload.bin'); payload_path.write_bytes(payload)
            frame_path = temp / (name+'.frame.bin')
            parameters = [args.kafka_jar.resolve(), '42', ','.join(map(str, indexes)), payload_path]
            run(oracle+['encode']+parameters+[frame_path])
            decoded = run([args.protoc, '-I'+str(here), '--decode='+message,
                           here / 'messages.proto'], payload).decode() if message else ''
            output(name+'.payload.bin', payload)
            output(name+'.frame.bin', frame_path.read_bytes())
            output(name+'.decoded.txt', decoded.encode())
            if args.rust_output:
                rust_frame = args.rust_output / (name+'.frame.bin')
                rust_payload = args.rust_output / (name+'.payload.bin')
                if rust_payload.read_bytes() != payload: raise ValueError('Rust payload differs: '+name)
                run(oracle+['decode']+parameters+[rust_frame])
                if message and run([args.protoc, '-I'+str(here), '--decode='+message,
                                    here / 'messages.proto'], rust_payload.read_bytes()).decode() != decoded:
                    raise ValueError('Google decode mismatch: '+name)
            manifest['cells'].append({'name': name, 'schema_id': 42, 'message': message,
                                     'indexes': indexes, 'frame_sha256': sha(frame_path.read_bytes()),
                                     'payload_sha256': sha(payload), 'text': text,
                                     'qualification': 'framing and Google payload' if message else 'framing-only numeric boundaries'})
            print(name+' independently verified')
        output('manifest.json', (json.dumps(manifest, indent=2)+'\n').encode())


if __name__ == '__main__':
    main()

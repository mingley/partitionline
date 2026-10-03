#!/usr/bin/env python3
"""Independently check actual Rust journal framing/CRC and reverse-decode all acknowledged data."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import struct
import subprocess

ROOT = Path(__file__).resolve().parent
TIME = 1_700_000_000_000

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff

def make_receipt(topic, offset, ordinal, prefix):
    record = {'topic': topic, 'partition': 0, 'offset': offset, 'timestamp': TIME + ordinal,
              'key_hex': f'raw-key:{ordinal}'.encode().hex(), 'value_hex': f'raw-value:{ordinal}'.encode().hex(),
              'headers': [{'key': 'receipt', 'value_hex': f'{prefix}:raw:{ordinal}'.encode().hex()}]}
    return {'sha256': hashlib.sha256(json.dumps(record, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest(), 'record': record}

def extract(journal, output):
    assert 24 <= journal.stat().st_size <= 4 * 1024 * 1024
    data = journal.read_bytes()
    assert data[:8] == b'PLJRNL01'
    base, flags, header_crc = struct.unpack_from('>QII', data, 8)
    assert base == 0 and flags == 0 and header_crc == crc32c(data[:20])
    output.mkdir(parents=True, exist_ok=False)
    position = 24
    next_offset = 0
    entries = []
    while position < len(data):
        assert len(entries) < 128 and len(data) - position >= 32
        header = data[position:position + 32]
        assert header[:8] == b'PLENTRY1'
        length, first, count, payload_crc, entry_crc = struct.unpack_from('>IQIII', header, 8)
        assert 1 <= length <= 128 * 1024 and 1 <= count <= 512 and first == next_offset
        assert entry_crc == crc32c(header[:28]) and length <= len(data) - position - 32
        payload = data[position + 32:position + 32 + length]
        assert crc32c(payload) == payload_crc
        destination = output / f'{len(entries):04}.payload.bin'
        destination.write_bytes(payload)
        next_offset += count
        assert next_offset <= 2**63 - 1
        entries.append({'first_offset': first, 'count': count, 'bytes': length,
                        'payload_crc32c': payload_crc, 'payload_sha256': sha(destination)})
        position += 32 + length
    assert position == len(data)
    return {'journal_bytes': len(data), 'journal_sha256': sha(journal), 'next_offset': next_offset,
            'entries': entries, 'framing': 'independent standard-library big-endian parsing and bitwise Castagnoli CRC32C'}

def main():
    parser = argparse.ArgumentParser()
    for name in ['journal-state', 'peer-state', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    assert crc32c(b'123456789') == 0xe3069283
    source = (ROOT / 'JournalPeer.java').read_text()
    classes = {}
    compiled = []
    pins = {row['version']: row['jar_sha256'] for row in json.loads((ROOT / 'upstream-pins.json').read_text())['releases']}
    for version in ['4.1.2', '4.2.1', '4.3.1']:
        package = 'org.apache.kafka.common.record.internal' if version == '4.3.1' else 'org.apache.kafka.common.record'
        scratch = Path('/workspace/work/broker-log-oracle/journal-peer') / version
        scratch.mkdir(parents=True, exist_ok=True)
        src = scratch / 'JournalPeer.java'
        src.write_text(source.replace('org.apache.kafka.common.record.internal.', package + '.'))
        target = scratch / 'classes'
        target.mkdir(exist_ok=True)
        jar = Path(f'/workspace/work/broker-wire/jars/kafka-clients-{version}.jar')
        assert sha(jar) == pins[version]
        command = ['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main',
                   '-Xlint:all', '-Werror', '-cp', str(jar), '-d', str(target), str(src)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        (args.output / f'{version}-compile.log').write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
        assert result.returncode == 0
        classes[version] = target
        compiled.append({'release': version, 'command': command, 'exit_code': 0, 'jar_sha256': sha(jar),
                         'adapted_source_sha256': sha(src), 'class_sha256': sha(target / 'JournalPeer.class')})
    report = {'source_sha': args.source_sha, 'journal_peer_sha256': sha(ROOT / 'JournalPeer.java'),
              'scope': 'Actual offline reverse decoding of Rust-emitted journal bytes. No Fetch or replication claim.',
              'compile': compiled, 'topics': [], 'decoded_record_comparisons': 0}
    for writer in ['4.1.2', '4.2.1', '4.3.1']:
        prefix = 'ordinary-' + writer.replace('.', '-')
        expected = {}
        for phase in ['append', 'restart-append', 'compressed-enabled']:
            path = args.peer_state / f'{writer}-{phase}-history.json'
            history = json.loads(path.read_text())
            assert history['passed']
            for event in history['history']:
                receipt = event.get('expected_receipt') if event['label'] == 'actual-Producer' else event.get('receipt') if event['label'] in ['restart-Producer-checkpoint', 'compressed-expected-decoded-record'] else None
                if receipt is not None:
                    record = receipt['record']
                    expected.setdefault((record['topic'], record['partition']), []).append(receipt)
        expected[(prefix + '-raw', 0)] = [make_receipt(prefix + '-raw', n, n, prefix) for n in range(34)]
        expected[(prefix + '-codec-default', 0)] = [make_receipt(prefix + '-codec-default', 0, 0, prefix)]
        enabled = prefix + '-codec-enabled'
        expected[(enabled, 0)].append(make_receipt(enabled, 120, 0, prefix))
        for (topic, partition), receipts in sorted(expected.items()):
            uuid = args.peer_state / (topic + '.uuid')
            raw_id = base64.urlsafe_b64decode(uuid.read_text() + '==')
            assert len(raw_id) == 16
            journal = args.journal_state / 'partitions' / f'{raw_id.hex()}-{partition}.journal'
            output = args.output / f'{topic}-{partition}'
            framing = extract(journal, output)
            assert framing['next_offset'] == len(receipts)
            topic_report = {'writer_release': writer, 'topic': topic, 'partition': partition,
                            'topic_uuid': uuid.read_text(), 'framing': framing, 'readers': []}
            for reader, target in classes.items():
                destination = output / f'decoded-{reader}.json'
                command = ['taskset', '-c', '0-2,4', 'java', '-Xmx64m', '-cp',
                           f'{target}:/workspace/work/broker-wire/jars/kafka-clients-{reader}.jar:/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar',
                           'JournalPeer', topic, str(partition), str(output), str(destination)]
                result = subprocess.run(command, capture_output=True, text=True, timeout=30)
                (output / f'decode-{reader}.log').write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
                assert result.returncode == 0
                decoded = json.loads(destination.read_text())
                assert decoded['receipts'] == receipts
                topic_report['readers'].append({'release': reader, 'command': command, 'exit_code': 0,
                                               'records': decoded['records'], 'batches': decoded['batches'],
                                               'receipt_hashes': [row['sha256'] for row in decoded['receipts']],
                                               'output_sha256': sha(destination)})
                report['decoded_record_comparisons'] += len(receipts)
            report['topics'].append(topic_report)
            (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    report['passed'] = True
    (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'topics_partitions': len(report['topics']), 'record_comparisons': report['decoded_record_comparisons'], 'passed': True}))

if __name__ == '__main__':
    main()

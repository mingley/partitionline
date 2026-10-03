#!/usr/bin/env python3
"""Run three pinned Apache decoders on actual selected Rust journal payloads."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[6]
BASE = Path(__file__).resolve().parent
SOURCE_ROOT = Path('/workspace/work/retention-final-d147bcf1/source')
SCRATCH = Path('/workspace/work/retention-final-peers/reverse-java')
import importlib.util
spec = importlib.util.spec_from_file_location('seal_live', BASE / 'seal-live.py')
sealer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sealer)
require, bounded, pin, sha = sealer.require, sealer.bounded, sealer.pin, sealer.sha


def run(command, log):
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=30, check=False)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_bytes(result.stdout)
    return {'command': command, 'exit_code': result.returncode, 'log': pin(log)}


def main():
    records_path = BASE / 'records-validation.json'
    records = json.loads(bounded(records_path))
    require(records['passed'] and records['source_sha'] == sealer.SOURCE and
            len(records['selected_states']) == 24, 'actual selected input states')
    common_path = SOURCE_ROOT / 'docs/evidence/broker/KL11-68/JournalPeer.java'
    common = common_path.read_text()
    common_hash = sha(common.encode())
    require(sha((ROOT / 'docs/evidence/broker/KL11-68/JournalPeer.java').read_bytes()) == common_hash,
            'pinned previously accepted decoder source')
    build, results, physical_count, retained_count = [], [], 0, 0
    jars = Path('/workspace/work/broker-wire/jars')
    java_build = json.loads(bounded(BASE / 'java-build-attempt-1/validation.json'))
    for release in ['4.1.2', '4.2.1', '4.3.1']:
        directory = SCRATCH / release
        classes = directory / 'classes'
        classes.mkdir(parents=True, exist_ok=True)
        adapted = common if release == '4.3.1' else common.replace('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.')
        source = directory / 'JournalPeer.java'
        source.write_text(adapted)
        classpath = ':'.join(str(jars / name) for name in [f'kafka-clients-{release}.jar', 'slf4j-api-1.7.36.jar'])
        pinned = next(r for r in java_build['results'] if r['release'] == release)['jars']
        for jar in pinned:
            require(sha(Path(jar['path']).read_bytes()) == jar['sha256'], 'actual official jar identity')
        command = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '--add-modules', 'jdk.compiler',
                   'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', classpath,
                   '-d', str(classes), str(source)]
        receipt = run(command, BASE / f'reverse-records/build/{release}/compile.log')
        receipt.update(release=release, common_source_sha256=common_hash,
                       adapted_source_sha256=sha(adapted.encode()), jars=pinned,
                       classes=[{'path': str(p), 'sha256': sha(p.read_bytes())}
                                for p in sorted(classes.rglob('*.class'))],
                       adaptation='Only the four official record-class import package paths differ before4.3; decoder behavior unchanged.')
        build.append(receipt)
        require(receipt['exit_code'] == 0, 'strict independent Java decoder compilation')
        for state in records['selected_states']:
            label = state['lane'] + '-' + state['phase'] + '-' + state['release']
            output = BASE / f'reverse-records/decoded/{release}/{label}.json'
            output.parent.mkdir(parents=True, exist_ok=True)
            command = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '-cp', str(classes) + ':' + classpath,
                       'JournalPeer', state['topic'], '0', str(ROOT / state['payload_directory']), str(output)]
            receipt = run(command, output.with_suffix('.log'))
            require(receipt['exit_code'] == 0, 'actual Apache reverse journal execution')
            decoded = json.loads(bounded(output))
            require(decoded['records'] == decoded['next_offset'] == state['actual_end'], 'actual physical Apache record count/end')
            require([r['record']['offset'] for r in decoded['receipts']] == list(range(state['actual_end'])),
                    'actual physical record continuity')
            for r in decoded['receipts']:
                require(sealer.record_receipt(r['record']) == r, 'actual Apache reverse exact canonical bytes')
            retained = [r for r in decoded['receipts'] if r['record']['offset'] >= state['logical_floor']]
            expected = [sealer.record_receipt(sealer.expected(state['topic'], index))
                        for index in range(state['logical_floor'], state['actual_end'])]
            require(retained == expected, 'actual Apache logical-floor suffix matches all live peers')
            physical_count += decoded['records']
            retained_count += len(retained)
            receipt.update(decoder_release=release, lane=state['lane'], phase=state['phase'],
                           topic_release=state['release'], report=pin(output),
                           physical_records=decoded['records'], retained_records=len(retained),
                           logical_floor=state['logical_floor'], selected_input=state['manifest'])
            results.append(receipt)
    require(len(results) == 72 and physical_count == 540 and retained_count == 324, 'actual reverse execution denominators')
    result = {'schema_version': 1, 'source_sha': sealer.SOURCE, 'passed': True,
              'input_receipt': pin(records_path), 'common_source': {'path': 'docs/evidence/broker/KL11-68/JournalPeer.java', 'sha256': common_hash},
              'builds': build, 'results': results, 'actual_apache_executions': len(results),
              'physical_record_comparisons': physical_count, 'retained_record_comparisons': retained_count,
              'checker_sha256': sha(Path(__file__).read_bytes()),
              'command': ['python3', str(Path(__file__).relative_to(ROOT))],
              'scope': 'Actual Apache4.1.2/4.2.1/4.3.1 MemoryRecords CRC/full-byte/record decoding of extracted Rust-selected rolling journal batches, then logical-floor suffix equality with actual Java/native/public Rust reads.',
              'limitations': ['The whole first batch remains physically present below floor3; filtering is explicit and not physical prefix deletion.',
                              'Finite ordinary/uncompressed/CreateTime/RF1 histories; no replication, transactions or power-loss claim.']}
    output = BASE / 'reverse-validation.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['passed', 'actual_apache_executions', 'physical_record_comparisons', 'retained_record_comparisons']}))


if __name__ == '__main__':
    main()

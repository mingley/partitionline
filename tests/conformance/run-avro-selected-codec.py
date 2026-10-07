#!/usr/bin/env python3
"""Run the hash-pinned Java Avro peer against actual selected-codec Rust output."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = ROOT / 'tests/fixtures/avro_selected_codec'
# Independent pins: never take expected dependency checksums from the downloaded files.
JARS = (
    ('org/apache/avro', 'avro', '1.12.1', '72600f057bfbe2efe35fa55433e7756d45667dc938934be38a6e2be368aca237'),
    ('com/fasterxml/jackson/core', 'jackson-core', '2.20.0', 'bc0cf46075877201f8406ee7de2741ae7df6c066f5f0457bd80632a718c06e72'),
    ('com/fasterxml/jackson/core', 'jackson-databind', '2.20.0', 'a70e146a6bf2cba4f9cd367169787f50adcfbb57122bc2e9c8390cd0b397ac30'),
    ('com/fasterxml/jackson/core', 'jackson-annotations', '2.20', '959a2ffb2d591436f51f183c6a521fc89347912f711bf0cae008cdf045d95319'),
    ('org/slf4j', 'slf4j-api', '2.0.17', '7b751d952061954d5abfed7181c1f645d336091b679891591d63329c622eb832'),
    ('org/slf4j', 'slf4j-simple', '2.0.17', 'ddfea59ac074c6d3e24ac2c38622d2d963895e17f70b38ed4bdae4d780be6964'),
)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rust-output', type=pathlib.Path, required=True)
    parser.add_argument('--peer-cache', type=pathlib.Path, required=True)
    parser.add_argument('--report', type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.peer_cache.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {'status': 'fail', 'process_exited': False, 'jars': [], 'rust_artifacts': {}}
    started = time.monotonic()
    try:
        manifest = json.loads((FIXTURES / 'manifest.json').read_text())
        for filename, expected in manifest['artifact_sha256'].items():
            if sha(FIXTURES / filename) != expected:
                raise ValueError(f'fixture hash mismatch: {filename}')
        # Require every generated output; source/run provenance is recorded by the caller.
        for case in manifest['cases']:
            for suffix in ('frame.bin', 'reader.json'):
                path = args.rust_output / f'{case}.{suffix}'
                report['rust_artifacts'][path.name] = sha(path)
        paths = []
        for group, name, version, expected in JARS:
            filename = f'{name}-{version}.jar'
            path = args.peer_cache / filename
            url = f'https://repo.maven.apache.org/maven2/{group}/{name}/{version}/{filename}'
            if not path.exists():
                with urllib.request.urlopen(url, timeout=30) as response:
                    data = response.read(8 * 1024 * 1024 + 1)
                if len(data) > 8 * 1024 * 1024:
                    raise ValueError('peer artifact exceeds download limit')
                if hashlib.sha256(data).hexdigest() != expected:
                    raise ValueError(f'download hash mismatch: {filename}')
                path.write_bytes(data)
            if sha(path) != expected:
                raise ValueError(f'cached peer hash mismatch: {filename}')
            paths.append(str(path.resolve()))
            report['jars'].append({'name': filename, 'url': url, 'sha256': expected})
        import os
        source = ROOT / 'tests/conformance/java/ConformanceAvroSelectedCodec.java'
        command = ['java', '-Xmx128m', '--class-path', os.pathsep.join(paths), str(source),
                   'verify', str(ROOT / 'partitionline-schema/tests/fixtures/avro'),
                   str(FIXTURES), str(args.rust_output.resolve())]
        report['source_sha256'] = sha(source)
        report['command'] = command
        version = subprocess.run(['java', '--version'], capture_output=True, text=True, timeout=10, check=True)
        report['java_version'] = version.stdout
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        report['pid'] = process.pid
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            report['timed_out'] = True
        report.update(stdout=stdout, stderr=stderr, exit=process.returncode, process_exited=True)
        if process.returncode or report.get('timed_out'):
            raise RuntimeError('Java peer failed; see retained streams')
        peer = json.loads(stdout)
        if peer['status'] != 'pass' or peer['cases'] != 9 or len(peer['negative_cases']) != 6:
            raise ValueError('incomplete peer receipt')
        report.update(status='pass', peer=peer)
    except Exception as error:
        report['error'] = f'{type(error).__name__}: {error}'
    finally:
        report['elapsed_seconds'] = round(time.monotonic() - started, 6)
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(f"selected Avro Java peer: {report['status']}; report: {args.report}")
    return 0 if report['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Generate/check pinned Apache InitProducerId fields and actual Rust output."""
import argparse
import hashlib
import http.client
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'tests/conformance/java/InitProducerIdV6Fixtures.java'
FIXTURES = ROOT / 'tests/fixtures/init-producer-id-v6'
PINS = {
    '4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
    '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
    '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36',
}
COMMITS = {'4.1.2': 'c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c',
           '4.2.1': '18d5ecd939c8d510fdd72d0abb1f7099659dcd58',
           '4.3.1': '26b251a451ce941d3d7a55e6487bcb7f16b5ad48'}

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def download_bytes(url, max_bytes, attempts=4, base_delay=1.0):
    request = urllib.request.Request(url, headers={'User-Agent': 'partitionline-ci/1'})
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError(f'peer artifact exceeds {max_bytes}-byte download limit: {url}')
            return data
        except urllib.error.HTTPError as error:
            last_error = error
            retryable = error.code in (404, 408, 429) or 500 <= error.code < 600
            if not retryable or attempt == attempts:
                raise RuntimeError(
                    f'failed to download {url} (attempt {attempt}/{attempts}, HTTP {error.code}): {error}'
                ) from error
        except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError) as error:
            last_error = error
            if attempt == attempts:
                raise RuntimeError(
                    f'failed to download {url} (attempt {attempt}/{attempts}): {error}'
                ) from error
        time.sleep(base_delay * (2 ** (attempt - 1)))
    raise RuntimeError(f'failed to download {url}: {last_error}')

def peer(cache, release):
    cache.mkdir(parents=True, exist_ok=True)
    p = cache / f'kafka-clients-{release}.jar'
    if not p.exists():
        url = f'https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/{release}/{p.name}'
        data = download_bytes(url, 16*1024*1024)
        assert len(data) <= 16*1024*1024 and hashlib.sha256(data).hexdigest() == PINS[release]
        p.write_bytes(data)
    assert p.stat().st_size <= 16*1024*1024 and sha(p) == PINS[release]
    return p.resolve()

def run(cache, release, out, mode):
    jar = peer(cache, release)
    command = ['java', '-Xmx128m', '--class-path', str(jar), str(SOURCE),
               release, str(jar), str(out.resolve()), mode]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(json.dumps({'command': command, 'exit_code': result.returncode,
                                       'stdout': result.stdout, 'stderr': result.stderr}))
    return {'command': command, 'exit_code': result.returncode,
            'stdout': result.stdout, 'stderr': result.stderr}

def files(directory):
    paths = sorted(p for p in directory.rglob('*') if p.is_file() and p.name != 'manifest.json')
    assert len(paths) == 78 and all(p.stat().st_size <= 65536 and not p.is_symlink() for p in paths)
    return {str(p.relative_to(directory)): sha(p) for p in paths}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peer-cache', required=True, type=Path)
    parser.add_argument('--generate', action='store_true')
    parser.add_argument('--rust-output', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.generate:
        FIXTURES.mkdir()
        receipts = [run(args.peer_cache, release, FIXTURES/release, 'generate') for release in PINS]
        manifest = {'api': 'InitProducerId', 'api_key': 22, 'versions': list(range(7)),
                    'pairs_per_release': 13, 'jar_sha256': PINS, 'upstream_commits': COMMITS,
                    'generator_sha256': sha(SOURCE), 'runner_sha256': sha(Path(__file__)),
                    'sha256': files(FIXTURES), 'generation': receipts}
        (FIXTURES/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    else:
        if args.rust_output is None or args.report is None:
            parser.error('--rust-output and --report required')
        manifest = json.loads((FIXTURES/'manifest.json').read_text())
        assert manifest['jar_sha256'] == PINS and manifest['upstream_commits'] == COMMITS
        assert manifest['generator_sha256'] == sha(SOURCE) and manifest['runner_sha256'] == sha(Path(__file__))
        assert manifest['sha256'] == files(FIXTURES)
        rust_files = files(args.rust_output)
        assert rust_files.keys() == manifest['sha256'].keys()
        receipts = []
        for release in PINS:
            receipts.append(run(args.peer_cache, release, FIXTURES/release, 'verify'))
            receipts.append(run(args.peer_cache, release, args.rust_output/release, 'rust'))
        report = {'status': 'pass', 'jar_sha256': PINS, 'pairs_per_release': 13,
                  'release_count': 3, 'rust_sha256': rust_files, 'receipts': receipts,
                  'scope': 'Actual Apache message serializers/parsers and request factories. Ordinary defaults only; full prepared transaction lifecycle is separate.'}
        with args.report.open('x') as f:
            json.dump(report, f, indent=2); f.write('\n')
    print(json.dumps({'status': 'pass', 'pairs': 39}))

if __name__ == '__main__':
    main()

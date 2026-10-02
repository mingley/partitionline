#!/usr/bin/env python3
"""Reproduce pinned Apache Java fixtures; jars are independently downloaded inputs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

RELEASES = {
    '4.1.2': ('33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed', 'c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c'),
    '4.2.1': ('9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159', '18d5ecd939c8d510fdd72d0abb1f7099659dcd58'),
    '4.3.1': ('dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e', '26b251a451ce941d3d7a55e6487bcb7f16b5ad48'),
}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def run(command, cwd, log):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
    log.write_text('$ ' + ' '.join(map(str, command)) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
    if result.returncode: raise RuntimeError(f'command failed, see {log}')
    return {'command': list(map(str, command)), 'exit_code': result.returncode, 'output': result.stdout + result.stderr}
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jars', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    evidence = Path(__file__).resolve().parent
    repo = evidence.parents[3]
    args.scratch.mkdir(parents=True, exist_ok=True)
    logs = evidence / 'logs'; logs.mkdir(exist_ok=True)
    classes = args.scratch / 'classes'; classes.mkdir(exist_ok=True)
    slf4j = args.jars / 'slf4j-api-1.7.36.jar'
    assert sha(slf4j) == SLF4J
    summary = {'generator_sha256': sha(evidence / 'MetadataOracle.java'), 'independence': 'Only official Apache classes loaded; Rust source neither parsed nor imported.', 'releases': []}
    for version, (jar_hash, source_sha) in RELEASES.items():
        jar = args.jars / f'kafka-clients-{version}.jar'
        assert sha(jar) == jar_hash
        compile_command = ['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', str(jar), '-d', str(classes), str(evidence / 'MetadataOracle.java')]
        compiled = run(compile_command, repo, logs / f'compile-{version}.txt')
        dest = args.output / version
        runtime = ['taskset', '-c', '0-2,4', 'java', '-cp', f'{classes}:{jar}:{slf4j}', 'MetadataOracle', version, str(dest)]
        actual = run(runtime, repo, logs / f'generate-{version}.txt')
        with tempfile.TemporaryDirectory(prefix='metadata-replay-', dir=args.scratch) as tmp:
            repeated = run(runtime[:-1] + [tmp], repo, logs / f'replay-{version}.txt')
            original = {str(p.relative_to(dest)): sha(p) for p in dest.rglob('*') if p.is_file()}
            replay = {str(p.relative_to(Path(tmp))): sha(p) for p in Path(tmp).rglob('*') if p.is_file()}
            assert original == replay, (version, 'nondeterministic fixture output')
        manifest = json.loads((dest / 'goldens.json').read_text())
        pairs = {(c['api_key'], c['api_version']) for c in manifest['cases']}
        required = {(3, v) for v in range(14)} | {(18, v) for v in range(5)} | {(19, v) for v in range(2, 5)} | {(20, v) for v in range(1, 7)}
        assert pairs == required
        assert len((dest / 'cases.tsv').read_text().splitlines()) == len(manifest['cases'])
        for line in (dest / 'cases.tsv').read_text().splitlines(): assert len(line.split('\t')) == 4
        for case in manifest['cases']:
            for direction in ['request', 'response']:
                path = dest / f"{case['name']}.{direction}.bin"
                if case[f'{direction}_hex'] is None:
                    assert direction == 'response' and case['handler_policy'] == 'reject_neither_identity'
                    assert not path.exists()
                    continue
                assert path.read_bytes().hex() == case[f'{direction}_hex']
                assert sha(path) == case[f'{direction}_sha256']
        summary['releases'].append({'version': version, 'source_sha': source_sha, 'jar_sha256': jar_hash, 'slf4j_sha256': SLF4J, 'compile': compiled, 'runtime': actual, 'replay': repeated, 'case_count': len(manifest['cases']), 'response_golden_count': sum(c['response_hex'] is not None for c in manifest['cases']), 'deliberate_rejection_count': sum(c['response_hex'] is None for c in manifest['cases']), 'advertised_pairs': len(pairs), 'fixture_hashes': original})
    manifests = [json.loads((args.output / v / 'goldens.json').read_text())['cases'] for v in RELEASES]
    assert manifests[0] == manifests[1] == manifests[2], 'cross-release generated cases differ'
    summary['all_release_cases_byte_identical'] = True
    summary['total_cases'] = sum(r['case_count'] for r in summary['releases'])
    (evidence / 'oracle-generation.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'total_cases': summary['total_cases'], 'release_cases': [r['case_count'] for r in summary['releases']], 'all_release_cases_byte_identical': True}))
if __name__ == '__main__': main()

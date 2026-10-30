#!/usr/bin/env python3
"""Resolve dependency metadata and downstream floor controls without compiling."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import time
import tomllib

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/security-dependencies-20261030')
CANDIDATE = OUT / 'candidate'
ENV = os.environ.copy()
ENV.update(RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo',
           CARGO_TARGET_DIR=str(OUT / 'no-build-target'), CARGO_NET_RETRY='2',
           CARGO_HTTP_TIMEOUT='30')
CARGO = ['/workspace/work/cargo/bin/cargo', '+stable']
RESULTS = []

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(name, command, cwd=CANDIDATE, timeout=150):
    log = OUT / f'{name}.log'
    start = time.monotonic()
    with log.open('wb') as stream:
        process = subprocess.run(command, cwd=cwd, env=ENV, stdout=stream,
                                 stderr=subprocess.STDOUT, timeout=timeout)
    receipt = {'name': name, 'command': command, 'cwd': str(cwd),
               'exit': process.returncode, 'elapsed_seconds': time.monotonic()-start,
               'log_sha256': digest(log), 'scope': 'dependency resolution only; no compilation'}
    (OUT / f'{name}.json').write_text(json.dumps(receipt, indent=2)+'\n')
    RESULTS.append(receipt)
    print(json.dumps(receipt), flush=True)
    return receipt, log.read_text()

def main():
    before = {str(p.relative_to(CANDIDATE)): digest(p)
              for p in CANDIDATE.rglob('Cargo.*') if p.is_file()}
    # This retrieves registry sources, but runs no build scripts or compiler.
    receipt, _ = run('root-fetch', CARGO+['fetch', '--locked', '--target', 'x86_64-unknown-linux-gnu'])
    assert receipt['exit'] == 0
    for name, manifest, features in [
        ('root-all', 'Cargo.toml', ['--all-features']),
        ('broker-all', 'partitionline-broker/Cargo.toml', ['--all-features']),
        ('broker-default', 'partitionline-broker/Cargo.toml', []),
        ('schema-all', 'partitionline-schema/Cargo.toml', ['--all-features']),
        ('schema-core', 'partitionline-schema/Cargo.toml', ['--no-default-features']),
    ]:
        stdout = OUT / f'{name}-metadata.json'
        stderr = OUT / f'{name}-metadata.stderr'
        cmd = CARGO+['metadata', '--locked', '--format-version', '1', '--filter-platform',
                     'x86_64-unknown-linux-gnu', '--manifest-path', manifest]+features
        start = time.monotonic()
        with stdout.open('wb') as out, stderr.open('wb') as err:
            process = subprocess.run(cmd, cwd=CANDIDATE, env=ENV, stdout=out, stderr=err, timeout=150)
        receipt = {'name': name+'-metadata', 'command': cmd, 'exit': process.returncode,
                   'elapsed_seconds': time.monotonic()-start, 'stdout_sha256': digest(stdout),
                   'stderr_sha256': digest(stderr), 'scope': 'dependency metadata only; no compilation'}
        (OUT / f'{name}-metadata-receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
        RESULTS.append(receipt)
        print(json.dumps(receipt), flush=True)
        assert process.returncode == 0

    baseline = OUT / 'baseline'
    baseline.mkdir(exist_ok=True)
    for path in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']:
        shutil.copy2(ROOT / path, baseline / path)
    for dirname in ['src', 'examples']:
        shutil.copytree(CANDIDATE / dirname, baseline / dirname, dirs_exist_ok=True)
    old = {'ctutils': '0.4.2', 'rustls': '0.23.45', 'rustls-webpki': '0.103.15',
           'tokio': '1.53.2', 'webpki-roots': '1.0.9'}
    safe = {'ctutils': '0.4.7', 'rustls': '0.23.46', 'rustls-webpki': '0.103.16',
            'tokio': '1.53.3', 'webpki-roots': '1.0.11'}
    controls=[]
    for variant, path in [('before', baseline), ('after', CANDIDATE)]:
        for package, version in old.items():
            name=f'floor-{variant}-{package}'
            consumer=OUT/'controls'/name
            (consumer/'src').mkdir(parents=True, exist_ok=True)
            (consumer/'src/lib.rs').write_text('// Resolver control; never compiled.\n')
            (consumer/'Cargo.toml').write_text(
                '[package]\nname = "floor-control"\nversion = "0.0.0"\nedition = "2021"\n'
                '[workspace]\n[dependencies]\n'
                f'partitionline = {{ path = "{path}", default-features = false }}\n'
                f'{package} = {{ version = "={version}", default-features = false }}\n')
            receipt, log=run(name, CARGO+['generate-lockfile', '--offline'], consumer)
            expected=variant=='before'
            assert (receipt['exit']==0)==expected, log
            if expected:
                data=tomllib.loads((consumer/'Cargo.lock').read_text())
                selected=[p['version'] for p in data['package'] if p['name']==package]
                assert version in selected
            else:
                assert 'failed to select a version' in log and package in log, log
            controls.append({'variant':variant,'package':package,'version':version,
                             'exit':receipt['exit'],'expected_success':expected,'verified':True})
    consumer=OUT/'controls'/'floor-after-patched'
    (consumer/'src').mkdir(parents=True, exist_ok=True)
    (consumer/'src/lib.rs').write_text('// Resolver control; never compiled.\n')
    (consumer/'Cargo.toml').write_text(
        '[package]\nname = "floor-control"\nversion = "0.0.0"\nedition = "2021"\n'
        '[workspace]\n[dependencies]\n'
        f'partitionline = {{ path = "{CANDIDATE}", default-features = false }}\n'+
        ''.join(f'{name} = {{ version = "={version}", default-features = false }}\n'
                for name,version in safe.items()))
    receipt, log=run('floor-after-patched', CARGO+['generate-lockfile', '--offline'], consumer)
    assert receipt['exit']==0, log
    controls.append({'variant':'after','packages':safe,'exit':receipt['exit'],
                     'expected_success':True,'verified':True})
    (OUT/'floor-controls.json').write_text(json.dumps(controls, indent=2)+'\n')
    after={str(p.relative_to(CANDIDATE)):digest(p)
           for p in CANDIDATE.rglob('Cargo.*') if p.is_file()}
    assert before==after, 'metadata command changed a candidate manifest or lock'
    (OUT/'resolution-summary.json').write_text(json.dumps(
        {'commands':RESULTS,'floor_controls':controls,'candidate_manifests_locks_unchanged':True,
         'no_build_target_created':not (OUT/'no-build-target').exists(),
         'scope':'No compilation, runtime, cargo-audit or cargo-deny performed by this runner.'},indent=2)+'\n')

if __name__=='__main__':
    main()

#!/usr/bin/env python3
"""Build against the existing independently pinned KL04-03 native installation."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def command(args, env=None):
    return subprocess.check_output(args, text=True, env=env).strip()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--native-build', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--target-dir', type=Path, required=True)
    p.add_argument('--toolchain', default='stable')
    p.add_argument('--check', action='store_true', help='Also test, format, lint, and document the isolated crate')
    a = p.parse_args()
    native = a.native_build.resolve(); out = a.out.resolve(); target = a.target_dir.resolve()
    pin = json.loads((HERE/'source-pin.json').read_text())
    manifest = json.loads((native/'build-manifest.json').read_text())
    library = native/'lib/librdkafka.so.1'
    if (manifest['commit'] != pin['native']['commit'] or manifest['library_sha256'] != sha(library)
            or command(['git','-C',str(native/'source'),'rev-parse','HEAD']) != pin['native']['commit']
            or command(['git','-C',str(native/'source'),'rev-parse','HEAD^{tree}']) != pin['native']['tree']):
        raise ValueError('native source/library differs from the pinned KL04-03 build')
    if ' ' in str(out):
        raise ValueError('native verification prefix must have no spaces')
    out.mkdir(parents=True, exist_ok=False)
    (out/'lib/pkgconfig').mkdir(parents=True)
    shutil.copy2(library, out/'lib/librdkafka.so.1')
    (out/'lib/librdkafka.so').symlink_to('librdkafka.so.1')
    (out/'lib/pkgconfig/rdkafka.pc').write_text(
        f'prefix={out}\nlibdir=${{prefix}}/lib\nincludedir={native}/source/src\n\n'
        'Name: librdkafka\nDescription: Pinned benchmark native library\nVersion: 2.15.0\n'
        'Libs: -L${libdir} -lrdkafka\nCflags: -I${includedir}\n')
    env = os.environ.copy()
    env.pop('LD_PRELOAD', None); env.pop('LD_LIBRARY_PATH', None)
    env.update(PKG_CONFIG_PATH=str(out/'lib/pkgconfig'), CARGO_TARGET_DIR=str(target),
               CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1',
               RUSTFLAGS=f'-C link-arg=-Wl,-rpath,$ORIGIN/lib -C link-arg=-Wl,-rpath,{native}/lib')
    cargo = ['cargo', '+'+a.toolchain]
    commands = [cargo+['build','--locked','--release','--manifest-path',str(HERE/'Cargo.toml')]]
    if a.check:
        commands += [cargo+['test','--locked','--manifest-path',str(HERE/'Cargo.toml')],
                     cargo+['fmt','--manifest-path',str(HERE/'Cargo.toml'),'--','--check'],
                     cargo+['clippy','--locked','--manifest-path',str(HERE/'Cargo.toml'),'--all-targets','--','-D','warnings'],
                     cargo+['doc','--locked','--manifest-path',str(HERE/'Cargo.toml'),'--no-deps']]
    statuses = []
    for index, cmd in enumerate(commands):
        with (out/f'command-{index:02d}.log').open('x') as log:
            result = subprocess.run(cmd, env=dict(env, RUSTDOCFLAGS='-D warnings'), stdout=log, stderr=subprocess.STDOUT)
        statuses.append(dict(command=cmd, exit_code=result.returncode, log=f'command-{index:02d}.log'))
        (out/'commands.json').write_text(json.dumps(statuses, indent=2)+'\n')
        if result.returncode:
            raise RuntimeError(f'build/check failed; retained {out}/command-{index:02d}.log')
    binary = out/'rust-peer'
    shutil.copy2(target/'release/partitionline-rust-rdkafka-peer', binary)
    sources = {str(p.relative_to(HERE)):sha(p) for p in sorted(HERE.rglob('*'))
               if p.is_file() and '__pycache__' not in p.parts and p.name != 'build-manifest.json'}
    result = dict(wrapper_version=pin['wrapper']['version'], binding_version=pin['bindings']['version'],
                  native_version=pin['native']['version'], commit=pin['native']['commit'],
                  library_sha256=sha(out/'lib/librdkafka.so.1'), binary_sha256=sha(binary),
                  native_build_manifest=manifest, source_sha256=sources, lock_sha256=sha(HERE/'Cargo.lock'),
                  shared_helpers_sha256={'benchmarks/peers/librdkafka/run.py':sha(HERE.parent/'librdkafka/run.py'),
                                         'scripts/check-record-history.py':sha(ROOT/'scripts/check-record-history.py')},
                  compiler=command(['rustc','+'+a.toolchain,'--version']),
                  build_tool=command(cargo+['--version']), build_environment={k:env[k] for k in ('PKG_CONFIG_PATH','RUSTFLAGS','CARGO_INCREMENTAL','CARGO_BUILD_JOBS')})
    (out/'build-manifest.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(dict(binary=str(binary),native_version='2.15.0',library_sha256=result['library_sha256'])))

if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f'rust peer build: {error}',file=sys.stderr); sys.exit(2)

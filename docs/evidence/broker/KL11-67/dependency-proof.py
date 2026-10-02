#!/usr/bin/env python3
"""Pin codec graph/source audit and unchanged inactive/default/client boundaries."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tomllib


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('assertions must be enabled')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    args = parser.parse_args()
    repo, source, baseline = args.repo.resolve(), args.source.resolve(), args.baseline.resolve()
    base_sha = '37d72a18eef933f9005b9f7453763945882f104e'
    if not baseline.exists():
        baseline.mkdir(parents=True)
        archive = subprocess.run(['git', 'archive', base_sha], cwd=repo,
                                 capture_output=True, check=True).stdout
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(baseline, filter='data')
    env = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               PATH='/workspace/work/cargo/bin:' + os.environ['PATH'])
    commands = []
    active_graphs = {}

    def metadata(root, codecs=False, client=False):
        manifest = root / ('Cargo.toml' if client else 'partitionline-broker/Cargo.toml')
        command = ['taskset', '-c', '0-2,4', 'cargo', '+1.85.0', 'metadata', '--locked', '--offline',
                   '--format-version', '1', '--manifest-path', str(manifest)]
        if codecs:
            command += ['--features', 'codecs']
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=True, timeout=60)
        commands.append(dict(command=command, exit_code=result.returncode))
        metadata_result = json.loads(result.stdout)
        # Cargo metadata overapproximates weak optional dependency edges (e.g.
        # flate2's zlib-rs?/std); Cargo tree gives the actually activated graph.
        command = ['taskset', '-c', '0-2,4', 'cargo', '+1.85.0', 'tree', '--locked', '--offline',
                   '--manifest-path', str(manifest), '--target', 'x86_64-unknown-linux-gnu',
                   '--edges', 'normal,build', '--prefix', 'none', '--no-dedupe', '--format', '{p}|{f}']
        if codecs:
            command += ['--features', 'codecs']
        result = subprocess.run(command, env=env, capture_output=True, text=True, check=True, timeout=60)
        commands.append(dict(command=command, exit_code=result.returncode, stdout=result.stdout))
        active = set()
        for line in result.stdout.splitlines():
            if '|' not in line:
                continue
            package, features = line.split('|', 1)
            match = re.match(r'(\S+) v(\S+)', package)
            assert match, line
            active.add((match[1], match[2], tuple(sorted(filter(None, features.split(','))))))
        active_graphs[id(metadata_result)] = sorted(active)
        return metadata_result

    def graph(m):
        return active_graphs[id(m)]

    old_default, new_default = metadata(baseline), metadata(source)
    assert graph(old_default) == graph(new_default), 'broker default active graph changed'
    old_client, new_client = metadata(baseline, client=True), metadata(source, client=True)
    assert graph(old_client) == graph(new_client), 'published client active graph changed'
    assert (baseline / 'Cargo.lock').read_bytes() == (source / 'Cargo.lock').read_bytes()
    original = tomllib.loads((baseline / 'partitionline-broker/Cargo.lock').read_text())
    changed = tomllib.loads((source / 'partitionline-broker/Cargo.lock').read_text())
    new_versions = {(p['name'], p['version']) for p in changed['package']}
    assert all((p['name'], p['version']) in new_versions for p in original['package'])
    enabled = metadata(source, codecs=True)
    active = graph(enabled)
    names = {name for name, _, _ in active}
    assert {'flate2', 'lz4_flex', 'snap', 'zstd-rs'} <= names
    assert not names & {'zstd-sys', 'zstd-safe', 'zlib-rs', 'libz-sys', 'openssl-sys', 'lz4-sys', 'ring'}
    assert not {name for name, _, _ in graph(new_default)} & {'flate2', 'lz4_flex', 'snap', 'zstd-rs'}
    packages = {p['id']: p for p in enabled['packages']}
    lock = {(p['name'], p['version']): p.get('checksum') for p in changed['package']}
    details = []
    active_versions = {(name, version) for name, version, _ in active}
    for node in enabled['resolve']['nodes']:
        p = packages[node['id']]
        if (p['name'], p['version']) not in active_versions:
            continue
        details.append(dict(name=p['name'], version=p['version'], license=p['license'],
                            declared_msrv=p['rust_version'], features=node['features'],
                            checksum=lock[p['name'], p['version']]))
    zstd = next(p for p in packages.values() if p['name'] == 'zstd-rs')
    zroot = Path(zstd['manifest_path']).parent
    crate = Path('/workspace/work/cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/zstd-rs-0.1.0.crate')
    assert sha(crate) == '41514ccc30389f95bb9e6ab6d5634f17121c7b4fead6722a5fceec1c7891d78f'
    vcs = json.loads((zroot / '.cargo_vcs_info.json').read_text())
    assert vcs['git']['sha1'] == 'bac4c37d86e5307537c145823a106f7ed8f386d7'
    assert not any(d['kind'] != 'dev' for d in zstd['dependencies'])
    assert '#![forbid(unsafe_code)]' in (zroot / 'src/lib.rs').read_text()
    backend_sources = {}
    backend_archives = {}
    for name in ['flate2', 'miniz_oxide', 'lz4_flex', 'snap', 'zstd-rs']:
        p = next(p for p in packages.values() if p['name'] == name)
        root = Path(p['manifest_path']).parent
        selected = [root / 'Cargo.toml', root / '.cargo_vcs_info.json'] + sorted((root / 'src').rglob('*.rs'))
        backend_sources[name + '-' + p['version']] = {str(f.relative_to(root)): sha(f) for f in selected if f.exists()}
        archive = crate.parent / (name + '-' + p['version'] + '.crate')
        assert archive.stat().st_size <= 8 * 1024 * 1024
        assert sha(archive) == lock[name, p['version']]
        with tarfile.open(archive) as tar:
            for relative, digest in backend_sources[name + '-' + p['version']].items():
                member = tar.getmember(name + '-' + p['version'] + '/' + relative)
                assert member.size <= 2 * 1024 * 1024
                data = tar.extractfile(member).read()
                assert hashlib.sha256(data).hexdigest() == digest
        backend_archives[name + '-' + p['version']] = dict(checksum=sha(archive),
            inspected_source_files_match_locked_archive=True)
    result = dict(source_sha=args.source_sha, tested_base_sha=base_sha,
                  broker_default_active_graph_identical=True, client_active_graph_identical=True,
                  client_lock_byte_identical=True, original_broker_package_versions_preserved=True,
                  codec_packages_absent_from_broker_default=True, native_codec_packages_absent=True,
                  default_active_graph=graph(new_default), codec_active_graph=active,
                  added_active_packages=sorted(names - {name for name, _, _ in graph(new_default)}),
                  packages=sorted(details, key=lambda p: (p['name'], p['version'])),
                  graph_scope='Active normal/build graph on x86_64-unknown-linux-gnu from Cargo tree; metadata weak optional edges can overapproximate activation.',
                  metadata_only_packages=sorted((p['name'], p['version']) for p in packages.values()
                      if (p['name'], p['version']) not in active_versions),
                  vetted_zstd=dict(version='0.1.0', checksum=sha(crate), vcs=vcs,
                                  no_runtime_or_build_dependencies=True, unsafe_code_forbidden=True),
                  pinned_backend_sources_sha256=backend_sources, commands=commands,
                  pinned_backend_archive_verification=backend_archives,
                  scratch_accounting=dict(gzip_bytes=512 * 1024, snappy_bytes=0,
                       lz4='3 * advertised block size + 65536; source plus worst linked-block destination',
                       zstd_bytes=1024 * 1024,
                       output='min(sum(compressed decoded-batch ceiling or exact uncompressed lengths), normalized ceiling) + 32-byte zstd slack',
                       concurrency='backend contexts run sequentially; output plus maximum admitted scratch checked before context creation',
                       limit='requested/reported Vec capacity plus conservative pinned source scratch; not allocator metadata, stack, caller-retained buffers or RSS'))
    (repo / 'docs/evidence/broker/KL11-67/dependency-proof.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result[k] for k in ['source_sha', 'broker_default_active_graph_identical',
          'client_active_graph_identical', 'native_codec_packages_absent', 'added_active_packages']}, indent=2))


if __name__ == '__main__':
    main()

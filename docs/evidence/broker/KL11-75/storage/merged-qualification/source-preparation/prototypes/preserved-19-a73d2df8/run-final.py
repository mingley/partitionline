#!/usr/bin/env python3
"""Qualify a complete immutable Git snapshot; keep every gate/source receipt."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import time

P = argparse.ArgumentParser()
P.add_argument('--source', required=True)
P.add_argument('--repository', default='/workspace/partitionline')
P.add_argument('--scratch', required=True)
P.add_argument('--prepare-only', action='store_true')
P.add_argument('--reuse-source', help='Reuse an existing complete immutable source tree, without copying or extracting')
P.add_argument('--source-provenance', help='Optional existing source-archive receipt to bind by hash')
A = P.parse_args()
DRIVER_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
os.sched_setaffinity(0, {0, 1, 2, 4})
REPO = Path(A.repository)
OUT = Path(A.scratch)
OUT.mkdir(parents=True, exist_ok=False)
TREE = Path(A.reuse_source).resolve() if A.reuse_source else OUT / 'source'
if A.reuse_source:
    assert TREE.is_dir(), ('reused source tree absent', TREE)
else:
    TREE.mkdir()
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + os.environ['PATH'], CARGO_INCREMENTAL='0',
           CARGO_BUILD_JOBS='1', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments')
for key in ('PARTITIONLINE_METADATA_LIVE_PORT', 'PARTITIONLINE_PRODUCE_LIVE_PORT',
            'PARTITIONLINE_FETCH_LIVE_PORT', 'PARTITIONLINE_RETENTION_LIVE_PORT'):
    ENV.pop(key, None)
sha = subprocess.check_output(['git', 'rev-parse', A.source + '^{commit}'], cwd=REPO, text=True).strip()
archive_sha256 = None
if not A.reuse_source:
    archive_hash = hashlib.sha256()
    archive_process = subprocess.Popen(['git', 'archive', '--format=tar', sha], cwd=REPO, stdout=subprocess.PIPE)
    class ArchiveReader:
        def read(self, size=-1):
            chunk = archive_process.stdout.read(size)
            archive_hash.update(chunk)
            return chunk
    reader = ArchiveReader()
    with tarfile.open(fileobj=reader, mode='r|') as tar:
        tar.extractall(TREE, filter='data')
    while reader.read(65536):
        pass
    archive_process.stdout.close()
    assert archive_process.wait() == 0
    archive_sha256 = archive_hash.hexdigest()
expected = {}
raw = subprocess.check_output(['git', 'ls-tree', '-r', '-z', sha], cwd=REPO)
for item in raw.split(b'\0'):
    if not item:
        continue
    meta, name = item.split(b'\t', 1)
    mode, kind, blob = meta.decode().split()
    assert kind == 'blob', (mode, kind, name)
    path = name.decode()
    file = TREE / path
    data = os.readlink(file).encode() if mode == '120000' else file.read_bytes()
    actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
    assert actual == blob, path
    expected[path] = {'mode': mode, 'git_blob_sha1': blob, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
(OUT / 'source-integrity.json').write_text(json.dumps({'source_commit': sha, 'file_count': len(expected), 'files': expected}, indent=2) + '\n')

def verify():
    actual_paths = {str(p.relative_to(TREE)) for p in TREE.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual_paths == set(expected), {'missing': sorted(set(expected) - actual_paths), 'extra': sorted(actual_paths - set(expected))}
    hashes = {}
    for name, pin in expected.items():
        path = TREE / name
        data = os.readlink(path).encode() if pin['mode'] == '120000' else path.read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert blob == pin['git_blob_sha1'], name
        actual_mode = '120000' if path.is_symlink() else ('100755' if path.stat().st_mode & 0o111 else '100644')
        assert actual_mode == pin['mode'], (name, actual_mode, pin['mode'])
        hashes[name] = {'blob': blob, 'mode': actual_mode}
    encoded = json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()
    return {'file_count': len(hashes), 'all_git_blobs_match': True, 'set_and_blob_sha256': hashlib.sha256(encoded).hexdigest()}

receipt = {'schema': 1, 'source_commit': sha, 'archive_sha256': archive_sha256,
           'source_manifest_sha256': hashlib.sha256((OUT / 'source-integrity.json').read_bytes()).hexdigest(),
           'scope': 'complete immutable source; ordinary compaction/retention/rolling serving and preserved snapshot/replication tests; process/IO histories, not physical power loss or full Kafka cleaner qualification',
           'environment': {k: ENV[k] for k in ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG')},
           'commands': [], 'toolchains': {}, 'retained_binaries': [],
           'driver_sha256': DRIVER_SHA256}
receipt['source_tree'] = str(TREE)
receipt['source_materialization'] = 'reused existing complete tree; no copy or extraction' if A.reuse_source else 'fresh streamed git archive'
if A.source_provenance:
    provenance = Path(A.source_provenance).resolve()
    assert provenance.is_file()
    receipt['source_origin_receipt'] = {'path': str(provenance), 'sha256': hashlib.sha256(provenance.read_bytes()).hexdigest()}

def cache_owners():
    """Record cache references without dumping process environments."""
    target = str(Path(ENV['CARGO_TARGET_DIR']).resolve())
    owners = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            args = (entry / 'cmdline').read_bytes().split(b'\0')
            exe = os.readlink(entry / 'exe').removesuffix(' (deleted)')
            references = [a.decode(errors='replace') for a in args
                          if a == target.encode() or a.startswith((target + '/').encode())]
            if exe == target or exe.startswith(target + '/'):
                references.append(exe)
            cwd = os.readlink(entry / 'cwd').removesuffix(' (deleted)')
            if cwd == target or cwd.startswith(target + '/'):
                references.append('cwd:' + cwd)
            compiler = Path(exe).name in ('cargo', 'rustc', 'rustdoc', 'clippy-driver')
            if compiler and b'CARGO_TARGET_DIR=' + target.encode() in (entry / 'environ').read_bytes().split(b'\0'):
                references.append('CARGO_TARGET_DIR=' + target)
            for fd in (entry / 'fd').iterdir():
                try:
                    name = os.readlink(fd).removesuffix(' (deleted)')
                    if name == target or name.startswith(target + '/'):
                        references.append(name)
                except OSError:
                    pass
            if references:
                owners.append({'pid': int(entry.name), 'executable_name': Path(exe).name,
                               'cache_references': sorted(set(references))})
        except (OSError, ProcessLookupError):
            continue
    return owners

elf_objects = {}
elf_snapshots = {}

def sha256_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        while chunk := source.read(1_048_576):
            value.update(chunk)
    return value.hexdigest()

def elf_identity(stat):
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns,
            stat.st_ctime_ns, stat.st_mode & 0o7777)

def retain_elf(path, reason, command_name, force_verify=False):
    """Preserve exact bytes before another command can overwrite this path."""
    path = Path(path)
    with path.open('rb') as source:
        if source.read(4) != b'\x7fELF':
            return None
    before = path.stat()
    identity = elf_identity(before)
    old = elf_snapshots.get(str(path))
    if old and old['identity'] == identity:
        digest = old['sha256']
        if force_verify:
            assert sha256_file(path) == digest, ('unchanged-stat ELF bytes differ', path)
    else:
        directory = OUT / 'bin' / 'elf-objects'
        directory.mkdir(parents=True, exist_ok=True)
        temporary = directory / ('.retaining-' + str(os.getpid()) + '.gz')
        digest_state = hashlib.sha256()
        with path.open('rb') as source, temporary.open('wb') as target:
            with gzip.GzipFile(fileobj=target, mode='wb', mtime=0) as encoded:
                while chunk := source.read(1_048_576):
                    digest_state.update(chunk)
                    encoded.write(chunk)
        after = path.stat()
        assert identity == elf_identity(after), ('ELF changed during retention', path)
        digest = digest_state.hexdigest()
        destination = directory / (digest + '.gz')
        if destination.exists():
            temporary.unlink()
        else:
            os.replace(temporary, destination)
        restored = hashlib.sha256()
        with gzip.open(destination, 'rb') as decoded:
            while chunk := decoded.read(1_048_576):
                restored.update(chunk)
        assert restored.hexdigest() == digest, ('ELF decompression mismatch', path)
        elf_objects[digest] = {'path': str(destination.relative_to(OUT)),
                               'sha256': sha256_file(destination),
                               'uncompressed_sha256': digest, 'uncompressed_bytes': before.st_size,
                               'compression': 'gzip', 'decompression_verified': True}
        elf_snapshots[str(path)] = {'identity': identity, 'sha256': digest}
    if force_verify:
        destination = OUT / elf_objects[digest]['path']
        restored = hashlib.sha256()
        restored_bytes = 0
        with gzip.open(destination, 'rb') as decoded:
            while chunk := decoded.read(1_048_576):
                restored.update(chunk)
                restored_bytes += len(chunk)
        assert restored.hexdigest() == digest and restored_bytes == before.st_size, ('pre-clean ELF object differs', path)
        assert identity == elf_identity(path.stat()), ('ELF changed during pre-clean verification', path)
    return {'original_path': str(path), 'sha256': digest,
            'original_mode': before.st_mode & 0o7777, 'bytes': before.st_size,
            'retained_object': elf_objects[digest]['path'], 'reason': reason,
            'command': command_name, 'pre_clean_bytes_and_gzip_verified': force_verify}

def retain_cache(command_name, reason, force_verify=False):
    target = Path(ENV['CARGO_TARGET_DIR'])
    records = []
    if target.exists():
        for path in sorted(target.rglob('*')):
            if path.is_file() and not path.is_symlink():
                record = retain_elf(path, reason, command_name, force_verify)
                if record:
                    records.append(record)
    receipt['retained_elf_objects'] = list(elf_objects.values())
    return records

def executed_elfs(log, command_name):
    # Cargo names the exact harness it executes, including a failed harness.
    # The cache snapshot additionally preserves build scripts, shared objects
    # and any compiled target omitted from a failed run's execution list.
    records = []
    for name in sorted(set(re.findall(r'Running [^\n]*\(([^)]+)\)', log))):
        path = Path(name)
        if not path.is_absolute():
            path = TREE / path
        assert path.is_file(), ('executed ELF missing before retention', path)
        record = retain_elf(path, 'Cargo-reported execution, success or failure', command_name)
        assert record is not None, ('Cargo execution was not ELF', path)
        records.append(record)
    return records
if A.prepare_only:
    receipt['preparation_only'] = True
    receipt['final_source'] = verify()
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print('complete source prepared', len(expected), flush=True)
    raise SystemExit(0)
for tc in ('stable', '1.85.0'):
    receipt['toolchains'][tc] = subprocess.check_output(['rustc', '+' + tc, '-Vv'], env=ENV, text=True)
receipt['elf_retention_scope'] = 'every Cargo-reported harness, including failed tests and same-harness fault children, and all target ELF artifacts; OpenSSL separately retained; compiler/runtime prerequisites bound by toolchain provenance'
receipt['external_test_tools'] = []
openssl_name = shutil.which('openssl', path=ENV['PATH'])
if openssl_name:
    openssl = Path(openssl_name).resolve()
    retained = retain_elf(openssl, 'external TLS test prerequisite before any tests', 'openssl-prerequisite')
    assert retained is not None, ('OpenSSL prerequisite is not ELF', openssl)
    version_argv = [str(openssl), 'version', '-a']
    version = subprocess.check_output(version_argv, env=ENV, text=True)
    receipt['external_test_tools'].append({**retained, 'resolved_path': str(openssl),
                                           'version_command': version_argv, 'version': version})
    receipt['retained_elf_objects'] = list(elf_objects.values())

def verify_external_tools():
    checks = []
    for tool in receipt['external_test_tools']:
        path = Path(tool['resolved_path'])
        assert Path(shutil.which('openssl', path=ENV['PATH'])).resolve() == path
        assert sha256_file(path) == tool['sha256']
        assert path.stat().st_mode & 0o7777 == tool['original_mode']
        checks.append({'path': str(path), 'sha256': tool['sha256'], 'full_permission_mode': tool['original_mode']})
    return checks

commands = [('clean-before-source-switch', ['cargo', '+stable', 'clean', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR'], '--package', 'partitionline-broker'], {})]
commands += [('format', ['cargo', '+stable', 'fmt', '--manifest-path', 'partitionline-broker/Cargo.toml', '--check'], {})]
for tc in ('stable', '1.85.0'):
    if tc == '1.85.0':
        commands.append(('clean-between-toolchains', ['cargo', '+stable', 'clean', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR'], '--package', 'partitionline-broker'], {}))
    prefix = ['cargo', '+' + tc]
    manifest = ['--locked', '--manifest-path', 'partitionline-broker/Cargo.toml']
    for feature, flags in (('default', []), ('all-features', ['--all-features'])):
        commands += [
            (tc + '-' + feature + '-all-targets', [*prefix, 'test', *manifest, *flags, '--all-targets', '--', '--test-threads=1', '--nocapture'], {'capture': True}),
            (tc + '-' + feature + '-strict-clippy', [*prefix, 'clippy', *manifest, *flags, '--all-targets', '--', '-D', 'warnings'], {}),
            (tc + '-' + feature + '-strict-doc', [*prefix, 'doc', *manifest, *flags, '--no-deps'], {'RUSTDOCFLAGS': '-D warnings'}),
            (tc + '-' + feature + '-strict-doctest', [*prefix, 'test', *manifest, *flags, '--doc'], {'RUSTDOCFLAGS': '-D warnings'})]
for name, argv, extra in commands:
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == DRIVER_SHA256
    before = verify()
    external_tools_before = verify_external_tools()
    free = os.statvfs(OUT).f_bavail * os.statvfs(OUT).f_frsize
    assert free >= 350 * 1024 * 1024, ('350MiB free-space guard', name, free)
    clean_guard = None
    if argv[2] == 'clean':
        owners_before = cache_owners()
        clean_guard = {'owners_before': owners_before, 'owners_after': None,
                       'retained_cache_elfs': [], 'clean_authorized': False}
        guard_path = OUT / (name + '-cache-guard.json')
        guard_path.write_text(json.dumps(clean_guard, indent=2) + '\n')
        if owners_before:
            receipt['pre_command_failure'] = {'command': name, 'reason': 'active cache owners before retention/clean',
                                              'guard': str(guard_path.relative_to(OUT))}
            (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
            raise SystemExit(1)
        try:
            records = retain_cache(name, 'cache ELF preserved before package clean', force_verify=True)
        except Exception as error:
            clean_guard['retention_error'] = repr(error)
            guard_path.write_text(json.dumps(clean_guard, indent=2) + '\n')
            receipt['pre_command_failure'] = {'command': name, 'reason': 'cache retention failed; clean skipped',
                                              'error': repr(error), 'guard': str(guard_path.relative_to(OUT))}
            receipt['retained_elf_objects'] = list(elf_objects.values())
            (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
            raise
        owners_after = cache_owners()
        clean_guard.update(owners_after=owners_after, retained_cache_elfs=records,
                           clean_authorized=not owners_after)
        guard_path.write_text(json.dumps(clean_guard, indent=2) + '\n')
        if owners_after:
            receipt['pre_command_failure'] = {'command': name, 'reason': 'active cache owners after retention/before clean',
                                              'guard': str(guard_path.relative_to(OUT))}
            (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
            raise SystemExit(1)
    env = dict(ENV)
    env.update({k: v for k, v in extra.items() if k != 'capture'})
    lane = OUT / name
    lane.mkdir()
    if extra.get('capture'):
        cell = OUT / name.removesuffix('-all-targets')
        cell.mkdir()
        mappings = {'PARTITIONLINE_WIRE_REPORT': 'wire-report.json', 'PARTITIONLINE_METADATA_REPORT': 'metadata-report.json',
                    'PARTITIONLINE_PRODUCE_REPORT': 'produce-report.json', 'PARTITIONLINE_FETCH_REPORT': 'fetch-report.json',
                    'PARTITIONLINE_SEGMENTS_REPORT': 'rolling-report.json', 'PARTITIONLINE_SEGMENTS_ORACLE_REPORT': 'apache-router-report.json',
                    'PARTITIONLINE_SEGMENTS_RESPONSE_DIR': 'responses', 'PARTITIONLINE_SEGMENTS_PROOF_DIR': 'proof',
                    'PARTITIONLINE_SEGMENTS_FAULT_DIR': 'rolling-fault-histories',
                    'PL_COMPACTION_FAULT_DIR': 'compaction-fault-histories',
                    'PARTITIONLINE_COMPACTION_CORPUS_DIR': 'compaction-corpus',
                    'PARTITIONLINE_RETENTION_FAULT_DIR': 'retention-fault-histories',
                    'PARTITIONLINE_RETENTION_REPORT': 'retention-report.json',
                    'PARTITIONLINE_FETCH_RESPONSE_DIR': 'fetch-responses',
                    'PL_BROKER_CONTROLLER_REPORT': 'controller-report.json',
                    'PL_CONTROLLER_RESPONSE_DIR': 'controller-responses',
                    'PL_SNAPSHOT_RESPONSE_DIR': 'snapshot',
                    'PL_SNAPSHOT_NODE_INNER_PROOF_DIR': 'snapshot-inner',
                    'PL_REPLICATION_RESPONSE_DIR': 'replication'}
        env.update({k: str(cell / v) for k, v in mappings.items()})
        env.update(PL_SNAPSHOT_SOURCE_SHA=sha, PL_REPLICATION_SOURCE_SHA=sha)
    started = time.time()
    with (lane / 'command.log').open('w') as log:
        code = subprocess.call(['taskset', '-c', '0-2,4', *argv], cwd=TREE, env=env, stdout=log, stderr=subprocess.STDOUT)
    output = (lane / 'command.log').read_text()
    item = {'name': name, 'argv': ['taskset', '-c', '0-2,4', *argv], 'environment_additions': {k: v for k, v in env.items() if ENV.get(k) != v},
            'exit_code': code, 'elapsed_seconds': round(time.time() - started, 3), 'source_before': before,
            'external_test_tools_before': external_tools_before,
            'log_sha256': sha256_file(lane / 'command.log')}
    receipt['commands'].append(item)
    try:
        # Do this on both success and failure, before source verification or any
        # later Cargo command can replace a harness, library or build script.
        item['executed_elfs'] = executed_elfs(output, name)
        item['post_command_cache_elfs'] = retain_cache(name, 'immediate post-command cache snapshot before any later overwrite')
        item['source_after'] = verify()
        item['external_test_tools_after'] = verify_external_tools()
        assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == DRIVER_SHA256
    except Exception as error:
        item['post_command_error'] = repr(error)
        receipt['retained_elf_objects'] = list(elf_objects.values())
        (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
        raise
    if clean_guard is not None:
        item['cache_clean_guard'] = clean_guard
    if extra.get('capture'):
        item['capture_directory'] = str(cell.relative_to(OUT))
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    if code == 0 and extra.get('capture'):
        default = name.endswith('default-all-targets')
        toolchain = name.split('-')[0]
        for executable in ('fetch', 'retention'):
            matches = re.findall(r'Running tests/' + executable + r'.rs \(([^)]+)\)', output)
            assert len(matches) == 1, (executable, matches)
            binary = Path(matches[0])
            if not binary.is_absolute():
                binary = TREE / binary
            label = executable if default else executable + '-all-features'
            destination = OUT / 'bin' / toolchain / label
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(binary, destination)
            original_mode = binary.stat().st_mode & 0o7777
            raw_hash = sha256_file(binary)
            assert sha256_file(destination) == raw_hash and destination.stat().st_mode & 0o7777 == original_mode
            receipt['retained_binaries'].append({'lane': name, 'path': str(destination.relative_to(OUT)), 'sha256': raw_hash, 'source_commit': sha, 'build_command': item['argv'], 'toolchain': receipt['toolchains'][toolchain], 'original_mode': original_mode, 'copied_bytes_and_mode_verified': True})
        for label, pattern in (
                ('lib-unit', r'Running unittests src/lib.rs \(([^)]+)\)'),
                ('raft-replication', r'Running tests/raft_replication.rs \(([^)]+)\)'),
                ('raft-snapshot', r'Running tests/raft_snapshot.rs \(([^)]+)\)')):
            matches = re.findall(pattern, output)
            assert len(matches) == 1, (label, matches)
            binary = Path(matches[0])
            if not binary.is_absolute():
                binary = TREE / binary
            retained = retain_elf(binary, 'selected proof ELF', name)
            assert retained is not None
            destination = OUT / 'bin' / cell.name / 'proof' / (label + '.gz')
            destination.parent.mkdir(parents=True, exist_ok=True)
            object_path = OUT / retained['retained_object']
            os.link(object_path, destination)
            raw_hash = retained['sha256']
            receipt['retained_binaries'].append({
                'lane': name, 'path': str(destination.relative_to(OUT)), 'compression': 'gzip',
                'sha256': sha256_file(destination),
                'uncompressed_sha256': raw_hash, 'uncompressed_bytes': retained['bytes'],
                'original_mode': retained['original_mode'], 'decompression_verified': True,
                'same_object_hardlink': retained['retained_object'],
                'source_commit': sha, 'build_command': item['argv'], 'toolchain': receipt['toolchains'][toolchain]})
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(name, code, flush=True)
    if code:
        raise SystemExit(code)
assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == DRIVER_SHA256
receipt['final_source'] = verify()
(OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')

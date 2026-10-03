#!/usr/bin/env python3
"""Qualify six broker feature profiles on two compilers, with bounded disk use."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
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
P.add_argument('--cpus', default='2,4', help='Explicit CPU list/ranges; CPU3 is reserved')
A = P.parse_args()
def parse_cpus(value):
    selected = []
    for item in value.split(','):
        parts = item.split('-')
        assert all(part.isdigit() for part in parts) and len(parts) in (1, 2), ('invalid CPU affinity', value)
        start = int(parts[0])
        end = int(parts[-1])
        assert start <= end <= 4, ('invalid CPU range', value)
        selected.extend(range(start, end + 1))
    assert selected and len(selected) == len(set(selected)) and set(selected) <= {0, 1, 2, 4}, ('CPU3 reserved or duplicate CPUs', value)
    return tuple(sorted(selected))
CPUS = parse_cpus(A.cpus)
CPU_ARGUMENT = ','.join(str(cpu) for cpu in CPUS)
def interrupted(signum, _frame):
    raise KeyboardInterrupt('qualification runner received signal ' + str(signum))
signal.signal(signal.SIGTERM, interrupted)
DRIVER_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
os.sched_setaffinity(0, set(CPUS))
assert os.sched_getaffinity(0) == set(CPUS)
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
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments', CARGO_NET_OFFLINE='true')
cache_path = Path(ENV['CARGO_TARGET_DIR']).resolve()
for protected in (REPO.resolve(), OUT.resolve(), TREE.resolve()):
    assert not protected.is_relative_to(cache_path) and not cache_path.is_relative_to(protected), ('generated cache overlaps repository/source/proof', cache_path, protected)
PROFILES = (('default', []), ('tls', ['--no-default-features', '--features', 'tls']),
            ('sasl', ['--no-default-features', '--features', 'sasl']),
            ('sasl+tls', ['--no-default-features', '--features', 'sasl,tls']),
            ('oidc', ['--no-default-features', '--features', 'oidc']),
            ('all-features', ['--all-features']))
TOOLCHAINS = ('stable', '1.85.0')
DISK_FLOOR_BYTES = 350 * 1024 * 1024
DISK_POLL_SECONDS = 0.5
AUDIT_COMPRESSION_THRESHOLD = 4 * 1024 * 1024

def sha256_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        while chunk := source.read(1_048_576):
            value.update(chunk)
    return value.hexdigest()

def write_json_audit(path, value):
    """Losslessly retain large audit JSON with its raw hash and mode."""
    path = Path(path)
    with path.open('x') as output:
        for chunk in json.JSONEncoder(indent=2).iterencode(value):
            output.write(chunk)
        output.write('\n')
    before = path.stat()
    raw_hash = sha256_file(path)
    raw_mode = before.st_mode & 0o7777
    result = {'path': str(path.relative_to(OUT)), 'sha256': raw_hash,
              'uncompressed_sha256': raw_hash, 'uncompressed_bytes': before.st_size,
              'original_mode': raw_mode, 'restore_path': str(path.relative_to(OUT)),
              'compression': None}
    if before.st_size > AUDIT_COMPRESSION_THRESHOLD:
        destination = path.with_suffix(path.suffix + '.gz')
        with path.open('rb') as source, destination.open('xb') as target:
            with gzip.GzipFile(fileobj=target, mode='wb', mtime=0) as encoded:
                shutil.copyfileobj(source, encoded, length=1_048_576)
        restored = hashlib.sha256()
        restored_bytes = 0
        with gzip.open(destination, 'rb') as decoded:
            while chunk := decoded.read(1_048_576):
                restored.update(chunk)
                restored_bytes += len(chunk)
        assert restored.hexdigest() == raw_hash and restored_bytes == before.st_size
        assert sha256_file(path) == raw_hash and path.stat().st_mode & 0o7777 == raw_mode
        destination.chmod(raw_mode)
        assert destination.stat().st_mode & 0o7777 == raw_mode
        result.update(path=str(destination.relative_to(OUT)), sha256=sha256_file(destination),
                      compression='gzip', decompression_verified=True, compressed_mode=raw_mode)
        path.unlink()  # Only this freshly generated, verified audit is removed.
    return result
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
    expected[path] = {'mode': mode, 'git_blob_sha1': blob, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
                      'filesystem_permission_mode': file.lstat().st_mode & 0o7777}
source_manifest = write_json_audit(OUT / 'source-integrity.json', {'source_commit': sha, 'file_count': len(expected), 'files': expected})

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
        full_mode = path.lstat().st_mode & 0o7777
        assert full_mode == pin['filesystem_permission_mode'], (name, full_mode, pin['filesystem_permission_mode'])
        hashes[name] = {'blob': blob, 'mode': actual_mode, 'full_permission_mode': full_mode}
    encoded = json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()
    return {'file_count': len(hashes), 'all_git_blobs_match': True, 'all_baseline_permission_modes_match': True,
            'set_and_blob_sha256': hashlib.sha256(encoded).hexdigest()}

receipt = {'schema': 1, 'source_commit': sha, 'archive_sha256': archive_sha256,
           'source_manifest_sha256': source_manifest['uncompressed_sha256'], 'source_manifest': source_manifest,
           'scope': 'complete immutable source; default, TLS, SASL, SASL+TLS, OIDC and all-features profiles on stable and Rust1.85; ordinary compaction/retention/rolling serving and fixed/joint snapshot, membership and replication tests; process/IO histories, not physical power loss, full Kafka cleaner or general KRaft qualification',
           'environment': {k: ENV[k] for k in ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'CARGO_NET_OFFLINE')},
           'commands': [], 'maintenance_commands': [], 'execution_order': [], 'toolchains': {}, 'retained_binaries': [],
           'driver_sha256': DRIVER_SHA256}
receipt['cpu_affinity'] = {'requested': A.cpus, 'taskset_argument': CPU_ARGUMENT, 'actual_cpu_ids': sorted(os.sched_getaffinity(0))}
receipt['qualification_matrix'] = {'toolchains': list(TOOLCHAINS),
                                    'profiles': [{'name': name, 'cargo_flags': flags} for name, flags in PROFILES],
                                    'test_cells': 12, 'gates_per_cell': ['all-targets', 'strict-clippy', 'strict-doc', 'strict-doctest'],
                                    'behavior_lint_doc_gates': 48, 'format_gates': 1, 'package_clean_commands': 2,
                                    'expected_command_count': 51, 'dependency_flags': ['--offline', '--locked'],
                                    'maintenance_command_count': 10, 'expected_actual_command_count': 61,
                                    'format_network_policy': 'cargo fmt --check with CARGO_NET_OFFLINE=true; dependency flags apply to built-in Cargo commands',
                                    'peer_profiles': ['default', 'all-features']}
receipt['disk_policy'] = {'minimum_free_bytes': DISK_FLOOR_BYTES, 'poll_interval_seconds': DISK_POLL_SECONDS,
                           'action': 'terminate only isolated command process group at threshold, retain failed ELF/log receipt; never delete data automatically'}
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
            if b'CARGO_TARGET_DIR=' + target.encode() in (entry / 'environ').read_bytes().split(b'\0'):
                references.append('CARGO_TARGET_DIR=' + target)
            for row in (entry / 'maps').read_text().splitlines():
                columns = row.split(maxsplit=5)
                if len(columns) == 6:
                    mapped = columns[5].removesuffix(' (deleted)')
                    if mapped == target or mapped.startswith(target + '/'):
                        references.append('mapped:' + mapped)
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

def retention_forecast(probe=None):
    """Reserve a conservative gzip upper bound before retaining cache bytes."""
    probe = probe or (lambda: os.statvfs(OUT).f_bavail * os.statvfs(OUT).f_frsize)
    target = Path(ENV['CARGO_TARGET_DIR'])
    assert not target.is_symlink(), ('refuse symlinked generated-cache target', target)
    assert target.resolve() == Path('/workspace/work/target-broker-segments') or receipt.get('mock_helper_scope'), ('unexpected generated-cache ownership path', target)
    unknown = []
    total = 0
    if target.exists():
        for path in sorted(target.rglob('*')):
            if not path.is_file() or path.is_symlink():
                continue
            with path.open('rb') as source:
                if source.read(4) != b'\x7fELF':
                    continue
            stat = path.stat()
            old = elf_snapshots.get(str(path))
            if old and old['identity'] == elf_identity(stat):
                continue
            # Deflate stored-block overhead plus framing; no compression ratio
            # assumption. Cross-path duplicate bytes remain conservatively
            # charged until their digest is known.
            upper = stat.st_size + ((stat.st_size + 16382) // 16383) * 5 + 64
            total += upper
            unknown.append({'path': str(path), 'raw_bytes': stat.st_size, 'gzip_upper_bound_bytes': upper,
                            'full_permission_mode': stat.st_mode & 0o7777})
    free = probe()
    metadata_reserve = 1024 * 1024
    required = DISK_FLOOR_BYTES + metadata_reserve + total
    return {'sampled_free_bytes': free, 'disk_reserve_bytes': DISK_FLOOR_BYTES,
            'metadata_reserve_bytes': metadata_reserve, 'new_gzip_upper_bound_bytes': total,
            'required_free_bytes': required, 'sufficient': free >= required,
            'unretained_elfs': unknown, 'scope': 'before cache-retention output writes; no destructive cleanup on refusal'}

def retain_cache(command_name, reason, force_verify=False):
    target = Path(ENV['CARGO_TARGET_DIR'])
    forecast = retention_forecast()
    receipt.setdefault('retention_forecasts', []).append({'command': command_name, 'reason': reason, **forecast})
    assert forecast['sufficient'], ('compressed retention plus350MiB reserve forecast refused; keep originals in target', forecast)
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

def stop_command_group(process):
    """Stop only the session group created for this runner's command."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()

def monitored_command(argv, cwd, env, log, monitor_path, probe=None):
    """Stop an isolated command group before it consumes the retention reserve."""
    probe = probe or (lambda: os.statvfs(OUT).f_bavail * os.statvfs(OUT).f_frsize)
    minimum = probe()
    pre_launch_time = time.time()
    assert minimum >= DISK_FLOOR_BYTES, ('350MiB pre-command free-space guard', minimum)
    samples = 1
    trigger = None
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT,
                               start_new_session=True)
    try:
        with Path(monitor_path).open('w') as observations:
            observations.write(json.dumps({'utc_unix_seconds': pre_launch_time, 'free_bytes': minimum,
                                            'process_group': process.pid, 'below_reserve': False,
                                            'pre_launch': True}, sort_keys=True) + '\n')
            observations.flush()
            while True:
                free = probe()
                minimum = min(minimum, free)
                samples += 1
                observation = {'utc_unix_seconds': time.time(), 'free_bytes': free,
                               'process_group': process.pid, 'below_reserve': free < DISK_FLOOR_BYTES}
                observations.write(json.dumps(observation, sort_keys=True) + '\n')
                observations.flush()
                if free < DISK_FLOOR_BYTES:
                    trigger = observation
                    stop_command_group(process)
                    break
                try:
                    process.wait(timeout=DISK_POLL_SECONDS)
                    final_free = probe()
                    minimum = min(minimum, final_free)
                    samples += 1
                    final = {'utc_unix_seconds': time.time(), 'free_bytes': final_free,
                             'process_group': process.pid, 'below_reserve': final_free < DISK_FLOOR_BYTES,
                             'process_completed': True}
                    observations.write(json.dumps(final, sort_keys=True) + '\n')
                    observations.flush()
                    if final_free < DISK_FLOOR_BYTES:
                        trigger = final
                    break
                except subprocess.TimeoutExpired:
                    pass
    except BaseException:
        stop_command_group(process)
        raise
    code = process.returncode
    if trigger is not None and code == 0:
        code = 75
    return code, {'threshold_bytes': DISK_FLOOR_BYTES, 'poll_interval_seconds': DISK_POLL_SECONDS,
                  'minimum_free_bytes': minimum, 'samples': samples, 'triggered': trigger is not None,
                  'trigger': trigger, 'actual_process_exit_code': process.returncode,
                  'sample_log_sha256': sha256_file(monitor_path)}
if A.prepare_only:
    receipt['preparation_only'] = True
    receipt['final_source'] = verify()
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print('complete source prepared', len(expected), flush=True)
    raise SystemExit(0)
for tc in TOOLCHAINS:
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

commands = [('clean-before-source-switch', ['cargo', '+stable', 'clean', '--offline', '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR'], '--package', 'partitionline-broker'], {})]
commands += [('format', ['cargo', '+stable', 'fmt', '--manifest-path', 'partitionline-broker/Cargo.toml', '--check'], {})]
for tc in TOOLCHAINS:
    if tc == '1.85.0':
        commands.append(('clean-between-toolchains', ['cargo', '+stable', 'clean', '--offline', '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR'], '--package', 'partitionline-broker'], {}))
    prefix = ['cargo', '+' + tc]
    manifest = ['--offline', '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml']
    for feature, flags in PROFILES:
        commands += [
            (tc + '-' + feature + '-all-targets', [*prefix, 'test', *manifest, *flags, '--all-targets', '--', '--test-threads=1', '--nocapture'], {'capture': True}),
            (tc + '-' + feature + '-strict-clippy', [*prefix, 'clippy', *manifest, *flags, '--all-targets', '--', '-D', 'warnings'], {}),
            (tc + '-' + feature + '-strict-doc', [*prefix, 'doc', *manifest, *flags, '--no-deps'], {'RUSTDOCFLAGS': '-D warnings'}),
            (tc + '-' + feature + '-strict-doctest', [*prefix, 'test', *manifest, *flags, '--doc'], {'RUSTDOCFLAGS': '-D warnings'})]
assert len(commands) == receipt['qualification_matrix']['expected_command_count'] == 51
assert len({name for name, _, _ in commands}) == 51
schedule = []
maintenance_specs = []
for name, argv, extra in commands:
    schedule.append(('commands', name, argv, extra))
    if name.endswith('-strict-doctest'):
        tc = name.split('-')[0]
        profile = name.removeprefix(tc + '-').removesuffix('-strict-doctest')
        if profile != 'all-features':
            label = tc + '-maintenance-after-' + profile
            clean = ['cargo', '+' + tc, 'clean', '--offline', '--locked', '--manifest-path',
                     'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR']]
            maintenance_specs.append({'name': label, 'after_profile': profile, 'toolchain': tc,
                                       'argv': ['taskset', '-c', CPU_ARGUMENT, *clean],
                                       'operation': 'full-generated-cache-clean'})
            schedule.append(('maintenance_commands', label, clean, {}))
assert len(maintenance_specs) == 10 and len(schedule) == 61
receipt['qualification_matrix']['maintenance_operations'] = maintenance_specs
for ledger, name, argv, extra in schedule:
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == DRIVER_SHA256
    before = verify()
    external_tools_before = verify_external_tools()
    free = os.statvfs(OUT).f_bavail * os.statvfs(OUT).f_frsize
    if free < DISK_FLOOR_BYTES:
        receipt['pre_command_failure'] = {'command': name, 'reason': '350MiB free-space reserve; command not launched',
                                          'threshold_bytes': DISK_FLOOR_BYTES, 'sampled_free_bytes': free,
                                          'source_before': before}
        (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
        raise SystemExit(75)
    clean_guard = None
    if argv[2] == 'clean':
        forecast = retention_forecast()
        owners_before = cache_owners()
        clean_guard = {'owners_before': owners_before, 'owners_after': None,
                       'retained_cache_elfs': [], 'clean_authorized': False,
                       'retention_forecast': forecast, 'cache_path': str(Path(ENV['CARGO_TARGET_DIR']).resolve()),
                       'operation': 'full-generated-cache-clean' if ledger == 'maintenance_commands' else 'package-clean',
                       'process_guard_fields': ['cwd', 'exe', 'fd', 'CARGO_TARGET_DIR_environment', 'maps']}
        guard_path = OUT / (name + '-cache-guard.json')
        guard_path.write_text(json.dumps(clean_guard, indent=2) + '\n')
        if owners_before or not forecast['sufficient']:
            receipt['pre_command_failure'] = {'command': name, 'ledger': ledger,
                                              'reason': 'active cache owners or insufficient compressed-retention reserve; clean skipped',
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
    explicit_environment_keys = set(extra) - {'capture'}
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
                    'PL_REPLICATION_RESPONSE_DIR': 'replication',
                    'PL_MEMBERSHIP_CAPTURE_DIR': 'membership'}
        env.update({k: str(cell / v) for k, v in mappings.items()})
        env.update(PL_SNAPSHOT_SOURCE_SHA=sha, PL_REPLICATION_SOURCE_SHA=sha, PL_MEMBERSHIP_SOURCE_SHA=sha)
        explicit_environment_keys.update(mappings)
        explicit_environment_keys.update(('PL_SNAPSHOT_SOURCE_SHA', 'PL_REPLICATION_SOURCE_SHA', 'PL_MEMBERSHIP_SOURCE_SHA'))
    started = time.time()
    command_error = None
    disk_monitor = None
    with (lane / 'command.log').open('w') as log:
        try:
            code, disk_monitor = monitored_command(['taskset', '-c', CPU_ARGUMENT, *argv], TREE, env, log,
                                                    lane / 'disk-monitor.jsonl')
        except BaseException as error:
            # monitored_command first stops its isolated process group. Keep
            # the executed/compiled ELFs and log before exiting this runner.
            code = 75
            command_error = repr(error)
    output = (lane / 'command.log').read_text()
    item = {'name': name, 'argv': ['taskset', '-c', CPU_ARGUMENT, *argv], 'ledger': ledger,
            'cpu_affinity': receipt['cpu_affinity'], 'environment_additions': {k: v for k, v in env.items() if ENV.get(k) != v or k in explicit_environment_keys},
            'exit_code': code, 'elapsed_seconds': round(time.time() - started, 3), 'source_before': before,
            'external_test_tools_before': external_tools_before,
            'disk_monitor': disk_monitor,
            'log_sha256': sha256_file(lane / 'command.log')}
    receipt[ledger].append(item)
    receipt['execution_order'].append({'sequence': len(receipt['execution_order']) + 1, 'ledger': ledger, 'name': name})
    receipt['execution_counts'] = {'qualification_commands': len(receipt['commands']),
                                    'maintenance_commands': len(receipt['maintenance_commands']),
                                    'actual_commands': len(receipt['execution_order'])}
    if command_error is not None:
        item['command_interruption'] = command_error
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
        toolchain = name.split('-')[0]
        profile = name.removeprefix(toolchain + '-').removesuffix('-all-targets')
        # Actual public compaction/retention peers consume these four cells.
        # Every other profile's harness remains in the content-addressed gzip
        # cache proof, without duplicating direct executable disk space.
        for executable in ('fetch', 'retention') if profile in ('default', 'all-features') else ():
            matches = re.findall(r'Running tests/' + executable + r'.rs \(([^)]+)\)', output)
            assert len(matches) == 1, (executable, matches)
            binary = Path(matches[0])
            if not binary.is_absolute():
                binary = TREE / binary
            label = executable if profile == 'default' else executable + '-all-features'
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
                ('raft-snapshot', r'Running tests/raft_snapshot.rs \(([^)]+)\)'),
                ('raft-membership', r'Running tests/raft_membership.rs \(([^)]+)\)')):
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
assert len(receipt['commands']) == 51 and len(receipt['maintenance_commands']) == 10 and len(receipt['execution_order']) == 61
receipt['final_source'] = verify()
(OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')

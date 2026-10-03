def sha256_file(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        while chunk := source.read(1_048_576):
            value.update(chunk)
    return value.hexdigest()

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

def cache_owners():
    """Refuse extant unreadable processes; optional root-reviewed exact pin only."""
    from process_guards import bounded_docker_empty, cache_references
    pin_rows = ()
    proof = None
    ordinal = len(receipt.get('cache_process_inspections', []))
    guard_write()
    if A.platform_daemon_pin:
        pin_rows, proof = bounded_docker_empty(
            A.platform_daemon_pin, A.platform_daemon_sha256, OUT, ordinal)
    inspection = cache_references('/proc', ENV['CARGO_TARGET_DIR'], os.getpid(), pin_rows)
    inspection['actual_empty_docker_query'] = proof
    receipt.setdefault('cache_process_inspections', []).append(inspection)
    assert not inspection['live_inspection_faults'], (
        'unreadable or replaced live process refused; cache not safe to overwrite',
        inspection['live_inspection_faults'])
    return inspection['owners']

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
    from artifact_paths import harness_paths
    records = []
    for name in harness_paths(log, ENV['CARGO_TARGET_DIR'], TREE):
        path = Path(name)
        assert path.is_file(), ('executed test ELF missing before retention', path)
        record = retain_elf(path, 'exact Cargo test harness execution', command_name, force_verify=True)
        assert record is not None, ('test harness was not ELF', path)
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

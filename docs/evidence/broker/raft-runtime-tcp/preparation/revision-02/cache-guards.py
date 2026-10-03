def free_bytes():
    stat = os.statvfs(OUT)
    return stat.f_bavail * stat.f_frsize

def guard_write():
    assert free_bytes() >= DISK_FLOOR_BYTES, ('350MiB write guard', free_bytes())

def work_inodes():
    names = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=REPO)
    result = set()
    for name in names.split(b'\0'):
        if name:
            path = REPO / os.fsdecode(name)
            if path.exists() or path.is_symlink():
                stat = path.lstat()
                result.add((stat.st_dev, stat.st_ino))
    return result

def verify_rows(tree, pins, disjoint=True):
    actual_paths = {str(p.relative_to(tree)) for p in tree.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual_paths == set(pins), {'missing': sorted(set(pins) - actual_paths), 'extra': sorted(actual_paths - set(pins))}
    work = work_inodes() if disjoint else set()
    digest = hashlib.sha256()
    total = 0
    for name, pin in sorted(pins.items()):
        path = tree / name
        before = path.lstat()
        data = os.readlink(path).encode() if pin['mode'] == '120000' else path.read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        mode = '120000' if path.is_symlink() else ('100755' if before.st_mode & 0o111 else '100644')
        fullmode = before.st_mode & 0o7777
        assert blob == pin['git_blob_sha1'] and hashlib.sha256(data).hexdigest() == pin['sha256'], name
        assert mode == pin['mode'] and fullmode == pin['full_permission_mode'] and len(data) == pin['bytes'], name
        after = path.lstat()
        assert elf_identity(before) == elf_identity(after), ('source changed during read', name)
        assert (before.st_dev, before.st_ino) not in work, ('source shares repository WORK inode', name)
        digest.update(json.dumps([name, blob, pin['sha256'], mode, fullmode, len(data)], separators=(',', ':')).encode())
        digest.update(b'\n')
        total += len(data)
    return {'files': len(pins), 'bytes': total, 'exact_path_blob_sha256_and_full07777_match': True,
            'repository_tracked_untracked_inodes_disjoint': disjoint, 'audit_sha256': digest.hexdigest()}

def cache_map():
    rows = {}
    for path in sorted(CACHE.rglob('*')):
        stat = path.lstat()
        name = str(path.relative_to(CACHE))
        assert not path.is_symlink(), ('special cache path refused before cleanup', path)
        if path.is_dir():
            rows[name] = {'type': 'directory', 'full_mode': stat.st_mode & 0o7777}
        else:
            assert path.is_file(), ('special cache path refused', path)
            rows[name] = {'type': 'file', 'bytes': stat.st_size, 'sha256': sha256_file(path),
                          'full_mode': stat.st_mode & 0o7777}
            assert elf_identity(stat) == elf_identity(path.stat()), ('cache changed during map', path)
    return rows

def combined_forecast():
    files = []; directories = []
    retained = reference_cache_map
    for path in sorted(CACHE.rglob('*')):
        assert not path.is_symlink()
        if path.is_file():
            name = str(path.relative_to(CACHE)); stat = path.stat()
            pin = {'type': 'file', 'bytes': stat.st_size, 'sha256': sha256_file(path), 'full_mode': stat.st_mode & 0o7777}
            if retained.get(name) != pin: files.append(stat.st_size)
        elif path.is_dir():
            name = str(path.relative_to(CACHE))
            if retained.get(name) != {'type':'directory','full_mode':path.stat().st_mode & 0o7777}: directories.append(path)
        else: raise AssertionError(('special cache path', path))
    tar_upper = sum(files) + sum(512 + (-size % 512) + 1024 for size in files) + 2048 * len(directories) + 10240
    archive_upper = tar_upper + ((tar_upper + 16382) // 16383) * 5 + 64
    elf = retention_forecast()
    reserves = {'complete_cache_archive': archive_upper, 'all_new_elf_objects': elf['new_gzip_upper_bound_bytes'],
                'additional_generated_growth': plan['generated_growth_bytes'], 'future_delta_and_elf_retention': plan['future_retention_bytes'], 'raw_capture_and_facts': plan['raw_capture_bytes_upper_bound'], 'scenario_temporary_data': plan['scenario_temporary_bytes_upper_bound'],
                'source_and_audit_metadata': 12 * 1024 * 1024, 'free_floor': DISK_FLOOR_BYTES}
    required = sum(reserves.values())
    return {'sampled_free_bytes': free_bytes(), 'new_or_changed_cache_bytes_to_preserve': sum(files), 'new_or_changed_cache_files_to_preserve': len(files),
            'cache_directories': len(directories), 'reserves': reserves, 'required_free_bytes': required,
            'sufficient': free_bytes() >= required}

def preserve_whole_cache(label):
    assert not cache_owners(), ('active cache references', cache_owners())
    forecast = combined_forecast()
    receipt.setdefault('whole_cache_forecasts', []).append({'label': label, **forecast})
    save()
    assert forecast['sufficient'], ('complete cache archive reserve refusal; originals retained', forecast)
    current = cache_map()
    if label == 'pre-clean':
        assert current == reference_cache_map, 'prior archive does not exactly cover present cache'
        validate_reference_archive()
        assert cache_map() == current, 'cache changed during prior full archive verification'
        record = {'reference': reference_cache_receipt, 'reference_validation': prior_validation_pin, 'present_cache_paths':len(current), 'source_before_after_and_archive_pathset_sha256_bytes_full07777_verified':True, 'scope':'verified reused complete-cache archive, no omitted current generated outputs'}
        receipt.setdefault('whole_cache_retention',[]).append(record)
        save()
        return record
    before = {name:pin for name,pin in current.items() if reference_cache_map.get(name) != pin}
    target = OUT / (label + '-all-cache.tar.gz')
    guard_write()
    with target.open('wb') as raw_target, gzip.GzipFile(fileobj=raw_target, mode='wb', mtime=0, compresslevel=1) as zipped:
        with tarfile.open(fileobj=zipped, mode='w|', format=tarfile.PAX_FORMAT) as archive:
            for name, pin in before.items():
                guard_write()
                path = CACHE / name
                info = tarfile.TarInfo(name)
                info.mode = pin['full_mode']
                info.mtime = 0
                if pin['type'] == 'directory':
                    info.type = tarfile.DIRTYPE
                    archive.addfile(info)
                else:
                    info.size = pin['bytes']
                    with path.open('rb') as source:
                        class GuardedReader:
                            def read(self, count=-1):
                                guard_write()
                                return source.read(count)
                        archive.addfile(info, GuardedReader())
    decoded = {}
    with tarfile.open(target, 'r|gz') as archive:
        for member in archive:
            name = member.name.rstrip('/')
            assert name not in decoded
            if member.isdir(): decoded[name] = {'type': 'directory', 'full_mode': member.mode & 0o7777}
            else:
                assert member.isfile()
                state = hashlib.sha256(); size = 0
                stream = archive.extractfile(member)
                while chunk := stream.read(1024 * 1024): state.update(chunk); size += len(chunk)
                decoded[name] = {'type': 'file', 'bytes': size, 'sha256': state.hexdigest(), 'full_mode': member.mode & 0o7777}
    assert decoded == before, 'complete cache archive decode/pathset/bytes/fullmodes differs'
    assert cache_map() == current, 'original cache changed during delta retention'
    map_path = OUT / (label + '-cache-map.json')
    guard_write()
    map_path.write_text(json.dumps(current, indent=2) + '\n')
    record = {'archive': str(target), 'archive_sha256': sha256_file(target), 'archive_bytes': target.stat().st_size,
              'map': str(map_path), 'map_sha256': sha256_file(map_path), 'paths': len(before),
              'regular_files': sum(pin['type'] == 'file' for pin in before.values()),
              'source_before_after_and_archive_pathset_sha256_bytes_full07777_verified': True,
              'scope': 'complete current map reconstructible from verified prior complete archive plus all new/changed outputs in this delta archive', 'inherited_archive': reference_cache_receipt, 'delta_rows': before, 'deleted_prior_paths': sorted(set(reference_cache_map)-set(current))}
    receipt.setdefault('whole_cache_retention', []).append(record)
    save()
    return record

def decode_cache_receipt(record):
    archive=Path(record['archive'])
    assert sha256_file(archive)==record['archive_sha256']
    map_path=Path(record['map'])
    assert sha256_file(map_path)==record['map_sha256']
    expected_map=json.loads(map_path.read_text())
    decoded={}
    with tarfile.open(archive,'r|gz') as source:
        for member in source:
            name=member.name.rstrip('/')
            assert name not in decoded
            if member.isdir():decoded[name]={'type':'directory','full_mode':member.mode&0o7777}
            else:
                assert member.isfile()
                state=hashlib.sha256();count=0
                stream=source.extractfile(member)
                while chunk:=stream.read(1024*1024):state.update(chunk);count+=len(chunk)
                decoded[name]={'type':'file','bytes':count,'sha256':state.hexdigest(),'full_mode':member.mode&0o7777}
    if 'delta_rows' in record:
        assert decoded==record['delta_rows'], 'delta archive differs'
        full=decode_cache_receipt(record['inherited_archive'])
        for name in record['deleted_prior_paths']:
            assert name in full
            del full[name]
        full.update(decoded)
    else:full=decoded
    assert full==expected_map, 'full composed map differs'
    return full

def validate_reference_archive():
    assert decode_cache_receipt(reference_cache_receipt)==reference_cache_map

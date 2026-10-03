#!/usr/bin/env python3
"""Lossless WORK-only publication packaging; never edits original proof/source."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import stat

OUT = Path('/workspace/work/broker-final403-publication')
ROOTS = {
    'qualification': Path('/workspace/work/broker-merged-qualification-403be1e3'),
    'normalization': Path('/workspace/work/integration/broker-403-normalization'),
    'origin': Path('/workspace/work/integration/broker-merged-source-403be1e3'),
}
PINS = {
    ('qualification', 'validation.json'): '26071d6c6c73b2f61c481c6d802cec9dfa6ca500c8f9691b2a9b151de8424ee2',
    ('normalization', 'normalized.json'): '248b83d10489c4210d7a378f64f0ec2db7662e13a077737adf4d3abae30a0916',
    ('origin', 'receipt.json'): 'd6e9a3578b31fe76ba8e764303a83cc615437744498e4874b37ef0c8af2d2fff',
}
SOURCE = '403be1e3db073df86921d6fb21189f695c4f1eaf'
DRIVER = '3be705a6dd52bb5707a471fdcd27772886dbc8aa039b0a38c38b994836c047b8'


def sha(b):
    return hashlib.sha256(b).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def deterministic_gzip(data):
    stream = io.BytesIO()
    with gzip.GzipFile(fileobj=stream, filename='', mode='wb', mtime=0, compresslevel=9) as g:
        g.write(data)
    return stream.getvalue()


def compiled_head(head):
    return (head.startswith(b'\x7fELF') or head.startswith(b'!<arch>\n')
            or head[:4] in (b'\0asm', b'\xfe\xed\xfa\xce', b'\xce\xfa\xed\xfe',
                            b'\xfe\xed\xfa\xcf', b'\xcf\xfa\xed\xfe'))


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o600)


def inventory():
    result = {}
    for label, root in ROOTS.items():
        entries = [root] + sorted(root.rglob('*'))
        for p in entries:
            s = p.lstat()
            rel = str(p.relative_to(root))
            if p.is_symlink() or not (p.is_file() or p.is_dir()):
                raise RuntimeError('unexpected special path: ' + str(p))
            result[(label, rel)] = {'type': 'directory' if p.is_dir() else 'file',
                                   'full_mode': stat.S_IMODE(s.st_mode),
                                   'bytes': s.st_size if p.is_file() else None,
                                   'inode': [s.st_dev, s.st_ino], 'mtime_ns': s.st_mtime_ns}
    return result


def main():
    assert not OUT.exists(), 'refuse to overwrite an existing publication candidate'
    before = inventory()
    files, dirs, excluded, objects = [], [], [], {}
    for (label, rel), s in sorted(before.items()):
        path = ROOTS[label] / rel
        is_bin = label == 'qualification' and (rel == 'bin' or rel.startswith('bin/'))
        if s['type'] == 'directory':
            if not is_bin:
                dirs.append({'root': label, 'path': rel, 'full_mode': s['full_mode']})
            continue
        data = path.read_bytes()
        digest = sha(data)
        if (label, rel) in PINS:
            assert digest == PINS[(label, rel)], (label, rel, digest)
        if is_bin:
            head = gzip.decompress(data)[:8] if data.startswith(b'\x1f\x8b') else data[:8]
            assert head.startswith(b'\x7fELF'), 'non-ELF in declared excluded bin: ' + str(path)
            excluded.append({'root': label, 'path': rel, 'bytes': len(data),
                             'sha256': digest, 'full_mode': s['full_mode'],
                             'storage': 'gzip_ELF' if data.startswith(b'\x1f\x8b') else 'ELF',
                             'reason': 'retained compiled artifact; original remains in WORK; payload omitted'})
            continue
        assert not compiled_head(data[:8]), 'compiled artifact outside bin: ' + str(path)
        if data.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as g:
                assert not compiled_head(g.read(8)), 'compressed compiled artifact outside bin: ' + str(path)
        if digest not in objects:
            compressed = deterministic_gzip(data)
            assert gzip.decompress(compressed) == data
            assert compressed == deterministic_gzip(data), 'non-deterministic gzip'
            assert len(compressed) < 12_000_000
            object_path = 'objects/' + digest + '.gz'
            write(OUT / object_path, compressed)
            objects[digest] = {'path': object_path, 'raw_bytes': len(data),
                               'raw_sha256': digest, 'gzip_bytes': len(compressed),
                               'gzip_sha256': sha(compressed), 'stored_full_mode': 0o600}
        files.append({'root': label, 'path': rel, 'bytes': len(data),
                      'sha256': digest, 'full_mode': s['full_mode'],
                      'object': objects[digest]['path']})
    assert len(files) == 39225 and len(excluded) == 454
    assert sum(e['storage'] == 'gzip_ELF' for e in excluded) == 398
    assert sum(e['storage'] == 'ELF' for e in excluded) == 56
    assert len(objects) == 778
    restore_map = {'schema': 1, 'original_roots': {k: str(v) for k, v in ROOTS.items()},
                   'restoration': 'restore each root under a supplied empty destination; no absolute original path writes',
                   'directories': dirs, 'files': files, 'objects': list(objects.values()),
                   'scope': 'all original non-bin files and directories; omitted compiled bin subtree is explicit'}
    raw_map = canonical(restore_map)
    compressed_map = deterministic_gzip(raw_map)
    write(OUT / 'restore-map.json.gz', compressed_map)
    excluded_raw = canonical({'schema': 1, 'original_roots': restore_map['original_roots'],
                             'files': excluded, 'payloads_published': False})
    write(OUT / 'excluded-compiled-map.json.gz', deterministic_gzip(excluded_raw))
    # Verify every selected original path against its stored object, including
    # exact bytes/hash and its current full 07777 mode. No inflated restore tree.
    cache = {}
    for f in files:
        obj = f['object']
        if obj not in cache:
            compressed = (OUT / obj).read_bytes()
            cache[obj] = gzip.decompress(compressed)
            assert sha(cache[obj]) == f['sha256']
        original = ROOTS[f['root']] / f['path']
        assert cache[obj] == original.read_bytes()
        assert stat.S_IMODE(original.stat().st_mode) == f['full_mode']
    assert gzip.decompress((OUT / 'restore-map.json.gz').read_bytes()) == raw_map
    assert gzip.decompress((OUT / 'excluded-compiled-map.json.gz').read_bytes()) == excluded_raw
    after = inventory()
    assert after == before, 'original paths/types/full modes/bytes/inodes/mtime changed during packaging'
    v = json.loads((ROOTS['qualification'] / 'validation.json').read_bytes())
    n = json.loads((ROOTS['normalization'] / 'normalized.json').read_bytes())
    assert v['source_commit'] == SOURCE and n['source_commit'] == SOURCE
    assert v['driver_sha256'] == DRIVER and n['passed'] is True
    commands = v['commands'] + v['maintenance_commands']
    command_checks = []
    for c in commands:
        log = ROOTS['qualification'] / c['name'] / 'command.log'
        disk = ROOTS['qualification'] / c['name'] / 'disk-monitor.jsonl'
        assert sha(log.read_bytes()) == c['log_sha256']
        assert sha(disk.read_bytes()) == c['disk_monitor']['sample_log_sha256']
        assert c['exit_code'] == c['disk_monitor']['actual_process_exit_code'] == 0
        command_checks.append({'name': c['name'], 'exit_code': c['exit_code'],
                               'log_sha256': c['log_sha256'],
                               'disk_monitor_sha256': c['disk_monitor']['sample_log_sha256'],
                               'source_before': c['source_before'], 'source_after': c['source_after']})
    assert len(command_checks) == 61
    normalizer_exit = (ROOTS['normalization'] / 'command.exit').read_text().strip()
    assert normalizer_exit == '0'
    lanes = []
    for p in n['broker_profiles']:
        label = p['toolchain'] + '-' + p['profile']
        captured = [f for f in files if f['root'] == 'qualification' and f['path'].startswith(label + '/')]
        membership = [f for f in captured if '/membership/' in f['path']]
        traces = []
        for f in membership:
            if f['path'].endswith('/trace.json'):
                j = json.loads((ROOTS['qualification'] / f['path']).read_bytes())
                traces.append({'path': f['path'], 'events': len(j['events']),
                               'checkpoints': len(j['checkpoints']), 'limits': j['limits']})
        lanes.append({'name': label, 'captured_files': len(captured),
                      'captured_bytes': sum(f['bytes'] for f in captured),
                      'membership_files': len(membership), 'membership_traces': traces})
    receipt = {
        'schema': 1, 'source_commit': SOURCE, 'frozen_driver_sha256': DRIVER,
        'input_pins': [{'root': k[0], 'path': k[1], 'sha256': value} for k, value in PINS.items()],
        'raw_original_file_count': 39674, 'published_original_file_count': len(files),
        'published_original_logical_bytes': sum(f['bytes'] for f in files),
        'published_original_directory_count': len(dirs), 'unique_objects': len(objects),
        'unique_raw_bytes': sum(o['raw_bytes'] for o in objects.values()),
        'object_gzip_bytes': sum(o['gzip_bytes'] for o in objects.values()),
        'excluded_compiled_files': len(excluded), 'excluded_compiled_gzips': 398,
        'excluded_selected_ELFs': 56, 'excluded_compiled_bytes': sum(f['bytes'] for f in excluded),
        'compiled_payloads_published': False, 'original_proof_source_or_artifacts_modified': False,
        'original_before_after_inventory_sha256': sha(canonical(before_to_json(before))),
        'all_original_path_sets_types_full_07777_modes_bytes_inodes_mtimes_unchanged': True,
        'all_selected_file_contents_gzip_roundtrip_verified': True,
        'all_excluded_ELF_or_gzip_ELF_headers_verified': True,
        'restore_map_raw_bytes': len(raw_map), 'restore_map_raw_sha256': sha(raw_map),
        'restore_map_gzip_sha256': sha(compressed_map),
        'excluded_map_raw_bytes': len(excluded_raw), 'excluded_map_raw_sha256': sha(excluded_raw),
        'commands': command_checks, 'normalizer_exit_code': 0, 'capture_lanes': lanes,
        'accepted_execution_counts_preserved': v['execution_counts'],
        'limits': [
            'Packaging checks do not rerun or expand broker/runtime qualification',
            'Original command.log is the actual combined stdout/stderr log; exits are preserved inside original validation.json, not invented split log/exit files',
            'Compiled artifact hashes/maps are retained metadata only; all454 payloads remain solely in WORK',
            'Actual process/IO histories retain their own limits; no physical-power-loss, complete Kafka cleaner or general KRaft claim',
            'Full file/directory modes and exact path-set checks here describe packaging observations; original runtime guards retain their own stated scope']}
    write(OUT / 'receipt.json', json.dumps(receipt, indent=2).encode() + b'\n')
    print(json.dumps({'published_paths': len(files), 'objects': len(objects),
                      'objects_bytes': receipt['object_gzip_bytes'], 'map_gzip_bytes': len(compressed_map),
                      'commands_verified': len(command_checks), 'capture_lanes': len(lanes),
                      'excluded_compiled': len(excluded), 'receipt_sha256': sha((OUT/'receipt.json').read_bytes())}))


def before_to_json(before):
    return [{'root': label, 'path': rel, **s} for (label, rel), s in sorted(before.items())]


if __name__ == '__main__':
    main()

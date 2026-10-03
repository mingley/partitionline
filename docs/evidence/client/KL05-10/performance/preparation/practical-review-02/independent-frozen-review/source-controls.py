"""Independent bounded source/semantic review; no benchmark, SQLite or process runs."""
import ast
import hashlib
import json
import os
import stat
import struct
from pathlib import Path
from types import SimpleNamespace

ROOT = Path('/workspace/work/client-sticky-performance-practical-497f')
OUT = Path(__file__).parent
PINNED_MANIFEST = '5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1'


def identity(path):
    assert path.is_file() and not path.is_symlink()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'bytes': path.stat().st_size, 'full_mode': stat.S_IMODE(path.stat().st_mode)}


def verify_packet():
    manifest_path = ROOT / 'stage-handoff.json'
    assert identity(manifest_path)['sha256'] == PINNED_MANIFEST
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest['files']) == manifest['path_count'] == 71
    observed = {}
    for name, expected in manifest['files'].items():
        path = ROOT / name
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        actual = identity(path)
        assert actual == {k: expected[k] for k in actual}, name
        observed[name] = {**actual, 'source_path': str(path), 'target': expected['target']}
    assert sum(v['bytes'] for v in observed.values()) == manifest['logical_payload_bytes']
    for name, mode in manifest['directories_full_modes'].items():
        directory = ROOT / name
        assert directory.is_dir() and not directory.is_symlink()
        assert stat.S_IMODE(directory.stat().st_mode) == mode
    return observed


def extracted(path, names, globals_):
    tree = ast.parse(path.read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in nodes} == set(names)
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(module, str(path) + ':extracted-only', 'exec'), globals_)
    return {node.name: hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest() for node in nodes}


def main():
    os.sched_setaffinity(0, {2, 4})
    before = verify_packet()
    controls = []
    def check(name, condition, detail=None):
        assert condition, name
        controls.append({'name': name, 'passed': True, 'detail': detail})
    bench = ROOT / 'benchmarks/sticky-partitioner'
    settings = json.loads((bench / 'qualification.json').read_text())
    ranking = json.loads((bench / 'profiles.json').read_text())
    forecast = json.loads((bench / 'qualification-resource-forecast.json').read_text())
    profiles = {p['name'] for p in settings['profiles']}
    check('six exact distinct qualification profiles', len(profiles) == 6 and profiles == {p['name'] for p in ranking['profiles']})
    check('five genuine planned paired permutations', len(ranking['paired_blocks']) == 5 and all(set(p['randomized_order']) == profiles and len(p['randomized_order']) == 6 for p in ranking['paired_blocks']))
    check('qualification exact counts versus ranking minima', settings['records'] == {'warmup': 8192, 'exercise': 16384, 'total_exact': 24576, 'outstanding_max': 8192} and not settings['ranking_minima_satisfied'] and ranking['minimum_measure_seconds'] == 60 and ranking['minimum_independently_delivered_measure_records'] == 1_000_000)
    check('journal row and byte forecast', struct.calcsize('>QIiqQQ32sI') == 76 and 2 * (8 + 24576 * 76) == forecast['components']['two_journals_bytes'] == 3735568)
    check('runtime additive forecast arithmetic', sum(forecast['components'].values()) == forecast['per_cell_generated_allocation_upper_bound_bytes'] == 102301712 and sum(forecast[k] for k in ('per_cell_generated_allocation_upper_bound_bytes', 'physical_floor_bytes', 'stop_margin_bytes', 'other_lane_growth_reserve_bytes')) == 620298256)
    check('cold graph forecast arithmetic', (450 + 40 + 16 + 16 + 350) * 1024**2 == 914358272)
    check('broker provisioning remains an explicit separate precondition', 'must be complete' in forecast['already_ready_broker_precondition'] and not forecast['build_cache_included'] and not forecast['archive_transient_included'])
    # Source-equivalent bounded scheduler model, not a Rust/Java execution.
    for minimum in (8192, 16384):
        for drain in (1, 37, 8192):
            admitted = acknowledged = pending = peak = 0
            while admitted < minimum or pending:
                while admitted < minimum and pending < 8192:
                    admitted += 1
                    pending += 1
                    peak = max(peak, pending)
                completed = min(drain, pending)
                pending -= completed
                acknowledged += completed
            check(f'qualification admission/drain model minimum{minimum} drain{drain}', admitted == acknowledged == minimum and peak <= 8192 and pending == 0)

    hash_globals = {'hashlib': hashlib}
    hash_nodes = extracted(bench / 'audit.py', ['expected_hash'], hash_globals)
    def independently_project_input(seed, id_, keyed):
        mask = 2**64 - 1
        def word(value):
            value = (value + 0x9e3779b97f4a7c15) & mask
            value = ((value ^ (value >> 30)) * 0xbf58476d1ce4e5b9) & mask
            value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & mask
            return value ^ (value >> 31)
        payload = b'PLSTKVAL' + struct.pack('>QQ', seed, id_)
        current = word(seed ^ id_)
        while len(payload) < 100:
            payload += struct.pack('>Q', current)
            current = word(current)
        payload = payload[:100]
        key = struct.pack('>QQ', seed, id_) if keyed else b''
        assert len(payload) == 100 and len(key) == (16 if keyed else 0)
        return hashlib.sha256(bytes([keyed]) + key + payload).digest()
    for seed in (0, 79443, 2**63, 2**64 - 1):
        for id_ in (0, 8191, 8192, 24575, 2**63 - 1):
            for keyed in (False, True):
                check(f'public source-input projection seed{seed} id{id_} key{int(keyed)}', hash_globals['expected_hash'](seed, id_, keyed) == independently_project_input(seed, id_, keyed))

    # In-memory fake /proc. No real /proc read, process creation, signals or cleanup.
    entries, signals = [], []
    class FakeStat:
        def __init__(self, value): self.value = value
        def read_text(self):
            if isinstance(self.value, BaseException): raise self.value
            return self.value
    class FakeDirectory:
        def __init__(self, name, raw): self.name, self.raw = str(name), raw
        def __truediv__(self, name):
            assert name == 'stat'
            return FakeStat(self.raw)
    class FakeProc:
        def iterdir(self): return list(entries)
    def fake_path(path):
        assert path == '/proc'
        return FakeProc()
    pgid = 700
    member_globals = {'Path': fake_path, 'os': SimpleNamespace(killpg=lambda group, signum: signals.append((group, signum))), 'signal': SimpleNamespace(SIGKILL=9)}
    member_nodes = extracted(bench / 'run-cell.py', ['members', 'stop'], member_globals)
    def group(name, group_=pgid, session=pgid):
        return FakeDirectory(name, f'{name} (fake) R 1 {group_} {session} 0')
    entries[:] = [group(701)]
    check('extracted readable matching process membership', member_globals['members'](pgid) == [{'pid': 701, 'group': pgid, 'session': pgid, 'state': 'R'}])
    member_globals['stop'](pgid)
    check('extracted owned group would receive only intended signal', signals == [(pgid, 9)])
    entries[:] = [group(702, 702, 702)]
    check('extracted readable unrelated group excluded', member_globals['members'](pgid) == [])
    entries[:] = [FakeDirectory(703, FileNotFoundError('synthetic gone'))]
    check('extracted vanished entry is excluded', member_globals['members'](pgid) == [])
    entries[:] = [group(704, session=999)]
    rejected = False
    try: member_globals['stop'](pgid)
    except AssertionError: rejected = True
    check('extracted session mismatch refuses signal', rejected and signals == [(pgid, 9)])
    entries[:] = [FakeDirectory(705, PermissionError('synthetic live unreadable stat'))]
    false_empty = member_globals['members'](pgid)
    signals.clear()
    member_globals['stop'](pgid)
    check('reproduced frozen-source unreadable false-empty counterexample', false_empty == [] and signals == [], {'returned_members': false_empty, 'signals': signals.copy(), 'meaning': 'The frozen source treats unknown membership as proven absence; this is the review counterexample, not a real process event.'})

    audit_source = (bench / 'audit.py').read_text()
    check('frozen SQLite source leaves page_size unspecified', 'PRAGMA page_size' not in audit_source and "str(4096 if qualification else 524288)" in audit_source)
    check('frozen SQLite source uses separate DELETE rollback journal', "PRAGMA journal_mode=DELETE" in audit_source)
    check('reproduced SQLite cap arithmetic counterexample', 4096 * 65536 == 268435456 and 4096 * 65536 > forecast['components']['sqlite_file_cap_bytes'], {'legal_page_size_bytes': 65536, 'max_page_count': 4096, 'possible_database_cap_bytes': 268435456, 'advertised_database_cap_bytes': forecast['components']['sqlite_file_cap_bytes'], 'actual_SQLite_connections_or_disk_files': 0})
    check('DELETE journal is uncharged by explicit SQLite component', forecast['components']['sqlite_file_cap_bytes'] == 16 * 1024**2 and not any('journal' in k and k != 'two_journals_bytes' for k in forecast['components']))
    after = verify_packet()
    assert after == before
    result = {'classification': 'Independent extracted-source/semantic controls only; zero Cargo/javac/JVM/SDK/native/SQLite/benchmark/process/cleanup executions', 'source_manifest_sha256': PINNED_MANIFEST, 'source_payloads_unchanged_before_after': True, 'payload_count': len(before), 'logical_payload_bytes': sum(v['bytes'] for v in before.values()), 'source_payloads': before, 'extracted_AST_SHA256': {**hash_nodes, **member_nodes}, 'cpu_set': sorted(os.sched_getaffinity(0)), 'controls': controls, 'passed_control_count': len(controls)}
    out = OUT / 'controls.json'
    assert not out.exists()
    out.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    out.chmod(0o600)
    print(json.dumps({'controls': len(controls), 'receipt': str(out), **identity(out)}))


if __name__ == '__main__': main()

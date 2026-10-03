import difflib
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
from pathlib import Path

ROOT = Path('/workspace/work/client-sticky-implementation-prep')
REPO = Path('/workspace/partitionline')
CANDIDATE = ROOT / 'candidate'
OUTPUT = ROOT / 'corrected-stage'
PREFIX = Path('docs/evidence/client/KL05-10/source-correction-1a3a91c6')
CLAIM = '1a3a91c6facc25e0b250fe06d9aefde9086a184c'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def info(path, git=False):
    data = path.read_bytes()
    result = {'sha256': sha(data), 'bytes': len(data),
              'mode': oct(stat.S_IMODE(path.stat().st_mode))}
    if git:
        result['git_mode'] = '100755' if path.stat().st_mode & stat.S_IXUSR else '100644'
    return result


def verify(path, expected):
    actual = info(path, git='git_mode' in expected)
    assert actual == expected, (str(path), actual, expected)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)


old_handoff = json.loads((ROOT / 'stage-handoff.json').read_text())
old_freeze = json.loads((ROOT / 'source-freeze.json').read_text())
assert len(old_handoff['stage_files']) == 69
assert info(ROOT / 'stage-handoff.json')['sha256'] == '39aac98fdcd360f074ae0c4f53ec7b6d2124acb28b0a118ee8e4da1567e71af3'
assert info(ROOT / 'source-freeze.json')['sha256'] == 'b01ae19fed885193e264031a217bcf4376462031aee16e108e6663245bc8cffd'
phase3 = ROOT / 'history/phase-03-frozen-69-pre-rejection'
phase4 = ROOT / 'history/phase-04-rejection-only-uncompiled'
for rel, expected in old_handoff['stage_files'].items():
    verify(REPO / rel, expected)
    verify(phase3 / 'published' / rel, expected)
for rel, expected in old_freeze['source_files'].items():
    verify(phase3 / 'candidate' / rel, expected)
for name in ['stage-handoff.json', 'source-freeze.json', 'source-review.patch',
             'lifecycle-analysis.md', 'preparation/receipt.json']:
    verify(phase3 / 'metadata' / name, info(ROOT / name))
phase4_receipt = json.loads((phase4 / 'preservation-receipt.json').read_text())
for expected in phase4_receipt['files']:
    rel = expected['path']
    verify(phase4 / 'candidate' / rel, {k: v for k, v in expected.items() if k != 'path'})
assert len(phase4_receipt['files']) == 11
held_before = {rel: info(REPO / rel, True) for rel in old_freeze['held_observed_at_handoff']}
assert held_before == old_freeze['held_observed_at_handoff']
source_files = {str(p.relative_to(CANDIDATE)): info(p, True)
                for p in sorted(CANDIDATE.rglob('*')) if p.is_file()}
assert len(source_files) == 11
test_re = r'#\[(?:tokio::test(?:\([^\]]*\))?|test)\]\s*(?:async\s+)?fn\s+(\w+)'
policy_tests = re.findall(test_re, (CANDIDATE / 'src/partitioner.rs').read_text())[3:]
socket_tests = re.findall(test_re, (CANDIDATE / 'tests/sticky_partitioner.rs').read_text())
assert len(policy_tests) == 11 and len(socket_tests) == 28
assert not OUTPUT.exists(), 'A sealed corrected packet already exists; do not overwrite it.'
OUTPUT.mkdir(mode=0o700)

# Exact phase snapshots were retained before edits; archive every file and directory.
entries = {}
tar_buffer = io.BytesIO()
with tarfile.open(fileobj=tar_buffer, mode='w', format=tarfile.PAX_FORMAT) as archive:
    for phase in [phase3, phase4]:
        for path in [phase] + sorted(phase.rglob('*')):
            assert not path.is_symlink(), path
            relative = str(path.relative_to(ROOT))
            member = tarfile.TarInfo(relative)
            member.uid = member.gid = member.mtime = 0
            member.uname = member.gname = ''
            member.mode = stat.S_IMODE(path.stat().st_mode)
            if path.is_dir():
                member.type = tarfile.DIRTYPE
                entries[relative] = {'kind': 'directory', 'mode': oct(member.mode)}
                archive.addfile(member)
            else:
                data = path.read_bytes()
                assert not data.startswith(b'\x7fELF'), relative
                member.size = len(data)
                entries[relative] = {'kind': 'file', **info(path)}
                archive.addfile(member, io.BytesIO(data))
raw_tar = tar_buffer.getvalue()
archive_path = OUTPUT / 'prior-source-phases.tar.gz'
with archive_path.open('wb') as target:
    with gzip.GzipFile(fileobj=target, mode='wb', filename='', mtime=0, compresslevel=9) as gz:
        gz.write(raw_tar)
archive_path.chmod(0o600)
assert gzip.decompress(archive_path.read_bytes()) == raw_tar
observed = {}
with tarfile.open(fileobj=io.BytesIO(gzip.decompress(archive_path.read_bytes())), mode='r:') as archive:
    for member in archive.getmembers():
        assert member.name not in observed
        if member.isdir():
            observed[member.name] = {'kind': 'directory', 'mode': oct(member.mode)}
        else:
            assert member.isfile(), member.name
            data = archive.extractfile(member).read()
            observed[member.name] = {'kind': 'file', 'sha256': sha(data),
                                     'bytes': len(data), 'mode': oct(member.mode)}
assert observed == entries
restore_map = {
    'schema': 1, 'scope': 'Exact original frozen packet and RNG-only source phase; no compiled artifacts',
    'archive': {'path': archive_path.name, **info(archive_path),
                'uncompressed_tar_sha256': sha(raw_tar), 'uncompressed_tar_bytes': len(raw_tar)},
    'original_69_packet_handoff': info(ROOT / 'stage-handoff.json'),
    'original_phase_preservation_receipt': info(phase3 / 'preservation-receipt.json'),
    'rejection_only_phase_preservation_receipt': info(phase4 / 'preservation-receipt.json'),
    'entries': entries, 'verification': 'Exact path set, SHA256, byte length, and full 07777 mode for every archive member',
    'restore': 'Extract the gzip tar into an empty WORK directory, then verify all entries against this map and apply the recorded full modes. Original uncompressed snapshots remain in WORK.'
}
write_json(OUTPUT / 'prior-source-phases-restore-map.json', restore_map)

# New phase observations do not rewrite any earlier source/receipt.
for name in ['rejection_correction.py', 'bounded_record_correction.py', 'seal_corrected.py']:
    shutil.copy2(ROOT / name, OUTPUT / name)
old_prefix = 'docs/evidence/client/KL05-10/source-preparation-1a3a91c6/preparation/'
for path in sorted((ROOT / 'preparation').iterdir()):
    if old_prefix + path.name not in old_handoff['stage_files']:
        target = OUTPUT / 'preparation' / path.name
        target.parent.mkdir(mode=0o700, exist_ok=True)
        shutil.copy2(path, target)

commands = []
for label, toolchain in [('corrected-install-stable-format-check', 'stable'),
                         ('corrected-install-msrv-format-check', '1.85.0')]:
    commands.append({
        'label': label,
        'command': ['taskset', '-c', '0,1', f'/workspace/work/rustup/toolchains/{toolchain}-x86_64-unknown-linux-gnu/bin/rustfmt',
                    '--check', '--edition', '2021', '--config', 'skip_children=true'] +
                   [str(CANDIDATE / rel) for rel in ['src/partitioner.rs', 'src/producer.rs', 'tests/sticky_partitioner.rs', 'tests/client_api.rs']],
        'exit': int((ROOT / 'preparation' / (label + '.exit')).read_text()),
        'stdout': info(ROOT / 'preparation' / (label + '.stdout')),
        'stderr': info(ROOT / 'preparation' / (label + '.stderr')),
        'source_hashes': source_files,
    })
assert all(c['exit'] == 0 for c in commands)
write_json(OUTPUT / 'preparation/receipt.json', {
    'schema': 1, 'scope': 'Absolute-toolchain syntax/format checks only; no Cargo/rustc/Clippy/Rust or Java tests/JVM/broker/performance execution',
    'cpu_set': '0,1', 'commands': commands, 'prepared_policy_tests': 11,
    'prepared_socket_tests': 28, 'prepared_does_not_mean_executed': True,
})

patch = []
correction_patch = []
for rel in source_files:
    before = (ROOT / 'base' / rel).read_text() if (ROOT / 'base' / rel).exists() else ''
    old_candidate = (phase3 / 'candidate' / rel).read_text()
    after = (CANDIDATE / rel).read_text()
    patch.extend(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
                                    fromfile='a/' + rel if before else '/dev/null', tofile='b/' + rel))
    correction_patch.extend(difflib.unified_diff(old_candidate.splitlines(keepends=True), after.splitlines(keepends=True),
                                               fromfile='first-candidate/' + rel, tofile='corrected-candidate/' + rel))
(OUTPUT / 'source-review.patch').write_text(''.join(patch))
(OUTPUT / 'correction-review.patch').write_text(''.join(correction_patch))

appendix = '''\nThe post-review correction replaces 31-bit modulo selection with deterministic rejection sampling. The pure choice returns its post-draw RNG state after every attempted draw, including rejected values. A route/append plan installs that state only after successful channel enqueue; failed routing, buffer/slot/channel admission, or cancellation cannot consume RNG or charge policy state. A cached partition with no selection draw does not consume a draw in pressure mode. Three additional policy cases exercise rejection for non-power-of-two counts, returned-state rotation/pressure behavior, and cached pressure without a selection draw. These cases are prepared, not executed.\n\nThe producer-wide opt-in record budget is separate from payload byte accounting and channel row capacity. Its default and hard ceiling are 100,000, clamped to 1..=100,000. The count includes reserved admission slots plus accepted records: an asynchronous future blocked on byte/channel capacity may retain a reserved slot before enqueue. A private non-Clone RAII permit is acquired once and owned by each Pending through worker queues, in-flight requests, retry actors, and terminal drop. Failed admission and pre-enqueue cancellation release it; accepted caller cancellation preserves it until delivery/error ownership terminates. Retries neither acquire another slot nor recharge packed bytes. Null/empty payloads and buffer_memory=0 remain bounded by this record capacity. Existing QueueFull and absolute max_block Timeout behavior apply when capacity is exhausted. Close wakes waiters without synthetically clearing slots still legitimately owned by paused futures.\n\nThe caller input vector staged by send_all is outside this admitted/reserved-record bound, as are other existing producer/network buffers. This is not a whole-process RSS bound. Six additional framed socket cases prepare cap-two null/empty pressure, reserved-slot cancellation while byte-blocked, cancellation while capacity-blocked, accepted caller cancellation, retained retry slots and terminal release, and encoding/routing failure release. No new performance result or passed runtime case is claimed.\n'''
(OUTPUT / 'lifecycle-analysis.md').write_text((ROOT / 'lifecycle-analysis.md').read_text() + appendix)
freeze = {
    'schema': 1, 'task': 'KL05-10', 'claimed_main_sha': CLAIM,
    'status': 'Corrected source checkpoint; uncompiled and unqualified',
    'compiled': False, 'rust_or_java_tests_executed': False, 'performance_claims': False,
    'contract': old_freeze['contract'], 'source_files': source_files,
    'supersedes_source_candidate': {'handoff': info(ROOT / 'stage-handoff.json'),
                                   'source_freeze': info(ROOT / 'source-freeze.json'),
                                   'lossless_history_archive': info(archive_path)},
    'reviewed_corrections': ['Unbiased eligible-index rejection sampling with successful-enqueue post-draw state commit',
                            'Producer-wide reserved-plus-accepted record budget with per-Pending RAII ownership'],
    'prepared_policy_test_count': len(policy_tests), 'prepared_policy_tests': policy_tests,
    'prepared_socket_test_count': len(socket_tests), 'prepared_socket_tests': socket_tests,
    'held_observed_before_install': held_before,
    'limits': {**old_freeze['limits'], 'max_pending_records_default_and_hard_ceiling': 100000,
               'max_pending_records_minimum': 1, 'record_budget_counts': 'Reserved admission slots plus accepted records, including null/empty payloads and unlimited payload-byte budgets',
               'record_capacity_exhaustion': 'Existing QueueFull/max_block Timeout; no new error type',
               'outside_record_budget': 'Caller send_all staged input vector and other existing producer/network buffers'},
    'root_integration': {'exports': ['StickyPartitioner', 'StickyPartitionerConfig'],
                         'owner': 'root; same source checkpoint; not edited by this agent'},
    'next_gates': 'Actual pushed immutable source; format first; stable/MSRV default/all strict checks and meaningful policy/socket tests; full four-client QA; genuine pinned Java reference; matched keyed/unkeyed benchmark with explicit accounting and leader/seed configuration',
}
write_json(OUTPUT / 'source-freeze.json', freeze)

# Install the exact reviewed candidate only after historical verification succeeds.
for rel, expected in source_files.items():
    target = REPO / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(CANDIDATE / rel, target)
    verify(target, expected)
for path in sorted(OUTPUT.rglob('*')):
    if path.is_file():
        path.chmod(0o600)
        target = REPO / PREFIX / path.relative_to(OUTPUT)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        assert not target.exists(), target
        shutil.copy2(path, target)
        verify(target, info(path))
for rel, expected in old_handoff['stage_files'].items():
    if rel not in source_files:
        verify(REPO / rel, expected)
    verify(phase3 / 'published' / rel, expected)
for rel, expected in held_before.items():
    verify(REPO / rel, expected)
for expected in phase4_receipt['files']:
    verify(phase4 / 'candidate' / expected['path'], {k: v for k, v in expected.items() if k != 'path'})
stage_paths = sorted(set(old_handoff['stage_files']) |
                     {str(PREFIX / p.relative_to(OUTPUT)) for p in OUTPUT.rglob('*') if p.is_file()})
handoff = {
    'schema': 1, 'task': 'KL05-10', 'claim_main_sha': CLAIM,
    'status': 'safe_to_stage corrected uncompiled source-only checkpoint; root owns commit/push/exports',
    'compiled': False, 'rust_or_java_tests_executed': False, 'performance_claims': False,
    'prepared_policy_tests': 11, 'prepared_socket_tests': 28,
    'all_stage_files': {rel: info(REPO / rel) for rel in stage_paths},
    'source_freeze': {'path': str(PREFIX / 'source-freeze.json'), **info(OUTPUT / 'source-freeze.json')},
    'source_review_patch': info(OUTPUT / 'source-review.patch'),
    'prior_packet_preserved': {'original69_exact_in_work': True, 'original_candidate11_exact_in_work': True,
                             'rejection_only_candidate11_exact_in_work': True,
                             'archive_members': len(entries), 'archive': info(archive_path),
                             'restore_map': info(OUTPUT / 'prior-source-phases-restore-map.json')},
    'held_after_install': {rel: info(REPO / rel, True) for rel in held_before},
    'integration_head_observation': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
    'stage_paths_exclude_root_exports_shared_plan_registry_and_human_held_sources': True,
}
write_json(ROOT / 'corrected-stage-handoff.json', handoff)
print(json.dumps({'status': handoff['status'], 'stage_files': len(stage_paths),
                  'stage_bytes': sum(v['bytes'] for v in handoff['all_stage_files'].values()),
                  'handoff': info(ROOT / 'corrected-stage-handoff.json'),
                  'source_freeze': handoff['source_freeze'], 'source_files': source_files,
                  'archive_files': sum(v['kind'] == 'file' for v in entries.values()),
                  'archive_directories': sum(v['kind'] == 'directory' for v in entries.values()),
                  'archive': info(archive_path)}, indent=2))

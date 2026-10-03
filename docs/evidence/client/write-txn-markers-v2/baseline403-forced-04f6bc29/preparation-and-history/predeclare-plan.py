#!/usr/bin/env python3
"""Create a source-only, explicitly unexecuted QA contract and disk forecast."""
from pathlib import Path
import hashlib
import json
import os
import stat
import subprocess
import time

BASE = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
SOURCE = Path('/workspace/work/client-capabilities-source-e90efb49')
OLD = Path('/workspace/work/broker-merged-source-403be1e3')
OLD_SHA = '403be1e3db073df86921d6fb21189f695c4f1eaf'
SOURCE_SHA = 'e90efb494401fbf6c52998501e2d4b5a42471a73'
TARGET = '/workspace/work/client-share-target'
FLOOR = 350 * 1024 * 1024

def identity(path):
    data = path.read_bytes()
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest(),
            'bytes': len(data), 'full_mode': stat.S_IMODE(path.stat().st_mode)}

raw = subprocess.check_output(['git', '-C', '/workspace/partitionline', 'ls-tree', '-r', '-z', OLD_SHA])
inputs = []
for row in raw.split(b'\0'):
    if not row:
        continue
    desc, name = row.split(b'\t', 1)
    mode, kind, blob = desc.decode().split()
    name = name.decode()
    if name in ('Cargo.toml', 'Cargo.lock', 'README.md', 'clippy.toml') or name.startswith(('src/', 'examples/', '.cargo/')):
        assert kind == 'blob' and mode in ('100644', '100755')
        info = identity(OLD / name)
        data = (OLD / name).read_bytes()
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == blob
        info.update({'relative_path': name, 'git_blob_sha1': blob, 'git_mode': mode})
        inputs.append(info)

overlay_specs = [
    ('tests/fail_first_write_txn_markers_v2.rs', SOURCE / 'docs/evidence/client/write-txn-markers-v2/preparation/fail-first.rs'),
    ('tests/fail_first_share_offsets_v1.rs', SOURCE / 'docs/evidence/client/share-offsets-v1/preparation/fail-first.rs'),
    ('tests/baseline_positive_controls.rs', BASE / 'baseline_positive_controls.rs'),
    ('tests/fixtures/write-txn-markers-v2/socket_peer.rs', SOURCE / 'tests/fixtures/write-txn-markers-v2/socket_peer.rs'),
    ('tests/fixtures/share-offsets-v1/schema-lag-1.response.bin', SOURCE / 'tests/fixtures/share-offsets-v1/schema-lag-1.response.bin'),
]
overlays = [dict(identity(path), destination=dest) for dest, path in overlay_specs]
reference_cache = []
for lane in ('stable-default', 'stable-all', 'msrv-default', 'msrv-all'):
    root = Path('/workspace/work/client-share-assessment/final-12f43986') / lane
    receipt = root / 'cache-cleanup.json'
    info = json.loads(receipt.read_bytes())
    archive = root / 'retained-cache/cache-elfs.tar.gz'
    reference_cache.append({'lane': lane, 'receipt': identity(receipt),
                            'generated_bytes_before_cleanup': info['removed_generated_file_bytes'],
                            'lossless_retained_elf_archive_bytes': archive.stat().st_size,
                            'lossless_retained_elf_archive_sha256': info['retained_elf_archive_sha256']})
cache_peak = max(x['generated_bytes_before_cleanup'] for x in reference_cache)
archive_peak = max(x['lossless_retained_elf_archive_bytes'] for x in reference_cache)
cache_forecast = (cache_peak * 125 + 99) // 100
archive_forecast = (archive_peak * 110 + 99) // 100
metadata_reserve = 32 * 1024 * 1024
required = cache_forecast + archive_forecast + metadata_reserve + FLOOR
vfs = os.statvfs('/workspace')
free = vfs.f_bavail * vfs.f_frsize

jar_pin_path = SOURCE / 'docs/evidence/broker/KL11-68/live-peer-build/ordinary-java.json'
jar_pins = json.loads(jar_pin_path.read_bytes())
jars = []
for pin in jar_pins['releases']:
    jar = Path('/workspace/work/broker-wire/jars') / ('kafka-clients-' + pin['release'] + '.jar')
    info = identity(jar)
    assert info['sha256'] == pin['jar_sha256']
    info.update({'release': pin['release'], 'published_origin_pin': str(jar_pin_path),
                 'published_origin_pin_sha256': identity(jar_pin_path)['sha256']})
    jars.append(info)
slf4j = identity(Path('/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar'))

environment = {
    'CARGO_HOME': '/workspace/work/cargo', 'RUSTUP_HOME': '/workspace/work/rustup',
    'CARGO_TARGET_DIR': TARGET, 'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0',
    'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0', 'CARGO_NET_OFFLINE': 'true',
    'RUSTC': '<absolute selected installed toolchain>/bin/rustc',
    'RUSTDOC': '<absolute selected installed toolchain>/bin/rustdoc',
    'RUSTFMT': '<absolute selected installed toolchain>/bin/rustfmt',
    'PATH': '<absolute selected toolchain>/bin:/workspace/work/cargo/bin:<existing system PATH>',
}

def cargo(tc, args, root=SOURCE):
    installed = '/workspace/work/rustup/toolchains/' + ('stable' if tc == 'stable' else '1.85.0') + '-x86_64-unknown-linux-gnu/bin/cargo'
    return ['taskset', '-c', '0,1', installed, *args, '--offline', '--locked', '--manifest-path', str(root / 'Cargo.toml')]

focused = []
for tc in ('stable', '1.85.0'):
    focused.append({'name': tc + '-focused-socket-codec', 'toolchain': tc, 'features': 'default',
                    'argv': cargo(tc, ['test', '--test', 'write_txn_markers_v2', '--test', 'share_offsets_v1']) + ['--', '--test-threads=1'],
                    'expected': '21 meaningful tests plus one inactive SDK-harness scaffold; actual counts taken from retained compiler/test outputs'})
    focused.append({'name': tc + '-focused-strict-clippy', 'toolchain': tc, 'features': 'default',
                    'argv': cargo(tc, ['clippy', '--lib', '--test', 'write_txn_markers_v2', '--test', 'share_offsets_v1']) + ['--', '-D', 'warnings']})

whole = []
for tc in ('stable', '1.85.0'):
    whole.append({'name': tc + '-strict-fmt', 'toolchain': tc,
                  'argv': ['taskset', '-c', '0,1', '/workspace/work/rustup/toolchains/' + ('stable' if tc == 'stable' else '1.85.0') + '-x86_64-unknown-linux-gnu/bin/cargo', 'fmt', '--manifest-path', str(SOURCE / 'Cargo.toml'), '--all', '--', '--check']})
    for profile, flags in [('default', []), ('all-features', ['--all-features'])]:
        for suffix, args in [
            ('all-targets', ['test', '--all-targets']),
            ('strict-clippy', ['clippy', '--all-targets']),
            ('strict-doc', ['doc', '--no-deps']),
            ('strict-doctest', ['test', '--doc']),
        ]:
            row = {'name': tc + '-' + profile + '-' + suffix, 'toolchain': tc, 'features': profile,
                   'argv': cargo(tc, args + flags)}
            if suffix == 'strict-clippy':
                row['argv'] += ['--', '-D', 'warnings']
            if suffix in ('strict-doc', 'strict-doctest'):
                row['forced_environment'] = {'RUSTDOCFLAGS': '-D warnings'}
            whole.append(row)

plan = {
    'schema_version': 1, 'source_sha': SOURCE_SHA, 'status': 'predeclared; no Cargo/JVM/socket process launched; awaiting ROOT CPU/disk/cache lease',
    'source_root': str(SOURCE), 'source_review': identity(BASE / 'source-review.json'),
    'owned_111_file_stage_matches_exact_e90': True,
    'sole_generated_cache': TARGET, 'current_cache_empty': not any(Path(TARGET).iterdir()),
    'requested_cpu_affinity_after_root_release': '0,1', 'zero_build_or_jvm_overlap': True,
    'environment': environment,
    'disk_forecast': {
        'sampled_at_unix': time.time(), 'sampled_free_bytes': free, 'minimum_sampled_reserve_bytes': FLOOR,
        'prior_max_complete_client_cache_bytes': cache_peak, 'cache_forecast_with_25_percent_margin': cache_forecast,
        'prior_max_lossless_elf_archive_bytes': archive_peak, 'archive_forecast_with_10_percent_margin': archive_forecast,
        'log_metadata_overlay_and_control_reserve_bytes': metadata_reserve,
        'required_free_bytes_before_each_cold_lane': required, 'initial_headroom_bytes': free - required,
        'current_forecast_fits': free >= required,
        'limitations': ['Historical full-client generated bytes are a measured reference, not a guaranteed upper bound for e90.',
                       'A short-interval isolated process-group monitor will fail and stop its own command at the sampled 350MiB reserve; no unseen instant-write bound is claimed.',
                       'Re-sample and reforecast after OIDC releases the slot and before each lane. Accumulated retained archives count against later lanes.',
                       'If forecast does not fit, preserve failure/partial outputs and report; never remove sources, receipts, fixtures or executable evidence.'],
        'reference_receipts': reference_cache,
    },
    'execution_sequence': [
        {'phase': 'bounded first candidate typecheck',
         'argv': cargo('stable', ['test', '--no-run', '--test', 'write_txn_markers_v2', '--test', 'share_offsets_v1']),
         'purpose': 'catch unqualified compile issues before a broad matrix; this is not a behavioral pass'},
        {'phase': 'old source regression experiment', 'baseline_source_sha': OLD_SHA,
         'staging_root': str(BASE / 'baseline-403-regression-source'),
         'baseline_scope': 'only enumerated exact403 client compiler inputs plus the exact five-file declared overlay; not a full old-tree qualification',
         'source_input_count': len(inputs), 'source_input_bytes': sum(x['bytes'] for x in inputs),
         'source_inputs_manifest': 'baseline-compiler-inputs.json',
         'materialization': 'hardlink verified immutable403 selected files without mutations; copy five exact overlays into a separate regression source root',
         'overlay': overlays,
         'commands': [
             cargo('stable', ['test', '--test', 'baseline_positive_controls'], BASE / 'baseline-403-regression-source') + ['--', '--test-threads=1'],
             cargo('stable', ['test', '--test', 'fail_first_write_txn_markers_v2'], BASE / 'baseline-403-regression-source') + ['--', '--test-threads=1'],
             cargo('stable', ['test', '--test', 'fail_first_share_offsets_v1'], BASE / 'baseline-403-regression-source') + ['--', '--test-threads=1'],
         ],
         'required_actual_outcomes': ['two old-supported public-operation positive controls pass',
             'three API27 behavioral failures: empty authoritative result falsely succeeds, 8/9 terminal instead of refresh, v2-only rejected',
             'one API90 behavioral failure: existing public operation rejects v1-only broker',
             'compile or preparation failures never count as behavioral fail-first proof'],
         'candidate_behavior_starts_after_baseline_evidence': True},
        {'phase': 'focused exact e90 tests and strict lint', 'commands': focused},
        {'phase': 'separate official SDK component and actual public Admin wire qualification',
         'java_sources': [identity(SOURCE / 'docs/evidence/client/write-txn-markers-v2/oracle/CapabilityOracle.java'),
                          identity(SOURCE / 'docs/evidence/client/write-txn-markers-v2/oracle/PublicAdminProbe.java')],
         'jars': jars, 'slf4j_runtime': slf4j,
         'compile': 'absolute system Java compiler; -Xlint:all -Werror, release-specific class directories, exact pinned jar classpaths; 128MiB heap, finite process timeout',
         'generation': '97 predeclared rows/release; actual unsupported-version rows retained and never counted as supported. Applicable official serializer/parser/handler executions, deterministic second regeneration and full-header binaries/hash manifests retained.',
         'reverse': 'bounded WORK-only Rust emitter or actual captured public Rust frames using exact e90 public codecs/test binaries; official Java parse/reserialize of actual Rust-emitted bytes with no trailing bytes. New emitter source/hash/compiler command retained separately.',
         'public_wire': 'genuine public Java Admin.abortTransaction and listShareGroupOffsets against bounded exact e90 two-node scripted peer; release-applicable versions, matching authoritative success/error, missing/wrong/duplicate identities, 8/9 retry, actual request versions/defaultTV0/lag/error, exact correlation and full request/response payloads. This is public SDK wire qualification, not a full broker TX/share-state implementation.',
         'bounds': 'each encoded fixture <=64KiB; peer <=32 connections/listener, <=64 frames/connection, <=256 observations; each harness <=30s and each public SDK call <=4s +1s close; whole child group independently joined/terminated',
         'retained_non_equivalence': ['All negative lag maps to absent in the official public result; raw signed lag remains in the additive Rust type.',
             'Official share handler skips errored partitions and completes missing group as empty; local typed operation preserves partition errors and rejects missing groups.',
             'Java public AbortTransaction uses leader routing; bootstrap-only per-node operation range selection in Rust remains a separate rolling-upgrade gap.']},
        {'phase': 'whole immutable exact e90 client qualification only after focused/compiler/SDK issues resolved',
         'commands': whole, 'qualification_command_count': len(whole), 'toolchains': ['stable', '1.85.0'],
         'profiles': ['default', 'all-features'],
         'source_correction_rule': 'concrete source/test correction requires preserved first failure and ROOT source re-freeze/push before final whole immutable qualification'}
    ],
    'per_command_receipts': ['exact argv/cwd/explicit forced environment/compiler identity/source pin/CPU affinity',
                            'full source pathset/blob/SHA-256/full-file-mode verification before and after against hash-bound complete manifest',
                            'raw stdout/stderr/exit/safe outcome counts; losing first preparations retained separately',
                            'sampled raw disk monitor log, hash/count/minimum/trigger/process-group cleanup',
                            'actual ELF and class identities with bytes/full modes and source/compiler/build command'],
    'clean_policy': 'only ROOT-authorized generated client-share-target cache after no-process-reference guards and lossless ELF byte/full-mode capture+decompression verification+retention forecast; no source/worktree/fixture/receipt/binary cleanup',
    'source_or_compile_side_effects_of_this_preparation': 'none: read-only source/JAR hashing plus small WORK JSON/Rust-source preparation only',
    'preparer_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
}
(BASE / 'baseline-compiler-inputs.json').write_text(json.dumps({'source_sha': OLD_SHA, 'files': inputs}, indent=2) + '\n')
(BASE / 'qa-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
print(json.dumps({'required_free_bytes': required, 'sampled_free_bytes': free,
                  'headroom_bytes': free-required, 'baseline_inputs': len(inputs),
                  'baseline_overlay_files': len(overlays), 'whole_qualification_commands': len(whole),
                  'qa_plan_sha256': identity(BASE / 'qa-plan.json')['sha256']}))

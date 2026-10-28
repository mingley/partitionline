import hashlib, json, os, pathlib, shutil, signal, subprocess, sys, time

root = pathlib.Path('/workspace/partitionline')
out = pathlib.Path('/workspace/work/goal-qualification-20261028/heartbeat-integrated-validation-01')
env = os.environ.copy()
env.update({
    'RUSTUP_HOME': '/workspace/work/rustup',
    'CARGO_HOME': '/workspace/work/cargo',
    'PATH': '/workspace/work/cargo/bin:' + env.get('PATH', ''),
    'CARGO_TARGET_DIR': '/workspace/work/qualification-20261027/broker-check-target',
    'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0',
    'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
})

def command(args, output, error=None, timeout=600, overrides=None):
    print('running:', ' '.join(map(str, args)), flush=True)
    runenv = env | (overrides or {})
    with (out / output).open('wb') as stdout:
        stderr = (out / error).open('wb') if error else None
        try:
            p = subprocess.Popen(args, cwd=root, env=runenv, stdout=stdout, stderr=stderr or subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + timeout
            while True:
                try:
                    code = p.wait(timeout=2)
                    break
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline or shutil.disk_usage(root).free < 2 * 1024**3:
                        os.killpg(p.pid, signal.SIGTERM)
                        try:
                            p.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(p.pid, signal.SIGKILL)
                            p.wait()
                        raise RuntimeError('deadline expired or 2 GiB disk reserve reached')
        finally:
            if stderr:
                stderr.close()
    print('exit:', code, flush=True)
    return code

def manifest(output):
    shell = "set -o pipefail; rg --files --hidden -0 -g '!/.git' -g '!/target/**' -g '!/docs/**' | sort -z | xargs -0 sha256sum"
    if command(['bash', '-c', shell], output):
        raise RuntimeError('input hash collection failed')

if shutil.disk_usage(root).free < 3 * 1024**3:
    raise RuntimeError('less than 3 GiB available before build; preserving 2 GiB reserve')
manifest('inputs-before.sha256')
command(['git', 'rev-parse', 'HEAD'], 'head.txt')
command(['git', 'status', '--short'], 'status-before.txt')
command(['git', 'diff', '--binary', '--', 'src', 'tests', 'Cargo.toml', 'Cargo.lock'], 'tracked-source.patch')
command(['rustc', '+stable', '-Vv'], 'rustc.txt')
command(['cargo', '+stable', '-V'], 'cargo.txt')
build = command(['cargo', '+stable', 'test', '--locked', '--no-run', '--lib', '--test', 'full_surface', '--message-format=json-render-diagnostics'], 'build.jsonl', 'build.stderr.log')
if build:
    manifest('inputs-after-build-failure.sha256')
    sys.exit(build)

found = {}
for line in (out / 'build.jsonl').read_text().splitlines():
    try: row = json.loads(line)
    except json.JSONDecodeError: continue
    if row.get('reason') != 'compiler-artifact' or not row.get('executable') or not row.get('profile', {}).get('test'):
        continue
    target = row.get('target', {}).get('name')
    if target in ('partitionline', 'full_surface'):
        found[target] = {'source': row['executable'], 'fresh': row.get('fresh'), 'package_id': row.get('package_id')}
if set(found) != {'partitionline', 'full_surface'}:
    raise RuntimeError(f'missing exact test artifacts: {found!r}')
for target, row in found.items():
    if row['fresh']:
        raise RuntimeError(f'build reported a fresh pre-existing {target} executable; no independent build provenance')
    source = pathlib.Path(row['source'])
    copy = out / f'{target}-candidate.elf'
    shutil.copyfile(source, copy)
    copy.chmod(0o700)
    digest = hashlib.sha256(copy.read_bytes()).hexdigest()
    if digest != hashlib.sha256(source.read_bytes()).hexdigest():
        raise RuntimeError(f'copied test executable mismatch: {target}')
    row.update({'retained': str(copy), 'sha256': digest, 'bytes': copy.stat().st_size})
(out / 'executables.json').write_text(json.dumps(found, indent=2) + '\n')

cases = [
 ('partitionline', 'protocol::cgheartbeat::tests::heartbeat_metadata_limit_is_shared_across_nested_assignments'),
 ('partitionline', 'protocol::cgheartbeat::tests::heartbeat_request_counts_and_strings_share_one_limit'),
 ('partitionline', 'protocol::cgheartbeat::tests::heartbeat_rejects_inflated_topic_count_before_allocation'),
 ('partitionline', 'protocol::cgheartbeat::tests::heartbeat_cooperative_decode_can_be_cancelled_after_yield'),
 ('partitionline', 'protocol::cgheartbeat::tests::heartbeat_large_valid_assignment_finishes_with_default_budget'),
 ('partitionline', 'group::tests::decoding_many_heartbeat_assignments_does_not_hold_the_caller'),
 ('full_surface', 'kip848_broker_heartbeat_interval_scheduling'),
]
results = []
for index, (target, name) in enumerate(cases, 1):
    args = [found[target]['retained'], '--exact', name, '--test-threads=1']
    started = time.monotonic()
    try:
        code = command(args, f'test-{index:02}.log', timeout=120, overrides={'RUST_MIN_STACK': '2097152'})
        timed_out = False
    except RuntimeError:
        code, timed_out = None, True
    data = (out / f'test-{index:02}.log').read_bytes()
    row = {'name': name, 'argv': args, 'exit_code': code, 'timed_out': timed_out, 'seconds': round(time.monotonic()-started, 3), 'log_sha256': hashlib.sha256(data).hexdigest(), 'exactly_one_pass': b'test result: ok. 1 passed;' in data}
    results.append(row)
(out / 'test-results.json').write_text(json.dumps(results, indent=2) + '\n')
clippy = command(['cargo', '+stable', 'clippy', '--locked', '--all-features', '--lib', '--', '-D', 'warnings'], 'clippy.log')
manifest('inputs-after.sha256')
same = (out/'inputs-before.sha256').read_bytes() == (out/'inputs-after.sha256').read_bytes()
summary = {'build_exit': build, 'tests': results, 'clippy_exit': clippy, 'same_visible_inputs': same, 'source_scope': 'visible files selected by rg excluding docs, .git and target; external dependencies and ignored files are not pinned'}
(out/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
if clippy or not same or any(row['exit_code'] != 0 or not row['exactly_one_pass'] for row in results):
    sys.exit(1)

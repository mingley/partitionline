from pathlib import Path
import hashlib, json, os, shutil, signal, subprocess, sys, time
B = Path('/workspace/work/broker-oidc')
SOURCE = Path('/workspace/work/broker-merged-source-fcac9d1d')
MANIFEST = B / 'live-configs-fcac9d1d-java411-temporal82-01/manifest.json'
RUN = B / 'live-runs-fcac9d1d-java411-temporal82-01'
FLOOR = 350 * 1024 * 1024
RESERVE = 48 * 1024 * 1024
os.umask(0o077)
os.sched_setaffinity(0, {0, 1})
interrupted = False

def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        while data := f.read(1048576):
            h.update(data)
    return h.hexdigest()

def write(p, obj):
    p.write_text(json.dumps(obj, indent=2) + '\n')
    os.chmod(p, 0o600)

def on_signal(signum, frame):
    global interrupted
    interrupted = True

signal.signal(signal.SIGTERM, on_signal)
signal.signal(signal.SIGINT, on_signal)
assert sha(MANIFEST) == '2464428289402c20e21443db75cc426a86c03abb6ad36aeb397eb191515b4685'
row = json.loads(MANIFEST.read_text())
assert row['steps'] == 82
output = Path(row['outer_command'][row['outer_command'].index('--output') + 1])
private = Path(row['outer_command'][row['outer_command'].index('--private-scratch') + 1])
assert not output.exists() and not private.exists() and not RUN.exists()
RUN.mkdir(mode=0o700)
producer_sha = sha(Path(__file__))

def artifacts():
    assert sha(MANIFEST) == '2464428289402c20e21443db75cc426a86c03abb6ad36aeb397eb191515b4685'
    config = Path(row['config'])
    assert sha(config) == row['config_sha256'] == '8be7d8aca291e062f486fa32e14649b03fe0bfc20a271ef10f085bb6112da5c5'
    cfg = json.loads(config.read_text())
    assert cfg['source_sha'] == 'fcac9d1d783b63890b3316601de72a4075973e10'
    assert all(not os.environ.get(k) for k in ['LD_PRELOAD', 'LD_LIBRARY_PATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'])
    for name in ['driver', 'outer', 'issuer']:
        assert sha(SOURCE / cfg[name + '_relative_path']) == cfg[name + '_sha256']
    for name in ['immutable_origin', 'normal_example_build']:
        assert sha(Path(cfg[name + '_receipt'])) == cfg[name + '_receipt_sha256']
    count = 0
    for peer in cfg['peers']:
        p = Path(peer['build_receipt'])
        assert sha(p) == peer['build_receipt_sha256']
        bound = json.loads(p.read_text())
        assert bound['source_sha'] == cfg['source_sha'] and bound['exit_code'] == 0
        for r in peer['runtime_inputs']:
            p = Path(r['path'])
            assert sha(p) == r['sha256'] and p.stat().st_size == r['bytes']
            assert oct(p.stat().st_mode & 0o7777) == r['full_mode']
            count += 1
        for r in peer['source_inputs']:
            assert sha(SOURCE / r['path']) == r['sha256']
    n = json.loads(Path(cfg['normal_example_build_receipt']).read_text())
    p = Path(n['executable'])
    assert n['source_sha'] == cfg['source_sha'] and n['exit_code'] == 0
    assert n['actual_Cargo_artifact']['target']['kind'] == ['example'] and not n['actual_Cargo_artifact']['profile']['test']
    assert sha(p) == n['executable_sha256'] and p.stat().st_mode & 0o7777 == 0o700
    original = B / 'live-configs-fcac9d1d-predeclared-01/manifest.json'
    assert sha(original) == '0639fc4f66058a657d14f45db113347e451efc62a60f0e1d47953d75d8e45515'
    old = json.loads(original.read_text())['cohorts']
    assert len(old) == 32
    for r in old:
        assert sha(Path(r['config'])) == r['config_sha256']
    return {'passed': True, 'runtime_input_rehashes': count, 'normal_sha256': n['executable_sha256'], 'config_sha256': sha(config), 'all_original_32_configs_unchanged': True}

before = artifacts()
free = shutil.disk_usage('/workspace').free
assert free - RESERVE >= FLOOR
write(RUN / 'launch-preflight.json', {'passed': True, 'source_sha': row['source_sha'], 'manifest_sha256': sha(MANIFEST), 'artifact_before': before, 'free_before': free, 'operational_reserve_bytes': RESERVE, 'projected_free': free - RESERVE, 'floor_bytes': FLOOR, 'reserve_limit': 'Operational forecast, not an arbitrary SDK-output hard cap.', 'command': row['outer_command'], 'producer_sha256': producer_sha, 'execution_rootGO': True})
print(json.dumps({'event': 'cohort-start', 'name': row['name'], 'steps': 82, 'free_bytes': free, 'command': row['outer_command']}), flush=True)
started = time.monotonic()
samples = []
abort = None
last_progress = started
log = RUN / 'outer.log'
with log.open('wb') as handle:
    child = subprocess.Popen(row['outer_command'], stdout=handle, stderr=subprocess.STDOUT, start_new_session=True, umask=0o077)
    while child.poll() is None:
        now = time.monotonic()
        current = shutil.disk_usage('/workspace').free
        samples.append({'elapsed_seconds': now - started, 'free_bytes': current})
        if abort is None and (interrupted or current < FLOOR or now - started > 310):
            abort = 'controlled-interruption' if interrupted else 'disk-floor' if current < FLOOR else 'outer-observed-overrun'
            child.send_signal(signal.SIGTERM)
        if now - last_progress >= 30:
            print(json.dumps({'event': 'cohort-pending', 'name': row['name'], 'elapsed_seconds': round(now - started, 2), 'free_bytes': current}), flush=True)
            last_progress = now
        time.sleep(.5)
    exit_code = child.wait()
after = artifacts()
v = json.loads((output / 'validation.json').read_text()) if (output / 'validation.json').exists() else None
outer = json.loads((output / 'outer-ownership.json').read_text()) if (output / 'outer-ownership.json').exists() else None
joined = bool(v and outer and not outer['remaining'] and all(r['joined'] for r in v['cleanup']))
audit_exit = None
if joined:
    with (RUN / 'mode-audit.log').open('wb') as handle:
        audit_exit = subprocess.run([sys.executable, str(B / 'audit-private-modes.py'), '--private-scratch', str(private), '--output', str(output / 'private-mode-inventory.json')], stdout=handle, stderr=subprocess.STDOUT, timeout=20).returncode
modes = json.loads((output / 'private-mode-inventory.json').read_text()) if (output / 'private-mode-inventory.json').exists() else None
passed = bool(exit_code == 0 and abort is None and v and v['passed'] and v['complete_source_unchanged'] and outer and outer['passed'] and joined and audit_exit == 0 and modes and modes['passed'])
result = {'passed': passed, 'source_sha': row['source_sha'], 'manifest_sha256': sha(MANIFEST), 'producer_sha256': producer_sha, 'command': row['outer_command'], 'steps_declared': 82, 'steps_observed': len(v['steps']) if v else 0, 'exit_code': exit_code, 'elapsed_seconds': time.monotonic() - started, 'minimum_free_bytes': min((r['free_bytes'] for r in samples), default=free), 'free_after': shutil.disk_usage('/workspace').free, 'artifact_before': before, 'artifact_after': after, 'complete_source_unchanged': bool(v and v['complete_source_unchanged']), 'all_cleanup_joined': joined, 'outer_passed': bool(outer and outer['passed']), 'private_paths_only_audit_passed': bool(modes and modes['passed']), 'private_path_count': modes['path_count'] if modes else None, 'failure_stage': v['failure_stage'] if v else 'missing-validator', 'failed_step': v['failed_step'] if v else None, 'abort_reason': abort, 'disk_samples': samples, 'outer_log_sha256': sha(log), 'validation_path': str(output / 'validation.json'), 'validation_sha256': sha(output / 'validation.json') if v else None, 'scope': 'One authorized revised Java4.1.2 signed-policy cohort. Actual SDK-local, broker-authentication, transport and unproved classifications remain distinct. No other live/Cargo or product/config mutation.'}
assert sha(Path(__file__)) == producer_sha
write(RUN / 'validation.json', result)
print(json.dumps({'event': 'cohort-closed', 'passed': passed, 'steps': result['steps_observed'], 'failure_stage': result['failure_stage'], 'failed_step': result['failed_step'], 'exit_code': exit_code, 'elapsed_seconds': round(result['elapsed_seconds'], 2), 'minimum_free_bytes': result['minimum_free_bytes'], 'free_after': result['free_after']}), flush=True)
raise SystemExit(0 if passed else 1)

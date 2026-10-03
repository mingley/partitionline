from pathlib import Path
import datetime
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time

root = Path('/workspace/work/client-share-assessment/whole-compat-12f43986')
source = Path('/workspace/work/client-share-compat-12f43986')
version = sys.argv[1]
cell = next(c for c in json.loads((source / 'tests/conformance/current-broker-cells.json').read_text())['cells'] if c['version'] == version)
out = root / 'image-pulls' / version
out.mkdir(parents=True, exist_ok=False)
shutil.copyfile(__file__, out / 'executed-pull-helper.py')
env = os.environ.copy()
for key in ['DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH']:
    env.pop(key, None)
env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
floor = 350 * 1024 * 1024
compile_reservation = 400 * 1024 * 1024
pull_expansion_budget = 900 * 1024 * 1024

def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def daemon_df(name):
    raw = subprocess.check_output(['curl', '-fsS', '--noproxy', '*', '--unix-socket', '/var/run/docker.sock', 'http://localhost/system/df'], env=env)
    (out / name).write_bytes(raw)
    return json.loads(raw)

before = daemon_df('docker-df-before.json')
before_ids = [image['Id'] for image in before['Images']]
before_images = json.loads(subprocess.check_output(['docker', 'image', 'inspect', *before_ids], env=env))
(out / 'images-before.json').write_text(json.dumps(before_images, indent=2) + '\n')
free_before = shutil.disk_usage('/workspace').free
assert free_before >= floor + compile_reservation + pull_expansion_budget, free_before
launch = {'schema_version': 1, 'version': version, 'reference': cell['reference'], 'captured_at': timestamp(), 'cpuset': '0,1', 'free_bytes_before': free_before, 'global_floor_bytes': floor, 'compile_reservation_bytes': compile_reservation, 'initial_pull_expansion_budget_bytes': pull_expansion_budget, 'initial_budget_basis': 'Conservative 900MiB per missing image, more than twice the cached current image logical size; actual layers and filesystem deltas are measured, never assumed zero.', 'docker_host': env['DOCKER_HOST'], 'argv': ['docker', 'pull', '--platform', 'linux/amd64', cell['reference']]}
(out / 'launch.json').write_text(json.dumps(launch, indent=2) + '\n')
minimum = free_before
stopped = False
with (out / 'pull.stdout.log').open('wb') as stdout, (out / 'pull.stderr.log').open('wb') as stderr, (out / 'disk-monitor.jsonl').open('w') as monitor:
    process = subprocess.Popen(launch['argv'], env=env, stdout=stdout, stderr=stderr, start_new_session=True)
    while True:
        free = shutil.disk_usage('/workspace').free
        minimum = min(minimum, free)
        monitor.write(json.dumps({'at': timestamp(), 'free_bytes': free, 'own_pull_pid': process.pid}) + '\n')
        monitor.flush()
        if free < floor + compile_reservation and process.poll() is None:
            stopped = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
        result = process.poll()
        if result is not None:
            break
        time.sleep(3)
after = daemon_df('docker-df-after.json')
identity_result = subprocess.run(['docker', 'image', 'inspect', cell['reference']], env=env, capture_output=True)
(out / 'image-inspect-after.stdout.json').write_bytes(identity_result.stdout)
(out / 'image-inspect-after.stderr.log').write_bytes(identity_result.stderr)
identity = json.loads(identity_result.stdout)[0] if identity_result.returncode == 0 else None
known_layers = {layer for image in before_images for layer in image.get('RootFS', {}).get('Layers', [])}
new_layers = identity.get('RootFS', {}).get('Layers', []) if identity else []
receipt = {'schema_version': 1, 'version': version, 'reference': cell['reference'], 'exit_code': result, 'stopped_for_reservation_guard': stopped, 'minimum_observed_free_bytes': minimum, 'free_bytes_after': shutil.disk_usage('/workspace').free, 'global_floor_bytes': floor, 'docker_layers_bytes_before': before['LayersSize'], 'docker_layers_bytes_after': after['LayersSize'], 'docker_incremental_unique_layers_bytes': after['LayersSize'] - before['LayersSize'], 'filesystem_free_space_delta_bytes': shutil.disk_usage('/workspace').free - free_before, 'filesystem_delta_scope': 'Shared filesystem observation; concurrent authorized writers may contribute. Docker daemon LayersSize delta separately accounts for actual image layer storage.', 'image_inspect_exit_code': identity_result.returncode, 'new_image_logical_bytes': identity['Size'] if identity else None, 'rootfs_layer_count': len(new_layers), 'rootfs_layers_shared_with_existing_images': sorted(set(new_layers) & known_layers), 'rootfs_layers_new_to_existing_images': [layer for layer in new_layers if layer not in known_layers], 'image_id': identity['Id'] if identity else None, 'repo_digests': identity.get('RepoDigests') if identity else None, 'captured_at': timestamp(), 'partial_image_state_preserved_on_failure': True}
(out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt), flush=True)
assert not stopped and result == 0
assert identity and identity['Os'] == 'linux' and identity['Architecture'] == 'amd64'
assert 'apache/kafka@' + cell['digest'] in identity['RepoDigests']
assert minimum >= floor

from pathlib import Path
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys

out, expected_id = Path(sys.argv[1]), sys.argv[2]
shutil.copyfile(__file__, out / 'executed-image-cleanup-helper.py')
retention = json.loads((out / 'retention.json').read_text())
assert retention['image_id'] == expected_id
assert retention['all_layer_tar_hashes_equal_rootfs_diff_ids'] and retention['image_config_sha_equals_image_id']
assert hashlib.sha256(Path(retention['archive_path']).read_bytes()).hexdigest() == retention['archive_sha256']
env = os.environ.copy()
for key in ['DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH']:
    env.pop(key, None)
env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
refs = subprocess.check_output(['docker', 'container', 'ls', '-a', '--filter', 'ancestor=' + expected_id, '--format', '{{.ID}}'], env=env).decode().split()
assert not refs, refs
active = []
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit() or int(proc.name) in [os.getpid(), os.getppid()]:
        continue
    try:
        command = (proc / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
    except (OSError, PermissionError):
        continue
    if ('docker exec' in command or 'docker run' in command) and (expected_id in command or expected_id.removeprefix('sha256:')[:12] in command):
        active.append({'pid': int(proc.name), 'command': command})
assert not active, active
before = json.loads(subprocess.check_output(['docker', 'image', 'inspect', 'apache/kafka:3.9.1'], env=env))[0]
free = shutil.disk_usage('/workspace').free
argv = ['docker', 'image', 'rm', expected_id]
result = subprocess.run(argv, env=env, capture_output=True, text=True)
(out / 'exact-image-remove.stdout.log').write_text(result.stdout)
(out / 'exact-image-remove.stderr.log').write_text(result.stderr)
assert result.returncode == 0
missing = subprocess.run(['docker', 'image', 'inspect', expected_id], env=env, capture_output=True)
assert missing.returncode == 1
remaining = json.loads(subprocess.check_output(['docker', 'image', 'inspect', 'apache/kafka:3.9.1'], env=env))[0]
assert remaining['Id'] == before['Id'] and remaining['RootFS'] == before['RootFS']
receipt = {'schema_version': 1, 'authorized_scope': 'Root explicitly authorized reversible completed4.3 and completed4.1 exactimage lifecycle after validated retention; no broad prune or3.9 removal.', 'argv': argv, 'exit_code': result.returncode, 'removed_exact_image_id': expected_id, 'container_references_before': refs, 'active_docker_run_exec_process_references': active, 'retained_archive_sha256': retention['archive_sha256'], 'retained_archive_config_and_all_layers_verified': True, 'free_bytes_before': free, 'free_bytes_after': shutil.disk_usage('/workspace').free, 'actual_shared_filesystem_free_delta_bytes': shutil.disk_usage('/workspace').free - free, 'remaining_3_9_image_id': remaining['Id'], 'remaining_3_9_rootfs_unchanged': True, 'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(out / 'exact-image-cleanup.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt), flush=True)

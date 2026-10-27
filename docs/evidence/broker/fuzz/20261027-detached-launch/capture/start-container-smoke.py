import json
import os
from pathlib import Path
import subprocess

repo = Path('/workspace/partitionline')
build = Path('/workspace/work/qualification-20261027/fuzz-build-05')
output = Path('/workspace/work/broker-fuzz-container-20261027')
output.mkdir()
(output / 'supervisor-tests').mkdir()
checks = Path('/workspace/work/qualification-checks-20261027')
env = dict(os.environ)
for name in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH'):
    env.pop(name, None)
docker = ['docker', '--host=unix:///var/run/docker.sock']
image = 'sha256:3f3bce42ebbf8f78bb2c2d54ec8067a92eeb225e37a9dfb37d16b7d9a3d49ea7'
base = ['--init', '--read-only', '--network=none', '--restart=no', '--cpus=3', '--memory=4g', '--pids-limit=128',
        '--user', f'{os.getuid()}:{os.getgid()}', '--env', 'HOME=/tmp',
        '--tmpfs', '/tmp:rw,nosuid,mode=1777,size=134217728',
        '--mount', f'type=bind,src={repo},dst={repo},readonly',
        '--mount', f'type=bind,src={build},dst={build},readonly',
        '--mount', f'type=bind,src={output},dst={output}', '--workdir', str(repo)]
command = [*docker, 'run', '--detach', '--name', 'pl-broker-fuzz-controls-20261027', *base,
           '--env', f'PL_BROKER_FUZZ_BUILD={build}', '--env', f'PL_BROKER_FUZZ_TEST_OUTPUT={output}/supervisor-tests',
           image, 'python3', '-B', '-m', 'unittest', 'discover', '-s', 'tests/ci', '-p', 'test_broker_fuzz_campaign.py', '-v']
result = subprocess.run(command, env=env, capture_output=True, text=True)
(checks / 'container-smoke-launch.log').write_text(result.stdout + result.stderr)
assert result.returncode == 0, result.stderr
container = result.stdout.strip()
(checks / 'container-launch-settings.json').write_text(json.dumps({'docker': docker, 'image': image, 'base': base,
    'build': str(build), 'output': str(output), 'smoke_container': container, 'command': command}, indent=2) + '\n')
print(json.dumps({'container_id': container, 'kind': 'real nine-test supervisor control run'}))

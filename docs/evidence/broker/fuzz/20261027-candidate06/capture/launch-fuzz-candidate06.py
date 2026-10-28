import json
import os
from pathlib import Path
import subprocess
import sys

study = Path('/workspace/work/qualification-checks-20261027/candidate06')
study.mkdir(exist_ok=True)
settings = json.loads(Path('/workspace/work/qualification-checks-20261027/container-launch-settings.json').read_text())
build = '/workspace/work/qualification-20261027/fuzz-build-06'
output = Path(settings['output'])
base = [arg.replace('fuzz-build-05', 'fuzz-build-06') for arg in settings['base']]
env = dict(os.environ)
for key in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH'):
    env.pop(key, None)
docker = settings['docker']

def call(arguments):
    return subprocess.check_output(docker + arguments, env=env, text=True).strip()

def state(name):
    return json.loads(call(['inspect', '--format', '{{json .State}}', name]))

def logs(name, path):
    result = subprocess.run(docker + ['logs', name], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert result.returncode == 0
    path.write_text(result.stdout)
    return result.stdout

action = sys.argv[1]
if action == 'controls':
    assert json.loads(Path(build, 'build.json').read_text())['state'] == 'built'
    controls = output / 'controls06'
    controls.mkdir(exist_ok=False)
    name = 'pl-broker-fuzz-controls-20261027-06'
    args = ['run', '--detach', '--name', name] + base + [
        '--env', 'PL_BROKER_FUZZ_BUILD=' + build,
        '--env', 'PL_BROKER_FUZZ_TEST_OUTPUT=' + str(controls),
        settings['image'], 'python3', '-B', '-m', 'unittest', 'discover', '-s', 'tests/ci',
        '-p', 'test_broker_fuzz_campaign.py', '-v']
    container = call(args)
    receipt = {'container_id': container, 'container_name': name, 'command': docker + args, 'state': state(name)}
    (study / 'controls-launch.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))
elif action == 'inspect-controls':
    name = 'pl-broker-fuzz-controls-20261027-06'
    observed = state(name)
    log = logs(name, study / 'controls.log')
    (study / 'controls-state.json').write_text(json.dumps(observed, indent=2) + '\n')
    print(json.dumps(observed))
    print(log[-2400:])
elif action == 'campaign':
    name = 'pl-broker-fuzz-controls-20261027-06'
    observed = state(name)
    assert observed['Status'] == 'exited' and observed['ExitCode'] == 0 and not observed['OOMKilled']
    log = logs(name, study / 'controls.log')
    assert 'Ran 9 tests' in log and log.rstrip().endswith('OK')
    campaign = output / 'campaign-04'
    assert not campaign.exists()
    name = 'pl-broker-fuzz-campaign-20261027-04'
    args = ['run', '--detach', '--name', name] + base + [settings['image'], 'python3', '-B',
        'scripts/broker-fuzz-campaign.py', 'run', '--build', build, '--output', str(campaign),
        '--kind', 'campaign', '--seconds', '86400']
    container = call(args)
    receipt = {'container_id': container, 'container_name': name, 'image': settings['image'],
        'build': build, 'output': str(campaign), 'command': docker + args, 'state': state(name),
        'controls': {'tests': 9, 'exit': 0}}
    (study / 'campaign-launch.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))
elif action == 'observe-campaign':
    import datetime
    import shutil
    name = 'pl-broker-fuzz-campaign-20261027-04'
    observed = state(name)
    limits = json.loads(call(['inspect', '--format', '{{json .HostConfig}}', name]))
    limits = {key: limits[key] for key in ('NetworkMode', 'NanoCpus', 'Memory', 'PidsLimit', 'ReadonlyRootfs', 'Init', 'RestartPolicy')}
    probe = "import json; from pathlib import Path; print(json.dumps({n:Path('/sys/fs/cgroup',n).read_text().strip() for n in ['cpu.max','memory.max','pids.max']}))"
    cgroups = json.loads(call(['exec', name, 'python3', '-c', probe])) if observed['Running'] else None
    campaign = output / 'campaign-04'
    snapshot = study / 'launch-snapshot'
    snapshot.mkdir(exist_ok=False)
    for filename in ('manifest.json', 'frozen.json', 'jobs.json', 'status.json', 'telemetry.jsonl'):
        shutil.copyfile(campaign / filename, snapshot / filename)
    for target in ('parse_header', 'decode_kraft', 'controller_respond', 'recover_journal', 'recover_epoch'):
        (snapshot / target).mkdir()
        shutil.copyfile(campaign / target / 'fuzzer.log', snapshot / target / 'fuzzer.log')
    telemetry = [json.loads(line) for line in (snapshot / 'telemetry.jsonl').read_text().splitlines()]
    receipt = {'observed_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'state': observed, 'limits': limits, 'actual_cgroup': cgroups,
        'latest_telemetry': telemetry[-1], 'completed_qualification': False}
    (study / 'campaign-observation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))
else:
    raise ValueError(action)

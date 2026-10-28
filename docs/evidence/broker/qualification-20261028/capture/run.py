import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

STUDY = Path(__file__).resolve().parent
ENV = dict(os.environ)
ENV.update(RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo',
    PATH='/workspace/work/cargo/bin:' + ENV['PATH'],
    CARGO_TARGET_DIR='/workspace/work/qualification-20261027/broker-check-target',
    CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')

def run(name, command):
    log = STUDY / (name + '.log')
    assert not log.exists(), log
    base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    committed = subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', 'src', 'partitionline-broker/src']).returncode == 0
    receipt = {'command': command, 'source_sha': base if committed else None, 'base_source_sha': base,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    before = {}
    for prefix in ['partitionline-broker/src', 'src']:
        for path in sorted(Path(prefix).rglob('*.rs')):
            before[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    receipt['source_files'] = before
    print('Running ' + name, flush=True)
    start = time.monotonic()
    with log.open('x') as stream:
        result = subprocess.run(command, env=ENV, stdout=stream, stderr=subprocess.STDOUT)
    text = log.read_text()
    receipt.update(exit=result.returncode, elapsed_seconds=time.monotonic() - start,
        results=re.findall(r'^test result:.*$', text, re.M),
        failures=re.findall(r'^test (.*?) \.\.\. FAILED$', text, re.M),
        log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    for name_, digest in before.items():
        assert hashlib.sha256(Path(name_).read_bytes()).hexdigest() == digest, name_
    executables = []
    for index, match in enumerate(re.finditer(r'Running .*?\((/[^)]+)\)', text)):
        binary = Path(match[1])
        if binary.is_file():
            retained = STUDY / (name + f'-{index}.elf')
            shutil.copyfile(binary, retained)
            executables.append({'path': str(retained), 'sha256': hashlib.sha256(retained.read_bytes()).hexdigest()})
    receipt['executables'] = executables
    (STUDY / (name + '.json')).write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({k: receipt[k] for k in ['exit', 'elapsed_seconds', 'results', 'failures']}), flush=True)
    return result.returncode

if __name__ == '__main__':
    name = sys.argv[1]
    args = sys.argv[2:]
    run(name, ['cargo', '+stable'] + args)

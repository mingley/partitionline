import gzip
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

root = Path('/workspace/partitionline')
work = Path(__file__).resolve().parent
env = os.environ.copy()
env.update({'RUSTUP_HOME':'/workspace/work/rustup', 'CARGO_HOME':'/workspace/work/cargo',
            'PATH':'/workspace/work/cargo/bin:' + env['PATH'],
            'CARGO_TARGET_DIR':str(work / 'cargo-target'), 'CARGO_BUILD_JOBS':'1',
            'CARGO_INCREMENTAL':'0', 'CARGO_PROFILE_DEV_DEBUG':'0', 'CARGO_PROFILE_TEST_DEBUG':'0'})
floor = 1610612736
records = []
inputs = {}
for p in [root/'Cargo.toml',root/'Cargo.lock',root/'README.md',root/'CHANGELOG.md',*sorted((root/'docs').glob('*.md')),root/'scripts/check-doc-examples.py',root/'scripts/check-package-docs.py',*sorted((root/'tests/docs').glob('test_*.py'))]:
    inputs[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
(work/'actual-inputs-before.json').write_text(json.dumps(inputs,indent=2)+'\n')

def run(name, cmd):
    start = time.monotonic()
    samples = []
    with (work/(name+'.log')).open('wb') as output:
        process = subprocess.Popen(cmd,cwd=root,env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        stopped = None
        while process.poll() is None:
            available = shutil.disk_usage(root).free
            samples.append(available)
            if available < floor or time.monotonic()-start > 60:
                stopped = 'disk floor' if available < floor else '60s timeout'
                import signal
                os.killpg(process.pid,signal.SIGTERM)
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                break
            time.sleep(.1)
        code = process.wait()
    receipt = {'name':name,'command':cmd,'exit':code,'stopped':stopped,
               'elapsed_seconds':time.monotonic()-start,'disk_floor_bytes':floor,
               'minimum_free_bytes':min(samples) if samples else shutil.disk_usage(root).free,
               'log_sha256':hashlib.sha256((work/(name+'.log')).read_bytes()).hexdigest()}
    records.append(receipt)
    (work/(name+'.json')).write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt),flush=True)
    if code != 0 or stopped:
        raise SystemExit(1)

run('docs-tests',['python3','-B','-m','unittest','discover','-s','tests/docs','-p','test_*.py','-v'])
run('package',['cargo','+stable','package','--locked','--allow-dirty','--no-verify'])
archive = work/'cargo-target/package/partitionline-0.1.0.crate'
unpacked = work/'actual-extracted'
unpacked.mkdir()
with tarfile.open(archive) as packed:
    packed.extractall(unpacked,filter='data')
run('actual-navigation',['python3','-B',str(root/'scripts/check-package-docs.py'),str(unpacked/'partitionline-0.1.0'),'--report',str(work/'actual-navigation-report.json')])
after = {name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in inputs}
(work/'actual-inputs-after.json').write_text(json.dumps(after,indent=2)+'\n')
assert after == inputs
summary = {'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
           'archive_bytes':archive.stat().st_size,'inputs_unchanged':True,
           'qualification':'actual dirty source package; no library compilation, package consumer or publish validation',
           'commands':records}
(work/'actual-summary.json').write_text(json.dumps(summary,indent=2)+'\n')

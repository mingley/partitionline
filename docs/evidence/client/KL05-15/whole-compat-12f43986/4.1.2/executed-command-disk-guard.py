from pathlib import Path
import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import time

report, version = Path(sys.argv[1]), sys.argv[2]
argv = ['bash', 'scripts/ci-broker-compatibility.sh', version]
floor = 350 * 1024 * 1024
cancellation_margin = 350 * 1024 * 1024
shutil.copyfile(__file__, report / 'executed-command-disk-guard.py')
minimum = shutil.disk_usage('/workspace').free
stopped = False
with (report / 'original-runner.log').open('wb') as output, (report / 'runtime-disk-monitor.jsonl').open('w') as monitor:
    process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    count = 0
    while True:
        free = shutil.disk_usage('/workspace').free
        minimum = min(minimum, free)
        if count % 10 == 0:
            monitor.write(json.dumps({'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'free_bytes': free, 'own_runner_pid': process.pid}) + '\n')
            monitor.flush()
        if free < floor + cancellation_margin and process.poll() is None:
            stopped = True
            monitor.write(json.dumps({'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'free_bytes': free, 'stopped_own_runner_for_disk_guard': True}) + '\n')
            monitor.flush()
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
        result = process.poll()
        if result is not None:
            break
        count += 1
        time.sleep(0.1)
receipt = {'schema_version': 1, 'argv': argv, 'exit_code': result, 'cpuset': '0,1', 'unchanged_original_script': True, 'monitor_poll_seconds': 0.1, 'monitor_record_seconds': 1, 'global_floor_bytes': floor, 'cancellation_margin_bytes': cancellation_margin, 'minimum_observed_free_bytes': minimum, 'stopped_for_disk_guard': stopped}
(report / 'original-runner-command.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt), flush=True)
sys.exit(result if result >= 0 else 128 - result)

import datetime, hashlib, json, os, pathlib, platform, subprocess, time
out = pathlib.Path('/workspace/work/reliability-20261028/fault-observer-01')
out.mkdir(exist_ok=False)
tmp = out / 'tmp'
tmp.mkdir()
binary = pathlib.Path('/workspace/work/qualification-20261027/broker-check-target/debug/deps/fault_campaign-76f44d9fa1cc748a')
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
expected = '0a21aaaec496e2289e6df9927794a90ddec0455ffdeefe85df6d9c40f2453010'
assert sha(binary) == expected
command = [str(binary), '--nocapture', '--test-threads=1', 'seeded_faults_preserve_committed_outcomes']
env = dict(os.environ, TMPDIR=str(tmp), PL_FAULT_CAMPAIGN_OUT=str(out / 'report.json'))
start = datetime.datetime.now(datetime.timezone.utc).isoformat()
t = time.monotonic()
with (out / 'output.log').open('xb') as log:
    result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=180, preexec_fn=lambda: os.sched_setaffinity(0, {min(os.sched_getaffinity(0))}))
end = datetime.datetime.now(datetime.timezone.utc).isoformat()
report = json.loads((out / 'report.json').read_text()) if (out / 'report.json').exists() else None
receipt = {'command': command, 'started_utc':start, 'finished_utc':end, 'elapsed_seconds':time.monotonic()-t, 'exit_code':result.returncode, 'binary_sha256':sha(binary), 'binary_source_receipt':'/workspace/work/goal-qualification-20261028/broker-default-group-fixed.json', 'test_file_sha256':sha(pathlib.Path('/workspace/partitionline/partitionline-broker/tests/fault_campaign.rs')), 'host_machine':platform.machine(), 'cpu_affinity':min(os.sched_getaffinity(0)), 'cpu_quota':pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip(), 'log_sha256':sha(out / 'output.log'), 'report_sha256':sha(out / 'report.json') if report else None, 'scope':'Local seeded journal behavior, not live crash injection, fsync error injection, broker production readiness, nor a multi-broker campaign.'}
(out / 'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'receipt':receipt, 'report':report},indent=2))
raise SystemExit(result.returncode)

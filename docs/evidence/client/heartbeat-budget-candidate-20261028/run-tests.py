import hashlib, json, os, pathlib, subprocess, sys, time
root = pathlib.Path('/workspace/work/goal-qualification-20261028/parser-budget-validation-01')
lib = root / 'partitionline-b07aa256026bc081'
surface = root / 'full_surface-e102b38d1447c2a3'
cases = [
  (lib, 'protocol::cgheartbeat::tests::heartbeat_metadata_limit_is_shared_across_nested_assignments'),
  (lib, 'protocol::cgheartbeat::tests::heartbeat_request_counts_and_strings_share_one_limit'),
  (lib, 'protocol::cgheartbeat::tests::heartbeat_rejects_inflated_topic_count_before_allocation'),
  (lib, 'protocol::cgheartbeat::tests::heartbeat_cooperative_decode_can_be_cancelled_after_yield'),
  (lib, 'protocol::cgheartbeat::tests::heartbeat_large_valid_assignment_finishes_with_default_budget'),
  (lib, 'group::tests::decoding_many_heartbeat_assignments_does_not_hold_the_caller'),
  (surface, 'kip848_broker_heartbeat_interval_scheduling'),
]
env = os.environ.copy()
env['RUST_MIN_STACK'] = '2097152'
results = []
for index, (binary, name) in enumerate(cases):
    started = time.monotonic()
    command = [str(binary), '--exact', name, '--test-threads=1']
    try:
        result = subprocess.run(command, cwd='/workspace/work/goal-qualification-20261028/parser-budget-prototype', env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        output, code, timeout = result.stdout, result.returncode, False
    except subprocess.TimeoutExpired as error:
        output, code, timeout = error.stdout or b'', None, True
    log = root / f'test-{index + 1:02}.log'
    log.write_bytes(output)
    row = {'name': name, 'argv': command, 'exit_code': code, 'timeout': timeout, 'seconds': round(time.monotonic() - started, 3), 'log': log.name, 'log_sha256': hashlib.sha256(output).hexdigest(), 'exactly_one_pass': b'test result: ok. 1 passed;' in output}
    results.append(row)
    print(json.dumps(row), flush=True)
(root / 'test-results.json').write_text(json.dumps(results, indent=2) + '\n')
if any(row['exit_code'] != 0 or not row['exactly_one_pass'] for row in results):
    sys.exit(1)

import hashlib, importlib.util, json, os, pathlib, subprocess, time
root = pathlib.Path('/workspace/work/goal-qualification-20261028/producer-diagnostic-v2')
spec = importlib.util.spec_from_file_location('receipt_runner', '/workspace/work/goal-qualification-20261028/run-v2.py')
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
build = json.loads(pathlib.Path('/workspace/work/goal-qualification-20261028/producer-diagnostic-build-02.json').read_text())
before = mod.inputs()
if before != build['input_files'] or not build['inputs_stable'] or build['exit'] != 0:
    raise SystemExit('source input mismatch or invalid build receipt')
binaries = {'unit': 'unit.elf', 'full': 'full-surface.elf', 'buffer': 'buffer-ownership.elf'}
expected = {'unit': '71a187fb55da0ce2174ce8a71b78d3f9e6aebd44ad6912108d7a64b53640d09b', 'full': '712cadeb733be208da7e8fdfbe5d7f84a8f58e4a1209be1f310717235cfa07f5', 'buffer': '16a24585fd2aae01f2a3b9024a5b3232643682f3a6e8835415ef0ecde940fec8'}
for key, filename in binaries.items():
    if mod.sha256(root / filename) != expected[key]: raise SystemExit('binary mismatch: ' + key)
cases = [
('unit', 'producer::tests::cancelled_retry_batch_completes_records_and_releases_accounting'),
('unit', 'producer::tests::cancelling_current_retry_accounts_for_it_and_the_remaining_batch'),
('unit', 'producer::tests::closed_transport_retry_does_not_redefine_local_shutdown'),
('unit', 'producer::tests::closed_worker_retries_keep_pending_and_do_not_remove_replacement'),
('unit', 'producer::tests::retired_worker_handle_keeps_transfer_alive_and_preserves_other_slots'),
('buffer', 'presend_failure_write_retry_exhaustion_releases_permit'),
('full', 'idempotent_new_admission_cannot_overtake_pre_send_retry'),
('full', 'idempotent_lost_ack_single_batch_retry_succeeds_without_duplicate'),
('full', 'idempotent_lost_ack_several_inflight_batches_same_partition'),
('full', 'idempotent_lost_ack_several_inflight_appended_before_disconnect'),
('full', 'idempotent_lost_ack_with_progressing_partition'),
('full', 'idempotent_lost_ack_multi_record_batch_retries_without_duplicate'),
('full', 'idempotent_lost_ack_ambiguous_expiration_remains_explicit'),
]
results = []
mem_before = pathlib.Path('/sys/fs/cgroup/memory.events').read_text()
for index, (key, name) in enumerate(cases, 1):
    start = time.monotonic(); out = root / f'case-{index:02}.log'
    command = [str(root / binaries[key]), '--exact', name, '--test-threads=1']
    try:
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=45)
        output, code = proc.stdout, proc.returncode
    except subprocess.TimeoutExpired as error:
        output, code = (error.stdout or b'') + b'\nTimed out at 45 seconds\n', 124
    out.write_bytes(output)
    text = output.decode(errors='replace')
    selected = 'running 1 test' in text
    results.append({'name': name, 'binary_sha256': expected[key], 'command': command,
        'exit': code, 'selected_one': selected, 'seconds': time.monotonic()-start,
        'log': out.name, 'log_sha256': mod.sha256(out)})
    print(f'{index}/{len(cases)} {name}: exit={code} selected_one={selected}', flush=True)
    if __import__('shutil').disk_usage(root).free < 2 * 1024**3:
        print('stopping: disk reserve', flush=True); break
end = mod.inputs()
receipt = {'build_receipt': '/workspace/work/goal-qualification-20261028/producer-diagnostic-build-02.json',
    'base_head': build['base_source_sha'], 'source_sha': None, 'sparse_dirty_candidate': True,
    'source_inputs_match_build_before': before == build['input_files'], 'inputs_stable': before == end,
    'patch_sha256': mod.sha256(pathlib.Path('/workspace/work/producer-retry-candidate-v2.patch')),
    'binaries': expected, 'cases': results, 'memory_events_before': mem_before,
    'memory_events_after': pathlib.Path('/sys/fs/cgroup/memory.events').read_text(),
    'disk_free_bytes_after': __import__('shutil').disk_usage(root).free}
(root / 'results.json').write_text(json.dumps(receipt, indent=2) + '\n')

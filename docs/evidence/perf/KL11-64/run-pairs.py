import json, os, pathlib, statistics, subprocess, sys, time

root = pathlib.Path('/workspace/work/pending-sparse-memory')
os.sched_setaffinity(0, {2})
env = os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup', PATH='/workspace/work/cargo/bin:' + env['PATH'], CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1')
frozen_root = pathlib.Path('/workspace/work/consumer-pending')
sources = {'A': '072ef8f90cec41bdf8c60243c47bdfb2c030584c', 'B': '6f181217ac9555de66012b8609eacb890d7507f6'}
hashes = {'A':'973f3344c7569fb40b9911276c899de08c569dd360e62982d47d48ff231865f2','B':'9c9e4979ca07b63557bda788b2dbf76ec65218208f805a0eb418b4af14ed400f'}
rows = []

def percentile(xs, p):
    xs = sorted(xs); pos = (len(xs)-1)*p; lo = int(pos)
    return xs[lo] + (xs[min(lo+1, len(xs)-1)] - xs[lo])*(pos-lo)

for cell in ['nb-fetch-1000p']:
    for pair in range(5):
        for variant in ('AB' if pair%2 == 0 else 'BA'):
            name = 'baseline' if variant == 'A' else 'candidate'
            cwd = frozen_root/name
            binary = frozen_root/f'{name}-bin'/'runtime'
            out = root/'paired'/cell/f'pair-{pair:02}-{variant}'
            if out.exists():
                raise RuntimeError(f'Output already exists: {out}; no replacement')
            out.mkdir(parents=True)
            cmd = ['taskset', '-c', '3', str(binary), '--cell', cell, '--out', str(out), '--repetitions', '1']
            samples = []; confirmed = False; confirm_time = None; start = time.monotonic_ns()
            with (root/f'pair-{pair:02}-{variant}.log').open('w') as log:
                pidfile = out/'runtime.pid'
                launch_cmd = ['taskset', '-c', '3', str(root/'low-rss-launcher'), str(pidfile)] + cmd[3:]
                p = subprocess.Popen(launch_cmd, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
                proc = None
                while True:
                    try:
                        if proc is None and pidfile.exists():
                            proc = pathlib.Path('/proc')/str(int(pidfile.read_text().strip()))
                        if proc is None:
                            raise FileNotFoundError('Runtime child PID not emitted yet')
                        if not confirmed and os.readlink(proc/'exe') == str(binary):
                            confirmed = True; confirm_time = time.monotonic_ns()-start
                        if confirmed:
                            smaps = (proc/'smaps_rollup').read_text()
                            rss = int(next(line.split()[1] for line in smaps.splitlines() if line.startswith('Rss:')))*1024
                            status = (proc/'status').read_text()
                            hwm = int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmHWM:')))*1024
                            samples.append({'time_ns': time.monotonic_ns()-start, 'rss_bytes': rss, 'hwm_bytes': hwm})
                    except (FileNotFoundError, ProcessLookupError, StopIteration):
                        pass
                    pid, status, usage = os.wait4(p.pid, os.WNOHANG)
                    if pid:
                        p.returncode = os.waitstatus_to_exitcode(status)
                        break
                    time.sleep(.001)
            intervals = [(b['time_ns']-a['time_ns'])/1e6 for a,b in zip(samples,samples[1:])]
            observation = {'method': 'CPU3 low-RSS C launcher forks a fresh low-current-RSS child before exec(runtime), removing Python pre-exec RUSAGE_SELF floor. CPU2 observer reads exact Rust child PID file, confirms /proc/childPID/exe frozen runtime, then samples smaps_rollup Rss and persistent status VmHWM with ~1ms sleeps through lifetime. Initialization, measured phase and teardown included; wait4 launcher+descendants retained separately.', 'exe_confirmed': confirmed, 'observed_runtime_pid': int(pidfile.read_text().strip()), 'launcher_pid': p.pid, 'exe_confirmed_at_ns': confirm_time, 'requested_sleep_ms': 1, 'observer_duration_ns': time.monotonic_ns()-start, 'sample_count': len(samples), 'sample_peak_rss_bytes': max((s['rss_bytes'] for s in samples), default=0), 'client_vmhwm_bytes': max((s['hwm_bytes'] for s in samples), default=0), 'wait4_peak_rss_bytes': int(usage.ru_maxrss)*1024, 'wait4_user_cpu_seconds': usage.ru_utime, 'wait4_system_cpu_seconds': usage.ru_stime, 'actual_interval_ms': {'median': statistics.median(intervals) if intervals else None, 'p95': percentile(intervals,.95) if intervals else None, 'max': max(intervals) if intervals else None}, 'samples': samples, 'exit_code': p.returncode}
            (out/'external-rss.json').write_text(json.dumps(observation,indent=2)+'\n')
            assert p.returncode == 0, f'Runtime failed {cell}/{pair}/{variant}; all data retained'
            assert confirmed and samples
            path = out/f'{cell}-rep0.result.json'
            j = json.loads(path.read_text()); e = j['execution']; r = j['measurements']['client_resources']
            assert j['provenance']['source']['git_commit'] == sources[variant] and not j['provenance']['source']['dirty_tree']
            assert not e['mismatched'] and not e['validation_failures'] and not j['measurements']['errors']
            assert e['records_consumed'] == e['fetched_records'] == e['records_offered'] == 10000 and e['fetch_rounds'] == e['fetch_requests'] == 1
            assert e['per_partition_delivered'] == [{'partition': p, 'records': 10} for p in range(1000)]
            assert j['provenance']['binary']['sha256'] == hashes[variant]
            assert j['provenance']['config']['sha256'] == '098e5aade2526858abc5d9c0295f162e7f93f4d8ef1fd89d20e78770307de9a1'
            row = {'cell': cell, 'pair': pair, 'variant': variant, 'cpu_ns_per_round': e['cpu_ns_per_round'], 'allocs_per_round': e['allocs_per_round'], 'allocated_bytes_per_round': r['allocations']['total_allocated_bytes']/e['fetch_rounds'], 'cpu_ns_per_record': r['cpu_ns_per_record'], 'allocations_per_record': r['allocations']['allocations_per_record'], 'allocation_count': r['allocations']['allocation_count'], 'allocated_bytes_per_record': r['allocations']['total_allocated_bytes']/e['records_consumed'], 'process_peak_rss_bytes': r['rss']['process_peak_rss_bytes'], 'phase_sampled_peak_rss_bytes': r['rss']['peak_rss_bytes'], 'phase_average_rss_bytes': r['rss']['average_rss_bytes'], 'rec_s': j['measurements']['throughput']['records_per_second'], 'p99_us': j['measurements']['latency']['p99'], 'verified_records': e['records_consumed'], 'returned_records': e.get('returned_records'), 'fetched_records': e['fetched_records'], 'fetch_rounds': e['fetch_rounds'], 'mismatched': e['mismatched'], 'validation_failures': e['validation_failures'], 'source_sha': j['provenance']['source']['git_commit'], 'result': str(path), 'external_rss_result': str(out/'external-rss.json'), 'external_client_vmhwm_bytes': observation['client_vmhwm_bytes'], 'external_sample_peak_rss_bytes': observation['sample_peak_rss_bytes'], 'external_wait4_peak_rss_bytes': observation['wait4_peak_rss_bytes'], 'external_sample_count': len(samples), 'external_actual_interval_ms': observation['actual_interval_ms']}
            row['vmhwm_vs_self_pct'] = 100*(row['external_client_vmhwm_bytes']/row['process_peak_rss_bytes']-1)
            rows.append(row)
            (root/'paired-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')
            print(json.dumps({k:v for k,v in row.items() if k not in ['result','external_rss_result']}), flush=True)

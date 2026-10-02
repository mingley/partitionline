import json, os, pathlib, statistics, subprocess, time

root = pathlib.Path('/workspace/work/consumer-aborts')
os.sched_setaffinity(0, {2})
env = os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup', PATH='/workspace/work/cargo/bin:' + env['PATH'], CARGO_PROFILE_RELEASE_DEBUG='true', CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1')
sources = {'A': '824662e39980604c799e3398a7e3bc46db8400b3', 'B': 'c0cc3cde39a156350502dc65890fc66dd5dd8b60'}
expected_history = [[p, g*2500, g*2500+2000] for p, n in [(0, 20), (1, 20), (2, 13)] for g in range(n)] + [[2, 32500, 33000], [3, 0, 500], [4, 0, 500], [5, 0, 500]]
expected_cursors = [[0, 50000], [1, 50000], [2, 33000], [3, 500], [4, 500], [5, 500]]
rows = []

def percentile(xs, p):
    xs = sorted(xs); pos = (len(xs)-1)*p; lo = int(pos)
    return xs[lo] + (xs[min(lo+1, len(xs)-1)] - xs[lo])*(pos-lo)

for cell in ['nb-fetch-committed-aborts', 'nb-fetch-bulk', 'nb-fetch-1000p']:
    for pair in range(5):
        for variant in ('AB' if pair%2 == 0 else 'BA'):
            name = 'corrected-baseline' if variant == 'A' else 'candidate'
            cwd = root/('harness-baseline' if variant == 'A' else 'candidate')
            binary = root/f'{name}-bin'/'runtime'
            out = root/'paired'/cell/f'pair-{pair:02}-{variant}'
            if out.exists():
                raise RuntimeError(f'Output already exists: {out}; no replacement')
            out.mkdir(parents=True)
            cmd = ['taskset', '-c', '3', str(binary), '--cell', cell, '--out', str(out), '--repetitions', '1']
            samples = []; confirmed = False; confirm_time = None; start = time.monotonic_ns()
            with (root/f'{cell}-pair-{pair:02}-{variant}.log').open('w') as log:
                p = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)
                proc = pathlib.Path('/proc')/str(p.pid)
                while True:
                    try:
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
            observation = {'method': 'CPU2 observer confirms exact /proc/PID/exe runtime binary before ~1ms smaps_rollup Rss and persistent status VmHWM sampling through runtime lifetime; initialization, measured phase and teardown included.', 'exe_confirmed': confirmed, 'exe_confirmed_at_ns': confirm_time, 'requested_sleep_ms': 1, 'observer_duration_ns': time.monotonic_ns()-start, 'sample_count': len(samples), 'sample_peak_rss_bytes': max((s['rss_bytes'] for s in samples), default=0), 'client_vmhwm_bytes': max((s['hwm_bytes'] for s in samples), default=0), 'wait4_peak_rss_bytes': int(usage.ru_maxrss)*1024, 'wait4_user_cpu_seconds': usage.ru_utime, 'wait4_system_cpu_seconds': usage.ru_stime, 'actual_interval_ms': {'median': statistics.median(intervals) if intervals else None, 'p95': percentile(intervals,.95) if intervals else None, 'max': max(intervals) if intervals else None}, 'samples': samples, 'exit_code': p.returncode}
            (out/'external-rss.json').write_text(json.dumps(observation,indent=2)+'\n')
            assert p.returncode == 0, f'Runtime failed {cell}/{pair}/{variant}; all data retained'
            assert confirmed and samples
            path = out/f'{cell}-rep0.result.json'
            j = json.loads(path.read_text()); e = j['execution']; r = j['measurements']['client_resources']
            assert j['provenance']['source']['git_commit'] == sources[variant] and not j['provenance']['source']['dirty_tree']
            assert not e['mismatched'] and not e['validation_failures'] and not j['measurements']['errors']
            if cell == 'nb-fetch-committed-aborts':
                assert e['target_records'] == 20000 and e['returned_records'] == 108000 and e['records_consumed'] == 108000 and e['fetched_records'] == 134500
                assert e['committed_history'] == expected_history and e['committed_partition_cursors'] == expected_cursors
                assert e['committed_aborted_deliveries'] == 0 and e['committed_abort_gap_records'] == 25500 and e['filtered_records'] == 26500
            elif cell == 'nb-fetch-bulk':
                assert e['records_consumed'] == 20000 and e['fetched_records'] == 135000 and e['fetch_rounds'] == 1
            else:
                assert e['records_consumed'] == 10000 and e['fetched_records'] == 10000 and e['fetch_rounds'] == 1
            row = {'cell': cell, 'pair': pair, 'variant': variant, 'cpu_ns_per_record': r['cpu_ns_per_record'], 'allocations_per_record': r['allocations']['allocations_per_record'], 'allocation_count': r['allocations']['allocation_count'], 'allocated_bytes_per_record': r['allocations']['total_allocated_bytes']/e['records_consumed'], 'process_peak_rss_bytes': r['rss']['process_peak_rss_bytes'], 'phase_sampled_peak_rss_bytes': r['rss']['peak_rss_bytes'], 'phase_average_rss_bytes': r['rss']['average_rss_bytes'], 'rec_s': j['measurements']['throughput']['records_per_second'], 'p99_us': j['measurements']['latency']['p99'], 'verified_records': e['records_consumed'], 'returned_records': e.get('returned_records'), 'fetched_records': e['fetched_records'], 'fetch_rounds': e['fetch_rounds'], 'mismatched': e['mismatched'], 'validation_failures': e['validation_failures'], 'source_sha': j['provenance']['source']['git_commit'], 'result': str(path), 'external_rss_result': str(out/'external-rss.json'), 'external_client_vmhwm_bytes': observation['client_vmhwm_bytes'], 'external_sample_peak_rss_bytes': observation['sample_peak_rss_bytes'], 'external_wait4_peak_rss_bytes': observation['wait4_peak_rss_bytes'], 'external_sample_count': len(samples), 'external_actual_interval_ms': observation['actual_interval_ms']}
            row['vmhwm_vs_self_pct'] = 100*(row['external_client_vmhwm_bytes']/row['process_peak_rss_bytes']-1)
            rows.append(row)
            (root/'paired-summary-raw.json').write_text(json.dumps(rows,indent=2)+'\n')
            print(json.dumps({k:v for k,v in row.items() if k not in ['result','external_rss_result']}), flush=True)

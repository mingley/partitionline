"""Convert actual native observations to the shared benchmark result format.

Bulk latency is a sampled admission-call-to-final-flush bound. Open-loop
latency is intended-arrival-to-observed-ack time. Whole-workload resource
observations are kept distinct from the timed phase and from null-broker data.
"""
import collections
import hashlib
import json
import math
from pathlib import Path
import statistics


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def actual_runtime(observation, completion, flavor, workers):
    for row in (observation, completion):
        if (row['requested_flavor'] != flavor or row['observed_flavor'] != flavor
                or row['requested_background_workers'] != workers
                or row['observed_scheduler_workers'] != (workers or 1)):
            raise ValueError('requested and observed runtime differ')
    if completion['observed_alive_tasks'] != 0 or not completion['post_close_barrier_outside_measurement']:
        raise ValueError('post-workload runtime is not quiescent')
    return completion


def bulk_latencies(raw, producer):
    n = producer['acked']
    end = raw['phase_end_ns']
    if (raw['kind'] != 'sampled_admission_call_to_final_flush_upper_bound'
            or raw['stride'] != 512 or raw['timed_records'] != n or n != 8_000_000
            or type(end) is not int or end <= 0
            or abs(end / 1e9 - producer['elapsed_s']) > 0.000001):
        raise ValueError('bulk timing population differs')
    samples = raw['samples']
    if len(samples) != len(range(0, n, 512)):
        raise ValueError('bulk sampling population differs')
    result = []
    previous = 0
    for i, row in enumerate(samples):
        begin, admitted, observed = (row[k] for k in
            ('call_start_ns', 'call_end_ns', 'final_flush_observed_ns'))
        if (row['id'] != i * 512 or not all(type(v) is int for v in (begin, admitted, observed))
                or not 0 <= previous <= begin <= admitted <= observed == end
                or row['upper_bound_ns'] != observed - begin):
            raise ValueError('bulk sample identity or causal timing differs')
        previous = admitted
        result.append((observed - begin) // 1000)
    return result


def latency_stats(values):
    values = sorted(values)
    n = len(values)
    if n < 10_000 or any(type(v) is not int or v < 0 for v in values):
        raise ValueError('native latency sample floor not met')
    def quantile(permille):
        return values[(n * permille + 999) // 1000 - 1]
    mean = statistics.mean(values)
    stddev = statistics.pstdev(values)
    error = 1.96 * stddev / math.sqrt(n)
    histogram = collections.Counter(0 if v == 0 else 1 << (v - 1).bit_length() for v in values)
    return dict(sample_count=n, unit='microseconds', sample_floor_met=True,
        p50=quantile(500), p90=quantile(900), p95=quantile(950), p99=quantile(990),
        p99_9=quantile(999), min=values[0], max=values[-1], mean=mean, stddev=stddev,
        confidence_interval_95=dict(lower=max(0, mean-error), upper=mean+error, unit='microseconds',
            method='Nominal normal mean interval under an independent-sample model; serial dependence is not corrected. Use between-repetition bootstrap intervals for comparisons.'),
        raw_histogram=dict(bucket_unit='microseconds', buckets=[dict(upper_bound_us=k, count=v)
            for k,v in sorted(histogram.items())]))


def proc_field(text, name):
    matches = [line.split(':', 1)[1].strip().split()[0] for line in text.splitlines()
               if line.startswith(name + ':')]
    if len(matches) != 1:
        raise ValueError('missing or repeated observed /proc field: ' + name)
    return int(matches[0])


def broker_resources(before, after):
    # /proc/PID/stat names can contain spaces and parentheses. Fields 14/15
    # are offsets 11/12 after the final closing parenthesis.
    first = before['stat'].rsplit(')', 1)[1].split()
    last = after['stat'].rsplit(')', 1)[1].split()
    if (before['stat'].split(' ', 1)[0] != after['stat'].split(' ', 1)[0]
            or first[19] != last[19] or before['clock_ticks'] != after['clock_ticks']):
        raise ValueError('broker process identity changed')
    seconds = (after['monotonic_ns'] - before['monotonic_ns']) / 1e9
    user = (int(last[11]) - int(first[11])) / before['clock_ticks']
    system = (int(last[12]) - int(first[12])) / before['clock_ticks']
    disk = proc_field(after['io'], 'write_bytes') - proc_field(before['io'], 'write_bytes')
    if seconds <= 0 or min(user, system, disk) < 0:
        raise ValueError('broker measurement regressed')
    return dict(cpu_utilization_pct=100*(user+system)/seconds, cpu_unit='percent',
        user_cpu_seconds=user, system_cpu_seconds=system, cpu_seconds_unit='seconds',
        peak_rss_bytes=proc_field(after['status'], 'VmHWM')*1024, rss_unit='bytes',
        disk_write_bytes=disk, disk_write_unit='bytes', observation_seconds=seconds,
        note='CPU and write_bytes deltas between owned broker snapshots around the whole client workload; VmHWM is the broker lifetime high-water mark observed afterward, not a timed-phase RSS peak.')


def build_result(r, directory, config, rep, cohort, cell, produced, audited, created,
                 broker, rtt_ms, resources, observation, completion, raw_path,
                 values, effective, duration_seconds, warmup_seconds, load=None):
    flavor, workers = config['flavor'], config['workers']
    actual = actual_runtime(observation, completion, flavor, workers)
    acked = produced['acked'] if cell == 'lb-bulk' else produced['outcomes']['acknowledged']
    if audited['status'] != 'pass' or not audited['consumer_closed'] or audited['verified'] != 10_000 + acked:
        raise ValueError('independent Java full readback differs')
    if cell == 'lb-bulk':
        if (produced['accepted'] != acked or produced['acknowledged_total'] != acked+10_000
                or produced['warmup_records'] != 10_000 or produced['run_disposition'] != 'executed'):
            raise ValueError('bulk terminal accounting differs')
        outcomes = dict(offered=acked, accepted=acked, acknowledged=acked, consumed=0,
                        rejected=0, timed_out=0, unknown=0)
        latency_note = 'Every 512th accepted record: successful admission call start to final measured-phase flush completion upper bound. This is not a per-record acknowledgment timestamp or an ack-latency percentile.'
        partitions = [dict(partition=p, start_offset=(10_000+5-p)//6,
            end_offset=(10_000+5-p)//6 + (acked+5-p)//6, offset_delta=(acked+5-p)//6)
            for p in range(6)]
        identity_note = 'Genuine Kafka Java 4.3.1 full seeded byte/key readback in warmup and timed namespaces, with per-partition end offsets checked by the verifier.'
        start_id, end_id = 0, acked-1
    else:
        outcomes = produced['outcomes']
        latency_note = 'Intended fixed-rate arrival to successful send-future completion; includes schedule lag and enqueue delay. Only acknowledged samples form these latency percentiles; all 20000 offers and rejected samples remain in the raw JSONL.'
        partitions = [dict(partition=0, start_offset=10_000, end_offset=10_000+acked, offset_delta=acked)]
        identity_note = 'Genuine Kafka Java 4.3.1 complete fixed-x payload and contiguous Kafka offset verification. Kafka partition offsets identify the readback records; unique producer payload IDs and arrival-ID-to-offset mapping are unavailable in this unchanged workload.'
        start_id, end_id = 10_000, 10_000+acked-1
    disposition = produced['run_disposition']
    effective = dict(effective, runtime=actual)
    config_bytes = json.dumps(effective, sort_keys=True, separators=(',', ':')).encode()
    total_cpu = resources['user_cpu_seconds'] + resources['system_cpu_seconds']
    client = dict(cpu_utilization_pct=100*total_cpu/resources['wall_seconds'], cpu_unit='percent',
        user_cpu_seconds=resources['user_cpu_seconds'], system_cpu_seconds=resources['system_cpu_seconds'],
        cpu_seconds_unit='seconds', cpu_ns_per_record=total_cpu*1e9/acked,
        allocations=dict(unit='bytes', allocation_count=resources['allocation_count'],
            total_allocated_bytes=resources['total_allocated_bytes'],
            scope=resources['allocation_scope'], allocations_per_record=resources['allocation_count']/acked),
        rss=dict(unit='bytes', peak_rss_bytes=resources['rss_sample_peak_bytes'],
            average_rss_bytes=resources['rss_sample_mean_bytes'],
            process_peak_rss_bytes=resources['process_peak_rss_bytes'], note=resources['rss_scope']),
        threads_count=proc_field(resources['thread_snapshot_before_workload'], 'Threads'),
        threads_scope='Actual /proc/self/status snapshot after executor/sampler construction and before workload; not a lifetime peak',
        resource_scope=resources['scope'])
    artifacts = [dict(path=str(path), type='native-runtime-observation', sha256=digest(path),
                      size_bytes=path.stat().st_size)
        for path in sorted(directory.iterdir()) if path.is_file() and not path.name.endswith('.result.json')]
    binary = r.binaries['native-produce-runtime' if cell == 'lb-bulk' else 'native-latency-runtime']
    doc = dict(schema_version='1.0.0', contract_version='1.1.0',
        suite_hold=dict(status='active', policy='Local unsigned runtime sensitivity; no ranking or production qualification'),
        scenario=dict(scenario_id=cell, peer='partitionline', profile='bulk' if cell == 'lb-bulk' else 'low-latency',
            tier='exploratory', cell_disposition=disposition,
            equal_semantics=dict(durability=dict(replication_factor=1, min_insync_replicas=1),
                acks=1, idempotence=False, isolation='read_uncommitted',
                security=dict(protocol='PLAINTEXT', mechanism='NONE'), payload_bytes=100,
                partitions=created['partitions'], offered_load=load)),
        provenance=dict(source=dict(git_commit=r.args.commit, git_branch='detached pinned review source',
            dirty_tree=False, clean=True, repo_url='https://github.com/mingley/partitionline',
            tree_hash=r.tree_hash, source_pins=str(r.args.source_pins)),
            binary=dict(name=binary.name, path=str(binary), sha256=digest(binary), size_bytes=binary.stat().st_size),
            config=dict(path=str(directory/'effective-settings.json'), sha256=hashlib.sha256(config_bytes).hexdigest(), effective_settings=effective),
            toolchains=dict(compiler=r.host_observation['rustc'], build_tool=r.host_observation['cargo'],
                runtime='Tokio 1.53.2; explicit runtime recorded from actual handle/metrics'),
            broker=dict(image='Apache Kafka distribution archive '+broker['archive_sha256'], version='4.3.1',
                mode='Owned single-node KRaft broker/controller; acks=1 RF=1 minISR=1',
                cluster_id=broker['cluster'], node_count=1, endpoints=[broker['bootstrap']]),
            host=resources['host'], topology=dict(environment='loopback', rtt_ms=rtt_ms,
                rtt_unit='milliseconds', client_nodes=1, broker_nodes=1, network_interface='lo',
                client_affinity=[2,4], broker_affinity=[0,1], rtt_scope='Actual TCP-connect wall time immediately before workload; no emulated network delay'),
            timestamps=dict(start_time_utc=resources['start_time_utc'], end_time_utc=resources['end_time_utc'],
                duration_seconds=resources['wall_seconds'], duration_unit='seconds'),
            seeds=dict(payload_seed=1592590337 if cell == 'lb-bulk' else 0,
                key_seed=1592590337 if cell == 'lb-bulk' else 0, partition_seed=0,
                repetition_seed=961, note='Bulk phase-local monotonic IDs, round-robin partitions, shared key/value seed; latency fixed-x payload, null key, partition 0, no payload PRNG'),
            artifacts=artifacts),
        execution=dict(phase='steady_state', warmup_completed=True,
            warmup_duration_seconds=warmup_seconds, steady_state_duration_seconds=duration_seconds,
            total_repetitions=5, pairing_order='Randomized within matched repetition; seed 961; exact order retained in matrix-plan.json',
            coordinated_omission_avoidance=dict(enabled=(cell != 'lb-bulk'),
                schedule_type='closed_loop' if cell == 'lb-bulk' else 'open_loop_fixed_rate'),
            cell_id=cell, repetition_index=rep, cohort=cohort, runtime_config=config['name'],
            latency_note=latency_note, measured_seconds=duration_seconds,
            warmup_records=10_000, warmup_excluded=True, raw_path=str(raw_path), load=load,
            qualification_scope='Runtime sensitivity only. Whole-workload resources and sampled bulk bounds are diagnostic; they are not equal-semantics peer comparisons.'),
        outcomes=outcomes,
        measurements=dict(throughput=dict(records_per_second=acked/duration_seconds,
                records_per_second_unit='records/s', megabytes_per_second=acked*100/duration_seconds/1e6,
                megabytes_per_second_unit='MB/s', total_bytes_transferred=acked*100,
                total_bytes_unit='bytes', byte_scope='Acknowledged timed payload bytes only; excludes keys, framing and retries'),
            latency=latency_stats(values), client_resources=client,
            broker_resources=broker_resources(read(directory/'broker-before.json'),read(directory/'broker-after.json')),
            errors=[] if not outcomes['rejected'] else [dict(code='pending_capacity', name='capacity_rejection', fatal=False, phase='steady_state', count=outcomes['rejected'],
                message='benchmark pending capacity exhausted; every rejected raw sample retained')]),
        integrity=dict(verified=True, integrity_failure=False, idempotence_sequence_verified=False,
            high_watermark_audit=dict(partitions=partitions, total_offset_delta=acked, matches_acknowledged=True,
                source='Full independent readback verifies end-offset fences; measured delta excludes the separately verified warmup prefix'),
            record_ids=dict(start_id=start_id, end_id=end_id, expected_count=acked, verified_count=acked,
                missing_ids_count=0, duplicate_ids_count=0, checksum_algorithm=identity_note,
                payload_checksum_matches=True, unique_producer_payload_ids_checked=(cell == 'lb-bulk'))),
        repetition_history=dict(total_attempts=1, failed_attempts=int(disposition=='failed'),
            attempts=[dict(repetition_index=rep, status='failed_capacity' if disposition=='failed' else 'passed_measurement',
                integrity_failure=False)]))
    return doc

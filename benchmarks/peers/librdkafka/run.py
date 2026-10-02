#!/usr/bin/env python3
"""C-only benchmark peer adapter; Python never produces/consumes Kafka records."""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def output(args, default='unknown'):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return default

def integer(name, default, low=0, high=1_000_000_000):
    try:
        value = int(os.environ.get(name, str(default)), 0)
    except ValueError as e:
        raise ValueError(f'{name} must be an integer') from e
    if not low <= value <= high:
        raise ValueError(f'{name} outside [{low}, {high}]')
    return value

def settings():
    c = dict(bootstrap=os.environ.get('KAFKA_BOOTSTRAP', '127.0.0.1:9092'),
             topic=os.environ.get('KAFKA_TOPIC', 'plbench'),
             count=integer('COUNT', 100000, 1), warmup=integer('WARMUP', 10000),
             payload_bytes=integer('PAYLOAD_BYTES', 100, 0, 10_000_000),
             partitions=integer('PARTITIONS', 6, 1, 10000),
             acks=integer('ACKS', 1, -1, 1),
             linger_ms=integer('LINGER_MS', 5), batch_size_bytes=integer('BATCH_BYTES', 1000000, 1),
             batch_num_messages=integer('BATCH_RECORDS', 32768, 1),
             max_in_flight=integer('MAX_IN_FLIGHT', 5, 1),
             queue_max_messages=integer('QUEUE_MESSAGES', 1000000, 1),
             queue_max_kbytes=integer('QUEUE_KBYTES', 32768, 1),
             delivery_timeout_ms=integer('DELIVERY_TIMEOUT_MS', 30000, 1),
             flush_timeout_ms=integer('FLUSH_TIMEOUT_MS', 35000, 1),
             run_timeout_ms=integer('RUN_TIMEOUT_MS', 120000, 1),
             consume_timeout_ms=integer('CONSUME_TIMEOUT_MS', 30000, 1),
             record_seed=integer('RECORD_SEED', 0x5EED0001, 0, 2**64-1),
             latency_samples=integer('LATENCY_SAMPLES', 1000000000))
    idem = os.environ.get('IDEMPOTENT', '0').lower()
    if idem not in ('0', '1', 'true', 'false'):
        raise ValueError('IDEMPOTENT must be 0|1|true|false')
    c['idempotence'] = idem in ('1','true')
    if c['idempotence'] and (c['acks'] != -1 or c['max_in_flight'] > 5):
        raise ValueError('idempotence requires ACKS=-1 and MAX_IN_FLIGHT<=5; implicit library adjustment prohibited')
    c['compression'] = os.environ.get('COMPRESSION', 'none').lower()
    if c['compression'] not in ('none','gzip','snappy','lz4','zstd'):
        raise ValueError('unsupported COMPRESSION')
    c['isolation_level'] = os.environ.get('ISOLATION','read_uncommitted')
    if c['isolation_level'] not in ('read_uncommitted','read_committed'):
        raise ValueError('unsupported ISOLATION')
    c['payload_mode'] = os.environ.get('PAYLOAD_MODE','seeded')
    c['key_mode'] = os.environ.get('KEY_MODE','id')
    if c['payload_mode'] not in ('seeded','constant-x') or c['key_mode'] not in ('id','none'):
        raise ValueError('PAYLOAD_MODE=seeded|constant-x and KEY_MODE=id|none required')
    if c['key_mode']=='none' and c['payload_mode']!='constant-x':
        raise ValueError('KEY_MODE=none requires PAYLOAD_MODE=constant-x (no ID-based verification possible)')
    c['sasl_mechanism'] = os.environ.get('SASL_MECHANISM','').upper()
    if c['sasl_mechanism'] not in ('','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512'):
        raise ValueError('SASL supports PLAIN/SCRAM-SHA-256/SCRAM-SHA-512; OAuth/GSSAPI are unsupported')
    c['has_sasl_credentials'] = bool(os.environ.get('SASL_USERNAME') and os.environ.get('SASL_PASSWORD'))
    if c['sasl_mechanism'] and not c['has_sasl_credentials']:
        raise ValueError('SASL requires SASL_USERNAME and SASL_PASSWORD')
    if (os.environ.get('SASL_USERNAME') or os.environ.get('SASL_PASSWORD')) and not c['sasl_mechanism']:
        raise ValueError('SASL credentials require explicit SASL_MECHANISM')
    c['has_custom_ca'] = bool(os.environ.get('TLS_CA_PEM'))
    c['has_client_identity'] = bool(os.environ.get('TLS_CLIENT_CERT_PEM'))
    if c['has_client_identity'] != bool(os.environ.get('TLS_CLIENT_KEY_PEM')):
        raise ValueError('mTLS requires certificate and key')
    if os.environ.get('TLS_SERVER_NAME'):
        raise ValueError('TLS_SERVER_NAME override unsupported; use broker hostname matching its certificate')
    tls = c['has_custom_ca'] or c['has_client_identity']
    c['security_protocol'] = ('SASL_SSL' if tls else 'SASL_PLAINTEXT') if c['sasl_mechanism'] else ('SSL' if tls else 'PLAINTEXT')
    if os.environ.get('SECURITY_PROTOCOL','').upper() not in ('',c['security_protocol']):
        raise ValueError('SECURITY_PROTOCOL conflicts with configured TLS/SASL')
    c.update(partitioner='explicit_round_robin', connections_per_broker=1,
             socket_nagle_disable=True, delivery_definition='successful delivery callback with acks!=0; enqueue never acknowledged',
             record_id_definition='key=be64(index)||be64(splitmix64(seed^index)); splitmix64 value stream',
             unsupported=['open-loop latency','transactions','groups/share','OAuth/OIDC refresh','GSSAPI'])
    return c

ENV_MAP = {'bootstrap':'KAFKA_BOOTSTRAP','topic':'KAFKA_TOPIC','count':'COUNT','warmup':'WARMUP',
 'payload_bytes':'PAYLOAD_BYTES','partitions':'PARTITIONS','acks':'ACKS','linger_ms':'LINGER_MS',
 'batch_size_bytes':'BATCH_BYTES','batch_num_messages':'BATCH_RECORDS','max_in_flight':'MAX_IN_FLIGHT',
 'queue_max_messages':'QUEUE_MESSAGES','queue_max_kbytes':'QUEUE_KBYTES','compression':'COMPRESSION',
 'delivery_timeout_ms':'DELIVERY_TIMEOUT_MS','flush_timeout_ms':'FLUSH_TIMEOUT_MS','run_timeout_ms':'RUN_TIMEOUT_MS',
 'consume_timeout_ms':'CONSUME_TIMEOUT_MS','record_seed':'RECORD_SEED','payload_mode':'PAYLOAD_MODE',
 'key_mode':'KEY_MODE','latency_samples':'LATENCY_SAMPLES','security_protocol':'SECURITY_PROTOCOL','isolation_level':'ISOLATION'}

def process_env(c):
    e=os.environ.copy()
    # Ensure the artifact's hashed $ORIGIN native library is the loaded library.
    e.pop('LD_LIBRARY_PATH',None); e.pop('LD_PRELOAD',None)
    e.update({v:str(c[k]) for k,v in ENV_MAP.items()})
    e['IDEMPOTENT']='true' if c['idempotence'] else 'false'
    if c['sasl_mechanism']: e['SASL_MECHANISM']=c['sasl_mechanism']
    return e

def effective(c, dump):
    pairs={'acks':'request.required.acks','linger_ms':'queue.buffering.max.ms','batch_size_bytes':'batch.size',
           'batch_num_messages':'batch.num.messages','max_in_flight':'max.in.flight.requests.per.connection',
           'queue_max_messages':'queue.buffering.max.messages','queue_max_kbytes':'queue.buffering.max.kbytes',
           'delivery_timeout_ms':'message.timeout.ms'}
    for k,v in pairs.items():
        if v not in dump:
            raise ValueError(f'missing effective librdkafka config {v}')
        c[k]=float(dump[v]) if k=='linger_ms' else int(dump[v])
    c['idempotence']=dump['enable.idempotence']=='true'
    c['compression']=dump.get('compression.codec',c['compression'])
    c['security_protocol']=dump['security.protocol'].upper()
    c['librdkafka_effective']=dump
    return c

def quantile(a, p):
    return a[max(0,math.ceil(len(a)*p)-1)] if a else 0.0

def latency(samples):
    a=sorted(samples); p99=quantile(a,.99)
    # Fixed-seed, nonparametric bootstrap of the p99; smoke numbers are diagnostic.
    rng=random.Random(0x5EED0004)
    if a:
        # Retain all raw samples; resample a deterministic capped subset for cost.
        population=a if len(a)<=10000 else [a[rng.randrange(len(a))] for _ in range(10000)]
        estimates=sorted(quantile(sorted(rng.choices(population,k=len(population))),.99) for _ in range(200))
        low,high=quantile(estimates,.025),quantile(estimates,.975)
    else: low=high=0.0
    buckets={}
    for value in a:
        lo=2**math.floor(math.log2(max(1,value)))
        buckets[lo]=buckets.get(lo,0)+1
    return dict(sample_count=len(a), unit='microseconds', p50=quantile(a,.5),p90=quantile(a,.9),
      p95=quantile(a,.95),p99=p99,p99_9=quantile(a,.999),min=min(a,default=0),max=max(a,default=0),
      mean=statistics.mean(a) if a else 0,stddev=statistics.pstdev(a) if a else 0,
      confidence_interval_95=dict(lower=low,upper=high,unit='microseconds',method='200 fixed-seed bootstrap p99 resamples; population capped at 10000'),
      raw_histogram=dict(bucket_unit='microseconds',buckets=[dict(min_us=k if k>1 else 0,max_us=2*k,count=v) for k,v in sorted(buckets.items())] or [dict(min_us=0,max_us=0,count=0)]),
      definition='First LATENCY_SAMPLES IDs sampled; queue attempt to successful delivery callback, includes local backpressure; closed-loop diagnostic, no open-loop latency claim')

def host():
    cpu={}; cores=set()
    try:
        text=Path('/proc/cpuinfo').read_text()
        for block in text.split('\n\n'):
            fields=dict(line.split(':',1) for line in block.splitlines() if ':' in line)
            fields={k.strip():v.strip() for k,v in fields.items()}
            cpu.update(fields)
            if 'physical id' in fields and 'core id' in fields: cores.add((fields['physical id'],fields['core id']))
        mem=int(next(l.split()[1] for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith('MemTotal:')))*1024
    except (OSError, StopIteration):
        mem=int(output(['sysctl','-n','hw.memsize'],'1'))
    return dict(hostname=platform.node(),os=platform.platform(),os_family=platform.system().lower(),
      kernel_version=platform.release(),arch=platform.machine(),
      cpu=dict(model=cpu.get('model name',platform.processor() or 'unavailable'),physical_cores=len(cores) or os.cpu_count() or 1,
               logical_cores=os.cpu_count() or 1,frequency_mhz=float(cpu.get('cpu MHz','0'))),memory=dict(total_bytes=mem,unit='bytes'))

def write(path,data):
    with Path(path).open('x',encoding='utf-8') as f: json.dump(data,f,indent=2); f.write('\n')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['emit-config','produce','roundtrip'])
    parser.add_argument('--binary',type=Path,default=Path(os.environ.get('C_PEER_BUILD_DIR',ROOT/'work/librdkafka-peer'))/'c-peer')
    parser.add_argument('--result',type=Path,default=Path(os.environ.get('RESULT_PATH','c-peer-result.json')))
    args=parser.parse_args(); c=settings(); e=process_env(c)
    if args.command=='roundtrip' and not all(os.environ.get(k) for k in ('BROKER_IMAGE','BROKER_VERSION')):
        raise ValueError('roundtrip requires BROKER_IMAGE and BROKER_VERSION from the inspected broker distribution')
    binary=args.binary.resolve(); manifest_path=binary.parent/'build-manifest.json'
    manifest=json.loads(manifest_path.read_text()); pin=json.loads((HERE/'source-pin.json').read_text())
    if manifest['commit']!=pin['commit'] or manifest['binary_sha256']!=sha(binary) or manifest['library_sha256']!=sha(binary.parent/'lib/librdkafka.so.1') or manifest['peer_source_sha256']!=sha(HERE/'peer.c'):
        raise ValueError('binary/library/peer source differs from pinned build manifest; rebuild')
    if args.command=='emit-config':
        dump=json.loads(subprocess.check_output([str(binary),'emit-config'],env=e,text=True))
        print(json.dumps(effective(c,dump),indent=2)); return 0
    args.result.parent.mkdir(parents=True,exist_ok=True)
    suffixes=['','.raw.json','.samples.csv','.effective-config.json','.stderr.log','.build-manifest.json']
    if any(Path(str(args.result)+x).exists() for x in suffixes):
        raise ValueError('result or companion exists; choose a new RESULT_PATH to retain every attempt')
    raw_path=Path(str(args.result)+'.raw.json'); samples_path=Path(str(args.result)+'.samples.csv')
    config_path=Path(str(args.result)+'.effective-config.json'); stderr_path=Path(str(args.result)+'.stderr.log')
    build_path=Path(str(args.result)+'.build-manifest.json')
    e.update(C_PEER_RAW=str(raw_path),C_PEER_SAMPLES=str(samples_path))
    start=dt.datetime.now(dt.timezone.utc)
    with stderr_path.open('x') as log:
        run=subprocess.run([str(binary),args.command],env=e,stderr=log)
    end=dt.datetime.now(dt.timezone.utc)
    if not raw_path.exists():
        raise RuntimeError(f'C peer failed before delivery artifact (exit={run.returncode}); retained stderr at {stderr_path}')
    raw=json.loads(raw_path.read_text()); c=effective(c,raw['effective_config']); write(config_path,c); write(build_path,manifest)
    samples=[float(row['latency_us']) for row in csv.DictReader(samples_path.open())]
    timed=raw['timed']; warm=raw['warmup']; acked=timed['acknowledged']; duration=timed['elapsed_s']
    v=raw['verification']; hw=raw['high_watermarks']; d=raw['durability']; res=raw['resources']
    verified=(run.returncode==0 and d['verified'] and hw['queried'] and hw['total_offset_delta']==acked and
              v['verified_ids']==c['count'] and not v['duplicate_ids'] and not v['bad_records'])
    failed=run.returncode!=0
    utc=lambda t:t.isoformat().replace('+00:00','Z')
    artifacts=[]
    for path,kind in [(raw_path,'raw_delivery'),(samples_path,'raw_latency'),(config_path,'effective_config'),(stderr_path,'stderr'),(build_path,'build_manifest')]:
        artifacts.append(dict(path=str(path),type=kind,sha256=sha(path),size_bytes=path.stat().st_size))
    outcomes={k:timed[k] for k in ('offered','accepted','acknowledged','rejected','timed_out','unknown')}
    outcomes['consumed']=v['verified_ids']
    errors=[]
    for phase,counts in [('warmup',warm),('steady_state',timed)]:
        errors.extend(dict(**err,fatal=True,phase=phase) for err in counts['errors'])
    result=dict(schema_version='1.0.0',contract_version='1.1.0',
      suite_hold=dict(status='active',policy='Unsigned samples do not lift Suite HOLD',note='Adapter validation only; no comparison or scenario qualification'),
      scenario=dict(scenario_id=os.environ.get('SCENARIO_ID','librdkafka-c-peer-smoke'),profile='secure' if c['security_protocol']!='PLAINTEXT' else 'bulk',
        tier='exploratory',peer='librdkafka',cell_disposition='failed' if failed else 'executed',
        equal_semantics=dict(durability=dict(replication_factor=d['replication_factor'],min_insync_replicas=d['min_insync_replicas']),
          acks=c['acks'],idempotence=c['idempotence'],isolation=c['isolation_level'],security=dict(protocol=c['security_protocol'],mechanism=c['sasl_mechanism'] or 'NONE'))),
      provenance=dict(source=dict(git_commit=output(['git','-C',str(ROOT),'rev-parse','HEAD']),git_branch=output(['git','-C',str(ROOT),'branch','--show-current']),
          repo_url='https://github.com/mingley/partitionline',clean=not output(['git','-C',str(ROOT),'status','--porcelain'],''),
          tree_hash=output(['git','-C',str(ROOT),'rev-parse','HEAD^{tree}']),library_repository=pin['repository'],library_commit=pin['commit'],library_tag=pin['tag'],
          peer_source_sha256=manifest['peer_source_sha256'],adapter_source_sha256=sha(HERE/'run.py'),note='Source tree may include parallel work; adapter bytes and pinned native library are hashed in build manifest'),
        binary=dict(name='librdkafka-native-c-peer',path=str(binary),sha256=sha(binary)),
        config=dict(path=str(config_path),sha256=sha(config_path),effective_settings=c),
        toolchains=dict(compiler=manifest['compiler'],runtime='native C / librdkafka '+raw['version'],build_tool=manifest['build_tool'],library_commit=pin['commit']),
        broker=dict(image=os.environ.get('BROKER_IMAGE','unreported'),version=os.environ.get('BROKER_VERSION','unreported'),
          mode=os.environ.get('BROKER_MODE','kraft'),cluster_id=raw['cluster_id'] or 'unreported',node_count=raw['broker_nodes'] or 1,endpoints=c['bootstrap'].split(','),
          identity_note='Image/version supplied by operator; cluster ID, nodes, RF and minISR queried by C driver. Version is not inferred from ApiVersions'),
        host=host(),topology=dict(environment=os.environ.get('BENCH_ENVIRONMENT','loopback'),rtt_ms=float(os.environ.get('RTT_MS','0')),rtt_unit='milliseconds',
          client_nodes=1,broker_nodes=raw['broker_nodes'] or 1,network_interface=os.environ.get('NETWORK_INTERFACE','lo'),rtt_note='RTT unmeasured unless RTT_MS supplied'),
        timestamps=dict(start_time_utc=utc(start),end_time_utc=utc(end),duration_seconds=(end-start).total_seconds(),duration_unit='seconds'),
        seeds=dict(payload_seed=hex(c['record_seed']),key_seed=hex(c['record_seed']),partition_seed='explicit index modulo partitions',repetition_seed='0x5EED0004'),artifacts=artifacts),
      execution=dict(phase='steady_state',warmup_completed=warm['acknowledged']==c['warmup'],warmup_records=warm['acknowledged'],warmup_duration_seconds=warm['elapsed_s'],
        steady_state_duration_seconds=duration,repetition_index=integer('REPETITION_INDEX',1,1),total_repetitions=integer('TOTAL_REPETITIONS',1,1),
        pairing_order=os.environ.get('PAIRING_ORDER','standalone smoke'),coordinated_omission_avoidance=dict(enabled=False,schedule_type='closed_loop'),
        contract_sample_floors_met=False,note='Single adapter correctness check; a campaign must independently enforce contract warmup/repetition/sample floors'),
      outcomes=outcomes,
      measurements=dict(throughput=dict(records_per_second=acked/duration,records_per_second_unit='records/s',megabytes_per_second=acked*c['payload_bytes']/duration/1e6,
          megabytes_per_second_unit='MB/s',total_bytes_transferred=acked*c['payload_bytes'],total_bytes_unit='bytes',mb_definition='1 MB=1000000 value bytes; excludes keys/protocol'),
        latency=latency(samples),client_resources=dict(cpu_utilization_pct=100*(res['user_cpu_seconds']+res['system_cpu_seconds'])/(end-start).total_seconds(),
          cpu_unit='percent',user_cpu_seconds=res['user_cpu_seconds'],system_cpu_seconds=res['system_cpu_seconds'],cpu_seconds_unit='seconds',
          allocations=dict(total_allocated_bytes=0,allocation_count=0,unit='bytes',note='Uninstrumented, not measured'),
          rss=dict(peak_rss_bytes=res['peak_rss_bytes'],average_rss_bytes=0,unit='bytes',note='Peak from C getrusage; average unmeasured'),threads_count=res['threads_count'],
          threads_note='rd_kafka_thread_cnt plus driver main thread; CPU/RSS cover C produce + audit, not steady-state isolation'),
        broker_resources=dict(cpu_utilization_pct=0,cpu_unit='percent',peak_rss_bytes=0,rss_unit='bytes',disk_write_bytes=0,disk_write_unit='bytes',note='Unmeasured external broker'),
        errors=errors,callback_failures=timed['callback_failures'],warmup_callback_failures=warm['callback_failures'],queue_full_retries=timed['queue_full_retries']),
      integrity=dict(verified=verified,record_ids=dict(start_id=0,end_id=c['count']-1,expected_count=acked,verified_count=v['verified_ids'],
        missing_ids_count=max(0,acked-v['verified_ids']),duplicate_ids_count=v['duplicate_ids'],checksum_algorithm='exact deterministic bytes',
        payload_checksum_matches=verified),high_watermark_audit=dict(partitions=hw['partitions'],total_offset_delta=hw['total_offset_delta'],matches_acknowledged=hw['queried'] and hw['total_offset_delta']==acked),
        idempotence_sequence_verified=False,integrity_failure=failed),
      repetition_history=dict(total_attempts=1,failed_attempts=int(failed),attempts=[dict(attempt_number=1,repetition_index=integer('REPETITION_INDEX',1,1),
        status='failed_timeout' if timed['timed_out'] else ('failed_broker_error' if failed else 'passed_measurement'),integrity_failure=failed,
        error_message=f'native C peer exit={run.returncode}; delivery callback failures={timed["callback_failures"]}' if failed else None,timestamp_utc=utc(end))]))
    write(args.result,result)
    print(json.dumps(dict(result=str(args.result),acked=acked,callback_failures=timed['callback_failures'],verified=verified,exit_code=run.returncode)))
    return run.returncode

if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError, OSError, RuntimeError, KeyError) as error:
        print(f'C peer: {error}',file=sys.stderr); sys.exit(2)

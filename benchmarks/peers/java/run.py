#!/usr/bin/env python3
"""Apache Java SDK peer: pin checks, JVM ownership and shared result packaging."""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    loaded=importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded); return loaded

shared=module('java_shared_result',HERE.parent/'librdkafka/run.py')
build=module('java_build',HERE/'build.py')
sha,output,integer,latency,host,write=(getattr(shared,k) for k in ('sha','output','integer','latency','host','write'))

def settings():
    c=shared.settings()
    for key in ('BATCH_RECORDS','TRANSACTIONAL_ID','GROUP_ID','OPEN_LOOP_RATE'):
        if os.environ.get(key): raise ValueError(f'Unsupported setting: {key}')
    if c['security_protocol']!='PLAINTEXT' or c['key_mode']!='id':
        raise ValueError('Java peer supports PLAINTEXT and KEY_MODE=id only')
    if c['count']>10000000 or c['warmup']>1000000 or c['queue_max_messages']>1000000:
        raise ValueError('Java audit capped at 10000000 IDs; warmup/cohort at 1000000')
    if c['delivery_timeout_ms']<=c['linger_ms']:
        raise ValueError('DELIVERY_TIMEOUT_MS must exceed LINGER_MS')
    c.pop('batch_num_messages'); c.pop('socket_nagle_disable')
    c.update(unsupported=['open-loop latency','transactions','groups/share','TLS/SASL','batch record-count cap','null-key ID audit'],
             pipeline='bounded asynchronous send with completion callbacks',
             batch_records='unsupported: byte-based batching only')
    return c

def process_env(c):
    e=build.environment()
    for key,name in shared.ENV_MAP.items():
        if key in c: e[name]=str(c[key])
    e.pop('BATCH_RECORDS',None)
    e['IDEMPOTENT']='true' if c['idempotence'] else 'false'
    return e

def effective(c,dump):
    p=dump['producer']
    for key,name in {'acks':'acks','linger_ms':'linger.ms','batch_size_bytes':'batch.size',
        'max_in_flight':'max.in.flight.requests.per.connection','delivery_timeout_ms':'delivery.timeout.ms'}.items():
        c[key]=int(p[name])
    c['queue_max_kbytes']=int(p['buffer.memory'])//1024
    c['idempotence']=p['enable.idempotence']
    c['compression']=p['compression.type']
    c['security_protocol']=p['security.protocol']
    c['isolation_level']=dump['consumer']['isolation.level']
    c['java_effective']=dump
    return c

def verified_command(binary,manifest,pin):
    if manifest['tag']!=pin['tag'] or manifest['binary_sha256']!=sha(binary) or manifest['sources']!=build.sources():
        raise ValueError('peer/source differs from build manifest; rebuild')
    if manifest['dependencies']!=pin['dependencies'] or manifest['java_version']!=pin['java_version']:
        raise ValueError('build dependency/toolchain pins differ')
    java=Path(manifest['java_path'])
    if sha(java)!=manifest['java_sha256']:
        raise ValueError('JDK executable changed')
    version=subprocess.check_output([str(java),'-XshowSettings:properties','-version'],
        env=build.environment(),stderr=subprocess.STDOUT,text=True)
    if not any(line.strip()=='java.version = '+pin['java_version'] for line in version.splitlines()):
        raise ValueError('JDK runtime differs from pin')
    jars=[binary]
    for item in pin['dependencies']:
        jar=binary.parent/Path(item['path']).name
        if sha(jar)!=item['sha256']: raise ValueError('dependency differs from pin: '+jar.name)
        jars.append(jar)
    return [str(java),'-Xms32m','-Xmx256m','-cp',os.pathsep.join(map(str,jars)),'BenchmarkPeer']

def owned_run(command,env,log,c,receipt):
    if not Path('/proc/self/status').exists():
        raise ValueError('Linux /proc process measurements required by this adapter')
    started=time.monotonic()
    # Bound both producer phases, all metadata RPCs and the audit/close path.
    deadline=started+2*c['run_timeout_ms']/1000+12*c['consume_timeout_ms']/1000+60
    child=subprocess.Popen(command,env=env,stdout=log,stderr=log)
    peak=0; samples=0; timed_out=False
    try:
        while True:
            waited,status,usage=os.wait4(child.pid,os.WNOHANG)
            if waited:
                child.returncode=os.waitstatus_to_exitcode(status); break
            try:
                status_text=Path(f'/proc/{child.pid}/status').read_text()
                peak=max(peak,int(next(line.split()[1] for line in status_text.splitlines() if line.startswith('VmRSS:')))*1024)
                samples+=1
            except (OSError,StopIteration): pass
            if time.monotonic()>deadline:
                timed_out=True;child.kill()
                waited,status,usage=os.wait4(child.pid,0)
                child.returncode=os.waitstatus_to_exitcode(status);break
            time.sleep(.02)
    finally:
        if child.returncode is None:
            child.kill(); _,status,usage=os.wait4(child.pid,0)
            child.returncode=os.waitstatus_to_exitcode(status)
        write(receipt,dict(pid=child.pid,command=command,parent_waited=True,exit_code=child.returncode,
            deadline_exceeded=timed_out,elapsed_seconds=time.monotonic()-started,rss_samples=samples,
            max_rss_bytes=max(peak,int(usage.ru_maxrss)*1024),user_cpu_seconds=usage.ru_utime,
            system_cpu_seconds=usage.ru_stime))
    return SimpleNamespace(returncode=child.returncode),dict(user_cpu_seconds=usage.ru_utime,
        system_cpu_seconds=usage.ru_stime,peak_rss_bytes=max(peak,int(usage.ru_maxrss)*1024))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['emit-config','produce','roundtrip'])
    parser.add_argument('--binary',type=Path,default=Path(os.environ.get('JAVA_PEER_BUILD_DIR',ROOT/'work/java-peer'))/'java-peer.jar')
    parser.add_argument('--result',type=Path,default=Path(os.environ.get('RESULT_PATH','java-peer-result.json')))
    args=parser.parse_args(); c=settings(); e=process_env(c)
    if args.command=='roundtrip' and not all(os.environ.get(k) for k in ('BROKER_IMAGE','BROKER_VERSION')):
        raise ValueError('roundtrip requires BROKER_IMAGE and BROKER_VERSION from the inspected broker distribution')
    binary=args.binary.resolve(); manifest_path=binary.parent/'build-manifest.json'
    manifest=json.loads(manifest_path.read_text()); pin=json.loads((HERE/'source-pin.json').read_text())
    command=verified_command(binary,manifest,pin)
    if args.command=='emit-config':
        dump=json.loads(subprocess.check_output(command+['emit-config'],env=e,text=True))
        print(json.dumps(effective(c,dump),indent=2)); return 0
    args.result.parent.mkdir(parents=True,exist_ok=True)
    suffixes=['','.raw.json','.samples.csv','.effective-config.json','.stderr.log','.build-manifest.json','.process.json']
    if any(Path(str(args.result)+x).exists() for x in suffixes):
        raise ValueError('result or companion exists; choose a new RESULT_PATH to retain every attempt')
    raw_path=Path(str(args.result)+'.raw.json'); samples_path=Path(str(args.result)+'.samples.csv')
    config_path=Path(str(args.result)+'.effective-config.json'); stderr_path=Path(str(args.result)+'.stderr.log')
    build_path=Path(str(args.result)+'.build-manifest.json')
    e.update(JAVA_PEER_RAW=str(raw_path),JAVA_PEER_SAMPLES=str(samples_path))
    start=dt.datetime.now(dt.timezone.utc)
    with stderr_path.open('x') as log:
        run,resources=owned_run(command+[args.command],e,log,c,Path(str(args.result)+'.process.json'))
    end=dt.datetime.now(dt.timezone.utc)
    if not raw_path.exists():
        raise RuntimeError(f'Java peer failed before delivery artifact (exit={run.returncode}); retained stderr at {stderr_path}')
    raw=json.loads(raw_path.read_text()); raw['resources'].update(resources); c=effective(c,raw['effective_config']); write(config_path,c); write(build_path,manifest)
    # Detect accidental source/product changes during a measurement, too.
    verified_command(binary,manifest,pin)
    samples=[float(row['latency_us']) for row in csv.DictReader(samples_path.open())]
    timed=raw['timed']; warm=raw['warmup']; acked=timed['acknowledged']; duration=timed['elapsed_s']
    v=raw['verification']; hw=raw['high_watermarks']; d=raw['durability']; res=raw['resources']
    verified=(run.returncode==0 and d['verified'] and hw['queried'] and hw['total_offset_delta']==acked and
              v['verified_ids']==c['count'] and not v['duplicate_ids'] and not v['bad_records'])
    failed=run.returncode!=0
    utc=lambda t:t.isoformat().replace('+00:00','Z')
    artifacts=[]
    for path,kind in [(raw_path,'raw_delivery'),(samples_path,'raw_latency'),(config_path,'effective_config'),(stderr_path,'stderr'),(build_path,'build_manifest'),(Path(str(args.result)+'.process.json'),'process_ownership')]:
        artifacts.append(dict(path=str(path),type=kind,sha256=sha(path),size_bytes=path.stat().st_size))
    outcomes={k:timed[k] for k in ('offered','accepted','acknowledged','rejected','timed_out','unknown')}
    outcomes['consumed']=v['verified_ids']
    errors=[]
    for phase,counts in [('warmup',warm),('steady_state',timed)]:
        errors.extend(dict(**err,fatal=True,phase=phase) for err in counts['errors'])
    result=dict(schema_version='1.0.0',contract_version='1.1.0',
      suite_hold=dict(status='active',policy='Unsigned samples do not lift Suite HOLD',note='Adapter validation only; no comparison or scenario qualification'),
      scenario=dict(scenario_id=os.environ.get('SCENARIO_ID','java-pipelined-peer-smoke'),profile='secure' if c['security_protocol']!='PLAINTEXT' else 'bulk',
        tier='exploratory',peer='kafka-clients',cell_disposition='failed' if failed else 'executed',
        equal_semantics=dict(durability=dict(replication_factor=d['replication_factor'],min_insync_replicas=d['min_insync_replicas']),
          acks=c['acks'],idempotence=c['idempotence'],isolation=c['isolation_level'],security=dict(protocol=c['security_protocol'],mechanism=c['sasl_mechanism'] or 'NONE'))),
      provenance=dict(source=dict(git_commit=output(['git','-C',str(ROOT),'rev-parse','HEAD']),git_branch=output(['git','-C',str(ROOT),'branch','--show-current']),
          repo_url='https://github.com/mingley/partitionline',clean=not output(['git','-C',str(ROOT),'status','--porcelain'],''),
          tree_hash=output(['git','-C',str(ROOT),'rev-parse','HEAD^{tree}']),library_repository=pin['repository'],library_release=pin['tag'],
          peer_source_sha256=manifest['sources']['benchmarks/peers/java/BenchmarkPeer.java'],adapter_source_sha256=sha(HERE/'run.py'),note='Dirty checkout: exact peer, adapter and dependency bytes are hashed in the build manifest'),
        binary=dict(name='apache-java-pipelined-peer',path=str(binary),sha256=sha(binary)),
        config=dict(path=str(config_path),sha256=sha(config_path),effective_settings=c),
        toolchains=dict(compiler=manifest['compiler'],runtime='JDK '+manifest['java_version']+' / Apache kafka-clients '+raw['version'],build_tool=manifest['build_tool'],library_release=pin['tag']),
        broker=dict(image=os.environ.get('BROKER_IMAGE','unreported'),version=os.environ.get('BROKER_VERSION','unreported'),
          mode=os.environ.get('BROKER_MODE','kraft'),cluster_id=raw['cluster_id'] or 'unreported',node_count=raw['broker_nodes'] or 1,endpoints=c['bootstrap'].split(','),
          identity_note='Image/version supplied by operator; cluster ID, nodes, RF and minISR queried by Java Admin API. Version is not inferred from ApiVersions'),
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
          rss=dict(peak_rss_bytes=res['peak_rss_bytes'],average_rss_bytes=0,unit='bytes',note='Peak from owned JVM Linux /proc sampling; average unmeasured'),threads_count=res['threads_count'],
          threads_note='Peak Java thread count; CPU from wait4 and RSS sampled across produce + audit, not steady-state isolation'),
        broker_resources=dict(cpu_utilization_pct=0,cpu_unit='percent',peak_rss_bytes=0,rss_unit='bytes',disk_write_bytes=0,disk_write_unit='bytes',note='Unmeasured external broker'),
        errors=errors,callback_failures=timed['callback_failures'],warmup_callback_failures=warm['callback_failures'],queue_full_retries=timed['queue_full_retries']),
      integrity=dict(verified=verified,record_ids=dict(start_id=0,end_id=c['count']-1,expected_count=acked,verified_count=v['verified_ids'],
        missing_ids_count=max(0,acked-v['verified_ids']),duplicate_ids_count=v['duplicate_ids'],checksum_algorithm='exact deterministic bytes',
        payload_checksum_matches=verified),high_watermark_audit=dict(partitions=hw['partitions'],total_offset_delta=hw['total_offset_delta'],matches_acknowledged=hw['queried'] and hw['total_offset_delta']==acked),
        idempotence_sequence_verified=False,integrity_failure=failed),
      repetition_history=dict(total_attempts=1,failed_attempts=int(failed),attempts=[dict(attempt_number=1,repetition_index=integer('REPETITION_INDEX',1,1),
        status='failed_timeout' if timed['timed_out'] else ('failed_broker_error' if failed else 'passed_measurement'),integrity_failure=failed,
        error_message=f'Java SDK peer exit={run.returncode}; delivery callback failures={timed["callback_failures"]}' if failed else None,timestamp_utc=utc(end))]))
    write(args.result,result)
    print(json.dumps(dict(result=str(args.result),acked=acked,callback_failures=timed['callback_failures'],verified=verified,exit_code=run.returncode)))
    return run.returncode

if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError, OSError, RuntimeError, KeyError) as error:
        print(f'Java peer: {error}',file=sys.stderr); sys.exit(2)

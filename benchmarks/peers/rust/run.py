#!/usr/bin/env python3
"""Rust wrapper adapter; Java receipts and Python history gate are outside timing."""
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
import audit

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
spec=importlib.util.spec_from_file_location('native_c_peer_helpers', HERE.parent/'librdkafka/run.py')
cpeer=importlib.util.module_from_spec(spec); spec.loader.exec_module(cpeer)

def write(path,data):
    with Path(path).open('x') as f: json.dump(data,f,indent=2); f.write('\n')

def tier():
    value=os.environ.get('TIER','exploratory')
    if value not in ('required','exploratory'):
        raise ValueError('TIER must be required|exploratory')
    return value

def zero_raw(c):
    phase=dict(offered=0,accepted=0,acknowledged=0,rejected=0,timed_out=0,unknown=0,
               callback_failures=0,queue_full_retries=0,elapsed_s=0,errors=[])
    return dict(client='rust-rdkafka-BaseProducer',version='0.39.0',runtime={},effective_config={},
                warmup=phase.copy(),timed=phase.copy(),cluster_id='',broker_nodes=0,
                durability=dict(verified=False,replication_factor=1,min_insync_replicas=1),
                high_watermarks=dict(queried=False,partitions=[],total_offset_delta=0),
                verification=dict(verified_ids=0,duplicate_ids=0,bad_records=0,performed=False),
                resources=dict(user_cpu_seconds=0,system_cpu_seconds=0,peak_rss_bytes=0,threads_count=1))

def verify_build(binary):
    manifest=json.loads((binary.parent/'build-manifest.json').read_text())
    pin=json.loads((HERE/'source-pin.json').read_text())
    if (manifest['wrapper_version']!=pin['wrapper']['version'] or manifest['binding_version']!=pin['bindings']['version']
            or manifest['native_version']!=pin['native']['version'] or manifest['commit']!=pin['native']['commit']
            or manifest['binary_sha256']!=cpeer.sha(binary)
            or manifest['library_sha256']!=cpeer.sha(binary.parent/'lib/librdkafka.so.1')):
        raise ValueError('binary/native library differs from exact build manifest')
    for relative,digest in manifest['source_sha256'].items():
        if cpeer.sha(HERE/relative)!=digest:
            raise ValueError('adapter/source/lock bytes changed since this build; rebuild')
    for relative,digest in manifest['shared_helpers_sha256'].items():
        if cpeer.sha(ROOT/relative)!=digest:
            raise ValueError('shared settings/history helper bytes changed since build; rebuild')
    return manifest,pin

def child(args,env,log):
    """Linux wait4 gives this child process's own CPU/RSS, excluding the audit JVM."""
    p=subprocess.Popen(args,env=env,stdout=log,stderr=subprocess.STDOUT)
    _,status,usage=os.wait4(p.pid,0); p.returncode=os.waitstatus_to_exitcode(status)
    return p.returncode,usage

def java_audit(a,c,raw,paths,env):
    if c['security_protocol']!='PLAINTEXT':
        raise ValueError('independent Java receipt audit supports PLAINTEXT only; secure cells remain not_run')
    if a.kafka_home is None:
        raise ValueError('roundtrip requires --kafka-home from the inspected Kafka 3.9.1 distribution')
    home=a.kafka_home.resolve(); jar=home/'libs/kafka-clients-3.9.1.jar'
    pin=json.loads((HERE/'source-pin.json').read_text())['independent_auditor']
    if not jar.is_file() or cpeer.sha(jar)!=pin['jar_sha256']:
        raise ValueError('independent Kafka 3.9.1 auditor jar differs from exact pin')
    classes=Path(str(a.result)+'.java-classes'); classes.mkdir()
    classpath=str(jar)+os.pathsep+str(home/'libs/*')
    with paths['audit_log'].open('x') as log:
        compile_run=subprocess.run(['java','--module','jdk.compiler/com.sun.tools.javac.Main','-cp',classpath,'-d',str(classes),str(HERE/'ReceiptAudit.java')],stdout=log,stderr=subprocess.STDOUT)
        if compile_run.returncode: raise RuntimeError('independent Java auditor compilation failed')
        with paths['fences'].open('x') as f:
            for p in raw['high_watermarks']['partitions']:
                f.write(f"{p['partition']}\t{p['start_offset']}\t{p['end_offset']}\n")
        cmd=['java','-cp',str(classes)+os.pathsep+classpath,'ReceiptAudit',c['bootstrap'],c['topic'],
             c['isolation_level'],str(paths['fences']),str(paths['receipts']),str(c['consume_timeout_ms'])]
        run=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=c['consume_timeout_ms']/1000+30)
    identity=dict(client='org.apache.kafka:kafka-clients',version='3.9.1',jar_sha256=cpeer.sha(jar),
                  source_sha256=cpeer.sha(HERE/'ReceiptAudit.java'),class_sha256=cpeer.sha(classes/'ReceiptAudit.class'),
                  compiler=cpeer.output(['java','--module','jdk.compiler/com.sun.tools.javac.Main','-version']),
                  runtime=subprocess.check_output(['java','-version'],text=True,stderr=subprocess.STDOUT).strip(),
                  command=cmd,exit_code=run.returncode)
    write(paths['auditor'],identity)
    history,verdict=audit.verify(c,raw,audit.read_rows(paths['attempts']),audit.read_rows(paths['receipts']))
    write(paths['history'],history); write(paths['verdict'],verdict)
    return verdict, run.returncode

def result(c,raw,manifest,pin,binary,paths,start,end,disposition,reason,exit_code):
    timed=raw['timed']; warm=raw['warmup']; v=raw['verification']; hw=raw['high_watermarks']; d=raw['durability']; res=raw['resources']
    samples=[float(row['latency_us']) for row in csv.DictReader(paths['samples'].open())]
    acked=timed['acknowledged']; duration=timed['elapsed_s']; elapsed=max((end-start).total_seconds(),1e-9)
    verified=(exit_code==0 and v.get('independent_history_valid',False) and d['verified'] and hw['queried']
              and hw['total_offset_delta']==acked and v['verified_ids']==c['count'] and not v['duplicate_ids'] and not v['bad_records'])
    failed=disposition=='failed'; utc=lambda t:t.isoformat().replace('+00:00','Z')
    artifacts=[dict(path=str(p),type=kind,sha256=cpeer.sha(p),size_bytes=p.stat().st_size)
               for kind,p in paths.items() if p.is_file()]
    host=cpeer.host(); host['hostname']='sanitized-local-host'
    errors=[dict(**e,fatal=True,phase=phase) for phase,counts in [('warmup',warm),('steady_state',timed)] for e in counts['errors']]
    if failed and not errors: errors=[dict(code='adapter_failure',name=reason or 'driver/auditor failure',count=1,fatal=True,phase='steady_state')]
    outcomes={k:timed[k] for k in ('offered','accepted','acknowledged','rejected','timed_out','unknown')}; outcomes['consumed']=v['verified_ids']
    return dict(schema_version='1.0.0',contract_version='1.1.0',
      suite_hold=dict(status='active',policy='Unsigned adapter samples do not lift Suite HOLD',note='No comparison, qualification, or performance claim'),
      scenario=dict(scenario_id=os.environ.get('SCENARIO_ID','rust-rdkafka-peer-smoke'),profile=os.environ.get('PROFILE','bulk'),tier=tier(),peer='peer-adapter',cell_disposition=disposition,
        equal_semantics=dict(durability=dict(replication_factor=d['replication_factor'],min_insync_replicas=d['min_insync_replicas']),acks=c['acks'],idempotence=c['idempotence'],isolation=c['isolation_level'],security=dict(protocol=c['security_protocol'],mechanism=c['sasl_mechanism'] or 'NONE'),note=reason)),
      provenance=dict(source=dict(git_commit=cpeer.output(['git','-C',str(ROOT),'rev-parse','HEAD']),git_branch=cpeer.output(['git','-C',str(ROOT),'branch','--show-current']),repo_url='https://github.com/mingley/partitionline',clean=not cpeer.output(['git','-C',str(ROOT),'status','--porcelain'],''),tree_hash=cpeer.output(['git','-C',str(ROOT),'rev-parse','HEAD^{tree}']),peer='rust-rdkafka',wrapper_pin=pin['wrapper'],binding_pin=pin['bindings'],native_pin=pin['native'],source_sha256=manifest['source_sha256'],note='Standalone native wrapper; its instrumentation/API cost belongs to this bar, never the C-only bar'),
        binary=dict(name='rust-rdkafka-BaseProducer-peer',path=str(binary),sha256=cpeer.sha(binary)),
        config=dict(path=str(paths['config']),sha256=cpeer.sha(paths['config']),effective_settings=c),
        toolchains=dict(compiler=manifest['compiler'],runtime='rust-rdkafka 0.39.0 / actual librdkafka 2.15.0 / binding headers 2.12.1',build_tool=manifest['build_tool'],native_runtime=raw.get('runtime',{})),
        broker=dict(image=os.environ.get('BROKER_IMAGE','unreported'),version=os.environ.get('BROKER_VERSION','unreported'),mode=os.environ.get('BROKER_MODE','kraft'),cluster_id=raw['cluster_id'] or 'unreported',node_count=raw['broker_nodes'] or 1,endpoints=c['bootstrap'].split(','),identity_note='Distribution identity supplied by operator; metadata/cluster/RF/minISR queried before timed phase'),
        host=host,topology=dict(environment=os.environ.get('BENCH_ENVIRONMENT','loopback'),rtt_ms=float(os.environ.get('RTT_MS','0')),rtt_unit='milliseconds',client_nodes=1,broker_nodes=raw['broker_nodes'] or 1,network_interface='lo',rtt_note='Unmeasured unless RTT_MS supplied; process affinity recorded externally'),
        timestamps=dict(start_time_utc=utc(start),end_time_utc=utc(end),duration_seconds=(end-start).total_seconds(),duration_unit='seconds'),
        seeds=dict(payload_seed=hex(c['record_seed']),key_seed=hex(c['record_seed']),partition_seed='explicit index modulo partitions',repetition_seed='0x5EED0004'),artifacts=artifacts),
      execution=dict(phase='steady_state',warmup_completed=warm['acknowledged']==c['warmup'],warmup_records=warm['acknowledged'],warmup_duration_seconds=warm['elapsed_s'],steady_state_duration_seconds=duration or (end-start).total_seconds(),measured_phase_present=bool(duration),duration_interpretation='Producer timed phase' if duration else 'Schema-required positive duration is actual metadata-filing/failed-driver wall time; no steady-state workload occurred',repetition_index=cpeer.integer('REPETITION_INDEX',1,1),total_repetitions=cpeer.integer('TOTAL_REPETITIONS',1,1),pairing_order=os.environ.get('PAIRING_ORDER','standalone adapter validation'),coordinated_omission_avoidance=dict(enabled=False,schedule_type='closed_loop'),contract_sample_floors_met=False,note=reason or 'Exploratory correctness smoke; independent Java receipt audit is outside producer timing; no frozen campaign executed'),
      outcomes=outcomes,measurements=dict(throughput=dict(records_per_second=acked/duration if duration else 0,records_per_second_unit='records/s',megabytes_per_second=acked*c['payload_bytes']/duration/1e6 if duration else 0,megabytes_per_second_unit='MB/s',total_bytes_transferred=acked*c['payload_bytes'],total_bytes_unit='bytes',mb_definition='1 MB=1000000 value bytes; keys/protocol excluded'),
        latency=cpeer.latency(samples),client_resources=dict(cpu_utilization_pct=100*(res['user_cpu_seconds']+res['system_cpu_seconds'])/max(res.get('producer_wall_seconds',elapsed),1e-9),cpu_unit='percent',user_cpu_seconds=res['user_cpu_seconds'],system_cpu_seconds=res['system_cpu_seconds'],cpu_seconds_unit='seconds',allocations=dict(total_allocated_bytes=0,allocation_count=0,unit='bytes',note='Uninstrumented'),rss=dict(peak_rss_bytes=res['peak_rss_bytes'],average_rss_bytes=0,unit='bytes',note='Producer child wait4 peak; average unmeasured'),threads_count=res['threads_count'],threads_note='Producer procfs snapshot; CPU/RSS include warmup, native hash/inspection and producer history hashing/I/O; CPU denominator is producer child lifetime, excludes Java audit; no isolated steady-state efficiency claim'),
        broker_resources=dict(cpu_utilization_pct=0,cpu_unit='percent',peak_rss_bytes=0,rss_unit='bytes',disk_write_bytes=0,disk_write_unit='bytes',note='Unmeasured'),errors=errors,callback_failures=timed['callback_failures'],warmup_callback_failures=warm['callback_failures'],queue_full_retries=timed['queue_full_retries']),
      integrity=dict(verified=verified,record_ids=dict(start_id=0,end_id=c['count']-1,expected_count=acked,verified_count=v['verified_ids'],missing_ids_count=max(0,acked-v['verified_ids']),duplicate_ids_count=v['duplicate_ids'],checksum_algorithm='Independent exact deterministic bytes plus SHA256 and shared history gate',payload_checksum_matches=verified),high_watermark_audit=dict(partitions=hw['partitions'],total_offset_delta=hw['total_offset_delta'],matches_acknowledged=hw['queried'] and hw['total_offset_delta']==acked),idempotence_sequence_verified=False,integrity_failure=failed),
      repetition_history=dict(total_attempts=1,failed_attempts=int(disposition!='executed'),attempts=[dict(attempt_number=1,repetition_index=cpeer.integer('REPETITION_INDEX',1,1),status='failed_timeout' if timed['timed_out'] else ('failed_broker_error' if failed else 'passed_measurement' if disposition=='executed' else 'failed_abort'),integrity_failure=failed,error_message=reason if disposition!='executed' else None,timestamp_utc=utc(end),broker_workload_attempted=disposition in ('executed','failed'))]))

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['emit-config','produce','roundtrip','unsupported','not-run'])
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--result',type=Path,default=Path('rust-peer-result.json'))
    p.add_argument('--kafka-home',type=Path)
    p.add_argument('--reason',default='')
    a=p.parse_args(); c=cpeer.settings(); tier(); binary=a.binary.resolve(); manifest,pin=verify_build(binary)
    if a.command in ('unsupported','not-run') and not a.reason: raise ValueError('explicit disposition reason required')
    if a.command=='roundtrip' and not all(os.environ.get(k) for k in ('BROKER_IMAGE','BROKER_VERSION')):
        raise ValueError('roundtrip requires inspected BROKER_IMAGE and BROKER_VERSION')
    if a.command=='emit-config':
        import tempfile
        with tempfile.TemporaryDirectory(prefix='rust-peer-config-') as temp:
            config=Path(temp)/'config.json'; write(config,c)
            env=cpeer.process_env(c); env.update(RUST_PEER_CONFIG=str(config),RUST_PEER_NATIVE_SHA256=manifest['library_sha256'])
            dump=json.loads(subprocess.check_output([str(binary),'emit-config'],env=env,text=True))
        print(json.dumps(dict(settings=cpeer.effective(c,dump['effective_config']),runtime=dump['runtime']),indent=2)); return 0
    a.result=a.result.resolve(); a.result.parent.mkdir(parents=True,exist_ok=True)
    suffix=dict(input='.input-config.json',config='.effective-config.json',raw='.raw.json',producer_raw='.producer-raw.json',samples='.samples.csv',stderr='.stderr.log',build='.build-manifest.json',attempts='.attempts.jsonl',receipts='.receipts.jsonl',fences='.fences.tsv',audit_log='.audit.log',auditor='.auditor.json',history='.history.json',verdict='.history-verdict.json')
    paths={k:Path(str(a.result)+v) for k,v in suffix.items()}
    if a.result.exists() or any(p.exists() for p in paths.values()) or Path(str(a.result)+'.java-classes').exists():
        raise ValueError('attempt or companion artifact exists; choose a fresh result path')
    write(paths['input'],c); write(paths['build'],manifest)
    env=cpeer.process_env(c); env.update(RUST_PEER_CONFIG=str(paths['input']),RUST_PEER_NATIVE_SHA256=manifest['library_sha256'],RUST_PEER_RAW=str(paths['raw']),RUST_PEER_SAMPLES=str(paths['samples']),RUST_PEER_HISTORY=str(paths['attempts']))
    start=dt.datetime.now(dt.timezone.utc); exit_code=0; raw=zero_raw(c); reason=a.reason
    if a.command in ('unsupported','not-run'):
        with paths['stderr'].open('x') as f: f.write(reason+'\n')
        paths['samples'].write_text('record_id,latency_us\n'); write(paths['raw'],raw)
        disposition='unsupported' if a.command=='unsupported' else 'not_run'
    else:
        with paths['stderr'].open('x') as log: exit_code,usage=child([str(binary),'produce'],env,log)
        producer_end=dt.datetime.now(dt.timezone.utc)
        if paths['raw'].exists():
            raw=json.loads(paths['raw'].read_text())
            with paths['producer_raw'].open('xb') as original: original.write(paths['raw'].read_bytes())
        else: write(paths['raw'],raw); reason='Driver failed before delivery artifact; stderr retained'
        if not paths['samples'].exists(): paths['samples'].write_text('record_id,latency_us\n')
        raw['resources'].update(user_cpu_seconds=usage.ru_utime,system_cpu_seconds=usage.ru_stime,peak_rss_bytes=usage.ru_maxrss*1024,producer_wall_seconds=(producer_end-start).total_seconds())
        if raw['effective_config']: c=cpeer.effective(c,raw['effective_config'])
        if a.command=='roundtrip' and raw['high_watermarks']['queried']:
            try:
                verdict,audit_exit=java_audit(a,c,raw,paths,env)
                raw['verification']=dict(verified_ids=verdict['verified_ids'],duplicate_ids=verdict['duplicate_ids'],bad_records=verdict['bad_records'],performed=True,independent_history_valid=verdict['valid'])
                if audit_exit or not verdict['valid']: exit_code=exit_code or 1; reason='Independent receipt/history integrity gate failed'
            except (ValueError,OSError,RuntimeError,subprocess.TimeoutExpired) as error:
                exit_code=exit_code or 1; reason=str(error)
                with paths['stderr'].open('a') as log: log.write('audit failure: '+str(error)+'\n')
        elif a.command=='roundtrip': exit_code=exit_code or 1; reason='High-watermark inspection failed; receipt gate could not run'
        if a.command=='roundtrip' and not raw['durability']['verified']:
            exit_code=exit_code or 1; reason='Broker RF/minISR inspection failed; roundtrip cannot qualify integrity'
        # Initial raw file is exclusively created by driver; this rewrite appends the independent audit outcome.
        paths['raw'].write_text(json.dumps(raw,indent=2)+'\n')
        disposition='failed' if exit_code else 'executed'
    end=dt.datetime.now(dt.timezone.utc); write(paths['config'],c)
    artifact=result(c,raw,manifest,pin,binary,paths,start,end,disposition,reason,exit_code)
    write(a.result,artifact)
    print(json.dumps(dict(result=str(a.result),disposition=disposition,acknowledged=raw['timed']['acknowledged'],verified=artifact['integrity']['verified'],exit_code=exit_code)))
    return exit_code

if __name__=='__main__':
    try: sys.exit(main())
    except (ValueError,OSError,RuntimeError,KeyError,subprocess.CalledProcessError) as error:
        print(f'rust peer adapter: {error}',file=sys.stderr); sys.exit(2)

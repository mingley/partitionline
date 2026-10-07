#!/usr/bin/env python3
"""Measure explicit Tokio runtimes against owned null and native Kafka brokers."""
import argparse
import copy
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time

CONFIGS = [dict(name='current_thread', flavor='current_thread', workers=0)] + [
    dict(name=f'multi_thread_{n}', flavor='multi_thread', workers=n) for n in (1,2,4,5)]


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def json_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]


def broker_sample(pid):
    root = Path('/proc')/str(pid)
    return dict(stat=(root/'stat').read_text(), status=(root/'status').read_text(),
        io=(root/'io').read_text(), monotonic_ns=time.monotonic_ns(), clock_ticks=os.sysconf('SC_CLK_TCK'))


def free_ports():
    holders = [socket.socket(), socket.socket()]
    try:
        for sock in holders: sock.bind(('127.0.0.1',0))
        return [sock.getsockname()[1] for sock in holders]
    finally:
        for sock in holders: sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('family', choices=['nb','native'])
    for name in ('source','source-pins','output','binaries'):
        parser.add_argument('--'+name,type=Path,required=True)
    for name in ('kafka-homes','sdk-jar','slf4j'):
        parser.add_argument('--'+name,type=Path)
    parser.add_argument('--commit',required=True)
    a = parser.parse_args()
    source = a.source.resolve()
    tools = source/'benchmarks/runtime/tools'
    def interrupt(signum, frame):
        raise InterruptedError(f'owner received signal {signum}')
    for signum in (signal.SIGTERM, signal.SIGINT): signal.signal(signum, interrupt)
    baseline = module(source/'scripts/record-local-baseline.py','runtime_owned_recorder')
    adapter = module(tools/'native-result.py','runtime_native_result')
    replay = module(tools/'open-loop-replay.py','runtime_open_loop_replay')
    args = argparse.Namespace(source=source,source_pins=a.source_pins,output=a.output,commit=a.commit,
        family='runtime-'+a.family,repetitions=5,seed=961,cpus='2,4',cells=None,
        binary=[(name,a.binaries/name) for name in
            ('runtime','nb-serve','native-produce-runtime','native-latency-runtime')])
    r = baseline.Recorder(args)
    r.host_observation = json.loads((r.output/'host-before.json').read_text())
    if r.host_observation['cpu_affinity'] != [0,1,2,3,4]:
        raise ValueError('the declared five-CPU host policy does not match the observed affinity')
    r.tree_hash = subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD^{tree}'],text=True).strip()
    base_command = r.command
    def command(argv,directory,label,timeout=400,extra_env=None):
        return base_command([sys.executable,'-B',tools/'parent-bound-exec.py',str(os.getpid()),*argv],
            directory,label,timeout,extra_env)
    r.command = command
    launcher = r.output/'nb-serve-parent-bound'
    launcher.write_text('#!/bin/sh\nexec '+shlex.join([sys.executable,'-B',str(tools/'parent-bound-exec.py')])+
        ' "$PPID" '+shlex.quote(str(r.binaries['nb-serve']))+' "$@"\n')
    launcher.chmod(0o700)
    r.env['NB_SERVE'] = str(launcher)
    for path in [Path(__file__).resolve(),launcher,*tools.glob('*')]:
        if path.is_file(): r.input_pins[str(path)]=baseline.sha(path)
    rng = random.Random(961)
    primary=[]
    cells=['nb-produce-bulk','nb-fetch-bulk'] if a.family=='nb' else ['lb-bulk','lb-latency-openloop']
    for rep in range(1,6):
        jobs=[dict(config=config,cell=cell,rep=rep,cohort='primary',load=load)
            for config in CONFIGS for cell in cells
            for load in ([10,50,80] if cell=='lb-latency-openloop' else [None])]
        rng.shuffle(jobs)
        primary.extend(jobs)
    reproduce=[]
    for rep in range(1,6):
        for cell in cells:
            for load in ([10,50,80] if cell=='lb-latency-openloop' else [None]):
                reproduce.append(dict(config=CONFIGS[0],cell=cell,rep=rep,cohort='reproduce',load=load))
    baseline.save(r.output/'matrix-plan.json',dict(scope='local/unsigned',suite_hold='active',
        source_commit=a.commit,configs=CONFIGS,client_cpus=[2,4],native_broker_cpus=[0,1],
        N_definition='5 is the observed host affinity CPU count, fixed before measurement. Four/five workers oversubscribe the two client CPUs; no extra CPU is granted.',
        primary=primary,reproduce=reproduce,random_seed=961,repetitions=5,
        native_bulk=dict(timed_records=8_000_000,warmup=10_000,payload_bytes=100,partitions=6,
            key_mode='id',payload_mode='seeded',seed=1592590337,acks=1,idempotence=False,
            linger_ms=5,batch_bytes=1048576,batch_records=32768,max_in_flight=5,connections=1),
        open_loop=dict(timed_offers=20000,warmup=10000,sample_floor=10000,
            offered_load_policy='Same absolute 10/50/80 percent rates for every runtime, based on five fresh current-thread sequential calibration runs. Percentages refer to the current-thread calibration only, not per-flavor saturation.',
            failed_capacity_runs='Retain normal exit1 and every raw rejection; independently verify all acknowledged payloads and offsets. Never relabel as executed.',
            payload='100 bytes of x; null key; partition0; no unique payload IDs',batch_records=1,
            max_in_flight=1,connections=1,linger_ms=0,max_pending=1024),
        uncertainty='Five matched repetitions; paired bootstrap comparisons and per-arm median bootstrap CIs. Raw per-run normal mean intervals are diagnostic.',
        reproduce_policy='Five fresh current-thread repetitions after the primary matrix; retain all differences and report median deltas without treating noisy local observations as a production gate.'))
    baseline.save(r.output/'runner-inputs.json',r.input_pins)
    report = module(source/'scripts/benchmark-report.py','runtime_report_controls')
    def validator(path,directory):
        r.command([sys.executable,'-B',source/'scripts/benchmark-report.py',path],directory,'validator',15)
        data=json.loads(path.read_text())
        if data['provenance']['source']['git_commit'] != a.commit:
            raise ValueError('artifact source differs')
        return data
    def nb():
        for index,job in enumerate(primary+reproduce):
            directory=r.output/f'{index:03d}-{job["cohort"]}-r{job["rep"]}-{job["config"]["name"]}-{job["cell"]}'
            directory.mkdir();config=job['config']
            print('start '+directory.name,flush=True)
            r.command([r.binaries['runtime'],'--cell',job['cell'],'--out',directory,'--repetitions','1',
                '--runtime',config['flavor'],'--workers',str(config['workers'])],directory,'runtime',180)
            paths=list(directory.glob('*.result.json'))
            if len(paths)!=1: raise ValueError('one actual null-broker result required')
            data=validator(paths[0],directory)
            actual=data['provenance']['config']['effective_settings']['runtime']
            adapter.actual_runtime(actual,actual,config['flavor'],config['workers'])
            if data['scenario']['cell_disposition']!='executed': raise ValueError('null-broker result failed')
            row=dict(job,artifact=str(paths[0]),sha256=baseline.sha(paths[0]),
                measurements=data['measurements'],outcomes=data['outcomes'],runtime=actual)
            r.rows.append(row);baseline.save(directory/'validated.json',row)
            print('done '+directory.name,flush=True)
    def native():
        if not all((a.kafka_homes,a.sdk_jar,a.slf4j)): raise ValueError('native input paths are required')
        distribution=module(source/'tests/conformance/run-codec-matrix.py','runtime_native_distribution')
        home=distribution.archive_home(a.kafka_homes.resolve(),'4.3.1')
        if baseline.sha(a.sdk_jar)!=distribution.JAR_SHA:
            raise ValueError('Kafka SDK differs from the pinned 4.3.1 artifact')
        if baseline.sha(a.slf4j)!='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0':
            raise ValueError('slf4j artifact differs')
        for path in [a.sdk_jar.resolve(),a.slf4j.resolve(),*[p for d in ('bin','libs','config')
            for p in (home/d).rglob('*') if p.is_file()]]:
            r.input_pins[str(path)]=baseline.sha(path)
        baseline.save(r.output/'native-inputs.json',r.input_pins)
        classes=r.output/'java-classes';classes.mkdir()
        classpath=str(a.sdk_jar.resolve())+':'+str(a.slf4j.resolve())
        java_sources=[tools/(name+'.java') for name in ('RuntimeTopic','LatencyReadback','AuditAndDelete')]+[
            source/'tests/conformance/java/ConformanceBenchProduceSettings.java']
        r.command(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21',
            '-Xlint:all','-Werror','-cp',classpath,'-d',classes,*java_sources],r.output,'java-compile',30)
        for path in classes.rglob('*.class'):r.input_pins[str(path)]=baseline.sha(path)
        baseline.save(r.output/'compiled-inputs.json',r.input_pins)
        cp=str(classes)+':'+classpath
        broker_port,controller_port=free_ports();bootstrap=f'127.0.0.1:{broker_port}'
        properties=r.output/'server.properties'
        properties.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller_port}
listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}
advertised.listeners=PLAINTEXT://127.0.0.1:{broker_port}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={r.output/'data'}
num.network.threads=2
num.io.threads=4
num.partitions=6
offsets.topic.replication.factor=1
offsets.topic.num.partitions=3
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=3
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
delete.topic.enable=true
log.segment.bytes=134217728
log.segment.delete.delay.ms=0
''')
        r.env.update(KAFKA_HEAP_OPTS='-Xms128m -Xmx512m')
        cluster=r.command([home/'bin/kafka-storage.sh','random-uuid'],r.output,'cluster-uuid',30).read_text().strip()
        r.command([home/'bin/kafka-storage.sh','format','-t',cluster,'-c',properties],r.output,'format',30)
        owner=None;supervisor=None;stop=threading.Event();log=(r.output/'broker.log').open('xb')
        try:
            r.guard()
            owner=subprocess.Popen([sys.executable,'-B',str(tools/'parent-bound-exec.py'),str(os.getpid()),
                'taskset','-c','0,1',str(home/'bin/kafka-server-start.sh'),str(properties)],env=r.env,
                stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            def supervise():
                deadline=time.monotonic()+7200
                while not stop.wait(.1):
                    if time.monotonic()>=deadline or (r.output/'broker.log').stat().st_size>64*1024*1024:
                        os.kill(os.getpid(),signal.SIGTERM);return
            supervisor=threading.Thread(target=supervise,name='runtime-native-lease');supervisor.start()
            deadline=time.monotonic()+60
            while 'Kafka Server started' not in (r.output/'broker.log').read_text():
                if owner.poll() is not None:raise RuntimeError('owned broker exited at startup')
                if time.monotonic()>=deadline:raise TimeoutError('broker startup deadline')
                time.sleep(.05)
            identity=r.command([home/'bin/kafka-cluster.sh','cluster-id','--bootstrap-server',bootstrap],
                r.output,'identity',30).read_text()
            if cluster not in identity:raise ValueError('actual broker cluster differs')
            broker=dict(pid=owner.pid,cluster=cluster,bootstrap=bootstrap,
                archive_sha256=distribution.ARCHIVES['4.3.1'],ports=[broker_port,controller_port])
            baseline.save(r.output/'broker-started.json',broker)
            def create(directory,topic,partitions):
                path=r.command(['java','-Xms64m','-Xmx256m','-cp',cp,'RuntimeTopic',bootstrap,topic,str(partitions)],
                    directory,'topic-create',30)
                rows=json_lines(path)
                if len(rows)!=1 or rows[0]['status']!='created' or rows[0]['partitions']!=partitions:
                    raise ValueError('actual topic creation differs')
                return rows[0]
            def audit_delete(directory,topic,mode,parameters,expected):
                path=r.command(['java','-Xms128m','-Xmx384m','-cp',cp,'AuditAndDelete',mode,bootstrap,topic,*parameters],
                    directory,'java-audit-delete',60)
                rows=json_lines(path)
                if (len(rows)!=2 or rows[0]['status']!='pass' or rows[0]['verified']!=expected
                        or not rows[0]['consumer_closed'] or rows[1]['status']!='deletion_requested'
                        or not rows[1]['admin_closed'] or rows[1]['topic']!=topic):
                    raise ValueError('independent readback/deletion differs')
                deadline=time.monotonic()+30
                while list((r.output/'data').glob(topic+'-*')):
                    if time.monotonic()>=deadline:raise TimeoutError('owned topic files not removed')
                    time.sleep(.05)
                baseline.save(directory/'topic-closure.json',dict(topic=topic,readback_completed=True,
                    deletion_after_readback=True,owned_topic_files_absent=True))
                return rows[0]
            def runtime_env(config,directory):
                return dict(PL_BENCH_RUNTIME_FLAVOR=config['flavor'],PL_BENCH_RUNTIME_WORKERS=str(config['workers']),
                    PL_BENCH_RUNTIME_OBSERVATION=str(directory/'runtime.json'),
                    PL_BENCH_WARMUP_OBSERVATION=str(directory/'warmup.json'))
            latency_env=dict(KAFKA_BOOTSTRAP=bootstrap,MODE='produce',COUNT='20000',WARMUP='10000',
                PAYLOAD_BYTES='100',ACKS='1',LINGER_MS='0',MAX_PENDING='1024',SAMPLE_FLOOR='10000',
                BUFFER_MEMORY='33554432',MAX_BLOCK_MS='1000',DELIVERY_TIMEOUT_MS='30000',REQUEST_TIMEOUT_MS='30000')
            calibrations=[]
            for rep in range(1,6):
                directory=r.output/f'calibration-{rep:02d}';directory.mkdir()
                topic=f'pl-runtime-{owner.pid}-cal-{rep}';create(directory,topic,1)
                print('start '+directory.name,flush=True)
                path=r.command([r.binaries['native-latency-runtime']],directory,'calibrate',180,
                    dict(latency_env,**runtime_env(CONFIGS[0],directory),KAFKA_TOPIC=topic,LATENCY_MODE='sequential-smoke'))
                values=json_lines(path)
                if len(values)!=1 or values[0]['kind']!='produce_ack' or values[0]['samples']!=20000:
                    raise ValueError('fresh sequential calibration differs')
                adapter.actual_runtime(adapter.read(directory/'runtime.json'),adapter.read(directory/'runtime.closure.json'),
                    'current_thread',0)
                actual_java=audit_delete(directory,topic,'latency',['30000'],30000)
                capacity=1e6/values[0]['mean_us']
                if not math.isfinite(capacity) or capacity<=0:raise ValueError('invalid observed calibration')
                row=dict(rep=rep,diagnostic=values[0],records_per_second=capacity,java=actual_java)
                calibrations.append(row);baseline.save(directory/'validated.json',row)
                print('done '+directory.name,flush=True)
            median=statistics.median(row['records_per_second'] for row in calibrations)
            rates={percent:max(1,round(median*percent/100)) for percent in (10,50,80)}
            baseline.save(r.output/'calibrated-rates.json',dict(capacity_values=[x['records_per_second'] for x in calibrations],
                current_thread_median_capacity=median,rates=rates,scope='Five fresh current-thread sequential means, integer-microsecond resolution; identical absolute arrivals across all flavors'))
            for index,job in enumerate(primary+reproduce):
                config=job['config'];cell=job['cell'];percent=job['load']
                directory=r.output/f'{index:03d}-{job["cohort"]}-r{job["rep"]}-{config["name"]}-{cell}-{percent or 0}'
                directory.mkdir();topic=f'pl-runtime-{owner.pid}-{index}'
                if os.statvfs(r.output).f_bavail*os.statvfs(r.output).f_frsize<1400*1024*1024:
                    raise ValueError('native cell requires at least 1400MiB free for its owned topic')
                created=create(directory,topic,6 if cell=='lb-bulk' else 1)
                env=runtime_env(config,directory)
                t0=time.monotonic_ns()
                with socket.create_connection(('127.0.0.1',broker_port),timeout=2):pass
                rtt_ms=(time.monotonic_ns()-t0)/1e6
                print('start '+directory.name,flush=True)
                if cell=='lb-bulk':
                    env.update(KAFKA_BOOTSTRAP=bootstrap,KAFKA_TOPIC=topic,COUNT='8000000',WARMUP='10000',
                        WARMUP_SECS='0',MEASURE_SECS='0',PAYLOAD_BYTES='100',RECORD_SEED='1592590337',
                        KEY_MODE='id',PAYLOAD_MODE='seeded',PARTITIONS='6',ACKS='1',IDEMPOTENT='0',
                        LINGER_MS='5',BATCH_BYTES='1048576',BATCH_RECORDS='32768',MAX_IN_FLIGHT='5',
                        CONNECTIONS='1',QUEUE_KBYTES='32768',RUN_TIMEOUT_MS='300000',COMPRESSION='none',
                        PL_BENCH_BULK_LATENCY_PATH=str(directory/'bulk-samples.json'))
                    r.command([r.binaries['native-produce-runtime'],'--print-config'],directory,'producer-config',15,
                        dict(env,PL_BENCH_RUNTIME_OBSERVATION=str(directory/'runtime-config.json')))
                else:
                    env.update(latency_env,KAFKA_TOPIC=topic,LATENCY_MODE='open-loop',RATE_PER_SECOND=str(rates[percent]))
                baseline.save(directory/'broker-before.json',broker_sample(owner.pid))
                binary=r.binaries['native-produce-runtime' if cell=='lb-bulk' else 'native-latency-runtime']
                try:
                    raw=r.command([sys.executable,'-B',tools/'measure-process.py',directory/'wait4.json',binary],
                        directory,'client',360 if cell=='lb-bulk' else max(180,math.ceil(20000/rates[percent])+90),env)
                except ValueError:
                    receipt=adapter.read(directory/'client.process.json')
                    if (cell!='lb-latency-openloop' or not receipt['parent_waited'] or receipt['exit_code']!=1
                            or receipt.get('failure') or r.owned.group_members(receipt['pid'])):raise
                    raw=directory/'client.stdout';r.guard()
                baseline.save(directory/'broker-after.json',broker_sample(owner.pid))
                rows=json_lines(raw);produced=rows.pop()
                if cell=='lb-bulk':
                    if rows:raise ValueError('unexpected bulk stdout population')
                    values=adapter.bulk_latencies(adapter.read(directory/'bulk-samples.json'),produced)
                    effective=produced['effective_settings'];duration=produced['elapsed_s'];warmup_seconds=produced['warmup_elapsed_s']
                    audited=audit_delete(directory,topic,'bulk',['10000','8000000','6','1592590337','100','id','seeded'],8_010_000)
                else:
                    replay.validate_open_loop(rows,produced,rates[percent])
                    values=[row['end_to_end_ns']//1000 for row in rows if row['outcome']=='acknowledged']
                    effective=dict(acks=1,linger_ms=0,batch_size_bytes=356,batch_records=1,max_in_flight=1,
                        idempotence=False,connections=1,compression='none',payload_bytes=100,buffer_memory_bytes=33554432,
                        max_block_ms=1000,delivery_timeout_ms=30000,request_timeout_ms=30000,
                        max_pending=1024,rate_per_second=rates[percent],count=20000)
                    duration=produced['elapsed_ns']/1e9
                    warmup=adapter.read(directory/'warmup.json')
                    if not warmup['completed'] or warmup['records']!=10000:raise ValueError('actual warmup incomplete')
                    warmup_seconds=warmup['elapsed_ns']/1e9
                    audited=audit_delete(directory,topic,'latency',[str(10000+produced['outcomes']['acknowledged'])],
                        10000+produced['outcomes']['acknowledged'])
                baseline.save(directory/'actual-completion.json',produced)
                observed=adapter.read(directory/'runtime.json');completed=adapter.read(directory/'runtime.closure.json')
                settings=dict(effective,runtime=adapter.actual_runtime(observed,completed,config['flavor'],config['workers']))
                with (directory/'effective-settings.json').open('xb') as file:
                    file.write(json.dumps(settings,sort_keys=True,separators=(',',':')).encode());file.flush();os.fsync(file.fileno())
                doc=adapter.build_result(r,directory,config,job['rep'],job['cohort'],cell,produced,audited,created,
                    broker,rtt_ms,adapter.read(directory/'runtime.resources.json'),observed,completed,raw,values,
                    effective,duration,warmup_seconds,None if percent is None else dict(percent=percent,rate_per_second=rates[percent],basis='current_thread_calibration'))
                result=directory/(cell+'.result.json');baseline.save(result,doc)
                data=validator(result,directory)
                # Check changed actual result controls, without running timed work again.
                controls=[]
                for field in ('watermark','identity','acks','runtime'):
                    changed=copy.deepcopy(data)
                    if field=='watermark':changed['integrity']['high_watermark_audit']['total_offset_delta']-=1
                    elif field=='identity':changed['integrity']['record_ids']['verified_count']-=1
                    elif field=='acks':changed['provenance']['config']['effective_settings']['acks']=-1
                    else:
                        wrong=copy.deepcopy(completed);wrong['observed_scheduler_workers']+=1
                        try:adapter.actual_runtime(observed,wrong,config['flavor'],config['workers'])
                        except ValueError:controls.append(dict(control=field,rejected=True));continue
                        raise ValueError('runtime mutation was accepted')
                    valid,errors,_=report.BenchmarkValidator().validate(changed)
                    if valid:raise ValueError('actual result corruption control was accepted: '+field)
                    controls.append(dict(control=field,rejected=True,errors=errors))
                baseline.save(directory/'changed-result-controls.json',controls)
                row=dict(job,artifact=str(result),sha256=baseline.sha(result),measurements=data['measurements'],
                    outcomes=data['outcomes'],runtime=completed,disposition=data['scenario']['cell_disposition'])
                r.rows.append(row);baseline.save(directory/'validated.json',row)
                print('done '+directory.name+' '+data['scenario']['cell_disposition'],flush=True)
        finally:
            stop.set()
            if supervisor is not None:
                supervisor.join(timeout=2)
                if supervisor.is_alive():raise RuntimeError('broker supervisor did not join')
            if owner is not None:
                try:os.killpg(owner.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                try:owner.wait(timeout=30)
                except subprocess.TimeoutExpired:pass
                adopted=r.owned.stop_group(owner)
                ports=[]
                for port in (broker_port,controller_port):
                    with socket.socket() as sock:
                        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);sock.bind(('127.0.0.1',port))
                    ports.append(dict(port=port,reusable=True))
                baseline.save(r.output/'closure.json',dict(parent_waited=True,exit_code=owner.returncode,
                    group_empty=not r.owned.group_members(owner.pid),adopted_children=adopted,
                    supervisor_joined=True,ports=ports))
            log.close()
    try:
        if a.family=='nb':nb()
        else:native()
        r.finish()
    except BaseException as error:
        baseline.save(r.output/'failure.json',dict(error=type(error).__name__,message=str(error)))
        raise


if __name__=='__main__':main()

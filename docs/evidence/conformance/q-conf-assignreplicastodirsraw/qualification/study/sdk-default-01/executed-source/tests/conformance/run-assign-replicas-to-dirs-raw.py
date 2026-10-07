#!/usr/bin/env python3
"""Run the finite API73 v0 cohort with pinned SDKs and owned controllers."""
import argparse
import base64
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('assign_generator', REPO/'tests/conformance/java/generate_assign_replicas_to_dirs_raw.py')
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
owner = gen.owner
GUARD = {}
END = None


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())


def check_guard():
    if time.monotonic() >= END:
        raise TimeoutError('overall qualification deadline')
    for path, digest in GUARD.items():
        if sha(path) != digest:
            raise ValueError('executed input changed: '+str(path))


original_execute = owner.execute


def execute(command, env, directory, label, timeout):
    check_guard()
    result = original_execute(command, env, directory, label, min(timeout, END-time.monotonic()))
    check_guard()
    return result


owner.execute = execute


def port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0));return stream.getsockname()[1]


def native(args, sdk):
    release = sdk['release'];root = args.output/(release+'-native');root.mkdir()
    home = args.brokers/('kafka_2.13-'+release)
    ports = [port(), port()]
    if ports[0] == ports[1]:
        raise ValueError('port reservation collided')
    broker_address, controller_address = [f'127.0.0.1:{p}' for p in ports]
    properties = root/'server.properties'
    properties.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{controller_address}
listeners=PLAINTEXT://{broker_address},CONTROLLER://{controller_address}
advertised.listeners=PLAINTEXT://{broker_address}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={root/'data'}
num.network.threads=2
num.io.threads=4
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
unstable.api.versions.enable=true
auto.create.topics.enable=false
log.segment.bytes=1048576
log.index.size.max.bytes=1048576
metadata.log.segment.bytes=1048576
''')
    native_inputs = {str(p): sha(p) for name in ('bin','libs','config') for p in sorted((home/name).rglob('*')) if p.is_file()}
    save(root/'native-source-bindings.json', native_inputs)
    GUARD.update(native_inputs)
    env = owner.base_env() | {'KAFKA_HEAP_OPTS':'-Xms64m -Xmx128m','LOG_DIR':str(root/'logs')}
    cluster = execute([str(home/'bin/kafka-storage.sh'),'random-uuid'],env,root,'cluster-id',15).read_text().strip()
    execute([str(home/'bin/kafka-storage.sh'),'format','-t',cluster,'-c',str(properties)],env,root,'format',30)
    env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
    child = None;topic_created = False;topic = 'assign-dirs-'+release.replace('.','-')
    receipt = dict(release=release, cluster_id=cluster, parent_waited=False, controller=controller_address, broker=broker_address)
    try:
        with (root/'server.log').open('x') as log:
            command = ['python3',str(REPO/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),str(home/'bin/kafka-server-start.sh'),str(properties)]
            child = subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            receipt.update(pid=child.pid,command=command)
            startup = min(END,time.monotonic()+60)
            while True:
                if (root/'server.log').stat().st_size > 64*1024*1024:
                    raise ValueError('broker log budget')
                text = (root/'server.log').read_text()
                epochs = re.findall(r'Successfully registered broker 1 with broker epoch (\d+)',text)
                if 'Kafka Server started' in text and epochs:
                    epoch = int(epochs[-1]);break
                if child.poll() is not None or time.monotonic() >= startup:
                    raise RuntimeError('native startup failed')
                time.sleep(.1)
            metadata = (root/'data/meta.properties').read_text()
            encoded = re.search(r'^directory.id=(\S+)$',metadata,re.M).group(1)
            directory_id = base64.urlsafe_b64decode(encoded+'='*((4-len(encoded)%4)%4)).hex()
            receipt.update(registered_broker_epoch=epoch,online_directory_id=directory_id)
            topic_id_file = root/'topic-id.txt'
            execute(sdk['java']+['create-topic',broker_address,topic,str(topic_id_file)],owner.base_env(),root,'create-topic',10)
            topic_created = True;topic_id = topic_id_file.read_text().strip()
            rust_output = root/'rust'
            rust_env = owner.base_env() | {'ASSIGN_DIRS_CONTROLLER':controller_address,'ASSIGN_DIRS_BROKER':broker_address,
                'ASSIGN_DIRS_BROKER_EPOCH':str(epoch),'ASSIGN_DIRS_DIRECTORY_ID':directory_id,'ASSIGN_DIRS_TOPIC_ID':topic_id,'ASSIGN_DIRS_OUTPUT':str(rust_output)}
            execute([str(args.binary),'native_assign_replicas_to_dirs_raw_history','--ignored','--exact','--nocapture'],rust_env,root,'Rust-live',15)
            execute(sdk['java']+['parse-live',str(rust_output)],owner.base_env(),root,'SDK-parse-Rust-live',8)
            java_output = root/'java'
            execute(sdk['java']+['live',controller_address,str(epoch),directory_id,topic_id,str(java_output)],owner.base_env(),root,'Java-live',15)
            execute(sdk['java']+['parse-live',str(java_output)],owner.base_env(),root,'SDK-parse-Java-live',8)
            for i in range(9):
                for kind in ('request','response'):
                    a=(rust_output/f'live-{i}-{kind}.bin').read_bytes();b=(java_output/f'live-{i}-{kind}.bin').read_bytes()
                    if kind=='response':a,b=a[4:],b[4:]  # The observed throttle is per-request, not a field parity requirement.
                    if a != b:raise ValueError(f'actual caller bodies differ: {i} {kind}')
            receipt.update(actual_raw_requests=18,independent_live_body_parses=36,actual_raw_request_bodies_match=True,
                actual_raw_response_fields_match_except_per_request_throttle=True,ordinary_broker_Admin_policy='Unsupported',topic_id=topic_id)
    finally:
        if topic_created and child is not None and child.poll() is None:
            try:
                execute(sdk['java']+['delete-topic',broker_address,topic,str(root/'unused-delete-output')],owner.base_env(),root,'delete-topic',10)
                receipt['owned_topic_deleted']=True
            except BaseException as error:
                receipt.update(owned_topic_deleted=False,cleanup_failure=type(error).__name__)
        if child is not None:
            if child.poll() is None:os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                receipt['forced_shutdown']=True;receipt['adopted_children']=owner.stop_group(child)
            receipt.update(exit_code=child.returncode,parent_waited=True,process_group_empty=not owner.group_members(child.pid))
        closed=[]
        for value in ports:
            with socket.socket() as stream:
                stream.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);stream.bind(('127.0.0.1',value))
            closed.append(dict(port=value,reusable=True))
        receipt['ports']=closed
        save(root/'process.json',receipt)
        for path in native_inputs:GUARD.pop(path)
    if receipt.get('forced_shutdown') or not receipt.get('process_group_empty') or not receipt.get('owned_topic_deleted'):
        raise ValueError('owned topology cleanup failed')
    if any(sha(path)!=digest for path,digest in native_inputs.items()):
        raise ValueError('native input changed')
    return receipt


def main():
    global END
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('binary','binding','jars','slf4j','brokers','sources','applicability','output'):
        p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args();args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    END=time.monotonic()+900
    if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('subreaper unavailable')
    binding=json.loads(args.binding.read_text())
    if sha(args.binary)!=binding['binary_sha256']:raise ValueError('binary binding differs')
    for name,digest in binding['sources'].items():
        if sha(REPO/name)!=digest:raise ValueError('source binding differs '+name)
    inputs=[args.binary,args.binding,args.slf4j,args.applicability,Path(__file__).resolve(),REPO/'tests/conformance/java/generate_assign_replicas_to_dirs_raw.py',
            REPO/'tests/conformance/java/ConformanceAssignReplicasToDirsRaw.java',REPO/'scripts/run-benchmark-matrix.py',REPO/'benchmarks/runtime/tools/parent-bound-exec.py']
    inputs += [REPO/name for name in binding['sources']]
    inputs += [path for path in sorted(args.sources.rglob('*')) if path.is_file()]
    inputs += [args.jars/('kafka-clients-'+release+'.jar') for release in gen.PINS]
    GUARD.update({str(path):sha(path) for path in inputs})
    save(args.output/'source-bindings.json',GUARD)
    for path in inputs:
        if path.is_relative_to(REPO):
            dst=args.output/'executed-source'/path.relative_to(REPO);dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dst)
    listing=execute([str(args.binary),'--list'],owner.base_env(),args.output,'required-lanes',8).read_text()
    for name in ('actual_sdk_assign_replicas_to_dirs_bodies','native_assign_replicas_to_dirs_raw_history'):
        if name+': test' not in listing:raise ValueError('missing mandatory lane')
    sdks=gen.generate(args.jars,args.slf4j,args.output);results=[]
    for sdk in sdks:
        release=sdk['release'];reverse=args.output/(release+'-reverse')
        env=owner.base_env() | {'ASSIGN_DIRS_FIXTURES':sdk['fixtures'],'ASSIGN_DIRS_REVERSE':str(reverse)}
        execute([str(args.binary),'actual_sdk_assign_replicas_to_dirs_bodies','--ignored','--exact','--nocapture'],env,args.output,release+'-Rust-wire',15)
        result=execute(sdk['java']+['verify',str(reverse),sdk['fixtures']],owner.base_env(),args.output,release+'-SDK-parse-Rust',8).read_text()
        if json.loads(result.splitlines()[-1])['independently_parsed_Rust_bodies']!=23:raise ValueError('missing reverse body')
        results.append(native(args,sdk))
    check_guard()
    save(args.output/'summary.json',dict(status='pass',actual_sdk=True,actual_native_controllers=True,source_bound=True,
        all_processes_parent_waited=True,independent_wire_bodies=69,independent_live_body_parses=108,raw_controller_requests=54,results=results,
        scope='API73 v0 declared current raw-extension cohort only. No standard Java Admin API, offline-directory movement, controller failover or production qualification.'))


if __name__=='__main__':main()

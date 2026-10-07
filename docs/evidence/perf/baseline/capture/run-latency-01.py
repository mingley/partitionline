#!/usr/bin/env python3
"""Measure the pinned client against an owned Apache Kafka process."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import statistics
import random
import math
import socket
import subprocess
import sys
import threading
import time


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def free_ports():
    holders = [socket.socket(), socket.socket()]
    try:
        for s in holders: s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in holders]
    finally:
        for s in holders: s.close()


def one_json(path):
    values = [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]
    if len(values) != 1: raise ValueError("one actual completion JSON required")
    return values[0]


def broker_sample(pid):
    root = Path("/proc") / str(pid)
    return dict(stat=(root / "stat").read_text(), status=(root / "status").read_text(),
                io=(root / "io").read_text(), command=(root / "cmdline").read_bytes().decode().split("\0"),
                monotonic_ns=time.monotonic_ns(), clock_ticks=os.sysconf("SC_CLK_TCK"))


def validate_open_loop(samples, summary, rate):
    count=20000
    if len(samples)!=count or summary['kind']!='open_loop_produce_ack': raise ValueError('raw population differs')
    if summary['rate_per_second']!=rate or summary['warmup_records']!=10000: raise ValueError('rate/warmup differs')
    outcomes=summary['outcomes']; ack=outcomes['acknowledged']; rejected=outcomes['rejected']
    if outcomes != dict(offered=count,accepted=ack,acknowledged=ack,consumed=0,rejected=rejected,timed_out=0,unknown=0):
        raise ValueError('terminal accounting differs or delivery is ambiguous')
    if ack+rejected!=count or rejected!=summary['capacity_rejections']: raise ValueError('capacity accounting differs')
    if summary['run_disposition']!=('failed' if rejected else 'executed'): raise ValueError('failed disposition lost')
    if not summary['coordinated_omission_avoidance'] or not summary['warmup_excluded']: raise ValueError('schedule semantics differ')
    distributions={k:[] for k in ('end_to_end','enqueue_to_ack_upper_bound','schedule_lag','enqueue_wait_upper_bound')}
    actual_ack=0
    for i,sample in enumerate(samples):
        intended=sample['intended_arrival_ns'];offered=sample['actual_offer_ns'];completed=sample['completed_ns']
        if sample['id']!=i or intended!=i*1_000_000_000//rate: raise ValueError('absolute arrival schedule differs')
        if not all(isinstance(v,int) and v>=0 for v in (intended,offered,completed)) or not intended<=offered<=completed:
            raise ValueError('invalid raw schedule timing')
        lag=offered-intended
        if sample['schedule_lag_ns']!=lag: raise ValueError('schedule lag differs')
        distributions['schedule_lag'].append(lag//1000)
        if sample['outcome']=='rejected':
            if sample['accepted'] or sample['error']!='benchmark pending capacity exhausted': raise ValueError('rejection differs')
            if any(sample[k] is not None for k in ('actual_enqueue_lower_ns','actual_enqueue_upper_ns',
                    'acknowledgment_observed_ns','end_to_end_ns','enqueue_to_ack_upper_ns','enqueue_wait_upper_ns')):
                raise ValueError('rejected record fabricated acceptance or acknowledgment')
            continue
        if sample['outcome']!='acknowledged' or not sample['accepted'] or sample['error'] is not None:
            raise ValueError('terminal record differs')
        actual_ack+=1
        lower,upper,seen=[sample[k] for k in ('actual_enqueue_lower_ns','actual_enqueue_upper_ns','acknowledgment_observed_ns')]
        if not all(isinstance(v,int) and v>=0 for v in (lower,upper,seen)) or not offered<=lower<=upper<=seen==completed:
            raise ValueError('causal acknowledgment timing differs')
        for key,field,value in (('end_to_end','end_to_end_ns',seen-intended),
                ('enqueue_to_ack_upper_bound','enqueue_to_ack_upper_ns',seen-lower),
                ('enqueue_wait_upper_bound','enqueue_wait_upper_ns',upper-offered)):
            if sample[field]!=value: raise ValueError('derived duration differs')
            distributions[key].append(value//1000)
    if actual_ack!=ack: raise ValueError('raw acknowledgment count differs')
    for key,values in distributions.items():
        values.sort(); actual=summary[key]; n=len(values); eligible=n>=10000
        if actual['sample_count']!=n or actual['sample_floor_met']!=eligible: raise ValueError('sample population differs')
        for name,permille in (('p50_us',500),('p95_us',950),('p99_us',990),('p99_9_us',999)):
            expected=values[(n*permille+999)//1000-1] if eligible else None
            if actual[name]!=expected: raise ValueError('raw percentile differs')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ("source", "source-pins", "output", "kafka-homes", "sdk-jar", "slf4j",
                "producer", "fetch-verifier", "verifier-source", "latency", "recorder", "parent-exec", "measure-process", "java-verifier", "calibration-reference"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--commit", required=True)
    a = p.parse_args()
    def interrupt(signum, frame):
        raise InterruptedError(f"owner received signal {signum}")
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    baseline = module(a.recorder, "native_baseline_recorder")
    args = argparse.Namespace(source=a.source, source_pins=a.source_pins, output=a.output,
             commit=a.commit, family="latency", repetitions=5, seed=912, cpus="2,4", cells=None,
             binary=[("bench_produce", a.producer), ("fetch-verifier", a.fetch_verifier),
                     ("bench_latency", a.latency)])
    r = baseline.Recorder(args)
    native = module(a.source / "tests/conformance/run-codec-matrix.py", "native_distribution")
    home = native.archive_home(a.kafka_homes, "4.3.1")
    inputs = [Path(__file__).resolve(), a.recorder.resolve(), a.parent_exec.resolve(), a.measure_process.resolve(), a.java_verifier.resolve(), a.sdk_jar.resolve(), a.slf4j.resolve(),
              *a.verifier_source.rglob("*.rs"), a.verifier_source / "Cargo.toml", a.verifier_source / "Cargo.lock"]
    inputs += [p for directory in ("bin", "libs", "config") for p in (home / directory).rglob("*") if p.is_file()]
    r.input_pins.update({str(p): baseline.sha(p) for p in inputs})
    base_command = r.command
    def parent_bound_command(command, directory, label, timeout=400, extra_env=None):
        return base_command([sys.executable, "-B", a.parent_exec.resolve(), str(os.getpid()), *command],
                            directory, label, timeout, extra_env)
    r.command = parent_bound_command
    baseline.save(r.output / "native-inputs.json", r.input_pins)
    baseline.save(r.output / "latency-profile.json", dict(broker="Apache Kafka 4.3.1",
        source_commit=a.commit, scope="local/unsigned", suite_hold="active",
        calibration_repetitions=5, reused_calibration=str(a.calibration_reference.resolve()), load_percentages=[10, 50, 80], repetitions_per_rate=5,
        measured_records=20_000, excluded_sequential_warmup=10_000, payload="100 bytes of x",
        partitions=1, batch_records=1, max_in_flight=1, connections=1, linger_ms=0, acks=1,
        saturation_basis="median sequential reciprocal mean_us, matching single-record single-inflight workload; integer microsecond diagnostic resolution",
        integrity_scope="independent Java full fixed-payload and contiguous-offset verification; driver has no unique record IDs",
        client_cpus=[2, 4], broker_cpus=[0, 1], raw_timestamps="nanoseconds since measurement origin"))
    classes = r.output / "java-classes"
    classes.mkdir()
    cp = str(a.sdk_jar.resolve()) + ":" + str(a.slf4j.resolve())
    r.command(["java", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
               "-source", "21", "-target", "21", "-Xlint:all", "-Werror", "-cp", cp,
               "-d", classes, a.java_verifier.resolve()],
              r.output, "java-compile", timeout=30)
    r.input_pins.update({str(p): baseline.sha(p) for p in classes.rglob("*.class")})
    baseline.save(r.output / "compiled-inputs.json", r.input_pins)
    broker_port, controller_port = free_ports()
    bootstrap = f"127.0.0.1:{broker_port}"
    properties = r.output / "server.properties"
    properties.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller_port}
listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}
advertised.listeners=PLAINTEXT://127.0.0.1:{broker_port}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={r.output / 'data'}
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
""")
    r.env.update(KAFKA_HEAP_OPTS="-Xms128m -Xmx512m")
    cluster = r.command([home / "bin/kafka-storage.sh", "random-uuid"], r.output, "cluster-uuid", 30).read_text().strip()
    r.command([home / "bin/kafka-storage.sh", "format", "-t", cluster, "-c", properties], r.output, "format", 30)
    owner = None
    supervisor = None
    stop_supervisor = threading.Event()
    log = (r.output / "broker.log").open("xb")
    topics = []
    try:
        r.guard()
        owner = subprocess.Popen([sys.executable, "-B", str(a.parent_exec.resolve()), str(os.getpid()),
                    "taskset", "-c", "0,1", str(home / "bin/kafka-server-start.sh"), str(properties)],
                    env=r.env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        def supervise():
            deadline = time.monotonic() + 5400
            while not stop_supervisor.wait(.1):
                if time.monotonic() >= deadline or (r.output / "broker.log").stat().st_size > 64 * 1024 * 1024:
                    os.kill(os.getpid(), signal.SIGTERM)
                    return
        supervisor = threading.Thread(target=supervise, name="native-baseline-lease")
        supervisor.start()
        deadline = time.monotonic() + 60
        while "Kafka Server started" not in (r.output / "broker.log").read_text():
            if owner.poll() is not None: raise RuntimeError("broker exited at startup")
            if time.monotonic() >= deadline: raise TimeoutError("broker startup deadline")
            time.sleep(.05)
        identity = r.command([home / "bin/kafka-cluster.sh", "cluster-id", "--bootstrap-server", bootstrap], r.output, "identity", 30).read_text()
        if cluster not in identity: raise ValueError("actual cluster identity differs")
        baseline.save(r.output / "broker-started.json", dict(pid=owner.pid, cluster=cluster,
             bootstrap=bootstrap, ports=[broker_port, controller_port], archive_sha256=native.ARCHIVES["4.3.1"]))
        def topic_start(directory, topic):
            topics.append(topic)
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--create",
                       "--topic", topic, "--partitions", "1", "--replication-factor", "1",
                       "--config", "min.insync.replicas=1", "--config", "retention.ms=-1"], directory, "create", 30)
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap,
                       "--describe", "--topic", topic], directory, "describe", 30)
        def verify_close(directory, topic, expected):
            readback = r.command(["java", "-Xms64m", "-Xmx256m", "-cp", str(classes)+":"+cp,
                "LatencyReadback", bootstrap, topic, str(expected)], directory, "java-readback", 45)
            audited = one_json(readback)
            if audited["status"] != "pass" or audited["verified"] != expected or not audited["consumer_closed"]:
                raise ValueError("independent latency readback incomplete")
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap,
                       "--delete", "--topic", topic], directory, "delete", 30)
            deadline = time.monotonic()+30
            while list((r.output / "data").glob(topic+"-*")):
                if time.monotonic() >= deadline: raise TimeoutError("topic files retained")
                time.sleep(.05)
            return audited
        env = dict(KAFKA_BOOTSTRAP=bootstrap, MODE="produce", COUNT="20000", WARMUP="10000",
                   PAYLOAD_BYTES="100", ACKS="1", LINGER_MS="0", MAX_PENDING="1024",
                   SAMPLE_FLOOR="10000", BUFFER_MEMORY="33554432", MAX_BLOCK_MS="1000",
                   DELIVERY_TIMEOUT_MS="30000", REQUEST_TIMEOUT_MS="30000")
        donor = a.calibration_reference.resolve()
        if baseline.sha(donor/'bin/bench_latency') != baseline.sha(r.binaries['bench_latency']):
            raise ValueError("calibration ELF differs")
        old_plan = json.loads((donor/'plan.json').read_text())
        if old_plan['source_pins'] != r.pins or old_plan['source_commit'] != a.commit:
            raise ValueError("calibration source differs")
        for file in donor.glob('calibration-*/*'):
            if file.is_file(): r.input_pins[str(file)]=baseline.sha(file)
        calibration = json.loads((donor/'calibrated-rates.json').read_text())
        r.input_pins[str(donor/'calibrated-rates.json')]=baseline.sha(donor/'calibrated-rates.json')
        r.input_pins[str(donor/'executed-latency-wrapper.py')]=baseline.sha(donor/'executed-latency-wrapper.py')
        rows = [json.loads((donor/f'calibration-{rep:02d}/validated.json').read_text()) for rep in range(1,6)]
        for row in rows:
            row['original_calibration_cohort']=str(donor)
            r.rows.append(row)
        if [row['records_per_second'] for row in rows] != calibration['capacity_values']:
            raise ValueError("calibration values differ")
        rates = {int(k):v for k,v in calibration['rates'].items()}
        if rates != {10:1000,50:5000,80:8000}: raise ValueError("frozen arrival rates differ")
        baseline.save(r.output/'calibrated-rates.json',calibration)
        baseline.save(r.output/'calibration-reference-pins.json',r.input_pins)
        rng = random.Random(912)
        for rep in range(1,6):
            order = list(rates)
            rng.shuffle(order)
            for percent in order:
                rate = rates[percent]
                directory = r.output / f"r{rep:02d}-load-{percent}"
                directory.mkdir()
                topic = f"pl-latency-{owner.pid}-{rep}-{percent}"
                topic_start(directory, topic)
                print(f"start open-loop load {percent}% repetition {rep} at {rate}/s", flush=True)
                try:
                    raw = r.command([r.binaries["bench_latency"]], directory, "latency",
                        max(180, math.ceil(20000/rate)+90), dict(env, KAFKA_TOPIC=topic,
                        LATENCY_MODE="open-loop", RATE_PER_SECOND=str(rate)))
                except ValueError:
                    r.guard()
                    receipt = json.loads((directory/'latency.process.json').read_text())
                    if not receipt['parent_waited'] or receipt['exit_code'] != 1 or receipt.get('failure'):
                        raise
                    if r.owned.group_members(receipt['pid']): raise RuntimeError("latency group remains")
                    raw = directory/'latency.stdout'

                samples = [json.loads(line) for line in raw.read_text().splitlines() if line.startswith("{")]
                summary = samples.pop()
                validate_open_loop(samples, summary, rate)
                row = dict(cell="lb-latency-openloop", repetition=rep, load_percent=percent,
                    rate_per_second=rate, summary=summary, raw_sha256=baseline.sha(raw),
                    java=verify_close(directory,topic,10000+summary["outcomes"]["acknowledged"]))
                r.rows.append(row)
                baseline.save(directory / "validated.json", row)
                print(f"done open-loop load {percent}% repetition {rep}", flush=True)
    except BaseException as error:
        baseline.save(r.output / "failure.json", dict(error=type(error).__name__, message=str(error)))
        raise
    finally:
        stop_supervisor.set()
        if supervisor is not None:
            supervisor.join(timeout=2)
            if supervisor.is_alive(): raise RuntimeError("broker supervisor did not join")
        if owner is not None:
            try: os.killpg(owner.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            try: owner.wait(timeout=30)
            except subprocess.TimeoutExpired: pass
            adopted = r.owned.stop_group(owner)
            ports = []
            for port in (broker_port, controller_port):
                with socket.socket() as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(("127.0.0.1", port))
                ports.append(dict(port=port, reusable=True))
            baseline.save(r.output / "closure.json", dict(parent_waited=True, exit_code=owner.returncode,
                  group_empty=not r.owned.group_members(owner.pid), adopted_children=adopted,
                  supervisor_joined=True, ports=ports))
        log.close()
    r.finish()


if __name__ == "__main__":
    main()

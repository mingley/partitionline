#!/usr/bin/env python3
"""Measure the pinned client against an owned Apache Kafka process."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
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


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ("source", "source-pins", "output", "kafka-homes", "sdk-jar", "slf4j",
                "producer", "fetch-verifier", "verifier-source", "latency", "recorder", "parent-exec", "measure-process"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--commit", required=True)
    a = p.parse_args()
    def interrupt(signum, frame):
        raise InterruptedError(f"owner received signal {signum}")
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    baseline = module(a.recorder, "native_baseline_recorder")
    args = argparse.Namespace(source=a.source, source_pins=a.source_pins, output=a.output,
             commit=a.commit, family="native", repetitions=5, seed=912, cpus="2,4", cells=None,
             binary=[("bench_produce", a.producer), ("fetch-verifier", a.fetch_verifier),
                     ("bench_latency", a.latency)])
    r = baseline.Recorder(args)
    native = module(a.source / "tests/conformance/run-codec-matrix.py", "native_distribution")
    home = native.archive_home(a.kafka_homes, "4.3.1")
    inputs = [Path(__file__).resolve(), a.recorder.resolve(), a.parent_exec.resolve(), a.measure_process.resolve(), a.sdk_jar.resolve(), a.slf4j.resolve(),
              *a.verifier_source.rglob("*.rs"), a.verifier_source / "Cargo.toml", a.verifier_source / "Cargo.lock"]
    inputs += [p for directory in ("bin", "libs", "config") for p in (home / directory).rglob("*") if p.is_file()]
    r.input_pins.update({str(p): baseline.sha(p) for p in inputs})
    base_command = r.command
    def parent_bound_command(command, directory, label, timeout=400, extra_env=None):
        return base_command([sys.executable, "-B", a.parent_exec.resolve(), str(os.getpid()), *command],
                            directory, label, timeout, extra_env)
    r.command = parent_bound_command
    baseline.save(r.output / "native-inputs.json", r.input_pins)
    baseline.save(r.output / "native-profile.json", dict(broker="Apache Kafka 4.3.1", repetitions=5,
        rerun_repetitions=5, bulk_measured_records=8_000_000, warmup_records=10_000,
        partitions=6, payload_bytes=100, seed=1592590337, acks=1, idempotence=False,
        compression="none", batch_bytes=1_048_576, batch_records=32768, max_in_flight=5,
        client_cpus=[2, 4], broker_cpus=[0, 1], scope="local/unsigned", suite_hold="active",
        open_loop_rates="10/50/80 percent of matching sequential 1-record RPC capacity",
        retained_broker_data="Topics removed after exact fences and full Rust/Java byte verification; raw segments not retained"))
    classes = r.output / "java-classes"
    classes.mkdir()
    cp = str(a.sdk_jar.resolve()) + ":" + str(a.slf4j.resolve())
    r.command(["java", "--add-modules", "jdk.compiler", "com.sun.tools.javac.Main",
               "-source", "21", "-target", "21", "-Xlint:all", "-Werror", "-cp", cp,
               "-d", classes, a.source / "tests/conformance/java/ConformanceBenchProduceSettings.java"],
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
        for rep in range(1, 11):
            directory = r.output / (f"primary-{rep:02d}" if rep <= 5 else f"reproduce-{rep-5:02d}")
            directory.mkdir()
            topic = f"pl-baseline-{owner.pid}-{rep}"
            topics.append(topic)
            print(f"start native bulk/fetch repetition {rep}", flush=True)
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--create",
                       "--topic", topic, "--partitions", "6", "--replication-factor", "1",
                       "--config", "min.insync.replicas=1", "--config", "retention.ms=-1"], directory, "create", 30)
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--describe", "--topic", topic], directory, "describe", 30)
            before = r.command([home / "bin/kafka-get-offsets.sh", "--bootstrap-server", bootstrap,
                        "--topic", topic], directory, "offsets-before", 30).read_text()
            offsets = [int(line.rsplit(":", 1)[1]) for line in before.splitlines() if line.startswith(topic + ":")]
            if len(offsets) != 6 or any(offsets): raise ValueError("fresh topic is not empty")
            env = dict(KAFKA_BOOTSTRAP=bootstrap, KAFKA_TOPIC=topic, COUNT="8000000", WARMUP="10000",
                       WARMUP_SECS="0", MEASURE_SECS="0", PAYLOAD_BYTES="100", RECORD_SEED="1592590337",
                       KEY_MODE="id", PAYLOAD_MODE="seeded", PARTITIONS="6", ACKS="1", IDEMPOTENT="0",
                       LINGER_MS="5", BATCH_BYTES="1048576", BATCH_RECORDS="32768", MAX_IN_FLIGHT="5",
                       CONNECTIONS="1", QUEUE_KBYTES="32768", RUN_TIMEOUT_MS="300000", COMPRESSION="none")
            r.command([r.binaries["bench_produce"], "--print-config"], directory, "producer-config", 15, env)
            baseline.save(directory / "broker-before-produce.json", broker_sample(owner.pid))
            resource = directory / "producer-time.json"
            produced = r.command([sys.executable, "-B", a.measure_process.resolve(), resource, r.binaries["bench_produce"]], directory, "producer", 360, env)
            producer = one_json(produced)
            if producer["acked"] != 8_000_000 or producer["acknowledged_total"] != 8_010_000 or producer["run_disposition"] != "executed":
                raise ValueError("producer accounting differs")
            baseline.save(directory / "broker-after-produce.json", broker_sample(owner.pid))
            after = r.command([home / "bin/kafka-get-offsets.sh", "--bootstrap-server", bootstrap,
                        "--topic", topic], directory, "offsets-after", 30).read_text()
            offsets = {int(line.split(":")[-2]): int(line.rsplit(":", 1)[1]) for line in after.splitlines() if line.startswith(topic + ":")}
            expected = {p: (10_000 + 5 - p)//6 + (8_000_000 + 5 - p)//6 for p in range(6)}
            if offsets != expected: raise ValueError("independent per-partition offsets differ")
            baseline.save(directory / "broker-before-fetch.json", broker_sample(owner.pid))
            fetched = r.command([r.binaries["fetch-verifier"], bootstrap, topic, "10000", "8000000", "1592590337"],
                                 directory, "fetch", 240)
            consumer = one_json(fetched)
            if consumer["records_verified"] != 8_000_000 or consumer["consumer_closed"] is not True:
                raise ValueError("full measured receipt verification required")
            baseline.save(directory / "broker-after-fetch.json", broker_sample(owner.pid))
            java = r.command(["java", "-Xms128m", "-Xmx384m", "-cp", str(classes) + ":" + cp,
                        "ConformanceBenchProduceSettings", bootstrap, topic, "10000", "8000000", "6",
                        "1592590337", "100", "id", "seeded"], directory, "java-readback", 60)
            audited = one_json(java)
            if audited.get("status") != "pass" or audited["verified"] != 8_010_000 or not audited["consumer_closed"]:
                raise ValueError("genuine independent Java readback incomplete")
            r.rows.append(dict(repetition=rep, cohort="primary" if rep <= 5 else "reproduce",
                               topic=topic, producer=producer, fetch=consumer, java=audited,
                               producer_resources=json.loads(resource.read_text()), offsets=offsets))
            baseline.save(directory / "validated.json", r.rows[-1])
            r.command([home / "bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--delete", "--topic", topic], directory, "delete", 30)
            # Kafka performs asynchronous deletion; wait for its owned files to disappear.
            deadline = time.monotonic() + 30
            while list((r.output / "data").glob(topic + "-*")):
                if time.monotonic() >= deadline: raise TimeoutError("owned topic files not removed")
                time.sleep(.05)
            print(f"done native bulk/fetch repetition {rep}", flush=True)
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

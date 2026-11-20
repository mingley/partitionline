#!/usr/bin/env python3
"""Verify the installed RF1 operator with the pinned Apache Java SDK."""
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

REPOSITORY = Path(__file__).resolve().parents[1]
PEER = REPOSITORY / "tests/conformance/broker/interop"
MAVEN = "https://repo.maven.apache.org/maven2"
RELEASE = "4.3.1"
TOPIC = "archive-orders"
RECORDS_PER_PARTITION = 12
PARTITIONS = 2


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run(command, log, timeout):
    print(shlex.join(map(str, command)), flush=True)
    try:
        with log.open("w") as output:
            subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=timeout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        print(log.read_text(), file=sys.stderr)
        raise


def jar(directory, maven_path, digest):
    path = directory / maven_path.rsplit("/", 1)[1]
    if not path.is_file():
        with urlopen(f"{MAVEN}/{maven_path}", timeout=15) as response:
            data = response.read(16 * 1024 * 1024 + 1)
        if len(data) > 16 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != digest:
            raise RuntimeError(f"unexpected bytes for {path.name}")
        path.write_bytes(data)
    elif hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise RuntimeError(f"cached {path.name} differs from the peer pin")
    return path


def prepare(directory, report):
    directory.mkdir(exist_ok=True)
    pins = json.loads((PEER / "pins.json").read_text())
    peer = next(item for item in pins["official_apache_releases"] if item["release"] == RELEASE)
    kafka = jar(directory, f"org/apache/kafka/kafka-clients/{RELEASE}/kafka-clients-{RELEASE}.jar",
                peer["kafka_clients_jar_sha256"])
    slf4j = jar(directory, "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
                pins["slf4j_api_1_7_36_sha256"])
    libraries = os.pathsep.join(map(str, (kafka, slf4j)))
    classes = directory / "classes"
    if not classes.is_dir():
        classes.mkdir()
        run(["javac", "-Xlint:all", "-Werror", "-cp", libraries, "-d", classes,
             PEER / "ArchiveProducePeer.java", PEER / "CrossReadPeer.java"],
            report / "peer-compile.log", 20)
        run(["java", "-version"], report / "peer-java-version.log", 10)
        save(report / "peer-pins.json", {"kafka": peer, "slf4j_api_1_7_36_sha256": pins["slf4j_api_1_7_36_sha256"]})
    return os.pathsep.join((str(classes), libraries))


def await_ready(process, log, endpoint):
    expected = f"ready {endpoint} advertised={endpoint} recovered_entries=0 profile=ordinary-rf1"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"operator exited before readiness\n{log.read_text()}")
        if expected in log.read_text().splitlines():
            return
        time.sleep(0.05)
    raise TimeoutError(f"operator readiness was not observed\n{log.read_text()}")


def stop(process, log, port):
    signaled = process.poll() is None
    if signaled:
        process.terminate()
    forced = False
    try:
        code = process.wait(10)
    except subprocess.TimeoutExpired:
        process.kill()
        code = process.wait(5)
        forced = True
    stopped = re.search(r"^stopped accepted=(\d+) joined=(\d+) worker_failures=(\d+)$",
                        log.read_text(), re.MULTILINE)
    accepted, joined, failures = map(int, stopped.groups()) if stopped else (None, None, None)
    with socket.socket() as connection:
        connection.settimeout(1)
        listener_closed = connection.connect_ex(("127.0.0.1", port)) == errno.ECONNREFUSED
    return {
        "sigterm_sent": signaled,
        "forced": forced,
        "exit_code": code,
        "accepted": accepted,
        "joined": joined,
        "worker_failures": failures,
        "listener_closed": listener_closed,
        "passed": signaled and not forced and code == 0 and listener_closed
            and accepted is not None and accepted > 0 and accepted == joined and failures == 0,
    }


def qualify(operator, profile, scratch, report):
    classpath = prepare(scratch, report)
    # The operator requires a nonzero port. Leave the kernel-selected port
    # available immediately before spawning the owned loopback listener.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    endpoint = f"127.0.0.1:{port}"
    server_log = report / f"{profile}-serve.log"
    produce_log = report / f"{profile}-produce.log"
    receipt = {
        "profile": profile, "broker_profile": "ordinary-rf1", "security": "PLAINTEXT",
        "kafka_clients": RELEASE, "replication_factor": 1, "acks": "all",
        "idempotence": False, "compression": "none", "topic": TOPIC,
        "partitions": PARTITIONS, "records_per_partition": RECORDS_PER_PARTITION,
        "passed": False,
    }
    with tempfile.TemporaryDirectory(prefix="pl-broker-archive-", dir=scratch) as directory:
        data = Path(directory)
        client = data / "records"
        witness = client / f"{RELEASE}-cross-read-initial-{TOPIC}.json"
        command = [operator, "serve", "--data", data / "broker", "--listen", endpoint,
                   "--advertise-host", "127.0.0.1", "--cluster", "archive-serving", "--broker-id", "0"]
        print(shlex.join(map(str, command)), flush=True)
        with server_log.open("w") as output:
            process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            await_ready(process, server_log, endpoint)
            java = ["java", "-Xmx128m", "-cp", classpath]
            run([*java, "ArchiveProducePeer", str(port)], produce_log, 35)
            acknowledged = [json.loads(line) for line in produce_log.read_text().splitlines()
                            if line.startswith("{")]
            expected = [{"topic": TOPIC, "partition": partition, "offset": offset}
                        for partition in range(PARTITIONS) for offset in range(RECORDS_PER_PARTITION)]
            if acknowledged != expected:
                raise RuntimeError("the Produce receipt history differs from the expected offsets")
            receipt["acknowledged_records"] = len(acknowledged)
            run([*java, "CrossReadPeer", RELEASE, str(port), "initial", client, TOPIC,
                 str(RECORDS_PER_PARTITION)], report / f"{profile}-fetch.log", 50)
            fetched = json.loads(witness.read_text())
            count = sum(entry.get("label") == "foreign-written-java-read" for entry in fetched["history"])
            if not fetched["passed"] or count != len(expected):
                raise RuntimeError("the independent Java Fetch witness is incomplete")
            receipt.update(fetched_records=count, peer_assertions=fetched["assertions"], passed=True)
        finally:
            receipt["cleanup"] = stop(process, server_log, port)
            receipt["passed"] &= receipt["cleanup"]["passed"]
            if witness.is_file():
                shutil.copyfile(witness, report / f"{profile}-fetched-records.json")
            save(report / f"{profile}-serving.json", receipt)
    if not receipt["passed"]:
        raise RuntimeError(f"installed operator cleanup failed\n{server_log.read_text()}")
    print(json.dumps(receipt), flush=True)


def interrupt(signum, _frame):
    raise SystemExit(f"interrupted by signal {signum}")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit("usage: broker-package-serving.py OPERATOR PROFILE SCRATCH REPORT")
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    qualify(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]))

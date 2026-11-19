#!/usr/bin/env python3
"""Pair two ordinary RF1 broker binaries on one host."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import select
import socket
import statistics
import struct
import subprocess
import tempfile
import threading
import time

HOST = "127.0.0.1"
TOPIC = b"sync"
MAX_FRAME = 1 << 20
CLIENT_TIMEOUT = 15
PAYLOAD_BYTES = 128
CREATE_TIME_MS = 1_790_000_000_000


def pack(fmt, *fields):
    return struct.pack(">" + fmt, *fields)


def string(value):
    return pack("h", len(value)) + value


def request(key, version, correlation):
    return pack("hhih", key, version, correlation, -1)


def crc_table_entry(value):
    for _ in range(8):
        value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value


CRC_TABLE = tuple(crc_table_entry(i) for i in range(256))


def crc32c(payload):
    crc = 0xFFFF_FFFF
    for octet in payload:
        crc = (crc >> 8) ^ CRC_TABLE[(crc ^ octet) & 0xff]
    return crc ^ 0xFFFF_FFFF


def varint(value):
    zigzag = value * 2 if value >= 0 else -2 * value - 1
    encoded = bytearray()
    while zigzag >= 128:
        encoded.append((zigzag & 127) | 128)
        zigzag >>= 7
    encoded.append(zigzag)
    return bytes(encoded)


def record_batch(client, partition, sequence):
    identity = pack("III", client, partition, sequence)
    value = identity + bytes(PAYLOAD_BYTES - len(identity))
    record = b"\0" + varint(0) + varint(0) + varint(-1)
    record += varint(len(value)) + value + varint(0)
    fields = pack("hiqqqhii", 0, 0, CREATE_TIME_MS, CREATE_TIME_MS, -1, -1, -1, 1)
    protected = fields + varint(len(record)) + record
    tail = pack("ibI", -1, 2, crc32c(protected)) + protected
    return pack("qi", 0, len(tail)) + tail


class Reply:
    def __init__(self, payload):
        self.payload = memoryview(payload)
        self.position = 0

    def fields(self, fmt):
        spec = struct.Struct(">" + fmt)
        result = spec.unpack(self.take(spec.size))
        return result[0] if len(result) == 1 else result

    def take(self, size):
        end = self.position + size
        assert 0 <= size and end <= len(self.payload), (size, end, len(self.payload))
        field = self.payload[self.position:end]
        self.position = end
        return field

    def string(self):
        return self.take(self.fields("h"))

    def done(self):
        return self.position == len(self.payload)


def receive(sock, length):
    data = bytearray(length)
    buffer = memoryview(data)
    while buffer:
        count = sock.recv_into(buffer)
        assert count > 0, "broker closed the connection"
        buffer = buffer[count:]
    return data


def exchange(sock, frame):
    sock.sendall(frame)
    size = struct.unpack(">i", receive(sock, 4))[0]
    assert 0 < size <= MAX_FRAME, size
    return receive(sock, size)


def frame(body):
    return pack("i", len(body)) + body


def connect(port):
    sock = socket.create_connection((HOST, port), CLIENT_TIMEOUT)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


@contextmanager
def broker(binary, data):
    with socket.socket() as reserve_port:
        reserve_port.bind((HOST, 0))
        port = reserve_port.getsockname()[1]
    with tempfile.TemporaryFile() as error_log:
        with subprocess.Popen(
            [
                str(binary), "serve", "--data", str(data), "--listen", f"{HOST}:{port}",
                "--advertise-host", HOST, "--cluster", "sync-pairs", "--broker-id", "0",
                "--sync-concurrency", "4",
            ],
            stdout=subprocess.PIPE,
            stderr=error_log,
        ) as child:
            try:
                ready, _, _ = select.select([child.stdout], [], [], CLIENT_TIMEOUT)
                assert ready, "broker did not become ready"
                line = child.stdout.readline().decode().strip()
                assert line.startswith(f"ready {HOST}:{port} "), line
                yield port
            finally:
                if child.poll() is None:
                    child.terminate()
                try:
                    status = child.wait(timeout=CLIENT_TIMEOUT)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
                    raise
                if status:
                    error_log.seek(0)
                    raise RuntimeError(f"broker status {status}: {error_log.read().decode()}")


def create_topics(port, partitions):
    body = request(19, 4, 1) + pack("i", 1) + string(TOPIC)
    body += pack("ihiiib", partitions, 1, 0, 0, 60_000, 0)
    with connect(port) as sock:
        reply = Reply(exchange(sock, frame(body)))
    assert reply.fields("iii") == (1, 0, 1)
    assert reply.string() == TOPIC
    assert reply.fields("h") == 0


def make_produce(client, partitions, sequence, per_client):
    correlation = 10 + client * per_client + sequence
    batches = tuple(record_batch(client, partition, sequence) for partition in range(partitions))
    body = request(0, 3, correlation) + pack("hhii", -1, 1, 60_000, 1)
    body += string(TOPIC) + pack("i", partitions)
    body += b"".join(
        pack("ii", partition, len(batch)) + batch for partition, batch in enumerate(batches)
    )
    return correlation, frame(body), batches


def check_ack(payload, correlation, partitions):
    reply = Reply(payload)
    assert reply.fields("ii") == (correlation, 1)
    assert reply.string() == TOPIC
    assert reply.fields("i") == partitions
    offsets = []
    for expected in range(partitions):
        partition, status, offset, append_time = reply.fields("ihqq")
        assert (partition, status, append_time) == (expected, 0, -1), (
            correlation, partition, status, offset, append_time
        )
        offsets.append(offset)
    assert reply.fields("i") == 0 and reply.done()
    return offsets


def fetch(port, partition, expected):
    limit = MAX_FRAME // 2
    total = len(expected)
    with connect(port) as sock:
        position = 0
        while position < total:
            body = request(1, 4, 2) + pack("iiiibi", -1, 0, 1, limit, 0, 1)
            body += string(TOPIC) + pack("iiqi", 1, partition, position, limit)
            reply = Reply(exchange(sock, frame(body)))
            assert reply.fields("iii") == (2, 0, 1)
            assert reply.string() == TOPIC
            assert reply.fields("iihq") == (1, partition, 0, total), (partition, position)
            reply.fields("q")  # Last stable offset is irrelevant for this ordinary read.
            assert reply.fields("i") in (0, -1)
            records = Reply(reply.take(reply.fields("i")))
            assert reply.done()
            first = position
            while not records.done():
                offset, batch_size = records.fields("qi")
                assert offset == position, (partition, offset, position)
                batch_tail = Reply(records.take(batch_size))
                assert batch_tail.fields("i") == 0
                assert batch_tail.take(batch_size - 4) == expected[position][16:], (
                    partition, position
                )
                position += 1
            assert position > first, (partition, first, total)


@dataclass(frozen=True)
class Shape:
    clients: int
    partitions: int

    @property
    def label(self):
        return f"{self.clients}c-{self.partitions}p"


def drive_client(port, batch_plans, warmups, ready):
    with connect(port) as sock:
        try:
            replies = [exchange(sock, wire) for _, wire, _ in batch_plans[:warmups]]
            ready.wait(timeout=CLIENT_TIMEOUT)
            started = time.perf_counter_ns()
            times = []
            for _, wire, _ in batch_plans[warmups:]:
                begin = time.perf_counter_ns()
                replies.append(exchange(sock, wire))
                times.append(time.perf_counter_ns() - begin)
            elapsed_end = time.perf_counter_ns()
            return started, elapsed_end, times, replies
        except BaseException:
            ready.abort()
            raise


def run_arm(binary, scratch, shape, plan, warmups):
    with tempfile.TemporaryDirectory(dir=scratch) as temporary:
        data = Path(temporary) / "data"
        with broker(binary, data) as port:
            create_topics(port, shape.partitions)
            ready = threading.Barrier(shape.clients)
            with ThreadPoolExecutor(max_workers=shape.clients) as executor:
                futures = [
                    executor.submit(drive_client, port, batches, warmups, ready) for batches in plan
                ]
                outcomes = [future.result() for future in futures]

        expected = [{} for _ in range(shape.partitions)]
        for (_, _, _, replies), batches in zip(outcomes, plan, strict=True):
            previous = [-1] * shape.partitions
            for payload, (correlation, _, request_batches) in zip(replies, batches, strict=True):
                offsets = check_ack(payload, correlation, shape.partitions)
                for partition, offset in enumerate(offsets):
                    assert offset > previous[partition], (partition, offset, previous[partition])
                    assert offset not in expected[partition], (partition, offset)
                    previous[partition] = offset
                    expected[partition][offset] = request_batches[partition]
        total = len(plan[0]) * shape.clients
        for batches in expected:
            assert sorted(batches) == list(range(total))

        with broker(binary, data) as port:
            for partition, batches in enumerate(expected):
                fetch(port, partition, batches)

    earliest = min(result[0] for result in outcomes)
    latest = max(result[1] for result in outcomes)
    times = sorted(duration for result in outcomes for duration in result[2])
    records = len(times) * shape.partitions
    return {
        "records_per_s": records * 1e9 / (latest - earliest),
        "p99_us": times[math.ceil(len(times) * .99) - 1] / 1e3,
        "timed_records": records,
        "verified_records": total * shape.partitions,
    }


def emit(label, value):
    print(label, json.dumps(value, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--requests", type=int, default=240)
    parser.add_argument("--warmup", type=int, default=24)
    parser.add_argument("--scratch", type=Path, required=True)
    options = parser.parse_args()
    assert options.pairs >= 2 and options.requests > 0 and options.warmup >= 0
    assert crc32c(b"123456789") == 0xE3069283
    os.environ.setdefault("TOKIO_WORKER_THREADS", "2")
    binaries = {"baseline": options.baseline.resolve(), "candidate": options.candidate.resolve()}
    shapes = [Shape(1, 1), Shape(1, 4), Shape(1, 8), Shape(4, 4)]
    per_client = options.requests + options.warmup
    emit("config", {
        "pairs": options.pairs, "requests_per_client": options.requests,
        "warmup_per_client": options.warmup, "runtime_threads": os.environ["TOKIO_WORKER_THREADS"],
        "sync_concurrency": 4, "acks": 1, "payload_bytes": PAYLOAD_BYTES,
        "machine": os.uname().machine, "produce_version": 3, "fetch_version": 4,
    })
    for shape in shapes:
        plan = [
            [make_produce(client, shape.partitions, seq, per_client) for seq in range(per_client)]
            for client in range(shape.clients)
        ]
        observations = []
        for pair in range(options.pairs):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            arms = {}
            for arm in order:
                arms[arm] = run_arm(binaries[arm], options.scratch, shape, plan, options.warmup)
                emit("arm", {"shape": shape.label, "pair": pair + 1, "arm": arm, **arms[arm]})
            observations.append(arms)
        rates = [
            (pair["candidate"]["records_per_s"] / pair["baseline"]["records_per_s"] - 1) * 100
            for pair in observations
        ]
        emit("summary", {
            "shape": shape.label,
            "pair_rate_change_pct_median": statistics.median(rates),
            "pair_rate_change_pct_range": [min(rates), max(rates)],
            **{
                f"{metric}_{arm}_median": statistics.median(
                    pair[arm][metric] for pair in observations
                )
                for arm in binaries
                for metric in ("records_per_s", "p99_us")
            },
            "verified_records": sum(
                pair[arm]["verified_records"] for pair in observations for arm in binaries
            ),
        })


if __name__ == "__main__":
    main()

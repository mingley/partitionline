#!/usr/bin/env python3
"""Observe Apache Kafka's response and connection mute around Produce quota.

One plaintext loopback connection per version, one partition, sequential
requests. This diagnostic does not establish quota equivalence or throughput.
"""
import json
import socket
import struct
import time
from pathlib import Path

FIXTURES = Path("partitionline-broker/tests/fixtures/produce/4.3.1")
MAX_FRAME = 2 * 1024 * 1024


def read_exact(sock, count):
    chunks = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            raise RuntimeError("unexpected EOF")
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def exchange(sock, request):
    start = time.monotonic()
    sock.sendall(struct.pack(">i", len(request)) + request)
    size, = struct.unpack(">i", read_exact(sock, 4))
    if size < 4 or size > MAX_FRAME:
        raise ValueError(f"invalid response frame length {size}")
    body = read_exact(sock, size)
    return body, (time.monotonic() - start) * 1000


def parse_produce(version, body):
    pos = 0

    def number(fmt):
        nonlocal pos
        value, = struct.unpack_from(fmt, body, pos)
        pos += struct.calcsize(fmt)
        return value

    if number(">i") != 7 or number(">i") != 1:
        raise ValueError("bad correlation or topic count")
    size = number(">h")
    if size < 0 or body[pos:pos+size] != b"alpha":
        raise ValueError("bad topic name")
    pos += size
    if number(">i") != 1 or number(">i") != 0:
        raise ValueError("bad partition count or index")
    error = number(">h")
    offset = number(">q")
    number(">q")  # log append timestamp
    if version >= 5:
        number(">q")  # log start offset
    throttle = number(">i")
    if pos != len(body) or error != 0 or offset < 0 or throttle < 0:
        raise ValueError(f"bad response error={error} offset={offset} throttle={throttle}")
    return offset, throttle


def observe(version):
    request = bytearray((FIXTURES / f"produce-v{version}-acks1.request.bin").read_bytes())
    client = f"quota-probe-v{version}".encode("ascii")
    if len(client) != 14 or request[8:10] != struct.pack(">h", 14):
        raise ValueError("fixture header changed")
    request[10:24] = client
    metadata = struct.pack(">hhih", 3, 1, 77, -1) + struct.pack(">i", -1)
    last_offset = -1
    with socket.create_connection(("127.0.0.1", 9092), timeout=20) as sock:
        sock.settimeout(20)
        for attempt in range(1, 21):
            response, elapsed_ms = exchange(sock, request)
            offset, throttle_ms = parse_produce(version, response)
            if offset <= last_offset:
                raise ValueError("offset did not increase")
            last_offset = offset
            print(json.dumps({"version": version, "attempt": attempt,
                              "offset": offset, "throttle_ms": throttle_ms,
                              "produce_rtt_ms": round(elapsed_ms, 3)}), flush=True)
            if throttle_ms >= 300:
                reply, metadata_ms = exchange(sock, metadata)
                if len(reply) < 8 or struct.unpack_from(">i", reply)[0] != 77:
                    raise ValueError("invalid metadata response")
                brokers, = struct.unpack_from(">i", reply, 4)
                if brokers < 1:
                    raise ValueError("metadata response has no broker")
                print(json.dumps({"version": version, "observation": "metadata_after_throttle",
                                  "preceding_throttle_ms": throttle_ms,
                                  "metadata_rtt_ms": round(metadata_ms, 3),
                                  "preceding_produce_rtt_ms": round(elapsed_ms, 3)}),
                      flush=True)
                return
    raise RuntimeError(f"version {version}: no throttle of at least 300ms observed")


for v in (3, 5, 6):
    observe(v)

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



def request_for(version, client):
    request = bytearray((FIXTURES / f"produce-v{version}-acks1.request.bin").read_bytes())
    encoded = client.encode("ascii")
    if len(encoded) != 14 or request[8:10] != struct.pack(">h", 14):
        raise ValueError("fixture header changed")
    request[10:24] = encoded
    return request


def observe(version):
    alpha = request_for(version, f"quota-alpha-v{version}")
    bravo = request_for(version, f"quota-bravo-v{version}")
    metadata = struct.pack(">hhih", 3, 1, 77, -1) + struct.pack(">i", -1)
    with socket.create_connection(("127.0.0.1", 9092), timeout=20) as a, \
         socket.create_connection(("127.0.0.1", 9092), timeout=20) as b1, \
         socket.create_connection(("127.0.0.1", 9092), timeout=20) as b2:
        for sock in (a, b1, b2):
            sock.settimeout(20)
        last_offset = -1
        for attempt in range(1, 6):
            response, elapsed = exchange(a, alpha)
            offset, delay = parse_produce(version, response)
            if offset <= last_offset:
                raise ValueError("alpha offset did not advance")
            last_offset = offset
            print(json.dumps({"version": version, "client": "alpha",
                              "attempt": attempt, "offset": offset,
                              "throttle_ms": delay, "rtt_ms": round(elapsed, 3)}), flush=True)
            if delay >= 300:
                break
        else:
            raise RuntimeError("alpha did not reach observed throttle")

        # Queue metadata on the muted alpha socket, then use two independent
        # sockets bearing the same bravo client ID.
        start = time.monotonic()
        a.sendall(struct.pack(">i", len(metadata)) + metadata)
        first, first_ms = exchange(b1, bravo)
        first_offset, first_delay = parse_produce(version, first)
        second, second_ms = exchange(b2, bravo)
        second_offset, second_delay = parse_produce(version, second)
        if first_offset <= last_offset or second_offset <= first_offset:
            raise ValueError("bravo offsets did not advance")
        if first_delay != 0:
            raise ValueError(f"fresh bravo inherited a throttle: {first_delay}")
        if second_delay < 300:
            raise ValueError(f"second bravo connection did not share its client quota: {second_delay}")
        size, = struct.unpack(">i", read_exact(a, 4))
        if size < 8 or size > MAX_FRAME:
            raise ValueError("invalid metadata frame")
        reply = read_exact(a, size)
        metadata_ms = (time.monotonic() - start) * 1000
        if struct.unpack_from(">i", reply)[0] != 77 or struct.unpack_from(">i", reply, 4)[0] < 1:
            raise ValueError("invalid metadata response")
        if metadata_ms < 300:
            raise ValueError("alpha's subsequent response was not observably muted")
        print(json.dumps({"version": version, "client": "bravo",
                          "first_offset": first_offset, "second_offset": second_offset,
                          "first_throttle_ms": first_delay, "second_throttle_ms": second_delay,
                          "first_rtt_ms": round(first_ms, 3), "second_rtt_ms": round(second_ms, 3),
                          "alpha_previous_throttle_ms": delay,
                          "alpha_metadata_rtt_ms": round(metadata_ms, 3)}), flush=True)


for version in (3, 6):
    observe(version)

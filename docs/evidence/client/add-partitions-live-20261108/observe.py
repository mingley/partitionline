#!/usr/bin/env python3
"""Loopback-only Kafka frame witness. Forwards frames unchanged."""
import argparse
import asyncio
import json
import struct
from pathlib import Path

class Decode:
    def __init__(self, data):
        self.data, self.pos = data, 0
    def take(self, n):
        if n < 0 or self.pos + n > len(self.data):
            raise ValueError("truncated input")
        result = self.data[self.pos:self.pos + n]
        self.pos += n
        return result
    def number(self, fmt):
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))[0]
    def uvar(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.take(1)[0]
            value |= (byte & 127) << shift
            if not byte & 128:
                return value
        raise ValueError("overlong varint")
    def string(self):
        size = self.uvar()
        if size == 0:
            raise ValueError("unexpected null compact string")
        return self.take(size - 1).decode("utf-8")
    def arraylen(self):
        size = self.uvar()
        if size == 0 or size > 1001:
            raise ValueError("invalid compact array length")
        return size - 1
    def tags(self):
        count = self.uvar()
        if count > 1000:
            raise ValueError("invalid tag count")
        for _ in range(count):
            self.uvar()
            self.take(self.uvar())
    def finished(self):
        if self.pos != len(self.data):
            raise ValueError("trailing bytes")

def decode_request(frame):
    d = Decode(frame)
    key, version, corr = d.number(">h"), d.number(">h"), d.number(">i")
    if (key, version) != (24, 3):
        raise ValueError("unexpected request API/version")
    client_len = d.number(">h")
    if client_len < -1:
        raise ValueError("invalid client id")
    if client_len >= 0:
        d.take(client_len)
    d.tags()
    txn, pid, epoch = d.string(), d.number(">q"), d.number(">h")
    topics = []
    for _ in range(d.arraylen()):
        name = d.string()
        parts = [d.number(">i") for _ in range(d.arraylen())]
        d.tags()
        topics.append({"topic": name, "partitions": parts})
    d.tags()
    d.finished()
    return {"correlation": corr, "transactional_id": txn, "producer_id": pid,
            "producer_epoch": epoch, "topics": topics}

def decode_response(frame):
    d = Decode(frame)
    corr = d.number(">i")
    d.tags()
    throttle = d.number(">i")
    topics = []
    for _ in range(d.arraylen()):
        name = d.string()
        parts = []
        for _ in range(d.arraylen()):
            parts.append({"partition": d.number(">i"), "error": d.number(">h")})
            d.tags()
        d.tags()
        topics.append({"topic": name, "partitions": parts})
    d.tags()
    d.finished()
    return {"correlation": corr, "throttle_ms": throttle, "topics": topics}

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=Path, required=True)
    args = p.parse_args()
    count = 0

    def event(obj):
        with args.events.open("a", encoding="utf8") as out:
            out.write(json.dumps(obj, sort_keys=True) + "\n")
            out.flush()

    async def client(reader, writer):
        nonlocal count
        count += 1
        conn = count
        outstanding = {}
        try:
            remote_reader, remote_writer = await asyncio.open_connection("127.0.0.1", 9093)
        except OSError as exc:
            event({"kind": "connect_error", "connection": conn, "error": str(exc)})
            writer.close()
            return

        async def relay(src, dst, request):
            try:
                while True:
                    header = await src.readexactly(4)
                    size = struct.unpack(">i", header)[0]
                    if size < 4 or size > 128 * 1024 * 1024:
                        raise ValueError("invalid frame size")
                    frame = await src.readexactly(size)
                    if request and size >= 8:
                        key, version, corr = struct.unpack(">hhi", frame[:8])
                        if key == 24:
                            outstanding[corr] = version
                            try:
                                parsed = decode_request(frame)
                                event({"kind": "request", "connection": conn,
                                       "version": version, "data": parsed})
                            except Exception as exc:
                                event({"kind": "decode_error", "connection": conn,
                                       "direction": "request", "error": str(exc)})
                    elif not request:
                        corr = struct.unpack(">i", frame[:4])[0]
                        if corr in outstanding:
                            version = outstanding.pop(corr)
                            try:
                                if version != 3:
                                    raise ValueError("unexpected response API version")
                                parsed = decode_response(frame)
                                event({"kind": "response", "connection": conn,
                                       "version": version, "data": parsed})
                            except Exception as exc:
                                event({"kind": "decode_error", "connection": conn,
                                       "direction": "response", "error": str(exc)})
                    dst.write(header + frame)
                    await dst.drain()
            except asyncio.IncompleteReadError:
                pass
            except (ConnectionError, OSError):
                pass
            except Exception as exc:
                event({"kind": "relay_error", "connection": conn, "error": str(exc)})
            finally:
                dst.close()

        tasks = [asyncio.create_task(relay(reader, remote_writer, True)),
                 asyncio.create_task(relay(remote_reader, writer, False))]
        await asyncio.gather(*tasks, return_exceptions=True)
        writer.close()
        remote_writer.close()

    server = await asyncio.start_server(client, "127.0.0.1", 19092)
    print("listening on 127.0.0.1:19092 -> 127.0.0.1:9093", flush=True)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

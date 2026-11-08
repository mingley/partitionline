#!/usr/bin/env python3
"""Transparent local Kafka proxy that records Produce frames and delays replies."""
import asyncio
import itertools
import os
from pathlib import Path
import struct
import time

CAPTURE = Path(os.environ["PL_WIRE_CAPTURE"])
CAPTURE.mkdir(parents=True, exist_ok=True)
DELAY = 0.200
IDS = itertools.count(1)

async def read_frame(reader):
    prefix = await reader.readexactly(4)
    length = struct.unpack(">i", prefix)[0]
    if not 4 <= length <= 32 * 1024 * 1024:
        raise ValueError(f"invalid frame length {length}")
    return prefix, await reader.readexactly(length)

async def session(client_reader, client_writer):
    connection = next(IDS)
    peer_writer = None
    try:
        peer_reader, peer_writer = await asyncio.open_connection("127.0.0.1", 9093)
        pending = {}

        async def requests():
            while True:
                prefix, body = await read_frame(client_reader)
                api, version, correlation = struct.unpack_from(">hhi", body, 0)
                pending[correlation] = (api, version)
                if api == 0:
                    (CAPTURE / f"c{connection}_q{correlation}_v{version}.req").write_bytes(body)
                peer_writer.write(prefix + body)
                await peer_writer.drain()

        async def responses():
            while True:
                prefix, body = await read_frame(peer_reader)
                correlation = struct.unpack_from(">i", body, 0)[0]
                api, version = pending.pop(correlation, (-1, -1))
                if api == 0:
                    (CAPTURE / f"c{connection}_q{correlation}_v{version}.resp").write_bytes(body)
                started = time.monotonic_ns()
                await asyncio.sleep(DELAY)
                peer_delay = time.monotonic_ns() - started
                client_writer.write(prefix + body)
                await client_writer.drain()
                print(f"PROXY_RESPONSE api={api} correlation={correlation} delay_ns={peer_delay}", flush=True)

        tasks = [asyncio.create_task(requests()), asyncio.create_task(responses())]
        done, remaining = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in remaining:
            task.cancel()
        for task in done:
            try:
                task.result()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
        await asyncio.gather(*remaining, return_exceptions=True)
    except (ConnectionError, OSError, asyncio.IncompleteReadError) as error:
        print(f"PROXY_SESSION_END connection={connection} error={error!r}", flush=True)
    finally:
        client_writer.close()
        if peer_writer is not None:
            peer_writer.close()

async def main():
    server = await asyncio.start_server(session, "127.0.0.1", 9092)
    print("PROXY_READY", flush=True)
    async with server:
        await server.serve_forever()

asyncio.run(main())

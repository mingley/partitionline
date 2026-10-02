#!/usr/bin/env python3
"""Capture actual librdkafka frames using a bounded fake discovery-only TCP peer.
This endpoint is diagnostic only; no Rust/Apache broker runtime qualification.
"""
import hashlib
import json
from pathlib import Path
import socket
import struct
import subprocess
import threading
ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
OUT = ROOT / 'native-all-topics'; OUT.mkdir(exist_ok=True)
PORT = 19126
FRAMES = []
STOP = threading.Event()
LOCK = threading.Lock()
FAILURES = []
def read_exact(conn, length):
    data = bytearray()
    while len(data) < length:
        part = conn.recv(length-len(data))
        if not part: raise EOFError()
        data.extend(part)
    return bytes(data)
def serve(conn):
    conn.settimeout(15)
    try:
        while not STOP.is_set():
            length = struct.unpack('>i', read_exact(conn, 4))[0]
            assert 10 <= length <= 128*1024
            request = read_exact(conn, length)
            key, version, cid = struct.unpack('>hhi', request[:8])
            client_length = struct.unpack('>h', request[8:10])[0]
            client_id = request[10:10+client_length].decode()
            if key == 18:
                response = (REPO / 'partitionline-broker/tests/fixtures/metadata/4.3.1/api-versions-v3.response.bin').read_bytes()
            elif key == 3:
                response = (REPO / 'partitionline-broker/tests/fixtures/metadata/4.3.1/metadata-v13-empty.response.bin').read_bytes()
                old = struct.pack('>i', 19095); assert response.count(old) == 1
                response = response.replace(old, struct.pack('>i', PORT))
            else: raise AssertionError(('unexpected API', key))
            response = struct.pack('>i', cid) + response[4:]
            with LOCK:
                number = len(FRAMES)
                name = f'frame-{number:02}-{key}-v{version}.request.bin'
                (OUT / name).write_bytes(request)
                FRAMES.append({'file':name,'api_key':key,'version':version,'correlation_id':cid,'client_id':client_id,'length_without_prefix':len(request),'sha256':hashlib.sha256(request).hexdigest(),'hex':request.hex()})
            conn.sendall(struct.pack('>i',len(response)) + response)
    except EOFError: pass
    except Exception as error:
        FAILURES.append(repr(error))
    finally: conn.close()
def accept(listener):
    threads = []
    while not STOP.is_set():
        try: conn, _ = listener.accept()
        except socket.timeout: continue
        thread=threading.Thread(target=serve,args=(conn,),daemon=True);thread.start();threads.append(thread)
    for thread in threads: thread.join(timeout=20)
with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    listener.bind(('127.0.0.1',PORT));listener.listen(8);listener.settimeout(0.2)
    thread=threading.Thread(target=accept,args=(listener,));thread.start()
    cmd=['taskset','-c','0-2,4','/workspace/work/broker-metadata/metadata-c-peer',f'127.0.0.1:{PORT}','all-probe']
    try: result=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
    finally: STOP.set();thread.join(timeout=25)
(OUT / 'capture-native.txt').write_text('$ '+' '.join(cmd)+'\n'+result.stdout+result.stderr+f'\nexit_code={result.returncode}\n')
report={'scope':'Actual pinned librdkafka client frames captured by diagnostic fake discovery endpoint; no broker compatibility qualification. Response values originate in official golden serialization, with only correlation/advertised port adjusted for discovery.','port':PORT,'native_source_commit':'9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab','native_library_sha256':hashlib.sha256(Path('/workspace/work/c-peer/lib/librdkafka.so.1').read_bytes()).hexdigest(),'command':cmd,'exit_code':result.returncode,'frames':FRAMES,'capture_errors':FAILURES}
(OUT / 'capture.json').write_text(json.dumps(report,indent=2)+'\n')
assert result.returncode==0 and not FAILURES
print(json.dumps({'frames':len(FRAMES),'metadata_lengths':[f['length_without_prefix'] for f in FRAMES if f['api_key']==3]}))

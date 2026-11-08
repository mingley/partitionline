#!/usr/bin/env python3
"""One independent disposable three-node quorum diagnostic; never retries writes."""
import hashlib
import json
import os
import queue
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

OUT = Path(os.environ["QD_EVIDENCE"]).resolve()
BIN = os.environ["QD_OPERATOR"]
OUT.mkdir(parents=True, exist_ok=True)
EVENTS = (OUT / "events.jsonl").open("w", encoding="utf-8", buffering=1)
CAP = 1024 * 1024
ALL_NODES = []

def emit(event, **fields):
    row = {"utc_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
           "event": event, **fields}
    EVENTS.write(json.dumps(row, sort_keys=True) + "\n")
    print(json.dumps(row, sort_keys=True), flush=True)

class Drain:
    def __init__(self, pipe, path, ready=None):
        self.pipe, self.path, self.ready = pipe, path, ready
        self.total = self.kept = self.dropped = 0
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
    def run(self):
        pending = b""
        try:
            with self.path.open("wb", buffering=0) as out:
                while True:
                    data = os.read(self.pipe.fileno(), 8192)
                    if not data:
                        break
                    self.total += len(data)
                    n = min(len(data), max(0, CAP - self.kept))
                    if n:
                        out.write(data[:n])
                        self.kept += n
                    self.dropped += len(data) - n
                    if self.ready is not None and len(pending) < 8192:
                        pending += data[:8192-len(pending)]
                        if b"\n" in pending:
                            self.ready.put(pending.split(b"\n", 1)[0].decode("utf-8", "replace"))
                            self.ready = None
        except Exception as error:
            self.error = repr(error)
        finally:
            self.pipe.close()
    def join(self):
        self.thread.join(timeout=5)
        return {"file": str(self.path.relative_to(OUT)), "total_bytes": self.total,
                "retained_bytes": self.kept, "dropped_bytes": self.dropped,
                "drain_alive": self.thread.is_alive(), "drain_error": self.error}

class Node:
    def __init__(self, root, ident, members, sequence):
        self.ident, self.root, self.sequence = ident, root, sequence
        ready = queue.Queue(maxsize=1)
        root.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["PL_QUORUM_DIAGNOSTIC"] = "1"
        self.p = subprocess.Popen(
            [BIN, "serve-quorum", "--data", str(root), "--cluster",
             "qd-disposable-quorum", "--id", str(ident), "--members", members],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        self.stdout = Drain(self.p.stdout, OUT / f"node-{ident}-start-{sequence}.stdout", ready)
        self.stderr = Drain(self.p.stderr, OUT / f"node-{ident}-start-{sequence}.stderr")
        ALL_NODES.append(self)
        emit("node_start", node=ident, sequence=sequence, pid=self.p.pid)
        try:
            line = ready.get(timeout=20)
        except queue.Empty as error:
            self.stop("startup_timeout")
            raise RuntimeError(f"node {ident} startup timed out; exit={self.p.poll()}") from error
        if "profile=metadata-only-quorum" not in line:
            self.stop("bad_startup")
            raise RuntimeError(f"node {ident} bad startup line {line!r}")
        emit("node_ready", node=ident, sequence=sequence, line=line)
    def stop(self, reason):
        if getattr(self, "stopped", False) and self.p.poll() is not None:
            return
        self.stopped = True
        emit("node_stop_begin", node=self.ident, sequence=self.sequence, reason=reason)
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait(timeout=10)
                emit("node_forced_kill", node=self.ident, sequence=self.sequence)
        result = {"node": self.ident, "sequence": self.sequence, "reason": reason,
                  "returncode": self.p.returncode,
                  "stdout": self.stdout.join(), "stderr": self.stderr.join()}
        emit("node_stopped", **result)
        if result["stdout"]["drain_alive"] or result["stderr"]["drain_alive"]:
            raise RuntimeError("capture thread did not drain after process exit")
        if result["stdout"]["drain_error"] or result["stderr"]["drain_error"]:
            raise RuntimeError("capture drain error")

def be16(n): return struct.pack(">h", n)
def be32(n): return struct.pack(">i", n)
def kstr(s):
    value = s.encode()
    return be16(len(value)) + value
def header(api, version):
    return be16(api) + be16(version) + be32(77) + be16(-1)
def create(name):
    return (header(19, 3) + be32(1) + kstr(name) + be32(1) + be16(1)
            + be32(0) + be32(0) + be32(3000) + b"\0")
def delete(name): return header(20, 1) + be32(1) + kstr(name) + be32(3000)
def metadata(name): return header(3, 1) + be32(1) + kstr(name)

class Reader:
    def __init__(self, raw): self.raw, self.i = raw, 0
    def take(self, n):
        if n < 0 or self.i + n > len(self.raw): raise ValueError("truncated response")
        result = self.raw[self.i:self.i+n]
        self.i += n
        return result
    def i16(self): return struct.unpack(">h", self.take(2))[0]
    def i32(self): return struct.unpack(">i", self.take(4))[0]
    def string(self):
        n = self.i16()
        if n == -1: return None
        if n < 0: raise ValueError("negative string")
        return self.take(n).decode()

def exchange(port, payload, kind, name):
    emit("request_once", port=port, kind=kind, name=name, request_hex=payload.hex(),
         semantics="disposable fixture mutation" if kind in ("create","delete") else
                   "Kafka Metadata: may refresh and sync application projection")
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=6) as s:
            s.settimeout(6)
            s.sendall(be32(len(payload)) + payload)
            def exact(n):
                parts = []
                while n:
                    block = s.recv(n)
                    if not block: raise EOFError("unexpected EOF")
                    parts.append(block); n -= len(block)
                return b"".join(parts)
            length = struct.unpack(">i", exact(4))[0]
            if not 0 <= length <= 1024 * 1024: raise ValueError("response length")
            raw = exact(length)
        emit("response", port=port, kind=kind, name=name, response_hex=raw.hex())
        return raw
    except Exception as error:
        emit("transport_error", port=port, kind=kind, name=name, error=repr(error))
        return None

def mutation_error(raw, name):
    if raw is None: return None
    r = Reader(raw)
    if r.i32() != 77: raise ValueError("correlation")
    r.i32()
    if r.i32() != 1 or r.string() != name: raise ValueError("topic response")
    return r.i16()
def meta(port, name):
    raw = exchange(port, metadata(name), "metadata", name)
    if raw is None: return None
    r = Reader(raw)
    if r.i32() != 77: raise ValueError("metadata correlation")
    n = r.i32()
    if n != 3: raise ValueError("broker count")
    for _ in range(n):
        r.i32(); r.string(); r.i32(); r.string()
    controller = r.i32()
    if r.i32() != 1: raise ValueError("metadata topic count")
    code = r.i16()
    topic = r.string()
    if topic != name: raise ValueError("metadata topic name")
    r.take(1)
    parts = r.i32()
    emit("metadata_observation", port=port, name=name, controller=controller,
         topic_error=code, partitions=parts, caveat="applied projection, not linearizable")
    return controller, code

def leader(ports, alive, exclude=None):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        for i, port in enumerate(ports):
            if not alive[i]: continue
            observation = meta(port, "qd-probe")
            if observation is not None:
                controller = observation[0]
                if 0 <= controller < 3 and alive[controller] and controller != exclude:
                    emit("leader_hint", observer=i, reported_controller=controller,
                         caveat="hint only; not an admission proof")
                    return controller
        time.sleep(0.05)
    raise RuntimeError("leader hint timeout")

def wait_present(ports, alive, name):
    deadline = time.monotonic() + 20
    for i in range(3):
        if not alive[i]: continue
        while time.monotonic() < deadline:
            result = meta(ports[i], name)
            if result is not None and result[1] == 0:
                break
            time.sleep(.05)
        else:
            raise RuntimeError(f"topic {name} not observed on node {i}")

def crc32c(data):
    c = 0xffffffff
    for byte in data:
        c ^= byte
        for _ in range(8):
            c = (c >> 1) ^ (0x82f63b78 if c & 1 else 0)
    return c ^ 0xffffffff

def digest(path):
    with path.open("rb") as f:
        h = hashlib.sha256()
        for chunk in iter(lambda: f.read(65536), b""): h.update(chunk)
        return h.hexdigest()

def decode_command(payload):
    if len(payload) < 50 or payload[:8] != b"PLMETCMD": return None
    n = int.from_bytes(payload[48:50], "big")
    if len(payload) != 50 + n: raise ValueError("command length")
    return {"token": payload[8:24].hex(), "opcode": payload[24],
            "target": payload[28:44].hex(), "count": int.from_bytes(payload[44:48],"big"),
            "name_hex": payload[50:].hex(), "command_hex": payload.hex()}

def parse_journal(path):
    raw = path.read_bytes()
    if len(raw) < 24 or raw[:8] != b"PLJRNL01" or crc32c(raw[:20]) != int.from_bytes(raw[20:24],"big"):
        raise ValueError("invalid journal header")
    at, offset, entries = 24, int.from_bytes(raw[8:16],"big"), []
    while at < len(raw):
        if len(raw) - at < 32: raise ValueError(f"incomplete entry header at {at}")
        h = raw[at:at+32]
        length = int.from_bytes(h[8:12],"big")
        first = int.from_bytes(h[12:20],"big")
        count = int.from_bytes(h[20:24],"big")
        if h[:8] != b"PLENTRY1" or crc32c(h[:28]) != int.from_bytes(h[28:32],"big"):
            raise ValueError(f"entry header/checksum at {at}")
        if not length or first != offset or not count: raise ValueError(f"entry range at {at}")
        p = raw[at+32:at+32+length]
        if len(p) != length or crc32c(p) != int.from_bytes(h[24:28],"big"):
            raise ValueError(f"entry payload/checksum at {at}")
        entries.append((first, p))
        at += 32 + length
        offset += count
    return entries

def inspect_content(entries):
    live, history, committed, complete = {}, [], 0, True
    for journal_index, payload in entries:
        if payload[:8] not in (b"PLREPL01", b"PLREPL02") or len(payload) < 16:
            complete = False; history.append({"journal_index":journal_index,"unknown":payload[:8].hex()}); continue
        op = payload[8]
        if op == 1: continue
        cursor = 16
        if payload[:8] == b"PLREPL02":
            if len(payload) < 24: raise ValueError("dynamic op header")
            voters = int.from_bytes(payload[16:20],"big")
            cursor = 24 + voters + 80
        if cursor > len(payload): raise ValueError("dynamic authority bounds")
        if op == 2:
            start = int.from_bytes(payload[cursor:cursor+8],"big")
            count = int.from_bytes(payload[cursor+8:cursor+12],"big")
            cursor += 16
            for j in range(count):
                if cursor + 24 > len(payload): raise ValueError("record header")
                term = int.from_bytes(payload[cursor:cursor+8],"big")
                kind = payload[cursor+8]
                length = int.from_bytes(payload[cursor+16:cursor+20],"big")
                cursor += 24
                data = payload[cursor:cursor+length]
                if len(data) != length: raise ValueError("record length")
                cursor += length
                index = start + j
                if index in live: raise ValueError("append overwrites live record")
                record = {"index":index,"term":term,"kind":kind,"command":decode_command(data)}
                live[index] = record
                history.append({"journal_index":journal_index,"append":record})
        elif op == 3:
            keep = int.from_bytes(payload[cursor:cursor+8],"big")
            history.append({"journal_index":journal_index,"truncate_keep_through":keep})
            live = {i:r for i,r in live.items() if i <= keep}
        elif op == 4:
            committed = int.from_bytes(payload[cursor:cursor+8],"big")
            history.append({"journal_index":journal_index,"commit_through":committed})
        else:
            complete = False
            history.append({"journal_index":journal_index,"unsupported_op":op})
    return {"complete_interpretation":complete, "committed_through":committed,
            "live":list(live.values()), "history":history}

def inspect_application(entries):
    receipts = []
    for journal_index, payload in entries:
        if payload[:8] != b"PLMETAPP": continue
        if len(payload) < 32: raise ValueError("application receipt header")
        length = int.from_bytes(payload[28:32],"big")
        data = payload[32:32+length]
        if len(data) != length or len(payload) != 32+length: raise ValueError("application receipt length")
        receipts.append({"journal_index":journal_index,
                         "term":int.from_bytes(payload[8:16],"big"),
                         "index":int.from_bytes(payload[16:24],"big"),
                         "kind":payload[24], "command":decode_command(data)})
    return receipts

def snapshot_and_inspect(roots):
    results = []
    for ident, root in enumerate(roots):
        dst = OUT / "raw" / f"node-{ident}"
        dst.mkdir(parents=True, exist_ok=True)
        for name in ("quorum.identity","content.wal","election.wal","application.wal"):
            source = root / name
            if not source.exists():
                results.append({"node":ident,"file":name,"missing":True})
                continue
            target = dst / name
            shutil.copyfile(source, target)
            before = digest(target)
            item = {"node":ident, "file":name, "bytes":target.stat().st_size,
                    "sha256_before":before}
            try:
                if name.endswith(".wal"):
                    entries = parse_journal(target)
                    item["journal_entries"] = len(entries)
                    item["checksum_validation"] = "ok"
                    if name == "content.wal": item["content"] = inspect_content(entries)
                    if name == "application.wal": item["application_receipts"] = inspect_application(entries)
                else:
                    item["identity_utf8"] = target.read_text(encoding="utf-8")
            except Exception as error:
                item["parse_error"] = repr(error)
            item["sha256_after"] = digest(target)
            item["unchanged_by_inspection"] = item["sha256_before"] == item["sha256_after"]
            results.append(item)
    (OUT / "journal-analysis.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    emit("raw_copies_inspected", files=len(results),
         parse_errors=sum("parse_error" in x for x in results),
         changed=sum(not x.get("unchanged_by_inspection", True) for x in results))
    return results

def main():
    tmp = Path(tempfile.mkdtemp(prefix="qd-disposable-"))
    roots = [tmp / str(i) for i in range(3)]
    reservations = [socket.socket() for _ in range(6)]
    for s in reservations: s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in reservations]
    for s in reservations: s.close()
    clients = [ports[1], ports[3], ports[5]]
    members = ",".join(f"{i}@{i+1:032x}@127.0.0.1:{ports[2*i]}@127.0.0.1:{clients[i]}" for i in range(3))
    label = str(time.time_ns())
    before, after, minority = (f"qd-before-{label}", f"qd-after-{label}", f"qd-minority-{label}")
    live = [None, None, None]
    all_nodes = ALL_NODES
    starts = [0, 0, 0]
    result = {"before":before,"after":after,"minority":minority,
              "delete_attempts":0, "minority_attempts":0, "outcome":"incomplete"}
    emit("fixture_start", roots=[str(x) for x in roots], members=members,
         names={"before":before,"after":after,"minority":minority},
         mutation_label="synthetic disposable fixture; one mutation attempt per name")
    def start(i):
        starts[i] += 1
        live[i] = Node(roots[i], i, members, starts[i])
    def alive(): return [x is not None for x in live]
    try:
        for i in range(3): start(i)
        first = leader(clients, alive())
        response = exchange(clients[first], create(before), "create", before)
        code = mutation_error(response, before)
        emit("create_once_result", name=before, contacted_node=first, code=code)
        if code != 0: raise RuntimeError("initial create failed or ambiguous; not retrying")
        wait_present(clients, alive(), before)
        live[first].stop("first_leader_failover"); live[first] = None
        new = leader(clients, alive(), first)
        response = exchange(clients[new], create(after), "create", after)
        code = mutation_error(response, after)
        emit("create_once_result", name=after, contacted_node=new, code=code)
        if code != 0: raise RuntimeError("post-failover create failed or ambiguous; not retrying")
        wait_present(clients, alive(), after)
        start(first)
        wait_present(clients, alive(), before)
        wait_present(clients, alive(), after)
        isolated = leader(clients, alive())
        for i in range(3):
            if i != isolated:
                live[i].stop("isolate_minority"); live[i] = None
        result["minority_attempts"] += 1
        response = exchange(clients[isolated], create(minority), "create", minority)
        result["minority_code"] = mutation_error(response, minority)
        emit("minority_once_result", name=minority, contacted_node=isolated,
             code=result["minority_code"], admission="unproven until exact owner append/result event")
        for i in range(3):
            if i != isolated: start(i)
        wait_present(clients, alive(), before)
        wait_present(clients, alive(), after)
        current = leader(clients, alive())
        result["delete_attempts"] += 1
        response = exchange(clients[current], delete(before), "delete", before)
        result["delete_code"] = mutation_error(response, before)
        result["delete_contacted_node"] = current
        result["outcome"] = "response_captured" if response is not None else "ambiguous_transport"
        emit("delete_once_result", name=before, contacted_node=current, code=result["delete_code"],
             caveat="one fresh disposable Delete only; never replay on any result")
    except Exception as error:
        result["error"] = repr(error)
        emit("fixture_incomplete", error=repr(error))
    finally:
        emit("stop_all_before_copy")
        for i in range(3):
            if live[i] is not None:
                try: live[i].stop("final_snapshot")
                except Exception as error: emit("stop_error", node=i, error=repr(error))
                live[i] = None
        for node in all_nodes:
            if node.p.poll() is None:
                try: node.stop("final_orphan_cleanup")
                except Exception as error: emit("orphan_stop_error", node=node.ident, error=repr(error))
        all_joined = all(n.p.poll() is not None and not n.stdout.thread.is_alive()
                         and not n.stderr.thread.is_alive() for n in all_nodes)
        result["all_processes_and_captures_joined"] = all_joined
        qevents = []
        for path in sorted(OUT.glob("node-*-start-*.stderr")):
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                if not line.startswith("QDIAG\t"): continue
                fields = dict(part.split("=", 1) for part in line.split("\t")[1:] if "=" in part)
                fields["capture_file"] = path.name
                if "command" in fields:
                    try: fields["decoded"] = decode_command(bytes.fromhex(fields["command"]))
                    except Exception as error: fields["decode_error"] = repr(error)
                qevents.append(fields)
        (OUT / "qdiag-events.json").write_text(json.dumps(qevents, indent=2, sort_keys=True) + "\n")
        minority_events = [e for e in qevents if e.get("decoded")
                           and e["decoded"].get("name_hex") == minority.encode().hex()]
        exact_commands = {e["command"] for e in minority_events
                          if e.get("stage") == "execute_enter"}
        admitted = [e for e in minority_events
                    if e.get("command") in exact_commands
                    and e.get("stage") == "owner_propose_result"
                    and e.get("detail", "").startswith("(Ok(")]
        result["minority_proposal_admission"] = "confirmed" if admitted else "unproven"
        result["minority_admission_evidence"] = admitted
        result["delete_command_traces"] = [e for e in qevents if e.get("decoded") and e["decoded"].get("opcode") == 2]
        result["diagnostic_bytes_dropped"] = sum(n.stdout.dropped + n.stderr.dropped for n in all_nodes)
        if all_joined:
            analysis = snapshot_and_inspect(roots)
            result["snapshot_parse_errors"] = sum("parse_error" in x for x in analysis)
            result["snapshot_hash_changes"] = sum(not x.get("unchanged_by_inspection",True) for x in analysis)
        else:
            result["snapshot_status"] = "refused: a process or capture thread was not joined"
            emit("snapshot_refused", reason=result["snapshot_status"])
        (OUT / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        emit("fixture_finished", **result)
    return result

if __name__ == "__main__":
    main()
    EVENTS.close()

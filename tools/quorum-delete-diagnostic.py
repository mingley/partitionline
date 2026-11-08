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
MAX_COPY = 16 * 1024 * 1024
IDENTITY_MAX = 16 * 1024
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

def state_since(node, start):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            raw = node.stderr.path.read_bytes()
        except FileNotFoundError:
            raw = b""
        for line in reversed(raw[start:].splitlines()):
            if not line.startswith(b"QSTATE\t"): continue
            try:
                fields = dict(part.split("=", 1) for part in line.decode("utf-8").split("\t")[1:])
                if {"node","term","role","leader","active_term","ready","poisoned"} <= fields.keys():
                    return fields
            except (ValueError, UnicodeError):
                pass
        time.sleep(.01)
    return None

def leader(ports, live, exclude=None):
    deadline = time.monotonic() + 20
    previous, stable = None, 0
    while time.monotonic() < deadline:
        alive = [n is not None for n in live]
        observations = []
        for i, port in enumerate(ports):
            if not alive[i]: continue
            observed = meta(port, "qd-probe")
            observations.append((i, observed[0] if observed is not None else None))
        controllers = [x[1] for x in observations]
        agreed = (controllers[0] if controllers and None not in controllers
                  and len(set(controllers)) == 1 else None)
        if agreed is None or not 0 <= agreed < 3 or not alive[agreed] or agreed == exclude:
            previous, stable = None, 0
            time.sleep(.05)
            continue
        stable = stable + 1 if previous == agreed else 1
        previous = agreed
        if stable < 2:
            time.sleep(.05)
            continue
        node = live[agreed]
        try: start = node.stderr.path.stat().st_size
        except FileNotFoundError: start = 0
        observation = meta(ports[agreed], "qd-owner-preflight")
        sample = state_since(node, start)
        good = bool(observation is not None and observation[0] == agreed and sample
                    and sample["node"] == str(agreed) and sample["role"] == "Leader"
                    and sample["leader"] == f"Some({agreed})"
                    and sample["active_term"] == f"Some({sample['term']})"
                    and sample["ready"] == "true" and sample["poisoned"] == "false")
        emit("leader_preflight", observations=observations, stable_rounds=stable,
             proposed_controller=agreed, owner_state=sample, qualifies=good,
             caveat="all observations are hints; leadership may change before the one request")
        if good:
            return agreed
        previous, stable = None, 0
        time.sleep(.05)
    raise RuntimeError("stable metadata and active owner-state hint timeout")

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
                command = decode_command(data)
                if kind == 0 and command is None: complete = False
                record = {"index":index,"term":term,"kind":kind,"command":command}
                live[index] = record
                history.append({"journal_index":journal_index,"append":record})
        elif op == 3:
            if cursor + 8 > len(payload): raise ValueError("truncate body")
            keep = int.from_bytes(payload[cursor:cursor+8],"big")
            history.append({"journal_index":journal_index,"truncate_keep_through":keep})
            live = {i:r for i,r in live.items() if i <= keep}
        elif op == 4:
            if cursor + 8 > len(payload): raise ValueError("commit body")
            committed = int.from_bytes(payload[cursor:cursor+8],"big")
            history.append({"journal_index":journal_index,"commit_through":committed})
        else:
            complete = False
            history.append({"journal_index":journal_index,"unsupported_op":op})
    return {"complete_interpretation":complete, "committed_through":committed,
            "verification_scope":"CRC32C and operation/record extraction; not full Raft authority or voter validation",
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
            limit = IDENTITY_MAX if name == "quorum.identity" else MAX_COPY
            item = {"node":ident, "file":name, "source_bytes":source.stat().st_size,
                    "copy_limit":limit}
            if item["source_bytes"] > limit:
                item["over_limit"] = True
                results.append(item)
                continue
            try:
                total = 0
                with source.open("rb") as inp, target.open("xb") as out:
                    while True:
                        block = inp.read(min(65536, limit + 1 - total))
                        if not block: break
                        total += len(block)
                        if total > limit: raise ValueError("source grew beyond copy bound")
                        out.write(block)
                before = digest(target)
                item.update(bytes=target.stat().st_size, sha256_before=before)
            except Exception as error:
                item["copy_error"] = repr(error)
                results.append(item)
                continue
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
        source_images = root / "images"
        image_item = {"node":ident, "file":"images", "directory":True,
                      "root_members":sorted(p.name for p in root.iterdir()) if root.exists() else []}
        if not source_images.is_dir():
            image_item["missing"] = True
        else:
            members = []
            for member in source_images.iterdir():
                if len(members) >= 32:
                    image_item["directory_over_limit"] = True
                    break
                members.append(member)
            members.sort()
            image_item["members"] = [p.name for p in members]
            image_item["complete_interpretation"] = not members
            for member in members:
                if not member.is_file() or member.is_symlink():
                    image_item.setdefault("unsupported_members", []).append(member.name)
                    continue
                size = member.stat().st_size
                if size > MAX_COPY:
                    image_item.setdefault("over_limit_members", []).append(member.name)
                    continue
                image_dir = dst / "images"
                image_dir.mkdir(exist_ok=True)
                target = image_dir / member.name
                try:
                    total = 0
                    with member.open("rb") as inp, target.open("xb") as out:
                        while True:
                            block = inp.read(min(65536, MAX_COPY + 1 - total))
                            if not block: break
                            total += len(block)
                            if total > MAX_COPY: raise ValueError("image exceeded copy bound")
                            out.write(block)
                    before = digest(target)
                    after = digest(target)
                    image_item.setdefault("copied_members", []).append({
                        "name":member.name, "bytes":total, "sha256_before":before,
                        "sha256_after":after, "unchanged_by_inspection":before == after})
                except Exception as error:
                    image_item.setdefault("copy_errors", []).append(repr(error))
        results.append(image_item)
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
              "delete_attempts":0, "minority_attempts":0, "outcome":"incomplete",
              "snapshot_timing_note":"after graceful stop/join, not necessarily at Delete response"}
    emit("fixture_start", roots=[str(x) for x in roots], members=members,
         names={"before":before,"after":after,"minority":minority},
         mutation_label="synthetic disposable fixture; one mutation attempt per name")
    def start(i):
        starts[i] += 1
        live[i] = Node(roots[i], i, members, starts[i])
    def alive(): return [x is not None for x in live]
    try:
        for i in range(3): start(i)
        first = leader(clients, live)
        response = exchange(clients[first], create(before), "create", before)
        code = mutation_error(response, before)
        emit("create_once_result", name=before, contacted_node=first, code=code)
        if code != 0: raise RuntimeError("initial create failed or ambiguous; not retrying")
        wait_present(clients, alive(), before)
        live[first].stop("first_leader_failover"); live[first] = None
        new = leader(clients, live, first)
        response = exchange(clients[new], create(after), "create", after)
        code = mutation_error(response, after)
        emit("create_once_result", name=after, contacted_node=new, code=code)
        if code != 0: raise RuntimeError("post-failover create failed or ambiguous; not retrying")
        wait_present(clients, alive(), after)
        start(first)
        wait_present(clients, alive(), before)
        wait_present(clients, alive(), after)
        isolated = leader(clients, live)
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
        current = leader(clients, live)
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
            for raw_line in path.read_bytes().splitlines():
                if not (raw_line.startswith(b"QDIAG\t") or raw_line.startswith(b"QSTATE\t")): continue
                try:
                    line = raw_line.decode("utf-8", "strict")
                    parts = line.split("\t")
                    pieces = parts[1:]
                    if any("=" not in piece for piece in pieces): raise ValueError("malformed diagnostic field")
                    fields = dict(part.split("=", 1) for part in pieces)
                    if parts[0] == "QSTATE":
                        fields["stage"] = "owner_state_sample"
                        if not {"node","term","role","leader","active_term","ready","poisoned"} <= fields.keys():
                            raise ValueError("missing QSTATE field")
                    elif "stage" not in fields: raise ValueError("missing QDIAG stage")
                    if "command" in fields:
                        fields["decoded"] = decode_command(bytes.fromhex(fields["command"]))
                        if fields["decoded"] is None: raise ValueError("invalid QDIAG command")
                    elif parts[0] == "QDIAG" and fields["stage"] not in ("position_mismatch","delete_token","delete_command"):
                        raise ValueError("missing QDIAG command")
                except Exception as error:
                    fields = {"decode_error":repr(error), "raw_hex":raw_line.hex()}
                fields["capture_file"] = path.name
                qevents.append(fields)
        (OUT / "qdiag-events.json").write_text(json.dumps(qevents, indent=2, sort_keys=True) + "\n")
        minority_events = [e for e in qevents if e.get("decoded")
                           and e["decoded"].get("name_hex") == minority.encode().hex()]
        exact_commands = {e["command"] for e in minority_events
                          if e.get("stage") == "execute_enter"}
        owner_entered = {(e.get("capture_file"),e.get("command")) for e in minority_events
                         if e.get("stage") == "owner_before_propose"}
        admitted = [e for e in minority_events
                    if e.get("command") in exact_commands
                    and (e.get("capture_file"),e.get("command")) in owner_entered
                    and e.get("stage") == "owner_propose_result"
                    and e.get("detail", "").startswith("(Ok(")]
        result["minority_proposal_admission"] = "confirmed" if admitted else "unproven"
        result["minority_admission_evidence"] = admitted
        result["delete_command_traces"] = [e for e in qevents if e.get("decoded") and e["decoded"].get("opcode") == 2]
        result["delete_catalog_stage"] = "command_observed" if result["delete_command_traces"] else "unobserved"
        result["delete_terminal_errors"] = [e for e in result["delete_command_traces"] if e.get("stage") in ("catalog_execute_result","catalog_outcome") and e.get("detail","").startswith("Err(")]
        result["diagnostic_bytes_dropped"] = sum(n.stdout.dropped + n.stderr.dropped for n in all_nodes)
        result["diagnostic_decode_errors"] = sum("decode_error" in e for e in qevents)
        result["capture_errors"] = sum(bool(c.error) for n in all_nodes for c in (n.stdout,n.stderr))
        result["nonzero_node_exits"] = sum(n.p.returncode != 0 for n in all_nodes)
        if all_joined:
            analysis = snapshot_and_inspect(roots)
            result["snapshot_parse_errors"] = sum("parse_error" in x for x in analysis)
            result["snapshot_missing_files"] = sum(bool(x.get("missing")) for x in analysis)
            result["snapshot_copy_errors"] = sum(bool(x.get("copy_error") or x.get("copy_errors")) for x in analysis)
            result["snapshot_over_limit"] = sum(bool(x.get("over_limit") or x.get("over_limit_members") or x.get("directory_over_limit")) for x in analysis)
            result["snapshot_uninterpreted"] = sum(x.get("content",{}).get("complete_interpretation") is False or x.get("complete_interpretation") is False or bool(x.get("unsupported_members")) for x in analysis)
            result["snapshot_hash_changes"] = sum(not x.get("unchanged_by_inspection",True) for x in analysis) + sum(not y.get("unchanged_by_inspection",True) for x in analysis for y in x.get("copied_members",[]))
            result["snapshot_inventory"] = [{"node":x["node"],"file":x["file"],"bytes":x.get("bytes"),"sha256":x.get("sha256_before"),"missing":x.get("missing",False),"members":x.get("members"),"root_members":x.get("root_members")} for x in analysis]
        else:
            result["snapshot_status"] = "refused: a process or capture thread was not joined"
            emit("snapshot_refused", reason=result["snapshot_status"])
        (OUT / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        emit("fixture_finished", **result)
    return result

if __name__ == "__main__":
    main()
    EVENTS.close()

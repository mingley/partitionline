#!/usr/bin/env python3
"""Independent bounded dynamic-membership bytes, without broker imports.

The existing independently authored CRC/cursor/record helpers are shared with
the fixed-prefix oracle. Dynamic voter configurations, authority envelopes,
images and directory-qualified election histories are decoded here afresh.
This is a finite captured-history oracle, not exhaustive consensus proof.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
import struct
import sys

FIXED_ORACLE = Path(__file__).resolve().parents[3] / "KL11-15/oracle/history"
sys.path.insert(0, str(FIXED_ORACLE))
from wal_oracle import Cursor, Record, Rejected, crc32c, require

MAX_FILE = 32 * 1024 * 1024
MAX_OPERATION = 4 * 1024 * 1024
MAX_OPERATIONS = 8192
MAX_ENTRIES = 4096
MAX_PAYLOAD = 16 * 1024 * 1024
MAX_RECORD = 1024 * 1024
MAX_TERM = 1 << 31
MAX_CONFIGURATION = 135168


def text(r: Cursor, size: int) -> str:
    require(1 <= size <= 249, "identity string bound")
    try:
        return r.take(size).decode("utf-8")
    except UnicodeDecodeError as error:
        raise Rejected("identity UTF-8") from error


def key(r: Cursor) -> dict:
    node, directory = r.number(">I"), r.take(16)
    require(node <= 0x7fffffff and any(directory), "full voter identity")
    return {"id": node, "directory": directory.hex()}


def key_tuple(value: dict) -> tuple:
    return value["id"], value["directory"]


def contains(view: dict, value: dict) -> bool:
    return any(row["key"] == value for row in view["voters"])


def decode_configuration(data: bytes) -> dict:
    require(40 <= len(data) <= MAX_CONFIGURATION, "configuration byte bound")
    r = Cursor(data)
    require(r.take(8) == b"PLVOTR01", "configuration magic")
    epoch, term, index = (r.number(">Q") for _ in range(3))
    feature, count = r.number(">h"), r.number(">H")
    r.zero(4)
    require(feature == 1 and 1 <= count <= 64 and
            ((epoch == 0 and term == index == 0) or
             (epoch > 0 and 1 <= term <= MAX_TERM and 1 <= index <= MAX_ENTRIES)),
            "configuration feature/count/position")
    voters = []
    for _ in range(count):
        identity = key(r)
        minimum, maximum, endpoint_count = r.number(">h"), r.number(">h"), r.number(">H")
        r.zero(2)
        require(0 <= minimum <= 1 <= maximum and 1 <= endpoint_count <= 4,
                "voter feature/endpoints")
        endpoints = []
        for _ in range(endpoint_count):
            listener_size, host_size, port = (r.number(">H") for _ in range(3))
            listener, host = text(r, listener_size), text(r, host_size)
            require(re.fullmatch(r"[A-Z0-9_.-]+", listener) is not None and
                    "\0" not in host and port > 0, "endpoint syntax")
            endpoints.append({"listener": listener, "host": host, "port": port})
        names = [e["listener"] for e in endpoints]
        require(names == sorted(set(names)), "canonical distinct endpoints")
        voters.append({"key": identity, "kraft_min": minimum,
                       "kraft_max": maximum, "endpoints": endpoints})
    r.finish()
    ids = [v["key"]["id"] for v in voters]
    require(ids == sorted(set(ids)), "canonical distinct voters")
    return {"epoch": epoch, "position": {"term": term, "index": index},
            "feature": feature, "canonical_hex": data.hex(), "voters": voters}


def configuration(r: Cursor) -> dict:
    size = r.number(">I")
    require(40 <= size <= MAX_CONFIGURATION, "configuration declared bound")
    return decode_configuration(r.take(size))


def successor(old: dict, new: dict) -> None:
    require(new["epoch"] == old["epoch"] + 1 and
            new["position"]["index"] > old["position"]["index"] and
            new["position"]["term"] >= old["position"]["term"],
            "configuration successor position/epoch")
    a, b = ({v["key"]["id"]: v for v in x["voters"]} for x in (old, new))
    require(len(set(a) ^ set(b)) == 1 and all(a[n] == b[n] for n in set(a) & set(b)),
            "one voter change preserving retained descriptors")


def configuration_in(rows: list[Record], genesis: dict, commit_floor: int | None = None) -> dict:
    view = genesis
    for row in rows:
        if row.kind == 2:
            if commit_floor is not None:
                require(view["position"]["index"] <= commit_floor,
                        "second configuration before prior commit")
            new = decode_configuration(row.payload)
            require(new["position"] == {"term": row.term, "index": row.index},
                    "voter record exact position")
            successor(view, new)
            view = new
    return view


def read_journal_entries(path: Path) -> list[bytes]:
    require(path.is_file() and not path.is_symlink() and 24 <= path.stat().st_size <= MAX_FILE,
            "journal regular file/byte bound")
    entries = []
    with path.open("rb") as stream:
        header = stream.read(24)
        require(header[:8] == b"PLJRNL01" and header[8:20] == bytes(12) and
                crc32c(header[:20]) == int.from_bytes(header[20:24], "big"),
                "journal initialization CRC")
        while True:
            header = stream.read(32)
            if not header:
                break
            require(len(header) == 32 and len(entries) < MAX_OPERATIONS and
                    header[:8] == b"PLENTRY1" and crc32c(header[:28]) ==
                    int.from_bytes(header[28:32], "big"), "journal entry header CRC/count")
            length, offset, count, checksum = struct.unpack(">IQII", header[8:28])
            require(1 <= length <= MAX_OPERATION and offset == len(entries) and count == 1,
                    "journal entry length/offset/span")
            payload = stream.read(length)
            require(len(payload) == length and crc32c(payload) == checksum,
                    "journal payload completeness/CRC")
            entries.append(payload)
    require(entries, "empty initialized journal")
    return entries


def group_body(group: dict) -> bytes:
    cluster, topic = group["cluster_id"].encode(), group["topic"].encode()
    voters = [v["key"]["id"] for v in group["genesis"]["voters"]]
    return (struct.pack(">iHHH2x", group["partition"], len(voters), len(cluster), len(topic)) +
            b"".join(struct.pack(">I", n) for n in voters) + cluster + topic +
            bytes.fromhex(group["genesis"]["canonical_hex"]))


def decode_image(data: bytes, group: dict) -> dict:
    require(118 <= len(data) <= MAX_FILE, "image byte bound")
    r = Cursor(data)
    require(r.take(8) == b"PLSNAP02" and r.number(">H") == 2, "dynamic image format")
    r.zero(2)
    header_size, generation = r.number(">I"), r.take(16)
    require(94 <= header_size <= 838 + 4 + MAX_CONFIGURATION and any(generation),
            "image header/generation")
    base, term, count, payload_size = (r.number(">Q") for _ in range(4))
    require(base == count <= MAX_ENTRIES and payload_size <= MAX_PAYLOAD and
            ((base == term == 0) or (base > 0 and 1 <= term <= MAX_TERM)),
            "image base/count/term/payload")
    cluster, topic = (text(r, r.number(">I")) for _ in range(2))
    partition, voter_count = r.number(">I"), r.number(">I")
    require(partition <= 0x7fffffff and 1 <= voter_count <= 64, "image group bound")
    voters = [r.number(">I") for _ in range(voter_count)]
    genesis = configuration(r)
    require(genesis == group["genesis"] and cluster == group["cluster_id"] and
            topic == group["topic"] and partition == group["partition"] and
            voters == [v["key"]["id"] for v in genesis["voters"]], "image immutable group/genesis")
    require(r.offset + 4 == header_size and crc32c(data[:r.offset]) == r.number(">I"),
            "image exact header length/CRC")
    require(len(data) == header_size + count * 32 + payload_size + 24,
            "image exact encoded length")
    rows, previous, observed_payload = [], 0, 0
    for index in range(1, count + 1):
        epoch, actual_index, kind = r.number(">Q"), r.number(">Q"), r.number(">B")
        r.zero(7)
        length = r.number(">I")
        r.zero(4)
        require(actual_index == index and previous <= epoch <= MAX_TERM and epoch > 0 and
                ((kind in (0, 2) and 1 <= length <= MAX_RECORD) or (kind == 1 and length == 0)),
                "image record continuity/kind/payload")
        observed_payload += length
        require(observed_payload <= payload_size, "image payload budget")
        rows.append(Record(index, epoch, kind, r.take(length)))
        previous = epoch
    require(observed_payload == payload_size and previous == term, "image aggregate/base term")
    configuration_in(rows, genesis)
    body_end = r.offset
    require(r.take(8) == b"PLSNEND1" and r.number(">Q") == len(data), "image completion seal")
    require(r.number(">I") == crc32c(data[:body_end]) and
            r.number(">I") == crc32c(data[body_end:body_end + 20]), "image body/footer CRC")
    r.finish()
    return {"descriptor": {"generation": generation.hex(), "base": {"term": term, "index": base},
                           "records": count, "payload_bytes": payload_size, "bytes": len(data),
                           "checksum": crc32c(data)}, "records": rows,
            "sha256": hashlib.sha256(data).hexdigest()}


class DynamicReplay:
    def __init__(self, images_dir: Path, expected_local: dict | None = None,
                 expected_group: dict | None = None):
        self.images_dir, self.expected_local, self.expected_group = images_dir, expected_local, expected_group
        self.local = self.group = self.view = None
        self.rows = []
        self.committed = self.max_term = self.source_operation = 0
        self.selected = None
        self.operations = []

    def tail(self) -> tuple:
        return (self.rows[-1].term, len(self.rows)) if self.rows else (0, 0)

    def eligible(self, leader: dict, term: int) -> bool:
        if contains(self.view, leader):
            return True
        position = self.view["position"]
        return (position["index"] > self.committed and position["term"] == term and
                contains(configuration_in(self.rows[:position["index"] - 1], self.group["genesis"]), leader))

    def authority(self, r: Cursor, opcode: int) -> dict:
        kind = r.number(">B")
        r.zero(7)
        term, leader_id, peer_id = r.number(">Q"), r.number(">I"), r.number(">I")
        sequence, commit = r.number(">Q"), r.number(">Q")
        leader, peer = {"id": leader_id, "directory": r.take(16).hex()}, {"id": peer_id, "directory": r.take(16).hex()}
        epoch = r.number(">Q")
        require(kind in (0, 1) and self.max_term <= term <= MAX_TERM and term > 0 and
                peer == self.local, "authority kind/term/intended directory")
        if kind == 0:
            require(leader == self.local and sequence == 0 and commit == self.committed and
                    epoch == self.view["epoch"] and (opcode == 5 or self.eligible(leader, term)),
                    "local authority/configuration/commit")
        else:
            require(leader != self.local and sequence > 0 and self.eligible(leader, term),
                    "remote known-directory authority")
        return {"kind": kind, "term": term, "leader": leader, "peer": peer,
                "sequence": sequence, "leader_commit": commit, "configuration_epoch": epoch}

    def apply(self, data: bytes) -> None:
        r = Cursor(data)
        require(r.take(8) == b"PLREPL02", "dynamic operation magic")
        opcode = r.number(">B")
        r.zero(7)
        require(opcode in (1, 2, 3, 4, 5), "dynamic operation kind")
        if opcode == 1:
            require(not self.operations, "repeated content initialization")
            node, partition = r.number(">I"), r.number(">i")
            count, cluster_size, topic_size = (r.number(">H") for _ in range(3))
            r.zero(2)
            require(node <= 0x7fffffff and partition >= 0 and 1 <= count <= 64, "content identity bounds")
            voters = [r.number(">I") for _ in range(count)]
            cluster, topic = text(r, cluster_size), text(r, topic_size)
            directory = r.take(16)
            genesis = configuration(r)
            require(any(directory) and genesis["epoch"] == 0 and voters ==
                    [v["key"]["id"] for v in genesis["voters"]], "content genesis/directory")
            self.local = {"id": node, "directory": directory.hex()}
            self.group = {"cluster_id": cluster, "topic": topic, "partition": partition, "genesis": genesis}
            require((self.expected_local is None or self.local == self.expected_local) and
                    (self.expected_group is None or self.group == self.expected_group), "foreign content identity")
            self.view, claimed, auth = genesis, genesis, None
        else:
            require(self.group is not None, "operation before content identity")
            size = r.number(">I")
            r.zero(4)
            require(40 <= size <= MAX_CONFIGURATION, "result configuration byte bound")
            claimed = decode_configuration(r.take(size))
            auth = self.authority(r, opcode)
            if opcode == 2:
                start, count = r.number(">Q"), r.number(">I")
                r.zero(4)
                require(start == len(self.rows) + 1 and 1 <= count <= MAX_ENTRIES and
                        len(self.rows) + count <= MAX_ENTRIES, "append positions/count")
                previous = self.tail()[0]
                added = []
                for number in range(count):
                    term, kind = r.number(">Q"), r.number(">B")
                    r.zero(7)
                    length = r.number(">I")
                    r.zero(4)
                    require(1 <= term <= auth["term"] and term >= previous and
                            (auth["kind"] == 1 or term == auth["term"]) and
                            ((kind in (0, 2) and 1 <= length <= MAX_RECORD) or (kind == 1 and length == 0)),
                            "append record term/kind/payload")
                    added.append(Record(start + number, term, kind, r.take(length)))
                    previous = term
                prior = configuration_in(self.rows, self.group["genesis"])
                for row in added:
                    if row.kind == 2:
                        require(prior["position"]["index"] <= auth["leader_commit"],
                                "append second configuration before prior commit")
                        next_view = decode_configuration(row.payload)
                        require(next_view["position"] == {"term": row.term, "index": row.index},
                                "append voter exact position")
                        successor(prior, next_view)
                        prior = next_view
                self.rows.extend(added)
                self.max_term = max(self.max_term, auth["term"])
            elif opcode == 3:
                target, old_term, old_index, term, floor = (r.number(">Q") for _ in range(5))
                require(auth["kind"] == 1 and auth["term"] == term and
                        (old_term, old_index) == self.tail() and floor == self.committed and
                        floor <= target < old_index and old_term < term, "truncate prior/floor/authority")
                self.rows = self.rows[:target]
                self.max_term = max(self.max_term, term)
            elif opcode == 4:
                commit, term, leader, kind = r.number(">Q"), r.number(">Q"), r.number(">I"), r.number(">B")
                r.zero(3)
                sequence = r.number(">Q")
                require((kind, term, leader, sequence) == (auth["kind"], auth["term"], auth["leader"]["id"], auth["sequence"]) and
                        self.committed < commit <= len(self.rows) and self.rows[commit - 1].term <= term and
                        (kind == 0 and self.rows[commit - 1].term == term or kind == 1 and commit <= auth["leader_commit"]),
                        "commit body/authority/current term")
                self.committed, self.max_term = commit, max(self.max_term, term)
            else:
                self.install(r, auth)
            next_view = configuration_in(self.rows, self.group["genesis"])
            require(next_view == claimed, "forged resulting configuration")
            if next_view != self.view:
                self.source_operation = len(self.operations)
            self.view = next_view
        r.finish()
        require(sum(len(row.payload) for row in self.rows) <= MAX_PAYLOAD, "live payload budget")
        self.operations.append({"opcode": opcode, "authority": auth, "view": self.view,
                                "source_operation": self.source_operation, "tail": self.tail(),
                                "committed": self.committed,
                                "payload_sha256": hashlib.sha256(data).hexdigest()})

    def install(self, r: Cursor, auth: dict) -> None:
        kind = r.number(">B")
        r.zero(7)
        term, leader, peer = r.number(">Q"), r.number(">I"), r.number(">I")
        sequence, leader_commit = r.number(">Q"), r.number(">Q")
        generation = r.take(16).hex()
        base_term, base, count = r.number(">Q"), r.number(">Q"), r.number(">I")
        r.zero(4)
        payload_bytes, encoded_bytes, checksum = r.number(">Q"), r.number(">Q"), r.number(">I")
        r.zero(4)
        prior_tail, prior_commit = (r.number(">Q"), r.number(">Q")), r.number(">Q")
        retained_tail, new_commit = (r.number(">Q"), r.number(">Q")), r.number(">Q")
        has_prior = r.number(">B")
        r.zero(7)
        old_generation, old_base, old_checksum = r.take(16).hex(), (r.number(">Q"), r.number(">Q")), r.number(">I")
        r.zero(4)
        require(r.take(len(group_body(self.group))) == group_body(self.group), "Install immutable group")
        require((kind, term, leader, peer, sequence, leader_commit) ==
                (auth["kind"], auth["term"], auth["leader"]["id"], auth["peer"]["id"], auth["sequence"], auth["leader_commit"]),
                "Install body/authority correlation")
        require(prior_tail == self.tail() and prior_commit == self.committed and
                has_prior in (0, 1) and bool(has_prior) == (self.selected is not None), "Install prior durable state")
        expected_prior = ((self.selected["generation"],
                           (self.selected["base"]["term"], self.selected["base"]["index"]), self.selected["checksum"])
                          if self.selected else ("00" * 16, (0, 0), 0))
        require((old_generation, old_base, old_checksum) == expected_prior, "Install prior selected descriptor")
        require(base >= self.committed and (self.selected is None or base >= self.selected["base"]["index"]) and
                base_term <= term and leader_commit >= base, "Install selected/committed floor")
        require((kind == 0 and base == self.committed == new_commit) or
                (kind == 1 and new_commit == base), "Install commit policy")
        path = self.images_dir / ("snapshot-" + generation + ".image")
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_FILE, "selected image availability/bound")
        image = decode_image(path.read_bytes(), self.group)
        descriptor = {"generation": generation, "base": {"term": base_term, "index": base},
                      "records": count, "payload_bytes": payload_bytes, "bytes": encoded_bytes, "checksum": checksum}
        require(image["descriptor"] == descriptor, "Install exact image descriptor")
        rows = image["records"]
        require(rows[:self.committed] == self.rows[:self.committed], "Install committed overlap")
        exact = True
        for old, new in zip(self.rows, rows):
            if old != new:
                require(old.term != new.term and term > self.tail()[0], "Install same-term or stale conflict")
                exact = False
        result = rows + self.rows[base:] if exact and len(self.rows) > base else rows.copy()
        tail = (result[-1].term, len(result)) if result else (0, 0)
        require(len(result) <= MAX_ENTRIES and tail == retained_tail, "Install retained suffix/tail")
        self.rows, self.committed, self.selected = result, new_commit, descriptor
        self.max_term = max(self.max_term, term)


def read_content(path: Path, images_dir: Path, local: dict | None = None,
                 group: dict | None = None) -> DynamicReplay:
    result = DynamicReplay(images_dir, local, group)
    for payload in read_journal_entries(path):
        result.apply(payload)
    return result


def election_group(group: dict) -> bytes:
    cluster, topic = group["cluster_id"].encode(), group["topic"].encode()
    return struct.pack(">H", len(cluster)) + cluster + struct.pack(">H", len(topic)) + topic + struct.pack(">I", group["partition"])


def read_election(path: Path, content: DynamicReplay) -> dict:
    entries = read_journal_entries(path)
    initial = Cursor(entries[0])
    require(initial.take(8) == b"PLDINIT2" and key(initial) == content.local, "election immutable local directory")
    size = initial.number(">I")
    initial.zero(8)
    require(40 <= size <= MAX_CONFIGURATION, "election genesis bound")
    require(decode_configuration(initial.take(size)) == content.group["genesis"], "election immutable genesis")
    group_size = initial.number(">I")
    require(1 <= group_size <= 506 and initial.take(group_size) == election_group(content.group), "election immutable group")
    initial.finish()
    old = {"term": 0, "vote": None, "log": (0, 0), "view": content.group["genesis"], "source": 0}
    states = [old]
    for payload in entries[1:]:
        r = Cursor(payload)
        magic = r.take(8)
        require(magic in (b"PLELECT2", b"PLRECON2", b"PLELCFG2") and key(r) == content.local,
                "election state format/local directory")
        term, vote_id, present = r.number(">Q"), r.number(">I"), r.number(">B")
        r.zero(3)
        directory = r.take(16)
        summary = (r.number(">Q"), r.number(">Q"))
        source, size = r.number(">Q"), r.number(">I")
        r.zero(8)
        require(40 <= size <= MAX_CONFIGURATION, "election configuration bound")
        view = decode_configuration(r.take(size))
        require(present in (0, 1) and ((present and any(directory) and vote_id <= 0x7fffffff) or
                (not present and vote_id == 0 and directory == bytes(16))), "election full vote flag")
        vote = {"id": vote_id, "directory": directory.hex()} if present else None
        require(old["term"] <= term <= MAX_TERM and 0 <= summary[0] <= term and
                (summary[0] == 0) == (summary[1] == 0) and summary[1] <= MAX_ENTRIES and
                (term > 0 or vote is None), "election term/log bounds")
        require(term != old["term"] or old["vote"] is None or vote == old["vote"], "same-term full-directory double vote")
        if vote is not None and not (term == old["term"] and vote == old["vote"]):
            require(contains(view, vote), "new vote outside current full-directory set")
        require(source < len(content.operations), "election source operation availability")
        origin = content.operations[source]
        require(origin["view"] == view and origin["source_operation"] == source,
                "election source must be actual configuration-changing operation")
        state = {"term": term, "vote": vote, "log": summary, "view": view, "source": source}
        if magic == b"PLELCFG2":
            floor = r.number(">Q")
            require(term == old["term"] and vote == old["vote"] and summary == old["log"] and
                    source >= old["source"] and floor <= view["position"]["index"] <= summary[1],
                    "configuration binding preserves election and committed floor")
            a, b = old["view"]["position"], view["position"]
            require((b["index"] < a["index"] and a["index"] > floor and view["epoch"] < old["view"]["epoch"]) or
                    (b["index"] > a["index"] and view["epoch"] > old["view"]["epoch"] and b["term"] >= a["term"]) or
                    (b["index"] == a["index"] and view == old["view"]), "configuration binding transition")
        else:
            require(view == old["view"] and source == old["source"], "election unbound configuration change")
            if magic == b"PLRECON2":
                expected, floor = (r.number(">Q"), r.number(">Q")), r.number(">Q")
                require(expected == old["log"] and term == old["term"] and vote == old["vote"] and
                        summary != old["log"] and term > old["log"][0] and floor <= old["log"][1] and
                        summary[1] >= floor, "election reconciliation prior/commit floor")
            else:
                require(summary[1] >= old["log"][1] and summary[0] >= old["log"][0] and
                        (summary[1] != old["log"][1] or summary == old["log"]) and
                        view["position"]["index"] <= summary[1], "election summary regression")
        r.finish()
        old = state
        states.append(state)
    require(old["view"] == content.view and old["source"] == content.source_operation and
            old["log"] == content.tail(), "final election/content exact binding")
    require(old["term"] >= content.max_term, "election below durable WAL authority term")
    return {"state_count": len(states), "states": states, "final": old,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}

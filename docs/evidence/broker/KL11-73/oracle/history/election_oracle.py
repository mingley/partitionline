#!/usr/bin/env python3
"""Independent election-journal decoding for captured replication checkpoints."""
from __future__ import annotations

import hashlib
import struct
from pathlib import Path

from wal_oracle import crc32c, require

MAX_STATES = 65_536
MAX_BYTES = 24 + MAX_STATES * (32 + 48 + 64 * 4 + 24)


def decode(payload, node, voters, old):
    count = len(voters)
    legacy_size = 48 + 4 * count
    magic = payload[:8]
    tagged = magic == b"PLRECON1"
    require(magic in (b"PLELECT1", b"PLRECON1") and
            len(payload) == legacy_size + (24 if tagged else 0), "election format/size")
    local, stored_count = struct.unpack(">IH", payload[8:14])
    require(local == node and stored_count == count and payload[14:16] == bytes(2) and
            payload[29:32] == bytes(3), "election identity/reserved")
    stored = list(struct.unpack(">" + "I" * count, payload[48:legacy_size]))
    require(stored == voters, "election voter identity")
    term = int.from_bytes(payload[16:24], "big")
    vote = int.from_bytes(payload[24:28], "big")
    present = payload[28]
    require(present in (0, 1) and (present or vote == 0), "election vote flag")
    vote = vote if present else None
    log_term, index = struct.unpack(">QQ", payload[32:48])
    require((log_term == 0) == (index == 0) and log_term <= term and
            (vote is None or vote in voters) and (term > 0 or vote is None),
            "election state term/vote/log")
    state = {"term": term, "voted_for": vote, "log": (log_term, index)}
    if old is None:
        require(not tagged and state == {"term": 0, "voted_for": None, "log": (0, 0)},
                "election initial state")
    elif tagged:
        expected_term, expected_index, floor = struct.unpack(">QQQ", payload[legacy_size:])
        require((expected_term, expected_index) == old["log"] and term == old["term"] and
                vote == old["voted_for"] and state["log"] != old["log"] and
                term > old["log"][0] and floor <= old["log"][1] and index >= floor,
                "election reconciliation prior/floor/term/vote")
        state["reconciliation"] = {"expected": old["log"], "floor": floor}
    else:
        require(term >= old["term"] and
                (term != old["term"] or old["voted_for"] is None or vote == old["voted_for"]),
                "election term regression/double vote")
        require(index >= old["log"][1] and log_term >= old["log"][0] and
                (index != old["log"][1] or state["log"] == old["log"]),
                "legacy election log regression")
    return state


def read_election(path: Path, node, voters):
    require(24 <= path.stat().st_size <= MAX_BYTES, "election file size ceiling")
    digest, states, size = hashlib.sha256(), [], 24
    with path.open("rb") as stream:
        header = stream.read(24)
        digest.update(header)
        require(header[:8] == b"PLJRNL01" and header[8:20] == bytes(12) and
                crc32c(header[:20]) == int.from_bytes(header[20:24], "big"),
                "election Journal header CRC/identity")
        while True:
            header = stream.read(32)
            if not header:
                break
            require(len(header) == 32 and len(states) < MAX_STATES and
                    header[:8] == b"PLENTRY1" and crc32c(header[:28]) ==
                    int.from_bytes(header[28:32], "big"), "election entry header CRC/count")
            length, offset, count, checksum = struct.unpack(">IQII", header[8:28])
            require(48 <= length <= 328 and offset == len(states) and count == 1,
                    "election entry bounds/offset")
            payload = stream.read(length)
            require(len(payload) == length and crc32c(payload) == checksum,
                    "election payload CRC/completeness")
            digest.update(header)
            digest.update(payload)
            size += 32 + length
            require(size <= MAX_BYTES, "growing election file bound")
            states.append(decode(payload, node, voters, states[-1] if states else None))
    require(states, "empty election journal")
    return {"sha256": digest.hexdigest(), "bytes": size, "state_count": len(states),
            "final": states[-1], "reconciliation_records": sum("reconciliation" in s for s in states)}

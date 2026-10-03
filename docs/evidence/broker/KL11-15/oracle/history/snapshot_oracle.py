#!/usr/bin/env python3
"""Independent bounded image and Install receipt decoder; no broker imports.

The unchanged independently authored KL11-73 decoder handles operations 1--4.
This module adds explicit image parsing and operation 5 replay. Complete images
without a selecting WAL receipt never change the recovered canonical prefix.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

from wal_oracle import (Cursor, MAX_ENTRIES, MAX_FILE, MAX_LIVE_BYTES,
                        MAX_OPERATION, MAX_OPERATIONS, MAX_RECORD, MAX_TERM,
                        Record, Replay, Rejected, crc32c, require)

MAX_IMAGE = 128 * 1024 * 1024


def decode_image(data: bytes, group: dict | None = None) -> dict:
    require(114 <= len(data) <= MAX_IMAGE, "image byte ceiling/minimum")
    r = Cursor(data)
    require(r.take(8) == b"PLSNAP01", "image magic")
    require(r.number(">H") == 1, "image revision")
    r.zero(2)
    header_size = r.number(">I")
    require(90 <= header_size <= 838 and header_size + 24 <= len(data),
            "image header length")
    generation = r.take(16)
    require(any(generation), "zero image generation")
    base, term, count, payload_size = (r.number(">Q") for _ in range(4))
    require(base == count <= MAX_ENTRIES and payload_size <= MAX_LIVE_BYTES and
            ((base == 0 and term == 0) or (base > 0 and 1 <= term <= MAX_TERM)),
            "image base/count/term/payload ceiling")
    strings = []
    for _ in range(2):
        length = r.number(">I")
        require(1 <= length <= 249, "image identity string length")
        try:
            strings.append(r.take(length).decode("utf-8"))
        except UnicodeDecodeError as error:
            raise Rejected("image identity UTF-8") from error
    partition, voter_count = r.number(">I"), r.number(">I")
    require(partition <= 0x7fffffff and 1 <= voter_count <= 64, "image group limits")
    voters = [r.number(">I") for _ in range(voter_count)]
    require(voters == sorted(set(voters)) and all(v <= 0x7fffffff for v in voters),
            "image voter identity")
    actual_group = {"cluster_id": strings[0], "topic": strings[1],
                    "partition": partition, "voters": voters}
    require(group is None or group == actual_group, "foreign image group")
    require(r.offset + 4 == header_size, "image exact header length")
    require(crc32c(data[:r.offset]) == r.number(">I"), "image header CRC")
    require(len(data) == header_size + count * 32 + payload_size + 24,
            "image exact encoded length")
    records, observed_payload, previous = [], 0, 0
    for index in range(1, count + 1):
        epoch, actual_index = r.number(">Q"), r.number(">Q")
        kind = r.number(">B")
        r.zero(7)
        length = r.number(">I")
        r.zero(4)
        require(actual_index == index and 1 <= epoch <= MAX_TERM and epoch >= previous,
                "image record index/term continuity")
        require((kind == 0 and 1 <= length <= MAX_RECORD) or (kind == 1 and length == 0),
                "image kind/payload length")
        observed_payload += length
        require(observed_payload <= payload_size, "image payload budget")
        records.append(Record(index, epoch, kind, r.take(length)))
        previous = epoch
    require(observed_payload == payload_size and previous == term, "image aggregate/base term")
    body_end = r.offset
    require(r.take(8) == b"PLSNEND1" and r.number(">Q") == len(data), "image completion seal")
    require(r.number(">I") == crc32c(data[:body_end]), "image body CRC")
    require(r.number(">I") == crc32c(data[body_end:body_end + 20]), "image footer CRC")
    r.finish()
    descriptor = {"generation": generation.hex(), "base": {"term": term, "index": base},
                  "records": count, "payload_bytes": payload_size, "bytes": len(data),
                  "checksum": crc32c(data)}
    return {"descriptor": descriptor, "group": actual_group, "records": records,
            "sha256": hashlib.sha256(data).hexdigest()}


def read_image(path: Path, group: dict | None = None) -> dict:
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_IMAGE,
            "image regular file/bound")
    return decode_image(path.read_bytes(), group)


def install_records(old: list[Record], committed: int, image: dict, receiving_term: int) -> list[Record]:
    rows = image["records"]
    base = len(rows)
    require(base >= committed, "snapshot committed floor regression")
    require(rows[:committed] == old[:committed], "snapshot changed committed overlap")
    overlap = min(len(old), base)
    exact = True
    for index in range(overlap):
        if old[index] != rows[index]:
            require(old[index].term != rows[index].term, "snapshot same-term changed content")
            require(receiving_term > (old[-1].term if old else 0), "stale snapshot conflict repair")
            exact = False
    result = rows + old[base:] if exact and len(old) > base else rows.copy()
    require(len(result) <= MAX_ENTRIES and sum(len(row.payload) for row in result) <= MAX_LIVE_BYTES,
            "snapshot result bounds")
    return result


class SnapshotReplay(Replay):
    def __init__(self, images_dir: Path, node: int | None = None, group: dict | None = None):
        super().__init__(node, group)
        self.images_dir = images_dir
        self.selected: dict | None = None
        self.image_receipts: list[dict] = []

    def apply(self, payload: bytes) -> None:
        if len(payload) < 9 or payload[8] != 5:
            super().apply(payload)
            return
        require(self.group is not None and len(self.operations) < MAX_OPERATIONS,
                "Install before identity/operation ceiling")
        r = Cursor(payload)
        require(r.take(8) == b"PLREPL01" and r.number(">B") == 5, "Install magic/opcode")
        r.zero(7)
        authority = r.number(">B")
        r.zero(7)
        term, leader, peer = r.number(">Q"), r.number(">I"), r.number(">I")
        sequence, leader_commit = r.number(">Q"), r.number(">Q")
        generation = r.take(16).hex()
        base_term, base = r.number(">Q"), r.number(">Q")
        count = r.number(">I")
        r.zero(4)
        payload_bytes, encoded_bytes, checksum = r.number(">Q"), r.number(">Q"), r.number(">I")
        r.zero(4)
        prior_tail = r.number(">Q"), r.number(">Q")
        prior_commit = r.number(">Q")
        retained_tail = r.number(">Q"), r.number(">Q")
        new_commit = r.number(">Q")
        has_prior = r.number(">B")
        r.zero(7)
        prior_generation = r.take(16).hex()
        prior_base = r.number(">Q"), r.number(">Q")
        prior_checksum = r.number(">I")
        r.zero(4)
        # Independently encode the Init transferable group; local ID is excluded.
        g = self.group
        cluster, topic = g["cluster_id"].encode(), g["topic"].encode()
        group_bytes = (struct.pack(">iHHH2x", g["partition"], len(g["voters"]), len(cluster), len(topic)) +
                       b"".join(struct.pack(">I", voter) for voter in g["voters"]) + cluster + topic)
        require(r.take(len(group_bytes)) == group_bytes, "Install foreign group")
        r.finish()
        actual_tail = (self.records[-1].term, len(self.records)) if self.records else (0, 0)
        require(prior_tail == actual_tail and prior_commit == self.committed,
                "Install prior tail/commit mismatch")
        require(has_prior in (0, 1) and bool(has_prior) == (self.selected is not None),
                "Install prior selection flag")
        expected_prior = ((self.selected["generation"],
                           (self.selected["base"]["term"], self.selected["base"]["index"]),
                           self.selected["checksum"]) if self.selected else ("00" * 16, (0, 0), 0))
        require((prior_generation, prior_base, prior_checksum) == expected_prior,
                "Install prior selected descriptor mismatch")
        require(1 <= term <= MAX_TERM and term >= self.max_term and term >= base_term and
                leader in g["voters"] and peer == self.local and authority in (0, 1),
                "Install authority/term/peer")
        require(base >= self.committed and (self.selected is None or base >= self.selected["base"]["index"]),
                "Install selected/committed base regression")
        if authority == 0:
            require(leader == peer == self.local and sequence == 0 and
                    leader_commit == base == self.committed == new_commit, "local Install authority")
        else:
            require(leader != self.local and sequence > 0 and leader_commit >= base and new_commit == base,
                    "remote Install authority/commit")
        image = read_image(self.images_dir / ("snapshot-" + generation + ".image"), g)
        descriptor = {"generation": generation, "base": {"term": base_term, "index": base},
                      "records": count, "payload_bytes": payload_bytes, "bytes": encoded_bytes,
                      "checksum": checksum}
        require(image["descriptor"] == descriptor, "Install descriptor/image binding")
        result = install_records(self.records, self.committed, image, term)
        result_tail = (result[-1].term, len(result)) if result else (0, 0)
        require(result_tail == retained_tail, "Install retained suffix/tail mismatch")
        self.records, self.committed, self.selected = result, new_commit, descriptor
        self.max_term = max(self.max_term, term)
        self.live_bytes = sum(len(row.payload) for row in result)
        self.image_receipts.append({"descriptor": descriptor, "sha256": image["sha256"]})
        self.operations.append({"opcode": "install", "operation_number": len(self.operations),
                                "authority": authority, "leader": leader, "peer": peer,
                                "term": term, "sequence": sequence, "descriptor": descriptor,
                                "committed_end": new_commit})

    def receipt(self) -> dict:
        result = super().receipt()
        result.update(selected_snapshot=self.selected, selected_images=self.image_receipts)
        return result


def read_journal(path: Path, node: int | None = None, group: dict | None = None,
                 images_dir: Path | None = None) -> SnapshotReplay:
    require(path.is_file() and not path.is_symlink() and 24 <= path.stat().st_size <= MAX_FILE,
            "journal regular file/size ceiling/header")
    replay = SnapshotReplay(images_dir or path.parent / "images", node, group)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        header = stream.read(24)
        digest.update(header)
        replay.journal_bytes = 24
        require(header[:8] == b"PLJRNL01" and header[8:20] == bytes(12) and
                crc32c(header[:20]) == int.from_bytes(header[20:24], "big"), "journal header identity/CRC")
        offset = 0
        while True:
            header = stream.read(32)
            if not header:
                break
            require(len(header) == 32, "partial journal entry header")
            digest.update(header)
            require(header[:8] == b"PLENTRY1" and crc32c(header[:28]) == int.from_bytes(header[28:32], "big"),
                    "journal entry header CRC")
            length, first, count, checksum = struct.unpack(">IQII", header[8:28])
            require(1 <= length <= MAX_OPERATION and first == offset and count == 1,
                    "operation journal length/offset/count")
            require(replay.journal_bytes + 32 + length <= MAX_FILE, "growing journal ceiling")
            payload = stream.read(length)
            digest.update(payload)
            replay.journal_bytes += 32 + len(payload)
            require(len(payload) == length and crc32c(payload) == checksum, "partial or corrupt journal payload")
            replay.apply(payload)
            offset += 1
    require(replay.group is not None, "missing group identity")
    replay.journal_sha256 = digest.hexdigest()
    return replay


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--node", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(crc32c(b"123456789") == 0xe3069283, "published Castagnoli vector")
    replay = read_journal(args.journal, args.node)
    result = replay.receipt()
    result.update(journal_sha256=replay.journal_sha256, journal_bytes=replay.journal_bytes)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()

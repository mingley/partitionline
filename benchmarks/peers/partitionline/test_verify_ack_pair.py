import csv
import json
from pathlib import Path
import tempfile
import unittest

import verify_ack_pair as gate
import fences


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.warmup, self.count, self.partitions, self.size = 3, 5, 2, 16
        self.rows = []
        self.receipts = []
        for phase, count in [("warmup", self.warmup), ("measure", self.count)]:
            for index in range(count):
                partition, offset = gate.expected_offset(phase, index, self.warmup, self.partitions)
                self.rows.append(["record", phase, str(index), str(partition), str(offset), "100"])
                key, value = gate.expected_record(index, self.size)
                self.receipts.append(dict(partition=partition, offset=offset, key=key.hex(),
                                          value=value.hex(), headers=0))
            self.rows.append(["phase_end", phase, str(count), "1000", "1"])
        self.rows.append(["complete"])
        self.config = [
            ["config", str(gate.SEED), str(self.warmup), str(self.count),
             str(self.partitions), str(self.size)],
            ["settings", "-1", "true", "0", "1000000", "1", "1", "1", "33554432", "30000", "1048576", "256"],
        ]
        self.fences = self.root / "fences.tsv"
        self.fences.write_text("0\t0\t5\n1\t0\t3\n")

    def write_csv(self, name, rows):
        path = self.root / name
        with path.open("w", newline="", encoding="utf-8") as output:
            csv.writer(output, lineterminator="\n").writerows(rows)
        return path

    def write_receipts(self, rows):
        path = self.root / "receipts.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        return path

    def verify(self, rows=None, receipts=None):
        trace = self.write_csv("ack.csv", self.config + (rows if rows is not None else self.rows))
        acks = gate.parse_trace(trace, "rust", self.warmup, self.count, self.partitions, self.size)
        fences = gate.verify_fences(self.fences, self.warmup, self.count, self.partitions)
        gate.verify_java_receipts(self.write_receipts(receipts if receipts is not None else self.receipts),
                                  acks, fences, self.warmup, self.count, self.partitions, self.size)

    def test_known_peer_record_bytes(self):
        key, payload = gate.expected_record(1, 16)
        self.assertEqual(key.hex(), "00000000000000011cc152e47d174d3c")
        self.assertEqual(payload.hex(), "67f1d8673433df28524167c44df711f8")

    def test_valid_warmup_and_measure_receipts(self):
        self.verify()

    def test_missing_or_duplicate_ack_is_rejected(self):
        self.verify()
        broken = [row[:] for row in self.rows]
        del broken[1]
        with self.assertRaises(ValueError):
            self.verify(rows=broken)
        broken = [row[:] for row in self.rows]
        broken[1] = broken[0][:]
        with self.assertRaises(ValueError):
            self.verify(rows=broken)

    def test_tampered_duplicate_or_missing_receipt_is_rejected(self):
        broken = [dict(row) for row in self.receipts]
        broken[0]["value"] = "00" * self.size
        with self.assertRaises(ValueError):
            self.verify(receipts=broken)
        broken = [dict(row) for row in self.receipts]
        broken[1] = dict(broken[0])
        with self.assertRaises(ValueError):
            self.verify(receipts=broken)
        with self.assertRaises(ValueError):
            self.verify(receipts=self.receipts[:-1])

    def test_incomplete_warmup_and_extra_headers_are_rejected(self):
        broken = [row[:] for row in self.rows]
        broken[self.warmup][2] = str(self.warmup - 1)
        with self.assertRaises(ValueError):
            self.verify(rows=broken)
        broken_receipts = [dict(row) for row in self.receipts]
        broken_receipts[0]["headers"] = 1
        with self.assertRaises(ValueError):
            self.verify(receipts=broken_receipts)

    def test_independent_offset_mismatch_is_rejected(self):
        self.fences.write_text("0\t0\t6\n1\t0\t3\n")
        with self.assertRaises(ValueError):
            self.verify()

    def test_fences_reject_old_or_missing_data(self):
        source = "topic:0:5\ntopic:1:3\n"
        self.assertEqual(fences.fences_from_offsets(source, "topic", 2, 3, 5),
                         "0\t0\t5\n1\t0\t3\n")
        with self.assertRaises(ValueError):
            fences.fences_from_offsets("topic:0:6\ntopic:1:3\n", "topic", 2, 3, 5)
        with self.assertRaises(ValueError):
            fences.fences_from_offsets("topic:0:5\n", "topic", 2, 3, 5)

    def test_c_callback_and_queue_checks_fail_closed(self):
        effective = {
            "request.required.acks": "-1", "enable.idempotence": "true",
            "queue.buffering.max.ms": "0", "batch.size": "1000000",
            "batch.num.messages": "1", "max.in.flight.requests.per.connection": "1",
            "queue.buffering.max.messages": "1000000", "queue.buffering.max.kbytes": "32768",
            "compression.codec": "none", "message.timeout.ms": "30000",
            "message.max.bytes": "1048576", "security.protocol": "plaintext",
            "allow.auto.create.topics": "false", "socket.nagle.disable": "true",
        }
        def phase(total):
            return dict(offered=total, accepted=total, acknowledged=total, rejected=0, timed_out=0,
                        unknown=0, callback_failures=0, queue_full_retries=0, max_outstanding=1,
                        elapsed_s=1.0, errors=[])
        raw = dict(client="librdkafka-native-c", version="2.15.0", effective_config=effective,
                   warmup=phase(self.warmup), timed=phase(self.count), broker_nodes=1,
                   durability=dict(verified=True, replication_factor=1, min_insync_replicas=1),
                   high_watermarks=dict(queried=True, total_offset_delta=self.count,
                       partitions=[dict(partition=0, start_offset=2, end_offset=5, offset_delta=3),
                                   dict(partition=1, start_offset=1, end_offset=3, offset_delta=2)]),
                   verification=dict(performed=True, verified_ids=self.count, duplicate_ids=0, bad_records=0))
        samples = self.write_csv("samples.csv", [["record_id", "latency_us"]]
                                 + [[str(index), "1.0"] for index in range(self.count)])
        path = self.root / "native.json"
        path.write_text(json.dumps(raw))
        gate.verify_c_raw(path, samples, self.warmup, self.count, self.partitions)
        raw["timed"]["queue_full_retries"] = 1
        path.write_text(json.dumps(raw))
        with self.assertRaises(ValueError):
            gate.verify_c_raw(path, samples, self.warmup, self.count, self.partitions)
        raw["timed"]["queue_full_retries"] = 0
        raw["timed"]["unknown"] = 1
        path.write_text(json.dumps(raw))
        with self.assertRaises(ValueError):
            gate.verify_c_raw(path, samples, self.warmup, self.count, self.partitions)


if __name__ == "__main__":
    unittest.main()

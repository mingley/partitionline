import csv
import json
from pathlib import Path
import tempfile
import unittest

import verify

class IndependentAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.trace = self.root / "trace.csv"
        self.fences = self.root / "fences.tsv"
        self.receipts = self.root / "receipts.jsonl"
        self.fences.write_text("0\t0\t4\n", encoding="utf-8")
        self.records = []
        for index in range(4):
            key, value = verify.expected_record(index, 100)
            self.records.append({"partition": 0, "offset": index, "key": key.hex(), "value": value.hex(), "headers": 0})
        self.write_receipts()
        self.rows = [["config", str(verify.SEED), "2", "2", "1", "100"],
                     ["settings", "-1", "true", "0", "1000000", "1", "1", "1", "33554432", "30000", "1048576", "540000"]]
        for phase, first in (("warmup", 0), ("measure", 2)):
            self.rows.extend([["record", phase, str(i), "0", str(i), "100"] for i in range(first, first + 2)])
            self.rows.append(["phase_end", phase, "2", "500", "1"])
        self.rows.append(["complete"])
        self.write_trace()

    def write_trace(self):
        with self.trace.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerows(self.rows)

    def write_receipts(self):
        self.receipts.write_text("".join(json.dumps(r) + "\n" for r in self.records), encoding="utf-8")

    def check_receipts(self):
        verify.verify_receipts(self.receipts, self.fences, 4, 100)

    def test_valid(self):
        samples = verify.verify_trace(self.trace, 2, 2, 100)
        self.check_receipts()
        self.assertEqual(verify.metrics(*samples["measure"])["samples"], 2)
        self.assertEqual(verify.expected_record(1, 16)[0].hex(), "00000000000000011cc152e47d174d3c")

    def test_duplicate_coordinate(self):
        self.records[3] = dict(self.records[2])
        self.write_receipts()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.check_receipts()

    def test_corrupt_value_and_header(self):
        self.records[1]["value"] = "00"
        self.write_receipts()
        with self.assertRaisesRegex(ValueError, "bytes"):
            self.check_receipts()
        self.records[1]["value"] = verify.expected_record(1, 100)[1].hex()
        self.records[1]["headers"] = 1
        self.write_receipts()
        with self.assertRaisesRegex(ValueError, "headers"):
            self.check_receipts()

    def test_broker_request_timeout_is_checked(self):
        cfg = {
            "request.required.acks": "-1", "enable.idempotence": "true",
            "queue.buffering.max.ms": "0", "batch.size": "1000000",
            "batch.num.messages": "1", "max.in.flight.requests.per.connection": "1",
            "queue.buffering.max.messages": "1000000", "queue.buffering.max.kbytes": "32768",
            "compression.codec": "none", "message.timeout.ms": "30000",
            "request.timeout.ms": "30000", "message.max.bytes": "1048576",
            "security.protocol": "plaintext", "allow.auto.create.topics": "false",
            "socket.nagle.disable": "true", "socket.timeout.ms": "30000",
            "socket.connection.setup.timeout.ms": "10000", "connections.max.idle.ms": "540000",
        }
        path = self.root / "c-config.json"
        path.write_text(json.dumps(cfg), encoding="utf-8")
        verify.verify_c_config(path)
        cfg["request.timeout.ms"] = "60000"
        path.write_text(json.dumps(cfg), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "request.timeout.ms"):
            verify.verify_c_config(path)

    def test_truncated_or_bad_ack_and_fence(self):
        self.rows[2][4] = "9"
        self.write_trace()
        with self.assertRaisesRegex(ValueError, "delivery"):
            verify.verify_trace(self.trace, 2, 2, 100)
        self.fences.write_text("0\t0\t3\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "fence"):
            self.check_receipts()

if __name__ == "__main__":
    unittest.main()

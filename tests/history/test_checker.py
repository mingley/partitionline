"""
Comprehensive tests for deterministic record-history checker (KL03-18).

Verifies:
  - Empty history fail-closed semantics (cannot pass required case, exits 2).
  - Unique ID, payload hash, and per-key/partition ordering checks (never cross-topic counts or offsets).
  - Duplicate and visibility rules for acks=0, acks=1, acks=-1/all, idempotence.
  - Transactions: read_committed vs read_uncommitted, aborted exposure, uncommitted exposure,
    control record exposure, transaction atomicity, non-atomic output/offset histories.
  - Share group delivery: KIP-932 accept/release/reject lifecycle and redelivery checks.
  - Synthetic missing/duplicate swaps (matching total counts with loss and duplicate).
  - Payload, key, topic, and partition corruption.
  - Minimal counterexample present on every deliberately invalid history.
  - CLI integration via subprocess (exit codes 0, 1, 2).

Standard library only.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check-record-history.py"

# Dynamically import scripts/check-record-history.py
spec = importlib.util.spec_from_file_location("check_record_history", str(SCRIPT_PATH))
if spec is None or spec.loader is None:
    raise ImportError(f"Cannot load module from {SCRIPT_PATH}")
checker = importlib.util.module_from_spec(spec)
sys.modules["check_record_history"] = checker
spec.loader.exec_module(checker)

HistoryValidationError = checker.HistoryValidationError
verify_history = checker.verify_history
verify_history_source = checker.verify_history_source


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class TestEmptyHistoryFailClosed(unittest.TestCase):
    """An empty history must not exit 0 as a pass of a required case."""

    def test_empty_string_fails(self):
        with self.assertRaises(HistoryValidationError) as ctx:
            verify_history_source("<test>", "")
        self.assertIn("empty", str(ctx.exception).lower())

    def test_whitespace_string_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history_source("<test>", "   \n\t  ")

    def test_empty_object_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history_source("<test>", "{}")

    def test_empty_list_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history_source("<test>", "[]")

    def test_missing_attempted_records_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history({
                "id": "missing-attempted",
                "consumed": [{"id": "r1", "topic": "t", "partition": 0}]
            })

    def test_empty_attempted_records_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history({
                "id": "empty-attempted",
                "attempted": [],
                "consumed": [{"id": "r1", "topic": "t", "partition": 0}]
            })

    def test_missing_consumed_records_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history({
                "id": "missing-consumed",
                "attempted": [{"id": "r1", "topic": "t", "partition": 0}]
            })

    def test_empty_consumed_records_fails(self):
        with self.assertRaises(HistoryValidationError):
            verify_history({
                "id": "empty-consumed",
                "attempted": [{"id": "r1", "topic": "t", "partition": 0}],
                "consumed": []
            })


class TestValidHistories(unittest.TestCase):
    """Valid histories across multiple delivery models and configurations."""

    def test_clean_single_partition_acks1(self):
        history = {
            "id": "valid-clean-single-partition",
            "config": {"acks": 1, "idempotent": False},
            "attempted": [
                {"id": "r1", "topic": "events", "partition": 0, "payload": "p1", "status": "acked"},
                {"id": "r2", "topic": "events", "partition": 0, "payload": "p2", "status": "acked"},
                {"id": "r3", "topic": "events", "partition": 0, "payload": "p3", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "events", "partition": 0, "offset": 0, "payload": "p1"},
                {"id": "r2", "topic": "events", "partition": 0, "offset": 1, "payload": "p2"},
                {"id": "r3", "topic": "events", "partition": 0, "offset": 2, "payload": "p3"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)
        self.assertEqual(len(res.violations), 0)
        self.assertIn("PASS", res.summary)

    def test_multi_partition_with_per_partition_order(self):
        history = {
            "id": "valid-multi-partition",
            "config": {"acks": -1, "idempotent": True},
            "attempted": [
                {"id": "p0-r1", "topic": "t", "partition": 0, "payload": "a", "status": "acked"},
                {"id": "p1-r1", "topic": "t", "partition": 1, "payload": "b", "status": "acked"},
                {"id": "p0-r2", "topic": "t", "partition": 0, "payload": "c", "status": "acked"},
                {"id": "p1-r2", "topic": "t", "partition": 1, "payload": "d", "status": "acked"},
            ],
            "consumed": [
                # Notice consumed order interleaves partitions, but per-partition order is strictly preserved
                {"id": "p1-r1", "topic": "t", "partition": 1, "offset": 0, "payload": "b"},
                {"id": "p0-r1", "topic": "t", "partition": 0, "offset": 0, "payload": "a"},
                {"id": "p1-r2", "topic": "t", "partition": 1, "offset": 1, "payload": "d"},
                {"id": "p0-r2", "topic": "t", "partition": 0, "offset": 1, "payload": "c"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)
        self.assertEqual(len(res.violations), 0)

    def test_cross_topic_order_independence(self):
        """Checker must not compare offsets or ordering across different topics."""
        history = {
            "id": "valid-cross-topic",
            "config": {"acks": 1},
            "attempted": [
                {"id": "topicA-r1", "topic": "topicA", "partition": 0, "payload": "x", "status": "acked"},
                {"id": "topicB-r1", "topic": "topicB", "partition": 0, "payload": "y", "status": "acked"},
            ],
            "consumed": [
                # topicB received first, even though topicA was attempted first
                {"id": "topicB-r1", "topic": "topicB", "partition": 0, "offset": 100, "payload": "y"},
                {"id": "topicA-r1", "topic": "topicA", "partition": 0, "offset": 5, "payload": "x"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)

    def test_valid_transactional_read_committed(self):
        history = {
            "id": "valid-txn-read-committed",
            "config": {"transactional": True, "isolation_level": "read_committed"},
            "transactions": [
                {"txn_id": "tx1", "status": "committed", "committed_offsets": {"in-0": 10}},
                {"txn_id": "tx2", "status": "aborted", "committed_offsets": {"in-0": 20}}
            ],
            "offset_commits": [
                {"topic": "in", "partition": 0, "offset": 10, "txn_id": "tx1"}
            ],
            "attempted": [
                {"id": "tx1-r1", "topic": "out", "partition": 0, "payload": "val1", "txn_id": "tx1", "status": "acked"},
                {"id": "tx1-r2", "topic": "out", "partition": 0, "payload": "val2", "txn_id": "tx1", "status": "acked"},
                {"id": "tx2-r1", "topic": "out", "partition": 0, "payload": "val3", "txn_id": "tx2", "status": "acked"},
            ],
            "consumed": [
                # Only tx1 records are visible; tx2 was aborted
                {"id": "tx1-r1", "topic": "out", "partition": 0, "offset": 0, "payload": "val1", "txn_id": "tx1"},
                {"id": "tx1-r2", "topic": "out", "partition": 0, "offset": 1, "payload": "val2", "txn_id": "tx1"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)

    def test_valid_transactional_read_uncommitted(self):
        """Under read_uncommitted, aborted records may appear."""
        history = {
            "id": "valid-txn-read-uncommitted",
            "config": {"transactional": True, "isolation_level": "read_uncommitted"},
            "transactions": [
                {"txn_id": "tx_abort", "status": "aborted"}
            ],
            "attempted": [
                {"id": "aborted-rec", "topic": "out", "partition": 0, "payload": "dirty", "txn_id": "tx_abort", "status": "acked"},
            ],
            "consumed": [
                {"id": "aborted-rec", "topic": "out", "partition": 0, "offset": 0, "payload": "dirty", "txn_id": "tx_abort"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)

    def test_valid_share_delivery_lifecycle(self):
        """KIP-932 share group delivery with accept, release, redelivery, accept, reject."""
        history = {
            "id": "valid-share-delivery",
            "config": {"delivery": "share"},
            "attempted": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "status": "acked"},
                {"id": "sh-2", "topic": "queue", "partition": 0, "payload": "m2", "status": "acked"},
                {"id": "sh-3", "topic": "queue", "partition": 0, "payload": "m3", "status": "acked"},
            ],
            "consumed": [
                # sh-1 is delivered and accepted
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "ack_type": "accept"},
                # sh-2 is delivered and released (transient error)
                {"id": "sh-2", "topic": "queue", "partition": 0, "payload": "m2", "ack_type": "release"},
                # sh-3 is delivered and rejected (dead lettered)
                {"id": "sh-3", "topic": "queue", "partition": 0, "payload": "m3", "ack_type": "reject"},
                # sh-2 is redelivered and this time accepted
                {"id": "sh-2", "topic": "queue", "partition": 0, "payload": "m2", "ack_type": "accept"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)

    def test_valid_acks0_loss_tolerated(self):
        """In acks=0 fire-and-forget, lost records are tolerated, but consumed records must be uncorrupted."""
        history = {
            "id": "valid-acks0",
            "config": {"acks": 0},
            "attempted": [
                {"id": "r1", "topic": "telemetry", "partition": 0, "payload": "d1", "status": "attempted"},
                {"id": "r2", "topic": "telemetry", "partition": 0, "payload": "d2", "status": "attempted"},
                {"id": "r3", "topic": "telemetry", "partition": 0, "payload": "d3", "status": "attempted"},
            ],
            "consumed": [
                # r2 was dropped in flight by broker/network; r1 and r3 received
                {"id": "r1", "topic": "telemetry", "partition": 0, "offset": 0, "payload": "d1"},
                {"id": "r3", "topic": "telemetry", "partition": 0, "offset": 2, "payload": "d3"},
            ]
        }
        res = verify_history(history)
        self.assertTrue(res.valid)


class TestInvalidHistoriesFailWithCounterexample(unittest.TestCase):
    """Every deliberately invalid history must fail with a minimal counterexample."""

    def test_synthetic_missing_duplicate_swap_fails(self):
        """Total count matches (3 produced, 3 consumed), but r3 is lost and r2 is duplicated!"""
        history = {
            "id": "invalid-synthetic-swap",
            "config": {"acks": 1, "idempotent": True},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "val1", "status": "acked"},
                {"id": "r2", "topic": "t", "partition": 0, "payload": "val2", "status": "acked"},
                {"id": "r3", "topic": "t", "partition": 0, "payload": "val3", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "val1"},
                {"id": "r2", "topic": "t", "partition": 0, "offset": 1, "payload": "val2"},
                # Synthetic swap: r2 duplicated instead of r3!
                {"id": "r2", "topic": "t", "partition": 0, "offset": 2, "payload": "val2"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertIsNotNone(res.minimal_counterexample)
        self.assertEqual(res.minimal_counterexample["type"], "SYNTHETIC_SWAP")
        details = res.minimal_counterexample["details"]
        self.assertEqual(details["missing_record_ids"], ["r3"])
        self.assertEqual(details["duplicated_record_ids"], ["r2"])
        self.assertEqual(details["attempted_count"], 3)
        self.assertEqual(details["consumed_count"], 3)

    def test_payload_corruption_fails(self):
        """Consumed payload does not match attempted payload."""
        history = {
            "id": "invalid-payload-corruption",
            "config": {"acks": 1},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "good_payload", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "tampered_payload"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "PAYLOAD_CORRUPTION")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])
        details = res.minimal_counterexample["details"]
        self.assertEqual(details["expected_payload_hash"], _sha256("good_payload"))
        self.assertEqual(details["actual_payload_hash"], _sha256("tampered_payload"))

    def test_key_corruption_fails(self):
        history = {
            "id": "invalid-key-corruption",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "key": "k_orig", "payload": "p", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "key": "k_altered", "payload": "p"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "KEY_CORRUPTION")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])

    def test_topic_corruption_fails(self):
        history = {
            "id": "invalid-topic-corruption",
            "attempted": [
                {"id": "r1", "topic": "orders", "partition": 0, "payload": "p", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "payments", "partition": 0, "payload": "p"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "TOPIC_CORRUPTION")

    def test_partition_corruption_fails(self):
        history = {
            "id": "invalid-partition-corruption",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 1, "payload": "p"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "PARTITION_CORRUPTION")

    def test_phantom_record_fails(self):
        """Consumed record that was never produced."""
        history = {
            "id": "invalid-phantom",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
                {"id": "phantom-x", "topic": "t", "partition": 0, "offset": 1, "payload": "ghost"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "PHANTOM_RECORD")
        self.assertIn("phantom-x", res.minimal_counterexample["record_ids"])

    def test_failed_record_exposure_fails(self):
        """Record marked failed by producer must not be consumed."""
        history = {
            "id": "invalid-failed-exposure",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "failed"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "FAILED_RECORD_EXPOSURE")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])

    def test_acks0_cannot_claim_acked_status(self):
        """Acks=0 cannot be labeled acknowledged throughput."""
        history = {
            "id": "invalid-acks0-claim",
            "config": {"acks": 0},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "INVALID_ACKS_MODE")

    def test_data_loss_under_acks1_fails(self):
        """Acked record is missing from consumed output."""
        history = {
            "id": "invalid-data-loss",
            "config": {"acks": 1},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
                {"id": "r2", "topic": "t", "partition": 0, "payload": "p2", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "DATA_LOSS")
        self.assertIn("r2", res.minimal_counterexample["record_ids"])

    def test_unexpected_duplicate_under_idempotence_fails(self):
        """Idempotence forbids duplicate deliveries."""
        history = {
            "id": "invalid-dup-idempotent",
            "config": {"acks": -1, "idempotent": True},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
                {"id": "r1", "topic": "t", "partition": 0, "offset": 1, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "UNEXPECTED_DUPLICATE")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])

    def test_unexpected_duplicate_without_retry_fails(self):
        """Even non-idempotent producer must not duplicate cleanly acked record without ambiguous retry."""
        history = {
            "id": "invalid-dup-no-retry",
            "config": {"acks": 1, "idempotent": False},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
                {"id": "r1", "topic": "t", "partition": 0, "offset": 1, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "UNEXPECTED_DUPLICATE")

    def test_partition_ordering_violation_fails(self):
        """Records on partition 0 produced in order r1 then r2, but consumed r2 then r1."""
        history = {
            "id": "invalid-partition-order",
            "config": {"acks": 1, "idempotent": True},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
                {"id": "r2", "topic": "t", "partition": 0, "payload": "p2", "status": "acked"},
            ],
            "consumed": [
                {"id": "r2", "topic": "t", "partition": 0, "offset": 0, "payload": "p2"},
                {"id": "r1", "topic": "t", "partition": 0, "offset": 1, "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "PARTITION_ORDERING_VIOLATION")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])
        self.assertIn("r2", res.minimal_counterexample["record_ids"])

    def test_non_monotonic_offset_fails(self):
        """Offsets within partition must strictly increase."""
        history = {
            "id": "invalid-offset-order",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
                {"id": "r2", "topic": "t", "partition": 0, "payload": "p2", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 10, "payload": "p1"},
                {"id": "r2", "topic": "t", "partition": 0, "offset": 5, "payload": "p2"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "OFFSET_ORDERING_VIOLATION")

    def test_key_ordering_violation_fails(self):
        """Records for same key must preserve produce order."""
        history = {
            "id": "invalid-key-order",
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "key": "k1", "payload": "p1", "status": "acked"},
                {"id": "r2", "topic": "t", "partition": 0, "key": "k1", "payload": "p2", "status": "acked"},
            ],
            "consumed": [
                {"id": "r2", "topic": "t", "partition": 0, "offset": 0, "key": "k1", "payload": "p2"},
                {"id": "r1", "topic": "t", "partition": 0, "offset": 1, "key": "k1", "payload": "p1"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        # Violates both partition order and key order; counterexample must be explanatory
        violation_types = [v.type for v in res.violations]
        self.assertTrue("KEY_ORDERING_VIOLATION" in violation_types or "PARTITION_ORDERING_VIOLATION" in violation_types)

    def test_aborted_transaction_exposure_fails(self):
        """Records from an aborted transaction must not be exposed to read_committed consumer."""
        history = {
            "id": "invalid-aborted-exposure",
            "config": {"transactional": True, "isolation_level": "read_committed"},
            "transactions": [
                {"txn_id": "tx_fail", "status": "aborted"}
            ],
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "aborted_data", "txn_id": "tx_fail", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "aborted_data", "txn_id": "tx_fail"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "ABORTED_EXPOSURE")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])

    def test_uncommitted_transaction_exposure_fails(self):
        """Records from an open/uncommitted transaction must not be exposed under read_committed."""
        history = {
            "id": "invalid-uncommitted-exposure",
            "config": {"transactional": True, "isolation_level": "read_committed"},
            "transactions": [
                {"txn_id": "tx_open", "status": "open"}
            ],
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "uncommitted_data", "txn_id": "tx_open", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "uncommitted_data", "txn_id": "tx_open"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "UNCOMMITTED_EXPOSURE")
        self.assertIn("r1", res.minimal_counterexample["record_ids"])

    def test_control_record_exposure_fails(self):
        """Control markers (e.g. COMMIT / ABORT) must never be emitted as application records."""
        history = {
            "id": "invalid-control-record-exposure",
            "config": {"transactional": True},
            "attempted": [
                {"id": "r1", "topic": "t", "partition": 0, "payload": "app_record", "status": "acked"},
            ],
            "consumed": [
                {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "app_record"},
                {"id": "ctrl-commit", "topic": "t", "partition": 0, "offset": 1, "is_control_record": True},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        violation_types = [v.type for v in res.violations]
        self.assertIn("CONTROL_RECORD_EXPOSURE", violation_types)

    def test_non_atomic_transaction_output_fails(self):
        """Partial delivery of committed transaction records violates atomicity."""
        history = {
            "id": "invalid-non-atomic-txn",
            "config": {"transactional": True, "isolation_level": "read_committed"},
            "transactions": [
                {"txn_id": "tx_all_or_nothing", "status": "committed"}
            ],
            "attempted": [
                {"id": "tx-r1", "topic": "t", "partition": 0, "payload": "p1", "txn_id": "tx_all_or_nothing", "status": "acked"},
                {"id": "tx-r2", "topic": "t", "partition": 0, "payload": "p2", "txn_id": "tx_all_or_nothing", "status": "acked"},
                {"id": "tx-r3", "topic": "t", "partition": 0, "payload": "p3", "txn_id": "tx_all_or_nothing", "status": "acked"},
            ],
            "consumed": [
                # tx-r3 is missing!
                {"id": "tx-r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1", "txn_id": "tx_all_or_nothing"},
                {"id": "tx-r2", "topic": "t", "partition": 0, "offset": 1, "payload": "p2", "txn_id": "tx_all_or_nothing"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        violation_types = [v.type for v in res.violations]
        self.assertIn("NON_ATOMIC_TRANSACTION", violation_types)

    def test_non_atomic_output_offset_history_fails(self):
        """Output records consumed, but declared input offset was never committed."""
        history = {
            "id": "invalid-non-atomic-output-offset",
            "config": {"transactional": True, "isolation_level": "read_committed"},
            "transactions": [
                {"txn_id": "tx_rpw", "status": "committed", "committed_offsets": {"in_topic-0": 50}}
            ],
            # offset_commits is missing the commit for in_topic-0: 50!
            "offset_commits": [
                {"topic": "in_topic", "partition": 0, "offset": 20, "txn_id": "other_tx"}
            ],
            "attempted": [
                {"id": "out-r1", "topic": "out_topic", "partition": 0, "payload": "out1", "txn_id": "tx_rpw", "status": "acked"},
            ],
            "consumed": [
                {"id": "out-r1", "topic": "out_topic", "partition": 0, "offset": 0, "payload": "out1", "txn_id": "tx_rpw"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        violation_types = [v.type for v in res.violations]
        self.assertIn("NON_ATOMIC_OFFSET_OUTPUT", violation_types)

    def test_offsets_committed_for_aborted_transaction_fails(self):
        """Offsets must not be committed when transaction aborts."""
        history = {
            "id": "invalid-aborted-offset-commit",
            "config": {"transactional": True},
            "transactions": [
                {"txn_id": "tx_abort_offsets", "status": "aborted"}
            ],
            "offset_commits": [
                {"topic": "in_topic", "partition": 0, "offset": 99, "txn_id": "tx_abort_offsets"}
            ],
            "attempted": [
                {"id": "dummy", "topic": "t", "partition": 0, "payload": "d", "status": "acked"},
            ],
            "consumed": [
                {"id": "dummy", "topic": "t", "partition": 0, "offset": 0, "payload": "d"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        violation_types = [v.type for v in res.violations]
        self.assertIn("NON_ATOMIC_OFFSET_OUTPUT", violation_types)

    def test_share_delivery_redelivery_after_accept_fails(self):
        """Share record must not be redelivered after being ACCEPTED."""
        history = {
            "id": "invalid-share-after-accept",
            "config": {"delivery": "share"},
            "attempted": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "status": "acked"},
            ],
            "consumed": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "ack_type": "accept"},
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "ack_type": "accept"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "SHARE_ACK_VIOLATION")
        self.assertIn("ACCEPTED", res.minimal_counterexample["message"])

    def test_share_delivery_redelivery_after_reject_fails(self):
        """Share record must not be redelivered after being REJECTED."""
        history = {
            "id": "invalid-share-after-reject",
            "config": {"delivery": "share"},
            "attempted": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "poison", "status": "acked"},
            ],
            "consumed": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "poison", "ack_type": "reject"},
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "poison", "ack_type": "accept"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "SHARE_ACK_VIOLATION")
        self.assertIn("REJECTED", res.minimal_counterexample["message"])

    def test_share_delivery_without_release_fails(self):
        """Share record delivered twice without intermediate release."""
        history = {
            "id": "invalid-share-no-release",
            "config": {"delivery": "share"},
            "attempted": [
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "status": "acked"},
            ],
            "consumed": [
                # Delivered without ack_type specified (in-flight)
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1"},
                # Delivered again without previous release
                {"id": "sh-1", "topic": "queue", "partition": 0, "payload": "m1", "ack_type": "accept"},
            ]
        }
        res = verify_history(history)
        self.assertFalse(res.valid)
        self.assertEqual(res.minimal_counterexample["type"], "SHARE_ACK_VIOLATION")


class TestCLIIntegration(unittest.TestCase):
    """Subprocess CLI execution tests for scripts/check-record-history.py."""

    def test_cli_valid_file_exits_0(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({
                "id": "cli-valid",
                "attempted": [{"id": "r1", "topic": "t", "partition": 0, "payload": "p", "status": "acked"}],
                "consumed": [{"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p"}]
            }, f)
            temp_path = f.name

        try:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), temp_path],
                capture_output=True,
                text=True
            )
            self.assertEqual(proc.returncode, 0, f"Expected rc=0, got {proc.returncode}: {proc.stderr}")
            self.assertIn("PASS", proc.stdout)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_cli_valid_stdin_exits_0(self):
        payload = json.dumps({
            "id": "cli-stdin-valid",
            "attempted": [{"id": "r1", "topic": "t", "partition": 0, "payload": "p", "status": "acked"}],
            "consumed": [{"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p"}]
        })
        proc = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "-"],
            input=payload,
            capture_output=True,
            text=True
        )
        self.assertEqual(proc.returncode, 0, f"Expected rc=0, got {proc.returncode}: {proc.stderr}")

    def test_cli_invalid_file_exits_1(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({
                "id": "cli-invalid",
                "attempted": [
                    {"id": "r1", "topic": "t", "partition": 0, "payload": "orig", "status": "acked"}
                ],
                "consumed": [
                    {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "tampered"}
                ]
            }, f)
            temp_path = f.name

        try:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), temp_path],
                capture_output=True,
                text=True
            )
            self.assertEqual(proc.returncode, 1, f"Expected rc=1, got {proc.returncode}")
            self.assertIn("PAYLOAD_CORRUPTION", proc.stdout)
            self.assertIn("Minimal Counterexample", proc.stdout)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_cli_json_flag(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({
                "id": "cli-json",
                "attempted": [
                    {"id": "r1", "topic": "t", "partition": 0, "payload": "p1", "status": "acked"},
                    {"id": "r2", "topic": "t", "partition": 0, "payload": "p2", "status": "acked"},
                ],
                "consumed": [
                    {"id": "r1", "topic": "t", "partition": 0, "offset": 0, "payload": "p1"},
                    {"id": "r1", "topic": "t", "partition": 0, "offset": 1, "payload": "p1"},
                ]
            }, f)
            temp_path = f.name

        try:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT_PATH), "--json", temp_path],
                capture_output=True,
                text=True
            )
            self.assertEqual(proc.returncode, 1)
            parsed = json.loads(proc.stdout)
            self.assertFalse(parsed["all_valid"])
            self.assertEqual(parsed["failed"], 1)
            first_res = parsed["results"][0]
            self.assertIsNotNone(first_res["minimal_counterexample"])
            self.assertEqual(first_res["minimal_counterexample"]["type"], "SYNTHETIC_SWAP")
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_cli_empty_input_exits_2_fail_closed(self):
        """Empty input must not exit 0."""
        proc = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "-"],
            input="",
            capture_output=True,
            text=True
        )
        self.assertEqual(proc.returncode, 2, f"Expected rc=2 on empty stdin, got {proc.returncode}")
        self.assertIn("Empty input", proc.stderr)

    def test_cli_empty_json_object_exits_2_fail_closed(self):
        proc = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "-"],
            input="{}",
            capture_output=True,
            text=True
        )
        self.assertEqual(proc.returncode, 2, f"Expected rc=2 on empty json object, got {proc.returncode}")

    def test_cli_missing_file_exits_2(self):
        proc = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "/path/to/nonexistent/history.json"],
            capture_output=True,
            text=True
        )
        self.assertEqual(proc.returncode, 2, f"Expected rc=2 on missing file, got {proc.returncode}")


if __name__ == "__main__":
    unittest.main()

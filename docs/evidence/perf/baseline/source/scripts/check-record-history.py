#!/usr/bin/env python3
"""
Deterministic Record-History Correctness Checker (KL03-18).

Validates attempted/accepted/acked/failed/ambiguous records against independently
consumed output in a fail-closed manner:
  1. Compares unique IDs, payload hashes, and per-key/partition order (never counts or cross-topic offsets).
  2. Applies separate duplicate/visibility rules for acks modes (0, 1, -1/all), idempotence,
     transactions (read_committed vs read_uncommitted), and share delivery (KIP-932).
  3. Detects synthetic missing/duplicate swaps, corruption, aborted exposure, control record
     exposure, and non-atomic output/offset histories.
  4. Requires every deliberately invalid history to fail with a minimal explanatory counterexample.
  5. Enforces that an empty history must not exit 0 as a pass of a required case.

Python standard library only.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class HistoryValidationError(Exception):
    """Raised when history structure, schema, or inputs are invalid or empty (fail-closed)."""
    pass


@dataclass
class Violation:
    """Minimal counterexample of a correctness or contract violation."""
    type: str
    message: str
    record_ids: List[str] = field(default_factory=list)
    topic: Optional[str] = None
    partition: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "message": self.message,
            "record_ids": self.record_ids,
            "topic": self.topic,
            "partition": self.partition,
            "details": self.details,
        }


@dataclass
class CheckResult:
    """Outcome of validating a single record history."""
    history_id: str
    valid: bool
    attempted_count: int
    consumed_count: int
    violations: List[Violation] = field(default_factory=list)
    minimal_counterexample: Optional[Dict[str, Any]] = None
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "history_id": self.history_id,
            "valid": self.valid,
            "attempted_count": self.attempted_count,
            "consumed_count": self.consumed_count,
            "violations": [v.to_dict() for v in self.violations],
            "minimal_counterexample": self.minimal_counterexample,
            "summary": self.summary,
        }


@dataclass
class Record:
    """Represents an attempted or consumed record."""
    id: str
    topic: str
    partition: int
    offset: Optional[int] = None
    key: Optional[str] = None
    payload: Optional[str] = None
    payload_hash: str = ""
    status: str = "acked"  # attempted, accepted, acked, failed, ambiguous
    txn_id: Optional[str] = None
    is_control_record: bool = False
    delivery_attempt: Optional[int] = None
    ack_type: Optional[str] = None  # accept, release, reject
    attempt_index: Optional[int] = None

    def __post_init__(self):
        self.id = str(self.id)
        self.topic = str(self.topic)
        self.partition = int(self.partition)
        if self.offset is not None:
            self.offset = int(self.offset)
        if self.key is not None:
            self.key = str(self.key)
        if self.payload is not None and not isinstance(self.payload, str):
            self.payload = str(self.payload)
        if not self.payload_hash:
            if self.payload is not None:
                self.payload_hash = hashlib.sha256(self.payload.encode("utf-8")).hexdigest()
            else:
                self.payload_hash = hashlib.sha256(b"").hexdigest()
        self.status = str(self.status).lower()
        if self.txn_id is not None:
            self.txn_id = str(self.txn_id)
        if self.ack_type is not None:
            self.ack_type = str(self.ack_type).lower()


@dataclass
class HistoryConfig:
    """Execution configuration and semantic contract under test."""
    acks: Union[int, str] = 1  # 0, 1, -1, "all"
    idempotent: bool = False
    transactional: bool = False
    isolation_level: str = "read_committed"  # read_committed, read_uncommitted
    delivery: str = "partition"  # partition, share

    def __post_init__(self):
        # Normalize acks
        if isinstance(self.acks, str):
            if self.acks.lower() in ("all", "-1"):
                self.acks = -1
            elif self.acks in ("0", "1"):
                self.acks = int(self.acks)
            else:
                raise HistoryValidationError(f"Invalid acks setting: {self.acks}")
        elif isinstance(self.acks, int):
            if self.acks not in (0, 1, -1):
                raise HistoryValidationError(f"Invalid acks integer: {self.acks}")
        else:
            raise HistoryValidationError(f"Unsupported acks type: {type(self.acks)}")

        self.isolation_level = str(self.isolation_level).lower()
        if self.isolation_level not in ("read_committed", "read_uncommitted"):
            raise HistoryValidationError(f"Invalid isolation_level: {self.isolation_level}")

        self.delivery = str(self.delivery).lower()
        if self.delivery not in ("partition", "share"):
            raise HistoryValidationError(f"Invalid delivery mode: {self.delivery}")


def _parse_record(raw: Any, record_type: str, index: int) -> Record:
    """Parse and validate a record dict."""
    if not isinstance(raw, dict):
        raise HistoryValidationError(f"{record_type} record at index {index} must be an object")

    if "id" not in raw or raw["id"] is None or str(raw["id"]).strip() == "":
        raise HistoryValidationError(f"{record_type} record at index {index} missing 'id'")

    if "topic" not in raw or not raw["topic"]:
        raise HistoryValidationError(f"{record_type} record '{raw.get('id')}' missing 'topic'")

    if "partition" not in raw or raw["partition"] is None:
        raise HistoryValidationError(f"{record_type} record '{raw.get('id')}' missing 'partition'")

    try:
        partition = int(raw["partition"])
        if partition < 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise HistoryValidationError(f"{record_type} record '{raw.get('id')}' invalid partition: {raw.get('partition')}")

    offset = None
    if "offset" in raw and raw["offset"] is not None:
        try:
            offset = int(raw["offset"])
            if offset < 0:
                raise ValueError()
        except (ValueError, TypeError):
            raise HistoryValidationError(f"{record_type} record '{raw.get('id')}' invalid offset: {raw.get('offset')}")

    status = raw.get("status", "acked")
    valid_statuses = {"attempted", "accepted", "acked", "failed", "ambiguous"}
    if str(status).lower() not in valid_statuses:
        raise HistoryValidationError(f"{record_type} record '{raw.get('id')}' invalid status '{status}'")

    return Record(
        id=raw["id"],
        topic=raw["topic"],
        partition=partition,
        offset=offset,
        key=raw.get("key"),
        payload=raw.get("payload"),
        payload_hash=raw.get("payload_hash", ""),
        status=status,
        txn_id=raw.get("txn_id"),
        is_control_record=bool(raw.get("is_control_record", False)),
        delivery_attempt=raw.get("delivery_attempt"),
        ack_type=raw.get("ack_type"),
        attempt_index=raw.get("attempt_index", index),
    )


def parse_history(raw_data: Any, history_id_default: str = "history") -> Tuple[str, HistoryConfig, List[Record], List[Record], Dict[str, Any], List[Dict[str, Any]]]:
    """
    Parses and validates the raw history dictionary.
    Rejects empty or missing attempted/consumed records fail-closed.
    """
    if raw_data is None:
        raise HistoryValidationError("History data is null")

    if not isinstance(raw_data, dict):
        raise HistoryValidationError("History root must be a JSON object")

    if not raw_data:
        raise HistoryValidationError("Empty history object {}: an empty history must not pass")

    history_id = str(raw_data.get("id") or raw_data.get("history_id") or history_id_default)

    # Extract config
    cfg_raw = raw_data.get("config", {})
    if not isinstance(cfg_raw, dict):
        raise HistoryValidationError("'config' must be an object if present")

    acks = cfg_raw.get("acks", raw_data.get("acks", 1))
    idempotent = bool(cfg_raw.get("idempotent", raw_data.get("idempotent", raw_data.get("idempotence", False))))
    transactional = bool(cfg_raw.get("transactional", raw_data.get("transactional", False)))
    isolation_level = cfg_raw.get("isolation_level", raw_data.get("isolation_level", "read_committed"))
    delivery = cfg_raw.get("delivery", raw_data.get("delivery", "partition"))

    config = HistoryConfig(
        acks=acks,
        idempotent=idempotent,
        transactional=transactional,
        isolation_level=isolation_level,
        delivery=delivery,
    )

    # Extract attempted
    attempted_raw = raw_data.get("attempted")
    if attempted_raw is None:
        attempted_raw = raw_data.get("produced") or raw_data.get("records_produced") or raw_data.get("attempted_records")
    if attempted_raw is None:
        raise HistoryValidationError("History must contain 'attempted' (or 'produced') records list")
    if not isinstance(attempted_raw, list):
        raise HistoryValidationError("'attempted' records must be a list")
    if len(attempted_raw) == 0:
        raise HistoryValidationError("Empty attempted records: an empty history must not exit 0")

    # Extract consumed
    consumed_raw = raw_data.get("consumed")
    if consumed_raw is None:
        consumed_raw = raw_data.get("output") or raw_data.get("records_consumed") or raw_data.get("consumed_records")
    if consumed_raw is None:
        raise HistoryValidationError("History must contain 'consumed' (or 'output') records list")
    if not isinstance(consumed_raw, list):
        raise HistoryValidationError("'consumed' records must be a list")
    if len(consumed_raw) == 0:
        raise HistoryValidationError("Empty consumed records: an empty history must not exit 0")

    attempted_records = [_parse_record(r, "attempted", idx) for idx, r in enumerate(attempted_raw)]
    consumed_records = [_parse_record(r, "consumed", idx) for idx, r in enumerate(consumed_raw)]

    # Transactions metadata
    transactions_raw = raw_data.get("transactions", raw_data.get("txns", []))
    if not isinstance(transactions_raw, list):
        raise HistoryValidationError("'transactions' must be a list if present")

    txn_map: Dict[str, Any] = {}
    for txn_item in transactions_raw:
        if isinstance(txn_item, dict) and "txn_id" in txn_item:
            txn_map[str(txn_item["txn_id"])] = txn_item

    offset_commits = raw_data.get("offset_commits", [])
    if not isinstance(offset_commits, list):
        raise HistoryValidationError("'offset_commits' must be a list if present")

    return history_id, config, attempted_records, consumed_records, txn_map, offset_commits


def verify_history(raw_data: Any, history_id_default: str = "history") -> CheckResult:
    """
    Deterministically verifies record history against client correctness contracts:
      - Unique IDs, payload hashes, and per-key/partition order (never counts or cross-topic offsets).
      - Duplicate/visibility rules for acks modes, idempotence, transactions, and share delivery.
      - Detects synthetic missing/duplicate swaps, corruption, aborted exposure, non-atomic histories.
      - Produces minimal explanatory counterexamples for any failure.
    """
    history_id, config, attempted, consumed, txn_map, offset_commits = parse_history(raw_data, history_id_default)

    violations: List[Violation] = []

    # Index attempted records by ID
    attempted_by_id: Dict[str, List[Record]] = defaultdict(list)
    for r in attempted:
        attempted_by_id[r.id].append(r)

    # 1. Acks mode rules
    if config.acks == 0:
        for r in attempted:
            if r.status == "acked":
                violations.append(Violation(
                    type="INVALID_ACKS_MODE",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Acks=0 is not acknowledged throughput; record '{r.id}' cannot be marked 'acked'",
                    details={"record_id": r.id, "acks": 0, "status": r.status},
                ))

    # 2. Failed records exposure
    failed_ids = {r.id for r in attempted if r.status == "failed"}
    for r in consumed:
        if r.id in failed_ids:
            violations.append(Violation(
                type="FAILED_RECORD_EXPOSURE",
                record_ids=[r.id],
                topic=r.topic,
                partition=r.partition,
                message=f"Record '{r.id}' failed during produce but was consumed on {r.topic}-{r.partition}",
                details={"record_id": r.id, "topic": r.topic, "partition": r.partition, "offset": r.offset},
            ))

    # 3. Phantom / unattempted records
    for r in consumed:
        if r.id not in attempted_by_id:
            violations.append(Violation(
                type="PHANTOM_RECORD",
                record_ids=[r.id],
                topic=r.topic,
                partition=r.partition,
                message=f"Consumed record '{r.id}' was never attempted by a producer",
                details={"record_id": r.id, "topic": r.topic, "partition": r.partition, "offset": r.offset},
            ))

    # 4. Payload and metadata corruption
    for r in consumed:
        if r.id in attempted_by_id:
            expected = attempted_by_id[r.id][0]
            if r.topic != expected.topic:
                violations.append(Violation(
                    type="TOPIC_CORRUPTION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Topic corruption for record '{r.id}': expected '{expected.topic}', got '{r.topic}'",
                    details={"record_id": r.id, "expected_topic": expected.topic, "actual_topic": r.topic},
                ))
            if r.partition != expected.partition:
                violations.append(Violation(
                    type="PARTITION_CORRUPTION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Partition corruption for record '{r.id}': expected partition {expected.partition}, got {r.partition}",
                    details={"record_id": r.id, "expected_partition": expected.partition, "actual_partition": r.partition},
                ))
            if r.key != expected.key:
                violations.append(Violation(
                    type="KEY_CORRUPTION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Key corruption for record '{r.id}': expected key '{expected.key}', got '{r.key}'",
                    details={"record_id": r.id, "expected_key": expected.key, "actual_key": r.key},
                ))
            if r.payload_hash != expected.payload_hash:
                violations.append(Violation(
                    type="PAYLOAD_CORRUPTION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Payload corruption for record '{r.id}': expected hash '{expected.payload_hash}', got '{r.payload_hash}'",
                    details={
                        "record_id": r.id,
                        "expected_payload_hash": expected.payload_hash,
                        "actual_payload_hash": r.payload_hash,
                        "offset": r.offset,
                    },
                ))

    # 5. Control record exposure
    for r in consumed:
        if r.is_control_record:
            violations.append(Violation(
                type="CONTROL_RECORD_EXPOSURE",
                record_ids=[r.id],
                topic=r.topic,
                partition=r.partition,
                message=f"Control record '{r.id}' was exposed to application consumer on {r.topic}-{r.partition}",
                details={"record_id": r.id, "topic": r.topic, "partition": r.partition, "offset": r.offset},
            ))

    # 6. Transactional Isolation (Aborted & Uncommitted exposure)
    for r in consumed:
        txn_id = r.txn_id or (attempted_by_id[r.id][0].txn_id if r.id in attempted_by_id else None)
        if txn_id:
            txn_info = txn_map.get(txn_id, {})
            txn_status = txn_info.get("status", "open").lower()
            if config.isolation_level == "read_committed":
                if txn_status == "aborted":
                    violations.append(Violation(
                        type="ABORTED_EXPOSURE",
                        record_ids=[r.id],
                        topic=r.topic,
                        partition=r.partition,
                        message=f"Record '{r.id}' from aborted transaction '{txn_id}' exposed under read_committed",
                        details={"record_id": r.id, "txn_id": txn_id, "offset": r.offset},
                    ))
                elif txn_status == "open":
                    violations.append(Violation(
                        type="UNCOMMITTED_EXPOSURE",
                        record_ids=[r.id],
                        topic=r.topic,
                        partition=r.partition,
                        message=f"Record '{r.id}' from uncommitted transaction '{txn_id}' exposed under read_committed",
                        details={"record_id": r.id, "txn_id": txn_id, "offset": r.offset},
                    ))

    # 7. Transactional Atomicity (All-or-Nothing per transaction)
    all_txn_ids = set(txn_map.keys()) | {r.txn_id for r in attempted if r.txn_id}
    for txn_id in all_txn_ids:
        txn_info = txn_map.get(txn_id, {})
        txn_status = txn_info.get("status", "committed").lower()
        if txn_status == "committed":
            produced_in_txn = {r.id for r in attempted if r.txn_id == txn_id}
            if produced_in_txn:
                consumed_in_txn = {r.id for r in consumed if (r.txn_id == txn_id or (r.id in attempted_by_id and attempted_by_id[r.id][0].txn_id == txn_id))}
                missing_in_txn = produced_in_txn - consumed_in_txn
                if consumed_in_txn and missing_in_txn:
                    violations.append(Violation(
                        type="NON_ATOMIC_TRANSACTION",
                        record_ids=sorted(list(missing_in_txn)),
                        message=f"Committed transaction '{txn_id}' violated atomicity: {len(consumed_in_txn)} records consumed, but {len(missing_in_txn)} missing",
                        details={
                            "txn_id": txn_id,
                            "consumed_record_ids": sorted(list(consumed_in_txn)),
                            "missing_record_ids": sorted(list(missing_in_txn)),
                        },
                    ))

    # 8. Non-atomic output / offset histories
    for txn_id, txn_info in txn_map.items():
        txn_status = txn_info.get("status", "open").lower()
        committed_offsets = txn_info.get("committed_offsets", {})
        consumed_txn_recs = [r for r in consumed if (r.txn_id == txn_id or (r.id in attempted_by_id and attempted_by_id[r.id][0].txn_id == txn_id))]
        
        if txn_status == "committed":
            if consumed_txn_recs and committed_offsets and offset_commits:
                # Check that declared committed offsets match recorded offset_commits
                for tp_key, expected_offset in committed_offsets.items():
                    if "-" in tp_key:
                        t, p = tp_key.rsplit("-", 1)
                    elif ":" in tp_key:
                        t, p = tp_key.split(":")
                    else:
                        continue
                    p_num = int(p)
                    matching = [oc for oc in offset_commits if oc.get("topic") == t and oc.get("partition") == p_num and oc.get("offset") == expected_offset]
                    if not matching:
                        violations.append(Violation(
                            type="NON_ATOMIC_OFFSET_OUTPUT",
                            record_ids=[r.id for r in consumed_txn_recs],
                            message=f"Non-atomic output/offset: transaction '{txn_id}' consumed output records, but input offset for {t}-{p_num} was not committed",
                            details={"txn_id": txn_id, "topic": t, "partition": p_num, "expected_offset": expected_offset},
                        ))
        elif txn_status == "aborted":
            # Offsets must not be committed for aborted transaction
            if offset_commits:
                aborted_offsets = [oc for oc in offset_commits if oc.get("txn_id") == txn_id]
                if aborted_offsets:
                    violations.append(Violation(
                        type="NON_ATOMIC_OFFSET_OUTPUT",
                        record_ids=[r.id for r in consumed_txn_recs],
                        message=f"Non-atomic output/offset: consumer offsets were committed for aborted transaction '{txn_id}'",
                        details={"txn_id": txn_id, "aborted_offsets": aborted_offsets},
                    ))
            if consumed_txn_recs and config.isolation_level == "read_committed":
                # Already captured in ABORTED_EXPOSURE, but explicitly note non-atomic history
                pass

    # 9. Duplicate & Visibility Rules (Standard Partition vs Share Delivery)
    if config.delivery == "share":
        # Share delivery rules (KIP-932)
        share_states: Dict[str, str] = {}
        for r in consumed:
            ack = r.ack_type
            prev_state = share_states.get(r.id, "AVAILABLE")

            if prev_state == "ACCEPTED":
                violations.append(Violation(
                    type="SHARE_ACK_VIOLATION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Share record '{r.id}' delivered again after being ACCEPTED",
                    details={"record_id": r.id, "ack_type": ack, "previous_state": "ACCEPTED", "offset": r.offset},
                ))
            elif prev_state == "REJECTED":
                violations.append(Violation(
                    type="SHARE_ACK_VIOLATION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Share record '{r.id}' delivered again after being REJECTED",
                    details={"record_id": r.id, "ack_type": ack, "previous_state": "REJECTED", "offset": r.offset},
                ))
            elif prev_state == "DELIVERED":
                violations.append(Violation(
                    type="SHARE_ACK_VIOLATION",
                    record_ids=[r.id],
                    topic=r.topic,
                    partition=r.partition,
                    message=f"Share record '{r.id}' redelivered without intermediate RELEASE",
                    details={"record_id": r.id, "ack_type": ack, "previous_state": "DELIVERED", "offset": r.offset},
                ))

            # State transition
            if ack == "accept":
                share_states[r.id] = "ACCEPTED"
            elif ack == "reject":
                share_states[r.id] = "REJECTED"
            elif ack == "release":
                share_states[r.id] = "AVAILABLE"
            else:
                share_states[r.id] = "DELIVERED"

    else:
        # Standard partition delivery
        # Determine acked records that should be consumed
        acked_expected: List[Record] = []
        for r in attempted:
            if r.status == "acked":
                if r.txn_id:
                    txn_info = txn_map.get(r.txn_id, {})
                    if txn_info.get("status", "committed").lower() == "aborted":
                        continue
                acked_expected.append(r)

        acked_ids = {r.id for r in acked_expected}
        consumed_counts = Counter(r.id for r in consumed)

        missing_ids = [rid for rid in acked_ids if rid not in consumed_counts]
        duplicate_ids = [rid for rid, count in consumed_counts.items() if count > 1]

        # Detect Synthetic Missing/Duplicate Swaps (e.g. counts match, but IDs swapped)
        if missing_ids and duplicate_ids:
            violations.append(Violation(
                type="SYNTHETIC_SWAP",
                record_ids=sorted(list(set(missing_ids + duplicate_ids))),
                message=(
                    f"Synthetic swap detected: record(s) {sorted(missing_ids)} lost while "
                    f"record(s) {sorted(duplicate_ids)} duplicated (attempted: {len(attempted)}, consumed: {len(consumed)})"
                ),
                details={
                    "missing_record_ids": sorted(missing_ids),
                    "duplicated_record_ids": sorted(duplicate_ids),
                    "attempted_count": len(attempted),
                    "consumed_count": len(consumed),
                },
            ))

        # Check for Data Loss (missing acked records) when acks != 0
        if config.acks != 0:
            for mid in missing_ids:
                mrec = attempted_by_id[mid][0]
                violations.append(Violation(
                    type="DATA_LOSS",
                    record_ids=[mid],
                    topic=mrec.topic,
                    partition=mrec.partition,
                    message=f"Acked record '{mid}' on {mrec.topic}-{mrec.partition} was lost (never consumed)",
                    details={"record_id": mid, "topic": mrec.topic, "partition": mrec.partition},
                ))

        # Check for unexpected duplicates
        for did in duplicate_ids:
            cnt = consumed_counts[did]
            if config.idempotent:
                d_topic = attempted_by_id[did][0].topic if did in attempted_by_id else None
                d_part = attempted_by_id[did][0].partition if did in attempted_by_id else None
                violations.append(Violation(
                    type="UNEXPECTED_DUPLICATE",
                    record_ids=[did],
                    topic=d_topic,
                    partition=d_part,
                    message=f"Record '{did}' consumed {cnt} times under idempotent delivery",
                    details={"record_id": did, "consumed_count": cnt, "idempotent": True},
                ))
            else:
                attempts = attempted_by_id.get(did, [])
                has_ambiguous = any(a.status in ("ambiguous", "attempted") for a in attempts)
                if not has_ambiguous and len(attempts) == 1 and attempts[0].status == "acked":
                    violations.append(Violation(
                        type="UNEXPECTED_DUPLICATE",
                        record_ids=[did],
                        topic=attempts[0].topic,
                        partition=attempts[0].partition,
                        message=f"Record '{did}' consumed {cnt} times without ambiguous retry",
                        details={"record_id": did, "consumed_count": cnt, "idempotent": False},
                    ))

    # 10. Per-Key and Per-Partition Ordering Checks
    # Group consumed records by (topic, partition)
    consumed_by_part: Dict[Tuple[str, int], List[Record]] = defaultdict(list)
    for r in consumed:
        consumed_by_part[(r.topic, r.partition)].append(r)

    for (topic, partition), part_recs in consumed_by_part.items():
        # A. Check offset monotonicity within partition
        for i in range(len(part_recs) - 1):
            curr_r = part_recs[i]
            next_r = part_recs[i + 1]
            if curr_r.offset is not None and next_r.offset is not None:
                if next_r.offset <= curr_r.offset:
                    violations.append(Violation(
                        type="OFFSET_ORDERING_VIOLATION",
                        record_ids=[curr_r.id, next_r.id],
                        topic=topic,
                        partition=partition,
                        message=(
                            f"Non-monotonic offset order on {topic}-{partition}: "
                            f"record '{next_r.id}' (offset {next_r.offset}) <= record '{curr_r.id}' (offset {curr_r.offset})"
                        ),
                        details={
                            "record_before": curr_r.id,
                            "offset_before": curr_r.offset,
                            "record_after": next_r.id,
                            "offset_after": next_r.offset,
                        },
                    ))

        # B. Check partition sequence relative to produce order
        if config.delivery == "partition":
            # Attempted records for this partition in attempt order
            part_attempted = [a for a in attempted if a.topic == topic and a.partition == partition and a.status == "acked"]
            produce_order = {a.id: idx for idx, a in enumerate(part_attempted)}

            consumed_part_ids = [r.id for r in part_recs if r.id in produce_order]
            if config.idempotent:
                for i in range(len(consumed_part_ids) - 1):
                    id_a = consumed_part_ids[i]
                    id_b = consumed_part_ids[i + 1]
                    pos_a = produce_order[id_a]
                    pos_b = produce_order[id_b]
                    if pos_b < pos_a:
                        violations.append(Violation(
                            type="PARTITION_ORDERING_VIOLATION",
                            record_ids=[id_b, id_a],
                            topic=topic,
                            partition=partition,
                            message=(
                                f"Partition ordering violation on {topic}-{partition}: record '{id_b}' "
                                f"(produced #{pos_b}) consumed after record '{id_a}' (produced #{pos_a})"
                            ),
                            details={
                                "topic": topic,
                                "partition": partition,
                                "record_first": id_a,
                                "produce_pos_first": pos_a,
                                "record_second": id_b,
                                "produce_pos_second": pos_b,
                            },
                        ))

            # C. Per-key order within partition
            by_key: Dict[str, List[Record]] = defaultdict(list)
            for r in part_recs:
                if r.key is not None:
                    by_key[r.key].append(r)

            for key_val, key_recs in by_key.items():
                key_attempted = [a for a in part_attempted if a.key == key_val]
                key_produce_order = {a.id: idx for idx, a in enumerate(key_attempted)}
                consumed_key_ids = [r.id for r in key_recs if r.id in key_produce_order]
                for i in range(len(consumed_key_ids) - 1):
                    id_a = consumed_key_ids[i]
                    id_b = consumed_key_ids[i + 1]
                    pos_a = key_produce_order[id_a]
                    pos_b = key_produce_order[id_b]
                    if pos_b < pos_a:
                        violations.append(Violation(
                            type="KEY_ORDERING_VIOLATION",
                            record_ids=[id_b, id_a],
                            topic=topic,
                            partition=partition,
                            message=(
                                f"Key ordering violation for key '{key_val}' on {topic}-{partition}: "
                                f"record '{id_b}' (produced #{pos_b}) consumed after record '{id_a}' (produced #{pos_a})"
                            ),
                            details={
                                "key": key_val,
                                "topic": topic,
                                "partition": partition,
                                "record_first": id_a,
                                "produce_pos_first": pos_a,
                                "record_second": id_b,
                                "produce_pos_second": pos_b,
                            },
                        ))

    # Format result
    valid = len(violations) == 0
    minimal_counterexample = violations[0].to_dict() if violations else None

    if valid:
        summary = (
            f"PASS: history '{history_id}' verified ({len(attempted)} attempted, "
            f"{len(consumed)} consumed, acks={config.acks}, delivery={config.delivery})"
        )
    else:
        summary = (
            f"FAIL: {len(violations)} violation(s) in history '{history_id}'. "
            f"Counterexample: {violations[0].type}: {violations[0].message}"
        )

    return CheckResult(
        history_id=history_id,
        valid=valid,
        attempted_count=len(attempted),
        consumed_count=len(consumed),
        violations=violations,
        minimal_counterexample=minimal_counterexample,
        summary=summary,
    )


def verify_history_source(source_name: str, raw_content: str) -> List[CheckResult]:
    """Parse JSON and verify one or more histories from a source string."""
    stripped = raw_content.strip()
    if not stripped:
        raise HistoryValidationError(f"Empty input from {source_name}: an empty history must not pass")

    try:
        data = json.loads(stripped)
    except Exception as e:
        raise HistoryValidationError(f"Malformed JSON from {source_name}: {e}")

    if data is None:
        raise HistoryValidationError(f"Empty data from {source_name}")

    if isinstance(data, list):
        if not data:
            raise HistoryValidationError(f"Empty list in {source_name}: an empty history must not pass")
        results = []
        for idx, item in enumerate(data):
            results.append(verify_history(item, f"{source_name}[{idx}]"))
        return results
    elif isinstance(data, dict):
        if not data:
            raise HistoryValidationError(f"Empty object in {source_name}: an empty history must not pass")
        return [verify_history(data, source_name)]
    else:
        raise HistoryValidationError(f"Root JSON from {source_name} must be an object or list")


def format_human_result(result: CheckResult, verbose: bool = False) -> str:
    """Format check result for human-readable CLI display."""
    lines = []
    if result.valid:
        lines.append(f"✓ {result.summary}")
    else:
        lines.append(f"✗ {result.summary}")
        if result.minimal_counterexample:
            ce = result.minimal_counterexample
            lines.append("  Minimal Counterexample:")
            lines.append(f"    Violation: {ce['type']}")
            if ce.get("record_ids"):
                lines.append(f"    Record ID(s): {', '.join(ce['record_ids'])}")
            if ce.get("topic") or ce.get("partition") is not None:
                lines.append(f"    Location: topic '{ce.get('topic')}' partition {ce.get('partition')}")
            lines.append(f"    Reason: {ce['message']}")
            if verbose and ce.get("details"):
                lines.append(f"    Details: {json.dumps(ce['details'], indent=6)}")
        if verbose and len(result.violations) > 1:
            lines.append(f"  Additional Violations ({len(result.violations) - 1}):")
            for idx, v in enumerate(result.violations[1:], 1):
                lines.append(f"    [{idx}] {v.type}: {v.message}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic Record-History Correctness Checker (KL03-18)"
    )
    parser.add_argument(
        "histories",
        nargs="*",
        default=["-"],
        help="Path(s) to history JSON file(s), or '-' for stdin (default: '-')",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output structured JSON results",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print verbose violation details and additional counterexamples",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress passing output and summary",
    )

    args = parser.parse_args(argv)

    all_results: List[CheckResult] = []
    has_validation_error = False

    for src in args.histories:
        if src == "-":
            try:
                content = sys.stdin.read()
                results = verify_history_source("<stdin>", content)
                all_results.extend(results)
            except HistoryValidationError as e:
                sys.stderr.write(f"Validation error: {e}\n")
                has_validation_error = True
            except Exception as e:
                sys.stderr.write(f"Unexpected error reading stdin: {e}\n")
                has_validation_error = True
        else:
            p = Path(src)
            if not p.is_file():
                sys.stderr.write(f"File not found: {p}\n")
                has_validation_error = True
                continue
            try:
                content = p.read_text(encoding="utf-8")
                results = verify_history_source(str(p), content)
                all_results.extend(results)
            except HistoryValidationError as e:
                sys.stderr.write(f"Validation error in {p}: {e}\n")
                has_validation_error = True
            except Exception as e:
                sys.stderr.write(f"Unexpected error in {p}: {e}\n")
                has_validation_error = True

    if has_validation_error:
        return 2

    if not all_results:
        sys.stderr.write("No histories provided or processed\n")
        return 2

    all_valid = all(r.valid for r in all_results)

    if args.json:
        out = {
            "all_valid": all_valid,
            "total_histories": len(all_results),
            "passed": sum(1 for r in all_results if r.valid),
            "failed": sum(1 for r in all_results if not r.valid),
            "results": [r.to_dict() for r in all_results],
        }
        print(json.dumps(out, indent=2))
    else:
        for r in all_results:
            if not args.quiet or not r.valid:
                print(format_human_result(r, verbose=args.verbose))

    return 0 if all_valid else 1


if __name__ == "__main__":
    sys.exit(main())

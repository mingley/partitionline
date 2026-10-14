#!/usr/bin/env python3
"""Frozen KL03-20 consume-transform-produce crash/fencing cases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FREEZE = REPO_ROOT / "docs" / "evidence" / "transactions" / "campaign-freeze.json"
PROFILE = "transactional-crash-fencing"
REQUIRED_CRASHES = {
    "before_produce_ack",
    "after_produce_ack",
    "after_offset_stage",
    "before_endtxn",
    "during_endtxn",
}
HISTORY_CONFIG = {
    "acks": -1,
    "idempotent": True,
    "transactional": True,
    "isolation_level": "read_committed",
    "delivery": "partition",
}


def load_freeze(path: Path = DEFAULT_FREEZE) -> Dict[str, Any]:
    raw = json.loads(path.read_text())
    if raw.get("profile") != PROFILE:
        raise ValueError(f"unexpected profile {raw.get('profile')!r}")
    cases = raw.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("freeze must declare named cases")
    return raw


def cases(freeze: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    freeze = freeze or load_freeze()
    return list(freeze["cases"])


def case_by_id(case_id: str, freeze: Dict[str, Any] | None = None) -> Dict[str, Any]:
    for case in cases(freeze):
        if case["id"] == case_id:
            return case
    raise KeyError(case_id)


def validate_freeze(freeze: Dict[str, Any] | None = None) -> Dict[str, Any]:
    freeze = freeze or load_freeze()
    found = {c.get("crash") for c in freeze["cases"] if c.get("crash")}
    missing = REQUIRED_CRASHES - found
    if missing:
        raise ValueError(f"freeze missing crash boundaries {sorted(missing)}")
    if not any(c.get("fence") for c in freeze["cases"]):
        raise ValueError("freeze missing replacement-producer fencing")
    if not any(c.get("abort") for c in freeze["cases"]):
        raise ValueError("freeze missing explicit abort")
    replay = freeze.get("replay_case")
    if replay not in {c["id"] for c in freeze["cases"]}:
        raise ValueError(f"replay_case {replay!r} is not a frozen case")
    txn_ms = int(freeze["budgets"]["transaction_timeout_ms"])
    leader_ms = int(freeze["budgets"]["leader_recovery_ms"])
    if txn_ms >= leader_ms:
        raise ValueError("transaction timeout must be distinct from and below leader recovery")
    return {
        "ok": True,
        "profile": PROFILE,
        "case_count": len(freeze["cases"]),
        "crashes": sorted(found),
        "replay_case": freeze["replay_case"],
        "transaction_timeout_ms": txn_ms,
        "leader_recovery_ms": leader_ms,
    }


def copied_ids(input_records: int = 2) -> List[str]:
    return [f"out-in-{i}" for i in range(input_records)]


def _record(
    rec_id: str,
    topic: str,
    *,
    key: str,
    payload: str,
    offset: Optional[int] = None,
    txn_id: Optional[str] = None,
    status: str = "acked",
) -> Dict[str, Any]:
    rec: Dict[str, Any] = {
        "id": rec_id,
        "topic": topic,
        "partition": 0,
        "key": key,
        "payload": payload,
        "status": status,
    }
    if offset is not None:
        rec["offset"] = offset
    if txn_id is not None:
        rec["txn_id"] = txn_id
    return rec


def evaluate_observation(
    case: Dict[str, Any],
    payloads: Sequence[str],
    committed_offset: int,
    input_records: int = 2,
) -> Dict[str, Any]:
    """Check read_committed output and committed input offsets against a frozen case."""
    copies = [p for p in payloads if p.startswith("out-in-")]
    fence_out = "fence-out" in payloads
    fenced_leaked = any(p.startswith("out-fenced") for p in payloads)
    has_anchor = "anchor" in payloads
    offsets_advanced = committed_offset >= input_records
    expect_out = case.get("expect_output")
    expect_off = case.get("expect_offsets")
    errors: List[str] = []
    if not has_anchor:
        errors.append("missing_anchor")
    if expect_out == "copied_input":
        if sorted(copies) != copied_ids(input_records):
            errors.append("missing_or_partial_copies")
        if fence_out or fenced_leaked:
            errors.append("unexpected_fence_output")
        if expect_off == "advanced" and not offsets_advanced:
            errors.append("offsets_not_advanced")
    elif expect_out == "anchor_only":
        if copies or fence_out or fenced_leaked:
            errors.append("aborted_or_fenced_output_visible")
        if expect_off == "unchanged" and offsets_advanced:
            errors.append("offsets_advanced_after_abort")
    elif expect_out == "replacement_only":
        if not fence_out:
            errors.append("missing_replacement_output")
        if copies or fenced_leaked:
            errors.append("stale_or_copied_output_visible")
        if expect_off == "unchanged" and offsets_advanced:
            errors.append("offsets_advanced_on_fence")
    elif expect_out == "atomic":
        if copies:
            if sorted(copies) != copied_ids(input_records):
                errors.append("partial_atomic_output")
            if not offsets_advanced:
                errors.append("output_without_offsets")
        else:
            if fence_out or fenced_leaked:
                errors.append("unexpected_fence_output")
            if offsets_advanced:
                errors.append("offsets_without_output")
        if fenced_leaked:
            errors.append("stale_output_visible")
    else:
        errors.append(f"unknown_expect_output:{expect_out}")
    return {
        "ok": not errors,
        "errors": errors,
        "payloads": list(payloads),
        "committed_offset": committed_offset,
        "copies_visible": bool(copies),
        "offsets_advanced": offsets_advanced,
    }


def assemble_history(
    case: Dict[str, Any],
    *,
    output_topic: str,
    input_topic: str,
    txn_id: str,
    consumed: Sequence[Dict[str, Any]],
    committed_offset: int,
    input_records: int = 2,
) -> Dict[str, Any]:
    """Checker-facing history from a live observation."""
    copies = [
        _record(
            f"out-in-{i}",
            output_topic,
            key=f"in-{i}",
            payload=f"out-in-{i}",
            txn_id=txn_id,
        )
        for i in range(input_records)
    ]
    anchor = _record("anchor", output_topic, key="anchor", payload="anchor", offset=0)
    stale_id = f"{txn_id}-stale"
    fence_stale = _record(
        "out-fenced", output_topic, key="fenced", payload="out-fenced", txn_id=stale_id
    )
    fence_out = _record(
        "fence-out", output_topic, key="fence-out", payload="fence-out", txn_id=txn_id
    )
    payloads = [str(r.get("payload")) for r in consumed]
    copies_visible = any(p.startswith("out-in-") for p in payloads)
    fence_visible = "fence-out" in payloads
    if case.get("fence"):
        attempted = [anchor, fence_stale, fence_out]
        transactions: List[Dict[str, Any]] = [
            {"txn_id": stale_id, "status": "aborted"},
            {"txn_id": txn_id, "status": "committed" if fence_visible else "aborted"},
        ]
        offset_commits: List[Dict[str, Any]] = []
    else:
        attempted = [anchor, *copies]
        if copies_visible:
            transactions = [
                {
                    "txn_id": txn_id,
                    "status": "committed",
                    "committed_offsets": {f"{input_topic}-0": committed_offset},
                }
            ]
            offset_commits = [
                {
                    "topic": input_topic,
                    "partition": 0,
                    "offset": committed_offset,
                    "txn_id": txn_id,
                }
            ]
        else:
            transactions = [{"txn_id": txn_id, "status": "aborted"}]
            offset_commits = []
    consumed_recs = []
    for rec in consumed:
        item = dict(rec)
        payload = str(item.get("payload", ""))
        if payload.startswith("out-in-"):
            item["txn_id"] = txn_id
        elif payload == "fence-out":
            item["txn_id"] = txn_id
        elif payload.startswith("out-fenced"):
            item["txn_id"] = stale_id
        consumed_recs.append(item)
    return {
        "id": case["id"],
        "config": dict(HISTORY_CONFIG),
        "attempted": attempted,
        "consumed": consumed_recs,
        "transactions": transactions,
        "offset_commits": offset_commits,
    }


def synthetic_history(
    case: Dict[str, Any],
    *,
    leak_aborted: bool = False,
    atomic_committed: bool = False,
    input_records: int = 2,
) -> Dict[str, Any]:
    """Checker-facing history for offline tests (not a live run)."""
    output = "pl-txn-out"
    input_topic = "pl-txn-in"
    txn_id = "txn-worker"
    if case.get("fence"):
        consumed = [
            _record("anchor", output, key="anchor", payload="anchor", offset=0),
            _record(
                "fence-out",
                output,
                key="fence-out",
                payload="fence-out",
                offset=1,
                txn_id=txn_id,
            ),
        ]
        committed = -1
    elif case["id"] == "happy_commit" or case.get("expect_output") == "copied_input":
        consumed = [_record("anchor", output, key="anchor", payload="anchor", offset=0)]
        for i in range(input_records):
            consumed.append(
                _record(
                    f"out-in-{i}",
                    output,
                    key=f"in-{i}",
                    payload=f"out-in-{i}",
                    offset=i + 1,
                    txn_id=txn_id,
                )
            )
        committed = input_records
    elif case.get("expect_output") == "atomic" and atomic_committed:
        consumed = [_record("anchor", output, key="anchor", payload="anchor", offset=0)]
        for i in range(input_records):
            consumed.append(
                _record(
                    f"out-in-{i}",
                    output,
                    key=f"in-{i}",
                    payload=f"out-in-{i}",
                    offset=i + 1,
                    txn_id=txn_id,
                )
            )
        committed = input_records
    else:
        consumed = [_record("anchor", output, key="anchor", payload="anchor", offset=0)]
        committed = -1
        if leak_aborted:
            consumed.append(
                _record(
                    "out-in-0",
                    output,
                    key="in-0",
                    payload="out-in-0",
                    offset=1,
                    txn_id=txn_id,
                )
            )
    history = assemble_history(
        case,
        output_topic=output,
        input_topic=input_topic,
        txn_id=txn_id,
        consumed=consumed,
        committed_offset=committed,
        input_records=input_records,
    )
    if leak_aborted:
        history["transactions"] = [{"txn_id": txn_id, "status": "aborted"}]
        history["offset_commits"] = []
    return history


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = validate_freeze(load_freeze(args.freeze))
    print(json.dumps(report, indent=2) if args.json else report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

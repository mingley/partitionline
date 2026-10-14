#!/usr/bin/env python3
"""Frozen KL03-21 group-protocol churn cells."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FREEZE = REPO_ROOT / "docs" / "evidence" / "groups" / "campaign-freeze.json"
PROFILE = "group-protocol-churn"
REQUIRED_PROFILES = ("classic", "cooperative", "kip848", "share")
REQUIRED_CHURNS = (
    "member_loss",
    "coordinator_loss",
    "pause",
    "max_poll_expiry",
    "secure_reconnect",
)
DISTINCT_BUDGETS = (
    "session_timeout_ms",
    "heartbeat_interval_ms",
    "max_poll_interval_ms",
    "share_lock_hold_ms",
    "pause_ms",
    "coordinator_recovery_ms",
)


def load_freeze(path: Path = DEFAULT_FREEZE) -> Dict[str, Any]:
    raw = json.loads(path.read_text())
    if raw.get("profile") != PROFILE:
        raise ValueError(f"unexpected profile {raw.get('profile')!r}")
    return raw


def cells(freeze: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    freeze = freeze or load_freeze()
    out = []
    for proto in freeze["profiles"]:
        for churn in freeze["churns"]:
            out.append(
                {
                    "id": f"{proto}/{churn}",
                    "protocol": proto,
                    "churn": churn,
                    "secure": churn == "secure_reconnect",
                    "two_members": churn in ("member_loss", "max_poll_expiry"),
                }
            )
    return out


def cell_by_id(cell_id: str, freeze: Dict[str, Any] | None = None) -> Dict[str, Any]:
    for cell in cells(freeze):
        if cell["id"] == cell_id:
            return cell
    raise KeyError(cell_id)


def validate_freeze(freeze: Dict[str, Any] | None = None) -> Dict[str, Any]:
    freeze = freeze or load_freeze()
    if tuple(freeze.get("profiles") or ()) != REQUIRED_PROFILES:
        raise ValueError(f"freeze profiles must be {REQUIRED_PROFILES}")
    if tuple(freeze.get("churns") or ()) != REQUIRED_CHURNS:
        raise ValueError(f"freeze churns must be {REQUIRED_CHURNS}")
    replay = freeze.get("replay_cell")
    if replay not in {c["id"] for c in cells(freeze)}:
        raise ValueError(f"replay_cell {replay!r} is not a frozen cell")
    budgets = freeze["budgets"]
    values = [int(budgets[name]) for name in DISTINCT_BUDGETS]
    if len(set(values)) != len(values):
        raise ValueError("timer budgets must stay distinct")
    older = freeze.get("older_brokers") or {}
    if "3.9.1" not in older:
        raise ValueError("older Kafka 3.9.1 cells must be explicit")
    for proto in ("kip848", "share"):
        note = str(older["3.9.1"].get(proto, ""))
        if "unsupported" not in note.lower():
            raise ValueError(f"3.9.1 {proto} must be explicitly unsupported")
    planned = cells(freeze)
    return {
        "ok": True,
        "profile": PROFILE,
        "cell_count": len(planned),
        "profiles": list(freeze["profiles"]),
        "churns": list(freeze["churns"]),
        "replay_cell": replay,
        "distinct_budgets": {name: int(budgets[name]) for name in DISTINCT_BUDGETS},
    }


def record_id(index: int) -> str:
    return f"g{index:02d}"


def evaluate_cell(
    cell: Dict[str, Any],
    committed_ids: List[str],
    events_by_member: Dict[str, List[str]],
    records: int = 6,
) -> Dict[str, Any]:
    expected = {record_id(i) for i in range(records)}
    errors: List[str] = []
    if len(committed_ids) != len(set(committed_ids)):
        errors.append("duplicate_committed_ids")
    if set(committed_ids) != expected:
        errors.append("missing_or_extra_committed_ids")
    events_a = events_by_member.get("a") or []
    if cell["churn"] == "pause":
        if cell["protocol"] == "share":
            if "paused_hold" not in events_a:
                errors.append("missing_share_pause_hold")
        elif "paused" not in events_a:
            errors.append("missing_pause")
    if cell["churn"] == "max_poll_expiry":
        if cell["protocol"] == "share":
            if "lock_held" not in events_a and "released_for_redelivery" not in events_a:
                errors.append("missing_share_lock_expiry")
        elif "max_poll_left" not in events_a:
            errors.append("missing_max_poll_left")
    return {
        "ok": not errors,
        "errors": errors,
        "committed_ids": committed_ids,
        "events": events_by_member,
    }


def synthetic_history(
    cell: Dict[str, Any],
    *,
    duplicate: bool = False,
    input_records: int = 6,
) -> Dict[str, Any]:
    topic = f"pl-grp-{cell['protocol']}-{cell['churn']}"
    share = cell["protocol"] == "share"
    attempted = []
    consumed = []
    for i in range(input_records):
        rec = {
            "id": record_id(i),
            "topic": topic,
            "partition": i % 3,
            "offset": i // 3,
            "key": record_id(i),
            "payload": record_id(i),
            "status": "acked",
        }
        attempted.append(dict(rec))
        got = dict(rec)
        if share:
            got["ack_type"] = "accept"
            got["delivery_attempt"] = 1
        consumed.append(got)
    if duplicate and consumed:
        extra = dict(consumed[0])
        extra["offset"] = 99
        if share:
            extra["ack_type"] = "accept"
            extra["delivery_attempt"] = 2
        consumed.append(extra)
    return {
        "id": cell["id"],
        "config": {
            "acks": -1,
            "idempotent": True,
            "transactional": False,
            "isolation_level": "read_uncommitted",
            "delivery": "share" if share else "partition",
        },
        "attempted": attempted,
        "consumed": consumed,
    }


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

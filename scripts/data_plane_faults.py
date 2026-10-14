#!/usr/bin/env python3
"""Deterministic KL03-19 nontransactional data-plane fault plan.

Freezes seeds, fault types, timing, and restoration/deadline budgets. The
live campaign runner consumes this plan; this module does not start a cluster.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FREEZE = REPO_ROOT / "docs" / "evidence" / "data-plane" / "campaign-freeze.json"

FAULTS = (
    "leader_move",
    "lost_ack",
    "stale_metadata",
    "disconnect",
    "restart",
)

PROFILE = "nontransactional-data-plane"


def load_freeze(path: Path = DEFAULT_FREEZE) -> Dict[str, Any]:
    raw = json.loads(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError("campaign freeze must be a JSON object")
    if raw.get("profile") != PROFILE:
        raise ValueError(f"unexpected profile {raw.get('profile')!r}")
    if int(raw["seeds"]["count"]) < 1000:
        raise ValueError("freeze must declare at least 1000 seeds")
    return raw


def seed_range(freeze: Dict[str, Any]) -> range:
    seeds = freeze["seeds"]
    return range(int(seeds["start"]), int(seeds["end"]) + 1)


def plan_seed(seed: int, freeze: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Map one seed to a frozen trial. Pure function of seed + freeze."""
    freeze = freeze or load_freeze()
    budgets = freeze["budgets"]
    partitions = int(freeze["partitions"])
    records = int(freeze["records_per_seed"])
    fault_after = int(freeze["fault_after"])
    if fault_after < 1 or fault_after >= records:
        raise ValueError("fault_after must be in [1, records_per_seed)")
    fault = FAULTS[seed % len(FAULTS)]
    node = (seed % 3) + 1
    partition = seed % partitions
    return {
        "id": f"dp-{seed:04d}",
        "seed": seed,
        "profile": PROFILE,
        "fault": fault,
        "node": node,
        "partition": partition,
        "records": records,
        "fault_after": fault_after,
        "topic": freeze["topic"],
        "acks": freeze["acks"],
        "idempotent": freeze["idempotent"],
        "transactional": freeze["transactional"],
        "isolation_level": freeze["isolation_level"],
        "delivery": freeze["delivery"],
        "budgets": {
            "trial_deadline_ms": int(budgets["trial_deadline_ms"]),
            "restoration_target_ms": int(budgets["restoration_target_ms"]),
            "isolation_ms": int(budgets.get("isolation_ms", 2000)),
            "produce_request_timeout_ms": int(budgets["produce_request_timeout_ms"]),
            "delivery_timeout_ms": int(budgets["delivery_timeout_ms"]),
            "ready_wait_ms": int(budgets["ready_wait_ms"]),
        },
    }


def plan_all(freeze: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    freeze = freeze or load_freeze()
    return [plan_seed(seed, freeze) for seed in seed_range(freeze)]


def fault_counts(plans: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {name: 0 for name in FAULTS}
    for plan in plans:
        counts[plan["fault"]] += 1
    return counts


def record_id(seed: int, index: int) -> str:
    return f"s{seed:04d}-r{index}"


def synthetic_history(plan: Dict[str, Any], *, swap: bool = False) -> Dict[str, Any]:
    """Build a checker-facing history for planner tests (not a live run)."""
    attempted = []
    consumed = []
    for i in range(plan["records"]):
        rid = record_id(plan["seed"], i)
        rec = {
            "id": rid,
            "topic": plan["topic"],
            "partition": plan["partition"],
            "offset": i,
            "key": rid,
            "payload": f"payload-{rid}",
            "status": "acked",
        }
        attempted.append(rec)
        consumed.append(dict(rec))
    if swap and len(consumed) >= 2:
        consumed[0]["id"] = record_id(plan["seed"], 99)
        consumed[0]["payload"] = "swapped"
        consumed[0]["payload_hash"] = ""
    return {
        "id": plan["id"],
        "config": {
            "acks": plan["acks"],
            "idempotent": plan["idempotent"],
            "transactional": plan["transactional"],
            "isolation_level": plan["isolation_level"],
            "delivery": plan["delivery"],
        },
        "attempted": attempted,
        "consumed": consumed,
        "plan": {k: plan[k] for k in ("seed", "fault", "node", "partition")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, default=DEFAULT_FREEZE)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    freeze = load_freeze(args.freeze)
    if args.seed is not None:
        plans = [plan_seed(args.seed, freeze)]
    else:
        plans = plan_all(freeze)
    payload = {
        "profile": PROFILE,
        "count": len(plans),
        "fault_counts": fault_counts(plans),
        "replay_seed": freeze["replay_seed"],
        "plans": plans if args.seed is not None else None,
        "first": plans[0],
        "last": plans[-1],
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"{PROFILE} seeds={payload['count']} faults={payload['fault_counts']} "
            f"replay={payload['replay_seed']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Fail-closed benchmark result and provenance format validator and reporter.

Implements the equal-semantics benchmark contract (docs/benchmark-contract.md)
for partitionline and all peer benchmark drivers (librdkafka, kafka-clients).

Enforces:
1. Complete provenance (source commit, binary sha256, config sha256, toolchains,
   actual broker image/version, host CPU/OS, topology, timestamps, seeds, artifacts).
2. Exhaustive 7-way outcome accounting (offered, accepted, acknowledged, consumed,
   rejected, timed_out, unknown).
3. Raw measurements retention and strict unit matching (latency in microseconds,
   throughput in records/s and MB/s, memory/RSS in bytes, CPU in percent).
4. Equal-semantics validation (frozen acks, idempotence, max-in-flight bounds).
5. High-watermark audit parity and record ID integrity (zero missing IDs).
6. Suite HOLD preservation (Suite HOLD is active; a result file is not a scenario pass).
7. Non-erasure of failed attempts (a later success does not erase an earlier integrity failure).
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


HEX_SHA_REGEX = re.compile(r"^[0-9a-fA-F]{7,40}$")
HEX_SHA256_REGEX = re.compile(r"^[0-9a-fA-F]{64}$")
ISO_DATETIME_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)

# Standard required units
EXPECTED_UNITS = {
    "latency": "microseconds",
    "throughput_records": "records/s",
    "throughput_mb": "MB/s",
    "throughput_bytes": "bytes",
    "cpu": "percent",
    "cpu_seconds": "seconds",
    "memory": "bytes",
    "rss": "bytes",
    "duration": "seconds",
    "rtt": "milliseconds",
    "lag": "records",
}


def normalize_acks(acks: Any) -> Optional[int]:
    """Normalize acks to integer (-1, 0, 1)."""
    if acks is None:
        return None
    if isinstance(acks, int):
        return acks
    s = str(acks).strip().lower()
    if s in ("all", "-1"):
        return -1
    if s in ("1", "+1"):
        return 1
    if s in ("0",):
        return 0
    return None


class BenchmarkValidator:
    """Fail-closed validator for benchmark result documents."""

    def __init__(self, schema_path: Optional[Path] = None):
        self.schema_path = schema_path
        self.schema: Optional[Dict[str, Any]] = None
        if schema_path and schema_path.is_file():
            try:
                self.schema = json.loads(schema_path.read_text(encoding="utf-8"))
            except Exception as e:
                raise RuntimeError(f"Failed to load schema from {schema_path}: {e}") from e

    def validate(
        self, data: Dict[str, Any], require_clean: bool = False
    ) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Validate result dictionary.

        Returns:
            (is_valid, errors, summary_dict)
        """
        errors: List[str] = []
        summary: Dict[str, Any] = {
            "scenario_id": None,
            "peer": None,
            "suite_hold": None,
            "throughput_rps": 0.0,
            "latency_p99_us": 0.0,
            "total_acknowledged": 0,
            "integrity_verified": False,
            "has_retained_integrity_failure": False,
        }

        if not isinstance(data, dict):
            return False, ["Root benchmark result must be a JSON object"], summary

        # 1. Required Top-Level Sections
        required_top_level = [
            "schema_version",
            "contract_version",
            "suite_hold",
            "scenario",
            "provenance",
            "execution",
            "outcomes",
            "measurements",
            "integrity",
            "repetition_history",
        ]
        for key in required_top_level:
            if key not in data:
                errors.append(f"Missing required top-level section: '{key}'")

        if errors:
            # Stop early if fundamental skeleton is absent
            return False, errors, summary

        # 2. Suite HOLD & Disposition Rules
        # Contract Section 2: "Preserve Suite HOLD: Suite HOLD remains active. Unsigned samples,
        # shared-runner CI runs, and local-VM samples do not lift Suite HOLD."
        suite_hold = data.get("suite_hold", {})
        if not isinstance(suite_hold, dict):
            errors.append("'suite_hold' section must be an object")
        else:
            status = suite_hold.get("status")
            summary["suite_hold"] = status
            if status != "active":
                errors.append(
                    f"Suite HOLD violation: status must be 'active', got '{status}'. "
                    "Unsigned benchmark results cannot lift Suite HOLD."
                )

        scenario = data.get("scenario", {})
        if not isinstance(scenario, dict):
            errors.append("'scenario' section must be an object")
        else:
            summary["scenario_id"] = scenario.get("scenario_id")
            summary["peer"] = scenario.get("peer")
            disposition = scenario.get("cell_disposition")
            # Contract Section 1.5: "A result file is not a pass of a scenario cell.
            # No cell has disposition passed."
            if disposition == "passed":
                errors.append(
                    "Disposition violation: 'cell_disposition' cannot be 'passed'. "
                    "A result file records execution evidence; it does not confer passed status."
                )
            elif disposition not in ("executed", "failed", "not_run", "unsupported", "historical"):
                errors.append(
                    f"Invalid scenario cell_disposition: '{disposition}'. Must be one of "
                    "['executed', 'failed', 'not_run', 'unsupported', 'historical']."
                )

        # 3. Provenance Integrity
        provenance = data.get("provenance", {})
        if not isinstance(provenance, dict):
            errors.append("'provenance' must be an object")
        else:
            # Source
            source = provenance.get("source", {})
            if not isinstance(source, dict):
                errors.append("provenance.source must be an object")
            else:
                commit = str(source.get("git_commit", "")).strip()
                if not commit or not HEX_SHA_REGEX.match(commit):
                    errors.append(
                        f"Missing or invalid provenance.source.git_commit: '{commit}'. "
                        "Must be a valid Git commit SHA."
                    )
                if not source.get("tree_hash"):
                    errors.append("Missing provenance.source.tree_hash")

            # Binary
            binary = provenance.get("binary", {})
            if not isinstance(binary, dict):
                errors.append("provenance.binary must be an object")
            else:
                bin_sha = str(binary.get("sha256", "")).strip()
                if not bin_sha or not HEX_SHA256_REGEX.match(bin_sha):
                    errors.append(
                        f"Missing or invalid provenance.binary.sha256: '{bin_sha}'. Must be 64-char hex SHA-256."
                    )
                if not binary.get("name") or not binary.get("path"):
                    errors.append("Missing provenance.binary name or path")

            # Config
            config = provenance.get("config", {})
            if not isinstance(config, dict):
                errors.append("provenance.config must be an object")
            else:
                cfg_sha = str(config.get("sha256", "")).strip()
                if not cfg_sha or not HEX_SHA256_REGEX.match(cfg_sha):
                    errors.append(
                        f"Missing or invalid provenance.config.sha256: '{cfg_sha}'. Must be 64-char hex SHA-256."
                    )
                effective = config.get("effective_settings", {})
                if not isinstance(effective, dict):
                    errors.append("provenance.config.effective_settings must be an object")
                else:
                    for req_cfg in ("acks", "linger_ms", "batch_size_bytes", "max_in_flight", "idempotence"):
                        if req_cfg not in effective:
                            errors.append(f"Missing required config in effective_settings: '{req_cfg}'")
                    # Check in-flight bound when idempotence enabled (Kafka protocol rule)
                    if effective.get("idempotence") is True:
                        mif = effective.get("max_in_flight")
                        if isinstance(mif, int) and mif > 5:
                            errors.append(
                                f"Kafka protocol violation: max.in.flight ({mif}) exceeds 5 while idempotence=true"
                            )

            # Toolchains
            toolchains = provenance.get("toolchains", {})
            if not isinstance(toolchains, dict):
                errors.append("provenance.toolchains must be an object")
            else:
                for tc_key in ("compiler", "runtime", "build_tool"):
                    if not toolchains.get(tc_key):
                        errors.append(f"Missing required toolchain provenance: '{tc_key}'")

            # Broker
            broker = provenance.get("broker", {})
            if not isinstance(broker, dict):
                errors.append("provenance.broker must be an object")
            else:
                for b_key in ("image", "version", "mode", "cluster_id", "node_count", "endpoints"):
                    if b_key not in broker or broker[b_key] is None or broker[b_key] == "":
                        errors.append(f"Missing required broker provenance: '{b_key}'")
                endpoints = broker.get("endpoints")
                if not isinstance(endpoints, list) or len(endpoints) == 0:
                    errors.append("provenance.broker.endpoints must be a non-empty list")

            # Host & CPU & OS
            host = provenance.get("host", {})
            if not isinstance(host, dict):
                errors.append("provenance.host must be an object")
            else:
                for h_key in ("hostname", "os", "os_family", "kernel_version", "arch", "cpu", "memory"):
                    if h_key not in host or (isinstance(host[h_key], str) and not host[h_key].strip()):
                        errors.append(f"Missing required host provenance: '{h_key}'")
                cpu = host.get("cpu", {})
                if not isinstance(cpu, dict):
                    errors.append("provenance.host.cpu must be an object")
                else:
                    for c_key in ("model", "physical_cores", "logical_cores", "frequency_mhz"):
                        if c_key not in cpu or (isinstance(cpu[c_key], str) and not cpu[c_key].strip()):
                            errors.append(f"Missing required host.cpu property: '{c_key}'")
                mem = host.get("memory", {})
                if not isinstance(mem, dict):
                    errors.append("provenance.host.memory must be an object")
                else:
                    if mem.get("unit") != EXPECTED_UNITS["memory"]:
                        errors.append(
                            f"Host memory unit must be '{EXPECTED_UNITS['memory']}', got '{mem.get('unit')}'"
                        )
                    if not isinstance(mem.get("total_bytes"), int) or mem.get("total_bytes") <= 0:
                        errors.append("Host memory total_bytes must be a positive integer")

            # Topology
            topology = provenance.get("topology", {})
            if not isinstance(topology, dict):
                errors.append("provenance.topology must be an object")
            else:
                for top_key in ("environment", "rtt_ms", "rtt_unit", "client_nodes", "broker_nodes", "network_interface"):
                    if top_key not in topology:
                        errors.append(f"Missing required topology property: '{top_key}'")
                if topology.get("rtt_unit") != EXPECTED_UNITS["rtt"]:
                    errors.append(
                        f"Topology rtt_unit must be '{EXPECTED_UNITS['rtt']}', got '{topology.get('rtt_unit')}'"
                    )

            # Timestamps
            timestamps = provenance.get("timestamps", {})
            if not isinstance(timestamps, dict):
                errors.append("provenance.timestamps must be an object")
            else:
                start_t = str(timestamps.get("start_time_utc", "")).strip()
                end_t = str(timestamps.get("end_time_utc", "")).strip()
                if not ISO_DATETIME_REGEX.match(start_t):
                    errors.append(f"Invalid ISO 8601 start_time_utc: '{start_t}'")
                if not ISO_DATETIME_REGEX.match(end_t):
                    errors.append(f"Invalid ISO 8601 end_time_utc: '{end_t}'")
                dur = timestamps.get("duration_seconds")
                if not isinstance(dur, (int, float)) or dur <= 0:
                    errors.append(f"duration_seconds must be > 0, got {dur}")
                if timestamps.get("duration_unit") != EXPECTED_UNITS["duration"]:
                    errors.append(
                        f"duration_unit must be '{EXPECTED_UNITS['duration']}', got '{timestamps.get('duration_unit')}'"
                    )

            # Seeds
            seeds = provenance.get("seeds", {})
            if not isinstance(seeds, dict):
                errors.append("provenance.seeds must be an object")
            else:
                for seed_key in ("payload_seed", "key_seed", "partition_seed", "repetition_seed"):
                    if seed_key not in seeds:
                        errors.append(f"Missing required seed: '{seed_key}'")

            # Artifacts
            artifacts = provenance.get("artifacts")
            if not isinstance(artifacts, list) or len(artifacts) == 0:
                errors.append("provenance.artifacts must be a non-empty list of generated artifacts")
            else:
                for i, art in enumerate(artifacts):
                    if not isinstance(art, dict):
                        errors.append(f"Artifact #{i} must be an object")
                        continue
                    if not art.get("path") or not art.get("type"):
                        errors.append(f"Artifact #{i} missing path or type")
                    art_sha = str(art.get("sha256", "")).strip()
                    if not HEX_SHA256_REGEX.match(art_sha):
                        errors.append(f"Artifact #{i} '{art.get('path')}' has invalid sha256 checksum: '{art_sha}'")
                    if not isinstance(art.get("size_bytes"), int) or art.get("size_bytes") < 0:
                        errors.append(f"Artifact #{i} '{art.get('path')}' size_bytes must be >= 0")

        # 4. Outcomes Accounting
        # Acceptance: "Record offered, accepted, acknowledged, consumed, rejected, timed-out and unknown outcomes separately."
        outcomes = data.get("outcomes", {})
        required_outcomes = [
            "offered",
            "accepted",
            "acknowledged",
            "consumed",
            "rejected",
            "timed_out",
            "unknown",
        ]
        if not isinstance(outcomes, dict):
            errors.append("'outcomes' must be an object containing all 7 required outcome categories")
        else:
            for out_key in required_outcomes:
                if out_key not in outcomes:
                    errors.append(f"Missing required outcome category: '{out_key}'")
                elif not isinstance(outcomes[out_key], int) or outcomes[out_key] < 0:
                    errors.append(f"Outcome '{out_key}' must be a non-negative integer")

            if all(k in outcomes and isinstance(outcomes[k], int) for k in required_outcomes):
                offered = outcomes["offered"]
                accepted = outcomes["accepted"]
                acked = outcomes["acknowledged"]
                rejected = outcomes["rejected"]
                summary["total_acknowledged"] = acked

                # Semantic checks
                if accepted + rejected > offered:
                    errors.append(
                        f"Outcome accounting error: accepted ({accepted}) + rejected ({rejected}) > offered ({offered})"
                    )
                if acked > accepted:
                    errors.append(
                        f"Outcome accounting error: acknowledged ({acked}) cannot exceed accepted ({accepted}). "
                        "Enqueue acceptance must never be counted as broker acknowledgment."
                    )

                # Acks == 0 semantic rule
                eq_sem = scenario.get("equal_semantics", {})
                req_acks = normalize_acks(eq_sem.get("acks"))
                if req_acks == 0 and acked > 0:
                    errors.append(
                        f"Equal semantics error: scenario specifies acks=0 (fire-and-forget), "
                        f"but acknowledged={acked} > 0. Broker does not ack under acks=0."
                    )

        # 5. Measurements & Unit Enforcement
        measurements = data.get("measurements", {})
        if not isinstance(measurements, dict):
            errors.append("'measurements' must be an object")
        else:
            # Throughput
            tp = measurements.get("throughput", {})
            if not isinstance(tp, dict):
                errors.append("measurements.throughput must be an object")
            else:
                for tp_key in (
                    "records_per_second",
                    "records_per_second_unit",
                    "megabytes_per_second",
                    "megabytes_per_second_unit",
                    "total_bytes_transferred",
                    "total_bytes_unit",
                ):
                    if tp_key not in tp:
                        errors.append(f"Missing required measurements.throughput field: '{tp_key}'")
                if tp.get("records_per_second_unit") != EXPECTED_UNITS["throughput_records"]:
                    errors.append(
                        f"Throughput unit mismatch: records_per_second_unit must be '{EXPECTED_UNITS['throughput_records']}', "
                        f"got '{tp.get('records_per_second_unit')}'"
                    )
                if tp.get("megabytes_per_second_unit") != EXPECTED_UNITS["throughput_mb"]:
                    errors.append(
                        f"Throughput unit mismatch: megabytes_per_second_unit must be '{EXPECTED_UNITS['throughput_mb']}', "
                        f"got '{tp.get('megabytes_per_second_unit')}'"
                    )
                if tp.get("total_bytes_unit") != EXPECTED_UNITS["throughput_bytes"]:
                    errors.append(
                        f"Throughput unit mismatch: total_bytes_unit must be '{EXPECTED_UNITS['throughput_bytes']}', "
                        f"got '{tp.get('total_bytes_unit')}'"
                    )
                summary["throughput_rps"] = tp.get("records_per_second", 0.0)

            # Latency
            lat = measurements.get("latency", {})
            if not isinstance(lat, dict):
                errors.append("measurements.latency must be an object")
            else:
                for lat_key in (
                    "sample_count",
                    "unit",
                    "p50",
                    "p90",
                    "p95",
                    "p99",
                    "p99_9",
                    "min",
                    "max",
                    "mean",
                    "stddev",
                    "confidence_interval_95",
                    "raw_histogram",
                ):
                    if lat_key not in lat:
                        errors.append(f"Missing required measurements.latency field: '{lat_key}'")
                if lat.get("unit") != EXPECTED_UNITS["latency"]:
                    errors.append(
                        f"Latency unit mismatch: unit must be '{EXPECTED_UNITS['latency']}', got '{lat.get('unit')}'"
                    )
                ci95 = lat.get("confidence_interval_95", {})
                if isinstance(ci95, dict):
                    if ci95.get("unit") != EXPECTED_UNITS["latency"]:
                        errors.append(
                            f"Latency CI unit mismatch: must be '{EXPECTED_UNITS['latency']}', got '{ci95.get('unit')}'"
                        )
                raw_hist = lat.get("raw_histogram", {})
                if isinstance(raw_hist, dict):
                    if raw_hist.get("bucket_unit") != EXPECTED_UNITS["latency"]:
                        errors.append(
                            f"Histogram bucket unit mismatch: must be '{EXPECTED_UNITS['latency']}', got '{raw_hist.get('bucket_unit')}'"
                        )
                    buckets = raw_hist.get("buckets")
                    if not isinstance(buckets, list) or len(buckets) == 0:
                        errors.append("raw_histogram.buckets must be a non-empty list of histogram buckets")
                summary["latency_p99_us"] = lat.get("p99", 0.0)

            # Client Resources
            c_res = measurements.get("client_resources", {})
            if not isinstance(c_res, dict):
                errors.append("measurements.client_resources must be an object")
            else:
                for cr_key in (
                    "cpu_utilization_pct",
                    "cpu_unit",
                    "user_cpu_seconds",
                    "system_cpu_seconds",
                    "cpu_seconds_unit",
                    "allocations",
                    "rss",
                    "threads_count",
                ):
                    if cr_key not in c_res:
                        errors.append(f"Missing required measurements.client_resources field: '{cr_key}'")
                if c_res.get("cpu_unit") != EXPECTED_UNITS["cpu"]:
                    errors.append(
                        f"Client CPU unit mismatch: must be '{EXPECTED_UNITS['cpu']}', got '{c_res.get('cpu_unit')}'"
                    )
                if c_res.get("cpu_seconds_unit") != EXPECTED_UNITS["cpu_seconds"]:
                    errors.append(
                        f"Client CPU seconds unit mismatch: must be '{EXPECTED_UNITS['cpu_seconds']}', got '{c_res.get('cpu_seconds_unit')}'"
                    )
                allocs = c_res.get("allocations", {})
                if isinstance(allocs, dict):
                    if allocs.get("unit") != EXPECTED_UNITS["memory"]:
                        errors.append(
                            f"Allocations unit mismatch: must be '{EXPECTED_UNITS['memory']}', got '{allocs.get('unit')}'"
                        )
                    for ak in ("total_allocated_bytes", "allocation_count"):
                        if ak not in allocs:
                            errors.append(f"Missing allocations.{ak}")
                rss = c_res.get("rss", {})
                if isinstance(rss, dict):
                    if rss.get("unit") != EXPECTED_UNITS["rss"]:
                        errors.append(
                            f"Client RSS unit mismatch: must be '{EXPECTED_UNITS['rss']}', got '{rss.get('unit')}'"
                        )
                    for rk in ("peak_rss_bytes", "average_rss_bytes"):
                        if rk not in rss:
                            errors.append(f"Missing client_resources.rss.{rk}")

            # Broker Resources
            b_res = measurements.get("broker_resources", {})
            if not isinstance(b_res, dict):
                errors.append("measurements.broker_resources must be an object")
            else:
                for br_key in (
                    "cpu_utilization_pct",
                    "cpu_unit",
                    "peak_rss_bytes",
                    "rss_unit",
                    "disk_write_bytes",
                    "disk_write_unit",
                ):
                    if br_key not in b_res:
                        errors.append(f"Missing required measurements.broker_resources field: '{br_key}'")
                if b_res.get("cpu_unit") != EXPECTED_UNITS["cpu"]:
                    errors.append(
                        f"Broker CPU unit mismatch: must be '{EXPECTED_UNITS['cpu']}', got '{b_res.get('cpu_unit')}'"
                    )
                if b_res.get("rss_unit") != EXPECTED_UNITS["rss"]:
                    errors.append(
                        f"Broker RSS unit mismatch: must be '{EXPECTED_UNITS['rss']}', got '{b_res.get('rss_unit')}'"
                    )
                if b_res.get("disk_write_unit") != EXPECTED_UNITS["memory"]:
                    errors.append(
                        f"Broker disk write unit mismatch: must be '{EXPECTED_UNITS['memory']}', got '{b_res.get('disk_write_unit')}'"
                    )

            # Errors list
            err_list = measurements.get("errors")
            if not isinstance(err_list, list):
                errors.append("measurements.errors must be an array of error events")

            # Consumer lag (if present)
            if "consumer_lag" in measurements:
                c_lag = measurements.get("consumer_lag", {})
                if isinstance(c_lag, dict):
                    if c_lag.get("lag_unit") != EXPECTED_UNITS["lag"]:
                        errors.append(
                            f"Consumer lag unit mismatch: must be '{EXPECTED_UNITS['lag']}', got '{c_lag.get('lag_unit')}'"
                        )
                    if "records_lag" not in c_lag or "partition_lags" not in c_lag:
                        errors.append("measurements.consumer_lag missing records_lag or partition_lags")

        # 6. Mismatched Acks Verification
        # Check scenario equal_semantics vs effective configuration
        scenario_acks = normalize_acks(scenario.get("equal_semantics", {}).get("acks"))
        config_acks = normalize_acks(
            provenance.get("config", {}).get("effective_settings", {}).get("acks")
        )
        if scenario_acks is not None and config_acks is not None:
            if scenario_acks != config_acks:
                errors.append(
                    f"Mismatched acks: scenario equal_semantics requires acks={scenario_acks}, "
                    f"but client effective_settings configured acks={config_acks}."
                )

        # 7. Integrity & High-Watermark Audit Verification
        # Requirements: "The validator must reject a fixture with excellent throughput but missing record IDs or mismatched acks."
        integrity = data.get("integrity", {})
        if not isinstance(integrity, dict):
            errors.append("'integrity' must be an object")
        else:
            summary["integrity_verified"] = bool(integrity.get("verified"))
            if integrity.get("integrity_failure") is True:
                errors.append("Integrity failure: result indicates integrity_failure=true")

            # High-watermark audit check
            hw_audit = integrity.get("high_watermark_audit", {})
            if not isinstance(hw_audit, dict):
                errors.append("integrity.high_watermark_audit must be an object")
            else:
                for hw_k in ("partitions", "total_offset_delta", "matches_acknowledged"):
                    if hw_k not in hw_audit:
                        errors.append(f"Missing integrity.high_watermark_audit field: '{hw_k}'")
                offset_delta = hw_audit.get("total_offset_delta", 0)
                acked_count = outcomes.get("acknowledged", 0) if isinstance(outcomes, dict) else 0

                # If produce scenario with acknowledged records, high-watermark delta MUST match acknowledged count!
                scen_profile = scenario.get("profile")
                if scen_profile in ("bulk", "low-latency", "transactional", "secure") and acked_count > 0:
                    if offset_delta != acked_count:
                        errors.append(
                            f"Mismatched acks: broker high-watermark offset delta ({offset_delta}) "
                            f"does not match acknowledged record count ({acked_count}). "
                            "Every acknowledged record must be independently proven by log end offset deltas."
                        )
                    if hw_audit.get("matches_acknowledged") is not True:
                        errors.append("integrity.high_watermark_audit.matches_acknowledged must be true")

            # Record IDs and Payload Verification
            record_ids = integrity.get("record_ids", {})
            if not isinstance(record_ids, dict):
                errors.append("Missing required integrity.record_ids object")
            else:
                for rk in (
                    "start_id",
                    "end_id",
                    "expected_count",
                    "verified_count",
                    "missing_ids_count",
                    "duplicate_ids_count",
                    "checksum_algorithm",
                    "payload_checksum_matches",
                ):
                    if rk not in record_ids:
                        errors.append(f"Missing required integrity.record_ids field: '{rk}'")

                expected = record_ids.get("expected_count", 0)
                verified = record_ids.get("verified_count", 0)
                missing = record_ids.get("missing_ids_count", 0)
                payload_ok = record_ids.get("payload_checksum_matches")

                acked_count = outcomes.get("acknowledged", 0) if isinstance(outcomes, dict) else 0
                consumed_count = outcomes.get("consumed", 0) if isinstance(outcomes, dict) else 0
                target_count = acked_count if acked_count > 0 else consumed_count

                if target_count > 0:
                    if expected != target_count:
                        errors.append(
                            f"Record ID accounting error: expected_count ({expected}) != target record count ({target_count})"
                        )
                    if verified != expected:
                        errors.append(
                            f"Integrity check failed: verified record IDs ({verified}) != expected ({expected})"
                        )
                    if missing > 0:
                        errors.append(
                            f"Integrity check failed: missing {missing} record IDs. "
                            "Throughput numbers cannot be accepted without 100% record ID verification."
                        )
                    if payload_ok is not True:
                        errors.append("Integrity check failed: payload_checksum_matches is false")

        # 8. Repetition History & Failed Attempts Non-Erasure
        # Requirements: "Preserve failed attempts; a later success does not erase an earlier integrity failure."
        # Contract Section 4.6: "A Failed Cell Stays Failed: If any repetition encounters an unhandled broker error,
        # dropped record, or panic, that repetition is marked FAILED. No Rerun Erasure."
        rep_hist = data.get("repetition_history", {})
        if not isinstance(rep_hist, dict):
            errors.append("'repetition_history' must be an object")
        else:
            for rh_key in ("total_attempts", "failed_attempts", "attempts"):
                if rh_key not in rep_hist:
                    errors.append(f"Missing required repetition_history field: '{rh_key}'")

            attempts = rep_hist.get("attempts")
            if not isinstance(attempts, list) or len(attempts) == 0:
                errors.append("repetition_history.attempts must be a non-empty list of attempts")
            else:
                total_attempts = rep_hist.get("total_attempts", 0)
                declared_failed = rep_hist.get("failed_attempts", 0)
                if len(attempts) != total_attempts:
                    errors.append(
                        f"Repetition accounting error: total_attempts ({total_attempts}) != len(attempts) ({len(attempts)})"
                    )

                actual_failed = 0
                has_integrity_failure_in_attempts = False
                for idx, att in enumerate(attempts):
                    if not isinstance(att, dict):
                        errors.append(f"Attempt #{idx} must be an object")
                        continue
                    status = att.get("status")
                    if status != "passed_measurement":
                        actual_failed += 1
                    if att.get("integrity_failure") is True or status == "failed_integrity":
                        has_integrity_failure_in_attempts = True

                if declared_failed != actual_failed:
                    errors.append(
                        f"Rerun accounting violation: declared failed_attempts ({declared_failed}) "
                        f"!= counted failed attempts ({actual_failed}). Failed attempts must be preserved."
                    )

                summary["has_retained_integrity_failure"] = has_integrity_failure_in_attempts
                if has_integrity_failure_in_attempts:
                    if require_clean:
                        errors.append(
                            "Integrity policy violation: repetition history contains a failed attempt "
                            "with an integrity failure. A subsequent successful rerun does not erase an earlier integrity failure."
                        )

        is_valid = len(errors) == 0
        return is_valid, errors, summary


def format_report_summary(data: Dict[str, Any], errors: List[str]) -> str:
    """Format human-readable summary report of benchmark result."""
    lines = []
    lines.append("=" * 78)
    lines.append("PARTITIONLINE BENCHMARK RESULT & PROVENANCE REPORT")
    lines.append("=" * 78)

    scenario = data.get("scenario", {})
    provenance = data.get("provenance", {})
    outcomes = data.get("outcomes", {})
    measurements = data.get("measurements", {})
    tp = measurements.get("throughput", {})
    lat = measurements.get("latency", {})
    cr = measurements.get("client_resources", {})
    br = measurements.get("broker_resources", {})
    integrity = data.get("integrity", {})
    rep_hist = data.get("repetition_history", {})

    lines.append(f"Scenario:    {scenario.get('scenario_id', 'UNKNOWN')}")
    lines.append(f"Profile:     {scenario.get('profile', 'UNKNOWN')} ({scenario.get('tier', 'UNKNOWN')} tier)")
    lines.append(f"Peer:        {scenario.get('peer', 'UNKNOWN')}")
    lines.append(f"Disposition: {scenario.get('cell_disposition', 'UNKNOWN')}")
    lines.append(f"Suite HOLD:  {data.get('suite_hold', {}).get('status', 'UNKNOWN')}")
    lines.append("-" * 78)

    # Provenance
    src = provenance.get("source", {})
    broker = provenance.get("broker", {})
    host = provenance.get("host", {})
    cpu = host.get("cpu", {})
    lines.append("PROVENANCE:")
    lines.append(f"  Source:     {src.get('git_commit', 'N/A')} on branch '{src.get('git_branch', 'N/A')}'")
    lines.append(f"  Binary:     {provenance.get('binary', {}).get('name', 'N/A')} (sha256: {provenance.get('binary', {}).get('sha256', 'N/A')[:16]}...)")
    lines.append(f"  Broker:     {broker.get('image', 'N/A')} (v{broker.get('version', 'N/A')}, {broker.get('mode', 'N/A')})")
    lines.append(f"  Host:       {host.get('hostname', 'N/A')} ({host.get('os', 'N/A')}, {host.get('arch', 'N/A')})")
    lines.append(f"  CPU:        {cpu.get('model', 'N/A')} ({cpu.get('physical_cores', 'N/A')}c/{cpu.get('logical_cores', 'N/A')}t)")
    lines.append("-" * 78)

    # Outcomes
    lines.append("OUTCOMES ACCOUNTING:")
    lines.append(
        f"  Offered:      {outcomes.get('offered', 0):>10,d}  |  Accepted:     {outcomes.get('accepted', 0):>10,d}"
    )
    lines.append(
        f"  Acknowledged: {outcomes.get('acknowledged', 0):>10,d}  |  Consumed:     {outcomes.get('consumed', 0):>10,d}"
    )
    lines.append(
        f"  Rejected:     {outcomes.get('rejected', 0):>10,d}  |  Timed Out:    {outcomes.get('timed_out', 0):>10,d}"
    )
    lines.append(
        f"  Unknown:      {outcomes.get('unknown', 0):>10,d}"
    )
    lines.append("-" * 78)

    # Performance
    lines.append("PERFORMANCE MEASUREMENTS:")
    lines.append(
        f"  Throughput:   {tp.get('records_per_second', 0.0):>12,.2f} {tp.get('records_per_second_unit', '')} "
        f"({tp.get('megabytes_per_second', 0.0):.2f} {tp.get('megabytes_per_second_unit', '')})"
    )
    lines.append(
        f"  Latency (us): p50={lat.get('p50', 0):.1f}  p90={lat.get('p90', 0):.1f}  "
        f"p95={lat.get('p95', 0):.1f}  p99={lat.get('p99', 0):.1f}  p99.9={lat.get('p99_9', 0):.1f}"
    )
    lines.append(
        f"  Client Res:   CPU={cr.get('cpu_utilization_pct', 0):.1f}%  "
        f"Peak RSS={cr.get('rss', {}).get('peak_rss_bytes', 0) / (1024*1024):.2f} MB  "
        f"Allocations={cr.get('allocations', {}).get('allocation_count', 0):,d}"
    )
    lines.append(
        f"  Broker Res:   CPU={br.get('cpu_utilization_pct', 0):.1f}%  "
        f"Peak RSS={br.get('peak_rss_bytes', 0) / (1024*1024):.2f} MB"
    )
    lines.append("-" * 78)

    # Integrity
    hw = integrity.get("high_watermark_audit", {})
    rec_ids = integrity.get("record_ids", {})
    lines.append("INTEGRITY & VERIFICATION:")
    lines.append(
        f"  HW Delta:     {hw.get('total_offset_delta', 0):,d} (matches acknowledged: {hw.get('matches_acknowledged', False)})"
    )
    lines.append(
        f"  Record IDs:   Verified={rec_ids.get('verified_count', 0):,d}/{rec_ids.get('expected_count', 0):,d}  "
        f"Missing={rec_ids.get('missing_ids_count', 0)}  Payload OK={rec_ids.get('payload_checksum_matches', False)}"
    )
    lines.append(
        f"  Attempts:     Total={rep_hist.get('total_attempts', 0)}  Failed={rep_hist.get('failed_attempts', 0)}"
    )
    lines.append("-" * 78)

    if errors:
        lines.append(f"VALIDATION FAILED ({len(errors)} error(s)):")
        for err in errors:
            lines.append(f"  [FAIL] {err}")
    else:
        lines.append("VALIDATION PASSED: Result conforms strictly to fail-closed result schema.")
    lines.append("=" * 78)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate and report partitionline raw benchmark result documents."
    )
    parser.add_argument("result_file", type=Path, help="Path to benchmark result JSON file")
    parser.add_argument(
        "--schema",
        type=Path,
        default=None,
        help="Path to benchmarks/result-schema.json (defaults to repo schema if available)",
    )
    parser.add_argument(
        "--require-clean",
        action="store_true",
        help="Fail closed if any prior attempt suffered an integrity failure (no rerun erasure)",
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Output validation result as JSON",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress human-readable summary output",
    )

    args = parser.parse_args()

    if not args.result_file.exists():
        sys.stderr.write(f"Error: result file not found: {args.result_file}\n")
        return 2

    # Locate default schema if not provided
    schema_path = args.schema
    if schema_path is None:
        default_schema = (
            Path(__file__).resolve().parent.parent / "benchmarks" / "result-schema.json"
        )
        if default_schema.is_file():
            schema_path = default_schema

    try:
        content = args.result_file.read_text(encoding="utf-8")
        data = json.loads(content)
    except Exception as e:
        sys.stderr.write(f"Error reading JSON from {args.result_file}: {e}\n")
        return 2

    validator = BenchmarkValidator(schema_path=schema_path)
    is_valid, errors, summary = validator.validate(data, require_clean=args.require_clean)

    if args.json_output:
        out_doc = {
            "valid": is_valid,
            "error_count": len(errors),
            "errors": errors,
            "summary": summary,
        }
        print(json.dumps(out_doc, indent=2))
    elif not args.quiet:
        print(format_report_summary(data, errors))

    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())

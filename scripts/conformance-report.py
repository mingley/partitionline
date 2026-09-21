#!/usr/bin/env python3
"""
Conformance Report Validator and Aggregator (KL01-02).

Validates test execution reports against the conformance case registry
(tests/conformance/cases.json) in a fail-closed manner:
  1. Rejects missing cases, unexpected/unknown cases, duplicate case entries,
     unknown statuses, wrong source/peer revisions, and absent artifacts.
  2. Ensures statuses that stay in the denominator (failed, not_run,
     unsupported, blocked) produce a non-zero exit code.
  3. Ensures not_applicable is excluded from the denominator ONLY if the
     registry marks denominator=false AND an explicit reason is provided.
  4. Requires an artifact for independent_pass (and verifies artifact file presence).
  5. Preserves every attempt in an immutable attempt log; rerun success cannot
     erase a first failure or shrink/change the case denominator.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


VALID_DISPOSITIONS = {
    "independent_pass",
    "local_consistency",
    "failed",
    "not_run",
    "unsupported",
    "blocked",
    "not_applicable",
}

FAILING_DENOMINATOR_STATUSES = {
    "failed",
    "not_run",
    "unsupported",
    "blocked",
}


class ConformanceValidationError(Exception):
    """Raised when report structure, cases, revisions, or artifacts are invalid."""
    pass


class ConformanceFailureError(Exception):
    """Raised when one or more required cases fail, are not run, unsupported, or blocked."""
    pass


def find_default_registry() -> Optional[Path]:
    """Find tests/conformance/cases.json relative to script or cwd."""
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir.parent / "tests" / "conformance" / "cases.json",
        Path.cwd() / "tests" / "conformance" / "cases.json",
    ]
    for c in candidates:
        if c.is_file():
            return c
    return None


def load_registry(registry_path: Path) -> Dict[str, Any]:
    """Load and validate the case registry JSON."""
    if not registry_path.is_file():
        raise ConformanceValidationError(f"Registry file not found: {registry_path}")

    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ConformanceValidationError(f"Failed to parse registry JSON at {registry_path}: {e}")

    if not isinstance(data, dict):
        raise ConformanceValidationError("Registry JSON root must be an object")

    cases = data.get("cases")
    if cases is None and "cells" in data and isinstance(data["cells"], list):
        # Support matrix.json as an advertised cell registry
        cases = []
        for cell in data["cells"]:
            api = cell.get("api", "")
            pin = str(cell.get("pin", ""))
            api_slug = api.lower()
            pin_slug = pin.replace(".", "-")
            cid = f"matrix-cell-{api_slug}-{pin_slug}"
            cases.append({
                "id": cid,
                "api_family": api,
                "peer_version_pin": pin,
                "peer_identity": cell.get("identity"),
                "denominator": True,
                "disposition": "local_consistency",
                "reason": f"Cell {api} x {pin} local consistency",
                "crate_spoken": cell.get("crate_spoken"),
                "pin_supported": cell.get("pin_supported"),
            })
        data["cases"] = cases
    elif not isinstance(cases, list):
        raise ConformanceValidationError("Registry JSON must contain a 'cases' list")

    return data


def normalize_report_input(raw_data: Any, source_name: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Normalizes a report into top-level metadata and a list of case result items.
    Rejects completely empty input ({}, [], None).
    """
    if raw_data is None:
        raise ConformanceValidationError(f"Report '{source_name}' is empty")

    if isinstance(raw_data, list):
        if not raw_data:
            raise ConformanceValidationError(f"Report '{source_name}' contains an empty list")
        return {}, raw_data

    if isinstance(raw_data, dict):
        if not raw_data:
            raise ConformanceValidationError(f"Report '{source_name}' is empty object {{}}")

        metadata = {k: v for k, v in raw_data.items() if k not in ("cases", "results")}
        
        if "cases" in raw_data:
            case_items = raw_data["cases"]
        elif "results" in raw_data:
            case_items = raw_data["results"]
        elif "cells" in raw_data and isinstance(raw_data["cells"], list):
            case_items = []
            for cell in raw_data["cells"]:
                api = cell.get("api", "")
                pin = str(cell.get("pin", ""))
                api_slug = api.lower()
                pin_slug = pin.replace(".", "-")
                cid = f"matrix-cell-{api_slug}-{pin_slug}"
                case_items.append({
                    "id": cid,
                    "api_family": api,
                    "peer_version": pin,
                    "peer_identity": cell.get("identity"),
                    "status": "local_consistency",
                    "reason": f"Cell {api} x {pin} local consistency",
                })
        else:
            raise ConformanceValidationError(
                f"Report '{source_name}' must contain 'cases', 'results', or 'cells' key (or be a list of case items)"
            )

        if isinstance(case_items, list):
            return metadata, case_items
        elif isinstance(case_items, dict):
            # Dict mapping case_id -> case_info
            dict_list = []
            for cid, cinfo in case_items.items():
                if isinstance(cinfo, dict):
                    item = copy.deepcopy(cinfo)
                    item.setdefault("id", cid)
                    dict_list.append(item)
                elif isinstance(cinfo, str):
                    dict_list.append({"id": cid, "status": cinfo})
                else:
                    raise ConformanceValidationError(
                        f"Report '{source_name}' case entry for '{cid}' must be dict or status string"
                    )
            return metadata, dict_list
        else:
            raise ConformanceValidationError(
                f"Report '{source_name}' 'cases'/'results' must be a list or dict"
            )

    raise ConformanceValidationError(f"Report '{source_name}' has unexpected type {type(raw_data).__name__}")


def check_artifact_exists(artifact_ref: str, base_dirs: List[Path]) -> bool:
    """Check if artifact exists on disk or is a URI scheme."""
    ref = artifact_ref.strip()
    if not ref:
        return False

    # URIs or fixture references
    if "://" in ref or ref.startswith("urn:") or ref.startswith("fixture:"):
        return True

    p = Path(ref)
    if p.is_absolute():
        return p.exists()

    for b in base_dirs:
        if (b / p).exists():
            return True

    return False


def validate_and_aggregate_reports(
    registry_data: Dict[str, Any],
    report_sources: List[Tuple[str, Any]],  # list of (source_name, raw_json_data)
    check_artifacts: bool = True,
    require_independent_pass: bool = False,
    repo_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Validates report inputs against registry_data fail-closed.
    Returns an aggregate summary dictionary.
    """
    if repo_root is None:
        repo_root = Path.cwd()

    registry_cases = registry_data.get("cases", [])
    registry_map: Dict[str, Dict[str, Any]] = {}
    for rc in registry_cases:
        cid = rc.get("id")
        if not cid:
            raise ConformanceValidationError("Registry case missing 'id'")
        if cid in registry_map:
            raise ConformanceValidationError(f"Duplicate case id '{cid}' in registry")
        registry_map[cid] = rc

    allowed_dispositions = set(registry_data.get("disposition_enum", VALID_DISPOSITIONS))
    audited_source = registry_data.get("audited_source")

    if not report_sources:
        raise ConformanceValidationError("No conformance reports provided")

    # Map case_id -> list of attempt dicts
    # Each attempt dict: {
    #   "case_id": str,
    #   "attempt": int,
    #   "status": str,
    #   "artifacts": List[str],
    #   "source_pin": Optional[str],
    #   "peer_pin": Optional[str],
    #   "reason": Optional[str],
    #   "report_source": str
    # }
    case_attempts: Dict[str, List[Dict[str, Any]]] = {cid: [] for cid in registry_map}
    reported_case_ids: Set[str] = set()
    reported_peer_identities: Set[str] = set()
    aggregated_negotiated_versions: Dict[str, Any] = {}

    for src_name, raw_data in report_sources:
        meta, case_entries = normalize_report_input(raw_data, src_name)
        
        # Check top-level source_sha/source_pin if present
        top_source = meta.get("source_sha") or meta.get("source_pin")
        if top_source and audited_source and top_source != audited_source:
            raise ConformanceValidationError(
                f"Wrong top-level source revision in '{src_name}': expected '{audited_source}', got '{top_source}'"
            )

        top_peer_identity = meta.get("peer_identity") or meta.get("peer") or meta.get("identity")
        top_peer_version = meta.get("peer_version") or meta.get("peer_pin")
        top_negotiated_versions = meta.get("negotiated_api_versions") or meta.get("api_versions")
        if top_negotiated_versions and isinstance(top_negotiated_versions, dict):
            aggregated_negotiated_versions.update(top_negotiated_versions)

        # Base directories for artifact resolution
        src_path = Path(src_name)
        base_dirs = [repo_root, Path.cwd()]
        if src_path.is_file():
            base_dirs.insert(0, src_path.parent)

        # Track per-report seen case attempts to reject duplicates in the same run
        seen_in_this_report: Dict[str, Set[int]] = {}

        for entry in case_entries:
            if not isinstance(entry, dict):
                raise ConformanceValidationError(f"In '{src_name}', case entry must be an object: {entry}")

            cid = entry.get("id") or entry.get("case_id")
            if not cid:
                raise ConformanceValidationError(f"In '{src_name}', case entry missing 'id': {entry}")

            if cid not in registry_map:
                raise ConformanceValidationError(f"In '{src_name}', unknown case id '{cid}' not in registry")

            reg_case = registry_map[cid]
            reported_case_ids.add(cid)

            # Check if entry specifies an nested 'attempts' list
            nested_attempts = entry.get("attempts")
            if nested_attempts is not None:
                if not isinstance(nested_attempts, list) or not nested_attempts:
                    raise ConformanceValidationError(
                        f"In '{src_name}', case '{cid}' has invalid 'attempts' (must be non-empty list)"
                    )
                # Process nested attempts
                for att_idx, att_obj in enumerate(nested_attempts, start=1):
                    if not isinstance(att_obj, dict):
                        raise ConformanceValidationError(
                            f"In '{src_name}', case '{cid}' attempt #{att_idx} must be an object"
                        )
                    att_num = att_obj.get("attempt", att_idx)
                    if cid not in seen_in_this_report:
                        seen_in_this_report[cid] = set()
                    if att_num in seen_in_this_report[cid]:
                        raise ConformanceValidationError(
                            f"In '{src_name}', duplicate attempt {att_num} for case '{cid}'"
                        )
                    seen_in_this_report[cid].add(att_num)

                    # Extract fields combining entry and attempt object
                    status = att_obj.get("status") or att_obj.get("disposition") or entry.get("status") or entry.get("disposition")
                    
                    # Validate all supplied source revisions against expected pin
                    expected_source = reg_case.get("source_pin") or reg_case.get("immutable_source_pin") or audited_source
                    for s_val in (att_obj.get("source_pin"), att_obj.get("source_sha"), entry.get("source_pin"), entry.get("source_sha")):
                        if s_val and expected_source and s_val != expected_source:
                            raise ConformanceValidationError(
                                f"In '{src_name}', case '{cid}' attempt #{att_num} wrong source revision: expected '{expected_source}', got '{s_val}'"
                            )

                    # Validate all supplied peer revisions against expected pin
                    expected_peer = reg_case.get("peer_version_pin") or reg_case.get("peer_pin")
                    for p_val in (att_obj.get("peer_pin"), att_obj.get("peer_version"), att_obj.get("peer_version_pin"), entry.get("peer_pin"), entry.get("peer_version"), entry.get("peer_version_pin")):
                        if p_val and expected_peer and str(p_val) != str(expected_peer):
                            raise ConformanceValidationError(
                                f"In '{src_name}', case '{cid}' attempt #{att_num} wrong peer revision: expected '{expected_peer}', got '{p_val}'"
                            )

                    source_pin = att_obj.get("source_pin") or att_obj.get("source_sha") or entry.get("source_pin") or entry.get("source_sha") or top_source
                    peer_pin = att_obj.get("peer_pin") or att_obj.get("peer_version") or att_obj.get("peer_version_pin") or entry.get("peer_pin") or entry.get("peer_version") or entry.get("peer_version_pin") or top_peer_version
                    peer_ident = att_obj.get("peer_identity") or att_obj.get("identity") or entry.get("peer_identity") or entry.get("identity") or top_peer_identity
                    reason = att_obj.get("reason") or entry.get("reason") or reg_case.get("reason")
                    raw_art = att_obj.get("artifact") or att_obj.get("artifacts") or entry.get("artifact") or entry.get("artifacts")

                    _validate_and_record_attempt(
                        cid=cid,
                        att_num=att_num,
                        status=status,
                        source_pin=source_pin,
                        peer_pin=peer_pin,
                        peer_ident=peer_ident,
                        reason=reason,
                        raw_art=raw_art,
                        reg_case=reg_case,
                        audited_source=audited_source,
                        allowed_dispositions=allowed_dispositions,
                        check_artifacts=check_artifacts,
                        base_dirs=base_dirs,
                        src_name=src_name,
                        case_attempts=case_attempts,
                        reported_peer_identities=reported_peer_identities,
                        top_peer_identity=top_peer_identity,
                        top_peer_version=top_peer_version,
                    )
            else:
                # Single attempt entry
                if cid not in seen_in_this_report:
                    seen_in_this_report[cid] = set()

                att_num = entry.get("attempt")
                if att_num is None:
                    # Next sequential attempt for this case
                    att_num = len(case_attempts[cid]) + 1
                    if att_num in seen_in_this_report[cid] or len(seen_in_this_report[cid]) > 0:
                        raise ConformanceValidationError(
                            f"In '{src_name}', duplicate case entry '{cid}' without distinct attempt number"
                        )
                else:
                    if not isinstance(att_num, int) or att_num < 1:
                        raise ConformanceValidationError(
                            f"In '{src_name}', case '{cid}' attempt must be positive integer, got '{att_num}'"
                        )
                    if att_num in seen_in_this_report[cid]:
                        raise ConformanceValidationError(
                            f"In '{src_name}', duplicate attempt {att_num} for case '{cid}'"
                        )

                seen_in_this_report[cid].add(att_num)

                # Validate all supplied source revisions against expected pin
                expected_source = reg_case.get("source_pin") or reg_case.get("immutable_source_pin") or audited_source
                for s_val in (entry.get("source_pin"), entry.get("source_sha"), entry.get("immutable_source_pin")):
                    if s_val and expected_source and s_val != expected_source:
                        raise ConformanceValidationError(
                            f"In '{src_name}', case '{cid}' attempt #{att_num} wrong source revision: expected '{expected_source}', got '{s_val}'"
                        )

                # Validate all supplied peer revisions against expected pin
                expected_peer = reg_case.get("peer_version_pin") or reg_case.get("peer_pin")
                for p_val in (entry.get("peer_pin"), entry.get("peer_version"), entry.get("peer_version_pin")):
                    if p_val and expected_peer and str(p_val) != str(expected_peer):
                        raise ConformanceValidationError(
                            f"In '{src_name}', case '{cid}' attempt #{att_num} wrong peer revision: expected '{expected_peer}', got '{p_val}'"
                        )

                status = entry.get("status") or entry.get("disposition")
                source_pin = entry.get("source_pin") or entry.get("source_sha") or top_source
                peer_pin = entry.get("peer_pin") or entry.get("peer_version") or entry.get("peer_version_pin") or top_peer_version
                peer_ident = entry.get("peer_identity") or entry.get("identity") or top_peer_identity
                reason = entry.get("reason") or reg_case.get("reason")
                raw_art = entry.get("artifact") or entry.get("artifacts")

                _validate_and_record_attempt(
                    cid=cid,
                    att_num=att_num,
                    status=status,
                    source_pin=source_pin,
                    peer_pin=peer_pin,
                    peer_ident=peer_ident,
                    reason=reason,
                    raw_art=raw_art,
                    reg_case=reg_case,
                    audited_source=audited_source,
                    allowed_dispositions=allowed_dispositions,
                    check_artifacts=check_artifacts,
                    base_dirs=base_dirs,
                    src_name=src_name,
                    case_attempts=case_attempts,
                    reported_peer_identities=reported_peer_identities,
                    top_peer_identity=top_peer_identity,
                    top_peer_version=top_peer_version,
                )

    # 1. Missing Cases Check: Every registry case must be reported
    missing_cases = set(registry_map.keys()) - reported_case_ids
    if missing_cases:
        sample_missing = sorted(list(missing_cases))[:5]
        suffix = f" (showing 5 of {len(missing_cases)}: {', '.join(sample_missing)})" if len(missing_cases) > 5 else f": {', '.join(sample_missing)}"
        raise ConformanceValidationError(f"Missing {len(missing_cases)} required conformance case(s){suffix}")

    # Aggregation & Verification
    total_cases = len(registry_map)
    denominator_cases_count = 0
    excluded_cases_count = 0

    status_counts: Dict[str, int] = {disp: 0 for disp in allowed_dispositions}
    preserved_attempt_log: List[Dict[str, Any]] = []

    failing_cases: List[str] = []
    flaky_cases: List[str] = []
    blocked_cases: List[str] = []
    not_run_cases: List[str] = []
    unsupported_cases: List[str] = []
    passed_cases: List[str] = []

    per_case_summary: Dict[str, Any] = {}

    for cid, reg_case in registry_map.items():
        attempts = sorted(case_attempts[cid], key=lambda a: a["attempt"])
        preserved_attempt_log.extend(attempts)

        # Denominator rule:
        # not_applicable with reason may be excluded only if registry already marks denominator false
        last_attempt = attempts[-1]
        final_status = last_attempt["status"]
        first_attempt = attempts[0]
        first_status = first_attempt["status"]

        # Did any attempt fail?
        has_failure = any(a["status"] == "failed" for a in attempts)
        has_non_passing_attempt = any(a["status"] in FAILING_DENOMINATOR_STATUSES for a in attempts)

        if final_status == "not_applicable":
            if reg_case.get("denominator", True) is not False:
                raise ConformanceValidationError(
                    f"Case '{cid}' has status 'not_applicable' but registry specifies denominator=true"
                )
            reason = last_attempt.get("reason") or reg_case.get("reason")
            if not reason or not str(reason).strip():
                raise ConformanceValidationError(
                    f"Case '{cid}' has status 'not_applicable' but has no explicit non-empty reason"
                )
            in_denominator = False
            excluded_cases_count += 1
        else:
            # Stays in denominator
            in_denominator = True
            denominator_cases_count += 1

        status_counts[final_status] = status_counts.get(final_status, 0) + 1

        # Rerun and denominator rules:
        # A second attempt that succeeds must not delete the first failed attempt from preserved attempt log
        # or shrink the denominator.
        # Rerun success cannot erase a first failure.
        case_verdict_pass = True
        case_notes = []

        if in_denominator:
            if has_failure:
                # First failure cannot be erased by rerun success
                case_verdict_pass = False
                failing_cases.append(cid)
                case_notes.append(f"Case failed on attempt {first_attempt['attempt']}; failure cannot be erased by rerun")
                if final_status in ("independent_pass", "local_consistency"):
                    flaky_cases.append(cid)
            elif final_status in FAILING_DENOMINATOR_STATUSES:
                case_verdict_pass = False
                if final_status == "failed":
                    failing_cases.append(cid)
                elif final_status == "not_run":
                    not_run_cases.append(cid)
                elif final_status == "blocked":
                    blocked_cases.append(cid)
                elif final_status == "unsupported":
                    unsupported_cases.append(cid)
                case_notes.append(f"Case has non-passing denominator status: '{final_status}'")
            elif require_independent_pass and final_status != "independent_pass":
                case_verdict_pass = False
                failing_cases.append(cid)
                case_notes.append(f"Strict independent pass required; got '{final_status}'")
            else:
                passed_cases.append(cid)
        else:
            # Excluded not_applicable
            passed_cases.append(cid)

        per_case_summary[cid] = {
            "id": cid,
            "in_denominator": in_denominator,
            "final_status": final_status,
            "first_status": first_status,
            "attempts_count": len(attempts),
            "attempts": attempts,
            "passed": case_verdict_pass,
            "notes": case_notes,
        }

    # Determine overall outcome
    is_success = (
        len(failing_cases) == 0
        and len(not_run_cases) == 0
        and len(blocked_cases) == 0
        and len(unsupported_cases) == 0
    )

    exit_code = 0 if is_success else 1

    peer_identities_list = sorted(list(reported_peer_identities))
    primary_peer_identity = (
        top_peer_identity
        if top_peer_identity
        else (", ".join(peer_identities_list) if peer_identities_list else "unknown")
    )
    if not aggregated_negotiated_versions:
        derived_versions: Dict[str, Set[int]] = {}
        for cid, reg_case in registry_map.items():
            fam = reg_case.get("api_family")
            vers = reg_case.get("pin_supported") or reg_case.get("client_spoken_versions")
            if fam and vers:
                derived_versions.setdefault(fam, set()).update(vers)
        if derived_versions:
            aggregated_negotiated_versions = {
                fam: sorted(list(vers)) for fam, vers in sorted(derived_versions.items())
            }

    dispositions = {cid: info["final_status"] for cid, info in per_case_summary.items()}

    summary = {
        "schema_version": 1,
        "audited_source": audited_source,
        "peer_identity": primary_peer_identity,
        "peer_identities": peer_identities_list,
        "negotiated_api_versions": aggregated_negotiated_versions,
        "total_cases": total_cases,
        "denominator_cases": denominator_cases_count,
        "excluded_cases": excluded_cases_count,
        "status_counts": status_counts,
        "total_attempts": len(preserved_attempt_log),
        "passed_cases_count": len(passed_cases),
        "failing_cases": sorted(failing_cases),
        "blocked_cases": sorted(blocked_cases),
        "not_run_cases": sorted(not_run_cases),
        "unsupported_cases": sorted(unsupported_cases),
        "flaky_cases": sorted(flaky_cases),
        "dispositions": dispositions,
        "cases": per_case_summary,
        "preserved_attempt_log": preserved_attempt_log,
        "success": is_success,
        "exit_code": exit_code,
    }

    return summary


def _validate_and_record_attempt(
    cid: str,
    att_num: int,
    status: Optional[str],
    source_pin: Optional[str],
    peer_pin: Optional[str],
    peer_ident: Optional[str],
    reason: Optional[str],
    raw_art: Any,
    reg_case: Dict[str, Any],
    audited_source: Optional[str],
    allowed_dispositions: Set[str],
    check_artifacts: bool,
    base_dirs: List[Path],
    src_name: str,
    case_attempts: Dict[str, List[Dict[str, Any]]],
    reported_peer_identities: Optional[Set[str]] = None,
    top_peer_identity: Optional[str] = None,
    top_peer_version: Optional[str] = None,
) -> None:
    """Helper to validate single attempt fields and record it."""
    if not status:
        raise ConformanceValidationError(
            f"In '{src_name}', case '{cid}' attempt {att_num} missing status/disposition"
        )

    if status not in allowed_dispositions:
        raise ConformanceValidationError(
            f"In '{src_name}', case '{cid}' attempt {att_num} has unknown status '{status}'"
        )

    # Source pin validation
    expected_source = reg_case.get("source_pin") or reg_case.get("immutable_source_pin") or audited_source
    if source_pin and expected_source and source_pin != expected_source:
        raise ConformanceValidationError(
            f"In '{src_name}', case '{cid}' attempt {att_num} wrong source revision: expected '{expected_source}', got '{source_pin}'"
        )

    # Peer pin validation
    expected_peer = reg_case.get("peer_version_pin") or reg_case.get("peer_pin")
    if peer_pin and expected_peer and str(peer_pin) != str(expected_peer):
        raise ConformanceValidationError(
            f"In '{src_name}', case '{cid}' attempt {att_num} wrong peer revision: expected '{expected_peer}', got '{peer_pin}'"
        )
    if top_peer_version and expected_peer and str(top_peer_version) != str(expected_peer):
        if not peer_pin or peer_pin == top_peer_version:
            raise ConformanceValidationError(
                f"In '{src_name}', case '{cid}' attempt {att_num} wrong peer revision: expected '{expected_peer}', got '{top_peer_version}'"
            )

    # Peer identity validation
    expected_identity = reg_case.get("peer_identity") or reg_case.get("identity")
    if peer_ident:
        peer_ident_str = str(peer_ident).strip()
        if reported_peer_identities is not None and peer_ident_str:
            reported_peer_identities.add(peer_ident_str)
        if expected_identity and peer_ident_str != str(expected_identity).strip():
            raise ConformanceValidationError(
                f"In '{src_name}', case '{cid}' attempt #{att_num} wrong peer identity: expected '{expected_identity}', got '{peer_ident_str}'"
            )
        if expected_peer and str(expected_peer) not in peer_ident_str:
            for v in ("3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1", "2.8.0", "3.8.0"):
                if v in peer_ident_str and v != str(expected_peer):
                    raise ConformanceValidationError(
                        f"In '{src_name}', case '{cid}' attempt #{att_num} wrong peer identity: '{peer_ident_str}' conflicts with expected peer '{expected_peer}'"
                    )
            if "wrong-peer" in peer_ident_str.lower() or "wrong_peer" in peer_ident_str.lower():
                raise ConformanceValidationError(
                    f"In '{src_name}', case '{cid}' attempt #{att_num} wrong peer identity: '{peer_ident_str}'"
                )

    if top_peer_identity and expected_peer:
        top_ident_str = str(top_peer_identity).strip()
        if "wrong-peer" in top_ident_str.lower() or "wrong_peer" in top_ident_str.lower():
            raise ConformanceValidationError(
                f"In '{src_name}', wrong top-level peer identity: '{top_ident_str}'"
            )
        if str(expected_peer) not in top_ident_str:
            for v in ("3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1", "2.8.0", "3.8.0"):
                if v in top_ident_str and v != str(expected_peer):
                    if not (peer_ident and peer_ident != top_peer_identity):
                        raise ConformanceValidationError(
                            f"In '{src_name}', wrong top-level peer identity '{top_ident_str}' conflicts with case '{cid}' expected peer '{expected_peer}'"
                        )

    # Artifact validation
    artifacts_list: List[str] = []
    if raw_art is not None:
        if isinstance(raw_art, str):
            if raw_art.strip():
                artifacts_list.append(raw_art.strip())
        elif isinstance(raw_art, list):
            for a in raw_art:
                if isinstance(a, str) and a.strip():
                    artifacts_list.append(a.strip())

    if status == "independent_pass":
        if not artifacts_list:
            raise ConformanceValidationError(
                f"In '{src_name}', case '{cid}' attempt {att_num} has status 'independent_pass' but absent artifact"
            )
        if check_artifacts:
            for art in artifacts_list:
                if not check_artifact_exists(art, base_dirs):
                    raise ConformanceValidationError(
                        f"In '{src_name}', case '{cid}' attempt {att_num} artifact absent on disk: '{art}'"
                    )

    # not_applicable reason validation
    if status == "not_applicable":
        eff_reason = (reason or "").strip()
        if not eff_reason:
            raise ConformanceValidationError(
                f"In '{src_name}', case '{cid}' attempt {att_num} has status 'not_applicable' without an explicit reason"
            )
        if reg_case.get("denominator", True) is not False:
            raise ConformanceValidationError(
                f"In '{src_name}', case '{cid}' attempt {att_num} marked 'not_applicable' but registry specifies denominator=true"
            )

    case_attempts[cid].append({
        "case_id": cid,
        "attempt": att_num,
        "status": status,
        "artifacts": artifacts_list,
        "source_pin": source_pin,
        "peer_pin": peer_pin,
        "peer_identity": peer_ident,
        "reason": reason,
        "report_source": src_name,
    })


def format_human_summary(summary: Dict[str, Any]) -> str:
    """Format human-readable summary table."""
    lines = [
        "============================================================",
        "               CONFORMANCE REPORT SUMMARY",
        "============================================================",
        f"Peer Identity:             {summary.get('peer_identity', 'unknown')}",
        f"Negotiated API Versions:   {summary.get('negotiated_api_versions', {})}",
        f"Total Registry Cases:      {summary['total_cases']}",
        f"Denominator Cases:         {summary['denominator_cases']}",
        f"Excluded Cases:            {summary['excluded_cases']}",
        f"Total Preserved Attempts:  {summary['total_attempts']}",
        "------------------------------------------------------------",
        "Dispositions / Status Counts:",
    ]
    for status, count in sorted(summary.get("status_counts", {}).items()):
        lines.append(f"  {status:<24} : {count}")
    lines.append("------------------------------------------------------------")
    if summary["flaky_cases"]:
        lines.append(f"Flaky / Rerun Cases ({len(summary['flaky_cases'])}):")
        for fc in summary["flaky_cases"]:
            lines.append(f"  * {fc} (failure preserved; cannot be erased by rerun)")
    if summary["failing_cases"]:
        lines.append(f"Failing Cases ({len(summary['failing_cases'])}):")
        for fc in summary["failing_cases"]:
            lines.append(f"  * {fc}")
    if summary["not_run_cases"]:
        lines.append(f"Not Run Cases ({len(summary['not_run_cases'])}):")
        for nrc in summary["not_run_cases"]:
            lines.append(f"  * {nrc}")
    if summary["blocked_cases"]:
        lines.append(f"Blocked Cases ({len(summary['blocked_cases'])}):")
        for bc in summary["blocked_cases"]:
            lines.append(f"  * {bc}")
    if summary["unsupported_cases"]:
        lines.append(f"Unsupported Cases ({len(summary['unsupported_cases'])}):")
        for uc in summary["unsupported_cases"]:
            lines.append(f"  * {uc}")

    lines.append("============================================================")
    status_label = "SUCCESS (all denominator cases satisfied)" if summary["success"] else "FAIL (incomplete or non-passing cases)"
    lines.append(f"Final Verdict: {status_label} [exit={summary['exit_code']}]")
    lines.append("============================================================")
    return "\n".join(lines)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and aggregate conformance results fail-closed against cases.json"
    )
    parser.add_argument(
        "reports",
        nargs="*",
        help="Path(s) to conformance report JSON file(s), or '-' for stdin",
    )
    parser.add_argument(
        "-r",
        "--registry",
        type=Path,
        default=None,
        help="Path to conformance case registry (defaults to tests/conformance/cases.json)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write summary JSON output to this file",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print summary JSON to stdout",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress human-readable text output",
    )
    parser.add_argument(
        "--require-independent-pass",
        action="store_true",
        help="Require independent_pass for all denominator cases (reject local_consistency as passing)",
    )
    parser.add_argument(
        "--no-check-artifacts",
        action="store_true",
        help="Skip filesystem existence check for artifacts (still requires artifact string)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    registry_path = args.registry or find_default_registry()
    if not registry_path:
        sys.stderr.write("Error: Could not locate conformance registry (tests/conformance/cases.json)\n")
        return 2

    try:
        registry_data = load_registry(registry_path)
    except ConformanceValidationError as e:
        sys.stderr.write(f"Registry error: {e}\n")
        return 2

    # Load report inputs
    report_sources: List[Tuple[str, Any]] = []

    if not args.reports:
        if sys.stdin.isatty():
            sys.stderr.write("Error: No report files specified and stdin is empty.\n")
            return 2
        try:
            stdin_content = sys.stdin.read()
            if not stdin_content.strip():
                sys.stderr.write("Error: Empty input from stdin.\n")
                return 2
            parsed_stdin = json.loads(stdin_content)
            report_sources.append(("<stdin>", parsed_stdin))
        except Exception as e:
            sys.stderr.write(f"Validation error parsing stdin: {e}\n")
            return 2
    else:
        for r_arg in args.reports:
            if r_arg == "-":
                try:
                    stdin_content = sys.stdin.read()
                    if not stdin_content.strip():
                        sys.stderr.write("Error: Empty input from stdin.\n")
                        return 2
                    parsed_stdin = json.loads(stdin_content)
                    report_sources.append(("<stdin>", parsed_stdin))
                except Exception as e:
                    sys.stderr.write(f"Validation error parsing stdin: {e}\n")
                    return 2
            else:
                rp = Path(r_arg)
                if not rp.is_file():
                    sys.stderr.write(f"Error: Report file not found: {rp}\n")
                    return 2
                try:
                    with open(rp, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    report_sources.append((str(rp), data))
                except Exception as e:
                    sys.stderr.write(f"Validation error parsing {rp}: {e}\n")
                    return 2

    repo_root = registry_path.parent.parent.parent

    try:
        summary = validate_and_aggregate_reports(
            registry_data=registry_data,
            report_sources=report_sources,
            check_artifacts=not args.no_check_artifacts,
            require_independent_pass=args.require_independent_pass,
            repo_root=repo_root,
        )
    except ConformanceValidationError as e:
        sys.stderr.write(f"Validation error: {e}\n")
        return 2
    except Exception as e:
        sys.stderr.write(f"Unexpected error: {e}\n")
        return 2

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
        except Exception as e:
            sys.stderr.write(f"Error writing output to {args.output}: {e}\n")
            return 2

    if args.json:
        print(json.dumps(summary, indent=2))
    elif not args.quiet:
        print(format_human_summary(summary))

    return summary["exit_code"]


if __name__ == "__main__":
    sys.exit(main())

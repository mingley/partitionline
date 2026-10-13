#!/usr/bin/env python3
"""Build and check the broker compatibility matrix without shrinking it.

Every feature in tests/conformance/broker/features.json is emitted for Kafka
4.1.2, 4.2.1, and 4.3.1. Required cells whose qualification is not an
independent pass stay blocking. Removed upstream APIs stay in the denominator
as not applicable. This file does not claim the broker passed a live
Java/C/Rust campaign.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FEATURES = ROOT / "tests" / "conformance" / "broker" / "features.json"
MATRIX = ROOT / "tests" / "conformance" / "broker" / "api-matrix.json"
CELLS = ROOT / "tests" / "conformance" / "current-broker-cells.json"
OUT = ROOT / "tests" / "conformance" / "broker" / "compatibility.json"
RELEASES = ("4.1.2", "4.2.1", "4.3.1")
PINS = {
    "4.1.2": "c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c",
    "4.2.1": "18d5ecd939c8d510fdd72d0abb1f7099659dcd58",
    "4.3.1": "26b251a451ce941d3d7a55e6487bcb7f16b5ad48",
}


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def matrix_versions(matrix: dict) -> dict[tuple[str, int], str]:
    found: dict[tuple[str, int], str] = {}
    for release in matrix["releases"]:
        version = release["version"]
        if version not in RELEASES:
            continue
        if release["commit"] != PINS[version]:
            raise ValueError(f"pin mismatch for {version}")
        for row in release["inventory"]:
            found[(version, row["api_key"])] = row["stable_valid_versions"]
    return found


def feature_versions(feature: dict, release: str) -> str | None:
    ranges = feature.get("version_ranges") or {}
    block = ranges.get(release) or {}
    return block.get("stable") or block.get("request")


def cell_status(feature: dict) -> str:
    if feature.get("upstream_applicability") == "not_applicable_removed_upstream":
        return "not_applicable"
    return "blocking"


def broker_cells(features: dict, matrix: dict) -> list[dict]:
    versions = matrix_versions(matrix)
    cells = []
    for feature in features["features"]:
        for release in RELEASES:
            upstream = feature_versions(feature, release)
            api_key = feature.get("api_key")
            matrix_range = versions.get((release, api_key)) if isinstance(api_key, int) else None
            pin_match = True if matrix_range is None or upstream is None else matrix_range == upstream
            status = cell_status(feature)
            if not pin_match:
                status = "blocking"
            cells.append(
                {
                    "id": feature["id"],
                    "release": release,
                    "api_key": api_key,
                    "name": feature.get("name"),
                    "applicability": feature.get("upstream_applicability"),
                    "implementation": feature.get("implementation"),
                    "qualification": feature.get("qualification"),
                    "upstream_versions": upstream,
                    "matrix_versions": matrix_range,
                    "pin_match": pin_match,
                    "status": status,
                    "blocking": status == "blocking",
                }
            )
    return cells


def client_cells(document: dict) -> list[dict]:
    cells = []
    for cell in document["cells"]:
        if cell["version"] not in RELEASES:
            raise ValueError(f"unexpected client cell {cell['version']}")
        for scenario in cell["required_scenarios"]:
            cells.append(
                {
                    "id": f"client.{cell['version']}.{scenario}",
                    "release": cell["version"],
                    "scenario": scenario,
                    "historical_apache_qualification": cell.get("qualification"),
                    "historical_source_sha": cell.get("qualifying_source_sha"),
                    "status": "blocking",
                    "blocking": True,
                    "reason": "not re-executed against this broker",
                }
            )
    return cells


def build() -> dict:
    features = load(FEATURES)
    matrix = load(MATRIX)
    clients = load(CELLS)
    broker = broker_cells(features, matrix)
    client = client_cells(clients)
    expected = len(features["features"]) * len(RELEASES)
    if len(broker) != expected:
        raise ValueError(f"broker denominator {len(broker)} != {expected}")
    if any(cell["status"] == "passed" for cell in broker + client):
        raise ValueError("unqualified cell marked passed")
    return {
        "schema_version": 1,
        "releases": list(RELEASES),
        "pins": PINS,
        "denominator": len(broker) + len(client),
        "broker_denominator": len(broker),
        "client_denominator": len(client),
        "blocking": sum(1 for cell in broker + client if cell["blocking"]),
        "broker_cells": broker,
        "client_cells": client,
    }


def check(document: dict) -> None:
    expected = build()
    if document["denominator"] != expected["denominator"]:
        raise ValueError("denominator changed")
    if document["broker_denominator"] != expected["broker_denominator"]:
        raise ValueError("broker denominator changed")
    if document["client_denominator"] != expected["client_denominator"]:
        raise ValueError("client denominator changed")
    if document["pins"] != expected["pins"]:
        raise ValueError("release pins changed")
    got = {(cell["id"], cell["release"]) for cell in document["broker_cells"]}
    want = {(cell["id"], cell["release"]) for cell in expected["broker_cells"]}
    if got != want:
        raise ValueError("broker cells were added or removed")
    got_client = {cell["id"] for cell in document["client_cells"]}
    want_client = {cell["id"] for cell in expected["client_cells"]}
    if got_client != want_client:
        raise ValueError("client cells were added or removed")
    for cell in document["broker_cells"] + document["client_cells"]:
        if cell["status"] == "passed" or (cell["blocking"] and cell["status"] == "not_applicable"):
            raise ValueError(f"blocking cell reclassified: {cell['id']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    document = build()
    if args.write:
        OUT.write_text(json.dumps(document, indent=2) + "\n")
    else:
        check(load(OUT))
    print(
        json.dumps(
            {
                "denominator": document["denominator"],
                "broker_denominator": document["broker_denominator"],
                "client_denominator": document["client_denominator"],
                "blocking": document["blocking"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

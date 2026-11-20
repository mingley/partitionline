#!/usr/bin/env python3
"""Paired Registry JSON resource measurements using valid fixture responses."""

import argparse
import hashlib
import json
import os
import random
import re
from pathlib import Path
import statistics
import subprocess
import tempfile


def json_bytes(value, ensure_ascii=False):
    return json.dumps(value, ensure_ascii=ensure_ascii, separators=(",", ":")).encode("utf-8")


def record(fields=2, doc="A measured record from Zürich 東京 🙂."):
    return json_bytes({
        "type": "record",
        "name": "MeasuredRecord",
        "namespace": "example.registry",
        "doc": doc,
        "fields": [
            {"name": f"field_{i:05d}", "type": ["null", "string"], "default": None}
            for i in range(fields)
        ],
    }).decode()


def write_fixture(directory, name, mode, iterations, schema=None, *, size=None,
                  fields=0, refs=0, ensure_ascii=False):
    if size is not None:
        schema = record(doc="")
    body = {
        "schema": schema,
        "schemaType": "AVRO",
        "references": [
            {
                "name": f"example.registry.Reference{i}",
                "subject": f"reference-{i}",
                "version": i % 3 + 1,
            }
            for i in range(refs)
        ],
    }
    if mode == "subject":
        body.update({"subject": "measured-record", "version": 4, "id": 71})
    if fields:
        body["metadata"] = {
            "properties": {f"property-{i:05d}": f"value-{i:05d}" for i in range(fields)}
        }
    if size is not None:
        raw = json_bytes(body)
        doc_bytes = size - len(raw)
        assert doc_bytes > 0
        pattern = "ordinary schema documentation "
        doc = (pattern * (doc_bytes // len(pattern)) + pattern[:doc_bytes % len(pattern)])
        body["schema"] = record(doc=doc)
    data = json_bytes(body, ensure_ascii)
    if size is not None:
        assert len(data) == size
    assert len(data) <= 64 * 1024 * 1024

    # Extract expected fields with Python's standard JSON parser.
    independently_decoded = json.loads(data)
    schema = independently_decoded.pop("schema").encode()
    json.loads(schema)
    independently_decoded.pop("metadata", None)
    independently_decoded.update({"id": 71, "mode": mode})
    fixture = directory / name
    fixture.with_suffix(".json").write_bytes(data)
    fixture.with_suffix(".schema").write_bytes(schema)
    fixture.with_suffix(".expected").write_bytes(json_bytes(independently_decoded))
    manifest = {
        "name": name,
        "mode": mode,
        "iterations": iterations,
        "body_bytes": len(data),
        "body_sha256": hashlib.sha256(data).hexdigest(),
        "schema_bytes": len(schema),
        "schema_sha256": hashlib.sha256(schema).hexdigest(),
        "metadata_keys": fields,
        "references": refs,
    }
    return fixture, manifest


def generate_fixtures(directory):
    def single_body(name, size, iterations):
        return write_fixture(directory, name, "id", iterations, size=size)

    def metadata(name, width, iterations):
        return write_fixture(directory, name, "subject", iterations, record(), fields=width)

    return [
        write_fixture(directory, "small-id", "id", 8_000, record()),
        write_fixture(directory, "small-subject", "subject", 8_000, record(), refs=2),
        write_fixture(directory, "escaped-unicode", "subject", 4_000,
                      record(doc='Quoted "fragment", tab\t, line\n and Zürich 東京 🙂.'), ensure_ascii=True),
        single_body("doc-64k", 64 * 1024, 240),
        single_body("doc-1m", 1024 * 1024, 30),
        single_body("doc-8m", 8 * 1024 * 1024, 6),
        single_body("doc-64m", 64 * 1024 * 1024, 3),
        write_fixture(directory, "avro-512", "id", 240, record(512)),
        write_fixture(directory, "avro-8192", "subject", 36, record(8192)),
        metadata("metadata-63", 63, 500),
        metadata("metadata-256", 256, 180),
        metadata("metadata-4096", 4096, 12),
        metadata("metadata-16384", 16384, 6),
        write_fixture(directory, "references-256", "subject", 160, record(), refs=256),
    ]


MEASURE = re.compile(
    r"REGISTRY_PARSE body=(\d+) schema=(\d+) iterations=(\d+) median_ns=(\d+) max_ns=(\d+) total_ns=(\d+)"
)


def measure(binary, fixture, identity, cpu, rss_path):
    env = dict(os.environ, PL_PARSE_FIXTURE=str(fixture),
               PL_PARSE_REPETITIONS=str(identity["iterations"]))
    rss_path.unlink(missing_ok=True)
    process = subprocess.run(
        ["timeout", "--kill-after=2s", "45s", "/usr/bin/time", "-f", "%M", "-o",
         str(rss_path), "taskset", "-c", str(cpu), str(binary),
         "--exact", "registry::parse_resource_measurement::measure", "--ignored",
         "--nocapture", "--test-threads=1"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False,
    )
    match = MEASURE.search(process.stdout)
    if process.returncode or match is None:
        return {"error": process.returncode, "output": process.stdout}
    body, schema, iterations, median_ns, max_ns, total_ns = map(int, match.groups())
    if (body, schema, iterations) != (
            identity["body_bytes"], identity["schema_bytes"], identity["iterations"]):
        return {"error": "decoded fixture differs", "output": process.stdout}
    peak_rss_kib = int(rss_path.read_text().strip())
    if min(peak_rss_kib, median_ns, max_ns, total_ns) <= 0:
        return {"error": "measurement is missing", "output": process.stdout}
    return {
        "body_bytes": body,
        "schema_bytes": schema,
        "iterations": iterations,
        "median_ns": median_ns,
        "max_ns": max_ns,
        "total_ns": total_ns,
        "peak_rss_kib": peak_rss_kib,
    }


def summarize(samples, randomizer, key):
    before = [sample["baseline"][key] for sample in samples]
    after = [sample["candidate"][key] for sample in samples]
    paired = [(candidate / baseline - 1) * 100 for baseline, candidate in zip(before, after)]
    bootstrap = sorted(
        statistics.median(randomizer.choices(paired, k=len(paired))) for _ in range(16_384)
    )
    return {
        "baseline": statistics.median(before),
        "candidate": statistics.median(after),
        "delta_pct": statistics.median(paired),
        "ci95_pct": [bootstrap[int(0.025 * len(bootstrap))], bootstrap[int(0.975 * len(bootstrap))]],
        "worst_baseline": max(before),
        "worst_candidate": max(after),
        "higher_pairs": sum(delta > 0 for delta in paired),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    cpu = min(os.sched_getaffinity(0))
    print(f"REGISTRY_CPU affinity={cpu} visible_logical={len(os.sched_getaffinity(0))}", flush=True)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    rng = random.Random(20261120)
    with tempfile.TemporaryDirectory(prefix="registry-parse-") as temporary:
        directory = Path(temporary)
        fixtures = generate_fixtures(directory)
        manifest = [identity for _, identity in fixtures]
        (args.out / "fixtures.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print("REGISTRY_FIXTURES " + json.dumps(manifest, separators=(",", ":")), flush=True)
        summaries = []
        with (args.out / "pairs.jsonl").open("w") as raw:
            for fixture, identity in fixtures:
                samples = []
                for pair in range(1, 13):
                    order = ("baseline", "candidate") if pair % 2 else ("candidate", "baseline")
                    result = {"case": identity["name"], "pair": pair, "order": order}
                    for arm in order:
                        result[arm] = measure(binaries[arm], fixture, identity, cpu, directory / "rss")
                        if "error" in result[arm]:
                            break
                    encoded = json.dumps(result, separators=(",", ":"))
                    print(encoded, file=raw, flush=True)
                    print("REGISTRY_PAIR " + encoded, flush=True)
                    if any("error" in result[arm] for arm in order if arm in result):
                        raise SystemExit(f"failed owned parser check for {identity['name']}")
                    samples.append(result)
                summary = {
                    "case": identity["name"],
                    "median_ns": summarize(samples, rng, "median_ns"),
                    "max_ns": summarize(samples, rng, "max_ns"),
                    "peak_rss_kib": summarize(samples, rng, "peak_rss_kib"),
                }
                summaries.append(summary)
                print("REGISTRY_SUMMARY " + json.dumps(summary, separators=(",", ":")), flush=True)
        (args.out / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")


if __name__ == "__main__":
    main()

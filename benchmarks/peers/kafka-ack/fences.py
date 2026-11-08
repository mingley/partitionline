#!/usr/bin/env python3
"""Turn independently queried Kafka end offsets into strict Java audit fences."""
import argparse
import json
from pathlib import Path

def fences_from_offsets(text, topic, partitions, warmup, count):
    if partitions <= 0 or warmup < 0 or count < 0:
        raise ValueError("invalid workload")
    found = {}
    for line in text.splitlines():
        fields = line.rsplit(":", 2)
        if len(fields) != 3 or fields[0] != topic:
            raise ValueError("malformed or unexpected offset topic")
        partition, end = int(fields[1]), int(fields[2])
        if partition in found or not 0 <= partition < partitions:
            raise ValueError("invalid or duplicate partition")
        def share(total):
            return 0 if partition >= total else 1 + (total - 1 - partition) // partitions
        if end != share(warmup) + share(count):
            raise ValueError("actual high watermark differs from fresh-topic workload")
        found[partition] = end
    if len(found) != partitions:
        raise ValueError("incomplete independently queried offsets")
    return "".join(f"{part}\t0\t{found[part]}\n" for part in range(partitions))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--offsets", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", required=True, type=int)
    parser.add_argument("--warmup", required=True, type=int)
    parser.add_argument("--count", required=True, type=int)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = fences_from_offsets(Path(args.offsets).read_text(encoding="utf-8"), args.topic,
                                  args.partitions, args.warmup, args.count)
    with Path(args.output).open("x", encoding="utf-8") as out:
        out.write(result)
    print(json.dumps({"topic": args.topic, "partitions": args.partitions,
          "total_offsets": args.warmup + args.count, "start": 0}, sort_keys=True))

if __name__ == "__main__":
    main()

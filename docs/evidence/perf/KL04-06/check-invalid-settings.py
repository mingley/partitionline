#!/usr/bin/env python3
"""Check that malformed load settings fail before trying to connect."""
import json
import os
import subprocess
import sys

binary = sys.argv[1]
base = os.environ.copy()
for key in ("COUNT", "RATE_PER_SECOND", "MAX_PENDING", "SAMPLE_FLOOR", "BUFFER_MEMORY", "MAX_BLOCK_MS", "DELIVERY_TIMEOUT_MS", "REQUEST_TIMEOUT_MS", "MODE", "WARMUP", "ACKS", "PAYLOAD_BYTES", "LINGER_MS"):
    base.pop(key, None)
base.update(LATENCY_MODE="open-loop", WARMUP="0", KAFKA_BOOTSTRAP="127.0.0.1:1")
cases = [
    ({"RATE_PER_SECOND": "oops"}, "invalid RATE_PER_SECOND"),
    ({"RATE_PER_SECOND": "0"}, "RATE_PER_SECOND must"),
    ({"COUNT": "0"}, "COUNT must"),
    ({"COUNT": "-1"}, "invalid COUNT"),
    ({"COUNT": "18446744073709551615", "RATE_PER_SECOND": "1000000000"}, "COUNT must"),
    ({"COUNT": "20000000000", "RATE_PER_SECOND": "1"}, "arrival schedule overflow"),
    ({"MAX_PENDING": "0"}, "MAX_PENDING must"),
    ({"SAMPLE_FLOOR": "9999"}, "SAMPLE_FLOOR must"),
    ({"MAX_BLOCK_MS": "0"}, "timeout settings must"),
    ({"DELIVERY_TIMEOUT_MS": "0"}, "timeout settings must"),
    ({"REQUEST_TIMEOUT_MS": "0"}, "timeout settings must"),
    ({"ACKS": "0"}, "broker acknowledgment timing requires"),
    ({"ACKS": "oops"}, "invalid ACKS"),
    ({"MODE": "fetch"}, "open-loop requires MODE=produce"),
    ({"LATENCY_MODE": "oops"}, "LATENCY_MODE must"),
]
results = []
for settings, expected in cases:
    result = subprocess.run([binary], env={**base, **settings}, text=True, capture_output=True, timeout=5)
    assert result.returncode != 0, settings
    assert expected in result.stderr, (settings, result.stderr)
    assert "Connection refused" not in result.stderr, (settings, result.stderr)
    results.append({"settings": settings, "rejected_before_connection": True})
print(json.dumps({"status": "passed", "case_count": len(results), "cases": results}, indent=2))

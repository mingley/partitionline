#!/usr/bin/env python3
"""Independently verify retained certificate purposes, chains, names and expiry."""

import hashlib
import json
from pathlib import Path
import subprocess


root = Path(__file__).resolve().parents[4]
evidence = Path(__file__).resolve().parent
fixtures = root / "partitionline-broker/tests/fixtures/tls"
pins = json.loads((evidence / "fixtures.json").read_text())["sha256"]
for name, checksum in pins.items():
    assert hashlib.sha256((fixtures / name).read_bytes()).hexdigest() == checksum, name
cases = [
    ("valid-server", True, ["-purpose", "sslserver", "-verify_hostname", "localhost", "server1.cert.pem"]),
    ("valid-client", True, ["-purpose", "sslclient", "client1.cert.pem"]),
    ("valid-intermediate-chain", True, ["-purpose", "sslclient", "-untrusted", "intermediate.cert.pem", "client-chain.cert.pem"]),
    ("untrusted-client", False, ["-purpose", "sslclient", "client2.cert.pem"]),
    ("expired-client", False, ["-purpose", "sslclient", "client-expired.cert.pem"]),
    ("wrong-client-purpose", False, ["-purpose", "sslclient", "client-wrong-purpose.cert.pem"]),
    ("wrong-server-name", False, ["-purpose", "sslserver", "-verify_hostname", "localhost", "server-wrong-name.cert.pem"]),
    ("expired-server", False, ["-purpose", "sslserver", "-verify_hostname", "localhost", "server-expired.cert.pem"]),
]
result = {"openssl": subprocess.check_output(["openssl", "version"], text=True).strip(), "sha256_checked": len(pins), "cases": []}
passed = True
for name, valid, arguments in cases:
    command = ["openssl", "verify", "-CAfile", "ca1.cert.pem", *arguments]
    completed = subprocess.run(command, cwd=fixtures, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    agrees = (completed.returncode == 0) == valid
    passed &= agrees
    result["cases"].append({"name": name, "expected_valid": valid, "command": command, "exit_code": completed.returncode, "agrees": agrees, "output": completed.stdout})
result["passed"] = passed
(evidence / "openssl-fixture-results.json").write_text(json.dumps(result, indent=2) + "\n")
print(f"OpenSSL: {len(cases)} independent fixture checks; {sum(c['agrees'] for c in result['cases'])} agree; {len(pins)} SHA256 pins match")
raise SystemExit(0 if passed else 1)

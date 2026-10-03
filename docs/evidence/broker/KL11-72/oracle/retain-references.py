#!/usr/bin/env python3
"""Retain bounded pinned Apache SASL/admin schemas and implementation references."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[5]
NAMES = ["SaslHandshake", "SaslAuthenticate", "DescribeUserScramCredentials", "AlterUserScramCredentials"]
WANTED = {"LICENSE", "NOTICE"}
for name in NAMES:
    for direction in ["Request", "Response"]:
        WANTED.add("clients/src/main/resources/common/message/" + name + direction + ".json")
        WANTED.add("clients/src/main/java/org/apache/kafka/common/requests/" + name + direction + ".java")
WANTED |= {"clients/src/main/java/org/apache/kafka/common/" + name for name in [
    "security/authenticator/SaslClientAuthenticator.java",
    "security/authenticator/SaslServerAuthenticator.java",
    "security/scram/internals/ScramFormatter.java",
    "security/scram/internals/ScramSaslClient.java",
    "security/scram/internals/ScramSaslServer.java",
    "security/plain/internals/PlainSaslServer.java",
    "security/scram/internals/ScramCredentialUtils.java",
    "protocol/Errors.java", "protocol/ApiKeys.java",
]}
WANTED |= {"clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java",
           "metadata/src/main/java/org/apache/kafka/controller/ScramControlManager.java"}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    if not __debug__:
        raise SystemExit("Use normal Python without -O; pins are asserted.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir()
    matrix = json.loads((ROOT / "tests/conformance/broker/api-matrix.json").read_text())
    rows = []
    for release in matrix["releases"]:
        source = args.archives / (release["version"] + ".tar.gz")
        assert 0 < source.stat().st_size < 32 * 1024 * 1024
        assert sha(source.read_bytes()) == release["source_archive_sha256"]
        selected = {}
        prefix = "kafka-" + release["commit"] + "/"
        with tarfile.open(source, "r|gz") as archive:
            for member in archive:
                if not member.name.startswith(prefix):
                    continue
                name = member.name[len(prefix):]
                if name not in WANTED:
                    continue
                assert member.isfile() and 0 < member.size <= 512 * 1024 and name not in selected
                raw = archive.extractfile(member).read(512 * 1024 + 1)
                assert len(raw) == member.size
                selected[name] = raw
        assert selected.keys() == WANTED, sorted(WANTED - selected.keys())
        for name, raw in selected.items():
            path = args.output / release["version"] / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        rows.append({"version": release["version"], "upstream_commit": release["commit"],
                     "source_archive_url": release["source_archive_url"],
                     "source_archive_sha256": release["source_archive_sha256"],
                     "files_sha256": {name: sha(raw) for name, raw in sorted(selected.items())}})
    (args.output / "pins.json").write_text(json.dumps(rows, indent=2) + "\n")
    print(json.dumps({"releases": 3, "files_per_release": len(WANTED), "verdict": "passed"}))


if __name__ == "__main__":
    main()

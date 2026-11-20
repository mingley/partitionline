# Broker crate archive

Commit `747e22cee14fd6f29e26687ba530be87985b3b77` packaged
`partitionline-broker 0.1.0` on Linux x86_64 with stable Rust 1.100.0.
Cargo verified the archive and its clean checkout receipt. The archive had
77 files and was 615.5 KiB compressed. Its license and notice files and
Clippy configuration matched the repository copies byte for byte.
[Qualification run](https://github.com/mingley/partitionline/actions/runs/44647817744)

The extracted default and all-feature profiles passed locked strict Clippy
and release installation. Both installed operators initialized a local
admin journal, assigned `orders-0` to `data-a` and raised
`metadata.version` to 3. A downgrade returned `FeatureDowngrade` and
left the journal bytes unchanged. The restored journal matched the backup
and reported the same assignment and feature level.

The archived broker README passed navigation checks. The client archive
also passed checks for 30 documents, 235 local links and 58 immutable
repository targets, including the broker operations guide.
[Job log](https://github.com/mingley/partitionline/actions/runs/44647817744/job/135994792587)

Run `sh scripts/broker-package.sh` from the repository root to reproduce
the broker archive check. The job retained both profile logs, the archive
inventory and SHA-256, the installed binary checksums and both docs reports.
The full conformance suite uses fixtures in the repository checkout.
[Verification record](verification.json) ·
[Run artifacts](https://github.com/mingley/partitionline/actions/runs/44647817744/artifacts/14590061739)

The first attempt reached the extracted default profile and stopped before
the operator ran. The second recorded the missing Clippy policy. The final
archive includes that policy.
[First attempt](https://github.com/mingley/partitionline/actions/runs/44646682619) ·
[Second attempt](https://github.com/mingley/partitionline/actions/runs/44647091528)

KL11-54 still needs installed Kafka listener qualification, secure deployment
and capacity evidence, catalog and partition-data backup and restore, and
upgrade workflows. This run checked only the local admin journal on Linux
x86_64.

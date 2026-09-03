# Security policy

## Supported versions

Security fixes target the latest published crates.io release of `partitionline`
on the 0.x line. Older 0.x releases may not receive backports.

## Reporting a vulnerability

Please use [GitHub Security Advisories](https://github.com/mingley/partitionline/security/advisories/new)
for this repository. Do not open a public issue with exploit details.

## Scope

In scope: protocol decode/encode memory safety, authentication material handling
in this crate, and dependency advisories affecting default features.

Out of scope: broker-side ACL misconfiguration, application secret management,
and unsigned benchmark / Suite HOLD claims (see `docs/STATUS.md`).

Threat model and verification posture: [`docs/security.md`](docs/security.md).

# Dependency security floors, 30 October 2026

This candidate updates dependencies reported by GitHub CI at main
`02ce806d7ea4970897d51604e71e6a1fa069ca4c`. It changes three manifests,
ten active lockfiles and a dependency-policy comment. Historical evidence
lockfiles are unchanged. No compiler build, runtime suite, cargo-audit or
cargo-deny run has passed for this candidate.

| Package | Earlier active locks | New locks | Manifest minimum | Advisory |
| --- | --- | --- | --- | --- |
| `ctutils` | 0.4.2 | 0.4.7 | 0.4.4 | RUSTSEC-2026-0385 |
| `rustls` | 0.23.43 or 0.23.45 | 0.23.46 | 0.23.46 | RUSTSEC-2026-0346–0349 |
| `rustls-webpki` | 0.103.15 | 0.103.16 | 0.103.16 | RUSTSEC-2026-0382 |
| `webpki-roots` | 1.0.9 | 1.0.11 | 1.0.10 | RUSTSEC-2026-0381 |
| `tokio` | 1.53.1, 1.53.2 or 1.53.3 | 1.53.3 | 1.53.3 | RUSTSEC-2026-0386, unsoundness |

Tokio stays on the existing 1.53 minor line in the locks. The manifests allow
newer compatible versions. Updating a lock alone would leave downstream
applications free to retain vulnerable compatible dependencies. In particular,
rustls 0.23.46 still permits webpki 0.103.14, and digest permits ctutils 0.4.
Explicit minimum constraints cover these two transitive dependencies.

The broker enables the extra constraints with its existing `sasl` and `tls`
features. Schema enables webpki with `registry`; its core still has no normal
dependencies. Ring remains the TLS provider. No advisory is ignored, and no
dependency feature is enabled to change the provider.

The affected contexts differ. The client and schema Registry client use TLS
and bundled trust roots. Broker TLS/OIDC uses certificate verification; broker
SASL and client authentication use the RustCrypto graph. ECH, Brotli certificate
decompression and hybrid post-quantum key exchange are absent from the recorded
TLS features. These differences limit exposure to particular advisories; they
do not substitute for updating or auditing the resolved graph.

## Resolution checks

Five locked feature graphs resolved successfully: client all features, broker
default/all features, and schema core/all features. Schema core has no normal
dependency edges, and broker default does not acquire TLS or SASL dependencies.
All nine auxiliary manifests also resolved under their existing locks. Four
offline attempts lacked cached registry sources/index entries; separate online
retries succeeded. Original failures are retained.

Eleven downstream controls checked the manifest constraints. Each of five
insecure exact versions resolves with the original client manifest. Each is
rejected by the updated manifest. One graph pinning all five patched versions
resolves. These checks exercise Cargo's resolver without compiling the client.

The root lock changes only the five packages in the table. Broker fuzz already
used Tokio 1.53.3; regenerating its stale lock adds `errno` and
`signal-hook-registry`, required by the broker's existing `signal` feature.
The lock delta is retained separately. Peer and zero-dependency null-broker
locks are unchanged.

`security-dependencies-v1.patch` has SHA-256
`7530fd7368d7363c4ebc35c8ea039cf6a92865b6a7129b988306e5be48305b73`.
`patch-manifest-v1.json` pins all fourteen before/after files.
`qualify-resolution.py` and command receipts describe the checks. All candidate
manifests and locks stayed unchanged during qualification. No build target
directory was created.

The initial offline root update failed before resolution because the patched
webpki source was not cached. Its shell wrapper then printed the log, so its
underlying Cargo exit status was not captured; it is not a successful receipt.
The independent online update and subsequent locked metadata checks succeeded.

## Remaining validation

Run default/all-feature client, broker and schema tests and strict lint with
these dependencies, including TLS, authentication and shutdown cases. Refresh
cargo-audit and cargo-deny against a current pinned advisory database for each
product graph, including unsoundness warnings. Neither tool is installed on
this task's PATH or in its Cargo bin directory, so no new audit result is
claimed. Existing release gates remain in force.

The retained input audit used RustSec database commit
`2295466c4e9df26647fd67d8fa1c705ac2fc7f81`, last updated
`2026-10-30T12:12:39+01:00`. Its seven vulnerability entries and one Tokio
unsoundness entry motivated these changes. Removing those exact versions does
not establish that the entire dependency graph is free of advisories.

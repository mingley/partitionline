# Rust TLS provider review

The client and broker implement the Kafka protocol in Rust. The client enables
Rustls's Ring provider unconditionally; the broker enables it with `tls` or
`oidc`, and the Schema Registry companion enables it with `registry`. Ring
0.17.14 compiles bundled C and assembly. The broker also uses Ring directly
for OIDC RS256 and ES256 verification and key fingerprints. The default gzip
and optional zstd implementations use Rust crates; unsafe Rust and operating
system calls are separate questions from bundled C code.

This review inspected source and package metadata. It did not build a new
provider, test its complete dependency graph, perform a security audit, or
measure performance. The inspected repository files and commit are pinned in
`source-review.json`. This does not change the production or speed status.

## Available providers

| Package | Reported support | Source-review limit |
| --- | --- | --- |
| `rustls-rustcrypto` 0.0.2-alpha | TLS 1.2/1.3, RSA, ECDSA, Ed25519, AES-GCM and ChaCha20-Poly1305 | The package explicitly says not to use it in production; its latest published version dates to 2024. |
| `rustls-rustcrypto-azul` 0.1.0 | Fork of the upstream provider with newer RustCrypto versions; TLS 1.2/1.3 and RSA | Its README also says not to use it in production and it depends on a prerelease RSA implementation. |
| `oxitls-rustcrypto-provider` 0.3.0 | Fork with TLS 1.2/1.3 and RSA | Its README says not to use it in production. |
| `rustls-provider-rustcrypto-pq` 0.1.1 | TLS 1.2/1.3, RSA, ECDSA, Ed25519, AES-GCM, ChaCha20-Poly1305 and additional post-quantum groups | Its README states there is no security audit and identifies RUSTSEC-2023-0071 in its prerelease RSA dependency: timing private-key operations can expose RSA private keys. Public-key verification has different exposure. |
| `rustls-pure` 0.15.4 | TLS 1.3, ECDSA/Ed25519, AES-GCM and ChaCha20-Poly1305 | No RSA certificate-chain verification or TLS 1.2; its README says it has no outside audit. |
| `ferritls-rustls` / `ferritls-core` 0.9.0 | TLS 1.3, ECDSA, Ed25519 and RSA | Its README lists TLS 1.2 as unfinished and states its crypto module is not CMVP certified. This review did not establish its security or maintenance suitability. |
| `moirai-crypto` 0.11.3 | Documents TLS 1.2/1.3, RSA, ECDSA, Ed25519, AES-GCM and ChaCha20-Poly1305 | Its manifest depends on `rsa` 0.9. Its private-key timing exposure and provider security need independent assessment. |
| `ps-rustcrypto` 0.4.0 | TLS 1.2/1.3 with ChaCha20-Poly1305, ECDSA and Ed25519 | No AES or RSA; its README calls for an audit before publication. |

These package descriptions and caveats are their authors' statements, checked
against the retained source. Algorithm lists have not been independently
verified against live peers. Rustls's `CryptoProvider` interface supports
separate cipher suites, key exchange, certificate and handshake verification,
randomness, and private-key loading. None of the listed alternatives has yet
been shown to preserve this project's full TLS and security requirements.
Switching to aws-lc would still use native cryptography.

## Path to qualification

Select and review an actively maintained provider whose dependency graph and
advisories meet the security requirements. Preserve TLS 1.2/1.3, certificate
chain and hostname verification, RSA/ECDSA support and mTLS; do not silently
downgrade those capabilities. Replace the explicit provider selections in
the client, broker and Schema Registry companion. Replace the broker's direct
JWT verifier separately, preserving the current RSA key-size and fixed-width
P-256 signature checks and testing invalid signatures, malformed keys and
algorithm confusion. Use the existing TLS and OIDC lifecycle suites, then
independent Java, C and Rust peers, failure/cancellation tests and matched
performance measurements before promotion. The provider and JWT code need
appropriate cryptographic and side-channel review; passing interoperability
tests does not provide that review.

The package archives in `packages/` came from `static.crates.io`; the matching
version records from `crates.io` are in `registry/`. `registry-checks.json`
records matching SHA-256 checksums for all nine archives and the UTC check
time. The package-manager destinations were permitted by the enforced network
policy, and the normal proxy and certificate verification were used. Run
`sha256sum -c SHA256SUMS` here to verify retained files.

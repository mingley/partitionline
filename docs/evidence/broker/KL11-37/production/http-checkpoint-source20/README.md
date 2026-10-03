# Managed HTTPS source checkpoint

These seven frozen source files add real HTTPS discovery/JWKS refresh and explicit authenticated RFC 7662 revocation to the signed-JWT foundation. This checkpoint deliberately precedes the actual socket integration; KL11-37 remains open.

`manifest.json` binds exact source bytes. The checkpoint omits only `pub(crate) mod sasl;` from the passing full developer draft, so the existing socket layer does not compile an unused private OAuth parser. The other six files are identical to source20. Working and retained proposal files have filesystem mode 0600; ordinary Git source mode is 100644, and the tested standalone inputs use full mode 0644.

The standalone comparison uses the complete immutable Git tree at `0057972d153e3630578e8e7dcb67773189301b2d` plus seven explicitly declared checkpoint files. Its eight stable/Rust 1.85 toolchain, formatting, behavioral and strict all-target Clippy commands passed. The two test commands passed 192 checks with zero failures, skips or filtered cases. Before and after every command, 59,384 input paths were checked against exact bytes and full modes. This is a declared candidate overlay, not a pushed source qualification.

Two deliberate regressions independently remove the reviewed refresh scheduling and closed-status fixes. Both selected HTTPS tests fail against their respective regressed implementation and pass after restoration. Each selected command filters the other eight HTTP tests; these targeted controls are recorded separately from the unfiltered 192 checks. Earlier source19's fixture failure and source17's two independent review findings remain retained.

Discovery requires exact configured issuer and an explicit HTTPS JWKS-origin allowlist. Both discovery and direct configured HTTPS JWKS require positive online revocation authority. Publication freshness, token expiry and revocation deadlines remain finite during outages. Key replacement/removal invalidates existing leases, including removal and reintroduction coalesced by a watch receiver. No attacker-selected URL or unbounded per-kid cache is retained.

Actual resolver and cryptographic blocking work retain admission until the closure exits, even after cancellation. Shutdown joins these workers and remains retryable after a cancelled wait. An admitted operating-system name-resolution call cannot be forcibly stopped; shutdown may wait for the kernel resolver. Controlled DNS-stall tests prove ownership, rather than claiming a forced kernel timeout. Source bearer/authorization buffers zeroize, while serde, hyper and TLS intermediate copies have no blanket zeroization guarantee.

The checkpoint does not yet qualify real Java/C/Rust token acquisition and authenticated socket histories. Independent issuer/service proof, pushed socket source, complete feature/MSRV checks and genuine peer reconnect/rotation/outage/expiry evidence remain required. Lifetime stays zero until separate live reauthentication work qualifies.

Proof: `../development/http-stage2-source20/validation.json`. Lossless test executable preservation, including both deliberately regressed binaries, is bound by that directory's `bin/manifest.json`; the compressed binary archive lives in scratch, outside Git, with byte hashes, full modes and exact restore paths.

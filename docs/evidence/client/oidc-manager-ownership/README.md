# OIDC manager ownership checks

KL06-13 limits the shared cache to 64 configurations and 64 KiB of input per
configuration. Idle entries retain tokens for reuse but run no refresh work.
Active leases prevent eviction. Cancellation keeps its slot until its fetch
finishes. The final application-owned manager clone cancels acquisition and
refresh tasks.

Seven new regressions cover cache capacity, repeated credentials, input size,
TLS field isolation, token sharing/reuse, HTTP cancellation, and final-owner
acquisition/refresh cancellation. The baseline failed both final-owner tests
and the TLS key test; the failure logs are retained.

The final stable and Rust 1.85 default matrices pass 1,177 tests each. All-feature
matrices pass 1,178 each. Strict lint, formatting, rustdoc, four doctests, 18
Markdown checker tests, and 19 packaged Markdown examples pass.

`peer.py` independently implements literal HTTP token responses and a fixed
SASL v1 exchange. `probe.rs` uses the public Rust connection/authentication APIs.
Each of four repository-pinned compiler/feature runs opens 87 authenticated connections
across 71 fixture credential sets. The issuer sees 72 requests: concurrent and
sequential opens reuse one token; the original credentials fetch again after
70 other idle sets force eviction. Both servers stop and join their threads.
The initial unlocked run is retained as preparation only.

To repeat the peer check, copy `probe.rs` into a small Cargo binary using the
repository as a path dependency and Tokio with `macros`/`rt-multi-thread`.
`probe-Cargo.toml` and `probe-Cargo.lock` retain the exact validation inputs;
update only the local repository path. Run:

```sh
python3 peer.py /path/to/oidc-ownership-probe report.json
```

The candidate is uncommitted. `source-input-hashes.json` identifies the tested
files, and [task evidence](../../../plan/evidence/KL06-13.json) records commands,
counts, development failures, and limits. These are local behavioral checks;
production and comparative performance qualification remain separate.

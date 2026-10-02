# Frozen independent oracle qualification

Source: `029355f95687bb0528eb70f7872ce2e901adf4de`.
The three authentic Apache Kafka release lanes passed **333 cases**, and all
three altered literal RFC proofs failed with the expected independent Java
assertion. The five fixture tables and detailed outcomes are byte-identical
across Kafka 4.1.2, 4.2.1 and 4.3.1. Published fixtures exactly match this pushed
source snapshot and the current owned fixture files.

[The raw matrix report](final-matrix/results.json) retains jar and dependency
pins, embedded release/commit properties, Java version, generator/runner/class
hashes, exact commands, statuses and stdout/stderr hashes. Each release contains
48 SCRAM transcripts, 36 parser messages, 12 PLAIN messages, one literal RFC 7677
vector, two extension references and 12 actual Apache client proof decisions.
[The independent validation receipt](final-validation.json) checks actual table
row counts, every raw log/class hash, all fixture bytes and 39 retained source or
RFC reference file hashes. The reference and policy distinctions in
[README.md](README.md) apply to these results.

[The source receipt](frozen-source.json) records the archive hash and critical
input hashes. All 4,879 extracted source files were byte-compared to their exact
Git objects before execution; the complete path set and all file hashes were
unchanged after execution. Compilation output and case artifacts were outside
the source archive. The initial `2f869157` archive was superseded before Java
execution by the coordinator's module-order formatting correction; no oracle
run on that initial snapshot is represented as final qualification.

The failed RFC variants retain their partial generated tables, raw JVM stderr
and exit status. Each `*-rfc-mutant/literal-mutation-input.json` also reconstructs
the exact deterministic proof mutation from the pinned generator and published
literal bytes. It explicitly identifies that reconstruction rather than
claiming a separately captured runtime message.

[The independent mechanism review](independent-mechanism-review.json) found no
blocking issue within the bounded server mechanism scope. It reviews raw
AuthMessage binding, fixed-size constant-time comparison, terminal state,
credential generations, parsing/work limits, cancellation permit ownership and
redacted diagnostics. It records its finite scope and existing policies,
including generation capture and the Kafka raw-UTF-8 password/ASCII SCRAM-name
profile. Rust builds and their stable/MSRV gates belong to the separate
mechanism-worker evidence; this oracle worker did not duplicate those builds.

These results qualify the tested mechanism primitives and independent fixture
replay. They establish neither Kafka SASL framing or TLS/session interoperability
nor general SASLprep conformance or production security qualification.

To replay, use the command in README against an archive of this exact source,
new output/class directories and its existing fixture directory. `SHA256SUMS`
covers this oracle's retained sources, references, development/final artifacts
and the five published fixture tables; check it from the repository root with:

```sh
sha256sum --check --quiet docs/evidence/broker/KL11-66/oracle/SHA256SUMS
```

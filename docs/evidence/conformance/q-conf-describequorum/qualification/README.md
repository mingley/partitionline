# DescribeQuorum qualification

Apache Kafka SDKs 4.1.2, 4.2.1 and 4.3.1 generated and parsed version 0–2
messages. Each Rust feature build checked 135 independent bodies, 126 public
socket profiles and nine deliberate corruptions. Public Java and Rust Admin
calls agree on negotiated fields, errors and successful retries. Rust caps
retries at eight attempts; Java runs to its deadline. Deadline and exhaustion
profiles record a separate warm-up call before the timed 300 ms operation.

Three fresh native Kafka broker/controllers per feature build handled 18 raw
requests and six public calls. SDKs parsed 36 live bodies and 12 public native
frames per build. Public results match their captured responses. All owned
threads, sockets and processes joined; native and proxy ports were reusable.
Native executable files match the retained release archives.

Rust bounds nested allocations, strings and tags, rejects incomplete/trailing
bodies, and checks the complete encoding model before writing output. It
matches SDK string and nonignorable field version limits. Unknown tags are
skipped. Generic socket framing still has its separate 100 MiB limit; these
codec checks do not establish global network memory or RSS limits.

Latest stable Rust passed 2,090 default and 2,102 all-feature tests. Strict
Clippy, formatting and documentation checks passed. Four packaged feature
configurations compiled 24 documentation examples each. The exact source was
published before the final SDK qualification. Earlier failed attempts and
executables are retained. summary.json records the published source and scope.

Nine current client ledger cells qualify. Historical exclusions and original
pins remain. Internal Raft replay, multi-voter recovery, security, full
conformance, production qualification and performance comparisons remain open.
validation/ and peers/ retain the sources, commands, messages, failures and
process receipts. FILES.json and SHA256SUMS describe this snapshot. Do not
regenerate it.

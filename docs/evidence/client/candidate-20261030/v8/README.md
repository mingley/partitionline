# Client candidate v8

The isolated v7 x86 run failed in both feature profiles. Each library run
had 2,253 passes and one test setup panic; Mock Produce had 31 passes and 15
failures, retained backing had seven passes and one timeout, and Clippy found
two test-only errors. Two baseline cases passed when failure was predicted.

This candidate adds the two test repairs, preserves all 141 previous required
identities and 22 baseline identities, and changes the two baseline predictions
to match the observed results. It includes the independently reviewed producer
worker ownership candidate and its 26 additional required controls. It does
not include fixes for the Mock Produce or retained-backing failures.

Exact source reconstruction, forward/reverse patches and stable Rustfmt checks
passed. Cargo compilation, Clippy, native tests and performance measurements
have not run for this combination. The ten chosen binaries and both feature
profiles remain required. Live Kafka, standard-stack safety, whole-main CI,
producer ordering and replay, release readiness and performance rank remain
open. The complete v7 assets and results remain preserved.

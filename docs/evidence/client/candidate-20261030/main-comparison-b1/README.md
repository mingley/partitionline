# Main client comparison

This source-bound comparison runs the current main library and complete
`mock_produce` and `producer_retained_backing` binaries in default and
all-feature profiles on x86. It specifically retains the 15 v8 mock failure
identities and the retained-backing timeout identity. Failures make the
comparison fail; ignored cases remain reported.

The Rust source, test files, Cargo manifests and lockfile are unchanged from
main at the recorded parent commit. The isolated phase patches only change a
README comment so the existing guarded qualification driver can compare
phases. Both feature profiles use one test thread and an 8 MiB stack. This is
not an all-target runtime matrix, live Kafka qualification, release signoff or
performance benchmark. The comparison has not yet run.

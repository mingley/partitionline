# Initial baseline profiles

Two fresh `nb-produce-bulk` captures each run 25 repetitions of the pinned
`c4b9157` baseline under 997 Hz userspace CPU sampling. Actual result timestamps
exclude fixture construction, setup and shutdown from the client analysis.
The first raw capture was replayed after a resolver cleanup failure; that
replay is not another fresh capture.

The analyses retain 511 and 493 measured client samples. Unresolved libc leaf
samples account for 42.86% and 45.23%. The available debug package has a different
build ID and was not used. Both profiles identify clock reads, frees, producer
admission and task wakeups among the resolved costs. Inlining and the unresolved
samples prevent a complete ranking or a below-one-percent conclusion.

A separate five-repetition raw syscall trace isolates the actual runtime
main PID and measured phase intervals. It counts 1,378 entries, excluding
2760 client startup/shutdown entries and all descendant lines. Elapsed syscall
time includes blocking and observation overhead. Raw hexadecimal arguments
exclude strings, packet payloads, I/O-vector contents and TLS record sizes.

`capture/` retains commands, phase results, raw profiles, observers, analyses,
source/ELF pins, process receipts and failed attempts. Executed wrappers from
different attempts are preserved separately. `tools/` retains the perf
executable and libraries, parent-binding helper and the libc object inspected
afterward. The 111-file source pins refer to the unchanged sealed baseline source.
Paths in receipts retain their original workspace locations.

This is partial evidence for KL09-13. Other cells and allocation call-site
shares remain open. No full ranking, production readiness or performance
leadership result is established. `SHA256SUMS` covers every other archive file.

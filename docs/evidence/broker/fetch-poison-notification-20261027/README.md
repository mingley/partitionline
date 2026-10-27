# Fetch notifications after read corruption

A cached read could poison its partition and return its error before recording
that the storage state changed. The actor then had no change to publish, leaving
an existing long-poll Fetch asleep until its deadline.

The existing corruption-waiter test failed with `Elapsed` on unchanged source.
The corrected read path returns the result together with the poison transition;
both direct and relocated handle paths record the transition before returning.
This adds no heap allocation or extra read, and preserves read validation,
cache invalidation and legacy error mapping.

The real router test now passes in both feature modes: the waiting v6 request
returns storage error 56, v4/v5 map to 6, the other partition remains healthy and
restart refuses the corrupt store. A focused storage test exercises direct,
mismatched-position and absent-position cached handles, output-buffer rollback,
healthy/invalid reads and notification only on the first poison transition.
Both focused tests pass in both modes. Strict Clippy and formatting pass.
Tested source is recorded in `capture/matrix.json`; Rust is stable 1.99.1.

The default whole-broker run passes 602 tests and fails 10; all features passes
735 and fails 21. Fetch itself passes 24 and fails the same two Apache golden
response comparisons in each mode. Other failures include group caller timing,
Raft histories, Produce/transport accounting and security deadlines. The Produce
and transport accounting failures also recur with one test thread. These runs
are failed qualification checks; none are waived. Independent fixture bytes
were preserved. Client core/full conformance also remains failed.

`capture/` retains the original failing executable/log, corrected executables,
full command/source/compiler receipts, whole-suite failures, serial rechecks and
a committed source archive. Verify stored and reconstructed bytes with
`python3 benchmarks/runtime/verify-evidence-archive.py` and this directory.
This repair does not qualify production readiness or fastest performance.

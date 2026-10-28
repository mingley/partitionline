# Broker checks and recovery diagnostic

These checks ran on the shared x86_64 workspace with Rust 1.99.1. They do not
establish production readiness or comparative performance. The active goal is
in [goal-qualification.json](../../../plan/goal-qualification.json).

## Broker tests

The default-feature all-target suite passed 615 tests and the all-feature suite
passed 759. Both had zero failed and zero ignored tests. Strict Clippy passed on
all targets in both feature modes; formatting passed. The commands, full logs,
individual test results, executable digests and source-file digests are in
`runs/`. Earlier failed runs are retained there too.

The successful suite runs were made with uncommitted changes. The older runner
recorded the base commit in a field named `source_sha`; that value is not the
commit of the tested tree. Use the recorded per-file SHA-256 values to identify
the source files. The default suite preceded the credential-buffer cleanup and
the final lint edits; the all-feature suite preceded the final lint edits.
Neither suite establishes a pass for the later committed tree. The later Clippy
receipts list a null `source_sha` and a base commit; their broker source-file
hashes match commit `e7f5761a9`. The receipts hash source files but do not
include a complete snapshot of the workspace or all build inputs. Rerun the
full suites on a clean candidate before treating them as release gates.

`executables/` contains the default and all-feature library test executables,
the earlier failing transport integration executable, and the repaired Fetch
integration executable. The suite receipts also identify other test executables;
those other binaries are not included in this archive.

## Apache Kafka checks

The pinned `apache/kafka:4.1.2` image, image digest, launch arguments, topic
creation, and ApiVersions response are in `capture/`. The broker had one
replica and the test topic had two partitions. The final Fetch probes used a
four-record log. Fetch versions 4–6 returned error 1 and unknown boundaries
for out-of-range offsets, and returned high watermark 4 at offset 4. For
ListOffsets versions 1–3, the tested negative selectors below -2 returned error
35. Raw requests, responses and decoded values are retained. These are checks
against one live Kafka version, not a complete compatibility matrix.

The first Fetch probe is retained separately in `capture/apache-fetch/`. Its
second Produce batch was rejected with error 87 because the fixture contained
a nonzero base offset, while the probe read the wrong response byte for that
error. Its first batch was accepted. The corrected probe in
`capture/apache-fetch-02/` used base offset zero for the second Produce request
and checked the proper response field before testing the four-record log.

## Fixed-input recovery diagnostic

A diagnostic reran the exact binary and 60-byte input from the failed
[previous fuzz campaign](../fuzz/20261027-candidate06/README.md). It used
`strace`, a 2-second input timeout, 0.25 CPU and 1 GiB of memory. It exited 70
after 904.7 seconds and 80,753 executed units. Around the alarm the recorded
trace includes a 1.059-second `fdatasync`, followed by a 0.469-second directory
`fsync`; the longest retained syscalls include a 1.526-second `unlink` and a
1.399-second `fdatasync`. Wall times include scheduling and tracing overhead.

This instrumented run used a different CPU and memory limit from the original
untraced campaign, which had 3 CPUs and 4 GiB. It does not establish the cause
of that campaign's timeout. The diagnostic did not complete a fuzz campaign and
counts for zero qualification time. Its launch, binary and input hashes,
manifest, bounded trace, fuzzer log and final result are retained under
`capture/` and `diagnostic/`. The result contains the last 256 trace lines and
256 slowest syscalls, not a complete trace.

## Archive verification

`archive-inventory.json` identifies the original and stored bytes. Large files
are stored with gzip. Verify every stored and reconstructed digest from the
repository root with:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/qualification-20261028
```

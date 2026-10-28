# Client cancellation and scheduling investigation

The retained full client matrix ran on the shared x86_64 workspace with latest
stable Rust 1.99.1. Its default-feature library target reported 2,064 passed
and 43 failed tests. The entire matrix exited 101 with 20 failing targets; the
complete output and build-input receipt are retained under `runs/`. This is an
uncommitted candidate based on `99868cad0`: the receipt records the input hashes
and their stability during the run, not a clean committed tree.

A later library test executable, SHA-256
`5a6d0d24b92ebeaffcc3a6b66359526dc54103f5799b535321b8bcd2b650e0b5`,
was used for isolated replays of 41 scheduling-test names selected from the
original matrix failures. It is identified by `later-binary-build.json`; it is
**not** the original matrix executable, whose recorded SHA-256 is
`85fac653d27e763431e32a561397ab08f450866560d68b84e030532451dd5397`.
The same target path was rebuilt between runs. The driver text originally
called it the old matrix executable; that scope text was incorrect. The
corrected replay receipts preserve both identities. Neither executable is
included in this small archive; their hashes and the runner's retained paths
are in the receipts. The complete source snapshot is not included either.

Each selected test ran alone with one test thread and a 30-second process
limit. Of the 41 later-binary runs, 24 passed, 16 failed and one timed out.
The timed-out test reported a failed elapsed assertion before its process was
terminated; its output is retained and it is not counted as a pass. Results
cannot be attributed to the original executable or to the current source
without a fresh run. Before those individual runs, a broader serial replay was
stopped after 85 successes and five failures; its `exit_code: 143` receipt and
partial log are retained. It did not complete all 415 selected tests.

`proposal.md` explains one candidate cancellation design for the group
rebalance path, including owner lifetime, memory accounting, saturation,
unpolled futures, final drop, and validation. Large owned iterators and partial
maps are destroyed synchronously when their futures are canceled. That is a
source-based explanation of a possible stall; these logs do not isolate how
much time came from destruction, collection growth, or host scheduling. The
proposal has not been implemented or benchmarked and does not qualify the
client for production or performance claims.

`probe/` contains a standalone destructor microprobe and its planned controls.
It has not been compiled or run; it cannot establish Tokio scheduling behavior.

Verify retained files and decompressed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/cancellation-fairness-20261028
```

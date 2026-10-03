Actual GitHub CI for34a2e672: strict client Clippy and all three current
Kafka interoperability jobs passed. The retained Linux stable/all-feature and
macOS stable raw logs all show17 ListTransactions tests passing, then22 sticky
partitioner tests passing and6 failing. No full-suite pass is claimed.

The broker stable raw log preserves66 passing library tests and two failed
3/5-peer observer additions with Deadline. The Windows raw log records checkout
failing on two long retained evidence paths before any compiler/test execution.
The workflow now enables Git for Windows long paths before checkout. Runtime
corrections for the sticky and observer failures are still being prepared.

Decoded logs and full tool results are preserved without whitespace changes.
These are decoded GitHub job logs, not a claim of original HTTP ZIP bytes.

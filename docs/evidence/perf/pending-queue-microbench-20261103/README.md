# Pending queue microbenchmark prototype

This inactive [prototype](prototype.tar.gz) compares the v14 pending queue
with its v10 predecessor in paired release-mode cases. It covers expiration
probes, ordered and nonmonotonic drains, vector shifts and individual record
admission. Both arms use the same fixture and record identity checks. Its
same-job witness binds the qualification report to the run and attempt.

The [manifest](manifest.json), [bundle receipt](bundle-receipt.json) and
[source review](independent-review.json) record the exact inputs. Static
controls passed, but the Rust harness and workflow have not been compiled or
run. The benchmark stays disabled until the client candidate passes its full
correctness gates. Its witness is bound to v14 and needs updating for v15.
Synthetic completion does not prove Kafka delivery, and no speedup or ranking
has been measured.

# Transaction coordinator replacement

A transaction coordinator remains owned in the shared slot while it is marked
closed and retired. If the replacement wait is canceled or times out, a
later operation can rediscover instead of finding an empty slot. The final
slot wait and retirement wait use the request deadline; a closed or missing
coordinator is rediscovered before its cached versions are used.

The [combined qualification](https://github.com/mingley/partitionline/actions/runs/42440217637)
tested [`696bc9198b60f30d61c366d505029d9608a601b8`](https://github.com/mingley/partitionline/commit/696bc9198b60f30d61c366d505029d9608a601b8),
based on `8b5de2e2cfe5f3164a5f89546804ab36c1771ade`, on x86_64 with
stable Rust 1.99.1. The tested SHA-256 values were
`fd4ebd8230be5d82ab245283478b916dc92cb7160dfb84a755b8c94bd3259bd9`
for `src/producer.rs` and
`27aac491033d6ae73e85927d13289b0054cb658b029b5b9382b64d9b259b3431`
for `src/net.rs`. The latter change is a test-only stream-lock gate.

In each feature profile, the runner reproduced two original lock-wait
failures and three reconstructed replacement failure witnesses. It passed
14 selected candidate tests, including cancellation and timeout during
retirement, missing and closed coordinator rediscovery, epoch recovery over
mock TCP, and the earlier lock controls. Formatting and strict all-feature
library Clippy passed. The replacement witnesses reconstruct the former
take/retire/install sequence in a helper; they do not run the original full
rediscovery path.

A separate [Kafka 3.9.1 comparison](https://github.com/mingley/partitionline/actions/runs/42440743674)
passed on both the exact main baseline and candidate. Each used a fresh
single-node broker, observed a successful two-partition AddPartitions v3
exchange, received successful produce acknowledgements, saw `CompleteCommit`
from Kafka's transaction CLI, and read both values with Kafka's console
consumer. This is a plaintext happy path, not a real failover test. Job logs
were inspected; uploaded artifacts were not independently inspected.

This repair does not enforce an end-to-end transaction deadline: response
decoding, parts of discovery and epoch-bump waiting remain unbounded. Concurrent
rediscoveries can still install an older result over a newer one. Real failure
recovery, cancellation after discovery, TLS/SASL behavior, physical cleanup
under every contention path, transaction error-state races and performance
remain open.

After adoption, the [main all-target Clippy job](https://github.com/mingley/partitionline/actions/runs/42441209189/job/128610627588)
found a loop-style lint in one new mock server. A test-only correction passed
[stable formatting, strict all-target/all-feature Clippy and 12 selected tests](https://github.com/mingley/partitionline/actions/runs/42441786927)
on x86 with Rust 1.99.1. The corrected `src/producer.rs` SHA-256 is
`e21d03f495f504f167bc56139dc03798b0f323ca51370c7dceade23a2dad066e`.
It does not alter the production replacement code or repeat the Kafka smoke.

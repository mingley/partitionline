# Producer admission and grouped decoding

This patch closes producer admission before worker retirement, wakes blocked
senders, counts workers until their resources finish dropping, and retains retry
records and counts across cancellation. It also uses the ready channel path,
fixes incremental Vec reservation, decodes grouped assignments directly, and
bounds small Share record-set capacity hints.

Validation used stable Rust 1.99.1 on a shared x86_64 host, one Cargo build
job, one test thread, and RUST_MIN_STACK=8 MiB. All-features runtime and the
live broker default-stack compatibility failure remain open. The corrected old-code
baseline ran 31 controls: 17 passed and 14 failed. The fixed candidate passed all
33 focused controls. Strict Clippy passed for every target and feature.

The full default library run passed 2,176 tests and failed 20. Seven integration
targets passed 118 tests, failed 19 and ignored two native Kafka peer tests.
Production readiness and performance claims remain open. The failures and all
earlier compile/setup/lint attempts are retained in the archive.

The retained-backing cancellation assertion observed 4,096 bytes instead of
4,097. The same test failed with a timeout on commit 53b5af47. The causes of these observations
remain unqualified. The current fixture allows acknowledgements
while awaiting task cancellation and does not capture acknowledgement counters;
this run does not prove an accounting regression or resolve the test.

[validation.json](validation.json) lists every scope, failure, source hash,
resource receipt and local retained executable hash.
[producer-and-protocol.patch.gz](producer-and-protocol.patch.gz) contains the reviewed patch
against 77021f7dc. [qualification-evidence.tar.gz](qualification-evidence.tar.gz)
contains the source capsules, commands, before/after input inventories, raw logs,
case receipts, applied patch history and independent static review. Repeated
receipt inventories are stored once: files.json maps each original path to
its SHA-256 object. [read-evidence.py](read-evidence.py) reads one original file
or verifies every referenced object without extracting the archive. Executables
remain in verified local gzip archives and are omitted from the public bundle.

Inside the archive, runs/fixed-source.tar.gz contains 1,741 source and fixture
files. Apply runs/baseline04-source-delta.tar.gz over that source to reconstruct
the genuine old-code baseline. Check all paths against the corresponding build
input manifest. Each capsule member was read back and checked.

The disk guard sampled every 250 ms at a 1.5 GiB floor. Earlier qualification
stages used a 2 GiB floor; their failures and resource choices stay recorded
in their original evidence. These local passes do not qualify default-stack
live broker compatibility. There was no recorded
resource stop or cgroup memory event change. This is local correctness evidence;
it does not capture every dependency/toolchain byte or qualify sustained load.

Accepted payloads in Worker::fire still need cancellation ownership across
assembly and encoding awaits. Producer sequence/identity/replay ordering and
cross-worker issue ownership also remain open. Whole-result Drop and variable
width decode work can remain linear. No crate release is justified by this scope.

Verify the archive with:

```sh
python read-evidence.py qualification-evidence.tar.gz --verify
```

The standalone reader checks every virtual record, including aliases. Its three
pure controls reject contradictory alias metadata; see `reader-validation.json`.

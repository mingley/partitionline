# Resource driver evidence

Ten tests exercise actual finite producer overload, stalled replies, slow
consumption, stop/resume and rejected corrupt receipts. Resume preserves its
original freeze and first attempt byte for byte. Failed attempts are retained
and block resume. All successful short runs release payload, join their tasks
and close their ephemeral peer port.

A fresh Kafka4.3.1 RF1 broker runs a five-second development calibration and
a two-second controlled-mode rehearsal above twice that observed completion
rate. Client samples and Linux RSS/thread/socket samples are retained. The
broker process is stopped and waited; both ports close and can be rebound.
This is driver validation, not the24-hour or performance qualification.

Stable Rust1.99 passes2002 default and2014 all-feature tests, strict format,
Clippy and rustdoc. Four packaged feature profiles compile20 documented
examples each. Python documentation tests pass18 cases. CI YAML and nine
existing CI gate negative cases pass. The new hosted lane has not been run.

`summary.json` records file hashes, exact scope and limits. `validation/`
retains successful and failed attempts, manifests, raw samples, logs, native
configuration and process receipts. Disposable native data directories and
test executable copies are omitted; referenced sample files and hashes are
retained. `source/` records the executed implementation and documentation.

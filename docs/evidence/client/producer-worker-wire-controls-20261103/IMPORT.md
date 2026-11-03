# Worker socket write controls

This is a test-only source patch for the v16/v17 fixed client. It remains
uncompiled and unrun. It does not implement socket reuse or establish a
performance result.

The proposed controls use a negotiated plaintext loopback TCP connection and
the actual Worker write path. The gate is limited to the single-writer,
release-after-reached fixture. The tests do not exercise TLS, SASL, an owner
transfer, local descriptor retirement, `Producer::send` or `Worker::run`.

The [patch](wire-controls.patch), [evidence](evidence.tar.gz), [pins](pins.json),
[receipt](receipt.json) and [independent review](independent-review.json) are
frozen. The prior v1 and v2 evidence and receipts are retained. The independent
review's original [script](independent-verify.py) uses the workspace paths
recorded in its receipt; [verify-import.py](verify-import.py) checks the copied
archives and can reconstruct the patch from a supplied fixed source tree and
catalog. Run `sha256sum -c SHA256SUMS` to check this directory.

The v17 queue benchmark also remains blocked until its baseline case-23
phase-specific witness is repaired and complete v17 qualification passes in
the same job.

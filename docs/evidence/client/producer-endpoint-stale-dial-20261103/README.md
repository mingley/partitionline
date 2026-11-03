# Producer endpoint changes during a dial

The v16 producer source can publish a completed worker dial after the cached address for that node changes. A conservative local route identity could distinguish A→B→A; it would not be a Kafka broker epoch. Metadata snapshot publication and existing or in-flight workers need separate handling.

The test-only patch gates a real loopback ApiVersions exchange while a node endpoint changes. It adds an unchanged-endpoint positive control and A→B and A→B→A rejection controls. These tests have not been compiled or run; their baseline outcomes are predictions. They inspect the slot after publication returns and do not establish transient-publication safety, actual misrouting, file-descriptor retirement, or retry safety.

The archive contains the patch, source pins, source-only verifier, sequential illustration, and design and diagnostic notes. The v16 failure projection does not prove this race caused those failures. No production fix or performance result is claimed.

The [independent source review](independent-review.json) checked the pinned patch and recorded the same limits. The [test patch](endpoint-stale-dial-tests.patch) has not been adopted.

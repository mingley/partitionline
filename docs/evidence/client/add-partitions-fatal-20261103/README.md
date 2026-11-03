# AddPartitions mixed fatal errors: isolated v6 evidence

This is a source-only draft against the exact v16/v17 fixed inputs. It has not
been compiled, run or independently approved for adoption. It does not change
main and does not include the separate coordinator LOAD retry draft. Do not
claim a measured fix, readiness or speedup from it.

## Source finding

Current main `7e91e2bee412857c728ecd5c7a368fdefacf7af7` and the v16/v17
candidate reduce the complete AddPartitionsToTxn result to its first nonzero
partition code. On a response `[COORDINATOR_LOAD_IN_PROGRESS,
PRODUCER_FENCED]`, the `txn_roundtrip` validator classifies the first code as
coordinator-retriable and may rediscover before the caller can inspect the
later fence. That is a source-derived path, not an observed broker run. A
rediscovery or transport failure can prevent the later error being applied.
Classic AddPartitions client versions 0–2 use classic encoding; version 3 is
flexible. This producer does not use broker-only version 4+; Produce v12 can
perform transaction-v2 partition registration without this RPC.

## Intended narrow behavior

The producer-specific reducer first completes the existing cooperative v0–v3
topics parser, checks for trailing response bytes, then selects the first
fatal code if any. This parses framing and known fields; it does not establish
that returned partition identities are complete or valid for the request. `UNKNOWN_PRODUCER_ID` is fatal only if the negotiated
coordinator versions cannot handle abortable errors. If there is no fatal
code, the first nonzero code is selected exactly as before. In particular,
this does not redefine the interaction between `OPERATION_NOT_ATTEMPTED`,
other abortable errors, and retryable errors. The public first-error decoder
and the existing retry sleeps and absolute deadline calculations are left
unchanged. Rejecting trailing bytes is an additional intentional change for
malformed responses.

The validator passed to `txn_roundtrip` and the caller's post-roundtrip decode
both use the reducer. Once the complete body has been parsed and its first fatal result observed,
they record fatal transaction state before the remainder of the reduction
scan, another decode, or cooperative partition cleanup can yield. V1 did not
publish until the reduction scan completed; v2 changes that behavior. The original classification function
shares the new fatal predicate; a source-derived full-i16 model verifies its
old and new categories are the same, including unknown codes and both
capability settings. This is a model check, not execution of Rust.

## Controls and limits

Four new tests are appended after the byte-identical original
`prepare_tests.rs` prefix. They are uncompiled and unrun:

* A version 0–3 matrix checks mixed orders, fencing, state-dependent
  `UNKNOWN_PRODUCER_ID`, unknown codes, first-nonfatal and first-public-error
  compatibility, truncated bodies and trailing bytes.
* A Worker-level local TCP fixture supplies LOAD/FENCED in both orders to
  `Worker::add_txn_partitions`, asserts the request fields, expects fatal
  state and retains the original test's absent FindCoordinator capability.
  A malformed trailing-byte variant expects a protocol error without fencing.
* A manual-poll control feeds 8,193 partitions with the first one fenced.
  The first poll stops in the structural parser with the flag still clear;
  the second stops in the reducer only after the fatal callback set the flag.
  Dropping at that witnessed second pending point must leave the fence set.
* Another Worker-level local TCP fixture supplies a nonfatal retriable mixed
  response, then either success or LOAD/FENCED; it checks both correlations,
  the resulting state and `txn_added`. The first response is
  `CONCURRENT_TRANSACTIONS`, which uses the outer Worker retry loop; this
  control does not exercise `txn_roundtrip` internal rediscovery or its second
  response. Setup and exchange are bounded by test-only timeouts. The fixture supplies version ranges directly; it does
  not negotiate ApiVersions, TLS or SASL and does not call the public
  `Producer::send` API.

V3 corrects the v2 source comment that called the body "valid". The existing
structural parser returning with no trailing bytes does not imply complete
semantic or request/response validation. The v3 source difference from v2
contains comments only; production behavior and all controls are unchanged.

The decoder currently accepts some malformed nullable arrays/strings as empty
and does not validate returned topics/partitions against the request, reject
duplicates, impose new allocation limits, or apply response throttle. This
patch does not repair those inherited gaps. Cancellation during the cooperative structural decode can prevent fatal state
from being recorded. So can cancellation after the body is parsed but at a
scan yield before a later fatal partition is reached. The cancellation control
only covers dropping the scan after it has observed the fatal code. No general
cancellation-safe transaction ownership or ordering guarantee is established. A cached earlier transaction error can remain the reported root
cause after a later fatal flag is set. Fatal response ordering, no later
Produce/EndTxn after fencing, retry behavior and malformed peers still need
real public-API/broker tests under supported authentication, concurrent calls,
deadlines and cancellation. The added tests are source only and have not
established those properties.

`verify.py` checks exact v16/v17 preimages, original test-prefix bytes,
forward/reverse patch reconstruction, and the classifier model. The archive
includes the exact sparse `before/` and `after/` trees; the verifier also
requires the retained `/workspace/partitionline` source/catalog, writable
`/workspace/work`, and `git`. It checks the source-derived retry model against
the exact `src/error.rs` SHA-256
`014c33e7716b004aa22ec46b4b7e0e9e67b54d982b69b8f152ee02eb421d45f2`
and both v16/v17 catalogs. It does not run Cargo or a broker. The
original candidate source and test timing thresholds are unchanged. Independent
review, Rust compilation, native tests and qualification remain necessary.

V1 is retained separately at `/workspace/work/add-partitions-fatal-20261103/`: its
patch SHA-256 is `66097f857a550d0a5647a057ccbd6c3bc8707f208196d7ea7885673f7ecc96f9`
and its evidence archive SHA-256 is `35959ff00753c688736849bc75367bd387d3c5e00906e52b6e993e968c4cac79`.

V2 is retained separately at `/workspace/work/add-partitions-fatal-v2-20261103/`: its
patch SHA-256 is `a4b26709bc76d487aaf4fc71e45d90dfe1943965e2c7ff645ca1874d406a7361`
and its archive SHA-256 is `420cd958d41a3a4c89931ff4fe812c92365f11522f061a41eb74849b98ecc202`.

V3 is retained separately at `/workspace/work/add-partitions-fatal-v3-20261103/`:
its patch SHA-256 is `cfdd103c0682819e1d113ff1b4f86aa389e2b36446decb7a09dbe56678bfbc39`
and its archive SHA-256 is `2ce03464e51e0647eb0087f1b3b413a49d5ffca20c89e11a3760de46a264d150`.
V4 changes only evidence packaging and these descriptions; its source and
patch bytes are identical to v3.

V4 is retained separately at `/workspace/work/add-partitions-fatal-v4-20261103/`:
its archive SHA-256 is `cd75679a69ae8a6ba86f8b7dbfc5af855d7e4d307cc1c415cc63ce71cfaf54fb`.
V5 changes only this documented cancellation limitation and the evidence
manifest; source, patch, verifier and controls are byte-identical to v4.

V5 is retained at `/workspace/work/add-partitions-fatal-v5-20261103/` with
archive SHA-256 `5f9dbbcae7b2e08c5891466822acdb870cbbbf23afa050da41bced56905b6f69`.
V6 pins the verifier's `src/error.rs` dependency; the source patch and tests
are byte-identical to v5.

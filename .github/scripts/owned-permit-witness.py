#!/usr/bin/env python3
"""Create controlled, test-only baseline and drop-order mutations for one CI job."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

BASE = "80a687ecf000601c2c374fb85b46aacf87b432de"
CANDIDATE = "7dcef586a8c6205cca10b71222088d83a84689fd"
PATH = "partitionline-broker/src/metadata.rs"
CANDIDATE_SHA256 = "4b58da7de8daf51d12c658c35a9252b91907e18475e49cb04fac3de751cb9a5b"


def source(commit, path=PATH):
    return subprocess.check_output(["git", "show", f"{commit}:{path}"], text=True)


def replace_once(value, old, new):
    if value.count(old) != 1:
        raise AssertionError(f"expected one matching hunk, found {value.count(old)}: {old[:90]!r}")
    return value.replace(old, new)


candidate = source(CANDIDATE)
if hashlib.sha256(candidate.encode()).hexdigest() != CANDIDATE_SHA256:
    raise AssertionError("candidate source mismatch")
baseline = source(BASE)
registry = json.loads(source(BASE, "tests/conformance/broker/implemented-api-versions.json"))
if hashlib.sha256(baseline.encode()).hexdigest() != registry["metadata_source_sha256"]:
    raise AssertionError("baseline source mismatch")
mode = sys.argv[1]
if mode == "baseline":
    # Add only test-only actor gates/call arguments plus the unchanged candidate
    # witness. Production behavior remains the baseline's borrowed permit.
    value = replace_once(
        baseline,
        "\n                            &change_tx,\n                        );",
        "\n                            &change_tx,\n                            #[cfg(test)]\n                            None,\n                        );",
    )
    value = replace_once(
        value,
        "\n                    &change_tx,\n                );",
        "\n                    &change_tx,\n                    None,\n                );",
    )
    value = replace_once(
        value,
        "    options: DataOptions<'_>,\n    changes: &watch::Sender<()>,\n) {\n    let mut pending: Option<Command> = None;",
        "    options: DataOptions<'_>,\n    changes: &watch::Sender<()>,\n    #[cfg(test)] mut pending: Option<Command>,\n) {\n    #[cfg(not(test))]\n    let mut pending: Option<Command> = None;",
    )
    helper_start = candidate.index("    type ClaimedFetchInfo")
    helper_end = candidate.index("    fn request(key: i16)", helper_start)
    helper = candidate[helper_start:helper_end]
    if helper.count("job.lease.request") != 2:
        raise AssertionError("candidate helper access mismatch")
    helper = helper.replace("job.lease.request", "job.request")
    helper = replace_once(helper, "Arc::new(Semaphore::new(1))", "Semaphore::new(1)")
    value = replace_once(value, "    fn request(key: i16)", helper + "    fn request(key: i16)")
    test_start = candidate.index("    #[tokio::test]\n    async fn canceled_claimed_fetch_keeps_admission")
    test_end = candidate.index("    #[tokio::test]", test_start + 10)
    test = candidate[test_start:test_end]
    anchor = "    #[tokio::test]\n    async fn unconsumed_data_receipt_keeps_admission_bounded()"
    value = replace_once(value, anchor, test + anchor)
elif mode == "lease":
    value = replace_once(
        candidate,
        "    request: R,\n    spare: S,\n    // Fields drop in order, so retained buffers are destroyed before admission.\n    permit: OwnedSemaphorePermit,",
        "    permit: OwnedSemaphorePermit,\n    request: R,\n    spare: S,",
    )
elif mode == "completion":
    value = replace_once(
        candidate,
        "    // Response storage drops before the lease if the receiver is canceled.\n    result: T,\n    lease: DataLease<R, S>,",
        "    lease: DataLease<R, S>,\n    result: T,",
    )
elif mode == "batch":
    value = replace_once(
        candidate,
        "    // Fields drop in order even if a reply waker panics.\n    bodies: Vec<R>,\n    spares: Vec<S>,\n    results: Vec<O>,\n    senders: Vec<Option<DataSender>>,",
        "    senders: Vec<Option<DataSender>>,\n    bodies: Vec<R>,\n    spares: Vec<S>,\n    results: Vec<O>,",
    )
elif mode == "restore":
    value = candidate
else:
    raise ValueError(mode)
Path(PATH).write_text(value)
print(f"{mode}: metadata source sha256={hashlib.sha256(value.encode()).hexdigest()}")

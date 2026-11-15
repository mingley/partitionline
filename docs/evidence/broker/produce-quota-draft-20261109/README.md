# Produce quota draft checkpoint

9 November 2026

The broker does not implement Kafka Produce bandwidth quotas. This is an
isolated, unqualified draft, not an implementation to enable or merge.

The [draft source](https://github.com/mingley/partitionline/tree/043b529acc1c71ee039529df2a516806405948ce)
is based on `a7b6edc1645e3a4789cf699a3b9e7656c55a3d10`. It explores an
explicit router-wide token bucket, actor-owned charges, a writer-captured
throttle-field offset, and an absolute transport resume time. It is a single
in-memory bucket shared by all identities; it is not Kafka per-user or
per-client quota behavior, and debt would reset on restart.

The draft adds a provided handler method and a new controlled outcome, while
keeping the existing response struct unchanged. A separate quota router
returns an error from legacy handler entry points that cannot carry a resume
time. Its actor code attempts to charge every Produce job that has started,
including grouped jobs and dropped reply receivers. None of these behaviors
has been established by a completed test.

## Checks

[Run 42671569976](https://github.com/mingley/partitionline/actions/runs/42671569976)
and [run 42672008026](https://github.com/mingley/partitionline/actions/runs/42672008026)
both ended with failure and GitHub reported zero jobs. The permitted run
metadata did not identify a cause. Neither run compiled or tested the code.

[Run 42672908995](https://github.com/mingley/partitionline/actions/runs/42672908995)
used a single pinned Ubuntu 24.04 job scaffold, but also ended in failure with
zero jobs. Its cause is not established, and it did not compile or test the
code. No live Kafka, socket, grouped-accounting, authentication-expiry,
shutdown, or comparative performance check has passed on the draft.

## Blockers

- The public controlled outcome accepts an arbitrary future resume time. The
  quota configuration caps its own delay, but another handler could otherwise
  hold a connection indefinitely. The shared transport needs a documented,
  independently enforced bound and fail-closed overflow behavior. Authentication
  expiry and shutdown must remain effective, and custom direct dispatchers must
  know how to enforce the same contract.
- Actor-owned charging must be demonstrated after admission when a caller drops
  its reply, including grouped jobs, errors and `acks=0`. The emitted Kafka
  throttle fields and muting need controls for versions 3–13, pipelining,
  prompt responses, slow writes and no-response requests.
- Adding a provided handler method can affect downstream method resolution.
  Existing handler implementations and struct literals need a consumer
  compatibility check. The changed Produce result and transport hot paths also
  require matched performance measurements with quotas disabled.
- The policy's accounting arithmetic, overflow behavior, persistent capped
  debt, authentication expiry/revocation and shutdown must pass deterministic
  tests. The current draft is not formatted or lint-qualified.

The [quota handoff design](https://github.com/mingley/partitionline/blob/2f5f6b2742e15aa6d22a89af4b3cd81bb34c0e09/docs/evidence/broker/quota-handoff-20261109/README.md)
records the intended accounting and transport boundaries. The previously
applied transport backpressure change does not implement quotas.

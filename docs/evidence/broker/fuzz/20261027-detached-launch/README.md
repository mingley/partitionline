# Detached broker fuzz campaign

The replacement campaign started on October 27, 2026 at 2:45 p.m. Chicago
time. Each of its five targets requires 24 uninterrupted hours. Its earliest
completion is October 28 at 2:45 p.m., if the environment and daemon remain
available. The launch snapshot is running evidence, not a completed result.

The detached container passed all nine supervisor checks using actual
instrumented executables before launch. It runs with an init process, three
CPUs, 4 GiB memory, 128 PIDs, read-only source/build mounts and no network.
Automatic restart is disabled. The tested build uses stable Rust 1.99.1 at
`d6643def5a08b302b05ef7380d72f13dd7e2a002`.

Container: `pl-broker-fuzz-campaign-20261027-03`

Output: `/workspace/work/broker-fuzz-container-20261027/campaign-03`

Inspect the daemon state and live heartbeat together; an old `running` file
alone is insufficient. Use the live result receipt and audit after completion.

```sh
docker --host=unix:///var/run/docker.sock inspect \
  --format '{{json .State}}' pl-broker-fuzz-campaign-20261027-03
python3 -B scripts/broker-fuzz-campaign.py audit \
  --output /workspace/work/broker-fuzz-container-20261027/campaign-03
```

For joined shutdown, stop that exact container with `docker stop --time 15`.
Neither earlier stopped attempt contributes elapsed time. `capture/` records
the image, launch arguments, limits, runtime versions and real container tests.
`live-snapshot/` contains immutable prefixes captured during the run.

Verify stored and reconstructed bytes with the common evidence verifier.
Production readiness and performance claims remain unqualified. The current
Fetch, waiter, write-deadline and conformance failures are recorded in the
[readiness assessment](../../qualification-20261027/status.json).

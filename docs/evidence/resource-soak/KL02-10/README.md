# KL02-10 controlled 24-hour overload soak

Launched 2026-10-14 on a Cursor cloud Linux VM against local Kafka 4.1.0
(`127.0.0.1:9092`). This is **not** Lab A. A launched job is not a pass.
Suite HOLD remains. No performance claim.

## Freeze

| Knob | Value |
|---|---|
| Profile | 256-byte values, 1 partition, 4 KiB producer/consumer payload budgets, max in flight 1, `acks=all`, idempotent, no compression |
| Topic | `pl-soak-kl0210` (operator-created, RF=1, not auto-created) |
| Calibration | 15 s at 800 offered/s → 799.41 completed/s |
| Offered rate | 1600 rec/s (≥ 2× sustainable) |
| Duration | 86 400 000 ms (24 h) |
| `slow_ms` | 25 |
| Binary | `/workspace/target/debug/examples/resource_soak` SHA-256 `d2ea6227c3303ecea927a2437e26d781b137ec5efad9e1163342869dfe6d50e4` |
| Output | `/tmp/kl02-10-soak` |

Baseline and source hashes: [baseline.json](baseline.json).
Launch freeze: [launch/frozen.json](launch/frozen.json).
2 s 2× rehearsal (invariant checker passed): [rehearsal/](rehearsal/).

## Job

| Item | Value |
|---|---|
| tmux | `kl02-10-soak` |
| Driver PID | 901849 |
| Child PID | 901851 |
| Receipt | [launch/receipt.json](launch/receipt.json) |

## Stop / resume

Stop:

```sh
touch /tmp/kl02-10-soak/attempt-0001/stop
# or SIGINT/SIGTERM to PID 901849
```

Resume without rebuilding (required; `scripts/run-resource-soak.sh` rebuilds):

```sh
python3 -B scripts/resource-soak.py --output /tmp/kl02-10-soak --resume
```

Check:

```sh
python3 -B scripts/resource-soak.py --output /tmp/kl02-10-soak --check
```

After 24 h, compare first steady-state hour vs final hour median RSS
(10% target) and run the invariant checker on the completed artifact.
Do not mark this card `done` from the launch alone.

## Host / time approval

This VM is not an approved Lab A / Kernel Integrity host. If that
approval is required before the 24 h result can qualify, the official
disposition is `blocked_external`. The local job still runs and retains
raw samples.

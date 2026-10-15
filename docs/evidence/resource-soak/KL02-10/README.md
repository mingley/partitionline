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

RSS drift (does not rebuild the frozen binary):

```sh
python3 -B scripts/check-soak-rss-drift.py /tmp/kl02-10-soak
```

The checker skips the first 60 s as startup, takes the next hour and the
final hour, and fails closed if elapsed time is under 24 h. A 10% median
RSS rise or fall fails. Passing it is not a performance claim.

First steady-state hour (startup + 60 min of soak monotonic time) is
recorded in [progress/first-hour.json](progress/first-hour.json):
median RSS 11.21 MiB over 69 861 host samples. That is a monitoring
snapshot, not the 24 h drift comparison.

T+2h (startup + 120 min) is recorded in
[progress/t-plus-2h.json](progress/t-plus-2h.json). Second-hour median
RSS is 10.39 MiB over 69 859 host samples (`ambiguous=0`,
`fetch_errors=0`). Latest RSS at capture was 6.96 MiB. Still not a
24 h pass.

T+3h (startup + 180 min) is recorded in
[progress/t-plus-3h.json](progress/t-plus-3h.json). Third-hour median
RSS is 6.90 MiB over 69 836 host samples (`ambiguous=0`,
`fetch_errors=0`). Latest RSS at capture was 6.77 MiB. Still not a
24 h pass.

T+4h (startup + 240 min) is recorded in
[progress/t-plus-4h.json](progress/t-plus-4h.json). Fourth-hour median
RSS is 6.70 MiB over 69 863 host samples (`ambiguous=0`,
`fetch_errors=0`). Latest RSS at capture was 6.70 MiB. Still not a
24 h pass.

T+5h (startup + 300 min) is recorded in
[progress/t-plus-5h.json](progress/t-plus-5h.json). Fifth-hour median
RSS is 6.70 MiB over 69 931 host samples (`ambiguous=0`,
`fetch_errors=0`). Latest RSS at capture was 6.70 MiB. Still not a
24 h pass.

T+6h (startup + 360 min) is recorded in
[progress/t-plus-6h.json](progress/t-plus-6h.json). Sixth-hour median
RSS is 6.70 MiB over 69 861 host samples (`fetch_errors=0`). Latest
RSS at capture was 8.04 MiB. Workload `ambiguous` rose from 0 to 16
in this window. Still not a 24 h pass.

T+7h (startup + 420 min) is recorded in
[progress/t-plus-7h.json](progress/t-plus-7h.json). Seventh-hour median
RSS is 6.57 MiB over 69 816 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.56 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+8h (startup + 480 min) is recorded in
[progress/t-plus-8h.json](progress/t-plus-8h.json). Eighth-hour median
RSS is 6.53 MiB over 69 850 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.95 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+9h (startup + 540 min) is recorded in
[progress/t-plus-9h.json](progress/t-plus-9h.json). Ninth-hour median
RSS is 6.47 MiB over 69 877 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.47 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+10h (startup + 600 min) is recorded in
[progress/t-plus-10h.json](progress/t-plus-10h.json). Tenth-hour median
RSS is 6.47 MiB over 69 857 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.47 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+11h (startup + 660 min) is recorded in
[progress/t-plus-11h.json](progress/t-plus-11h.json). Eleventh-hour median
RSS is 6.47 MiB over 69 904 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.47 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+12h (startup + 720 min) is recorded in
[progress/t-plus-12h.json](progress/t-plus-12h.json). Twelfth-hour median
RSS is 6.47 MiB over 69 923 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.47 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+13h (startup + 780 min) is recorded in
[progress/t-plus-13h.json](progress/t-plus-13h.json). Thirteenth-hour median
RSS is 6.47 MiB over 69 939 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.47 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+14h (startup + 840 min) is recorded in
[progress/t-plus-14h.json](progress/t-plus-14h.json). Fourteenth-hour median
RSS is 6.47 MiB over 69 950 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.48 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+15h (startup + 900 min) is recorded in
[progress/t-plus-15h.json](progress/t-plus-15h.json). Fifteenth-hour median
RSS is 6.48 MiB over 69 947 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.48 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+16h (startup + 960 min) is recorded in
[progress/t-plus-16h.json](progress/t-plus-16h.json). Sixteenth-hour median
RSS is 6.48 MiB over 69 960 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.48 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

T+17h (startup + 1020 min) is recorded in
[progress/t-plus-17h.json](progress/t-plus-17h.json). Seventeenth-hour median
RSS is 6.48 MiB over 69 962 host samples (`fetch_errors=0`). Latest
RSS at capture was 6.48 MiB. Workload `ambiguous` stayed 16. Still
not a 24 h pass.

Live progress (never a 24 h pass):

```sh
python3 -B scripts/check-soak-rss-drift.py /tmp/kl02-10-soak --progress
```

## Host / time approval

This VM is not an approved Lab A / Kernel Integrity host. If that
approval is required before the 24 h result can qualify, the official
disposition is `blocked_external`. The local job still runs and retains
raw samples.

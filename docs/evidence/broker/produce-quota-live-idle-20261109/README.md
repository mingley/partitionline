# Produce quota timing and idle disconnect observations

These observations concern the isolated
[quota prototype](../produce-quota-candidate-20261109/README.md), not the
broker on main. The prototype remains unqualified and unapplied. It implements
a router-wide in-memory token bucket, not Kafka's sampled per-client or
per-user quota. Its earlier full all-feature run failed a minority-restart
router test outside this investigation; that failure's cause remains unknown.
A separate earlier default failure also remains unresolved.

## Apache Kafka timing

[Run 42701446902](https://github.com/mingley/partitionline/actions/runs/42701446902)
started Apache Kafka 4.3.1 on an x86 hosted runner using image digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
It created one topic with one partition and configured a 20-byte-per-second
producer quota for each of three client IDs. The
[probe](live-probe.py.txt) sent existing Apache-derived Produce fixtures over
one plaintext loopback connection per version. It changed the client ID in
each fixture, checked the success code and increasing offsets on each
connection, then sent Metadata immediately after the first response reporting
at least 300 ms of throttling. The [workflow](live-workflow.yml.txt) and
[raw samples](live-samples.jsonl) are retained here. The probe checked the
Metadata correlation ID and a nonempty broker list; it did not independently
read back the produced records.

| Produce version | Reported throttle | Produce response RTT | Following Metadata RTT |
| --- | ---: | ---: | ---: |
| 3 | 4,695 ms | 2.654 ms | 4,697.742 ms |
| 5 | 4,699 ms | 1.208 ms | 4,699.950 ms |
| 6 | 4,699 ms | 1.240 ms | 4,700.117 ms |

For these three observations, the broker sent the Produce response promptly
and the next response followed the reported mute. The draft's earlier socket
controls separately observed prompt responses and later resumption; they did
not run in the same process or under matching quota rules. These samples do
not establish quota equivalence, a timing bound, TLS behavior, compatibility
for other clients or versions, or a performance ranking.

## Disconnect during a controlled pause

The [closed-peer witness](closed-peer-witness.rs.txt) uses a synthetic
controlled handler, plaintext loopback, one connection slot and a ten-second
pause. It closes the first peer after receiving its response, then observes a
second connection rejected before shutdown. The shutdown report shows one
accepted connection, one overload rejection and the first task stopped by
shutdown. The witness passed in both default and all-feature profiles in
[run 42701818107](https://github.com/mingley/partitionline/actions/runs/42701818107).
It does not establish behavior for TLS, SASL, every close mode or sustained
load. Its [workflow](closed-peer-workflow.yml.txt) is retained.

An isolated [refinement](idle-eof.patch) to the draft watches for EOF only
when no application input is already buffered. It retains at most one
additional application byte for the next frame and stops watching after that
byte. Thus a close behind an already buffered or newly arrived byte can still
hold the connection until the pause ends. It changes the draft's documented
no-read-during-pause behavior and has no measured overhead or live TLS/Kafka
qualification.

Two [controls](idle-eof-controls.rs.txt) check replacement admission after an
idle close and retention of a split frame prefix through the pause. On the
baseline with controls added
[`83b58fa`](https://github.com/mingley/partitionline/commit/83b58fa48955ced7357b8f0e2bbed8de6abf5d1e),
the first failed and the second passed in each feature profile
([run 42702078329](https://github.com/mingley/partitionline/actions/runs/42702078329)).
On refinement
[`7a8fbab`](https://github.com/mingley/partitionline/commit/7a8fbab3457ba3ad4677e612b2ac36631843919d),
both passed in each profile
([run 42702307127](https://github.com/mingley/partitionline/actions/runs/42702307127)).
The [workflow](idle-eof-workflow.yml.txt) is retained. Applying the retained
patch hunks to the draft's exact transport source reproduced the refinement
source bytes in an independent text comparison.

An [expanded run](https://github.com/mingley/partitionline/actions/runs/42702610260) on
[`c441a1f`](https://github.com/mingley/partitionline/commit/c441a1f9b01455ecef3d2b78232269dde86a4c9d)
used the same refinement and control source with an expanded
[workflow](idle-eof-expanded-workflow.yml.txt). It passed the two new controls
in each feature profile, six existing quota controls in each profile, six
all-feature controlled-transport tests (549 filtered out), one authenticated
quota control (27 filtered out), and all-target, all-feature Clippy. Formatting
failed on the new `quota_pause_reuse.rs` test file, so the overall workflow
failed. No broad behavioral suite, doctest, live TLS test, live Kafka test of
the prototype, or performance comparison ran for the refinement. Neither the
refinement nor the quota prototype is applied to main.

# Private runtime causal oracle preparation

This is WORK-only independent Python preparation. It imports no Rust and runs
no Cargo or broker process. The original `peer_bytes.py`, `check-peer-bytes.py`
and `decoder-controls.json` remain byte- and full-mode-identical; their initial
two positive and 26 negative preparation cases are preserved.

The new checker decodes actual private proxy packets and typed owner bodies,
then independently replays the recorded dynamic content/election/image journal
bytes with the previously authored KL11-74/KL11-15 Python oracle. It checks
complete Hello group/genesis and full directory identities, exact per-session
RPC and directional order, request/response configuration and sequence,
source-generated record bytes, receive-side durable grants/prefixes, exact
once-only prepared-request consumption, current-view election and commit
majorities, and admission against the owner's actual recorded deadline values.
Every changed durable counter must have an independently decoded checkpoint;
confirmed content/election operation prefixes cannot change between checkpoints.

A proxy forwarding attempt, destination callback dispatch and source owner
consumption are separate facts. Only the exact owner ACK plus its actual packet,
request origin and persisted peer response supplies consumed response authority.
The causal graph orders each owner and TCP direction. It never orders unrelated
PID/restart milliseconds. Restart bindings use byte-exact prior content/election
states. A positive control shifts an entire restart clock epoch while keeping
its local elapsed intervals; that must remain accepted.

`check-runtime-capture.py` also binds the actual diagnostic command, log,
capture environment, complete producer capture map, materialization origin and
actual executed retained ELF. It independently checks every immutable source
path's Git blob, SHA256, byte length and full 07777 permission mode before and
after replay. This does not transfer the historical403 full qualification to
new runtime source.

The final `sealed-development-ea9ff293-01` run uses the actual diagnostic at
`ea9ff29393526b57e1203612bb12c9c99deccc61`. It verifies all 73,171 source files.
Its two genuine three/five-node histories contain ten owner clock epochs,
1,218 actual TCP frames, 90 complete Hello sessions, 492 successful exact owner
ACK packet bindings, 94 remote WAL authority bindings, 18 current-term NEW-view
commit majority proofs, 15 majority leader activations, ten bounded write
admissions and two exact restart bridges. Another 13 owner traces have raw
durable-summary/budget checks only; they are not independently qualified as
TCP-consumption histories. The actual producer integration command has four
behavior tests and an inactive child launcher; its unit command also runs four
owner controls. Those Rust assertion counts are not substituted for independent
causal results.

The final control run has two positives and 32 intended negative rejections.
Packet negatives repair Castagnoli CRC and keep TCP length, then pass the
independent frame decoder before being rejected on session/identity/correlation
or authority grounds. They cover wrong RPC, leader/peer directory, epoch,
sequence, leader ID, fabricated match/refusal, altered opaque source bytes and
unknown Hello target. Trace controls cover missing request origin, missing
consumed-majority election, expired ACK/quorum write, clock/ordinal corruption,
raw-summary mismatch and actual configured task/socket/queue/byte/frame/record
limits. Source bytes/modes, original captures and the legacy 2/26 files are
unchanged around the two recorded Python commands; actual affinity is2,4.

This remains development replay, not final KL11-76 qualification. The retained
ea9 frames include Begin/Begun and one Chunk attempt, but no Finish/Finished
34/35 packets. Complete image stream/Install checks are prepared in the checker
but their successful runtime path remains unexecuted in this input. Actual
snapshot installation cannot be claimed from these cases. Timeout/disconnect
owner rows omit exact targets; typed error outcomes and peak/ joined-worker
lifecycle counters are absent. Numeric Failure packets bind to actual rejected
input, but their specific error-code meaning cannot be independently checked.
Recorded configuration envelopes bound explicit Rust buffers; they do not
measure OS socket buffers, allocator overhead or RSS. The observed topology
uses genesis voters; no future non-genesis observer route admission is proved.
Trusted private framing is not native Kafka Fetch/FetchSnapshot or general
KRaft compatibility.

The earlier reviewer-only Hello-ack reversal and lexicographic filename-order
assumptions are preserved under `development-causal-attempt-1` and2. They were
checker corrections, not product failures. Earlier narrower successful drafts
and control runs remain separate. No captured producer file was edited.

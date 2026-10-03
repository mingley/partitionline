This independent Python checker reads captured WAL, election, image and typed
exchange bytes. It imports no broker implementation. `wal_oracle.py` and
`election_oracle.py` are unchanged copies of the independently authored KL11-73
decoders; their hashes are pinned with this source. `snapshot_oracle.py` parses
PLSNAP01 itself and adds authoritative operation-5 replay without selecting
unreceipted images. Reserved fields, bounds, all checksums, group/descriptor
binding, prior state, committed overlap, same-term content and retained suffix
are checked explicitly.

`history_oracle.py` extends the previous causal checker with selected-image
offers, exact emitted chunk bytes, declared partial slicing, bounded ordered
assemblies, durable selecting receipt provenance and correlated snapshot ACKs.
Commit advances require a current-term distinct majority or a causally verified
configured leader delivery. Captured canonical committed bytes must agree with
both independently replayed raw WALs and the other replicas. Local term and vote
come from the receiving election WAL, never from an image. Parent clock reversal
is rejected: the real failed development captures and the subsequent test-only
handoff correction remain preserved separately.

Raw checkpoint ordinal can be the following non-mutating exchange event. The
selecting receipt is therefore bound to its exact prior WAL operation count in
the earliest subsequent captured WAL, and all captured states are checked at
their actual ordinals. Missing receipt, wrong descriptor or wrong correlation
fails independently of the reported selected image.

`check-counterexamples.py` retains original positive controls and creates fresh
explicitly changed inputs. Outer Journal checksums are repaired; structural image
mutants repair image seals except the deliberate truncation. These checker
controls are separate from actual process exits/I/O failures and compiled Rust
guard mutants. `run-final.py` binds the pushed checker source and hashes every
raw input before/after each execution. Development outputs are not final source
qualification.

This is a finite captured-history checker, not exhaustive model checking, power
loss certification, a physical compaction/application-state fold, a Kafka
snapshot-wire implementation or production qualification. It does not infer
election liveness, external network behavior or a performance result from
accepted local histories. The full immutable broker matrix, actual owner fault
cuts, Apache component mapping and compiled guard results are separate evidence.

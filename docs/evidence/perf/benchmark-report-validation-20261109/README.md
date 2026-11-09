# Benchmark report validation

The report validator now checks several internal inconsistencies that the
previous version accepted. It compares declared and effective acks and
idempotence, rejects unsupported acks and idempotence without acks=all, and
rejects duplicate IDs or an unverified record history when records are counted.
It checks that each reported partition delta matches its start and end offsets
and that the deltas sum to the reported total. Missing IDs remain errors.
`--require-clean` now rejects any retained failed attempt, including broker
errors, timeouts and aborts, as well as recorded integrity failures. Normal
validation can still retain a failed attempt as evidence.

Transactional control records can advance log offsets, so transactional
profiles no longer require the total offset delta to equal the number of
acknowledged records. Their partition arithmetic and record-ID checks still
apply. This does not itself prove a transaction or its read isolation.

## Controls

The baseline source with new controls was
[`5fbc54d`](https://github.com/mingley/partitionline/tree/5fbc54dcea0776a5878d41b8b1395e7867062543).
Its [run](https://github.com/mingley/partitionline/actions/runs/42701475784) ran 31 report
test methods and failed 12 new subcases: invalid acks, mismatched idempotence,
an unverified or duplicate history, and inconsistent offset rows or totals.
The matrix step did not run after that failure.

The [candidate](https://github.com/mingley/partitionline/tree/b41c0f3bb753fdb305ce0ada84ac5c0a2196b6ff)
passed [34 report tests and 27 matrix tests](https://github.com/mingley/partitionline/actions/runs/42702200037)
on a hosted x86 Linux runner with Python 3.12. The tests include valid ack
aliases, null-broker formats, a transactional offset with an extra control
record, and failed-attempt retention. They use synthetic fixtures.

The validator checks the values in the result document. It does not read or
authenticate the claimed raw artifacts, verify that declared settings were
used at runtime, establish that peers did equal work, or independently verify
records. A successful validation or `--require-clean` result is not a
benchmark-cell pass and does not lift Suite HOLD. No Kafka performance
comparison was run as part of this change.

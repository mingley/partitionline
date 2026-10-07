# Paired-run qualification

The runner randomizes cells and A/B order, warms up each arm, retains every
attempt and checks actual effective settings before provisioning. Unique owned
topics have durable create/delete receipts. Resume checks source and artifact
hashes. Linux process groups are terminated and joined on normal interruption,
crash or timeout, including adopted children.

Fifteen process tests and twenty reporter tests pass. Unit result fixtures are
explicitly synthetic. Actual Kafka4.3.1 control runs used the same C driver in
both arms: two repetitions, four fresh topics,256 timed records acknowledged
and verified, and32 excluded warmup records. Independent Apache CLI checks
confirmed36 records per partition. All four topics were deleted, the broker
was waited, and both ports closed and were reusable. An actual Java preflight
was rejected before topic creation for missing comparable settings.

The source copies, first failures, command receipts, raw records and native
broker closure are retained under validation/. summary.json contains scope,
hashes and limits. The workflow includes these tests, but hosted CI was not
run. These controls do not establish a peer speed comparison or production
readiness. The frozen scenario registry and Suite HOLD remain unchanged.

# Isolated AddPartitions live qualification

This branch-only workflow tests the existing source-only candidate. The focused frozen runner checks the candidate against its original pinned base. A separate job applies the same pinned patch to commit `3f45fe6255e72abc6b0c4488bc13117779c50787` and runs its focused tests, a single-node Apache Kafka 3.9.1 transaction, loopback protocol observation, coordinator state inspection, and Kafka CLI readback. It captures the image digest, exact source hashes, commands and attempt logs.

The observer forwards Kafka frames unchanged and parses only API key 24 version 3. The readback explicitly uses read-uncommitted, and the coordinator state is checked separately. This does not test recovery after failure, broker replication, authentication, load, performance, cancellation, concurrency, or broader broker versions. A successful run does not establish production readiness or global performance claims.

# AddPartitions retry deadline control

This isolated control applies the frozen AddPartitions response-coverage patch to source commit b631809f0e03ba6f65f808e51d8e47e3fd9380d1, then inserts a test-only checkpoint immediately after the post-backoff deadline check. The checkpoint waits until the already-computed deadline has passed before the next loop iteration. A second case exhausts the budget naturally inside retry backoff.

The control submits two synthetic accepted records to a real worker method, checks both record outcomes, buffer release, pending identities, and sequence rewind, then compares the transactional error state. It does not exercise public enqueue, transaction generation changes, or live-broker failure injection. The crossing case is expected to fail the abort-required assertion on the existing code. No repair is included.

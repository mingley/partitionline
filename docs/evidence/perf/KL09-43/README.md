# KL09-43 per-round fetch task spawn

Parent `2cd461d2c3d335f40a1492f4b645f86a363dba07`. No production change.

`nb-fetch-multinode` used one fetch round and three leader requests. Client CPU was 131.929 ms. A current-thread `JoinSet` of three already-complete tasks cost 1.02 µs per round, 0.0008% of that CPU. Allowing 100× for the real fetch future still leaves the spawn under 0.1% of the round. Three task allocations would be about 0.001% of the round's 289,865 allocations.

The cell's wall time is 351 ms. Most of the non-CPU gap is the 50 ms slow leader, which is the KL09-44 question, not task-spawn CPU.

The spawn is below 1% of the named cell, so the card is `rejected-no-gain`.

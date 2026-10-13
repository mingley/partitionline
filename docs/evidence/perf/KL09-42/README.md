# KL09-42 prefetch overlap on nb-fetch-appdelay

Parent `2cd461d2c3d335f40a1492f4b645f86a363dba07`. No production change.

The cell fetches until 20,000 records, then sleeps 1 ms once per returned batch. The null broker returned 135,000 records in a single Fetch, which already covers the target. Measured wall time was 174.2 ms, so that one sleep is 0.57% of the cell. There is no later Fetch whose network wait can hide behind application work. Client CPU for the round was 54.632 ms and does not include the sleep.

The overlap the card targets is below 1% of the named cell, so the card is `rejected-no-gain`.

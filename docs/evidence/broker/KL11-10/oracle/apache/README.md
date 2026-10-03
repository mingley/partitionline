This oracle executes the official Apache Kafka 4.1.2, 4.2.1 and 4.3.1 `UnifiedLog` and `LogSegment` classes from their checksum-pinned release distributions. The three client JARs report the exact source commits retained in `pins.json`. No Kafka broker process, Scala `Partition` instance, partitionline runtime, replicated durability or DeleteRecords network behavior is exercised here.

The independently Apache-produced inputs are the committed Fetch corpus's 104-byte three-record batch and 78-byte one-record batch: offsets 0–3, timestamps 1000, 1007, 1003 and 1010. The probe resets only the input batch base offset to zero for actual `appendAsLeader`; that field lies outside the batch CRC. Apache assigns offsets and leader epoch through its normal append method. The pinned `LogConfig` internal segment-size override is 120 bytes, producing selected segment bases 0 and 3. Partitionline's journal framing uses a different rolling byte threshold; the equivalent segmentation and record history matter here.

The positive matrix has 67 actual assertions per replay, two clean histories per release: 402 assertions. It checks high-watermark protection, monotonic logical floor, rejection before floor/above high watermark, containing whole-batch reads, supplied floor checkpoint replay, whole-segment deletion, strict age equality/one-millisecond expiry, size targets 78/79, active retirement to an empty successor, and Apache's `NO_TIMESTAMP` file-time fallback. Four deliberately incorrect assertions per release must fail at the exact named assertion: 12 controlled failures. Every earlier assertion must pass, so an unrelated crash cannot qualify a negative control. Full source, class, JAR, command, observation and raw component-directory hashes are retained.

Three differences are explicit:

- Apache does not synchronously write the logical-floor checkpoint in `maybeIncrementLogStartOffset`. Reopening with supplied floor 2 preserves it; reopening with supplied floor 0 returns floor 0 while the containing segment remains. This component replay is not proof of Apache durable acknowledgement. Partitionline publishes and synchronizes its V2 manifest before success, a stronger local floor contract whose fault/restart proof is separate.
- Apache uses a segment's file modification time when its verified maximum record timestamp is negative. The probe executes a `NO_TIMESTAMP` batch and controlled public `Time` input, sets the selected segment's file time to 9000, retains it at time 9100 under age limit 100, and deletes it at 9101. Partitionline intentionally retains wholly negative/unknown-time segments under age. Guarded explicit deletion and size retention still apply locally.
- With floor 2 inside the first batch, timestamp query 1004 returns no result in Apache: the first segment's historical maximum 1007 qualifies, but its qualifying record at offset 1 has been retired. `UnifiedLog` returns that segment's empty filtered search without examining the later segment. Partitionline's bounded complete scan returns offset 3/time 1010. This is a local scan policy difference; neither result is presented as cross-implementation equivalence. Query 1008 reaches Apache offset 3 normally.

`reviewed-source-annotations.json` identifies separately retained source-only facade handling: DeleteRecords `-1` conversion, other negative-offset rejection, mapped duplicate offsets and unspecified response order. Those source references are not counted as executed component assertions. Apache schedules physical file cleanup through its real single-thread scheduler with a 60-second delay; the short probe retains raw renamed files and shuts the scheduler down. Logical deletion observations do not assert immediate physical unlink, power-loss safety or partitionline's cleanup ordering.

Run from the repository root with fresh output and scratch directories:

```sh
python3 docs/evidence/broker/KL11-10/oracle/apache/prepare-and-run.py \
  --work /workspace/work/retention-apache/reproduction \
  --output /workspace/work/retention-apache/reproduction-results \
  --seed-root partitionline-broker/tests/fixtures/fetch
```

The runner needs JDK 17's compiler module, existing checksum-matching source archives and official distributions. It uses CPU affinity 0–2,4 and a 128 MiB heap for each component execution. The only source adaptation for older releases changes the three record-package imports; actual storage methods and input histories remain identical. Private test doubles, rebuilt Apache classes and mocked storage are not used. The custom clock is an input through Apache's public `Time` interface; all scenarios use the real `KafkaScheduler`, filesystem, indexes and record classes.

The initial expected offset-3 timestamp assertion failed against Apache and is preserved under `development/attempt-1`. The next run reached the separate sentinel scenario and rejected its missing follower leader epoch; `development/attempt-2` preserves that harness input defect. Setting the actual sentinel batch's epoch to zero corrected the input. `attempt-3` then passed all 67 assertions. These are retained oracle-development outcomes, not partitionline regressions.

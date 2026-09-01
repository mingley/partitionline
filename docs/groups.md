# Groups

Classic range, sticky, cooperative-sticky (KIP-429), KIP-848
(`join_consumer`), and KIP-932 share groups.

## Classic

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::{ConsumerConfig, ConsumerGroup};

let mut group = ConsumerGroup::join_topics(
    ConsumerConfig::bootstrap(["127.0.0.1:9092"]),
    "workers",
    ["orders", "payments"],
)
.await?;
let recs = group.poll().await?;
group.commit_with_metadata(recs.next_offsets()).await?;
group.leave().await?;
# Ok(())
# }
```

| Join | Assignor |
|---|---|
| `join` / `join_topics` | range |
| `join_sticky` / `join_sticky_topics` | sticky |
| `join_cooperative_sticky` / `join_cooperative_sticky_topics` | cooperative-sticky |
| `join_with_assignors` / `join_with_assignors_topics` | JoinGroup Protocols of N |
| `join_consumer` / `join_consumer_topics` | KIP-848 |

`*_matching` variants are Java `subscribe(Pattern)`: the client re-lists
cluster topics on poll when `metadata.max.age.ms` elapses (names starting
with `__` are skipped).

`subscribe` / `unsubscribe` change topics without dropping the handle.
`group_instance_id` is static membership. `enforce_rebalance` /
`enforce_rebalance_with` rejoin on the next poll (JoinGroup v8+ Reason).

Range and sticky assign each topic independently among members who
subscribed to it. Cooperative-sticky keeps owned partitions until the
owner revokes them, then rejoins so the new owner can take them.

## Commits

`auto_commit(true)` commits after poll (off by default; Java defaults to
on). `auto_offset_reset` is used when the group has no committed offset
(**Earliest** by default; Java's default is `latest`).

| Method | Java |
|---|---|
| `commit` / `commit_timeout` | `commitSync` / `commitSync(Duration)` |
| `commit_with_metadata` | `commitSync(Map)` |
| `commit_async` / `commit_async_with` | `commitAsync` (queued, sent on poll / leave) |
| `committed` / `committed_timeout` | `committed` / `committed(Duration)` |

`commit_with_metadata(recs.next_offsets())` matches
`commitSync(records.nextOffsets())`.

`max_poll_interval` is Kafka `max.poll.interval.ms` (default 5 minutes).
If it is exceeded, the next poll errors and the heartbeat thread leaves
the group.

`close` / `close_timeout` cap `leave`. `fetch_timeout` / `poll_timeout`
are Java `poll(Duration)`.

## Share groups (KIP-932)

`ShareGroup::join` / `join_topics` / `join_matching` / `subscribe` /
`subscribe_matching` / `unsubscribe` / `poll` / `accept` / `release` /
`reject` / `leave`.

`poll` returns `ShareRecords`. Acknowledge with ACCEPT, RELEASE, or
REJECT.

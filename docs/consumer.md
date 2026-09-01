# Consumer

Manual assignment. For groups, see [groups.md](groups.md).

```rust,no_run
# async fn example() -> partitionline::Result<()> {
use partitionline::Consumer;

let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
consumer.assign("events", 0, 0).await?;
let recs = consumer.fetch().await?;
# let _ = recs;
# Ok(())
# }
```

`fetch` / group `poll` return `ConsumerRecords` (`count`, `partitions`,
`records`, `next_offsets`). `fetch` talks to every partition leader at
once when there is more than one.

## Assignment

| Method | What it does |
|---|---|
| `assign(topic, partition, offset)` | one partition |
| `assign_topic(topic, offset)` | every partition of that topic |
| `assign_many` / `assign_partitions` | replace the assignment |
| `unassign` | drop it |

`assign_partitions` is Java `assign(Collection)` and uses
`auto.offset.reset` for the start offset.

## Seek, pause, wakeup

`seek` / `seek_to` / `seek_with_metadata` / `seek_to_beginning` /
`seek_to_end` (and the `_of` variants) move the next fetch offset.
`seek_with_metadata` is Java `seek(TopicPartition, OffsetAndMetadata)` and
sends Fetch `LastFetchedEpoch`.

`pause` / `resume` skip partitions without dropping the assignment.

`Consumer::wakeup` / `WakeupHandle` interrupt an in-flight fetch.

`position` is the next fetch offset. `current_lag` is high watermark minus
position.

## Offsets and metadata

`partitions_for` returns leader, replicas, ISR, offline replicas, leader
epoch. `beginning_offsets` / `end_offsets` / `list_offset` /
`offsets_for_times` wrap ListOffsets. Most of these have a `_timeout`
overload matching the Java `Duration` methods.

`FetchedRecord::leader_epoch` is the record-batch partition leader epoch.

## Fetch knobs

`ConsumerConfig::max_bytes` sets both `fetch.max.bytes` and
`max.partition.fetch.bytes`. `fetch_max_bytes` / `max_partition_fetch_bytes`
set them independently (default 16 MiB each).

`isolation_level` is `IsolationLevel`. `rack` enables fetch-from-follower.

Fetch is **v4–v17**. v12+ sends `LastFetchedEpoch` and seeks on
`DivergingEpoch`. Fenced partitions recover with OffsetForLeaderEpoch.

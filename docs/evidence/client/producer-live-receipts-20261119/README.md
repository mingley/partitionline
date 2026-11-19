# Producer delivery on Kafka 4.3.1

Each lane starts an owned Apache Kafka broker on Linux x86_64. The
broker runs from the current compatibility pin for Kafka 4.3.1. It has
three topics with two partitions each, replication factor one, and
`min.insync.replicas=1`. The test uses plaintext and a producer with
`acks=all`, idempotence disabled and one request in flight.

The Rust test verified 3,072 `send_all` receipts against topic, partition
and offset in input order. It queued another 384 records with `try_send`
and verified the aggregate counts and released buffer after flush.
It queued the final 384 records and closed the producer. A surviving
handle saw 3,840 queued and acknowledged, zero delivery errors, zero
buffered bytes and `Error::Closed` on another send.

The Apache Java command-line tools independently checked end offset 640
on all six partitions and read every key, value and offset in order.
Both lanes verified 3,840 stored records and produced the same six
readback digests. [Readback receipts](verification.json)

Rust 1.100.0 passed formatting and strict Clippy. The default and
all-feature live tests passed at
`e4e94790eb51fa5c777db72f506a18da6de9b396`. The test requires successful removal of its broker and
volumes. The first attempt stopped at the crate documentation lint.
[Passed run](https://github.com/mingley/partitionline/actions/runs/44634000781) ·
[Initial run](https://github.com/mingley/partitionline/actions/runs/44633648646)

Run the test on a Linux x86_64 host with Docker and stable Rust.

```sh
bash scripts/ci-producer-live-receipts.sh default
bash scripts/ci-producer-live-receipts.sh all
```

The test covers healthy delivery with fixed partitions on one broker.
Fault recovery, idempotent retry, transactional and replicated profiles
have separate qualification work.

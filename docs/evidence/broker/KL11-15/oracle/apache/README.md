This probe executes official Apache snapshot writer/reader components from Kafka
4.1.2, 4.2.1 and 4.3.1. It checks publication, incomplete-file cleanup, frozen
state, record order, checksums and the exclusive snapshot offset versus inclusive
last-contained offset. It executes a deliberately wrong offset assertion against
each unchanged library. This is component evidence; it does not establish
partitionline installation, durable replication or Kafka network interoperability.

The source archives are pinned to peeled release commits. The original Apache
distributions are verified against their published SHA512 files, and embedded
client version/commit properties must match. Each run retains LICENSE, NOTICE and
the selected implementation/test sources, every source/JAR/class hash, adapted
Java sources, exact commands and raw output. The reviewed adaptations are the
pre-4.3 record package and the 4.1 builder's `setMaxBatchSize` method name.

The full frozen Apache snapshot contains two opaque records, `alpha=a` and
`beta=b`, with snapshot offset 3 and epoch 5. That offset maps to last-contained
offset 2. Partitionline's normalized term is 6 for this epoch. The custom
partitionline image stores its complete opaque prefix and an explicit completion
seal; its serialization is independent of Apache's snapshot record format.

A controlled boundary deletes the complete final footer batch of an Apache
snapshot. The bare Apache reader accepts the complete preceding record prefix.
This input violates the completed transfer contract and is not a KafkaRaftClient
history. Partitionline must reject a missing completion seal before installation.
The earlier setup failure using the later builder method name on 4.1 is retained
under `preparation/`; it makes no runtime claim.

Run `python3 prepare-and-run.py --work /absolute/fresh-work --output
/absolute/fresh-output`. Inputs default to the existing downloaded source archives
and verified Apache distributions; alternate paths are explicit CLI arguments.

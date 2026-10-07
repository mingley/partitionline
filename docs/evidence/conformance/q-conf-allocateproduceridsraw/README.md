# AllocateProducerIds checks

AllocateProducerIds v0 has fixed fields and is available on Kafka controller
listeners. There is no standard Java Admin method. The checks use actual Apache
request builders, serializers and parsers, plus Java and Rust raw connections
to disposable Kafka 4.1.2, 4.2.1 and 4.3.1 controllers.

The declared cases cover field defaults and limits, error factories, truncated
messages and unknown tags. Rust preserves the known fields and skips unknown
tags; Java retains their payloads. Live calls allocate successive 1,000-ID
blocks and reject unknown brokers or stale epochs. The ordinary broker listener
does not advertise this API, and Rust Admin returns Unsupported there.

These checks cover the raw extension. They do not qualify allocator exhaustion,
controller failover, permissions or the experimental partitionline broker.
The broader upstream case reconciliation remains open under KL01-14.

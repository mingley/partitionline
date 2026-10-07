# zstd encoder evidence

KL05-04 covers optional encoding with explicit levels 1 through 19, bounded
record sections, complete encoded-batch limits and reservation cleanup.
Independent Java checks actual Rust frames; a fresh native Kafka run uses Java
and librdkafka readback. Source snapshots and process receipts identify the
candidate tested on the latest stable Rust. The complete live codec matrix
remains a separate open card.

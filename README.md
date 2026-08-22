# partitionline

Pure-Rust Kafka client. No C. Drop-in protocol coverage.

Produce rec/s 105% of librdkafka 2.15.0 C on the locked 60s+180s×3 window;
p50 2.9% worse; fetch/e2e unmeasured; not done.

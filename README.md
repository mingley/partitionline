# partitionline

Pure-Rust Kafka client. No C. Drop-in protocol coverage.

Produce rec/s 109% of librdkafka 2.15.0 C on the locked 60s+180s×3 window;
p50 1.2% better. C rec/s this window 922k vs 938k prior. Fetch/e2e
unmeasured; not done.

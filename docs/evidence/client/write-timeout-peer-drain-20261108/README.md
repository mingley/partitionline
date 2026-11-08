# Write-timeout peer drain

The network suite failed `write_timeout_shuts_the_broker_down` on both the
baseline and the coordinator-retirement candidate. The test sends an 8 MiB
payload to a peer that does not read until the 200 ms write timeout returns,
then requires the peer to finish reading within 300 ms.

A [same-host diagnostic](https://github.com/mingley/partitionline/actions/runs/42445641892)
kept those deadlines and recorded when the peer started reading and reached
EOF. With an 8-byte read buffer, the baseline saw EOF after 331.5 ms and the
candidate after 309.4 ms. Each read 4,270,080 bytes in 533,760 reads, so both
original assertions failed. The peer started reading within 59 µs of the gate.

With an 8 KiB peer buffer, the same diagnostic read the same number of bytes in
545 reads. EOF arrived after 1.13 ms on the baseline and 0.97 ms on the
candidate, and the full network suite passed in both cases. The diagnostic
added counters and used one shared x86-64 runner; its timings are observations,
not a general guarantee about socket shutdown.

The fixture now uses an 8 KiB peer buffer while preserving the write timeout,
the peer deadline, and the EOF-or-error check. This removes the 8-byte drain
cost from a test of whether the peer can finish reading.

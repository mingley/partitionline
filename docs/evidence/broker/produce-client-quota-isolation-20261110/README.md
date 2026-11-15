# Kafka Produce client-ID quota observation

This is a small reference-broker observation, not a client or broker
performance comparison. It extends the earlier [single-connection timing
probe](../produce-quota-live-idle-20261109/README.md).

[Run 42865300873](https://github.com/mingley/partitionline/actions/runs/42865300873) used
Apache Kafka 4.3.1 on one hosted x86_64 Linux runner, with image digest
`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
It configured equal 20-byte-per-second Produce quotas for two client IDs.
For each Produce version, it sent sequential acks=1 requests to one
partition on plaintext loopback connections. The second client used two
connections with the same client ID. The probe checked successful Produce
responses and increasing offsets, but did not independently read back records.

| Version | Alpha throttle | First Bravo throttle | Second Bravo connection throttle | Alpha subsequent Metadata RTT |
| --- | ---: | ---: | ---: | ---: |
| 3 | 4,692 ms | 0 ms | 4,698 ms | 4,694.388 ms |
| 6 | 4,699 ms | 0 ms | 4,698 ms | 4,700.248 ms |

In these two observations, Bravo did not inherit Alpha's quota charge and
Bravo's second connection saw the same client-ID quota. The samples do not
establish a timing bound, TLS or SASL behavior, every quota configuration,
or parity with the unpublished token-bucket prototype. Client IDs alone are
caller-supplied labels and are not authenticated tenant identities.

The exact [probe](live-probe.py.txt), [workflow](workflow.yml.txt) and
[raw samples](samples.jsonl) are retained. The source commit for the run was
[`8b333fe`](https://github.com/mingley/partitionline/commit/8b333fe19ddebbffea05ddce2fb8e3892f281e86).

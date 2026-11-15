# Balanced Registry JSON object experiments

9 November 2026

The Registry JSON parser checks each new object key against a vector of
earlier names. Both experiments below preserve the input order and reject a
duplicate after parsing its value. They improve scaling for large objects,
but materially slow the tested ordinary objects. Neither is applied.

The AVL prototype stores child indices and a height beside each ordered
entry. It uses the original strings for comparisons and adds no key copy or
separate hash table. Its expected logarithmic lookup depends on its custom
balancing implementation and invariants. On this x86_64 build each
`JsonEntry` occupied 72 bytes versus 56 bytes for the original
`(String, JsonValue)` tuple, excluding vector capacity and string/value
allocations. The 32-bit slot sentinel is above the number of keys that can
fit in the configured 64 MiB response limit.

The standard-container prototype instead owns entries in a `BTreeMap`
while parsing and restores response order by insertion ordinal when the
object closes. It avoids custom balancing and key copies, but creates tree
nodes and temporary order-restoration vectors. Their heap or process-memory
cost was not measured.

## Controls

Both experiments started from main
`f1be3052aac5ec70a424ac951fe1189c27a31417`, whose Registry source blob
is `d97c106775976bac06808bdcb113488ccfe71148`. The AVL
[qualification](https://github.com/mingley/partitionline/actions/runs/42687575204)
passed rotation, ordering, duplicate and nested/malformed-value precedence
controls, tree invariants through 8,192 sorted, reverse and shuffled keys,
and a public large-response control. Full default, all-feature and
no-default-feature schema tests, format, strict Clippy and warning-denied
rustdoc passed. Its tested formatted Registry blob is
`560c2b7ecf524844431c9bbe9e2558263eafd150`. The first
[qualification attempt](https://github.com/mingley/partitionline/actions/runs/42687529255)
failed on a shallow-checkout `HEAD^` harness assertion before tests ran.

The BTreeMap arm's focused ordering, duplicate, nested/malformed-value
precedence and public large-response controls passed in the comparison
below. It did not run the full schema profiles or strict checks. Neither
test suite is a formal proof; both use mock or offline data and no live
Schema Registry.

## Paired parsing measurements

The [two-arm run](https://github.com/mingley/partitionline/actions/runs/42687798627)
used Rust 1.99.1, `--no-default-features --features registry`, separate
release target directories and 12 alternating pairs pinned to one x86_64
CPU. Prepared successful JSON bodies were parsed in-process. Schema-by-ID
was +12.32% (range +1.87% to +16.42%) and subject/version +18.37%
(+12.37% to +22.37%) for AVL versus baseline. The
[312 raw samples](avl-pairs.csv) retain all timings.

The [three-arm run](https://github.com/mingley/partitionline/actions/runs/42688186194)
used the same feature flags, separate target directories, 12 pairs and all
six rotated arm orders on another x86_64 runner. It is not a live HTTP or
Kafka benchmark. The [468 raw samples](three-arm-pairs.csv) retain each
timing. Percentages below are paired median changes versus baseline; ranges
are the smallest and largest paired changes.

| Shape | AVL | Range | BTreeMap | Range |
| --- | ---: | ---: | ---: | ---: |
| Schema by ID | +8.48% | +4.16% to +30.25% | +51.98% | +44.03% to +69.64% |
| Subject/version | +17.74% | +12.84% to +23.63% | +41.44% | +36.78% to +74.49% |
| Five keys | +22.56% | +17.97% to +25.77% | +50.35% | +41.18% to +56.30% |
| 16 keys | +31.66% | +27.11% to +36.33% | +34.68% | +28.18% to +91.16% |
| 32 keys | +26.17% | +23.44% to +31.84% | +35.80% | +29.56% to +50.25% |
| 64 keys | +12.55% | +4.34% to +16.36% | +6.60% | −3.28% to +22.98% |
| 128 keys | −30.65% | −35.36% to −25.10% | −20.68% | −30.22% to +20.19% |
| 256 keys | −56.70% | −59.95% to −50.64% | −53.00% | −57.00% to −43.78% |
| 512 keys | −71.54% | −74.17% to −70.27% | −72.47% | −74.99% to −71.15% |
| 4,097 keys | −95.23% | −96.71% to −95.07% | −93.95% | −95.86% to −93.01% |
| 8,193 ascending | −97.63% | −97.87% to −97.46% | −97.20% | −97.57% to −97.07% |
| 8,193 descending | −97.89% | −98.15% to −97.84% | −98.36% | −98.60% to −98.23% |
| 8,193 shuffled | −96.52% | −96.89% to −96.23% | −97.01% | −97.39% to −96.90% |

The two named ordinary cases use small explicit mock Registry response
bodies, including a nested reference. Other objects contain a schema field
and generated unique names. Large-object gains do not establish Kafka
performance or a production-readiness claim. Parsing remains synchronous;
neither approach establishes a wall-time or process RSS bound.

## Reproduction pins

Benchmark arms used identical test-only workloads with source feature flags
`--no-default-features --features registry`. The manifest and lockfile Git
blobs were `077a5f729d4e2c2accc2a138f872dd87e8f20a0b` and
`4cc5fc2a7502df2d4cb1a7785a63fe97650b5fa8`. Source columns below are
qualification Git blobs; executable values are SHA-256.

| Arm | Source blob | Executable SHA-256 |
| --- | --- | --- |
| Baseline | `0824e39cc48e2943f1cc4d8f9d513b3880a43df9` | `e3134d267e1728eb6da625dab6127604676990a57ee92646117263fd721bfc01` |
| AVL | `7040f88cad2b823a7d5bcdcb2bb6db1463609f05` | `3fa455e68b181eca7055e0993032b2cebbcb7c80bad53f3c4dd61a2e41c24787` |
| BTreeMap | `3a192e4290aa10d43515387dc76910026a2db3d3` | `720348cf369c773a58730f87ac5e4d610399f3ef9e39a199c73a5e12dda0f0d8` |

The AVL benchmark source matches the unformatted candidate production
prefix byte-for-byte before `#[cfg(test)] mod tests`; the qualification
applied rustfmt before testing. The BTreeMap focused control source is
`036df3350dc76a9b6ece8a07d3bf637c73b5c2ef`. Benchmark scaffolding is
test-only.

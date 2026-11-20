# Registry JSON resource measurements

Both trials are held for regressions. On hosted Linux x86_64,
the current parser at `e7c6e3c` had median parse times of
0.95 ms for the 1 MiB documentation response, 7.99–8.04 ms for
8 MiB, and 111.01–114.60 ms for 64 MiB. The 64 MiB baseline's
process peak RSS medians were 195.67–195.71 MiB. These observations
cover the synchronous parser and the compiled test process.

- Copying string spans and moving fields improved the 64 MiB case.
  The 8,192-field Avro case took 27.14% longer and the 8 MiB peak RSS
  rose 28.96%. [Combined trial](combined/README.md)
- Moving fields with the original scanner improved the 64 MiB case.
  The 1 MiB and 8 MiB cases took 23.33% and 22.13% longer.
  [Owned-field trial](owned-fields/README.md)

Each trial ran 168 matched pairs with the same 14 valid fixtures.
Both arms verified returned bytes, fields, ordered references and
cache-charged string capacities. The raw timings, memory results,
and source and build identities are in each trial directory.

## Setup failures

| Run | Stage |
| --- | --- |
| [Initial qualification](https://github.com/mingley/partitionline/actions/runs/44646908113) | Rustfmt requested call wrapping, and Cargo rejected the nested baseline checkout. |
| [Initial live check](https://github.com/mingley/partitionline/actions/runs/44646908106) | Rustfmt requested the same wraps. |
| [Capacity check update](https://github.com/mingley/partitionline/actions/runs/44647137054) | The live check still needed those wraps. |
| [Formatted parser](https://github.com/mingley/partitionline/actions/runs/44647266529) | Correctness and packaging passed. Cargo still rejected the nested checkout. |

The final paired workflows compiled both crate copies in separate
temporary directories. The baseline manifests and Rust source were
copied byte for byte before adding the same measurement module to
both builds.
[Combined workflow](https://github.com/mingley/partitionline/actions/runs/44647583240) ·
[Owned-field workflow](https://github.com/mingley/partitionline/actions/runs/44648404945)

# Benchmark report JSON validation

The standalone reporter previously accepted duplicate JSON object keys,
including escaped keys that decode to the same name. Python's parser kept the
last value, so conflicting settings could be hidden. The human formatter also
assumed validation had received correctly typed nested fields and could raise
a traceback while displaying an invalid result. These controls use authored
fixtures and exercise the reporter, not Kafka or benchmark performance.

The candidate rejects duplicate object keys when loading result and selected
schema JSON. It reports malformed structures without formatting their
untrusted values. Unexpected validation or formatting exceptions fail closed;
this does not prove every possible input has been exercised.

A supplied `--schema` still replaces the repository schema and can be used by
a relocated standalone reporter. Alternate-schema human and JSON output names
the selected path and states that the canonical repository schema was not
checked. A permissive alternate is not canonical qualification. The reporter
checks the selected local schema but does not authenticate its contents or raw
benchmark artifacts. `BenchmarkValidator.validate` receives an already
parsed dictionary and cannot detect duplicate keys discarded by another parser.

## Controls

- [42879754242](https://github.com/mingley/partitionline/actions/runs/42879754242)
  at `ae7b2ca0d1aab68c28c3981f1d50c46c00aff2b2` ran 37 tests with 11
  failures on the original source: duplicate keys and malformed-output cases
  exposed the baseline defects.
- [42880012842](https://github.com/mingley/partitionline/actions/runs/42880012842)
  at `5f215804e0be3c9414961caeaa5fd3097b971824` passed the initial 37
  controls, but did not cover alternate-schema structural assumptions.
- [42880526480](https://github.com/mingley/partitionline/actions/runs/42880526480)
  at `d87b303e3e746a6162ffd896062cbfecae3283f9` ran 40 tests with 21
  errors, including attribute and comparison errors under permissive,
  `anyOf` and `oneOf` schemas.
- [42880679020](https://github.com/mingley/partitionline/actions/runs/42880679020)
  at `620d65723ed420a4ed914c057516c2464aab11e0` passed 40 controls,
  but the candidate wrongly restricted string-valued record ID bounds and
  did not identify alternate-schema output clearly.
- [42881291247](https://github.com/mingley/partitionline/actions/runs/42881291247)
  at `e047eeeaaccac0695c7530c8f7d221b38d73eb65` ran 46 tests with
  three failures and 126 errors. Most error subtests invoked the proposed
  formatter's new keyword arguments before that API existed; they are not
  126 independently established production crashes.
- [42881431378](https://github.com/mingley/partitionline/actions/runs/42881431378)
  at `d8730ea60a0554d111a0cfb70116c9180e3797a4` passed all 46 reporter
  tests on Python 3.13.18. They cover duplicate decoded keys, malformed
  nested objects, permissive and composite alternate schemas, missing fields,
  string ID bounds, relocated explicit-schema use, alternate output scope,
  and injected validation and formatting exceptions.

The tested source, test and contract-document Git blobs are respectively
`e35b471a5ceb9075b0ba9a1e0e1f36e6f1d8d432`,
`403ab5ec24bc6e1f6f9bbbdd7c00d9535f7ff520` and
`bc21a4c7b9ce552a8099bef267e33e15a68f7cb9`.
The matrix workflow added to the handoff runs the existing matrix tests as a
separate check; its result must be recorded separately when complete.

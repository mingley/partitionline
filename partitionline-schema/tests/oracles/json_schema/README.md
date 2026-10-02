# Independent JSON Schema profile oracle

The peer is **python-jsonschema 4.26.0**, MIT, on Python **3.12.14**.
`peer-lock.json` pins the validator and every installed dependency by exact
wheel version/URL/SHA-256; `requirements.txt` supports hash-verified offline
installation. `generate.py` checks wheel hashes, installed package versions and
every installed Python/shared-library/schema-JSON source against its wheel.
The wheels and virtual environment live in scratch, not in the crate or repo.
The retained `rpds-py` reference wheel is CPython 3.12 / Linux x86_64; another
platform needs its own reviewed oracle pin, not a silent wheel substitution.
No oracle package enters the Rust production dependency graph.

Profile `partitionline.json-schema.draft2020-12.v1` uses Draft 2020-12,
explicit offline references and format/default annotations. The independent
engine is unmodified. The JSON parser uses arbitrary-precision Python integers
and `Decimal` for fractional tokens; mathematically integral decimal/exponent
tokens become integers before validation. This avoids f64 rounding at int64
bounds and implements JSON Schema's mathematical integer semantics for `1.0`.
NaN and infinities are rejected by the strict JSON parser. `Registry` permits
only the supplied resource; its retrieval callback raises `NoSuchResource`
instead of performing I/O. A missing-reference evaluation is asserted to fail.

The generator emits 27 instance cases: 10 valid and 17 invalid. It checks the
writer, an annotation-evolved reader (`default` does not rewrite data), and an
incompatible nonnegative reader. Cases cover null, integer minima/maxima,
numbers beyond f64's exact range, exact boundary decimal tokens, exponents,
fractional/out-of-range numbers, NaN/Infinity, wrong JSON types, leading zeros,
empty/whitespace input and trailing documents. Boolean schemas are meta-checked
and the Rust adapter tests explicitly exercise acceptance/rejection callbacks.

Run from the repository root after installing the exact pinned wheels into a
scratch virtual environment:

```sh
python3 -m venv ../work/json-schema/venv
../work/json-schema/venv/bin/python -m pip download \
  --require-hashes -r partitionline-schema/tests/oracles/json_schema/requirements.txt \
  --dest ../work/json-schema/wheels
../work/json-schema/venv/bin/python -m pip install --no-index \
  --find-links ../work/json-schema/wheels --require-hashes \
  -r partitionline-schema/tests/oracles/json_schema/requirements.txt
../work/json-schema/venv/bin/python partitionline-schema/tests/oracles/json_schema/generate.py \
  --wheel-dir ../work/json-schema/wheels \
  --output partitionline-schema/tests/fixtures/json_schema --verify
JSON_SCHEMA_RUST_OUTPUT="$PWD/../work/json-schema/rust-output" \
  cargo test --locked --manifest-path partitionline-schema/Cargo.toml \
  --no-default-features --features json-schema --test json_schema
../work/json-schema/venv/bin/python partitionline-schema/tests/oracles/json_schema/generate.py \
  --wheel-dir ../work/json-schema/wheels \
  --output partitionline-schema/tests/fixtures/json_schema --verify \
  --rust-output ../work/json-schema/rust-output
```

Generation without `--verify` intentionally writes fixtures; use it only when
reviewing a changed profile/oracle. Verification is byte-exact and checks actual
Rust-generated frames for all ten valid cases with the independent validator.
The Rust fixture codec implements only these scalar schemas, with its own
integer arithmetic; it is not a general-purpose JSON Schema engine. These
offline tests do not qualify a live registry, Kafka broker or complete
ecosystem profile. No schema publication or mutation is performed.

# Selected JSON Schema validator (KL05-44)

The tests run jsonschema 0.33.0 through `json_schema::Adapter` and compare actual Rust frames and decoded values with Python jsonschema 4.26.0. All 18 independent cases pass in default, all-feature and offline companion profiles. A numeric-to-string corruption fails the independent check.

The family uses Draft 2020-12, null or signed 32-bit integers, nullable record fields and explicit URI references. Missing references, cycles, other dialects and unsupported schema/numeric work fail. Writer and reader validation both run. Defaults do not insert fields, and strings are not converted to numbers.

The test bridge limits complete frames to 4 KiB and combined schema text to 16 KiB with eight references. Value/schema traversal has depth, node and branch limits; `summary.json` lists them. These are input and graph bounds, not process RSS or allocator quotas. Rust HTTP/file features are disabled and custom Rust/Python resource retrievers deny all implicit retrieval. This is test-only validation of one finite family; applications choose their codec, and the companion remains unpublished.

Reproduce on latest stable:

```sh
python3 -m pip install 'jsonschema==4.26.0' 'referencing==0.37.0' 'rpds-py==2026.6.3' 'attrs==26.1.0' 'jsonschema-specifications==2025.9.1'
PL_JSON_SCHEMA_SELECTED_OUTPUT=work/json-schema-output cargo test --locked --manifest-path partitionline-schema/Cargo.toml --all-features
python3 tests/conformance/run-json-schema-selected-codec.py --rust-output work/json-schema-output --report work/json-schema-peer.json
cargo clippy --locked --manifest-path partitionline-schema/Cargo.toml --all-targets --all-features -- -D warnings
```

Use a fresh output/report path. The verifier checks source and fixture hashes and exact peer package versions. `validation/` retains actual Rust output, independent checks and the rejected corruption; `source/` pins the candidate.

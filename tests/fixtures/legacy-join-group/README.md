# Legacy JoinGroup v0/v1 fixtures

Pinned Apache kafka-clients builders and parsers for JoinGroup v0 and v1.
All five pins (3.9.1, 4.1.0, 4.1.2, 4.2.1, 4.3.1) report
`ApiKeys.JOIN_GROUP` 0-9 and emit identical leftover-empty bodies.
A present `group.instance.id` is `UnsupportedVersionException` on both
versions (ten retained receipts).

`tests/common` Mock public join runs are local and are not this oracle.
Do not infer a successful old-group lifecycle from a field roundtrip.

Regenerate with:

```
python3 tests/fixtures/legacy-join-group/generate_legacy_join_group.py \
  --fetch --output /tmp/legacy-join-group-gen \
  --install tests/fixtures/legacy-join-group
```

# Streams v0 codecs

The existing API88 heartbeat and API89 description codecs preserve topology,
tasks, offsets, endpoints, member fields, signed defaults and null/empty values.
The caller supplies positive limits for wire bytes, strings, arrays, tags and
decoded vector/string reservations. Encoding validates before output growth.

All24 codec tests passed on latest-stable Rust in default and all-feature
builds. Three pinned Apache SDKs compiled both Java oracles, generated every
fixture twice and reproduced the installed fixture trees exactly. Each SDK
then parsed26 Rust bodies and12 header/body messages in each feature build,
checking full field equality, complete consumption and canonical bytes.
All external processes were waited; input and source hashes stayed unchanged.

The current production codec and existing tests match live origin/main. The
new runner and header oracle remain local changes. Strict Rust checks passed;
the immediately preceding full suites are reused with identical production
source/Cargo inputs (2,011 default and2,023 all-feature passes).

This qualifies codecs. Streams client operations, coordinator/server behavior
and framework execution are separate work. Parser leniency from actual Apache
is recorded separately from Rust admission policy. No performance rank is
claimed. `summary.json` describes the checks; `validation/` retains sources,
commands, class/binary hashes and outputs. Historical failure directories are
unchanged. `SHA256SUMS` covers this final snapshot.

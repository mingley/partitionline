# AssignReplicasToDirs checks

AssignReplicasToDirs v0 (API73) is a controller-listener API. There is no standard
Java Admin method. These checks use Apache's generated request builders,
serializers and parsers, plus Rust and Java raw connections to disposable Kafka
4.1.2, 4.2.1 and 4.3.1 controllers.

The declared cohort covers nested field identities, defaults, signed limits,
top-level and partition errors, truncated bodies and unknown tags at every
nesting level. Both SDK and Rust accept 2,251 assignments: Apache's 2,250 constant
is a packet-size recommendation, not an enforced builder limit. Rust skips
unknown tags; an independent Java parse checks every normalized known field.
Unrecognized numeric error codes stay raw integers in Rust, while Java maps
them to its unknown-error enum. Wire preservation does not imply identical
typed enum behavior for those values.

Default and all-feature builds each ran the cohort against all three versions:

| Check | Total across both builds |
| --- | ---: |
| Independent parses of Rust-generated bodies | 138 |
| Raw controller calls | 108 |
| Independent parses of live request/response bodies | 216 |
| Corrupted-body subprocess controls rejected | 9 |
| Disposable broker topologies | 6 |

Live calls checked valid empty requests, stale and unknown broker epochs,
unknown topics and partitions, empty nesting, the owned topic's online directory
assignment and a duplicate partition assignment. Rust's ordinary broker Admin
connection returned Unsupported because that listener does not advertise API73.
Every owned topic was deleted. Every broker was waited after shutdown, its
process group was empty and both listener ports were reusable.

Two ordinary tests passed in each feature build. Both conditional SDK and native
test lanes then executed against every selected peer; their ordinary ignored
status is not counted as a pass. Strict Clippy and formatting passed. The
[summary](qualification/summary.json) records the counts, sources and limits.
The initial format attempt used a metadata segment smaller than Kafka permits;
its nonzero receipt remains in the archive. The corrected fixture uses the
actual eight-MiB minimum.

Rust binaries were compiled from commit
`6575e97792a710dba35426cb989abaed57b84202` with latest-stable Rust 1.99.0. The
executed controller runner is at
`85e96d48c5bfac2ac2a9b5fdc3069d896d269441`; only its Python fixture and class
guards changed between these commits. Rust source, manifests and the test body
are byte-identical. Source, SDK jars, native distribution files, test executables
and compiled Java classes were checked by hash. The native inputs also matched
their pinned distribution archives.

This qualifies the declared current raw-extension cohort. It does not qualify
offline-directory movement, an unassigned replica, controller failover,
authorization, a replica directory store in the experimental Rust broker, or
exhaustive upstream applicability under KL01-14. Rust's post-read fixture-size
assertion is not a hostile-frame memory qualification. Production and full
conformance gates remain open.

[The applicability record](applicability.json) declares the cases and limits.
[The archive inventory](qualification/archive-inventory.json) maps original
paths and hashes to the published files. Large files and executables are gzip
compressed; original bytes are recoverable with `gzip -dc`. Absolute paths in
receipts retain the original workspace provenance. Upstream distribution and
SDK archives are external pinned inputs. Verify this dossier from the repository
root with:

```bash
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/conformance/q-conf-assignreplicastodirsraw/qualification
```

`SHA256SUMS` covers both the declared sources and the qualification archive.

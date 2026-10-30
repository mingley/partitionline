# Required Java capability fixtures

At `fe65ac783`, the capability lane selects 93 required cases but leaves
two Java-fixture cases ignored. The original report remains failed. Replaying
its retained default-lane output with the corrected parser finds 91 raw passes
and two ignored required tests; this replay executes no Rust or Java.

The gate now generates fresh fixtures with pinned Kafka 4.1.2, 4.2.1 and
4.3.1 Java clients. It runs both required fixture tests with `--include-ignored`
and skips only the two separately named SDK servers. Discovery still requires
all 95 names, and every one of the 93 selected cases must pass. The original
four public controls and 234-file Streams export gate remain required.

The Java helper checks jar and generator hashes, generated case identities,
API versions, supported/unsupported cases and null-field behavior. After Rust
exports its frames, Java must parse them back and report exactly 450 checks.
Generated input hashes must remain unchanged through the run. Java 21 is
installed by the workflow. Download, setup, generation or reverse-check failure
keeps the lane failed.

All 31 Python controls pass: 14 selection/report controls and 17 fixture
metadata/command controls. An independent static review found no blocker.
Those controls use synthetic bytes and run no Java, Cargo or downloads;
actual fixture qualification awaits CI on the new commit. They establish
neither independent SDK socket behavior nor Streams Java reverse parsing.

`receipt.json` pins the reviewed files and pure-control logs. The original
default-lane validation report and its four native output streams are retained
unchanged. Its artifact ZIP is GitHub artifact 13011652138, SHA-256
`fe09a2166b74d864085aefe5b92b74d2e245641cbb738dfb62dddf6a21b15d60`.
Only the manifest, validation report and four relevant streams are copied here.

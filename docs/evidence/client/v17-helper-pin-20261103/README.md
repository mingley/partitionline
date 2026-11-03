# V17 client helper pin trust boundary

The v17 qualification workflow checks the SHA-256 and size of its helper guard,
then has that guard read six helper hashes from the v17 import manifest. The
workflow supplies the manifest path without independently pinning the manifest
bytes. At commit `9b796378395982405baa84188dba463d218506f9`, the guard accepts
both a helper and its manifest entry changed together. It rejects the same
helper mutation while the original manifest is retained.

Run `python3 -B reproduce.py` to repeat the source-only check. It uses the real
v17 guard in a temporary copy of the six committed helper files. It changes one
byte in the client driver's docstring without changing file size, first runs the
guard against the original manifest, then updates the relevant manifest hash
and runs it again. The modified driver is never executed. `result.json` records
the three outcomes. The guard script, manifest, other helper files, and workflow
are retained under `source/`; `source-pins.json` binds them to the commit.
`build-evidence.py` records how those committed files were extracted and
compared with the checkout. `SHA256SUMS` binds this complete directory.

A trusted, immutable checkout of a reviewed commit is a separate provenance
boundary and fixes both files. This result does not show an actual compromise,
a malformed commit, or any hosted failure; it shows that the guard alone does
not independently attest the manifest or reject a paired edit. No Cargo,
Rust compiler, native tests, GitHub workflow, or network were run. This is
historical v17 evidence; a later candidate needs its own workflow controls.

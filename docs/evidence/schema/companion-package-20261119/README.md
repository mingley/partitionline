# Schema companion crate archive, 19 November 2026

Cargo packaged `partitionline-schema 0.0.0` from `c9745f4ef3cb1da88982dc98080a0501abfd01b0`
on Linux x86_64 with Rust 1.100.0. The archive had 229 files and was
139.0 KiB compressed. The license texts match the repository copies.
The README passed its local link check, and the three packaged fixture
trees matched the original independent peer fixtures byte for byte.
[Archive qualification](https://github.com/mingley/partitionline/actions/runs/44640185362)

| Extracted crate profile | Tests and doctests passed | Ignored live tests |
| --- | ---: | ---: |
| Default | 112 | 2 |
| No default features | 20 | 0 |
| All features | 153 | 2 |

The prior archive at `7ad99cc` had no license or notice files. The first
candidate had those files but failed extracted tests whose fixtures
lived above the crate. A separate workflow parse failed when an edit
truncated the CI workflow. The final candidate includes the missing
fixtures and the complete workflow.
[Prior inventory and first extracted test failure](https://github.com/mingley/partitionline/actions/runs/44639543218) ·
[Workflow parse](https://github.com/mingley/partitionline/actions/runs/44639541106)

Run `bash scripts/ci-schema-package.sh` from the repository root.
The job retains the archive, its digest, file list, license and links
report, and all three logs. The live Registry and Kafka fixture runs
separately. The crate is unpublished.
[Run metadata](verification.json) · [Live CI](https://github.com/mingley/partitionline/actions/runs/44640038645)

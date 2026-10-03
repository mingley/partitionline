This WORK-only derivative tightens only the disk audit forecast. It does not materialize sources, change Git, clean caches or run Cargo/SDKs.

Original c9a31c31 driver remains unchanged. Candidate88ec4297 preserves the exact materialization/link/new-blob/byte/Git/full07777/pathset/WORK-inode/baseline-before-after/raw+gzip retention operations. The only nonforecast change asserts final encoded JSON length equals its exact prediction rather than merely staying below an upper estimate.

The metadata skeleton has every exact Git path/mode/blob/byte-length and final owned permission bits; its SHA256 placeholder is exactly64 ASCII hex characters. The final sorted compact JSON serializer therefore has exactly the same byte length for all actual SHA256 values, including path escaping and integer widths. Controls compare an independent punctuation/digit/JSON-path calculation and alternate64hex hashes against all73938 current metadata rows and1322 adversarial JSON cases. Serializer-only surrogate controls do not broaden the unchanged Git UTF8/path policy. Supplemental real baseline proof matches all73818 retained actual SHA256 rows byte-for-byte at23344752B.

For admitted n<=128MiB, stored-block upper is n+5*ceil(n/16383)+64. It dominates the unchanged default-zlib gzip bound n+(n>>12)+(n>>14)+(n>>25)+25: the block overhead is at least floor(n/4096)+floor(n/16384), and n>>25<=4. Boundary and actual gzip controls supplement this mathematical bound. gzip.compress defaults and actual length assertion are unchanged; raw and gzip are both retained.

One rounded raw audit allocation +one rounded rigorous gzip upper +unchanged1MiB receipt/minimalmetadata reserve replaces the fictitious second raw audit and loose per-path estimate. The conservative >=4KiB per-entry/directory reserve and350MiB floor remain unchanged. No additional filesystem allocation assumptions are introduced.

04f6 exact forecast: newallocated9052160B +entry/directory345075712B +raw23384064B +gzip23388160B +metadata1048576B =401948672B. Required including unchanged floor367001600B is768950272B; initial sampled731660288B correctly refuses. Original forecast required969126932B. Resource availability can change and the actual driver will recheck before every write. The proposed launch is not a permission/qualification receipt.

The first supplemental stdin control had a syntax-only missing-space error before execution; its diagnostic remains. Corrected supplemental control passed and did not mutate any source. Full materialization remains coordinator-owned after its actual GO.

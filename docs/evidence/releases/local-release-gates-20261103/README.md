# Local release gates

The release scripts now distinguish a registry identity from a successful
external consumer. An index entry can prevent a duplicate upload, but reporting
confirmation requires matching unyanked API and index entries. Registry and
Git consumer checks bind the requested package to its own lockfile source and
version. Inconclusive registry and CI results fail closed on publication paths.

The token check now reports only local presence. Authorization and scope are
unverified until an actual upload. The obsolete first-publish dispatcher is
retired. Release paths request live exact-SHA CI with the required profiles
and do not inherit fixture overrides.

The frozen patch passed 100 offline release tests twice, including an
independent run with real Cargo, GitHub and registry commands guarded. The
preceding patch passed 100 tests for both 0.1.0 and 0.2.0 manifests; the final
status-specific control also passed for both. The retained negative cases
show the earlier CI exit-code bypass, simulated green fixture, adjacent
lockfile source, stale version, misleading token response and retired
dispatcher override. No real publish or live credential check occurred.

The [manifest](manifest.json), [validation](validation.json) and
[independent review](independent-review.json) bind the exact source patch and
offline results. Live GitHub job-response shape and attempt binding remain
unverified. API/index identity alone does not prove a crate can be downloaded
and compiled. Current client and broker failures and the remaining release
gates still block a release.

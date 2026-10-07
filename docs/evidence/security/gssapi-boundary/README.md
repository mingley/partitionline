# Kerberos provider review

This is a source review and a proposal awaiting maintainer approval for KL06-08.
No provider was installed, compiled, executed, or qualified against a KDC.
The [proposal](../../../gssapi-boundary.md) names the backend, optional dependency
graph, process ownership, credential lifecycle, mechanism exchange, and remaining
qualification work. Client and broker defaults are unchanged.

Six registry archives are verified against the recorded sparse-index checksums.
They cover five candidates plus the proposed native binding. The member hashes,
manifests, and selected source excerpts support the comparison. Installed Debian
package/library observations describe this host only. Complete transitive/system
license review and builds remain pending. Public metadata API requests returned
403; the standard sparse registry and static archive requests succeeded.

The archive and source records are review inputs. Upstream version claims and
examples are not this project's test results. Enterprise completeness remains
blocked while KL06-08, KL06-09, KL06-10, or KL11-38 is open.

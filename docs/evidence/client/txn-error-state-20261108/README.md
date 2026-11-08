# Transaction error-state candidate

This isolated candidate starts from main commit 8b5de2e2cfe5f3164a5f89546804ab36c1771ade. It does not include the AddPartitions response-coverage patch.

A single atomic severity summarizes the transactional error state. The existing mutex owns the cause and serializes transitions; a fatal cause replaces an earlier abortable cause and the first fatal cause stays terminal. Healthy send checks still use one atomic load. Test-only hooks coordinate exact interleavings; the workflow first requires those controls to expose the old behavior, then runs them against the candidate with default and all features.

This addresses coherent error publication and clearing only. It does not establish transaction-generation ordering, close the response-validation/coordinator-lock window, qualify AddPartitions, or establish production readiness or performance.

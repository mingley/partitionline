# Transaction error-state candidate on coordinator replacement

This isolated candidate starts from main commit b631809f0e03ba6f65f808e51d8e47e3fd9380d1, which includes the coordinator replacement. It does not include the AddPartitions response-coverage patch.

A single atomic severity summarizes the transactional error state. The mutex owns the cause and serializes transitions; a fatal cause replaces an earlier abortable cause and the first fatal cause stays terminal. Healthy send checks use one atomic load. Test-only hooks coordinate precise interleavings; qualification first requires the controls to expose the prior behavior, then tests the candidate in both feature profiles and repeats the two-lock and coordinator-replacement controls.

This addresses coherent error publication and clearing only. Transaction-generation ordering, AddPartitions response-validation ordering, and broader production and performance qualification remain open.

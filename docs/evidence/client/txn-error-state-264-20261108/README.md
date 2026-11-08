# Transaction error-state candidate on Clippy-corrected main

This isolated candidate starts from main commit 264322ff430b0594f4c3864e53dcd832de2a930b. It does not include the AddPartitions response-coverage patch.

One atomic severity summarizes transactional error state. The mutex owns the cause and serializes transitions; fatal replaces an earlier abortable cause and the first fatal cause remains terminal. Healthy send checks use one atomic load. Test-only hooks coordinate precise interleavings. Qualification checks the old behavior using the same controls, then tests both feature profiles, two-lock and coordinator-replacement controls, stable formatting, and all-target all-feature Clippy.

Transaction-generation ordering, AddPartitions response validation and retry timeout behavior, and broader production and performance qualification remain open.

# Retry timeout and cancellation probe

This isolated experiment starts from main commit 264322ff430b0594f4c3864e53dcd832de2a930b and applies the frozen AddPartitions coverage patch only in a disposable checkout. A narrow candidate remembers a validated retriable response and records an abortable disposition before cooperative cleanup if a later request-budget timeout occurs. It preserves Timeout as the worker and per-record result.

The controls compare natural and forced deadline crossing, preserve an already fatal state, and cancel the worker after the disposition is recorded but before partition cleanup. The cancellation test requires the accepted synthetic records to complete and their reserved bytes to be released. A failure is reported as a failure; no ownership or generation guarantees are assumed.

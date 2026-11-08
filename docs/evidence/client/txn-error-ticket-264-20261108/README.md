# Ticketed transactional error state

This isolated candidate starts from main commit 264322ff430b0594f4c3864e53dcd832de2a930b. It does not include the AddPartitions response-coverage patch.

An atomic word packs a 61-bit abort ticket and error severity. Abortable and fatal setters claim pending severity before waiting for the cause mutex, so transaction checks fail closed while a cause is being installed. Clear invalidates the exact abort ticket; a stalled old setter cannot overwrite a later abort cause. Fatal remains terminal, its first claimant owns the cause, and exhaustion of the ticket space fails closed as fatal.

The controls cover held-lock publication and readers, old-abort/clear/new-abort ordering, fatal competing with clear and another fatal, reader rechecks and ticket exhaustion. The ticket protects error-state publication; it does not identify the transaction generation in which an operation originally started. Late responses and stale clears across transaction generations remain unresolved. Full production and performance qualification remain open.

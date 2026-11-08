# Ticketed transactional error state

This isolated candidate starts from main commit 374dcf45db20ee728b00c0b0188e77ca1f47fa9c. It does not include the AddPartitions response-coverage patch.

An atomic word packs a 61-bit abort ticket and error severity. Abortable and fatal setters claim pending severity before waiting for the cause mutex, so transaction checks fail closed while a cause is being installed. Clear invalidates the exact abort ticket; a stalled old setter cannot overwrite a later abort cause. Fatal remains terminal, its first claimant owns the cause, and exhaustion of the ticket space fails closed as fatal.

The controls cover held-lock publication and readers, old-abort/clear/new-abort ordering, fatal competing with clear and another fatal, reader rechecks and ticket exhaustion. The ticket protects error-state publication; it does not identify the transaction generation in which an operation originally started. Late responses and stale clears across transaction generations remain unresolved. Full production and performance qualification remain open.

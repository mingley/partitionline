# Transaction fatal publication candidate

This isolated candidate starts from main commit 264322ff430b0594f4c3864e53dcd832de2a930b. It does not include the AddPartitions response-coverage patch.

An atomic state distinguishes no error, abortable, fatal pending, and fatal committed. A fatal setter claims pending before waiting for the cause mutex, so readers fail closed while the cause is being installed. The first fatal winner owns the cause. Abortable publication and clear use compare-exchange while holding the cause mutex and cannot overwrite a pending fatal transition. A committed fatal replaces any earlier abortable cause.

The controls cover both error readers with the cause mutex held, simultaneous fatal setters, clear racing a fatal, abortable publication racing fatal or clear, and preservation of a fatal cause. This does not establish transaction-generation ordering or AddPartitions response/timeout behavior. Full production and performance qualification remain open.

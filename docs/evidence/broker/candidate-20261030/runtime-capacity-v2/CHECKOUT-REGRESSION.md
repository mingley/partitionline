# Checkout regression in the first candidate

This first qualification draft is retained for history. Its static patch
roundtrip passed outside a Git checkout, but failed in the actual repository:
the test created its scratch tree inside the checkout, where `git apply`
skipped the intended temporary source. The production runtime file remained
unchanged. The corrected candidate records the failure and tests its scratch
placement in a committed Git checkout.

The original source-review receipt `52bac5c8da2c632a7651191300950b8b8c6d6006ce9b6a8d57882ecfa8decbc1`
is withdrawn because it did not exercise that checkout condition. The
[replacement review](../runtime-capacity-v2-r2/independent-review.json) covers
the corrected candidate.

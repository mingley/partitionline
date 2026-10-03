The native seek correction assigns each topic once, then polls that initial
assignment before seeking within it. Later seeks use the existing consumed
assignment and retain numeric SDK error codes on failure.

The prior corrected peer stopped on seek3 after delivering the exact first nine
records and EOF. Its returned seek error was discarded, so its specific error
code remains unknown. The complete failure and the pinned SDK source rationale
are preserved in `native-seek-correction/`.

This is an uncompiled source correction. Record, EOF, watermark, and position
expectations are unchanged. `verify-live-v2.py` continues to check server and
native helper source pins separately. The original and v2 freezes remain
historical; use `verify-freeze-native-seek-v3.py` for the current additive freeze.

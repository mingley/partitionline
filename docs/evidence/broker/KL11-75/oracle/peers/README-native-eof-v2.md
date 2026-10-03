The additive `verify-live-v2.py` checks actual native EOF events separately from
the SDK's consumed position. After a seek that delivers no records, librdkafka
can retain its INVALID position; the verifier requires an actual EOF and matching
public watermarks as well as the exact ordered record history for every seek.

Server and Rust artifacts retain source `403be1e3`. The corrected native peer and
live driver use source `fcac9d1d`; those pins are checked separately. The native
compile binding preserves the original compiler receipt and checks the actual
source, executable, SDK dependency bytes, Git blob, and full permission modes.

`native-eof-v2-root/` retains the independent review history and executed controls.
Three previously executed Java reads and the closed native compiler binding pass
the new checks. Six synthetic phases and twenty rejecting synthetic controls test
the verifier; they do not constitute new native or broker runtime qualification.

The original `verify-live.py`, `verify-freeze.py`, `source-freeze.json`, and
`SHA256SUMS` remain unchanged historical artifacts. The separate
`source-freeze-native-eof-v2.json` records the updated input set and its actual
full07777 modes. Use `verify-freeze-native-eof-v2.py` to verify this additive freeze.

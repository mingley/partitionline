#!/usr/bin/env python3
"""One broker/client pair, both descendants of the caller's owned session."""
import sys
from pathlib import Path
from run import CASES, run_attempt

if len(sys.argv) != 8:
    raise SystemExit("usage: trial.py SERVER CLIENT CASE ARM INDEX timing|diagnostic OUT")
server, client, case, arm, index, kind, out = sys.argv[1:]
if case not in CASES or arm not in ("baseline", "candidate") or kind not in ("timing", "diagnostic"):
    raise SystemExit("invalid trial identity")
raise SystemExit(0 if run_attempt(Path(server), Path(client), case, arm, index, kind == "diagnostic", Path(out)) else 1)

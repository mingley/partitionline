"""Reject four deliberately broken model transitions."""
from pathlib import Path
import subprocess
import sys
import tempfile

source = Path("tests/models/producer_owned_batch_model.py").read_text(encoding="utf-8")
mutants = [
    (
        "lost-unknown-fence",
        'batch.phase = "unknown" if batch.phase in ("offered", "unknown") else "retiring"',
        'batch.phase = "unknown" if batch.phase == "offered" else "retiring"',
        "OwnerControls.test_unknown_batch_retains_individual_deadlines_and_fence",
    ),
    (
        "flush-loses-error",
        "barrier.error = earlier(barrier.error, error)",
        "barrier.error = barrier.error",
        "OwnerControls.test_callback_claim_and_flush_wait_for_terminal_publication",
    ),
    (
        "close-leaks-reservation",
        "self.discard_reservation(token)",
        "pass",
        "OwnerControls.test_limits_and_close_revoke_pre_admission",
    ),
    (
        "split-retry",
        "if partition in self.active or max_records <= 0 or max_bytes <= 0:",
        "if max_records <= 0 or max_bytes <= 0:",
        "OwnerControls.test_unknown_handoff_remains_a_whole_batch_during_flush_and_close",
    ),
]
with tempfile.TemporaryDirectory() as directory:
    for name, original, replacement, test in mutants:
        if source.count(original) != 1:
            raise RuntimeError(f"mutant anchor is not unique: {name}")
        path = Path(directory) / f"{name}.py"
        path.write_text(source.replace(original, replacement), encoding="utf-8")
        result = subprocess.run([sys.executable, str(path), test], text=True, capture_output=True)
        if result.returncode == 0 or "FAIL:" not in result.stderr or "SyntaxError" in result.stderr:
            print(result.stdout, result.stderr, file=sys.stderr)
            raise RuntimeError(f"mutant was not rejected as a test failure: {name}")
        print(f"MUTANT_REJECTED {name} test={test}", flush=True)

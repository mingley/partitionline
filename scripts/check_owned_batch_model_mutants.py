"""Reject deliberately broken ownership, fencing and completion transitions."""
from pathlib import Path
import subprocess
import sys
import tempfile

source = Path("tests/models/producer_owned_batch_model.py").read_text(encoding="utf-8")
mutants = [
    ("lost-unknown-fence", [
        ('batch.phase = "unknown" if batch.phase in ("offered", "unknown") else "retiring"',
         'batch.phase = "unknown" if batch.phase == "offered" else "retiring"'),
    ], "OwnerControls.test_unknown_batch_retains_individual_deadlines_and_fence"),
    ("flush-loses-error", [
        ("barrier.error = earlier(barrier.error, error)", "barrier.error = barrier.error"),
    ], "OwnerControls.test_callback_claim_and_flush_wait_for_terminal_publication"),
    ("close-leaks-reservation", [
        ("for token in list(self.reservations):\n            self.discard_reservation(token)",
         "for token in list(self.reservations):\n            pass"),
    ], "OwnerControls.test_limits_and_close_revoke_pre_admission"),
    ("split-retry", [
        ("if partition in self.active or max_records <= 0 or max_bytes <= 0:",
         "if max_records <= 0 or max_bytes <= 0:"),
    ], "OwnerControls.test_unknown_handoff_remains_a_whole_batch_during_flush_and_close"),
    ("terminal-lease-reused", [
        ('or batch.phase not in ("claimed", "offered", "unknown")):',
         'or batch.phase not in ("claimed", "offered", "unknown", "done")):'),
        ('batch.phase = "done"\n        batch.lease = None\n        del self.active[batch.partition]',
         'batch.phase = "done"\n        del self.active[batch.partition]'),
    ], "OwnerControls.test_completed_batch_stale_lease_cannot_clear_new_batch"),
    ("cohort-crosses-partition", [
        ('if state["partition"] is not None and state["partition"] != partition:', 'if False:'),
    ], "OwnerControls.test_sticky_interleaving_and_cross_partition_rejected_before_admission"),
    ("cohort-state-leak", [
        ('if state["live"] == 0 and (state["sealed"] or state["cancelled"]):\n                del self.cohorts[rec.cohort]',
         'if state["live"] == 0 and (state["sealed"] or state["cancelled"]):\n                pass'),
    ], "OwnerControls.test_cohort_metadata_is_reclaimed_but_sequence_state_is_bounded_separately"),
    ("sequence-state-forgotten", [
        ("rec = self.records.pop(ticket)\n        assert rec.callback", 
         "rec = self.records.pop(ticket)\n        self.next_sequence.pop(rec.partition, None)\n        assert rec.callback"),
    ], "OwnerControls.test_cohort_metadata_is_reclaimed_but_sequence_state_is_bounded_separately"),
    ("callback-overtakes", [
        ("or self.callback_order[rec.partition][0] != ticket):", "or False):"),
    ], "OwnerControls.test_callbacks_remain_ordered_within_a_partition"),
    ("close-cutoff-erased", [
        ('return waiter in self.barriers or self.close_result is not None',
         'return self.barriers.pop(waiter, None) is not None'),
    ], "OwnerControls.test_canceled_close_observer_does_not_erase_durable_cutoff"),
    ("close-reports-unsettled-success", [
        ('if waiter == "close" and any(\n            batch.phase != "done" and any(ticket <= barrier.cutoff for ticket in batch.members)\n            for batch in self.batches.values()\n        ):',
         'if False:'),
    ], "OwnerControls.test_close_cannot_report_success_after_unknown_error_was_consumed"),
    ("ack-beats-deadline", [
        ("# timer fairness are not represented by this synthetic clock.\n        self.expire(now)",
         "# timer fairness are not represented by this synthetic clock.\n        pass"),
    ], "OwnerControls.test_ack_at_record_deadline_is_not_reported_as_success"),
]
with tempfile.TemporaryDirectory() as directory:
    for name, replacements, test in mutants:
        candidate = source
        for original, replacement in replacements:
            if candidate.count(original) != 1:
                raise RuntimeError(f"mutant anchor is not unique: {name}")
            candidate = candidate.replace(original, replacement)
        path = Path(directory) / f"{name}.py"
        path.write_text(candidate, encoding="utf-8")
        result = subprocess.run([sys.executable, str(path), test], text=True, capture_output=True)
        if result.returncode == 0 or "FAIL:" not in result.stderr or "SyntaxError" in result.stderr:
            print(result.stdout, result.stderr, file=sys.stderr)
            raise RuntimeError(f"mutant was not rejected as a test failure: {name}")
        print(f"MUTANT_REJECTED {name} test={test}", flush=True)

"""Real Python compiler-input admission controls; no SDK/JVM/protocol vectors."""
import sys
sys.dont_write_bytecode = True
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from test_prelaunch import runner


class OracleOverlayControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="streams-oracle-overlay-control-")
        self.base = Path(self.temp.name)
        self.snapshot = self.base / "snapshot"
        self.original = self.snapshot / runner.ORACLE
        self.original.parent.mkdir(parents=True)
        self.original.write_bytes(b"independent synthetic original")
        self.original.chmod(0o600)
        self.overlay = self.base / "standalone.java"
        self.overlay.write_bytes(b"independent synthetic corrected caller")
        self.overlay.chmod(0o644)
        self.patches = [patch.object(runner, "OVERLAY_ORACLE", self.overlay),
                        patch.object(runner, "OVERLAY_ORACLE_SHA", runner.digest(self.overlay)),
                        patch.object(runner, "OVERLAY_ORACLE_BYTES", self.overlay.stat().st_size),
                        patch.object(runner, "ORACLE_SHA", runner.digest(self.original))]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        # Only independent synthetic controls are cleaned; no actual inputs,
        # snapshots or retained SDK/evidence outputs are modified or deleted.
        self.temp.cleanup()

    def test_synthetic_exact_input_keeps_both_separate_source_identities(self):
        source, receipt = runner.compiler_oracle(self.overlay, self.snapshot)
        self.assertEqual(source, self.overlay)
        self.assertEqual(receipt["standalone_compiler_oracle"]["mode"], "0644")
        self.assertEqual(receipt["complete_source_original_oracle"]["mode"], "0600")
        self.assertNotEqual(receipt["standalone_compiler_oracle"]["path"],
                            receipt["complete_source_original_oracle"]["path"])
        self.assertEqual(receipt["complete_product_source_sha"], runner.PIN)

    def test_same_length_oracle_byte_change_rejected_before_dispatch(self):
        data=self.overlay.read_bytes()
        self.overlay.write_bytes(bytes([data[0]^1])+data[1:])
        with self.assertRaisesRegex(ValueError, "byte/hash pin"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_changed_length_oracle_rejected_before_dispatch(self):
        self.overlay.write_bytes(self.overlay.read_bytes()+b"extra")
        with self.assertRaisesRegex(ValueError, "byte/hash pin"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_read_permission_change_rejected_before_dispatch(self):
        self.overlay.chmod(0o640)
        with self.assertRaisesRegex(ValueError, "full0644"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_special_permission_bits_rejected_before_dispatch(self):
        self.overlay.chmod(0o1644)
        with self.assertRaisesRegex(ValueError, "full0644"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_symlink_is_rejected_instead_of_resolving_to_other_input(self):
        target=self.base/"other.java"
        target.write_bytes(self.overlay.read_bytes())
        self.overlay.unlink()
        self.overlay.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "regular"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_directory_rejected_before_digest(self):
        self.overlay.unlink()
        self.overlay.mkdir(mode=0o644)
        with self.assertRaisesRegex(ValueError, "regular"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_relative_path_rejected_even_if_same_basename(self):
        with self.assertRaisesRegex(ValueError, "absolute WORK path"):
            runner.compiler_oracle(Path(self.overlay.name), self.snapshot)

    def test_other_absolute_path_rejected_even_if_identical_bytes(self):
        other=self.base/"other.java"
        other.write_bytes(self.overlay.read_bytes())
        other.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "absolute WORK path"):
            runner.compiler_oracle(other, self.snapshot)

    def test_original_snapshot_oracle_still_pinned_with_valid_standalone(self):
        self.original.write_bytes(b"changed synthetic snapshot original")
        with self.assertRaisesRegex(ValueError, "original oracle differs"):
            runner.compiler_oracle(self.overlay, self.snapshot)

    def test_after_input_identity_catches_later_permission_mutation(self):
        _, receipt=runner.compiler_oracle(self.overlay,self.snapshot)
        before=receipt["standalone_compiler_oracle"]
        self.overlay.chmod(0o600)
        self.assertNotEqual(before,runner.identity(self.overlay))


class ActualFrozenOracleControl(unittest.TestCase):
    def test_actual_standalone_and_original_read_only_admission(self):
        source,receipt=runner.compiler_oracle(runner.OVERLAY_ORACLE,
            Path("/workspace/work/client-capabilities-source-04f6bc29"))
        self.assertEqual(source,Path("/workspace/work/streams-codecs/revision-06/StreamsWireOracle.java"))
        self.assertEqual(receipt["standalone_compiler_oracle"]["sha256"],
            "848047c5fb87ca57e3d19dd76c575f3b2edbe86ff3ac1d378daca13f1206110a")
        self.assertEqual(receipt["complete_source_original_oracle"]["sha256"],
            "7f81f6eeb83938ed6b5411e17b2d308ae9153feabf4bbcc50d13cc16c4557f2d")


if __name__ == "__main__":
    unittest.main(verbosity=2)

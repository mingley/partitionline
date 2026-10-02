"""Artifact false-green guards for the pinned behavioral assertion."""
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('c0125',Path(__file__).with_name('run.py'))
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)

class BehavioralArtifactTests(unittest.TestCase):
    def fixture(self):
        rows=[dict(event='timing_assertion',name='NO_FLUSH',elapsed_ms=10050,lower_ms=10000,upper_ms=15000,passed=True),
              dict(event='timing_assertion',name='FLUSH',elapsed_ms=10,lower_ms=0,upper_ms=2500,passed=True)]
        audit=[dict(event='consumed') for _ in range(100)]
        return rows,audit

    def test_widened_upstream_bounds_cannot_report_green(self):
        rows,audit=self.fixture();rows[1]['upper_ms']=15000;rows[1]['elapsed_ms']=10000
        self.assertFalse(runner.validate_result(rows,audit,'normal',0)['behavior_pass'])

    def test_forged_pass_flag_cannot_hide_bad_elapsed(self):
        rows,audit=self.fixture();rows[1]['elapsed_ms']=10000
        self.assertFalse(runner.validate_result(rows,audit,'normal',0)['behavior_pass'])

    def test_missing_receipt_artifact_cannot_report_green(self):
        rows,audit=self.fixture()
        self.assertFalse(runner.validate_result(rows,audit[:-1],'normal',0)['behavior_pass'])

    def test_unchanged_upstream_source_is_pinned(self):
        here=Path(__file__).parent;pin=json.loads((here/'pin.json').read_text())
        self.assertEqual(hashlib.sha256((here/'upstream/0125-immediate_flush.c').read_bytes()).hexdigest(),pin['source_sha256'])
        self.assertEqual(pin['commit'],'9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab')

if __name__=='__main__':unittest.main()

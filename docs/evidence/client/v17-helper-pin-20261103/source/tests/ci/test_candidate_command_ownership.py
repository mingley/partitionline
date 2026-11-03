"""Small real Python process trees; no Cargo, Kafka, or performance workload."""
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('candidate_owned', ROOT/'scripts/candidate-command-ownership.py')
OWNED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OWNED)

LEAF = '''import json,os,signal,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
if len(sys.argv)>2 and sys.argv[2]=='new-group':os.setpgrp()
fields=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()
path=Path(sys.argv[1]);temporary=path.with_suffix('.tmp')
temporary.write_text(json.dumps({'pid':os.getpid(),'start':int(fields[19]),'pgrp':os.getpgrp(),'session':os.getsid(0)}))
temporary.replace(path)
time.sleep(30)
'''
PARENT = '''import signal,subprocess,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
child=subprocess.Popen([sys.executable,'-c',%r,sys.argv[1],*sys.argv[3:]])
while not Path(sys.argv[1]).exists():time.sleep(.01)
if sys.argv[2]=='exit':sys.exit(0)
time.sleep(30)
''' % LEAF


def until(condition):
    deadline = time.monotonic()+5
    while not condition():
        if time.monotonic() >= deadline:
            raise RuntimeError('synthetic process control deadline')
        time.sleep(.01)


def original_process_gone(witness):
    row = json.loads(witness.read_text())
    try:
        fields = Path('/proc', str(row['pid']), 'stat').read_text().rsplit(')', 1)[1].split()
    except FileNotFoundError:
        return True
    return int(fields[19]) != row['start']


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux command ownership primitive')
class CandidateCommandOwnership(unittest.TestCase):
    def test_success_restores_subreaper_and_waits_for_its_leader(self):
        library = ctypes.CDLL(None)
        old = ctypes.c_int()
        self.assertEqual(library.prctl(37, ctypes.byref(old), 0, 0, 0), 0)
        with OWNED.owned_command([sys.executable, '-c', 'pass'], stdout=subprocess.DEVNULL) as owner:
            until(owner.finished)
        after = ctypes.c_int()
        self.assertEqual(library.prctl(37, ctypes.byref(after), 0, 0, 0), 0)
        self.assertEqual(after.value, old.value)
        self.assertTrue(owner.cleanup_receipt['leader_waited'])
        self.assertEqual(owner.cleanup_receipt['leader_exit'], 0)

    def test_exited_leader_cannot_leave_its_ignoring_grandchild_or_kill_a_bystander(self):
        with tempfile.TemporaryDirectory() as temporary:
            witness = Path(temporary)/'leaf.json'
            bystander = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'],
                start_new_session=True, stdout=subprocess.DEVNULL)
            try:
                with OWNED.owned_command([sys.executable, '-c', PARENT, str(witness), 'exit'],
                        stdout=subprocess.DEVNULL) as owner:
                    until(owner.finished)
                self.assertTrue(original_process_gone(witness))
                self.assertEqual(owner.cleanup_receipt['leader_exit'], 0)
                self.assertTrue(owner.cleanup_receipt['reaped_descendants'])
                self.assertFalse(owner.cleanup_receipt['remaining_descendants'])
                self.assertIsNone(bystander.poll())
            finally:
                bystander.terminate()
                bystander.wait(timeout=3)

    def test_observer_exception_still_kills_and_joins_the_owned_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            witness = Path(temporary)/'leaf.json'
            with self.assertRaisesRegex(RuntimeError, 'synthetic observer'):
                with OWNED.owned_command([sys.executable, '-c', PARENT, str(witness), 'wait'],
                        stdout=subprocess.DEVNULL) as owner:
                    until(witness.exists)
                    raise RuntimeError('synthetic observer')
            self.assertTrue(owner.cleanup_receipt['leader_waited'])
            self.assertTrue(original_process_gone(witness))

    def test_child_in_another_group_in_the_private_session_is_joined(self):
        with tempfile.TemporaryDirectory() as temporary:
            witness = Path(temporary)/'leaf.json'
            with OWNED.owned_command([sys.executable, '-c', PARENT, str(witness), 'wait', 'new-group'],
                    stdout=subprocess.DEVNULL) as owner:
                until(witness.exists)
                child = json.loads(witness.read_text())
                self.assertNotEqual(child['pgrp'], owner.process.pid)
                self.assertEqual(child['session'], owner.process.pid)
            self.assertTrue(owner.cleanup_receipt['leader_waited'])
            self.assertTrue(owner.cleanup_receipt['reaped_descendants'])
            self.assertTrue(original_process_gone(witness))

    def test_term_int_and_hup_interruptions_join_children_and_restore_handlers(self):
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            with self.subTest(signal=number), tempfile.TemporaryDirectory() as temporary:
                witness = Path(temporary)/'leaf.json'
                previous = signal.getsignal(number)
                with self.assertRaises(InterruptedError):
                    with OWNED.owned_command([sys.executable, '-c', PARENT, str(witness), 'wait'],
                            stdout=subprocess.DEVNULL) as owner:
                        until(witness.exists)
                        os.kill(os.getpid(), number)
                        owner.finished()
                self.assertTrue(owner.cleanup_receipt['leader_waited'])
                self.assertTrue(original_process_gone(witness))
                self.assertEqual(signal.getsignal(number), previous)

    def test_driver_timeout_retains_failure_after_joining_its_tree(self):
        spec = importlib.util.spec_from_file_location('broker_timeout_driver', ROOT/'scripts/ci-qualify-broker-candidate.py')
        driver = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(driver)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root/'source', root/'output'
            source.mkdir()
            output.mkdir()
            witness = output/'leaf.json'
            qualification = driver.Qualification(output, source, {})
            with self.assertRaisesRegex(ValueError, 'resource guard failed'):
                qualification.run('timeout', [sys.executable, '-c', PARENT, str(witness), 'wait'], timeout=1)
            receipt = json.loads((output/'timeout.json').read_text())
            self.assertEqual(receipt['stop_reason'], 'command-timeout')
            self.assertFalse(receipt['passed'])
            self.assertTrue(receipt['ownership_complete'])
            self.assertTrue(original_process_gone(witness))


if __name__ == '__main__':
    unittest.main()

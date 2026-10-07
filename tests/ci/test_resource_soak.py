"""Execute resource driver profiles, interrupt/resume, and corrupt real receipts."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("resource_soak", ROOT / "scripts/resource-soak.py")
soak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(soak)


class ResourceSoak(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configured = os.environ.get("PL_RESOURCE_SOAK_BINARY")
        if not configured:
            raise RuntimeError("build the example and set PL_RESOURCE_SOAK_BINARY; no skipped/fake pass")
        cls.binary = Path(configured).resolve(strict=True)
        cls.directory = Path(tempfile.mkdtemp(prefix="resource-soak-", dir=os.environ.get("PL_RESOURCE_SOAK_OUTPUT")))
        cls.nominal = cls.directory / "nominal"
        cls.call("--output", cls.nominal, "--binary", cls.binary, "--duration-ms", "1100")

    @classmethod
    def tearDownClass(cls):
        # CI retains the entire directory when an explicit artifact destination is set.
        if not os.environ.get("PL_RESOURCE_SOAK_OUTPUT"):
            shutil.rmtree(cls.directory)

    @classmethod
    def call(cls, *args, expected=0):
        result = subprocess.run(["python3", "-B", str(ROOT / "scripts/resource-soak.py"), *map(str,args)],
                                capture_output=True, text=True, timeout=25)
        if result.returncode != expected:
            raise AssertionError(f"exit {result.returncode} != {expected}: {result.stdout}\n{result.stderr}")
        return result

    def test_nominal_overload_has_rejections_completion_and_bounded_payload(self):
        result = soak.audit(self.nominal)
        self.assertTrue(result["completed"])
        receipt = json.loads((self.nominal / "manifest.json").read_text())["attempts"][0]["check"]
        final = receipt["final"]
        self.assertEqual(final["offered"], 880)
        self.assertGreater(final["failed"], 0)
        self.assertGreater(final["completed"], 0)
        self.assertGreater(final["delivered"], 0)
        self.assertFalse(result["production_qualification"])

    def test_stalled_replies_record_ambiguous_delivery(self):
        output = self.directory / "stalled"
        self.call("--output",output,"--binary",self.binary,"--duration-ms","1100","--peer-delay-ms","60000")
        final = json.loads((output/"manifest.json").read_text())["attempts"][0]["check"]["final"]
        self.assertGreater(final["ambiguous"],0)
        self.assertGreater(final["failed"],0)
        self.assertEqual(final["completed"],0)
        self.assertEqual(final["pending"],0)

    def test_slow_application_retains_one_batch_and_buffered_records(self):
        output = self.directory / "slow"
        self.call("--output",output,"--binary",self.binary,"--duration-ms","1100","--slow-ms","100")
        samples = list(soak.rows(output/"attempt-0001/samples.jsonl"))
        self.assertTrue(any(r.get("consumer_buffered_bytes",0)>0 and r.get("application_decoded_bytes",0)>0 for r in samples))
        self.assertLessEqual(max(r.get("consumer_buffered_bytes",0) for r in samples),4096)

    def interrupt(self, output, binary):
        child = subprocess.Popen(["python3","-B",str(ROOT/"scripts/resource-soak.py"),"--output",str(output),
                                  "--binary",str(binary),"--duration-ms","2200"],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            raw = output/"attempt-0001/samples.jsonl"
            deadline = time.monotonic()+10
            while time.monotonic()<deadline:
                if raw.exists() and len(raw.read_text().splitlines())>=4:
                    break
                if child.poll() is not None:
                    self.fail("driver exited before interrupt")
                time.sleep(.02)
            else:
                self.fail("no samples before interrupt deadline")
            child.send_signal(signal.SIGTERM)
            stdout,stderr = child.communicate(timeout=12)
            self.assertEqual(child.returncode,0,stdout+stderr)
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
            if child.stdout: child.stdout.close()
            if child.stderr: child.stderr.close()

    def test_interrupt_resume_keeps_every_prior_byte_and_frozen_manifest(self):
        output = self.directory/"resumed"
        self.interrupt(output,self.binary)
        first = json.loads((output/"manifest.json").read_text())["attempts"][0]
        frozen = soak.digest(output/"frozen.json")
        self.assertEqual(first["status"],"interrupted")
        self.assertFalse(soak.audit(output)["completed"])
        self.call("--output",output,"--resume")
        manifest = json.loads((output/"manifest.json").read_text())
        self.assertEqual(manifest["attempts"][0],first)
        self.assertEqual(soak.digest(output/"frozen.json"),frozen)
        for p,digest in first["artifacts"].items(): self.assertEqual(soak.digest(output/p),digest)
        self.assertEqual(len(manifest["attempts"]),2)
        self.assertTrue(soak.audit(output)["completed"])
        self.assertGreaterEqual(soak.audit(output)["load_elapsed_ms"],2200)
        # A completed resume is idempotent and starts no child.
        self.call("--output",output,"--resume")
        self.assertEqual(len(json.loads((output/"manifest.json").read_text())["attempts"]),2)

    def test_changed_binary_cannot_resume(self):
        output = self.directory/"changed-binary"
        binary = self.directory/"binary-copy"
        shutil.copy2(self.binary,binary)
        binary.chmod(0o700)
        self.interrupt(output,binary)
        with binary.open("ab") as stream: stream.write(b"changed")
        rejected = self.call("--output",output,"--resume",expected=1)
        self.assertIn("source/binary changed",rejected.stderr)
        self.assertEqual(len(json.loads((output/"manifest.json").read_text())["attempts"]),1)

    def test_checker_rejects_corrupted_actual_samples_and_missing_closures(self):
        original = list(soak.rows(self.nominal/"attempt-0001/samples.jsonl"))
        final_index = next(i for i,r in enumerate(original) if r.get("final") is True)
        mutations = {
            "lost outcome": (final_index,"accepted",-1),
            "reservation leak": (final_index,"producer_queue_bytes",1),
            "budget bypass": (1,"producer_queue_bytes",4097),
            "decode cap": (1,"consumer_buffered_bytes",4097),
            "detached tasks": (len(original)-1,"runtime_tasks",1),
            "socket leak": (len(original)-1,"connections",1),
            "wrong type": (1,"offered",True),
        }
        target = self.directory/"mutated.jsonl"
        for label,(index,key,value) in mutations.items():
            with self.subTest(label=label):
                candidate = copy.deepcopy(original)
                candidate[index][key]=value
                target.write_text("".join(json.dumps(r)+"\n" for r in candidate))
                with self.assertRaises(ValueError):
                    soak.check_samples(target,self.nominal/"attempt-0001/host.jsonl","short")
        target.write_text("".join(json.dumps(r)+"\n" for r in original[:-1]))
        with self.assertRaisesRegex(ValueError,"incomplete"):
            soak.check_samples(target,self.nominal/"attempt-0001/host.jsonl","short")

    def test_check_rejects_changed_artifact_and_frozen_receipt(self):
        output = self.directory/"tampered"
        shutil.copytree(self.nominal,output)
        with (output/"attempt-0001/samples.jsonl").open("a") as stream: stream.write("{}\n")
        self.assertIn("artifact changed",self.call("--output",output,"--check",expected=1).stderr)
        with (output/"frozen.json").open("a") as stream: stream.write(" \n")
        self.assertIn("frozen manifest changed",self.call("--output",output,"--check",expected=1).stderr)

    def test_completion_cannot_hide_missing_load_time(self):
        output = self.directory/"false-completion"
        shutil.copytree(self.nominal,output)
        manifest = json.loads((output/"manifest.json").read_text())
        manifest["completed"]=False
        (output/"manifest.json").write_text(json.dumps(manifest))
        self.assertIn("completion receipt differs",self.call("--output",output,"--check",expected=1).stderr)

    def test_controlled_missing_baseline_and_shared_topic_fail_before_child(self):
        for name,args in (("no-baseline",["--mode","controlled","--bootstrap","127.0.0.1:1","--topic","pl-soak-owned"]),
                          ("shared-topic",["--topic","production-topic"])):
            output = self.directory/name
            self.call("--output",output,"--binary",self.binary,*args,expected=1)
            self.assertFalse((output/"manifest.json").exists())

    def test_failed_child_is_retained_and_never_hidden_by_resume(self):
        output = self.directory/"failed"
        binary = self.directory/"fails"
        binary.write_text("#!/bin/sh\nexit 9\n")
        binary.chmod(0o700)
        self.call("--output",output,"--binary",binary,expected=1)
        manifest = json.loads((output/"manifest.json").read_text())
        first = manifest["attempts"][0]
        self.assertEqual(first["exit_code"],9)
        self.assertTrue(first["waited"])
        self.assertEqual(first["status"],"failed")
        self.assertIn("retained failed attempt",self.call("--output",output,"--resume",expected=1).stderr)
        self.assertEqual(json.loads((output/"manifest.json").read_text())["attempts"][0],first)


if __name__ == "__main__":
    unittest.main()

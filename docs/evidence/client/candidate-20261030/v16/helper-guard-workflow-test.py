#!/usr/bin/env python3
"""Exercise the v16-only workflow bootstrap with synthetic local helper files."""
import hashlib,json,os,shutil,subprocess,tempfile,unittest
from pathlib import Path
import yaml
HERE=Path(__file__).resolve().parent
REPO=Path('/workspace/partitionline')
GUARD=HERE/'ci-verify-client-candidate-helpers.py'
WORKFLOW='.github/workflows/candidates.yml'
NAMES=('scripts/ci-qualify-client-candidate.py','tests/ci/test_client_candidate_qualification.py',
       'scripts/candidate-command-ownership.py','tests/ci/test_candidate_command_ownership.py',
       'scripts/ci-qualify-broker-candidate.py','scripts/ci-verify-client-candidate-helpers.py')

class BootstrapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory(prefix='v16-workflow-parse-',dir=HERE) as tmp:
            root=Path(tmp);path=root/WORKFLOW;path.parent.mkdir(parents=True)
            base=subprocess.run(['git','show','020f3a734a9dccb1b778c5fd60782e604a13e3a9:'+WORKFLOW],cwd=REPO,check=True,capture_output=True).stdout
            path.write_bytes(base)
            subprocess.run(['git','apply',str(HERE/'workflow-full.patch')],cwd=root,check=True,capture_output=True)
            text=path.read_text();rows=yaml.safe_load(text)['jobs']['qualify']['steps']
            cls.steps=[next(x for x in rows if x.get('name')==name) for name in ('Verify v16 helper pins and controls','Recheck v16 helper pins')]
            cls.workflow_text=text
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='v16-bootstrap-',dir=HERE);self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        for name in NAMES:
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True)
            p.write_bytes(GUARD.read_bytes() if name.endswith('ci-verify-client-candidate-helpers.py') else ('fixture:'+name).encode())
        self.manifest=self.root/'docs/evidence/client/candidate-20261030/v16/import-manifest.json';self.manifest.parent.mkdir(parents=True)
        self.write_manifest()
    def write_manifest(self):
        pins={}
        for name in NAMES:
            data=(self.root/name).read_bytes();pins[name]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)}
        self.manifest.write_text(json.dumps({'required_existing_root_files':pins}))
    def run_step(self,step):
        return subprocess.run(['bash','-e','-o','pipefail','-c',step['run']],cwd=self.root,capture_output=True,text=True,timeout=30)
    def test_both_steps_accept_exact_fixture(self):
        for step in self.steps:
            result=self.run_step(step);self.assertEqual(result.returncode,0,result.stderr)
    def test_guard_noop_with_forged_manifest_rejected(self):
        target=self.root/NAMES[-1];old=target.read_bytes();prefix=b'print("no-op verifier")\n'
        target.write_bytes(prefix+b' '*(len(old)-len(prefix)));self.write_manifest()
        for step in self.steps:
            result=self.run_step(step);self.assertNotEqual(result.returncode,0);self.assertIn('guard hash mismatch',result.stderr)
    def test_symlink_guard_or_parent_rejected(self):
        target=self.root/NAMES[-1];real=self.root/'real-guard';target.rename(real);target.symlink_to(real)
        for step in self.steps:self.assertNotEqual(self.run_step(step).returncode,0)
        target.unlink();real.rename(target);scripts=self.root/'scripts';moved=self.root/'scripts-real';scripts.rename(moved);scripts.symlink_to(moved,target_is_directory=True)
        for step in self.steps:self.assertNotEqual(self.run_step(step).returncode,0)
    def test_unmodified_guard_rejects_changed_helper(self):
        (self.root/NAMES[0]).write_bytes(b'changed')
        for step in self.steps:
            result=self.run_step(step);self.assertNotEqual(result.returncode,0);self.assertIn('helper size or hash mismatch',result.stderr)
    def test_owner_test_and_broker_driver_same_size_mutations_fail(self):
        for name in ('tests/ci/test_candidate_command_ownership.py','scripts/ci-qualify-broker-candidate.py'):
            path=self.root/name;before=path.read_bytes();path.write_bytes(bytes([before[0]^1])+before[1:])
            for step in self.steps:
                result=self.run_step(step);self.assertNotEqual(result.returncode,0);self.assertIn('helper size or hash mismatch',result.stderr)
            path.write_bytes(before)
    def test_v16_conditions_and_trigger(self):
        for step in self.steps:self.assertEqual(step['if'],"matrix.kind == 'client' && matrix.source_version == 'v16'")
        self.assertIn('      - scripts/ci-verify-client-candidate-helpers.py\n',self.workflow_text)
        self.assertIn('checkout_sha: 8f960c3e60609703f23f79b3f368355ed8c057d8',self.workflow_text)

if __name__=='__main__':unittest.main()

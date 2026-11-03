#!/usr/bin/env python3
"""Offline capsule, panic provenance and source reconstruction checks; no Cargo."""
import ast
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import textwrap
import types
import sys
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('capacity_qualification', HERE/'qualify.py')
qualify = importlib.util.module_from_spec(spec);spec.loader.exec_module(qualify)

def clean_git_env():
    env={key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
    env['LC_ALL']='C'
    return env


def detached_scratch_base(candidate, checkout=None):
    """Reject a scratch base in the checkout or in any Git worktree."""
    path=Path(candidate)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise ValueError('scratch base must be a real absolute directory')
    path=path.resolve(strict=True)
    if checkout is not None:
        checkout=Path(checkout).resolve(strict=True)
        if path.is_relative_to(checkout):raise ValueError('scratch base is inside the checkout')
    for parent in (path,*path.parents):
        marker=parent/'.git'
        if marker.exists() or marker.is_symlink():raise ValueError('scratch base is inside a Git worktree')
    probe=subprocess.run(['git','-C',str(path),'rev-parse','--is-inside-work-tree'],
                         capture_output=True,text=True,timeout=60,env=clean_git_env())
    if probe.returncode != 128 or not probe.stderr.startswith('fatal: not a git repository'):
        raise ValueError('scratch base could not be proven outside Git')
    return path


class Controls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = json.loads((HERE/'spec.json').read_text())
        cls.manifest = json.loads((HERE/'base-files.json').read_text())

    def test_assets_and_exact_base_archive(self):
        self.assertEqual(qualify.sha(HERE/'spec.json'),qualify.SPEC_SHA)
        for key in ('base_source','base_files','baseline_patch','fixed_patch','candidate_manifest','source_review','second_review','candidate_validation'):
            item=self.spec[key];path=HERE/item['path']
            self.assertEqual((qualify.sha(path),path.stat().st_size),(item['sha256'],item['bytes']))
        self.assertEqual(self.spec['base_commit'],qualify.BASE)
        seen=set()
        with tarfile.open(HERE/'base-source.tar.gz','r:gz') as archive:
            for member in archive:
                self.assertTrue(member.isfile(),member.name);self.assertNotIn(member.name,seen);seen.add(member.name)
                expected=self.manifest[member.name];data=archive.extractfile(member).read()
                self.assertEqual((len(data),member.mode,hashlib.sha256(data).hexdigest()),
                                 (expected['bytes'],expected['mode'],expected['sha256']),member.name)
        self.assertEqual(seen,set(self.manifest))
        self.assertEqual(len(seen),5982)
        self.assertEqual(self.manifest['partitionline-broker/Cargo.lock']['sha256'],self.spec['broker_lock_sha256'])

    def test_sparse_roundtrip_and_unchanged_test_bodies(self):
        runtime=self.spec['runtime_path'];base=None
        with tarfile.open(HERE/'base-source.tar.gz','r:gz') as archive:base=archive.extractfile(runtime).read()
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        scratch=detached_scratch_base(os.environ.get('RUNNER_TEMP') or tempfile.gettempdir(),checkout)
        with tempfile.TemporaryDirectory(prefix='capacity-static-',dir=scratch) as temp:
            root=Path(temp);path=root/runtime;path.parent.mkdir(parents=True);path.write_bytes(base)
            def apply(name,reverse=False):
                for check in (True,False):
                    subprocess.run(['git','apply']+(['--reverse'] if reverse else [])+(['--check'] if check else [])+[str(HERE/name)],cwd=root,check=True,capture_output=True,timeout=120,env=clean_git_env())
            apply('baseline-tests-only.patch');baseline=path.read_text()
            self.assertEqual(qualify.sha(path),self.spec['baseline_runtime']['sha256'])
            self.assertEqual(baseline[:baseline.index('mod ownership_tests {')],base.decode()[:base.decode().index('mod ownership_tests {')])
            marker='assert!(matches!(\n            tokio::time::timeout(Duration::from_secs(2), handle.propose(over_capacity)).await?,\n            Err(Error::InvalidConfig)\n        ));'
            self.assertEqual(baseline.count(marker),1)
            self.assertEqual(baseline[:baseline.index(marker)].count('\n')+1,self.spec['baseline_negative_panic_line'])
            apply('baseline-tests-only.patch',True);self.assertEqual(path.read_bytes(),base)
            apply('runtime-capacity-v2.patch');fixed=path.read_text();self.assertEqual(qualify.sha(path),self.spec['fixed_runtime']['sha256'])
            start='    #[test]\n    fn proposal_capacity_check_keeps_the_original_allocation()'
            end='    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]\n    async fn configured_route_does_not_authorize_unknown_configuration_leader_over_tcp('
            self.assertEqual(baseline[baseline.index(start):baseline.index(end)],fixed[fixed.index(start):fixed.index(end)])
            self.assertIn('baseline_over_capacity_hits_admission_deadline_with_permits_held',baseline)
            self.assertNotIn('baseline_over_capacity_hits_admission_deadline_with_permits_held',fixed)
            self.assertEqual(base.decode().count('command_ms: 20,'),1)
            self.assertIn('                20,\n            )\n            .await',fixed)
            apply('runtime-capacity-v2.patch',True);self.assertEqual(path.read_bytes(),base)

    def test_existing_broker_runner_controls_are_pinned(self):
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        if not checkout:self.skipTest('set CAPACITY_CHECKOUT to verify the broker runner pins')
        root=Path(checkout)
        for key in ('legacy_driver','ownership_helper'):
            item=self.spec[key];self.assertEqual(qualify.sha(root/item['path']),item['sha256'])
        module_spec=importlib.util.spec_from_file_location('reviewed_broker_driver',root/self.spec['legacy_driver']['path'])
        module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
        self.assertEqual(len(module.FIXED),12);self.assertEqual(len(module.ASYNC_FS_CASES),3)
        self.assertEqual(module.FETCH_DEADLINE_REPEATS,3)
        self.assertEqual(module.FETCH_DEADLINE_CASE,'transport_deadline_cancels_wait_and_releases_read_admission')

    def test_scratch_rejects_checkout_and_other_git_trees(self):
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        scratch=detached_scratch_base(os.environ.get('RUNNER_TEMP') or tempfile.gettempdir(),checkout)
        with tempfile.TemporaryDirectory(prefix='capacity-placement-',dir=scratch) as temp:
            root=Path(temp);plain=root/'plain';plain.mkdir()
            self.assertEqual(detached_scratch_base(plain,checkout),plain.resolve())
            other=root/'other-checkout';other.mkdir()
            subprocess.run(['git','init','--quiet',str(other)],check=True,capture_output=True,timeout=60,env=clean_git_env())
            nested=other/'scratch';nested.mkdir()
            with self.assertRaises(ValueError):detached_scratch_base(nested,checkout)
            with self.assertRaises(ValueError):detached_scratch_base(nested,other)
            nonrepo=root/'another-checkout';nonrepo.mkdir();inside=nonrepo/'scratch';inside.mkdir()
            with self.assertRaises(ValueError):detached_scratch_base(inside,nonrepo)

    def test_workflow_runs_the_verified_suite_and_rejects_zero_skips_or_failures(self):
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        workflow=os.environ.get('CAPACITY_WORKFLOW_FILE')
        if not workflow and checkout:workflow=str(Path(checkout)/'.github/workflows/candidates.yml')
        if not workflow:self.skipTest('provide a workflow file for its loader regression')
        text=Path(workflow).read_text()
        start="          python3 -B - <<'PYCODE'\n"
        self.assertEqual(text.count(start),1)
        block=text.split(start,1)[1].split('\n          PYCODE',1)[0]
        tree=ast.parse(textwrap.dedent(block))
        definitions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run_static']
        self.assertEqual(len(definitions),1)
        namespace={'types':types,'unittest':unittest,'sys':sys}
        exec(compile(ast.fix_missing_locations(ast.Module(body=definitions,type_ignores=[])),'workflow-loader','exec'),namespace)
        run=namespace['run_static']
        good=b'import unittest\nclass Check(unittest.TestCase):\n def test_one(self): pass\n def test_two(self): pass\n'
        run(good,'good-static.py',2,io.StringIO())
        with self.assertRaises(SystemExit):run(b'import unittest\n','empty-static.py',2,io.StringIO())
        skipped=good.replace(b'def test_two(self): pass',b"@unittest.skip('skip')\n def test_two(self): pass")
        with self.assertRaises(SystemExit):run(skipped,'skipped-static.py',2,io.StringIO())
        failed=good.replace(b'def test_two(self): pass',b'def test_two(self): self.fail()')
        with self.assertRaises(SystemExit):run(failed,'failed-static.py',2,io.StringIO())
        expected_failure=good.replace(b'def test_two(self): pass',b'@unittest.expectedFailure\n def test_two(self): self.fail()')
        with self.assertRaises(SystemExit):run(expected_failure,'expected-failure-static.py',2,io.StringIO())

    def test_expected_negative_is_confined_to_one_failure_section(self):
        name=qualify.NEGATIVE;line=self.spec['baseline_negative_panic_line']
        head=f'---- {name} stdout ----\n';site=f"thread '{name}' panicked at src/raft/runtime.rs:{line}:9:\n"
        assertion='assertion failed: matches!(tokio::time::timeout(Duration::from_secs(2), handle.propose(over_capacity)).await?, Err(Error::InvalidConfig))\n'
        valid='failures:\n\n'+head+site+assertion+'\nfailures:\n    '+name+'\n'
        self.assertTrue(qualify.expected_negative(valid,line))
        with_thread_id=valid.replace(f"thread '{name}' panicked",f"thread '{name}' (2796) panicked")
        self.assertTrue(qualify.expected_negative(with_thread_id,line))
        self.assertFalse(qualify.expected_negative(with_thread_id.replace('(2796)','(not-a-number)'),line))
        self.assertFalse(qualify.expected_negative(valid.replace(f':{line}:9:',f':{line-1}:9:'),line))
        self.assertFalse(qualify.expected_negative(valid.replace('Err(Error::InvalidConfig)','Err(Error::Deadline)'),line))
        self.assertFalse(qualify.expected_negative(valid.replace('assertion failed:', 'something else:'),line))
        self.assertFalse(qualify.expected_negative(valid.replace(assertion,'assertion failed: false\n'+assertion),line))
        self.assertFalse(qualify.expected_negative(valid+head+site+assertion,line))
        fake=valid.replace(site,site.replace(f':{line}:9:',f':{line-1}:9:'))+f'\n---- unrelated stdout ----\n{site}{assertion}'
        self.assertFalse(qualify.expected_negative(fake,line))
        self.assertFalse(qualify.expected_negative(f'{site}{assertion}',line))

    def test_join_assets_source_and_control_contract(self):
        for key in ('join_baseline_patch','join_fixed_patch','join_receipt','join_independent_review','workflow_preimage','capacity_wrapper_preimage'):
            item=self.spec[key];path=HERE/item['path']
            self.assertEqual((qualify.sha(path),path.stat().st_size),(item['sha256'],item['bytes']))
        author=json.loads((HERE/'join-receipt.json').read_text())
        review=json.loads((HERE/'join-independent-review.json').read_text())
        self.assertEqual(author['source_preimage_sha256'],self.spec['join_base_source']['sha256'])
        self.assertEqual(author['fixed_source_sha256'],self.spec['join_fixed_source']['sha256'])
        self.assertEqual(author['baseline_test_source_sha256'],self.spec['join_baseline_source']['sha256'])
        self.assertEqual(review['patch_sha256'],self.spec['join_fixed_patch']['sha256'])
        self.assertEqual(review['baseline_patch_sha256'],self.spec['join_baseline_patch']['sha256'])
        self.assertEqual(set(self.spec['join_positive']),qualify.JOIN_POSITIVE)
        self.assertEqual(set(self.spec['join_negative']),qualify.JOIN_NEGATIVE)
        self.assertEqual(set(self.spec['join_fixed_cases']),qualify.JOIN_FIXED)
        self.assertEqual(self.spec['join_profiles'],['default','all-features'])
        self.assertEqual(len(qualify.JOIN_NEGATIVE),4)
        self.assertEqual(len(qualify.JOIN_FIXED),5)
        begin="        gate_errors = report['gate_errors']\n"
        end='        legacy.verify_source(source,fixed)\n'
        original=(HERE/'capacity-wrapper-preimage.py').read_text()
        candidate=(HERE/'qualify.py').read_text()
        original_gates=original.split(begin,1)[1].split(end,1)[0]
        candidate_gates=candidate.split(begin,1)[1].split(end,1)[0]
        added="                attempt('fixed-'+profile+'-join',lambda:runner.exact('fixed-'+profile+'-join',unit[0],JOIN_FIXED))\n"
        self.assertEqual(candidate_gates.count(added),1)
        self.assertEqual(candidate_gates.replace(added,'',1),original_gates)

    def test_join_capacity_then_baseline_and_fixed_roundtrip(self):
        runtime=self.spec['runtime_path'];protocol=self.spec['join_source_path']
        with tarfile.open(HERE/'base-source.tar.gz','r:gz') as archive:
            runtime_base=archive.extractfile(runtime).read();protocol_base=archive.extractfile(protocol).read()
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        scratch=detached_scratch_base(os.environ.get('RUNNER_TEMP') or tempfile.gettempdir(),checkout)
        with tempfile.TemporaryDirectory(prefix='join-static-',dir=scratch) as temp:
            root=Path(temp)
            for name,data in ((runtime,runtime_base),(protocol,protocol_base)):
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            def apply(name,reverse=False):
                for check in (True,False):
                    subprocess.run(['git','apply']+(['--reverse'] if reverse else [])+(['--check'] if check else [])+[str(HERE/name)],cwd=root,check=True,capture_output=True,timeout=120,env=clean_git_env())
            apply('runtime-capacity-v2.patch')
            capacity=(root/runtime).read_bytes()
            self.assertEqual(hashlib.sha256(capacity).hexdigest(),self.spec['fixed_runtime']['sha256'])
            apply('join-baseline.patch');baseline=(root/protocol).read_text()
            self.assertTrue(baseline.startswith(protocol_base.decode()))
            self.assertEqual(qualify.sha(root/protocol),self.spec['join_baseline_source']['sha256'])
            provenance=self.spec['join_baseline_provenance']
            for text,line in [('panic!("injected controller actor failure");',provenance['injected_actor_panic_line']),
                ('panic!("injected panic payload destructor failure");',provenance['payload_destructor_panic_line']),
                ('std::panic::panic_any(LockCheckingPanic {',provenance['payload_injection_line']),
                ('assert!(timeout(WAIT, observed_rx).await??);',provenance['payload_observation_assertion_line'])]:
                self.assertEqual(baseline.count(text),1)
                self.assertEqual(baseline[:baseline.index(text)].count('\n')+1,line)
            apply('join-baseline.patch',True)
            self.assertEqual((root/protocol).read_bytes(),protocol_base)
            self.assertEqual((root/runtime).read_bytes(),capacity)
            apply('join-fixed.patch');fixed=(root/protocol).read_text()
            self.assertEqual(qualify.sha(root/protocol),self.spec['join_fixed_source']['sha256'])
            self.assertEqual((root/runtime).read_bytes(),capacity)
            marker='\n#[cfg(test)]\nmod controller_join_tests {\n'
            self.assertEqual(baseline.count(marker),1);self.assertEqual(fixed.count(marker),1)
            bm=baseline.split(marker,1)[1];fm=fixed.split(marker,1)[1]
            for a,b in [('use super::{Command, Config, ControllerHandler, ControllerJoin, Error};',
                         'use super::{Command, Config, ControllerHandler, Error};'),
                        ('join: Mutex::new(ControllerJoin::Running(join)),','join: Mutex::new(Some(join)),'),
                        ('matches!(&*join, ControllerJoin::Running(task) if !task.is_finished())',
                         'matches!(&*join, Some(task) if !task.is_finished())')]:
                self.assertEqual(fm.count(a),1);fm=fm.replace(a,b)
            self.assertEqual(fm,bm)
            for name in qualify.JOIN_FIXED:
                self.assertEqual(fm.count('async fn '+name.rsplit('::',1)[1]+'('),1)
            apply('join-fixed.patch',True)
            self.assertEqual((root/protocol).read_bytes(),protocol_base)
            self.assertEqual((root/runtime).read_bytes(),capacity)
            apply('runtime-capacity-v2.patch',True)
            self.assertEqual((root/runtime).read_bytes(),runtime_base)

    def test_join_negative_parser_rejects_wrong_or_unrelated_failures(self):
        meta=self.spec['join_baseline_provenance']
        for name in sorted(qualify.JOIN_NEGATIVE):
            heading=f'---- {name} stdout ----\n'
            if name in qualify.JOIN_REPOLL:
                site=(f"thread 'tokio-runtime-worker' (12) panicked at src/raft/protocol.rs:{meta['injected_actor_panic_line']}:17:\n"
                      'injected controller actor failure\n'
                      f"thread '{name}' (34) panicked at /tmp/registry/tokio-{meta['tokio_version']}/src/runtime/task/core.rs:{meta['tokio_repoll_panic_line']}:18:\n"
                      'JoinHandle polled after completion\n')
                wrong=site.replace('JoinHandle polled after completion','some other panic')
                wrongline=site.replace(f":{meta['injected_actor_panic_line']}:17:",':1:17:')
            else:
                site=(f"thread '{name}' (34) panicked at src/raft/protocol.rs:{meta['payload_destructor_panic_line']}:13:\n"
                      'injected panic payload destructor failure\n'
                      f"thread '{name}' (34) panicked at src/raft/protocol.rs:{meta['payload_observation_assertion_line']}:9:\n"
                      'assertion failed: timeout(WAIT, observed_rx).await??\n')
                wrong=site.replace('await??','await?')
                wrongline=site.replace(f":{meta['payload_destructor_panic_line']}:13:",':1:13:')
            valid='failures:\n\n'+heading+site+'\nfailures:\n    '+name+'\n'
            self.assertTrue(qualify.expected_join_negative(valid,name,meta),name)
            self.assertTrue(qualify.expected_join_negative(valid.replace(' (34)',''),name,meta),name)
            self.assertFalse(qualify.expected_join_negative(valid.replace(' (34)',' (invalid)'),name,meta))
            self.assertFalse(qualify.expected_join_negative(valid.replace(site,wrong),name,meta))
            self.assertFalse(qualify.expected_join_negative(valid.replace(site,wrongline),name,meta))
            unrelated="thread 'other' (77) panicked at src/other.rs:1:1:\nunrelated panic\n"
            self.assertFalse(qualify.expected_join_negative(valid.replace(site,site+unrelated),name,meta))
            if name == qualify.JOIN_PAYLOAD:
                actor=(f"thread 'tokio-runtime-worker' (12) panicked at src/raft/protocol.rs:{meta['payload_injection_line']}:17:\n"
                       'Box<dyn Any>\n')
                self.assertTrue(qualify.expected_join_negative(valid.replace(site,actor+site),name,meta))
                self.assertFalse(qualify.expected_join_negative(valid.replace(site,actor+site+unrelated),name,meta))
                self.assertFalse(qualify.expected_join_negative(valid.replace(site,actor.replace(f":{meta['payload_injection_line']}:17:",':1:17:')+site),name,meta))
            self.assertFalse(qualify.expected_join_negative(valid+heading+site,name,meta))
            self.assertFalse(qualify.expected_join_negative(site,name,meta))
            other=valid.replace(heading,'---- unrelated stdout ----\n')
            self.assertFalse(qualify.expected_join_negative(other,name,meta))
        self.assertFalse(qualify.expected_join_negative('anything','wrong-name',meta))

    def test_join_workflow_loader_and_unchanged_existing_steps(self):
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        workflow=os.environ.get('CAPACITY_WORKFLOW_FILE')
        if not workflow and checkout:workflow=str(Path(checkout)/'.github/workflows/candidates.yml')
        if not workflow:self.skipTest('provide a workflow file for its loader regression')
        text=Path(workflow).read_text()
        start="          python3 -B - <<'PYJOIN'\n"
        self.assertEqual(text.count(start),1)
        block=text.split(start,1)[1].split('\n          PYJOIN',1)[0]
        tree=ast.parse(textwrap.dedent(block))
        definitions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='run_static']
        self.assertEqual(len(definitions),1)
        namespace={'types':types,'unittest':unittest,'sys':sys}
        exec(compile(ast.fix_missing_locations(ast.Module(body=definitions,type_ignores=[])),'join-workflow-loader','exec'),namespace)
        run=namespace['run_static']
        good=b'import unittest\nclass Check(unittest.TestCase):\n def test_one(self): pass\n def test_two(self): pass\n'
        run(good,'good-static.py',2,io.StringIO())
        with self.assertRaises(SystemExit):run(b'import unittest\n','empty-static.py',2,io.StringIO())
        skipped=good.replace(b'def test_two(self): pass',b"@unittest.skip('skip')\n def test_two(self): pass")
        with self.assertRaises(SystemExit):run(skipped,'skipped-static.py',2,io.StringIO())
        failed=good.replace(b'def test_two(self): pass',b'def test_two(self): self.fail()')
        with self.assertRaises(SystemExit):run(failed,'failed-static.py',2,io.StringIO())
        expected=good.replace(b'def test_two(self): pass',b'@unittest.expectedFailure\n def test_two(self): self.fail()')
        with self.assertRaises(SystemExit):run(expected,'expected-failure-static.py',2,io.StringIO())
        self.assertIn("run_static(verified['test_static.py'], str(root/base/'test_static.py'), 11)",text)
        self.assertIn('cancel-in-progress: false',text)
        self.assertEqual(text.count('source_version: v17'),1)
        pre=(HERE/'workflow-preimage.yml').read_text()
        addition="          - kind: broker\n            source_version: controller-join-v2-r1\n            checkout_sha: ${{ github.sha }}\n"
        self.assertEqual(text.count(addition),1)
        reverted=text.replace(addition,'',1)
        reverted=reverted.replace(" && matrix.source_version != 'controller-join-v2-r1'",'',1)
        join_start='      - name: Check and qualify pinned broker controller join\n'
        join_end='      - name: Retain attempts and failures\n'
        self.assertEqual(reverted.count(join_start),1)
        reverted=reverted.split(join_start,1)[0]+join_end+reverted.split(join_start,1)[1].split(join_end,1)[1]
        self.assertEqual(reverted,pre)

    def test_wrapper_sanitizes_git_and_rejects_other_worktrees(self):
        checkout=os.environ.get('CAPACITY_CHECKOUT') or os.environ.get('GITHUB_WORKSPACE')
        self.assertIsNotNone(checkout)
        checkout=Path(checkout).resolve(strict=True)
        scratch=detached_scratch_base(os.environ.get('RUNNER_TEMP') or tempfile.gettempdir(),checkout)
        with tempfile.TemporaryDirectory(prefix='join-wrapper-placement-',dir=scratch) as temp:
            root=Path(temp);plain=root/'plain';plain.mkdir()
            other=root/'other';other.mkdir()
            subprocess.run(['git','init','--quiet',str(other)],check=True,capture_output=True,timeout=60,env=clean_git_env())
            nested=other/'output';nested.mkdir()
            variables=('GIT_DIR','GIT_WORK_TREE')
            old={key:os.environ.get(key) for key in variables}
            try:
                os.environ['GIT_DIR']=str(other/'.git')
                os.environ['GIT_WORK_TREE']=str(other)
                unsafe=subprocess.run(['git','-C',str(plain),'rev-parse','--absolute-git-dir'],
                    capture_output=True,text=True,timeout=60,env=os.environ.copy())
                self.assertEqual((unsafe.returncode,unsafe.stdout.strip()),(0,str(other/'.git')))
                self.assertFalse(any(key.startswith('GIT_') for key in qualify.clean_git_env()))
                qualify.require_detached_output_parent(plain,checkout)
                with self.assertRaises(ValueError):qualify.require_detached_output_parent(nested,checkout)
                with self.assertRaises(ValueError):qualify.require_detached_output_parent(checkout,checkout)
            finally:
                for key,value in old.items():
                    if value is None:os.environ.pop(key,None)
                    else:os.environ[key]=value
        source=(HERE/'qualify.py').read_text();tree=ast.parse(source)
        git_calls=[]
        for node in ast.walk(tree):
            if not isinstance(node,ast.Call):continue
            if not isinstance(node.func,ast.Attribute) or not isinstance(node.func.value,ast.Name):continue
            if node.func.value.id!='subprocess' or node.func.attr!='run' or not node.args:continue
            first=node.args[0]
            if isinstance(first,ast.List) and first.elts and isinstance(first.elts[0],ast.Constant) and first.elts[0].value=='git':
                git_calls.append(node)
        self.assertEqual(len(git_calls),3)
        for call in git_calls:
            env=[kw.value for kw in call.keywords if kw.arg=='env']
            self.assertEqual(len(env),1)
            self.assertIsInstance(env[0],ast.Call)
            self.assertIsInstance(env[0].func,ast.Name)
            self.assertEqual(env[0].func.id,'clean_git_env')

if __name__ == '__main__':unittest.main()

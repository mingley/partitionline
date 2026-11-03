#!/usr/bin/env python3
"""Static and source-roundtrip controls; never invokes Cargo or a Rust compiler."""
import gzip
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('bench_runner',HERE/'run.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)

class PlanTests(unittest.TestCase):
    def test_bounded_and_balanced_pairs(self):
        cases=runner.case_plan()
        self.assertEqual(len(cases),19)
        self.assertEqual(len({tuple(sorted(case.items())) for case in cases}),19)
        for case in cases:
            self.assertGreater(case['count'],0);self.assertLessEqual(case['count'],8192)
            self.assertGreater(case['chunk'],0);self.assertLessEqual(case['chunk'],case['count'])
            self.assertGreater(case['probes'],0);self.assertLessEqual(case['probes'],256)
        orders=runner.pair_orders()
        self.assertEqual(orders,runner.pair_orders())
        self.assertEqual(orders.count(('v10-queue','v16')),5)
        self.assertEqual(orders.count(('v16','v10-queue')),5)
        fixed=json.loads((HERE/'v16-fixed-inputs.json').read_text())
        control=json.loads((HERE/'v10-queue-inputs.json').read_text())
        self.assertEqual(len(fixed),1755);self.assertEqual(len(control),1755)
        self.assertEqual({k for k in fixed if fixed[k]!=control.get(k)},{'src/producer.rs','src/producer/prepare_tests.rs'})
        self.assertEqual(fixed['src/producer.rs'],runner.V16_PRODUCER);self.assertEqual(control['src/producer.rs'],runner.V10_PRODUCER)

    def test_sample_validation_rejects_lost_or_ambiguous_identities(self):
        case=runner.case_plan()[0]
        zero=('missing','duplicate','invalid','out_of_order','ack_wrong','ack_error','ack_closed','ack_pending','remaining','buffer_bytes')
        details=('missing_ids','duplicate_ids','invalid_positions','out_of_order_ids','ack_wrong_ids','ack_error_ids','ack_closed_ids','ack_pending_ids')
        row={**case,'valid':True,'accepted':case['count'],'returned':case['count'],'ack_ok':case['count'],'elapsed_ns':17}
        row.update({name:0 for name in zero});row.update({name:[] for name in details})
        self.assertTrue(runner.validate_sample(row,case))
        for name in zero:
            broken={**row,name:1};self.assertFalse(runner.validate_sample(broken,case),name)
        for name in details:
            broken={**row,name:[0]};self.assertFalse(runner.validate_sample(broken,case),name)
        for broken in ({**row,'elapsed_ns':0},{**row,'valid':False},{**row,'ack_ok':case['count']-1},None,{}):
            self.assertFalse(runner.validate_sample(broken,case))

    def test_same_job_witness_rejects_stale_run_and_attempt(self):
        with tempfile.TemporaryDirectory(prefix='pending-witness-static-',dir=HERE) as temp:
            root=Path(temp);workspace=root/'workspace';workspace.mkdir()
            qualification=root/'candidate-qualification';qualification.mkdir()
            env={'GITHUB_ACTIONS':'true','GITHUB_WORKFLOW':'candidate qualification','GITHUB_JOB':'qualify',
                 'GITHUB_RUN_ID':'1245','GITHUB_RUN_ATTEMPT':'2','GITHUB_SHA':'a'*40,
                 'RUNNER_TEMP':str(root),'GITHUB_WORKSPACE':str(workspace)}
            context=runner.binding.same_job_context(env,qualification,workspace)
            witness=runner.binding.expected_witness(context,'b'*64,'c'*64,'d'*64,'e'*64,'1'*64,'2'*64)
            self.assertTrue(runner.binding.validate_witness(witness,witness.copy()))
            for field,value in [('github_run_id','1244'),('github_run_attempt','1'),('github_job','other'),
                                ('github_sha','f'*40),('contract_sha256','f'*64),('summary_sha256','f'*64),
                                ('binder_sha256','f'*64),('helper_manifest_sha256','f'*64),('helper_guard_sha256','f'*64),('qualification_path','/other')]:
                self.assertFalse(runner.binding.validate_witness({**witness,field:value},witness),field)
            for field,value in [('GITHUB_RUN_ID','0'),('GITHUB_RUN_ATTEMPT','0'),('GITHUB_JOB','other'),
                                ('GITHUB_ACTIONS','false'),('GITHUB_SHA','bad')]:
                with self.assertRaises(RuntimeError,msg=field):
                    runner.binding.same_job_context({**env,field:value},qualification,workspace)
            with self.assertRaises(RuntimeError):
                runner.binding.same_job_context(env,root/'other',workspace)

    def test_complete_gate_set_rejects_missing_duplicate_extra_and_failed(self):
        contract={'fixed_full_targets':['partitionline','mock_produce']}
        controls={'integrations':[{'target':'produce_cancel'}]}
        names=['format','clippy']+[lane+'-'+target for lane in ('default','all-features')
                                  for target in ('partitionline','mock_produce','produce_cancel')]
        gates=[{'name':name,'passed':True} for name in names]
        runner.binding.require_complete_gates({'gates':gates},contract,controls)
        invalid=(gates[:-1],gates+[gates[0]],gates+[{'name':'default-extra','passed':True}],
                 [{**row,'passed':False} if index==2 else row for index,row in enumerate(gates)],
                 [{**row,'name':'all-features-unknown'} if index==2 else row for index,row in enumerate(gates)])
        for rows in invalid:
            with self.assertRaises(RuntimeError):runner.binding.require_complete_gates({'gates':rows},contract,controls)

    def test_real_v17_complete_gate_set_rejects_each_missing_target(self):
        checkout_path=os.environ.get('PL_BENCH_CHECKOUT_PATH')
        if not checkout_path:self.skipTest('set PL_BENCH_CHECKOUT_PATH for the exact gate set')
        spec_path=Path(checkout_path)/'docs/evidence/client/candidate-20261030/v17/spec.json'
        spec=json.loads(spec_path.read_text())
        self.assertEqual(runner.digest(spec_path),runner.SPEC_SHA)
        controls=runner.binding.load_pinned_controls(spec_path,spec)
        targets=set(spec['fixed_full_targets']) | {'partitionline'} | {row['target'] for row in controls['integrations']}
        names=sorted({'format','clippy'} | {lane+'-'+target for lane in ('default','all-features') for target in targets})
        self.assertEqual(len(names),22)
        gates=[{'name':name,'passed':True} for name in names]
        runner.binding.require_complete_gates({'gates':gates},spec,controls)
        for index in range(len(gates)):
            with self.assertRaises(RuntimeError,msg=names[index]):
                runner.binding.require_complete_gates({'gates':gates[:index]+gates[index+1:]},spec,controls)
            duplicate=gates.copy();duplicate[index]=gates[(index+1)%len(gates)]
            with self.assertRaises(RuntimeError,msg=names[index]):
                runner.binding.require_complete_gates({'gates':duplicate},spec,controls)

    def test_pinned_control_asset_rejects_tampering_and_symlinks(self):
        checkout_path=os.environ.get('PL_BENCH_CHECKOUT_PATH')
        if not checkout_path:self.skipTest('set PL_BENCH_CHECKOUT_PATH for exact control asset')
        spec_path=Path(checkout_path)/'docs/evidence/client/candidate-20261030/v17/spec.json'
        spec=json.loads(spec_path.read_text())
        raw=(spec_path.parent/spec['controls']['path']).read_bytes()
        with tempfile.TemporaryDirectory(prefix='pending-controls-static-',dir=HERE) as temp:
            root=Path(temp);local_spec=root/'spec.json';local_spec.write_text('{}')
            local=root/spec['controls']['path'];local.write_bytes(raw)
            runner.binding.load_pinned_controls(local_spec,spec)
            local.write_bytes(bytes([raw[0]^1])+raw[1:])
            with self.assertRaises(RuntimeError):runner.binding.load_pinned_controls(local_spec,spec)
            local.unlink();local.symlink_to(spec_path.parent/spec['controls']['path'])
            with self.assertRaises(RuntimeError):runner.binding.load_pinned_controls(local_spec,spec)
            malformed=json.loads(json.dumps(spec));malformed['controls']['path']='../controls.json'
            with self.assertRaises(RuntimeError):runner.binding.load_pinned_controls(local_spec,malformed)

    def test_exact_v17_helper_guard_and_negative_controls(self):
        checkout_path=os.environ.get('PL_BENCH_CHECKOUT_PATH')
        if not checkout_path:self.skipTest('set PL_BENCH_CHECKOUT_PATH for the exact helper controls')
        checkout=Path(checkout_path)
        spec=json.loads((checkout/'docs/evidence/client/candidate-20261030/v17/spec.json').read_text())
        manifest=checkout/runner.binding.MANIFEST_PATH
        self.assertEqual(runner.digest(manifest),runner.binding.MANIFEST_SHA)
        names=tuple(spec['qualification_helper_overlay']['helper_pins'])
        self.assertEqual(len(names),6)
        with tempfile.TemporaryDirectory(prefix='pending-helper-static-',dir=HERE) as temp:
            root=Path(temp);target=root/runner.binding.MANIFEST_PATH
            target.parent.mkdir(parents=True);shutil.copyfile(manifest,target)
            for name in names:
                dest=root/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(checkout/name,dest)
            self.assertEqual(runner.binding.verify_v17_helpers(root,spec),
                             {'helper_manifest_sha256':runner.binding.MANIFEST_SHA,'helper_guard_sha256':runner.binding.GUARD_SHA})
            for name in names:
                path=root/name;old=path.read_bytes();path.write_bytes(bytes([old[0]^1])+old[1:])
                with self.assertRaises((ValueError,RuntimeError),msg=name):runner.binding.verify_v17_helpers(root,spec)
                path.write_bytes(old)
            guard=root/'scripts/ci-verify-client-candidate-helpers.py';old=guard.read_bytes()
            guard.write_bytes(b'#'+b' '* (len(old)-1))
            with self.assertRaises(RuntimeError):runner.binding.verify_v17_helpers(root,spec)
            guard.write_bytes(old)
            guard.unlink();guard.symlink_to(checkout/'scripts/ci-verify-client-candidate-helpers.py')
            with self.assertRaises(OSError):runner.binding.verify_v17_helpers(root,spec)
            guard.unlink();guard.write_bytes(old)
            helper=root/'tests/ci/test_candidate_command_ownership.py';old_helper=helper.read_bytes()
            helper.unlink();helper.symlink_to(checkout/'tests/ci/test_candidate_command_ownership.py')
            with self.assertRaises(ValueError):runner.binding.verify_v17_helpers(root,spec)
            helper.unlink();helper.write_bytes(old_helper)
            malformed=json.loads(json.dumps(spec));malformed['qualification_helper_overlay']['helper_pins'].pop(names[0])
            with self.assertRaises(RuntimeError):runner.binding.verify_v17_helpers(root,malformed)
            original=target.read_bytes();target.write_bytes(original.replace(b'"required_existing_root_files"',b'"required_existing_root_files_X"',1))
            with self.assertRaises(RuntimeError):runner.binding.verify_v17_helpers(root,spec)

    def test_v17_preserves_baselines_controls_and_targets(self):
        checkout_path=os.environ.get('PL_BENCH_CHECKOUT_PATH')
        if not checkout_path:self.skipTest('set PL_BENCH_CHECKOUT_PATH for qualification control comparison')
        root=Path(checkout_path)/'docs/evidence/client/candidate-20261030'
        previous=json.loads((root/'v14/spec.json').read_text())
        current=json.loads((root/'v17/spec.json').read_text())
        self.assertEqual(runner.digest(root/'v17/spec.json'),runner.SPEC_SHA)
        for name in ('baseline_cases','controls','fixed_full_targets'):
            self.assertEqual(previous[name],current[name],name)
        self.assertEqual(len(current['baseline_cases']),23)
        self.assertEqual(len(current['fixed_full_targets']),10)
        controls_path=root/'v17'/current['controls']['path']
        self.assertEqual(runner.digest(controls_path),current['controls']['sha256'])
        controls=json.loads(controls_path.read_text())
        self.assertEqual(len(controls['library']),166);self.assertEqual(len(controls['integrations']),17)

    def test_exact_patch_roundtrip_and_hook_parity(self):
        names=('PL_BENCH_PRODUCER_PATH','PL_BENCH_PREPARE_TESTS_PATH','PL_BENCH_CARGO_PATH','PL_BENCH_IMPORT_PATH','PL_BENCH_DEADLINE_PATH')
        if not all(os.environ.get(name) for name in names):
            self.skipTest('set all five PL_BENCH_* source paths for the source-roundtrip control')
        source,tests,cargo,fix,deadline=(Path(os.environ[name]) for name in names)
        self.assertEqual(runner.digest(source),runner.V16_PRODUCER)
        self.assertEqual(runner.digest(tests),runner.V16_TESTS)
        self.assertEqual(runner.digest(fix),runner.IMPORT_SHA)
        self.assertEqual(runner.digest(deadline),runner.DEADLINE_SHA)
        self.assertEqual(runner.digest(HERE/'queue-v2.patch.gz'),runner.QUEUE_GZ_SHA)
        with tempfile.TemporaryDirectory(prefix='pending-bench-static-',dir=HERE) as temp:
            root=Path(temp);control=root/'control';candidate=root/'candidate'
            for arm in (control,candidate):
                (arm/'src/producer').mkdir(parents=True);(arm/'examples').mkdir()
                shutil.copy2(source,arm/'src/producer.rs');shutil.copy2(tests,arm/'src/producer/prepare_tests.rs');shutil.copy2(cargo,arm/'Cargo.toml')
            patch=root/'queue.patch';patch.write_bytes(gzip.decompress((HERE/'queue-v2.patch.gz').read_bytes()))
            runner.checked_patch(control,deadline,True);self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V14_PRODUCER)
            runner.checked_patch(control,fix,True);self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V13_PRODUCER)
            runner.checked_patch(control,patch,True)
            self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V10_PRODUCER)
            self.assertEqual(runner.digest(control/'src/producer/prepare_tests.rs'),runner.V10_TESTS)
            runner.checked_patch(control,patch,False);runner.checked_patch(control,fix,False);runner.checked_patch(control,deadline,False)
            self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V16_PRODUCER)
            self.assertEqual(runner.digest(control/'src/producer/prepare_tests.rs'),runner.V16_TESTS)
            runner.checked_patch(control,deadline,True);self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V14_PRODUCER)
            runner.checked_patch(control,fix,True);self.assertEqual(runner.digest(control/'src/producer.rs'),runner.V13_PRODUCER)
            runner.checked_patch(control,patch,True)
            a=runner.inject(control);b=runner.inject(candidate)
            self.assertEqual(a['modified_cargo_sha256'],b['modified_cargo_sha256'])
            extra=('        pending_deadlines_ordered: true,\n'
                   '        pending_stats_valid: true,\n'
                   '        #[cfg(test)]\n'
                   '        pending_work: Arc::new(PendingWork::default()),\n')
            self.assertEqual(b['constructor'].replace(extra,'',1),a['constructor'])
            for adapter in (a,b):
                self.assertIn('WorkerLease::register(&shared)',adapter['constructor'])
                self.assertNotIn('WorkerLease::register(shared)',adapter['constructor'])
            common=(HERE/'pending_queue_bench.rs.in').read_text()
            for name,arm,adapter in (('v10-queue',control,a),('v16',candidate,b)):
                frozen=json.loads((HERE/(name+'-hook-inputs.json')).read_text())
                self.assertEqual(len(frozen),1757)
                for path in ('src/producer.rs','src/producer/prepare_tests.rs','Cargo.toml','src/producer/pending_queue_bench.rs','examples/pending_queue_bench.rs'):
                    self.assertEqual(frozen[path],runner.digest(arm/path),path)
                self.assertEqual((HERE/(name+'-worker-initializer.rs.txt')).read_text(),adapter['constructor'])
                generated=(arm/'src/producer/pending_queue_bench.rs').read_text()
                self.assertEqual(generated.replace(adapter['constructor'],'__WORKER_INITIALIZER__',1),common)
                self.assertIn('#[cfg(any(test, feature = "pending-queue-bench"))]\nfn test_shared(', (arm/'src/producer.rs').read_text())
            self.assertEqual((control/'examples/pending_queue_bench.rs').read_bytes(),(candidate/'examples/pending_queue_bench.rs').read_bytes())

if __name__=='__main__':unittest.main()

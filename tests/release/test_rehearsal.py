"""Executable recovery scenarios and incomplete-report rejection; entirely offline."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('release_rehearsal', ROOT/'scripts/report-release-rehearsal.py')
rehearsal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearsal)


class ReleaseRehearsal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='pl-test-release-')
        cls.result = rehearsal.isolated_cases(Path(cls.temporary.name)/'complete')

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_complete_recovery_has_all_cases_and_no_release_claim(self):
        self.assertEqual(len(self.result['scenarios']), 25)
        self.assertEqual(self.result['candidate_version'], rehearsal.candidate_version(ROOT))
        self.assertFalse(self.result['release_complete'])
        self.assertTrue(self.result['validation_success'])
        self.assertEqual(len(self.result['owner_actions']), 6)
        self.assertTrue(all(row['status']=='not_run' for row in self.result['owner_actions']))

    def test_unknown_registry_and_pending_sparse_index_both_stop(self):
        rows={row['name']:row for row in self.result['scenarios']}
        for name in ['workflow-registry-unavailable','local-registry-unavailable',
                     'workflow-index-pending','local-index-pending',
                     'workflow-index-unavailable','local-index-unavailable',
                     'workflow-api-unavailable','local-api-unavailable',
                     'workflow-malformed-index','local-malformed-index',
                     'workflow-yanked-version','local-yanked-version']:
            with self.subTest(name=name):
                self.assertEqual(rows[name]['exit_code'], 1)
                self.assertIn('registry unavailable',rows[name]['stderr'])
        self.assertFalse(any(command['command']=='cargo' for command in self.result['commands']))

    def test_confirmation_resume_and_repeated_notes_use_actual_steps(self):
        rows={row['name']:row for row in self.result['scenarios']}
        self.assertEqual(rows['confirmation-interrupted']['exit_code'],130)
        self.assertEqual(rows['confirmation-resumed']['exit_code'],0)
        self.assertEqual(rows['confirmation-interrupted']['artifact_directory'],
                         rows['confirmation-resumed']['artifact_directory'])
        self.assertIn('already exists', rows['release-notes-repeated']['stdout'])
        creates=[c for c in self.result['commands'] if c['command']=='gh' and c['args'][:2]==['release','create']]
        self.assertEqual(len(creates),1)

    def test_incomplete_or_invented_report_cannot_pass(self):
        major,minor,patch=self.result['candidate_version'].split('.')
        other_version=f'{major}.{minor}.{int(patch)+1}'
        edits=[lambda r:r['scenarios'].pop(),
               lambda r:r['scenarios'].append(copy.deepcopy(r['scenarios'][0])),
               lambda r:r['scenarios'][0].update(exit_code=False),
               lambda r:r['scenarios'][0].update(status='not_run'),
               lambda r:r.update(release_complete=True),
               lambda r:r.update(validation_success=False),
               lambda r:r.update(candidate_source_sha='main'),
               lambda r:r.update(candidate_version='0.2.0-rc.1'),
               lambda r:r.update(candidate_version=other_version),
               lambda r:r.update(candidate_manifest=r['candidate_manifest']+'\n# mutated\n'),
               lambda r:r['source_file_sha256'].pop('Cargo.toml'),
               lambda r:r['owner_actions'][0].update(status='passed'),
               lambda r:r['commands'].append({'command':'cargo','args':['publish']}),
               lambda r:r['commands'].append({'command':'git','args':['tag','v0.1.0']}),
               lambda r:r['commands'].append({'command':'gh','args':['secret','set','token']})]
        for edit in edits:
            with self.subTest(edit=edit):
                result=copy.deepcopy(self.result)
                edit(result)
                with self.assertRaises(ValueError): rehearsal.validate(result)

    def test_report_mutation_is_rejected_when_manifest_is_0_2_0(self):
        # Run the real test under a changed outer manifest, not only an inner
        # simulated release. Select one method so the child does not recurse.
        with tempfile.TemporaryDirectory(prefix='pl-rehearsal-outer-version-') as temporary:
            root=Path(temporary)/'source'
            for name in set(rehearsal.SOURCE_FILES) | {'tests/release/test_rehearsal.py'}:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,path)
            manifest=root/'Cargo.toml'
            text=manifest.read_text()
            old='version = "'+rehearsal.candidate_version(ROOT)+'"'
            self.assertEqual(text.count(old),1)
            manifest.write_text(text.replace(old,'version = "0.2.0"',1))
            env={'PATH':os.environ.get('PATH','/usr/bin:/bin'), 'LANG':'C.UTF-8',
                 'PYTHONDONTWRITEBYTECODE':'1'}
            result=subprocess.run([sys.executable,'-B',str(root/'tests/release/test_rehearsal.py'),
                                   'ReleaseRehearsal.test_incomplete_or_invented_report_cannot_pass'],
                                  cwd=root,env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stdout+'\n'+result.stderr)

    def test_next_version_runs_actual_shell_and_offline_recovery(self):
        with tempfile.TemporaryDirectory(prefix='pl-next-release-') as temporary:
            root=Path(temporary)/'source'
            required=set(rehearsal.SOURCE_FILES) | {
                'scripts/owner-cut-release.sh', 'scripts/ci-publish-ready.sh',
                'scripts/day1-after-publish.sh', 'scripts/owner-post-installable-handoff.sh',
            }
            for name in required:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,path)
            manifest=root/'Cargo.toml'
            text=manifest.read_text()
            old='version = "'+rehearsal.candidate_version(ROOT)+'"'
            self.assertEqual(text.count(old), 1)
            manifest.write_text(text.replace(old, 'version = "0.2.0"', 1))

            # The actual --run path insists on a clean commit and unchanged tags.
            env={'PATH':os.environ.get('PATH', '/usr/bin:/bin'), 'LANG':'C.UTF-8',
                 'GIT_CONFIG_NOSYSTEM':'1', 'GIT_CONFIG_GLOBAL':'/dev/null',
                 'GIT_AUTHOR_NAME':'Rehearsal', 'GIT_AUTHOR_EMAIL':'rehearsal@example.invalid',
                 'GIT_COMMITTER_NAME':'Rehearsal', 'GIT_COMMITTER_EMAIL':'rehearsal@example.invalid',
                 'PYTHONDONTWRITEBYTECODE':'1'}
            for command in [['git','init','-q'],['git','add','.'],['git','commit','-qm','fixture']]:
                subprocess.run(command,cwd=root,env=env,check=True,capture_output=True,text=True)
            sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,env=env,text=True).strip()
            artifact=Path(temporary)/'artifacts'
            completed=subprocess.run(['bash','scripts/rehearse-partial-release.sh','--run',str(artifact)],
                                     cwd=root,env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(completed.returncode,0,completed.stdout+'\n'+completed.stderr)
            reports=list(artifact.glob('*/report.json'))
            self.assertEqual(len(reports),1)
            result=json.loads(reports[0].read_text())
            self.assertEqual(result['candidate_version'],'0.2.0')
            self.assertEqual(result['candidate_source_sha'],sha)
            self.assertTrue(result['actual_tag_refs_unchanged'])
            rows={row['name']:row for row in result['scenarios']}
            self.assertEqual(rows['workflow-existing-version']['exit_code'],0)
            self.assertEqual(rows['workflow-absent-version']['exit_code'],0)
            self.assertEqual(rows['workflow-index-only']['exit_code'],0)
            self.assertEqual(rows['local-index-only']['exit_code'],2)
            self.assertEqual(rows['workflow-yanked-version']['exit_code'],1)
            self.assertEqual(rows['local-yanked-version']['exit_code'],1)
            self.assertIn('crates.io has partitionline 0.2.0',rows['confirmation-resumed']['stdout'])
            self.assertIn('created GitHub release v0.2.0',rows['release-notes-first']['stdout'])
            self.assertIn('already exists',rows['release-notes-repeated']['stdout'])
            self.assertFalse(any(c['command']=='cargo' for c in result['commands']))

    def test_candidate_version_rejects_nonfinal_and_ambiguous_manifests(self):
        with tempfile.TemporaryDirectory(prefix='pl-invalid-version-') as temporary:
            root=Path(temporary)
            manifest=root/'Cargo.toml'
            for version in ['0.2.0-rc.1', '1.02.0', '1.2', '', '0.2.0+build']:
                with self.subTest(version=version):
                    manifest.write_text('[package]\nname = "partitionline"\nversion = "'+version+'"\n')
                    with self.assertRaises(ValueError): rehearsal.candidate_version(root)
            manifest.write_text('[package]\nname = "partitionline"\nversion="0.2.0"\n')
            with self.assertRaisesRegex(ValueError, 'release shell'): rehearsal.candidate_version(root)

    def test_removing_registry_unknown_guard_fails_executable_rehearsal(self):
        with tempfile.TemporaryDirectory(prefix='pl-mutated-release-') as temporary:
            root=Path(temporary)/'source'
            for name in rehearsal.SOURCE_FILES:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,path)
            path=root/'scripts/owner-publish.sh'
            text=path.read_text()
            start=text.index('if [[ "${PL_CRATES_PROBE_STATUS}" != "present" && "${PL_CRATES_PROBE_STATUS}" != "absent" ]]; then')
            end=text.index('\nfi\n',start)+4
            path.write_text(text[:start]+text[end:])
            artifact=Path(temporary)/'report'
            with self.assertRaisesRegex(ValueError,'local-registry-unavailable'):
                rehearsal.isolated_cases(artifact,source_root=root)
            report=json.loads((artifact/'scenario-report.json').read_text())
            self.assertFalse(report['validation_success'])
            self.assertTrue((artifact/'cases/local-registry-unavailable/local-registry-unavailable.stderr.log').is_file())

    def test_unmodeled_workflow_expression_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='pl-expression-release-') as temporary:
            root=Path(temporary)/'source'
            for name in rehearsal.SOURCE_FILES:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,path)
            path=root/'.github/workflows/release.yml'
            path.write_text(path.read_text().replace('tag="${REF_NAME#v}"','tag="${{ secrets.NOT_ALLOWED }}"'))
            with self.assertRaisesRegex(ValueError,'unmodeled workflow expression'):
                rehearsal.isolated_cases(Path(temporary)/'report',source_root=root)


if __name__=='__main__':
    unittest.main()

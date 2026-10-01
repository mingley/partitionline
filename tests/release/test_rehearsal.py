"""Executable recovery scenarios and incomplete-report rejection; entirely offline."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
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

    def test_complete_recovery_has_fifteen_cases_and_no_release_claim(self):
        self.assertEqual(len(self.result['scenarios']), 15)
        self.assertFalse(self.result['release_complete'])
        self.assertTrue(self.result['validation_success'])
        self.assertEqual(len(self.result['owner_actions']), 6)
        self.assertTrue(all(row['status']=='not_run' for row in self.result['owner_actions']))

    def test_unknown_registry_and_pending_sparse_index_both_stop(self):
        rows={row['name']:row for row in self.result['scenarios']}
        for name in ['workflow-registry-unavailable','local-registry-unavailable',
                     'workflow-index-pending','local-index-pending']:
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
        edits=[lambda r:r['scenarios'].pop(),
               lambda r:r['scenarios'].append(copy.deepcopy(r['scenarios'][0])),
               lambda r:r['scenarios'][0].update(exit_code=False),
               lambda r:r['scenarios'][0].update(status='not_run'),
               lambda r:r.update(release_complete=True),
               lambda r:r.update(validation_success=False),
               lambda r:r.update(candidate_source_sha='main'),
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

    def test_removing_registry_unknown_guard_fails_executable_rehearsal(self):
        with tempfile.TemporaryDirectory(prefix='pl-mutated-release-') as temporary:
            root=Path(temporary)/'source'
            for name in rehearsal.SOURCE_FILES:
                path=root/name
                path.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,path)
            path=root/'scripts/owner-publish.sh'
            text=path.read_text()
            start=text.index('if [[ "${PL_CRATES_PROBE_STATUS}" == "unknown" ]]; then')
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

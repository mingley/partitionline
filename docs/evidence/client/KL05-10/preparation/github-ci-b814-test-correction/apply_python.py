"""Apply the narrow registry-audited negative-test correction after failed-first."""
from pathlib import Path

root = Path(__file__).resolve().parent
target = root / 'candidate/tests/conformance/test_verifiable_scenario.py'
text = target.read_text()
old = """            with self.assertRaisesRegex(primary.ConformanceValidationError, 'Missing 105 required'):
                primary.validate_and_aggregate_reports(registry, [('single live scenario', result)],
                    require_independent_pass=True, repo_root=ROOT)
            self.assertEqual(sum(case['denominator'] for case in registry['cases']), 86)
"""
new = """            # The report requires every declared row, including rows excluded
            # from the qualification denominator. Audit both independent lists.
            contract = registry['coverage_contract']
            required_ids = contract['required_case_ids']
            denominator_ids = contract['denominator_case_ids']
            self.assertEqual(len(required_ids), len(set(required_ids)))
            self.assertEqual(len(denominator_ids), len(set(denominator_ids)))
            self.assertCountEqual([case['id'] for case in registry['cases']], required_ids)
            self.assertCountEqual([case['id'] for case in registry['cases'] if case['denominator']],
                                  denominator_ids)
            reported_ids = {case['id'] for case in result['cases']}
            self.assertEqual(reported_ids, {PROFILE['case_id']})
            missing_ids = sorted(set(required_ids) - reported_ids)
            self.assertTrue(missing_ids)
            sample = ', '.join(missing_ids[:5])
            suffix = (f' (showing 5 of {len(missing_ids)}: {sample})'
                      if len(missing_ids) > 5 else f': {sample}')
            expected = f'Missing {len(missing_ids)} required conformance case(s){suffix}'
            with self.assertRaises(primary.ConformanceValidationError) as rejected:
                primary.validate_and_aggregate_reports(registry, [('single live scenario', result)],
                    require_independent_pass=True, repo_root=ROOT)
            self.assertEqual(str(rejected.exception), expected)
            self.assertEqual(sum(case['denominator'] for case in registry['cases']), len(denominator_ids))
"""
assert text.count(old) == 1
target.write_text(text.replace(old, new))
target.chmod(0o600)

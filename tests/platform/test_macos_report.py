"""Synthetic negative report tests; these fixtures do not qualify a platform."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    'macos_report', Path(__file__).resolve().parents[2] / 'scripts/report-macos-arm64.py')
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def versions():
    return {'system': 'Darwin', 'machine': 'arm64', 'rustc_host': 'aarch64-apple-darwin',
            'requested_toolchain': '1.85.0', 'rustc_release': '1.85.0',
            'source_sha': '0' * 40, 'openssl_version': 'OpenSSL 3.5.0 fixture'}


def package():
    vcs = {'git': {'sha1': '0' * 40}, 'path_in_vcs': ''}
    return {'source_vcs_info': vcs, 'package_sha256': '1' * 64,
            'feature_matrix': [
                {'feature_mode': mode, 'dependency_features': features,
                 'package_vcs_info': copy.deepcopy(vcs), 'package_sha256': '1' * 64,
                 'toolchain': '1.85.0', 'rustc': 'rustc 1.85.0 (synthetic fixture)',
                 'compiled_snippets': 1, 'snippets': [{}], 'ignored_snippets': []}
                for mode, features in [('default', []), ('tracing', ['tracing'])]],
            'operator_lock_sha256': {f'operator-1.85.0-{mode}.lock': '2' * 64
                                     for mode in ('default', 'tracing')}}


def runtime():
    return '\n'.join('test '+test+' ... ok' for test in report.REQUIRED_TESTS) + (
        '\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n')


class ReportTest(unittest.TestCase):
    def test_valid_fixture_is_structurally_complete(self):
        report.validate_versions(versions())
        self.assertEqual(report.test_counts(runtime())['passed'], 4)
        self.assertEqual(report.validate_package(package(), versions()), 2)

    def test_wrong_native_platform_or_compiler_rejected(self):
        for field, value in [('system', 'Linux'), ('machine', 'x86_64'),
                             ('rustc_host', 'x86_64-apple-darwin'), ('rustc_release', '1.86.0'),
                             ('source_sha', ''), ('openssl_version', 'LibreSSL 3.3.6')]:
            with self.subTest(field=field):
                data = versions(); data[field] = value
                with self.assertRaises(ValueError): report.validate_versions(data)

    def test_failed_filtered_or_missing_results_rejected(self):
        for output in ['', 'running 4 tests', runtime().replace('0 failed', '1 failed'),
                       runtime().replace('0 filtered', '1 filtered'),
                       runtime().replace('result: ok', 'result: FAILED')]:
            with self.subTest(output=output):
                with self.assertRaises(ValueError): report.test_counts(output)

    def test_required_tls_cases_cannot_be_skipped(self):
        for test in report.REQUIRED_TESTS:
            output = runtime().replace('test '+test+' ... ok', 'test '+test+' ... ignored')
            with self.assertRaises(ValueError): report.test_counts(output)

    def test_stale_or_dirty_archive_rejected(self):
        for field, value in [('sha1', '3' * 40), ('dirty', True)]:
            data = package(); data['source_vcs_info']['git'][field] = value
            with self.assertRaises(ValueError): report.validate_package(data, versions())

    def test_incomplete_or_unsupported_feature_matrix_rejected(self):
        for mode in ['missing', 'duplicate', 'unsupported']:
            data = package()
            if mode == 'missing': data['feature_matrix'].pop()
            elif mode == 'duplicate': data['feature_matrix'][1]['feature_mode'] = 'default'
            else: data['feature_matrix'][1]['dependency_features'] = ['new_feature']
            with self.assertRaises(ValueError): report.validate_package(data, versions())

    def test_cell_revision_package_or_compiler_mismatch_rejected(self):
        for field, value in [('package_sha256', '3' * 64), ('toolchain', 'stable'),
                             ('rustc', 'rustc 1.86.0 (fixture)'),
                             ('package_vcs_info', {'git': {'sha1': '3' * 40}})]:
            data = package(); data['feature_matrix'][0][field] = value
            with self.assertRaises(ValueError): report.validate_package(data, versions())

    def test_empty_or_skipped_snippets_rejected(self):
        for field, value in [('compiled_snippets', 0), ('compiled_snippets', 2),
                             ('ignored_snippets', [{'reason': 'skip'}])]:
            data = package(); data['feature_matrix'][0][field] = value
            with self.assertRaises(ValueError): report.validate_package(data, versions())

    def test_operator_lock_manifest_must_match_both_feature_cells(self):
        for locks in [{}, {'operator-stable-default.lock': '2' * 64,
                          'operator-stable-tracing.lock': '2' * 64}]:
            data = package(); data['operator_lock_sha256'] = locks
            with self.assertRaises(ValueError): report.validate_package(data, versions())


if __name__ == '__main__':
    unittest.main()

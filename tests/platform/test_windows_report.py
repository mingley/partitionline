"""Synthetic fail-closed fixtures; these tests do not qualify Windows."""
import importlib.util
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    'windows_report', Path(__file__).resolve().parents[2] / 'scripts/report-windows.py')
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def versions():
    return {'system': 'Windows', 'machine': 'AMD64', 'pointer_bits': 64,
            'rustc_host': 'x86_64-pc-windows-msvc', 'requested_toolchain': '1.85.0',
            'rustc_release': '1.85.0', 'python_version': '3.13.1',
            'bash_version': '5.2.37(1)-release', 'openssl_version': 'OpenSSL 3.5.0 fixture',
            'openssl_executable': r'C:\fixture\openssl.exe',
            'windows_version': '10.0.fixture', 'source_sha': '0' * 40}


class WindowsReportTest(unittest.TestCase):
    def test_valid_native_fixture_and_artifact_identity(self):
        data = versions()
        REPORT.validate_versions(data)
        name = REPORT.artifact_name(data, 'tests4-4-snips2')
        self.assertIn('rust1.85.0-os10.0.fixture-ssl3.5.0-py3.13.1-bash5.2.37', name)
        self.assertIn('tests4-4-snips2', name)

    def test_cross_build_emulation_wrong_arch_or_compiler_rejected(self):
        for field, value in [('system', 'Linux'), ('machine', 'ARM64'),
                             ('pointer_bits', 32), ('rustc_host', 'x86_64-pc-windows-gnu'),
                             ('rustc_release', '1.86.0'), ('requested_toolchain', 'nightly')]:
            with self.subTest(field=field):
                data = versions(); data[field] = value
                with self.assertRaises(ValueError): REPORT.validate_versions(data)

    def test_missing_prerequisite_or_identity_is_not_a_green_skip(self):
        for field, value in [('python_version', '3.9.1'), ('bash_version', '3.2.1'),
                             ('openssl_version', 'LibreSSL 3.3.6'), ('source_sha', ''),
                             ('windows_version', ''), ('openssl_executable', '')]:
            with self.subTest(field=field):
                data = versions(); data[field] = value
                with self.assertRaises(ValueError): REPORT.validate_versions(data)

    def test_shared_runtime_parser_requires_all_public_tls_paths(self):
        output = '\n'.join('test '+name+' ... ok' for name in REPORT.BASE.REQUIRED_TESTS)
        output += '\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n'
        self.assertEqual(REPORT.BASE.test_counts(output)['passed'], 4)
        for name in REPORT.BASE.REQUIRED_TESTS:
            with self.assertRaises(ValueError):
                REPORT.BASE.test_counts(output.replace('test '+name+' ... ok', 'test '+name+' ... ignored'))


if __name__ == '__main__':
    unittest.main()

"""Packed navigation/exclusion failures, independent of checkout resolution."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/check-package-docs.py'
SPEC = importlib.util.spec_from_file_location('package_docs', SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class PackageDocsTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name) / 'package'
        self.root.mkdir()
        (self.root / 'Cargo.toml').write_text('[package]\nname="partitionline"\nversion="0.1.0"\nlicense="MIT OR Apache-2.0"\n')
        for name in ('LICENSE-MIT', 'LICENSE-APACHE', 'NOTICE'):
            (self.root / name).write_text('public fixture license/notice\n')
        (self.root / 'docs').mkdir()
        (self.root / 'docs/index.md').write_text('# Navigation\n')
        (self.root / 'README.md').write_text('[map](docs/index.md#navigation)\n')

    def test_actual_packaged_file_and_anchor_resolve(self):
        report = CHECK.check(self.root)
        self.assertEqual(report['local_links'], 1)
        self.assertEqual(len(report['license_notice_sha256']), 3)

    def test_checkout_file_does_not_satisfy_missing_package_link(self):
        checkout = Path(self.work.name) / 'checkout'
        checkout.mkdir()
        (checkout / 'missing.md').write_text('# Exists only in checkout\n')
        (self.root / 'README.md').write_text('[missing](missing.md)\n')
        with self.assertRaisesRegex(CHECK.DOC.CheckError, 'missing local file'):
            CHECK.check(self.root)

    def test_missing_package_anchor_rejected(self):
        (self.root / 'README.md').write_text('[missing](docs/index.md#missing)\n')
        with self.assertRaisesRegex(CHECK.DOC.CheckError, 'missing local anchor'):
            CHECK.check(self.root)

    def test_immutable_excluded_repository_target_allowed(self):
        target = 'https://github.com/mingley/partitionline/blob/' + 'a' * 40 + '/docs/plan/tasks.json'
        with (self.root / 'README.md').open('a') as out:
            out.write(f'[plan]({target})\n')
        self.assertEqual(CHECK.check(self.root)['versioned_repository_links'], [target])

    def test_mutable_repository_target_rejected(self):
        for target in ('https://github.com/mingley/partitionline/blob/main/docs/plan/tasks.json',
                       'https://raw.githubusercontent.com/mingley/partitionline/main/docs/plan/tasks.json'):
            with self.subTest(target=target):
                (self.root / 'README.md').write_text(f'[plan]({target})\n')
                with self.assertRaisesRegex(CHECK.DOC.CheckError, 'require'):
                    CHECK.check(self.root)

    def test_peer_raw_evidence_and_private_material_excluded(self):
        for name in ('benchmarks/peers/java/README.md', 'docs/evidence/raw.json', 'docs/plan/tasks.json', '.aws/config', '.env'):
            with self.subTest(name=name):
                path = self.root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('synthetic negative fixture\n')
                with self.assertRaisesRegex(CHECK.DOC.CheckError, 'excluded'):
                    CHECK.check(self.root)
                path.unlink()

    def test_notice_inventory_cannot_be_empty(self):
        (self.root / 'NOTICE').write_text('')
        with self.assertRaisesRegex(CHECK.DOC.CheckError, 'empty required license/notice'):
            CHECK.check(self.root)


if __name__ == '__main__':
    unittest.main()

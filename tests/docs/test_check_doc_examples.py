"""Behavioral failures for the Markdown gate; no copied public prose."""
import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/check-doc-examples.py'
SPEC = importlib.util.spec_from_file_location('doc_examples', SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class DocExamplesTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        self.doc = self.root / 'doc.md'

    def write(self, content):
        self.doc.write_text(content)
        return self.doc

    def test_extracts_actual_body_and_new_fences_without_copied_registry(self):
        body = '# let value = 3;\nassert_eq!(value, 3);\n'
        self.write('Intro\n```rust,no_run\n' + body + '```\n')
        selected, ignored = CHECK.snippets(self.doc)
        self.assertEqual(selected[0]['body'], body)
        self.assertEqual(selected[0]['line'], 2)
        self.assertEqual(ignored, [])
        self.doc.write_text(self.doc.read_text() + '\n~~~rust\nlet next = 4;\n~~~\n')
        self.assertEqual(len(CHECK.snippets(self.doc)[0]), 2)

    def test_unclosed_fence_fails(self):
        self.write('```rust\nlet never_closed = 1;\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'unclosed'):
            CHECK.snippets(self.doc)

    def test_unknown_flags_fail_instead_of_silently_skipping(self):
        self.write('```rust,compile_fail\nmissing();\n```\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'unsupported'):
            CHECK.snippets(self.doc)

    def test_ignore_requires_reason_and_is_reported_separately(self):
        self.write('```rust,ignore\nmissing();\n```\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'needs doc-example-ignore reason'):
            CHECK.snippets(self.doc)
        self.write('<!-- doc-example-ignore: schematic operator callback -->\n```rust,ignore\nmissing();\n```\n')
        selected, ignored = CHECK.snippets(self.doc)
        self.assertEqual(selected, [])
        self.assertEqual(ignored[0]['reason'], 'schematic operator callback')

    def compile_document(self, body):
        self.write('```rust,no_run\n' + body + '\n```\n')
        source = self.root / 'snippet.rs'
        source.write_text(CHECK.rustdoc_source([('doc.md', CHECK.snippets(self.doc)[0][0])]))
        self.assertIsNotNone(shutil.which('rustdoc'), 'Rust is required to prove snippet compilation')
        return subprocess.run(['rustdoc', '--test', '--edition=2021', str(source)], cwd=self.root,
                              capture_output=True, text=True)

    def test_actual_broken_rust_fence_fails_real_compiler(self):
        result = self.compile_document('let value: NonexistentType = 3;')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('NonexistentType', result.stdout + result.stderr)

    def test_hidden_setup_compiles_and_no_run_does_not_execute(self):
        result = self.compile_document('# fn setup() -> i32 { 3 }\nassert_eq!(setup(), 3);\npanic!("must never execute");')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_local_file_fails(self):
        self.write('[missing](absent.md)\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'missing local file'):
            CHECK.check_links(self.root, [self.doc])

    def test_missing_anchor_fails_even_when_file_exists(self):
        self.write('# Present\n[missing](#absent)\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'missing local anchor'):
            CHECK.check_links(self.root, [self.doc])

    def test_links_handle_unicode_duplicates_references_and_angle_paths(self):
        target = self.root / 'other doc.md'
        target.write_text('# Café — Status!\n# Same\n# Same\n<a id="explicit"></a>\n')
        self.write('[one](<other%20doc.md#café--status>)\n[two](<other%20doc.md#same-1>)\n[three][ref]\n[ref]: <other%20doc.md#explicit>\n')
        self.assertEqual(CHECK.check_links(self.root, [self.doc]), 3)

    def test_literal_links_in_code_are_not_navigation(self):
        self.write('```text\n[missing](absent.md)\n```\n`[missing](absent.md)`\n[external](https://example.com/#absent)\n')
        self.assertEqual(CHECK.check_links(self.root, [self.doc]), 0)

    def test_local_navigation_cannot_escape_repository(self):
        self.write('[outside](../outside.md)\n')
        with self.assertRaisesRegex(CHECK.CheckError, 'missing local file'):
            CHECK.check_links(self.root, [self.doc])


if __name__ == '__main__':
    unittest.main()

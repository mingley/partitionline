"""Source and host/path guard checks. This does not build or run Rust."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('queue_capsule', HERE/'qualify.py')
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)


class GuardControls(unittest.TestCase):
    def test_host_and_output_guards(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            temp, workspace = root/'runner-temp', root/'workspace'
            temp.mkdir()
            workspace.mkdir()
            output = temp/'controller-queue-qualification'
            env = {'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'github-hosted',
                   'RUNNER_TEMP': str(temp), 'GITHUB_WORKSPACE': str(workspace)}
            free = queue.FLOOR + 129 * 1024**2
            check = lambda out, values, **kw: queue.validated_output(
                out, values, system=kw.get('system', 'linux'), machine=kw.get('machine', 'x86_64'),
                free=kw.get('free', free))
            self.assertEqual(check(output, env), output)
            for changed in ({'GITHUB_ACTIONS': 'false'}, {'RUNNER_ENVIRONMENT': 'self-hosted'},
                            {'RUNNER_TEMP': str(workspace)}, {'GITHUB_WORKSPACE': str(temp)}):
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    check(output, env | changed)
            for options in ({'system': 'darwin'}, {'machine': 'aarch64'}, {'free': queue.FLOOR}):
                with self.subTest(options=options), self.assertRaises(ValueError):
                    check(output, env, **options)
            with self.assertRaises(ValueError):
                check(root/'elsewhere', env)
            symlink = root/'linked-temp'
            symlink.symlink_to(temp, target_is_directory=True)
            with self.assertRaises(ValueError):
                check(symlink/'controller-queue-qualification', env | {'RUNNER_TEMP': str(symlink)})
            target = temp/'controller-queue-qualification-native-target'
            target.mkdir()
            with self.assertRaises(ValueError):
                check(output, env)
            target.rmdir()
            target.symlink_to(root/'missing')
            with self.assertRaises(ValueError):
                check(output, env)
            target.unlink()
            output.symlink_to(root/'missing')
            with self.assertRaises(ValueError):
                check(output, env)

    def test_assets_and_patch_replay(self):
        self.assertEqual(queue.sha(HERE/'spec.json'), queue.SPEC_SHA)
        pinned = json.loads((HERE/'spec.json').read_text())
        for name, expected in pinned['assets'].items():
            path = HERE/name
            self.assertEqual(path.stat().st_size, expected['bytes'], name)
            self.assertEqual(queue.sha(path), expected['sha256'], name)
        legacy_spec = importlib.util.spec_from_file_location('queue_legacy', HERE/'legacy-driver.py')
        legacy = importlib.util.module_from_spec(legacy_spec)
        legacy_spec.loader.exec_module(legacy)
        base = legacy.load(HERE/'base-files.json')
        prefetch = legacy.load(HERE/'prefetch-manifest.json')
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)/'source'
            source.mkdir()
            legacy.extract_source(HERE/'base-source.tar.gz', source, base)
            legacy.verify_source(source, base)
            for label, patch, pins_name in [('baseline', 'controls-only.patch', 'controls-manifest.json'),
                                            ('candidate', 'candidate.patch', 'manifest.json')]:
                pins = legacy.load(HERE/pins_name)
                for name in (queue.PROTOCOL, queue.TRANSPORT, queue.TEST):
                    self.assertEqual(base[name]['sha256'], pins['files'][name]['before_sha256'])
                patches = [HERE/patch, HERE/(label+'-registry.patch'), HERE/'prefetch-control.patch']
                for item in patches:
                    queue.run_git(source, item)
                for name in (queue.PROTOCOL, queue.TEST):
                    self.assertEqual(queue.sha(source/name), pins['files'][name]['after_sha256'])
                self.assertEqual(queue.sha(source/queue.TRANSPORT), prefetch['after_sha256'])
                self.assertEqual(queue.sha(source/queue.REGISTRY), (HERE/(label+'-registry.sha256')).read_text().strip())
                registry = json.loads((source/queue.REGISTRY).read_text())
                self.assertEqual(registry['controller_source_sha256'], queue.sha(source/queue.PROTOCOL))
                self.assertEqual(registry['controller_test_source_sha256'], queue.sha(source/queue.TEST))
                for item in reversed(patches):
                    queue.run_git(source, item, reverse=True)
                for name in (queue.PROTOCOL, queue.TRANSPORT, queue.TEST, queue.REGISTRY):
                    (source/name).chmod(base[name]['mode'])
                legacy.verify_source(source, base)


if __name__ == '__main__':
    unittest.main(verbosity=2)

"""Offline tests for token availability and release caller return codes."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = [
    'check-registry-token.sh', 'lib/cargo-registry-token.sh',
    'check-installable-preflight.sh', 'owner-finish-installable.sh',
    'check-merge-ready.sh', 'check-cut-path.sh', 'owner-cut-release.sh',
    'owner-status.sh', 'audit-civilization-bars.sh',
    'owner-request-registry-token.sh', 'owner-unblock.sh',
    'lib/cursor-env-secrets-url.sh',
]
SECRET = 'offline-fixture-secret'


class TokenAvailability(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='pl-token-offline-')
        self.root = Path(self.temporary.name)
        for name in SCRIPTS:
            dest = self.root/'scripts'/name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT/'scripts'/name, dest)
        self.bin = self.root/'bin'
        self.bin.mkdir()
        self.calls = self.root/'forbidden-calls'
        for name in ['curl', 'cargo', 'gh', 'git', 'wget']:
            dest = self.bin/name
            dest.write_text('#!/bin/sh\necho "'+name+'" >> "$TEST_FORBIDDEN"\nexit 97\n')
            dest.chmod(0o755)
        self.env = {'PATH':str(self.bin)+':/usr/bin:/bin', 'LANG':'C.UTF-8',
                    'TMPDIR':str(self.root), 'TEST_FORBIDDEN':str(self.calls),
                    'PYTHONDONTWRITEBYTECODE':'1'}

    def tearDown(self):
        self.temporary.cleanup()

    def run_cmd(self, command, env=None):
        result = subprocess.run(command, cwd=self.root, env={**self.env, **(env or {})},
                                capture_output=True, text=True, timeout=10)
        self.assertNotIn(SECRET, result.stdout+result.stderr)
        self.assertFalse(self.calls.exists(), self.calls.read_text() if self.calls.exists() else '')
        return result

    def test_local_availability_and_self_test_never_make_network_calls(self):
        test_file = self.root/'token-file'
        test_file.write_text('  '+SECRET+' \n')
        cases = [({}, 2), ({'REQUIRE_TOKEN':'1'}, 2),
                 ({'CARGO_REGISTRY_TOKEN':'   '}, 2),
                 ({'CARGO_REGISTRY_TOKEN':SECRET}, 3),
                 ({'CARGO_REGISTRY_TOKEN':'   ', 'CARGO_REGISTRY_TOKEN_FILE':str(test_file)}, 3),
                 ({'CARGO_REGISTRY_TOKEN_FILE':str(self.root/'missing')}, 1),
                 ({'CARGO_REGISTRY_TOKEN_FILE':str(self.root)}, 1)]
        for env, expected in cases:
            with self.subTest(env=list(env), expected=expected):
                r = self.run_cmd(['bash','scripts/check-registry-token.sh'], env)
                self.assertEqual(r.returncode, expected, r.stdout+r.stderr)
                if expected == 3:
                    self.assertIn('unverified', r.stdout)
                    self.assertNotIn('accepted', r.stdout)
        r = self.run_cmd(['bash','scripts/check-registry-token.sh','--self-test'],
                         {'CARGO_REGISTRY_TOKEN':SECRET})
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertIn('self-test OK', r.stdout)
        self.assertEqual(self.run_cmd(['bash','scripts/check-registry-token.sh','--bad']).returncode, 1)

    def test_input_and_read_errors_fail_closed_in_shared_parent_helper(self):
        command = ('set -euo pipefail\nsource scripts/lib/cargo-registry-token.sh\n'
                   'pl_prepare_cargo_registry_token test\necho CONTINUED\n')
        r = self.run_cmd(['bash','-c',command], {'CARGO_REGISTRY_TOKEN_FILE':str(self.root/'missing')})
        self.assertNotEqual(r.returncode, 0)
        self.assertNotIn('CONTINUED',r.stdout)
        token_file=self.root/'token-file'
        token_file.write_text(' '+SECRET+' \n')
        r = self.run_cmd(['bash','-c',command], {'CARGO_REGISTRY_TOKEN':' ', 'CARGO_REGISTRY_TOKEN_FILE':str(token_file)})
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertIn('CONTINUED',r.stdout)
        tr=self.bin/'tr'
        tr.write_text('#!/bin/sh\nexit 1\n')
        tr.chmod(0o755)
        r = self.run_cmd(['bash','-c',command], {'CARGO_REGISTRY_TOKEN_FILE':str(token_file)})
        self.assertNotEqual(r.returncode, 0)
        self.assertNotIn('CONTINUED',r.stdout)

    def test_preflight_self_test_does_not_read_inherited_token_file(self):
        token_file=self.root/'inherited-token'
        token_file.write_text(SECRET)
        path=self.root/'scripts/check-installable-preflight.sh'
        path.write_text(path.read_text().replace('/tmp/pl-preflight-',str(self.root/'pl-preflight-')))
        r=self.run_cmd(['bash','scripts/check-installable-preflight.sh','--self-test'],
                       {'CARGO_REGISTRY_TOKEN_FILE':str(token_file)})
        self.assertEqual(r.returncode,0,r.stdout+r.stderr)
        self.assertIn('self-test OK',r.stdout)

    def fragment(self, name, start, end, prefix='', extra_env=None):
        text = (ROOT/'scripts'/name).read_text()
        first = text.index(start)
        last = text.index(end, first)
        fragment = text[first:last].replace('/tmp/pl-', str(self.root/'pl-'))
        script = 'set -euo pipefail\nROOT="$1"\n'+prefix+fragment+'\necho CONTINUED\n'
        return self.run_cmd(['bash','-c',script,'--',str(self.root)], extra_env)

    def stub_status(self, status):
        (self.root/'scripts/check-registry-token.sh').write_text('exit '+str(status)+'\n')

    def test_preflight_and_finish_accept_present_but_fail_on_check_errors(self):
        for status in [0,1,2,3,99]:
            with self.subTest(status=status):
                self.stub_status(status)
                env={'CARGO_REGISTRY_TOKEN':SECRET} if status==3 else {}
                pre=self.fragment('check-installable-preflight.sh', 'tok_rc=0\n',
                    '\necho "== post-cut parks stack',extra_env=env)
                if status==3:
                    self.assertEqual(pre.returncode,0,pre.stderr)
                    self.assertIn('CONTINUED',pre.stdout)
                elif status==2:
                    self.assertEqual(pre.returncode,0,pre.stderr)
                    self.assertIn('READY_EXCEPT_TOKEN',pre.stdout)
                    self.assertNotIn('CONTINUED',pre.stdout)
                else:
                    self.assertEqual(pre.returncode,1,pre.stdout+pre.stderr)
                finish=self.fragment('owner-finish-installable.sh', '# Load TOKEN_FILE',
                    '\necho\necho "== 2) Merge/tag readiness',prefix='DRY_RUN=1\n',extra_env=env)
                if status in [2,3]:
                    self.assertEqual(finish.returncode,0,finish.stdout+finish.stderr)
                    self.assertIn('CONTINUED',finish.stdout)
                else:
                    self.assertEqual(finish.returncode,1,finish.stdout+finish.stderr)
                self.assertNotIn('accepted for',finish.stdout)
        self.stub_status(3)
        r=self.fragment('check-installable-preflight.sh','tok_rc=0\n',
                        '\necho "== post-cut parks stack')
        self.assertEqual(r.returncode,1)

    def test_preflight_rejects_unexpected_ci_exit_codes(self):
        for status in [0,1,2,3,99]:
            with self.subTest(status=status):
                (self.root/'scripts/check-main-ci.sh').write_text('exit '+str(status)+'\n')
                r=self.fragment('check-installable-preflight.sh','ci_rc=0\n',
                    '\necho\nif [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]',
                    prefix='SKIP_MAIN_CI=0\nREQUIRE_MAIN_CI=1\n')
                self.assertEqual(r.returncode,0 if status==0 else 1,r.stdout+r.stderr)
                self.assertEqual('CONTINUED' in r.stdout,status==0)

    def test_preflight_uses_live_main_profile_and_skip_is_inconclusive(self):
        probe=self.root/'scripts/check-main-ci.sh'
        probe.write_text('''#!/bin/bash
for name in CHECK_SHA GH_RUNS_JSON GH_JOBS_JSON GH_ARTIFACTS_JSON REQUIRED_JOBS REQUIRED_ARTIFACTS GH_RUN_LIMIT; do
  if [[ -v $name ]]; then echo "fixture override leaked: $name" >&2; exit 19; fi
done
[[ "$MAIN_BRANCH" == main && "$REQUIRE_PROFILE_EVIDENCE" == 1 && "$REQUIRE_MAIN_CI" == 1 ]] || exit 20
echo LIVE_MAIN_PROFILE
''')
        values={name:'fixture-value' for name in ['CHECK_SHA','GH_RUNS_JSON','GH_JOBS_JSON',
            'GH_ARTIFACTS_JSON','REQUIRED_JOBS','REQUIRED_ARTIFACTS','GH_RUN_LIMIT']}
        values.update(MAIN_BRANCH='other',REQUIRE_PROFILE_EVIDENCE='0')
        start='ci_rc=0\n'
        end='\necho\nif [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]'
        r=self.fragment('check-installable-preflight.sh',start,end,
                        prefix='SKIP_MAIN_CI=0\nREQUIRE_MAIN_CI=1\n',extra_env=values)
        self.assertEqual(r.returncode,0,r.stdout+r.stderr)
        self.assertIn('LIVE_MAIN_PROFILE',r.stdout)
        r=self.fragment('check-installable-preflight.sh',start,end,
                        prefix='SKIP_MAIN_CI=1\nREQUIRE_MAIN_CI=0\n')
        self.assertEqual(r.returncode,3,r.stdout+r.stderr)
        self.assertNotIn('CONTINUED',r.stdout)
        r=self.fragment('check-installable-preflight.sh',start,end,
                        prefix='SKIP_MAIN_CI=1\nREQUIRE_MAIN_CI=1\n')
        self.assertEqual(r.returncode,1,r.stdout+r.stderr)

    def test_cut_callers_fail_closed_for_unexpected_token_statuses(self):
        for status in [0,1,2,3,99]:
            with self.subTest(status=status):
                self.stub_status(status)
                check=self.fragment('check-cut-path.sh','tok_rc=0\n',
                                    '\necho\necho "== check-cut-path: tip-delta')
                self.assertEqual(check.returncode,0 if status in [2,3] else 1,check.stdout+check.stderr)
                cut=self.fragment('owner-cut-release.sh','  # Check availability only',
                    '  if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]',
                    extra_env={'CARGO_REGISTRY_TOKEN':SECRET})
                self.assertEqual(cut.returncode,0 if status==3 else 1,cut.stdout+cut.stderr)
                if status==3:
                    self.assertIn('unverified',cut.stdout)
                    self.assertIn('CONTINUED',cut.stdout)

    def test_reporting_callers_never_infer_authorization(self):
        cases = [
          ('check-merge-ready.sh','tok_rc=0\n','\n# shellcheck source=scripts/lib/crates-io.sh',
           'ok(){ echo "PASS $*"; }; warn(){ echo "WARN $*"; }; bad(){ echo "FAIL $*"; exit 1; }\n'),
          ('owner-status.sh','tok_rc=0\n','\n# Compact preflight verdict',''),
        ]
        # The final token-status case in owner-request ends at EOF.
        request=ROOT/'scripts/owner-request-registry-token.sh'
        request_text=request.read_text(); start=request_text.rindex('tok_rc=0\n')
        req_fragment=request_text[start:].replace('/tmp/pl-',str(self.root/'pl-'))
        for status in [0,1,2,3,99]:
            with self.subTest(status=status):
                self.stub_status(status)
                for name,start,end,prefix in cases:
                    r=self.fragment(name,start,end,prefix=prefix)
                    if name=='check-merge-ready.sh':
                        self.assertEqual(r.returncode,0 if status in [2,3] else 1,r.stdout+r.stderr)
                    self.assertNotIn('accepted by crates.io',r.stdout)
                    self.assertNotIn('rejected by crates.io',r.stdout)
                    if status==3:
                        self.assertIn('unverified',r.stdout)
                r=self.run_cmd(['bash','-c','set -euo pipefail\n'+req_fragment])
                self.assertEqual(r.returncode,0 if status in [2,3] else 1,r.stdout+r.stderr)
                if status==3:self.assertIn('unverified',r.stdout)
                self.assertNotIn('accepted for',r.stdout)
                self.assertNotIn('rejected it',r.stdout)

    def test_audit_reports_scope_unverified_not_authorized(self):
        functions=('PL_CRATES_IDENTITY_STATUS=absent\nPL_CRATES_PROBE_STATUS=absent\n'
                   'ok(){ echo "PASS $*"; }; part(){ echo "PARTIAL $*"; }; '
                   'iblk(){ echo "BLOCKED $*"; }; bad(){ echo "FAIL $*"; exit 1; }\n')
        for status in [0,1,2,3,99]:
            with self.subTest(status=status):
                self.stub_status(status)
                r=self.fragment('audit-civilization-bars.sh','tok_rc=0\n',
                                '\n# --- 2. Verifiable',prefix=functions)
                self.assertEqual(r.returncode,0 if status in [2,3] else 1,r.stdout+r.stderr)
                if status==3:
                    self.assertIn('PARTIAL',r.stdout)
                    self.assertIn('unverified',r.stdout)
                self.assertNotIn('accepted by crates.io',r.stdout)

    def test_owner_guidance_distinguishes_token_present_from_preflight_error(self):
        (self.root/'Cargo.toml').write_text('[package]\nname = "partitionline"\nversion = "0.2.0"\n')
        # No real Git, Cargo, GitHub, or network is used by these full scripts.
        (self.bin/'git').write_text('#!/bin/sh\nexit 1\n')
        (self.bin/'git').chmod(0o755)
        for name in ['owner-status.sh','check-actions-hygiene.sh','owner-cancel-stuck-runs.sh',
                     'check-merge-ready.sh','check-post-cut-parks-stack.sh']:
            (self.root/'scripts'/name).write_text('exit 0\n')
        for name in ['owner-request-registry-token.sh','owner-unblock.sh']:
            path=self.root/'scripts'/name
            path.write_text(path.read_text().replace('/tmp/pl-',str(self.root/'pl-')))
        for status, body in [(0,'READY — token present; scope unverified'), (1,'registry unknown')]:
            with self.subTest(status=status):
                (self.root/'scripts/check-installable-preflight.sh').write_text(
                    'echo "'+body+'"\nexit '+str(status)+'\n')
                for name in ['owner-request-registry-token.sh','owner-unblock.sh']:
                    r=self.run_cmd(['bash','scripts/'+name])
                    self.assertEqual(r.returncode,0 if status==0 else 1,r.stdout+r.stderr)
                    if status==0:self.assertIn('unverified',r.stdout)
                    self.assertNotIn('accepted for publish-new',r.stdout)


if __name__=='__main__':
    unittest.main()
